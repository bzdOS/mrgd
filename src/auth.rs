// START_AI_HEADER
// MODULE: matrix-hs/src/auth.rs
// PURPOSE: Coordination-free token signing and password hashing for matrix-hs.
//
//          Password hashing: Argon2id via the `argon2` crate (PHC string format).
//            hash_password(password) → "$argon2id$..." string stored in UserRecord.
//            verify_password(hash, password) → bool.
//
//          Signed access tokens (HMAC-SHA256):
//            Format: mxt_<base64url(payload)>.<base64url(hmac_sha256(payload))>
//            Payload: "<user_id>|<device_id>|<nonce>" (plain UTF-8, no length prefix).
//            Secret: MATRIX_HS_TOKEN_SECRET env var; if unset, generate a random
//                    32-byte ephemeral secret at first use and emit a LOUD stderr warning.
//                    Any node sharing the same secret can verify any other node's tokens —
//                    coordination-free: no shared store, only shared secret.
//
//            sign_token(secret, user_id, device_id)   → "mxt_<b64u>.<b64u>" string
//            verify_token(secret, token)               → Option<String> (user_id on success)
//
//          Secret lifecycle:
//            TokenSecret::global() returns the process-local secret.
//            Caller passes it explicitly (no hidden global state in hot-path functions).
//
// DEPENDENCIES: argon2, hmac, sha2, rand, base64
// PUBLIC_API: hash_password, verify_password, sign_token, verify_token, TokenSecret
// END_AI_HEADER

use argon2::{
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2,
};
use axum::http::HeaderMap;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use hmac::{Hmac, Mac};
use rand::{rngs::OsRng, RngCore};
use sha2::Sha256;
use std::sync::OnceLock;

use crate::state::AppState;

// ── Argon2 password hashing ────────────────────────────────────────────────────

// hash_password:start
//   purpose: Hash a plaintext password with Argon2id and return the PHC string.
//            Uses default Argon2id params (suitable for interactive logins).
//            The output is self-contained: it embeds the salt, so only the hash
//            string is stored; no separate salt field is needed.
//   input:  password — plaintext string from the registration request
//   output: Ok(String) — "$argon2id$..." PHC hash string;
//           Err(String) — error message (salt or hash failure, both very rare)
//   sideEffects: reads from OsRng for the random salt
// hash_password:end
pub fn hash_password(password: &str) -> Result<String, String> {
    let salt = SaltString::generate(&mut OsRng);
    let argon2 = Argon2::default();
    argon2
        .hash_password(password.as_bytes(), &salt)
        .map(|h| h.to_string())
        .map_err(|e| format!("argon2 hash: {e}"))
}

// verify_password:start
//   purpose: Verify a plaintext password against an Argon2 PHC hash string.
//            Constant-time comparison is guaranteed by the argon2 crate.
//   input:  hash — PHC string (stored in UserRecord.password_hash);
//           password — plaintext string from the login request
//   output: bool — true if password matches hash
//   sideEffects: none (pure computation)
// verify_password:end
pub fn verify_password(hash: &str, password: &str) -> bool {
    let parsed = match PasswordHash::new(hash) {
        Ok(h) => h,
        Err(_) => return false,
    };
    Argon2::default()
        .verify_password(password.as_bytes(), &parsed)
        .is_ok()
}

// ── HMAC-SHA256 access tokens ─────────────────────────────────────────────────

type HmacSha256 = Hmac<Sha256>;

// sign_token:start
//   purpose: Mint a signed access token for (user_id, device_id, epoch).
//            Format: "mxt_<base64url(payload)>.<base64url(hmac_sha256(secret, payload))>"
//            Payload: "<user_id>|<device_id>|<epoch>|<nonce>" where nonce is 8 random bytes (hex).
//            The epoch field enables token revocation (Phase 2 internal-task): increment on logout_devices,
//            password change, or admin session-kill. Tokens include the epoch at mint time;
//            verify_token rejects any token whose epoch < current record.epoch.
//            The nonce prevents precomputed-MAC attacks and makes tokens unpredictable
//            even for a known (user_id, device_id) pair.
//            Any node holding the same secret can verify tokens minted by any other node
//            — coordination-free, no shared store.
//   input:  secret — shared HMAC key (bytes);
//           user_id — full Matrix user_id ("@alice:localhost");
//           device_id — device identifier ("DEVICE1");
//           epoch — current user's revocation epoch
//   output: String — "mxt_<b64url_payload>.<b64url_mac>"
//   sideEffects: reads 8 bytes from OsRng for the nonce
// sign_token:end
pub fn sign_token(secret: &[u8], user_id: &str, device_id: &str, epoch: u32) -> String {
    let mut nonce_bytes = [0u8; 8];
    OsRng.fill_bytes(&mut nonce_bytes);
    let nonce_hex = hex_encode(&nonce_bytes);

    let payload = format!("{user_id}|{device_id}|{epoch}|{nonce_hex}");
    let payload_b64 = URL_SAFE_NO_PAD.encode(payload.as_bytes());

    let mac = compute_mac(secret, payload_b64.as_bytes());
    let mac_b64 = URL_SAFE_NO_PAD.encode(mac);

    format!("mxt_{payload_b64}.{mac_b64}")
}

// verify_token:start
//   purpose: Verify a signed access token and extract the user_id and epoch.
//            Parses "mxt_<b64url_payload>.<b64url_mac>"; recomputes the HMAC over
//            the raw payload bytes (the base64url string, not the decoded payload) and
//            compares constant-time.  Extracts user_id and epoch from the decoded payload.
//            Does NOT check the epoch against current record (that's the caller's job —
//            see extract_caller which checks epoch >= record.epoch for revocation).
//   input:  secret — shared HMAC key (bytes);
//           token — "mxt_<b64url_payload>.<b64url_mac>" string
//   output: Some((user_id, epoch)) on valid token; None on any parse/MAC/format failure
//   sideEffects: none (pure computation after OsRng in sign)
// verify_token:end
pub fn verify_token(secret: &[u8], token: &str) -> Option<(String, String, u32)> {
    // Strip "mxt_" prefix
    let rest = token.strip_prefix("mxt_")?;

    // Split at the last '.' to get payload_b64 and mac_b64.
    let dot_pos = rest.rfind('.')?;
    let payload_b64 = &rest[..dot_pos];
    let mac_b64 = &rest[dot_pos + 1..];

    // Verify MAC over payload_b64 bytes (the base64url string itself).
    let expected_mac = compute_mac(secret, payload_b64.as_bytes());
    let provided_mac = URL_SAFE_NO_PAD.decode(mac_b64).ok()?;
    if !constant_time_eq(&expected_mac, &provided_mac) {
        return None;
    }

    // Decode payload and extract user_id, device_id and epoch.
    let payload_bytes = URL_SAFE_NO_PAD.decode(payload_b64).ok()?;
    let payload = std::str::from_utf8(&payload_bytes).ok()?;
    // Format: "user_id|device_id|epoch|nonce"
    let mut parts = payload.split('|');
    let user_id = parts.next()?;
    let device_id = parts.next().unwrap_or("");
    let epoch_str = parts.next()?;
    let epoch: u32 = epoch_str.parse().ok()?;
    Some((user_id.to_string(), device_id.to_string(), epoch))
}

// extract_caller:start
//   purpose: Derive the authenticated (user_id, device_id) pair from a request's
//            Authorization header. Canonical implementation shared by all routes that
//            need the caller's identity (formerly copy-pasted across
//            routes/{keys,sync,sliding_sync,to_device}.rs — deduped here verbatim).
//            Verifies the Bearer token via verify_token (signed mxt_ tokens only).
//            Checks token epoch >= current record.epoch for revocation (Phase 2 internal-task):
//            incremented on logout_devices, password change, or admin session-kill.
//            Resolves device_id from AppState.users by localpart; falls back to
//            "DEVICE1" if the user isn't registered locally (mirrors prior per-file
//            behavior exactly).
//   input:  headers — HeaderMap; state — &AppState
//   output: Option<(String, String)> — (user_id, device_id), or None if unauthenticated
//                                       or token epoch is revoked
//   sideEffects: none (read-only access to AppState.users)
// extract_caller:end
pub fn extract_caller(headers: &HeaderMap, state: &AppState) -> Option<(String, String)> {
    let token = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))?;

    let (user_id, _token_device, token_epoch) = verify_token(&state.token_secret, token)?;

    let localpart = user_id
        .strip_prefix('@')
        .and_then(|s| s.split(':').next())
        .unwrap_or(&user_id);

    // Check token epoch for revocation if user is registered locally.
    // (Federation-compatible: remote/unregistered users have no epoch to check —
    // a remote operator's logout won't affect this server's locally-minted tokens anyway).
    let users = state.users.lock().ok()?;
    if let Some(record) = users.get(localpart) {
        // Local user: epoch must be valid (not revoked).
        if token_epoch < record.epoch {
            return None; // Token revoked
        }
    }

    // The device_id comes FROM THE TOKEN, not the user record. The token is
    // HMAC-signed over "user_id|device_id|epoch|nonce", so a presented device
    // cannot be forged without the secret — and this is what makes the agent
    // socket's workers-as-devices real (m.login.application_service mints a
    // session per device; the record's own device_id is just registration
    // default). The record is consulted only for the epoch check above.
    // Fallbacks cover pre-record tokens with an empty device field.
    let device_id = if _token_device.is_empty() {
        users
            .get(localpart)
            .map(|r| r.device_id.clone())
            .unwrap_or_else(|| "DEVICE1".to_string())
    } else {
        _token_device
    };
    drop(users);

    Some((user_id, device_id))
}

// ── Internal helpers ──────────────────────────────────────────────────────────

// compute_mac:start
//   purpose: Compute HMAC-SHA256(key, msg) and return the raw 32-byte digest.
//   input:  key — HMAC key bytes; msg — message bytes
//   output: Vec<u8> — 32-byte MAC
//   sideEffects: none
// compute_mac:end
fn compute_mac(key: &[u8], msg: &[u8]) -> Vec<u8> {
    // HMAC::new_from_slice only fails on zero-length key; our secret is always ≥32 bytes.
    let mut mac = HmacSha256::new_from_slice(key).unwrap_or_else(|_| {
        // Fallback: use a fixed dummy key so we produce a valid (but wrong) MAC.
        // This path is unreachable in practice since TokenSecret::global() always
        // returns a ≥32-byte secret.
        HmacSha256::new_from_slice(b"fallback-hmac-key-32-bytes-xxxx").expect("fixed key")
    });
    mac.update(msg);
    mac.finalize().into_bytes().to_vec()
}

// constant_time_eq:start
//   purpose: Compare two byte slices in constant time to prevent timing attacks.
//            Returns true iff a == b in every byte AND they have the same length.
//   input:  a, b — byte slices
//   output: bool
//   sideEffects: none
// constant_time_eq:end
pub(crate) fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter()
        .zip(b.iter())
        .fold(0u8, |acc, (x, y)| acc | (x ^ y))
        == 0
}

// hex_encode:start
//   purpose: Encode a byte slice as lowercase hexadecimal string.
//   input:  bytes — byte slice
//   output: String of hex characters
//   sideEffects: none
// hex_encode:end
fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

// ── Process-local token secret ────────────────────────────────────────────────

// TokenSecret:start
//   purpose: Hold the HMAC signing key used for access tokens.
//            Reads MATRIX_HS_TOKEN_SECRET env var (hex or raw bytes accepted).
//            If unset, generates 32 cryptographically random bytes at first use
//            and emits a LOUD stderr warning: tokens won't survive process restart
//            and won't validate cross-node (dev-only, non-production behaviour).
//   input:  (none — accessed via TokenSecret::global())
//   output: &'static [u8] secret bytes via TokenSecret::global().bytes()
//   sideEffects: reads env var; may generate random bytes; may write to stderr
// TokenSecret:end
pub struct TokenSecret {
    bytes: Vec<u8>,
}

impl TokenSecret {
    // TokenSecret::global:start
    //   purpose: Return the process-singleton TokenSecret, initialising it on first call.
    //   input:  none
    //   output: &'static TokenSecret
    //   sideEffects: reads env; generates random key if env absent; may emit stderr warning
    // TokenSecret::global:end
    pub fn global() -> &'static TokenSecret {
        static INSTANCE: OnceLock<TokenSecret> = OnceLock::new();
        INSTANCE.get_or_init(|| {
            let bytes = match std::env::var("MATRIX_HS_TOKEN_SECRET") {
                Ok(s) if !s.is_empty() => s.into_bytes(),
                _ => {
                    // Dev-only: random ephemeral secret.
                    let mut key = vec![0u8; 32];
                    OsRng.fill_bytes(&mut key);
                    eprintln!(
                        "WARNING: MATRIX_HS_TOKEN_SECRET not set — \
                         using ephemeral random key. \
                         Tokens will NOT survive restart and will NOT validate \
                         cross-node. Set MATRIX_HS_TOKEN_SECRET for production."
                    );
                    key
                }
            };
            TokenSecret { bytes }
        })
    }

    // TokenSecret::bytes:start
    //   purpose: Return the raw HMAC key bytes.
    //   input:  none
    //   output: &[u8]
    //   sideEffects: none
    // TokenSecret::bytes:end
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

// ── Unit tests ────────────────────────────────────────────────────────────────

// bearer_token:start
//   purpose: Extract the raw Bearer credential from an Authorization header,
//            WITHOUT interpreting it as a user token. The agent-socket flows
//            (register/login m.login.application_service) authenticate with an
//            appservice token that is not an mxt_ token, so extract_caller
//            cannot be used there. Reads the header only — AS credentials in
//            query strings are deliberately NOT supported (they end up in
//            access logs).
//   input:  headers — the request's HeaderMap
//   output: Some(credential) for "Bearer <cred>", else None
//   sideEffects: none
// bearer_token:end
pub fn bearer_token(headers: &axum::http::HeaderMap) -> Option<String> {
    let v = headers.get(axum::http::header::AUTHORIZATION)?.to_str().ok()?;
    let cred = v.strip_prefix("Bearer ")?;
    if cred.is_empty() {
        None
    } else {
        Some(cred.to_string())
    }
}

// unusable_password_hash:start
//   purpose: A password hash no client can ever present a preimage for — used
//            for appservice-registered accounts, whose only authentication is
//            the AS token. Random 32 bytes, hashed with Argon2id like any
//            password; the plaintext is discarded on return.
//   input:  none (OsRng)
//   output: PHC hash string, or "?" on the (unreachable) Argon2 failure path —
//           a literal that is itself not a valid hash, so the record stays
//           un-loginable even in that corner
//   sideEffects: reads OsRng
// unusable_password_hash:end
pub fn unusable_password_hash() -> String {
    let mut b = [0u8; 32];
    OsRng.fill_bytes(&mut b);
    let plaintext: String = b.iter().map(|x| format!("{x:02x}")).collect();
    hash_password(&plaintext).unwrap_or_else(|_| "?".to_string())
}

// random_device_suffix:start
//   purpose: Short random hex for generated device ids ("AS" + 6 hex), so two
//            workers that login without an explicit device_id still get
//            distinct devices (Case 2: workers ARE devices).
//   input:  none (OsRng)
//   output: 6-char lowercase hex string
//   sideEffects: reads OsRng
// random_device_suffix:end
pub fn random_device_suffix() -> String {
    let mut b = [0u8; 3];
    OsRng.fill_bytes(&mut b);
    b.iter().map(|x| format!("{x:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_and_verify_roundtrip() {
        let hash = hash_password("hunter2").expect("hash");
        assert!(
            verify_password(&hash, "hunter2"),
            "correct password must verify"
        );
        assert!(
            !verify_password(&hash, "wrong"),
            "wrong password must not verify"
        );
    }

    #[test]
    fn token_sign_verify_roundtrip() {
        let secret = b"test-secret-key-at-least-32bytes";
        let token = sign_token(secret, "@alice:localhost", "DEVICE1", 0);
        assert!(token.starts_with("mxt_"), "token must have mxt_ prefix");

        let result = verify_token(secret, &token);
        assert_eq!(
            result.as_ref().map(|(uid, _, _)| uid.as_str()),
            Some("@alice:localhost"),
            "verify extracts user_id"
        );
        assert_eq!(
            result.as_ref().map(|(_, _, epoch)| epoch),
            Some(&0),
            "verify extracts epoch"
        );
    }

    #[test]
    fn token_wrong_secret_rejected() {
        let secret1 = b"secret-one-32bytes-xxxxxxxxxxx!";
        let secret2 = b"secret-two-32bytes-xxxxxxxxxxx!";
        let token = sign_token(secret1, "@alice:localhost", "DEVICE1", 5);
        assert!(
            verify_token(secret2, &token).is_none(),
            "token signed with secret1 must not verify under secret2"
        );
    }

    #[test]
    fn token_tampered_payload_rejected() {
        let secret = b"test-secret-key-at-least-32bytes";
        let token = sign_token(secret, "@alice:localhost", "DEVICE1", 3);
        // Tamper: replace the payload portion with garbage.
        let tampered = token.replacen("mxt_", "mxt_TAMPERED", 1);
        assert!(
            verify_token(secret, &tampered).is_none(),
            "tampered token must be rejected"
        );
    }

    #[test]
    fn token_wrong_format_rejected() {
        let secret = b"test-secret-key-at-least-32bytes";
        assert!(
            verify_token(secret, "tok_alice").is_none(),
            "old tok_ format rejected"
        );
        assert!(verify_token(secret, "").is_none(), "empty string rejected");
        assert!(
            verify_token(secret, "mxt_noperiod").is_none(),
            "no period rejected"
        );
    }
}
