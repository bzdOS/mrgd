// START_AI_HEADER
// MODULE: mac-companion/zenoh-link-obfs/src/crypto.rs
// PURPOSE: AEAD and handshake primitives for the DPI-resistant obfs transport (SS-AEAD style, X25519 + PSK + Elligator2 + TLS camouflage).
// INTENT: Provide a self-contained crypto layer that can be reused by other transports; separates the AEAD framing, key derivation, and Elligator2 handshake from the Zenoh link integration.
// DEPENDENCIES: chacha20poly1305, curve25519_elligator2, hkdf, sha2, rand_core (OsRng), tokio (AsyncRead/AsyncWrite), x25519-dalek, zenoh_result
// PUBLIC_API: PreSharedKey, PreSharedKey::from_bytes, StaticServerKey, StaticServerKey::derive_from_psk, server_static_public_from_psk, ObfsStream, ObfsStream::connect/accept/write_frames/flush/read_into/read_exact_into/inner_mut, KEY_LEN, MAX_PAYLOAD
// END_AI_HEADER

//
// bsdOS — zenoh-link-obfs
//
// DPI-resistant AEAD transport layer (SS-AEAD style) over a raw TcpStream.
// Implements the crypto design from PLAN-dpi-transport.md §4.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// NOTE: We do NOT roll our own crypto. All primitives come from audited crates:
//   - x25519-dalek           (ephemeral ECDH)
//   - curve25519-elligator2  (Elligator2 uniform-representative encoding)
//   - chacha20poly1305       (AEAD frame encryption)
//   - hkdf + sha2            (key derivation)
//   - rand_core              (CSPRNG for ephemeral keys)
//
// ELLIGATOR2 HANDSHAKE (passive-DPI hardening):
//
// Raw X25519 ephemeral public keys are NOT uniformly random: they are canonical
// Montgomery u-coordinates < 2^255-19, so the MSB of byte 31 is always 0 and
// the values are statistically distinguishable from random. A passive DPI box
// can classify these on the first 32 bytes of every new connection.
//
// We instead send/receive the Elligator2 *representative* of each ephemeral
// pubkey — a 32-byte value that is computationally indistinguishable from
// uniform random. Only ~50% of X25519 keys have a valid representative;
// gen_ephemeral_elligator() retries until it finds one (≈1.4 draws on average).
// The receiving peer decodes the representative back to the Montgomery point
// before performing DH. The DH computation and all subsequent key derivation
// are identical to the previous design; only the on-wire encoding changes.
//
// Tweak byte: a random byte used by representative_from_privkey() to select
// which of the two Elligator2 pre-images to emit. The crate already folds the
// tweak into the two high-order bits of the representative (standard Elligator2
// bit-stuffing); from_representative::<RFC9380> clears those bits on decode, so
// the tweak is NOT transmitted on the wire. Each ephemeral message is exactly
// 32 bytes — the representative itself, uniformly random end-to-end.
//
use core::fmt;

// hex_repr:start
//   purpose: Format a 32-byte array as a lowercase hex string for debug logging.
//   input:  b: &[u8; 32] — the byte slice to format
//   output: String — hex-encoded representation
//   sideEffects: none
fn hex_repr(b: &[u8; 32]) -> String {
    b.iter().map(|x| format!("{:02x}", x)).collect()
}
// hex_repr:end

// ---------------------------------------------------------------------------
// TLS camouflage — wraps the 32-byte Elligator2 representatives in a minimal
// but structurally valid TLS 1.3 ClientHello / ServerHello so ТСПУ passes the
// connection instead of injecting RST on non-TLS first bytes to port 443.
//
// Wire protocol (both sides):
//   client→server: TLS Handshake record containing ClientHello
//                  client_random = client Elligator2 representative (32 bytes)
//   server→client: TLS Handshake record containing ServerHello
//                  server_random = server Elligator2 representative (32 bytes)
//
// After this exchange the session key is derived exactly as before from the
// two Elligator2 representatives. No real TLS session is established — the
// fake handshake is purely cosmetic for DPI.
// ---------------------------------------------------------------------------

const SNI_HOST: &[u8] = b"selectel.ru";

// tls_client_hello:start
//   purpose: Build a structurally valid TLS 1.3 ClientHello record embedding the client Elligator2 representative as client_random.
//   input:  random: &[u8; 32] — client's Elligator2 representative to embed in the fake ClientHello random field
//   output: Vec<u8> — the complete TLS record bytes
//   sideEffects: none
fn tls_client_hello(random: &[u8; 32]) -> Vec<u8> {
    let sni_len = SNI_HOST.len() as u16;
    // SNI extension: type(2) + ext_data_len(2) + list_len(2) + name_type(1) + name_len(2) + name
    let list_len: u16 = 1 + 2 + sni_len;
    let ext_data_len: u16 = 2 + list_len;
    let mut sni_ext = Vec::with_capacity(2 + 2 + 2 + 1 + 2 + sni_len as usize);
    sni_ext.extend_from_slice(&[0x00, 0x00]);                   // ExtensionType: server_name
    sni_ext.extend_from_slice(&ext_data_len.to_be_bytes());
    sni_ext.extend_from_slice(&list_len.to_be_bytes());
    sni_ext.push(0x00);                                          // name_type: host_name
    sni_ext.extend_from_slice(&sni_len.to_be_bytes());
    sni_ext.extend_from_slice(SNI_HOST);

    // cipher_suites
    let cs: &[u8] = &[
        0x13, 0x01, // TLS_AES_128_GCM_SHA256
        0x13, 0x02, // TLS_AES_256_GCM_SHA384
        0x13, 0x03, // TLS_CHACHA20_POLY1305_SHA256
        0xc0, 0x2b, // TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256
        0xc0, 0x2c, // TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384
        0x00, 0xff, // SCSV
    ];

    // other extensions: supported_versions, supported_groups, ec_point_formats
    let other: &[u8] = &[
        0x00, 0x2b, 0x00, 0x03, 0x02, 0x03, 0x04,             // supported_versions: TLS 1.3
        0x00, 0x0a, 0x00, 0x06, 0x00, 0x04, 0x00, 0x1d, 0x00, 0x17, // supported_groups: x25519, secp256r1
        0x00, 0x0b, 0x00, 0x02, 0x01, 0x00,                    // ec_point_formats: uncompressed
    ];

    let all_exts_len = sni_ext.len() + other.len();
    // body = version(2) + random(32) + sid_len(1) + cs_len(2) + cs + comp_len(1) + comp(1) + exts_len(2) + exts
    let body_len = 2 + 32 + 1 + 2 + cs.len() + 1 + 1 + 2 + all_exts_len;

    let mut out = Vec::with_capacity(5 + 4 + body_len);
    out.push(0x16);                                              // ContentType: Handshake
    out.push(0x03); out.push(0x01);                             // outer version: TLS 1.0 (like Chrome)
    out.extend_from_slice(&((4 + body_len) as u16).to_be_bytes()); // record length
    out.push(0x01);                                              // HandshakeType: ClientHello
    out.push(0x00);                                              // length[0] (3-byte, high byte)
    out.push(((body_len >> 8) & 0xff) as u8);
    out.push((body_len & 0xff) as u8);
    out.push(0x03); out.push(0x03);  // client_version: TLS 1.2
    out.extend_from_slice(random);   // client_random = Elligator2 repr
    out.push(0x00);                  // session_id_length = 0
    out.extend_from_slice(&(cs.len() as u16).to_be_bytes());
    out.extend_from_slice(cs);
    out.push(0x01); out.push(0x00);  // compression_methods: [null]
    out.extend_from_slice(&(all_exts_len as u16).to_be_bytes());
    out.extend_from_slice(&sni_ext);
    out.extend_from_slice(other);
    out
}
// tls_client_hello:end

// tls_server_hello:start
//   purpose: Build a structurally valid TLS 1.3 ServerHello record embedding the server Elligator2 representative as server_random.
//   input:  random: &[u8; 32] — server's Elligator2 representative to embed in the fake ServerHello random field
//   output: Vec<u8> — the complete TLS record bytes with random session ID
//   sideEffects: Draws 32 random session ID bytes from OsRng
fn tls_server_hello(random: &[u8; 32]) -> Vec<u8> {
    let mut session_id = [0u8; 32];
    OsRng.fill_bytes(&mut session_id);

    let exts: &[u8] = &[
        0x00, 0x2b, 0x00, 0x02, 0x03, 0x04, // supported_versions: TLS 1.3
    ];
    // body = version(2) + random(32) + sid_len(1) + sid(32) + cipher(2) + comp(1) + exts_len(2) + exts
    let body_len = 2 + 32 + 1 + 32 + 2 + 1 + 2 + exts.len();

    let mut out = Vec::with_capacity(5 + 4 + body_len);
    out.push(0x16);
    out.push(0x03); out.push(0x03); // outer version: TLS 1.2
    out.extend_from_slice(&((4 + body_len) as u16).to_be_bytes());
    out.push(0x02);                 // HandshakeType: ServerHello
    out.push(0x00);
    out.push(((body_len >> 8) & 0xff) as u8);
    out.push((body_len & 0xff) as u8);
    out.push(0x03); out.push(0x03); // server_version: TLS 1.2
    out.extend_from_slice(random);  // server_random = Elligator2 repr
    out.push(32u8);                 // session_id_length = 32
    out.extend_from_slice(&session_id);
    out.push(0x13); out.push(0x01); // cipher_suite: TLS_AES_128_GCM_SHA256
    out.push(0x00);                 // compression_method: null
    out.extend_from_slice(&(exts.len() as u16).to_be_bytes());
    out.extend_from_slice(exts);
    out
}
// tls_server_hello:end

/// Read a TLS Handshake record and extract the 32-byte `random` field.
/// Works for both ClientHello (type 0x01) and ServerHello (type 0x02):
/// the `random` field is always at body[6..38] (after hs header(4) + version(2)).
async fn tls_read_random<S: tokio::io::AsyncRead + Unpin>(
    s: &mut S,
    expected_hs_type: u8,
    label: &str,
) -> ZResult<[u8; 32]> {
    let mut hdr = [0u8; 5];
    s.read_exact(&mut hdr)
        .await
        .map_err(|e| zerror!("obfs/tls: failed to read record header ({label}): {e}"))?;
    if hdr[0] != 0x16 {
        return Err(zerror!("obfs/tls: {label}: expected Handshake record 0x16, got {:#04x}", hdr[0]).into());
    }
    let record_len = u16::from_be_bytes([hdr[3], hdr[4]]) as usize;
    // Minimum: hs_header(4) + version(2) + random(32) = 38
    if record_len < 38 {
        return Err(zerror!("obfs/tls: {label}: record too short: {record_len}").into());
    }
    let mut body = vec![0u8; record_len];
    s.read_exact(&mut body)
        .await
        .map_err(|e| zerror!("obfs/tls: failed to read {label} body: {e}"))?;
    if body[0] != expected_hs_type {
        return Err(
            zerror!("obfs/tls: {label}: expected hs type {expected_hs_type:#04x}, got {:#04x}", body[0]).into(),
        );
    }
    // body layout: type(1) + length(3) + version(2) + random(32) + ...
    // index 6..38 = random
    let mut random = [0u8; 32];
    random.copy_from_slice(&body[6..38]);
    Ok(random)
}

use chacha20poly1305::{
    aead::{Aead, KeyInit, Payload},
    ChaCha20Poly1305, Key, Nonce,
};
use curve25519_elligator2::{
    elligator2::{representative_from_privkey, RFC9380},
    MontgomeryPoint as ElligatorMontgomery,
};
use hkdf::Hkdf;
use rand_core::{OsRng, RngCore};
use sha2::Sha256;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use x25519_dalek::{PublicKey, StaticSecret};
use zenoh_result::{zerror, ZResult};

/// HKDF `info` domain-separation tag. Bumping this breaks compatibility on purpose.
const HKDF_INFO: &[u8] = b"bsdos-obfs-v1";

/// Size of an X25519 public key / shared secret / PSK / session key, in bytes.
pub const KEY_LEN: usize = 32;

/// ChaCha20-Poly1305 authentication tag length, in bytes.
const TAG_LEN: usize = 16;

/// Length of the encrypted length-prefix frame: 2 bytes plaintext length + tag.
const LEN_FRAME: usize = 2 + TAG_LEN;

/// AEAD AAD domain separators: prevent a length-frame ciphertext from being
/// accepted in a payload-frame position and vice versa.
const AAD_LEN_FRAME: &[u8] = &[0x00];
const AAD_PAYLOAD_FRAME: &[u8] = &[0x01];

/// Maximum plaintext payload carried in a single AEAD record (SS-AEAD limit: 0x3FFF).
pub const MAX_PAYLOAD: usize = 0x3FFF;

/// 32-byte pre-shared key, identical on client and server.
///
/// Loaded out-of-band (env `BSDOS_OBFS_PSK`, base64) by the caller — this crate
/// only consumes the raw 32 bytes.
#[derive(Clone)]
pub struct PreSharedKey(pub [u8; KEY_LEN]);

impl PreSharedKey {
    /// Build a PSK from exactly 32 raw bytes.
    // from_bytes:start
//   purpose: Build a PreSharedKey from exactly 32 raw bytes with length validation.
//   input:  raw: &[u8] — the raw key bytes
//   output: ZResult<Self> — the PreSharedKey, or error if length != 32
//   sideEffects: none
    pub fn from_bytes(raw: &[u8]) -> ZResult<Self> {
        if raw.len() != KEY_LEN {
            return Err(
                zerror!("obfs PSK must be exactly {KEY_LEN} bytes, got {}", raw.len()).into(),
            );
        }
        let mut key = [0u8; KEY_LEN];
        key.copy_from_slice(raw);
        Ok(PreSharedKey(key))
    }
    // from_bytes:end
}

impl fmt::Debug for PreSharedKey {
    // fmt:start
//   purpose: Redact the PSK in debug output to prevent accidental key material leakage.
//   input:  f: &mut fmt::Formatter<'_> — the formatter
//   output: fmt::Result
//   sideEffects: none
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Never print key material.
        f.write_str("PreSharedKey(<redacted>)")
    }
    // fmt:end
}

/// The long-term static keypair of the *server*.
///
/// The server holds the secret; the client must know the matching public key.
/// In MVP both are derived deterministically from the PSK so no extra secret
/// distribution is needed (see [`StaticServerKey::derive_from_psk`]).
pub struct StaticServerKey {
    secret: StaticSecret,
    public: PublicKey,
}

impl StaticServerKey {
    /// Derive a deterministic static server keypair from the PSK.
    ///
    /// HKDF(PSK, info="bsdos-obfs-static-v1") → 32 bytes → X25519 scalar.
    /// Both sides can compute the server *public* key from the shared PSK, so we
    /// avoid shipping a second secret while still binding the handshake to the PSK
    /// twice (once as static DH input, once as HKDF salt mix).
    // derive_from_psk:start
//   purpose: Derive a deterministic static server X25519 keypair from the shared PSK via HKDF expansion.
//   input:  psk: &PreSharedKey — the pre-shared key to derive from
//   output: Self — the keypair (secret + public)
//   sideEffects: none
    pub fn derive_from_psk(psk: &PreSharedKey) -> Self {
        let hk = Hkdf::<Sha256>::new(None, &psk.0);
        let mut scalar = [0u8; KEY_LEN];
        if hk.expand(b"bsdos-obfs-static-v1", &mut scalar).is_err() {
            // 32 bytes is always a valid HKDF-SHA256 output length, so this branch
            // is unreachable; we avoid panicking and fall back to raw PSK bytes.
            scalar.copy_from_slice(&psk.0);
        }
        // StaticSecret applies X25519 clamping when diffie_hellman() is called;
        // StaticSecret::from stores the raw scalar bytes as-is (x25519-dalek v2).
        let secret = StaticSecret::from(scalar);
        let public = PublicKey::from(&secret);
        StaticServerKey { secret, public }
    }
    // derive_from_psk:end

    // public:start
//   purpose: Return the public key of the static server keypair.
//   input:  &self
//   output: PublicKey — the X25519 public key
//   sideEffects: none
    pub fn public(&self) -> PublicKey {
        self.public
    }
    // public:end

    /// server_static ⨯ peer_ephemeral DH (PSK-bound mix input).
    // dh:start
//   purpose: Perform X25519 Diffie-Hellman between the static server secret and a peer's public key.
//   input:  peer_pub: &PublicKey — the peer's X25519 public key
//   output: [u8; KEY_LEN] — the 32-byte shared secret
//   sideEffects: none
    fn dh(&self, peer_pub: &PublicKey) -> [u8; KEY_LEN] {
        *self.secret.diffie_hellman(peer_pub).as_bytes()
    }
    // dh:end
}

/// Derive the *public* static server key from the PSK (client side — no secret needed).
// server_static_public_from_psk:start
//   purpose: Derive the server's static public key from the PSK (client side, no secret needed).
//   input:  psk: &PreSharedKey — the pre-shared key
//   output: PublicKey — the static server public key
//   sideEffects: none
pub fn server_static_public_from_psk(psk: &PreSharedKey) -> PublicKey {
    StaticServerKey::derive_from_psk(psk).public()
}
// server_static_public_from_psk:end

/// Generate a reusable (StaticSecret-form) ephemeral keypair together with its
/// Elligator2 representative.
///
/// We use `StaticSecret` rather than `EphemeralSecret` because the handshake
/// needs TWO Diffie-Hellman operations from the same client scalar
/// (ephemeral⨯ephemeral and ephemeral⨯static), and `EphemeralSecret` is
/// consumed by its first DH. The secret still lives only for one connection.
///
/// Elligator2 constraint: only ~50% of X25519 keys have a valid representative.
/// This function retries until it finds a representable key. The expected number
/// of trials is 2 (geometric distribution, p=0.5). A random `tweak` byte is
/// chosen per trial; representative_from_privkey folds it into the high-order
/// bits of the representative (standard bit-stuffing) and from_representative
/// clears those bits on decode, so the tweak is NOT transmitted separately.
///
/// Returns `(secret, x25519_pubkey, representative_32_bytes)`.
// gen_ephemeral_elligator:start
//   purpose: Generate an ephemeral X25519 keypair whose public key has a valid Elligator2 uniform-random representative.
//   input:  none
//   output: ZResult<(StaticSecret, PublicKey, [u8; KEY_LEN])> — (secret, X25519 pubkey, Elligator2 representative), or error after 64 retries
//   sideEffects: Draws random bytes from OsRng (CSPRNG)
fn gen_ephemeral_elligator() -> ZResult<(StaticSecret, PublicKey, [u8; KEY_LEN])> {
    // Bound the retry loop to guard against a broken RNG producing a long run
    // of non-representable keys. In practice, 64 attempts gives failure
    // probability < 2^-64.
    for _ in 0..64u32 {
        let mut scalar = [0u8; KEY_LEN];
        OsRng.fill_bytes(&mut scalar);
        let mut tweak_buf = [0u8; 1];
        OsRng.fill_bytes(&mut tweak_buf);
        let tweak = tweak_buf[0];

        // representative_from_privkey clamps and derives the Montgomery pubkey
        // internally, then applies Elligator2 with the given tweak (folded into
        // the high bits of the representative). Returns None for ~50% of scalars.
        if let Some(repr) = representative_from_privkey(&scalar, tweak) {
            // Build the x25519-dalek types from the same scalar bytes.
            let secret = StaticSecret::from(scalar);
            let public = PublicKey::from(&secret);
            return Ok((secret, public, repr));
        }
    }
    Err(zerror!("obfs: failed to find Elligator2-representable ephemeral key in 64 attempts (RNG fault?)").into())
}
// gen_ephemeral_elligator:end

/// Decode a received 32-byte Elligator2 representative back to an x25519 `PublicKey`.
///
/// `MontgomeryPoint::from_representative::<RFC9380>` is the inverse of the
/// encode path; it returns `None` for invalid (non-image) inputs, which we
/// treat as a handshake error (likely an active probe or bit flip).
// decode_representative:start
//   purpose: Decode a 32-byte Elligator2 representative back to an X25519 PublicKey.
//   input:  repr: &[u8; KEY_LEN] — the Elligator2 representative received from the peer
//   output: ZResult<PublicKey> — the decoded public key, or error if representative is not a valid Elligator2 image
//   sideEffects: none
fn decode_representative(repr: &[u8; KEY_LEN]) -> ZResult<PublicKey> {
    // curve25519-elligator2's MontgomeryPoint is byte-compatible with
    // x25519-dalek's PublicKey: both are 32-byte little-endian Montgomery
    // u-coordinates on Curve25519. We decode via the elligator crate and then
    // re-wrap the raw bytes into an x25519-dalek PublicKey.
    let mp = ElligatorMontgomery::from_representative::<RFC9380>(repr)
        .ok_or_else(|| zerror!("obfs: invalid Elligator2 representative in handshake (not a valid image point)"))?;
    Ok(PublicKey::from(*mp.as_bytes()))
}
// decode_representative:end

/// Derive the session key per PLAN §4.2.
///
/// ```text
/// ikm  = X25519(client_eph, server_eph) || X25519(client_eph, server_static)
/// salt = client_eph_pub || server_eph_pub
/// info = "bsdos-obfs-v1"
/// ```
///
/// The two DH outputs are concatenated (not XORed) so the HKDF input is 64 bytes.
/// Concatenation prevents cross-cancellation: XOR of two DH outputs could collapse
/// to all-zeros if an attacker with the PSK forces dh_ephemeral == dh_static by
/// choosing server_eph_pub == server_static_pub, silently removing the ephemeral
/// contribution. Concatenation-then-HKDF is the standard Noise/SS-AEAD convention.
///
/// NOTE: this changes HKDF_INFO wire-compatibility vs the earlier XOR design;
/// the HKDF_INFO constant ("bsdos-obfs-v1") serves as the version break signal.
///
/// `salt` concatenates the two ephemeral public keys in a fixed order (client
/// first, server second) so both peers compute the identical salt.
// derive_session_key:start
//   purpose: Derive the 32-byte AEAD session key from two DH outputs and the two ephemeral public keys via HKDF.
//   input:  dh_ephemeral: &[u8; KEY_LEN] — DH(eph, eph); dh_static: &[u8; KEY_LEN] — DH(eph, static); client_eph_pub: &[u8; KEY_LEN] — client ephemeral public key bytes; server_eph_pub: &[u8; KEY_LEN] — server ephemeral public key bytes
//   output: ZResult<[u8; KEY_LEN]> — the 32-byte session key, or error on HKDF failure
//   sideEffects: none
fn derive_session_key(
    dh_ephemeral: &[u8; KEY_LEN],
    dh_static: &[u8; KEY_LEN],
    client_eph_pub: &[u8; KEY_LEN],
    server_eph_pub: &[u8; KEY_LEN],
) -> ZResult<[u8; KEY_LEN]> {
    // Concatenate both DH outputs as IKM (64 bytes total).
    let mut ikm = [0u8; KEY_LEN * 2];
    ikm[..KEY_LEN].copy_from_slice(dh_ephemeral);
    ikm[KEY_LEN..].copy_from_slice(dh_static);

    let mut salt = [0u8; KEY_LEN * 2];
    salt[..KEY_LEN].copy_from_slice(client_eph_pub);
    salt[KEY_LEN..].copy_from_slice(server_eph_pub);

    let hk = Hkdf::<Sha256>::new(Some(&salt), &ikm);
    let mut session = [0u8; KEY_LEN];
    hk.expand(HKDF_INFO, &mut session)
        .map_err(|e| zerror!("obfs HKDF expand failed: {e:?}"))?;
    Ok(session)
}
// derive_session_key:end

/// A 96-bit little-endian frame counter used as the ChaCha20-Poly1305 nonce.
///
/// Each direction has its own counter; it is incremented once per AEAD record.
struct NonceCounter {
    value: u128,
}

impl NonceCounter {
    // new:start
//   purpose: Create a new nonce counter starting at zero.
//   input:  none
//   output: Self — NonceCounter initialized to 0
//   sideEffects: none
    fn new() -> Self {
        NonceCounter { value: 0 }
    }
    // new:end

    /// Produce the current 12-byte LE nonce and advance the counter.
    // next:start
//   purpose: Produce the current 12-byte LE nonce and advance the counter.
//   input:  &mut self
//   output: ZResult<Nonce> — the current nonce (12 bytes), or error if counter exhausted (>= 2^96)
//   sideEffects: none
    fn next(&mut self) -> ZResult<Nonce> {
        if self.value >> 96 != 0 {
            return Err(zerror!("obfs nonce counter exhausted").into());
        }
        let bytes = self.value.to_le_bytes(); // 16 bytes
        let mut nonce = [0u8; 12];
        nonce.copy_from_slice(&bytes[..12]);
        self.value = self.value.wrapping_add(1);
        Ok(*Nonce::from_slice(&nonce))
    }
    // next:end
}

// SECURITY (handshake authentication — by design, MVP):
//   There is NO separate key-confirmation / "finished" frame. Authentication of
//   the peer is *implicit*: the first proof that both sides hold the same PSK is
//   the AEAD tag of the FIRST data record. Consequences:
//     - A mismatched PSK or on-path tampering surfaces only on the first decrypt
//       failure (read_record → "authentication failed"), not at handshake time.
//     - Transcript binding is limited: HKDF `salt` = client_eph || server_eph
//       only; the static_pub and HKDF `info` are mixed into `ikm`, not the salt.
//   The static server key is PSK-derived (see StaticServerKey::derive_from_psk),
//   so it provides PSK-binding ONLY — no server authentication and no extra
//   secrecy beyond the ephemeral DH. Active probing is out of scope (see README
//   §Limitations / PLAN §5). If deterministic handshake-time failure is needed,
//   add a fixed-length zero "finished" record exchanged right after accept/connect
//   so the server can reject a bad PSK before any payload is sent.
//
/// An AEAD-framed bidirectional stream over an arbitrary async byte stream.
///
/// Generic over `S` so it works on `TcpStream` directly. Holds independent
/// send/recv ciphers and nonce counters (matching SS-AEAD: a direction's nonce
/// space is never reused for the other).
pub struct ObfsStream<S> {
    inner: S,
    send_cipher: ChaCha20Poly1305,
    recv_cipher: ChaCha20Poly1305,
    send_nonce: NonceCounter,
    recv_nonce: NonceCounter,
    /// Leftover plaintext from a previously decoded record not yet handed to a
    /// short `read()` buffer.
    read_carry: Vec<u8>,
}

impl<S> ObfsStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    // new_with_keys:start
//   purpose: Create an ObfsStream with pre-derived directional send and receive keys.
//   input:  inner: S — the underlying async byte stream; send_key: [u8; KEY_LEN] — encryption key for outbound data; recv_key: [u8; KEY_LEN] — decryption key for inbound data
//   output: ObfsStream<S> — the AEAD-framed stream
//   sideEffects: none
    fn new_with_keys(
        inner: S,
        send_key: [u8; KEY_LEN],
        recv_key: [u8; KEY_LEN],
    ) -> ObfsStream<S> {
        let send_cipher = ChaCha20Poly1305::new(Key::from_slice(&send_key));
        let recv_cipher = ChaCha20Poly1305::new(Key::from_slice(&recv_key));
        ObfsStream {
            inner,
            send_cipher,
            recv_cipher,
            send_nonce: NonceCounter::new(),
            recv_nonce: NonceCounter::new(),
            read_carry: Vec::new(),
        }
    }
    // new_with_keys:end

    /// Split a single session key into directional sub-keys so that client→server
    /// and server→client use distinct keystreams even with overlapping nonces.
    // split_directional:start
//   purpose: Derive two directional AEAD sub-keys from a single session key via HKDF expansion.
//   input:  session_key: &[u8; KEY_LEN] — the 32-byte session key
//   output: ZResult<([u8; KEY_LEN], [u8; KEY_LEN])> — (client-to-server key, server-to-client key), or error on HKDF failure
//   sideEffects: none
    fn split_directional(session_key: &[u8; KEY_LEN]) -> ZResult<([u8; KEY_LEN], [u8; KEY_LEN])> {
        let hk = Hkdf::<Sha256>::new(None, session_key);
        let mut c2s = [0u8; KEY_LEN];
        let mut s2c = [0u8; KEY_LEN];
        hk.expand(b"bsdos-obfs-c2s", &mut c2s)
            .map_err(|e| zerror!("obfs sub-key c2s expand failed: {e:?}"))?;
        hk.expand(b"bsdos-obfs-s2c", &mut s2c)
            .map_err(|e| zerror!("obfs sub-key s2c expand failed: {e:?}"))?;
        Ok((c2s, s2c))
    }
    // split_directional:end

    /// Client-side handshake: send our ephemeral pub (Elligator2-encoded),
    /// read the server's ephemeral pub (Elligator2-encoded), derive the session
    /// key, return the framed stream.
    ///
    /// Wire format (each direction): 32-byte Elligator2 representative only.
    /// The tweak is folded into the high-order bits of the representative by
    /// representative_from_privkey; from_representative clears those bits on
    /// decode. No separate tweak byte is transmitted (handshake is 64 bytes total).
    /// The representative is computationally indistinguishable from uniform random,
    /// closing the passive-DPI statistical classifier on the handshake prefix.
    // connect:start
//   purpose: Perform the client side of the obfs handshake: send fake TLS ClientHello, read ServerHello, derive session key, return framed stream.
//   input:  inner: S — the raw TCP stream; psk: &PreSharedKey — the pre-shared key
//   output: ZResult<ObfsStream<S>> — the AEAD-framed stream ready for I/O, or error on handshake/decrypt failure
//   sideEffects: Sends TLS ClientHello on wire, reads TLS ServerHello from wire, draws random bytes via OsRng for ephemeral key
    pub async fn connect(mut inner: S, psk: &PreSharedKey) -> ZResult<ObfsStream<S>> {
        let (client_secret, client_pub, client_repr) = gen_ephemeral_elligator()?;

        // Send fake TLS ClientHello with client_random = our Elligator2 representative.
        inner
            .write_all(&tls_client_hello(&client_repr))
            .await
            .map_err(|e| zerror!("obfs handshake: failed to send TLS ClientHello: {e}"))?;
        inner
            .flush()
            .await
            .map_err(|e| zerror!("obfs handshake: flush failed: {e}"))?;

        // Read fake TLS ServerHello; server_random = server's Elligator2 representative.
        let server_repr = tls_read_random(&mut inner, 0x02, "ServerHello")
            .await
            .map_err(|e| zerror!("obfs handshake: failed to read server ephemeral: {e}"))?;
        // RFC9380 decode is deterministic from the 32 representative bytes alone.
        let server_eph_pub = decode_representative(&server_repr)?;

        // Static server public key, derived from the shared PSK.
        let server_static_pub = server_static_public_from_psk(psk);

        // DH(client_eph, server_eph) and DH(client_eph, server_static).
        let dh_ephemeral = *client_secret.diffie_hellman(&server_eph_pub).as_bytes();
        let dh_static = *client_secret.diffie_hellman(&server_static_pub).as_bytes();

        // HKDF salt uses the decoded pubkey bytes (same on both sides), not the
        // representatives — the representatives are transport encoding only.
        let session_key = derive_session_key(
            &dh_ephemeral,
            &dh_static,
            client_pub.as_bytes(),
            server_eph_pub.as_bytes(),
        )?;

        let (c2s, s2c) = Self::split_directional(&session_key)?;
        // Client sends with c2s, receives with s2c.
        Ok(Self::new_with_keys(inner, c2s, s2c))
    }
    // connect:end

    /// Server-side handshake: read the client Elligator2 message, send our
    /// Elligator2 message, derive the session key, return the framed stream.
    ///
    /// Wire format (each direction): 32-byte Elligator2 representative only.
    // accept:start
//   purpose: Perform the server side of the obfs handshake: read fake TLS ClientHello, send ServerHello, derive session key, return framed stream.
//   input:  inner: S — the raw TCP stream; server_static: &StaticServerKey — the server's static X25519 keypair
//   output: ZResult<ObfsStream<S>> — the AEAD-framed stream ready for I/O, or error on handshake/decrypt failure
//   sideEffects: Sends TLS ServerHello on wire, reads TLS ClientHello from wire, draws random bytes via OsRng for ephemeral key + session ID
    pub async fn accept(mut inner: S, server_static: &StaticServerKey) -> ZResult<ObfsStream<S>> {
        // Read fake TLS ClientHello; client_random = client's Elligator2 representative.
        let client_repr = tls_read_random(&mut inner, 0x01, "ClientHello")
            .await
            .map_err(|e| zerror!("obfs handshake: failed to read client ephemeral: {e}"))?;
        eprintln!("[obfs-crypto] accept: read client repr OK: {}", hex_repr(&client_repr));
        let client_eph_pub = decode_representative(&client_repr)?;
        eprintln!("[obfs-crypto] accept: decode repr OK");

        let (server_secret, server_eph_pub, server_repr) = gen_ephemeral_elligator()?;
        eprintln!("[obfs-crypto] accept: gen server ephemeral OK");

        // Send fake TLS ServerHello with server_random = our Elligator2 representative.
        inner
            .write_all(&tls_server_hello(&server_repr))
            .await
            .map_err(|e| zerror!("obfs handshake: failed to send server ephemeral: {e}"))?;
        inner
            .flush()
            .await
            .map_err(|e| zerror!("obfs handshake: flush failed: {e}"))?;

        // DH(server_eph, client_eph) == DH(client_eph, server_eph) (commutative).
        let dh_ephemeral = *server_secret.diffie_hellman(&client_eph_pub).as_bytes();
        // DH(server_static, client_eph) == DH(client_eph, server_static).
        let dh_static = server_static.dh(&client_eph_pub);

        // HKDF salt uses the decoded pubkey bytes (same on both sides).
        let session_key = derive_session_key(
            &dh_ephemeral,
            &dh_static,
            client_eph_pub.as_bytes(),
            server_eph_pub.as_bytes(),
        )?;

        let (c2s, s2c) = Self::split_directional(&session_key)?;
        // Server sends with s2c, receives with c2s.
        Ok(Self::new_with_keys(inner, s2c, c2s))
    }
    // accept:end

    /// Encrypt and write a buffer as one or more AEAD records.
    ///
    /// Splits `buf` into chunks of at most [`MAX_PAYLOAD`] bytes and emits, per
    /// chunk: `[enc(len)+tag][enc(payload)+tag]` (PLAN §4.3).
    // write_frames:start
//   purpose: Encrypt and write a buffer as one or more AEAD records, splitting at MAX_PAYLOAD boundaries.
//   input:  buf: &[u8] — the plaintext data to encrypt and send
//   output: ZResult<usize> — number of plaintext bytes written, or error on encrypt/write failure
//   sideEffects: Encrypts data via ChaCha20-Poly1305 and writes AEAD frames to the underlying stream I/O
    pub async fn write_frames(&mut self, buf: &[u8]) -> ZResult<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let mut written = 0usize;
        for chunk in buf.chunks(MAX_PAYLOAD) {
            // 1) Encrypt the 2-byte length prefix.
            let len = chunk.len() as u16;
            let len_bytes = len.to_be_bytes();
            let len_nonce = self.send_nonce.next()?;
            let enc_len = self
                .send_cipher
                .encrypt(&len_nonce, Payload { msg: &len_bytes, aad: AAD_LEN_FRAME })
                .map_err(|e| zerror!("obfs: length encryption failed: {e:?}"))?;

            // 2) Encrypt the payload chunk.
            let payload_nonce = self.send_nonce.next()?;
            let enc_payload = self
                .send_cipher
                .encrypt(&payload_nonce, Payload { msg: chunk, aad: AAD_PAYLOAD_FRAME })
                .map_err(|e| zerror!("obfs: payload encryption failed: {e:?}"))?;

            self.inner
                .write_all(&enc_len)
                .await
                .map_err(|e| zerror!("obfs: write length frame failed: {e}"))?;
            self.inner
                .write_all(&enc_payload)
                .await
                .map_err(|e| zerror!("obfs: write payload frame failed: {e}"))?;

            written += chunk.len();
        }
        Ok(written)
    }
    // write_frames:end

    /// Flush the underlying transport.
    // flush:start
//   purpose: Flush the underlying async transport stream.
//   input:  &mut self
//   output: ZResult<()> — Ok on flush success, error on flush failure
//   sideEffects: Flushes the underlying I/O stream
    pub async fn flush(&mut self) -> ZResult<()> {
        self.inner
            .flush()
            .await
            .map_err(|e| zerror!("obfs: flush failed: {e}").into())
    }
    // flush:end

    /// Read and decrypt exactly one AEAD record, returning the plaintext payload,
    /// or `Ok(None)` on a clean EOF at a record boundary (no bytes read yet).
    // read_record:start
//   purpose: Read and decrypt exactly one AEAD record from the stream, returning the plaintext payload.
//   input:  &mut self
//   output: ZResult<Option<Vec<u8>>> — Some(payload) on success, None on clean EOF at a record boundary, error on auth failure or I/O error
//   sideEffects: Reads from underlying I/O stream; decrypts via ChaCha20-Poly1305
    async fn read_record(&mut self) -> ZResult<Option<Vec<u8>>> {
        // 1) Peek the first byte to distinguish clean EOF from mid-record EOF.
        //    read_exact on a closed stream with 0 bytes consumed returns
        //    UnexpectedEof; we want to surface Ok(None) only when the peer
        //    closed the connection cleanly between records.
        let mut len_frame = [0u8; LEN_FRAME];
        // Read the first byte with a plain read() so we can detect 0-byte EOF.
        let first = self.inner.read(&mut len_frame[..1]).await
            .map_err(|e| zerror!("obfs: read length frame failed: {e}"))?;
        if first == 0 {
            // Clean EOF: peer closed the connection at a record boundary.
            return Ok(None);
        }
        // Read the remaining LEN_FRAME-1 bytes; any short read here is an error.
        self.inner
            .read_exact(&mut len_frame[1..])
            .await
            .map_err(|e| zerror!("obfs: read length frame failed (mid-record EOF): {e}"))?;
        let len_nonce = self.recv_nonce.next()?;
        let len_plain = self
            .recv_cipher
            .decrypt(&len_nonce, Payload { msg: &len_frame, aad: AAD_LEN_FRAME })
            .map_err(|_| {
                zerror!("obfs: length frame authentication failed (bad PSK or tampering)")
            })?;
        if len_plain.len() != 2 {
            return Err(zerror!("obfs: decrypted length frame has wrong size").into());
        }
        let payload_len = u16::from_be_bytes([len_plain[0], len_plain[1]]) as usize;
        if payload_len == 0 || payload_len > MAX_PAYLOAD {
            return Err(zerror!("obfs: invalid payload length {payload_len}").into());
        }

        // 2) Read + decrypt the payload frame.
        let mut payload_frame = vec![0u8; payload_len + TAG_LEN];
        self.inner
            .read_exact(&mut payload_frame)
            .await
            .map_err(|e| zerror!("obfs: read payload frame failed: {e}"))?;
        let payload_nonce = self.recv_nonce.next()?;
        let payload = self
            .recv_cipher
            .decrypt(&payload_nonce, Payload { msg: &payload_frame, aad: AAD_PAYLOAD_FRAME })
            .map_err(|_| {
                zerror!("obfs: payload authentication failed (bad PSK or tampering)")
            })?;
        Ok(Some(payload))
    }
    // read_record:end

    /// Read decrypted plaintext into `buf`, returning the number of bytes written.
    ///
    /// Decrypts whole records internally and buffers any plaintext that does not
    /// fit into the caller's `buf` (carry), so the stream presents a byte-stream
    /// interface like TCP/TLS even though the wire format is record-oriented.
    /// Returns `Ok(0)` only on clean EOF with no carry remaining.
    // read_into:start
//   purpose: Read decrypted plaintext into the caller's buffer, draining internal carry between records.
//   input:  buf: &mut [u8] — the buffer to fill with decrypted bytes
//   output: ZResult<usize> — number of bytes written to buf, 0 on clean EOF, error on I/O or auth failure
//   sideEffects: Reads AEAD records from underlying I/O stream and decrypts them
    pub async fn read_into(&mut self, buf: &mut [u8]) -> ZResult<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        if self.read_carry.is_empty() {
            // read_record returns Ok(None) on clean EOF (peer closed at a record
            // boundary — detected via a single read() returning 0 bytes).
            match self.read_record().await? {
                Some(payload) => self.read_carry = payload,
                None => return Ok(0),
            }
        }
        let n = core::cmp::min(buf.len(), self.read_carry.len());
        buf[..n].copy_from_slice(&self.read_carry[..n]);
        // Drain the consumed prefix.
        self.read_carry.drain(..n);
        Ok(n)
    }
    // read_into:end

    /// Read exactly `buf.len()` decrypted bytes (loops over records as needed).
    // read_exact_into:start
//   purpose: Read exactly buf.len() decrypted bytes, looping over AEAD records as needed.
//   input:  buf: &mut [u8] — the buffer to fill exactly
//   output: ZResult<()> — Ok on success, error on unexpected EOF or auth failure
//   sideEffects: Reads from underlying I/O stream via read_into
    pub async fn read_exact_into(&mut self, buf: &mut [u8]) -> ZResult<()> {
        let mut off = 0usize;
        while off < buf.len() {
            let n = self.read_into(&mut buf[off..]).await?;
            if n == 0 {
                return Err(zerror!("obfs: unexpected EOF during read_exact").into());
            }
            off += n;
        }
        Ok(())
    }
    // read_exact_into:end

    /// Access the inner async stream by mutable reference (e.g. to shut it down).
    // inner_mut:start
//   purpose: Access the underlying async byte stream by mutable reference.
//   input:  &mut self
//   output: &mut S — mutable reference to the inner stream
//   sideEffects: none (exposes stream for shutdown/socket tuning)
    pub fn inner_mut(&mut self) -> &mut S {
        &mut self.inner
    }
    // inner_mut:end
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::duplex;

    #[tokio::test]
    // handshake_and_roundtrip:start
//   purpose: Verify a full obfs handshake and round-trip encrypt/decrypt through an in-memory duplex pipe.
//   input:  none (test function)
//   output: asserts — no return value; panics on mismatch
//   sideEffects: Creates in-memory duplex streams; spawns async tasks
    async fn handshake_and_roundtrip() {
        let psk = PreSharedKey([7u8; KEY_LEN]);
        let server_static = StaticServerKey::derive_from_psk(&psk);
        let (a, b) = duplex(64 * 1024);

        let psk_c = psk.clone();
        let client = tokio::spawn(async move {
            let mut s = ObfsStream::connect(a, &psk_c).await?;
            s.write_frames(b"hello obfs").await?;
            s.flush().await?;
            let mut buf = [0u8; 5];
            s.read_exact_into(&mut buf).await?;
            Ok::<_, zenoh_result::Error>(buf)
        });

        let mut srv = ObfsStream::accept(b, &server_static).await.expect("accept");
        let mut buf = [0u8; 10];
        srv.read_exact_into(&mut buf).await.expect("read");
        assert_eq!(&buf, b"hello obfs");
        srv.write_frames(b"world").await.expect("write");
        srv.flush().await.expect("flush");

        let echoed = client.await.expect("join").expect("client ok");
        assert_eq!(&echoed, b"world");
    }
    // handshake_and_roundtrip:end
}
