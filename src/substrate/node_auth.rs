// START_AI_HEADER
// MODULE: mrgd/src/node_auth.rs
// PURPOSE: Per-node ed25519 keypair management and PDU signing/verification.
//          Each node holds one ed25519 signing key, persisted to <DATA_DIR>/node_ed25519.key.
//          On startup: load from file; generate+persist (mode 0600) if absent.
//
//          Design:
//            - NodeSigner: owns the signing key; produces 64-byte sig over canonical_bytes.
//            - NodeKeyStore: grow-set of node_id → ed25519 pubkey ([u8;32]).
//              Populated TOFU: first-seen pubkey for a node_id is recorded and trusted.
//              Network distribution (Zenoh) is gated behind `cluster` feature.
//              Verification is always available (needed even in single-node cluster tests).
//            - canonical_bytes: deterministic byte encoding of the PDU payload fields,
//              identical to the layout used by Pdu::compute_id but without the FNV step —
//              the raw bytes are what we sign and hash.
//
//          Rejection policy (applied by RoomLog::apply_delta_verified):
//            - Pdu with empty signer_node AND empty sig → rejected (unsigned).
//            - Pdu with signer_node set but sig empty / wrong length → rejected (malformed).
//            - signer_node not in key store → rejected (unknown node).
//            - sig fails ed25519 verification → rejected (bad signature).
//            - Locally-created PDUs (self-signed) are verified against own pubkey which is
//              always present in the key store (inserted at NodeSigner creation time).
//
// DEPENDENCIES: ed25519-dalek, sha2, rand, std::fs
// PUBLIC_API: canonical_bytes, NodeSigner, NodeKeyStore,
//             encode_key_announcement, decode_key_announcement
// END_AI_HEADER

use std::{
    collections::HashMap,
    fs, io,
    path::Path,
    sync::{Arc, Mutex},
};

use ed25519_dalek::{ed25519::signature::Signer as _, Signature, SigningKey, VerifyingKey};
use rand::rngs::OsRng;

// ── Canonical byte encoding ───────────────────────────────────────────────────

// canonical_bytes:start
//   purpose: Produce the deterministic canonical byte sequence over a PDU's content
//            fields.  This is the input to both:
//              - sha256 → event_id hash (collision-resistant content address)
//              - ed25519 sign/verify (authentication)
//            Format (LENGTH-PREFIXED, injective — internal-task):
//              len(room_id) room_id  len(sender) sender  len(kind) kind
//              len(content) content  count(prev) [len(p) p]...(sorted)  depth_le8 ts_le8
//            Every length is u64-LE; depth/ts are fixed 8-byte LE. Length-prefixing (rather
//            than NUL/SOH delimiters) guarantees injectivity: no field value — including
//            arbitrary `content` bytes or a &str containing U+0000 — can be confused with a
//            field boundary. Injectivity is REQUIRED here because these bytes are the input
//            to both the sha256 event_id and the ed25519 signature.
//   input:  room_id, sender, kind, content, prev_events, depth, ts — same as Pdu fields
//   output: Vec<u8> — canonical byte sequence (deterministic, no side-effects)
//   sideEffects: none
// canonical_bytes:end
pub fn canonical_bytes(
    room_id: &str,
    sender: &str,
    kind: &str,
    content: &[u8],
    prev_events: &[String],
    depth: u64,
    ts: u64,
) -> Vec<u8> {
    // LENGTH-PREFIXED, INJECTIVE encoding (internal-task fix). Each variable field is written as
    // (u64-LE length || bytes). This is security-critical: canonical_bytes is the input to
    // BOTH the sha256 event_id AND the ed25519 signature, so it MUST be injective. The prior
    // NUL/SOH-delimited layout was NOT — field values can contain those bytes (content is
    // arbitrary; a &str may hold U+0000), so two distinct field-tuples could produce identical
    // canonical bytes → a shared event_id and a signature valid for both interpretations.
    // Length prefixes remove all delimiter ambiguity.
    fn put_field(buf: &mut Vec<u8>, b: &[u8]) {
        buf.extend_from_slice(&(b.len() as u64).to_le_bytes());
        buf.extend_from_slice(b);
    }
    let mut buf: Vec<u8> =
        Vec::with_capacity(room_id.len() + sender.len() + kind.len() + content.len() + 64);
    put_field(&mut buf, room_id.as_bytes());
    put_field(&mut buf, sender.as_bytes());
    put_field(&mut buf, kind.as_bytes());
    put_field(&mut buf, content);
    // prev_events: sorted for order-independence, then count-prefixed, each length-prefixed.
    let mut sorted_prevs: Vec<&str> = prev_events.iter().map(|s| s.as_str()).collect();
    sorted_prevs.sort_unstable();
    buf.extend_from_slice(&(sorted_prevs.len() as u64).to_le_bytes());
    for p in &sorted_prevs {
        put_field(&mut buf, p.as_bytes());
    }
    // depth + ts are fixed-width 8-byte little-endian — unambiguous, no length needed.
    buf.extend_from_slice(&depth.to_le_bytes());
    buf.extend_from_slice(&ts.to_le_bytes());
    buf
}

// ── NodeSigner ────────────────────────────────────────────────────────────────

// NodeSigner:start
//   purpose: Holds this node's ed25519 signing key.
//            load_or_generate() loads from <data_dir>/node_ed25519.key (raw 32-byte seed);
//            generates and persists (mode 0600) if absent.
//            sign(bytes) → 64-byte signature over canonical_bytes.
//            verifying_key_bytes() → [u8;32] for inclusion in the NodeKeyStore.
//   input:  data_dir — path to DATA_DIR; node_id — this node's stable string id
//   output: NodeSigner value
//   sideEffects: may create <data_dir>/node_ed25519.key on first call
// NodeSigner:end
pub struct NodeSigner {
    signing_key: SigningKey,
    /// This node's id, stored so callers can insert the pubkey into NodeKeyStore.
    pub node_id: String,
}

impl NodeSigner {
    // NodeSigner::load_or_generate:start
    //   purpose: Load the ed25519 seed from <data_dir>/node_ed25519.key, or generate a
    //            fresh one and write it atomically (write to .tmp, rename).
    //            The file stores exactly 32 raw seed bytes; mode 0600 enforced on creation.
    //            Returns Err(String) on unrecoverable I/O errors.
    //   input:  data_dir — existing directory; node_id — stable identifier string
    //   output: Result<NodeSigner, String>
    //   sideEffects: may create/read <data_dir>/node_ed25519.key
    // NodeSigner::load_or_generate:end
    pub fn load_or_generate(data_dir: &Path, node_id: String) -> Result<Self, String> {
        let key_path = data_dir.join("node_ed25519.key");
        let seed: [u8; 32] = if key_path.exists() {
            let bytes = fs::read(&key_path)
                .map_err(|e| format!("node_auth: read key {}: {e}", key_path.display()))?;
            if bytes.len() != 32 {
                return Err(format!(
                    "node_auth: key file {} has wrong length {} (expected 32)",
                    key_path.display(),
                    bytes.len()
                ));
            }
            let mut arr = [0u8; 32];
            arr.copy_from_slice(&bytes);
            arr
        } else {
            // Generate fresh seed.
            let signing_key = SigningKey::generate(&mut OsRng);
            let seed = *signing_key.as_bytes();
            // Write to .tmp then rename for atomicity.
            let tmp_path = data_dir.join("node_ed25519.key.tmp");
            write_secret_file(&tmp_path, &seed)
                .map_err(|e| format!("node_auth: write tmp key: {e}"))?;
            fs::rename(&tmp_path, &key_path).map_err(|e| format!("node_auth: rename key: {e}"))?;
            seed
        };

        let signing_key = SigningKey::from_bytes(&seed);
        Ok(NodeSigner {
            signing_key,
            node_id,
        })
    }

    // NodeSigner::from_seed:start
    //   purpose: Construct a NodeSigner from a raw 32-byte seed (for testing).
    //            Does NOT write any file.
    //   input:  seed — 32-byte ed25519 private seed; node_id — stable identifier
    //   output: NodeSigner
    //   sideEffects: none
    // NodeSigner::from_seed:end
    pub fn from_seed(seed: [u8; 32], node_id: String) -> Self {
        NodeSigner {
            signing_key: SigningKey::from_bytes(&seed),
            node_id,
        }
    }

    // NodeSigner::generate:start
    //   purpose: Construct a NodeSigner with a fresh RANDOM ed25519 key (via OsRng),
    //            NOT persisted. For in-memory / no-data_dir instances that still need a
    //            genuine unforgeable key. MUST be used instead of any key derived from
    //            public data: node_id (== server_name) is public, so a key derived from it
    //            (e.g. sha256(node_id)) would let anyone recompute the private key and forge
    //            this node's signatures. A random key is unforgeable; the only trade-off is
    //            it is not stable across restarts (fine — these variants persist no state).
    //   input:  node_id — stable identifier (== server_name)
    //   output: NodeSigner with a random signing key
    //   sideEffects: consumes OS entropy via OsRng
    // NodeSigner::generate:end
    pub fn generate(node_id: String) -> Self {
        NodeSigner {
            signing_key: SigningKey::generate(&mut OsRng),
            node_id,
        }
    }

    // NodeSigner::sign:start
    //   purpose: Produce an ed25519 signature over `bytes`.
    //            Returns the 64-byte signature as a Vec<u8>.
    //   input:  bytes — canonical PDU byte sequence (from canonical_bytes())
    //   output: Vec<u8> — 64-byte ed25519 signature
    //   sideEffects: none
    // NodeSigner::sign:end
    pub fn sign(&self, bytes: &[u8]) -> Vec<u8> {
        let sig: Signature = self.signing_key.sign(bytes);
        sig.to_bytes().to_vec()
    }

    // NodeSigner::verifying_key_bytes:start
    //   purpose: Return the 32-byte compressed point of the verifying (public) key.
    //            Insert into NodeKeyStore to allow peers to verify signatures from this node.
    //   input:  none
    //   output: [u8;32]
    //   sideEffects: none
    // NodeSigner::verifying_key_bytes:end
    pub fn verifying_key_bytes(&self) -> [u8; 32] {
        self.signing_key.verifying_key().to_bytes()
    }
}

// write_secret_file:start
//   purpose: Write `bytes` to `path` with mode 0600 (owner read/write only).
//            On non-Unix platforms the mode step is skipped (best-effort security).
//   input:  path — destination path; bytes — data to write
//   output: Result<(), io::Error>
//   sideEffects: creates/overwrites file at path; may set permissions
// write_secret_file:end
fn write_secret_file(path: &Path, bytes: &[u8]) -> Result<(), io::Error> {
    fs::write(path, bytes)?;
    // Attempt to set 0600 permissions. Not fatal on failure (non-Unix, etc.).
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let perms = fs::Permissions::from_mode(0o600);
        let _ = fs::set_permissions(path, perms);
    }
    Ok(())
}

// parse_node_keys:start
//   purpose: Parse an out-of-band trust anchor: "node-a=<64 hex>,node-b=<64 hex>".
//            Strict on purpose — this is a security control, and a control that
//            silently ignores the half of its input it could not read is worse than
//            none. Any malformed entry fails the whole list.
//   input:  spec — comma-separated node_id=hex32 pairs; whitespace around either side
//           of a pair is ignored
//   output: Ok(map) or Err(reason) naming the offending entry
//   sideEffects: none
// parse_node_keys:end
pub fn parse_node_keys(spec: &str) -> Result<HashMap<String, [u8; 32]>, String> {
    let mut out = HashMap::new();
    for (i, entry) in spec.split(',').enumerate() {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let (node_id, hex) = entry
            .split_once('=')
            .ok_or_else(|| format!("entry {}: expected node_id=<64 hex chars>, got {entry:?}", i + 1))?;
        let node_id = node_id.trim();
        let hex = hex.trim();
        if node_id.is_empty() {
            return Err(format!("entry {}: empty node_id", i + 1));
        }
        if hex.len() != 64 {
            return Err(format!(
                "entry {} ({node_id}): key must be 64 hex chars (32 bytes), got {}",
                i + 1,
                hex.len()
            ));
        }
        let mut key = [0u8; 32];
        for (b, pair) in key.iter_mut().zip(hex.as_bytes().chunks(2)) {
            let s = std::str::from_utf8(pair).map_err(|e| format!("entry {}: {e}", i + 1))?;
            *b = u8::from_str_radix(s, 16)
                .map_err(|e| format!("entry {} ({node_id}): bad hex: {e}", i + 1))?;
        }
        if out.insert(node_id.to_string(), key).is_some() {
            return Err(format!("entry {}: duplicate node_id {node_id:?}", i + 1));
        }
    }
    Ok(out)
}

// ── NodeKeyStore ──────────────────────────────────────────────────────────────

// NodeKeyStore:start
//   purpose: Grow-set of node_id → ed25519 verifying key ([u8;32]).
//            TOFU: first-seen pubkey for a node_id is trusted and recorded; subsequent
//            entries for the SAME node_id are accepted only if the key matches (no
//            key rotation without out-of-band trust anchor).  Mismatches are logged.
//            Network distribution of entries (Zenoh) is handled by the cluster layer;
//            this struct is pure in-memory storage + verification logic.
//            Thread-safe: inner HashMap wrapped in Arc<Mutex<>>.
//   input:  insert(node_id, key_bytes) — TOFU insert; verify(node_id, bytes, sig) → bool
//   output: bool (verify), ()  (insert)
//   sideEffects: mutates inner map on insert (only for new node_ids)
// NodeKeyStore:end
#[derive(Clone)]
pub struct NodeKeyStore {
    inner: Arc<Mutex<HashMap<String, [u8; 32]>>>,
    /// Closed set: reject node_ids that were not pinned at construction.
    /// Set once when the store is built from a configured anchor, never after.
    closed: bool,
}

impl NodeKeyStore {
    // NodeKeyStore::new:start
    //   purpose: Create an empty NodeKeyStore.
    //   input:  none
    //   output: NodeKeyStore
    //   sideEffects: allocates one Arc<Mutex<HashMap>>
    // NodeKeyStore::new:end
    pub fn new() -> Self {
        NodeKeyStore {
            inner: Arc::new(Mutex::new(HashMap::new())),
            closed: false,
        }
    }

    // NodeKeyStore::anchored:start
    //   purpose: Build a CLOSED store from an out-of-band list of trusted node keys.
    //            TOFU's weakness is that the key-announcement channel is
    //            unauthenticated: whoever announces a node_id first owns it, and a
    //            race-claimed key is indistinguishable from the real one. An anchor
    //            removes the race — the set of nodes is fixed up front, and an
    //            announcement for anyone else is refused rather than learned.
    //   input:  pinned — node_id → verifying key, from configuration
    //   output: a NodeKeyStore that will not learn new nodes
    //   sideEffects: none
    // NodeKeyStore::anchored:end
    pub fn anchored(pinned: HashMap<String, [u8; 32]>) -> Self {
        NodeKeyStore {
            inner: Arc::new(Mutex::new(pinned)),
            closed: true,
        }
    }

    // NodeKeyStore::is_anchored:start
    //   purpose: Whether this store is a closed set (configured anchor) rather than
    //            TOFU. Used for startup logging so the posture is visible in the log.
    //   input:  none
    //   output: bool
    //   sideEffects: none
    // NodeKeyStore::is_anchored:end
    pub fn is_anchored(&self) -> bool {
        self.closed
    }

    // NodeKeyStore::insert:start
    //   purpose: TOFU insert: record `key_bytes` for `node_id` if not yet present.
    //            If `node_id` is already recorded with a DIFFERENT key, logs a warning
    //            to stderr and returns false (key mismatch — possible attack / misconfiguration).
    //            Returns true if the key was accepted (new entry or matching re-insert).
    //   input:  node_id — stable node identifier; key_bytes — 32-byte verifying key
    //   output: bool — true = accepted; false = rejected (key mismatch)
    //   sideEffects: may insert into inner map; may write to stderr
    // NodeKeyStore::insert:end
    pub fn insert(&self, node_id: &str, key_bytes: [u8; 32]) -> bool {
        let mut guard = match self.inner.lock() {
            Ok(g) => g,
            Err(_) => return false,
        };
        if let Some(existing) = guard.get(node_id) {
            if existing == &key_bytes {
                return true; // Idempotent re-insert — same key.
            }
            // Key mismatch: TOFU violation.
            eprintln!(
                "[node_auth] TOFU: node_id {:?} presented a DIFFERENT pubkey — rejected. \
                 This may indicate a key rotation or an attack. \
                 Override via config if rotation is intentional.",
                node_id
            );
            return false;
        }
        if self.closed {
            eprintln!(
                "[node_auth] anchored: node_id {:?} is not in the configured key list \
                 — announcement refused. Add it to MATRIX_HS_NODE_KEYS if it is ours.",
                node_id
            );
            return false;
        }
        // First time seeing this node — record under TOFU.
        eprintln!(
            "[node_auth] TOFU: first-seen pubkey for node {:?} — trusted.",
            node_id
        );
        guard.insert(node_id.to_string(), key_bytes);
        true
    }

    // NodeKeyStore::contains:start
    //   purpose: Return true if this node_id has a recorded pubkey.
    //   input:  node_id — node identifier string
    //   output: bool
    //   sideEffects: none
    // NodeKeyStore::contains:end
    pub fn contains(&self, node_id: &str) -> bool {
        self.inner
            .lock()
            .map(|g| g.contains_key(node_id))
            .unwrap_or(false)
    }

    // NodeKeyStore::verify:start
    //   purpose: Verify an ed25519 signature `sig` over `bytes` from `signer_node`.
    //            Returns false if:
    //              - signer_node is not in the key store (unknown node)
    //              - sig is not exactly 64 bytes (malformed)
    //              - the signature does not verify (bad signature)
    //            Returns true only when all checks pass.
    //   input:  signer_node — the node_id that claims to have signed; bytes — signed bytes;
    //           sig — 64-byte ed25519 signature
    //   output: bool — true = valid
    //   sideEffects: none
    // NodeKeyStore::verify:end
    pub fn verify(&self, signer_node: &str, bytes: &[u8], sig: &[u8]) -> bool {
        let guard = match self.inner.lock() {
            Ok(g) => g,
            Err(_) => return false,
        };
        let key_bytes = match guard.get(signer_node) {
            Some(k) => *k,
            None => return false, // unknown node
        };
        drop(guard);

        let vk = match VerifyingKey::from_bytes(&key_bytes) {
            Ok(vk) => vk,
            Err(_) => return false, // malformed stored key (should never happen)
        };

        let sig_arr: [u8; 64] = match sig.try_into() {
            Ok(a) => a,
            Err(_) => return false, // wrong sig length
        };
        let signature = Signature::from_bytes(&sig_arr);
        // verify_strict rejects non-canonical / malleable signatures and small-order
        // public keys (internal-task: defence-in-depth over the permissive verify()).
        vk.verify_strict(bytes, &signature).is_ok()
    }

    // NodeKeyStore::snapshot:start
    //   purpose: Return a cloned snapshot of the current node_id → pubkey map.
    //            Used for grow-set distribution over Zenoh.
    //   input:  none
    //   output: HashMap<String, [u8;32]>
    //   sideEffects: none
    // NodeKeyStore::snapshot:end
    pub fn snapshot(&self) -> HashMap<String, [u8; 32]> {
        self.inner.lock().map(|g| g.clone()).unwrap_or_default()
    }
}

impl Default for NodeKeyStore {
    fn default() -> Self {
        Self::new()
    }
}

// ── Key announcement wire format (TOFU distribution over the mesh — P1.1 internal-task item 4) ──

// encode_key_announcement:start
//   purpose: Serialise a (node_id, pubkey) pair for publication on the Zenoh mesh so
//            peers can TOFU-insert it into their NodeKeyStore.
//            Format: [node_id_len: u16 LE][node_id bytes][pubkey: 32 raw bytes].
//            ⚠ TOFU-only distribution: the announcement itself carries no signature —
//            an unauthenticated mesh participant could publish a false announcement
//            and race-claim a node_id before the legitimate node's first announce.
//            NOT sound until the mesh itself is authenticated (mTLS / PSK / out-of-band
//            trust anchor) — see internal-task (mesh auth is owner/infra, off-limits here).
//            The sender-binding check in Pdu::verify_sig limits the blast radius
//            meanwhile: a race-claimed key can only sign PDUs whose sender domain
//            equals the claimed node_id — it cannot forge senders on OTHER domains.
//   input:  node_id — this node's stable id; pubkey — 32-byte ed25519 verifying key
//   output: Vec<u8> — wire bytes suitable for CrdtSink::publish
//   sideEffects: none
// encode_key_announcement:end
pub fn encode_key_announcement(node_id: &str, pubkey: &[u8; 32]) -> Vec<u8> {
    let id_bytes = node_id.as_bytes();
    let mut buf = Vec::with_capacity(2 + id_bytes.len() + 32);
    buf.extend_from_slice(&(id_bytes.len() as u16).to_le_bytes());
    buf.extend_from_slice(id_bytes);
    buf.extend_from_slice(pubkey);
    buf
}

// decode_key_announcement:start
//   purpose: Parse a key announcement blob produced by encode_key_announcement.
//            Returns None on any malformed input (too short, truncated node_id, wrong
//            total length, non-UTF8 node_id) — callers MUST skip-and-log on None,
//            never panic, since the input originates from an untrusted mesh peer.
//   input:  bytes — wire bytes received from CrdtSink::drain
//   output: Option<(String, [u8;32])> — (node_id, pubkey) on success; None if malformed
//   sideEffects: none
// decode_key_announcement:end
pub fn decode_key_announcement(bytes: &[u8]) -> Option<(String, [u8; 32])> {
    if bytes.len() < 2 {
        return None;
    }
    let id_len = u16::from_le_bytes([bytes[0], bytes[1]]) as usize;
    let id_start = 2usize;
    let id_end = id_start.checked_add(id_len)?;
    let pubkey_end = id_end.checked_add(32)?;
    if bytes.len() != pubkey_end {
        return None; // trailing garbage or truncated — reject rather than guess.
    }
    let node_id = std::str::from_utf8(&bytes[id_start..id_end])
        .ok()?
        .to_string();
    let mut pubkey = [0u8; 32];
    pubkey.copy_from_slice(&bytes[id_end..pubkey_end]);
    Some((node_id, pubkey))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // canonical_bytes_deterministic:start
    //   purpose: Same inputs → same canonical_bytes output (deterministic).
    //   input:  two identical calls
    //   output: equal byte slices
    //   sideEffects: none
    // canonical_bytes_deterministic:end
    #[test]
    fn canonical_bytes_deterministic() {
        let b1 = canonical_bytes("!r:s", "@a:s", "m.message", b"hello", &[], 0, 42);
        let b2 = canonical_bytes("!r:s", "@a:s", "m.message", b"hello", &[], 0, 42);
        assert_eq!(b1, b2, "same inputs must yield same canonical bytes");
    }

    // canonical_bytes_prev_order_independent:start
    //   purpose: prev_events are sorted before encoding; order of the input slice
    //            must not affect the canonical bytes.
    //   input:  two calls with prev_events in different order
    //   output: equal byte slices
    //   sideEffects: none
    // canonical_bytes_prev_order_independent:end
    #[test]
    fn canonical_bytes_prev_order_independent() {
        let prevs_ab = vec!["$b".to_string(), "$a".to_string()];
        let prevs_ba = vec!["$a".to_string(), "$b".to_string()];
        let b1 = canonical_bytes("!r", "@u", "t", b"c", &prevs_ab, 1, 10);
        let b2 = canonical_bytes("!r", "@u", "t", b"c", &prevs_ba, 1, 10);
        assert_eq!(b1, b2, "prev_events order must not matter");
    }

    // node_signer_sign_verify:start
    //   purpose: A signature produced by NodeSigner.sign() verifies correctly against
    //            the node's own verifying key stored in NodeKeyStore.
    //   input:  fresh NodeSigner (from_seed), sign bytes, insert pubkey, verify
    //   output: verify returns true
    //   sideEffects: none
    // node_signer_sign_verify:end
    #[test]
    fn node_signer_sign_verify() {
        let signer = NodeSigner::from_seed([1u8; 32], "node-test".to_string());
        let bytes = canonical_bytes("!r:s", "@u:s", "m.t", b"payload", &[], 0, 100);
        let sig = signer.sign(&bytes);
        assert_eq!(sig.len(), 64, "ed25519 sig must be 64 bytes");

        let store = NodeKeyStore::new();
        store.insert(&signer.node_id, signer.verifying_key_bytes());
        assert!(
            store.verify("node-test", &bytes, &sig),
            "valid signature must verify"
        );
    }

    // node_signer_bad_sig_rejected:start
    //   purpose: A corrupted/wrong signature fails verification.
    //   input:  valid sig, flip one byte, verify
    //   output: verify returns false
    //   sideEffects: none
    // node_signer_bad_sig_rejected:end
    #[test]
    fn node_signer_bad_sig_rejected() {
        let signer = NodeSigner::from_seed([2u8; 32], "node-test2".to_string());
        let bytes = canonical_bytes("!r", "@u", "t", b"data", &[], 0, 0);
        let mut sig = signer.sign(&bytes);
        sig[0] ^= 0xff; // corrupt first byte

        let store = NodeKeyStore::new();
        store.insert("node-test2", signer.verifying_key_bytes());
        assert!(
            !store.verify("node-test2", &bytes, &sig),
            "bad sig must not verify"
        );
    }

    // node_keystore_unknown_node:start
    //   purpose: Verifying a sig for a node_id not in the store returns false.
    //   input:  empty store; verify call with arbitrary bytes
    //   output: false
    //   sideEffects: none
    // node_keystore_unknown_node:end
    #[test]
    fn node_keystore_unknown_node() {
        let store = NodeKeyStore::new();
        assert!(
            !store.verify("no-such-node", b"bytes", &[0u8; 64]),
            "unknown node must not verify"
        );
    }

    // node_keystore_tofu_idempotent:start
    //   purpose: Inserting the same (node_id, key) twice returns true both times.
    //   input:  insert x2 same key
    //   output: both return true
    //   sideEffects: only one entry in store
    // node_keystore_tofu_idempotent:end
    #[test]
    fn node_keystore_tofu_idempotent() {
        let store = NodeKeyStore::new();
        let k = [3u8; 32];
        assert!(store.insert("node-x", k));
        assert!(store.insert("node-x", k)); // idempotent
        let snap = store.snapshot();
        assert_eq!(snap.len(), 1);
    }

    // node_keystore_tofu_key_mismatch:start
    //   purpose: Inserting a DIFFERENT key for an already-registered node_id returns false.
    //   input:  insert k1 for node-y; then insert k2 (different) for same node-y
    //   output: second insert returns false; store still has k1
    //   sideEffects: none (store unchanged on mismatch)
    // node_keystore_tofu_key_mismatch:end
    #[test]
    fn node_keystore_tofu_key_mismatch() {
        let store = NodeKeyStore::new();
        let k1 = [4u8; 32];
        let k2 = [5u8; 32];
        assert!(store.insert("node-y", k1));
        assert!(
            !store.insert("node-y", k2),
            "different key must be rejected"
        );
        // Store still has k1.
        let snap = store.snapshot();
        assert_eq!(snap["node-y"], k1);
    }

    // key_announcement_roundtrip:start
    //   purpose: encode_key_announcement / decode_key_announcement round-trip
    //            preserves node_id and pubkey exactly.
    //   input:  encode then decode a (node_id, pubkey) pair
    //   output: decoded pair equals the original
    //   sideEffects: none
    // key_announcement_roundtrip:end
    #[test]
    fn key_announcement_roundtrip() {
        let pubkey = [42u8; 32];
        let bytes = encode_key_announcement("node-cluster-7", &pubkey);
        let (node_id, decoded_key) =
            decode_key_announcement(&bytes).expect("valid announcement must decode");
        assert_eq!(node_id, "node-cluster-7");
        assert_eq!(decoded_key, pubkey);
    }

    // key_announcement_rejects_malformed:start
    //   purpose: decode_key_announcement returns None (never panics) on truncated or
    //            too-short input — the input originates from an untrusted mesh peer.
    //   input:  empty bytes; bytes shorter than the declared node_id_len + pubkey
    //   output: None in both cases
    //   sideEffects: none
    // key_announcement_rejects_malformed:end
    #[test]
    fn key_announcement_rejects_malformed() {
        assert!(
            decode_key_announcement(&[]).is_none(),
            "empty input must be rejected"
        );
        assert!(
            decode_key_announcement(&[1, 0]).is_none(),
            "declared len with no body must be rejected"
        );
        // Valid header, but truncated pubkey.
        let mut truncated = encode_key_announcement("n", &[9u8; 32]);
        truncated.truncate(truncated.len() - 1);
        assert!(
            decode_key_announcement(&truncated).is_none(),
            "truncated pubkey must be rejected"
        );
    }

    // node_auth:anchor_parsing:start
    //   purpose: parse_node_keys is a security control's only input, so it must be
    //            strict: a malformed entry has to fail the whole list rather than be
    //            skipped, or an allow-list quietly becomes a shorter allow-list.
    //   input:  none (pure function)
    //   output: assertions on accepted and rejected specs
    //   sideEffects: none
    // node_auth:anchor_parsing:end
    #[test]
    fn anchor_parsing() {
        let a = "a".repeat(64);
        let b = "b".repeat(64);

        let ok = parse_node_keys(&format!("node-a={a}, node-b={b}")).expect("valid spec");
        assert_eq!(ok.len(), 2);
        assert_eq!(ok["node-a"], [0xaa; 32]);
        assert_eq!(ok["node-b"], [0xbb; 32]);

        // Empty entries and stray whitespace are tolerated; nothing else is.
        assert_eq!(
            parse_node_keys(&format!("  node-a = {a} , ")).expect("tolerant").len(),
            1
        );
        assert!(parse_node_keys("").expect("empty spec is empty, not an error").is_empty());

        for bad in [
            format!("node-a{a}"),                 // no '='
            format!("node-a={}", "a".repeat(63)), // too short
            format!("node-a={}", "a".repeat(65)), // too long
            format!("node-a={}", "z".repeat(64)), // not hex
            format!("=@{a}"),                     // empty node_id
            format!("node-a={a},node-a={b}"),     // duplicate node_id
        ] {
            assert!(
                parse_node_keys(&bad).is_err(),
                "must reject the whole list, got Ok for {bad:?}"
            );
        }
    }

    // node_auth:anchored_store_is_closed:start
    //   purpose: An anchored store must refuse node_ids it was not given. That refusal
    //            is the entire point: the announcement channel is unauthenticated, so
    //            under TOFU whoever claims a node_id first owns it, and a claim is
    //            indistinguishable from the real thing.
    //   input:  none
    //   output: pinned nodes verify, unknown nodes are refused, pinned keys cannot be
    //           replaced
    //   sideEffects: none
    // node_auth:anchored_store_is_closed:end
    #[test]
    fn anchored_store_is_closed() {
        let signer = NodeSigner::generate("node-a".to_string());
        let intruder = NodeSigner::generate("node-x".to_string());

        let mut pinned = HashMap::new();
        pinned.insert("node-a".to_string(), signer.verifying_key_bytes());
        let store = NodeKeyStore::anchored(pinned);
        assert!(store.is_anchored());

        assert!(
            store.insert("node-a", signer.verifying_key_bytes()),
            "re-announcing a pinned node with its pinned key is fine"
        );
        assert!(
            !store.insert("node-x", intruder.verifying_key_bytes()),
            "a node that is not on the list must not be learned"
        );
        assert!(
            !store.insert("node-a", intruder.verifying_key_bytes()),
            "and a pinned key must not be replaced by an announcement"
        );

        // The refusal has teeth: an unpinned node's signature does not verify.
        let msg = b"hello";
        assert!(store.verify("node-a", msg, &signer.sign(msg)));
        assert!(!store.verify("node-x", msg, &intruder.sign(msg)));

        // Whereas a TOFU store learns it, which is exactly the difference.
        let tofu = NodeKeyStore::new();
        assert!(!tofu.is_anchored());
        assert!(tofu.insert("node-x", intruder.verifying_key_bytes()));
        assert!(tofu.verify("node-x", msg, &intruder.sign(msg)));
    }
}
