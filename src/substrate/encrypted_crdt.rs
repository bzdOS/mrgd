// START_AI_HEADER
// MODULE: mrgd/src/substrate/encrypted_crdt.rs
// PURPOSE: Per-scope E2EE wrapper for CrdtSink — encrypts delta payloads before
//          publishing and decrypts after draining. Each scope gets its own key
//          (derived from the scope's PSK or configured separately) so that
//          replicated payloads for one scope cannot be read by nodes not
//          participating in that scope.
// DEPENDENCIES: crate::substrate::crdt::{CrdtSink, CrdtError}, chacha20poly1305
// PUBLIC_API: EncryptedCrdtSink, derive_scope_key
// END_AI_HEADER

use crate::substrate::crdt::{CrdtSink, CrdtError};
use chacha20poly1305::{
    aead::{Aead, KeyInit},
    ChaCha20Poly1305, Key, Nonce,
};
use rand::RngCore;
use std::sync::Arc;

/// Encryption key for a scope (32 bytes for ChaCha20-Poly1305).
pub type ScopeKey = [u8; 32];

/// Derive a scope encryption key from a PSK string.
/// Uses SHA256(PSK | "mrgd-scope-key") to get a 32-byte key.
pub fn derive_scope_key(psk: &str) -> ScopeKey {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(psk.as_bytes());
    hasher.update(b"mrgd-scope-key");
    let result = hasher.finalize();
    let mut key = [0u8; 32];
    key.copy_from_slice(&result);
    key
}

/// Encrypted wrapper around a CrdtSink that provides per-scope E2EE.
/// Each instance is bound to one scope's encryption key.
pub struct EncryptedCrdtSink {
    inner: Arc<dyn CrdtSink>,
    cipher: ChaCha20Poly1305,
}

impl EncryptedCrdtSink {
    /// Create a new encrypted sink wrapping `inner` with the given scope key.
    pub fn new(inner: Arc<dyn CrdtSink>, scope_key: ScopeKey) -> Self {
        let key = Key::from_slice(&scope_key);
        let cipher = ChaCha20Poly1305::new(key);
        Self { inner, cipher }
    }

    /// Encrypt a payload with a random nonce (prepended to ciphertext).
    fn encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>, CrdtError> {
        let mut nonce_bytes = [0u8; 12];
        rand::rngs::OsRng.fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);

        let mut ciphertext = self
            .cipher
            .encrypt(nonce, plaintext)
            .map_err(|e| CrdtError::Crypto(format!("encryption failed: {e}")))?;

        // Prepend nonce to ciphertext for transport
        let mut result = Vec::with_capacity(12 + ciphertext.len());
        result.extend_from_slice(&nonce_bytes);
        result.append(&mut ciphertext);
        Ok(result)
    }

    /// Decrypt a payload (nonce prepended).
    fn decrypt(&self, ciphertext: &[u8]) -> Result<Vec<u8>, CrdtError> {
        if ciphertext.len() < 12 {
            return Err(CrdtError::Crypto("ciphertext too short".to_string()));
        }
        let nonce = Nonce::from_slice(&ciphertext[..12]);
        let payload = &ciphertext[12..];

        self.cipher
            .decrypt(nonce, payload)
            .map_err(|e| CrdtError::Crypto(format!("decryption failed: {e}")))
    }
}

impl CrdtSink for EncryptedCrdtSink {
    fn publish(&self, key: &str, bytes: Vec<u8>) -> Result<(), CrdtError> {
        let encrypted = self.encrypt(&bytes)?;
        self.inner.publish(key, encrypted)
    }

    fn drain(&self, key: &str) -> Result<Vec<Vec<u8>>, CrdtError> {
        let blobs = self.inner.drain(key)?;
        let mut decrypted = Vec::with_capacity(blobs.len());
        for blob in blobs {
            decrypted.push(self.decrypt(&blob)?);
        }
        Ok(decrypted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::substrate::crdt::{MemCrdtSink, GCounter};

    #[test]
    fn encrypted_sink_roundtrip() {
        let (sink_a, sink_b) = MemCrdtSink::pair();

        // Derive a test scope key
        let scope_key = derive_scope_key("test-psk-123");

        // Wrap both sinks with encryption
        let enc_a = Arc::new(EncryptedCrdtSink::new(Arc::new(sink_a), scope_key));
        let enc_b = Arc::new(EncryptedCrdtSink::new(Arc::new(sink_b), scope_key));

        // Create a counter delta
        let mut counter = GCounter::new();
        counter.increment(1, 42);
        let delta = counter.delta();

        // Serialize delta (reuse test serialization)
        let mut buf = Vec::new();
        buf.extend_from_slice(&(delta.slots.len() as u32).to_le_bytes());
        for (&node, &val) in &delta.slots {
            buf.extend_from_slice(&node.to_le_bytes());
            buf.extend_from_slice(&val.to_le_bytes());
        }

        // Publish through encrypted sink
        enc_a.publish("test-counter", buf).expect("publish ok");

        // Drain through encrypted sink
        let received = enc_b.drain("test-counter").expect("drain ok");
        assert_eq!(received.len(), 1);

        // Deserialize and verify
        let mut received_delta = crate::substrate::crdt::GCounterDelta { slots: std::collections::HashMap::new() };
        let n = u32::from_le_bytes(received[0][0..4].try_into().unwrap()) as usize;
        let mut off = 4;
        for _ in 0..n {
            let node = u64::from_le_bytes(received[0][off..off+8].try_into().unwrap());
            let val = u64::from_le_bytes(received[0][off+8..off+16].try_into().unwrap());
            received_delta.slots.insert(node, val);
            off += 16;
        }

        assert_eq!(received_delta.slots.get(&1), Some(&42));
    }

    #[test]
    fn different_scope_keys_cannot_decrypt() {
        let (sink_a, sink_b) = MemCrdtSink::pair();

        let key_a = derive_scope_key("scope-a-psk");
        let key_b = derive_scope_key("scope-b-psk");

        let enc_a = Arc::new(EncryptedCrdtSink::new(Arc::new(sink_a), key_a));
        let enc_b = Arc::new(EncryptedCrdtSink::new(Arc::new(sink_b), key_b));

        let mut counter = GCounter::new();
        counter.increment(1, 42);
        let mut buf = Vec::new();
        buf.extend_from_slice(&(counter.delta().slots.len() as u32).to_le_bytes());
        for (&node, &val) in &counter.delta().slots {
            buf.extend_from_slice(&node.to_le_bytes());
            buf.extend_from_slice(&val.to_le_bytes());
        }

        enc_a.publish("test", buf).expect("publish ok");

        // Drain with different key should fail
        let result = enc_b.drain("test");
        assert!(result.is_err(), "different scope key must not decrypt");
    }
}