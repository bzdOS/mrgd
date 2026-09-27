// START_AI_HEADER
// MODULE: mrgd/src/substrate/publication.rs
// PURPOSE: Selective cross-scope federation publication primitive.
//          Allows controlled sharing of CRDT data from one scope to another
//          by forwarding selected keys from a source scope's CRDT sink to
//          a target scope's CRDT sink. This is the "publication primitive":
//          selective cross-scope federation.
// DEPENDENCIES: crate::substrate::crdt::{CrdtSink, CrdtError}
// PUBLIC_API: PublicationPolicy, PublicationSink, PublicationRule
// END_AI_HEADER

use crate::substrate::crdt::{CrdtSink, CrdtError};
use std::sync::Arc;

/// A rule defining which keys to publish from source to target scope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublicationRule {
    /// Pattern to match source keys (supports '*' wildcard at end).
    /// E.g., "counter/*" matches "counter/hits", "counter/users", etc.
    pub source_pattern: String,
    /// Target key prefix to prepend when forwarding.
    /// If empty, the source key is used as-is (with target scope's prefix added by sink).
    pub target_prefix: String,
    /// Optional filter: only forward if key contains this substring.
    pub filter_contains: Option<String>,
}

impl PublicationRule {
    /// Check if a source key matches this rule's pattern.
    pub fn matches(&self, key: &str) -> bool {
        if let Some(prefix) = self.source_pattern.strip_suffix('*') {
            if !key.starts_with(prefix) {
                return false;
            }
        } else if self.source_pattern != key {
            return false;
        }
        if let Some(filter) = &self.filter_contains {
            if !key.contains(filter) {
                return false;
            }
        }
        true
    }

    /// Transform a source key to its target form.
    pub fn transform_key(&self, key: &str) -> String {
        if self.target_prefix.is_empty() {
            return key.to_string();
        }
        if let Some(prefix) = self.source_pattern.strip_suffix('*') {
            if let Some(suffix) = key.strip_prefix(prefix) {
                return format!("{}{}", self.target_prefix, suffix);
            }
        } else if self.source_pattern == key {
            // Exact match: append the full key after the prefix
            return format!("{}{}", self.target_prefix, key);
        }
        // No match (should not happen if matches() returned true)
        format!("{}{}", self.target_prefix, key)
    }
}

/// Configuration for cross-scope publication.
#[derive(Clone, Debug, Default)]
pub struct PublicationPolicy {
    /// Rules evaluated in order; first match wins.
    pub rules: Vec<PublicationRule>,
}

impl PublicationPolicy {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_rule(mut self, rule: PublicationRule) -> Self {
        self.rules.push(rule);
        self
    }

    /// Find the first matching rule for a key.
    pub fn match_rule(&self, key: &str) -> Option<&PublicationRule> {
        self.rules.iter().find(|r| r.matches(key))
    }
}

/// A CRDT sink that selectively forwards publications to a target sink
/// based on a publication policy. Reads (drain) are passed through to the
/// inner sink (for local consumption).
pub struct PublicationSink {
    /// Source sink (reads from this scope).
    inner: Arc<dyn CrdtSink>,
    /// Target sink (writes to another scope).
    target: Arc<dyn CrdtSink>,
    /// Publication policy.
    policy: PublicationPolicy,
}

impl PublicationSink {
    /// Create a new publication sink.
    pub fn new(
        inner: Arc<dyn CrdtSink>,
        target: Arc<dyn CrdtSink>,
        policy: PublicationPolicy,
    ) -> Self {
        Self {
            inner,
            target,
            policy,
        }
    }

    /// Get a reference to the inner sink.
    pub fn inner(&self) -> &Arc<dyn CrdtSink> {
        &self.inner
    }

    /// Get a reference to the target sink.
    pub fn target(&self) -> &Arc<dyn CrdtSink> {
        &self.target
    }
}

impl CrdtSink for PublicationSink {
    fn publish(&self, key: &str, bytes: Vec<u8>) -> Result<(), CrdtError> {
        // Always publish to the inner (source) sink first.
        self.inner.publish(key, bytes.clone())?;

        // Check if this key should be forwarded to the target scope.
        if let Some(rule) = self.policy.match_rule(key) {
            let target_key = rule.transform_key(key);
            self.target.publish(&target_key, bytes)?;
        }
        Ok(())
    }

    fn drain(&self, key: &str) -> Result<Vec<Vec<u8>>, CrdtError> {
        // Drains come from the inner sink only (local scope data).
        self.inner.drain(key)
    }
}

/// Builder for common publication patterns.
pub mod presets {
    use super::*;

    /// Publish all keys from source to target with a prefix.
    pub fn publish_all(prefix: &str) -> PublicationPolicy {
        PublicationPolicy::new().with_rule(PublicationRule {
            source_pattern: "*".to_string(),
            target_prefix: prefix.to_string(),
            filter_contains: None,
        })
    }

    /// Publish only counter keys.
    pub fn publish_counters(target_prefix: &str) -> PublicationPolicy {
        PublicationPolicy::new().with_rule(PublicationRule {
            source_pattern: "counter/*".to_string(),
            target_prefix: target_prefix.to_string(),
            filter_contains: None,
        })
    }

    /// Publish only OrSet keys matching a pattern.
    pub fn publish_orset(prefix: &str, target_prefix: &str) -> PublicationPolicy {
        PublicationPolicy::new().with_rule(PublicationRule {
            source_pattern: format!("{prefix}/*"),
            target_prefix: target_prefix.to_string(),
            filter_contains: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::substrate::crdt::{MemCrdtSink, GCounter};

    fn make_counter_delta() -> Vec<u8> {
        let mut counter = GCounter::new();
        counter.increment(1, 42);
        let delta = counter.delta();
        let mut buf = Vec::new();
        buf.extend_from_slice(&(delta.slots.len() as u32).to_le_bytes());
        for (&node, &val) in &delta.slots {
            buf.extend_from_slice(&node.to_le_bytes());
            buf.extend_from_slice(&val.to_le_bytes());
        }
        buf
    }

    #[test]
    fn publication_sink_forwards_matching_keys() {
        let (source_inner, _) = MemCrdtSink::pair();
        let (target_inner, target_outer) = MemCrdtSink::pair();

        let policy = presets::publish_all("published/");
        let pub_sink = PublicationSink::new(
            Arc::new(source_inner),
            Arc::new(target_inner),
            policy,
        );

        let delta = make_counter_delta();
        pub_sink.publish("counter/hits", delta.clone()).expect("publish ok");

        // Target should receive the forwarded key with prefix
        let received = target_outer.drain("published/counter/hits").expect("drain ok");
        assert_eq!(received.len(), 1);
        assert_eq!(received[0], delta);
    }

    #[test]
    fn publication_sink_does_not_forward_non_matching() {
        let (source_inner, _) = MemCrdtSink::pair();
        let (target_inner, target_outer) = MemCrdtSink::pair();

        let policy = presets::publish_counters("pub/");
        let pub_sink = PublicationSink::new(
            Arc::new(source_inner),
            Arc::new(target_inner),
            policy,
        );

        let delta = make_counter_delta();
        pub_sink.publish("orset/users", delta).expect("publish ok");

        // Target should NOT receive non-matching key
        let received = target_outer.drain("pub/orset/users").expect("drain ok");
        assert!(received.is_empty());
    }

    #[test]
    fn publication_rule_transforms_keys() {
        let rule = PublicationRule {
            source_pattern: "counter/*".to_string(),
            target_prefix: "fed/".to_string(),
            filter_contains: None,
        };
        assert!(rule.matches("counter/hits"));
        assert!(rule.matches("counter/users"));
        assert!(!rule.matches("orset/users"));
        assert_eq!(rule.transform_key("counter/hits"), "fed/hits");
        assert_eq!(rule.transform_key("counter/users"), "fed/users");
    }

    #[test]
    fn publication_rule_exact_match() {
        let rule = PublicationRule {
            source_pattern: "special/key".to_string(),
            target_prefix: "mirror/".to_string(),
            filter_contains: None,
        };
        assert!(rule.matches("special/key"));
        assert!(!rule.matches("special/key2"));
        // Exact match with no wildcard prepends the full key
        assert_eq!(rule.transform_key("special/key"), "mirror/special/key");
    }

    #[test]
    fn publication_rule_with_filter() {
        let rule = PublicationRule {
            source_pattern: "*".to_string(),
            target_prefix: "ext/".to_string(),
            filter_contains: Some("public".to_string()),
        };
        assert!(rule.matches("counter/public_hits"));
        assert!(!rule.matches("counter/private_hits"));
        assert_eq!(rule.transform_key("counter/public_hits"), "ext/counter/public_hits");
    }
}