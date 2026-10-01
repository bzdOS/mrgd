// START_AI_HEADER
// MODULE: couplingd/src/svc.rs
// PURPOSE: Service registry primitive for couplingd — Ярус 2, SPEC_coupling_v1 §3.
//          Implements SVC REG|RESOLVE: name→node mapping tied to a session lease.
//          When the owning session expires, the registration is auto-removed.
// INTENT: Stub for next agent. Signatures are final; bodies use in-memory HashMap.
//         Next agent replaces with Zenoh-published svc table + raft-committed REG.
// DEPENDENCIES: std, thiserror
// PUBLIC_API: SvcEntry, SvcError, SvcStore, register, resolve, unregister, expire_for_session
// END_AI_HEADER

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use thiserror::Error;
use crate::session::SessionId;

/// A registered service instance — mirrors capnp SvcReg.
#[derive(Debug, Clone)]
pub struct SvcEntry {
    /// Service name (e.g. "pg-matrix", "synapse").
    pub name: String,
    /// Node hosting the instance.
    pub node: u64,
    /// Session ID owning this registration (auto-expires with session).
    pub sid:  SessionId,
}

/// Errors produced by service-registry operations.
#[derive(Debug, Error)]
pub enum SvcError {
    #[error("service '{0}' not found")]
    NotFound(String),
    /// Kept for future use (e.g. read-only observers). Not raised by M1 register().
    #[error("service '{0}' already registered by session {1}")]
    AlreadyRegistered(String, SessionId),
    /// Raised when a session attempts to unregister a name it does not own.
    #[error("service '{0}' is not owned by session {1}")]
    NotOwner(String, SessionId),
    #[error("store lock poisoned")]
    Poisoned,
}

/// Thread-safe service registry.
#[derive(Clone, Default)]
pub struct SvcStore {
    inner: Arc<Mutex<HashMap<String, SvcEntry>>>,
}

impl SvcStore {
    // new:start
    //   purpose: Construct an empty SvcStore.
    //   input:  none
    //   output: SvcStore
    //   sideEffects: allocates Arc<Mutex<HashMap>>
    // new:end
    pub fn new() -> Self {
        Self::default()
    }
}

// register:start
//   purpose: Establish or take over a service name → (node, sid) binding.
//            If the name is already registered by any session (including a different one),
//            the new registration wins — last registrant owns the name (take-over).
//            Same-session re-registration updates the node field.
//            TODO(raft): replace with raft-propose + Zenoh publish on bsdos/cf/<group>/svc/<name>.
//   input:  store — shared SvcStore; name — service name; node — hosting node ID; sid — owning session
//   output: Result<(), SvcError>
//   sideEffects: inserts or overwrites entry under Mutex write lock
// register:end
pub fn register(
    store: &SvcStore,
    name:  &str,
    node:  u64,
    sid:   SessionId,
) -> Result<(), SvcError> {
    let mut guard = store.inner.lock().map_err(|_| SvcError::Poisoned)?;
    // Take-over: last registrant wins unconditionally.
    guard.insert(name.to_string(), SvcEntry {
        name: name.to_string(),
        node,
        sid,
    });
    Ok(())
}

// resolve:start
//   purpose: Resolve a service name to its current SvcEntry (node + owning session).
//            STUB — next agent may fan out to Zenoh for cross-node resolution.
//   input:  store — shared SvcStore; name — service name to look up
//   output: Result<SvcEntry, SvcError>
//   sideEffects: Mutex read lock on store inner
// resolve:end
pub fn resolve(store: &SvcStore, name: &str) -> Result<SvcEntry, SvcError> {
    store.inner.lock().map_err(|_| SvcError::Poisoned)?
        .get(name)
        .cloned()
        .ok_or_else(|| SvcError::NotFound(name.to_string()))
}

// unregister:start
//   purpose: Explicitly remove a service registration. Only the owning session may unregister.
//            Absent name is treated as idempotent Ok(()) — safe for double-call after
//            expire_for_session already removed the entry.
//            Wrong sid (not the current owner) → Err(SvcError::NotOwner).
//            TODO(raft): add raft-committed removal + Zenoh undeclare.
//   input:  store — shared SvcStore; name — service name; sid — requesting session
//   output: Result<(), SvcError>; NotOwner if sid != current owner; Ok(()) if name absent
//   sideEffects: removes entry under Mutex write lock when owner matches
// unregister:end
pub fn unregister(
    store: &SvcStore,
    name:  &str,
    sid:   SessionId,
) -> Result<(), SvcError> {
    let mut guard = store.inner.lock().map_err(|_| SvcError::Poisoned)?;
    match guard.get(name) {
        // Absent → idempotent Ok(()): safe after expire_for_session already cleaned up.
        None => Ok(()),
        Some(e) if e.sid != sid => Err(SvcError::NotOwner(name.to_string(), sid)),
        Some(_) => {
            guard.remove(name);
            Ok(())
        }
    }
}

// expire_for_session:start
//   purpose: Remove all service registrations owned by the given session.
//            Called by the expiry sweep in session.rs when a session's TTL elapses.
//            STUB — next agent drives this from expire_dead() cascade.
//   input:  store — shared SvcStore; sid — session being reaped
//   output: Result<Vec<String>, SvcError> — names that were unregistered
//   sideEffects: removes matching entries under Mutex write lock
// expire_for_session:end
pub fn expire_for_session(
    store: &SvcStore,
    sid:   SessionId,
) -> Result<Vec<String>, SvcError> {
    let mut guard = store.inner.lock().map_err(|_| SvcError::Poisoned)?;
    let removed: Vec<String> = guard
        .iter()
        .filter(|(_, e)| e.sid == sid)
        .map(|(name, _)| name.clone())
        .collect();
    for name in &removed {
        guard.remove(name.as_str());
    }
    Ok(removed)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── register → resolve ────────────────────────────────────────────────────

    #[test]
    fn register_then_resolve_returns_entry() {
        let store = SvcStore::new();
        register(&store, "pg-matrix", 7, 101).expect("register must succeed");
        let e = resolve(&store, "pg-matrix").expect("resolve must succeed");
        assert_eq!(e.name, "pg-matrix");
        assert_eq!(e.node, 7);
        assert_eq!(e.sid, 101);
    }

    // ── resolve absent name → NotFound ────────────────────────────────────────

    #[test]
    fn resolve_absent_returns_not_found() {
        let store = SvcStore::new();
        let err = resolve(&store, "absent").expect_err("resolve of absent must fail");
        assert!(
            matches!(err, SvcError::NotFound(ref n) if n == "absent"),
            "expected NotFound(absent), got {err:?}"
        );
    }

    // ── unregister by owner removes entry ─────────────────────────────────────

    #[test]
    fn unregister_owner_removes_entry() {
        let store = SvcStore::new();
        register(&store, "synapse", 3, 200).expect("register");
        unregister(&store, "synapse", 200).expect("unregister by owner must succeed");
        let err = resolve(&store, "synapse").expect_err("must be gone");
        assert!(matches!(err, SvcError::NotFound(_)));
    }

    // ── unregister by non-owner → NotOwner (entry stays) ─────────────────────

    #[test]
    fn unregister_wrong_sid_returns_not_owner() {
        let store = SvcStore::new();
        register(&store, "redis", 5, 300).expect("register");
        let err = unregister(&store, "redis", 999).expect_err("wrong sid must fail");
        assert!(
            matches!(err, SvcError::NotOwner(ref n, 999) if n == "redis"),
            "expected NotOwner(redis, 999), got {err:?}"
        );
        // Entry must still be present.
        resolve(&store, "redis").expect("entry must survive failed unregister");
    }

    // ── take-over: second register with different sid wins ────────────────────

    #[test]
    fn register_takeover_last_wins() {
        let store = SvcStore::new();
        register(&store, "cache", 1, 400).expect("first register");
        // Different sid takes over.
        register(&store, "cache", 2, 401).expect("take-over must succeed");
        let e = resolve(&store, "cache").expect("resolve after take-over");
        assert_eq!(e.node, 2,  "node must be updated to new provider");
        assert_eq!(e.sid,  401, "sid must be the new owner");
    }

    // ── expire_for_session removes all names owned by sid ─────────────────────

    #[test]
    fn expire_for_session_removes_all_owned_names() {
        let store = SvcStore::new();
        // sid 500 owns two names.
        register(&store, "svc-a", 10, 500).expect("register svc-a");
        register(&store, "svc-b", 11, 500).expect("register svc-b");
        // sid 501 owns one name; must not be touched.
        register(&store, "svc-c", 12, 501).expect("register svc-c");

        let mut removed = expire_for_session(&store, 500).expect("expire_for_session");
        removed.sort(); // order is not guaranteed
        assert_eq!(removed, vec!["svc-a".to_string(), "svc-b".to_string()]);

        // sid 500's entries must be gone.
        assert!(matches!(resolve(&store, "svc-a"), Err(SvcError::NotFound(_))));
        assert!(matches!(resolve(&store, "svc-b"), Err(SvcError::NotFound(_))));
        // sid 501's entry must survive.
        resolve(&store, "svc-c").expect("svc-c must survive expiry of sid 500");
    }

    // ── expire_for_session on unknown sid → Ok, empty list ───────────────────

    #[test]
    fn expire_for_session_unknown_sid_returns_empty() {
        let store = SvcStore::new();
        register(&store, "x", 1, 600).expect("register");
        let removed = expire_for_session(&store, 9999).expect("expire unknown sid");
        assert!(removed.is_empty(), "no names owned by unknown sid");
        // Existing entry must be untouched.
        resolve(&store, "x").expect("x must still exist");
    }

    // ── unregister absent name → idempotent Ok(()) ───────────────────────────

    #[test]
    fn unregister_absent_name_is_idempotent() {
        let store = SvcStore::new();
        // Absent name with any sid → Ok(()).
        unregister(&store, "does-not-exist", 700)
            .expect("unregister of absent name must be Ok");
    }
}
