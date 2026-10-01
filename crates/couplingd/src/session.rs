// START_AI_HEADER
// MODULE: couplingd/src/session.rs
// PURPOSE: Session/lease primitive for couplingd — Ярус 2, SPEC_coupling_v1 §3.
//          Управляет выдачей, продлением и закрытием сессий (Session capnp struct).
//          Смерть узла → TTL истёк → все locks/ephemeral-keys/svc-reg сняты через
//          инъектируемые каскад-функции (избегаем circular dep: lock.rs → session.rs).
// INTENT: M1 single-node in-memory implementation.
//         Public API signatures match main.rs expectations; deterministic clock is
//         injected via _timed variants used in tests (no wall-clock in logic paths).
//         Cascade cleanup is injected as fn closures so lock/svc stay decoupled.
//         Raft integration deferred (marked TODO(raft)).
// DEPENDENCIES: std, thiserror
// PUBLIC_API: SessionId, NodeId, SessionEntry, SessionStore, SessionError,
//             open, keepalive, close, expire_dead
// DESIGN NOTE: lock.rs already imports crate::session::SessionId — a direct
//              use of lock::release_all_for_session here would form a circular
//              import between the two modules.  We break the cycle by accepting
//              cascade callbacks as fn(SessionId)->Result<(),String> closures
//              passed by the caller (main.rs or test harness).
//              Production wiring in main.rs:
//                lock::release_all_for_session  → lock_cascade closure
//                svc::expire_for_session        → svc_cascade closure
// END_AI_HEADER

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};
use thiserror::Error;

/// Opaque 64-bit session identifier (matches capnp Session.id).
pub type SessionId = u64;

/// Opaque 64-bit node identifier (matches capnp Session.node).
pub type NodeId = u64;

/// Per-session internal state tracked by the store.
/// last_seen is stored as a millisecond timestamp (u64) so that
/// expire logic can accept an injected now_ms — no wall-clock in the
/// comparison paths.
#[derive(Debug, Clone)]
pub struct SessionEntry {
    pub id:        SessionId,
    pub node:      NodeId,
    /// TTL in milliseconds.
    pub ttl_ms:    u64,
    /// Monotonic epoch at session creation (from the raft leader).
    pub epoch:     u64,
    /// Millisecond timestamp of last successful KEEPALIVE (or open).
    pub last_seen: u64,
}

/// Global monotonic session-ID counter. Shared across all SessionStore instances
/// in a process.  TODO(raft): replace with leader-assigned ID via raft-CAS.
static SESSION_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Return the current time as milliseconds since UNIX epoch.
/// Used only by the public (wall-clock) variants; logic paths use injected now_ms.
fn now_ms_wall() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Thread-safe session store shared between the accept loop and the expiry task.
#[derive(Clone, Default)]
pub struct SessionStore {
    inner: Arc<Mutex<HashMap<SessionId, SessionEntry>>>,
}

/// Errors produced by session operations.
#[derive(Debug, Error)]
pub enum SessionError {
    #[error("session {0} not found")]
    NotFound(SessionId),
    #[error("session {0} expired")]
    Expired(SessionId),
    #[error("store lock poisoned")]
    Poisoned,
    #[error("lock cascade error: {0}")]
    LockCascade(String),
    #[error("svc cascade error: {0}")]
    SvcCascade(String),
}

impl SessionStore {
    // new:start
    //   purpose: Construct an empty SessionStore.
    //   input:  none
    //   output: SessionStore
    //   sideEffects: allocates Arc<Mutex<HashMap>>
    // new:end
    pub fn new() -> Self {
        Self::default()
    }
}

// ── Core timed implementations (injected clock — used in tests) ───────────────

// open_timed:start
//   purpose: Create a new session entry, using a caller-supplied now_ms timestamp.
//            Allows deterministic testing without real wall-clock.
//   input:  store, node, ttl_ms, epoch — same semantics as open(); now_ms — injected time
//   output: Result<SessionId, SessionError>
//   sideEffects: inserts SessionEntry; increments SESSION_COUNTER
// open_timed:end
fn open_timed(
    store:  &SessionStore,
    node:   NodeId,
    ttl_ms: u32,
    epoch:  u64,
    now_ms: u64,
) -> Result<SessionId, SessionError> {
    // TODO(raft): replace with raft-proposed, leader-assigned monotonic ID.
    let id: SessionId = SESSION_COUNTER.fetch_add(1, Ordering::Relaxed);

    let entry = SessionEntry {
        id,
        node,
        ttl_ms:    u64::from(ttl_ms),
        epoch,
        last_seen: now_ms,
    };

    store.inner.lock().map_err(|_| SessionError::Poisoned)?
        .insert(id, entry);

    Ok(id)
}

// keepalive_timed:start
//   purpose: Renew a session's last_seen to now_ms, rejecting already-expired sessions.
//   input:  store, sid — same semantics as keepalive(); now_ms — injected time
//   output: Result<(), SessionError>
//   sideEffects: updates SessionEntry.last_seen under Mutex lock
// keepalive_timed:end
fn keepalive_timed(
    store:  &SessionStore,
    sid:    SessionId,
    now_ms: u64,
) -> Result<(), SessionError> {
    let mut guard = store.inner.lock().map_err(|_| SessionError::Poisoned)?;
    let entry = guard.get_mut(&sid).ok_or(SessionError::NotFound(sid))?;

    let elapsed = now_ms.saturating_sub(entry.last_seen);
    if elapsed > entry.ttl_ms {
        return Err(SessionError::Expired(sid));
    }

    entry.last_seen = now_ms;
    Ok(())
}

// close_timed:start
//   purpose: Remove a session and invoke cascade callbacks.
//            Idempotent: returns Ok(()) if the session was already absent.
//   input:  store, sid — session to close;
//           lock_cascade — called with sid to release all locks;
//           svc_cascade  — called with sid to expire all svc registrations
//   output: Result<(), SessionError>
//   sideEffects: removes entry; calls lock_cascade and svc_cascade
// close_timed:end
fn close_timed(
    store:        &SessionStore,
    sid:          SessionId,
    lock_cascade: &dyn Fn(SessionId) -> Result<(), String>,
    svc_cascade:  &dyn Fn(SessionId) -> Result<(), String>,
) -> Result<(), SessionError> {
    let removed = store.inner.lock().map_err(|_| SessionError::Poisoned)?
        .remove(&sid);

    if removed.is_none() {
        // Already gone — idempotent, no cascade.
        return Ok(());
    }

    lock_cascade(sid).map_err(SessionError::LockCascade)?;
    svc_cascade(sid).map_err(SessionError::SvcCascade)?;

    Ok(())
}

// expire_dead_timed:start
//   purpose: Scan store for sessions whose TTL elapsed relative to now_ms, remove
//            them, and invoke cascade callbacks for each.
//   input:  store, now_ms — injected current time;
//           lock_cascade — called per expired sid;
//           svc_cascade  — called per expired sid
//   output: Result<Vec<SessionId>, SessionError> — reaped session IDs
//   sideEffects: removes expired entries; calls cascades outside the store lock
// expire_dead_timed:end
fn expire_dead_timed(
    store:        &SessionStore,
    now_ms:       u64,
    lock_cascade: &dyn Fn(SessionId) -> Result<(), String>,
    svc_cascade:  &dyn Fn(SessionId) -> Result<(), String>,
) -> Result<Vec<SessionId>, SessionError> {
    // Collect and remove while holding the lock; release before cascading
    // to avoid lock-ordering issues with the lock/svc store Mutexes.
    let expired: Vec<SessionId> = {
        let mut guard = store.inner.lock().map_err(|_| SessionError::Poisoned)?;

        let expired: Vec<SessionId> = guard
            .iter()
            .filter(|(_, e)| now_ms.saturating_sub(e.last_seen) > e.ttl_ms)
            .map(|(id, _)| *id)
            .collect();

        for id in &expired {
            guard.remove(id);
        }

        expired
    };

    for sid in &expired {
        // Best-effort for M1: one failed cascade doesn't block the others.
        // TODO(raft): propagate or alert on cascade failure.
        let _ = lock_cascade(*sid);
        let _ = svc_cascade(*sid);
    }

    Ok(expired)
}

// ── Public API (wall-clock variants called by main.rs / text protocol) ────────

// open:start
//   purpose: Open a new session for the given node with the specified TTL.
//            Assigns a monotonically increasing SessionId, records epoch.
//            Wall-clock is used for last_seen; tests should use open_timed.
//            TODO(raft): replace counter with leader-assigned ID via raft-CAS.
//   input:  store   — shared SessionStore; node — caller's NodeId;
//           ttl_ms  — lease TTL in milliseconds; epoch — raft epoch (0 for M1)
//   output: Result<SessionId, SessionError>
//   sideEffects: inserts SessionEntry into store under Mutex lock;
//                increments global SESSION_COUNTER
// open:end
pub fn open(
    store:  &SessionStore,
    node:   NodeId,
    ttl_ms: u32,
    epoch:  u64,
) -> Result<SessionId, SessionError> {
    open_timed(store, node, ttl_ms, epoch, now_ms_wall())
}

// keepalive:start
//   purpose: Renew the TTL of an existing session by resetting its last_seen timestamp.
//            Returns NotFound if the session does not exist.
//            Returns Expired if the session's TTL has already elapsed.
//            Wall-clock variant: tests use keepalive_timed for determinism.
//            TODO(raft): broadcast renewal to all replicas.
//   input:  store — shared SessionStore; sid — session to renew
//   output: Result<(), SessionError>
//   sideEffects: updates SessionEntry.last_seen under Mutex lock
// keepalive:end
pub fn keepalive(
    store: &SessionStore,
    sid:   SessionId,
) -> Result<(), SessionError> {
    keepalive_timed(store, sid, now_ms_wall())
}

// close:start
//   purpose: Explicitly close a session and immediately cascade cleanup.
//            Idempotent: returns Ok(()) if the session is not found.
//            Cascade callbacks release locks and svc registrations owned by sid.
//            TODO(raft): drive raft-committed removal before local cascade.
//   input:  store — shared SessionStore; sid — session to close
//   output: Result<(), SessionError>
//   sideEffects: removes SessionEntry; invokes lock/svc cascade stubs (no-ops in M1)
// close:end
pub fn close(
    store: &SessionStore,
    sid:   SessionId,
) -> Result<(), SessionError> {
    // M1 wall-clock close: cascade stubs are no-ops.
    // Production wiring: replace with closures over lock::release_all_for_session
    // and svc::expire_for_session once lock.rs is complete.
    // TODO(raft): wire real cascade once lock::release_all_for_session is defined.
    close_timed(store, sid, &|_| Ok(()), &|_| Ok(()))
}

// expire_dead:start
//   purpose: Scan the store and remove all sessions whose TTL has elapsed.
//            Returns the list of expired SessionIds.
//            Wall-clock variant: tests use expire_dead_timed for determinism.
//            Cascade stubs are no-ops — callers that own all stores should use
//            expire_dead_cascaded to wire real cleanup.
//            TODO(raft): drive expiry via raft-committed log entry.
//   input:  store — shared SessionStore
//   output: Result<Vec<SessionId>, SessionError>
//   sideEffects: removes expired entries; no cascade (stubs)
// expire_dead:end
pub fn expire_dead(
    store: &SessionStore,
) -> Result<Vec<SessionId>, SessionError> {
    expire_dead_timed(store, now_ms_wall(), &|_| Ok(()), &|_| Ok(()))
}

// expire_dead_cascaded:start
//   purpose: Scan the store for TTL-expired sessions, remove them, and invoke
//            the provided cascade callbacks for each expired session.
//            Used by server.rs background tick, which owns all stores and can
//            supply real lock::release_all_for_session + svc::expire_for_session.
//            Avoids a circular import: session.rs stays free of lock/svc deps.
//            TODO(raft): replace with raft-committed heartbeat expiry.
//   input:  store — shared SessionStore;
//           lock_cascade — called per expired sid (releases locks);
//           svc_cascade  — called per expired sid (removes svc regs)
//   output: Result<Vec<SessionId>, SessionError> — reaped session IDs
//   sideEffects: removes expired entries; calls cascades outside the store lock
// expire_dead_cascaded:end
pub fn expire_dead_cascaded(
    store:        &SessionStore,
    lock_cascade: &dyn Fn(SessionId) -> Result<(), String>,
    svc_cascade:  &dyn Fn(SessionId) -> Result<(), String>,
) -> Result<Vec<SessionId>, SessionError> {
    expire_dead_timed(store, now_ms_wall(), lock_cascade, svc_cascade)
}

// close_with_cascade:start
//   purpose: Remove a session and invoke the provided cascade callbacks immediately.
//            Used by server.rs dispatch (SESSION CLOSE) which owns all stores and
//            can supply real lock::release_all_for_session + svc::expire_for_session.
//            Idempotent: Ok(()) if the session is already absent.
//            Avoids a circular import: session.rs stays free of lock/svc deps.
//            TODO(raft): drive raft-committed removal before local cascade.
//   input:  store — shared SessionStore; sid — session to close;
//           lock_cascade — called with sid to release locks;
//           svc_cascade  — called with sid to expire svc regs
//   output: Result<(), SessionError>
//   sideEffects: removes SessionEntry; calls lock_cascade and svc_cascade
// close_with_cascade:end
pub fn close_with_cascade(
    store:        &SessionStore,
    sid:          SessionId,
    lock_cascade: &dyn Fn(SessionId) -> Result<(), String>,
    svc_cascade:  &dyn Fn(SessionId) -> Result<(), String>,
) -> Result<(), SessionError> {
    close_timed(store, sid, lock_cascade, svc_cascade)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── Cascade recorder ──────────────────────────────────────────────────────
    //
    // Records every sid passed to a cascade closure so tests can assert
    // that the cascade was (or was not) invoked.

    #[derive(Clone, Default)]
    struct CascadeRecorder {
        called: Arc<Mutex<Vec<SessionId>>>,
    }

    impl CascadeRecorder {
        fn new() -> Self { Self::default() }

        fn closure(&self) -> impl Fn(SessionId) -> Result<(), String> + '_ {
            let called = Arc::clone(&self.called);
            move |sid| {
                called.lock().unwrap().push(sid);
                Ok(())
            }
        }

        fn was_called_with(&self, sid: SessionId) -> bool {
            self.called.lock().unwrap().contains(&sid)
        }
    }

    // Noop cascade — for tests that only care about session state, not cascades.
    fn noop(sid: SessionId) -> Result<(), String> { let _ = sid; Ok(()) }

    // Open at injected time 0 with ttl = 1000 ms.
    fn open_t0(store: &SessionStore) -> SessionId {
        open_timed(store, 42, 1000, 0, 0).expect("open_timed must succeed")
    }

    // ── open_timed ────────────────────────────────────────────────────────────

    #[test]
    fn open_returns_unique_ids() {
        let store = SessionStore::new();
        let a = open_timed(&store, 1, 500, 0, 0).expect("open a");
        let b = open_timed(&store, 1, 500, 0, 0).expect("open b");
        assert_ne!(a, b, "IDs must be distinct");
    }

    #[test]
    fn open_stores_entry_correctly() {
        let store = SessionStore::new();
        let sid = open_t0(&store);
        let guard = store.inner.lock().unwrap();
        let entry = guard.get(&sid).expect("entry must exist after open");
        assert_eq!(entry.node, 42);
        assert_eq!(entry.ttl_ms, 1000);
        assert_eq!(entry.last_seen, 0);
    }

    // ── keepalive_timed ───────────────────────────────────────────────────────

    #[test]
    fn keepalive_resets_last_seen() {
        let store = SessionStore::new();
        let sid = open_t0(&store);

        // Renew at t=500 ms (well within TTL = 1000 ms).
        keepalive_timed(&store, sid, 500).expect("keepalive_timed must succeed");

        let guard = store.inner.lock().unwrap();
        assert_eq!(guard[&sid].last_seen, 500, "last_seen must be updated to 500");
    }

    #[test]
    fn keepalive_rejects_already_expired() {
        let store = SessionStore::new();
        let sid = open_t0(&store); // last_seen=0, ttl=1000

        // t=2000: elapsed = 2000 > ttl = 1000 → Expired.
        let res = keepalive_timed(&store, sid, 2000);
        assert!(
            matches!(res, Err(SessionError::Expired(_))),
            "must be Expired, got {res:?}"
        );
    }

    #[test]
    fn keepalive_extends_session_life() {
        // After keepalive at t=800 (last_seen=800), session survives at t=1500
        // (elapsed = 700 < 1000) but expires at t=2000 (elapsed = 1200 > 1000).
        let store = SessionStore::new();
        let sid = open_t0(&store); // last_seen=0, ttl=1000

        keepalive_timed(&store, sid, 800).expect("keepalive at t=800");

        // t=1500: session must survive.
        let expired = expire_dead_timed(&store, 1500, &noop, &noop)
            .expect("expire_dead at 1500");
        assert!(!expired.contains(&sid), "renewed session must survive at t=1500");

        // t=2000: session must expire (elapsed = 2000 - 800 = 1200 > 1000).
        let expired2 = expire_dead_timed(&store, 2000, &noop, &noop)
            .expect("expire_dead at 2000");
        assert!(expired2.contains(&sid), "session must expire at t=2000");
    }

    #[test]
    fn keepalive_not_found_returns_error() {
        let store = SessionStore::new();
        let res = keepalive_timed(&store, 9999, 0);
        assert!(matches!(res, Err(SessionError::NotFound(9999))));
    }

    // ── expire_dead_timed ─────────────────────────────────────────────────────

    #[test]
    fn expire_dead_removes_expired_leaves_live() {
        let store = SessionStore::new();

        // old: t=0, ttl=1000 → expired by t=2000 (elapsed=2000)
        let old   = open_timed(&store, 1, 1000, 0, 0).expect("open old");
        // young: t=1500, ttl=1000 → alive at t=2000 (elapsed=500)
        let young = open_timed(&store, 2, 1000, 0, 1500).expect("open young");

        let expired = expire_dead_timed(&store, 2000, &noop, &noop)
            .expect("expire_dead");

        assert!(expired.contains(&old),    "old session must be in expired list");
        assert!(!expired.contains(&young), "young session must not be in expired list");

        let guard = store.inner.lock().unwrap();
        assert!(!guard.contains_key(&old),  "old session must be removed from store");
        assert!(guard.contains_key(&young), "young session must remain in store");
    }

    #[test]
    fn expire_dead_calls_lock_cascade_for_expired() {
        let store    = SessionStore::new();
        let lock_rec = CascadeRecorder::new();
        let svc_rec  = CascadeRecorder::new();

        let sid = open_timed(&store, 1, 1000, 0, 0).expect("open");

        // Not expired at t=999.
        expire_dead_timed(&store, 999, &lock_rec.closure(), &svc_rec.closure())
            .expect("expire_dead at 999");
        assert!(!lock_rec.was_called_with(sid), "lock cascade must NOT fire for live session");

        // Expired at t=2000.
        expire_dead_timed(&store, 2000, &lock_rec.closure(), &svc_rec.closure())
            .expect("expire_dead at 2000");
        assert!(lock_rec.was_called_with(sid), "lock cascade must fire for expired session");
        assert!(svc_rec.was_called_with(sid),  "svc cascade must fire for expired session");
    }

    #[test]
    fn expire_dead_does_not_call_cascade_for_live_sessions() {
        let store    = SessionStore::new();
        let lock_rec = CascadeRecorder::new();
        let svc_rec  = CascadeRecorder::new();

        let old   = open_timed(&store, 1, 500,  0, 0).expect("open old");
        let young = open_timed(&store, 2, 5000, 0, 0).expect("open young");

        // t=1000: old expired (elapsed=1000 > 500), young alive (elapsed=1000 < 5000).
        expire_dead_timed(&store, 1000, &lock_rec.closure(), &svc_rec.closure())
            .expect("expire_dead at 1000");

        assert!(lock_rec.was_called_with(old),    "old cascade must fire");
        assert!(!lock_rec.was_called_with(young), "young cascade must NOT fire");
    }

    #[test]
    fn expire_dead_returns_empty_on_empty_store() {
        let store = SessionStore::new();
        let expired = expire_dead_timed(&store, 1000, &noop, &noop)
            .expect("expire_dead on empty store");
        assert!(expired.is_empty());
    }

    // ── close_timed ──────────────────────────────────────────────────────────

    #[test]
    fn close_removes_session_and_calls_cascades() {
        let store    = SessionStore::new();
        let lock_rec = CascadeRecorder::new();
        let svc_rec  = CascadeRecorder::new();

        let sid = open_t0(&store);

        close_timed(&store, sid, &lock_rec.closure(), &svc_rec.closure())
            .expect("close_timed must succeed");

        let guard = store.inner.lock().unwrap();
        assert!(!guard.contains_key(&sid), "session must be removed after close");
        drop(guard);

        assert!(lock_rec.was_called_with(sid), "lock cascade must be called on close");
        assert!(svc_rec.was_called_with(sid),  "svc cascade must be called on close");
    }

    #[test]
    fn close_idempotent_second_call_succeeds() {
        let store = SessionStore::new();
        let sid = open_t0(&store);

        close_timed(&store, sid, &noop, &noop).expect("first close");
        close_timed(&store, sid, &noop, &noop).expect("second close must be idempotent");
    }

    #[test]
    fn close_nonexistent_is_idempotent() {
        let store = SessionStore::new();
        close_timed(&store, 12345, &noop, &noop)
            .expect("close of unknown sid must be Ok");
    }

    #[test]
    fn close_does_not_call_cascade_when_already_absent() {
        // Cascade must NOT fire for an already-absent session.
        let store    = SessionStore::new();
        let lock_rec = CascadeRecorder::new();
        let svc_rec  = CascadeRecorder::new();

        close_timed(&store, 99, &lock_rec.closure(), &svc_rec.closure())
            .expect("close of unknown sid");

        assert!(!lock_rec.was_called_with(99), "cascade must NOT fire for absent session");
        assert!(!svc_rec.was_called_with(99),  "cascade must NOT fire for absent session");
    }
}
