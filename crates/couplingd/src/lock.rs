// START_AI_HEADER
// MODULE: couplingd/src/lock.rs
// PURPOSE: Distributed lock primitive for couplingd — Ярус 2, SPEC_coupling_v1 §3 + §8.
//          Implements LOCK ACQ|REL: shared (multiple holders) + exclusive (single holder).
//          fence — monotonically increasing per-key token via MemFencer → no split-brain.
//          M1: in-memory single-node DLM (raft-committed CAS deferred to next phase).
// DEPENDENCIES: std, thiserror, crate::os::MemFencer
// PUBLIC_API: LockMode, LockGrant, LockStore, LockError, acquire, release,
//             release_all_for_session, get_fence, is_held_by
// END_AI_HEADER

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use thiserror::Error;
use crate::os::{Fencer, MemFencer};
use crate::session::SessionId;

// ── Lock mode ─────────────────────────────────────────────────────────────────

/// Lock mode — mirrors capnp enum Mode { shared; exclusive }.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockMode {
    Shared,
    Exclusive,
}

impl std::str::FromStr for LockMode {
    type Err = LockError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "shared"    => Ok(LockMode::Shared),
            "exclusive" => Ok(LockMode::Exclusive),
            other       => Err(LockError::BadMode(other.to_string())),
        }
    }
}

// ── LockGrant ─────────────────────────────────────────────────────────────────

/// Returned to the caller after a successful LOCK ACQ.
/// Mirrors capnp LockGrant.
#[derive(Debug, Clone)]
pub struct LockGrant {
    pub key:   String,
    pub mode:  LockMode,
    /// Monotonically increasing per-key fencing token.
    /// Bumped on every exclusive acquire via MemFencer; shared grants carry
    /// the current snapshot (not incremented).
    /// Storage/VFS rejects writes with a stale fence → split-brain impossible.
    pub fence: u64,
}

// ── Internal state ────────────────────────────────────────────────────────────

/// Internal per-key lock state.
#[derive(Debug)]
struct LockEntry {
    /// Active holders: (SessionId, LockMode).
    /// Invariant: at most one Exclusive holder; zero-or-more Shared holders.
    holders:        Vec<(SessionId, LockMode)>,
    /// Current fence value — set to the latest exclusive-grant fence each time
    /// an exclusive lock is acquired.  Shared grants read this without bumping it.
    fence_snapshot: u64,
}

impl LockEntry {
    fn new() -> Self {
        Self { holders: Vec::new(), fence_snapshot: 0 }
    }

    // has_exclusive:start
    //   purpose: Check whether any existing holder holds an exclusive lock.
    //   input:  &self
    //   output: bool
    //   sideEffects: none
    // has_exclusive:end
    fn has_exclusive(&self) -> bool {
        self.holders.iter().any(|(_, m)| *m == LockMode::Exclusive)
    }
}

// ── LockStore ─────────────────────────────────────────────────────────────────

/// Thread-safe in-memory lock store.
///
/// Holds a `MemFencer` that issues monotonic per-key fencing tokens on every
/// exclusive acquire.  Lock ordering: always acquire `inner` first, then
/// read/write `fencer` — the fencer is accessed only while `inner` is **not**
/// held, because `MemFencer` itself holds its own Mutex internally.
///
/// On the raft phase the `fencer` field will be replaced by a consensus-backed
/// counter; the public API surface stays identical.
#[derive(Clone)]
pub struct LockStore {
    inner:  Arc<Mutex<HashMap<String, LockEntry>>>,
    fencer: MemFencer,
}

impl Default for LockStore {
    fn default() -> Self {
        Self::new()
    }
}

// ── LockError ─────────────────────────────────────────────────────────────────

/// Errors produced by lock operations.
#[derive(Debug, Error)]
pub enum LockError {
    /// Lock cannot be acquired because an incompatible holder already exists.
    /// Non-blocking; caller must retry or enqueue in a waiter queue.
    /// TODO(raft): waiter/notify queue in the consensus phase.
    #[error("lock '{0}' is busy — incompatible holder exists")]
    Busy(String),
    #[error("session {0} does not hold lock '{1}'")]
    NotHeld(SessionId, String),
    #[error("unknown mode '{0}'; expected 'shared' or 'exclusive'")]
    BadMode(String),
    #[error("store lock poisoned")]
    Poisoned,
}

// ── LockStore constructor ─────────────────────────────────────────────────────

impl LockStore {
    // new:start
    //   purpose: Construct an empty LockStore with a fresh MemFencer.
    //   input:  none
    //   output: LockStore
    //   sideEffects: allocates Arc<Mutex<HashMap>> and MemFencer
    // new:end
    pub fn new() -> Self {
        Self {
            inner:  Arc::new(Mutex::new(HashMap::new())),
            fencer: MemFencer::new(),
        }
    }
}

// ── Public operations ─────────────────────────────────────────────────────────

// acquire:start
//   purpose: Acquire a lock on `key` for session `sid` in the requested mode.
//
//            Compatibility matrix (§3):
//              Exclusive vs any holder  → Busy
//              Shared    vs Exclusive   → Busy
//              Shared    vs Shared      → allowed (both succeed)
//
//            Fencing token (§8):
//              Every exclusive grant increments the per-key monotonic counter
//              via MemFencer and stores the new value in LockEntry.fence_snapshot.
//              Shared grants return the current fence_snapshot unchanged.
//              Lock ordering: inner Mutex released before fencer.next_fence() call
//              to avoid nested lock acquisition.
//
//            Non-blocking: conflict → Err(LockError::Busy); no thread sleep.
//            TODO(raft): replace in-memory check+insert with raft-committed CAS.
//            TODO(raft): add waiter queue / condvar notification on grant.
//
//   input:  store — shared LockStore; key — lock path; sid — owning session;
//           mode — Shared | Exclusive
//   output: Result<LockGrant, LockError>
//   sideEffects: inserts or updates LockEntry under Mutex; may call fencer.next_fence
// acquire:end
pub fn acquire(
    store: &LockStore,
    key:   &str,
    sid:   SessionId,
    mode:  LockMode,
) -> Result<LockGrant, LockError> {
    match mode {
        LockMode::Exclusive => acquire_exclusive(store, key, sid),
        LockMode::Shared    => acquire_shared(store, key, sid),
    }
}

/// Acquire an exclusive lock.
///
/// Strategy (lock-order safe):
///   1. Pre-increment the fencing token **before** locking `inner`.
///      The fencer is independent (its own Mutex) so this is safe.
///      If the acquire fails the token is "wasted" — gaps are acceptable;
///      monotonicity is the only invariant required by §8.
///   2. Lock `inner`, check no holders exist.
///   3. On success: record holder + write new fence_snapshot.
///   4. On failure: return Busy (wasted token remains in fencer, harmless).
fn acquire_exclusive(
    store: &LockStore,
    key:   &str,
    sid:   SessionId,
) -> Result<LockGrant, LockError> {
    // Step 1: Allocate next fence token (fencer Mutex, held briefly, then released).
    let new_fence = store.fencer.next_fence(key);

    // Step 2: Lock inner state.
    let mut guard = store.inner.lock().map_err(|_| LockError::Poisoned)?;
    let entry = guard.entry(key.to_string()).or_insert_with(LockEntry::new);

    // Step 3: Compatibility check — exclusive conflicts with any holder.
    if !entry.holders.is_empty() {
        // Token was pre-incremented; gap is acceptable (see fn doc).
        return Err(LockError::Busy(key.to_string()));
    }

    // Step 4: Grant — record holder and fence snapshot.
    entry.holders.push((sid, LockMode::Exclusive));
    entry.fence_snapshot = new_fence;

    Ok(LockGrant { key: key.to_string(), mode: LockMode::Exclusive, fence: new_fence })
}

/// Acquire a shared lock.
///
/// Shared is compatible with other shared holders; incompatible with exclusive.
/// Fence is not bumped — the current snapshot is returned.
fn acquire_shared(
    store: &LockStore,
    key:   &str,
    sid:   SessionId,
) -> Result<LockGrant, LockError> {
    let mut guard = store.inner.lock().map_err(|_| LockError::Poisoned)?;
    let entry = guard.entry(key.to_string()).or_insert_with(LockEntry::new);

    // Shared conflicts with an exclusive holder only.
    if entry.has_exclusive() {
        return Err(LockError::Busy(key.to_string()));
    }

    let fence = entry.fence_snapshot;
    entry.holders.push((sid, LockMode::Shared));

    Ok(LockGrant { key: key.to_string(), mode: LockMode::Shared, fence })
}

// release:start
//   purpose: Release the lock held by session `sid` on `key`.
//            Removes the first matching (sid, _) holder entry.
//            If no holders remain after removal the key entry is deleted.
//            TODO(raft): drive raft-committed release + notify waiter queue.
//   input:  store — shared LockStore; key — lock path; sid — session releasing
//   output: Result<(), LockError>
//   sideEffects: removes holder entry under Mutex; may remove key entry
// release:end
pub fn release(
    store: &LockStore,
    key:   &str,
    sid:   SessionId,
) -> Result<(), LockError> {
    let mut guard = store.inner.lock().map_err(|_| LockError::Poisoned)?;
    let entry = guard.get_mut(key)
        .ok_or_else(|| LockError::NotHeld(sid, key.to_string()))?;

    let pos = entry.holders.iter().position(|(s, _)| *s == sid)
        .ok_or_else(|| LockError::NotHeld(sid, key.to_string()))?;
    entry.holders.remove(pos);

    if entry.holders.is_empty() {
        guard.remove(key);
    }

    Ok(())
}

// release_all_for_session:start
//   purpose: Release every lock held by session `sid` (called on session expiry or close).
//            Sweeps all keys; removes `sid` from each entry's holder list.
//            Keys with no remaining holders are removed from the store.
//            Returns the list of keys from which `sid` was removed.
//            TODO(raft): drive from expire_dead() cascade with raft-committed entries.
//   input:  store — shared LockStore; sid — session being reaped
//   output: Result<Vec<String>, LockError> — keys from which sid was removed
//   sideEffects: removes holder entries and possibly key entries under Mutex
// release_all_for_session:end
pub fn release_all_for_session(
    store: &LockStore,
    sid:   SessionId,
) -> Result<Vec<String>, LockError> {
    let mut guard = store.inner.lock().map_err(|_| LockError::Poisoned)?;
    let mut released = Vec::new();

    for (key, entry) in guard.iter_mut() {
        let before = entry.holders.len();
        entry.holders.retain(|(s, _)| *s != sid);
        if entry.holders.len() < before {
            released.push(key.clone());
        }
    }

    // Remove now-empty entries to keep the map compact.
    guard.retain(|_, e| !e.holders.is_empty());

    Ok(released)
}

// get_fence:start
//   purpose: Read the current fence_snapshot for a key without acquiring a lock.
//            Used by dispatch_lock after propose(LockAcquire) to recover the fence
//            token that was assigned by LocalLog.apply() for the wire response.
//            Returns NotHeld if the key has no active holders (lock entry absent).
//   input:  store — shared LockStore; key — lock path
//   output: Result<u64, LockError> — fence_snapshot for the key
//   sideEffects: acquires Mutex read lock
// get_fence:end
pub fn get_fence(store: &LockStore, key: &str) -> Result<u64, LockError> {
    let guard = store.inner.lock().map_err(|_| LockError::Poisoned)?;
    guard.get(key)
        .map(|e| e.fence_snapshot)
        .ok_or_else(|| LockError::NotHeld(0, key.to_string()))
}

// is_held_by:start
//   purpose: Check whether session `sid` holds any lock (Shared or Exclusive) on `key`.
//            Used by reconcile.rs to detect whether the current node still owns
//            the singleton lock after a session expiry cascade.
//            Returns false if the key has no entry or sid is not in the holders list.
//   input:  store — shared LockStore; key — lock path; sid — session to check
//   output: bool — true iff sid is an active holder for key
//   sideEffects: acquires Mutex read lock briefly
// is_held_by:end
pub fn is_held_by(store: &LockStore, key: &str, sid: SessionId) -> bool {
    let Ok(guard) = store.inner.lock() else { return false; };
    guard.get(key)
        .map(|e| e.holders.iter().any(|(s, _)| *s == sid))
        .unwrap_or(false)
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    const SID_A: SessionId = 1;
    const SID_B: SessionId = 2;
    const SID_C: SessionId = 3;
    const KEY:   &str      = "lock/db";

    // excl_blocks_excl:start
    //   purpose: Verify that a second exclusive acquire on a key held exclusively fails with Busy.
    //   input:  fresh LockStore
    //   output: second acquire returns Err(LockError::Busy)
    //   sideEffects: one holder left in store
    // excl_blocks_excl:end
    #[test]
    fn excl_blocks_excl() {
        let store = LockStore::new();
        acquire(&store, KEY, SID_A, LockMode::Exclusive).expect("first excl must succeed");
        let res = acquire(&store, KEY, SID_B, LockMode::Exclusive);
        assert!(
            matches!(res, Err(LockError::Busy(_))),
            "second exclusive on exclusively-held key must return Busy, got: {:?}",
            res,
        );
    }

    // shared_allows_multiple:start
    //   purpose: Verify that multiple shared acquires on the same key all succeed.
    //   input:  fresh LockStore, three distinct sessions
    //   output: all three grants succeed
    //   sideEffects: three shared holders in store
    // shared_allows_multiple:end
    #[test]
    fn shared_allows_multiple() {
        let store = LockStore::new();
        acquire(&store, KEY, SID_A, LockMode::Shared).expect("shared A");
        acquire(&store, KEY, SID_B, LockMode::Shared).expect("shared B");
        acquire(&store, KEY, SID_C, LockMode::Shared).expect("shared C");
    }

    // excl_blocks_shared:start
    //   purpose: Verify that a shared acquire fails while an exclusive holder exists.
    //   input:  fresh LockStore, SID_A holds exclusive
    //   output: shared acquire by SID_B returns Err(LockError::Busy)
    //   sideEffects: none beyond initial excl grant
    // excl_blocks_shared:end
    #[test]
    fn excl_blocks_shared() {
        let store = LockStore::new();
        acquire(&store, KEY, SID_A, LockMode::Exclusive).expect("excl must succeed");
        let res = acquire(&store, KEY, SID_B, LockMode::Shared);
        assert!(
            matches!(res, Err(LockError::Busy(_))),
            "shared acquire on exclusively-held key must return Busy, got: {:?}",
            res,
        );
    }

    // shared_blocks_excl:start
    //   purpose: Verify that an exclusive acquire fails while any shared holder exists.
    //   input:  fresh LockStore, SID_A holds shared
    //   output: exclusive acquire by SID_B returns Err(LockError::Busy)
    //   sideEffects: none beyond initial shared grant
    // shared_blocks_excl:end
    #[test]
    fn shared_blocks_excl() {
        let store = LockStore::new();
        acquire(&store, KEY, SID_A, LockMode::Shared).expect("shared must succeed");
        let res = acquire(&store, KEY, SID_B, LockMode::Exclusive);
        assert!(
            matches!(res, Err(LockError::Busy(_))),
            "exclusive acquire on shared-held key must return Busy, got: {:?}",
            res,
        );
    }

    // fence_monotone_on_excl:start
    //   purpose: Verify that the fencing token strictly increases on every exclusive grant
    //            and never decreases (monotonicity invariant §8).
    //   input:  fresh LockStore, same key, alternating release + re-acquire
    //   output: each successive excl grant has fence > previous
    //   sideEffects: multiple acquire/release cycles
    // fence_monotone_on_excl:end
    #[test]
    fn fence_monotone_on_excl() {
        let store = LockStore::new();

        let g1 = acquire(&store, KEY, SID_A, LockMode::Exclusive).expect("grant 1");
        release(&store, KEY, SID_A).expect("release 1");

        let g2 = acquire(&store, KEY, SID_B, LockMode::Exclusive).expect("grant 2");
        release(&store, KEY, SID_B).expect("release 2");

        let g3 = acquire(&store, KEY, SID_A, LockMode::Exclusive).expect("grant 3");

        assert!(g2.fence > g1.fence, "fence must increase: g2={} g1={}", g2.fence, g1.fence);
        assert!(g3.fence > g2.fence, "fence must increase: g3={} g2={}", g3.fence, g2.fence);
    }

    // fence_no_rollback:start
    //   purpose: Verify that the fence does not reset to 0 after all holders are released
    //            (fence lives in fencer independently of entry lifetime).
    //   input:  fresh LockStore, acquire→release→acquire
    //   output: second grant fence > first grant fence (not 1 again)
    //   sideEffects: none
    // fence_no_rollback:end
    #[test]
    fn fence_no_rollback() {
        let store = LockStore::new();

        let g1 = acquire(&store, KEY, SID_A, LockMode::Exclusive).expect("grant 1");
        assert_eq!(g1.fence, 1, "first exclusive must produce fence=1");

        release(&store, KEY, SID_A).expect("release");
        // Entry is now deleted from the map. A fresh acquire must NOT reset fence to 1.

        let g2 = acquire(&store, KEY, SID_A, LockMode::Exclusive).expect("grant 2");
        assert!(g2.fence > g1.fence, "fence must not roll back after entry deletion");
    }

    // release_removes_holder:start
    //   purpose: Verify that releasing a lock allows the next caller to acquire it.
    //   input:  fresh LockStore, SID_A acquires excl, releases, SID_B acquires excl
    //   output: SID_B acquire succeeds
    //   sideEffects: none
    // release_removes_holder:end
    #[test]
    fn release_removes_holder() {
        let store = LockStore::new();
        acquire(&store, KEY, SID_A, LockMode::Exclusive).expect("first excl");
        release(&store, KEY, SID_A).expect("release");
        acquire(&store, KEY, SID_B, LockMode::Exclusive).expect("second excl after release");
    }

    // release_not_held_error:start
    //   purpose: Verify that releasing a key the session does not hold returns NotHeld.
    //   input:  fresh LockStore, no prior acquire
    //   output: Err(LockError::NotHeld)
    //   sideEffects: none
    // release_not_held_error:end
    #[test]
    fn release_not_held_error() {
        let store = LockStore::new();
        let res = release(&store, KEY, SID_A);
        assert!(
            matches!(res, Err(LockError::NotHeld(..))),
            "release on unheld key must return NotHeld, got: {:?}",
            res,
        );
    }

    // release_all_for_session_clears_all:start
    //   purpose: Verify that release_all_for_session removes all locks held by a session
    //            and returns the list of freed keys.
    //   input:  fresh LockStore, SID_A holds shared on two keys + excl on a third
    //   output: release_all returns a set containing all three keys; subsequent excl acquire succeeds
    //   sideEffects: store is empty for all three keys after call
    // release_all_for_session_clears_all:end
    #[test]
    fn release_all_for_session_clears_all() {
        let store = LockStore::new();
        let keys = ["lock/a", "lock/b", "lock/c"];

        acquire(&store, keys[0], SID_A, LockMode::Shared).expect("shared a");
        acquire(&store, keys[1], SID_A, LockMode::Shared).expect("shared b");
        acquire(&store, keys[2], SID_A, LockMode::Exclusive).expect("excl c");

        // SID_B also holds shared on lock/a — must not be removed.
        acquire(&store, keys[0], SID_B, LockMode::Shared).expect("shared a sid_b");

        let released = release_all_for_session(&store, SID_A).expect("release_all");

        let mut released_sorted = released.clone();
        released_sorted.sort();
        let mut expected: Vec<String> = keys.iter().map(|k| k.to_string()).collect();
        expected.sort();
        assert_eq!(released_sorted, expected, "all three keys must be in released list");

        // SID_B still holds lock/a — SID_A should not have removed that.
        // SID_A's share is gone so SID_A can re-acquire shared on lock/a.
        acquire(&store, keys[0], SID_A, LockMode::Shared)
            .expect("SID_A should be able to re-acquire shared on lock/a");

        // lock/b and lock/c are fully free — SID_B can exclusively acquire them.
        acquire(&store, keys[1], SID_B, LockMode::Exclusive)
            .expect("lock/b fully free after session release");
        acquire(&store, keys[2], SID_B, LockMode::Exclusive)
            .expect("lock/c fully free after session release");
    }

    // release_all_returns_only_held:start
    //   purpose: Verify that release_all_for_session returns only the keys actually held by the session.
    //   input:  fresh LockStore, SID_A holds one key, SID_B holds another
    //   output: release_all for SID_A returns only SID_A's key
    //   sideEffects: SID_B's key remains locked
    // release_all_returns_only_held:end
    #[test]
    fn release_all_returns_only_held() {
        let store = LockStore::new();
        acquire(&store, "lock/x", SID_A, LockMode::Exclusive).expect("SID_A excl x");
        acquire(&store, "lock/y", SID_B, LockMode::Exclusive).expect("SID_B excl y");

        let released = release_all_for_session(&store, SID_A).expect("release_all SID_A");
        assert_eq!(released, vec!["lock/x".to_string()]);

        // lock/y still held by SID_B.
        let res = acquire(&store, "lock/y", SID_A, LockMode::Exclusive);
        assert!(
            matches!(res, Err(LockError::Busy(_))),
            "lock/y must still be busy (held by SID_B)",
        );
    }
}
