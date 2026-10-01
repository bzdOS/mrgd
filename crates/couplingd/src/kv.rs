// START_AI_HEADER
// MODULE: couplingd/src/kv.rs
// PURPOSE: Linearisable KV store primitive for couplingd — Ярус 2, SPEC_coupling_v1 §3+§8.
//          Implements KV GET|PUT|CAS|WATCH: versioned byte-blob store with optional
//          CAS (expect_ver) and fencing-token enforcement (fence).
//          M1: in-memory single-node.  Raft + Zenoh watch deferred (see TODO markers).
// INTENT: Full M1 implementation with unit tests.  Signatures are final; raft agent
//         replaces fn bodies without changing the public API.
// DEPENDENCIES: std, thiserror
// PUBLIC_API: KvValue, KvError, KvStore, get, put, cas, watch_once
// END_AI_HEADER

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use thiserror::Error;

/// A versioned value stored in the KV store.
#[derive(Debug, Clone)]
pub struct KvValue {
    /// Payload bytes (matches capnp KvPut.val :Data).
    pub data:    Vec<u8>,
    /// Monotonic version — incremented on every successful write.
    pub version: u64,
    /// Max fencing token seen on this key (0 = never fenced).
    pub fence:   u64,
}

/// Internal per-key slot: value + max fence seen.
/// Splitting max_fence lets us retain the stored token even after data changes.
#[derive(Debug, Clone)]
struct Slot {
    value:     KvValue,
    /// Highest fence token ever accepted for this key.
    max_fence: u64,
}

/// Errors produced by KV operations.
#[derive(Debug, Error)]
pub enum KvError {
    #[error("key '{0}' not found")]
    NotFound(String),
    /// Returned by CAS when the current version != expect_ver.
    #[error("CAS conflict: expected version {expected}, actual {actual}")]
    Conflict { expected: u64, actual: u64 },
    /// Returned by put/cas when the supplied fence token is older than the
    /// max fence already recorded for this key (§8 fencing).
    #[error("fencing token rejected: received {received}, store has {stored}")]
    StaleFence { received: u64, stored: u64 },
    #[error("store lock poisoned")]
    Poisoned,
}

/// Thread-safe in-memory KV store (M1; single-node).
/// TODO(raft): replace inner HashMap with a Raft state-machine log.
#[derive(Clone, Default)]
pub struct KvStore {
    inner: Arc<Mutex<HashMap<String, Slot>>>,
}

impl KvStore {
    // new:start
    //   purpose: Construct an empty KvStore.
    //   input:  none
    //   output: KvStore
    //   sideEffects: allocates Arc<Mutex<HashMap>>
    // new:end
    pub fn new() -> Self {
        Self::default()
    }
}

// get:start
//   purpose: Retrieve the current value and version for a key.
//            Returns KvError::NotFound if the key does not exist.
//            M1: reads directly from in-memory HashMap (no raft barrier).
//            TODO(raft): add linearised-read option (ReadIndex barrier).
//   input:  store — shared KvStore; key — store key
//   output: Result<KvValue, KvError>
//   sideEffects: acquires Mutex read lock on store inner
// get:end
pub fn get(store: &KvStore, key: &str) -> Result<KvValue, KvError> {
    store
        .inner
        .lock()
        .map_err(|_| KvError::Poisoned)?
        .get(key)
        .map(|s| s.value.clone())
        .ok_or_else(|| KvError::NotFound(key.to_string()))
}

// put:start
//   purpose: Unconditional write: store val under key, bump version, enforce fence.
//            If fence > 0 and the stored max_fence for this key is greater than fence,
//            the write is rejected with KvError::StaleFence — a stale token-holder
//            (e.g. a dead node's lagging writer) cannot overwrite fresher data (§8).
//            On success the key's max_fence is updated to max(stored, fence).
//            TODO(raft): replace HashMap mutation with raft-propose → commit → apply.
//   input:  store — shared KvStore; key — store key; val — payload bytes;
//           fence — fencing token (0 = unfenced write, always permitted)
//   output: Result<u64, KvError> — the new version number
//   sideEffects: inserts or updates slot under Mutex write lock
// put:end
pub fn put(
    store: &KvStore,
    key:   &str,
    val:   Vec<u8>,
    fence: u64,
) -> Result<u64, KvError> {
    let mut guard = store.inner.lock().map_err(|_| KvError::Poisoned)?;
    let slot = guard.entry(key.to_string()).or_insert_with(|| Slot {
        value: KvValue { data: Vec::new(), version: 0, fence: 0 },
        max_fence: 0,
    });

    // §8 fencing: reject if the supplied token is strictly older than the max seen.
    if fence > 0 && fence < slot.max_fence {
        return Err(KvError::StaleFence { received: fence, stored: slot.max_fence });
    }

    slot.value.version += 1;
    slot.value.data   = val;
    slot.value.fence  = fence;
    slot.max_fence     = slot.max_fence.max(fence);

    Ok(slot.value.version)
}

// cas:start
//   purpose: Compare-and-swap: write val only if current version == expect_ver.
//            expect_ver == 0 means "key must not yet exist" (create-only / first-writer wins).
//            If the version does not match, returns KvError::Conflict — the caller must
//            re-read and retry (optimistic concurrency; universal coordination primitive).
//            If fence > 0, enforces the fencing token exactly as put does.
//            TODO(raft): replace HashMap mutation with raft-CAS propose.
//   input:  store — KvStore; key; val; expect_ver — required current version
//           (0 = key must be absent); fence — fencing token (0 = unfenced)
//   output: Result<u64, KvError> — new version on success
//   sideEffects: updates slot under Mutex write lock
// cas:end
pub fn cas(
    store:      &KvStore,
    key:        &str,
    val:        Vec<u8>,
    expect_ver: u64,
    fence:      u64,
) -> Result<u64, KvError> {
    let mut guard = store.inner.lock().map_err(|_| KvError::Poisoned)?;

    if expect_ver == 0 {
        // "must not exist" semantics — first-writer wins.
        if let Some(existing) = guard.get(key) {
            return Err(KvError::Conflict {
                expected: 0,
                actual:   existing.value.version,
            });
        }
        guard.insert(key.to_string(), Slot {
            value:     KvValue { data: val, version: 1, fence },
            max_fence: fence,
        });
        return Ok(1);
    }

    let slot = guard
        .get_mut(key)
        .ok_or_else(|| KvError::NotFound(key.to_string()))?;

    if slot.value.version != expect_ver {
        return Err(KvError::Conflict {
            expected: expect_ver,
            actual:   slot.value.version,
        });
    }

    // §8 fencing check (same policy as put).
    if fence > 0 && fence < slot.max_fence {
        return Err(KvError::StaleFence { received: fence, stored: slot.max_fence });
    }

    slot.value.version += 1;
    slot.value.data    = val;
    slot.value.fence   = fence;
    slot.max_fence      = slot.max_fence.max(fence);

    Ok(slot.value.version)
}

// watch_once:start
//   purpose: Poll whether the version of key has advanced beyond known_ver.
//            Returns Ok(Some(value)) if version > known_ver (change detected).
//            Returns Ok(None) if version <= known_ver, or the key does not yet exist
//            and known_ver == 0 (no change; caller should sleep and retry).
//            Note: a missing key with known_ver > 0 is treated as NotFound because
//            the caller has observed a version that should exist.
//            TODO(zenoh): replace busy-poll with Zenoh subscriber on
//            bsdos/cf/<group>/<key>; this fn becomes a fallback snapshot check.
//   input:  store — KvStore; key; known_ver — last version seen by caller (0 = never seen)
//   output: Result<Option<KvValue>, KvError>
//   sideEffects: acquires Mutex read lock on store inner
// watch_once:end
pub fn watch_once(
    store:     &KvStore,
    key:       &str,
    known_ver: u64,
) -> Result<Option<KvValue>, KvError> {
    let guard = store.inner.lock().map_err(|_| KvError::Poisoned)?;
    match guard.get(key) {
        // Key absent and caller never saw it → no change yet.
        None if known_ver == 0 => Ok(None),
        // Key absent but caller had a known version → something went wrong.
        None => Err(KvError::NotFound(key.to_string())),
        Some(slot) if slot.value.version > known_ver => Ok(Some(slot.value.clone())),
        Some(_) => Ok(None),
    }
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // put:version:start
    //   purpose: Verify that each successful put increments the version.
    //   input:  fresh KvStore, two puts to same key
    //   output: v1 == 1, v2 == 2
    //   sideEffects: none
    // put:version:end
    #[test]
    fn put_increments_version() {
        let store = KvStore::new();
        let v1 = put(&store, "k", b"a".to_vec(), 0).expect("first put");
        let v2 = put(&store, "k", b"b".to_vec(), 0).expect("second put");
        assert_eq!(v1, 1, "first write must be version 1");
        assert_eq!(v2, 2, "second write must be version 2");
    }

    // get:after_put:start
    //   purpose: Verify get returns the most recent data and version.
    //   input:  put then get
    //   output: KvValue with correct data and version
    //   sideEffects: none
    // get:after_put:end
    #[test]
    fn get_after_put_returns_value() {
        let store = KvStore::new();
        put(&store, "greeting", b"hello".to_vec(), 0).expect("put");
        let v = get(&store, "greeting").expect("get");
        assert_eq!(v.data, b"hello");
        assert_eq!(v.version, 1);
    }

    // get:missing:start
    //   purpose: Verify get returns NotFound for an absent key.
    //   input:  empty store, get unknown key
    //   output: Err(KvError::NotFound)
    //   sideEffects: none
    // get:missing:end
    #[test]
    fn get_missing_key_returns_not_found() {
        let store = KvStore::new();
        match get(&store, "ghost") {
            Err(KvError::NotFound(k)) => assert_eq!(k, "ghost"),
            other => panic!("expected NotFound, got {:?}", other),
        }
    }

    // cas:success:start
    //   purpose: Verify CAS succeeds when expect_ver matches current version.
    //   input:  put to create v1, then cas with expect_ver=1
    //   output: new version == 2, data updated
    //   sideEffects: none
    // cas:success:end
    #[test]
    fn cas_succeeds_on_matching_version() {
        let store = KvStore::new();
        put(&store, "x", b"old".to_vec(), 0).expect("put");
        let v2 = cas(&store, "x", b"new".to_vec(), 1, 0).expect("cas");
        assert_eq!(v2, 2);
        let got = get(&store, "x").expect("get");
        assert_eq!(got.data, b"new");
        assert_eq!(got.version, 2);
    }

    // cas:conflict:start
    //   purpose: Verify CAS returns Conflict when expect_ver does not match.
    //   input:  put twice (version=2), then cas with expect_ver=1
    //   output: Err(KvError::Conflict { expected:1, actual:2 })
    //   sideEffects: store unchanged
    // cas:conflict:end
    #[test]
    fn cas_returns_conflict_on_version_mismatch() {
        let store = KvStore::new();
        put(&store, "y", b"v1".to_vec(), 0).expect("put1");
        put(&store, "y", b"v2".to_vec(), 0).expect("put2");
        // Version is now 2; CAS against 1 must fail.
        match cas(&store, "y", b"v3".to_vec(), 1, 0) {
            Err(KvError::Conflict { expected: 1, actual: 2 }) => {}
            other => panic!("expected Conflict(1,2), got {:?}", other),
        }
        // Value must be unchanged.
        let got = get(&store, "y").expect("get");
        assert_eq!(got.data, b"v2");
    }

    // cas:create_only:start
    //   purpose: Verify CAS with expect_ver=0 creates key when absent.
    //   input:  fresh store, cas with expect_ver=0
    //   output: version == 1
    //   sideEffects: key created
    // cas:create_only:end
    #[test]
    fn cas_create_only_succeeds_when_absent() {
        let store = KvStore::new();
        let v = cas(&store, "new_key", b"init".to_vec(), 0, 0).expect("create-only cas");
        assert_eq!(v, 1);
    }

    // cas:create_only_conflict:start
    //   purpose: Verify CAS with expect_ver=0 returns Conflict when key already exists.
    //   input:  existing key, cas with expect_ver=0
    //   output: Err(KvError::Conflict)
    //   sideEffects: none
    // cas:create_only_conflict:end
    #[test]
    fn cas_create_only_conflicts_when_key_exists() {
        let store = KvStore::new();
        put(&store, "taken", b"already".to_vec(), 0).expect("put");
        match cas(&store, "taken", b"conflict".to_vec(), 0, 0) {
            Err(KvError::Conflict { expected: 0, actual: 1 }) => {}
            other => panic!("expected Conflict(0,1), got {:?}", other),
        }
    }

    // stale_fence:put:start
    //   purpose: Verify put rejects a write whose fence token is older than stored max_fence.
    //            This prevents a stale token-holder (dead node) from overwriting fresher data.
    //   input:  put with fence=5 (establishes max), then put with fence=3
    //   output: second put returns Err(KvError::StaleFence { received:3, stored:5 })
    //   sideEffects: value unchanged after rejected write
    // stale_fence:put:end
    #[test]
    fn put_rejects_stale_fence() {
        let store = KvStore::new();
        put(&store, "fenced", b"first".to_vec(), 5).expect("put with fence=5");
        match put(&store, "fenced", b"stale".to_vec(), 3) {
            Err(KvError::StaleFence { received: 3, stored: 5 }) => {}
            other => panic!("expected StaleFence(3,5), got {:?}", other),
        }
        // Original value must survive.
        let got = get(&store, "fenced").expect("get");
        assert_eq!(got.data, b"first");
    }

    // stale_fence:cas:start
    //   purpose: Verify CAS also enforces fencing — a stale token-holder cannot swap in.
    //   input:  put with fence=10 (v1), then cas with fence=7
    //   output: Err(KvError::StaleFence { received:7, stored:10 })
    //   sideEffects: value unchanged
    // stale_fence:cas:end
    #[test]
    fn cas_rejects_stale_fence() {
        let store = KvStore::new();
        put(&store, "fc", b"orig".to_vec(), 10).expect("put fence=10");
        match cas(&store, "fc", b"new".to_vec(), 1, 7) {
            Err(KvError::StaleFence { received: 7, stored: 10 }) => {}
            other => panic!("expected StaleFence(7,10), got {:?}", other),
        }
        let got = get(&store, "fc").expect("get");
        assert_eq!(got.data, b"orig");
    }

    // watch_once:new_version:start
    //   purpose: watch_once returns Some when the stored version exceeds known_ver.
    //   input:  put (version=1), watch_once with known_ver=0
    //   output: Ok(Some(KvValue { version:1, .. }))
    //   sideEffects: none
    // watch_once:new_version:end
    #[test]
    fn watch_once_returns_some_on_new_version() {
        let store = KvStore::new();
        put(&store, "w", b"data".to_vec(), 0).expect("put");
        let result = watch_once(&store, "w", 0).expect("watch_once");
        match result {
            Some(v) => {
                assert_eq!(v.version, 1);
                assert_eq!(v.data, b"data");
            }
            None => panic!("expected Some, got None"),
        }
    }

    // watch_once:same_version:start
    //   purpose: watch_once returns None when known_ver equals the current version.
    //   input:  put (version=1), watch_once with known_ver=1
    //   output: Ok(None)
    //   sideEffects: none
    // watch_once:same_version:end
    #[test]
    fn watch_once_returns_none_on_same_version() {
        let store = KvStore::new();
        put(&store, "w2", b"data".to_vec(), 0).expect("put");
        let result = watch_once(&store, "w2", 1).expect("watch_once");
        assert!(result.is_none(), "expected None when version unchanged");
    }

    // watch_once:absent_key:start
    //   purpose: watch_once returns Ok(None) for an absent key when known_ver==0
    //            (caller has never seen the key — no change yet).
    //   input:  empty store, known_ver=0
    //   output: Ok(None)
    //   sideEffects: none
    // watch_once:absent_key:end
    #[test]
    fn watch_once_absent_key_with_zero_ver_returns_none() {
        let store = KvStore::new();
        let result = watch_once(&store, "absent", 0).expect("watch_once");
        assert!(result.is_none(), "expected None for absent key with known_ver=0");
    }

    // fence:max_grows:start
    //   purpose: Verify max_fence tracks the highest fence ever accepted on a key,
    //            so a later lower-fence write is always rejected.
    //   input:  put fence=1, put fence=3, put fence=2 (stale)
    //   output: last put returns StaleFence, v stays at 2, data from fence=3 survives
    //   sideEffects: none
    // fence:max_grows:end
    #[test]
    fn max_fence_grows_monotonically() {
        let store = KvStore::new();
        put(&store, "m", b"f1".to_vec(), 1).expect("fence=1");
        put(&store, "m", b"f3".to_vec(), 3).expect("fence=3");
        match put(&store, "m", b"f2".to_vec(), 2) {
            Err(KvError::StaleFence { received: 2, stored: 3 }) => {}
            other => panic!("expected StaleFence(2,3), got {:?}", other),
        }
        let got = get(&store, "m").expect("get");
        assert_eq!(got.data, b"f3", "data from fence=3 must survive");
    }
}
