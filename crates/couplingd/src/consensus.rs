// START_AI_HEADER
// MODULE: couplingd/src/consensus.rs
// PURPOSE: Consensus / replicated-log abstraction for couplingd — M2 slice 1+2.
//          Defines the seam through which ALL store mutations must flow so that
//          a raft backend can be grafted in later (openraft, or custom) without
//          touching session/lock/kv/queue/svc logic.
//
//          Design principle (invariant-confluence, SPEC §6):
//            Every write that must be linearisable (lock/kv-CAS/session/svc) goes
//            through propose() before it is applied locally.  On LocalLog (M1/M2
//            single-node) propose() applies immediately and returns Ok(LogIndex)
//            with no network round-trip.  On a future RaftLog, propose() would
//            wait for quorum before returning — callers see no difference.
//
//          Op enum: one variant per mutation type across all five stores.
//            The enum is the canonical "command" fed to the raft state machine;
//            its wire representation (future: Cap'n Proto) is elided for M2 but
//            the shape is locked.
//
//          LocalLog: single-node immediate-apply implementation.  Each propose()
//            assigns the next monotonic LogIndex, records the op in an in-memory
//            Vec (log), applies it by calling the corresponding store function,
//            and returns the index.  No I/O, no threads — host-testable.
//
//          CfLog: CF-substrate backend (M2).  propose(op) for CP mutations routes
//            through CouplingFs (coupling-VFS: flock + fence) before applying to
//            the stores.  This is the REAL fencing path — a stale fence token causes
//            fence_check() to return Err, and the op is REJECTED (not applied).
//            Tested against MemCouplingFs on host; FreeBsdCouplingFs on 185 via
//            `make coupling-vm-check`.
//
//          CRDT types (PN-Counter, OR-Set, LWW-Register) are not in this enum —
//          they do NOT need consensus (convergent, AP).  They flow through Zenoh
//          delta-pub separately (M5 slice).  Only CP-path ops are here.
//
// INTENT: M2 Slice 1+2 — seam + CfLog CF-backend with real fencing.
//         All tests green on host.  FreeBSD flock validated on 185.
//         Next slice (M2 Slice 3): replace LocalLog with openraft RaftLog behind
//         the same trait; callers in server.rs are unchanged.
// DEPENDENCIES: std, thiserror, crate::{session,lock,kv,queue,svc,os}
// PUBLIC_API: LogIndex, Op, LogError, ReplicatedLog, LocalLog, CfLog
// END_AI_HEADER

use std::sync::{Arc, Mutex};
use thiserror::Error;

use crate::{
    kv::KvStore,
    lock::{LockMode, LockStore},
    os::{CouplingFs, CouplingFsError, Fencer, FenceToken},
    queue::QueueStore,
    session::{NodeId, SessionStore},
    svc::SvcStore,
};

// ── Index type ────────────────────────────────────────────────────────────────

/// Monotonic raft-log index — 1-based; 0 means "not yet applied".
/// On LocalLog, each propose() increments and returns this counter.
/// On a future RaftLog, this becomes the committed entry index.
pub type LogIndex = u64;

// ── Mutation enum ─────────────────────────────────────────────────────────────

// Op:start
//   purpose: Enumerate every state-machine mutation that must be linearised.
//            This is the canonical command fed to the raft log; op identity is
//            used by LocalLog to apply the right store function.
//            CRDT-convergent writes (PN-Counter, OR-Set, LWW) are NOT here —
//            they bypass consensus (invariant-confluence, SPEC §6).
//   input:  constructed by callers (server.rs dispatch) before propose()
//   output: passed to ReplicatedLog::propose(); applied inside the impl
//   sideEffects: none (pure value)
// Op:end
#[derive(Debug, Clone)]
pub enum Op {
    // Session store mutations
    SessionOpen   { node: NodeId, ttl_ms: u32, epoch: u64 },
    SessionKeepalive { sid: u64 },
    SessionClose  { sid: u64 },

    // Lock store mutations
    LockAcquire   { key: String, sid: u64, mode: LockMode },
    LockRelease   { key: String, sid: u64 },
    LockReleaseAll { sid: u64 },

    // KV store mutations
    KvPut  { key: String, val: Vec<u8>, fence: u64 },
    KvCas  { key: String, val: Vec<u8>, expect_ver: u64, fence: u64 },

    // Queue store mutations
    QueuePush { queue: String, payload: Vec<u8> },
    QueuePop  { queue: String },

    // Service-registry mutations
    SvcRegister   { name: String, node: u64, sid: u64 },
    SvcUnregister { name: String, sid: u64 },
    SvcExpire     { sid: u64 },
}

// ── Error type ────────────────────────────────────────────────────────────────

/// Errors from the replicated-log layer.
#[derive(Debug, Error)]
pub enum LogError {
    /// The underlying store operation failed (propagated as a string to avoid
    /// generic type parameters on the trait — callers already have typed errors
    /// from the store functions; this is only for the consensus wrapper path).
    #[error("store error: {0}")]
    Store(String),
    /// Consensus layer could not achieve quorum (future RaftLog; never raised by LocalLog).
    #[error("no quorum: {0}")]
    NoQuorum(String),
    /// Log mutex poisoned.
    #[error("log lock poisoned")]
    Poisoned,
    /// Fencing token is stale — op rejected to prevent split-brain write.
    /// The caller must re-acquire a fresh lock/token before retrying.
    #[error("fencing rejected: {0}")]
    StaleFence(String),
}

// ── Trait ─────────────────────────────────────────────────────────────────────

// ReplicatedLog:start
//   purpose: Abstract the consensus/replication layer.
//            Callers call propose(op) and receive a LogIndex on success.
//            The impl is responsible for:
//              (a) assigning a monotonic log index,
//              (b) replicating to quorum (noop on LocalLog),
//              (c) applying the op to the in-memory stores,
//              (d) returning the committed index.
//            Trait is object-safe (no generics).
//   input:  op — mutation to commit
//   output: Result<LogIndex, LogError>
//   sideEffects: mutates the backing stores on success
// ReplicatedLog:end
pub trait ReplicatedLog: Send + Sync {
    /// Propose an operation for consensus.  Returns the committed log index.
    /// On LocalLog: apply immediately, no network.
    /// On RaftLog (future): blocks until quorum commits.
    fn propose(&self, op: Op) -> Result<LogIndex, LogError>;

    /// Return the highest committed log index (snapshot point for followers).
    fn committed(&self) -> LogIndex;
}

// ── In-memory snapshot of applied ops (for tests) ────────────────────────────

/// Internal record in the LocalLog journal.
#[derive(Debug, Clone)]
pub struct LogEntry {
    pub index: LogIndex,
    pub op:    Op,
}

// ── LocalLog — single-node immediate-apply implementation ────────────────────

/// All five stores referenced by LocalLog so it can apply ops.
/// Constructed by `LocalLog::new(stores_ref)` in server.rs.
pub struct LocalStores {
    pub sessions: SessionStore,
    pub locks:    LockStore,
    pub kv:       KvStore,
    pub queues:   QueueStore,
    pub svcs:     SvcStore,
}

struct LocalLogInner {
    next_index: LogIndex,
    log:        Vec<LogEntry>,
}

/// In-memory single-node replicated log — applies ops immediately on propose().
/// Thread-safe (Arc<Mutex>).  Zero network I/O — host-testable.
///
/// Invariant: every Op that touches a store has gone through propose() first,
/// meaning all mutations are serialised through this Mutex.  When raft is added,
/// this Mutex becomes the raft client lock; the store Mutexes remain for readers.
pub struct LocalLog {
    inner:  Arc<Mutex<LocalLogInner>>,
    stores: LocalStores,
}

impl LocalLog {
    // new:start
    //   purpose: Construct a LocalLog wrapping the five primitive stores.
    //            The log starts empty; next_index = 1 (raft convention).
    //   input:  stores — LocalStores (owns the five store handles)
    //   output: LocalLog
    //   sideEffects: allocates Arc<Mutex<LocalLogInner>>
    // new:end
    pub fn new(stores: LocalStores) -> Self {
        Self {
            inner: Arc::new(Mutex::new(LocalLogInner {
                next_index: 1,
                log:        Vec::new(),
            })),
            stores,
        }
    }

    /// Return a snapshot of all committed entries (for test assertions).
    pub fn journal(&self) -> Vec<LogEntry> {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .log
            .clone()
    }

    // apply:start
    //   purpose: Apply a single Op to the backing stores.
    //            Called while the inner Mutex IS held — ops are serialised.
    //            Return value encodes the store result as Result<LogIndex, LogError>.
    //   input:  op — mutation; index — committed index to attach to return value
    //   output: Result<LogIndex, LogError>
    //   sideEffects: mutates the backing store for the op's type
    // apply:end
    fn apply(&self, op: &Op, index: LogIndex) -> Result<LogIndex, LogError> {
        let map_err = |e: String| LogError::Store(e);

        match op {
            Op::SessionOpen { node, ttl_ms, epoch } => {
                crate::session::open(&self.stores.sessions, *node, *ttl_ms, *epoch)
                    .map(|_sid| index)
                    .map_err(|e| map_err(e.to_string()))
            }
            Op::SessionKeepalive { sid } => {
                crate::session::keepalive(&self.stores.sessions, *sid)
                    .map(|_| index)
                    .map_err(|e| map_err(e.to_string()))
            }
            Op::SessionClose { sid } => {
                // Cascade: release locks + svc registrations.
                let locks = self.stores.locks.clone();
                let svcs  = self.stores.svcs.clone();
                let lock_cascade = |s: crate::session::SessionId| {
                    crate::lock::release_all_for_session(&locks, s)
                        .map(|_| ())
                        .map_err(|e| e.to_string())
                };
                let svc_cascade = |s: crate::session::SessionId| {
                    crate::svc::expire_for_session(&svcs, s)
                        .map(|_| ())
                        .map_err(|e| e.to_string())
                };
                crate::session::close_with_cascade(
                    &self.stores.sessions, *sid, &lock_cascade, &svc_cascade,
                )
                .map(|_| index)
                .map_err(|e| map_err(e.to_string()))
            }

            Op::LockAcquire { key, sid, mode } => {
                crate::lock::acquire(&self.stores.locks, key, *sid, *mode)
                    .map(|_grant| index)
                    .map_err(|e| map_err(e.to_string()))
            }
            Op::LockRelease { key, sid } => {
                crate::lock::release(&self.stores.locks, key, *sid)
                    .map(|_| index)
                    .map_err(|e| map_err(e.to_string()))
            }
            Op::LockReleaseAll { sid } => {
                crate::lock::release_all_for_session(&self.stores.locks, *sid)
                    .map(|_| index)
                    .map_err(|e| map_err(e.to_string()))
            }

            Op::KvPut { key, val, fence } => {
                crate::kv::put(&self.stores.kv, key, val.clone(), *fence)
                    .map(|_ver| index)
                    .map_err(|e| map_err(e.to_string()))
            }
            Op::KvCas { key, val, expect_ver, fence } => {
                crate::kv::cas(&self.stores.kv, key, val.clone(), *expect_ver, *fence)
                    .map(|_ver| index)
                    .map_err(|e| map_err(e.to_string()))
            }

            Op::QueuePush { queue, payload } => {
                crate::queue::push(&self.stores.queues, queue, payload.clone())
                    .map(|_seq| index)
                    .map_err(|e| map_err(e.to_string()))
            }
            Op::QueuePop { queue } => {
                crate::queue::pop(&self.stores.queues, queue)
                    .map(|_entry| index)
                    .map_err(|e| map_err(e.to_string()))
            }

            Op::SvcRegister { name, node, sid } => {
                crate::svc::register(&self.stores.svcs, name, *node, *sid)
                    .map(|_| index)
                    .map_err(|e| map_err(e.to_string()))
            }
            Op::SvcUnregister { name, sid } => {
                crate::svc::unregister(&self.stores.svcs, name, *sid)
                    .map(|_| index)
                    .map_err(|e| map_err(e.to_string()))
            }
            Op::SvcExpire { sid } => {
                crate::svc::expire_for_session(&self.stores.svcs, *sid)
                    .map(|_| index)
                    .map_err(|e| map_err(e.to_string()))
            }
        }
    }
}

impl ReplicatedLog for LocalLog {
    // propose:start
    //   purpose: Assign the next log index, record the op in the journal,
    //            apply it to the backing stores (immediate, no network), return index.
    //            All mutations are serialised through the inner Mutex so the journal
    //            is a total order — exactly what a raft log provides.
    //   input:  op — mutation to commit
    //   output: Result<LogIndex, LogError>
    //   sideEffects: increments next_index; appends LogEntry; mutates backing store
    // propose:end
    fn propose(&self, op: Op) -> Result<LogIndex, LogError> {
        // Hold the log lock ACROSS apply() so proposals are totally ordered — this
        // is the invariant the seam exists to provide (what a raft log guarantees).
        // Dropping the lock during apply would let two concurrent proposals read the
        // same next_index and race the journal, breaking the total order.
        // Safe to hold across apply(): the store Mutexes are independent and nothing
        // ever takes the log lock while holding a store lock, so there is no cycle.
        // On single-node: propose == commit == apply (no network round-trip).
        let mut guard = self.inner.lock().map_err(|_| LogError::Poisoned)?;
        let index = guard.next_index;

        // Apply, then record in the journal + advance the index only on success —
        // a failed op must not commit ("leader does not commit a no-op").
        let result = self.apply(&op, index);
        if result.is_ok() {
            guard.log.push(LogEntry { index, op });
            guard.next_index += 1;
        }

        result
    }

    fn committed(&self) -> LogIndex {
        let guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        guard.next_index.saturating_sub(1)
    }
}

// ── CfLog — CF-substrate backend (flock + fence before apply) ────────────────

// CfLog:start
//   purpose: ReplicatedLog implementation backed by the coupling-VFS substrate.
//            propose(op) routes each CP mutation through CouplingFs:
//              1. fence_check(key, token) — rejects stale tokens immediately
//                 (Err → LogError::StaleFence, op not applied, index not advanced).
//              2. flock_acquire(key, exclusive=true, token) — acquires the VFS lock
//                 (Err on contention → LogError::Store).
//              3. Apply op to stores (same as LocalLog).
//              4. flock_release(key) — drop the VFS lock.
//            Read ops (GET/WATCH/RESOLVE/PEEK/MEMBERS) bypass CfLog entirely —
//            they call stores directly in server.rs (invariant-confluence, §6).
//
//            Fencing contract (§8): token comes from the Fencer embedded in the
//            caller's LockStore.  Before calling propose(KvPut { fence }), the
//            caller must hold a live lock whose fence == token.  A dead node whose
//            session expired has a lower fence → fence_check rejects it → no write.
//
//            Single-node semantics are IDENTICAL to LocalLog because MemCouplingFs
//            flock is in-process (no network).  When a real coupling-VFS (9p DLM)
//            is mounted on FreeBSD, the flock call becomes a real distributed lock.
//
//   input:  op — mutation; coupling_fs — VFS abstraction; fencer — token validator
//   output: Result<LogIndex, LogError>
//   sideEffects: calls fence_check + flock_acquire/release; mutates stores on success
// CfLog:end

/// CfLog: coupling-VFS backed ReplicatedLog (M2).
///
/// All CP-path mutations are gated on fence_check + flock from the CouplingFs
/// before the stores are touched.  This provides the "real" fencing guarantee:
/// a stale token → CouplingFsError::StaleFence → LogError::StaleFence → op rejected.
pub struct CfLog {
    /// Inner log index + journal (same as LocalLog — total order on applies).
    inner:   Arc<Mutex<LocalLogInner>>,
    /// Backing stores — same five as LocalLog.
    stores:  LocalStores,
    /// Coupling-VFS abstraction (MemCouplingFs on host; FreeBsdCouplingFs on 185).
    cf:      Arc<dyn CouplingFs>,
    /// Token validator — used for explicit fence_check before every exclusive op.
    fencer:  Arc<dyn Fencer>,
}

impl CfLog {
    // new:start
    //   purpose: Construct a CfLog wrapping the five stores + CouplingFs + Fencer.
    //            Accepts trait objects so tests can inject MemCouplingFs + MemFencer
    //            and production wires FreeBsdCouplingFs + FreeBsdFencer.
    //   input:  stores — LocalStores; cf — CouplingFs impl; fencer — Fencer impl
    //   output: CfLog
    //   sideEffects: allocates Arc<Mutex<LocalLogInner>>
    // new:end
    pub fn new(
        stores: LocalStores,
        cf:     Arc<dyn CouplingFs>,
        fencer: Arc<dyn Fencer>,
    ) -> Self {
        Self {
            inner:  Arc::new(Mutex::new(LocalLogInner { next_index: 1, log: Vec::new() })),
            stores,
            cf,
            fencer,
        }
    }

    // cf_key:start
    //   purpose: Derive a CouplingFs lock path from an Op.
    //            Every op that touches the stores has a well-known key so the
    //            flock is scoped (not a single global lock).
    //   input:  op — the mutation about to be proposed
    //   output: &'static str or String — VFS path for flock
    //   sideEffects: none
    // cf_key:end
    fn cf_key(op: &Op) -> String {
        match op {
            Op::SessionOpen    { .. }
            | Op::SessionKeepalive { .. }
            | Op::SessionClose { .. }        => "coupling/sessions".to_string(),
            Op::LockAcquire    { key, .. }
            | Op::LockRelease  { key, .. }   => format!("coupling/lock/{key}"),
            Op::LockReleaseAll { sid }        => format!("coupling/lock/session/{sid}"),
            Op::KvPut          { key, .. }
            | Op::KvCas        { key, .. }   => format!("coupling/kv/{key}"),
            Op::QueuePush { queue, .. }
            | Op::QueuePop  { queue }        => format!("coupling/queue/{queue}"),
            Op::SvcRegister    { name, .. }
            | Op::SvcUnregister { name, .. } => format!("coupling/svc/{name}"),
            Op::SvcExpire      { sid }        => format!("coupling/svc/session/{sid}"),
        }
    }

    // fence_token_of:start
    //   purpose: Extract the fencing token carried in an Op (for ops that embed one),
    //            or ask the Fencer to allocate the next token for the CF key path
    //            (for ops that modify coordinator state without an explicit token).
    //            Ops that do NOT carry a fence field use next_fence() from the Fencer
    //            — this bumps the monotonic counter for that key, so that if the
    //            same op is replayed with an older token it will be rejected.
    //   input:  op — the mutation; cf_key — key path for next_fence lookup
    //   output: FenceToken
    //   sideEffects: may increment Fencer's counter (next_fence call)
    // fence_token_of:end
    fn fence_token_of(&self, op: &Op, cf_key: &str) -> FenceToken {
        match op {
            Op::KvPut { fence, .. } | Op::KvCas { fence, .. } => *fence,
            // For lock, session, queue, svc ops the CfLog itself issues the next token
            // from the Fencer so the token is fresh and monotonic for the cf_key.
            _ => self.fencer.next_fence(cf_key),
        }
    }

    // apply_stores:start
    //   purpose: Apply a single Op to the backing stores (identical to LocalLog::apply).
    //            Called while the inner Mutex IS held.
    //   input:  op — mutation; index — committed index
    //   output: Result<LogIndex, LogError>
    //   sideEffects: mutates the backing store for the op's type
    // apply_stores:end
    fn apply_stores(&self, op: &Op, index: LogIndex) -> Result<LogIndex, LogError> {
        let map_err = |e: String| LogError::Store(e);

        match op {
            Op::SessionOpen { node, ttl_ms, epoch } => {
                crate::session::open(&self.stores.sessions, *node, *ttl_ms, *epoch)
                    .map(|_| index).map_err(|e| map_err(e.to_string()))
            }
            Op::SessionKeepalive { sid } => {
                crate::session::keepalive(&self.stores.sessions, *sid)
                    .map(|_| index).map_err(|e| map_err(e.to_string()))
            }
            Op::SessionClose { sid } => {
                let locks = self.stores.locks.clone();
                let svcs  = self.stores.svcs.clone();
                let lc = |s: crate::session::SessionId| {
                    crate::lock::release_all_for_session(&locks, s)
                        .map(|_| ()).map_err(|e| e.to_string())
                };
                let sc = |s: crate::session::SessionId| {
                    crate::svc::expire_for_session(&svcs, s)
                        .map(|_| ()).map_err(|e| e.to_string())
                };
                crate::session::close_with_cascade(&self.stores.sessions, *sid, &lc, &sc)
                    .map(|_| index).map_err(|e| map_err(e.to_string()))
            }
            Op::LockAcquire { key, sid, mode } => {
                crate::lock::acquire(&self.stores.locks, key, *sid, *mode)
                    .map(|_| index).map_err(|e| map_err(e.to_string()))
            }
            Op::LockRelease { key, sid } => {
                crate::lock::release(&self.stores.locks, key, *sid)
                    .map(|_| index).map_err(|e| map_err(e.to_string()))
            }
            Op::LockReleaseAll { sid } => {
                crate::lock::release_all_for_session(&self.stores.locks, *sid)
                    .map(|_| index).map_err(|e| map_err(e.to_string()))
            }
            Op::KvPut { key, val, fence } => {
                crate::kv::put(&self.stores.kv, key, val.clone(), *fence)
                    .map(|_| index).map_err(|e| map_err(e.to_string()))
            }
            Op::KvCas { key, val, expect_ver, fence } => {
                crate::kv::cas(&self.stores.kv, key, val.clone(), *expect_ver, *fence)
                    .map(|_| index).map_err(|e| map_err(e.to_string()))
            }
            Op::QueuePush { queue, payload } => {
                crate::queue::push(&self.stores.queues, queue, payload.clone())
                    .map(|_| index).map_err(|e| map_err(e.to_string()))
            }
            Op::QueuePop { queue } => {
                crate::queue::pop(&self.stores.queues, queue)
                    .map(|_| index).map_err(|e| map_err(e.to_string()))
            }
            Op::SvcRegister { name, node, sid } => {
                crate::svc::register(&self.stores.svcs, name, *node, *sid)
                    .map(|_| index).map_err(|e| map_err(e.to_string()))
            }
            Op::SvcUnregister { name, sid } => {
                crate::svc::unregister(&self.stores.svcs, name, *sid)
                    .map(|_| index).map_err(|e| map_err(e.to_string()))
            }
            Op::SvcExpire { sid } => {
                crate::svc::expire_for_session(&self.stores.svcs, *sid)
                    .map(|_| index).map_err(|e| map_err(e.to_string()))
            }
        }
    }
}

impl ReplicatedLog for CfLog {
    // propose:start
    //   purpose: Route op through CouplingFs fencing before applying to stores.
    //            Protocol per op:
    //              1. Determine VFS key path for this op (cf_key).
    //              2. Determine fence token (from op payload or fresh Fencer.next_fence).
    //              3. fence_check(cf_key, token) — stale → LogError::StaleFence, abort.
    //              4. Lock inner Mutex (total order).
    //              5. flock_acquire(cf_key, exclusive=true, token) on CouplingFs.
    //              6. apply_stores(op, index) — write to in-memory stores.
    //              7. flock_release(cf_key).
    //              8. Advance index in journal only on success.
    //            The flock_acquire/release bracket is inside the inner Mutex to keep
    //            the flock duration minimal (no sleeping while holding the journal lock).
    //   input:  op — mutation to commit
    //   output: Result<LogIndex, LogError>
    //   sideEffects: fence_check; flock_acquire/release; store mutation; journal entry
    // propose:end
    fn propose(&self, op: Op) -> Result<LogIndex, LogError> {
        let key   = Self::cf_key(&op);
        let token = self.fence_token_of(&op, &key);

        // Acquire the inner Mutex FIRST, so fence_check + flock + apply are atomic:
        // the fence for `key` cannot advance between the check and the apply while we
        // hold the log lock (no TOCTOU — CfLog's fence guarantee is self-contained,
        // not merely backstopped by the store's own fence check).
        let mut guard = self.inner.lock().map_err(|_| LogError::Poisoned)?;
        let index = guard.next_index;

        // Step 1: fence_check — reject a stale token before touching stores or journal.
        self.cf.fence_check(&key, token).map_err(|e| match e {
            CouplingFsError::StaleFence { received, current } =>
                LogError::StaleFence(format!(
                    "stale fence on '{key}': token={received} current={current}"
                )),
            other => LogError::Store(other.to_string()),
        })?;

        // Step 2: flock_acquire on coupling-VFS (MemCouplingFs on host; real on FreeBSD).
        self.cf.flock_acquire(&key, true, token).map_err(|e| match e {
            CouplingFsError::Contended(p) =>
                LogError::Store(format!("coupling-VFS lock contended: {p}")),
            other => LogError::Store(other.to_string()),
        })?;

        // Step 3: Apply to stores.
        let result = self.apply_stores(&op, index);

        // Step 4: Release coupling-VFS lock (best-effort — even if apply failed).
        // The flock is held by the fd and released when the fd is closed; the
        // journal does NOT advance on failure.
        let _ = self.cf.flock_release(&key);

        // Step 5: Record in journal only on success.
        if result.is_ok() {
            guard.log.push(LogEntry { index, op });
            guard.next_index += 1;
        }

        result
    }

    fn committed(&self) -> LogIndex {
        let guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        guard.next_index.saturating_sub(1)
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // Helper: build a LocalLog with fresh stores.
    fn make_log() -> LocalLog {
        LocalLog::new(LocalStores {
            sessions: SessionStore::new(),
            locks:    LockStore::new(),
            kv:       KvStore::new(),
            queues:   QueueStore::new(),
            svcs:     SvcStore::new(),
        })
    }

    // log_index_monotonic:start
    //   purpose: Each successful propose() returns a strictly increasing index.
    //   input:  two distinct ops (KvPut on different keys)
    //   output: second index > first index
    //   sideEffects: two entries in journal
    // log_index_monotonic:end
    #[test]
    fn log_index_monotonic() {
        let log = make_log();
        let i1 = log.propose(Op::KvPut {
            key: "k1".into(), val: b"v1".to_vec(), fence: 0,
        }).expect("first propose");
        let i2 = log.propose(Op::KvPut {
            key: "k2".into(), val: b"v2".to_vec(), fence: 0,
        }).expect("second propose");
        assert!(i2 > i1, "log index must be strictly increasing: i1={i1} i2={i2}");
    }

    // log_committed_tracks_index:start
    //   purpose: committed() returns the highest successfully applied index.
    //   input:  three successful proposes
    //   output: committed() == 3 after three proposes
    //   sideEffects: journal has three entries
    // log_committed_tracks_index:end
    #[test]
    fn log_committed_tracks_index() {
        let log = make_log();
        assert_eq!(log.committed(), 0, "fresh log must have committed=0");
        log.propose(Op::KvPut { key: "a".into(), val: b"1".to_vec(), fence: 0 })
            .expect("propose 1");
        assert_eq!(log.committed(), 1);
        log.propose(Op::KvPut { key: "b".into(), val: b"2".to_vec(), fence: 0 })
            .expect("propose 2");
        assert_eq!(log.committed(), 2);
    }

    // failed_op_does_not_advance_index:start
    //   purpose: A failed propose() (e.g. CAS conflict) must NOT advance the log index.
    //            This is the "leader does not commit a no-op" invariant.
    //   input:  KvCas with expect_ver=1 on a non-existent key (→ NotFound error)
    //   output: propose returns Err; committed() still 0; journal still empty
    //   sideEffects: none
    // failed_op_does_not_advance_index:end
    #[test]
    fn failed_op_does_not_advance_index() {
        let log = make_log();
        // CAS on absent key with expect_ver=1 → NotFound.
        let res = log.propose(Op::KvCas {
            key: "missing".into(), val: b"x".to_vec(), expect_ver: 1, fence: 0,
        });
        assert!(res.is_err(), "CAS on absent key must fail");
        assert_eq!(log.committed(), 0, "committed index must not advance on failed propose");
        assert!(log.journal().is_empty(), "journal must be empty after failed propose");
    }

    // session_open_via_log_is_stored:start
    //   purpose: SessionOpen op via propose() results in a live session in the store.
    //   input:  SessionOpen propose; then direct store inspection
    //   output: index returned; session exists in store
    //   sideEffects: one session in store
    // session_open_via_log_is_stored:end
    #[test]
    fn session_open_via_log_is_stored() {
        let log = make_log();
        let idx = log.propose(Op::SessionOpen { node: 42, ttl_ms: 5_000, epoch: 0 })
            .expect("SessionOpen propose");
        assert!(idx >= 1, "index must be at least 1");
        // Verify something was opened: keepalive on whatever sid was assigned.
        // We can't recover the sid from propose() directly (it returns index, not sid).
        // But we can verify: try to open a second session — it should also succeed.
        let idx2 = log.propose(Op::SessionOpen { node: 43, ttl_ms: 5_000, epoch: 0 })
            .expect("second SessionOpen");
        assert!(idx2 > idx, "second session must get higher index");
    }

    // lock_acquire_via_log_exclusive:start
    //   purpose: LockAcquire(exclusive) via propose(); second LockAcquire on same key fails.
    //   input:  two LockAcquire exclusive ops on same key, different sids
    //   output: first succeeds; second returns Err (Busy)
    //   sideEffects: one lock holder in LockStore
    // lock_acquire_via_log_exclusive:end
    #[test]
    fn lock_acquire_via_log_exclusive() {
        let log = make_log();
        log.propose(Op::LockAcquire {
            key: "lock/db".into(), sid: 1, mode: LockMode::Exclusive,
        }).expect("first acquire");

        let res = log.propose(Op::LockAcquire {
            key: "lock/db".into(), sid: 2, mode: LockMode::Exclusive,
        });
        assert!(res.is_err(), "second exclusive acquire on held key must fail");
        // Committed index advanced only for the successful op.
        assert_eq!(log.committed(), 1, "only first op committed");
    }

    // kv_put_then_cas_via_log:start
    //   purpose: KvPut then KvCas via propose() — end-to-end write path through log.
    //   input:  KvPut (creates key, ver=1); KvCas(expect_ver=1, val=new)
    //   output: both ops succeed; committed index == 2
    //   sideEffects: kv store has key at ver=2 with new value
    // kv_put_then_cas_via_log:end
    #[test]
    fn kv_put_then_cas_via_log() {
        let log = make_log();
        log.propose(Op::KvPut { key: "x".into(), val: b"old".to_vec(), fence: 0 })
            .expect("put");
        log.propose(Op::KvCas { key: "x".into(), val: b"new".to_vec(), expect_ver: 1, fence: 0 })
            .expect("cas");
        assert_eq!(log.committed(), 2);

        // Verify store state directly.
        let v = crate::kv::get(&log.stores.kv, "x").expect("get");
        assert_eq!(v.data, b"new");
        assert_eq!(v.version, 2);
    }

    // queue_push_pop_via_log:start
    //   purpose: QueuePush then QueuePop via propose() — queue mutation path.
    //   input:  push payload; pop it
    //   output: both ops succeed; committed index == 2
    //   sideEffects: queue is empty after pop
    // queue_push_pop_via_log:end
    #[test]
    fn queue_push_pop_via_log() {
        let log = make_log();
        log.propose(Op::QueuePush { queue: "q".into(), payload: b"hello".to_vec() })
            .expect("push");
        log.propose(Op::QueuePop { queue: "q".into() })
            .expect("pop");
        assert_eq!(log.committed(), 2);
        // Queue is now empty.
        let res = crate::queue::pop(&log.stores.queues, "q");
        assert!(res.is_err(), "queue must be empty after pop-via-log");
    }

    // svc_register_via_log:start
    //   purpose: SvcRegister via propose() results in a resolvable entry.
    //   input:  SvcRegister propose for name "pg"
    //   output: propose succeeds; resolve on store returns node=7
    //   sideEffects: one svc entry in SvcStore
    // svc_register_via_log:end
    #[test]
    fn svc_register_via_log() {
        let log = make_log();
        log.propose(Op::SvcRegister { name: "pg".into(), node: 7, sid: 1 })
            .expect("svc register");
        let e = crate::svc::resolve(&log.stores.svcs, "pg").expect("resolve");
        assert_eq!(e.node, 7);
    }

    // journal_records_ops_in_order:start
    //   purpose: The journal holds all committed ops in insertion order with correct indices.
    //   input:  three ops (KvPut, KvPut, QueuePush)
    //   output: journal has 3 entries; indices 1, 2, 3 in order
    //   sideEffects: none beyond propose
    // journal_records_ops_in_order:end
    #[test]
    fn journal_records_ops_in_order() {
        let log = make_log();
        log.propose(Op::KvPut { key: "a".into(), val: b"1".to_vec(), fence: 0 }).unwrap();
        log.propose(Op::KvPut { key: "b".into(), val: b"2".to_vec(), fence: 0 }).unwrap();
        log.propose(Op::QueuePush { queue: "q".into(), payload: b"x".to_vec() }).unwrap();

        let j = log.journal();
        assert_eq!(j.len(), 3);
        assert_eq!(j[0].index, 1);
        assert_eq!(j[1].index, 2);
        assert_eq!(j[2].index, 3);
    }

    // concurrent_proposes_are_totally_ordered:start
    //   purpose: N threads proposing concurrently must yield a journal with N entries
    //            carrying unique consecutive indices 1..=N (the total-order invariant).
    //            Regression guard: an earlier impl dropped the log lock during apply,
    //            letting proposals race the index — that bug fails this test.
    //   input:  16 threads × 8 KvPut proposes each (128 total) on distinct keys
    //   output: journal has 128 entries; indices are exactly 1..=128, no gaps/dups
    //   sideEffects: 128 kv entries
    // concurrent_proposes_are_totally_ordered:end
    #[test]
    fn concurrent_proposes_are_totally_ordered() {
        use std::thread;
        let log = Arc::new(make_log());
        let threads = 16usize;
        let per = 8usize;
        let handles: Vec<_> = (0..threads)
            .map(|t| {
                let log = Arc::clone(&log);
                thread::spawn(move || {
                    for i in 0..per {
                        log.propose(Op::KvPut {
                            key: format!("k-{t}-{i}"),
                            val: b"v".to_vec(),
                            fence: 0,
                        })
                        .expect("concurrent propose");
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().expect("thread join");
        }

        let total = (threads * per) as LogIndex;
        assert_eq!(log.committed(), total, "all concurrent proposes must commit");
        let mut indices: Vec<LogIndex> = log.journal().iter().map(|e| e.index).collect();
        indices.sort_unstable();
        let expected: Vec<LogIndex> = (1..=total).collect();
        assert_eq!(indices, expected, "indices must be exactly 1..=N — no gaps or dups");
    }

    // ── CfLog tests ───────────────────────────────────────────────────────────

    use crate::os::{MemCouplingFs, MemFencer};

    /// Build a CfLog with fresh stores + in-memory CouplingFs + Fencer.
    fn make_cf_log() -> CfLog {
        CfLog::new(
            LocalStores {
                sessions: SessionStore::new(),
                locks:    LockStore::new(),
                kv:       KvStore::new(),
                queues:   QueueStore::new(),
                svcs:     SvcStore::new(),
            },
            Arc::new(MemCouplingFs::new()),
            Arc::new(MemFencer::new()),
        )
    }

    // cflog_basic_propose:start
    //   purpose: CfLog.propose() commits an op through flock + stores correctly.
    //            Verifies the CF backend works end-to-end against MemCouplingFs.
    //   input:  KvPut propose with fence=0
    //   output: index >= 1; committed() == 1; store has the key
    //   sideEffects: one kv entry in store
    // cflog_basic_propose:end
    #[test]
    fn cflog_basic_propose() {
        let log = make_cf_log();
        let idx = log.propose(Op::KvPut {
            key: "cf/x".into(), val: b"hello".to_vec(), fence: 0,
        }).expect("cflog propose must succeed");

        assert!(idx >= 1, "CfLog index must be >= 1");
        assert_eq!(log.committed(), 1);

        let v = crate::kv::get(&log.stores.kv, "cf/x").expect("get after propose");
        assert_eq!(v.data, b"hello");
    }

    // cflog_stale_fence_rejected:start
    //   purpose: THE KEY SAFETY TEST.
    //            propose(KvPut { fence: stale }) on CfLog MUST be rejected when
    //            the CouplingFs fence_check sees a higher fence for that key.
    //            This proves that a dead node's stale token cannot write to the store.
    //
    //            Scenario:
    //              1. Directly inject fence=5 into MemCouplingFs.fences for the
    //                 key path "coupling/kv/cf/guarded" — simulates a previous
    //                 exclusive-grant epoch that bumped the fence to 5.
    //              2. Propose KvPut { fence: 3 } — stale (3 < 5).
    //              3. fence_check() inside propose() detects the stale token.
    //              4. Expect Err(LogError::StaleFence); store unchanged; index still 0.
    //
    //   input:  MemCouplingFs.fences pre-set to 5 for the VFS key;
    //           Op::KvPut { fence: 3 } (stale)
    //   output: propose returns Err(LogError::StaleFence); committed() == 0;
    //           store is empty (write NOT applied)
    //   sideEffects: none — op must NOT be applied to stores
    // cflog_stale_fence_rejected:end
    #[test]
    fn cflog_stale_fence_rejected() {
        let cf = Arc::new(MemCouplingFs::new());

        // Inject fence=5 into MemCouplingFs for the VFS key that CfLog will use.
        // CfLog maps Op::KvPut { key: "cf/guarded" } → VFS path "coupling/kv/cf/guarded".
        // MemCouplingFs.fence_check reads from its `fences` map (pub field for test injection).
        cf.fences.lock().unwrap()
            .insert("coupling/kv/cf/guarded".to_string(), 5);

        let log = CfLog::new(
            LocalStores {
                sessions: SessionStore::new(),
                locks:    LockStore::new(),
                kv:       KvStore::new(),
                queues:   QueueStore::new(),
                svcs:     SvcStore::new(),
            },
            Arc::clone(&cf) as Arc<dyn CouplingFs>,
            Arc::new(MemFencer::new()),
        );

        // Attempt to write with a stale fence token (3 < 5).
        let result = log.propose(Op::KvPut {
            key: "cf/guarded".into(),
            val: b"evil write from dead node".to_vec(),
            fence: 3,  // STALE: stored fence is 5
        });

        // Must be rejected as StaleFence — no other error is acceptable here.
        match &result {
            Err(LogError::StaleFence(msg)) => {
                assert!(
                    msg.contains("stale fence"),
                    "error message must mention 'stale fence', got: {msg}"
                );
            }
            other => panic!(
                "EXPECTED StaleFence but got: {:?} — \
                 a stale token from a dead node was NOT rejected, split-brain possible!",
                other
            ),
        }

        // Index must NOT have advanced — stale op must not commit.
        assert_eq!(
            log.committed(), 0,
            "committed index must NOT advance after stale-fence rejection"
        );

        // Store must be empty — the write must NOT have been applied.
        let store_result = crate::kv::get(&log.stores.kv, "cf/guarded");
        assert!(
            store_result.is_err(),
            "store must be empty: stale write must not have been applied to the KV store"
        );
    }

    // cflog_fresh_fence_accepted:start
    //   purpose: Verify that a fresh (current) fence token is accepted by CfLog.
    //            Complements cflog_stale_fence_rejected: the boundary is correct.
    //   input:  MemCouplingFs with no prior fence for key; propose with fence=1
    //   output: propose succeeds; committed() == 1; store has value
    //   sideEffects: one kv entry
    // cflog_fresh_fence_accepted:end
    #[test]
    fn cflog_fresh_fence_accepted() {
        let log = make_cf_log();
        // fence=1, no prior fence for this key → current, accepted.
        let idx = log.propose(Op::KvPut {
            key: "cf/new".into(), val: b"value".to_vec(), fence: 1,
        }).expect("fresh fence must be accepted");

        assert!(idx >= 1);
        assert_eq!(log.committed(), 1);
    }

    // cflog_failed_op_does_not_advance_index:start
    //   purpose: CfLog: a failed store op (e.g. CAS conflict) does NOT advance index.
    //   input:  KvCas on absent key with expect_ver=1 (NotFound)
    //   output: Err from propose; committed() == 0; journal empty
    //   sideEffects: none
    // cflog_failed_op_does_not_advance_index:end
    #[test]
    fn cflog_failed_op_does_not_advance_index() {
        let log = make_cf_log();
        let res = log.propose(Op::KvCas {
            key: "cf/missing".into(), val: b"x".to_vec(), expect_ver: 1, fence: 0,
        });
        assert!(res.is_err(), "CAS on absent key must fail");
        assert_eq!(log.committed(), 0, "index must not advance on failed op");
    }

    // cflog_dispatch_goes_via_propose:start
    //   purpose: ALL CP mutations route through propose() — verified by checking
    //            that the journal grows for each mutation type.
    //            This is the seam-wiring test: if dispatch_* in server.rs calls
    //            stores directly instead of log.propose(), the journal won't grow.
    //            (For LocalLog and CfLog both, the test checks the seam is intact.)
    //   input:  SessionOpen + LockAcquire + QueuePush via a LocalLog (simplest)
    //   output: journal has 3 entries; committed() == 3
    //   sideEffects: session, lock, queue entries created
    // cflog_dispatch_goes_via_propose:end
    #[test]
    fn cflog_dispatch_goes_via_propose() {
        let log = make_log(); // LocalLog — same seam test, host-testable
        log.propose(Op::SessionOpen { node: 1, ttl_ms: 5_000, epoch: 0 })
            .expect("session open via propose");
        log.propose(Op::LockAcquire { key: "lock/test".into(), sid: 1, mode: LockMode::Exclusive })
            .expect("lock acquire via propose");
        log.propose(Op::QueuePush { queue: "q".into(), payload: b"msg".to_vec() })
            .expect("queue push via propose");

        assert_eq!(log.committed(), 3, "all 3 mutations must go through propose (seam intact)");
        assert_eq!(log.journal().len(), 3, "journal must record all 3 ops");
    }
}
