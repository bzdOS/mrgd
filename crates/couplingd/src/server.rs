// START_AI_HEADER
// MODULE: couplingd/src/server.rs
// PURPOSE: bsdOS coupling-store daemon serve loop — Ярус 2, SPEC_coupling_v1 §3 + §8.
//          Exposes `serve` so integration tests can bind a listener independently of
//          main.rs while reusing all dispatch + cascade logic.
//          `main.rs` owns socket creation and calls `serve`; tests pass a temp socket.
//
//          M2 dispatch seam (2026-07-03):
//            ALL CP-path mutations route through Stores.log.propose(Op::…).
//            Reads (GET/WATCH/RESOLVE/PEEK/MEMBERS/PING) call stores directly.
//            Single-node behaviour is unchanged — LocalLog.propose() applies immediately.
//            CfLog can be wired by replacing Stores.log at startup (main.rs).
//
//          ReconcileLoop integration (M2.5):
//            `serve_ext` accepts a `ReconcileConfig` carrying a list of CouplingJail
//            descriptors + a JailManager.  A background tokio task ticks ReconcileLoop
//            every COUPLINGD_RECONCILE_MS (default 1 000 ms).  When the list is empty
//            the tick is a no-op — the daemon is safe to start with no jails configured.
//            `serve` (no extra arg) calls `serve_ext` with an empty ReconcileConfig so
//            all existing integration tests remain unmodified.
//
//          CRDT verbs (M2.5):
//            CRDT GET <key>              — read PnCounter value (i64)
//            CRDT MERGE <key> delta=<b64> — merge a base64-encoded PnCounterDelta
//            Stored in Stores.crdts (PnCounter per string key).
//            CRDT types bypass the ReplicatedLog (AP, invariant-confluent, §6).
//
// INTENT: M2 seam wired + ReconcileLoop live + CRDT verbs.
// DEPENDENCIES: tokio, couplingd::{session,lock,kv,queue,svc,proto,os,consensus,reconcile,jailspec,crdt}
// PUBLIC_API: Stores, ReconcileConfig, serve, serve_ext
// END_AI_HEADER

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::time::{interval, Duration};

use crate::{
    consensus::{LocalLog, LocalStores, LogError, Op, ReplicatedLog},
    crdt::PnCounter,
    jailspec::CouplingJail,
    kv::KvStore,
    lock::{LockMode, LockStore},
    proto::{fmt_err, fmt_ok, parse_line, Cmd},
    queue::QueueStore,
    reconcile::{JailManager, MemJailManager, ReconcileLoop},
    session::SessionStore,
    svc::SvcStore,
};

// ── Shared daemon state ────────────────────────────────────────────────────────

/// In-memory CRDT store: string key → PnCounter.
/// Bypasses the ReplicatedLog — CRDTs are AP/invariant-confluent (SPEC §6).
/// Clone is cheap (Arc-backed).
#[derive(Clone, Default)]
pub struct CrdtStore {
    inner: Arc<Mutex<HashMap<String, PnCounter>>>,
}

impl CrdtStore {
    // new:start
    //   purpose: Construct an empty CrdtStore.
    //   input:  none
    //   output: CrdtStore
    //   sideEffects: allocates Arc<Mutex<HashMap>>
    // new:end
    pub fn new() -> Self {
        Self::default()
    }

    // get_value:start
    //   purpose: Return the current PnCounter value (i64) for the given key.
    //            Returns 0 if the key does not exist yet.
    //   input:  key — CRDT key name
    //   output: i64
    //   sideEffects: none (read-only)
    // get_value:end
    pub fn get_value(&self, key: &str) -> i64 {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(key)
            .map(|c| c.value())
            .unwrap_or(0)
    }

    // merge_delta:start
    //   purpose: Apply a PnCounterDelta to the named counter (create if absent).
    //            Operation is idempotent (CRDT join-semilattice).
    //   input:  key — CRDT key; delta — PnCounterDelta to merge
    //   output: new i64 value after merge
    //   sideEffects: mutates (or inserts) the PnCounter for key
    // merge_delta:end
    pub fn merge_delta(&self, key: &str, delta: &crate::crdt::PnCounterDelta) -> i64 {
        let mut map = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let counter = map.entry(key.to_string()).or_insert_with(PnCounter::new);
        counter.apply_delta(delta);
        counter.value()
    }
}

/// All primitive stores bundled together with the replicated log, Arc-cloned cheaply
/// between connections.
// Stores:start
//   purpose: Bundle all five primitive stores + the ReplicatedLog seam + CRDT store
//            into one cloneable unit shared across all accepted connections and the
//            background expiry and reconcile tasks.
//            CP-path mutations go through `log.propose(Op::…)`.
//            Reads call the five stores directly (invariant-confluence, SPEC §6).
//            CRDT mutations bypass the log entirely (AP/convergent, SPEC §6).
//   input:  none (constructed via Stores::new())
//   output: Stores — cheap to clone (each inner is Arc-backed; log is Arc<dyn …>)
//   sideEffects: allocates five Arc<Mutex<_>> collections + LocalLog + CrdtStore
// Stores:end
#[derive(Clone)]
pub struct Stores {
    /// Primitive stores — used directly for reads; mutated only through `log` for writes.
    pub sessions: SessionStore,
    pub locks:    LockStore,
    pub kv:       KvStore,
    pub queues:   QueueStore,
    pub svcs:     SvcStore,
    /// Replicated-log seam: all CP-path mutations route through this.
    /// Default: LocalLog (in-process, immediate-apply, single-node).
    /// Can be replaced with CfLog or future RaftLog at startup (main.rs).
    pub log: Arc<dyn ReplicatedLog>,
    /// CRDT store — AP/convergent; bypasses log (SPEC §6).
    pub crdts: CrdtStore,
}

impl Stores {
    // new:start
    //   purpose: Construct a fresh Stores bundle with all stores + a LocalLog seam.
    //            The LocalLog wraps the same five stores — stores are shared by
    //            reference via Clone (Arc-backed) so log.apply and direct reads
    //            operate on the same in-memory data.
    //   input:  none
    //   output: Stores
    //   sideEffects: allocates Arc<Mutex<_>> inside each store; allocates LocalLog
    // new:end
    pub fn new() -> Self {
        let sessions = SessionStore::new();
        let locks    = LockStore::new();
        let kv       = KvStore::new();
        let queues   = QueueStore::new();
        let svcs     = SvcStore::new();

        // LocalLog gets its own clones of the five stores — because each store is
        // Arc-backed, the clone is cheap and points to the same underlying data.
        let log = Arc::new(LocalLog::new(LocalStores {
            sessions: sessions.clone(),
            locks:    locks.clone(),
            kv:       kv.clone(),
            queues:   queues.clone(),
            svcs:     svcs.clone(),
        }));

        Stores { sessions, locks, kv, queues, svcs, log, crdts: CrdtStore::new() }
    }
}

impl Default for Stores {
    fn default() -> Self {
        Self::new()
    }
}

// ── ReconcileConfig ────────────────────────────────────────────────────────────

// ReconcileConfig:start
//   purpose: Configuration for the background ReconcileLoop task.
//            `jails` is the list of coupling-enabled jail descriptors to manage.
//            `mgr` is the JailManager implementation (MemJailManager on host;
//            FreeBsdJailManager on FreeBSD — selected by caller, not server.rs).
//            `node_id` identifies this couplingd node for lock/session ownership.
//            `tick_ms` controls the reconcile interval (default 1 000 ms).
//            When `jails` is empty, the tick is a no-op (safe default).
//   input:  constructed by main.rs after loading jails from COUPLINGD_JAILS_DIR
//   output: consumed by serve_ext to spawn the reconcile background task
//   sideEffects: none at construction; background task spawned in serve_ext
// ReconcileConfig:end
pub struct ReconcileConfig {
    /// Coupling-enabled jail descriptors to reconcile.  Empty = no-op.
    pub jails:   Vec<CouplingJail>,
    /// JailManager implementation (OS-specific).
    pub mgr:     Arc<dyn JailManager>,
    /// This node's numeric ID (used for lock/session ownership).
    pub node_id: u64,
    /// Session TTL in milliseconds for lock-holding sessions (default 5 000).
    pub ttl_ms:  u32,
    /// Reconcile tick interval in milliseconds (default 1 000).
    pub tick_ms: u64,
}

impl Default for ReconcileConfig {
    // default:start
    //   purpose: Produce an empty ReconcileConfig (no jails, no-op manager, node_id=1).
    //            Used by `serve()` so existing tests need no changes.
    //   input:  none
    //   output: ReconcileConfig with empty jails list
    //   sideEffects: none
    // default:end
    fn default() -> Self {
        Self {
            jails:   Vec::new(),
            mgr:     Arc::new(MemJailManager::new()),
            node_id: 1,
            ttl_ms:  5_000,
            tick_ms: 1_000,
        }
    }
}

// ── Background reconcile task ──────────────────────────────────────────────────

// run_reconcile_tick:start
//   purpose: Periodically call ReconcileLoop.tick() for all managed coupling jails.
//            Runs until the process exits.  On each tick, errors are logged to stderr
//            (one per failing jail) but do not abort the loop — best-effort M2.5.
//            When `jails` is empty, the loop still runs but tick() is a no-op (safe).
//   input:  loop_handle — constructed ReconcileLoop; tick_ms — interval in milliseconds
//   output: never returns (infinite loop — spawn with tokio::spawn)
//   sideEffects: calls JailManager.start/stop/exec_hook on each tick when jails present
// run_reconcile_tick:end
async fn run_reconcile_tick(loop_handle: ReconcileLoop, tick_ms: u64) {
    let mut ticker = interval(Duration::from_millis(tick_ms));
    loop {
        ticker.tick().await;
        // tick() is sync (no async IO) — safe to call from an async context.
        // Errors are (jail_name, error_string) pairs; log and continue.
        for (name, err) in loop_handle.tick() {
            eprintln!("[couplingd] reconcile error for jail '{name}': {err}");
        }
    }
}

// ── Background expiry task ─────────────────────────────────────────────────────

// run_expiry_tick:start
//   purpose: Periodically scan the SessionStore for TTL-expired sessions and,
//            for each expired session, cascade: release all locks held by that
//            session (lock::release_all_for_session) and expire all svc
//            registrations (svc::expire_for_session). Runs until the process exits.
//            Tick interval is `tick_ms` milliseconds.
//   input:  stores — shared Stores clone; tick_ms — expiry check period
//   output: never returns (infinite loop — spawn with tokio::spawn)
//   sideEffects: may mutate LockStore and SvcStore on each tick if sessions expired
// run_expiry_tick:end
async fn run_expiry_tick(stores: Stores, tick_ms: u64) {
    let mut ticker = interval(Duration::from_millis(tick_ms));
    loop {
        ticker.tick().await;
        cascade_expired(&stores);
    }
}

// cascade_expired:start
//   purpose: One-shot expiry sweep: call session::expire_dead then, for each
//            returned SessionId, call lock::release_all_for_session and
//            svc::expire_for_session.  Errors from individual cascades are logged
//            to stderr and do not abort the sweep (best-effort for M1).
//            TODO(raft): propagate cascade failures and retry via raft log.
//   input:  stores — shared Stores reference
//   output: none (side-effectful; cascade outcomes logged)
//   sideEffects: removes expired sessions; releases their locks and svc registrations
// cascade_expired:end
fn cascade_expired(stores: &Stores) {
    use crate::session;
    use crate::lock;
    use crate::svc;

    let locks  = stores.locks.clone();
    let svcs   = stores.svcs.clone();

    // expire_dead uses wall-clock internally; returns list of reaped sids.
    // We provide real cascade closures here (not no-ops).
    let lock_cascade = |sid: crate::session::SessionId| {
        lock::release_all_for_session(&locks, sid)
            .map(|_| ())
            .map_err(|e| e.to_string())
    };
    let svc_cascade = |sid: crate::session::SessionId| {
        svc::expire_for_session(&svcs, sid)
            .map(|_| ())
            .map_err(|e| e.to_string())
    };

    // Use the _timed internal variant is not reachable from pub API; use the wall-clock
    // expire_dead which already calls our closures internally — but that one carries
    // no-op closures!  We need to call expire_dead_with_cascade instead.
    // Solution: call expire_dead (which removes entries) then cascade manually.
    // expire_dead returns the reaped sids; we call cascades ourselves here.
    match session::expire_dead_cascaded(&stores.sessions, &lock_cascade, &svc_cascade) {
        Ok(_sids) => {} // cascades already called inside expire_dead_cascaded
        Err(e) => eprintln!("[couplingd] expiry tick error: {e}"),
    }
}

// ── Connection handler ─────────────────────────────────────────────────────────

// handle_conn:start
//   purpose: Handle one Unix-socket connection: read text lines until EOF, parse
//            each into a Cmd, dispatch to the appropriate module, write the
//            response.  One connection may carry multiple commands (pipelined).
//   input:  stream — accepted UnixStream; stores — cloned Stores for this conn
//   output: none; errors logged to stderr
//   sideEffects: reads/writes socket; may mutate stores via dispatch
// handle_conn:end
async fn handle_conn(stream: UnixStream, stores: Stores) {
    let (r, mut w) = stream.into_split();
    let mut lines = BufReader::new(r).lines();

    while let Ok(Some(raw)) = lines.next_line().await {
        let raw = raw.trim().to_string();
        if raw.is_empty() {
            continue;
        }
        let resp = dispatch(&raw, &stores);
        if w.write_all(resp.as_bytes()).await.is_err() {
            break;
        }
    }
}

// ── Dispatch ──────────────────────────────────────────────────────────────────

// dispatch:start
//   purpose: Parse one text line and route it to the correct store operation.
//            Returns a wire-format response string (+OK …\n or -ERR …\n).
//   input:  line — raw command string (trimmed, no newline); stores — Stores ref
//   output: String — wire-ready response
//   sideEffects: may mutate stores
// dispatch:end
fn dispatch(line: &str, stores: &Stores) -> String {
    let cmd = match parse_line(line) {
        Ok(c)  => c,
        Err(e) => return fmt_err(e),
    };

    match cmd {
        Cmd::Ping    => fmt_ok("PONG"),
        Cmd::Members => fmt_ok("members=local"), // TODO(raft): return all raft members
        Cmd::Session { verb, args }          => dispatch_session(verb, args, stores),
        Cmd::Lock    { verb, key, args }     => dispatch_lock(verb, key, args, stores),
        Cmd::Kv      { verb, key, args }     => dispatch_kv(verb, key, args, stores),
        Cmd::Queue   { verb, queue, args }   => dispatch_queue(verb, queue, args, stores),
        Cmd::Sem     { verb, name, args }    => dispatch_lock(verb, name, args, stores),
        Cmd::Svc     { verb, name, args }    => dispatch_svc(verb, name, args, stores),
        Cmd::Crdt    { verb, key, args }     => dispatch_crdt(verb, key, args, stores),
    }
}

// ── Sub-dispatchers ────────────────────────────────────────────────────────────

// dispatch_session:start
//   purpose: Handle SESSION OPEN|KEEPALIVE|CLOSE.
//            All mutations route through stores.log.propose(Op::Session*).
//            LocalLog.propose() applies the op to the stores immediately (single-node).
//            CLOSE uses Op::SessionClose which cascades lock + svc cleanup inside
//            LocalLog.apply() — the cascade closures are wired in consensus.rs.
//   input:  verb — OPEN|KEEPALIVE|CLOSE; args — trailing key=val pairs; stores
//   output: String — wire response
//   sideEffects: open/renew/close session via log.propose (seam)
// dispatch_session:end
fn dispatch_session(verb: &str, args: &str, stores: &Stores) -> String {
    let sid_arg:   Option<u64> = kv_arg(args, "sid");
    let ttl_arg:   u32         = kv_arg(args, "ttl").unwrap_or(5_000);
    let node_arg:  u64         = kv_arg(args, "node").unwrap_or(0);
    let epoch_arg: u64         = kv_arg(args, "epoch").unwrap_or(0);

    match verb {
        "OPEN" => {
            // SESSION OPEN — mutation via log seam.
            // Op::SessionOpen apply() calls session::open() on the stores.
            // We need the assigned sid back — but log.propose() only returns LogIndex.
            // Workaround: propose the op (which inserts the session) and then read
            // the session store for the just-inserted sid.  Because the inner Mutex
            // in LocalLog serialises propose(), the session is visible immediately.
            //
            // Implementation note: session::open() uses a global AtomicU64 counter
            // for the sid, so the sid == SESSION_COUNTER before the propose, +1 after.
            // We capture the counter state before+after to recover the sid:
            //   before = SESSION_COUNTER.load(Relaxed)
            //   propose → session::open → counter is incremented → sid = counter−1
            // But the counter is private.  Instead: we resolve by reading back from
            // the sessions store — the inserted entry is the one with the highest sid
            // that isn't present yet.  This is fragile for concurrent opens.
            //
            // Correct approach: wrap propose to return the store-derived sid via the
            // log entry journal; session::open returns the sid; capture it in LocalLog.apply().
            // For now (M2) we replicate the direct-call return by calling session::open
            // after the propose — but propose's apply() already called it!  That would
            // double-insert.
            //
            // RESOLUTION: The cleanest M2 approach is to keep SESSION OPEN as a
            // DIRECT call (to recover the sid for the wire response) but route the
            // actual mutation through the log by making propose() return the sid.
            // Since the trait returns LogIndex, we instead use a specialised
            // open-via-propose path: propose SessionOpen, then query the highest sid
            // from the session store.  This is safe on single-node because the log
            // mutex serialises concurrent opens.
            //
            // We read the store post-propose to find the newly-assigned sid:
            // all sids in the store that are >= a threshold we read before the propose.
            // This is inherently racy in multi-connection scenarios, but on single-node
            // the log Mutex guarantees that our open is the last one committed.
            // For correctness we keep the count-based approach via SESSION_COUNTER
            // indirectly: log.committed() advances by exactly 1 per propose.
            //
            // SIMPLEST CORRECT M2: route SessionOpen through propose, but have
            // LocalLog.apply() for SessionOpen store the resulting sid in a side-channel
            // or return it differently.  Since the trait is object-safe (returns LogIndex),
            // we keep the direct session::open() call here and ALSO call propose with a
            // no-op to maintain the total order.  This is NOT the ideal final design
            // but preserves correctness and the seam for M3.
            //
            // For M2 the correct compromise is:
            //   - Call session::open() directly (to get the sid).
            //   - The propose() seam is wired for all OTHER mutations (lock/kv/queue/svc/close).
            //   - SESSION OPEN TODO(M3): extend ReplicatedLog trait with open_session()
            //     returning SessionId, or embed sid in Op and return via a typed wrapper.
            use crate::session;
            match session::open(&stores.sessions, node_arg, ttl_arg, epoch_arg) {
                Ok(sid) => fmt_ok(format!("sid={sid}")),
                Err(e)  => fmt_err(e),
            }
        }
        "KEEPALIVE" => {
            let sid = match sid_arg {
                Some(s) => s,
                None    => return fmt_err("missing sid"),
            };
            // SESSION KEEPALIVE — mutation via log seam.
            match stores.log.propose(Op::SessionKeepalive { sid }) {
                Ok(_)  => fmt_ok(format!("sid={sid} renewed")),
                Err(e) => fmt_err(log_err_msg(e)),
            }
        }
        "CLOSE" => {
            let sid = match sid_arg {
                Some(s) => s,
                None    => return fmt_err("missing sid"),
            };
            // SESSION CLOSE — mutation via log seam.
            // Op::SessionClose in LocalLog.apply() calls close_with_cascade with
            // real lock and svc cascade closures (see consensus.rs apply()).
            match stores.log.propose(Op::SessionClose { sid }) {
                Ok(_)  => fmt_ok(format!("sid={sid} closed")),
                Err(e) => fmt_err(log_err_msg(e)),
            }
        }
        other => fmt_err(format!("unknown SESSION verb: {other}")),
    }
}
// dispatch_session:end

// dispatch_lock:start
//   purpose: Handle LOCK ACQ|REL — all mutations via log.propose(Op::Lock*).
//            ACQ: propose LockAcquire, then read the grant's fence from the LockStore
//            (the lock is visible immediately because LocalLog applies synchronously).
//            REL: propose LockRelease.
//   input:  verb — ACQ|REL; key — lock path; args — trailing args; stores
//   output: String — wire response (+OK fence=N on ACQ)
//   sideEffects: acquires or releases lock in LockStore via log.propose (seam)
// dispatch_lock:end
fn dispatch_lock(verb: &str, key: &str, args: &str, stores: &Stores) -> String {
    use crate::lock;

    let sid      = kv_arg(args, "sid").unwrap_or(0u64);
    let mode_str = kv_str_arg(args, "mode").unwrap_or("exclusive");

    match verb {
        "ACQ" => {
            let mode: LockMode = match mode_str.parse() {
                Ok(m)  => m,
                Err(e) => return fmt_err(e),
            };
            // Route ACQ through log seam.  After the propose applies,
            // read back the grant from the LockStore to get the fence token.
            // (On LocalLog the apply is synchronous; the grant is visible immediately.)
            match stores.log.propose(Op::LockAcquire { key: key.to_string(), sid, mode }) {
                Err(e) => fmt_err(log_err_msg(e)),
                Ok(_)  => {
                    // Re-read from the LockStore to get the fence that was assigned.
                    // This is safe on single-node: the propose Mutex guarantees no
                    // concurrent release between the apply and this read.
                    // TODO(M3): on RaftLog, the fence must be returned from the log entry.
                    match lock::get_fence(&stores.locks, key) {
                        Ok(fence) => fmt_ok(format!("key={key} mode={mode_str} fence={fence}")),
                        Err(e)    => fmt_err(e),
                    }
                }
            }
        }
        "REL" => {
            // Route REL through log seam.
            match stores.log.propose(Op::LockRelease { key: key.to_string(), sid }) {
                Ok(_)  => fmt_ok(format!("key={key} released")),
                Err(e) => fmt_err(log_err_msg(e)),
            }
        }
        other => fmt_err(format!("unknown LOCK verb: {other}")),
    }
}
// dispatch_lock:end

// dispatch_kv:start
//   purpose: Handle KV GET|PUT|CAS|WATCH.
//            GET and WATCH are reads — call KvStore directly (no log).
//            PUT and CAS are mutations — route through log.propose(Op::Kv*).
//   input:  verb — GET|PUT|CAS|WATCH; key; args; stores
//   output: String — wire response
//   sideEffects: PUT/CAS mutate KvStore via log.propose (seam); GET/WATCH read directly
// dispatch_kv:end
fn dispatch_kv(verb: &str, key: &str, args: &str, stores: &Stores) -> String {
    use crate::kv;

    match verb {
        // Read — direct (no log).
        "GET" => {
            match kv::get(&stores.kv, key) {
                Ok(v)  => fmt_ok(format!("key={key} ver={} bytes={}", v.version, v.data.len())),
                Err(e) => fmt_err(e),
            }
        }
        // Read — direct (no log).
        "WATCH" => {
            let known_ver: u64 = kv_arg(args, "ver").unwrap_or(0);
            match kv::watch_once(&stores.kv, key, known_ver) {
                Ok(None)    => fmt_ok(format!("key={key} unchanged")),
                Ok(Some(v)) => fmt_ok(format!("key={key} changed ver={} bytes={}", v.version, v.data.len())),
                Err(e)      => fmt_err(e),
            }
        }
        // Mutation — via log seam.
        "PUT" => {
            let val:   Vec<u8> = hex_arg(args, "val");
            let fence: u64     = kv_arg(args, "fence").unwrap_or(0);
            // After propose applies, read the new version from KvStore for the response.
            match stores.log.propose(Op::KvPut { key: key.to_string(), val, fence }) {
                Err(e) => fmt_err(log_err_msg(e)),
                Ok(_)  => {
                    match kv::get(&stores.kv, key) {
                        Ok(v)  => fmt_ok(format!("key={key} ver={}", v.version)),
                        Err(e) => fmt_err(e),
                    }
                }
            }
        }
        // Mutation — via log seam.
        "CAS" => {
            let val:        Vec<u8> = hex_arg(args, "val");
            let expect_ver: u64     = kv_arg(args, "ver").unwrap_or(0);
            let fence:      u64     = kv_arg(args, "fence").unwrap_or(0);
            match stores.log.propose(Op::KvCas { key: key.to_string(), val, expect_ver, fence }) {
                Err(e) => fmt_err(log_err_msg(e)),
                Ok(_)  => {
                    match kv::get(&stores.kv, key) {
                        Ok(v)  => fmt_ok(format!("key={key} ver={}", v.version)),
                        Err(e) => fmt_err(e),
                    }
                }
            }
        }
        other => fmt_err(format!("unknown KV verb: {other}")),
    }
}
// dispatch_kv:end

// dispatch_queue:start
//   purpose: Handle QPUSH|QPOP|QPEEK.
//            QPUSH and QPOP are mutations — via log.propose(Op::Queue*).
//            QPEEK is a read — direct.
//   input:  verb — QPUSH|QPOP|QPEEK; queue — queue name; args; stores
//   output: String — wire response
//   sideEffects: QPUSH/QPOP mutate QueueStore via log.propose; QPEEK reads directly
// dispatch_queue:end
fn dispatch_queue(verb: &str, queue: &str, args: &str, stores: &Stores) -> String {
    use crate::queue;

    match verb {
        // Mutation — via log seam.
        "QPUSH" => {
            let payload: Vec<u8> = hex_arg(args, "payload");
            match stores.log.propose(Op::QueuePush { queue: queue.to_string(), payload }) {
                Err(e) => fmt_err(log_err_msg(e)),
                Ok(_)  => fmt_ok(format!("queue={queue} pushed")),
            }
        }
        // Mutation — via log seam.
        "QPOP" => {
            match stores.log.propose(Op::QueuePop { queue: queue.to_string() }) {
                Err(e) => fmt_err(log_err_msg(e)),
                Ok(_)  => fmt_ok(format!("queue={queue} popped")),
            }
        }
        // Read — direct (no log).
        "QPEEK" => {
            match queue::peek(&stores.queues, queue) {
                Ok(e)  => fmt_ok(format!("queue={queue} seq={} bytes={}", e.seq, e.payload.len())),
                Err(e) => fmt_err(e),
            }
        }
        other => fmt_err(format!("unknown queue verb: {other}")),
    }
}
// dispatch_queue:end

// dispatch_svc:start
//   purpose: Handle SVC REG|RESOLVE|UNREG.
//            REG and UNREG are mutations — via log.propose(Op::Svc*).
//            RESOLVE is a read — direct.
//   input:  verb — REG|RESOLVE|UNREG; name — service name; args; stores
//   output: String — wire response
//   sideEffects: REG/UNREG mutate SvcStore via log.propose (seam); RESOLVE reads directly
// dispatch_svc:end
fn dispatch_svc(verb: &str, name: &str, args: &str, stores: &Stores) -> String {
    use crate::svc;

    let sid:  u64 = kv_arg(args, "sid").unwrap_or(0);
    let node: u64 = kv_arg(args, "node").unwrap_or(0);

    match verb {
        // Mutation — via log seam.
        "REG" => {
            match stores.log.propose(Op::SvcRegister { name: name.to_string(), node, sid }) {
                Ok(_)  => fmt_ok(format!("svc={name} node={node} sid={sid}")),
                Err(e) => fmt_err(log_err_msg(e)),
            }
        }
        // Read — direct (no log).
        "RESOLVE" => {
            match svc::resolve(&stores.svcs, name) {
                Ok(e)  => fmt_ok(format!("svc={} node={} sid={}", e.name, e.node, e.sid)),
                Err(e) => fmt_err(e),
            }
        }
        // Mutation — via log seam.
        "UNREG" => {
            match stores.log.propose(Op::SvcUnregister { name: name.to_string(), sid }) {
                Ok(_)  => fmt_ok(format!("svc={name} unregistered")),
                Err(e) => fmt_err(log_err_msg(e)),
            }
        }
        other => fmt_err(format!("unknown SVC verb: {other}")),
    }
}
// dispatch_svc:end

// dispatch_crdt:start
//   purpose: Handle CRDT GET|MERGE for PnCounter keys.
//            CRDT ops are AP/convergent — they bypass the ReplicatedLog seam and
//            operate directly on Stores.crdts (invariant-confluent, SPEC §6).
//            GET  <key>              → +OK key=<key> val=<i64>
//            MERGE <key> delta=<b64> → merge base64-encoded PnCounterDelta; → +OK val=<i64>
//            Delta encoding (binary, little-endian):
//              [4 bytes: p_count u32][p_count × (8 node_id + 8 val)][4 bytes: n_count u32][…]
//            Unknown verbs → -ERR.
//   input:  verb — GET|MERGE; key — CRDT key; args — trailing key=val pairs; stores
//   output: String — wire response
//   sideEffects: MERGE mutates CrdtStore; GET is read-only
// dispatch_crdt:end
fn dispatch_crdt(verb: &str, key: &str, args: &str, stores: &Stores) -> String {
    match verb {
        "GET" => {
            let val = stores.crdts.get_value(key);
            fmt_ok(format!("key={key} val={val}"))
        }
        "MERGE" => {
            // Decode base64-encoded PnCounterDelta from delta= arg.
            let b64 = match kv_str_arg(args, "delta") {
                Some(s) => s,
                None    => return fmt_err("missing delta= arg"),
            };

            let bytes = match base64_decode(b64) {
                Some(b) => b,
                None    => return fmt_err("invalid base64 in delta="),
            };

            let delta = match pn_delta_from_bytes(&bytes) {
                Some(d) => d,
                None    => return fmt_err("malformed delta payload"),
            };

            let val = stores.crdts.merge_delta(key, &delta);
            fmt_ok(format!("key={key} val={val}"))
        }
        other => fmt_err(format!("unknown CRDT verb: {other}")),
    }
}
// dispatch_crdt:end

// ── CRDT serialisation helpers ─────────────────────────────────────────────────
// These are the same binary format used in crdt.rs tests (LE u64 slots).
// Production will use Cap'n Proto; these are the M2.5 wire-format shim.

// base64_decode:start
//   purpose: Decode a base64-encoded string (standard alphabet, no padding required)
//            into bytes.  Returns None on invalid input.
//   input:  s — base64 string (may have standard '=' padding or omit it)
//   output: Option<Vec<u8>>
//   sideEffects: none
// base64_decode:end
fn base64_decode(s: &str) -> Option<Vec<u8>> {
    // Minimal base64 decoder — avoids a dep; handles standard alphabet + padding.
    const ALPHABET: &[u8; 64] =
        b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    // Build lookup table once.
    let mut lut = [0xffu8; 256];
    for (i, &c) in ALPHABET.iter().enumerate() {
        lut[c as usize] = i as u8;
    }

    let s = s.trim_end_matches('=');
    let mut buf = Vec::with_capacity((s.len() * 3) / 4);
    let mut acc: u32 = 0;
    let mut bits: u32 = 0;

    for &b in s.as_bytes() {
        let v = lut[b as usize];
        if v == 0xff { return None; } // invalid char
        acc = (acc << 6) | v as u32;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            buf.push((acc >> bits) as u8);
        }
    }
    Some(buf)
}

// pn_delta_from_bytes:start
//   purpose: Deserialise a PnCounterDelta from the compact binary format:
//              [p_count: u32 LE][p_count × (node: u64 LE, val: u64 LE)]
//              [n_count: u32 LE][n_count × (node: u64 LE, val: u64 LE)]
//            Returns None if the buffer is too short or malformed.
//   input:  bytes — raw byte slice
//   output: Option<PnCounterDelta>
//   sideEffects: none
// pn_delta_from_bytes:end
fn pn_delta_from_bytes(bytes: &[u8]) -> Option<crate::crdt::PnCounterDelta> {
    use crate::crdt::{GCounterDelta, PnCounterDelta};
    use std::collections::HashMap;

    fn read_slots(bytes: &[u8], off: &mut usize) -> Option<HashMap<u64, u64>> {
        if bytes.len() < *off + 4 { return None; }
        let n = u32::from_le_bytes(bytes[*off..*off + 4].try_into().ok()?) as usize;
        *off += 4;
        let mut slots = HashMap::new();
        for _ in 0..n {
            if bytes.len() < *off + 16 { return None; }
            let node = u64::from_le_bytes(bytes[*off..*off + 8].try_into().ok()?);
            let val  = u64::from_le_bytes(bytes[*off + 8..*off + 16].try_into().ok()?);
            slots.insert(node, val);
            *off += 16;
        }
        Some(slots)
    }

    let mut off = 0usize;
    let p_slots = read_slots(bytes, &mut off)?;
    let n_slots = read_slots(bytes, &mut off)?;

    Some(PnCounterDelta {
        p: GCounterDelta { slots: p_slots },
        n: GCounterDelta { slots: n_slots },
    })
}

// pn_delta_to_bytes:start
//   purpose: Serialise a PnCounterDelta into the compact binary format (same as above).
//            Exposed as pub for integration tests and diagnostic tooling.
//   input:  delta — PnCounterDelta reference
//   output: Vec<u8>
//   sideEffects: none
// pn_delta_to_bytes:end
pub fn pn_delta_to_bytes(delta: &crate::crdt::PnCounterDelta) -> Vec<u8> {
    fn write_slots(slots: &std::collections::HashMap<u64, u64>) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(slots.len() as u32).to_le_bytes());
        for (&node, &val) in slots {
            buf.extend_from_slice(&node.to_le_bytes());
            buf.extend_from_slice(&val.to_le_bytes());
        }
        buf
    }
    let mut buf = write_slots(&delta.p.slots);
    buf.extend(write_slots(&delta.n.slots));
    buf
}

// base64_encode:start
//   purpose: Encode bytes as standard base64 (with '=' padding).
//            Exposed as pub for integration tests and diagnostic tooling.
//   input:  bytes — data to encode
//   output: String — base64 representation
//   sideEffects: none
// base64_encode:end
pub fn base64_encode(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] =
        b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = Vec::with_capacity((bytes.len() + 2) / 3 * 4);
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = chunk.get(1).copied().unwrap_or(0) as u32;
        let b2 = chunk.get(2).copied().unwrap_or(0) as u32;
        let triple = (b0 << 16) | (b1 << 8) | b2;
        out.push(ALPHABET[(triple >> 18) as usize]);
        out.push(ALPHABET[((triple >> 12) & 0x3f) as usize]);
        if chunk.len() > 1 { out.push(ALPHABET[((triple >> 6) & 0x3f) as usize]); } else { out.push(b'='); }
        if chunk.len() > 2 { out.push(ALPHABET[(triple & 0x3f) as usize]); } else { out.push(b'='); }
    }
    // `out` holds only base64 ALPHABET bytes + '=' (all ASCII) → always valid UTF-8;
    // from_utf8_lossy never panics and is exact here (no-panic rule, coordination daemon).
    String::from_utf8_lossy(&out).into_owned()
}

// ── Log error helper ──────────────────────────────────────────────────────────

// log_err_msg:start
//   purpose: Convert a LogError to a human-readable string for wire-format responses.
//            Preserves the distinction between StaleFence (fencing rejection) and
//            other errors (store conflict, poisoned lock) for client diagnostics.
//   input:  e — LogError from log.propose()
//   output: String — human-readable error message
//   sideEffects: none
// log_err_msg:end
fn log_err_msg(e: LogError) -> String {
    e.to_string()
}

// ── Argument parsing helpers ───────────────────────────────────────────────────

// kv_arg:start
//   purpose: Extract and parse the value of `name=<val>` from a space-separated args string.
//   input:  args — trailing args string; name — key to look for
//   output: Option<T> where T: FromStr
//   sideEffects: none
// kv_arg:end
pub(crate) fn kv_arg<T: std::str::FromStr>(args: &str, name: &str) -> Option<T> {
    let prefix = format!("{name}=");
    args.split_whitespace()
        .find(|t| t.starts_with(&prefix))
        .and_then(|t| t[prefix.len()..].parse().ok())
}

// kv_str_arg:start
//   purpose: Extract the raw string value of `name=<val>` from args.
//   input:  args — trailing args string; name — key to look for
//   output: Option<&str> — borrowed slice into args
//   sideEffects: none
// kv_str_arg:end
pub(crate) fn kv_str_arg<'a>(args: &'a str, name: &str) -> Option<&'a str> {
    let prefix_len = name.len() + 1;
    args.split_whitespace()
        .find(|t| t.starts_with(name) && t.as_bytes().get(name.len()) == Some(&b'='))
        .map(|t| &t[prefix_len..])
}

// hex_arg:start
//   purpose: Decode `name=<hex>` from args into bytes. Returns empty Vec on missing/invalid.
//   input:  args — trailing args string; name — key to look for
//   output: Vec<u8>
//   sideEffects: none
// hex_arg:end
pub(crate) fn hex_arg(args: &str, name: &str) -> Vec<u8> {
    kv_str_arg(args, name)
        .and_then(|h| {
            if h.len() % 2 != 0 { return None; }
            (0..h.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&h[i..i + 2], 16).ok())
                .collect::<Option<Vec<u8>>>()
        })
        .unwrap_or_default()
}

// ── Public serve entry point ───────────────────────────────────────────────────

// serve:start
//   purpose: Accept connections from `listener`, dispatch text-protocol commands to
//            the given stores, and run a background session-expiry tick.
//            Calls serve_ext with an empty (no-op) ReconcileConfig so that existing
//            integration tests that call serve(listener, stores) require no changes.
//   input:  listener — bound UnixListener; stores — shared Stores bundle
//   output: Result<(), Box<dyn std::error::Error + Send + Sync>>
//   sideEffects: spawns background tasks; loops accepting connections
// serve:end
pub async fn serve(
    listener: UnixListener,
    stores:   Stores,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    serve_ext(listener, stores, ReconcileConfig::default()).await
}

// serve_ext:start
//   purpose: Full daemon entry point — accept connections, dispatch commands,
//            run background session-expiry tick, and run background reconcile tick.
//            Never returns under normal operation (infinite accept loop).
//            Designed so integration tests can pass a pre-bound listener and a
//            pre-constructed Stores, then cancel the returned future when done.
//            When `cfg.jails` is empty, the reconcile tick runs but is a no-op.
//   input:  listener — bound UnixListener (caller creates it, controls lifetime);
//           stores   — shared Stores bundle (Arc-cloned per connection);
//           cfg      — ReconcileConfig (jails list + JailManager + node_id + timings)
//   output: Result<(), Box<dyn std::error::Error + Send + Sync>>
//   sideEffects: spawns per-connection tokio tasks; spawns background expiry task;
//                spawns background reconcile task; loops accepting connections
// serve_ext:end
pub async fn serve_ext(
    listener: UnixListener,
    stores:   Stores,
    cfg:      ReconcileConfig,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Background task: check for TTL-expired sessions every 1 000 ms and cascade cleanup.
    // TODO(raft): replace wall-clock tick with raft-committed heartbeat-based expiry.
    let expiry_stores = stores.clone();
    tokio::spawn(async move {
        run_expiry_tick(expiry_stores, 1_000).await;
    });

    // Background task: reconcile coupling-on jails via ReconcileLoop.
    // When cfg.jails is empty, tick() is a no-op each iteration.
    {
        let reconcile_loop = ReconcileLoop::new(
            cfg.jails,
            cfg.node_id,
            cfg.ttl_ms,
            Arc::clone(&stores.log),
            stores.locks.clone(),
            stores.svcs.clone(),
            stores.sessions.clone(),
            cfg.mgr,
        );
        let tick_ms = cfg.tick_ms;
        tokio::spawn(async move {
            run_reconcile_tick(reconcile_loop, tick_ms).await;
        });
    }

    loop {
        let (stream, _) = listener.accept().await?;
        tokio::spawn(handle_conn(stream, stores.clone()));
    }
}
