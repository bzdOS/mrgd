// START_AI_HEADER
// MODULE: couplingd/src/reconcile.rs
// PURPOSE: coupling=on reconcile-loop + JailManager trait (SPEC_coupling_v1 §13C).
//          couplingd (one per node, base-service) reads CouplingJail descriptors and
//          drives elections/start/stop/fencing for coupling=on jails — no sidecars.
//          The axis (not the app) holds the lock, starts the jail, runs hooks.
//
//          Design (§13C, §2):
//            For each role=singleton jail:
//              1. Try lock:<svc> exclusive (via LockStore through ReplicatedLog).
//              2. WINNER: JailManager.start() + SvcStore.register() + exec on_promote.
//                 Fence from LockGrant is threaded through subsequent writes.
//              3. LOSER:  standby — jail NOT started; on_demote NOT called yet.
//              4. On lock loss (session expiry cascades lock away) → JailManager.stop()
//                 + svc unregister + exec on_demote.
//            role=worker:  always start (no contention); no svc lock.
//            role=crdt:    always start (no coordination; Zenoh delta-sync, Ярус-3 deferred).
//
//          Clock-injection: `tick_fn` → injected by tests for determinism (same pattern
//          as session.rs).  Production passes `tokio::time::sleep`-equivalent.
//
//          Circular-dep avoidance: reconcile.rs imports lock/svc/jailspec/session;
//          none of those import reconcile.rs — the loop is the top of the dep tree.
//
// INTENT: M1 slice [a] — host-testable loop; FreeBSD jail ops behind cfg gate.
//         All tests green with cargo test -p couplingd.
// DEPENDENCIES: std, thiserror, crate::{lock,svc,session,jailspec,consensus}
// PUBLIC_API: JailManager, MemJailManager, JailError, ReconcileState, ReconcileLoop
// END_AI_HEADER

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};
use thiserror::Error;

use crate::{
    consensus::{Op, ReplicatedLog},
    jailspec::{CouplingJail, JailRole},
    lock::{self, LockMode, LockStore},
    session::{self, NodeId, SessionStore},
    svc::SvcStore,
};

// ── Errors ────────────────────────────────────────────────────────────────────

/// Errors produced by JailManager operations.
#[derive(Debug, Error)]
pub enum JailError {
    #[error("failed to start jail '{0}': {1}")]
    StartFailed(String, String),
    #[error("failed to stop jail '{0}': {1}")]
    StopFailed(String, String),
    #[error("failed to exec hook in jail '{0}': {1}")]
    HookFailed(String, String),
}

// ── JailManager trait ─────────────────────────────────────────────────────────

// JailManager:start
//   purpose: Abstract FreeBSD jail lifecycle behind a portable interface.
//            On FreeBSD: delegates to `jail -c`/`jail -r`/`jexec` via std::process::Command.
//            On host (test): records calls in a Vec — no OS calls.
//            ReconcileLoop depends only on this trait; FreeBSD impl is gated at startup.
//   input:  (per method) jail_name — jail identifier; cmd — hook command
//   output: Result<(), JailError>
//   sideEffects: may invoke OS process (FreeBSD) or push to event log (Mem)
// JailManager:end
pub trait JailManager: Send + Sync {
    /// Start the named jail.  Idempotent: already-running jails are not an error.
    fn start(&self, jail: &CouplingJail) -> Result<(), JailError>;

    /// Stop the named jail.  Idempotent: already-stopped jails are not an error.
    fn stop(&self, jail_name: &str) -> Result<(), JailError>;

    /// Execute a hook command inside the jail (e.g. on_promote / on_demote).
    /// `cmd` is the raw command string from the coupling.* declaration.
    /// Empty cmd is a no-op (returns Ok immediately).
    fn exec_hook(&self, jail_name: &str, cmd: &str) -> Result<(), JailError>;
}

// ── MemJailManager — in-memory (host tests) ──────────────────────────────────

/// Event recorded by MemJailManager for test assertions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JailEvent {
    Start(String),
    Stop(String),
    Hook { jail: String, cmd: String },
}

/// In-memory JailManager — no OS calls; records events for assertion in tests.
#[derive(Clone, Default)]
pub struct MemJailManager {
    /// Chronological log of all calls.
    pub events: Arc<Mutex<Vec<JailEvent>>>,
    /// Currently running jails (set by start / cleared by stop).
    pub running: Arc<Mutex<HashSet<String>>>,
}

impl MemJailManager {
    // new:start
    //   purpose: Construct an empty MemJailManager (no running jails, no events).
    //   input:  none
    //   output: MemJailManager
    //   sideEffects: allocates two Arc<Mutex<_>>
    // new:end
    pub fn new() -> Self {
        Self::default()
    }

    /// Check whether a jail is currently marked as running.
    pub fn is_running(&self, jail_name: &str) -> bool {
        self.running.lock().unwrap_or_else(|e| e.into_inner())
            .contains(jail_name)
    }

    /// Return all events collected so far.
    pub fn all_events(&self) -> Vec<JailEvent> {
        self.events.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
}

impl JailManager for MemJailManager {
    // start:start
    //   purpose: Mark a jail as running; record JailEvent::Start.
    //   input:  jail — descriptor; jail.name used as identifier
    //   output: Ok(())
    //   sideEffects: inserts jail.name into running set; appends to events log
    // start:end
    fn start(&self, jail: &CouplingJail) -> Result<(), JailError> {
        let name = jail.name.clone();
        self.running.lock().unwrap_or_else(|e| e.into_inner()).insert(name.clone());
        self.events.lock().unwrap_or_else(|e| e.into_inner())
            .push(JailEvent::Start(name));
        Ok(())
    }

    // stop:start
    //   purpose: Mark a jail as stopped; record JailEvent::Stop.
    //   input:  jail_name — jail identifier
    //   output: Ok(())
    //   sideEffects: removes jail_name from running set; appends to events log
    // stop:end
    fn stop(&self, jail_name: &str) -> Result<(), JailError> {
        self.running.lock().unwrap_or_else(|e| e.into_inner()).remove(jail_name);
        self.events.lock().unwrap_or_else(|e| e.into_inner())
            .push(JailEvent::Stop(jail_name.to_string()));
        Ok(())
    }

    // exec_hook:start
    //   purpose: Record JailEvent::Hook if cmd is non-empty; skip empty commands.
    //   input:  jail_name — jail identifier; cmd — hook command string
    //   output: Ok(())
    //   sideEffects: appends to events log if cmd non-empty
    // exec_hook:end
    fn exec_hook(&self, jail_name: &str, cmd: &str) -> Result<(), JailError> {
        if cmd.is_empty() { return Ok(()); }
        self.events.lock().unwrap_or_else(|e| e.into_inner())
            .push(JailEvent::Hook { jail: jail_name.to_string(), cmd: cmd.to_string() });
        Ok(())
    }
}

// ── FreeBsdJailManager — real jail(8) ops (gated) ────────────────────────────

// FreeBsdJailManager:start
//   purpose: JailManager implementation using FreeBSD jail(8) command-line tools.
//            start()     → `jail -c name=<jail>` (creates and starts from /etc/jail.conf).
//            stop()      → `jail -r <jail>` (removes/stops the named jail).
//            exec_hook() → `jexec <jail> sh -c "<cmd>"` (exec inside running jail).
//            Commands are constructed as Vec<&str> args — no sh -c string injection.
//            Idempotent semantics: non-zero exit on already-running/stopped is treated
//            as Ok (FreeBSD jail -c returns 1 if jail already exists in some configs;
//            jail -r returns non-zero if jail not found).
//   input:  (per method) jail — CouplingJail descriptor; jail_name; cmd — hook string
//   output: Result<(), JailError>
//   sideEffects: spawns child process; inherits environment; no network calls
// FreeBsdJailManager:end
#[cfg(target_os = "freebsd")]
pub struct FreeBsdJailManager;

#[cfg(target_os = "freebsd")]
impl FreeBsdJailManager {
    // new:start
    //   purpose: Construct a FreeBsdJailManager (no state; all ops are stateless).
    //   input:  none
    //   output: FreeBsdJailManager
    //   sideEffects: none
    // new:end
    pub fn new() -> Self { Self }
}

#[cfg(target_os = "freebsd")]
impl JailManager for FreeBsdJailManager {
    fn start(&self, jail: &CouplingJail) -> Result<(), JailError> {
        // `jail -c name=<jail_name>` — reads config from /etc/jail.conf.
        // Non-blocking: if jail is already running, jail(8) returns 1 — treat as Ok.
        let out = std::process::Command::new("jail")
            .args(["-c", &format!("name={}", jail.name)])
            .output()
            .map_err(|e| JailError::StartFailed(jail.name.clone(), e.to_string()))?;

        if !out.status.success() {
            let stderr = String::from_utf8_lossy(&out.stderr).to_string();
            // Idempotent: "already running" is not a failure.
            if !stderr.contains("already") {
                return Err(JailError::StartFailed(jail.name.clone(), stderr));
            }
        }
        Ok(())
    }

    fn stop(&self, jail_name: &str) -> Result<(), JailError> {
        // `jail -r <name>` — removes (stops) the named jail.
        let out = std::process::Command::new("jail")
            .args(["-r", jail_name])
            .output()
            .map_err(|e| JailError::StopFailed(jail_name.to_string(), e.to_string()))?;

        if !out.status.success() {
            let stderr = String::from_utf8_lossy(&out.stderr).to_string();
            // Idempotent: "not found" / "no jail named" = already stopped.
            if !stderr.contains("not found") && !stderr.contains("no jail") {
                return Err(JailError::StopFailed(jail_name.to_string(), stderr));
            }
        }
        Ok(())
    }

    fn exec_hook(&self, jail_name: &str, cmd: &str) -> Result<(), JailError> {
        if cmd.is_empty() { return Ok(()); }

        // `jexec <jail> sh -c "<cmd>"` — safe: cmd is a user-declared hook string,
        // not constructed from untrusted network input.  Still uses sh -c to allow
        // simple shell expressions in hooks (e.g. "pg_ctl promote").
        let out = std::process::Command::new("jexec")
            .args([jail_name, "sh", "-c", cmd])
            .output()
            .map_err(|e| JailError::HookFailed(jail_name.to_string(), e.to_string()))?;

        if !out.status.success() {
            let stderr = String::from_utf8_lossy(&out.stderr).to_string();
            return Err(JailError::HookFailed(jail_name.to_string(), stderr));
        }
        Ok(())
    }
}

// ── ReconcileState — per-jail runtime state ───────────────────────────────────

/// Internal runtime state for a single coupling-enabled jail.
#[derive(Debug, Clone, PartialEq, Eq)]
enum JailState {
    /// Not started; standby (singleton: waiting for lock; worker: should never be here).
    Standby,
    /// Running as primary (singleton: holds the lock; worker/crdt: always running).
    Primary { fence: u64 },
}

// ── ReconcileLoop ─────────────────────────────────────────────────────────────

// ReconcileLoop:start
//   purpose: Drive the coupling=on reconciliation for a list of CouplingJail descriptors.
//            Runs one tick per call to `tick()`.  For each jail:
//              - singleton: attempt lock:<svc> exclusive via LockStore; on win →
//                  start jail + register svc + exec on_promote; on loss (lock already
//                  held by another session) → remain standby; on lock gone (session
//                  expiry detected by absence from lock store) → stop + on_demote.
//              - worker/crdt: always start immediately (no contention).
//            Clock-injection: `tick_fn` allows tests to simulate time passing
//            without sleeping.  Production wires a real sleep (tokio::time::sleep).
//   input:  jails — list of CouplingJail; node_id — this node's identity;
//           session_ttl_ms — how long the lock-holding session lives between keepalives;
//           log — ReplicatedLog (mutations go through seam); locks/svcs/sessions stores;
//           mgr — JailManager impl (MemJailManager for tests, FreeBsdJailManager for prod)
//   output: drives JailManager side-effects on each tick
//   sideEffects: acquires/releases LockStore entries; registers/deregisters SvcStore;
//                calls JailManager.start/stop/exec_hook; opens/keepalives SessionStore sessions
// ReconcileLoop:end
pub struct ReconcileLoop {
    jails:    Vec<CouplingJail>,
    node_id:  NodeId,
    ttl_ms:   u32,
    log:      Arc<dyn ReplicatedLog>,
    locks:    LockStore,
    /// Kept for future direct reads (invariant-confluence: reads bypass the log).
    #[allow(dead_code)]
    svcs:     SvcStore,
    sessions: SessionStore,
    mgr:      Arc<dyn JailManager>,
    /// Per-jail runtime state: jail.name → (SessionId, JailState)
    state:    Arc<Mutex<HashMap<String, (u64, JailState)>>>,
}

impl ReconcileLoop {
    // new:start
    //   purpose: Construct a ReconcileLoop with the given stores, log seam, and jail manager.
    //            No OS calls; clock injection happens in tick().
    //   input:  jails — descriptors to manage; node_id — this node; ttl_ms — session TTL;
    //           log — ReplicatedLog seam; locks/svcs/sessions — backing stores;
    //           mgr — jail lifecycle manager
    //   output: ReconcileLoop
    //   sideEffects: none at construction time
    // new:end
    pub fn new(
        jails:    Vec<CouplingJail>,
        node_id:  NodeId,
        ttl_ms:   u32,
        log:      Arc<dyn ReplicatedLog>,
        locks:    LockStore,
        svcs:     SvcStore,
        sessions: SessionStore,
        mgr:      Arc<dyn JailManager>,
    ) -> Self {
        Self {
            jails,
            node_id,
            ttl_ms,
            log,
            locks,
            svcs,
            sessions,
            mgr,
            state: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    // tick:start
    //   purpose: Run one reconcile iteration over all managed jails.
    //            For each jail, calls reconcile_one() which handles the
    //            election/start/stop/hook logic.  Errors from individual jails
    //            are collected and returned as a Vec — one failure does not
    //            prevent reconciliation of the remaining jails.
    //   input:  none (uses internal state)
    //   output: Vec<(jail_name, error_string)> — empty if all jails reconciled cleanly
    //   sideEffects: calls reconcile_one for each jail; may mutate stores + run hooks
    // tick:end
    pub fn tick(&self) -> Vec<(String, String)> {
        let mut errors = Vec::new();
        let jails = self.jails.clone();

        for jail in &jails {
            if let Err(e) = self.reconcile_one(jail) {
                errors.push((jail.name.clone(), e));
            }
        }

        errors
    }

    // reconcile_one:start
    //   purpose: Reconcile a single jail: open/keepalive its session, attempt
    //            lock acquisition (singleton), drive start/stop/hooks as needed.
    //   input:  jail — the CouplingJail descriptor to reconcile
    //   output: Ok(()) if reconciled without error; Err(String) describing the failure
    //   sideEffects: may call JailManager.start/stop/exec_hook; may acquire/release
    //                lock; may register/unregister svc; may open/keepalive session
    // reconcile_one:end
    fn reconcile_one(&self, jail: &CouplingJail) -> Result<(), String> {
        match jail.role {
            JailRole::Worker | JailRole::Crdt => {
                // Worker/CRDT: start unconditionally (no contention).
                self.ensure_started(jail)
                    .map_err(|e| e.to_string())
            }
            JailRole::Singleton => {
                self.reconcile_singleton(jail)
            }
        }
    }

    /// Ensure a worker/crdt jail is running; start it if not already.
    fn ensure_started(&self, jail: &CouplingJail) -> Result<(), JailError> {
        let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let entry = st.entry(jail.name.clone())
            .or_insert((0, JailState::Standby));

        if entry.1 == JailState::Standby {
            self.mgr.start(jail)?;
            entry.1 = JailState::Primary { fence: 0 };
        }
        Ok(())
    }

    /// Core singleton reconcile: election → start/stop → hooks.
    fn reconcile_singleton(&self, jail: &CouplingJail) -> Result<(), String> {
        let lock_key = format!("lock:{}", jail.svc);

        // Ensure we have a session open for this jail's lock tenure.
        let sid = self.ensure_session(&jail.name)
            .map_err(|e| format!("session error: {e}"))?;

        let st = self.state.lock().unwrap_or_else(|e| e.into_inner());

        let current_state = st.get(&jail.name).map(|(_, s)| s.clone());

        match current_state {
            None | Some(JailState::Standby) => {
                // Attempt to win the election via the ReplicatedLog seam.
                drop(st); // release state lock before calling into log/lock
                let result = self.log.propose(Op::LockAcquire {
                    key:  lock_key.clone(),
                    sid,
                    mode: LockMode::Exclusive,
                });

                match result {
                    Ok(_index) => {
                        // Won the election.  Recover fence from LockStore.
                        let fence = lock::get_fence(&self.locks, &lock_key).unwrap_or(0);

                        self.mgr.start(jail).map_err(|e| e.to_string())?;

                        // Register service in SvcStore via seam.
                        self.log.propose(Op::SvcRegister {
                            name: jail.svc.clone(),
                            node: self.node_id,
                            sid,
                        }).map_err(|e| format!("svc register: {e}"))?;

                        // Execute promote hook.
                        self.mgr.exec_hook(&jail.name, &jail.on_promote)
                            .map_err(|e| e.to_string())?;

                        // Record primary state with fence token.
                        let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
                        st.insert(jail.name.clone(), (sid, JailState::Primary { fence }));
                    }
                    Err(_) => {
                        // Lost election — remain standby.
                        // Ensure state entry exists to avoid repeated log spam.
                        let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
                        st.entry(jail.name.clone()).or_insert((sid, JailState::Standby));
                    }
                }
                Ok(())
            }

            Some(JailState::Primary { .. }) => {
                // Check we still hold the lock (session cascade may have removed it).
                let held = lock::is_held_by(&self.locks, &lock_key, sid);

                if !held {
                    // Lost the lock (e.g. our session expired and cascade ran).
                    drop(st);
                    self.mgr.stop(&jail.name).map_err(|e| e.to_string())?;
                    // Best-effort svc unregister via seam; ignore if already gone.
                    let _ = self.log.propose(Op::SvcUnregister { name: jail.svc.clone(), sid });
                    self.mgr.exec_hook(&jail.name, &jail.on_demote)
                        .map_err(|e| e.to_string())?;

                    let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
                    st.insert(jail.name.clone(), (sid, JailState::Standby));
                } else {
                    // Still primary: keepalive session.
                    drop(st);
                    session::keepalive(&self.sessions, sid)
                        .map_err(|e| format!("keepalive: {e}"))?;
                }
                Ok(())
            }
        }
    }

    /// Return a LIVE session for this jail: keepalive the tracked one, or open a fresh
    /// one if it expired. Called every tick for BOTH standby and primary — otherwise a
    /// long-idle standby's session would expire past TTL and later LockAcquire attempts
    /// would run under a dead session (flap / stuck primacy).
    fn ensure_session(&self, jail_name: &str) -> Result<u64, String> {
        let existing = self.state.lock().unwrap_or_else(|e| e.into_inner())
            .get(jail_name).map(|(sid, _)| *sid);

        if let Some(sid) = existing {
            // Renew; if the session is gone (expired), fall through to reopen.
            if session::keepalive(&self.sessions, sid).is_ok() {
                return Ok(sid);
            }
        }

        let sid = session::open(&self.sessions, self.node_id, self.ttl_ms, 0)
            .map_err(|e| e.to_string())?;

        // Persist the (possibly new) sid, preserving any existing JailState.
        let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let jstate = st.get(jail_name).map(|(_, s)| s.clone()).unwrap_or(JailState::Standby);
        st.insert(jail_name.to_string(), (sid, jstate));
        Ok(sid)
    }

    // simulate_lock_loss:start
    //   purpose: TEST HELPER — forcibly remove the lock held by this node's session for
    //            `jail.svc`, simulating session expiry / cascade on another node.
    //            Uses lock::release_all_for_session (public API) so internal fields
    //            stay private.  Resets internal JailState to Primary{fence:0} so the
    //            next tick goes into the Primary arm, calls is_held_by (false) → stop + on_demote.
    //            Only compiled under #[cfg(test)].
    //   input:  jail — the jail whose lock should be cleared
    //   output: none
    //   sideEffects: releases all locks for the jail's session; sets state to Primary{0}
    //                so next tick detects the loss
    // simulate_lock_loss:end
    #[cfg(test)]
    pub fn simulate_lock_loss(&self, jail: &CouplingJail) {
        // Find the session ID this loop opened for the jail.
        let sid = {
            let st = self.state.lock().unwrap_or_else(|e| e.into_inner());
            st.get(&jail.name).map(|(s, _)| *s)
        };

        if let Some(sid) = sid {
            // Release all locks held by this session (same as session-expiry cascade).
            let _ = lock::release_all_for_session(&self.locks, sid);

            // Leave internal state as Primary so the next tick enters the Primary arm
            // and detects the lock is gone via is_held_by → false.
            let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
            st.insert(jail.name.clone(), (sid, JailState::Primary { fence: 0 }));
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        consensus::{LocalLog, LocalStores},
        jailspec::JailRole,
        kv::KvStore,
        queue::QueueStore,
    };

    // ── helpers ───────────────────────────────────────────────────────────────

    fn make_stores() -> (Arc<dyn ReplicatedLog>, LockStore, SvcStore, SessionStore) {
        let sessions = SessionStore::new();
        let locks    = LockStore::new();
        let kv       = KvStore::new();
        let queues   = QueueStore::new();
        let svcs     = SvcStore::new();

        let log: Arc<dyn ReplicatedLog> = Arc::new(LocalLog::new(LocalStores {
            sessions: sessions.clone(),
            locks:    locks.clone(),
            kv:       kv.clone(),
            queues:   queues.clone(),
            svcs:     svcs.clone(),
        }));

        (log, locks, svcs, sessions)
    }

    fn singleton_jail(name: &str, svc: &str) -> CouplingJail {
        CouplingJail {
            name:       name.to_string(),
            role:       JailRole::Singleton,
            svc:        svc.to_string(),
            dataset:    String::new(),
            on_promote: "promote-hook".to_string(),
            on_demote:  "demote-hook".to_string(),
        }
    }

    fn make_loop(
        jails:   Vec<CouplingJail>,
        node_id: NodeId,
        log:     Arc<dyn ReplicatedLog>,
        locks:   LockStore,
        svcs:    SvcStore,
        sessions: SessionStore,
        mgr:     Arc<dyn JailManager>,
    ) -> ReconcileLoop {
        ReconcileLoop::new(jails, node_id, 30_000, log, locks, svcs, sessions, mgr)
    }

    // ── election: one winner, rest standby ────────────────────────────────────

    // singleton_only_one_starts:start
    //   purpose: Two reconcile-loops competing for the same singleton jail: only the
    //            first to tick starts the jail; the second remains standby.
    //            Verifies SPEC §13C: lock:<svc> exclusive → one winner, no sidecars.
    //   input:  two ReconcileLoops with same jails list, separate JailManagers, shared stores
    //   output: exactly one mgr has Start event; the other has none
    //   sideEffects: LockStore holds one exclusive holder for lock:<svc>
    // singleton_only_one_starts:end
    #[test]
    fn singleton_only_one_starts() {
        let jail = singleton_jail("pg-matrix-jail", "pg-matrix");
        let (log, locks, svcs, sessions) = make_stores();

        let mgr_a = Arc::new(MemJailManager::new());
        let mgr_b = Arc::new(MemJailManager::new());

        let loop_a = make_loop(
            vec![jail.clone()], 1, Arc::clone(&log),
            locks.clone(), svcs.clone(), sessions.clone(), Arc::clone(&mgr_a) as Arc<dyn JailManager>,
        );
        let loop_b = make_loop(
            vec![jail.clone()], 2, Arc::clone(&log),
            locks.clone(), svcs.clone(), sessions.clone(), Arc::clone(&mgr_b) as Arc<dyn JailManager>,
        );

        // First tick: loop_a wins the election.
        let errs_a = loop_a.tick();
        assert!(errs_a.is_empty(), "loop_a tick must succeed: {:?}", errs_a);

        // Second tick: loop_b tries to acquire — lock is already held by loop_a.
        let errs_b = loop_b.tick();
        assert!(errs_b.is_empty(), "loop_b tick must succeed (standby is not an error): {:?}", errs_b);

        // Exactly one Start event total: only the winner starts.
        assert!(mgr_a.is_running("pg-matrix-jail"),  "loop_a must have started the jail");
        assert!(!mgr_b.is_running("pg-matrix-jail"), "loop_b must remain standby");

        // on_promote hook called for the winner, NOT the loser.
        let events_a = mgr_a.all_events();
        assert!(
            events_a.iter().any(|e| matches!(e, JailEvent::Hook { cmd, .. } if cmd == "promote-hook")),
            "winner must have received the on_promote hook: {:?}", events_a
        );
        let events_b = mgr_b.all_events();
        assert!(
            !events_b.iter().any(|e| matches!(e, JailEvent::Hook { .. })),
            "standby must NOT receive any hook: {:?}", events_b
        );
    }

    // ── failover: lock released → standby wins next tick ─────────────────────

    // singleton_failover_on_lock_loss:start
    //   purpose: Simulate the primary losing its lock (session expiry cascade);
    //            on the next tick loop_b detects the lock is free and wins.
    //            Verifies §13C failover: JailManager.stop + on_demote on primary;
    //            JailManager.start + on_promote on new winner.
    //   input:  loop_a holds the lock initially; simulate_lock_loss() clears it;
    //           loop_a ticks again (detects loss → stop + demote); loop_b ticks
    //           (wins election → start + promote).
    //   output: loop_a stopped; loop_b started; hooks called in correct order
    //   sideEffects: LockStore transitions from loop_a session to loop_b session
    // singleton_failover_on_lock_loss:end
    #[test]
    fn singleton_failover_on_lock_loss() {
        let jail = singleton_jail("pg-matrix-jail", "pg-matrix");
        let (log, locks, svcs, sessions) = make_stores();

        let mgr_a = Arc::new(MemJailManager::new());
        let mgr_b = Arc::new(MemJailManager::new());

        let loop_a = make_loop(
            vec![jail.clone()], 1, Arc::clone(&log),
            locks.clone(), svcs.clone(), sessions.clone(), Arc::clone(&mgr_a) as Arc<dyn JailManager>,
        );
        let loop_b = make_loop(
            vec![jail.clone()], 2, Arc::clone(&log),
            locks.clone(), svcs.clone(), sessions.clone(), Arc::clone(&mgr_b) as Arc<dyn JailManager>,
        );

        // Step 1: loop_a wins the election.
        let e = loop_a.tick(); assert!(e.is_empty(), "{:?}", e);
        assert!(mgr_a.is_running("pg-matrix-jail"), "loop_a must hold primary");

        // Step 2: loop_b tries and remains standby.
        let e = loop_b.tick(); assert!(e.is_empty(), "{:?}", e);
        assert!(!mgr_b.is_running("pg-matrix-jail"), "loop_b must be standby");

        // Step 3: Simulate lock loss for loop_a (session expiry / cascade).
        loop_a.simulate_lock_loss(&jail);

        // Step 4: loop_a ticks — detects lock gone → stop + on_demote.
        let e = loop_a.tick(); assert!(e.is_empty(), "{:?}", e);
        assert!(!mgr_a.is_running("pg-matrix-jail"), "loop_a must have stopped");
        let events_a = mgr_a.all_events();
        assert!(
            events_a.iter().any(|e| matches!(e, JailEvent::Stop(_))),
            "loop_a must have received Stop: {:?}", events_a
        );
        assert!(
            events_a.iter().any(|e| matches!(e, JailEvent::Hook { cmd, .. } if cmd == "demote-hook")),
            "loop_a must have received on_demote hook: {:?}", events_a
        );

        // Step 5: loop_b ticks — lock now free → wins → start + on_promote.
        let e = loop_b.tick(); assert!(e.is_empty(), "{:?}", e);
        assert!(mgr_b.is_running("pg-matrix-jail"), "loop_b must now be primary");
        let events_b = mgr_b.all_events();
        assert!(
            events_b.iter().any(|e| matches!(e, JailEvent::Start(_))),
            "loop_b must have received Start: {:?}", events_b
        );
        assert!(
            events_b.iter().any(|e| matches!(e, JailEvent::Hook { cmd, .. } if cmd == "promote-hook")),
            "loop_b must have received on_promote hook: {:?}", events_b
        );
    }

    // ── on_promote called exactly once on first win ───────────────────────────

    // promote_hook_called_once:start
    //   purpose: on_promote is called exactly once when a singleton jail first wins.
    //            Subsequent ticks (keepalive path) must not call it again.
    //   input:  one loop, one jail; two ticks
    //   output: exactly one Hook { cmd: "promote-hook" } event in mgr events
    //   sideEffects: two ticks; only first changes state
    // promote_hook_called_once:end
    #[test]
    fn promote_hook_called_once() {
        let jail = singleton_jail("pg-matrix-jail", "pg-matrix");
        let (log, locks, svcs, sessions) = make_stores();
        let mgr = Arc::new(MemJailManager::new());

        let lp = make_loop(
            vec![jail.clone()], 1, Arc::clone(&log),
            locks, svcs, sessions, Arc::clone(&mgr) as Arc<dyn JailManager>,
        );

        let e = lp.tick(); assert!(e.is_empty(), "{:?}", e);
        let e = lp.tick(); assert!(e.is_empty(), "{:?}", e); // keepalive tick

        let promote_count = mgr.all_events().iter()
            .filter(|e| matches!(e, JailEvent::Hook { cmd, .. } if cmd == "promote-hook"))
            .count();
        assert_eq!(promote_count, 1, "on_promote must be called exactly once; got {promote_count}");
    }

    // ── worker role: always starts, no lock ──────────────────────────────────

    // worker_always_starts:start
    //   purpose: role=worker jail starts on first tick without competing for any lock.
    //            Two loops managing the same worker both start it (no contention).
    //   input:  two loops with role=worker jail; shared stores
    //   output: both mgr_a and mgr_b have Start events; LockStore is empty
    //   sideEffects: both managers have the jail marked running
    // worker_always_starts:end
    #[test]
    fn worker_always_starts() {
        let jail = CouplingJail {
            name:       "synapse-worker".to_string(),
            role:       JailRole::Worker,
            svc:        "synapse".to_string(),
            dataset:    String::new(),
            on_promote: String::new(),
            on_demote:  String::new(),
        };

        let (log, locks, svcs, sessions) = make_stores();
        let mgr_a = Arc::new(MemJailManager::new());
        let mgr_b = Arc::new(MemJailManager::new());

        let loop_a = make_loop(
            vec![jail.clone()], 1, Arc::clone(&log),
            locks.clone(), svcs.clone(), sessions.clone(), Arc::clone(&mgr_a) as Arc<dyn JailManager>,
        );
        let loop_b = make_loop(
            vec![jail.clone()], 2, Arc::clone(&log),
            locks.clone(), svcs.clone(), sessions.clone(), Arc::clone(&mgr_b) as Arc<dyn JailManager>,
        );

        let e = loop_a.tick(); assert!(e.is_empty(), "{:?}", e);
        let e = loop_b.tick(); assert!(e.is_empty(), "{:?}", e);

        assert!(mgr_a.is_running("synapse-worker"), "worker must be running on loop_a");
        assert!(mgr_b.is_running("synapse-worker"), "worker must be running on loop_b");

        // No lock entries created for worker jails.
        let lock_key = "lock:synapse";
        assert!(
            lock::get_fence(&locks, lock_key).is_err(),
            "worker must not create lock entries"
        );
    }
}
