// START_AI_HEADER
// MODULE: couplingd/src/watchdog.rs
// PURPOSE: Self-fence watchdog — SPEC_coupling_v1 §2.3, milestone M1.5.
//          A node that cannot confirm cluster membership (via Zenoh liveliness in
//          production, or via injected time in tests) for longer than T ms must
//          assume it is network-partitioned and kill *all* its coupling=on jails
//          immediately (fail-stop, o2cb-style OCFS2 fencing).  It must NOT restart
//          them autonomously — that is the responsibility of the reconcile-loop once
//          membership is re-confirmed.
//
//          Clock and membership are both injected for determinism — no wall-clock
//          references in logic paths (same pattern as session.rs / reconcile.rs).
//
//          The Zenoh liveliness backend is wired in under the `cluster` feature flag
//          to keep the default host-test build free of the zenoh dependency graph.
//
// INTENT: M1.5 — self-fence on membership timeout.  Compiles and all tests pass on
//         host (Linux/macOS) with `cargo test -p couplingd`.  FreeBSD jail ops go
//         through the same JailManager seam as reconcile.rs — no OS calls here.
// DEPENDENCIES: std, crate::{jailspec::CouplingJail, reconcile::{JailManager, JailError}}
// PUBLIC_API: MembershipMonitor, MemMembership, SelfFenceWatchdog, FenceEvent
// END_AI_HEADER

use std::sync::{Arc, Mutex};

use crate::{
    jailspec::CouplingJail,
    reconcile::{JailError, JailManager},
};

// ── MembershipMonitor ─────────────────────────────────────────────────────────

// MembershipMonitor:start
//   purpose: Abstract source of the last confirmed cluster-membership timestamp.
//            In production (feature `cluster`) the implementation reads the most
//            recent Zenoh liveliness heartbeat from the local subscriber.
//            In tests `MemMembership` accepts an injected timestamp so determinism
//            is guaranteed without sleeping or touching wall-clock.
//   input:  none per call — implementations maintain internal state
//   output: last_confirmed_ms — millisecond timestamp of the last confirmed membership
//   sideEffects: implementations may hold a mutex or read from Zenoh subscriber state
// MembershipMonitor:end
pub trait MembershipMonitor: Send + Sync {
    /// Return the millisecond timestamp at which membership was last positively
    /// confirmed (e.g. last liveliness token received from Zenoh).
    fn last_confirmed_ms(&self) -> u64;
}

// ── MemMembership — injectable in-process implementation ──────────────────────

// MemMembership:start
//   purpose: Test-friendly MembershipMonitor that stores last-confirmed-ms in an
//            Arc<Mutex<u64>> so tests can advance it arbitrarily.
//            NOT safe for wall-clock production use — production wires a ZenohMembership
//            under the `cluster` feature.
//   input:  initial_ms — starting value for last_confirmed_ms
//   output: MemMembership ready for injection into SelfFenceWatchdog
//   sideEffects: allocates one Arc<Mutex<u64>>
// MemMembership:end
#[derive(Clone, Default)]
pub struct MemMembership {
    last_ms: Arc<Mutex<u64>>,
}

impl MemMembership {
    // new:start
    //   purpose: Construct a MemMembership with a given initial timestamp.
    //   input:  initial_ms — starting value for last_confirmed_ms
    //   output: MemMembership
    //   sideEffects: allocates one Arc<Mutex<u64>>
    // new:end
    pub fn new(initial_ms: u64) -> Self {
        Self { last_ms: Arc::new(Mutex::new(initial_ms)) }
    }

    // set:start
    //   purpose: Advance (or retreat) the confirmed-membership timestamp.
    //            Call from tests to simulate a fresh Zenoh heartbeat or a gap.
    //   input:  ms — new last-confirmed timestamp in milliseconds
    //   output: ()
    //   sideEffects: overwrites internal mutex value
    // set:end
    pub fn set(&self, ms: u64) {
        *self.last_ms.lock().unwrap_or_else(|e| e.into_inner()) = ms;
    }
}

impl MembershipMonitor for MemMembership {
    // last_confirmed_ms:start
    //   purpose: Return the injected last-confirmed timestamp.
    //   input:  none
    //   output: u64 millisecond timestamp
    //   sideEffects: acquires and immediately releases mutex
    // last_confirmed_ms:end
    fn last_confirmed_ms(&self) -> u64 {
        *self.last_ms.lock().unwrap_or_else(|e| e.into_inner())
    }
}

// ── FenceEvent — observable outcome of a tick ─────────────────────────────────

/// Record of what the watchdog did on a single tick.  Used in tests for assertions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FenceEvent {
    /// Membership was fresh; no action taken.
    Ok,
    /// Membership timed out; the listed jail names were stopped (self-fence fired).
    Fenced(Vec<String>),
    /// Already fenced in a prior tick; membership still absent; no repeat stop.
    StillFenced,
    /// Fenced before but membership was re-confirmed; watchdog is now active again.
    /// (Jails are NOT restarted here — that is reconcile-loop's job.)
    Recovered,
}

// ── SelfFenceWatchdog ─────────────────────────────────────────────────────────

// SelfFenceWatchdog:start
//   purpose: Run one watchdog tick per call to tick(now_ms).  Compares now_ms against
//            the last membership confirmation from the MembershipMonitor; if the gap
//            exceeds t_ms it stops all coupling=on jails via JailManager.stop() and
//            records them as fenced.  Subsequent ticks while still partitioned do NOT
//            call stop() again (idempotent; avoids log-spam).  When membership is
//            re-confirmed (gap <= t_ms) the fenced flag is cleared so the next timeout
//            will re-fence if needed.  Jails are NOT auto-restarted — the reconcile-loop
//            is responsible for that once membership is healthy.
//
//            Clock injection: now_ms is passed in by the caller — no SystemTime here.
//            This keeps tests deterministic and fast (no sleep).
//
//            T parameter: the spec recommends 2–3× lease TTL.  Pass that multiple
//            directly as t_ms when constructing (e.g. 3 * session_ttl_ms).
//
//   input:  monitor — MembershipMonitor impl; mgr — JailManager (MemJailManager in tests);
//           jails — Vec<CouplingJail> to protect; t_ms — membership timeout threshold in ms
//   output: drives JailManager.stop() side-effects; returns FenceEvent per tick
//   sideEffects: calls mgr.stop(jail_name) for each coupling=on jail on first timeout;
//                records fenced state in internal Mutex
// SelfFenceWatchdog:end
pub struct SelfFenceWatchdog {
    monitor: Arc<dyn MembershipMonitor>,
    mgr:     Arc<dyn JailManager>,
    jails:   Vec<CouplingJail>,
    t_ms:    u64,
    /// Internal state: Some(Vec<String>) = fenced (these jail names were stopped);
    ///                 None               = watchdog is active (not fenced).
    fenced:  Arc<Mutex<Option<Vec<String>>>>,
}

impl SelfFenceWatchdog {
    // new:start
    //   purpose: Construct a SelfFenceWatchdog.  No OS calls; all side-effects
    //            are deferred to tick().
    //   input:  monitor — membership source; mgr — jail lifecycle backend;
    //           jails — descriptors of coupling=on jails this node runs;
    //           t_ms — membership gap threshold (recommend 2–3× lease TTL)
    //   output: SelfFenceWatchdog
    //   sideEffects: allocates one Arc<Mutex<Option<Vec<String>>>>
    // new:end
    pub fn new(
        monitor: Arc<dyn MembershipMonitor>,
        mgr:     Arc<dyn JailManager>,
        jails:   Vec<CouplingJail>,
        t_ms:    u64,
    ) -> Self {
        Self {
            monitor,
            mgr,
            jails,
            t_ms,
            fenced: Arc::new(Mutex::new(None)),
        }
    }

    // tick:start
    //   purpose: Evaluate membership freshness and apply self-fence if timed out.
    //            Decision table:
    //              gap <= t_ms, not fenced  → FenceEvent::Ok
    //              gap <= t_ms, was fenced  → FenceEvent::Recovered (clear fenced state)
    //              gap >  t_ms, not fenced  → stop all jails → FenceEvent::Fenced(names)
    //              gap >  t_ms, was fenced  → FenceEvent::StillFenced (no-op; idempotent)
    //            Errors from mgr.stop() are logged to stderr but do not abort; the fence
    //            state is still recorded (fail-stop: we tried; the process should be
    //            considered unsafe regardless).
    //   input:  now_ms — current time in milliseconds (injected by caller)
    //   output: FenceEvent describing what happened this tick
    //   sideEffects: calls mgr.stop() for each jail on the first timeout tick only;
    //                mutates internal fenced state
    // tick:end
    pub fn tick(&self, now_ms: u64) -> FenceEvent {
        let last = self.monitor.last_confirmed_ms();
        // Saturating subtraction: if last > now_ms (clock skew / test) gap = 0.
        let gap = now_ms.saturating_sub(last);
        let timed_out = gap > self.t_ms;

        let mut fenced = self.fenced.lock().unwrap_or_else(|e| e.into_inner());

        match (timed_out, fenced.is_some()) {
            // Healthy and was previously fenced → recovered (jails stay stopped; reconcile
            // is responsible for restarting them).
            (false, true) => {
                *fenced = None;
                FenceEvent::Recovered
            }

            // Healthy, never fenced → normal operation.
            (false, false) => FenceEvent::Ok,

            // Timed out but already fenced on a previous tick → idempotent no-op.
            (true, true) => FenceEvent::StillFenced,

            // Timed out and NOT yet fenced → fire self-fence.
            (true, false) => {
                let mut stopped: Vec<String> = Vec::new();
                for jail in &self.jails {
                    match self.mgr.stop(&jail.name) {
                        Ok(()) => {
                            stopped.push(jail.name.clone());
                        }
                        Err(JailError::StopFailed(name, reason)) => {
                            // Log but do not abort — the node is considered fenced
                            // even if an individual stop call fails.
                            eprintln!(
                                "self-fence: stop({name}) failed: {reason}; \
                                 node is still considered fenced"
                            );
                            stopped.push(name);
                        }
                        Err(other) => {
                            eprintln!(
                                "self-fence: stop({}) unexpected error: {}; \
                                 node is still considered fenced",
                                jail.name, other
                            );
                            stopped.push(jail.name.clone());
                        }
                    }
                }
                eprintln!(
                    "self-fence: membership timeout (gap={}ms > t={}ms), \
                     killed coupled jails: {:?}",
                    gap, self.t_ms, stopped
                );
                *fenced = Some(stopped.clone());
                FenceEvent::Fenced(stopped)
            }
        }
    }

    // is_fenced:start
    //   purpose: Report whether the watchdog is currently in the fenced state.
    //            Useful for callers that want to gate further actions (e.g. refuse
    //            new client connections, reject API calls) while partitioned.
    //   input:  none
    //   output: true if node is currently self-fenced; false if membership is healthy
    //   sideEffects: acquires and releases mutex
    // is_fenced:end
    pub fn is_fenced(&self) -> bool {
        self.fenced.lock().unwrap_or_else(|e| e.into_inner()).is_some()
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reconcile::{JailEvent, MemJailManager};

    // ── helpers ───────────────────────────────────────────────────────────────

    fn coupled_jail(name: &str) -> CouplingJail {
        use crate::jailspec::JailRole;
        CouplingJail {
            name:       name.to_string(),
            role:       JailRole::Singleton,
            svc:        name.to_string(),
            dataset:    String::new(),
            on_promote: String::new(),
            on_demote:  String::new(),
        }
    }

    fn make_watchdog(
        jails:   Vec<CouplingJail>,
        t_ms:    u64,
        last_ms: u64,
    ) -> (SelfFenceWatchdog, Arc<MemMembership>, Arc<MemJailManager>) {
        let monitor = Arc::new(MemMembership::new(last_ms));
        let mgr     = Arc::new(MemJailManager::new());
        let wd = SelfFenceWatchdog::new(
            Arc::clone(&monitor) as Arc<dyn MembershipMonitor>,
            Arc::clone(&mgr)     as Arc<dyn JailManager>,
            jails,
            t_ms,
        );
        (wd, monitor, mgr)
    }

    // ── fresh membership: no fence ────────────────────────────────────────────

    // fresh_membership_no_fence:start
    //   purpose: When now_ms - last_confirmed_ms <= t_ms, tick() must return Ok
    //            and must not call mgr.stop() on any jail.
    //            Verifies §2.3 guard condition: "only fence if membership timed out".
    //   input:  last_ms = 1000, now_ms = 1500, t_ms = 1000 (gap 500 < 1000)
    //   output: FenceEvent::Ok; no JailEvent::Stop in mgr events
    //   sideEffects: none on JailManager
    // fresh_membership_no_fence:end
    #[test]
    fn fresh_membership_no_fence() {
        let jail = coupled_jail("pg-matrix-jail");
        let (wd, _monitor, mgr) = make_watchdog(vec![jail], 1000, 1000);

        let event = wd.tick(1500); // gap = 500 < t_ms=1000
        assert_eq!(event, FenceEvent::Ok, "fresh membership must not fence");
        assert!(
            !mgr.all_events().iter().any(|e| matches!(e, JailEvent::Stop(_))),
            "no Stop events expected"
        );
        assert!(!wd.is_fenced(), "watchdog must not be in fenced state");
    }

    // ── exact boundary: gap == t_ms → not yet fenced ─────────────────────────

    // boundary_equal_not_fenced:start
    //   purpose: Gap exactly equal to t_ms (not strictly greater) must NOT trigger fence.
    //            Verifies the strict inequality `gap > t_ms` used in tick().
    //   input:  last_ms = 0, now_ms = 1000, t_ms = 1000 (gap == t_ms)
    //   output: FenceEvent::Ok
    //   sideEffects: none on JailManager
    // boundary_equal_not_fenced:end
    #[test]
    fn boundary_equal_not_fenced() {
        let jail = coupled_jail("pg-matrix-jail");
        let (wd, _monitor, mgr) = make_watchdog(vec![jail], 1000, 0);

        let event = wd.tick(1000); // gap = 1000 == t_ms → NOT > t_ms
        assert_eq!(event, FenceEvent::Ok, "gap == t_ms must not trigger fence");
        assert!(
            !mgr.all_events().iter().any(|e| matches!(e, JailEvent::Stop(_))),
            "no Stop events at exact boundary"
        );
    }

    // ── timeout: gap > t_ms → all jails stopped ───────────────────────────────

    // timeout_stops_all_coupled_jails:start
    //   purpose: When now_ms - last_confirmed_ms > t_ms, tick() must call mgr.stop()
    //            for EVERY jail in the watchdog's list and return Fenced(names).
    //            Verifies §2.3: "жёстко гасим ВСЕ coupling=on jail'ы".
    //   input:  two jails; last_ms = 0, now_ms = 2001, t_ms = 2000 (gap > t_ms)
    //   output: FenceEvent::Fenced([jail-a, jail-b]); two JailEvent::Stop in mgr
    //   sideEffects: mgr.stop() called for each jail name
    // timeout_stops_all_coupled_jails:end
    #[test]
    fn timeout_stops_all_coupled_jails() {
        let jail_a = coupled_jail("jail-a");
        let jail_b = coupled_jail("jail-b");
        let (wd, _monitor, mgr) = make_watchdog(vec![jail_a, jail_b], 2000, 0);

        let event = wd.tick(2001); // gap = 2001 > t_ms = 2000
        match &event {
            FenceEvent::Fenced(names) => {
                assert!(names.contains(&"jail-a".to_string()), "jail-a must be fenced: {:?}", names);
                assert!(names.contains(&"jail-b".to_string()), "jail-b must be fenced: {:?}", names);
            }
            other => panic!("expected Fenced, got {:?}", other),
        }

        let stops: Vec<_> = mgr.all_events().into_iter()
            .filter(|e| matches!(e, JailEvent::Stop(_)))
            .collect();
        assert_eq!(stops.len(), 2, "must stop exactly 2 jails, got {}: {:?}", stops.len(), stops);
        assert!(wd.is_fenced(), "watchdog must be in fenced state after timeout");
    }

    // ── idempotent: second tick while still timed-out → no repeat stop ────────

    // second_tick_while_fenced_no_repeat_stop:start
    //   purpose: After the first timeout fires self-fence, subsequent ticks while
    //            membership is still absent must NOT call mgr.stop() again.
    //            Verifies idempotency: exactly one Stop per jail total.
    //   input:  one jail; gap > t_ms on tick 1 and tick 2 (membership unchanged)
    //   output: tick1 → Fenced; tick2 → StillFenced; total Stop events == 1
    //   sideEffects: mgr.stop() called exactly once per jail
    // second_tick_while_fenced_no_repeat_stop:end
    #[test]
    fn second_tick_while_fenced_no_repeat_stop() {
        let jail = coupled_jail("pg-matrix-jail");
        let (wd, _monitor, mgr) = make_watchdog(vec![jail], 1000, 0);

        let e1 = wd.tick(1001); // first fence
        assert!(matches!(e1, FenceEvent::Fenced(_)), "first tick must fence: {:?}", e1);

        let e2 = wd.tick(2000); // still timed out
        assert_eq!(e2, FenceEvent::StillFenced, "second tick must be StillFenced: {:?}", e2);

        let stop_count = mgr.all_events().iter()
            .filter(|e| matches!(e, JailEvent::Stop(_)))
            .count();
        assert_eq!(stop_count, 1, "stop must be called exactly once; got {stop_count}");
    }

    // ── recovery: membership re-confirmed → Recovered; jails NOT restarted ────

    // recovery_clears_fence_no_restart:start
    //   purpose: After a self-fence, if membership is re-confirmed (gap <= t_ms),
    //            the watchdog clears its fenced state and returns Recovered.
    //            Jails must NOT be restarted (that is reconcile-loop's responsibility).
    //   input:  one jail; fence on tick1 (gap > t_ms); re-confirm on tick2 (gap <= t_ms)
    //   output: tick1 → Fenced; tick2 → Recovered; no Start events in mgr
    //   sideEffects: fenced state cleared; no new JailManager calls after recovery
    // recovery_clears_fence_no_restart:end
    #[test]
    fn recovery_clears_fence_no_restart() {
        let jail = coupled_jail("pg-matrix-jail");
        let (wd, monitor, mgr) = make_watchdog(vec![jail], 1000, 0);

        // Tick 1: fence fires.
        let e1 = wd.tick(1001);
        assert!(matches!(e1, FenceEvent::Fenced(_)), "tick1 must fence: {:?}", e1);
        assert!(wd.is_fenced());

        // Membership re-confirmed.
        monitor.set(2000);

        // Tick 2: recovery.
        let e2 = wd.tick(2100); // gap = 100 < t_ms=1000
        assert_eq!(e2, FenceEvent::Recovered, "tick2 must be Recovered: {:?}", e2);
        assert!(!wd.is_fenced(), "fenced state must be cleared after recovery");

        // No Start events — watchdog must not restart jails.
        let starts: Vec<_> = mgr.all_events().into_iter()
            .filter(|e| matches!(e, JailEvent::Start(_)))
            .collect();
        assert!(starts.is_empty(), "watchdog must not restart jails; got: {:?}", starts);
    }

    // ── re-fence after recovery ───────────────────────────────────────────────

    // re_fence_after_recovery:start
    //   purpose: After recovery, a second membership timeout must trigger a fresh fence.
    //            Verifies that recovery properly resets the fenced state so the
    //            watchdog can fire again (not stuck in StillFenced forever).
    //   input:  fence (tick1) → recover (tick2) → re-fence (tick3)
    //   output: tick3 → Fenced; total Stop events == 2 (one per fence cycle)
    //   sideEffects: mgr.stop() called twice total (once per fence event)
    // re_fence_after_recovery:end
    #[test]
    fn re_fence_after_recovery() {
        let jail = coupled_jail("pg-matrix-jail");
        let (wd, monitor, mgr) = make_watchdog(vec![jail], 1000, 0);

        // Fence.
        let e1 = wd.tick(1001);
        assert!(matches!(e1, FenceEvent::Fenced(_)), "{:?}", e1);

        // Recover.
        monitor.set(2000);
        let e2 = wd.tick(2100);
        assert_eq!(e2, FenceEvent::Recovered);

        // Re-fence: membership goes stale again.
        let e3 = wd.tick(5000); // gap = 5000 - 2000 = 3000 > t_ms=1000
        assert!(matches!(e3, FenceEvent::Fenced(_)), "re-fence must fire: {:?}", e3);

        let stop_count = mgr.all_events().iter()
            .filter(|e| matches!(e, JailEvent::Stop(_)))
            .count();
        assert_eq!(stop_count, 2, "each fence cycle must produce one Stop; got {stop_count}");
    }

    // ── no jails configured: timeout with empty list is silent ───────────────

    // empty_jail_list_fence_is_silent:start
    //   purpose: If the watchdog is constructed with an empty jails list, a membership
    //            timeout still transitions to Fenced (so is_fenced() is true) but
    //            produces no Stop calls.  This covers the case where a node is fenced
    //            before any jails are registered.
    //   input:  empty jails list; gap > t_ms
    //   output: FenceEvent::Fenced([]); no JailManager events; is_fenced() == true
    //   sideEffects: none on JailManager
    // empty_jail_list_fence_is_silent:end
    #[test]
    fn empty_jail_list_fence_is_silent() {
        let (wd, _monitor, mgr) = make_watchdog(vec![], 500, 0);

        let event = wd.tick(1000); // gap = 1000 > 500
        assert_eq!(event, FenceEvent::Fenced(vec![]), "empty list must give Fenced([])");
        assert!(wd.is_fenced());
        assert!(mgr.all_events().is_empty(), "no Stop calls for empty list");
    }
}
