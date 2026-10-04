// START_AI_HEADER
// MODULE: matrix-hs/src/requery_backoff.rs
// PURPOSE: Backoff for the mid-life re-query loop (binary's main.rs) so that a peer
//          whose PDUs are systematically rejected (TOFU key mismatch, bad signature)
//          does not get a full-history re-query every MATRIX_HS_CATCHUP_INTERVAL_SECS
//          forever. Each catch-up pass materialises every room's whole history on both
//          sides (main.rs catchup_pass + the history queryable), so a peer that can
//          never be applied is paid for in memory and disk churn for nothing.
//
//          Two pieces, both pure so they can be tested without a cluster:
//            CatchupStats   — per-pass counters (applied / rejected PDUs).
//            RequeryBackoff — doubles the wait while a pass applies nothing and
//                             rejects something, resets to the base interval on any
//                             applied progress, and caps at base × MAX_MULTIPLE.
//          A "peer appeared" event still forces an immediate pass: a healed partition
//          is exactly when the backoff must not hold us back — but the backoff then
//          keeps the FOLLOWING passes from repeating at full rate.
//
// DEPENDENCIES: none (std only)
// END_AI_HEADER

/// Per-pass catch-up counters, filled in by the catch-up path and consumed by the
/// backoff. Reset before every pass so each decision sees one pass only.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CatchupStats {
    /// PDUs that verification accepted and that were new to us.
    pub applied: usize,
    /// PDUs refused by signature/sender-binding verification.
    pub rejected: usize,
}

impl CatchupStats {
    pub fn reset(&mut self) {
        *self = CatchupStats::default();
    }

    /// A pass that rejected something and applied nothing is the "systematic reject"
    /// signal: the peer is answering, we just cannot use a single event it sends.
    pub fn is_systematic_reject(&self) -> bool {
        self.rejected > 0 && self.applied == 0
    }
}

/// Every Nth pass asks for everything, whatever the dirty set says.
///
/// The safety net for the one thing a targeted pass cannot do: discover a room this
/// node has never heard of. A peer that came up while we were partitioned is only
/// found by a wildcard, so discovery is bought at 1/CATCHUP_FULL_EVERY of the cost
/// instead of on every pass.
pub const CATCHUP_FULL_EVERY: u32 = 12;

/// Per-room fingerprint: how much the room holds. A change in any of the three means
/// the room can answer differently than it did last pass, so it is worth asking about.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RoomFingerprint {
    pub events: usize,
    pub state: usize,
    pub timeline: usize,
}

/// What one catch-up pass is allowed to ask for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PassScope {
    /// Wildcard on both channels. Used when a peer appeared, when the slow backstop
    /// timer is due, and every `CATCHUP_FULL_EVERY` pass.
    Full,
    /// Ask only these rooms, by name.
    Targeted(Vec<String>),
    /// Nothing changed since the last completed pass and this is not a backstop pass:
    /// do not issue a query at all.
    Skip,
}

impl PassScope {
    /// Short name for the sweep-alloc line, so a report says which kind of pass it was.
    pub fn label(&self) -> &'static str {
        match self {
            PassScope::Full => "full",
            PassScope::Targeted(_) => "targeted",
            PassScope::Skip => "skip",
        }
    }
}

/// Decide the scope of one pass. Pure: no clock, no I/O, no locks.
///
/// `pass_index` is 1-based (it counts passes), so every `CATCHUP_FULL_EVERY`-th pass
/// is a full one: pass 12, 24, … rather than pass 1, 13, …
pub fn decide_scope(
    peer_grew: bool,
    backstop_due: bool,
    pass_index: u32,
    dirty: &[String],
) -> PassScope {
    if peer_grew || backstop_due || pass_index % CATCHUP_FULL_EVERY == 0 {
        return PassScope::Full;
    }
    if dirty.is_empty() {
        return PassScope::Skip;
    }
    PassScope::Targeted(dirty.to_vec())
}

/// The keys one pass sends on one channel. A `Full` scope is the bare wildcard;
/// `Targeted` names rooms and therefore contains no `*`; `Skip` asks nothing.
pub fn query_keys(prefix: &str, leaf: &str, scope: &PassScope) -> Vec<String> {
    match scope {
        PassScope::Full => vec![format!("{prefix}/*/{leaf}")],
        PassScope::Targeted(rooms) => {
            rooms.iter().map(|r| format!("{prefix}/{r}/{leaf}")).collect()
        }
        PassScope::Skip => Vec::new(),
    }
}

/// Remembers the last observed fingerprint per room and reports which rooms differ.
#[derive(Debug, Default)]
pub struct DirtyTracker {
    seen: std::collections::HashMap<String, RoomFingerprint>,
}

impl DirtyTracker {
    /// Rooms whose fingerprint moved since the previous call, sorted for a stable
    /// report. The first call reports every room: with nothing observed yet, every
    /// room is potentially new to us.
    pub fn update(
        &mut self,
        observed: impl IntoIterator<Item = (String, RoomFingerprint)>,
    ) -> Vec<String> {
        let mut dirty = Vec::new();
        for (room, fp) in observed {
            match self.seen.get(&room) {
                Some(prev) if *prev == fp => {}
                _ => dirty.push(room.clone()),
            }
            self.seen.insert(room, fp);
        }
        dirty.sort();
        dirty
    }

    /// Forget a room (it is gone); it will be reported dirty if it ever comes back.
    pub fn forget(&mut self, room: &str) {
        self.seen.remove(room);
    }

    /// Rooms currently tracked.
    pub fn tracked(&self) -> usize {
        self.seen.len()
    }
}

/// Ceiling for the backoff, as a multiple of the configured base interval.
pub const MAX_MULTIPLE: u64 = 8;

/// Delay schedule for the mid-life re-query loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequeryBackoff {
    base_secs: u64,
    current_secs: u64,
}

impl RequeryBackoff {
    /// `base_secs` is MATRIX_HS_CATCHUP_INTERVAL_SECS (the old fixed cadence).
    /// A zero base is clamped to 1 s so `delay_secs()` never returns 0 and the loop
    /// can never spin.
    pub fn new(base_secs: u64) -> Self {
        let base = base_secs.max(1);
        RequeryBackoff { base_secs: base, current_secs: base }
    }

    /// Feed the outcome of one finished pass. Any applied progress resets the delay;
    /// a systematic reject doubles it up to the ceiling.
    pub fn note_pass(&mut self, stats: CatchupStats) {
        if stats.is_systematic_reject() {
            let max_secs = self.base_secs.saturating_mul(MAX_MULTIPLE);
            self.current_secs = self.current_secs.saturating_mul(2).min(max_secs).max(self.base_secs);
        } else {
            self.current_secs = self.base_secs;
        }
    }

    /// Seconds the loop must wait before the next backstop-triggered pass.
    pub fn delay_secs(&self) -> u64 {
        self.current_secs
    }
}

// requery_backoff_test:start
#[cfg(test)]
mod tests {
    use super::*;

    fn reject_pass() -> CatchupStats {
        CatchupStats { applied: 0, rejected: 3 }
    }

    #[test]
    fn systematic_rejects_back_off_and_cap() {
        let mut b = RequeryBackoff::new(300);
        assert_eq!(b.delay_secs(), 300, "base interval is the first delay");
        let mut seen = vec![b.delay_secs()];
        for _ in 0..8 {
            b.note_pass(reject_pass());
            seen.push(b.delay_secs());
        }
        assert_eq!(
            seen,
            vec![300, 600, 1200, 2400, 2400, 2400, 2400, 2400, 2400],
            "delay must double per rejected pass and stop at base × {MAX_MULTIPLE}"
        );
    }

    #[test]
    fn applied_progress_resets_to_base() {
        let mut b = RequeryBackoff::new(300);
        b.note_pass(reject_pass());
        b.note_pass(reject_pass());
        assert_eq!(b.delay_secs(), 1200);
        b.note_pass(CatchupStats { applied: 7, rejected: 3 });
        assert_eq!(b.delay_secs(), 300, "any applied progress resets the delay");
    }

    #[test]
    fn a_quiet_pass_does_not_back_off() {
        let mut b = RequeryBackoff::new(300);
        b.note_pass(CatchupStats { applied: 0, rejected: 0 });
        assert_eq!(b.delay_secs(), 300, "no rejects is not a systematic reject");
        assert!(!CatchupStats { applied: 0, rejected: 0 }.is_systematic_reject());
    }

    #[test]
    fn zero_base_is_clamped_so_the_loop_cannot_spin() {
        let b = RequeryBackoff::new(0);
        assert_eq!(b.delay_secs(), 1);
    }

    #[test]
    fn hourly_window_has_fewer_passes_than_the_flat_cadence() {
        // Simulate one hour of backstop-triggered passes under a peer that rejects
        // everything. Flat cadence: 3600 / 300 = 12 passes. With the backoff the
        // count must drop well below that — this is the acceptance property.
        let window_secs = 3600u64;
        let base = 300u64;

        // Flat cadence = the behaviour before this change: a fixed base interval.
        let mut backed = RequeryBackoff::new(base);
        let (mut t_flat, mut t_back) = (0u64, 0u64);
        let (mut n_flat, mut n_back) = (0u32, 0u32);
        while t_flat < window_secs {
            n_flat += 1;
            t_flat += base;
        }
        while t_back < window_secs {
            n_back += 1;
            t_back += backed.delay_secs();
            backed.note_pass(reject_pass());
        }
        assert_eq!(n_flat, 12, "flat cadence baseline over the hour");
        assert!(
            n_back <= 6,
            "backoff must cut passes in an hour: got {n_back} (flat {n_flat})"
        );
    }

    // ── Explicit-room passes (level 1 of the catch-up fix) ────────────────────

    // empty_dirty_set_asks_nothing:start
    //   purpose: The whole point of the dirty set — a converged node with nothing to
    //            ask about must issue ZERO queries, not a wildcard that comes back
    //            empty. Pins it on the decision AND on the keys, because a decision
    //            that still produced a key would cost the same bytes as before.
    //   input:  none
    //   output: ()
    //   sideEffects: none
    // empty_dirty_set_asks_nothing:end
    #[test]
    fn empty_dirty_set_asks_nothing() {
        let scope = decide_scope(false, false, 5, &[]);
        assert_eq!(scope, PassScope::Skip, "nothing dirty, not a backstop pass → ask nothing");
        assert_eq!(scope.label(), "skip");
        for leaf in ["history", "state"] {
            assert!(
                query_keys("p", leaf, &scope).is_empty(),
                "{leaf}: a skip must produce no keys at all"
            );
        }
    }

    // targeted_pass_names_rooms_without_wildcard:start
    //   purpose: A targeted pass must ask per room and must NOT contain a wildcard:
    //            the responder already answers a concrete room with that room only
    //            (state.rs rooms_for_query), so a `*` here would undo the whole fix.
    //   input:  none
    //   output: ()
    //   sideEffects: none
    // targeted_pass_names_rooms_without_wildcard:end
    #[test]
    fn targeted_pass_names_rooms_without_wildcard() {
        let rooms = vec!["!one:localhost".to_string(), "!two:localhost".to_string()];
        let scope = decide_scope(false, false, 3, &rooms);
        assert_eq!(
            scope,
            PassScope::Targeted(rooms.clone()),
            "dirty rooms present, no peer growth, not a backstop pass → targeted"
        );
        assert_eq!(scope.label(), "targeted");
        let hist = query_keys("mrgd/matrix/room", "history", &scope);
        assert_eq!(
            hist,
            vec![
                "mrgd/matrix/room/!one:localhost/history",
                "mrgd/matrix/room/!two:localhost/history",
            ]
        );
        for key in &hist {
            assert!(!key.contains('*'), "targeted key must not be a wildcard: {key}");
        }
        assert_eq!(query_keys("mrgd/matrix/room", "state", &scope).len(), 2);
    }

    // every_twelfth_pass_is_full:start
    //   purpose: The safety net. Only a full pass can discover a room this node has
    //            never heard of, so every CATCHUP_FULL_EVERY-th pass must ask for
    //            everything regardless of the dirty set — with the pass counter
    //            1-based, that is pass 12 and not pass 1.
    //   input:  none
    //   output: ()
    //   sideEffects: none
    // every_twelfth_pass_is_full:end
    #[test]
    fn every_twelfth_pass_is_full() {
        let full_at: Vec<u32> = (1..=(CATCHUP_FULL_EVERY * 3))
            .filter(|n| decide_scope(false, false, *n, &[]) == PassScope::Full)
            .collect();
        assert_eq!(
            full_at,
            vec![CATCHUP_FULL_EVERY, CATCHUP_FULL_EVERY * 2, CATCHUP_FULL_EVERY * 3],
            "every 12th pass is full; pass 1 must not be"
        );
        assert_eq!(
            query_keys("p", "history", &PassScope::Full),
            vec!["p/*/history"],
            "a full pass is exactly the bare wildcard"
        );
        // Peer growth and a due backstop both force a full pass, whatever the dirty set.
        assert_eq!(decide_scope(true, false, 7, &[]), PassScope::Full, "peer appeared");
        assert_eq!(decide_scope(false, true, 7, &[]), PassScope::Full, "backstop due");
    }

    // dirty_tracker_reports_moved_rooms:start
    //   purpose: The dirty predicate itself — a room is dirty when its fingerprint moved
    //            since the previous completed pass, and the FIRST observation of a room
    //            is always dirty (we know nothing about it yet).
    //   input:  none
    //   output: ()
    //   sideEffects: none
    // dirty_tracker_reports_moved_rooms:end
    #[test]
    fn dirty_tracker_reports_moved_rooms() {
        let fp = |e, s, t| RoomFingerprint { events: e, state: s, timeline: t };
        let mut t = DirtyTracker::default();
        let first = t.update(vec![
            ("!a:localhost".to_string(), fp(5, 1, 5)),
            ("!b:localhost".to_string(), fp(5, 1, 5)),
        ]);
        assert_eq!(first, vec!["!a:localhost".to_string(), "!b:localhost".to_string()], "first observation is all dirty");
        let quiet = t.update(vec![
            ("!a:localhost".to_string(), fp(5, 1, 5)),
            ("!b:localhost".to_string(), fp(5, 1, 5)),
        ]);
        assert!(quiet.is_empty(), "nothing moved → nothing to ask about: {quiet:?}");
        let moved = t.update(vec![
            ("!a:localhost".to_string(), fp(6, 1, 6)),
            ("!b:localhost".to_string(), fp(5, 1, 5)),
        ]);
        assert_eq!(moved, vec!["!a:localhost".to_string()], "only the moved room");
        let state_moved = t.update(vec![("!a:localhost".to_string(), fp(6, 2, 6))]);
        assert_eq!(state_moved, vec!["!a:localhost".to_string()], "a state-only change counts");
        assert_eq!(t.tracked(), 2);
        t.forget("!b:localhost");
        assert_eq!(t.tracked(), 1);
    }

}
// requery_backoff_test:end