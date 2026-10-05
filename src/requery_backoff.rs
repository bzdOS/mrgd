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

/// Floor between two wildcard passes when no periodic interval is configured at all.
///
/// It paces only the `[skip]` line, never a pass: with
/// `MATRIX_HS_CATCHUP_INTERVAL_SECS=0` the clock trigger is off (see `full_pass_due`)
/// and a node asks for everything only when a peer appears.
pub const CATCHUP_FULL_MIN_INTERVAL_SECS: u64 = 300;

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
    /// Wildcard on both channels. Used when a peer appeared, and on the periodic
    /// discovery pass (see `full_pass_due`).
    Full,
    /// Ask only these rooms, by name.
    Targeted(Vec<String>),
    /// Nothing changed since the last completed pass: do not issue a query at all.
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
/// `full_due` is the discovery question — see `full_pass_due`. It is asked first and
/// it does not consult the dirty set: a wildcard is also how a room we have never
/// heard of gets found, so "nothing moved locally" is exactly when it still matters.
pub fn decide_scope(full_due: bool, dirty: &[String]) -> PassScope {
    if full_due {
        return PassScope::Full;
    }
    if dirty.is_empty() {
        return PassScope::Skip;
    }
    PassScope::Targeted(dirty.to_vec())
}

/// Whether this pass should ask for everything. Pure.
///
/// Two triggers. A peer appeared — immediate, nothing counted, that one predates this
/// function. And the interval elapsed since the last wildcard — a wall clock, and this
/// is the part that changed.
///
/// The rule used to be `peer_grew || (passes_since_full >= 12 && since_full >= interval)`,
/// and the conjunction is where a node stayed empty forever. `passes_since_full` only
/// grows when a pass EXECUTES, so take the order that really happens: the startup pass
/// runs before the peer's handshake lands, so it finds nothing; the loop then takes its
/// peer baseline with that peer already in it, so `peer_grew` is false from then on; and
/// with no local write there is no dirty room, so no pass executes, so the counter stays
/// at 0 and the wildcard never becomes due. The node has a peer, the peer has rooms, and
/// nothing in any log says a convergence was lost. A pass counter cannot cover every
/// order of events; a clock can, because it advances whether or not anybody passed.
///
/// `full_interval` is `MATRIX_HS_CATCHUP_INTERVAL_SECS` (default 300 s). Zero switches
/// the clock trigger off; the peer trigger stays.
pub fn full_pass_due(
    peer_grew: bool,
    since_full: std::time::Duration,
    full_interval: std::time::Duration,
) -> bool {
    if peer_grew {
        return true;
    }
    if full_interval.is_zero() {
        return false;
    }
    since_full >= full_interval
}

/// One poll of the re-query loop, as the planner sees it.
#[derive(Debug, Clone, Copy)]
pub struct PollTick<'a> {
    /// How much wall time this poll accounts for. Added to every clock exactly once,
    /// which is why the loop must not add it anywhere else.
    pub poll: std::time::Duration,
    /// A peer appeared since the previous poll.
    pub peer_grew: bool,
    /// Current backstop delay; zero means no periodic pacing is configured.
    pub backstop_delay: std::time::Duration,
    /// Rooms whose fingerprint moved since the last completed pass.
    pub dirty: &'a [String],
}

/// What the loop should do with one poll.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PassPlan {
    pub scope: PassScope,
    /// Issue the queries. False for a skip — the branch a quiet node lives in.
    pub execute: bool,
    /// Print the sweep-alloc line although nothing is asked, at most once per
    /// interval: without it, a skip is invisible, and with it every poll it is noise.
    pub report_skip: bool,
    pub full_due: bool,
}

/// Owns the loop's clocks, so their arithmetic is testable without a runtime.
///
/// This exists because the clocks were the bug: a gate above the decision made two
/// thirds of `PassScope` unreachable, and a counter placed next to a 5 s sleep counted
/// polls instead of passes. Both are invisible in a code read and obvious in a test
/// that drives a planner over a simulated hour.
#[derive(Debug, Clone)]
pub struct PassPlanner {
    /// Executed passes since the last wildcard. Skips do not count. Reported, not a
    /// trigger: gating the wildcard on this is what left a node empty forever.
    passes_since_full: u32,
    /// Wall time since the last wildcard.
    since_full: std::time::Duration,
    /// Wall time since the last executed pass. Reset ONLY by an executed pass.
    since_pass: std::time::Duration,
    /// Wall time since the last printed line, executed or skipped.
    since_report: std::time::Duration,
    /// Clock floor between wildcards; zero means the clock trigger is off.
    full_interval: std::time::Duration,
    fulls: u32,
}

impl PassPlanner {
    pub fn new(full_interval: std::time::Duration) -> Self {
        PassPlanner {
            passes_since_full: 0,
            since_full: std::time::Duration::ZERO,
            since_pass: std::time::Duration::ZERO,
            since_report: std::time::Duration::ZERO,
            full_interval,
            fulls: 0,
        }
    }

    /// Advance every clock by exactly one poll, then decide. Call once per poll.
    pub fn plan(&mut self, tick: PollTick<'_>) -> PassPlan {
        self.since_pass = self.since_pass.saturating_add(tick.poll);
        self.since_full = self.since_full.saturating_add(tick.poll);
        self.since_report = self.since_report.saturating_add(tick.poll);
        let full_due = full_pass_due(tick.peer_grew, self.since_full, self.full_interval);
        let scope = decide_scope(full_due, tick.dirty);
        let skip = matches!(scope, PassScope::Skip);
        // The [skip] line needs a floor of its own: a switched-off clock trigger leaves
        // full_interval zero, and "report every poll" would put a line in the log every
        // 5 s on a node that is correctly asking nothing.
        let report_interval = if tick.backstop_delay.is_zero() {
            if self.full_interval.is_zero() {
                std::time::Duration::from_secs(CATCHUP_FULL_MIN_INTERVAL_SECS)
            } else {
                self.full_interval
            }
        } else {
            tick.backstop_delay
        };
        PassPlan {
            scope,
            execute: !skip,
            report_skip: skip && self.since_report >= report_interval,
            full_due,
        }
    }

    /// A pass really ran. Only now does the clock since the last pass go back to zero,
    /// and only a wildcard clears the wildcard counters.
    pub fn note_executed(&mut self, full: bool) {
        self.since_pass = std::time::Duration::ZERO;
        self.since_report = std::time::Duration::ZERO;
        if full {
            self.passes_since_full = 0;
            self.since_full = std::time::Duration::ZERO;
            self.fulls = self.fulls.saturating_add(1);
        } else {
            self.passes_since_full = self.passes_since_full.saturating_add(1);
        }
    }

    /// A skip line was printed, so the next one waits out the interval.
    pub fn note_reported(&mut self) {
        self.since_report = std::time::Duration::ZERO;
    }

    pub fn passes_since_full(&self) -> u32 {
        self.passes_since_full
    }

    pub fn since_pass(&self) -> std::time::Duration {
        self.since_pass
    }

    pub fn since_report(&self) -> std::time::Duration {
        self.since_report
    }

    /// Wildcards issued so far. The number to watch: it is the expensive request.
    pub fn fulls(&self) -> u32 {
        self.fulls
    }
}

/// The keys one pass sends on one channel. A `Full` scope is the bare wildcard;
/// `Targeted` names rooms and therefore contains no `*`; `Skip` asks nothing.
pub fn query_keys(prefix: &str, leaf: &str, scope: &PassScope) -> Vec<String> {
    match scope {
        PassScope::Full => vec![format!("{prefix}/*/{leaf}")],
        PassScope::Targeted(rooms) => rooms
            .iter()
            // The room id is one key segment; a `#`/`?`/`*`/`/` inside it is forbidden
            // by the router, and an unescaped `/` would forge a deeper path. Encoded
            // here and decoded by the serving side (room_from_key).
            .map(|r| {
                format!(
                    "{prefix}/{}/{leaf}",
                    crate::substrate::keyexpr::encode_segment(r)
                )
            })
            .collect(),
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

    // ── Explicit-room passes ──────────────────────────────────────────────────
    //
    // These drive the planner the live loop uses, over simulated polls. The two
    // bugs they exist for were invisible in a code read and obvious here: a gate
    // above the decision that made Skip unreachable, and a pass counter sitting
    // next to a 5 s sleep, which counted polls and turned "every 12th pass" into
    // a wildcard every 60 s.

    // empty_dirty_set_asks_nothing:start
    //   purpose: A converged node with nothing to ask about must issue ZERO queries.
    //            Pinned on the decision AND on the keys: a Skip that still produced
    //            a key would cost exactly the bytes this change exists to remove.
    //   input:  none
    //   output: ()
    //   sideEffects: none
    // empty_dirty_set_asks_nothing:end
    #[test]
    fn empty_dirty_set_asks_nothing() {
        let scope = decide_scope(false, &[]);
        assert_eq!(scope, PassScope::Skip, "nothing dirty, no discovery due → ask nothing");
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
    //            the responder already answers a concrete room with that room alone,
    //            so a `*` here would undo the whole change.
    //   input:  none
    //   output: ()
    //   sideEffects: none
    // targeted_pass_names_rooms_without_wildcard:end
    #[test]
    fn targeted_pass_names_rooms_without_wildcard() {
        let rooms = vec!["!one:localhost".to_string(), "!two:localhost".to_string()];
        let scope = decide_scope(false, &rooms);
        assert_eq!(
            scope,
            PassScope::Targeted(rooms.clone()),
            "dirty rooms present, no discovery due → targeted"
        );
        assert_eq!(scope.label(), "targeted");
        let hist = query_keys("mrgd/matrix/room", "history", &scope);
        assert_eq!(
            hist,
            vec![
                "mrgd/matrix/room/%21one%3Alocalhost/history",
                "mrgd/matrix/room/%21two%3Alocalhost/history",
            ]
        );
        for key in &hist {
            assert!(!key.contains('*'), "targeted key must not be a wildcard: {key}");
            assert!(!key.contains('!'), "the room segment must be encoded: {key}");
        }
        assert_eq!(query_keys("mrgd/matrix/room", "state", &scope).len(), 2);
    }

    // a_discovery_pass_wins_over_an_empty_dirty_set:start
    //   purpose: "Nothing moved" is exactly when a wildcard still matters — it is how
    //            a room we have never heard of gets found. So the discovery question
    //            is asked first and does not consult the dirty set.
    //   input:  none
    //   output: ()
    //   sideEffects: none
    // a_discovery_pass_wins_over_an_empty_dirty_set:end
    #[test]
    fn a_discovery_pass_wins_over_an_empty_dirty_set() {
        assert_eq!(decide_scope(true, &[]), PassScope::Full, "dirty or not, discovery asks for everything");
        assert_eq!(query_keys("p", "history", &PassScope::Full), vec!["p/*/history"]);
    }

    // an_idle_node_asks_nothing_between_the_clock_wildcards:start
    //   purpose: THE reachability test, with the new contract. Twenty minutes of an idle
    //            node at the real 5 s poll: between wildcards every poll must decide Skip
    //            and execute nothing, so the skip branch is a state the loop actually
    //            spends its life in. But the node MUST pay exactly one wildcard per
    //            interval — that is the fix: an idle node used to pay none, forever, and a
    //            node whose startup pass lost its peer's handshake stayed empty with no
    //            error anywhere.
    //   input:  none
    //   output: () — prints a one-line summary of the simulation
    //   sideEffects: none
    // an_idle_node_asks_nothing_between_the_clock_wildcards:end
    #[test]
    fn an_idle_node_asks_nothing_between_the_clock_wildcards() {
        const POLL: std::time::Duration = std::time::Duration::from_secs(5);
        const BACKSTOP: std::time::Duration = std::time::Duration::from_secs(300);
        let mut p = PassPlanner::new(BACKSTOP);
        let (mut polls, mut executed, mut wildcards, mut skips) = (0u32, 0u32, 0u32, 0u32);
        for i in 0..240 {
            let plan = p.plan(PollTick { poll: POLL, peer_grew: false, backstop_delay: BACKSTOP, dirty: &[] });
            polls += 1;
            if plan.execute {
                executed += 1;
                assert_eq!(
                    plan.scope,
                    PassScope::Full,
                    "poll {i}: an idle node's only pass is the clock wildcard"
                );
                assert!(plan.full_due, "poll {i}: a wildcard came from the clock");
                wildcards += 1;
                p.note_executed(plan.full_due);
            } else {
                skips += 1;
                assert_eq!(plan.scope, PassScope::Skip, "poll {i}: nothing moved, ask nothing");
                assert!(!plan.execute, "poll {i}: a skip must not run a pass");
                assert!(!plan.full_due, "poll {i}: the interval has not elapsed yet");
                if plan.report_skip {
                    p.note_reported();
                }
            }
        }
        assert_eq!(polls, 240);
        assert_eq!(wildcards, 4, "one wildcard per 300 s over 20 min, and no more");
        assert_eq!(executed, wildcards, "a clock wildcard is the only pass an idle node runs");
        assert_eq!(skips, 236, "every other poll asks nothing");
        assert_eq!(p.fulls(), 4);
        assert_eq!(
            p.since_pass(),
            std::time::Duration::ZERO,
            "poll 240 is itself the fourth wildcard, so the clock since the last pass is zero"
        );
        println!("idle node: {polls} polls, {executed} passes ({wildcards} wildcards), {skips} skips");
    }

    // since_pass_resets_only_on_an_executed_pass:start
    //   purpose: The clock arithmetic the loop depends on. A skip must not reset it
    //            (there was no pass), and a pass must reset it exactly once — the
    //            old loop added `poll` in two places and reset it before deciding.
    //   input:  none
    //   output: ()
    //   sideEffects: none
    // since_pass_resets_only_on_an_executed_pass:end
    #[test]
    fn since_pass_resets_only_on_an_executed_pass() {
        fn tick(dirty: &[String]) -> PollTick<'_> {
            PollTick {
                poll: std::time::Duration::from_secs(5),
                peer_grew: false,
                backstop_delay: std::time::Duration::from_secs(300),
                dirty,
            }
        }
        let mut p = PassPlanner::new(std::time::Duration::from_secs(300));

        // Three idle polls: the clock grows by 5 s each time and is never reset.
        for expected in [5u64, 10, 15] {
            let plan = p.plan(tick(&[]));
            assert!(!plan.execute);
            assert_eq!(p.since_pass().as_secs(), expected, "grows by exactly one poll");
        }
        // One room moved: a targeted pass runs and clears the clock.
        let moved = vec!["!a:localhost".to_string()];
        let plan = p.plan(tick(&moved));
        assert!(plan.execute, "a moved room is a reason to ask");
        assert_eq!(plan.scope, PassScope::Targeted(moved));
        assert!(!plan.full_due);
        p.note_executed(plan.full_due);
        assert_eq!(p.since_pass(), std::time::Duration::ZERO, "an executed pass resets it");
        assert_eq!(p.passes_since_full(), 1, "a targeted pass counts toward discovery");
        assert_eq!(p.fulls(), 0, "a targeted pass is not a wildcard");
    }

    // wildcards_never_outpace_the_minimum_interval:start
    //   purpose: The pass counter must not become a rate. An hour of a busy node at
    //            the real 5 s poll — 720 passes — must not buy 60 wildcards; the
    //            interval floor is what allows one, so the count is bounded by the
    //            hour, not by the number of passes.
    //   input:  none
    //   output: () — prints the measured wildcard count
    //   sideEffects: none
    // wildcards_never_outpace_the_minimum_interval:end
    #[test]
    fn wildcards_never_outpace_the_minimum_interval() {
        const POLL: std::time::Duration = std::time::Duration::from_secs(5);
        const BACKSTOP: std::time::Duration = std::time::Duration::from_secs(300);
        let mut p = PassPlanner::new(BACKSTOP);
        let moved = vec!["!busy:localhost".to_string()];
        let mut passes = 0u32;
        for i in 0..720 {
            let plan = p.plan(PollTick { poll: POLL, peer_grew: false, backstop_delay: BACKSTOP, dirty: &moved });
            if plan.execute {
                passes += 1;
                p.note_executed(plan.full_due);
            } else {
                assert!(!plan.execute && !plan.full_due, "poll {i}: a dirty room always asks");
            }
        }
        assert_eq!(passes, 720, "every poll of a busy node asks something");
        // One hour, floor 300 s: a wildcard is allowed at most once per interval, so
        // the hour bounds the count — not the 720 passes.
        let bound = (3600 / BACKSTOP.as_secs() + 1) as u32;
        assert!(
            p.fulls() <= bound,
            "{} wildcards in an hour with a {BACKSTOP:?} floor: bound is {bound}",
            p.fulls()
        );
        assert!(p.fulls() >= 1, "discovery must still happen on a busy node");
        println!("busy node: {passes} passes in an hour, {} wildcards (bound {bound})", p.fulls());
    }

    // peer_appearance_is_still_a_wildcard:start
    //   purpose: The one discovery trigger that predates this change must survive it:
    //            a peer (re)appearing is what a healed partition looks like from this
    //            side, and only a wildcard can see rooms a new peer never told us
    //            about. It is immediate, with no counter and no interval.
    //   input:  none
    //   output: ()
    //   sideEffects: none
    // peer_appearance_is_still_a_wildcard:end
    #[test]
    fn peer_appearance_is_still_a_wildcard() {
        assert!(full_pass_due(
            true,
            std::time::Duration::ZERO,
            std::time::Duration::from_secs(300)
        ));
        assert!(
            full_pass_due(true, std::time::Duration::ZERO, std::time::Duration::ZERO),
            "a peer appeared — that trigger is never switched off"
        );
        let mut p = PassPlanner::new(std::time::Duration::from_secs(300));
        let plan = p.plan(PollTick {
            poll: std::time::Duration::from_secs(5),
            peer_grew: true,
            backstop_delay: std::time::Duration::from_secs(300),
            dirty: &[],
        });
        assert!(plan.full_due);
        assert_eq!(plan.scope, PassScope::Full, "a new peer is asked for everything");
        assert!(plan.execute);
    }

    // full_pass_is_a_clock_not_a_count:start
    //   purpose: The trigger that was a conjunction with a pass counter is now the clock
    //            alone. These are the cases the conjunction got wrong in both directions:
    //            an hour of silence with zero executed passes MUST discover (it never
    //            did), and a zero interval MUST discover nothing on a timer (that is what
    //            MATRIX_HS_CATCHUP_INTERVAL_SECS=0 means).
    //   input:  none
    //   output: ()
    //   sideEffects: none
    // full_pass_is_a_clock_not_a_count:end
    #[test]
    fn full_pass_is_a_clock_not_a_count() {
        let iv = std::time::Duration::from_secs(300);
        assert!(
            !full_pass_due(false, std::time::Duration::from_secs(295), iv),
            "one poll short of the interval"
        );
        assert!(
            full_pass_due(false, std::time::Duration::from_secs(300), iv),
            "at the interval, with no pass ever executed"
        );
        assert!(
            full_pass_due(false, std::time::Duration::from_secs(86_400), iv),
            "a day of silence still discovers"
        );
        assert!(
            !full_pass_due(false, std::time::Duration::from_secs(86_400), std::time::Duration::ZERO),
            "zero interval = the clock trigger is off"
        );
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