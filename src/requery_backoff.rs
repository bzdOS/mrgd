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
}
// requery_backoff_test:end