// startup_gate.rs
//
// A node that has a peer configured must not take grow-set writes (username
// registration, room alias claims) before it has heard the peer's claims.
// Until then its synced view is empty, so a name already held on the peer is
// accepted locally and only the grow-set resolver renames the loser later --
// convergent, but visible to the user (observed: register accepted 6 s after
// a restart, "RENAMED ... lost grow-set conflict" afterwards).
//
// The gate opens on the first of:
//   - both signals arrived: the startup catch-up pass finished AND the peer's
//     claims were received (ack from a peer's ReconcileDriver, see
//     barrier_growset);
//   - the deadline (MATRIX_HS_GROWSET_GATE_MAX_MS, default 30000 ms) passed:
//     the peer is unreachable or runs an older build, behaviour reverts to the
//     ungated one and a log line says so.
// A node without a configured peer has no gate at all (AppState.startup_gate
// is None).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Default for MATRIX_HS_GROWSET_GATE_MAX_MS.
pub const DEFAULT_GATE_MAX_MS: u64 = 30_000;

/// Retry hint returned to clients while the gate is closed.
pub const GATE_RETRY_AFTER_MS: u64 = 1000;

pub struct StartupGate {
    catchup_done: AtomicBool,
    claims_synced: AtomicBool,
    timed_out_logged: AtomicBool,
    opened_logged: AtomicBool,
    started: Instant,
    max_wait: Duration,
}

impl StartupGate {
    pub fn new(max_wait: Duration) -> Arc<Self> {
        Arc::new(StartupGate {
            catchup_done: AtomicBool::new(false),
            claims_synced: AtomicBool::new(false),
            timed_out_logged: AtomicBool::new(false),
            opened_logged: AtomicBool::new(false),
            started: Instant::now(),
            max_wait,
        })
    }

    /// MATRIX_HS_GROWSET_GATE_MAX_MS, falling back to the default.
    pub fn max_wait_from_env() -> Duration {
        let ms = std::env::var("MATRIX_HS_GROWSET_GATE_MAX_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(DEFAULT_GATE_MAX_MS);
        Duration::from_millis(ms)
    }

    pub fn note_catchup_done(&self) {
        self.catchup_done.store(true, Ordering::SeqCst);
    }

    pub fn note_claims_synced(&self) {
        self.claims_synced.store(true, Ordering::SeqCst);
    }

    pub fn claims_synced(&self) -> bool {
        self.claims_synced.load(Ordering::SeqCst)
    }

    /// Ok(()) when a grow-set write may proceed; Err(retry_after_ms) while the
    /// node is still catching up with its peer.
    pub fn check(&self) -> Result<(), u64> {
        if self.catchup_done.load(Ordering::SeqCst) && self.claims_synced.load(Ordering::SeqCst) {
            if !self.opened_logged.swap(true, Ordering::SeqCst) {
                println!(
                    "cluster mode: grow-set gate open after {} ms (catch-up done, peer claims received)",
                    self.started.elapsed().as_millis()
                );
            }
            return Ok(());
        }
        if self.started.elapsed() >= self.max_wait {
            if !self.timed_out_logged.swap(true, Ordering::SeqCst) {
                eprintln!(
                    "[matrix-hs] grow-set gate: no completed catch-up with the peer after {} ms \
                     (catchup_done={}, claims_synced={}) -- accepting grow-set writes anyway",
                    self.max_wait.as_millis(),
                    self.catchup_done.load(Ordering::SeqCst),
                    self.claims_synced.load(Ordering::SeqCst),
                );
            }
            return Ok(());
        }
        Err(GATE_RETRY_AFTER_MS)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn closed_until_both_signals() {
        let g = StartupGate::new(Duration::from_secs(60));
        assert!(g.check().is_err());
        g.note_catchup_done();
        assert!(g.check().is_err());
        g.note_claims_synced();
        assert!(g.check().is_ok());
    }

    #[test]
    fn opens_on_timeout() {
        let g = StartupGate::new(Duration::from_millis(0));
        assert!(g.check().is_ok());
    }
}
