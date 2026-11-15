// START_AI_HEADER
// MODULE: src/startup_gate.rs
// PURPOSE: Startup gate for grow-set writes: a node with a configured peer refuses username/alias
//          claims until the catch-up pass finished and the peers claims arrived, or a deadline
//          passed.
// DEPENDENCIES: std
// PUBLIC_API: gate_wanted, StartupGate, DEFAULT_GATE_MAX_MS, GATE_RETRY_AFTER_MS
// END_AI_HEADER

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
// is None). "Configured peer" means CONNECT, or an anchored node list
// (MATRIX_HS_NODE_KEYS) naming a node other than ourselves: a node that only
// LISTENS (the stand clients register on) knows its peers from that list, and
// they will dial in after its restart. See gate_wanted().

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Default for MATRIX_HS_GROWSET_GATE_MAX_MS.
pub const DEFAULT_GATE_MAX_MS: u64 = 30_000;

/// Retry hint returned to clients while the gate is closed.
pub const GATE_RETRY_AFTER_MS: u64 = 1000;

// gate_wanted:start
//   purpose: Whether this node arms the startup gate: CONNECT set, or an anchored node list naming
//            a node other than own_id.
//   input:  connect, node_keys (MATRIX_HS_NODE_KEYS spec), own_id
//   output: bool
//   sideEffects: none
// gate_wanted:end
/// Whether this node arms the gate: a CONNECT endpoint is set, or the anchored
/// node list (MATRIX_HS_NODE_KEYS) holds at least one node_id other than `own_id`.
/// A listen-only node has no CONNECT but, when anchored, still knows peers exist.
/// A malformed or empty list yields no peers (the anchor itself fails closed).
pub fn gate_wanted(connect: Option<&str>, node_keys: Option<&str>, own_id: &str) -> bool {
    if connect.is_some_and(|c| !c.trim().is_empty()) {
        return true;
    }
    match node_keys {
        Some(spec) if !spec.trim().is_empty() => {
            match crate::substrate::node_auth::parse_node_keys(spec) {
                Ok(keys) => keys.keys().any(|k| k.as_str() != own_id),
                Err(_) => false,
            }
        }
        _ => false,
    }
}

pub struct StartupGate {
    catchup_done: AtomicBool,
    claims_synced: AtomicBool,
    timed_out_logged: AtomicBool,
    opened_logged: AtomicBool,
    started: Instant,
    max_wait: Duration,
}

impl StartupGate {
    // StartupGate::new:start
    //   purpose: Create a closed gate that gives up waiting after max_wait.
    //   input:  max_wait
    //   output: Arc<StartupGate>
    //   sideEffects: starts the gate clock
    // StartupGate::new:end
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

    // StartupGate::max_wait_from_env:start
    //   purpose: Read MATRIX_HS_GROWSET_GATE_MAX_MS, falling back to the default.
    //   input:  env MATRIX_HS_GROWSET_GATE_MAX_MS
    //   output: Duration
    //   sideEffects: reads the process env
    // StartupGate::max_wait_from_env:end
    /// MATRIX_HS_GROWSET_GATE_MAX_MS, falling back to the default.
    pub fn max_wait_from_env() -> Duration {
        let ms = std::env::var("MATRIX_HS_GROWSET_GATE_MAX_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(DEFAULT_GATE_MAX_MS);
        Duration::from_millis(ms)
    }

    // StartupGate::note_catchup_done:start
    //   purpose: Record that the startup catch-up pass finished.
    //   input:  &self
    //   output: none
    //   sideEffects: sets an atomic flag
    // StartupGate::note_catchup_done:end
    pub fn note_catchup_done(&self) {
        self.catchup_done.store(true, Ordering::SeqCst);
    }

    // StartupGate::note_claims_synced:start
    //   purpose: Record that the peer claims were received (ack from its ReconcileDriver).
    //   input:  &self
    //   output: none
    //   sideEffects: sets an atomic flag
    // StartupGate::note_claims_synced:end
    pub fn note_claims_synced(&self) {
        self.claims_synced.store(true, Ordering::SeqCst);
    }

    // StartupGate::claims_synced:start
    //   purpose: Whether the peer claims were received.
    //   input:  &self
    //   output: bool
    //   sideEffects: none
    // StartupGate::claims_synced:end
    pub fn claims_synced(&self) -> bool {
        self.claims_synced.load(Ordering::SeqCst)
    }

    // StartupGate::check:start
    //   purpose: Decide whether a grow-set write may proceed: open when both signals arrived or the
    //            deadline passed.
    //   input:  &self
    //   output: Ok(()) when open, Err(retry_after_ms) while closed
    //   sideEffects: logs one line on opening and one on timeout
    // StartupGate::check:end
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

    // closed_until_both_signals:start
    //   purpose: The gate stays closed until both catch-up and claims signals arrive.
    // closed_until_both_signals:end
    #[test]
    fn closed_until_both_signals() {
        let g = StartupGate::new(Duration::from_secs(60));
        assert!(g.check().is_err());
        g.note_catchup_done();
        assert!(g.check().is_err());
        g.note_claims_synced();
        assert!(g.check().is_ok());
    }

    const KEY_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const KEY_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    // gate_wanted_matrix:start
    //   purpose: gate_wanted over CONNECT set/blank and anchored key lists: own-only, with peer,
    //            empty, malformed.
    // gate_wanted_matrix:end
    #[test]
    fn gate_wanted_matrix() {
        let both = format!("node-a={KEY_A},node-b={KEY_B}");
        let only_own = format!("node-a={KEY_A}");
        // CONNECT set: gate, whatever the key list says.
        assert!(gate_wanted(Some("tcp/127.0.0.1:7449"), None, "node-a"));
        assert!(gate_wanted(Some("tcp/127.0.0.1:7449"), Some(&only_own), "node-a"));
        // Blank CONNECT counts as unset.
        assert!(!gate_wanted(Some("  "), None, "node-a"));
        // LISTEN-only + anchored with a peer: gate.
        assert!(gate_wanted(None, Some(&both), "node-a"));
        // LISTEN-only + anchored, only ourselves: no peer known, no gate.
        assert!(!gate_wanted(None, Some(&only_own), "node-a"));
        // No NODE_KEYS (TOFU), empty, or malformed list: no gate.
        assert!(!gate_wanted(None, None, "node-a"));
        assert!(!gate_wanted(None, Some(""), "node-a"));
        assert!(!gate_wanted(None, Some("garbage"), "node-a"));
    }

    // opens_on_timeout:start
    //   purpose: A gate with a zero deadline is open at once.
    // opens_on_timeout:end
    #[test]
    fn opens_on_timeout() {
        let g = StartupGate::new(Duration::from_millis(0));
        assert!(g.check().is_ok());
    }
}
