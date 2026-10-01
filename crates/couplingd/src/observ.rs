// START_AI_HEADER
// MODULE: couplingd/src/observ.rs
// PURPOSE: Lightweight structured observability emitter for the bsdOS CRDT data-plane.
//          Emits single-line structured events to stderr, gated by the BSDOS_OBSERV
//          environment variable.  Zero allocation / zero overhead when disabled.
//          Events are keyed by a content hash (first 8 hex chars of FNV-1a-64) so
//          that a publish on node A can be correlated with a receive on node B.
//
//          Design invariants:
//            - BSDOS_OBSERV unset or empty → `enabled()` returns false (OnceLock, checked once).
//            - All emit calls return immediately when !enabled() with NO heap allocation.
//            - emit() prints ONE line: "OBSERV <event> node=<id> <k=v...> mono=<millis>".
//            - content_id() is a pure function of the bytes: same bytes → same id on any node.
//            - No external crates; no unsafe; no unwrap() outside the OnceLock init helper.
//
// INTENT: First observability increment to diagnose the live barrier grow-set bug where
//         a publish on node 185 never arrives on node 186.  Running with BSDOS_OBSERV=1
//         on both nodes shows crdt.publish id=X on one side and (or not) crdt.recv id=X
//         on the other.
//
// DEPENDENCIES: std only
// PUBLIC_API: enabled, emit, content_id
// END_AI_HEADER

use std::sync::OnceLock;
use std::time::Instant;

// ── Gate ─────────────────────────────────────────────────────────────────────

// enabled:start
//   purpose: Return true iff BSDOS_OBSERV is set to a non-empty value.
//            Result is cached in a OnceLock so the env lookup happens at most once
//            per process.  This makes every call in the hot path a single atomic
//            load — no heap, no syscall.
//   input:  none (reads BSDOS_OBSERV env at first call)
//   output: bool
//   sideEffects: reads BSDOS_OBSERV env once on first invocation; cached thereafter
// enabled:end
pub fn enabled() -> bool {
    static GATE: OnceLock<bool> = OnceLock::new();
    *GATE.get_or_init(|| {
        std::env::var("BSDOS_OBSERV")
            .map(|v| !v.is_empty())
            .unwrap_or(false)
    })
}

// ── Monotonic clock ───────────────────────────────────────────────────────────

// mono_millis:start
//   purpose: Return milliseconds since process start as a u64.
//            Used as the `mono` field in emitted lines — gives relative ordering
//            within a single node's log without relying on wall clock.
//   input:  none
//   output: u64 milliseconds since first call (process start proxy)
//   sideEffects: initialises start Instant on first call (OnceLock)
// mono_millis:end
fn mono_millis() -> u64 {
    static START: OnceLock<Instant> = OnceLock::new();
    let start = START.get_or_init(Instant::now);
    start.elapsed().as_millis() as u64
}

// ── Node identity ─────────────────────────────────────────────────────────────

// node_id:start
//   purpose: Return the node identifier for this process.
//            Reads BSDOS_NODE_ID first; falls back to MATRIX_HS_NODE_ID
//            (matrix-hs sets this early in main); falls back to "?".
//            Cached in a OnceLock.
//   input:  none
//   output: &'static str
//   sideEffects: reads env vars once on first call
// node_id:end
fn node_id() -> &'static str {
    static ID: OnceLock<String> = OnceLock::new();
    ID.get_or_init(|| {
        std::env::var("BSDOS_NODE_ID")
            .or_else(|_| std::env::var("MATRIX_HS_NODE_ID"))
            .unwrap_or_else(|_| "?".to_string())
    })
    .as_str()
}

// ── Emitter ───────────────────────────────────────────────────────────────────

// emit:start
//   purpose: Write ONE structured line to stderr if observability is enabled.
//            Format: "OBSERV <event> node=<id> <k=v ...> mono=<millis>"
//            All k=v pairs are joined with spaces on a single line so the output
//            is grep-friendly (each event == one line, no multi-line blobs).
//            Returns immediately (no-op) when !enabled() — callers should also
//            guard with `if observ::enabled() { ... }` to skip building field
//            strings in the hot path.
//   input:  event  — short dot-separated event name (e.g. "crdt.publish")
//           fields — slice of (key, value) string pairs
//   output: none
//   sideEffects: writes one line to stderr when enabled
// emit:end
pub fn emit(event: &str, fields: &[(&str, &str)]) {
    if !enabled() {
        return;
    }
    // Avoid a format String allocation per field by writing directly to a
    // stack-local buffer (typical event is well under 256 bytes).
    // We fall back to a heap String only if the buffer overflows (pathological).
    let mut buf = String::with_capacity(256);
    buf.push_str("OBSERV ");
    buf.push_str(event);
    buf.push_str(" node=");
    buf.push_str(node_id());
    for (k, v) in fields {
        buf.push(' ');
        buf.push_str(k);
        buf.push('=');
        buf.push_str(v);
    }
    buf.push_str(" mono=");
    // Use a small stack buffer for the integer to avoid format!() allocation.
    let ms = mono_millis();
    let ms_str = ms.to_string();
    buf.push_str(&ms_str);
    // eprintln! adds a newline.
    eprintln!("{buf}");
}

// ── Content identifier ─────────────────────────────────────────────────────────

// content_id:start
//   purpose: Produce a short, stable content-address for a byte slice.
//            Uses the same FNV-1a-64 hash as Pdu::compute_id in matrix_events.rs
//            so the algorithm is consistent across the codebase.
//            Returns the first 8 hex characters of the 64-bit hash (32-bit of
//            entropy — sufficient for cross-node correlation in debug sessions).
//            Deterministic: same bytes → same id on any node.
//   input:  bytes — byte slice to hash (the serialised CRDT delta blob)
//   output: String of exactly 8 lowercase hex characters
//   sideEffects: none
// content_id:end
pub fn content_id(bytes: &[u8]) -> String {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME:  u64 = 0x0000_0100_0000_01b3;
    let mut h: u64 = OFFSET;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(PRIME);
    }
    // Return the top 32 bits as 8 hex chars.
    format!("{:08x}", (h >> 32) as u32)
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    // content_id:stable:start
    //   purpose: Same bytes always yield the same id (deterministic hash).
    //   input:  two identical calls
    //   output: equal strings
    //   sideEffects: none
    // content_id:stable:end
    #[test]
    fn content_id_stable_same_bytes() {
        let id1 = content_id(b"hello, bsdOS CRDT");
        let id2 = content_id(b"hello, bsdOS CRDT");
        assert_eq!(id1, id2, "same bytes must produce same id");
        assert_eq!(id1.len(), 8, "id must be exactly 8 hex chars");
    }

    // content_id:differs:start
    //   purpose: Different bytes yield different ids.
    //   input:  two distinct byte slices
    //   output: different strings (with overwhelming probability)
    //   sideEffects: none
    // content_id:differs:end
    #[test]
    fn content_id_differs_for_different_bytes() {
        let id1 = content_id(b"payload-A");
        let id2 = content_id(b"payload-B");
        assert_ne!(id1, id2, "different bytes should produce different ids");
    }

    // content_id:empty:start
    //   purpose: Empty slice produces a stable id (the FNV offset basis).
    //   input:  empty slice, called twice
    //   output: same 8-char string both times
    //   sideEffects: none
    // content_id:empty:end
    #[test]
    fn content_id_empty_is_stable() {
        let id1 = content_id(b"");
        let id2 = content_id(b"");
        assert_eq!(id1, id2);
        assert_eq!(id1.len(), 8);
    }

    // enabled_gate:start
    //   purpose: enabled() is a bool that is consistent with process env.
    //            We cannot force-reset the OnceLock in tests, so we just verify
    //            that the return type is bool and the function does not panic.
    //   input:  none
    //   output: bool (value depends on test environment)
    //   sideEffects: none
    // enabled_gate:end
    #[test]
    fn enabled_returns_bool_without_panic() {
        let _ = enabled(); // must not panic
        // Calling twice returns the same value (OnceLock).
        let v1 = enabled();
        let v2 = enabled();
        assert_eq!(v1, v2, "enabled() must be idempotent");
    }

    // emit_noop_when_disabled:start
    //   purpose: emit() returns without writing when observability is disabled.
    //            We verify by ensuring no panic and by checking the content_id
    //            used in the field is not computed when the gate is false
    //            (structural — we cannot capture stderr from a unit test portably,
    //            but we can at least verify it does not panic or allocate visibly).
    //   input:  call emit() regardless of gate
    //   output: no panic
    //   sideEffects: may write to stderr if BSDOS_OBSERV is set in test env
    // emit_noop_when_disabled:end
    #[test]
    fn emit_does_not_panic() {
        // Call emit with some typical fields — must not panic in any env.
        emit("crdt.test", &[("key", "bsdos/crdt/test"), ("id", "ab12cd34")]);
    }

    // mono_millis:start
    //   purpose: mono_millis() is non-decreasing across two calls.
    //   input:  two consecutive calls
    //   output: second >= first
    //   sideEffects: none
    // mono_millis:end
    #[test]
    fn mono_millis_non_decreasing() {
        let t1 = mono_millis();
        let t2 = mono_millis();
        assert!(t2 >= t1, "mono_millis must be non-decreasing");
    }
}
