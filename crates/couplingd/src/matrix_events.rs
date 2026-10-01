// START_AI_HEADER
// MODULE: couplingd/src/matrix_events.rs
// PURPOSE: Matrix event-DAG as a CRDT (Stage 0 — events only, no state-res).
//          A Matrix room's history is modelled as a grow-only set of PDUs keyed by
//          content-addressed event_id.  The set is a join-semilattice (add-only,
//          union on merge) — idempotent, commutative, associative.
//          `ordered()` returns a deterministic topological sort of the causal DAG
//          defined by `prev_events` references; tie-break: (depth, ts, event_id).
//          Two replicas that receive the same PDUs in any order converge to
//          identical `ordered()` output (Strong Eventual Consistency).
// INTENT: PoC demonstrating CRDT-laws + convergence for Matrix-event history;
//         reuses CrdtSink / MemCrdtSink from crdt.rs for delta exchange.
//         State-resolution (room-state) is explicitly out of scope (next slice).
// DEPENDENCIES: std, crate::crdt::{CrdtSink, CrdtError}
// PUBLIC_API: Pdu, RoomLogDelta, RoomLog
// END_AI_HEADER

use std::collections::{HashMap, HashSet, VecDeque};
use crate::crdt::{CrdtError, CrdtSink};

// ═══════════════════════════════════════════════════════════════════════════════
// Pdu — immutable Protocol Data Unit (Matrix event, content-addressed)
// ═══════════════════════════════════════════════════════════════════════════════

// Pdu:start
//   purpose: Immutable Matrix PDU. `event_id` is a deterministic content-hash of
//            (room_id, sender, kind, content, prev_events, depth, ts) — injected
//            by the creator so tests stay deterministic (no wall-clock, no RNG).
//            `prev_events` forms the causal DAG: each PDU references the set of
//            event_ids that causally precede it in the room.
//   input:  all fields explicit; `compute_id()` derives event_id from content
//   output: Pdu value (Clone+PartialEq+Eq+Hash via event_id)
//   sideEffects: none (pure value)
// Pdu:end
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pdu {
    /// Content-addressed identifier (deterministic hash of payload).
    pub event_id:    String,
    pub room_id:     String,
    pub sender:      String,
    pub kind:        String,
    /// Opaque event content (JSON body, Cap'n Proto payload, etc.).
    pub content:     Vec<u8>,
    /// Causal parents: event_ids this PDU directly follows in the DAG.
    pub prev_events: Vec<String>,
    /// Depth in the DAG (0 = room-create event, monotone).
    pub depth:       u64,
    /// Origin server timestamp injected by sender (not Date::now in tests).
    pub ts:          u64,
}

impl Pdu {
    // Pdu::compute_id:start
    //   purpose: Derive a deterministic event_id from content fields using a
    //            simple FNV-1a-64 hash.  Production would use SHA-256/canonical-JSON
    //            per the Matrix spec; FNV suffices for PoC determinism.
    //            The hash is over (room_id + sender + kind + content + prev_events sorted
    //            + depth.to_string + ts.to_string) byte-concatenated.
    //   input:  room_id, sender, kind, content, prev_events, depth, ts — all by reference
    //   output: String — hex-encoded 64-bit hash prefixed with "$"
    //   sideEffects: none
    // Pdu::compute_id:end
    pub fn compute_id(
        room_id:     &str,
        sender:      &str,
        kind:        &str,
        content:     &[u8],
        prev_events: &[String],
        depth:       u64,
        ts:          u64,
    ) -> String {
        const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
        const PRIME:  u64 = 0x0000_0100_0000_01b3;

        let mut h: u64 = OFFSET;
        let feed = |h: &mut u64, bytes: &[u8]| {
            for &b in bytes {
                *h ^= b as u64;
                *h = h.wrapping_mul(PRIME);
            }
        };

        feed(&mut h, room_id.as_bytes());
        feed(&mut h, b"\x00");
        feed(&mut h, sender.as_bytes());
        feed(&mut h, b"\x00");
        feed(&mut h, kind.as_bytes());
        feed(&mut h, b"\x00");
        feed(&mut h, content);
        feed(&mut h, b"\x00");
        // Sort prev_events for canonical order.
        let mut sorted_prevs = prev_events.to_vec();
        sorted_prevs.sort();
        for p in &sorted_prevs {
            feed(&mut h, p.as_bytes());
            feed(&mut h, b"\x01");
        }
        feed(&mut h, &depth.to_le_bytes());
        feed(&mut h, &ts.to_le_bytes());

        format!("${:016x}", h)
    }

    // Pdu::new:start
    //   purpose: Construct a Pdu with a computed event_id.
    //            Caller injects ts and sender; no Date::now.
    //   input:  room_id, sender, kind, content, prev_events, depth, ts — all owned
    //   output: Pdu with event_id = compute_id(...)
    //   sideEffects: none
    // Pdu::new:end
    pub fn new(
        room_id:     String,
        sender:      String,
        kind:        String,
        content:     Vec<u8>,
        prev_events: Vec<String>,
        depth:       u64,
        ts:          u64,
    ) -> Self {
        let event_id = Self::compute_id(
            &room_id, &sender, &kind, &content, &prev_events, depth, ts,
        );
        Pdu { event_id, room_id, sender, kind, content, prev_events, depth, ts }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// RoomLogDelta — set of PDUs not yet seen by the remote replica
// ═══════════════════════════════════════════════════════════════════════════════

// RoomLogDelta:start
//   purpose: Delta form of RoomLog: a subset of PDUs to send to a lagging replica.
//            Applying a delta is idempotent (add-only, duplicate event_id is a no-op).
//   input:  Vec<Pdu> to transmit
//   output: opaque carrier; apply_delta() on the receiver merges them in
//   sideEffects: none (pure value)
// RoomLogDelta:end
#[derive(Debug, Clone)]
pub struct RoomLogDelta {
    pub pdus: Vec<Pdu>,
}

// ═══════════════════════════════════════════════════════════════════════════════
// RoomLog — grow-only set of PDUs, causal DAG, topological ordering
// ═══════════════════════════════════════════════════════════════════════════════

// RoomLog:start
//   purpose: CRDT-correct history of a Matrix room.
//            Invariant: once an event_id is in the set it is never removed
//            (grow-only / add-wins). Merge = union of event sets (join-semilattice:
//            idempotent, commutative, associative).
//            `ordered()` returns a deterministic linearisation of the causal DAG
//            using Kahn's BFS topological sort; events whose prev_events have
//            not yet arrived are deferred until those ancestors appear (forward-
//            reference tolerance).  Tie-breaking within the same topological
//            horizon: (depth ASC, ts ASC, event_id ASC) — fully deterministic.
//   input:  `add(pdu)`, `merge(&other)`, `apply_delta`, `delta()`, `ordered()`
//   output: convergent room history; `ordered()` identical on all fully-synced replicas
//   sideEffects: mutates internal HashMap on add/merge/apply_delta
// RoomLog:end
#[derive(Debug, Clone, Default)]
pub struct RoomLog {
    /// The grow-only set: event_id → Pdu.
    events: HashMap<String, Pdu>,
}

impl RoomLog {
    // RoomLog::new:start
    //   purpose: Construct an empty RoomLog.
    //   input:  none
    //   output: RoomLog
    //   sideEffects: none
    // RoomLog::new:end
    pub fn new() -> Self {
        Self::default()
    }

    // RoomLog::add:start
    //   purpose: Add a PDU to the grow-only set.
    //            If event_id already exists the add is silently ignored (idempotent).
    //            Does NOT validate prev_events references — forward-references are
    //            allowed; `ordered()` defers them until parents arrive.
    //   input:  pdu — PDU to add
    //   output: none
    //   sideEffects: inserts into self.events if event_id is new
    // RoomLog::add:end
    pub fn add(&mut self, pdu: Pdu) {
        self.events.entry(pdu.event_id.clone()).or_insert(pdu);
    }

    // RoomLog::merge:start
    //   purpose: Join this RoomLog with another (union of event sets).
    //            Satisfies join-semilattice laws:
    //              idempotent  — merge(a, a) == a
    //              commutative — merge(a, b) == merge(b, a)
    //              associative — merge(merge(a,b),c) == merge(a,merge(b,c))
    //   input:  other — another RoomLog (borrowed)
    //   output: none
    //   sideEffects: inserts missing PDUs from other into self.events
    // RoomLog::merge:end
    pub fn merge(&mut self, other: &RoomLog) {
        for (id, pdu) in &other.events {
            self.events.entry(id.clone()).or_insert_with(|| pdu.clone());
        }
    }

    // RoomLog::delta:start
    //   purpose: Produce a full-state delta (all PDUs in this log).
    //            Recipients call apply_delta; applying the same delta twice is
    //            idempotent (grow-only guarantees).
    //            Production would compute an incremental delta against a version
    //            vector; full-snapshot is correct and sufficient for PoC.
    //   input:  none
    //   output: RoomLogDelta containing clones of all PDUs
    //   sideEffects: none
    // RoomLog::delta:end
    pub fn delta(&self) -> RoomLogDelta {
        RoomLogDelta { pdus: self.events.values().cloned().collect() }
    }

    // RoomLog::apply_delta:start
    //   purpose: Merge a received RoomLogDelta into this log.
    //            Equivalent to `add(pdu)` for each pdu in the delta (idempotent).
    //   input:  delta — RoomLogDelta received from a peer
    //   output: none
    //   sideEffects: inserts new PDUs; ignores duplicates
    // RoomLog::apply_delta:end
    pub fn apply_delta(&mut self, delta: &RoomLogDelta) {
        for pdu in &delta.pdus {
            self.events.entry(pdu.event_id.clone()).or_insert_with(|| pdu.clone());
        }
    }

    // RoomLog::len:start
    //   purpose: Return count of events currently in the log.
    //   input:  none
    //   output: usize
    //   sideEffects: none
    // RoomLog::len:end
    pub fn len(&self) -> usize {
        self.events.len()
    }

    // RoomLog::is_empty:start
    //   purpose: Return true iff the log contains no events.
    //   input:  none
    //   output: bool
    //   sideEffects: none
    // RoomLog::is_empty:end
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    // RoomLog::ordered:start
    //   purpose: Return a deterministic topological linearisation of all events
    //            whose causal ancestors are fully present in this log.
    //            Events with a forward-reference (prev_event not yet arrived) are
    //            silently deferred — they will appear in subsequent calls once their
    //            ancestors are added via add()/merge()/apply_delta().
    //
    //            Algorithm: Kahn's BFS.  The ready-set is events whose entire
    //            prev_events set is already delivered.  On each step, pick the
    //            lexicographically smallest (depth ASC, ts ASC, event_id ASC)
    //            event from the ready-set — ensures full determinism under
    //            concurrent branches with equal depth/ts.
    //
    //   input:  none
    //   output: Vec<&Pdu> in causal + tie-broken order; only fully-resolvable prefix
    //   sideEffects: none (read-only, allocates BFS work structures)
    // RoomLog::ordered:end
    pub fn ordered(&self) -> Vec<&Pdu> {
        // in_degree: how many of each event's prev_events are still un-delivered in sort.
        // An event is only eligible for ordering if ALL its prev_events are present in
        // the log.  If any prev is missing the event is added to `blocked` and excluded
        // from the Kahn BFS entirely — deferred until the prev arrives in a future call.
        let mut in_degree: HashMap<&str, usize> = HashMap::new();
        let mut blocked: HashSet<&str> = HashSet::new();
        for pdu in self.events.values() {
            // Ensure every event has an entry, even if in_degree stays 0.
            in_degree.entry(&pdu.event_id).or_insert(0);
            for prev in &pdu.prev_events {
                if self.events.contains_key(prev.as_str()) {
                    // Known prev: contributes to Kahn in-degree (decremented when prev emitted).
                    // Only update if not already blocked (avoid touching blocked events' degrees).
                    if !blocked.contains(pdu.event_id.as_str()) {
                        *in_degree.entry(&pdu.event_id).or_insert(0) += 1;
                    }
                } else {
                    // Unknown prev: event is unreachable until the prev arrives.
                    blocked.insert(&pdu.event_id);
                    // Reset in_degree to a sentinel so it is never emitted.
                    *in_degree.entry(&pdu.event_id).or_insert(0) = usize::MAX;
                }
            }
        }

        // Seed the BFS queue with zero-in-degree events (causal roots).
        // Explicitly exclude blocked events (those with a missing ancestor).
        // Use a sorted Vec as a priority queue (small N; good enough for PoC).
        let mut ready: Vec<&Pdu> = in_degree
            .iter()
            .filter_map(|(&id, &deg)| {
                if deg == 0 && !blocked.contains(id) {
                    self.events.get(id)
                } else {
                    None
                }
            })
            .collect();
        Self::sort_ready(&mut ready);

        // Track which event_ids have been emitted.
        let mut emitted: HashSet<&str> = HashSet::new();
        // Build a reverse-edge index: event_id → list of events that have it as prev.
        let mut successors: HashMap<&str, Vec<&Pdu>> = HashMap::new();
        for pdu in self.events.values() {
            for prev in &pdu.prev_events {
                if self.events.contains_key(prev.as_str()) {
                    successors.entry(prev.as_str()).or_default().push(pdu);
                }
            }
        }

        let mut result: Vec<&Pdu> = Vec::with_capacity(self.events.len());
        let mut work: VecDeque<&Pdu> = VecDeque::new();
        // Seed work queue from the initial ready set (already sorted).
        for pdu in ready.drain(..) {
            work.push_back(pdu);
        }

        while let Some(pdu) = work.pop_front() {
            if !emitted.insert(&pdu.event_id) {
                continue; // duplicate from multi-prev convergence — skip
            }
            result.push(pdu);

            // Decrement in_degree for successors; if zero, add to a batch.
            // Guard: usize::MAX marks permanently-blocked events (missing ancestor);
            // never decrement those — they stay blocked.
            let mut newly_ready: Vec<&Pdu> = Vec::new();
            if let Some(succs) = successors.get(pdu.event_id.as_str()) {
                for succ in succs {
                    let deg = in_degree.get_mut(succ.event_id.as_str()).expect("in_degree entry");
                    if *deg != usize::MAX && *deg > 0 {
                        *deg -= 1;
                    }
                    if *deg == 0
                        && !emitted.contains(succ.event_id.as_str())
                        && !blocked.contains(succ.event_id.as_str())
                    {
                        newly_ready.push(succ);
                    }
                }
            }
            Self::sort_ready(&mut newly_ready);
            // Prepend to work in sorted order so we respect deterministic ordering
            // even when concurrent branches become ready at the same BFS level.
            // Prepend in reverse so the first element (smallest) ends up at front.
            for pdu in newly_ready.into_iter().rev() {
                work.push_front(pdu);
            }
        }

        result
    }

    // RoomLog::sort_ready:start
    //   purpose: Sort a slice of ready PDUs by (depth ASC, ts ASC, event_id ASC).
    //            Deterministic tie-break for concurrent branches in the DAG.
    //   input:  ready — mutable slice of &Pdu references
    //   output: none (sorts in place)
    //   sideEffects: reorders elements
    // RoomLog::sort_ready:end
    fn sort_ready(ready: &mut Vec<&Pdu>) {
        ready.sort_by(|a, b| {
            a.depth.cmp(&b.depth)
                .then(a.ts.cmp(&b.ts))
                .then(a.event_id.cmp(&b.event_id))
        });
    }

    // RoomLog::publish_delta:start
    //   purpose: Publish this log's full-state delta to a CrdtSink under `key`.
    //            The sink routes the bytes to the peer's inbox.
    //   input:  sink — CrdtSink impl; key — routing key (e.g. "room/!abc:srv")
    //   output: Result<(), CrdtError>
    //   sideEffects: serialises all PDUs and enqueues in sink
    // RoomLog::publish_delta:end
    pub fn publish_delta(&self, sink: &dyn CrdtSink, key: &str) -> Result<(), CrdtError> {
        let bytes = delta_to_bytes(&self.delta());
        sink.publish(key, bytes)
    }

    // RoomLog::drain_delta:start
    //   purpose: Drain pending delta bytes from a CrdtSink and apply each one.
    //   input:  sink — CrdtSink impl; key — routing key
    //   output: Result<usize, CrdtError> — number of deltas applied
    //   sideEffects: mutates self via apply_delta for each received blob
    // RoomLog::drain_delta:end
    pub fn drain_delta(&mut self, sink: &dyn CrdtSink, key: &str) -> Result<usize, CrdtError> {
        let blobs = sink.drain(key)?;
        let count = blobs.len();
        for bytes in &blobs {
            let delta = delta_from_bytes(bytes);
            self.apply_delta(&delta);
        }
        Ok(count)
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Serialisation helpers — compact binary encoding for MemCrdtSink round-trips
// (Not Cap'n Proto — PoC only; proves sink correctness independent of schema)
// ═══════════════════════════════════════════════════════════════════════════════

// delta_to_bytes:start
//   purpose: Serialise a RoomLogDelta to a length-prefixed binary blob.
//            Format: [n_pdus: u32 LE] (pdu_block)* where pdu_block:
//              [event_id_len: u16 LE][event_id bytes]
//              [room_id_len: u16 LE][room_id bytes]
//              [sender_len: u16 LE][sender bytes]
//              [kind_len: u16 LE][kind bytes]
//              [content_len: u32 LE][content bytes]
//              [n_prevs: u16 LE] ([prev_len: u16 LE][prev bytes])*
//              [depth: u64 LE]
//              [ts: u64 LE]
//   input:  delta — RoomLogDelta reference
//   output: Vec<u8>
//   sideEffects: none
// delta_to_bytes:end
pub fn delta_to_bytes(delta: &RoomLogDelta) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&(delta.pdus.len() as u32).to_le_bytes());
    for pdu in &delta.pdus {
        write_str(&mut buf, &pdu.event_id);
        write_str(&mut buf, &pdu.room_id);
        write_str(&mut buf, &pdu.sender);
        write_str(&mut buf, &pdu.kind);
        write_bytes32(&mut buf, &pdu.content);
        buf.extend_from_slice(&(pdu.prev_events.len() as u16).to_le_bytes());
        for prev in &pdu.prev_events {
            write_str(&mut buf, prev);
        }
        buf.extend_from_slice(&pdu.depth.to_le_bytes());
        buf.extend_from_slice(&pdu.ts.to_le_bytes());
    }
    buf
}

// delta_from_bytes:start
//   purpose: Deserialise a binary blob produced by delta_to_bytes back into RoomLogDelta.
//            Panics (with expect) on malformed input — acceptable for PoC test helpers.
//   input:  bytes — slice of bytes from delta_to_bytes
//   output: RoomLogDelta
//   sideEffects: none
// delta_from_bytes:end
pub fn delta_from_bytes(bytes: &[u8]) -> RoomLogDelta {
    let mut off = 0usize;
    let n_pdus = read_u32(bytes, &mut off) as usize;
    let mut pdus = Vec::with_capacity(n_pdus);
    for _ in 0..n_pdus {
        let event_id    = read_str(bytes, &mut off);
        let room_id     = read_str(bytes, &mut off);
        let sender      = read_str(bytes, &mut off);
        let kind        = read_str(bytes, &mut off);
        let content     = read_bytes32(bytes, &mut off);
        let n_prevs     = read_u16(bytes, &mut off) as usize;
        let prev_events = (0..n_prevs).map(|_| read_str(bytes, &mut off)).collect();
        let depth       = read_u64(bytes, &mut off);
        let ts          = read_u64(bytes, &mut off);
        pdus.push(Pdu { event_id, room_id, sender, kind, content, prev_events, depth, ts });
    }
    RoomLogDelta { pdus }
}

// ── low-level encode/decode ────────────────────────────────────────────────────

fn write_str(buf: &mut Vec<u8>, s: &str) {
    let bytes = s.as_bytes();
    buf.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
    buf.extend_from_slice(bytes);
}

fn write_bytes32(buf: &mut Vec<u8>, b: &[u8]) {
    buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
    buf.extend_from_slice(b);
}

fn read_u16(buf: &[u8], off: &mut usize) -> u16 {
    let v = u16::from_le_bytes(buf[*off..*off + 2].try_into().expect("u16"));
    *off += 2;
    v
}

fn read_u32(buf: &[u8], off: &mut usize) -> u32 {
    let v = u32::from_le_bytes(buf[*off..*off + 4].try_into().expect("u32"));
    *off += 4;
    v
}

fn read_u64(buf: &[u8], off: &mut usize) -> u64 {
    let v = u64::from_le_bytes(buf[*off..*off + 8].try_into().expect("u64"));
    *off += 8;
    v
}

fn read_str(buf: &[u8], off: &mut usize) -> String {
    let len = read_u16(buf, off) as usize;
    let s = std::str::from_utf8(&buf[*off..*off + len]).expect("utf8").to_string();
    *off += len;
    s
}

fn read_bytes32(buf: &[u8], off: &mut usize) -> Vec<u8> {
    let len = read_u32(buf, off) as usize;
    let b = buf[*off..*off + len].to_vec();
    *off += len;
    b
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crdt::MemCrdtSink;

    // ── helper: build a Pdu with an explicit event_id for readability ──────────

    fn pdu(id: &str, prev: &[&str], depth: u64, ts: u64) -> Pdu {
        Pdu {
            event_id:    id.to_string(),
            room_id:     "!room:srv".to_string(),
            sender:      "@alice:srv".to_string(),
            kind:        "m.room.message".to_string(),
            content:     id.as_bytes().to_vec(),
            prev_events: prev.iter().map(|s| s.to_string()).collect(),
            depth,
            ts,
        }
    }

    // ── CRDT laws ─────────────────────────────────────────────────────────────

    // roomlog:idempotent:start
    //   purpose: merge(a, a) == a  (idempotence of join / add-only grow-set).
    //   input:  RoomLog with two events; merge with itself
    //   output: len unchanged, ordered() unchanged
    //   sideEffects: none
    // roomlog:idempotent:end
    #[test]
    fn roomlog_merge_idempotent() {
        let mut a = RoomLog::new();
        let e0 = pdu("$e0", &[], 0, 100);
        let e1 = pdu("$e1", &["$e0"], 1, 200);
        a.add(e0);
        a.add(e1);

        let snap_ids: Vec<String> = a.ordered().iter().map(|p| p.event_id.clone()).collect();
        let snap_len = a.len();

        let clone_a = a.clone();
        a.merge(&clone_a);

        assert_eq!(a.len(), snap_len, "idempotent: len unchanged");
        let ids_after: Vec<String> = a.ordered().iter().map(|p| p.event_id.clone()).collect();
        assert_eq!(ids_after, snap_ids, "idempotent: ordered() unchanged");
    }

    // roomlog:commutative:start
    //   purpose: merge(a, b).ordered() == merge(b, a).ordered()  (commutativity).
    //   input:  two RoomLogs with disjoint events; merge in both orders
    //   output: identical ordered() output
    //   sideEffects: none
    // roomlog:commutative:end
    #[test]
    fn roomlog_merge_commutative() {
        let e0 = pdu("$e0", &[], 0, 100);
        let e1 = pdu("$e1", &["$e0"], 1, 200);
        let e2 = pdu("$e2", &["$e0"], 1, 300); // concurrent with e1

        let mut a = RoomLog::new();
        a.add(e0.clone());
        a.add(e1.clone());

        let mut b = RoomLog::new();
        b.add(e0.clone());
        b.add(e2.clone());

        let mut ab = a.clone(); ab.merge(&b);
        let mut ba = b.clone(); ba.merge(&a);

        let ids_ab: Vec<String> = ab.ordered().iter().map(|p| p.event_id.clone()).collect();
        let ids_ba: Vec<String> = ba.ordered().iter().map(|p| p.event_id.clone()).collect();
        assert_eq!(ids_ab, ids_ba, "commutative: merge order must not affect ordered()");
    }

    // roomlog:associative:start
    //   purpose: merge(merge(a,b),c).ordered() == merge(a,merge(b,c)).ordered()
    //   input:  three RoomLogs; merge in two different groupings
    //   output: identical ordered() output
    //   sideEffects: none
    // roomlog:associative:end
    #[test]
    fn roomlog_merge_associative() {
        let e0 = pdu("$e0", &[], 0, 10);
        let e1 = pdu("$e1", &["$e0"], 1, 20);
        let e2 = pdu("$e2", &["$e1"], 2, 30);

        let mut a = RoomLog::new(); a.add(e0.clone());
        let mut b = RoomLog::new(); b.add(e1.clone());
        let mut c = RoomLog::new(); c.add(e2.clone());

        // (a ⊔ b) ⊔ c
        let mut ab = a.clone(); ab.merge(&b);
        let mut ab_c = ab; ab_c.merge(&c);

        // a ⊔ (b ⊔ c)
        let mut bc = b.clone(); bc.merge(&c);
        let mut a_bc = a; a_bc.merge(&bc);

        let ids1: Vec<String> = ab_c.ordered().iter().map(|p| p.event_id.clone()).collect();
        let ids2: Vec<String> = a_bc.ordered().iter().map(|p| p.event_id.clone()).collect();
        assert_eq!(ids1, ids2, "associative: both groupings yield same ordered()");
    }

    // roomlog:add_idempotent:start
    //   purpose: Adding the same PDU (same event_id) twice is a no-op — no duplicates.
    //   input:  add same pdu twice; check len and ordered()
    //   output: len == 1; ordered() contains event once
    //   sideEffects: none
    // roomlog:add_idempotent:end
    #[test]
    fn roomlog_add_duplicate_is_noop() {
        let e = pdu("$e0", &[], 0, 42);
        let mut log = RoomLog::new();
        log.add(e.clone());
        log.add(e.clone()); // same event_id — must be ignored
        assert_eq!(log.len(), 1, "duplicate add must not grow set");
        assert_eq!(log.ordered().len(), 1, "ordered() has no duplicates");
        assert_eq!(log.ordered()[0].event_id, "$e0");
    }

    // roomlog:convergence_different_order:start
    //   purpose: Two replicas that receive the same events in DIFFERENT orders
    //            converge to identical ordered() after delta exchange.
    //            This is the core CRDT convergence property for Matrix event history.
    //   input:  replica A receives [e0, e1, e2] in order; replica B receives [e2, e0, e1];
    //            then exchange full-state deltas via MemCrdtSink
    //   output: both replicas have len==3 and identical ordered()
    //   sideEffects: none
    // roomlog:convergence_different_order:end
    #[test]
    fn roomlog_convergence_different_arrival_order() {
        let e0 = pdu("$e0", &[], 0, 100);
        let e1 = pdu("$e1", &["$e0"], 1, 200);
        let e2 = pdu("$e2", &["$e1"], 2, 300);

        // replica A: receives in natural order
        let mut ra = RoomLog::new();
        ra.add(e0.clone());
        ra.add(e1.clone());
        ra.add(e2.clone());

        // replica B: receives in reverse order (e2 arrives before its ancestors)
        let mut rb = RoomLog::new();
        rb.add(e2.clone()); // forward-reference: e1 not yet present
        rb.add(e0.clone());
        rb.add(e1.clone());

        // At this point both have all events; order of arrival differs but sets are equal.
        // Exchange full-state deltas via MemCrdtSink.
        let (sink_a, sink_b) = MemCrdtSink::pair();
        ra.publish_delta(&sink_a, "room").expect("publish a");
        rb.publish_delta(&sink_b, "room").expect("publish b");
        ra.drain_delta(&sink_a, "room").expect("drain a");
        rb.drain_delta(&sink_b, "room").expect("drain b");

        assert_eq!(ra.len(), 3);
        assert_eq!(rb.len(), 3);

        let ids_a: Vec<String> = ra.ordered().iter().map(|p| p.event_id.clone()).collect();
        let ids_b: Vec<String> = rb.ordered().iter().map(|p| p.event_id.clone()).collect();
        assert_eq!(ids_a, ids_b, "convergence: ordered() must be identical on both replicas");
        assert_eq!(ids_a, vec!["$e0", "$e1", "$e2"]);
    }

    // roomlog:convergence_partial_deltas:start
    //   purpose: Convergence under interleaved partial deltas.
    //            Replica A only knows [e0, e1]; replica B only knows [e1, e2].
    //            After delta exchange both learn all three events and converge.
    //   input:  partial RoomLogs; exchange deltas
    //   output: both replicas have len==3, identical ordered()
    //   sideEffects: none
    // roomlog:convergence_partial_deltas:end
    #[test]
    fn roomlog_convergence_partial_deltas() {
        let e0 = pdu("$e0", &[], 0, 10);
        let e1 = pdu("$e1", &["$e0"], 1, 20);
        let e2 = pdu("$e2", &["$e1"], 2, 30);

        let mut ra = RoomLog::new();
        ra.add(e0.clone());
        ra.add(e1.clone());

        let mut rb = RoomLog::new();
        rb.add(e1.clone());
        rb.add(e2.clone());

        // Exchange.
        let delta_a = ra.delta();
        let delta_b = rb.delta();
        ra.apply_delta(&delta_b);
        rb.apply_delta(&delta_a);

        assert_eq!(ra.len(), rb.len(), "same len after exchange");
        assert_eq!(ra.len(), 3);
        let ids_a: Vec<String> = ra.ordered().iter().map(|p| p.event_id.clone()).collect();
        let ids_b: Vec<String> = rb.ordered().iter().map(|p| p.event_id.clone()).collect();
        assert_eq!(ids_a, ids_b, "convergence via partial deltas");
        assert_eq!(ids_a, vec!["$e0", "$e1", "$e2"]);
    }

    // roomlog:forward_reference_deferred:start
    //   purpose: An event referencing an unknown prev_event is deferred from
    //            ordered() until the prev arrives; after arrival it appears in
    //            correct topological position.
    //   input:  add e1 (refs e0) before adding e0; check ordered() before and after
    //   output: before e0 arrives: ordered() is empty (e1 deferred);
    //           after e0 added: ordered() == [e0, e1]
    //   sideEffects: none
    // roomlog:forward_reference_deferred:end
    #[test]
    fn roomlog_forward_reference_deferred_until_prev_arrives() {
        let e0 = pdu("$e0", &[], 0, 100);
        let e1 = pdu("$e1", &["$e0"], 1, 200);

        let mut log = RoomLog::new();
        log.add(e1.clone()); // arrives before its prev

        // e1 references e0 which is not in the log yet → deferred → ordered() empty
        let ids_before: Vec<String> = log.ordered().iter().map(|p| p.event_id.clone()).collect();
        assert!(ids_before.is_empty(), "e1 must be deferred until e0 arrives; got: {:?}", ids_before);

        // Now e0 arrives
        log.add(e0);
        let ids_after: Vec<String> = log.ordered().iter().map(|p| p.event_id.clone()).collect();
        assert_eq!(ids_after, vec!["$e0", "$e1"], "after e0 arrives, both events ordered correctly");
    }

    // roomlog:concurrent_branches:start
    //   purpose: Two concurrent branches (both reference e0, neither references the other)
    //            are both included in ordered(); tie-break by (depth, ts, event_id) is stable.
    //   input:  e0 (root), e1a and e1b both reference e0 (concurrent), e2 references both
    //   output: ordered() == [e0, <e1a or e1b by ts>, <other e1>, e2]
    //            deterministic across multiple calls
    //   sideEffects: none
    // roomlog:concurrent_branches:end
    #[test]
    fn roomlog_concurrent_branches_deterministic_order() {
        let e0  = pdu("$e0",  &[], 0, 100);
        // e1a and e1b are concurrent: same depth, different ts → ts order
        let e1a = pdu("$e1a", &["$e0"], 1, 200);
        let e1b = pdu("$e1b", &["$e0"], 1, 300); // higher ts → comes after e1a
        let e2  = pdu("$e2",  &["$e1a", "$e1b"], 2, 400);

        let mut log = RoomLog::new();
        // Add in arbitrary order
        log.add(e1b.clone());
        log.add(e2.clone());
        log.add(e0.clone());
        log.add(e1a.clone());

        let ids: Vec<String> = log.ordered().iter().map(|p| p.event_id.clone()).collect();
        assert_eq!(ids.len(), 4, "all four events present");
        assert_eq!(ids[0], "$e0", "root first");
        // e1a (ts=200) before e1b (ts=300) — both at depth=1
        assert_eq!(ids[1], "$e1a");
        assert_eq!(ids[2], "$e1b");
        assert_eq!(ids[3], "$e2", "e2 last (both parents present)");

        // Determinism: calling ordered() again returns identical result
        let ids2: Vec<String> = log.ordered().iter().map(|p| p.event_id.clone()).collect();
        assert_eq!(ids, ids2, "ordered() is deterministic across calls");
    }

    // roomlog:compute_id_deterministic:start
    //   purpose: compute_id returns the same event_id for identical inputs —
    //            content-addressing is deterministic (no wall-clock, no RNG).
    //   input:  same args twice; also verify different content yields different id
    //   output: two calls → same id; different content → different id
    //   sideEffects: none
    // roomlog:compute_id_deterministic:end
    #[test]
    fn roomlog_compute_id_deterministic() {
        let id1 = Pdu::compute_id("!r:s", "@a:s", "m.message", b"hello", &[], 0, 42);
        let id2 = Pdu::compute_id("!r:s", "@a:s", "m.message", b"hello", &[], 0, 42);
        assert_eq!(id1, id2, "same inputs → same id");

        let id3 = Pdu::compute_id("!r:s", "@a:s", "m.message", b"world", &[], 0, 42);
        assert_ne!(id1, id3, "different content → different id");
    }

    // roomlog:delta_serialisation_roundtrip:start
    //   purpose: delta_to_bytes / delta_from_bytes round-trip preserves all PDU fields.
    //   input:  a RoomLog with two PDUs; serialise delta and deserialise
    //   output: reconstructed delta has identical PDU fields
    //   sideEffects: none
    // roomlog:delta_serialisation_roundtrip:end
    #[test]
    fn roomlog_delta_serialisation_roundtrip() {
        let e0 = pdu("$e0", &[], 0, 10);
        let e1 = pdu("$e1", &["$e0"], 1, 20);

        let mut log = RoomLog::new();
        log.add(e0.clone());
        log.add(e1.clone());

        let delta = log.delta();
        let bytes = delta_to_bytes(&delta);
        let delta2 = delta_from_bytes(&bytes);

        // Both PDUs survived the round-trip.
        let by_id: HashMap<&str, &Pdu> =
            delta2.pdus.iter().map(|p| (p.event_id.as_str(), p)).collect();
        let p0 = by_id["$e0"];
        let p1 = by_id["$e1"];
        assert_eq!(p0.room_id, "!room:srv");
        assert_eq!(p0.depth, 0);
        assert_eq!(p0.ts, 10);
        assert!(p0.prev_events.is_empty());
        assert_eq!(p1.prev_events, vec!["$e0"]);
        assert_eq!(p1.depth, 1);
        assert_eq!(p1.ts, 20);
    }

    // roomlog:sink_convergence_via_mem:start
    //   purpose: Two RoomLogs exchange deltas via MemCrdtSink and converge —
    //            end-to-end proof including serialisation round-trip.
    //   input:  ra has [e0, e1]; rb has [e0, e2]; exchange via sink
    //   output: both replicas have len==3, identical ordered()
    //   sideEffects: none
    // roomlog:sink_convergence_via_mem:end
    #[test]
    fn roomlog_sink_convergence_via_mem() {
        let e0 = pdu("$e0", &[], 0, 1);
        let e1 = pdu("$e1", &["$e0"], 1, 2);
        let e2 = pdu("$e2", &["$e0"], 1, 3); // concurrent with e1

        let mut ra = RoomLog::new();
        ra.add(e0.clone());
        ra.add(e1.clone());

        let mut rb = RoomLog::new();
        rb.add(e0.clone());
        rb.add(e2.clone());

        let (sink_a, sink_b) = MemCrdtSink::pair();

        // Publish deltas cross-way.
        ra.publish_delta(&sink_a, "!room:srv").expect("publish a");
        rb.publish_delta(&sink_b, "!room:srv").expect("publish b");

        // Drain.
        let na = ra.drain_delta(&sink_a, "!room:srv").expect("drain a");
        let nb = rb.drain_delta(&sink_b, "!room:srv").expect("drain b");
        assert_eq!(na, 1, "a drained 1 delta blob");
        assert_eq!(nb, 1, "b drained 1 delta blob");

        assert_eq!(ra.len(), 3);
        assert_eq!(rb.len(), 3);

        let ids_a: Vec<String> = ra.ordered().iter().map(|p| p.event_id.clone()).collect();
        let ids_b: Vec<String> = rb.ordered().iter().map(|p| p.event_id.clone()).collect();
        assert_eq!(ids_a, ids_b, "converged via MemCrdtSink");
        // e0 first (depth 0), then e1 (ts=2) before e2 (ts=3) at depth 1
        assert_eq!(ids_a[0], "$e0");
        assert_eq!(ids_a[1], "$e1");
        assert_eq!(ids_a[2], "$e2");
    }
}
