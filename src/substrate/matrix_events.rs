// START_AI_HEADER
// MODULE: mrgd/src/matrix_events.rs
// PURPOSE: Matrix event-DAG as a CRDT (Stage 0 — events only, no state-res).
//          A Matrix room's history is modelled as a grow-only set of PDUs keyed by
//          content-addressed event_id.  The set is a join-semilattice (add-only,
//          union on merge) — idempotent, commutative, associative.
//          `ordered()` returns a deterministic topological sort of the causal DAG
//          defined by `prev_events` references; tie-break: (depth, ts, event_id).
//          Two replicas that receive the same PDUs in any order converge to
//          identical `ordered()` output (Strong Eventual Consistency).
//
//          Security additions (P1):
//            - event_id: sha256-based content-address (collision-resistant).
//              Format: "$" + base64url_nopad(sha256(canonical_bytes)).
//              Replaces FNV-1a-64 which is forgeable/collidable.
//            - sig: optional ed25519 signature over canonical_bytes.
//            - signer_node: node_id that produced the signature.
//            - apply_delta_verified(): cluster-mode delta application that rejects
//              unsigned, badly-signed, or unknown-signer PDUs.
//            Locally-created PDUs (via Pdu::signed()) are self-signed and carry the
//            creator's node_id in signer_node.  The default Pdu::new() creates an
//            unsigned PDU for backward-compatible in-process use.
//
// INTENT: PoC demonstrating CRDT-laws + convergence for Matrix-event history;
//         reuses CrdtSink / MemCrdtSink from crdt.rs for delta exchange.
//         State-resolution (room-state) is explicitly out of scope (next slice).
// DEPENDENCIES: std, sha2, base64, crate::substrate::crdt::{CrdtSink, CrdtError},
//               crate::substrate::node_auth::{canonical_bytes, NodeSigner, NodeKeyStore}
// PUBLIC_API: Pdu, RoomLogDelta, RoomLog, delta_to_bytes, delta_from_bytes
// END_AI_HEADER

use crate::substrate::crdt::{CrdtError, CrdtSink};
use crate::substrate::node_auth::{canonical_bytes as pdu_canonical_bytes, NodeKeyStore, NodeSigner};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet, VecDeque};

// ═══════════════════════════════════════════════════════════════════════════════
// Pdu — immutable Protocol Data Unit (Matrix event, content-addressed)
// ═══════════════════════════════════════════════════════════════════════════════

// Pdu:start
//   purpose: Immutable Matrix PDU.  `event_id` is a collision-resistant sha256
//            content-address of (room_id, sender, kind, content, sorted prev_events,
//            depth, ts).  `sig` and `signer_node` carry an optional ed25519 signature
//            so receivers can authenticate the PDU origin.  An empty `signer_node`
//            with empty `sig` means unsigned (in-process / legacy).
//   input:  all fields explicit; `compute_id()` derives event_id from canonical_bytes;
//           `Pdu::new()` creates unsigned; `Pdu::signed()` signs with a NodeSigner.
//   output: Pdu value (Clone+PartialEq+Eq+Hash via event_id)
//   sideEffects: none (pure value)
// Pdu:end
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pdu {
    /// Collision-resistant content address: "$" + base64url_nopad(sha256(canonical_bytes)).
    pub event_id: String,
    pub room_id: String,
    pub sender: String,
    pub kind: String,
    /// Opaque event content (JSON body, Cap'n Proto payload, etc.).
    pub content: Vec<u8>,
    /// Causal parents: event_ids this PDU directly follows in the DAG.
    pub prev_events: Vec<String>,
    /// Depth in the DAG (0 = room-create event, monotone).
    pub depth: u64,
    /// Origin server timestamp injected by sender (not Date::now in tests).
    pub ts: u64,
    /// ed25519 signature (64 bytes) over canonical_bytes.  Empty = unsigned.
    pub sig: Vec<u8>,
    /// node_id of the signing node.  Empty = unsigned.
    pub signer_node: String,
}

impl Pdu {
    // Pdu::compute_id:start
    //   purpose: Derive a deterministic, collision-resistant event_id from content
    //            fields using SHA-256.  Format: "$" + base64url_nopad(sha256(bytes)).
    //            Replaces FNV-1a-64 (which was forgeable and collidable) for P1 security.
    //            The canonical bytes are the same layout as node_auth::canonical_bytes.
    //   input:  room_id, sender, kind, content, prev_events, depth, ts — all by reference
    //   output: String — "$" + base64url-nopad-encoded SHA-256 (starts with "$")
    //   sideEffects: none
    // Pdu::compute_id:end
    pub fn compute_id(
        room_id: &str,
        sender: &str,
        kind: &str,
        content: &[u8],
        prev_events: &[String],
        depth: u64,
        ts: u64,
    ) -> String {
        let bytes = pdu_canonical_bytes(room_id, sender, kind, content, prev_events, depth, ts);
        let hash = Sha256::digest(&bytes);
        format!("${}", URL_SAFE_NO_PAD.encode(hash))
    }

    // Pdu::new:start
    //   purpose: Construct an UNSIGNED Pdu with a sha256 event_id.
    //            Caller injects ts and sender; no Date::now.
    //            sig and signer_node are empty (unsigned).
    //            Use Pdu::signed() when a NodeSigner is available.
    //   input:  room_id, sender, kind, content, prev_events, depth, ts — all owned
    //   output: Pdu with sha256 event_id, empty sig+signer_node
    //   sideEffects: none
    // Pdu::new:end
    pub fn new(
        room_id: String,
        sender: String,
        kind: String,
        content: Vec<u8>,
        prev_events: Vec<String>,
        depth: u64,
        ts: u64,
    ) -> Self {
        let event_id =
            Self::compute_id(&room_id, &sender, &kind, &content, &prev_events, depth, ts);
        Pdu {
            event_id,
            room_id,
            sender,
            kind,
            content,
            prev_events,
            depth,
            ts,
            sig: Vec::new(),
            signer_node: String::new(),
        }
    }

    // Pdu::signed:start
    //   purpose: Construct a SIGNED Pdu.  Computes event_id via sha256, then signs
    //            the same canonical bytes with the provided NodeSigner.
    //            signer_node is set to signer.node_id.
    //   input:  room_id, sender, kind, content, prev_events, depth, ts — all owned;
    //           signer — &NodeSigner for this node
    //   output: Pdu with sha256 event_id + ed25519 sig + signer_node
    //   sideEffects: none
    // Pdu::signed:end
    #[allow(clippy::too_many_arguments)]
    pub fn signed(
        room_id: String,
        sender: String,
        kind: String,
        content: Vec<u8>,
        prev_events: Vec<String>,
        depth: u64,
        ts: u64,
        signer: &NodeSigner,
    ) -> Self {
        let cbytes =
            pdu_canonical_bytes(&room_id, &sender, &kind, &content, &prev_events, depth, ts);
        let event_id = format!("${}", URL_SAFE_NO_PAD.encode(Sha256::digest(&cbytes)));
        let sig = signer.sign(&cbytes);
        let signer_node = signer.node_id.clone();
        Pdu {
            event_id,
            room_id,
            sender,
            kind,
            content,
            prev_events,
            depth,
            ts,
            sig,
            signer_node,
        }
    }

    // Pdu::is_signed:start
    //   purpose: Return true iff this PDU carries a signature (sig non-empty and
    //            signer_node non-empty).  Unsigned PDUs (from in-process tests or
    //            legacy paths) return false.
    //   input:  none
    //   output: bool
    //   sideEffects: none
    // Pdu::is_signed:end
    pub fn is_signed(&self) -> bool {
        !self.sig.is_empty() && !self.signer_node.is_empty()
    }

    // Pdu::verify_sig:start
    //   purpose: Verify this PDU's signature against the NodeKeyStore, AND bind the
    //            claimed sender to the signing node's namespace (P1.1 internal-task item 2).
    //            Returns true iff ALL of:
    //              1. the PDU is signed (non-empty sig/signer_node);
    //              2. the store knows signer_node AND the ed25519 sig verifies over
    //                 this PDU's canonical_bytes;
    //              3. domain(sender) == signer_node, where domain is the part of
    //                 "@localpart:domain" after the FIRST ':' — a node may only sign
    //                 PDUs whose sender is homed on its own server_name.  This confines
    //                 a correctly-keyed node to its own namespace: it cannot forge
    //                 events on behalf of users homed on another node, even though the
    //                 crypto check alone would accept any sender string.
    //            The sender-binding check runs AFTER the crypto check (cheap first,
    //            reject fast on the common bad-sig / unknown-node cases).
    //   input:  key_store — &NodeKeyStore containing the claimed signer's pubkey
    //   output: bool — true = valid sig AND sender domain matches signer_node;
    //                  false = unsigned, bad sig, unknown node, or sender/signer mismatch
    //   sideEffects: none
    // Pdu::verify_sig:end
    pub fn verify_sig(&self, key_store: &NodeKeyStore) -> bool {
        if !self.is_signed() {
            return false;
        }
        let cbytes = pdu_canonical_bytes(
            &self.room_id,
            &self.sender,
            &self.kind,
            &self.content,
            &self.prev_events,
            self.depth,
            self.ts,
        );
        if !key_store.verify(&self.signer_node, &cbytes, &self.sig) {
            return false;
        }
        // Sender-binding: the signing node may only assert senders homed on its own
        // domain.  sender must be "@<localpart>:<domain>"; domain is everything after
        // the FIRST ':'.  A missing ':' (malformed sender) is rejected.
        match self.sender.split_once(':') {
            Some((_, domain)) => domain == self.signer_node,
            None => false,
        }
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
#[derive(Debug, Clone, Default)]
pub struct RoomLogDelta {
    pub pdus: Vec<Pdu>,
    /// GC watermark of the sender: every event at depth <= this has been collected
    /// and must never be re-added. Travels with the delta so the watermark converges
    /// by max() over the same paths the events themselves take — no separate channel,
    /// and no way to receive events from a peer without also learning what it dropped.
    /// 0 means "nothing collected", which is what every pre-GC sender reports.
    pub collected_depth: u64,
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
//   input:  `add(pdu)`, `merge(&other)`, `apply_delta`, `apply_delta_verified`,
//           `delta()`, `ordered()`
//   output: convergent room history; `ordered()` identical on all fully-synced replicas
//   sideEffects: mutates internal HashMap on add/merge/apply_delta
// RoomLog:end
#[derive(Debug, Clone, Default)]
pub struct RoomLog {
    /// The grow-only set: event_id → Pdu.
    events: HashMap<String, Pdu>,
    /// GC watermark: every event at depth <= this has been collected. Grow-only
    /// (merges by max), which is what keeps garbage collection convergent — see
    /// collect_below.
    collected_depth: u64,
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

    // RoomLog::contains_event_id:start
    //   purpose: O(1) membership test against the grow-only event set, without
    //            materialising a Vec of references or cloning any event_id. The sweep
    //            merge path used to build two `HashSet<String>` per room per cycle
    //            (before/after) purely to ask this question, which was 8.87 MB of the
    //            11.3 MB a sweep allocated on the stand.
    //   input:  event_id — &str to look for
    //   output: bool — whether the log already holds this event
    //   sideEffects: none
    // RoomLog::contains_event_id:end
    pub fn contains_event_id(&self, event_id: &str) -> bool {
        self.events.contains_key(event_id)
    }

    // RoomLog::add:start
    //   purpose: Add a PDU to the grow-only set.
    //            If event_id already exists the add is silently ignored (idempotent).
    //            Does NOT validate prev_events references — forward-references are
    //            allowed; `ordered()` defers them until parents arrive.
    //            Does NOT verify signatures — use apply_delta_verified for network PDUs.
    //   input:  pdu — PDU to add
    //   output: none
    //   sideEffects: inserts into self.events if event_id is new
    // RoomLog::add:end
    pub fn add(&mut self, pdu: Pdu) {
        if self.is_collected(&pdu) {
            return;
        }
        self.events.entry(pdu.event_id.clone()).or_insert(pdu);
    }

    // RoomLog::is_collected:start
    //   purpose: Has this PDU already been garbage-collected here? Anything at or
    //            below the watermark has, and must never come back — a peer that has
    //            not collected yet will keep offering it on every catch-up pass, and
    //            without this check the log would refill itself as fast as it was
    //            pruned.
    //   input:  pdu
    //   output: true if the PDU is at or below the collected watermark
    //   sideEffects: none
    // RoomLog::is_collected:end
    fn is_collected(&self, pdu: &Pdu) -> bool {
        self.collected_depth > 0 && pdu.depth <= self.collected_depth
    }

    // RoomLog::collected_depth:start
    //   purpose: The GC watermark: every event at depth <= this has been collected.
    //   input:  none
    //   output: u64 (0 = nothing collected)
    //   sideEffects: none
    // RoomLog::collected_depth:end
    pub fn collected_depth(&self) -> u64 {
        self.collected_depth
    }

    // RoomLog::collect_below:start
    //   purpose: Garbage-collect: drop every event at depth <= `depth` and raise the
    //            watermark so they cannot return.
    //
    //            Deleting from a grow-only set is not normally possible — a peer that
    //            still holds the events hands them straight back on the next merge,
    //            and the log oscillates instead of shrinking. The watermark is what
    //            makes it convergent: it is itself grow-only (merges by max), it
    //            travels with every delta, and both sides reject anything at or below
    //            it. So the collect decision spreads exactly like an event does, and
    //            two nodes that collect different amounts converge on the deeper cut
    //            rather than fighting.
    //
    //            Monotonic: a lower `depth` than the current watermark is ignored.
    //   input:  depth — collect everything at or below this depth
    //   output: number of events actually removed
    //   sideEffects: removes events; raises collected_depth
    // RoomLog::collect_below:end
    pub fn collect_below(&mut self, depth: u64) -> usize {
        if depth <= self.collected_depth {
            return 0;
        }
        self.collected_depth = depth;
        let before = self.events.len();
        self.events.retain(|_, p| p.depth > depth);
        before - self.events.len()
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
        // Adopt the deeper cut FIRST, so events the other side has already collected
        // are not briefly re-inserted here.
        self.collect_below(other.collected_depth);
        for (id, pdu) in &other.events {
            if self.is_collected(pdu) {
                continue;
            }
            self.events.entry(id.clone()).or_insert_with(|| pdu.clone());
        }
    }

    // RoomLog::delta:start
    //   purpose: Produce a full-state delta (all PDUs in this log).
    //            Recipients call apply_delta; applying the same delta twice is
    //            idempotent (grow-only guarantees).
    //   input:  none
    //   output: RoomLogDelta containing clones of all PDUs
    //   sideEffects: none
    // RoomLog::delta:end
    pub fn delta(&self) -> RoomLogDelta {
        RoomLogDelta {
            pdus: self.events.values().cloned().collect(),
            collected_depth: self.collected_depth,
        }
    }

    // RoomLog::apply_delta:start
    //   purpose: Merge a received RoomLogDelta into this log WITHOUT signature verification.
    //            Suitable for in-process (trusted) paths: local PDUs, test helpers,
    //            single-node persistence replay.  For network PDUs from untrusted peers
    //            use apply_delta_verified().
    //            Equivalent to `add(pdu)` for each pdu in the delta (idempotent).
    //   input:  delta — RoomLogDelta received from a trusted source
    //   output: none
    //   sideEffects: inserts new PDUs; ignores duplicates
    // RoomLog::apply_delta:end
    pub fn apply_delta(&mut self, delta: &RoomLogDelta) {
        self.collect_below(delta.collected_depth);
        for pdu in &delta.pdus {
            if self.is_collected(pdu) {
                continue;
            }
            self.events
                .entry(pdu.event_id.clone())
                .or_insert_with(|| pdu.clone());
        }
    }

    // RoomLog::apply_delta_verified:start
    //   purpose: Merge a received RoomLogDelta with full signature verification.
    //            For each incoming PDU:
    //              1. If unsigned (empty sig or empty signer_node) → REJECT (log + skip).
    //              2. If signer_node not in key_store → REJECT (unknown node, log + skip).
    //              3. If sig does not verify → REJECT (bad signature, log + skip).
    //              4. Otherwise → accept (add to log).
    //            Returns (accepted, rejected) counts.
    //            This is the ONLY path for PDUs arriving from the Zenoh mesh.
    //            Locally-created PDUs that were signed with Pdu::signed() pass because
    //            the own-node pubkey is inserted into key_store at NodeSigner creation.
    //   input:  delta — RoomLogDelta from network;
    //           key_store — &NodeKeyStore with trusted pubkeys
    //   output: (accepted: usize, rejected: usize)
    //   sideEffects: inserts verified PDUs; logs rejections to stderr
    // RoomLog::apply_delta_verified:end
    pub fn apply_delta_verified(
        &mut self,
        delta: &RoomLogDelta,
        key_store: &NodeKeyStore,
    ) -> (usize, usize) {
        let mut accepted = 0usize;
        let mut rejected = 0usize;
        self.collect_below(delta.collected_depth);
        for pdu in &delta.pdus {
            // Already garbage-collected here: not an error, and NOT counted as a
            // rejection — a rejection means "this PDU is bad", and a peer resending
            // history we chose to drop is neither bad nor worth logging every pass.
            if self.is_collected(pdu) {
                continue;
            }
            if !pdu.is_signed() {
                eprintln!(
                    "[matrix_events] REJECT PDU {}: unsigned (empty sig or signer_node)",
                    pdu.event_id
                );
                rejected += 1;
                continue;
            }
            if !pdu.verify_sig(key_store) {
                eprintln!(
                    "[matrix_events] REJECT PDU {}: bad/unknown sig from node {:?}",
                    pdu.event_id, pdu.signer_node
                );
                rejected += 1;
                continue;
            }
            // The signature proves the SENDER wrote this content; it does not prove
            // the event_id belongs to it, because event_id is not part of the signed
            // pre-image. Without this check a node whose key we have pinned can file
            // a validly-signed PDU under any id it likes — and dedup, topological
            // ordering, known_ids filtering and the redaction table all key on that
            // field, so a chosen id lets a trusted-but-buggy node shadow an existing
            // event or detach a redaction from its target.
            //
            // Safe against real history: every field feeding compute_id is already
            // covered by the signature just verified, and honestly-built PDUs derive
            // id and signature from the same canonical bytes (Pdu::signed). Unsigned
            // PDUs — including the pre-internal-task replay fallback in persist.rs — are
            // rejected above and never reach here.
            let expected_id = Pdu::compute_id(
                &pdu.room_id,
                &pdu.sender,
                &pdu.kind,
                &pdu.content,
                &pdu.prev_events,
                pdu.depth,
                pdu.ts,
            );
            if pdu.event_id != expected_id {
                eprintln!(
                    "[matrix_events] REJECT PDU {}: event_id is not the content address \
                     of its own canonical bytes (expected {expected_id}, signer {:?})",
                    pdu.event_id, pdu.signer_node
                );
                rejected += 1;
                continue;
            }
            self.events
                .entry(pdu.event_id.clone())
                .or_insert_with(|| pdu.clone());
            accepted += 1;
        }
        (accepted, rejected)
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
                } else if self.collected_depth > 0 && pdu.depth <= self.collected_depth + 1 {
                    // Missing because it was COLLECTED, not because it has yet to
                    // arrive. depth is assigned as parent+1 (routes/send.rs), so every
                    // prev of an event at depth d sits at d-1; a missing prev is
                    // therefore collected exactly when d <= watermark+1. Treat it as
                    // satisfied — otherwise garbage-collecting the tail of a room
                    // would silently hide the whole surviving head behind it.
                    // Anything deeper is a genuine forward reference and still blocks.
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
                    let deg = in_degree
                        .get_mut(succ.event_id.as_str())
                        .expect("in_degree entry");
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
            a.depth
                .cmp(&b.depth)
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
    //   purpose: Drain pending delta bytes from a CrdtSink and apply each one
    //            WITHOUT signature verification (trusted / in-process path).
    //            For verified cluster paths use drain_delta_verified().
    //   input:  sink — CrdtSink impl; key — routing key
    //   output: Result<usize, CrdtError> — number of deltas applied
    //   sideEffects: mutates self via apply_delta for each received blob
    // RoomLog::drain_delta:end
    pub fn drain_delta(&mut self, sink: &dyn CrdtSink, key: &str) -> Result<usize, CrdtError> {
        let blobs = sink.drain(key)?;
        let mut count = 0usize;
        for bytes in &blobs {
            // A blob that will not parse is dropped, not applied — and not counted,
            // since the count is "deltas applied".
            let Some(delta) = delta_from_bytes(bytes) else {
                continue;
            };
            self.apply_delta(&delta);
            count += 1;
        }
        Ok(count)
    }

    // RoomLog::drain_delta_verified:start
    //   purpose: Drain pending delta bytes from a CrdtSink and apply each one WITH
    //            signature verification (cluster / network path).
    //            Each deserialized PDU is passed through apply_delta_verified().
    //            PDUs that fail verification are counted but not added to the log.
    //   input:  sink — CrdtSink impl; key — routing key;
    //           key_store — &NodeKeyStore with trusted pubkeys
    //   output: Result<(accepted: usize, rejected: usize), CrdtError>
    //   sideEffects: mutates self via apply_delta_verified for each blob
    // RoomLog::drain_delta_verified:end
    pub fn drain_delta_verified(
        &mut self,
        sink: &dyn CrdtSink,
        key: &str,
        key_store: &NodeKeyStore,
    ) -> Result<(usize, usize), CrdtError> {
        let blobs = sink.drain(key)?;
        let mut total_accepted = 0usize;
        let mut total_rejected = 0usize;
        for bytes in &blobs {
            // An unparseable blob counts as one rejection: the caller's rejection
            // counter is its signal that something on the wire is wrong, and a
            // malformed blob is exactly that. Its PDU count is unknowable.
            let Some(delta) = delta_from_bytes(bytes) else {
                eprintln!(
                    "[matrix_events] REJECT blob on {key}: malformed delta ({} bytes)",
                    bytes.len()
                );
                total_rejected += 1;
                continue;
            };
            let (a, r) = self.apply_delta_verified(&delta, key_store);
            total_accepted += a;
            total_rejected += r;
        }
        Ok((total_accepted, total_rejected))
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
//              [room_id_len:  u16 LE][room_id bytes]
//              [sender_len:   u16 LE][sender bytes]
//              [kind_len:     u16 LE][kind bytes]
//              [content_len:  u32 LE][content bytes]
//              [n_prevs:      u16 LE] ([prev_len: u16 LE][prev bytes])*
//              [depth:        u64 LE]
//              [ts:           u64 LE]
//              [signer_node_len: u16 LE][signer_node bytes]
//              [sig_len:      u16 LE][sig bytes]   (0 if unsigned)
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
        // P1 additions: signer_node and sig.
        write_str(&mut buf, &pdu.signer_node);
        write_bytes16(&mut buf, &pdu.sig);
    }
    // GC watermark, appended AFTER the pdu blocks. A reader that stops at n_pdus
    // simply never sees it, so an older peer keeps working; a reader that finds no
    // trailing bytes treats the sender as having collected nothing.
    buf.extend_from_slice(&delta.collected_depth.to_le_bytes());
    buf
}

// delta_from_bytes:start
//   purpose: Deserialise a binary blob produced by delta_to_bytes back into a
//            RoomLogDelta.  Returns None on ANY malformed input: a truncated
//            blob, a length prefix that overruns the buffer, or a non-UTF-8
//            string.
//
//            None rather than a panic is load-bearing, not tidiness.  This
//            decodes UNTRUSTED NETWORK BYTES on both receive paths — the
//            cluster drain in routes/sync.rs and the catch-up merge in main.rs
//            — and it runs BEFORE any signature check, so the caller has to be
//            able to drop a bad blob instead of dying on it.  An earlier
//            version indexed the slice directly (buf[off..off+n]) and ended in
//            expect("utf8"), which let anyone able to publish on the Zenoh
//            prefix take down a /sync handler with five bytes and no key at
//            all.  Every JSON channel in this codebase already skips malformed
//            payloads; this one now does too.
//   input:  bytes — slice of bytes from delta_to_bytes
//   output: Some(RoomLogDelta), or None if bytes are not a well-formed delta
//   sideEffects: none
// delta_from_bytes:end
pub fn delta_from_bytes(bytes: &[u8]) -> Option<RoomLogDelta> {
    // Smallest a pdu_block can be: four u16-prefixed empty strings, an empty
    // u32-prefixed content, an empty prev list, depth, ts, then two more empty
    // length-prefixed fields.  Used only to bound the capacity hint — a count
    // read off the wire must never size an allocation by itself, or a 4-byte
    // blob claiming u32::MAX pdus asks for an allocation instead of a parse
    // error.
    const MIN_PDU_BLOCK: usize = 2 + 2 + 2 + 2 + 4 + 2 + 8 + 8 + 2 + 2;

    let mut off = 0usize;
    let n_pdus = read_u32(bytes, &mut off)? as usize;
    let mut pdus = Vec::with_capacity(n_pdus.min(bytes.len() / MIN_PDU_BLOCK));
    for _ in 0..n_pdus {
        let event_id = read_str(bytes, &mut off)?;
        let room_id = read_str(bytes, &mut off)?;
        let sender = read_str(bytes, &mut off)?;
        let kind = read_str(bytes, &mut off)?;
        let content = read_bytes32(bytes, &mut off)?;
        let n_prevs = read_u16(bytes, &mut off)? as usize;
        // Same reasoning as MIN_PDU_BLOCK: each prev costs at least its u16 length.
        let mut prev_events = Vec::with_capacity(n_prevs.min(bytes.len().saturating_sub(off) / 2));
        for _ in 0..n_prevs {
            prev_events.push(read_str(bytes, &mut off)?);
        }
        let depth = read_u64(bytes, &mut off)?;
        let ts = read_u64(bytes, &mut off)?;
        // P1 additions.
        let signer_node = read_str(bytes, &mut off)?;
        let sig = read_bytes16(bytes, &mut off)?;
        pdus.push(Pdu {
            event_id,
            room_id,
            sender,
            kind,
            content,
            prev_events,
            depth,
            ts,
            signer_node,
            sig,
        });
    }
    // Absent trailing watermark = a peer that predates GC, i.e. "collected
    // nothing". Distinct from a malformed blob, so unwrap_or rather than `?`.
    let collected_depth = read_u64(bytes, &mut off).unwrap_or(0);
    Some(RoomLogDelta {
        pdus,
        collected_depth,
    })
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

fn write_bytes16(buf: &mut Vec<u8>, b: &[u8]) {
    buf.extend_from_slice(&(b.len() as u16).to_le_bytes());
    buf.extend_from_slice(b);
}

// Every reader below returns None rather than indexing the slice directly: these
// run on untrusted network bytes (see delta_from_bytes). On None the offset may
// be left partly advanced, which is harmless because the caller discards the
// whole blob.

fn read_u16(buf: &[u8], off: &mut usize) -> Option<u16> {
    let end = off.checked_add(2)?;
    let v = u16::from_le_bytes(buf.get(*off..end)?.try_into().ok()?);
    *off = end;
    Some(v)
}

fn read_u32(buf: &[u8], off: &mut usize) -> Option<u32> {
    let end = off.checked_add(4)?;
    let v = u32::from_le_bytes(buf.get(*off..end)?.try_into().ok()?);
    *off = end;
    Some(v)
}

fn read_u64(buf: &[u8], off: &mut usize) -> Option<u64> {
    let end = off.checked_add(8)?;
    let v = u64::from_le_bytes(buf.get(*off..end)?.try_into().ok()?);
    *off = end;
    Some(v)
}

fn read_str(buf: &[u8], off: &mut usize) -> Option<String> {
    let len = read_u16(buf, off)? as usize;
    let end = off.checked_add(len)?;
    let s = std::str::from_utf8(buf.get(*off..end)?).ok()?.to_string();
    *off = end;
    Some(s)
}

fn read_bytes32(buf: &[u8], off: &mut usize) -> Option<Vec<u8>> {
    let len = read_u32(buf, off)? as usize;
    let end = off.checked_add(len)?;
    let b = buf.get(*off..end)?.to_vec();
    *off = end;
    Some(b)
}

fn read_bytes16(buf: &[u8], off: &mut usize) -> Option<Vec<u8>> {
    let len = read_u16(buf, off)? as usize;
    let end = off.checked_add(len)?;
    let b = buf.get(*off..end)?.to_vec();
    *off = end;
    Some(b)
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use crate::substrate::crdt::MemCrdtSink;

    // ── helper: build a Pdu with an explicit event_id for readability ──────────
    // NOTE: these helpers create PDUs with manually-specified event_ids (like "$e0")
    // for DAG structure tests.  They set sig/signer_node to empty (unsigned).

    fn pdu(id: &str, prev: &[&str], depth: u64, ts: u64) -> Pdu {
        Pdu {
            event_id: id.to_string(),
            room_id: "!room:srv".to_string(),
            sender: "@alice:srv".to_string(),
            kind: "m.room.message".to_string(),
            content: id.as_bytes().to_vec(),
            prev_events: prev.iter().map(|s| s.to_string()).collect(),
            depth,
            ts,
            sig: Vec::new(),
            signer_node: String::new(),
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

        let mut ab = a.clone();
        ab.merge(&b);
        let mut ba = b.clone();
        ba.merge(&a);

        let ids_ab: Vec<String> = ab.ordered().iter().map(|p| p.event_id.clone()).collect();
        let ids_ba: Vec<String> = ba.ordered().iter().map(|p| p.event_id.clone()).collect();
        assert_eq!(
            ids_ab, ids_ba,
            "commutative: merge order must not affect ordered()"
        );
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

        let mut a = RoomLog::new();
        a.add(e0.clone());
        let mut b = RoomLog::new();
        b.add(e1.clone());
        let mut c = RoomLog::new();
        c.add(e2.clone());

        // (a ⊔ b) ⊔ c
        let mut ab = a.clone();
        ab.merge(&b);
        let mut ab_c = ab;
        ab_c.merge(&c);

        // a ⊔ (b ⊔ c)
        let mut bc = b.clone();
        bc.merge(&c);
        let mut a_bc = a;
        a_bc.merge(&bc);

        let ids1: Vec<String> = ab_c.ordered().iter().map(|p| p.event_id.clone()).collect();
        let ids2: Vec<String> = a_bc.ordered().iter().map(|p| p.event_id.clone()).collect();
        assert_eq!(
            ids1, ids2,
            "associative: both groupings yield same ordered()"
        );
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
    //   input:  replica A receives [e0, e1, e2] in order; replica B receives [e2, e0, e1]
    //   output: both replicas have len==3 and identical ordered()
    //   sideEffects: none
    // roomlog:convergence_different_order:end
    #[test]
    fn roomlog_convergence_different_arrival_order() {
        let e0 = pdu("$e0", &[], 0, 100);
        let e1 = pdu("$e1", &["$e0"], 1, 200);
        let e2 = pdu("$e2", &["$e1"], 2, 300);

        let mut ra = RoomLog::new();
        ra.add(e0.clone());
        ra.add(e1.clone());
        ra.add(e2.clone());

        let mut rb = RoomLog::new();
        rb.add(e2.clone());
        rb.add(e0.clone());
        rb.add(e1.clone());

        let (sink_a, sink_b) = MemCrdtSink::pair();
        ra.publish_delta(&sink_a, "room").expect("publish a");
        rb.publish_delta(&sink_b, "room").expect("publish b");
        ra.drain_delta(&sink_a, "room").expect("drain a");
        rb.drain_delta(&sink_b, "room").expect("drain b");

        assert_eq!(ra.len(), 3);
        assert_eq!(rb.len(), 3);

        let ids_a: Vec<String> = ra.ordered().iter().map(|p| p.event_id.clone()).collect();
        let ids_b: Vec<String> = rb.ordered().iter().map(|p| p.event_id.clone()).collect();
        assert_eq!(
            ids_a, ids_b,
            "convergence: ordered() must be identical on both replicas"
        );
        assert_eq!(ids_a, vec!["$e0", "$e1", "$e2"]);
    }

    // roomlog:convergence_partial_deltas:start
    //   purpose: Convergence under interleaved partial deltas.
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
    //            ordered() until the prev arrives.
    //   input:  add e1 (refs e0) before adding e0
    //   output: before e0 arrives: ordered() is empty; after e0: [e0, e1]
    //   sideEffects: none
    // roomlog:forward_reference_deferred:end
    #[test]
    fn roomlog_forward_reference_deferred_until_prev_arrives() {
        let e0 = pdu("$e0", &[], 0, 100);
        let e1 = pdu("$e1", &["$e0"], 1, 200);

        let mut log = RoomLog::new();
        log.add(e1.clone());

        let ids_before: Vec<String> = log.ordered().iter().map(|p| p.event_id.clone()).collect();
        assert!(
            ids_before.is_empty(),
            "e1 must be deferred until e0 arrives; got: {:?}",
            ids_before
        );

        log.add(e0);
        let ids_after: Vec<String> = log.ordered().iter().map(|p| p.event_id.clone()).collect();
        assert_eq!(
            ids_after,
            vec!["$e0", "$e1"],
            "after e0 arrives, both events ordered correctly"
        );
    }

    // roomlog:concurrent_branches:start
    //   purpose: Two concurrent branches are both included in ordered();
    //            tie-break by (depth, ts, event_id) is stable.
    //   input:  e0 (root), e1a and e1b both reference e0, e2 references both
    //   output: ordered() == [e0, e1a, e1b, e2] (deterministic)
    //   sideEffects: none
    // roomlog:concurrent_branches:end
    #[test]
    fn roomlog_concurrent_branches_deterministic_order() {
        let e0 = pdu("$e0", &[], 0, 100);
        let e1a = pdu("$e1a", &["$e0"], 1, 200);
        let e1b = pdu("$e1b", &["$e0"], 1, 300);
        let e2 = pdu("$e2", &["$e1a", "$e1b"], 2, 400);

        let mut log = RoomLog::new();
        log.add(e1b.clone());
        log.add(e2.clone());
        log.add(e0.clone());
        log.add(e1a.clone());

        let ids: Vec<String> = log.ordered().iter().map(|p| p.event_id.clone()).collect();
        assert_eq!(ids.len(), 4, "all four events present");
        assert_eq!(ids[0], "$e0", "root first");
        assert_eq!(ids[1], "$e1a");
        assert_eq!(ids[2], "$e1b");
        assert_eq!(ids[3], "$e2", "e2 last");

        let ids2: Vec<String> = log.ordered().iter().map(|p| p.event_id.clone()).collect();
        assert_eq!(ids, ids2, "ordered() is deterministic across calls");
    }

    // roomlog:compute_id_deterministic:start
    //   purpose: compute_id returns the same event_id for identical inputs and starts with '$'.
    //   input:  same args twice; also verify different content yields different id
    //   output: same id; different content → different id; always starts with '$'
    //   sideEffects: none
    // roomlog:compute_id_deterministic:end
    #[test]
    fn roomlog_compute_id_deterministic() {
        let id1 = Pdu::compute_id("!r:s", "@a:s", "m.message", b"hello", &[], 0, 42);
        let id2 = Pdu::compute_id("!r:s", "@a:s", "m.message", b"hello", &[], 0, 42);
        assert_eq!(id1, id2, "same inputs → same id");
        assert!(
            id1.starts_with('$'),
            "event_id must start with '$'; got {id1}"
        );

        let id3 = Pdu::compute_id("!r:s", "@a:s", "m.message", b"world", &[], 0, 42);
        assert_ne!(id1, id3, "different content → different id");
    }

    // roomlog:delta_serialisation_roundtrip:start
    //   purpose: delta_to_bytes / delta_from_bytes round-trip preserves all PDU fields
    //            including the new sig and signer_node fields.
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
        let delta2 = delta_from_bytes(&bytes).expect("our own encoding must round-trip");

        let by_id: HashMap<&str, &Pdu> = delta2
            .pdus
            .iter()
            .map(|p| (p.event_id.as_str(), p))
            .collect();
        let p0 = by_id["$e0"];
        let p1 = by_id["$e1"];
        assert_eq!(p0.room_id, "!room:srv");
        assert_eq!(p0.depth, 0);
        assert_eq!(p0.ts, 10);
        assert!(p0.prev_events.is_empty());
        assert_eq!(p1.prev_events, vec!["$e0"]);
        assert_eq!(p1.depth, 1);
        assert_eq!(p1.ts, 20);
        // Unsigned PDUs: sig/signer_node must survive as empty.
        assert!(
            p0.sig.is_empty(),
            "unsigned sig must survive roundtrip as empty"
        );
        assert!(
            p0.signer_node.is_empty(),
            "unsigned signer_node must survive as empty"
        );
    }

    // roomlog:sink_convergence_via_mem:start
    //   purpose: Two RoomLogs exchange deltas via MemCrdtSink and converge.
    //   input:  ra has [e0, e1]; rb has [e0, e2]; exchange via sink
    //   output: both replicas have len==3, identical ordered()
    //   sideEffects: none
    // roomlog:sink_convergence_via_mem:end
    #[test]
    fn roomlog_sink_convergence_via_mem() {
        let e0 = pdu("$e0", &[], 0, 1);
        let e1 = pdu("$e1", &["$e0"], 1, 2);
        let e2 = pdu("$e2", &["$e0"], 1, 3);

        let mut ra = RoomLog::new();
        ra.add(e0.clone());
        ra.add(e1.clone());

        let mut rb = RoomLog::new();
        rb.add(e0.clone());
        rb.add(e2.clone());

        let (sink_a, sink_b) = MemCrdtSink::pair();

        ra.publish_delta(&sink_a, "!room:srv").expect("publish a");
        rb.publish_delta(&sink_b, "!room:srv").expect("publish b");

        let na = ra.drain_delta(&sink_a, "!room:srv").expect("drain a");
        let nb = rb.drain_delta(&sink_b, "!room:srv").expect("drain b");
        assert_eq!(na, 1, "a drained 1 delta blob");
        assert_eq!(nb, 1, "b drained 1 delta blob");

        assert_eq!(ra.len(), 3);
        assert_eq!(rb.len(), 3);

        let ids_a: Vec<String> = ra.ordered().iter().map(|p| p.event_id.clone()).collect();
        let ids_b: Vec<String> = rb.ordered().iter().map(|p| p.event_id.clone()).collect();
        assert_eq!(ids_a, ids_b, "converged via MemCrdtSink");
        assert_eq!(ids_a[0], "$e0");
        assert_eq!(ids_a[1], "$e1");
        assert_eq!(ids_a[2], "$e2");
    }

    // roomlog:sha256_event_id_starts_with_dollar:start
    //   purpose: Pdu::new() produces an event_id that starts with '$' (sha256-based).
    //   input:  construct a Pdu via Pdu::new()
    //   output: event_id starts with '$'
    //   sideEffects: none
    // roomlog:sha256_event_id_starts_with_dollar:end
    #[test]
    fn pdu_new_event_id_starts_with_dollar() {
        let p = Pdu::new(
            "!r:s".to_string(),
            "@u:s".to_string(),
            "m.t".to_string(),
            b"hello".to_vec(),
            vec![],
            0,
            99,
        );
        assert!(
            p.event_id.starts_with('$'),
            "sha256 event_id must start with '$'; got {}",
            p.event_id
        );
        // Base64url nopad chars: A-Z a-z 0-9 - _
        let suffix = &p.event_id[1..];
        assert!(!suffix.is_empty(), "sha256 suffix must not be empty");
    }

    // roomlog:signed_pdu_verify:start
    //   purpose: A PDU created with Pdu::signed() verifies against a NodeKeyStore
    //            containing the signer's pubkey.  An unsigned PDU does not verify.
    //   input:  NodeSigner, Pdu::signed(), NodeKeyStore with own pubkey inserted
    //   output: verify_sig returns true for signed; false for unsigned
    //   sideEffects: none
    // roomlog:signed_pdu_verify:end
    #[test]
    fn signed_pdu_verifies_against_own_key() {
        use crate::substrate::node_auth::NodeSigner;

        let signer = NodeSigner::from_seed([10u8; 32], "node-a".to_string());
        let store = NodeKeyStore::new();
        store.insert(&signer.node_id, signer.verifying_key_bytes());

        // sender domain ("node-a") must equal the signer's node_id for verify_sig's
        // sender-binding check (internal-task item 2) to pass.
        let p = Pdu::signed(
            "!r:s".to_string(),
            "@u:node-a".to_string(),
            "m.t".to_string(),
            b"body".to_vec(),
            vec![],
            0,
            1,
            &signer,
        );
        assert!(p.is_signed(), "Pdu::signed() must set sig + signer_node");
        assert!(
            p.verify_sig(&store),
            "signed PDU must verify against own pubkey"
        );

        // Unsigned PDU must NOT verify.
        let unsigned = Pdu::new(
            "!r:s".to_string(),
            "@u:node-a".to_string(),
            "m.t".to_string(),
            b"body".to_vec(),
            vec![],
            0,
            1,
        );
        assert!(!unsigned.verify_sig(&store), "unsigned PDU must not verify");
    }

    // roomlog:apply_delta_verified_rejects_unsigned:start
    //   purpose: apply_delta_verified() REJECTS unsigned PDUs.
    //            Proves the core security gap is closed: an unsigned PDU (missing
    //            sig and signer_node) arriving from the network is NOT added to the log.
    //   input:  unsigned PDU in a delta; apply_delta_verified with empty key_store
    //   output: (accepted=0, rejected=1); log remains empty
    //   sideEffects: none
    // roomlog:apply_delta_verified_rejects_unsigned:end
    #[test]
    fn apply_delta_verified_rejects_unsigned_pdu() {
        let unsigned = Pdu::new(
            "!r:s".to_string(),
            "@u:s".to_string(),
            "m.t".to_string(),
            b"payload".to_vec(),
            vec![],
            0,
            1,
        );
        let delta = RoomLogDelta {
            pdus: vec![unsigned],
            collected_depth: 0,
        };
        let store = NodeKeyStore::new();
        let mut log = RoomLog::new();
        let (accepted, rejected) = log.apply_delta_verified(&delta, &store);
        assert_eq!(accepted, 0, "unsigned PDU must not be accepted");
        assert_eq!(rejected, 1, "unsigned PDU must be counted as rejected");
        assert!(
            log.is_empty(),
            "log must stay empty when only unsigned PDUs arrive"
        );
    }

    // roomlog:apply_delta_verified_rejects_bad_sig:start
    //   purpose: apply_delta_verified() REJECTS a PDU whose signature is corrupted.
    //            Closes the forgery gap: a tampered PDU claiming a valid signer_node
    //            but with a wrong signature is rejected.
    //   input:  signed PDU, flip sig[0], apply_delta_verified
    //   output: (accepted=0, rejected=1); log remains empty
    //   sideEffects: none
    // roomlog:apply_delta_verified_rejects_bad_sig:end
    #[test]
    fn apply_delta_verified_rejects_bad_signature() {
        use crate::substrate::node_auth::NodeSigner;

        let signer = NodeSigner::from_seed([20u8; 32], "node-b".to_string());
        let store = NodeKeyStore::new();
        store.insert(&signer.node_id, signer.verifying_key_bytes());

        let mut p = Pdu::signed(
            "!r:s".to_string(),
            "@u:node-b".to_string(),
            "m.t".to_string(),
            b"real body".to_vec(),
            vec![],
            0,
            5,
            &signer,
        );
        // Corrupt the signature.
        p.sig[0] ^= 0xff;

        let delta = RoomLogDelta { pdus: vec![p], collected_depth: 0 };
        let mut log = RoomLog::new();
        let (accepted, rejected) = log.apply_delta_verified(&delta, &store);
        assert_eq!(accepted, 0, "bad-sig PDU must not be accepted");
        assert_eq!(rejected, 1, "bad-sig PDU must be counted as rejected");
        assert!(
            log.is_empty(),
            "log must stay empty when bad-sig PDU arrives"
        );
    }

    // roomlog:apply_delta_verified_rejects_unknown_node:start
    //   purpose: apply_delta_verified() REJECTS a PDU from a node not in the key store.
    //            Closes the unknown-sender gap: a node whose pubkey hasn't been
    //            distributed yet (or is not trusted) cannot inject PDUs.
    //   input:  signed PDU from node-c; node-c NOT in the store; apply_delta_verified
    //   output: (accepted=0, rejected=1); log remains empty
    //   sideEffects: none
    // roomlog:apply_delta_verified_rejects_unknown_node:end
    #[test]
    fn apply_delta_verified_rejects_unknown_node() {
        use crate::substrate::node_auth::NodeSigner;

        let signer = NodeSigner::from_seed([30u8; 32], "node-c".to_string());
        // Intentionally do NOT insert node-c into the store.
        let store = NodeKeyStore::new();

        let p = Pdu::signed(
            "!r:s".to_string(),
            "@u:node-c".to_string(),
            "m.t".to_string(),
            b"unknown node payload".to_vec(),
            vec![],
            0,
            10,
            &signer,
        );
        let delta = RoomLogDelta { pdus: vec![p], collected_depth: 0 };
        let mut log = RoomLog::new();
        let (accepted, rejected) = log.apply_delta_verified(&delta, &store);
        assert_eq!(accepted, 0, "unknown-node PDU must not be accepted");
        assert_eq!(rejected, 1, "unknown-node PDU must be counted as rejected");
        assert!(log.is_empty(), "log must stay empty");
    }

    // roomlog:apply_delta_verified_accepts_valid:start
    //   purpose: apply_delta_verified() ACCEPTS a validly-signed PDU from a known node.
    //            Proves the happy path: sign → distribute pubkey → verify → accept.
    //   input:  signed PDU from node-d (in store); apply_delta_verified
    //   output: (accepted=1, rejected=0); PDU is in log
    //   sideEffects: none
    // roomlog:apply_delta_verified_accepts_valid:end
    #[test]
    fn apply_delta_verified_accepts_valid_signed_pdu() {
        use crate::substrate::node_auth::NodeSigner;

        let signer = NodeSigner::from_seed([40u8; 32], "node-d".to_string());
        let store = NodeKeyStore::new();
        store.insert(&signer.node_id, signer.verifying_key_bytes());

        let p = Pdu::signed(
            "!r:s".to_string(),
            "@u:node-d".to_string(),
            "m.t".to_string(),
            b"valid payload".to_vec(),
            vec![],
            0,
            15,
            &signer,
        );
        let event_id = p.event_id.clone();
        assert!(event_id.starts_with('$'), "event_id must start with '$'");

        let delta = RoomLogDelta { pdus: vec![p], collected_depth: 0 };
        let mut log = RoomLog::new();
        let (accepted, rejected) = log.apply_delta_verified(&delta, &store);
        assert_eq!(accepted, 1, "valid signed PDU must be accepted");
        assert_eq!(rejected, 0, "no rejections");
        assert_eq!(log.len(), 1, "PDU must be in log");
    }

    // roomlog:two_cluster_nodes_converge_with_signing:start
    //   purpose: Two simulated cluster nodes (each with own NodeSigner) exchange
    //            signed PDUs via MemCrdtSink + drain_delta_verified.
    //            Both nodes share each other's pubkeys in their NodeKeyStore.
    //            After exchange, both have all PDUs and ordered() is identical.
    //            Proves the signing path does NOT break CRDT convergence.
    //   input:  node-A and node-B each create one signed PDU;
    //           exchange via MemCrdtSink + drain_delta_verified;
    //           both share each other's pubkeys
    //   output: both logs have 2 PDUs; ordered() identical; convergence preserved
    //   sideEffects: none
    // roomlog:two_cluster_nodes_converge_with_signing:end
    #[test]
    fn two_nodes_sign_and_converge() {
        use crate::substrate::node_auth::NodeSigner;

        let signer_a = NodeSigner::from_seed([50u8; 32], "node-e".to_string());
        let signer_b = NodeSigner::from_seed([60u8; 32], "node-f".to_string());

        // Each node's store knows BOTH node pubkeys (simulating grow-set distribution).
        let store_a = NodeKeyStore::new();
        store_a.insert(&signer_a.node_id, signer_a.verifying_key_bytes());
        store_a.insert(&signer_b.node_id, signer_b.verifying_key_bytes());

        let store_b = NodeKeyStore::new();
        store_b.insert(&signer_a.node_id, signer_a.verifying_key_bytes());
        store_b.insert(&signer_b.node_id, signer_b.verifying_key_bytes());

        // Node A creates a signed PDU (sender domain must equal signer_a's node_id).
        let pdu_a = Pdu::signed(
            "!r:s".to_string(),
            "@ua:node-e".to_string(),
            "m.t".to_string(),
            b"from node-e".to_vec(),
            vec![],
            0,
            100,
            &signer_a,
        );
        // Node B creates a signed PDU (sender domain must equal signer_b's node_id).
        let pdu_b = Pdu::signed(
            "!r:s".to_string(),
            "@ub:node-f".to_string(),
            "m.t".to_string(),
            b"from node-f".to_vec(),
            vec![],
            0,
            200,
            &signer_b,
        );

        let mut ra = RoomLog::new();
        ra.add(pdu_a.clone());

        let mut rb = RoomLog::new();
        rb.add(pdu_b.clone());

        let (sink_a, sink_b) = MemCrdtSink::pair();
        ra.publish_delta(&sink_a, "room").expect("publish a");
        rb.publish_delta(&sink_b, "room").expect("publish b");

        // Use verified drain — both sides verify the other's sig.
        let (acc_a, rej_a) = ra
            .drain_delta_verified(&sink_a, "room", &store_a)
            .expect("drain a");
        let (acc_b, rej_b) = rb
            .drain_delta_verified(&sink_b, "room", &store_b)
            .expect("drain b");

        assert_eq!(rej_a, 0, "no rejections on node-e (received node-f's PDU)");
        assert_eq!(rej_b, 0, "no rejections on node-f (received node-e's PDU)");
        assert_eq!(acc_a, 1, "node-e accepted 1 PDU from node-f");
        assert_eq!(acc_b, 1, "node-f accepted 1 PDU from node-e");

        assert_eq!(ra.len(), 2, "node-e has 2 PDUs after convergence");
        assert_eq!(rb.len(), 2, "node-f has 2 PDUs after convergence");

        let ids_a: Vec<String> = ra.ordered().iter().map(|p| p.event_id.clone()).collect();
        let ids_b: Vec<String> = rb.ordered().iter().map(|p| p.event_id.clone()).collect();
        assert_eq!(
            ids_a, ids_b,
            "two nodes with signing converge to same ordered()"
        );
    }

    // ── Garbage collection: the depth watermark ────────────────────────────────

    // gc:chain:start
    //   purpose: Build a linear room history of `n` events, depth 1..=n, each naming
    //            its parent — the shape routes/send.rs actually produces.
    //   input:  n — how many events
    //   output: a RoomLog holding them
    //   sideEffects: none
    // gc:chain:end
    fn gc_chain(n: u64) -> RoomLog {
        let mut log = RoomLog::new();
        let mut prev: Vec<String> = vec![];
        for d in 1..=n {
            let pdu = Pdu::new(
                "!r:s".to_string(),
                "@u:s".to_string(),
                "m.room.message".to_string(),
                format!("event {d}").into_bytes(),
                prev.clone(),
                d,
                1000 + d,
            );
            prev = vec![pdu.event_id.clone()];
            log.add(pdu);
        }
        log
    }

    // gc:collect_keeps_survivors_ordered:start
    //   purpose: Collecting the tail of a room must leave the head readable. The
    //            topological sort defers any event whose prev_events are missing, so
    //            pruning ancestors would otherwise hide every descendant behind them —
    //            garbage collection would look like total history loss.
    //   input:  a 10-event chain, collect below depth 6
    //   output: exactly the four deepest events, still in causal order
    //   sideEffects: none
    // gc:collect_keeps_survivors_ordered:end
    #[test]
    fn collect_keeps_survivors_ordered() {
        let mut log = gc_chain(10);
        assert_eq!(log.ordered().len(), 10, "precondition: all ten are readable");

        let removed = log.collect_below(6);
        assert_eq!(removed, 6, "events at depth 1..=6 are collected");
        assert_eq!(log.len(), 4);
        assert_eq!(log.collected_depth(), 6);

        let depths: Vec<u64> = log.ordered().iter().map(|p| p.depth).collect();
        assert_eq!(
            depths,
            vec![7, 8, 9, 10],
            "the surviving head must still be ordered — an event whose parent was \
             collected must not stay blocked forever"
        );
    }

    // gc:collected_events_do_not_come_back:start
    //   purpose: The whole difficulty of GC on a grow-only set: a peer that has not
    //            collected keeps offering the old events, and a plain merge would put
    //            them straight back. The watermark is what stops that, so pin it.
    //   input:  a collected log; a delta carrying the events it dropped
    //   output: the log does not grow, and the watermark holds
    //   sideEffects: none
    // gc:collected_events_do_not_come_back:end
    #[test]
    fn collected_events_do_not_come_back() {
        let full = gc_chain(10);
        let peer_delta = full.delta(); // a peer that has collected nothing

        let mut log = gc_chain(10);
        log.collect_below(6);
        assert_eq!(log.len(), 4);

        log.apply_delta(&peer_delta);
        assert_eq!(
            log.len(),
            4,
            "a peer resending collected history must not refill the log"
        );
        assert_eq!(log.collected_depth(), 6, "and must not lower the watermark");

        // merge() is the other refill route.
        log.merge(&full);
        assert_eq!(log.len(), 4, "merge must respect the watermark too");
    }

    // gc:watermark_converges_by_max:start
    //   purpose: Two nodes that collect different amounts must agree, and agree on the
    //            DEEPER cut — otherwise the shallower node keeps re-offering events the
    //            other has dropped, forever. Grow-only max is what makes that
    //            order-independent.
    //   input:  a log that has collected nothing; a delta from a node that collected 6
    //   output: the receiver adopts 6 and prunes to match, in either direction
    //   sideEffects: none
    // gc:watermark_converges_by_max:end
    #[test]
    fn watermark_converges_by_max() {
        let mut collected = gc_chain(10);
        collected.collect_below(6);

        // Shallow node learns the deeper cut from the delta alone.
        let mut shallow = gc_chain(10);
        shallow.apply_delta(&collected.delta());
        assert_eq!(shallow.collected_depth(), 6, "adopts the peer's watermark");
        assert_eq!(shallow.len(), 4, "and prunes to match it");

        // The reverse direction must not undo it.
        let untouched = gc_chain(10);
        let mut deep = gc_chain(10);
        deep.collect_below(6);
        deep.apply_delta(&untouched.delta());
        assert_eq!(deep.collected_depth(), 6, "a 0 watermark never lowers ours");
        assert_eq!(deep.len(), 4);
    }

    // gc:watermark_survives_the_wire:start
    //   purpose: The watermark travels appended to the delta, after the PDU blocks.
    //            If it did not survive serialisation the anti-resurrection rule would
    //            hold only in-process — exactly where it is not needed.
    //   input:  a collected log's delta, round-tripped through bytes
    //   output: watermark preserved; a legacy payload with no trailing field reads as 0
    //   sideEffects: none
    // gc:watermark_survives_the_wire:end
    #[test]
    fn watermark_survives_the_wire() {
        let mut log = gc_chain(10);
        log.collect_below(6);

        let bytes = delta_to_bytes(&log.delta());
        let back = delta_from_bytes(&bytes).expect("our own encoding must round-trip");
        assert_eq!(back.collected_depth, 6);
        assert_eq!(back.pdus.len(), 4);

        // A payload from a peer that predates the field: no trailing bytes.
        // This must still PARSE — a missing watermark is an old sender, not a
        // malformed blob, and conflating the two would reject every pre-GC peer.
        let legacy = &bytes[..bytes.len() - 8];
        let back = delta_from_bytes(legacy).expect("a pre-watermark payload must still parse");
        assert_eq!(
            back.collected_depth, 0,
            "a sender with no watermark field must read as 'collected nothing', \
             not as garbage"
        );
        assert_eq!(back.pdus.len(), 4);
    }

    // gc:collect_below_is_monotonic:start
    //   purpose: collect_below must never move the watermark backwards, or two nodes
    //            trading deltas would take turns undoing each other's collection.
    //   input:  collect 6, then attempt 3
    //   output: watermark stays at 6 and nothing is reported removed
    //   sideEffects: none
    // gc:collect_below_is_monotonic:end
    #[test]
    fn collect_below_is_monotonic() {
        let mut log = gc_chain(10);
        assert_eq!(log.collect_below(6), 6);
        assert_eq!(log.collect_below(3), 0, "a shallower cut is a no-op");
        assert_eq!(log.collected_depth(), 6);
        assert_eq!(log.len(), 4);
    }

    // gc:forward_references_still_defer:start
    //   purpose: The "missing prev means collected" rule is bounded by depth, so it
    //            must NOT swallow a genuine forward reference — an event that arrived
    //            before its parent, deeper than the watermark, still has to wait.
    //            Without the bound, out-of-order arrivals would be emitted in the wrong
    //            order the moment a room had ever been collected.
    //   input:  a collected log, plus an orphan far below the frontier
    //   output: the orphan is deferred, the collected-parent events are not
    //   sideEffects: none
    // gc:forward_references_still_defer:end
    #[test]
    fn forward_references_still_defer() {
        let mut log = gc_chain(10);
        log.collect_below(6);
        assert_eq!(log.ordered().len(), 4);

        // depth 20, naming a parent nobody has: a real forward reference.
        let orphan = Pdu::new(
            "!r:s".to_string(),
            "@u:s".to_string(),
            "m.room.message".to_string(),
            b"from the future".to_vec(),
            vec!["$nobody-has-this".to_string()],
            20,
            9999,
        );
        log.add(orphan);
        let depths: Vec<u64> = log.ordered().iter().map(|p| p.depth).collect();
        assert_eq!(
            depths,
            vec![7, 8, 9, 10],
            "an event well above the watermark whose parent is genuinely missing must \
             still be deferred, not emitted out of order"
        );
    }

    // The published east-west test vector. Mirrored verbatim in docs/WIRE.md §13
    // and consumed by scripts/wire_conformance.py, which decodes it from that
    // document alone. These three constants ARE the protocol's public contract:
    // changing any of them is a wire break, not a refactor.
    const GOLDEN_EVENT_ID: &str = "$0syn2lgeSX0g3-FK53G83mWXiTAINqN_Zx9o2cPz1xw";
    const GOLDEN_PUBKEY_HEX: &str =
        "ea4a6c63e29c520abef5507b132ec5f9954776aebebe7b92421eea691446d22c";
    const GOLDEN_DELTA_HEX: &str = "010000002c00243073796e326c676553583067332d464b35334738336d5758695441494e714e5f5a78396f3263507a3178770c0021726f6f6d3a6e6f64652d610d0040616c6963653a6e6f64652d610e006d2e726f6f6d2e6d6573736167650d0000007b22626f6479223a226869227d02000400247a7a7a04002461616104000000000000000068e5cf8b01000006006e6f64652d61400096b0b13b2e99a1964ce38276b5be738cf30c761f9750743426921e3c43cdadc9eeaa3c288b35708a0d17728a58b86da91190ca95f9643d9e9c7481ce71f100080200000000000000";

    // wire:golden_vector_matches_the_spec:start
    //   purpose: Lock the east-west framing to a byte-exact vector, so a refactor
    //            cannot silently change what peers see. This exact blob is
    //            published in docs/WIRE.md §13 as the test vector an independent
    //            implementation decodes, and scripts/wire_conformance.py decodes
    //            it using only that document. Change it and you have changed the
    //            protocol: update the spec and the script in the same commit.
    //   input:  one signed PDU with deliberately UNSORTED prev_events and a
    //           non-zero GC watermark
    //   output: delta_to_bytes equals the published hex; the event_id equals the
    //           published id
    //   sideEffects: none
    // wire:golden_vector_matches_the_spec:end
    #[test]
    fn golden_vector_matches_the_spec() {
        use crate::substrate::node_auth::NodeSigner;

        // prev_events are given zzz-then-aaa on purpose: the transport preserves
        // this order, while canonical_bytes sorts them. A spec reader who misses
        // the sort gets a different event_id and this vector catches it.
        let signer = NodeSigner::from_seed([7u8; 32], "node-a".to_string());
        let pdu = Pdu::signed(
            "!room:node-a".to_string(),
            "@alice:node-a".to_string(),
            "m.room.message".to_string(),
            b"{\"body\":\"hi\"}".to_vec(),
            vec!["$zzz".to_string(), "$aaa".to_string()],
            4,
            1_700_000_000_000,
            &signer,
        );

        fn to_hex(b: &[u8]) -> String {
            b.iter().map(|x| format!("{x:02x}")).collect()
        }

        assert_eq!(
            pdu.event_id, GOLDEN_EVENT_ID,
            "the published event_id changed — docs/WIRE.md §5.2 is now wrong"
        );
        assert_eq!(
            to_hex(&signer.verifying_key_bytes()),
            GOLDEN_PUBKEY_HEX,
            "the vector's signing key changed"
        );

        let bytes = delta_to_bytes(&RoomLogDelta {
            pdus: vec![pdu],
            collected_depth: 2,
        });
        assert_eq!(
            to_hex(&bytes),
            GOLDEN_DELTA_HEX,
            "the east-west framing changed — peers on the old format will mis-parse. \
             Update docs/WIRE.md §13 and scripts/wire_conformance.py in this commit."
        );
    }

    // ── decoder hardening ─────────────────────────────────────────────────────

    // wire:malformed_delta_returns_none_never_panics:start
    //   purpose: delta_from_bytes must return None for every shape of malformed
    //            input rather than panicking. It decodes untrusted network bytes
    //            before any signature check, so a panic here is reachable by
    //            anyone able to publish on the Zenoh prefix — no key required.
    //   input:  a battery of truncations, lying length prefixes, and bad UTF-8
    //   output: None for each; no panic
    //   sideEffects: none
    // wire:malformed_delta_returns_none_never_panics:end
    #[test]
    fn malformed_delta_returns_none_never_panics() {
        // Shorter than the 4-byte pdu count.
        for len in 0..4usize {
            assert!(
                delta_from_bytes(&vec![0xAAu8; len]).is_none(),
                "a {len}-byte blob is not a delta"
            );
        }

        // Claims one PDU, then stops. This is the five-byte case that used to
        // panic a /sync handler.
        assert!(
            delta_from_bytes(&[1, 0, 0, 0, 0]).is_none(),
            "a header claiming a PDU with no body must not parse"
        );

        // Claims u32::MAX PDUs in five bytes: must be a parse failure, not a
        // four-billion-element allocation.
        assert!(
            delta_from_bytes(&[0xFF, 0xFF, 0xFF, 0xFF, 0]).is_none(),
            "an absurd pdu count must fail to parse rather than allocate"
        );

        // A well-formed delta truncated at every possible point.
        let good = delta_to_bytes(&RoomLogDelta {
            pdus: vec![Pdu::new(
                "!r:s".to_string(),
                "@u:s".to_string(),
                "m.room.message".to_string(),
                b"body".to_vec(),
                vec!["$parent".to_string()],
                3,
                77,
            )],
            collected_depth: 0,
        });
        // Stop before the trailing watermark: that prefix is a VALID legacy
        // payload, so start one byte earlier than its end.
        for cut in 4..good.len() - 8 {
            assert!(
                delta_from_bytes(&good[..cut]).is_none(),
                "a delta truncated to {cut} bytes must not parse"
            );
        }

        // A string length prefix that overruns the buffer: 1 pdu, event_id
        // claiming 0xFFFF bytes that are not there.
        let mut lying = vec![1u8, 0, 0, 0];
        lying.extend_from_slice(&0xFFFFu16.to_le_bytes());
        lying.extend_from_slice(b"short");
        assert!(
            delta_from_bytes(&lying).is_none(),
            "a length prefix past the end of the buffer must not parse"
        );

        // Valid framing, invalid UTF-8 in event_id.
        let mut bad_utf8 = vec![1u8, 0, 0, 0];
        bad_utf8.extend_from_slice(&2u16.to_le_bytes());
        bad_utf8.extend_from_slice(&[0xFF, 0xFE]);
        assert!(
            delta_from_bytes(&bad_utf8).is_none(),
            "a non-UTF-8 string must not parse"
        );
    }

    // wire:good_delta_still_round_trips:start
    //   purpose: Guard against the hardening above being satisfied by a decoder
    //            that simply rejects everything — the honest payload, including a
    //            multi-prev PDU and the trailing watermark, must still decode
    //            field-for-field.
    //   input:  a two-PDU delta with prev_events and a non-zero watermark
    //   output: Some(delta) equal to the original in every field
    //   sideEffects: none
    // wire:good_delta_still_round_trips:end
    #[test]
    fn good_delta_still_round_trips() {
        let original = RoomLogDelta {
            pdus: vec![
                Pdu::new(
                    "!r:s".to_string(),
                    "@u:s".to_string(),
                    "m.room.message".to_string(),
                    b"first".to_vec(),
                    vec![],
                    0,
                    10,
                ),
                Pdu::new(
                    "!r:s".to_string(),
                    "@u:s".to_string(),
                    "m.room.message".to_string(),
                    b"second".to_vec(),
                    vec!["$a".to_string(), "$b".to_string()],
                    1,
                    11,
                ),
            ],
            collected_depth: 5,
        };
        let back = delta_from_bytes(&delta_to_bytes(&original)).expect("honest delta must parse");
        assert_eq!(back.collected_depth, 5);
        assert_eq!(back.pdus, original.pdus, "every PDU field must survive");
    }

    // ── event_id is the content address, and is checked on receive ────────────

    // wire:forged_event_id_is_rejected:start
    //   purpose: apply_delta_verified must reject a PDU whose event_id is not the
    //            content address of its own canonical bytes, even when the
    //            signature is perfectly valid. event_id is not part of the signed
    //            pre-image, so without this check a node whose key we pinned can
    //            file an event under any id — and dedup, ordering and the
    //            redaction table all key on that field.
    //   input:  a correctly signed PDU whose event_id is then overwritten
    //   output: rejected, and absent from the log; the honest twin is accepted
    //   sideEffects: none
    // wire:forged_event_id_is_rejected:end
    #[test]
    fn forged_event_id_is_rejected() {
        use crate::substrate::node_auth::NodeSigner;

        let signer = NodeSigner::from_seed([11u8; 32], "node-a".to_string());
        let store = NodeKeyStore::new();
        store.insert(&signer.node_id, signer.verifying_key_bytes());

        let honest = Pdu::signed(
            "!r:node-a".to_string(),
            "@u:node-a".to_string(),
            "m.room.message".to_string(),
            b"hello".to_vec(),
            vec![],
            0,
            42,
            &signer,
        );

        // Same signed bytes, different label on the shelf.
        let mut forged = honest.clone();
        forged.event_id = "$i-picked-this-myself".to_string();
        assert!(
            forged.verify_sig(&store),
            "precondition: the signature itself must still be valid — this test is \
             about the id, not the signature"
        );

        let mut log = RoomLog::new();
        let (accepted, rejected) = log.apply_delta_verified(
            &RoomLogDelta {
                pdus: vec![forged],
                collected_depth: 0,
            },
            &store,
        );
        assert_eq!(
            (accepted, rejected),
            (0, 1),
            "a chosen event_id must be rejected"
        );
        assert_eq!(log.len(), 0, "the forged PDU must not reach the log");

        // The honest twin, byte-identical apart from the id, must still land —
        // otherwise this check would just break replication.
        let (accepted, rejected) = log.apply_delta_verified(
            &RoomLogDelta {
                pdus: vec![honest],
                collected_depth: 0,
            },
            &store,
        );
        assert_eq!(
            (accepted, rejected),
            (1, 0),
            "an honest PDU must still be accepted"
        );
        assert_eq!(log.len(), 1);
    }
}
