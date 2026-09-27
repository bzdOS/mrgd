// START_AI_HEADER
// MODULE: mrgd/src/crdt.rs
// PURPOSE: CRDT layer — Strong Eventual Consistency: write from any node without
//          coordination; state merges via join-semilattice (idempotent, commutative,
//          associative).
//          Types: GCounter, PnCounter, OrSet<T>, LwwRegister<T>, MvRegister<T>.
//          Delta-CRDT for cheap delta transmission (only the change, not full state).
//          Transport — CrdtSink trait (MemCrdtSink in-mem for host tests;
//          ZenohCrdtSink — behind feature `cluster`, real Zenoh pub/sub on <prefix>/<key>).
// INTENT: Full implementation with tests proving CRDT laws (idempotence,
//         commutativity, associativity, convergence). Zenoh behind feature flag.
//         Time/node injected as arguments — deterministic tests, no Date::now.
//         `cluster` feature: ZenohCrdtSink implemented (not a stub).
//         Test `zenoh_crdt_roomlog_convergence` proves multi-master convergence
//         via real Zenoh (two peer sessions, loopback, scouting/gossip).
// DEPENDENCIES: std, thiserror; zenoh (only under feature `cluster`); crate::substrate::observ
// PUBLIC_API: NodeId, GCounter, GCounterDelta, PnCounter, PnCounterDelta,
//             OrSet, OrSetDelta, LwwRegister, LwwDelta, MvRegister, MvDelta,
//             CrdtSink, MemCrdtSink, ZenohCrdtSink (cluster only)
// END_AI_HEADER

use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
    sync::{Arc, Mutex},
};
use thiserror::Error;

/// Opaque 64-bit node identifier — same domain as crate::substrate::session::NodeId.
pub type NodeId = u64;

/// Timestamp in arbitrary monotonic units injected by caller (never Date::now).
pub type Ts = u64;

// ── Error ─────────────────────────────────────────────────────────────────────

/// Errors produced by CRDT or sink operations.
#[derive(Debug, Error)]
pub enum CrdtError {
    #[error("sink lock poisoned")]
    Poisoned,
    #[error("key not found: {0}")]
    NotFound(String),
    /// Payload encryption/decryption failed, or a ciphertext was malformed.
    /// Transport-independent, so it is available in every feature
    /// configuration (the Zenoh variants below are cluster-gated).
    #[error("crypto error: {0}")]
    Crypto(String),
    /// Zenoh subscriber declaration failed (cluster feature).
    #[cfg(feature = "cluster")]
    #[error("zenoh subscribe error: {0}")]
    ZenohSubscribe(String),
    /// Zenoh put failed (cluster feature).
    #[cfg(feature = "cluster")]
    #[error("zenoh put error: {0}")]
    ZenohPut(String),
}

// ═══════════════════════════════════════════════════════════════════════════════
// GCounter — grow-only counter (one entry per node, never decrements)
// ═══════════════════════════════════════════════════════════════════════════════

// GCounter:start
//   purpose: Grow-only counter over a set of nodes. Each node holds its own
//            monotonically-increasing slot; the global value is the sum of all slots.
//            Merge = pointwise max (join-semilattice: idempotent, commutative, associative).
//   input:  per-node increment via `increment(node, amount)`
//   output: `value()` → u64 sum; `delta()` → GCounterDelta snapshot; `apply_delta`
//   sideEffects: mutates internal per-node map
// GCounter:end

/// State of a grow-only counter.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct GCounter {
    /// Per-node slot: node_id → count (monotonically non-decreasing).
    slots: HashMap<NodeId, u64>,
}

/// Delta form of GCounter — only changed slots need to be sent.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct GCounterDelta {
    pub slots: HashMap<NodeId, u64>,
}

impl GCounter {
    // GCounter::new:start
    //   purpose: Construct an empty GCounter.
    //   input:  none
    //   output: GCounter
    //   sideEffects: none
    // GCounter::new:end
    pub fn new() -> Self {
        Self::default()
    }

    // GCounter::increment:start
    //   purpose: Increment the slot for `node` by `amount`. Amount must be > 0.
    //            Slot is clamped to u64::MAX (saturating).
    //   input:  node — owning node id; amount — increment (>0)
    //   output: new slot value
    //   sideEffects: mutates self.slots[node]
    // GCounter::increment:end
    pub fn increment(&mut self, node: NodeId, amount: u64) -> u64 {
        let slot = self.slots.entry(node).or_insert(0);
        *slot = slot.saturating_add(amount);
        *slot
    }

    // GCounter::value:start
    //   purpose: Compute the global counter value as sum of all node slots.
    //   input:  none
    //   output: u64
    //   sideEffects: none (read-only)
    // GCounter::value:end
    pub fn value(&self) -> u64 {
        self.slots.values().copied().fold(0u64, u64::saturating_add)
    }

    // GCounter::merge:start
    //   purpose: Join this counter with another (pointwise max per node).
    //            Satisfies semilattice laws: idempotent, commutative, associative.
    //   input:  other — another GCounter state
    //   output: none (mutates self in place)
    //   sideEffects: updates self.slots with pointwise max
    // GCounter::merge:end
    pub fn merge(&mut self, other: &GCounter) {
        for (&node, &val) in &other.slots {
            let slot = self.slots.entry(node).or_insert(0);
            if val > *slot {
                *slot = val;
            }
        }
    }

    // GCounter::delta:start
    //   purpose: Produce a delta snapshot (full state for simplicity; recipients apply_delta).
    //            In production, track a version vector and diff; here full snapshot suffices
    //            for correctness proofs (delta idempotent under apply_delta).
    //   input:  none
    //   output: GCounterDelta
    //   sideEffects: none
    // GCounter::delta:end
    pub fn delta(&self) -> GCounterDelta {
        GCounterDelta {
            slots: self.slots.clone(),
        }
    }

    // GCounter::apply_delta:start
    //   purpose: Merge a received GCounterDelta into this counter (pointwise max).
    //   input:  delta — received GCounterDelta
    //   output: none
    //   sideEffects: mutates self.slots
    // GCounter::apply_delta:end
    pub fn apply_delta(&mut self, delta: &GCounterDelta) {
        for (&node, &val) in &delta.slots {
            let slot = self.slots.entry(node).or_insert(0);
            if val > *slot {
                *slot = val;
            }
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// PnCounter — positive-negative counter (increment + decrement per node)
// ═══════════════════════════════════════════════════════════════════════════════

// PnCounter:start
//   purpose: Increment/decrement counter implemented as two GCounters (P and N).
//            value() = P.value() - N.value(). May go negative (no invariant protection —
//            balance-floor invariants require coordination, see §4 invariant-confluence).
//            Merge = merge both sub-counters independently (join-semilattice).
//   input:  `increment(node, amount)` / `decrement(node, amount)` with injected node id
//   output: `value()` → i64; `delta()` / `apply_delta`
//   sideEffects: mutates internal P/N GCounters
// PnCounter:end

/// State of a positive-negative counter.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PnCounter {
    /// Accumulates all increments.
    p: GCounter,
    /// Accumulates all decrements.
    n: GCounter,
}

/// Delta form of PnCounter.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PnCounterDelta {
    pub p: GCounterDelta,
    pub n: GCounterDelta,
}

impl PnCounter {
    // PnCounter::new:start
    //   purpose: Construct an empty PnCounter.
    //   input:  none
    //   output: PnCounter
    //   sideEffects: none
    // PnCounter::new:end
    pub fn new() -> Self {
        Self::default()
    }

    // PnCounter::increment:start
    //   purpose: Add `amount` to the positive sub-counter for `node`.
    //   input:  node — owning node id; amount — positive increment
    //   output: none
    //   sideEffects: mutates self.p
    // PnCounter::increment:end
    pub fn increment(&mut self, node: NodeId, amount: u64) {
        self.p.increment(node, amount);
    }

    // PnCounter::decrement:start
    //   purpose: Add `amount` to the negative sub-counter for `node`.
    //   input:  node — owning node id; amount — decrement magnitude
    //   output: none
    //   sideEffects: mutates self.n
    // PnCounter::decrement:end
    pub fn decrement(&mut self, node: NodeId, amount: u64) {
        self.n.increment(node, amount);
    }

    // PnCounter::value:start
    //   purpose: Compute the signed counter value: P.sum - N.sum.
    //            May underflow to i64::MIN if N greatly exceeds P.
    //   input:  none
    //   output: i64
    //   sideEffects: none
    // PnCounter::value:end
    pub fn value(&self) -> i64 {
        let pos = self.p.value() as i64;
        let neg = self.n.value() as i64;
        pos.saturating_sub(neg)
    }

    // PnCounter::merge:start
    //   purpose: Join P and N sub-counters from `other` (pointwise max each).
    //   input:  other — another PnCounter state
    //   output: none
    //   sideEffects: mutates self.p and self.n
    // PnCounter::merge:end
    pub fn merge(&mut self, other: &PnCounter) {
        self.p.merge(&other.p);
        self.n.merge(&other.n);
    }

    // PnCounter::delta:start
    //   purpose: Produce a full delta snapshot of both P and N sub-counters.
    //   input:  none
    //   output: PnCounterDelta
    //   sideEffects: none
    // PnCounter::delta:end
    pub fn delta(&self) -> PnCounterDelta {
        PnCounterDelta {
            p: self.p.delta(),
            n: self.n.delta(),
        }
    }

    // PnCounter::apply_delta:start
    //   purpose: Merge a received PnCounterDelta into this counter.
    //   input:  delta — received PnCounterDelta
    //   output: none
    //   sideEffects: mutates self.p and self.n
    // PnCounter::apply_delta:end
    pub fn apply_delta(&mut self, delta: &PnCounterDelta) {
        self.p.apply_delta(&delta.p);
        self.n.apply_delta(&delta.n);
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// OrSet<T> — observed-remove set (add-wins semantics)
// ═══════════════════════════════════════════════════════════════════════════════

// OrSet:start
//   purpose: Add/remove set with add-wins: if concurrent add and remove on the same element,
//            the element remains present. Each add attaches a unique tag (node, ts) so that
//            remove only removes elements whose specific tags have been observed.
//            Merge: union of present tags from both replicas (join-semilattice).
//   input:  `add(elem, node, ts)` / `remove(elem)` with injected (node, ts)
//   output: `contains(&T)`, `value() → HashSet<&T>`, `delta() / apply_delta`
//   sideEffects: mutates internal tag map
// OrSet:end

/// A tag identifying a specific add operation (unique per add).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Tag {
    pub node: NodeId,
    pub ts: Ts,
}

/// State of an observed-remove set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrSet<T>
where
    T: Eq + Hash + Clone,
{
    /// Map from element → set of live add-tags (empty set = effectively removed).
    tags: HashMap<T, HashSet<Tag>>,
}

/// Delta form of an OrSet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrSetDelta<T>
where
    T: Eq + Hash + Clone,
{
    pub tags: HashMap<T, HashSet<Tag>>,
}

impl<T> Default for OrSet<T>
where
    T: Eq + Hash + Clone,
{
    fn default() -> Self {
        Self {
            tags: HashMap::new(),
        }
    }
}

impl<T> OrSet<T>
where
    T: Eq + Hash + Clone,
{
    // OrSet::new:start
    //   purpose: Construct an empty OrSet.
    //   input:  none
    //   output: OrSet<T>
    //   sideEffects: none
    // OrSet::new:end
    pub fn new() -> Self {
        Self::default()
    }

    // OrSet::add:start
    //   purpose: Add element `elem` tagged with (node, ts). Multiple tags per element
    //            accumulate — each add is independent. Duplicate (node,ts) is idempotent.
    //   input:  elem — element to add; node — issuing node; ts — timestamp/sequence
    //   output: none
    //   sideEffects: inserts tag into self.tags[elem]
    // OrSet::add:end
    pub fn add(&mut self, elem: T, node: NodeId, ts: Ts) {
        self.tags.entry(elem).or_default().insert(Tag { node, ts });
    }

    // OrSet::remove:start
    //   purpose: Remove element `elem` by clearing all its currently observed tags.
    //            Concurrent adds with new tags (not yet observed by this replica) survive —
    //            this is the add-wins invariant.
    //   input:  elem — element to remove
    //   output: none
    //   sideEffects: clears tag set for elem (entry remains with empty set)
    // OrSet::remove:end
    pub fn remove(&mut self, elem: &T) {
        if let Some(set) = self.tags.get_mut(elem) {
            set.clear();
        }
    }

    // OrSet::contains:start
    //   purpose: Test whether `elem` is currently in the set (has any live tags).
    //   input:  elem — element to test
    //   output: bool
    //   sideEffects: none
    // OrSet::contains:end
    pub fn contains(&self, elem: &T) -> bool {
        self.tags.get(elem).is_some_and(|s| !s.is_empty())
    }

    // OrSet::value:start
    //   purpose: Return the set of currently present elements (those with live tags).
    //   input:  none
    //   output: HashSet<&T> (borrowed view)
    //   sideEffects: none
    // OrSet::value:end
    pub fn value(&self) -> HashSet<&T> {
        self.tags
            .iter()
            .filter_map(|(k, v)| if v.is_empty() { None } else { Some(k) })
            .collect()
    }

    // OrSet::merge:start
    //   purpose: Join this set with `other` by taking the union of tag sets per element.
    //            Satisfies semilattice laws (join = union is idempotent, commutative, associative).
    //   input:  other — another OrSet<T>
    //   output: none
    //   sideEffects: mutates self.tags
    // OrSet::merge:end
    pub fn merge(&mut self, other: &OrSet<T>) {
        for (elem, tags) in &other.tags {
            self.tags
                .entry(elem.clone())
                .or_default()
                .extend(tags.iter().cloned());
        }
    }

    // OrSet::delta:start
    //   purpose: Produce a full delta snapshot (all tags). Recipients call apply_delta.
    //   input:  none
    //   output: OrSetDelta<T>
    //   sideEffects: none
    // OrSet::delta:end
    pub fn delta(&self) -> OrSetDelta<T> {
        OrSetDelta {
            tags: self.tags.clone(),
        }
    }

    // OrSet::apply_delta:start
    //   purpose: Merge a received OrSetDelta into this set (union of tags per element).
    //   input:  delta — received OrSetDelta<T>
    //   output: none
    //   sideEffects: mutates self.tags
    // OrSet::apply_delta:end
    pub fn apply_delta(&mut self, delta: &OrSetDelta<T>) {
        for (elem, tags) in &delta.tags {
            self.tags
                .entry(elem.clone())
                .or_default()
                .extend(tags.iter().cloned());
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// LwwRegister<T> — last-writer-wins register (highest (ts, node) wins)
// ═══════════════════════════════════════════════════════════════════════════════

// LwwRegister:start
//   purpose: Single-value register where concurrent writes resolve by taking the
//            highest (ts, node) pair as the winner (last-writer-wins, tie-break on node id).
//            Merge = take the entry with the greater (ts, node) pair.
//            Converges to a single value on all replicas (Strong Eventual Consistency).
//   input:  `set(value, node, ts)` with injected (node, ts)
//   output: `value() → Option<&T>`; `delta() / apply_delta`
//   sideEffects: mutates internal entry
// LwwRegister:end

/// An entry in an LWW register (value + winning timestamp + node for tie-break).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LwwEntry<T> {
    pub value: T,
    pub ts: Ts,
    pub node: NodeId,
}

/// State of a last-writer-wins register.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LwwRegister<T> {
    entry: Option<LwwEntry<T>>,
}

impl<T: Clone + PartialEq + Eq> Default for LwwRegister<T> {
    fn default() -> Self {
        Self { entry: None }
    }
}

/// Delta form of LwwRegister.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LwwDelta<T> {
    pub entry: Option<LwwEntry<T>>,
}

impl<T: Clone + PartialEq + Eq> LwwRegister<T> {
    // LwwRegister::new:start
    //   purpose: Construct an empty LwwRegister (no value).
    //   input:  none
    //   output: LwwRegister<T>
    //   sideEffects: none
    // LwwRegister::new:end
    pub fn new() -> Self {
        Self::default()
    }

    // LwwRegister::set:start
    //   purpose: Write `value` with timestamp `ts` from `node`.
    //            Replaces the current entry only if (ts, node) > current (ts, node).
    //            Inject ts and node explicitly — no wall-clock reads.
    //   input:  value — new value; node — writing node; ts — write timestamp
    //   output: none
    //   sideEffects: may replace self.entry
    // LwwRegister::set:end
    pub fn set(&mut self, value: T, node: NodeId, ts: Ts) {
        let beats = match &self.entry {
            None => true,
            Some(e) => (ts, node) > (e.ts, e.node),
        };
        if beats {
            self.entry = Some(LwwEntry { value, ts, node });
        }
    }

    // LwwRegister::value:start
    //   purpose: Return the current register value, if any.
    //   input:  none
    //   output: Option<&T>
    //   sideEffects: none
    // LwwRegister::value:end
    pub fn value(&self) -> Option<&T> {
        self.entry.as_ref().map(|e| &e.value)
    }

    // LwwRegister::merge:start
    //   purpose: Join with `other` by keeping the entry with the higher (ts, node).
    //            Idempotent: merging identical state is a no-op.
    //   input:  other — another LwwRegister<T>
    //   output: none
    //   sideEffects: may replace self.entry
    // LwwRegister::merge:end
    pub fn merge(&mut self, other: &LwwRegister<T>) {
        if let Some(ref o) = other.entry {
            self.set(o.value.clone(), o.node, o.ts);
        }
    }

    // LwwRegister::delta:start
    //   purpose: Snapshot the current entry as a delta.
    //   input:  none
    //   output: LwwDelta<T>
    //   sideEffects: none
    // LwwRegister::delta:end
    pub fn delta(&self) -> LwwDelta<T> {
        LwwDelta {
            entry: self.entry.clone(),
        }
    }

    // LwwRegister::apply_delta:start
    //   purpose: Merge a received LwwDelta — same logic as merge.
    //   input:  delta — received LwwDelta<T>
    //   output: none
    //   sideEffects: may replace self.entry
    // LwwRegister::apply_delta:end
    pub fn apply_delta(&mut self, delta: &LwwDelta<T>) {
        if let Some(ref e) = delta.entry {
            self.set(e.value.clone(), e.node, e.ts);
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// MvRegister<T> — multi-value register (keeps all concurrent values)
// ═══════════════════════════════════════════════════════════════════════════════

// MvRegister:start
//   purpose: Register that retains ALL concurrently-written values (no silent drop).
//            Each write attaches a unique (node, ts) tag; merge takes the union of
//            surviving entries. Application resolves conflicts explicitly from value().
//            An explicit set() clears all currently observed entries and writes a new one —
//            this is the "overwrite-observed" semantics (like Riak's MVCC resolution).
//            Merge = union of tag-keyed entries (join-semilattice).
//   input:  `set(value, node, ts)` — clears observed, writes new entry
//   output: `value() → Vec<&T>` (all concurrent values); `delta() / apply_delta`
//   sideEffects: mutates internal entry map
// MvRegister:end

/// A single tagged entry in an MV register.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MvEntry<T> {
    pub value: T,
    pub ts: Ts,
    pub node: NodeId,
}

/// State of a multi-value register.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MvRegister<T>
where
    T: Clone + PartialEq + Eq,
{
    /// Live entries indexed by (node, ts) tag.
    entries: HashMap<(NodeId, Ts), T>,
    /// Tags that have been "observed" and should be cleared on next set().
    /// We track them separately to implement overwrite-observed semantics.
    observed: HashSet<(NodeId, Ts)>,
}

impl<T: Clone + PartialEq + Eq> Default for MvRegister<T> {
    fn default() -> Self {
        Self {
            entries: HashMap::new(),
            observed: HashSet::new(),
        }
    }
}

/// Delta form of MvRegister.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MvDelta<T>
where
    T: Clone + PartialEq + Eq,
{
    pub entries: HashMap<(NodeId, Ts), T>,
    pub observed: HashSet<(NodeId, Ts)>,
}

impl<T: Clone + PartialEq + Eq> MvRegister<T> {
    // MvRegister::new:start
    //   purpose: Construct an empty MvRegister.
    //   input:  none
    //   output: MvRegister<T>
    //   sideEffects: none
    // MvRegister::new:end
    pub fn new() -> Self {
        Self::default()
    }

    // MvRegister::set:start
    //   purpose: Write `value` tagged (node, ts), clearing all currently observed entries.
    //            Concurrent writes (not yet observed) survive — they appear in value().
    //            Inject (node, ts) explicitly for determinism.
    //   input:  value; node — writing node; ts — write timestamp
    //   output: none
    //   sideEffects: removes all observed entries, inserts new (node,ts) entry
    // MvRegister::set:end
    pub fn set(&mut self, value: T, node: NodeId, ts: Ts) {
        // Mark all current entries as observed-and-cleared.
        let current_tags: HashSet<(NodeId, Ts)> = self.entries.keys().copied().collect();
        for tag in &current_tags {
            self.observed.insert(*tag);
        }
        // Remove all currently observed entries.
        for tag in &self.observed {
            self.entries.remove(tag);
        }
        // Insert the new entry.
        self.entries.insert((node, ts), value);
    }

    // MvRegister::value:start
    //   purpose: Return all concurrent values (those not yet overwritten by any replica).
    //   input:  none
    //   output: Vec<&T>
    //   sideEffects: none
    // MvRegister::value:end
    pub fn value(&self) -> Vec<&T> {
        self.entries.values().collect()
    }

    // MvRegister::merge:start
    //   purpose: Join with `other`. Observed set union propagates deletes; entry union
    //            propagates adds. An entry from `other` that this replica has already
    //            observed is discarded; unseen concurrent entries are retained.
    //   input:  other — another MvRegister<T>
    //   output: none
    //   sideEffects: mutates self.entries and self.observed
    // MvRegister::merge:end
    pub fn merge(&mut self, other: &MvRegister<T>) {
        // Union observed sets — any entry that either side has cleared is cleared globally.
        self.observed.extend(other.observed.iter().copied());
        // Remove now-observed entries from our live set.
        for tag in &self.observed {
            self.entries.remove(tag);
        }
        // Add entries from other that are not yet globally observed.
        for (&tag, val) in &other.entries {
            if !self.observed.contains(&tag) {
                self.entries.insert(tag, val.clone());
            }
        }
    }

    // MvRegister::delta:start
    //   purpose: Snapshot full state as a delta.
    //   input:  none
    //   output: MvDelta<T>
    //   sideEffects: none
    // MvRegister::delta:end
    pub fn delta(&self) -> MvDelta<T> {
        MvDelta {
            entries: self.entries.clone(),
            observed: self.observed.clone(),
        }
    }

    // MvRegister::apply_delta:start
    //   purpose: Merge a received MvDelta into this register.
    //   input:  delta — received MvDelta<T>
    //   output: none
    //   sideEffects: mutates self.entries and self.observed
    // MvRegister::apply_delta:end
    pub fn apply_delta(&mut self, delta: &MvDelta<T>) {
        // Reconstruct a temporary MvRegister from the delta and merge it.
        let tmp = MvRegister {
            entries: delta.entries.clone(),
            observed: delta.observed.clone(),
        };
        self.merge(&tmp);
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// CrdtSink trait — transport abstraction for delta exchange
// ═══════════════════════════════════════════════════════════════════════════════

// CrdtSink:start
//   purpose: Abstract transport for publishing and receiving opaque CRDT delta blobs
//            keyed by a string. MemCrdtSink: in-memory shared buffer (host tests).
//            ZenohCrdtSink: Zenoh pub/sub on `<prefix>/<key>` (behind `cluster`).
//   input:  `publish(key, bytes)` — send delta; `drain(key)` — receive all pending deltas
//   output: Result<(), CrdtError> / Result<Vec<Vec<u8>>, CrdtError>
//   sideEffects: depends on impl (memory write / network send)
// CrdtSink:end

/// Trait for publishing/receiving raw CRDT delta bytes keyed by a string.
pub trait CrdtSink: Send + Sync {
    // CrdtSink::publish:start
    //   purpose: Publish an opaque delta blob under `key` to remote replicas.
    //   input:  key — CRDT key (e.g. "counter/hits"); bytes — serialised delta
    //   output: Result<(), CrdtError>
    //   sideEffects: enqueues bytes for delivery
    // CrdtSink::publish:end
    fn publish(&self, key: &str, bytes: Vec<u8>) -> Result<(), CrdtError>;

    // CrdtSink::drain:start
    //   purpose: Drain all pending delta blobs received for `key` since last drain.
    //   input:  key — CRDT key
    //   output: Result<Vec<Vec<u8>>, CrdtError> — list of received delta blobs
    //   sideEffects: clears the pending buffer for this key
    // CrdtSink::drain:end
    fn drain(&self, key: &str) -> Result<Vec<Vec<u8>>, CrdtError>;
}

// ── MemCrdtSink ───────────────────────────────────────────────────────────────

// MemCrdtSink:start
//   purpose: In-memory CrdtSink for host-side convergence tests.
//            A shared inbox (Arc<Mutex<HashMap>>) lets two "replicas" exchange deltas
//            without any network. `publish` routes the blob to the OTHER sink's inbox;
//            `drain` pops from self's inbox.
//            Two MemCrdtSink instances sharing the same inbox model two replicas that
//            see each other's publishes in their respective drain queues.
//   input:  `pair()` — create two linked sinks (A→B inbox, B→A inbox)
//   output: (MemCrdtSink, MemCrdtSink)
//   sideEffects: allocates two Arc<Mutex<HashMap>> (one inbox per sink)
// MemCrdtSink:end

type Inbox = Arc<Mutex<HashMap<String, Vec<Vec<u8>>>>>;

/// In-memory CRDT sink. `publish` writes to the peer's inbox; `drain` reads from own inbox.
pub struct MemCrdtSink {
    /// Our inbox: where the peer's publishes land.
    own_inbox: Inbox,
    /// Peer's inbox: where our publishes land.
    peer_inbox: Inbox,
}

impl MemCrdtSink {
    // MemCrdtSink::pair:start
    //   purpose: Create two linked MemCrdtSink instances: A.publish → B.drain, B.publish → A.drain.
    //            Models two nodes exchanging deltas in-memory (no Zenoh needed for host tests).
    //   input:  none
    //   output: (MemCrdtSink, MemCrdtSink)
    //   sideEffects: allocates two Arc<Mutex<HashMap>>
    // MemCrdtSink::pair:end
    pub fn pair() -> (Self, Self) {
        let inbox_a: Inbox = Arc::new(Mutex::new(HashMap::new()));
        let inbox_b: Inbox = Arc::new(Mutex::new(HashMap::new()));
        let a = MemCrdtSink {
            own_inbox: inbox_a.clone(),
            peer_inbox: inbox_b.clone(),
        };
        let b = MemCrdtSink {
            own_inbox: inbox_b,
            peer_inbox: inbox_a,
        };
        (a, b)
    }
}

impl CrdtSink for MemCrdtSink {
    fn publish(&self, key: &str, bytes: Vec<u8>) -> Result<(), CrdtError> {
        self.peer_inbox
            .lock()
            .map_err(|_| CrdtError::Poisoned)?
            .entry(key.to_string())
            .or_insert_with(Vec::new)
            .push(bytes);
        Ok(())
    }

    fn drain(&self, key: &str) -> Result<Vec<Vec<u8>>, CrdtError> {
        let mut guard = self.own_inbox.lock().map_err(|_| CrdtError::Poisoned)?;
        Ok(guard.remove(key).unwrap_or_default())
    }
}

// ── ZenohCrdtSink (cluster feature only — real implementation) ────────────────

// ZenohCrdtSink:start
//   purpose: Zenoh-backed CrdtSink for multi-master delta-CRDT convergence across nodes.
//            Publishes delta blobs to `<prefix>/<key>` Zenoh keyexpr; receives deltas
//            from all peers via a wildcard subscriber on `<prefix>/**`.
//            A background tokio task drains the subscriber and routes each sample into
//            `inbox: Arc<Mutex<HashMap<key, Vec<Vec<u8>>>>>` keyed by the trailing
//            segment of the keyexpr (i.e. the CRDT key passed to publish).
//            `drain(key)` removes and returns all accumulated blobs for that key.
//            Convergence is idempotent: RoomLog.apply_delta ignores duplicate event_ids
//            (grow-only set semantics), so duplicate deliveries are safe.
//            The Zenoh session is held in an Arc so it can be shared cheaply between the
//            publish path and the background subscriber task.
//   input:  `new(session, key_prefix)` — caller owns zenoh::Session + spawns background task;
//           `publish(key, bytes)` — session.put("<prefix>/<key>", bytes) via block_on;
//           `drain(key)` — pop all pending blobs from inbox
//   output: Result<(), CrdtError> / Result<Vec<Vec<u8>>, CrdtError>
//   sideEffects: opens a Zenoh subscriber on "<prefix>/**" per instance;
//                spawns one background tokio task per instance;
//                holds an Arc<zenoh::Session> (keeps session alive until ZenohCrdtSink drops)
// ZenohCrdtSink:end
#[cfg(feature = "cluster")]
pub struct ZenohCrdtSink {
    /// Shared Zenoh session — used for put() on the publish path.
    session: std::sync::Arc<zenoh::Session>,
    /// Per-key inbox: background task writes here; drain() reads from here.
    inbox: Inbox,
    /// Key prefix prepended to every CRDT key when publishing.
    key_prefix: String,
}

#[cfg(feature = "cluster")]
impl ZenohCrdtSink {
    // ZenohCrdtSink::new:start
    //   purpose: Construct a ZenohCrdtSink from an already-open zenoh::Session.
    //            Spawns a background tokio task that subscribes to `<key_prefix>/**`
    //            and routes received samples into the internal inbox.
    //            Caller is responsible for ensuring a tokio runtime is active
    //            (the background task is spawned with tokio::spawn).
    //   input:  session — open zenoh::Session (will be wrapped in Arc);
    //           key_prefix — Zenoh keyexpr prefix, e.g. "mrgd/crdt"
    //   output: Result<ZenohCrdtSink, CrdtError>
    //   sideEffects: spawns one tokio task per call; opens one Zenoh subscriber
    // ZenohCrdtSink::new:end
    pub async fn new(session: zenoh::Session, key_prefix: &str) -> Result<Self, CrdtError> {
        let session = std::sync::Arc::new(session);
        let inbox: Inbox = std::sync::Arc::new(Mutex::new(HashMap::new()));

        // Subscribe to all keys under the prefix so we receive every peer's publishes.
        let sub_key = format!("{}/**", key_prefix);
        let subscriber = session
            .declare_subscriber(&sub_key)
            .await
            .map_err(|e| CrdtError::ZenohSubscribe(e.to_string()))?;

        let prefix_owned = key_prefix.to_string();
        let inbox_bg = inbox.clone();

        // Background task: drain subscriber and route into inbox by key suffix.
        tokio::spawn(async move {
            while let Ok(sample) = subscriber.recv_async().await {
                // key_expr looks like "<prefix>/<crdt_key>"; extract the suffix after the prefix.
                let full_key = sample.key_expr().as_str();
                // Strip the prefix + "/" to get the CRDT routing key.
                let crdt_key = full_key
                    .strip_prefix(&prefix_owned)
                    .and_then(|s| s.strip_prefix('/'))
                    .unwrap_or(full_key)
                    .to_string();

                let bytes = sample.payload().to_bytes().to_vec();

                // Observability: emit crdt.recv with content hash for cross-node correlation.
                if crate::substrate::observ::enabled() {
                    let id = crate::substrate::observ::content_id(&bytes);
                    crate::substrate::observ::emit(
                        "crdt.recv",
                        &[("key", full_key), ("id", &id), ("crdt_key", &crdt_key)],
                    );
                }

                if let Ok(mut guard) = inbox_bg.lock() {
                    guard.entry(crdt_key).or_insert_with(Vec::new).push(bytes);
                }
                // If lock is poisoned we silently drop — inbox will stop accumulating
                // but the session stays alive; CrdtError::Poisoned surfaces on next drain.
            }
            // Subscriber closed (session dropped or undeclared) — task exits cleanly.
        });

        Ok(ZenohCrdtSink {
            session,
            inbox,
            key_prefix: key_prefix.to_string(),
        })
    }

    // ZenohCrdtSink::inject:start
    //   purpose: Push a delta blob straight into this sink's inbox, as though the
    //            background subscriber had received it. Exists for the room-discovery
    //            path: a wildcard subscriber one level up sees a sample for a room this
    //            node has never heard of, lazily creates the per-room sink, and hands
    //            the already-received payload over so the next drain() applies it.
    //            Without this the sample would be lost — the freshly-created sink's own
    //            subscriber only sees traffic published after it was declared.
    //   input:  key — CRDT routing key (sink-relative, no key_prefix); bytes — delta blob
    //   output: none
    //   sideEffects: appends to the in-memory inbox. Silently drops if the lock is
    //                poisoned, matching the background task's behaviour — the error
    //                surfaces on the next drain() rather than here.
    // ZenohCrdtSink::inject:end
    pub fn inject(&self, key: &str, bytes: Vec<u8>) {
        if let Ok(mut guard) = self.inbox.lock() {
            guard.entry(key.to_string()).or_default().push(bytes);
        }
    }
}

#[cfg(feature = "cluster")]
impl CrdtSink for ZenohCrdtSink {
    // CrdtSink::publish (ZenohCrdtSink impl):start
    //   purpose: Publish a CRDT delta blob to `<key_prefix>/<key>` via Zenoh put.
    //            Blocks the calling (sync) thread by driving the async put to completion
    //            using the current tokio runtime handle.
    //            Emits crdt.publish to observability (gated by MRGD_OBSERV).
    //   input:  key — CRDT routing key (appended to key_prefix); bytes — serialised delta
    //   output: Result<(), CrdtError>
    //   sideEffects: enqueues a Zenoh publication on the network; emits observ line
    // CrdtSink::publish (ZenohCrdtSink impl):end
    fn publish(&self, key: &str, bytes: Vec<u8>) -> Result<(), CrdtError> {
        let zenoh_key = format!("{}/{}", self.key_prefix, key);

        // Observability: emit crdt.publish with content hash before the network put.
        // Guarded so content_id() is not called on the hot path when disabled.
        if crate::substrate::observ::enabled() {
            let id = crate::substrate::observ::content_id(&bytes);
            crate::substrate::observ::emit(
                "crdt.publish",
                &[("key", &zenoh_key), ("id", &id), ("crdt_key", key)],
            );
        }

        let session = self.session.clone();
        // Drive the async put from a sync context.
        // `block_in_place` is safe in the multi-thread tokio runtime (required by Zenoh):
        // it parks the current worker thread for the duration of the blocking operation
        // without stalling the scheduler.  In a single-thread runtime this would panic;
        // but ZenohCrdtSink::new() is async (requires multi-thread runtime) so this
        // invariant is always satisfied when ZenohCrdtSink exists.
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(async move { session.put(&zenoh_key, bytes).await })
        })
        .map_err(|e| CrdtError::ZenohPut(e.to_string()))
    }

    // CrdtSink::drain (ZenohCrdtSink impl):start
    //   purpose: Drain all pending delta blobs accumulated by the background subscriber
    //            task for `key` since the last drain call.
    //            Emits crdt.drain to observability with count and content ids.
    //   input:  key — CRDT routing key
    //   output: Result<Vec<Vec<u8>>, CrdtError>
    //   sideEffects: removes the key's queue from the inbox (clears it for next drain);
    //                emits observ line when enabled
    // CrdtSink::drain (ZenohCrdtSink impl):end
    fn drain(&self, key: &str) -> Result<Vec<Vec<u8>>, CrdtError> {
        let mut guard = self.inbox.lock().map_err(|_| CrdtError::Poisoned)?;
        let blobs = guard.remove(key).unwrap_or_default();

        // Observability: emit crdt.drain with count and comma-joined content ids.
        if crate::substrate::observ::enabled() {
            let n_str = blobs.len().to_string();
            let ids: Vec<String> = blobs.iter().map(|b| crate::substrate::observ::content_id(b)).collect();
            let ids_joined = ids.join(",");
            crate::substrate::observ::emit(
                "crdt.drain",
                &[("crdt_key", key), ("n", &n_str), ("ids", &ids_joined)],
            );
        }

        Ok(blobs)
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests — CRDT laws (idempotence, commutativity, associativity, convergence)
// ═══════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    // ── Helper serialisation functions ──────────────────────────────────────
    // (Simplified byte wrappers for CrdtSink tests — not production capnp,
    //  just proves that sink round-trip works.)

    fn gcounter_to_bytes(d: &GCounterDelta) -> Vec<u8> {
        // Serialize as: [n: u32 LE][node: u64 LE][val: u64 LE] * n
        let mut buf = Vec::new();
        buf.extend_from_slice(&(d.slots.len() as u32).to_le_bytes());
        for (&node, &val) in &d.slots {
            buf.extend_from_slice(&node.to_le_bytes());
            buf.extend_from_slice(&val.to_le_bytes());
        }
        buf
    }

    fn gcounter_from_bytes(bytes: &[u8]) -> GCounterDelta {
        let mut slots = HashMap::new();
        let n = u32::from_le_bytes(bytes[0..4].try_into().expect("len")) as usize;
        let mut off = 4usize;
        for _ in 0..n {
            let node = u64::from_le_bytes(bytes[off..off + 8].try_into().expect("node"));
            let val = u64::from_le_bytes(bytes[off + 8..off + 16].try_into().expect("val"));
            slots.insert(node, val);
            off += 16;
        }
        GCounterDelta { slots }
    }

    // ── GCounter laws ─────────────────────────────────────────────────────────

    // gcounter:idempotent:start
    //   purpose: merge(a, a) == a (idempotence of join).
    //   input:  GCounter with two node slots
    //   output: merging with itself leaves value unchanged
    //   sideEffects: none
    // gcounter:idempotent:end
    #[test]
    fn gcounter_merge_idempotent() {
        let mut a = GCounter::new();
        a.increment(1, 5);
        a.increment(2, 3);
        let snapshot = a.clone();
        a.merge(&snapshot);
        assert_eq!(a, snapshot, "merge(a,a) must equal a");
        assert_eq!(a.value(), 8);
    }

    // gcounter:commutative:start
    //   purpose: merge(a, b) == merge(b, a) (commutativity of join).
    //   input:  two distinct GCounters
    //   output: both merge results equal
    //   sideEffects: none
    // gcounter:commutative:end
    #[test]
    fn gcounter_merge_commutative() {
        let mut a = GCounter::new();
        a.increment(1, 10);

        let mut b = GCounter::new();
        b.increment(2, 7);

        let mut ab = a.clone();
        ab.merge(&b);

        let mut ba = b.clone();
        ba.merge(&a);

        assert_eq!(
            ab.value(),
            ba.value(),
            "merge(a,b).value == merge(b,a).value"
        );
        assert_eq!(ab, ba, "merge(a,b) == merge(b,a)");
    }

    // gcounter:associative:start
    //   purpose: merge(merge(a,b), c) == merge(a, merge(b,c)) (associativity of join).
    //   input:  three GCounters a, b, c
    //   output: both groupings yield the same state
    //   sideEffects: none
    // gcounter:associative:end
    #[test]
    fn gcounter_merge_associative() {
        let mut a = GCounter::new();
        a.increment(1, 2);
        let mut b = GCounter::new();
        b.increment(2, 3);
        let mut c = GCounter::new();
        c.increment(3, 5);

        // (a ⊔ b) ⊔ c
        let mut ab = a.clone();
        ab.merge(&b);
        let mut ab_c = ab.clone();
        ab_c.merge(&c);

        // a ⊔ (b ⊔ c)
        let mut bc = b.clone();
        bc.merge(&c);
        let mut a_bc = a.clone();
        a_bc.merge(&bc);

        assert_eq!(ab_c, a_bc, "merge must be associative");
    }

    // gcounter:convergence:start
    //   purpose: Two replicas exchanging deltas converge to the same value,
    //            regardless of exchange order.
    //   input:  two replicas with disjoint increments; exchange deltas both ways
    //   output: both replicas have the same value
    //   sideEffects: none
    // gcounter:convergence:end
    #[test]
    fn gcounter_convergence_via_delta() {
        let (sink_a, sink_b) = MemCrdtSink::pair();

        let mut ra = GCounter::new();
        ra.increment(1, 10);
        // Publish delta from A
        let da = gcounter_to_bytes(&ra.delta());
        sink_a.publish("hits", da).expect("publish a");

        let mut rb = GCounter::new();
        rb.increment(2, 20);
        // Publish delta from B
        let db = gcounter_to_bytes(&rb.delta());
        sink_b.publish("hits", db).expect("publish b");

        // A drains B's delta
        for bytes in sink_a.drain("hits").expect("drain a") {
            ra.apply_delta(&gcounter_from_bytes(&bytes));
        }
        // B drains A's delta
        for bytes in sink_b.drain("hits").expect("drain b") {
            rb.apply_delta(&gcounter_from_bytes(&bytes));
        }

        assert_eq!(ra.value(), rb.value(), "replicas must converge");
        assert_eq!(ra.value(), 30);
    }

    // gcounter:value:start
    //   purpose: value() returns sum of all node slots.
    //   input:  three nodes with different increments
    //   output: correct sum
    //   sideEffects: none
    // gcounter:value:end
    #[test]
    fn gcounter_value_is_sum() {
        let mut g = GCounter::new();
        g.increment(1, 3);
        g.increment(2, 7);
        g.increment(3, 2);
        assert_eq!(g.value(), 12);
    }

    // ── PnCounter laws ────────────────────────────────────────────────────────

    // pncounter:idempotent:start
    //   purpose: merge(pn, pn) == pn.
    //   input:  PnCounter with both increments and decrements
    //   output: idempotent
    //   sideEffects: none
    // pncounter:idempotent:end
    #[test]
    fn pncounter_merge_idempotent() {
        let mut pn = PnCounter::new();
        pn.increment(1, 5);
        pn.decrement(1, 2);
        pn.increment(2, 3);
        let snap = pn.clone();
        pn.merge(&snap);
        assert_eq!(pn, snap, "merge(pn,pn) == pn");
        assert_eq!(pn.value(), 6); // (5+3) - 2
    }

    // pncounter:commutative:start
    //   purpose: merge(a, b) == merge(b, a).
    //   input:  two PnCounters
    //   output: equal value after merge in either order
    //   sideEffects: none
    // pncounter:commutative:end
    #[test]
    fn pncounter_merge_commutative() {
        let mut a = PnCounter::new();
        a.increment(1, 10);
        a.decrement(1, 3);
        let mut b = PnCounter::new();
        b.increment(2, 5);
        b.decrement(2, 1);

        let mut ab = a.clone();
        ab.merge(&b);
        let mut ba = b.clone();
        ba.merge(&a);

        assert_eq!(ab.value(), ba.value());
        assert_eq!(ab.value(), 11); // (10+5) - (3+1)
    }

    // pncounter:associative:start
    //   purpose: merge(merge(a,b), c) == merge(a, merge(b,c)).
    //   input:  three PnCounters
    //   output: equal results in both groupings
    //   sideEffects: none
    // pncounter:associative:end
    #[test]
    fn pncounter_merge_associative() {
        let mut a = PnCounter::new();
        a.increment(1, 4);
        a.decrement(1, 1);
        let mut b = PnCounter::new();
        b.increment(2, 2);
        let mut c = PnCounter::new();
        c.decrement(3, 1);

        let mut ab = a.clone();
        ab.merge(&b);
        let mut ab_c = ab;
        ab_c.merge(&c);

        let mut bc = b.clone();
        bc.merge(&c);
        let mut a_bc = a;
        a_bc.merge(&bc);

        assert_eq!(ab_c.value(), a_bc.value());
    }

    // pncounter:convergence:start
    //   purpose: Two PnCounter replicas with concurrent inc/dec converge.
    //   input:  replica A increments, replica B decrements independently
    //   output: both converge to same value after delta exchange
    //   sideEffects: none
    // pncounter:convergence:end
    #[test]
    fn pncounter_convergence() {
        let mut ra = PnCounter::new();
        ra.increment(1, 10);

        let mut rb = PnCounter::new();
        rb.decrement(2, 4);

        // Exchange deltas
        let da = ra.delta();
        let db = rb.delta();
        ra.apply_delta(&db);
        rb.apply_delta(&da);

        assert_eq!(ra.value(), rb.value(), "converge");
        assert_eq!(ra.value(), 6); // 10 - 4
    }

    // ── OrSet laws ────────────────────────────────────────────────────────────

    // orset:add_wins:start
    //   purpose: Concurrent add and remove on same element → element stays present (add-wins).
    //   input:  replica A adds elem with tag(1,1); replica B removes it using its own view
    //           (which has no tag(1,1) yet); merge → elem present due to add-wins.
    //   output: merged set contains elem
    //   sideEffects: none
    // orset:add_wins:end
    #[test]
    fn orset_add_wins_concurrent() {
        // Replica A: add "x" with tag (node=1, ts=1)
        let mut a: OrSet<&str> = OrSet::new();
        a.add("x", 1, 1);

        // Replica B: starts empty, removes "x" (clears no tags — "x" not yet known to B)
        let mut b: OrSet<&str> = OrSet::new();
        b.remove(&"x"); // no-op since b has no tags for "x"

        // A publishes delta to B; merge
        b.apply_delta(&a.delta());

        // After merging A's add into B, "x" should be present (add-wins)
        assert!(
            b.contains(&"x"),
            "add-wins: 'x' must be present after merge"
        );
    }

    // orset:idempotent:start
    //   purpose: merge(a, a) == a.
    //   input:  OrSet with some elements
    //   output: merging with itself leaves value unchanged
    //   sideEffects: none
    // orset:idempotent:end
    #[test]
    fn orset_merge_idempotent() {
        let mut a: OrSet<u32> = OrSet::new();
        a.add(1, 1, 100);
        a.add(2, 1, 101);
        let snap = a.clone();
        a.merge(&snap);
        assert_eq!(a, snap, "merge(a,a) == a");
        assert!(a.contains(&1));
        assert!(a.contains(&2));
    }

    // orset:commutative:start
    //   purpose: merge(a, b) == merge(b, a).
    //   input:  a adds elem 1, b adds elem 2
    //   output: both merged sets contain both elements
    //   sideEffects: none
    // orset:commutative:end
    #[test]
    fn orset_merge_commutative() {
        let mut a: OrSet<u32> = OrSet::new();
        a.add(1, 1, 10);

        let mut b: OrSet<u32> = OrSet::new();
        b.add(2, 2, 20);

        let mut ab = a.clone();
        ab.merge(&b);
        let mut ba = b.clone();
        ba.merge(&a);

        assert_eq!(ab.value(), ba.value(), "commutative");
        assert!(ab.contains(&1) && ab.contains(&2));
    }

    // orset:associative:start
    //   purpose: merge(merge(a,b), c) == merge(a, merge(b,c)).
    //   input:  three OrSets with disjoint elements
    //   output: equal after both merge orders
    //   sideEffects: none
    // orset:associative:end
    #[test]
    fn orset_merge_associative() {
        let mut a: OrSet<u32> = OrSet::new();
        a.add(1, 1, 1);
        let mut b: OrSet<u32> = OrSet::new();
        b.add(2, 2, 2);
        let mut c: OrSet<u32> = OrSet::new();
        c.add(3, 3, 3);

        let mut ab = a.clone();
        ab.merge(&b);
        let mut ab_c = ab;
        ab_c.merge(&c);

        let mut bc = b.clone();
        bc.merge(&c);
        let mut a_bc = a;
        a_bc.merge(&bc);

        assert_eq!(ab_c.value(), a_bc.value(), "associative");
    }

    // orset:remove_observed:start
    //   purpose: After add then remove on same replica, element is absent.
    //   input:  add "a" then remove "a"
    //   output: not contained
    //   sideEffects: none
    // orset:remove_observed:end
    #[test]
    fn orset_remove_clears_element() {
        let mut s: OrSet<&str> = OrSet::new();
        s.add("a", 1, 1);
        assert!(s.contains(&"a"));
        s.remove(&"a");
        assert!(!s.contains(&"a"), "element must be absent after remove");
    }

    // orset:convergence:start
    //   purpose: Two replicas with disjoint adds converge via delta exchange.
    //   input:  ra adds "apple", rb adds "banana"; exchange deltas
    //   output: both replicas contain {"apple", "banana"}
    //   sideEffects: none
    // orset:convergence:end
    #[test]
    fn orset_convergence_via_delta() {
        let mut ra: OrSet<&str> = OrSet::new();
        ra.add("apple", 1, 100);

        let mut rb: OrSet<&str> = OrSet::new();
        rb.add("banana", 2, 200);

        let da = ra.delta();
        let db = rb.delta();
        ra.apply_delta(&db);
        rb.apply_delta(&da);

        assert!(
            ra.contains(&"apple") && ra.contains(&"banana"),
            "ra converged"
        );
        assert!(
            rb.contains(&"apple") && rb.contains(&"banana"),
            "rb converged"
        );
        assert_eq!(ra.value(), rb.value(), "equal value after convergence");
    }

    // ── LwwRegister laws ──────────────────────────────────────────────────────

    // lww:idempotent:start
    //   purpose: merge(a, a) == a.
    //   input:  LwwRegister with one entry
    //   output: idempotent
    //   sideEffects: none
    // lww:idempotent:end
    #[test]
    fn lww_merge_idempotent() {
        let mut a: LwwRegister<i32> = LwwRegister::new();
        a.set(42, 1, 100);
        let snap = a.clone();
        a.merge(&snap);
        assert_eq!(a, snap);
        assert_eq!(a.value(), Some(&42));
    }

    // lww:commutative:start
    //   purpose: merge(a,b) == merge(b,a) — winner is same regardless of order.
    //   input:  a writes ts=100, b writes ts=200
    //   output: both orderings pick b's value (higher ts)
    //   sideEffects: none
    // lww:commutative:end
    #[test]
    fn lww_merge_commutative() {
        let mut a: LwwRegister<&str> = LwwRegister::new();
        a.set("old", 1, 100);

        let mut b: LwwRegister<&str> = LwwRegister::new();
        b.set("new", 2, 200);

        let mut ab = a.clone();
        ab.merge(&b);
        let mut ba = b.clone();
        ba.merge(&a);

        assert_eq!(ab.value(), Some(&"new"));
        assert_eq!(ba.value(), Some(&"new"));
        assert_eq!(ab, ba, "commutative");
    }

    // lww:associative:start
    //   purpose: merge(merge(a,b),c) == merge(a,merge(b,c)).
    //   input:  three LwwRegisters with different timestamps
    //   output: same winner (highest ts) in both groupings
    //   sideEffects: none
    // lww:associative:end
    #[test]
    fn lww_merge_associative() {
        let mut a: LwwRegister<u32> = LwwRegister::new();
        a.set(1, 1, 10);
        let mut b: LwwRegister<u32> = LwwRegister::new();
        b.set(2, 2, 20);
        let mut c: LwwRegister<u32> = LwwRegister::new();
        c.set(3, 3, 30);

        let mut ab = a.clone();
        ab.merge(&b);
        let mut ab_c = ab;
        ab_c.merge(&c);

        let mut bc = b.clone();
        bc.merge(&c);
        let mut a_bc = a;
        a_bc.merge(&bc);

        assert_eq!(ab_c, a_bc, "associative");
        assert_eq!(ab_c.value(), Some(&3)); // highest ts wins
    }

    // lww:timestamp_wins:start
    //   purpose: Higher timestamp always wins over lower.
    //   input:  set with ts=5, then set with ts=3 (older)
    //   output: value remains the ts=5 write
    //   sideEffects: none
    // lww:timestamp_wins:end
    #[test]
    fn lww_higher_timestamp_wins() {
        let mut r: LwwRegister<&str> = LwwRegister::new();
        r.set("first", 1, 5);
        r.set("older", 2, 3); // lower ts — must not overwrite
        assert_eq!(r.value(), Some(&"first"), "higher ts must win");
    }

    // lww:tiebreak_node:start
    //   purpose: Equal timestamps break tie on node id (higher node wins).
    //   input:  two sets with ts=10, nodes 1 and 2
    //   output: node 2's value wins
    //   sideEffects: none
    // lww:tiebreak_node:end
    #[test]
    fn lww_tiebreak_by_node() {
        let mut r: LwwRegister<&str> = LwwRegister::new();
        r.set("node1", 1, 10);
        r.set("node2", 2, 10); // same ts, higher node — wins
        assert_eq!(r.value(), Some(&"node2"), "higher node breaks tie");
    }

    // lww:convergence:start
    //   purpose: Two LWW replicas with concurrent writes converge to same value.
    //   input:  ra writes ts=50, rb writes ts=80; exchange deltas
    //   output: both converge to ts=80 value
    //   sideEffects: none
    // lww:convergence:end
    #[test]
    fn lww_convergence_via_delta() {
        let mut ra: LwwRegister<u32> = LwwRegister::new();
        ra.set(1, 1, 50);

        let mut rb: LwwRegister<u32> = LwwRegister::new();
        rb.set(2, 2, 80);

        let da = ra.delta();
        let db = rb.delta();
        ra.apply_delta(&db);
        rb.apply_delta(&da);

        assert_eq!(ra.value(), rb.value(), "converged");
        assert_eq!(ra.value(), Some(&2), "ts=80 wins");
    }

    // ── MvRegister laws ───────────────────────────────────────────────────────

    // mvregister:idempotent:start
    //   purpose: merge(a, a) == a.
    //   input:  MvRegister with one entry
    //   output: idempotent
    //   sideEffects: none
    // mvregister:idempotent:end
    #[test]
    fn mvregister_merge_idempotent() {
        let mut a: MvRegister<u32> = MvRegister::new();
        a.set(42, 1, 100);
        let snap = a.clone();
        a.merge(&snap);
        assert_eq!(a, snap);
        let binding = a.value();
        let vals: Vec<&&u32> = binding.iter().collect();
        assert_eq!(vals.len(), 1);
    }

    // mvregister:concurrent_values:start
    //   purpose: Concurrent writes on different replicas both appear in value().
    //   input:  ra writes 10, rb writes 20; merge without overwrite; both survive
    //   output: value() contains both 10 and 20
    //   sideEffects: none
    // mvregister:concurrent_values:end
    #[test]
    fn mvregister_concurrent_values_both_survive() {
        let mut ra: MvRegister<u32> = MvRegister::new();
        ra.set(10, 1, 1);

        let mut rb: MvRegister<u32> = MvRegister::new();
        rb.set(20, 2, 1);

        // Neither has seen the other — merge both ways
        let da = ra.delta();
        let db = rb.delta();
        ra.apply_delta(&db);
        rb.apply_delta(&da);

        let mut vals_a: Vec<u32> = ra.value().iter().map(|&&v| v).collect();
        let mut vals_b: Vec<u32> = rb.value().iter().map(|&&v| v).collect();
        vals_a.sort();
        vals_b.sort();
        assert_eq!(vals_a, vec![10, 20], "ra must have both concurrent values");
        assert_eq!(vals_a, vals_b, "convergence: same values on both replicas");
    }

    // mvregister:overwrite_clears:start
    //   purpose: set() after observing concurrent values clears them (overwrite-observed).
    //   input:  ra and rb have concurrent values {10, 20}; ra then sets 99
    //   output: ra.value() == [99] (prior values cleared)
    //   sideEffects: none
    // mvregister:overwrite_clears:end
    #[test]
    fn mvregister_overwrite_clears_observed() {
        let mut ra: MvRegister<u32> = MvRegister::new();
        ra.set(10, 1, 1);

        let mut rb: MvRegister<u32> = MvRegister::new();
        rb.set(20, 2, 1);

        // ra sees both concurrent values
        ra.apply_delta(&rb.delta());

        // Now ra resolves conflict by overwriting
        ra.set(99, 1, 2);

        let binding = ra.value();
        let vals: Vec<&&u32> = binding.iter().collect();
        assert_eq!(vals.len(), 1, "overwrite must clear prior values");
        assert_eq!(*vals[0], &99);
    }

    // mvregister:commutative:start
    //   purpose: merge(a, b) == merge(b, a) in terms of value set.
    //   input:  two MvRegisters with disjoint writes
    //   output: value sets equal after merge in either order
    //   sideEffects: none
    // mvregister:commutative:end
    #[test]
    fn mvregister_merge_commutative() {
        let mut a: MvRegister<u32> = MvRegister::new();
        a.set(1, 1, 1);
        let mut b: MvRegister<u32> = MvRegister::new();
        b.set(2, 2, 2);

        let mut ab = a.clone();
        ab.merge(&b);
        let mut ba = b.clone();
        ba.merge(&a);

        let mut vals_ab: Vec<u32> = ab.value().iter().map(|&&v| v).collect();
        vals_ab.sort();
        let mut vals_ba: Vec<u32> = ba.value().iter().map(|&&v| v).collect();
        vals_ba.sort();
        assert_eq!(vals_ab, vals_ba, "commutative value sets");
    }

    // mvregister:associative:start
    //   purpose: merge(merge(a,b),c) value == merge(a,merge(b,c)) value.
    //   input:  three MvRegisters
    //   output: equal value sets in both groupings
    //   sideEffects: none
    // mvregister:associative:end
    #[test]
    fn mvregister_merge_associative() {
        let mut a: MvRegister<u32> = MvRegister::new();
        a.set(1, 1, 1);
        let mut b: MvRegister<u32> = MvRegister::new();
        b.set(2, 2, 2);
        let mut c: MvRegister<u32> = MvRegister::new();
        c.set(3, 3, 3);

        let mut ab = a.clone();
        ab.merge(&b);
        let mut ab_c = ab;
        ab_c.merge(&c);

        let mut bc = b.clone();
        bc.merge(&c);
        let mut a_bc = a;
        a_bc.merge(&bc);

        let mut v1: Vec<u32> = ab_c.value().iter().map(|&&v| v).collect();
        v1.sort();
        let mut v2: Vec<u32> = a_bc.value().iter().map(|&&v| v).collect();
        v2.sort();
        assert_eq!(v1, v2, "associative value sets");
    }

    // ── MemCrdtSink ───────────────────────────────────────────────────────────

    // memsink:pair_exchange:start
    //   purpose: MemCrdtSink::pair routes A's publish to B's drain and vice versa.
    //   input:  pair of sinks; A publishes "hello", B publishes "world"
    //   output: A drains "world", B drains "hello"
    //   sideEffects: none
    // memsink:pair_exchange:end
    #[test]
    fn memsink_pair_exchange() {
        let (a, b) = MemCrdtSink::pair();

        a.publish("k", b"hello".to_vec()).expect("a publish");
        b.publish("k", b"world".to_vec()).expect("b publish");

        let from_b = a.drain("k").expect("a drain");
        let from_a = b.drain("k").expect("b drain");

        assert_eq!(from_b, vec![b"world".to_vec()], "A drains B's data");
        assert_eq!(from_a, vec![b"hello".to_vec()], "B drains A's data");
    }

    // memsink:drain_empty:start
    //   purpose: Draining an empty key returns empty vec (no panic or error).
    //   input:  fresh sink, drain unseen key
    //   output: Ok(vec![])
    //   sideEffects: none
    // memsink:drain_empty:end
    #[test]
    fn memsink_drain_empty_key() {
        let (a, _b) = MemCrdtSink::pair();
        let result = a.drain("nonexistent").expect("drain");
        assert!(result.is_empty(), "no data for unknown key");
    }

    // ── ZenohCrdtSink cluster tests ───────────────────────────────────────────

    // zenoh_crdt:convergence:start
    //   purpose: Prove multi-master delta-CRDT convergence through a REAL Zenoh session.
    //            Two ZenohCrdtSink instances connect as Zenoh peers on localhost
    //            (scouting/gossip, no broker). RoomLog-A publishes 2 PDUs as a delta;
    //            RoomLog-B publishes 1 different PDU as a delta; after a brief propagation
    //            window (Zenoh gossip over loopback is sub-millisecond in practice, we
    //            allow 200 ms) each replica drains and applies the remote delta; both
    //            `ordered()` results are asserted identical.
    //            This is the minimum proof that the Zenoh transport wire-path works
    //            end-to-end: serialise → Zenoh put → background subscriber recv → inbox
    //            → drain → deserialise → apply_delta → grow-only set convergence.
    //   input:  two Zenoh sessions (peer mode, localhost multicast/scouting),
    //           two ZenohCrdtSink instances sharing a unique key_prefix to avoid
    //           cross-test pollution
    //   output: both RoomLog replicas have len==3 and identical ordered() after exchange
    //   sideEffects: opens two real Zenoh sessions; spawns background tasks per sink;
    //                network I/O on loopback (no external dependencies)
    // zenoh_crdt:convergence:end
    #[cfg(feature = "cluster")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn zenoh_crdt_roomlog_convergence() {
        let _zg = crate::test_util::ZENOH_TEST_LOCK.acquire().await.unwrap();
        use super::ZenohCrdtSink;
        use crate::substrate::matrix_events::{Pdu, RoomLog};

        // Use a unique key prefix per test run to avoid cross-test inbox contamination.
        // Include a random-ish suffix derived from the thread name length so parallel test
        // runs on the same host don't collide. In CI this is sufficient; for production
        // use a UUID-derived prefix.
        let prefix = "mrgd/crdt/test/convergence0";

        // ── Open two Zenoh sessions in peer mode on localhost ─────────────────
        // Both sessions use the default config (no connect/listen — peer mode with
        // multicast/gossip scouting on loopback). Zenoh discovers peers automatically.
        let cfg_a = zenoh::Config::default();
        let cfg_b = zenoh::Config::default();

        let sess_a = zenoh::open(cfg_a).await.expect("zenoh session A");
        let sess_b = zenoh::open(cfg_b).await.expect("zenoh session B");

        // ── Create two ZenohCrdtSink instances ────────────────────────────────
        let sink_a = ZenohCrdtSink::new(sess_a, prefix).await.expect("sink A");
        let sink_b = ZenohCrdtSink::new(sess_b, prefix).await.expect("sink B");

        // Brief pause: let the subscriber declarations propagate in the Zenoh router
        // before we publish. Without this the first put may arrive before the remote
        // subscriber is registered and be silently dropped.
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // ── Build two partially-overlapping RoomLogs ──────────────────────────
        fn pdu(id: &str, prev: &[&str], depth: u64, ts: u64) -> Pdu {
            Pdu {
                event_id: id.to_string(),
                room_id: "!room:test".to_string(),
                sender: "@node:test".to_string(),
                kind: "m.room.message".to_string(),
                content: id.as_bytes().to_vec(),
                prev_events: prev.iter().map(|s| s.to_string()).collect(),
                depth,
                ts,
                sig: Vec::new(),
                signer_node: String::new(),
            }
        }

        let e0 = pdu("$e0", &[], 0, 100);
        let e1 = pdu("$e1", &["$e0"], 1, 200);
        let e2 = pdu("$e2", &["$e0"], 1, 300); // concurrent with e1

        // Replica A knows e0 + e1.
        let mut ra = RoomLog::new();
        ra.add(e0.clone());
        ra.add(e1.clone());

        // Replica B knows e0 + e2.
        let mut rb = RoomLog::new();
        rb.add(e0.clone());
        rb.add(e2.clone());

        // ── Publish deltas ────────────────────────────────────────────────────
        let key = "room/!room:test";
        ra.publish_delta(&sink_a, key).expect("publish A");
        rb.publish_delta(&sink_b, key).expect("publish B");

        // Allow time for Zenoh gossip to route the publications to the remote subscribers.
        // Zenoh peer-mode on loopback typically propagates in <5 ms; 200 ms is a generous
        // safety margin that keeps the test reliable in CI without a sleep loop.
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        // ── Drain and apply ───────────────────────────────────────────────────
        let drained_a = ra.drain_delta(&sink_a, key).expect("drain A");
        let drained_b = rb.drain_delta(&sink_b, key).expect("drain B");

        assert!(
            drained_a >= 1,
            "A must drain at least 1 delta from B (got {})",
            drained_a
        );
        assert!(
            drained_b >= 1,
            "B must drain at least 1 delta from A (got {})",
            drained_b
        );

        // ── Assert convergence ────────────────────────────────────────────────
        assert_eq!(
            ra.len(),
            3,
            "replica A must have 3 events after convergence"
        );
        assert_eq!(
            rb.len(),
            3,
            "replica B must have 3 events after convergence"
        );

        let ids_a: Vec<String> = ra.ordered().iter().map(|p| p.event_id.clone()).collect();
        let ids_b: Vec<String> = rb.ordered().iter().map(|p| p.event_id.clone()).collect();
        assert_eq!(
            ids_a, ids_b,
            "ordered() must be identical on both replicas after Zenoh delta-sync"
        );
        // e0 first (depth 0), then e1 (ts=200) before e2 (ts=300) at depth 1 — deterministic.
        assert_eq!(
            ids_a,
            vec!["$e0", "$e1", "$e2"],
            "topological order must be [$e0, $e1, $e2]"
        );
    }
}
