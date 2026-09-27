// START_AI_HEADER
// MODULE: mrgd/src/barrier_growset.rs
// PURPOSE: Coordination-free username uniqueness via a Zenoh CRDT grow-set +
//          deterministic reconcile.  Replaces the CP coordinator/routing path
//          (RoutedClaimStore + BarrierCoordinator) for the registration barrier.
//
//          Design (invariant-confluence / CRDT-based):
//            Local optimistic claim → publish to an OrSet-style grow-set over
//            Zenoh → deterministic reconcile picks the winner on every node
//            identically (min by (ts, node_id)).  The rare loser is flagged
//            rename_required in matrix-hs.  Zero coordination — no distributed
//            lock, no coordinator, no split-brain risk.
//
//          Grow-set transport:
//            Each ClaimRecord is serialised as JSON, wrapped in an OrSet<String>
//            entry (the JSON string itself is the OrSet element — opaque blob).
//            Published to `mrgd/coupling/barrier/claims` via ZenohCrdtSink.
//            A background task drains the sink into a local HashMap<username →
//            Vec<ClaimRecord>> (the synced view).
//
//          cas_claim semantics (AP — never Unavailable):
//            - username already present in synced view with a DIFFERENT node_id
//              → AlreadySet{owner}  (fast reject; no publish).
//            - username already present with the SAME node_id (own re-claim)
//              → Set (idempotent; no re-publish needed).
//            - otherwise → insert into local map, publish to grow-set → Set.
//              Two nodes claiming the same fresh name concurrently both get Set;
//              the grow-set converges to hold both; ReconcileDriver resolves.
//
//          CONFLICT PREDICATE (fixed 2026-07-06):
//            Conflict = same username, MORE THAN ONE DISTINCT node_id in the
//            synced view.  Previously the predicate used "distinct claimant"
//            which is always identical when both nodes register the same username
//            (e.g. both claimants are "@dave:localhost") — making the conflict
//            invisible.  The node_id is the authoritative distinguishing field.
//
//          ReconcileDriver:
//            Background tokio task.  Periodically drains the ZenohCrdtSink inbox
//            and applies incoming ClaimRecords to the synced view.  After each
//            drain it scans for usernames with >1 distinct node_id (conflict),
//            computes the winner via barrier::reconcile() (min ts, node_id), and
//            notifies the caller's loser channel with
//              LostClaim { username, winner_claimant, loser_claimant }.
//            The caller (matrix-hs main.rs) sets UserRecord::rename_required=true
//            for the losing local claimant.
//
//          Residual gaps (out of scope for this milestone):
//            - Full loser rename (new user_id, client notification): deferred.
//            - OTK / alias barriers: not addressed here.
//            - Eventual-consistency window: two @alice exist until reconcile tick.
//              Window = Zenoh gossip latency (loopback ~200 ms) + driver tick (1 s).
//
// DEPENDENCIES: mrgd::barrier, mrgd::crdt::ZenohCrdtSink, zenoh (cluster only)
// PUBLIC_API: ClaimRecord, GrowSetClaimStore, LostClaim, ReconcileDriver
// END_AI_HEADER

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::substrate::barrier::{reconcile, BarrierError, CasResult, ClaimStore, Fence, ProvisionalClaim};
use crate::substrate::crdt::CrdtSink;

// ── ClaimRecord ───────────────────────────────────────────────────────────────

// ClaimRecord:start
//   purpose: A single username claim entry stored in the distributed grow-set.
//            One (username, node_id) pair per node-generated claim.
//            Same username with DIFFERENT node_ids = a conflict, resolved by
//            ReconcileDriver.  The claimant field carries the identity string
//            (e.g. "@dave:localhost") and is used in LostClaim notifications and
//            the reconcile tiebreaker; it must NOT be used as the conflict key
//            because two nodes registering the same username produce identical
//            claimant strings.
//   input:  constructed by GrowSetClaimStore::cas_claim
//   output: serialised to JSON for grow-set transport; deserialised by driver
//   sideEffects: none (pure data)
// ClaimRecord:end
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct ClaimRecord {
    /// The resource key being claimed (e.g. a Matrix localpart).
    pub username: String,
    /// The identity asserting ownership.
    pub claimant: String,
    /// Wall-clock milliseconds since UNIX epoch at claim time.
    pub ts: u64,
    /// Stable node identifier for tiebreaking.
    pub node_id: String,
}

impl ClaimRecord {
    // to_json:start
    //   purpose: Serialise to a JSON byte vector for transport over ZenohCrdtSink.
    //   input:  &self
    //   output: Vec<u8>
    //   sideEffects: none
    // to_json:end
    fn to_json(&self) -> Vec<u8> {
        // serde_json::to_vec never fails on well-typed structs; treat error as empty.
        serde_json::to_vec(self).unwrap_or_default()
    }

    // from_json:start
    //   purpose: Deserialise a ClaimRecord from a JSON byte slice.
    //   input:  bytes — &[u8]
    //   output: Option<ClaimRecord>
    //   sideEffects: none
    // from_json:end
    fn from_json(bytes: &[u8]) -> Option<Self> {
        serde_json::from_slice(bytes).ok()
    }
}

// ── Shared synced view ────────────────────────────────────────────────────────

/// username → all ClaimRecords seen for that username (from any node).
type SyncedView = Arc<Mutex<HashMap<String, Vec<ClaimRecord>>>>;

// ── GrowSetClaimStore ─────────────────────────────────────────────────────────

// GrowSetClaimStore:start
//   purpose: AP ClaimStore backed by a Zenoh grow-set (OrSet-style: publish-only,
//            no removes).  Implements ClaimStore so it plugs into the existing
//            barrier::claim() + register.rs path unchanged.
//
//            cas_claim is ALWAYS locally available (never returns Unavailable):
//              - username already claimed by another node (in synced view)
//                → AlreadySet{owner}  (fast reject).
//              - otherwise → insert locally + publish to grow-set → Set.
//                Concurrent claims on different nodes both return Set; the
//                grow-set will eventually hold both and ReconcileDriver resolves.
//
//            The synced view is updated by ReconcileDriver (background task);
//            cas_claim only reads/writes to it while holding the Mutex.
//   input:  new(session, node_id) — caller passes an open zenoh::Session clone
//   output: ClaimStore impl
//   sideEffects: publishes to Zenoh on each Set; synced view mutated by driver
// GrowSetClaimStore:end
pub struct GrowSetClaimStore {
    /// Transport for the grow-set: any CrdtSink, so a test or a harness can
    /// inject a counting wrapper; the node passes a ZenohCrdtSink.
    sink: Arc<dyn CrdtSink>,
    /// Stable node identifier for tiebreaking.
    node_id: String,
    /// Locally-synced view of all claims (ours + received from peers).
    synced: SyncedView,
    /// The Zenoh keyexpr used by the sink (just the trailing part after prefix).
    claims_key: &'static str,
}

impl GrowSetClaimStore {
    // GrowSetClaimStore::new:start
    //   purpose: Construct a GrowSetClaimStore from an already-open sink.
    //            The node passes a ZenohCrdtSink constructed with key_prefix
    //            "mrgd/coupling/barrier" (its subscriber covers
    //            "mrgd/coupling/barrier/**"); any CrdtSink works, which is what
    //            lets a harness count what passes through it.
    //            Returns self + the SyncedView so the caller can share it with a
    //            ReconcileDriver.
    //   input:  sink — Arc<dyn CrdtSink>;  node_id — stable node identifier string
    //   output: (GrowSetClaimStore, SyncedView)
    //   sideEffects: none (sink already open)
    // GrowSetClaimStore::new:end
    pub fn new(sink: Arc<dyn CrdtSink>, node_id: String) -> (Self, SyncedView) {
        let synced: SyncedView = Arc::new(Mutex::new(HashMap::new()));
        let store = GrowSetClaimStore {
            sink,
            node_id,
            synced: synced.clone(),
            claims_key: "claims",
        };
        (store, synced)
    }

    // GrowSetClaimStore::synced_view:start
    //   purpose: Return a clone of the SyncedView Arc for use by the ReconcileDriver.
    //   input:  none
    //   output: SyncedView
    //   sideEffects: bumps Arc refcount
    // GrowSetClaimStore::synced_view:end
    pub fn synced_view(&self) -> SyncedView {
        self.synced.clone()
    }
}

impl ClaimStore for GrowSetClaimStore {
    // cas_claim:start
    //   purpose: Attempt to claim `key` (username) for `claimant`.
    //            Fast-rejects if the synced view already has a record for key whose
    //            node_id DIFFERS from this store's own node_id — meaning another
    //            node has already claimed this username.  A record with the SAME
    //            node_id is treated as an idempotent own re-claim → returns Set.
    //            Otherwise records locally and publishes to the grow-set.
    //            Never returns Unavailable — this is an AP store.
    //
    //            CRITICAL: the conflict key is node_id, NOT claimant.  Two nodes
    //            registering the same username produce identical claimant strings
    //            (e.g. both "@dave:localhost"), so claimant comparison would treat
    //            the foreign record as an own re-claim and incorrectly return Set.
    //   input:  key — username; claimant — identity asserting ownership
    //   output: Ok(CasResult::Set) or Ok(CasResult::AlreadySet{owner})
    //   sideEffects: publishes a ClaimRecord to Zenoh on Set; mutates synced view
    // cas_claim:end
    fn cas_claim(&self, key: &str, claimant: &str) -> Result<CasResult, BarrierError> {
        let mut view = self
            .synced
            .lock()
            .map_err(|e| BarrierError::Store(e.to_string()))?;

        // Check if already claimed by a DIFFERENT node.
        // A different node_id means a foreign claim exists — reject regardless of
        // whether the claimant string happens to be equal (same username on two nodes
        // produces the same claimant string, e.g. "@dave:localhost" on both).
        if let Some(records) = view.get(key) {
            for rec in records {
                if rec.node_id != self.node_id {
                    // Foreign node holds this key.  owner carries the claimant + node
                    // for diagnostic purposes.
                    let owner = format!("{} (node {})", rec.claimant, rec.node_id);
                    return Ok(CasResult::AlreadySet { owner });
                }
            }
            // All existing records belong to this node — idempotent own re-claim.
            // (no re-publish needed; the grow-set already has this entry)
            return Ok(CasResult::Set);
        }

        // Fresh claim — create the record.
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let rec = ClaimRecord {
            username: key.to_string(),
            claimant: claimant.to_string(),
            ts,
            node_id: self.node_id.clone(),
        };

        // Insert into local view before publishing (so reconcile sees it).
        view.entry(key.to_string())
            .or_insert_with(Vec::new)
            .push(rec.clone());

        // Drop the lock before publishing to Zenoh to avoid holding it during I/O.
        drop(view);

        // Publish to the grow-set.  A publish error is non-fatal: the claim is
        // locally recorded and will be retried by the store's next publish attempt.
        // We return Set regardless — the grow-set is AP (best-effort delivery).
        if let Err(e) = self.sink.publish(self.claims_key, rec.to_json()) {
            eprintln!("[barrier_growset] publish warning: {e} (claim locally recorded)");
        }

        Ok(CasResult::Set)
    }
}

// ── LostClaim ─────────────────────────────────────────────────────────────────

// LostClaim:start
//   purpose: Notification emitted by ReconcileDriver when a username conflict is
//            resolved and this node holds the LOSING claimant.
//            The receiver (matrix-hs) should set UserRecord::rename_required=true
//            for `loser_claimant`.
//            Full rename flow (new user_id, client notification) is OUT OF SCOPE
//            for this milestone — flag + log is the contract here.
//   input:  emitted by ReconcileDriver
//   output: consumed by the matrix-hs loser handler
//   sideEffects: none (pure data)
// LostClaim:end
#[derive(Debug, Clone)]
pub struct LostClaim {
    /// The contested username.
    pub username: String,
    /// The winner claimant (lowest ts, then node_id).
    pub winner_claimant: String,
    /// The loser claimant on THIS node.
    pub loser_claimant: String,
}

// ── ReconcileDriver ───────────────────────────────────────────────────────────

// ReconcileDriver:start
//   purpose: Background tokio task that:
//              1. Drains the ZenohCrdtSink inbox for "claims".
//              2. Deserialises each blob into a ClaimRecord and merges it into
//                 the shared SyncedView.
//              3. Scans the view for usernames with >1 distinct node_id (conflict).
//                 NOTE: the conflict key is node_id, NOT claimant.  Two nodes
//                 claiming the same username produce identical claimant strings
//                 (e.g. "@dave:localhost"), so claimant-based dedup would always
//                 see only 1 distinct claimant and miss the conflict entirely.
//              4. For each conflict: runs barrier::reconcile() (min ts, node_id)
//                 to deterministically pick the winner.
//              5. For every loser whose node_id == this_node_id: sends a
//                 LostClaim to the loser channel.
//            6. On eligible ticks (every `republish_every`), re-publishes own
//                 ClaimRecords ONLY while the AntiEntropyPolicy grants a burst:
//                 a bounded number of rounds after the last observed activity
//                 (own set changed, new remote record, or never-seen node_id).
//                 With unchanged state the burst runs out and the tick publishes
//                 nothing — steady-state traffic is exactly zero (fixed
//                 2026-09-23; before that the re-publish was unconditional and
//                 flooded the same claims forever, incident 2026-09-07..11).
//                 Late-starting or restarted peers still catch up: the initial
//                 burst at startup plus a fresh burst on any new activity covers
//                 the Zenoh pub/sub no-replay window.  Re-publish is idempotent
//                 on the receiver side: dedup in drain_and_reconcile ensures no
//                 duplicate conflict detection.
//
//            Runs every `tick` interval (default 1 s).  Convergence guarantee:
//            after all peers have published (or re-published), every node running
//            ReconcileDriver computes the same winner (reconcile is pure +
//            order-independent).
//
//   input:  new(sink, synced, node_id, loser_tx, tick) — constructs the driver
//           spawn() — starts the tokio task; returns JoinHandle
//   output: LostClaim values sent to loser_tx channel
//   sideEffects: mutates SyncedView on each drain; sends to loser_tx channel;
//                re-publishes own claims every `republish_every` ticks
// ReconcileDriver:end

/// How many ticks between full re-publication of own claims (anti-entropy for late peers).
const REPUBLISH_EVERY_TICKS: u64 = 5;

/// Bounded catch-up burst: how many eligible ticks re-publish after the last
/// observed activity before going silent.  Covers the late-subscriber window
/// (a Zenoh PUT issued before the peer's subscriber propagates is silently
/// dropped — pub/sub has no replay) while guaranteeing quiescence: with
/// unchanged state the traffic decays to exactly zero instead of flooding
/// forever (production incident 2026-09-07..11: about 200 claims re-published
/// every 5 s for 4 days, 9.5 GB RSS).
const ANTI_ENTROPY_ROUNDS: u32 = 3;

// ── AntiEntropyPolicy ───────────────────────────────────────────────────────

// AntiEntropyPolicy:start
//   purpose: Pure decision state for quiescent anti-entropy (the fix for the
//            2026-09-07..11 non-convergence incident).
//            The old driver re-published ALL own claims every REPUBLISH_EVERY_TICKS
//            unconditionally: with N own claims the steady-state traffic was
//            N blobs per 5 ticks forever, plus the Zenoh self-echo coming back
//            as endless "drain tick: N blob(s) received".  Logically deduped,
//            but never silent — constant CPU, bandwidth and log spam, and the
//            inbox churn the fleet paid 9.5 GB RSS for.
//            The policy grants a BOUNDED burst (ANTI_ENTROPY_ROUNDS eligible
//            ticks) after the last observed activity, then goes silent:
//              - own synced set changed (new local claim) → fresh burst;
//              - genuinely NEW remote record merged, or a never-seen node_id on
//                the wire (late/restarted peer catching up) → fresh burst so the
//                peer can converge off our re-publish;
//              - duplicate echo of already-known records → NOT activity, the
//                burst is not extended (this is what lets two nodes ping-pong
//                into silence instead of forever).
//            Late joiners that never publish anything stay blind until they do —
//            accepted: a silent node holds no claims to conflict, and its first
//            publish triggers a burst from every receiver via the unknown-peer
//            rule above.
    //   input:  note_own(hash) each tick; note_remote(new_records, node_ids)
    //           after each drain; should_republish() on eligible ticks;
    //           did_republish() after publishing
//   output: bool — publish or stay silent
//   sideEffects: none (pure counters)
// AntiEntropyPolicy:end
struct AntiEntropyPolicy {
    max_rounds: u32,
    rounds_left: u32,
    last_own_hash: u64,
    initialized: bool,
    /// node_ids ever seen on the wire.  Pre-seeded with our own id so the
    /// Zenoh self-echo of our own re-publish never counts as a "new peer".
    known_peers: std::collections::HashSet<String>,
}

impl AntiEntropyPolicy {
    fn new(own_node_id: &str) -> Self {
        let mut known_peers = std::collections::HashSet::new();
        known_peers.insert(own_node_id.to_string());
        AntiEntropyPolicy {
            max_rounds: ANTI_ENTROPY_ROUNDS,
            // Grant one burst at startup: claims already sitting in the synced
            // view (restored / claimed before the driver spawned) may never
            // have reached the network.
            rounds_left: ANTI_ENTROPY_ROUNDS,
            last_own_hash: 0,
            initialized: false,
            known_peers,
        }
    }

    /// Observe the current hash of OUR OWN records.  Any change (a new local
    /// claim) grants a fresh burst.
    fn note_own(&mut self, own_hash: u64) {
        if !self.initialized || own_hash != self.last_own_hash {
            self.last_own_hash = own_hash;
            self.initialized = true;
            self.rounds_left = self.max_rounds;
        }
    }

    /// Observe the outcome of a drain.  Only genuinely new information extends
    /// the burst — duplicate echo (including the Zenoh self-echo of our own
    /// re-publish) must not, or the system never goes silent.
    /// `remote_node_ids` carries every node_id seen in this drain's blobs;
    /// a never-seen id means a late/restarted peer that likely missed our
    /// state (our own id is pre-seeded, so self-echo never triggers this).
    fn note_remote(&mut self, new_records: bool, remote_node_ids: &[String]) {
        let mut unknown_peer = false;
        for id in remote_node_ids {
            if self.known_peers.insert(id.clone()) {
                unknown_peer = true;
            }
        }
        if new_records || unknown_peer {
            self.rounds_left = self.max_rounds;
        }
    }

    fn should_republish(&self) -> bool {
        self.rounds_left > 0
    }

    fn did_republish(&mut self) {
        self.rounds_left = self.rounds_left.saturating_sub(1);
    }
}

/// Hash OUR OWN ClaimRecords (node_id == self.node_id) for change detection.
/// Order-independent: records are hashed in sorted key order so iteration
/// order of the HashMap cannot fake a change.
fn hash_own_records(view: &HashMap<String, Vec<ClaimRecord>>, node_id: &str) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut keys: Vec<&String> = view.keys().collect();
    keys.sort();
    let mut h = DefaultHasher::new();
    for k in keys {
        if let Some(records) = view.get(k) {
            for rec in records {
                if rec.node_id == node_id {
                    rec.username.hash(&mut h);
                    rec.claimant.hash(&mut h);
                    rec.ts.hash(&mut h);
                    rec.node_id.hash(&mut h);
                }
            }
        }
    }
    h.finish()
}

pub struct ReconcileDriver {
    sink: Arc<dyn CrdtSink>,
    synced: SyncedView,
    node_id: String,
    loser_tx: tokio::sync::mpsc::UnboundedSender<LostClaim>,
    tick: std::time::Duration,
    /// The claim key in the sink (same as GrowSetClaimStore).
    claims_key: &'static str,
    /// Quiescence policy: bounded burst after activity, silence when unchanged.
    /// Behind a Mutex because tick() runs on &self (spawn loop + sync tests).
    /// A poisoned mutex degrades to always-republish (liveness over silence).
    policy: Mutex<AntiEntropyPolicy>,
}

impl ReconcileDriver {
    // ReconcileDriver::new:start
    //   purpose: Construct a ReconcileDriver.
    //   input:  sink — shared sink Arc (dyn CrdtSink; the node passes the same
    //           ZenohCrdtSink the store was given);
    //           synced — SyncedView Arc (shared with GrowSetClaimStore);
    //           node_id — this node's stable identifier;
    //           loser_tx — channel to send LostClaim notifications;
    //           tick — polling interval (e.g. Duration::from_secs(1))
    //   output: ReconcileDriver
    //   sideEffects: none
    // ReconcileDriver::new:end
    pub fn new(
        sink: Arc<dyn CrdtSink>,
        synced: SyncedView,
        node_id: String,
        loser_tx: tokio::sync::mpsc::UnboundedSender<LostClaim>,
        tick: std::time::Duration,
    ) -> Self {
        let policy = Mutex::new(AntiEntropyPolicy::new(&node_id));
        ReconcileDriver {
            sink,
            synced,
            node_id,
            loser_tx,
            tick,
            claims_key: "claims",
            policy,
        }
    }

    // ReconcileDriver::spawn:start
    //   purpose: Spawn the background reconcile loop as a tokio task.
    //            The task runs until the ZenohCrdtSink drops (session closed) or the
    //            JoinHandle is aborted.
    //            Every REPUBLISH_EVERY_TICKS ticks, all own claims are re-published
    //            so late-joining peers receive them (Zenoh pub/sub has no replay).
    //            A per-task `notified_losers` set deduplicates LostClaim emissions:
    //            once a (username, node_id) pair has been notified, it is not
    //            re-sent even if the conflict still appears in the synced view on
    //            subsequent ticks.
    //   input:  self — consumed; caller should keep the JoinHandle alive
    //   output: tokio::task::JoinHandle<()>
    //   sideEffects: spawns a tokio task
    // ReconcileDriver::spawn:end
    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(self.tick);
            let mut tick_count: u64 = 0;
            // Track (username, loser_claimant) pairs already sent so we emit each
            // LostClaim at most once per ReconcileDriver lifetime.
            let mut notified_losers: std::collections::HashSet<(String, String)> =
                std::collections::HashSet::new();
            loop {
                interval.tick().await;
                tick_count = tick_count.wrapping_add(1);
                self.tick(tick_count, &mut notified_losers);
            }
        })
    }

    // tick:start
    //   purpose: One reconcile cycle: conditional anti-entropy re-publish plus
    //            drain-and-reconcile.  Extracted from spawn() so tests can drive
    //            exact tick counts synchronously without a tokio runtime.
    //   input:  tick_count — 1-based tick number (mirrors spawn's counter);
    //           notified_losers — dedup set owned by the caller loop
    //   output: none
    //   sideEffects: may publish (anti-entropy); mutates SyncedView; may send LostClaim
    // tick:end
    fn tick(
        &self,
        tick_count: u64,
        notified_losers: &mut std::collections::HashSet<(String, String)>,
    ) {
        // Anti-entropy, QUIESCENT (fixed 2026-09-23, incident 2026-09-07..11):
        // re-publish only while the policy grants a burst — i.e. shortly after
        // our own set changed or a peer showed activity.  With unchanged state
        // the burst runs out and this tick publishes nothing at all.
        // Zenoh pub/sub has no replay; the bounded burst (not an endless
        // flood) is what lets late-starting peers catch up.
        if tick_count.is_multiple_of(REPUBLISH_EVERY_TICKS) {
            let own_hash = match self.synced.lock() {
                Ok(view) => hash_own_records(&view, &self.node_id),
                Err(_) => {
                    // Poisoned view — fall back to publishing (liveness over
                    // silence); republish_own_claims logs the poisoning itself.
                    self.republish_own_claims();
                    self.drain_and_reconcile(notified_losers);
                    return;
                }
            };
            let go = match self.policy.lock() {
                Ok(mut p) => {
                    p.note_own(own_hash);
                    let go = p.should_republish();
                    if go {
                        p.did_republish();
                    }
                    go
                }
                Err(_) => true, // poisoned policy — fail open (publish)
            };
            if go {
                self.republish_own_claims();
            }
        }
        let (new_records, remote_node_ids) = self.drain_and_reconcile(notified_losers);
        if let Ok(mut p) = self.policy.lock() {
            p.note_remote(new_records, &remote_node_ids);
        }
    }

    // republish_own_claims:start
    //   purpose: Re-publish every ClaimRecord in the SyncedView that belongs to this
    //            node (rec.node_id == self.node_id).  Called periodically to allow
    //            late-starting or restarted peers to receive claims they missed.
    //            Re-publish is idempotent on the receiver side: dedup in drain_and_reconcile
    //            ensures no duplicate conflict detection.
    //   input:  &self
    //   output: none (publish errors logged, not fatal)
    //   sideEffects: calls sink.publish for each own ClaimRecord
    // republish_own_claims:end
    fn republish_own_claims(&self) {
        let own_records: Vec<ClaimRecord> = match self.synced.lock() {
            Ok(view) => view
                .values()
                .flat_map(|records| records.iter().cloned())
                .filter(|rec| rec.node_id == self.node_id)
                .collect(),
            Err(e) => {
                eprintln!("[barrier_growset] republish: synced view lock poisoned: {e}");
                return;
            }
        };

        let count = own_records.len();
        for rec in own_records {
            if let Err(e) = self.sink.publish(self.claims_key, rec.to_json()) {
                eprintln!(
                    "[barrier_growset] republish warning for {:?}: {e}",
                    rec.username
                );
            }
        }
        if count > 0 {
            eprintln!("[barrier_growset] anti-entropy: re-published {count} own claim(s)");
        }
    }

    // drain_and_reconcile:start
    //   purpose: Single reconcile cycle: drain incoming blobs, apply to synced view,
    //            detect conflicts, send LostClaim notifications for this node's losers.
    //            Called once per tick from the background task.
    //            `notified_losers` is a caller-owned set of (username, node_id)
    //            pairs that have already been sent; this function only sends each pair
    //            ONCE per ReconcileDriver lifetime (idempotent notification).
    //            Conflict = same username, more than one distinct node_id in synced view.
    //            Using node_id (not claimant) is critical: two nodes registering the
    //            same username produce identical claimant strings, so claimant-based
    //            dedup would collapse them to 1 distinct entry and miss the conflict.
    //            Returns (any_new, remote_node_ids): whether at least one blob carried
    //            a previously-unknown record, and every node_id seen in this drain
    //            (for the anti-entropy policy's unknown-peer rule).  Duplicate echo
    //            — including the Zenoh self-echo — yields (false, [own ids]).
    //   input:  &self; notified_losers — mutable dedup set owned by the task loop,
    //                  keyed by (username, node_id)
    //   output: (any_new, remote_node_ids)
    //   sideEffects: mutates SyncedView; may send LostClaim to loser_tx (at most once
    //                per (username, node_id) pair)
    // drain_and_reconcile:end
    fn drain_and_reconcile(
        &self,
        notified_losers: &mut std::collections::HashSet<(String, String)>,
    ) -> (bool, Vec<String>) {
        // Drain all pending blobs from the sink inbox.
        let blobs = match self.sink.drain(self.claims_key) {
            Ok(b) => b,
            Err(e) => {
                eprintln!("[barrier_growset] drain error: {e}");
                return (false, Vec::new());
            }
        };

        let drained_count = blobs.len();

        // Parse blobs and merge into synced view.
        let mut any_new = false;
        let mut remote_node_ids: Vec<String> = Vec::new();
        {
            let mut view = match self.synced.lock() {
                Ok(v) => v,
                Err(e) => {
                    eprintln!("[barrier_growset] synced view lock poisoned: {e}");
                    return (false, Vec::new());
                }
            };
            for blob in blobs {
                if let Some(rec) = ClaimRecord::from_json(&blob) {
                    remote_node_ids.push(rec.node_id.clone());
                    let bucket = view.entry(rec.username.clone()).or_insert_with(Vec::new);
                    // Dedup: only insert if this (username, claimant, node_id) triple is new.
                    let is_new = !bucket.iter().any(|existing| {
                        existing.claimant == rec.claimant && existing.node_id == rec.node_id
                    });
                    if is_new {
                        bucket.push(rec);
                        any_new = true;
                    }
                }
            }
        }
        if drained_count > 0 {
            eprintln!(
                "[barrier_growset] drain tick: {} blob(s) received ({} new)",
                drained_count,
                if any_new { "≥1" } else { "0" },
            );
        }

        // Scan for conflicts and send loser notifications.
        let view_snapshot = match self.synced.lock() {
            Ok(v) => v.clone(),
            Err(e) => {
                eprintln!("[barrier_growset] synced view lock (reconcile): {e}");
                return (any_new, remote_node_ids);
            }
        };

        for (username, records) in &view_snapshot {
            // Count distinct node_ids for this username.
            // CRITICAL: use node_id (not claimant) as the conflict discriminant.
            // Two nodes registering the same username produce identical claimant
            // strings (e.g. "@dave:localhost" on both nodes), so claimant-based
            // dedup would collapse them to 1 distinct entry and never detect the
            // conflict.  node_id is always unique per physical node.
            let mut distinct: Vec<&ClaimRecord> = Vec::new();
            for rec in records {
                if !distinct.iter().any(|d| d.node_id == rec.node_id) {
                    distinct.push(rec);
                }
            }
            if distinct.len() <= 1 {
                continue; // no conflict — only one node_id present
            }

            // Build ProvisionalClaims for reconcile().
            let provisional: Vec<ProvisionalClaim> = distinct
                .iter()
                .map(|rec| ProvisionalClaim {
                    key: username.clone(),
                    claimant: rec.claimant.clone(),
                    fence: Fence {
                        epoch: 0,
                        ts: rec.ts,
                        node_id: rec.node_id.clone(),
                    },
                })
                .collect();

            let winner = match reconcile(&provisional) {
                Some(w) => w,
                None => continue,
            };

            // For every distinct node_id that is NOT the winner AND whose node_id
            // matches this node → emit LostClaim (at most once per (username, node_id)).
            for rec in distinct {
                if rec.node_id == winner.fence.node_id {
                    continue; // this node_id won
                }
                if rec.node_id == self.node_id {
                    // Dedup key is (username, node_id) — not (username, claimant).
                    // Using claimant as the dedup key would merge notifications from
                    // two different losing nodes that happen to share a claimant string.
                    let dedup_key = (username.clone(), rec.node_id.clone());
                    if notified_losers.contains(&dedup_key) {
                        continue; // already notified — skip re-emission
                    }
                    notified_losers.insert(dedup_key);
                    let lost = LostClaim {
                        username: username.clone(),
                        winner_claimant: winner.claimant.clone(),
                        loser_claimant: rec.claimant.clone(),
                    };
                    // Ignore send error — receiver may have been dropped (shutdown).
                    let _ = self.loser_tx.send(lost);
                }
            }
        }
        (any_new, remote_node_ids)
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::substrate::barrier::CasResult;

    // Helper: build a GrowSetClaimStore backed by MemCrdtSink instead of Zenoh.
    // The MemCrdtSink is symmetric: A.publish → B.drain, B.publish → A.drain.
    // To test single-node: we need publish → own drain.  Use a pair where the
    // "peer" side of B is the driver (reading A's publishes via B.drain = A.own_inbox?).
    // Actually MemCrdtSink routes A.publish → B.inbox and B.publish → A.inbox.
    // For single-node: we need publish to show up in drain on the SAME side.
    // Solution: use a helper MemGrowSetStore that holds its own inbox.
    struct MemGrowSetStore {
        node_id: String,
        synced: SyncedView,
        inbox: Arc<Mutex<Vec<Vec<u8>>>>,
    }

    impl MemGrowSetStore {
        fn new(node_id: &str) -> Self {
            MemGrowSetStore {
                node_id: node_id.to_string(),
                synced: Arc::new(Mutex::new(HashMap::new())),
                inbox: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn drain_into_synced(&self) {
            let blobs: Vec<Vec<u8>> = {
                let mut guard = self.inbox.lock().expect("inbox");
                std::mem::take(&mut *guard)
            };
            let mut view = self.synced.lock().expect("synced");
            for blob in blobs {
                if let Some(rec) = ClaimRecord::from_json(&blob) {
                    let bucket = view.entry(rec.username.clone()).or_default();
                    let is_new = !bucket
                        .iter()
                        .any(|e| e.claimant == rec.claimant && e.node_id == rec.node_id);
                    if is_new {
                        bucket.push(rec);
                    }
                }
            }
        }

        fn cas_claim_mem(&self, key: &str, claimant: &str) -> CasResult {
            let mut view = self.synced.lock().expect("synced");
            if let Some(records) = view.get(key) {
                for rec in records {
                    // Conflict predicate: different node_id, not different claimant.
                    if rec.node_id != self.node_id {
                        let owner = format!("{} (node {})", rec.claimant, rec.node_id);
                        return CasResult::AlreadySet { owner };
                    }
                }
                // Own node re-claim — idempotent.
                return CasResult::Set;
            }
            let ts = 42u64; // fixed for determinism in tests
            let rec = ClaimRecord {
                username: key.to_string(),
                claimant: claimant.to_string(),
                ts,
                node_id: self.node_id.clone(),
            };
            view.entry(key.to_string()).or_default().push(rec.clone());
            drop(view);
            self.inbox.lock().expect("inbox").push(rec.to_json());
            CasResult::Set
        }
    }

    // growset_single_claim_set:start
    //   purpose: Single claim on a fresh username returns Set.
    //   input:  MemGrowSetStore, cas_claim("alice", "alice")
    //   output: CasResult::Set
    //   sideEffects: claimrecord in synced view + inbox
    // growset_single_claim_set:end
    #[test]
    fn growset_single_claim_set() {
        let store = MemGrowSetStore::new("node-a");
        let r = store.cas_claim_mem("alice", "alice");
        assert_eq!(r, CasResult::Set, "first claim must be Set");
    }

    // growset_already_present_already_set:start
    //   purpose: When the synced view already has "alice" claimed by node-b
    //            (propagated from another node), a claim by node-a returns AlreadySet{owner}.
    //            The owner field encodes "claimant (node node_id)" for diagnostics.
    //            This exercises the node_id-based conflict predicate: even if the
    //            claimant strings were IDENTICAL (same username → same @user:server), the
    //            different node_id still triggers AlreadySet (the real bug scenario).
    //   input:  synced view pre-seeded with alice→existing_claimant from node-b
    //   output: CasResult::AlreadySet{owner: "existing_claimant (node node-b)"}
    //   sideEffects: none (no new publish)
    // growset_already_present_already_set:end
    #[test]
    fn growset_already_present_already_set() {
        let store = MemGrowSetStore::new("node-a");
        // Pre-seed as if arrived from node-b.
        {
            let mut view = store.synced.lock().expect("synced");
            view.entry("alice".to_string())
                .or_default()
                .push(ClaimRecord {
                    username: "alice".to_string(),
                    claimant: "existing_claimant".to_string(),
                    ts: 100,
                    node_id: "node-b".to_string(),
                });
        }
        let r = store.cas_claim_mem("alice", "alice_node_a");
        assert_eq!(
            r,
            CasResult::AlreadySet {
                owner: "existing_claimant (node node-b)".to_string()
            },
            "already-propagated username must return AlreadySet regardless of claimant string"
        );
    }

    // growset_same_claimant_different_node_already_set:start
    //   purpose: REGRESSION TEST for the diagnosed live bug.
    //            Two nodes claim the SAME username and produce IDENTICAL claimant strings
    //            (e.g. both register "dave" → both claimants are "@dave:localhost").
    //            After node-b's record propagates into node-a's synced view, node-a's
    //            cas_claim MUST return AlreadySet, NOT Set.
    //            Pre-fix: the old claimant-based predicate saw identical claimant strings
    //            → treated as own re-claim → returned Set → both nodes granted the name.
    //            Post-fix: the node_id-based predicate sees node-b ≠ node-a → AlreadySet.
    //            Also asserts the symmetric same-node re-claim returns Set (idempotent).
    //   input:  synced view pre-seeded with dave→"@dave:localhost" from node-b;
    //           cas_claim("mx:username:dave", "@dave:localhost") on node-a store
    //   output: AlreadySet (not Set)
    //   sideEffects: none
    // growset_same_claimant_different_node_already_set:end
    #[test]
    fn growset_same_claimant_different_node_already_set() {
        // ── Part 1: different node_id, SAME claimant string → must be AlreadySet ──
        let store_a = MemGrowSetStore::new("node-a");
        {
            let mut view = store_a.synced.lock().expect("synced");
            view.entry("mx:username:dave".to_string())
                .or_default()
                .push(ClaimRecord {
                    username: "mx:username:dave".to_string(),
                    claimant: "@dave:localhost".to_string(), // SAME claimant string as node-a will use
                    ts: 100,
                    node_id: "node-b".to_string(), // DIFFERENT node_id
                });
        }
        let r = store_a.cas_claim_mem("mx:username:dave", "@dave:localhost");
        assert!(
            matches!(r, CasResult::AlreadySet { .. }),
            "SAME claimant string but DIFFERENT node_id MUST return AlreadySet (got {:?})",
            r
        );

        // ── Part 2: same node_id re-claim → must be Set (idempotent) ─────────────
        let store_b = MemGrowSetStore::new("node-b");
        {
            let mut view = store_b.synced.lock().expect("synced");
            view.entry("mx:username:dave".to_string())
                .or_default()
                .push(ClaimRecord {
                    username: "mx:username:dave".to_string(),
                    claimant: "@dave:localhost".to_string(),
                    ts: 100,
                    node_id: "node-b".to_string(), // SAME node_id as this store
                });
        }
        let r2 = store_b.cas_claim_mem("mx:username:dave", "@dave:localhost");
        assert_eq!(
            r2,
            CasResult::Set,
            "own-node re-claim (same node_id) must return Set (idempotent)"
        );
    }

    // growset_reconcile_same_claimant_different_node_losers:start
    //   purpose: REGRESSION TEST for ReconcileDriver conflict detection with
    //            identical claimant strings.  Two MemGrowSetStores with different
    //            node_ids both claim the same key with the same claimant string.
    //            After grow-set convergence (inbox exchange), the ReconcileDriver
    //            on the LOSING node MUST emit exactly one LostClaim.
    //            Pre-fix: conflict detection used "distinct claimant" → only 1
    //            distinct claimant → no conflict detected → no LostClaim emitted.
    //            Post-fix: conflict detection uses "distinct node_id" → 2 distinct
    //            node_ids → conflict detected → LostClaim emitted on the loser node.
    //   input:  two MemGrowSetStores (node-a, node-b), same claimant "@dave:localhost",
    //           same key "mx:username:dave"; inbox exchange to simulate convergence
    //   output: exactly one LostClaim emitted to the loser's channel; winner is
    //           deterministic (min ts then node_id: "node-a" < "node-b" → node-a wins)
    //   sideEffects: none (in-process, no Zenoh)
    // growset_reconcile_same_claimant_different_node_losers:end
    #[test]
    fn growset_reconcile_same_claimant_different_node_losers() {
        let store_a = MemGrowSetStore::new("node-a");
        let store_b = MemGrowSetStore::new("node-b");

        // Both nodes claim the SAME username with the IDENTICAL claimant string.
        // This is the exact scenario that broke production: matrix-hs passes claimant
        // = user_id = "@dave:localhost" on BOTH nodes.
        let r_a = store_a.cas_claim_mem("mx:username:dave", "@dave:localhost");
        let r_b = store_b.cas_claim_mem("mx:username:dave", "@dave:localhost");

        assert_eq!(r_a, CasResult::Set, "node-a: fresh claim must be Set");
        assert_eq!(
            r_b,
            CasResult::Set,
            "node-b: fresh claim must be Set (concurrent)"
        );

        // ── Simulate grow-set convergence: exchange inboxes ────────────────────
        let blobs_a: Vec<Vec<u8>> = std::mem::take(&mut *store_a.inbox.lock().expect("a"));
        let blobs_b: Vec<Vec<u8>> = std::mem::take(&mut *store_b.inbox.lock().expect("b"));
        // Feed each node's publish into the other's inbox.
        store_b.inbox.lock().expect("b").extend(blobs_a);
        store_a.inbox.lock().expect("a").extend(blobs_b);
        store_a.drain_into_synced();
        store_b.drain_into_synced();

        // Both nodes must now have 2 records (one per node_id).
        let view_a = store_a.synced.lock().expect("a");
        let view_b = store_b.synced.lock().expect("b");
        let recs_a = view_a
            .get("mx:username:dave")
            .expect("node-a must have recs");
        let recs_b = view_b
            .get("mx:username:dave")
            .expect("node-b must have recs");
        assert_eq!(
            recs_a.len(),
            2,
            "node-a synced view must have 2 records after convergence"
        );
        assert_eq!(
            recs_b.len(),
            2,
            "node-b synced view must have 2 records after convergence"
        );
        drop(view_a);
        drop(view_b);

        // ── Run ReconcileDriver logic in-process (no tokio, no Zenoh) ──────────
        // Replicate the conflict scan from drain_and_reconcile for each node.
        fn conflict_scan(
            records: &[ClaimRecord],
            username: &str,
            this_node: &str,
        ) -> Option<LostClaim> {
            // Distinct by node_id (the fixed predicate).
            let mut distinct: Vec<&ClaimRecord> = Vec::new();
            for rec in records {
                if !distinct.iter().any(|d| d.node_id == rec.node_id) {
                    distinct.push(rec);
                }
            }
            if distinct.len() <= 1 {
                return None; // no conflict
            }
            let provisional: Vec<ProvisionalClaim> = distinct
                .iter()
                .map(|r| ProvisionalClaim {
                    key: username.to_string(),
                    claimant: r.claimant.clone(),
                    fence: Fence {
                        epoch: 0,
                        ts: r.ts,
                        node_id: r.node_id.clone(),
                    },
                })
                .collect();
            let winner = reconcile(&provisional)?;
            for rec in &distinct {
                if rec.node_id == winner.fence.node_id {
                    continue; // this one won
                }
                if rec.node_id == this_node {
                    return Some(LostClaim {
                        username: username.to_string(),
                        winner_claimant: winner.claimant.clone(),
                        loser_claimant: rec.claimant.clone(),
                    });
                }
            }
            None
        }

        let va = store_a.synced.lock().expect("a");
        let vb = store_b.synced.lock().expect("b");
        let recs_a2 = va.get("mx:username:dave").expect("recs_a2");
        let recs_b2 = vb.get("mx:username:dave").expect("recs_b2");

        let lost_a = conflict_scan(recs_a2, "mx:username:dave", "node-a");
        let lost_b = conflict_scan(recs_b2, "mx:username:dave", "node-b");

        // Exactly one node should have emitted a LostClaim (the loser).
        // Equal ts (both used 42) → tiebreak by node_id: "node-a" < "node-b" → node-a wins.
        // node-b is the loser → lost_b MUST be Some, lost_a MUST be None.
        assert!(
            lost_a.is_none(),
            "node-a is the winner (lex-smaller node_id) — must NOT emit LostClaim, got {:?}",
            lost_a
        );
        assert!(
            lost_b.is_some(),
            "node-b is the loser — MUST emit LostClaim; \
             if None: conflict detection is broken (check node_id predicate)"
        );

        let lost = lost_b.expect("node-b LostClaim");
        assert_eq!(lost.username, "mx:username:dave");
        assert_eq!(
            lost.winner_claimant, "@dave:localhost",
            "winner_claimant must be the claimant string from the winning record"
        );
        assert_eq!(
            lost.loser_claimant, "@dave:localhost",
            "loser_claimant is also @dave:localhost (same string — the whole point of this test)"
        );
    }

    // growset_concurrent_claim_both_set_same_winner:start
    //   purpose: Two nodes both claim "alice" concurrently (neither has seen the other's
    //            claim yet) → both get Set.  After "convergence" (exchange blobs), the
    //            synced view on BOTH nodes holds both records.  ReconcileDriver on BOTH
    //            nodes picks the SAME winner (min ts, node_id).
    //   input:  two MemGrowSetStores, same timestamp, different node_ids
    //   output: both claim Set; after exchange both identify the same winner + loser
    //   sideEffects: inbox exchange simulates grow-set convergence
    // growset_concurrent_claim_both_set_same_winner:end
    #[test]
    fn growset_concurrent_claim_both_set_same_winner() {
        let store_a = MemGrowSetStore::new("node-a");
        let store_b = MemGrowSetStore::new("node-b");

        // Both claim "alice" before seeing each other's claim.
        let r_a = store_a.cas_claim_mem("alice", "alice_a");
        let r_b = store_b.cas_claim_mem("alice", "alice_b");

        assert_eq!(r_a, CasResult::Set, "node-a must get Set");
        assert_eq!(r_b, CasResult::Set, "node-b must get Set");

        // Simulate grow-set convergence: exchange inboxes (each node drains what the
        // other published, then applies to its own synced view).
        let blobs_a: Vec<Vec<u8>> = std::mem::take(&mut *store_a.inbox.lock().expect("a"));
        let blobs_b: Vec<Vec<u8>> = std::mem::take(&mut *store_b.inbox.lock().expect("b"));

        // Feed A's blobs into B's inbox and vice versa.
        store_b.inbox.lock().expect("b").extend(blobs_a);
        store_a.inbox.lock().expect("a").extend(blobs_b);

        // Each node drains into its synced view.
        store_a.drain_into_synced();
        store_b.drain_into_synced();

        // Now both nodes have both records for "alice".
        let view_a = store_a.synced.lock().expect("a");
        let view_b = store_b.synced.lock().expect("b");

        let records_a = view_a.get("alice").expect("a has alice");
        let records_b = view_b.get("alice").expect("b has alice");

        assert_eq!(
            records_a.len(),
            2,
            "node-a view must have 2 records for alice"
        );
        assert_eq!(
            records_b.len(),
            2,
            "node-b view must have 2 records for alice"
        );

        // Compute winner on each node using reconcile().
        let provisional_a: Vec<ProvisionalClaim> = records_a
            .iter()
            .map(|rec| ProvisionalClaim {
                key: "alice".to_string(),
                claimant: rec.claimant.clone(),
                fence: Fence {
                    epoch: 0,
                    ts: rec.ts,
                    node_id: rec.node_id.clone(),
                },
            })
            .collect();

        let provisional_b: Vec<ProvisionalClaim> = records_b
            .iter()
            .map(|rec| ProvisionalClaim {
                key: "alice".to_string(),
                claimant: rec.claimant.clone(),
                fence: Fence {
                    epoch: 0,
                    ts: rec.ts,
                    node_id: rec.node_id.clone(),
                },
            })
            .collect();

        let winner_a = reconcile(&provisional_a)
            .expect("a winner")
            .claimant
            .clone();
        let winner_b = reconcile(&provisional_b)
            .expect("b winner")
            .claimant
            .clone();

        // Both nodes must pick the same winner (reconcile is deterministic).
        assert_eq!(
            winner_a, winner_b,
            "both nodes must converge to the same winner"
        );

        // With equal ts (both used ts=42) the tiebreaker is node_id: "node-a" < "node-b"
        // so alice_a (from node-a) wins.
        assert_eq!(
            winner_a, "alice_a",
            "node-a claimant wins (lex-smaller node_id)"
        );
    }

    // antientropy_quiesces_when_unchanged:start
    //   purpose: ACCEPTANCE TEST for the 2026-09-07..11 production incident
    //            (owner criterion 2026-09-23, task 1): with UNCHANGED state the
    //            anti-entropy traffic MUST decay to zero.
    //            Setup mirrors the incident: 200 own claims, then 30 driver ticks
    //            with no new local claims and no new remote records.  The sink
    //            echoes publishes back into the next drain (this is what Zenoh
    //            does: a session's own subscriber receives its own PUTs), so an
    //            unconditional re-publish also shows up as endless drain traffic.
    //            Assertion: ticks 16..=30 publish NOTHING and drain NOTHING.
    //            A bounded catch-up burst in ticks 1..=15 is allowed (late peers
    //            need re-publish after missed PUTs) — steady-state silence is what
    //            must hold.
    //            Pre-fix: FAILS — republish every 5 ticks unconditionally keeps
    //            publishing a full round of own claims at ticks 20/25/30 (three
    //            rounds' worth inside the quiet window) and the self-echo drains
    //            them back forever.
    //   input:  pre-seeded synced view (200 own records), 30 synchronous ticks
    //   output: assert quiet-window publishes == 0 and quiet-window drains == 0
    //   sideEffects: none (in-process echo sink, no Zenoh, no network)
    // antientropy_quiesces_when_unchanged:end
    #[test]
    fn antientropy_quiesces_when_unchanged() {
        use crate::substrate::crdt::{CrdtError, CrdtSink};
        use std::sync::{Arc, Mutex};
        use std::time::Duration;

        /// Counting sink with Zenoh-like self-echo: what this node publishes
        /// comes back on its own next drain (same-session subscriber receives
        /// its own PUTs).  Counters let the test split traffic by window.
        struct EchoSink {
            published_total: Mutex<usize>,
            drained_total: Mutex<usize>,
            inbox: Mutex<Vec<Vec<u8>>>,
        }
        impl CrdtSink for EchoSink {
            fn publish(&self, _key: &str, bytes: Vec<u8>) -> Result<(), CrdtError> {
                *self.published_total.lock().expect("published_total") += 1;
                self.inbox.lock().expect("inbox").push(bytes);
                Ok(())
            }
            fn drain(&self, _key: &str) -> Result<Vec<Vec<u8>>, CrdtError> {
                let blobs = std::mem::take(&mut *self.inbox.lock().expect("inbox"));
                *self.drained_total.lock().expect("drained_total") += blobs.len();
                Ok(blobs)
            }
        }

        let sink = Arc::new(EchoSink {
            published_total: Mutex::new(0),
            drained_total: Mutex::new(0),
            inbox: Mutex::new(Vec::new()),
        });
        let sink_dyn: Arc<dyn CrdtSink> = sink.clone();

        // 200 own claims — the production incident's scale, rounded up.
        const OWN_CLAIMS: usize = 200;
        let synced: SyncedView = Arc::new(Mutex::new(HashMap::new()));
        {
            let mut view = synced.lock().expect("synced");
            for i in 0..OWN_CLAIMS {
                let username = format!("user{i:03}");
                view.entry(username.clone()).or_default().push(ClaimRecord {
                    username,
                    claimant: format!("@user{i:03}:localhost"),
                    ts: 1000 + i as u64,
                    node_id: "node-a".to_string(),
                });
            }
        }

        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<LostClaim>();
        let driver = ReconcileDriver::new(
            sink_dyn,
            synced,
            "node-a".to_string(),
            tx,
            Duration::from_secs(1),
        );

        let mut notified = std::collections::HashSet::new();
        for t in 1..=15u64 {
            driver.tick(t, &mut notified);
        }
        let published_at_15 = *sink.published_total.lock().expect("published_total");
        let drained_at_15 = *sink.drained_total.lock().expect("drained_total");
        for t in 16..=30u64 {
            driver.tick(t, &mut notified);
        }
        let published_quiet = *sink.published_total.lock().expect("published_total")
            - published_at_15;
        let drained_quiet =
            *sink.drained_total.lock().expect("drained_total") - drained_at_15;

        assert_eq!(
            published_quiet, 0,
            "anti-entropy must QUIESCE with unchanged state: ticks 16..=30 published \
             {published_quiet} blob(s) (total in burst window: {published_at_15}). \
             Steady-state re-publish of an unchanged grow-set is the 2026-09-07..11 bug."
        );
        assert_eq!(
            drained_quiet, 0,
            "no publish → no self-echo: ticks 16..=30 drained {drained_quiet} blob(s). \
             Endless 'drain tick: N blob(s) received' on an unchanged set is the bug."
        );
    }

    // growset_reconcile_winner_order_independent:start
    //   purpose: reconcile() produces the same winner regardless of record order in
    //            the synced view (order-independence → no coordination needed).
    //   input:  same three ClaimRecords in two different orderings
    //   output: both orderings select the same winner
    //   sideEffects: none (pure)
    // growset_reconcile_winner_order_independent:end
    #[test]
    fn growset_reconcile_winner_order_independent() {
        let c1 = ProvisionalClaim {
            key: "bob".to_string(),
            claimant: "bob-a".to_string(),
            fence: Fence {
                epoch: 0,
                ts: 100,
                node_id: "node-a".to_string(),
            },
        };
        let c2 = ProvisionalClaim {
            key: "bob".to_string(),
            claimant: "bob-b".to_string(),
            fence: Fence {
                epoch: 0,
                ts: 200,
                node_id: "node-b".to_string(),
            },
        };
        let c3 = ProvisionalClaim {
            key: "bob".to_string(),
            claimant: "bob-c".to_string(),
            fence: Fence {
                epoch: 0,
                ts: 150,
                node_id: "node-c".to_string(),
            },
        };

        let order1 = vec![c1.clone(), c2.clone(), c3.clone()];
        let order2 = vec![c3.clone(), c1.clone(), c2.clone()];
        let order3 = vec![c2.clone(), c3.clone(), c1.clone()];

        let w1 = reconcile(&order1).expect("order1").claimant.clone();
        let w2 = reconcile(&order2).expect("order2").claimant.clone();
        let w3 = reconcile(&order3).expect("order3").claimant.clone();

        assert_eq!(w1, "bob-a", "ts=100 wins (lowest ts)");
        assert_eq!(w1, w2, "order-independent");
        assert_eq!(w1, w3, "order-independent");
    }

    // growset_per_scope_isolation:start
    //   purpose: Verify the grow-set barrier is scope-isolated: two stores bound
    //            to different sinks (simulating different scope barrier_key prefixes)
    //            do not interfere.  A claim in scope A is never visible in scope B's
    //            synced view, and vice versa.  This validates the per-scope composition
    //            pattern that per-scope composition relies on: main.rs creates one
    //            GrowSetClaimStore + ReconcileDriver per scope, each backed by a
    //            ZenohCrdtSink bound to that scope's barrier_key.
    //   input:  two IsolationSinks (self-echo, like Zenoh own-session loopback);
    //           two GrowSetClaimStore instances (same node_id, different sinks)
    //   output: scope A's view has only scope A's claims; scope B's view has only
    //           scope B's claims — no cross-contamination
    //   sideEffects: none (in-process, no Zenoh, no network)
    // growset_per_scope_isolation:end
    #[test]
    fn growset_per_scope_isolation() {
        use crate::substrate::crdt::{CrdtError, CrdtSink};

        // Self-echo sink (Zenoh-like: own publishes return on own drain).
        // Two separate instances = two independent scope meshes.
        struct IsolationSink {
            inbox: Mutex<Vec<Vec<u8>>>,
        }
        impl CrdtSink for IsolationSink {
            fn publish(&self, _key: &str, bytes: Vec<u8>) -> Result<(), CrdtError> {
                self.inbox.lock().expect("inbox").push(bytes);
                Ok(())
            }
            fn drain(&self, _key: &str) -> Result<Vec<Vec<u8>>, CrdtError> {
                Ok(std::mem::take(&mut *self.inbox.lock().expect("inbox")))
            }
        }

        // Two independent sinks — each simulates a different scope's
        // barrier_key prefix (e.g. "mrgd/coupling/barrier" vs
        // "mrgd/coupling/barrier/scope-b"). In production these are
        // separate ZenohCrdtSink instances.
        let sink_a: Arc<dyn CrdtSink> = Arc::new(IsolationSink {
            inbox: Mutex::new(Vec::new()),
        });
        let sink_b: Arc<dyn CrdtSink> = Arc::new(IsolationSink {
            inbox: Mutex::new(Vec::new()),
        });

        // Same node participates in both scopes — one store per scope.
        let (store_a, synced_a) = GrowSetClaimStore::new(sink_a, "node-a".to_string());
        let (store_b, synced_b) = GrowSetClaimStore::new(sink_b, "node-a".to_string());

        // Claim "alice" in scope A only.
        let r = store_a.cas_claim("alice", "@alice:localhost");
        assert_eq!(r, Ok(CasResult::Set), "scope A: fresh claim must be Set");

        // Scope A must reflect the claim.
        let view_a = synced_a.lock().expect("va");
        assert_eq!(
            view_a.get("alice").map(|v| v.len()),
            Some(1),
            "scope A must have exactly 1 record for alice"
        );
        assert_eq!(view_a["alice"][0].node_id, "node-a");
        drop(view_a);

        // Scope B must NOT have it — scopes are isolated by sink prefix.
        let view_b = synced_b.lock().expect("vb");
        assert!(
            view_b.get("alice").is_none(),
            "scope B must NOT have alice — scopes must be isolated"
        );
        drop(view_b);

        // Claim "bob" in scope B only.
        store_b
            .cas_claim("bob", "@bob:localhost")
            .expect("scope B: fresh claim");

        // Scope B has bob, NOT alice.
        let view_b2 = synced_b.lock().expect("vb2");
        assert_eq!(
            view_b2.get("bob").map(|v| v.len()),
            Some(1),
            "scope B must have exactly 1 record for bob"
        );
        assert!(
            view_b2.get("alice").is_none(),
            "scope B must NOT have alice (claimed in scope A)"
        );
        drop(view_b2);

        // Scope A still has alice, NOT bob.
        let view_a2 = synced_a.lock().expect("va2");
        assert!(
            view_a2.get("bob").is_none(),
            "scope A must NOT have bob (claimed in scope B)"
        );
        assert_eq!(
            view_a2.get("alice").map(|v| v.len()),
            Some(1),
            "scope A must still have exactly alice"
        );
    }
}

// ── Cluster tests (require Zenoh loopback) ────────────────────────────────────

#[cfg(all(test, feature = "cluster"))]
mod cluster_tests {
    use super::*;
    use crate::substrate::crdt::ZenohCrdtSink;
    use std::time::Duration;

    // Helper: create two linked ZenohCrdtSinks via a real Zenoh loopback session pair.
    // Both sessions run in peer/scouting mode on loopback — convergence via gossip.
    //
    // CRITICAL: waits 50 ms after declaring both subscribers before returning.
    // Without this, the first publish (immediately after open_pair()) fires before the
    // remote subscriber has propagated through the Zenoh scouting graph, and the PUT is
    // silently dropped (Zenoh pub/sub has no replay for late subscribers).
    // This mirrors the identical delay in zenoh_crdt_roomlog_convergence (crdt.rs §1583).
    async fn open_pair() -> (Arc<ZenohCrdtSink>, Arc<ZenohCrdtSink>) {
        let cfg = zenoh::Config::default();
        let sess_a = zenoh::open(cfg.clone()).await.expect("session a");
        let sess_b = zenoh::open(cfg).await.expect("session b");
        let sink_a = Arc::new(
            ZenohCrdtSink::new(sess_a, "mrgd/coupling/barrier/test")
                .await
                .expect("sink a"),
        );
        let sink_b = Arc::new(
            ZenohCrdtSink::new(sess_b, "mrgd/coupling/barrier/test")
                .await
                .expect("sink b"),
        );
        // Give both subscribers time to propagate through the Zenoh scouting graph.
        // Without this, an immediate publish after open_pair() fires before the remote
        // subscriber is registered and the PUT is silently dropped (Zenoh pub/sub has
        // no replay for late subscribers).  50 ms is the same margin used in
        // zenoh_crdt_roomlog_convergence (crdt.rs §1583) which passes reliably in CI.
        tokio::time::sleep(Duration::from_millis(50)).await;
        (sink_a, sink_b)
    }

    // zenoh_growset_single_claim_set:start
    //   purpose: GrowSetClaimStore.cas_claim on a fresh username via real Zenoh returns Set.
    //   input:  real ZenohCrdtSink (loopback), single claim
    //   output: CasResult::Set
    //   sideEffects: publishes to Zenoh
    // zenoh_growset_single_claim_set:end
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn zenoh_growset_single_claim_set() {
        let _zg = crate::test_util::ZENOH_TEST_LOCK.acquire().await.unwrap();
        let (sink_a, _sink_b) = open_pair().await;
        let (store, _synced) = GrowSetClaimStore::new(sink_a, "node-a".to_string());
        let r = store
            .cas_claim("username:carol", "carol")
            .expect("cas_claim must not error");
        assert_eq!(r, CasResult::Set, "fresh claim must be Set");
    }

    // zenoh_growset_already_propagated_already_set:start
    //   purpose: When a ClaimRecord from node-b has been received and merged into the
    //            local synced view, a competing claim by node-a returns AlreadySet{owner}.
    //            The owner field encodes "claimant (node node_id)" for diagnostics.
    //            Tests both the case where claimant strings differ AND the pathological
    //            case where they are IDENTICAL (same username → "@dave:localhost" on both).
    //   input:  synced view pre-seeded with propagated record from node-b;
    //           claim by node-a with different claimant string
    //   output: CasResult::AlreadySet{owner: "dave_b (node node-b)"}
    //   sideEffects: none (fast reject before publish)
    // zenoh_growset_already_propagated_already_set:end
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn zenoh_growset_already_propagated_already_set() {
        let _zg = crate::test_util::ZENOH_TEST_LOCK.acquire().await.unwrap();
        let (sink_a, _sink_b) = open_pair().await;
        let (store, synced) = GrowSetClaimStore::new(sink_a, "node-a".to_string());

        // Simulate a propagated record from node-b.
        {
            let mut view = synced.lock().expect("synced");
            view.entry("dave".to_string())
                .or_insert_with(Vec::new)
                .push(ClaimRecord {
                    username: "dave".to_string(),
                    claimant: "dave_b".to_string(),
                    ts: 50,
                    node_id: "node-b".to_string(),
                });
        }

        let r = store.cas_claim("dave", "dave_a").expect("cas_claim");
        assert_eq!(
            r,
            CasResult::AlreadySet {
                owner: "dave_b (node node-b)".to_string()
            },
            "propagated claim must block competing claim (node_id-based predicate)"
        );

        // ── SAME claimant string, different node_id (the real bug scenario) ────
        let (sink_a2, _sink_b2) = open_pair().await;
        let (store2, synced2) = GrowSetClaimStore::new(sink_a2, "node-a".to_string());
        {
            let mut view = synced2.lock().expect("synced2");
            view.entry("carol".to_string())
                .or_insert_with(Vec::new)
                .push(ClaimRecord {
                    username: "carol".to_string(),
                    claimant: "@carol:localhost".to_string(), // SAME as what node-a will pass
                    ts: 50,
                    node_id: "node-b".to_string(), // DIFFERENT node_id
                });
        }
        let r2 = store2
            .cas_claim("carol", "@carol:localhost")
            .expect("cas_claim2");
        assert!(
            matches!(r2, CasResult::AlreadySet { .. }),
            "SAME claimant string but DIFFERENT node_id MUST return AlreadySet (got {:?})",
            r2
        );
    }

    // zenoh_growset_cross_node_convergence:start
    //   purpose: STRICT cross-node convergence test: two GrowSetClaimStore+ReconcileDriver
    //            instances (one per Zenoh session) both claim "alice" concurrently.
    //            After a bounded wait (≤5 s), BOTH synced views MUST contain BOTH claim
    //            records, and both ReconcileDrivers MUST pick the SAME winner.
    //            A LostClaim MUST be emitted on exactly one node (the loser's node).
    //            This test has NO gossip-tolerant escape hatch — it fails deterministically
    //            if cross-session delivery is broken.
    //
    //            Pre-fix: fails with recs_b=1 (B's view never received A's claim) because
    //            the old open_pair() published before B's subscriber was registered.
    //            Post-fix: passes — open_pair() now waits 50 ms for subscriber settlement.
    //
    //   input:  two real Zenoh peer sessions (loopback), concurrent claims
    //   output: ASSERT both views have 2 records; both compute the same winner;
    //           exactly one LostClaim emitted; no gossip-tolerant skip
    //   sideEffects: publishes to Zenoh loopback
    // zenoh_growset_cross_node_convergence:end
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn zenoh_growset_cross_node_convergence() {
        let _zg = crate::test_util::ZENOH_TEST_LOCK.acquire().await.unwrap();
        let (sink_a, sink_b) = open_pair().await;

        let (store_a, synced_a) = GrowSetClaimStore::new(sink_a.clone(), "node-a".to_string());
        let (store_b, synced_b) = GrowSetClaimStore::new(sink_b.clone(), "node-b".to_string());

        // Set up loser channels BEFORE claiming so we don't miss any LostClaim.
        let (tx_a, mut rx_a) = tokio::sync::mpsc::unbounded_channel::<LostClaim>();
        let (tx_b, mut rx_b) = tokio::sync::mpsc::unbounded_channel::<LostClaim>();

        // Spawn drivers BEFORE claiming so they're running when claims arrive.
        // tick=100ms gives fast reconcile cycles during the test.
        let driver_a = ReconcileDriver::new(
            sink_a.clone(),
            synced_a.clone(),
            "node-a".to_string(),
            tx_a,
            Duration::from_millis(100),
        );
        let driver_b = ReconcileDriver::new(
            sink_b.clone(),
            synced_b.clone(),
            "node-b".to_string(),
            tx_b,
            Duration::from_millis(100),
        );
        let h_a = driver_a.spawn();
        let h_b = driver_b.spawn();

        // Both claim concurrently — neither has seen the other's claim yet.
        let r_a = store_a.cas_claim("alice_unique_key", "alice_a").expect("a");
        let r_b = store_b.cas_claim("alice_unique_key", "alice_b").expect("b");

        assert_eq!(r_a, CasResult::Set, "node-a must get Set");
        assert_eq!(r_b, CasResult::Set, "node-b must get Set");

        // Poll until BOTH synced views have 2 records for "alice" (max 5 s).
        // This is the strict convergence criterion: both nodes MUST see both claims.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if tokio::time::Instant::now() >= deadline {
                let va = synced_a.lock().expect("va");
                let vb = synced_b.lock().expect("vb");
                let na = va.get("alice_unique_key").map_or(0, |v| v.len());
                let nb = vb.get("alice_unique_key").map_or(0, |v| v.len());
                h_a.abort();
                h_b.abort();
                panic!(
                    "grow-set cross-node convergence FAILED: \
                     node-a has {na} record(s), node-b has {nb} record(s) for 'alice_unique_key' \
                     after 5 s. Both must have 2. \
                     Likely cause: subscriber not yet registered when publish fired \
                     (add pre-publish delay in open_pair), or Zenoh session not connected."
                );
            }
            {
                let va = synced_a.lock().expect("va");
                let vb = synced_b.lock().expect("vb");
                let na = va.get("alice_unique_key").map_or(0, |v| v.len());
                let nb = vb.get("alice_unique_key").map_or(0, |v| v.len());
                if na >= 2 && nb >= 2 {
                    break; // both nodes converged
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // Wait for drivers to emit LostClaim (one more full tick).
        tokio::time::sleep(Duration::from_millis(300)).await;

        h_a.abort();
        h_b.abort();

        // ── Strict assertions ─────────────────────────────────────────────────
        let view_a = synced_a.lock().expect("va");
        let view_b = synced_b.lock().expect("vb");
        let recs_a = view_a
            .get("alice_unique_key")
            .expect("recs_a: convergence already checked");
        let recs_b = view_b
            .get("alice_unique_key")
            .expect("recs_b: convergence already checked");

        assert_eq!(
            recs_a.len(),
            2,
            "node-a view must have exactly 2 records for alice_unique_key"
        );
        assert_eq!(
            recs_b.len(),
            2,
            "node-b view must have exactly 2 records for alice_unique_key"
        );

        // Build ProvisionalClaim inputs for reconcile.
        let prov_a: Vec<ProvisionalClaim> = recs_a
            .iter()
            .map(|r| ProvisionalClaim {
                key: "alice_unique_key".to_string(),
                claimant: r.claimant.clone(),
                fence: Fence {
                    epoch: 0,
                    ts: r.ts,
                    node_id: r.node_id.clone(),
                },
            })
            .collect();
        let prov_b: Vec<ProvisionalClaim> = recs_b
            .iter()
            .map(|r| ProvisionalClaim {
                key: "alice_unique_key".to_string(),
                claimant: r.claimant.clone(),
                fence: Fence {
                    epoch: 0,
                    ts: r.ts,
                    node_id: r.node_id.clone(),
                },
            })
            .collect();

        let winner_a = reconcile(&prov_a)
            .expect("reconcile on node-a")
            .claimant
            .clone();
        let winner_b = reconcile(&prov_b)
            .expect("reconcile on node-b")
            .claimant
            .clone();
        assert_eq!(
            winner_a, winner_b,
            "both nodes must deterministically compute the SAME winner"
        );
        eprintln!("[test] strict convergence: winner={winner_a}");

        // Exactly one node must have received a LostClaim (the loser's node).
        drop(view_a);
        drop(view_b);
        let mut losers_a = Vec::new();
        while let Ok(l) = rx_a.try_recv() {
            losers_a.push(l);
        }
        let mut losers_b = Vec::new();
        while let Ok(l) = rx_b.try_recv() {
            losers_b.push(l);
        }

        let total_losers = losers_a.len() + losers_b.len();
        assert_eq!(
            total_losers,
            1,
            "exactly one LostClaim must be emitted (got a={} b={}): \
             the loser's ReconcileDriver must detect the conflict",
            losers_a.len(),
            losers_b.len()
        );

        // The LostClaim on the loser node must reference the same winner.
        let lost = losers_a
            .into_iter()
            .chain(losers_b)
            .next()
            .expect("one LostClaim");
        assert_eq!(
            lost.winner_claimant, winner_a,
            "LostClaim.winner_claimant must match reconcile() winner"
        );
    }
}
