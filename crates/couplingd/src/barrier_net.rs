// START_AI_HEADER
// MODULE: couplingd/src/barrier_net.rs
// PURPOSE: Distributed barrier coordination over Zenoh queryable RPC (cluster feature only).
//
//          Problem being solved:
//            KvFencedClaimStore is authoritative but single-node.  A non-coordinator node
//            cannot directly access the coordinator's KV store.  This module lets any node
//            route its ClaimStore::cas_claim() call to the coordinator via a Zenoh GET/reply,
//            without knowing the coordinator's address — Zenoh routes to whoever declares
//            the well-known queryable key.
//
//          Design — Rendezvous via well-known Zenoh queryable:
//            Coordinator: declares a queryable on `key_expr` (default
//              "bsdos/coupling/barrier/claim"); incoming queries carry a JSON-encoded
//              ClaimRequest; coordinator runs cas_claim on its local KvFencedClaimStore
//              and sends back a JSON-encoded CasResult.
//            Client node: issues a Zenoh GET to the same `key_expr` with a JSON-encoded
//              ClaimRequest as the payload; awaits the first reply up to `timeout`;
//              deserializes the CasResult.  On timeout / no reply / any error →
//              CasResult::Unavailable (AP signal — callers use Policy::Optimistic).
//
//          Coordinator failover: a new node declares the same queryable key; Zenoh
//          automatically routes new queries to it.  No address re-configuration needed.
//
//          Wire format: serde_json — this is a rare control-plane RPC (one call per user
//          registration), NOT the data plane, so JSON overhead is negligible.
//          The CLAUDE.md rule "JSON в агенте" forbids JSON in the agent text-protocol
//          and data-plane; this is an intra-cluster CP coordination RPC, a distinct context.
//
// INTENT: P2 barrier (SPEC_matrix_multimaster §8.6 distributed CAS):
//         ClaimResponder + RoutedClaimStore are cluster-only (feature = "cluster").
//         Host tests open two real Zenoh peer sessions (loopback) to verify the
//         query/reply round-trip, mirroring crdt.rs's zenoh_crdt_roomlog_convergence test.
// DEPENDENCIES: zenoh (cluster feature), serde_json, tokio, couplingd::barrier
// PUBLIC_API: ClaimRequest, ClaimResponder, RoutedClaimStore
// END_AI_HEADER

use std::{sync::Arc, time::Duration};
use serde::{Deserialize, Serialize};
use crate::barrier::{BarrierError, CasResult, ClaimStore};

// ── Wire types ────────────────────────────────────────────────────────────────

// ClaimRequest:start
//   purpose: JSON-serialisable request payload sent by a client node inside a Zenoh
//            GET query to the coordinator's well-known queryable key.
//            The coordinator deserializes this, runs cas_claim, and replies with CasResult.
//   input:  key — resource key to claim (e.g. "username:alice");
//           claimant — identity asserting ownership (e.g. Matrix localpart)
//   output: serialized to JSON bytes and attached as the Zenoh query payload
//   sideEffects: none (pure data)
// ClaimRequest:end
#[derive(Debug, Serialize, Deserialize)]
pub struct ClaimRequest {
    /// Resource key to claim (e.g. "username:alice", "otk:device123:0").
    pub key:      String,
    /// Identity asserting ownership (e.g. Matrix localpart, device ID).
    pub claimant: String,
}

// ── ClaimResponder — coordinator side ────────────────────────────────────────

// ClaimResponder:start
//   purpose: Coordinator-side queryable handler.
//            Declares a Zenoh queryable on `key_expr`; a background tokio task
//            drains incoming queries, deserializes ClaimRequest, calls
//            store.cas_claim(), serializes CasResult, and replies.
//            The queryable AND the background task are both held by ClaimResponder.
//            Dropping ClaimResponder drops the queryable first (via abort + _queryable
//            field), which signals the background task's recv_async loop to return Err
//            (channel closed), causing the task to exit.
//            Coordinator failover: a new node constructs a ClaimResponder on the same
//            key_expr; Zenoh routes subsequent queries to it automatically.
//   input:  session — open zenoh::Session; store — Arc<dyn ClaimStore + Send + Sync>;
//           key_expr — well-known Zenoh key (default "bsdos/coupling/barrier/claim")
//   output: ClaimResponder value (keep alive for the process lifetime or until failover)
//   sideEffects: declares one Zenoh queryable; spawns one tokio background task
// ClaimResponder:end
#[cfg(feature = "cluster")]
pub struct ClaimResponder {
    /// Held to keep the queryable alive.  When dropped the queryable is undeclared,
    /// which closes the channel and causes the background task's loop to exit.
    _queryable: zenoh::query::Queryable<zenoh::handlers::FifoChannelHandler<zenoh::query::Query>>,
    /// Background task handle — abort on drop ensures the task stops even if the
    /// queryable undeclaration takes time.
    _task: tokio::task::JoinHandle<()>,
}

#[cfg(feature = "cluster")]
impl ClaimResponder {
    // ClaimResponder::new:start
    //   purpose: Declare the Zenoh queryable and spawn the background handler task.
    //            The task loops forever on the queryable's recv_async(), processing each
    //            incoming query: deserialize ClaimRequest → cas_claim → serialize CasResult
    //            → query.reply(key_expr, bytes).
    //            Both the queryable AND the task handle are stored on the struct.
    //            When the struct is dropped: the queryable field drops (undeclares it),
    //            closing the internal channel; the task exits when recv_async returns Err.
    //   input:  session — open zenoh::Session;
    //           store   — Arc<dyn ClaimStore + Send + Sync> (shared with task);
    //           key_expr — Zenoh key this responder declares on
    //   output: Result<ClaimResponder, String>
    //   sideEffects: declares Zenoh queryable; spawns tokio task
    // ClaimResponder::new:end
    pub async fn new(
        session:  zenoh::Session,
        store:    Arc<dyn ClaimStore + Send + Sync>,
        key_expr: &str,
    ) -> Result<Self, String> {
        let queryable = session
            .declare_queryable(key_expr)
            .await
            .map_err(|e| format!("ClaimResponder: declare_queryable({key_expr}): {e}"))?;

        let key_owned = key_expr.to_string();

        // Clone the FifoReceiver handle out of the queryable for the background task.
        // zenoh::Queryable<FifoChannel> implements Deref to the receiver; we need
        // to drive it from the task.  Since Queryable does not impl Clone (it's a handle),
        // we use the queryable's handler() accessor to get a cloned receiver.
        // Zenoh v1: Queryable<FifoChannel> derefs to FifoChannel, and receiver is accessed
        // via .handler() which returns &FifoChannel (FifoChannelHandler implements recv_async).
        // We clone the handler (Arc-backed) for the background task.
        let handler = queryable.handler().clone();

        let task = tokio::spawn(async move {
            // Process queries until the queryable is dropped (channel closes).
            while let Ok(query) = handler.recv_async().await {
                // Extract and deserialize the request payload.
                let req: ClaimRequest = match query.payload() {
                    Some(p) => {
                        let bytes = p.to_bytes();
                        match serde_json::from_slice::<ClaimRequest>(&bytes) {
                            Ok(r) => r,
                            Err(e) => {
                                // Malformed request — reply Unavailable and continue.
                                eprintln!("ClaimResponder: bad request payload: {e}");
                                let reply = serde_json::to_vec(&CasResult::Unavailable)
                                    .unwrap_or_default();
                                let _ = query.reply(&key_owned, reply).await;
                                continue;
                            }
                        }
                    }
                    None => {
                        // No payload — reply Unavailable.
                        eprintln!("ClaimResponder: query has no payload");
                        let reply = serde_json::to_vec(&CasResult::Unavailable)
                            .unwrap_or_default();
                        let _ = query.reply(&key_owned, reply).await;
                        continue;
                    }
                };

                // Run the CAS on the authoritative local store.
                let result = match store.cas_claim(&req.key, &req.claimant) {
                    Ok(r)  => r,
                    Err(_) => CasResult::Unavailable,
                };

                // Serialize and reply.
                match serde_json::to_vec(&result) {
                    Ok(bytes) => {
                        if let Err(e) = query.reply(&key_owned, bytes).await {
                            eprintln!("ClaimResponder: reply error: {e}");
                        }
                    }
                    Err(e) => {
                        eprintln!("ClaimResponder: serialize CasResult: {e}");
                        let fallback = serde_json::to_vec(&CasResult::Unavailable)
                            .unwrap_or_default();
                        let _ = query.reply(&key_owned, fallback).await;
                    }
                }
            }
            // queryable handler channel closed — task exits cleanly.
        });

        Ok(ClaimResponder { _queryable: queryable, _task: task })
    }
}

// ── RoutedClaimStore — client node side ──────────────────────────────────────

// RoutedClaimStore:start
//   purpose: Client-node ClaimStore that routes every cas_claim() call to the
//            coordinator via a Zenoh GET + reply round-trip.
//            Implements the same ClaimStore trait as MemClaimStore / KvFencedClaimStore,
//            so register.rs and barrier::claim() are transparent to topology.
//
//            Protocol:
//              1. Serialize ClaimRequest{key, claimant} as JSON.
//              2. Issue session.get(key_expr).payload(bytes) — Zenoh routes to coordinator.
//              3. Await first reply up to `timeout`.
//              4. Deserialize CasResult from reply payload.
//              5. On any error / timeout / no reply → Ok(CasResult::Unavailable).
//                 (This is the AP signal: barrier::claim() + Policy::Optimistic →
//                  ClaimOutcome::Provisional; the caller records the claim and reconciles
//                  on partition heal via couplingd::barrier::reconcile().)
//
//            The sync-to-async bridge uses the same block_in_place / Handle::current()
//            pattern as ZenohCrdtSink::publish() in crdt.rs — required because
//            ClaimStore::cas_claim() is sync but Zenoh get() is async.
//   input:  session   — open zenoh::Session (Arc-backed, cheap to clone);
//           key_expr  — coordinator's queryable key (same as ClaimResponder's key_expr);
//           timeout   — how long to wait for coordinator reply before returning Unavailable
//   output: ClaimStore impl
//   sideEffects: issues one Zenoh GET per cas_claim() call
// RoutedClaimStore:end
#[cfg(feature = "cluster")]
pub struct RoutedClaimStore {
    session:  zenoh::Session,
    key_expr: String,
    timeout:  Duration,
}

#[cfg(feature = "cluster")]
impl RoutedClaimStore {
    // RoutedClaimStore::new:start
    //   purpose: Construct a RoutedClaimStore pointing at the coordinator's queryable key.
    //   input:  session  — open zenoh::Session;
    //           key_expr — coordinator's Zenoh queryable key;
    //           timeout  — reply wait duration (recommended: 3–5 s)
    //   output: RoutedClaimStore
    //   sideEffects: none
    // RoutedClaimStore::new:end
    pub fn new(session: zenoh::Session, key_expr: impl Into<String>, timeout: Duration) -> Self {
        RoutedClaimStore {
            session,
            key_expr: key_expr.into(),
            timeout,
        }
    }
}

#[cfg(feature = "cluster")]
impl ClaimStore for RoutedClaimStore {
    // cas_claim:start
    //   purpose: Route a CAS request to the coordinator via Zenoh GET/reply.
    //            Blocks the calling sync thread using the same block_in_place bridge as
    //            ZenohCrdtSink::publish().  Awaits the first reply up to self.timeout.
    //            On timeout / no reply / any error → Ok(CasResult::Unavailable)
    //            (AP signal; never Err — Unavailable is the correct way to signal
    //            coordinator unreachable to barrier::claim()).
    //   input:  key — resource key; claimant — identity asserting ownership
    //   output: Ok(CasResult) — Set | AlreadySet{owner} | Unavailable
    //   sideEffects: one Zenoh GET issued per call; blocks calling thread for up to timeout
    // cas_claim:end
    fn cas_claim(&self, key: &str, claimant: &str) -> Result<CasResult, BarrierError> {
        let req = ClaimRequest {
            key:      key.to_string(),
            claimant: claimant.to_string(),
        };
        let payload = match serde_json::to_vec(&req) {
            Ok(b)  => b,
            Err(_) => return Ok(CasResult::Unavailable),
        };

        let session   = self.session.clone();
        let key_expr  = self.key_expr.clone();
        let timeout   = self.timeout;

        // Bridge from sync ClaimStore::cas_claim into async Zenoh get().
        // block_in_place parks the current worker thread; safe in the multi-thread runtime
        // that both couplingd and matrix-hs use (same invariant as ZenohCrdtSink::publish).
        let result = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move {
                // Issue GET with the serialized request as the payload.
                let replies = session
                    .get(&key_expr)
                    .payload(payload)
                    .timeout(timeout)
                    .await;

                let receiver = match replies {
                    Ok(r)  => r,
                    Err(_) => return CasResult::Unavailable,
                };

                // Await the first reply.
                match tokio::time::timeout(timeout, receiver.recv_async()).await {
                    Ok(Ok(reply)) => {
                        // Extract the reply sample's payload.
                        match reply.result() {
                            Ok(sample) => {
                                let bytes = sample.payload().to_bytes();
                                serde_json::from_slice::<CasResult>(&bytes)
                                    .unwrap_or(CasResult::Unavailable)
                            }
                            Err(_) => CasResult::Unavailable,
                        }
                    }
                    // Timeout or channel closed — coordinator unreachable.
                    Ok(Err(_)) | Err(_) => CasResult::Unavailable,
                }
            })
        });

        Ok(result)
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(all(test, feature = "cluster"))]
mod tests {
    use super::*;
    use crate::barrier::{KvFencedClaimStore, MemClaimStore};
    use crate::{kv::KvStore, os::MemFencer};

    // routing_set_already_set_unavailable:start
    //   purpose: Full routing round-trip over real Zenoh loopback:
    //            1. client claims "alice" → Set
    //            2. client claims "alice" again → AlreadySet{owner contains "alice"}
    //            3. after dropping the responder, a new client claim → Unavailable
    //   input:  two Zenoh sessions sharing the same underlying runtime (session.clone()),
    //           ClaimResponder on session_coord, RoutedClaimStore on session_client.
    //            Using session.clone() (same Arc<SessionInner>) ensures the queryable is
    //            visible to the GET immediately without needing scouting propagation delay —
    //            same pattern as zenoh-1.9.0/tests/namespace.rs create_local_session().
    //   output: assertions on all three outcomes
    //   sideEffects: opens one Zenoh session (shared via clone); spawns background task
    // routing_set_already_set_unavailable:end
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn routing_set_already_set_unavailable() {
        // Unique key prefix per test to avoid cross-test collisions.
        let key_expr = "bsdos/coupling/barrier/claim/test0";

        // ── Open one Zenoh session, share via clone ───────────────────────────
        // session.clone() shares the same Arc<SessionInner> — a queryable declared
        // on the coordinator clone is immediately visible to a GET on the client clone
        // without waiting for scouting/gossip propagation.  This mirrors the
        // create_local_session() pattern used in zenoh's own namespace tests.
        let sess_root = zenoh::open(zenoh::Config::default()).await
            .expect("Zenoh session");
        let sess_coord  = sess_root.clone();
        let sess_client = sess_root.clone();

        // ── Build coordinator: KvFencedClaimStore + ClaimResponder ────────────
        let kv      = Arc::new(KvStore::new());
        let fencer  = Arc::new(MemFencer::new());
        let store: Arc<dyn ClaimStore + Send + Sync> =
            Arc::new(KvFencedClaimStore::new(kv, fencer));

        let responder = ClaimResponder::new(sess_coord, store, key_expr).await
            .expect("ClaimResponder::new");

        // Brief pause: allow the background task to start recv_async loop
        // and the queryable to register in the shared session.
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // ── Build client RoutedClaimStore ─────────────────────────────────────
        let client = RoutedClaimStore::new(
            sess_client.clone(),
            key_expr,
            Duration::from_secs(3),
        );

        // ── 1. First claim → Set ──────────────────────────────────────────────
        let r1 = client.cas_claim("username:alice", "@alice:local")
            .expect("cas_claim #1");
        assert_eq!(r1, CasResult::Set, "first claim must be Set");

        // ── 2. Second claim (same key) → AlreadySet ───────────────────────────
        let r2 = client.cas_claim("username:alice", "@bob:local")
            .expect("cas_claim #2");
        match r2 {
            CasResult::AlreadySet { ref owner } => {
                assert!(
                    owner.contains("alice"),
                    "AlreadySet owner must contain 'alice', got: {owner}"
                );
            }
            other => panic!("expected AlreadySet, got {:?}", other),
        }

        // ── 3. Drop responder, then claim → Unavailable ───────────────────────
        // Drop the responder — this ends the background task and undeclares the queryable.
        drop(responder);

        // Allow the drop to propagate: the background task's recv_async loop exits
        // when the queryable is dropped; the next GET finds no matching queryable.
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Use a short timeout so the test does not hang for 3 s.
        let client_fast = RoutedClaimStore::new(
            sess_client,
            key_expr,
            Duration::from_millis(400),
        );
        let r3 = client_fast.cas_claim("username:charlie", "@charlie:local")
            .expect("cas_claim #3");
        assert_eq!(r3, CasResult::Unavailable, "no responder must yield Unavailable");
    }

    // no_responder_unavailable:start
    //   purpose: RoutedClaimStore returns Unavailable immediately when no responder
    //            declares the queryable (coordinator is down from the start).
    //   input:  a single Zenoh session; RoutedClaimStore with short timeout; no responder
    //   output: Ok(CasResult::Unavailable)
    //   sideEffects: opens one Zenoh session; issues one GET that times out
    // no_responder_unavailable:end
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn no_responder_unavailable() {
        let key_expr = "bsdos/coupling/barrier/claim/test1";
        let sess = zenoh::open(zenoh::Config::default()).await
            .expect("Zenoh session");

        let client = RoutedClaimStore::new(
            sess,
            key_expr,
            Duration::from_millis(300),
        );

        let r = client.cas_claim("username:ghost", "@ghost:local")
            .expect("cas_claim must not err");
        assert_eq!(r, CasResult::Unavailable, "no coordinator must yield Unavailable");
    }

    // mem_store_still_works:start
    //   purpose: Regression — MemClaimStore (default feature, no Zenoh) is unaffected
    //            by the serde derives added to CasResult.
    //   input:  MemClaimStore, two cas_claim calls
    //   output: Set then AlreadySet (existing behaviour unchanged)
    //   sideEffects: none
    // mem_store_still_works:end
    #[test]
    fn mem_store_still_works() {
        let store = MemClaimStore::new();
        let r1 = store.cas_claim("k", "v").expect("first");
        assert_eq!(r1, CasResult::Set);
        let r2 = store.cas_claim("k", "w").expect("second");
        assert_eq!(r2, CasResult::AlreadySet { owner: "v".to_string() });
    }
}
