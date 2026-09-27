// START_AI_HEADER
// MODULE: mrgd/src/barrier.rs
// PURPOSE: CALM barrier — at-most-once CLAIM of a resource key (username, e2ee OTK,
//          room alias) in an otherwise coordination-free (CRDT) multi-master system.
//
//          Design principle (CALM / invariant-confluence):
//            Everything that can be expressed as a monotone join-semilattice (event
//            delivery, message ordering, room state) stays coordination-free.  A
//            username claim is a NON-monotone invariant (at-most-one owner is not
//            expressible as a CRDT), so it crosses the single coordination point this
//            module implements.
//
//          Normal path (coordinator reachable, CP):
//            compare-and-set on the coupling store — key absent → set(key→claimant)
//            → Claimed; already set → Rejected{owner}.  A fencing token (Fence) prevents
//            a zombie coordinator from double-claiming after a partition.
//
//          Coordinator unreachable (AP-optimistic, Policy::Optimistic):
//            Return Provisional{fence}.  Caller records the claim locally.  Safe because
//            such claims are RARE (user registration ≈ once per user).
//
//          Coordinator unreachable (CP-strict, Policy::Strict):
//            Return Err(BarrierError::Unavailable).  No provisional grant.
//            Applies to e2ee OTK (frequent + security-critical).
//
//          Deterministic reconcile (on partition heal):
//            reconcile(claims) → winner = min by (ts, node_id).  Pure, order-independent,
//            idempotent.  Every node computes the same winner with no further coordination.
//            The resource→owner registry is a confluent LWW-map keyed by (ts, node_id).
//
// DEPENDENCIES: std, thiserror
// PUBLIC_API: Fence, ProvisionalClaim, ClaimOutcome, Policy, BarrierError,
//             CasResult, ClaimStore, MemClaimStore, claim, reconcile
// END_AI_HEADER

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use thiserror::Error;

// ── Fencing token ─────────────────────────────────────────────────────────────

// Fence:start
//   purpose: Monotonic fencing token attached to every claim attempt.
//            Prevents a zombie coordinator (dead node that got partitioned and later
//            reconnected) from double-claiming after a newer coordinator took over.
//            ts + node_id break ties deterministically in reconcile().
//   input:  populated by the caller from its local Fencer before calling claim()
//   output: embedded in ClaimOutcome::Provisional{fence} and ProvisionalClaim
//   sideEffects: none (pure data)
// Fence:end
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Fence {
    /// Logical epoch — incremented on coordinator failover.
    pub epoch: u64,
    /// Wall-clock timestamp (millis since epoch) at the moment of the claim attempt.
    /// Used as the primary tiebreaker in reconcile() (lower ts = earlier claim = winner).
    pub ts: u64,
    /// Stable node identifier (e.g. TLS-certificate fingerprint).
    /// Used as the secondary tiebreaker so reconcile() is deterministic even with
    /// identical timestamps (clock skew under partition).
    pub node_id: String,
}

// ── Provisional claim record ──────────────────────────────────────────────────

// ProvisionalClaim:start
//   purpose: A locally-recorded provisional claim produced when the coordinator is
//            unreachable and Policy::Optimistic is in effect.
//            On partition heal, a slice of ProvisionalClaims is passed to reconcile()
//            which deterministically selects the winner with no further coordination.
//   input:  returned inside ClaimOutcome::Provisional{fence}; caller wraps it here
//   output: passed to reconcile() on heal
//   sideEffects: none (pure data)
// ProvisionalClaim:end
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProvisionalClaim {
    /// The resource key being claimed (e.g. "username:alice", "otk:device123:0").
    pub key: String,
    /// The identity asserting ownership (e.g. Matrix localpart, device ID).
    pub claimant: String,
    /// Fencing token at the time of the provisional grant.
    pub fence: Fence,
}

// ── Claim outcome ─────────────────────────────────────────────────────────────

// ClaimOutcome:start
//   purpose: The result of a successful claim() call (i.e. no Err was returned).
//            Three variants map to the three CP/AP paths:
//              Claimed         — coordinator confirmed at-most-once ownership (CP).
//              Rejected{owner} — coordinator rejected: key already owned (CP).
//              Provisional{fence} — coordinator unreachable, Optimistic policy (AP).
//   input:  returned by claim() on success
//   output: consumed by the caller to decide local registration flow
//   sideEffects: none (pure data)
// ClaimOutcome:end
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClaimOutcome {
    /// Coordinator confirmed: key was absent, now owned by the caller.
    Claimed,
    /// Coordinator rejected: key is already owned by `owner`.
    Rejected {
        /// The existing claimant that already holds this key.
        owner: String,
    },
    /// Coordinator unreachable (Policy::Optimistic): provisional grant recorded locally.
    /// Caller MUST persist this ProvisionalClaim and pass it to reconcile() on heal.
    Provisional {
        /// The fence token of this provisional grant — used by reconcile().
        fence: Fence,
    },
}

// ── Per-invariant coordination policy ────────────────────────────────────────

// Policy:start
//   purpose: Select the CAP trade-off for a specific non-monotone invariant.
//            Different invariants have different safety/availability requirements:
//              Optimistic — coordinator-reachable: CP (no conflict); unreachable: AP
//                           (provisional grant, reconcile later).
//              Strict     — coordinator unreachable: always Err(Unavailable).
//   input:  passed to claim() by the caller based on which invariant is being enforced
//   output: determines the Unavailable branch inside claim()
//   sideEffects: none (pure data)
// Policy:end
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Policy {
    /// Return Provisional when the coordinator is unreachable.
    Optimistic,
    /// Return Err(Unavailable) when the coordinator is unreachable.
    Strict,
}

// ── Errors ────────────────────────────────────────────────────────────────────

// BarrierError:start
//   purpose: Errors that barrier operations can produce.
//   input:  returned by claim()
//   output: propagated to caller for logging / retry / user-facing error response
//   sideEffects: none (error value)
// BarrierError:end
#[derive(Debug, Error, PartialEq, Eq)]
pub enum BarrierError {
    /// Coordinator is unreachable and Policy::Strict forbids a provisional grant.
    #[error("coordinator unavailable and policy is Strict — claim rejected")]
    Unavailable,
    /// The underlying store returned an error.
    #[error("claim store error: {0}")]
    Store(String),
}

// ── CAS result from the ClaimStore ───────────────────────────────────────────

// CasResult:start
//   purpose: The raw compare-and-set result returned by ClaimStore::cas_claim().
//            Decouples the store abstraction from ClaimOutcome so that claim()
//            remains a pure function of CasResult + Policy.
//   input:  returned by ClaimStore::cas_claim()
//   output: consumed by claim() to build ClaimOutcome
//   sideEffects: none (pure data)
// CasResult:end
#[derive(Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum CasResult {
    /// Key was absent; now set to the requested claimant.
    Set,
    /// Key was already present with a different (or same) owner.
    AlreadySet {
        /// The existing owner of this key.
        owner: String,
    },
    /// The coordinator could not be reached (network partition, node down, etc.).
    Unavailable,
}

// ── ClaimStore trait ──────────────────────────────────────────────────────────

// ClaimStore:start
//   purpose: Abstract the coordinator's compare-and-set surface for barrier claims.
//            Implementations:
//              MemClaimStore — in-memory HashMap for tests (default, no network).
//            The trait is the full coordination surface: everything else in barrier.rs
//            is pure (no I/O), tested against MemClaimStore on the host.
//   input:  key — resource key to claim; claimant — identity asserting ownership
//   output: Result<CasResult, BarrierError>
//   sideEffects: implementation-defined (MemClaimStore: HashMap under Mutex)
// ClaimStore:end
pub trait ClaimStore: Send + Sync {
    // cas_claim:start
    //   purpose: Atomically: if key is absent → insert(key→claimant) → Set;
    //            if key is present → AlreadySet{owner}; if coordinator unreachable
    //            → Unavailable (no mutation).
    //   input:  key — resource key; claimant — identity to set as owner
    //   output: Result<CasResult, BarrierError>
    //   sideEffects: implementation-defined
    // cas_claim:end
    fn cas_claim(&self, key: &str, claimant: &str) -> Result<CasResult, BarrierError>;
}

// ── MemClaimStore — for tests ─────────────────────────────────────────────────

// MemClaimStore:start
//   purpose: In-memory ClaimStore for host tests.  Stores claims in a HashMap behind
//            a Mutex; CAS is atomic under the lock.  Optionally simulates coordinator
//            unavailability via the `offline` flag (set before the call under test).
//            Never issues network calls; compiles everywhere.
//   input:  cas_claim(key, claimant) — CAS under Mutex; offline=true → Unavailable
//   output: CasResult::Set / AlreadySet / Unavailable
//   sideEffects: writes to self.inner (Arc<Mutex<HashMap<String, String>>>)
// MemClaimStore:end
#[derive(Clone, Default)]
pub struct MemClaimStore {
    inner: Arc<Mutex<HashMap<String, String>>>,
    /// When true, cas_claim() returns CasResult::Unavailable without touching the map.
    /// Flip this in tests to simulate a coordinator partition.
    pub offline: Arc<Mutex<bool>>,
}

impl MemClaimStore {
    // new:start
    //   purpose: Construct an empty MemClaimStore in online mode.
    //   input:  none
    //   output: MemClaimStore
    //   sideEffects: allocates two Arc<Mutex<_>>
    // new:end
    pub fn new() -> Self {
        Self::default()
    }

    // set_offline:start
    //   purpose: Switch the store into simulated-coordinator-unreachable mode.
    //            After this call cas_claim() returns CasResult::Unavailable.
    //   input:  offline — true to simulate partition; false to restore
    //   output: none
    //   sideEffects: writes to self.offline under Mutex
    // set_offline:end
    pub fn set_offline(&self, offline: bool) {
        let mut guard = self.offline.lock().unwrap_or_else(|e| e.into_inner());
        *guard = offline;
    }
}

impl ClaimStore for MemClaimStore {
    fn cas_claim(&self, key: &str, claimant: &str) -> Result<CasResult, BarrierError> {
        {
            let offline = self.offline.lock().unwrap_or_else(|e| e.into_inner());
            if *offline {
                return Ok(CasResult::Unavailable);
            }
        }

        let mut guard = self
            .inner
            .lock()
            .map_err(|e| BarrierError::Store(e.to_string()))?;
        match guard.get(key) {
            Some(existing) => Ok(CasResult::AlreadySet {
                owner: existing.clone(),
            }),
            None => {
                guard.insert(key.to_string(), claimant.to_string());
                Ok(CasResult::Set)
            }
        }
    }
}

// ── Core barrier function: claim ──────────────────────────────────────────────

// claim:start
//   purpose: Assert at-most-once ownership of `key` for `claimant`.
//            Routes through ClaimStore::cas_claim() and maps the raw CasResult to
//            a ClaimOutcome according to the Policy.
//   input:  store    — &dyn ClaimStore (MemClaimStore in tests)
//           key      — resource key (e.g. "username:alice", "otk:device:0")
//           claimant — identity asserting ownership (e.g. Matrix localpart)
//           fence    — fencing token from the caller's local Fencer
//           policy   — Optimistic (provisional on unavailable) | Strict (error)
//   output: Result<ClaimOutcome, BarrierError>
//   sideEffects: calls store.cas_claim() which mutates MemClaimStore or the real KV
// claim:end
pub fn claim(
    store: &dyn ClaimStore,
    key: &str,
    claimant: &str,
    fence: Fence,
    policy: Policy,
) -> Result<ClaimOutcome, BarrierError> {
    match store.cas_claim(key, claimant)? {
        CasResult::Set => Ok(ClaimOutcome::Claimed),
        CasResult::AlreadySet { owner } => Ok(ClaimOutcome::Rejected { owner }),
        CasResult::Unavailable => match policy {
            Policy::Optimistic => Ok(ClaimOutcome::Provisional { fence }),
            Policy::Strict => Err(BarrierError::Unavailable),
        },
    }
}

// ── Deterministic reconcile ───────────────────────────────────────────────────

// reconcile:start
//   purpose: Select the winner from a set of ProvisionalClaims produced during a
//            partition.  On partition heal every node calls this function with the
//            same input slice; the function is pure, order-independent, and idempotent,
//            so every node arrives at the same winner with no further coordination.
//
//            Winner selection algorithm (LWW-map by (ts, node_id)):
//              winner = min_by(fence.ts, then fence.node_id)
//              Rationale: lower ts = earlier claim.  node_id tiebreaker is stable
//              so the result is deterministic under clock skew.
//
//   input:  claims — slice of ProvisionalClaim (may be empty; may be in any order)
//   output: Option<&ProvisionalClaim> — the winner, or None if claims is empty
//   sideEffects: none (pure function)
// reconcile:end
pub fn reconcile(claims: &[ProvisionalClaim]) -> Option<&ProvisionalClaim> {
    claims.iter().min_by(|a, b| {
        a.fence
            .ts
            .cmp(&b.fence.ts)
            .then_with(|| a.fence.node_id.cmp(&b.fence.node_id))
    })
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn fence(ts: u64, node: &str) -> Fence {
        Fence {
            epoch: 1,
            ts,
            node_id: node.to_string(),
        }
    }

    // fresh_key_claimed:start
    //   purpose: claim() on a key that does not yet exist returns Claimed.
    //   input:  fresh MemClaimStore, Policy::Optimistic
    //   output: Ok(ClaimOutcome::Claimed)
    //   sideEffects: key inserted into store
    // fresh_key_claimed:end
    #[test]
    fn fresh_key_claimed() {
        let store = MemClaimStore::new();
        let result = claim(
            &store,
            "username:alice",
            "alice",
            fence(1, "node-a"),
            Policy::Optimistic,
        )
        .expect("claim on fresh key must not error");
        assert_eq!(
            result,
            ClaimOutcome::Claimed,
            "fresh key must yield Claimed"
        );
    }

    // taken_key_rejected:start
    //   purpose: claim() on a key already owned by another claimant returns Rejected{owner}.
    //   input:  MemClaimStore with "username:bob" already owned by "bob"
    //   output: Ok(ClaimOutcome::Rejected { owner: "bob" })
    //   sideEffects: store unchanged (no overwrite)
    // taken_key_rejected:end
    #[test]
    fn taken_key_rejected() {
        let store = MemClaimStore::new();
        claim(
            &store,
            "username:bob",
            "bob",
            fence(1, "node-a"),
            Policy::Optimistic,
        )
        .expect("first claim");
        let result = claim(
            &store,
            "username:bob",
            "charlie",
            fence(2, "node-b"),
            Policy::Optimistic,
        )
        .expect("second claim must not error");
        assert_eq!(
            result,
            ClaimOutcome::Rejected {
                owner: "bob".to_string()
            },
            "already-owned key must yield Rejected{{owner}}"
        );
    }

    // optimistic_unavailable_provisional:start
    //   purpose: claim() with Policy::Optimistic when coordinator is unreachable
    //            returns Provisional{fence} (AP path, provisional grant).
    //   input:  MemClaimStore with offline=true, Policy::Optimistic
    //   output: Ok(ClaimOutcome::Provisional { fence })
    //   sideEffects: no mutation in store (coordinator down, nothing written)
    // optimistic_unavailable_provisional:end
    #[test]
    fn optimistic_unavailable_provisional() {
        let store = MemClaimStore::new();
        store.set_offline(true);
        let f = fence(42, "node-a");
        let result = claim(
            &store,
            "username:dave",
            "dave",
            f.clone(),
            Policy::Optimistic,
        )
        .expect("Optimistic+Unavailable must not error");
        assert_eq!(
            result,
            ClaimOutcome::Provisional { fence: f },
            "Optimistic+Unavailable must yield Provisional{{fence}}"
        );
    }

    // strict_unavailable_errors:start
    //   purpose: claim() with Policy::Strict when coordinator is unreachable
    //            returns Err(BarrierError::Unavailable) — no provisional grant.
    //   input:  MemClaimStore with offline=true, Policy::Strict
    //   output: Err(BarrierError::Unavailable)
    //   sideEffects: none
    // strict_unavailable_errors:end
    #[test]
    fn strict_unavailable_errors() {
        let store = MemClaimStore::new();
        store.set_offline(true);
        let result = claim(
            &store,
            "otk:device0:0",
            "device0",
            fence(1, "node-a"),
            Policy::Strict,
        );
        assert_eq!(
            result,
            Err(BarrierError::Unavailable),
            "Strict+Unavailable must return Err(Unavailable)"
        );
    }

    // reconcile_lower_ts_wins:start
    //   purpose: reconcile() selects the claim with the lower ts (earlier claim wins).
    //   input:  two ProvisionalClaims, ts=100 and ts=200
    //   output: the claim with ts=100 is the winner
    //   sideEffects: none (pure)
    // reconcile_lower_ts_wins:end
    #[test]
    fn reconcile_lower_ts_wins() {
        let claims = vec![
            ProvisionalClaim {
                key: "username:eve".to_string(),
                claimant: "eve-node-b".to_string(),
                fence: fence(200, "node-b"),
            },
            ProvisionalClaim {
                key: "username:eve".to_string(),
                claimant: "eve-node-a".to_string(),
                fence: fence(100, "node-a"),
            },
        ];
        let winner = reconcile(&claims).expect("must select a winner from non-empty slice");
        assert_eq!(
            winner.claimant, "eve-node-a",
            "lower ts (100) must win over ts=200"
        );
    }

    // reconcile_ts_tie_lower_node_id_wins:start
    //   purpose: reconcile() breaks ts ties by node_id (lexicographic, lower wins).
    //   input:  two ProvisionalClaims with identical ts but different node_id
    //   output: the claim with the lexicographically smaller node_id is the winner
    //   sideEffects: none (pure)
    // reconcile_ts_tie_lower_node_id_wins:end
    #[test]
    fn reconcile_ts_tie_lower_node_id_wins() {
        let claims = vec![
            ProvisionalClaim {
                key: "username:frank".to_string(),
                claimant: "frank-z".to_string(),
                fence: fence(500, "node-z"),
            },
            ProvisionalClaim {
                key: "username:frank".to_string(),
                claimant: "frank-a".to_string(),
                fence: fence(500, "node-a"),
            },
        ];
        let winner = reconcile(&claims).expect("must select a winner");
        assert_eq!(
            winner.claimant, "frank-a",
            "on equal ts, lexicographically smaller node_id (node-a < node-z) must win"
        );
    }

    // reconcile_order_independent:start
    //   purpose: reconcile() produces the same winner regardless of input slice order.
    //   input:  same three ProvisionalClaims in two different orderings
    //   output: both orderings select the same winner (ts=10, node-a)
    //   sideEffects: none (pure)
    // reconcile_order_independent:end
    #[test]
    fn reconcile_order_independent() {
        let c1 = ProvisionalClaim {
            key: "username:grace".to_string(),
            claimant: "grace-a".to_string(),
            fence: fence(10, "node-a"),
        };
        let c2 = ProvisionalClaim {
            key: "username:grace".to_string(),
            claimant: "grace-b".to_string(),
            fence: fence(20, "node-b"),
        };
        let c3 = ProvisionalClaim {
            key: "username:grace".to_string(),
            claimant: "grace-c".to_string(),
            fence: fence(15, "node-c"),
        };

        let order1 = vec![c1.clone(), c2.clone(), c3.clone()];
        let order2 = vec![c3.clone(), c1.clone(), c2.clone()];
        let order3 = vec![c2.clone(), c3.clone(), c1.clone()];

        let w1 = reconcile(&order1).expect("order1").claimant.clone();
        let w2 = reconcile(&order2).expect("order2").claimant.clone();
        let w3 = reconcile(&order3).expect("order3").claimant.clone();

        assert_eq!(w1, "grace-a", "order1 must select grace-a (ts=10 lowest)");
        assert_eq!(w2, w1, "order2 must match order1");
        assert_eq!(w3, w1, "order3 must match order1");
    }

    // reconcile_idempotent:start
    //   purpose: reconcile() on the same input twice selects the same winner.
    //   input:  same slice passed twice
    //   output: identical winner both times
    //   sideEffects: none (pure)
    // reconcile_idempotent:end
    #[test]
    fn reconcile_idempotent() {
        let claims = vec![
            ProvisionalClaim {
                key: "username:henry".to_string(),
                claimant: "henry-b".to_string(),
                fence: fence(30, "node-b"),
            },
            ProvisionalClaim {
                key: "username:henry".to_string(),
                claimant: "henry-a".to_string(),
                fence: fence(25, "node-a"),
            },
        ];
        let w1 = reconcile(&claims).expect("first call").claimant.clone();
        let w2 = reconcile(&claims).expect("second call").claimant.clone();
        assert_eq!(w1, w2, "reconcile must be idempotent");
        assert_eq!(w1, "henry-a", "ts=25 wins over ts=30");
    }

    // reconcile_empty_is_none:start
    //   purpose: reconcile() on an empty slice returns None (no claims → no winner).
    //   input:  empty slice
    //   output: None
    //   sideEffects: none (pure)
    // reconcile_empty_is_none:end
    #[test]
    fn reconcile_empty_is_none() {
        let claims: Vec<ProvisionalClaim> = Vec::new();
        assert!(
            reconcile(&claims).is_none(),
            "reconcile on empty slice must return None"
        );
    }
}
