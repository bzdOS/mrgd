// START_AI_HEADER
// MODULE: matrix-hs/src/routes/keys.rs
// PURPOSE: Matrix CS-API E2EE key endpoints — keys/upload, keys/query, keys/claim.
//          Implements the OWNERSHIP-PARTITION exactly-once OTK claim barrier:
//          each device's OTKs are OWNED by the node they were uploaded to.  A claim
//          for that device is served ONLY by the owner, which pops the key atomically
//          (Mutex remove → never returned twice).  No cross-node contention →
//          exactly-once WITHOUT a distributed lock.
//
//          SCOPE: OTK claim-exactly-once barrier + minimal key storage.
//          NOT IN SCOPE (explicitly deferred):
//            - Full E2EE: actual encryption, device/cross-signing verification,
//              signature validation on uploads, fallback keys, key backup,
//              to-device key-share flows.
//
//          Cross-node OTK claim routing (OWNERSHIP-PARTITION extension):
//            When a claim targets a (user,device) whose OTKs are NOT owned locally
//            (not in device_otks on this node), the claim is routed to the owner
//            over a Zenoh wildcard queryable ("<prefix>/keys/claim/**", declared in
//            main.rs, keyed by base64url-encoded (user_id,device_id,algorithm) —
//            see ClusterState::claim_key/fetch_otk in state.rs) — the same "ask
//            the mesh, take whichever answer comes back" pull pattern the media
//            cross-node fetch already uses (routes/media.rs::fetch_media_cross_node).
//            try_claim_local (below) is the ONE function that ever pops a key —
//            called both by the local fast path here and by the queryable handler
//            in main.rs — so exactly-once holds cross-node for the same reason it
//            always held locally: a key exists in at most one node's device_otks,
//            so at most one caller of try_claim_local can ever have something to
//            return. Without the `cluster` feature, a claim for a non-local device
//            still silently returns absent (Matrix-correct: the spec allows absent
//            OTKs; the client retries or escalates).
//
//          E2EE device-list change tracking (device-lists feature): keys/upload with a
//          device_keys field records "this user's device list changed" (AppState::
//          mark_device_list_changed) and — under the cluster feature — gossips that
//          signal to every other node (gossip_device_list_change / drain_device_list_gossip,
//          mirroring routes/to_device.rs's store-and-forward gossip pattern). routes/sync.rs
//          and routes/sliding_sync.rs consume AppState::device_list_changes_since +
//          users_sharing_room_with to fill device_lists.changed.
//
// DEPENDENCIES: axum, serde_json, AppState, auth, (cluster) crate::substrate::crdt::CrdtSink, zenoh
// PUBLIC_API: post_keys_upload, post_keys_query, post_keys_claim, try_claim_local,
//             (cluster) gossip_device_list_change, drain_device_list_gossip, serve_otk_claims
// END_AI_HEADER

use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde_json::{json, Value};
use std::sync::Arc;

use crate::{error::HsError, state::AppState};

// ── Auth helper ────────────────────────────────────────────────────────────────
//
// extract_caller now lives in auth.rs (canonical, deduped implementation shared by
// every route module that needs the caller's identity). Re-exported here so
// existing `use super::keys::extract_caller` imports elsewhere in this crate
// (routes/pushers.rs, routes/ephemeral.rs, routes/media.rs, routes/room_keys.rs)
// keep working unchanged.
pub(crate) use crate::auth::extract_caller;

// ── Helpers ───────────────────────────────────────────────────────────────────

// count_by_algorithm:start
//   purpose: Count remaining OTKs per algorithm for (user_id, device_id).
//            Scans the key_id strings "alg:key_id" and tallies by prefix.
//   input:  otks — reference to the inner HashMap<"alg:key_id", key>
//   output: serde_json::Value object {"algorithm": count, ...}
//   sideEffects: none
//   pub(crate): also reused by routes/sync.rs and routes/sliding_sync.rs to fill
//   the sync response's device_one_time_keys_count for the calling device.
// count_by_algorithm:end
pub(crate) fn count_by_algorithm(otks: &std::collections::HashMap<String, Value>) -> Value {
    let mut counts: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
    for key_id in otks.keys() {
        let alg = key_id.split(':').next().unwrap_or("unknown");
        *counts.entry(alg.to_string()).or_insert(0) += 1;
    }
    // Serialise as {"alg": count}.
    let mut obj = serde_json::Map::new();
    for (alg, n) in counts {
        obj.insert(alg, json!(n));
    }
    Value::Object(obj)
}

// ── Request bodies ────────────────────────────────────────────────────────────

// post_keys_upload:start
//   purpose: POST /_matrix/client/v3/keys/upload — store device_keys and/or one_time_keys.
//            Requires a valid signed Bearer token (mxt_); unknown/missing token → 401.
//            Merges one_time_keys into device_otks for (caller_user_id, caller_device_id).
//            The node that stores them becomes the OWNER (ownership-partition invariant).
//            Stores device_keys blob for keys/query.
//            Returns {"one_time_key_counts": {alg: remaining_count}}.
//   input:  JSON body {"device_keys"?: {...}, "one_time_keys"?: {"alg:key_id": key, ...}}
//           Authorization: Bearer <signed mxt_ token>
//   output: JSON {"one_time_key_counts": {"curve25519": N, ...}}
//           401 M_UNKNOWN_TOKEN on missing/invalid token
//   sideEffects: writes to state.e2ee.device_otks and state.e2ee.device_keys
// post_keys_upload:end
pub async fn post_keys_upload(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Option<Json<Value>>,
) -> Result<Json<Value>, HsError> {
    let (user_id, device_id) = extract_caller(&headers, &state)
        .ok_or_else(|| HsError::UnknownToken("missing or invalid token".to_string()))?;
    let key = (user_id.clone(), device_id.clone());
    let body = body.map(|b| b.0).unwrap_or_else(|| json!({}));

    // ── Store device_keys if present ──────────────────────────────────────────
    if let Some(dk) = body.get("device_keys") {
        if let Ok(mut guard) = state.e2ee.device_keys.lock() {
            guard.insert(key.clone(), dk.clone());
        }

        // A device_keys upload establishes a (possibly brand new) Olm identity for
        // this device — e.g. a client reinstall/app-data-clear generates a fresh
        // curve25519/ed25519 keypair while reusing the same device_id. Any
        // one_time_keys left over from a PRIOR identity on this same device slot
        // are cryptographically tied to that old identity and are now meaningless
        // (and would be actively wrong to keep serving via keys/claim). Clear them
        // so the OTK count this call returns reflects only what's being uploaded
        // now — matrix-dart-sdk's OlmManager.init() requires the returned
        // one_time_key_counts.signed_curve25519 to exactly equal the number of
        // keys it just uploaded when creating a fresh account, and throws "Upload
        // key failed" otherwise; stale leftover keys inflate that count and trip
        // this check, reproduced live against a real FluffyChat client.
        if let Ok(mut guard) = state.e2ee.device_otks.lock() {
            guard.remove(&key);
        }
    }

    // ── Merge one_time_keys into device_otks ──────────────────────────────────
    if let Some(otk_map) = body.get("one_time_keys").and_then(|v| v.as_object()) {
        if let Ok(mut guard) = state.e2ee.device_otks.lock() {
            let slot = guard.entry(key.clone()).or_default();
            for (alg_key_id, key_val) in otk_map {
                // Merge; do not overwrite existing keys (idempotent re-upload).
                slot.entry(alg_key_id.clone())
                    .or_insert_with(|| key_val.clone());
            }
        }
    }

    // ── Device-list change tracking (E2EE device-lists feature) ──────────────
    // A device_keys upload means this user's device identity keys changed — this
    // is exactly the event /sync's device_lists.changed exists to announce to
    // every other user who shares a room with them. An one_time_keys-only upload
    // (no device_keys field) does NOT trigger this — OTK replenishment is not a
    // device-list change (a client can have thousands of routine OTK top-ups).
    if body.get("device_keys").is_some() {
        state
            .mark_device_list_changed(&user_id)
            .map_err(HsError::Internal)?;
        #[cfg(feature = "cluster")]
        gossip_device_list_change(&state, &user_id).await?;
    }

    // ── Compute and return counts ─────────────────────────────────────────────
    let counts = if let Ok(guard) = state.e2ee.device_otks.lock() {
        guard
            .get(&key)
            .map(count_by_algorithm)
            .unwrap_or_else(|| json!({}))
    } else {
        json!({})
    };

    Ok(Json(json!({ "one_time_key_counts": counts })))
}

// ── Device-list change gossip (cluster feature only) ─────────────────────────
//
// DESIGN NOTE: device_keys/device_otks themselves are NOT cluster-replicated
// (see the module header's DEFERRED note on cross-node OTK claim routing — the
// same is true of device_keys storage: it lives only on the node that received
// the keys/upload call). What DOES need to reach every node is the lightweight
// SIGNAL "user X's device list changed" — a client syncing from a DIFFERENT node
// than the one that received the upload still needs device_lists.changed to
// include X so it knows to run keys/query (which will then legitimately return
// nothing for X on that node — a pre-existing gap, out of scope here — but the
// client at least knows to re-check, and in practice a real deployment federates
// keys/query to X's actual home node). This mirrors routes/to_device.rs's
// STORE-AND-FORWARD GOSSIP pattern exactly: publish on the sending node, drain on
// every node's next /sync (see routes::sync::get_sync /
// routes::sliding_sync::post_sliding_sync).

// DeviceListGossipMsg:start
//   purpose: Wire format for one device-list-changed announcement broadcast over
//            Zenoh gossip. serde_json, no new crates — control-plane message.
//   input:  constructed by gossip_device_list_change
//   output: round-trips through serde_json::to_vec / from_slice
//   sideEffects: none
// DeviceListGossipMsg:end
#[cfg(feature = "cluster")]
#[derive(serde::Serialize, serde::Deserialize)]
struct DeviceListGossipMsg {
    user_id: String,
}

// device_list_gossip_room / device_list_gossip_key:start
//   purpose: Pseudo "room_id" / routing key under which the device-list-change
//            gossip channel is scoped inside ClusterState's per-room ZenohCrdtSink
//            registry — mirrors to_device_gossip_room/to_device_gossip_key.
//   input:  none
//   output: &'static str
//   sideEffects: none
// device_list_gossip_room / device_list_gossip_key:end
#[cfg(feature = "cluster")]
fn device_list_gossip_room() -> &'static str {
    "__device_list__"
}
#[cfg(feature = "cluster")]
fn device_list_gossip_key() -> &'static str {
    "changes"
}

// gossip_device_list_change:start
//   purpose: Broadcast one "user_id's device list changed" announcement to every
//            other node via Zenoh pub/sub (ClusterState::sink_for("__device_list__")).
//            Best-effort: single-node mode (no cluster) is a no-op — the local
//            mark_device_list_changed call already recorded the change on this node.
//            pub(crate): also called from routes/register.rs (device add) and
//            routes/account.rs (device removal) so all three device-list-changing
//            events reach the whole cluster the same way.
//   input:  state — Arc<AppState>; user_id — whose device list changed
//   output: Result<(), HsError>
//   sideEffects: (cluster active) opens/reuses the "__device_list__" ZenohCrdtSink;
//                publishes one Zenoh sample; every peer's background subscriber
//                receives it and (via drain_device_list_gossip on their next /sync)
//                calls mark_device_list_changed locally
// gossip_device_list_change:end
#[cfg(feature = "cluster")]
pub(crate) async fn gossip_device_list_change(
    state: &Arc<AppState>,
    user_id: &str,
) -> Result<(), HsError> {
    use crate::substrate::crdt::CrdtSink as _;

    let Some(cluster) = &state.cluster else {
        return Ok(()); // single-node mode — local mark_device_list_changed already happened
    };

    let msg = DeviceListGossipMsg {
        user_id: user_id.to_string(),
    };
    let bytes = serde_json::to_vec(&msg).map_err(|e| HsError::Internal(e.to_string()))?;

    let sink = cluster
        .sink_for(device_list_gossip_room())
        .await
        .map_err(HsError::Internal)?;

    sink.publish(device_list_gossip_key(), bytes)
        .map_err(|e| HsError::Internal(e.to_string()))?;

    Ok(())
}

// drain_device_list_gossip:start
//   purpose: Drain any pending device-list-change gossip samples received from
//            OTHER nodes and apply them locally via mark_device_list_changed.
//            Called from routes::sync::get_sync and
//            routes::sliding_sync::post_sliding_sync on every request (mirrors
//            routes::to_device::drain_to_device_gossip) so a change that arrived
//            here via gossip is visible before this node's response is built.
//            Each drained sample calls mark_device_list_changed again, which is
//            safe to call repeatedly for the same user (last-writer-wins register,
//            not an append-only log) — at-least-once redelivery just re-records the
//            same or a newer pos, never a duplicate/incorrect entry.
//   input:  state — Arc<AppState>
//   output: Result<(), HsError>
//   sideEffects: (cluster active) drains the "__device_list__" ZenohCrdtSink inbox;
//                mutates device_list_changes + stream_pos via
//                mark_device_list_changed; may wake /sync long-poll waiters
// drain_device_list_gossip:end
#[cfg(feature = "cluster")]
pub(crate) async fn drain_device_list_gossip(state: &Arc<AppState>) -> Result<(), HsError> {
    use crate::substrate::crdt::CrdtSink as _;

    let Some(cluster) = &state.cluster else {
        return Ok(());
    };

    let sink = cluster
        .sink_for(device_list_gossip_room())
        .await
        .map_err(HsError::Internal)?;

    let blobs = sink
        .drain(device_list_gossip_key())
        .map_err(|e| HsError::Internal(e.to_string()))?;

    for bytes in blobs {
        let msg: DeviceListGossipMsg = match serde_json::from_slice(&bytes) {
            Ok(m) => m,
            Err(_) => continue, // malformed sample — skip, never panic on network input
        };
        state
            .mark_device_list_changed(&msg.user_id)
            .map_err(HsError::Internal)?;
    }

    Ok(())
}

// post_keys_query:start
//   purpose: POST /_matrix/client/v3/keys/query — return stored device_keys for queried users.
//            Requires a valid signed Bearer token (mxt_); unknown/missing token → 401.
//            Body: {"device_keys": {user_id: [device_id, ...]}} (device list may be empty →
//            return all known devices for that user).
//            Also returns one_time_key_counts for the calling user.
//   input:  JSON body {"device_keys": {user_id: {device_id: {}, ...}}}
//           Authorization: Bearer <signed mxt_ token>
//   output: JSON {"device_keys": {user_id: {device_id: <stored blob>}},
//                 "failures": {}}
//           401 M_UNKNOWN_TOKEN on missing/invalid token
//   sideEffects: none (read-only)
// post_keys_query:end
pub async fn post_keys_query(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Option<Json<Value>>,
) -> Result<Json<Value>, HsError> {
    let (caller_user_id, caller_device_id) = extract_caller(&headers, &state)
        .ok_or_else(|| HsError::UnknownToken("missing or invalid token".to_string()))?;
    let body = body.map(|b| b.0).unwrap_or_else(|| json!({}));

    let mut result: serde_json::Map<String, Value> = serde_json::Map::new();
    // cross-signing:start
    //   purpose: master_keys/self_signing_keys are PUBLIC — populated for any
    //            queried user that has uploaded them via
    //            keys/device_signing/upload. user_signing_keys is PRIVATE per the
    //            Matrix spec — populated ONLY for the caller's own entry (a
    //            user's user_signing_key must never be exposed to anyone else).
    //   sideEffects: none (read-only)
    // cross-signing:end
    let mut master_keys: serde_json::Map<String, Value> = serde_json::Map::new();
    let mut self_signing_keys: serde_json::Map<String, Value> = serde_json::Map::new();
    let mut user_signing_keys: serde_json::Map<String, Value> = serde_json::Map::new();

    if let Some(query_map) = body.get("device_keys").and_then(|v| v.as_object()) {
        for (uid, devices_val) in query_map {
            let mut user_result: serde_json::Map<String, Value> = serde_json::Map::new();

            // Per the Matrix spec, the per-user value is a JSON ARRAY of device_id
            // strings (empty array = "all devices for this user") — NOT an object.
            // Every real client (matrix-dart-sdk, matrix-js-sdk, Synapse itself)
            // sends the array form; only accepting an object here silently returned
            // empty device_keys for every real query.
            let requested_device_ids: Option<Vec<String>> = devices_val.as_array().map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            });

            if let Ok(guard) = state.e2ee.device_keys.lock() {
                match requested_device_ids {
                    Some(ref ids) if !ids.is_empty() => {
                        for did in ids {
                            let slot_key = (uid.clone(), did.clone());
                            if let Some(dk) = guard.get(&slot_key) {
                                user_result.insert(did.clone(), dk.clone());
                            }
                        }
                    }
                    _ => {
                        // Empty array (or absent/malformed value) means "return all
                        // devices for this user".
                        for ((stored_uid, stored_did), dk) in guard.iter() {
                            if stored_uid == uid {
                                user_result.insert(stored_did.clone(), dk.clone());
                            }
                        }
                    }
                }
            }

            result.insert(uid.clone(), Value::Object(user_result));

            // Fold in this user's cross-signing keys, if any were uploaded.
            if let Some(csk) = state.get_cross_signing_keys(uid) {
                if let Some(mk) = csk.master_key {
                    master_keys.insert(uid.clone(), mk);
                }
                if let Some(ssk) = csk.self_signing_key {
                    self_signing_keys.insert(uid.clone(), ssk);
                }
                // PRIVATE: only ever included for the caller's own user_id.
                if uid == &caller_user_id {
                    if let Some(usk) = csk.user_signing_key {
                        user_signing_keys.insert(uid.clone(), usk);
                    }
                }
            }
        }
    }

    // Also include OTK counts for the caller (informational, not required by spec).
    let caller_key = (caller_user_id, caller_device_id);
    let counts = if let Ok(guard) = state.e2ee.device_otks.lock() {
        guard
            .get(&caller_key)
            .map(count_by_algorithm)
            .unwrap_or_else(|| json!({}))
    } else {
        json!({})
    };

    Ok(Json(json!({
        "device_keys":          Value::Object(result),
        "failures":             {},
        "one_time_key_counts":  counts,
        "master_keys":          Value::Object(master_keys),
        "self_signing_keys":    Value::Object(self_signing_keys),
        "user_signing_keys":    Value::Object(user_signing_keys),
    })))
}

// ── Cross-signing (POST /keys/device_signing/upload, keys/signatures/upload) ──

// post_device_signing_upload:start
//   purpose: POST /keys/device_signing/upload — accept and store the caller's
//            master_key, self_signing_key, and user_signing_key (each an opaque
//            CrossSigningKey JSON blob per the Matrix spec — stored exactly as
//            supplied, never validated or re-derived). Fields the client omits
//            are left unchanged (re-upload of just one key is valid).
//
//            UIA (Phase 2, 2026-08-04): gated on m.login.password, not on the
//            bearer token alone. A master key is the root of a user's device trust —
//            replacing it re-points every "is this device really theirs?" answer — so
//            a stolen or leaked access token must not be enough on its own. The
//            password re-entry is what distinguishes the person from the token.
//            Flow, per spec and mirroring routes/register.rs:
//              1. No `auth` block  → 401 carrying {flows:[{stages:["m.login.password"]}],
//                 session}.
//              2. Re-request with auth = {type, session, password, identifier|user}.
//                 The session must be one we issued (one-shot) and the password must
//                 verify against the CALLER's own account — not any account, which is
//                 why the user in `identifier` is checked against the token's user_id.
//   input:  JSON body {"master_key"?, "self_signing_key"?, "user_signing_key"?,
//           "auth"?: {...}}; Authorization: Bearer <signed mxt_ token>
//   output: JSON {} (empty object, per spec)
//           401 with a UIA challenge when `auth` is absent or unusable
//           401 M_UNKNOWN_TOKEN on missing/invalid token
//           403 M_FORBIDDEN on a wrong password
//   sideEffects: writes to state.e2ee.cross_signing_keys (node-local, in-memory only —
//                see AppState.e2ee.cross_signing_keys doc comment)
// post_device_signing_upload:end
pub async fn post_device_signing_upload(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Option<Json<Value>>,
) -> Result<Response, HsError> {
    let (user_id, _device_id) = extract_caller(&headers, &state)
        .ok_or_else(|| HsError::UnknownToken("missing or invalid token".to_string()))?;
    let body = body.map(|b| b.0).unwrap_or_else(|| json!({}));

    // ── UIA gate ─────────────────────────────────────────────────────────────
    match uia_password_check(&state, &user_id, body.get("auth"))? {
        UiaOutcome::Passed => {}
        UiaOutcome::Challenge(session) => {
            return Ok((
                StatusCode::UNAUTHORIZED,
                Json(json!({
                    "flows":   [{ "stages": ["m.login.password"] }],
                    "params":  {},
                    "session": session,
                })),
            )
                .into_response());
        }
    }

    state
        .set_cross_signing_keys(
            &user_id,
            body.get("master_key").cloned(),
            body.get("self_signing_key").cloned(),
            body.get("user_signing_key").cloned(),
        )
        .map_err(HsError::Internal)?;

    // A cross-signing identity change is exactly the kind of thing device_lists.changed
    // exists to announce (mirrors keys/upload's device_keys handling) — also wakes any
    // /sync long-poll waiter immediately instead of leaving it to time out.
    state
        .mark_device_list_changed(&user_id)
        .map_err(HsError::Internal)?;

    Ok(Json(json!({})).into_response())
}

// UiaOutcome:start
//   purpose: Result of the UIA gate: either the caller has re-authenticated, or they
//            must be handed a challenge and asked again.
//   input:  produced by uia_password_check
//   output: enum value
//   sideEffects: none
// UiaOutcome:end
enum UiaOutcome {
    Passed,
    Challenge(String),
}

// uia_password_check:start
//   purpose: The m.login.password stage of User-Interactive Auth. Anything short of a
//            complete, valid auth block — absent, wrong stage, unknown session, or a
//            username that is not the caller's own — produces a FRESH challenge rather
//            than a pass. Only a wrong password is an outright failure, so a client
//            cannot distinguish "your session expired" from "your password is wrong"
//            by the shape of the response alone.
//
//            The identifier is checked against the caller's own user_id: without that,
//            a stolen token plus any other account's password would clear the gate.
//   input:  state; user_id — from the bearer token; auth — body["auth"], if present
//   output: Ok(Passed) | Ok(Challenge(session_id)) | Err(Forbidden) on a bad password
//   sideEffects: issues a UIA session on challenge; consumes one on success
// uia_password_check:end
fn uia_password_check(
    state: &Arc<AppState>,
    user_id: &str,
    auth: Option<&Value>,
) -> Result<UiaOutcome, HsError> {
    let challenge = |state: &Arc<AppState>| -> Result<UiaOutcome, HsError> {
        let session = state.issue_uia_session().map_err(HsError::Internal)?;
        Ok(UiaOutcome::Challenge(session))
    };

    let Some(auth) = auth else {
        return challenge(state);
    };
    if auth.get("type").and_then(|v| v.as_str()) != Some("m.login.password") {
        return challenge(state);
    }
    let Some(session) = auth.get("session").and_then(|v| v.as_str()) else {
        return challenge(state);
    };
    if !state
        .consume_uia_session(session)
        .map_err(HsError::Internal)?
    {
        // Unknown or already-used session — one-shot by design.
        return challenge(state);
    }

    // The account being proved must be the caller's own.
    let claimed = auth
        .get("identifier")
        .and_then(|i| i.get("user"))
        .and_then(|v| v.as_str())
        .or_else(|| auth.get("user").and_then(|v| v.as_str()));
    let localpart = user_id
        .strip_prefix('@')
        .and_then(|s| s.split(':').next())
        .unwrap_or("");
    if let Some(claimed) = claimed {
        let claimed_local = claimed
            .strip_prefix('@')
            .and_then(|s| s.split(':').next())
            .unwrap_or(claimed);
        if claimed_local != localpart {
            return challenge(state);
        }
    }

    let supplied = auth.get("password").and_then(|v| v.as_str()).unwrap_or("");
    let users = state.users.lock().map_err(|e| HsError::Internal(e.to_string()))?;
    let Some(record) = users.get(localpart) else {
        return Err(HsError::Forbidden("unknown user".to_string()));
    };
    if !crate::auth::verify_password(&record.password_hash, supplied) {
        return Err(HsError::Forbidden("invalid password".to_string()));
    }
    Ok(UiaOutcome::Passed)
}

// post_signatures_upload:start
//   purpose: POST /keys/signatures/upload — accept cross-signature blobs attached
//            to devices/keys. Body shape per spec:
//              {user_id: {key_or_device_id: {<opaque signed-key-object>}}}
//            (a client only ever signs its OWN user_id's keys/devices, or another
//            user's master key after verification — both land in this same shape).
//            SCOPE (deliberately minimal): each blob is stored OPAQUELY, keyed by
//            (uploader_user_id, key_or_device_id) via
//            AppState::store_cross_signature. It is NOT merged into the stored
//            device_keys / cross_signing_keys objects (which would require
//            validating the target id actually belongs to the object being
//            signed) — a real implementation would fold each signature into the
//            "signatures" map of the matching device_keys or cross-signing-key
//            JSON so keys/query naturally returns it embedded. That merge is out
//            of scope here; this endpoint's contract is just "accept and durably
//            remember the blob, respond success" — sufficient for a client to
//            complete its upload flow without erroring.
//   input:  JSON body {user_id: {key_or_device_id: {...}}}
//           Authorization: Bearer <signed mxt_ token> (uploader's own identity;
//           the outer user_id in the body is used as the storage key, not the
//           caller's token identity — a verified client may upload signatures
//           it computed over another user's master key)
//   output: JSON {"failures": {}}
//           401 M_UNKNOWN_TOKEN on missing/invalid token
//   sideEffects: writes to state.e2ee.cross_signatures (node-local, in-memory only)
// post_signatures_upload:end
pub async fn post_signatures_upload(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Option<Json<Value>>,
) -> Result<Json<Value>, HsError> {
    let (_caller_user_id, _caller_device_id) = extract_caller(&headers, &state)
        .ok_or_else(|| HsError::UnknownToken("missing or invalid token".to_string()))?;
    let body = body.map(|b| b.0).unwrap_or_else(|| json!({}));

    if let Some(outer) = body.as_object() {
        for (target_user_id, per_key) in outer {
            let Some(per_key_map) = per_key.as_object() else {
                continue;
            };
            for (key_id, blob) in per_key_map {
                // Keep the raw blob durably (legacy opaque store).
                state
                    .store_cross_signature(target_user_id, key_id, blob.clone())
                    .map_err(HsError::Internal)?;
                // AND fold the blob's signatures into the stored device_keys /
                // cross_signing_keys object so keys/query returns them embedded
                // (master key device signature etc.) — completes cross-signing
                // bootstrap so clients' crypto identity can actually verify.
                if let Some(sigs) = blob.get("signatures").and_then(|v| v.as_object()) {
                    state
                        .merge_signature(target_user_id, key_id, sigs)
                        .map_err(HsError::Internal)?;
                }
            }
        }
    }

    Ok(Json(json!({ "failures": {} })))
}

// try_claim_local:start
//   purpose: Atomically pop one OTK for (user_id, device_id, algorithm) if this
//            node owns that device's keys. THE single call site that ever pops
//            a key — shared by the local HTTP fast path (post_keys_claim, below)
//            and the cross-node queryable handler (main.rs) — so exactly-once
//            holds cross-node the same way it always held locally: there is
//            exactly one function that can remove a key, not two independently
//            written copies that could subtly diverge.
//   input:  state, user_id, device_id, algorithm
//   output: Some((key_id, key_value)) if owned locally and a matching-algorithm
//           key was found (and popped); None if not owned locally, or owned but
//           no matching key remains
//   sideEffects: removes one key from state.e2ee.device_otks (pop)
// try_claim_local:end
pub fn try_claim_local(
    state: &Arc<AppState>,
    user_id: &str,
    device_id: &str,
    algorithm: &str,
) -> Option<(String, Value)> {
    let mut guard = state.e2ee.device_otks.lock().ok()?;
    let slot = guard.get_mut(&(user_id.to_string(), device_id.to_string()))?;
    let prefix = format!("{algorithm}:");
    let key_id = slot.keys().find(|k| k.starts_with(&prefix)).cloned()?;
    let key_val = slot.remove(&key_id)?;
    Some((key_id, key_val))
}

// serve_otk_claims:start
//   purpose: Declare this node's "<prefix>/keys/claim/**" queryable and spawn
//            the handler loop that answers it via try_claim_local — the
//            cross-node half of the OWNERSHIP-PARTITION claim barrier.
//            Extracted out of main.rs (rather than left inline like the
//            history/state/media queryables) specifically so a lib test can
//            call it directly: main.rs's build_state, where those three
//            live, is in the BINARY target, unreachable from a test compiled
//            as part of this library crate.
//   input:  state — Arc<AppState> with cluster configured (a ClusterConfig
//           already passed to AppState::with_cluster)
//   output: Result<(), String> — Err if this node has no cluster configured,
//           or the queryable failed to declare
//   sideEffects: declares one Zenoh queryable (leaked for process lifetime,
//                matching main.rs's other queryables); spawns one background task
// serve_otk_claims:end
#[cfg(feature = "cluster")]
pub async fn serve_otk_claims(state: Arc<AppState>) -> Result<(), String> {
    let cluster = state
        .cluster
        .clone()
        .ok_or_else(|| "serve_otk_claims: cluster not configured".to_string())?;
    let claim_wild = format!("{}/keys/claim/**", cluster.key_prefix());
    let qable = cluster
        .session()
        .declare_queryable(&claim_wild)
        .allowed_origin(zenoh::sample::Locality::Remote)
        .await
        .map_err(|e| e.to_string())?;
    let handler = qable.handler().clone();
    let prefix = cluster.key_prefix().to_string();
    let state_clone = state.clone();
    tokio::spawn(async move {
        while let Ok(query) = handler.recv_async().await {
            let qkey = query.key_expr().as_str().to_string();
            let Some((user_id, device_id, algorithm)) =
                crate::state::ClusterState::claim_from_key(&qkey, &prefix)
            else {
                continue;
            };
            // Stay SILENT on a miss — same reason the media queryable does:
            // an empty reply would be indistinguishable from a real one, and
            // every node would send one for every miss.
            let Some((key_id, key_value)) =
                try_claim_local(&state_clone, &user_id, &device_id, &algorithm)
            else {
                continue;
            };
            let payload = crate::state::ClusterState::encode_claim_reply(&key_id, &key_value);
            let reply_key = crate::state::ClusterState::claim_key(&prefix, &user_id, &device_id, &algorithm);
            let _ = query.reply(&reply_key, payload).await;
        }
    });
    Box::leak(Box::new(qable));
    Ok(())
}

// insert_claim:start
//   purpose: Fold one claimed (key_id, key_value) into the {user: {device:
//            {"alg:key_id": key}}} response shape, shared by both the local
//            pass and the cross-node pass in post_keys_claim.
//   input:  outer — the response map being built; user_id, device_id, key_id, key_value
//   output: none
//   sideEffects: mutates `outer`
// insert_claim:end
fn insert_claim(
    outer: &mut serde_json::Map<String, Value>,
    user_id: &str,
    device_id: &str,
    key_id: String,
    key_value: Value,
) {
    let user_entry = outer
        .entry(user_id.to_string())
        .or_insert_with(|| Value::Object(serde_json::Map::new()));
    if let Some(user_map) = user_entry.as_object_mut() {
        let mut device_result = serde_json::Map::new();
        device_result.insert(key_id, key_value);
        user_map.insert(device_id.to_string(), Value::Object(device_result));
    }
}

// CLAIM_FETCH_TIMEOUT_MS:start
//   purpose: How long a claim may wait on a single cross-node OTK fetch.
//            Shorter than media's fetch budget (media.rs's 5000 ms default) —
//            an OTK reply is a few bytes, not a file, so there is no transfer
//            time to budget for, only mesh round-trip latency.
//   input:  none
//   output: u64 milliseconds
//   sideEffects: none
// CLAIM_FETCH_TIMEOUT_MS:end
#[cfg(feature = "cluster")]
const CLAIM_FETCH_TIMEOUT_MS: u64 = 800;

// post_keys_claim:start
//   purpose: POST /_matrix/client/v3/keys/claim — claim one OTK per requested
//            (user_id, device_id, algorithm). Local pass first (try_claim_local,
//            the CLAIM-EXACTLY-ONCE barrier: the pop is Mutex-protected so the
//            same key is never returned twice, even under concurrent claims);
//            anything not owned locally is then routed to its owner over the
//            mesh (see module header), one concurrent task per pending item.
//
//            Response shape: {"one_time_keys": {user_id: {device_id: {"alg:key_id": key}}}}
//            Key absent in response means no OTK was available anywhere (client must escalate).
//   input:  JSON body {"one_time_keys": {user_id: {device_id: "algorithm"}}}
//           (no auth required for claim — public operation in Matrix spec)
//   output: JSON {"one_time_keys": {user_id: {device_id: {"alg:key_id": key_value}}}}
//           (absent entries mean no OTK available for that (user,device,algorithm))
//   sideEffects: removes one key per (user,device,algorithm) from device_otks —
//                locally, or (cluster feature) on whichever node owns it
// post_keys_claim:end
pub async fn post_keys_claim(
    State(state): State<Arc<AppState>>,
    body: Option<Json<Value>>,
) -> Json<Value> {
    let body = body.map(|b| b.0).unwrap_or_else(|| json!({}));

    // result shape: {user_id: {device_id: {"alg:key_id": key}}}
    let mut outer: serde_json::Map<String, Value> = serde_json::Map::new();

    let requested = match body.get("one_time_keys").and_then(|v| v.as_object()) {
        Some(m) => m.clone(),
        None => return Json(json!({ "one_time_keys": {} })),
    };

    #[cfg_attr(not(feature = "cluster"), allow(unused_mut, unused_variables))]
    let mut pending: Vec<(String, String, String)> = Vec::new();

    for (user_id, devices_val) in &requested {
        let Some(devices) = devices_val.as_object() else {
            continue;
        };
        for (device_id, alg_val) in devices {
            let Some(algorithm) = alg_val.as_str() else {
                continue;
            };
            match try_claim_local(&state, user_id, device_id, algorithm) {
                Some((key_id, key_val)) => insert_claim(&mut outer, user_id, device_id, key_id, key_val),
                #[cfg(feature = "cluster")]
                None => pending.push((user_id.clone(), device_id.clone(), algorithm.to_string())),
                #[cfg(not(feature = "cluster"))]
                None => {}
            }
        }
    }

    #[cfg(feature = "cluster")]
    if !pending.is_empty() {
        if let Some(cluster) = state.cluster.clone() {
            let timeout = std::time::Duration::from_millis(CLAIM_FETCH_TIMEOUT_MS);
            let tasks: Vec<_> = pending
                .into_iter()
                .map(|(user_id, device_id, algorithm)| {
                    let cluster = cluster.clone();
                    tokio::spawn(async move {
                        let result = cluster.fetch_otk(&user_id, &device_id, &algorithm, timeout).await;
                        (user_id, device_id, result)
                    })
                })
                .collect();
            for task in tasks {
                if let Ok((user_id, device_id, Some((key_id, key_val)))) = task.await {
                    insert_claim(&mut outer, &user_id, &device_id, key_id, key_val);
                }
            }
        }
    }

    Json(json!({ "one_time_keys": Value::Object(outer) }))
}
