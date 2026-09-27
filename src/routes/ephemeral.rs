// START_AI_HEADER
// MODULE: matrix-hs/src/routes/ephemeral.rs
// PURPOSE: Ephemeral EDU endpoints — typing indicators, read receipts, read markers.
//            PUT  /_matrix/client/v3/rooms/{roomId}/typing/{userId}
//            POST /_matrix/client/v3/rooms/{roomId}/receipt/{receiptType}/{eventId}
//            POST /_matrix/client/v3/rooms/{roomId}/read_markers
//          These replace the former no-op stubs in routes/stubs.rs.  Local state
//          lives on AppState (see state.rs "Ephemeral EDUs" section); this module
//          only does request parsing/auth/validation and (cluster feature) publishes
//          updates to remote peers over the SAME per-room ZenohCrdtSink used by
//          routes/send.rs — no new Zenoh session/subscriber is opened, just two new
//          routing keys ("typing", "receipt") published/drained under the existing
//          per-room sink (see crate::substrate::crdt::CrdtSink — publish/drain are keyed by an
//          arbitrary string, the sink already subscribes to "<room prefix>/**").
//
//          Delivery to clients happens via GET /sync and sliding-sync, which call
//          AppState::typing_user_ids / receipt_event_content to build the room's
//          "ephemeral" block (see routes/sync.rs, routes/sliding_sync.rs) and
//          drain_cluster_ephemeral (below) to pull in remote updates first.
//
//          Scope (see state.rs design notes for the full rationale):
//            - m.typing: expires automatically (timeout-based, lazy expiry at read).
//            - m.receipt: m.read and m.read.private, keyed by (room,user,type),
//              last-writer-wins on ts — safe to merge local + remote without an
//              ordering guarantee.
//            - read_markers: m.fully_read is recorded but NODE-LOCAL ONLY (not
//              replicated across the cluster) — accepted without error, per the
//              minimal scope requested.  m.read / m.read.private included in the
//              same request ARE replicated (they go through set_receipt same as
//              post_receipt).
// DEPENDENCIES: axum, serde_json, AppState, auth, (cluster) crate::substrate::crdt::CrdtSink
// PUBLIC_API: put_typing, post_receipt, post_read_markers, drain_cluster_ephemeral
// END_AI_HEADER

use axum::{
    extract::{Path, State},
    http::HeaderMap,
    Json,
};
use serde_json::{json, Value};
use std::sync::Arc;

use crate::{error::HsError, routes::keys::extract_caller, state::AppState};

// ── Request body shapes ───────────────────────────────────────────────────────

// parse_typing_body:start
//   purpose: Parse the PUT /typing request body into (typing, timeout_ms).
//            typing defaults to false, timeout_ms defaults to 30000 (Matrix spec's
//            conventional default) when absent/malformed.
//   input:  body — optional JSON value
//   output: (bool, u64)
//   sideEffects: none
// parse_typing_body:end
fn parse_typing_body(body: &Value) -> (bool, u64) {
    let typing = body
        .get("typing")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let timeout_ms = body
        .get("timeout")
        .and_then(|v| v.as_u64())
        .unwrap_or(30_000);
    (typing, timeout_ms)
}

// require_room:start
//   purpose: Resolve a room_id-or-alias to a canonical room_id and verify the room
//            exists (404 M_NOT_FOUND if not) before recording any ephemeral state
//            against it.
//   input:  state, id_or_alias
//   output: Result<String, HsError> — canonical room_id
//   sideEffects: none (read-only lock of state.rooms / state.aliases)
// require_room:end
fn require_room(state: &Arc<AppState>, id_or_alias: &str) -> Result<String, HsError> {
    let room_id = state.resolve_room_id(id_or_alias);
    let exists = state
        .rooms
        .lock()
        .map_err(|e| HsError::Internal(e.to_string()))?
        .contains_key(&room_id);
    if !exists {
        return Err(HsError::RoomNotFound(room_id));
    }
    Ok(room_id)
}

// put_typing:start
//   purpose: PUT /rooms/{roomId}/typing/{userId} — start/stop this user's typing
//            indicator in the room.  Requires a valid signed Bearer token whose
//            user_id matches the {userId} path param (a client may only set its
//            OWN typing state — 403 M_FORBIDDEN otherwise).
//            Body: {"typing": bool, "timeout": ms}.  timeout is only meaningful
//            when typing=true; ignored (but harmless) when typing=false.
//   input:  room_id, user_id path params; Authorization: Bearer <mxt_ token>;
//           optional JSON body
//   output: JSON {} on success
//           404 M_NOT_FOUND if the room does not exist
//           401 M_UNKNOWN_TOKEN if the token is missing/invalid
//           403 M_FORBIDDEN if userId does not match the caller
//   sideEffects: mutates state.ephemeral.typing[room_id]; (cluster) publishes a typing
//                snapshot to the room's ZenohCrdtSink; wakes local /sync long-pollers
// put_typing:end
pub async fn put_typing(
    State(state): State<Arc<AppState>>,
    Path((room_id, user_id)): Path<(String, String)>,
    headers: HeaderMap,
    body: Option<Json<Value>>,
) -> Result<Json<Value>, HsError> {
    let (caller_user_id, _device_id) = extract_caller(&headers, &state)
        .ok_or_else(|| HsError::UnknownToken("missing or invalid token".to_string()))?;
    if caller_user_id != user_id {
        return Err(HsError::Forbidden(
            "cannot set another user's typing state".to_string(),
        ));
    }

    let room_id = require_room(&state, &room_id)?;
    let body = body.map(|b| b.0).unwrap_or_else(|| json!({}));
    let (typing, timeout_ms) = parse_typing_body(&body);

    state
        .set_typing(&room_id, &user_id, typing, timeout_ms)
        .map_err(HsError::Internal)?;

    #[cfg(feature = "cluster")]
    publish_typing(&state, &room_id).await?;

    state.notify.notify_waiters();
    Ok(Json(json!({})))
}

// post_receipt:start
//   purpose: POST /rooms/{roomId}/receipt/{receiptType}/{eventId} — record a read
//            receipt.  Accepts any receipt_type (m.read, m.read.private, and any
//            future type) — unknown types are stored the same way; Element only
//            ever sends m.read / m.read.private.
//   input:  room_id, receipt_type, event_id path params;
//           Authorization: Bearer <mxt_ token>; optional JSON body (ignored —
//           the CS-API spec's receipt body has no required fields for these types)
//   output: JSON {} on success
//           404 M_NOT_FOUND if the room does not exist
//           401 M_UNKNOWN_TOKEN if the token is missing/invalid
//   sideEffects: mutates state.ephemeral.receipts[room_id]; (cluster) publishes the receipt
//                update to the room's ZenohCrdtSink; wakes local /sync long-pollers
// post_receipt:end
pub async fn post_receipt(
    State(state): State<Arc<AppState>>,
    Path((room_id, receipt_type, event_id)): Path<(String, String, String)>,
    headers: HeaderMap,
    _body: Option<Json<Value>>,
) -> Result<Json<Value>, HsError> {
    let (user_id, _device_id) = extract_caller(&headers, &state)
        .ok_or_else(|| HsError::UnknownToken("missing or invalid token".to_string()))?;

    let room_id = require_room(&state, &room_id)?;
    let ts_ms = crate::state::now_ms();

    state
        .set_receipt(&room_id, &user_id, &receipt_type, &event_id, ts_ms)
        .map_err(HsError::Internal)?;

    #[cfg(feature = "cluster")]
    publish_receipt(&state, &room_id, &user_id, &receipt_type, &event_id, ts_ms).await?;

    state.notify.notify_waiters();
    Ok(Json(json!({})))
}

// post_read_markers:start
//   purpose: POST /rooms/{roomId}/read_markers — set the fully-read marker and
//            (optionally, in the same call) an m.read / m.read.private receipt.
//            Body: {"m.fully_read": event_id, "m.read"?: event_id,
//                    "m.read.private"?: event_id} — all fields optional; any
//            combination is accepted.  m.fully_read is node-local only (see
//            module header); the m.read(.private) fields go through the same
//            set_receipt path as post_receipt, so they DO replicate.
//   input:  room_id path param; Authorization: Bearer <mxt_ token>; JSON body
//   output: JSON {} on success
//           404 M_NOT_FOUND if the room does not exist
//           401 M_UNKNOWN_TOKEN if the token is missing/invalid
//   sideEffects: mutates state.ephemeral.fully_read and (if present) state.ephemeral.receipts;
//                (cluster) publishes any receipt fields present; wakes local
//                /sync long-pollers
// post_read_markers:end
pub async fn post_read_markers(
    State(state): State<Arc<AppState>>,
    Path(room_id): Path<String>,
    headers: HeaderMap,
    body: Option<Json<Value>>,
) -> Result<Json<Value>, HsError> {
    let (user_id, _device_id) = extract_caller(&headers, &state)
        .ok_or_else(|| HsError::UnknownToken("missing or invalid token".to_string()))?;

    let room_id = require_room(&state, &room_id)?;
    let body = body.map(|b| b.0).unwrap_or_else(|| json!({}));
    let ts_ms = crate::state::now_ms();

    if let Some(event_id) = body.get("m.fully_read").and_then(|v| v.as_str()) {
        state
            .set_fully_read(&room_id, &user_id, event_id)
            .map_err(HsError::Internal)?;
    }

    for receipt_type in ["m.read", "m.read.private"] {
        if let Some(event_id) = body.get(receipt_type).and_then(|v| v.as_str()) {
            state
                .set_receipt(&room_id, &user_id, receipt_type, event_id, ts_ms)
                .map_err(HsError::Internal)?;
            #[cfg(feature = "cluster")]
            publish_receipt(&state, &room_id, &user_id, receipt_type, event_id, ts_ms).await?;
        }
    }

    state.notify.notify_waiters();
    Ok(Json(json!({})))
}

// ── Cluster replication (feature = "cluster") ─────────────────────────────────

// publish_typing:start
//   purpose: Publish this node's current local typing snapshot for `room_id` to
//            the room's ZenohCrdtSink under the "typing" key.  The payload carries
//            this node's server_name so remote nodes can bucket it separately
//            (see AppState::merge_typing_remote / typing_remote design note) —
//            a later publish that omits a user IS the "stopped typing" signal.
//   input:  state, room_id
//   output: Result<(), HsError>
//   sideEffects: opens the room's Zenoh sink lazily (first call per room);
//                publishes one message to the Zenoh network
// publish_typing:end
#[cfg(feature = "cluster")]
async fn publish_typing(state: &Arc<AppState>, room_id: &str) -> Result<(), HsError> {
    use crate::substrate::crdt::CrdtSink as _;

    let cluster = match &state.cluster {
        Some(c) => c,
        None => return Ok(()),
    };

    let snapshot = state.typing_snapshot_local(room_id);
    let payload = json!({ "node_id": state.server_name, "users": snapshot });
    let bytes = serde_json::to_vec(&payload).map_err(|e| HsError::Internal(e.to_string()))?;

    let sink = cluster.sink_for(room_id).await.map_err(HsError::Internal)?;
    sink.publish("typing", bytes)
        .map_err(|e| HsError::Internal(e.to_string()))?;
    Ok(())
}

// publish_receipt:start
//   purpose: Publish a single receipt update to the room's ZenohCrdtSink under the
//            "receipt" key.  Remote nodes apply it via the same last-writer-wins
//            AppState::set_receipt used locally (see drain_cluster_ephemeral).
//   input:  state, room_id, user_id, receipt_type, event_id, ts_ms
//   output: Result<(), HsError>
//   sideEffects: opens the room's Zenoh sink lazily; publishes one message
// publish_receipt:end
#[cfg(feature = "cluster")]
async fn publish_receipt(
    state: &Arc<AppState>,
    room_id: &str,
    user_id: &str,
    receipt_type: &str,
    event_id: &str,
    ts_ms: u64,
) -> Result<(), HsError> {
    use crate::substrate::crdt::CrdtSink as _;

    let cluster = match &state.cluster {
        Some(c) => c,
        None => return Ok(()),
    };

    let payload = json!({
        "user_id":      user_id,
        "receipt_type": receipt_type,
        "event_id":     event_id,
        "ts":           ts_ms,
    });
    let bytes = serde_json::to_vec(&payload).map_err(|e| HsError::Internal(e.to_string()))?;

    let sink = cluster.sink_for(room_id).await.map_err(HsError::Internal)?;
    sink.publish("receipt", bytes)
        .map_err(|e| HsError::Internal(e.to_string()))?;
    Ok(())
}

// drain_cluster_ephemeral:start
//   purpose: For every room currently known locally, drain pending "typing" and
//            "receipt" blobs from its ZenohCrdtSink and merge them into AppState
//            (typing_remote bucket replace / set_receipt last-writer-wins).
//            Called from routes/sync.rs and routes/sliding_sync.rs alongside
//            drain_cluster_deltas, so ephemeral state converges on the same
//            cadence as the room timeline.
//            Malformed blobs (should not happen — only this module ever publishes
//            on these keys) are skipped rather than treated as a hard error, so a
//            single bad message cannot take down a live /sync call.
//   input:  state — Arc<AppState> with cluster layer active
//   output: Result<(), HsError>
//   sideEffects: may mutate state.ephemeral.typing_remote / state.ephemeral.receipts; acquires
//                state.rooms lock briefly to enumerate room_ids
// drain_cluster_ephemeral:end
#[cfg(feature = "cluster")]
pub(crate) async fn drain_cluster_ephemeral(state: &Arc<AppState>) -> Result<(), HsError> {
    use crate::substrate::crdt::CrdtSink as _;

    let cluster = match &state.cluster {
        Some(c) => c,
        None => return Ok(()),
    };

    let room_ids: Vec<String> = {
        let guard = state
            .rooms
            .lock()
            .map_err(|e| HsError::Internal(e.to_string()))?;
        guard.keys().cloned().collect()
    };

    for room_id in &room_ids {
        let sink = cluster.sink_for(room_id).await.map_err(HsError::Internal)?;

        let typing_blobs = sink
            .drain("typing")
            .map_err(|e| HsError::Internal(e.to_string()))?;
        for bytes in typing_blobs {
            let Ok(v) = serde_json::from_slice::<Value>(&bytes) else {
                continue;
            };
            let Some(node_id) = v.get("node_id").and_then(|n| n.as_str()) else {
                continue;
            };
            let Some(users_v) = v.get("users").and_then(|u| u.as_object()) else {
                continue;
            };
            let users: std::collections::HashMap<String, u64> = users_v
                .iter()
                .filter_map(|(k, val)| val.as_u64().map(|n| (k.clone(), n)))
                .collect();
            state
                .merge_typing_remote(room_id, node_id, users)
                .map_err(HsError::Internal)?;
        }

        let receipt_blobs = sink
            .drain("receipt")
            .map_err(|e| HsError::Internal(e.to_string()))?;
        for bytes in receipt_blobs {
            let Ok(v) = serde_json::from_slice::<Value>(&bytes) else {
                continue;
            };
            let (Some(user_id), Some(receipt_type), Some(event_id), Some(ts)) = (
                v.get("user_id").and_then(|x| x.as_str()),
                v.get("receipt_type").and_then(|x| x.as_str()),
                v.get("event_id").and_then(|x| x.as_str()),
                v.get("ts").and_then(|x| x.as_u64()),
            ) else {
                continue;
            };
            state
                .set_receipt(room_id, user_id, receipt_type, event_id, ts)
                .map_err(HsError::Internal)?;
        }
    }

    Ok(())
}
