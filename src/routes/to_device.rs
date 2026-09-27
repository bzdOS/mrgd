// START_AI_HEADER
// MODULE: matrix-hs/src/routes/to_device.rs
// PURPOSE: PUT /_matrix/client/v3/sendToDevice/{eventType}/{txnId} — the real to-device
//          relay (replaces the old routes::stubs::put_send_to_device no-op). This is the
//          E2EE keystone: without it, Megolm session-key exchange (and any m.*.key_request
//          / m.*.forwarded_room_key / verification traffic) can never reach the target
//          device, so encrypted rooms never decrypt.
//
//          Delivery model — mirrors the OWNERSHIP-PARTITION pattern documented in
//          routes/keys.rs (per-device OTK ownership + exactly-once pop), adapted for
//          to-device because devices have no natural "upload owner": a device may sync
//          from ANY node in the cluster, and the sender has no way to know which one.
//          So instead of routing to a single owner, this uses STORE-AND-FORWARD GOSSIP,
//          the same transport primitive keys/claim's module header describes for its
//          (deferred) cross-node extension and that ZenohCrdtSink already uses live for
//          room-event replication (mrgd/src/crdt.rs, wired via
//          state::ClusterState::sink_for):
//            1. The sending node enqueues the message directly into its OWN
//               AppState::to_device_queue (local delivery — covers the common case where
//               sender and target device share a node, and matches every existing test's
//               single-process-per-node setup).
//            2. (cluster feature) The sending node ALSO publishes the message over a
//               dedicated Zenoh pub/sub channel — ClusterState::sink_for("__to_device__")
//               — so every OTHER node's background subscriber receives it and enqueues it
//               into ITS local to_device_queue too. Whichever node the target device is
//               actually syncing from will have it locally by the time that device's next
//               /sync runs (routes::sync::drain_to_device_gossip / sliding_sync same).
//          This is full gossip (every node ends up with a copy), not routed unicast to a
//          single owner — deliberately simpler than a queryable-based owner lookup because
//          there is no ownership assignment to look up. At-least-once across nodes is
//          Matrix-acceptable (explicitly allowed by the spec); AppState::enqueue_to_device's
//          msg_id dedup keeps it exactly-once PER NODE (a node's own gossip loop-back, or a
//          redelivered gossip sample, is a no-op).
//
//          Since-token semantics: see AppState::drain_to_device — messages are visible on
//          the sync/sliding-sync call AFTER they were sent, and are removed once the
//          client's since token proves it already received them.
//
// DEPENDENCIES: axum, serde_json, AppState, auth, (cluster) crate::substrate::crdt::CrdtSink, zenoh
// PUBLIC_API: put_send_to_device, (cluster) drain_to_device_gossip
// END_AI_HEADER

use axum::{
    extract::{Path, State},
    http::HeaderMap,
    Json,
};
use serde_json::{json, Value};
use std::sync::Arc;

use crate::{auth, error::HsError, state::AppState};

// ── Auth helper ────────────────────────────────────────────────────────────────
//
// extract_caller now lives in auth.rs (canonical implementation shared across route
// modules). Derives (user_id, device_id) from the signed Bearer token —
// auth::verify_token, only signed mxt_ tokens authenticate; no anonymous fallback.
use auth::extract_caller;

// wildcard_devices_for_user:start
//   purpose: Resolve the device_id list for a "*" wildcard target (Matrix allows
//            device_id "*" meaning "all of this recipient's devices"). This server models
//            one device per registered user (see UserRecord::device_id), so the wildcard
//            expands to that single device when the user is known locally; unknown users
//            expand to nothing (best-effort — cross-node user directories are out of scope,
//            same posture as keys.rs's OWNERSHIP-PARTITION notes).
//   input:  state — Arc<AppState>; target_user — full MXID
//   output: Vec<String> of device_ids (0 or 1 elements in the current single-device model)
//   sideEffects: none (read-only)
// wildcard_devices_for_user:end
fn wildcard_devices_for_user(state: &AppState, target_user: &str) -> Vec<String> {
    let localpart = target_user
        .strip_prefix('@')
        .and_then(|s| s.split(':').next())
        .unwrap_or(target_user);

    state
        .users
        .lock()
        .ok()
        .and_then(|u| u.get(localpart).map(|r| vec![r.device_id.clone()]))
        .unwrap_or_default()
}

// put_send_to_device:start
//   purpose: PUT /_matrix/client/v3/sendToDevice/{eventType}/{txnId} — deliver a batch of
//            to-device messages. Requires a valid signed Bearer token (mxt_); unknown/
//            missing token → 401. For every (target_user, target_device, content) triple in
//            the request body's "messages" map, enqueues the message into
//            state.to_device.to_device_queue (visible on the target's NEXT /sync) and — when the
//            cluster feature is enabled and a cluster is active — also broadcasts it over
//            Zenoh gossip so a node other than this one, if that's where the target device
//            is actually syncing from, receives it too (see module header).
//            device_id "*" expands to all of the target user's known devices (best-effort,
//            single-device model — see wildcard_devices_for_user).
//            txn_id is part of the per-recipient dedup key so client retries (same txn_id)
//            do not double-deliver, matching Matrix's txn_id idempotency contract.
//   input:  Path(event_type, txn_id); Authorization: Bearer <signed mxt_ token>;
//           JSON body {"messages": {user_id: {device_id: content, ...}, ...}}
//   output: JSON {} (200) on success; 401 M_UNKNOWN_TOKEN on missing/invalid token
//   sideEffects: writes to state.to_device.to_device_queue (+ to_device_seen); wakes /sync long-poll
//                waiters; (cluster) publishes one Zenoh sample per recipient
// put_send_to_device:end
pub async fn put_send_to_device(
    State(state): State<Arc<AppState>>,
    Path((event_type, txn_id)): Path<(String, String)>,
    headers: HeaderMap,
    body: Option<Json<Value>>,
) -> Result<Json<Value>, HsError> {
    let (sender_user_id, _sender_device_id) = extract_caller(&headers, &state)
        .ok_or_else(|| HsError::UnknownToken("missing or invalid token".to_string()))?;

    let body = body.map(|b| b.0).unwrap_or_else(|| json!({}));
    let messages = body.get("messages").and_then(|v| v.as_object());

    let Some(messages) = messages else {
        // No "messages" field — nothing to deliver, but still a valid (empty) call.
        return Ok(Json(json!({})));
    };

    for (target_user, devices_val) in messages {
        let Some(devices) = devices_val.as_object() else {
            continue;
        };

        for (target_device, content) in devices {
            let resolved_devices: Vec<String> = if target_device == "*" {
                wildcard_devices_for_user(&state, target_user)
            } else {
                vec![target_device.clone()]
            };

            for device_id in resolved_devices {
                let msg_id =
                    format!("{sender_user_id}:{event_type}:{txn_id}:{target_user}:{device_id}");

                state
                    .enqueue_to_device(
                        target_user,
                        &device_id,
                        &sender_user_id,
                        &event_type,
                        content,
                        &msg_id,
                    )
                    .map_err(HsError::Internal)?;

                #[cfg(feature = "cluster")]
                publish_to_device_gossip(
                    &state,
                    target_user,
                    &device_id,
                    &sender_user_id,
                    &event_type,
                    content,
                    &msg_id,
                )
                .await?;
            }
        }
    }

    Ok(Json(json!({})))
}

// ── Cross-node gossip (cluster feature only) ─────────────────────────────────

// ToDeviceGossipMsg:start
//   purpose: Wire format for one to-device message broadcast over Zenoh gossip.
//            Serialised with serde_json (already a dependency everywhere in this crate;
//            no new crates) — this is a control-plane message, not a hot loop, so JSON's
//            overhead is irrelevant here.
//   input:  constructed by publish_to_device_gossip
//   output: round-trips through serde_json::to_vec / from_slice
//   sideEffects: none
// ToDeviceGossipMsg:end
#[cfg(feature = "cluster")]
#[derive(serde::Serialize, serde::Deserialize)]
struct ToDeviceGossipMsg {
    target_user: String,
    target_device: String,
    sender: String,
    event_type: String,
    content: Value,
    msg_id: String,
}

// to_device_gossip_room:start
//   purpose: Pseudo "room_id" under which the to-device gossip channel is scoped inside
//            ClusterState's per-room ZenohCrdtSink registry (state.rs). Reusing sink_for
//            avoids inventing a second Zenoh session-management path — to-device gossip
//            is just another CRDT-style broadcast channel, scoped under this fixed key
//            instead of a real room_id.
//   input:  none
//   output: &'static str
//   sideEffects: none
// to_device_gossip_room:end
#[cfg(feature = "cluster")]
fn to_device_gossip_room() -> &'static str {
    "__to_device__"
}

// to_device_gossip_key:start
//   purpose: The CRDT routing key used inside the to-device gossip sink's prefix (mirrors
//            ClusterState::crdt_key's fixed "events" key for rooms).
//   input:  none
//   output: &'static str
//   sideEffects: none
// to_device_gossip_key:end
#[cfg(feature = "cluster")]
fn to_device_gossip_key() -> &'static str {
    "msgs"
}

// publish_to_device_gossip:start
//   purpose: Broadcast one to-device message to every other node's to_device_queue via
//            Zenoh pub/sub (ClusterState::sink_for("__to_device__")). Best-effort: if no
//            cluster is active (single-node mode), this is a no-op — the message already
//            landed in the local queue via put_send_to_device's direct enqueue_to_device
//            call, so single-node delivery is unaffected.
//   input:  state — Arc<AppState>; target_user, target_device, sender, event_type, content,
//           msg_id — same fields as enqueue_to_device
//   output: Result<(), HsError>
//   sideEffects: (cluster active) opens/reuses the "__to_device__" ZenohCrdtSink; publishes
//                one Zenoh sample; every peer's background subscriber receives it and (via
//                drain_to_device_gossip on their next /sync) enqueues it locally
// publish_to_device_gossip:end
#[cfg(feature = "cluster")]
async fn publish_to_device_gossip(
    state: &Arc<AppState>,
    target_user: &str,
    target_device: &str,
    sender: &str,
    event_type: &str,
    content: &Value,
    msg_id: &str,
) -> Result<(), HsError> {
    use crate::substrate::crdt::CrdtSink as _;

    let Some(cluster) = &state.cluster else {
        return Ok(()); // single-node mode — local enqueue already happened
    };

    let msg = ToDeviceGossipMsg {
        target_user: target_user.to_string(),
        target_device: target_device.to_string(),
        sender: sender.to_string(),
        event_type: event_type.to_string(),
        content: content.clone(),
        msg_id: msg_id.to_string(),
    };
    let bytes = serde_json::to_vec(&msg).map_err(|e| HsError::Internal(e.to_string()))?;

    let sink = cluster
        .sink_for(to_device_gossip_room())
        .await
        .map_err(HsError::Internal)?;

    sink.publish(to_device_gossip_key(), bytes)
        .map_err(|e| HsError::Internal(e.to_string()))?;

    Ok(())
}

// drain_to_device_gossip:start
//   purpose: Drain any pending to-device gossip samples received from OTHER nodes and
//            enqueue them into this node's local to_device_queue. Called from
//            routes::sync::get_sync and routes::sliding_sync::post_sliding_sync on every
//            request (mirrors routes::sync::drain_cluster_deltas for room events) so a
//            message that arrived here via gossip — because THIS is the node the target
//            device is actually syncing from — is visible before the response is built.
//            Idempotent: AppState::enqueue_to_device's msg_id dedup means redraining an
//            already-applied sample (e.g. after a later restart with no persistence) is
//            safe — it is a no-op, not a duplicate delivery, as long as to_device_seen
//            still holds the msg_id (in-memory only — see residual note below).
//            RESIDUAL: to_device_seen and to_device_queue are in-memory only (no
//            persistence sidecar, unlike room PDUs) — a node restart loses in-flight
//            to-device state for messages it had not yet delivered. Acceptable for this
//            milestone: Matrix to-device delivery is already at-least-once/best-effort by
//            spec, and this mirrors the existing "pure in-memory unless MATRIX_HS_DATA_DIR"
//            posture the rest of AppState uses for non-durable substructures.
//   input:  state — Arc<AppState>
//   output: Result<(), HsError>
//   sideEffects: (cluster active) drains the "__to_device__" ZenohCrdtSink inbox; mutates
//                to_device_queue + to_device_seen via enqueue_to_device; may call
//                state.notify.notify_waiters() (via enqueue_to_device)
// drain_to_device_gossip:end
#[cfg(feature = "cluster")]
pub(crate) async fn drain_to_device_gossip(state: &Arc<AppState>) -> Result<(), HsError> {
    use crate::substrate::crdt::CrdtSink as _;

    let Some(cluster) = &state.cluster else {
        return Ok(());
    };

    let sink = cluster
        .sink_for(to_device_gossip_room())
        .await
        .map_err(HsError::Internal)?;

    let blobs = sink
        .drain(to_device_gossip_key())
        .map_err(|e| HsError::Internal(e.to_string()))?;

    for bytes in blobs {
        let msg: ToDeviceGossipMsg = match serde_json::from_slice(&bytes) {
            Ok(m) => m,
            Err(_) => continue, // malformed sample — skip, never panic on network input
        };
        state
            .enqueue_to_device(
                &msg.target_user,
                &msg.target_device,
                &msg.sender,
                &msg.event_type,
                &msg.content,
                &msg.msg_id,
            )
            .map_err(HsError::Internal)?;
    }

    Ok(())
}
