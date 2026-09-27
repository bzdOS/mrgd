// START_AI_HEADER
// MODULE: matrix-hs/src/routes/redact.rs
// PURPOSE: PUT /_matrix/client/v3/rooms/{roomId}/redact/{eventId}/{txnId}
//          Creates an m.room.redaction event (redacts: eventId) and appends it to
//          the room timeline via the SAME insert_pdu path routes/send.rs uses for
//          ordinary messages — so a redaction replicates like any other event
//          (RoomLog add, cluster delta publish, persistence, push fan-out).
//          Separately records the redaction in AppState.redactions so every
//          timeline-read path (routes/sync.rs, routes/room_state.rs, routes/
//          sliding_sync.rs) can mask the target event's content at serve time via
//          AppState::apply_redaction — see that method's contract for the exact
//          masking shape (content -> {}, unsigned.redacted_because added).
//
//          WHERE THE TARGET LIVES ON THE WIRE:
//            `redacts` is written into the event CONTENT as well as at the top level.
//            Only content replicates — a Pdu carries content and nothing else — so
//            when the target lived only at the top level a redaction reached other
//            nodes as an m.room.redaction that named nothing, and they went on
//            serving the original body. "Delete this message" then worked on exactly
//            one node: whichever one the client was talking to. Room version 11
//            (MSC2174) moves `redacts` into content anyway; writing both satisfies
//            clients on either side of that change.
//
//          WHO RECORDS IT (all four write paths, since 2026-08-04):
//            local send + journal replay -> AppState::append_room_timeline
//            live cluster delta          -> routes/sync.rs drain
//            startup / mid-life catch-up -> main.rs merge_catchup_delta
//            Each extracts the target with AppState::redaction_target and then calls
//            mark_redacted AFTER releasing the room_timeline lock (read paths take
//            room_timeline then redactions; taking them the other way round under
//            that lock would risk a deadlock).
//            Replay is what makes a redaction survive a restart: the redactions map
//            is not persisted as its own file, it is rebuilt from the redaction
//            events in the room journal.
//
//          WHAT REDACTION IS NOT: it masks at serve time, it does not erase. The
//            original event stays in the RoomLog and in every node's journal, and a
//            node catching up still receives it. That matches Matrix (purge is a
//            separate operation) but should not be mistaken for deletion.
//
//          Scope check (documented seam): the caller must be a CURRENT joined
//          member of the room (AppState::joined_members) — this is the minimal
//          bar Matrix requires for redacting one's OWN events. Real Matrix also
//          lets a sufficiently-privileged user (power_levels "redact" >= their
//          power level) redact OTHER users' events; this server does not track
//          power levels for authorization anywhere yet (see routes/room_state.rs
//          module header's identical caveat for state writes), so that refinement
//          is left as a seam rather than half-implemented here.
// DEPENDENCIES: axum, serde_json, AppState, routes::send::insert_pdu, auth::extract_caller
// PUBLIC_API: put_redact_event
// END_AI_HEADER

use crate::{auth, error::HsError, routes::send::insert_pdu, state::AppState};
use axum::{
    extract::{Path, State},
    http::HeaderMap,
    Json,
};
use serde_json::{json, Value};
use std::sync::Arc;

// put_redact_event:start
//   purpose: Accept a Matrix redact-event request: build an m.room.redaction event
//            (content = {"reason": ...} if provided, top-level "redacts" = the
//            target event_id), insert it via insert_pdu (same path as put_send_event),
//            then record the redaction in AppState.redactions so reads mask the
//            target event. Returns the new redaction event's event_id.
//   input:  room_id, event_id (target being redacted), txn_id (path params);
//           Authorization header (signed "mxt_..." token, required);
//           optional JSON body {"reason": "..."}
//   output: JSON {"event_id":"$<redaction event id>"}
//   sideEffects: inserts the m.room.redaction Pdu into AppState.rooms[room_id]
//                (via insert_pdu — also appends to room_timeline, persists,
//                notifies waiters, dispatches push, (cluster) publishes delta);
//                inserts into AppState.redactions[event_id]
// put_redact_event:end
pub async fn put_redact_event(
    State(state): State<Arc<AppState>>,
    Path((room_id, event_id, _txn_id)): Path<(String, String, String)>,
    headers: HeaderMap,
    body: Option<Json<Value>>,
) -> Result<Json<Value>, HsError> {
    let room_id = state.resolve_room_id(&room_id);

    let (sender, _device_id) = auth::extract_caller(&headers, &state)
        .ok_or_else(|| HsError::UnknownToken("missing or invalid token".to_string()))?;

    // Minimal scope check: caller must be a current joined member of the room.
    // See module header for the documented power-level seam this does NOT enforce.
    if !state.joined_members(&room_id).iter().any(|u| u == &sender) {
        return Err(HsError::Forbidden(format!(
            "{sender} is not a member of {room_id}"
        )));
    }

    let reason = body
        .and_then(|Json(v)| v.get("reason").cloned())
        .unwrap_or(Value::Null);

    let mut content = serde_json::Map::new();
    if !reason.is_null() {
        content.insert("reason".to_string(), reason);
    }
    // `redacts` goes in the CONTENT as well as at the top level. Only content
    // crosses the mesh — a Pdu carries content and nothing else — so a redaction
    // whose target lived only at the top level replicated as an m.room.redaction
    // that said nothing about what it redacted, and every other node went on
    // serving the original. Room version 11 (MSC2174) moved `redacts` into content
    // for its own reasons; writing both satisfies clients on either side of that.
    content.insert("redacts".to_string(), Value::String(event_id.clone()));
    let content_bytes = serde_json::to_vec(&Value::Object(content))
        .map_err(|e| HsError::Internal(e.to_string()))?;

    let extra_fields = json!({ "redacts": event_id.clone() });

    let (redaction_event_id, redaction_client_event) = insert_pdu(
        &state,
        &room_id,
        sender,
        "m.room.redaction".to_string(),
        content_bytes,
        extra_fields,
    )
    .await?;

    // Record the redaction so every timeline-read path masks the target event.
    state
        .mark_redacted(&event_id, redaction_client_event)
        .map_err(HsError::Internal)?;

    Ok(Json(json!({ "event_id": redaction_event_id })))
}
