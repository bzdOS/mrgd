// START_AI_HEADER
// MODULE: matrix-hs/src/routes/account_data.rs
// PURPOSE: Account data + room tags CS-API endpoints.
//            PUT/GET /_matrix/client/v3/user/{userId}/account_data/{type}
//            PUT/GET /_matrix/client/v3/user/{userId}/rooms/{roomId}/account_data/{type}
//            GET/PUT/DELETE /_matrix/client/v3/user/{userId}/rooms/{roomId}/tags[/{tag}]
//          Element persists client settings, the m.direct DM map, and room
//          favourites/low-priority markers here — storage lives on AppState
//          (see state.rs "Account data + room tags" section); this module only
//          does request parsing/auth/validation. Every handler requires a valid
//          signed Bearer token whose user_id equals the {userId} path param —
//          a caller may only read/write their OWN account data (403 M_FORBIDDEN
//          otherwise), mirroring routes/ephemeral.rs::put_typing's ownership
//          check.
//          Delivery to clients happens via GET /sync (top-level account_data.events
//          for global data, per-room account_data.events for room data + a
//          synthetic m.tag event) and the sliding-sync account_data extension —
//          see AppState::account_data_global_events / account_data_room_events
//          and routes/sync.rs, routes/sliding_sync.rs.
//          Node-local only (same posture as routes/pushers.rs): not persisted to
//          disk and not cluster-replicated.
// DEPENDENCIES: axum, serde_json, AppState, auth
// PUBLIC_API: put_account_data_global, get_account_data_global,
//             put_account_data_room, get_account_data_room,
//             get_room_tags, put_room_tag, delete_room_tag
// END_AI_HEADER

use axum::{
    extract::{Path, State},
    http::HeaderMap,
    Json,
};
use serde_json::{json, Value};
use std::sync::Arc;

use crate::{auth::extract_caller, error::HsError, state::AppState};

// require_caller_is_user:start
//   purpose: Verify the Bearer token authenticates as exactly `user_id` (the
//            {userId} path param). A client may only read/write its OWN account
//            data — never another user's, even if it knows a valid token for
//            itself.
//   input:  headers, state, user_id — the {userId} path param
//   output: Ok(()) if the caller's verified user_id == user_id;
//           Err(HsError::UnknownToken) if the token is missing/invalid;
//           Err(HsError::Forbidden) if the caller authenticated as someone else
//   sideEffects: none
// require_caller_is_user:end
fn require_caller_is_user(
    headers: &HeaderMap,
    state: &Arc<AppState>,
    user_id: &str,
) -> Result<(), HsError> {
    let (caller_user_id, _device_id) = extract_caller(headers, state)
        .ok_or_else(|| HsError::UnknownToken("missing or invalid token".to_string()))?;
    if caller_user_id != user_id {
        return Err(HsError::Forbidden(
            "cannot access another user's account data".to_string(),
        ));
    }
    Ok(())
}

// put_account_data_global:start
//   purpose: PUT /user/{userId}/account_data/{type} — set (or replace) one
//            global account_data entry for the authenticated user. The body is
//            stored exactly as supplied (opaque JSON per the Matrix spec —
//            e.g. m.direct, m.push_rules, or any client-defined type).
//   input:  user_id, event_type path params; Authorization: Bearer <mxt_ token>;
//           JSON body (the opaque content)
//   output: 200 {} on success
//           401 M_UNKNOWN_TOKEN if the token is missing/invalid
//           403 M_FORBIDDEN if userId does not match the caller
//   sideEffects: mutates state.account_data.account_data_global[user_id][event_type]
// put_account_data_global:end
pub async fn put_account_data_global(
    State(state): State<Arc<AppState>>,
    Path((user_id, event_type)): Path<(String, String)>,
    headers: HeaderMap,
    body: Option<Json<Value>>,
) -> Result<Json<Value>, HsError> {
    require_caller_is_user(&headers, &state, &user_id)?;

    let content = body.map(|b| b.0).unwrap_or_else(|| json!({}));
    state
        .set_account_data_global(&user_id, &event_type, content)
        .map_err(HsError::Internal)?;

    Ok(Json(json!({})))
}

// get_account_data_global:start
//   purpose: GET /user/{userId}/account_data/{type} — return the previously-set
//            global account_data content for this user/type.
//   input:  user_id, event_type path params; Authorization: Bearer <mxt_ token>
//   output: 200 <content> JSON on success
//           401 M_UNKNOWN_TOKEN if the token is missing/invalid
//           403 M_FORBIDDEN if userId does not match the caller
//           404 M_NOT_FOUND if this type was never set for this user
//   sideEffects: none (read-only)
// get_account_data_global:end
pub async fn get_account_data_global(
    State(state): State<Arc<AppState>>,
    Path((user_id, event_type)): Path<(String, String)>,
    headers: HeaderMap,
) -> Result<Json<Value>, HsError> {
    require_caller_is_user(&headers, &state, &user_id)?;

    let content = state
        .get_account_data_global(&user_id, &event_type)
        .map_err(HsError::Internal)?
        .ok_or_else(|| HsError::NotFound(format!("account data type {event_type:?} not set")))?;

    Ok(Json(content))
}

// put_account_data_room:start
//   purpose: PUT /user/{userId}/rooms/{roomId}/account_data/{type} — set (or
//            replace) one per-room account_data entry for the authenticated
//            user. Does NOT require the room to exist locally (per-room account
//            data is client-scoped storage, not room state — Element sets it
//            for rooms this server may not even know about, e.g. federated
//            rooms in a fuller implementation).
//   input:  user_id, room_id, event_type path params;
//           Authorization: Bearer <mxt_ token>; JSON body (opaque content)
//   output: 200 {} on success
//           401 M_UNKNOWN_TOKEN if the token is missing/invalid
//           403 M_FORBIDDEN if userId does not match the caller
//   sideEffects: mutates state.account_data.account_data_room[user_id][room_id][event_type]
// put_account_data_room:end
pub async fn put_account_data_room(
    State(state): State<Arc<AppState>>,
    Path((user_id, room_id, event_type)): Path<(String, String, String)>,
    headers: HeaderMap,
    body: Option<Json<Value>>,
) -> Result<Json<Value>, HsError> {
    require_caller_is_user(&headers, &state, &user_id)?;

    let content = body.map(|b| b.0).unwrap_or_else(|| json!({}));
    state
        .set_account_data_room(&user_id, &room_id, &event_type, content)
        .map_err(HsError::Internal)?;

    Ok(Json(json!({})))
}

// get_account_data_room:start
//   purpose: GET /user/{userId}/rooms/{roomId}/account_data/{type} — return the
//            previously-set per-room account_data content.
//   input:  user_id, room_id, event_type path params;
//           Authorization: Bearer <mxt_ token>
//   output: 200 <content> JSON on success
//           401 M_UNKNOWN_TOKEN if the token is missing/invalid
//           403 M_FORBIDDEN if userId does not match the caller
//           404 M_NOT_FOUND if this type was never set for this (user, room)
//   sideEffects: none (read-only)
// get_account_data_room:end
pub async fn get_account_data_room(
    State(state): State<Arc<AppState>>,
    Path((user_id, room_id, event_type)): Path<(String, String, String)>,
    headers: HeaderMap,
) -> Result<Json<Value>, HsError> {
    require_caller_is_user(&headers, &state, &user_id)?;

    let content = state
        .get_account_data_room(&user_id, &room_id, &event_type)
        .map_err(HsError::Internal)?
        .ok_or_else(|| {
            HsError::NotFound(format!("room account data type {event_type:?} not set"))
        })?;

    Ok(Json(content))
}

// get_room_tags:start
//   purpose: GET /user/{userId}/rooms/{roomId}/tags — return every tag the
//            authenticated user has set on this room.
//   input:  user_id, room_id path params; Authorization: Bearer <mxt_ token>
//   output: 200 {"tags": {"<tag>": <content>, ...}} (empty object if none set)
//           401 M_UNKNOWN_TOKEN if the token is missing/invalid
//           403 M_FORBIDDEN if userId does not match the caller
//   sideEffects: none (read-only)
// get_room_tags:end
pub async fn get_room_tags(
    State(state): State<Arc<AppState>>,
    Path((user_id, room_id)): Path<(String, String)>,
    headers: HeaderMap,
) -> Result<Json<Value>, HsError> {
    require_caller_is_user(&headers, &state, &user_id)?;

    let tags = state.room_tags_for(&user_id, &room_id).unwrap_or_default();
    Ok(Json(json!({ "tags": tags })))
}

// put_room_tag:start
//   purpose: PUT /user/{userId}/rooms/{roomId}/tags/{tag} — set (or replace) one
//            tag on the room for the authenticated user (e.g. m.favourite,
//            m.lowpriority, or a client-defined u.* tag). Body is stored
//            opaquely (typically {"order": <f64>}).
//   input:  user_id, room_id, tag path params; Authorization: Bearer <mxt_
//           token>; JSON body (opaque content, e.g. {"order": 0.5})
//   output: 200 {} on success
//           401 M_UNKNOWN_TOKEN if the token is missing/invalid
//           403 M_FORBIDDEN if userId does not match the caller
//   sideEffects: mutates state.account_data.room_tags[user_id][room_id][tag]
// put_room_tag:end
pub async fn put_room_tag(
    State(state): State<Arc<AppState>>,
    Path((user_id, room_id, tag)): Path<(String, String, String)>,
    headers: HeaderMap,
    body: Option<Json<Value>>,
) -> Result<Json<Value>, HsError> {
    require_caller_is_user(&headers, &state, &user_id)?;

    let content = body.map(|b| b.0).unwrap_or_else(|| json!({}));
    state
        .set_room_tag(&user_id, &room_id, &tag, content)
        .map_err(HsError::Internal)?;

    Ok(Json(json!({})))
}

// delete_room_tag:start
//   purpose: DELETE /user/{userId}/rooms/{roomId}/tags/{tag} — remove one tag
//            from the room for the authenticated user. Idempotent: deleting a
//            tag that was never set still returns 200 {} (per the Matrix spec).
//   input:  user_id, room_id, tag path params; Authorization: Bearer <mxt_
//           token>
//   output: 200 {} on success (whether or not the tag existed)
//           401 M_UNKNOWN_TOKEN if the token is missing/invalid
//           403 M_FORBIDDEN if userId does not match the caller
//   sideEffects: removes state.account_data.room_tags[user_id][room_id][tag] if present
// delete_room_tag:end
pub async fn delete_room_tag(
    State(state): State<Arc<AppState>>,
    Path((user_id, room_id, tag)): Path<(String, String, String)>,
    headers: HeaderMap,
) -> Result<Json<Value>, HsError> {
    require_caller_is_user(&headers, &state, &user_id)?;

    state
        .delete_room_tag(&user_id, &room_id, &tag)
        .map_err(HsError::Internal)?;

    Ok(Json(json!({})))
}
