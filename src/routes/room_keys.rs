// START_AI_HEADER
// MODULE: matrix-hs/src/routes/room_keys.rs
// PURPOSE: Matrix CS-API key-backup endpoints (/room_keys) — the server-side
//          durability store for a client's encrypted Megolm session-key backup.
//          This server never decrypts anything here: auth_data and every
//          KeyBackupData blob are stored and returned exactly as the client
//          supplied them (opaque per the Matrix spec).
//
//          Endpoints implemented:
//            POST   /room_keys/version              — create a new backup version
//            GET    /room_keys/version               — current version's metadata
//            GET    /room_keys/version/{version}      — a specific version's metadata
//            PUT    /room_keys/version/{version}      — update algorithm/auth_data
//            DELETE /room_keys/version/{version}      — delete a version + its keys
//            PUT/GET/DELETE /room_keys/keys                      (all rooms/sessions)
//            PUT/GET/DELETE /room_keys/keys/{roomId}              (one room)
//            PUT/GET/DELETE /room_keys/keys/{roomId}/{sessionId}  (one session)
//          The three keys-scope variants share one implementation per HTTP method,
//          parameterised by Option<room_id>/Option<session_id> (mirrors
//          AppState::get_room_key_data / delete_room_key_data's own scoping).
//          `version` for the keys endpoints is always a QUERY parameter
//          (?version=<n>) per the Matrix spec — path segments are reserved for
//          room_id/session_id scoping.
//
//          SCOPE: single-node, per-authenticated-user storage. See
//          AppState.e2ee.room_key_versions doc comment for the cross-node honesty
//          statement (NOT cluster-replicated — same posture as device_otks/
//          device_keys in routes/keys.rs).
//
// DEPENDENCIES: axum, serde, serde_json, AppState, error::HsError,
//               routes::keys::extract_caller
// PUBLIC_API: post_room_keys_version, get_room_keys_version_current,
//             get_room_keys_version, put_room_keys_version,
//             delete_room_keys_version, put_room_keys_all, put_room_keys_room,
//             put_room_keys_session, get_room_keys_all, get_room_keys_room,
//             get_room_keys_session, delete_room_keys_all, delete_room_keys_room,
//             delete_room_keys_session
// END_AI_HEADER

use axum::{
    extract::{Path, Query, State},
    http::HeaderMap,
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;

use crate::{error::HsError, routes::keys::extract_caller, state::AppState};

// VersionQuery:start
//   purpose: Parse the ?version= query parameter accepted by every /room_keys/keys
//            endpoint. Per the Matrix spec, when omitted the request targets the
//            caller's CURRENT backup version.
//   input:  URL query string
//   output: VersionQuery { version: Option<String> }
//   sideEffects: none
// VersionQuery:end
#[derive(Debug, Deserialize)]
pub struct VersionQuery {
    pub version: Option<String>,
}

// ── Shared helpers ────────────────────────────────────────────────────────────

// resolve_version:start
//   purpose: Resolve the effective backup version for a /room_keys/keys request:
//            the query's ?version= if given, else the caller's current version.
//   input:  state — Arc<AppState>; user_id — caller; q — parsed VersionQuery
//   output: Ok(version_string); Err(HsError::NotFound) if no version was given AND
//           the caller has no current version (never created a backup, or their
//           current version was deleted with none created since)
//   sideEffects: none
// resolve_version:end
fn resolve_version(state: &AppState, user_id: &str, q: &VersionQuery) -> Result<String, HsError> {
    if let Some(v) = &q.version {
        return Ok(v.clone());
    }
    state
        .current_room_key_version(user_id)
        .ok_or_else(|| HsError::NotFound("no current key backup version".to_string()))
}

// version_metadata_response:start
//   purpose: Build the JSON body returned by every version-metadata endpoint
//            (POST's create response is a separate {"version"} shape; this is
//            for GET version[/{version}] and the implicit re-read after PUT).
//   input:  state — Arc<AppState>; user_id, version
//   output: Ok(Json body {"algorithm","auth_data","version","count","etag"});
//           Err(HsError::NotFound) if the version does not exist
//   sideEffects: none (read-only)
// version_metadata_response:end
fn version_metadata_response(
    state: &AppState,
    user_id: &str,
    version: &str,
) -> Result<Value, HsError> {
    let (algorithm, auth_data, etag, count) = state
        .get_room_key_version(user_id, version)
        .ok_or_else(|| HsError::NotFound(format!("no such backup version: {version}")))?;
    Ok(json!({
        "algorithm": algorithm,
        "auth_data": auth_data,
        "version":   version,
        "count":     count,
        "etag":      etag.to_string(),
    }))
}

// ── Version lifecycle: create / read / update / delete ───────────────────────

// post_room_keys_version:start
//   purpose: POST /room_keys/version — create a new backup version for the caller.
//            Requires a valid signed Bearer token; unknown/missing token → 401.
//            Body must include "algorithm" (opaque string per spec, not validated
//            here beyond presence) and "auth_data" (opaque object) — both stored
//            exactly as supplied. Version numbers are per-user monotonically
//            increasing strings ("1", "2", ...), minted by
//            AppState::create_room_key_version; never reused even after a delete.
//   input:  JSON body {"algorithm": <string>, "auth_data": <object>}
//           Authorization: Bearer <signed mxt_ token>
//   output: JSON {"version": "<n>"}
//           401 M_UNKNOWN_TOKEN on missing/invalid token
//           400 M_BAD_JSON if algorithm or auth_data is missing
//   sideEffects: inserts into state.e2ee.room_key_versions, room_key_backup_seq,
//                room_key_current_version; persists a "create" journal record
// post_room_keys_version:end
pub async fn post_room_keys_version(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Option<Json<Value>>,
) -> Result<Json<Value>, HsError> {
    let (user_id, _device_id) = extract_caller(&headers, &state)
        .ok_or_else(|| HsError::UnknownToken("missing or invalid token".to_string()))?;
    let body = body.map(|b| b.0).unwrap_or_else(|| json!({}));

    let algorithm = body
        .get("algorithm")
        .cloned()
        .ok_or_else(|| HsError::BadRequest("missing 'algorithm'".to_string()))?;
    let auth_data = body
        .get("auth_data")
        .cloned()
        .ok_or_else(|| HsError::BadRequest("missing 'auth_data'".to_string()))?;

    let version = state
        .create_room_key_version(&user_id, algorithm.clone(), auth_data.clone())
        .map_err(HsError::Internal)?;

    state.persist_room_key_version_create(&user_id, &version, &algorithm, &auth_data);

    Ok(Json(json!({ "version": version })))
}

// get_room_keys_version_current:start
//   purpose: GET /room_keys/version (no version in path) — return the caller's
//            CURRENT backup version's metadata.
//   input:  Authorization: Bearer <signed mxt_ token>
//   output: JSON {"algorithm","auth_data","version","count","etag"}
//           401 M_UNKNOWN_TOKEN on missing/invalid token
//           404 M_NOT_FOUND if the caller has no current version
//   sideEffects: none (read-only)
// get_room_keys_version_current:end
pub async fn get_room_keys_version_current(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<Value>, HsError> {
    let (user_id, _device_id) = extract_caller(&headers, &state)
        .ok_or_else(|| HsError::UnknownToken("missing or invalid token".to_string()))?;
    let version = state
        .current_room_key_version(&user_id)
        .ok_or_else(|| HsError::NotFound("no current key backup version".to_string()))?;
    Ok(Json(version_metadata_response(&state, &user_id, &version)?))
}

// get_room_keys_version:start
//   purpose: GET /room_keys/version/{version} — return a specific version's
//            metadata (need not be the caller's current version).
//   input:  Path(version); Authorization: Bearer <signed mxt_ token>
//   output: JSON {"algorithm","auth_data","version","count","etag"}
//           401 M_UNKNOWN_TOKEN on missing/invalid token
//           404 M_NOT_FOUND if that version does not exist for the caller
//   sideEffects: none (read-only)
// get_room_keys_version:end
pub async fn get_room_keys_version(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(version): Path<String>,
) -> Result<Json<Value>, HsError> {
    let (user_id, _device_id) = extract_caller(&headers, &state)
        .ok_or_else(|| HsError::UnknownToken("missing or invalid token".to_string()))?;
    Ok(Json(version_metadata_response(&state, &user_id, &version)?))
}

// put_room_keys_version:start
//   purpose: PUT /room_keys/version/{version} — update algorithm and/or auth_data
//            of an existing version in place. Fields omitted in the body are left
//            unchanged. If the body includes a "version" field, it must match the
//            path segment (Matrix spec leniency check) — mismatch → 400.
//   input:  Path(version); JSON body {"algorithm"?, "auth_data"?, "version"?};
//           Authorization: Bearer <signed mxt_ token>
//   output: JSON {} (empty object, per spec)
//           401 M_UNKNOWN_TOKEN; 400 M_BAD_JSON on version mismatch;
//           404 M_NOT_FOUND if the version does not exist
//   sideEffects: mutates state.e2ee.room_key_versions; persists an "update" journal
//                record
// put_room_keys_version:end
pub async fn put_room_keys_version(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(version): Path<String>,
    body: Option<Json<Value>>,
) -> Result<Json<Value>, HsError> {
    let (user_id, _device_id) = extract_caller(&headers, &state)
        .ok_or_else(|| HsError::UnknownToken("missing or invalid token".to_string()))?;
    let body = body.map(|b| b.0).unwrap_or_else(|| json!({}));

    if let Some(body_version) = body.get("version").and_then(|v| v.as_str()) {
        if body_version != version {
            return Err(HsError::BadRequest(format!(
                "body version {body_version:?} does not match path version {version:?}"
            )));
        }
    }

    let algorithm = body.get("algorithm").cloned();
    let auth_data = body.get("auth_data").cloned();

    let existed = state
        .update_room_key_version(&user_id, &version, algorithm.clone(), auth_data.clone())
        .map_err(HsError::Internal)?;
    if !existed {
        return Err(HsError::NotFound(format!(
            "no such backup version: {version}"
        )));
    }

    state.persist_room_key_version_update(
        &user_id,
        &version,
        algorithm.as_ref(),
        auth_data.as_ref(),
    );

    Ok(Json(json!({})))
}

// delete_room_keys_version:start
//   purpose: DELETE /room_keys/version/{version} — delete a backup version and
//            every session key stored under it. If this was the caller's current
//            version, current is cleared (the client must create a new version to
//            resume backing up).
//   input:  Path(version); Authorization: Bearer <signed mxt_ token>
//   output: JSON {} (empty object, per spec)
//           401 M_UNKNOWN_TOKEN; 404 M_NOT_FOUND if the version does not exist
//   sideEffects: removes from state.e2ee.room_key_versions, room_key_data,
//                room_key_current_version; persists a "delete" journal record
// delete_room_keys_version:end
pub async fn delete_room_keys_version(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(version): Path<String>,
) -> Result<Json<Value>, HsError> {
    let (user_id, _device_id) = extract_caller(&headers, &state)
        .ok_or_else(|| HsError::UnknownToken("missing or invalid token".to_string()))?;

    let existed = state
        .delete_room_key_version(&user_id, &version)
        .map_err(HsError::Internal)?;
    if !existed {
        return Err(HsError::NotFound(format!(
            "no such backup version: {version}"
        )));
    }

    state.persist_room_key_version_delete(&user_id, &version);

    Ok(Json(json!({})))
}

// ── Keys storage: PUT / GET / DELETE, scoped by (room_id?, session_id?) ──────

// put_keys_scoped:start
//   purpose: Shared implementation for PUT .../keys[/{roomId}[/{sessionId}]].
//            Stores one or more KeyBackupData session blobs from the request
//            body, scoped by which path segments are present:
//              - room_id+session_id given: body IS the single KeyBackupData blob.
//              - room_id given only: body is {"sessions": {session_id: data, ...}}.
//              - neither given: body is {"rooms": {room_id: {"sessions": {...}}}}.
//            The target version must already exist (created via POST
//            /room_keys/version) — 404 if not. Bumps that version's etag once per
//            session actually stored (mirrors replay's per-line bump so a restart
//            reproduces the same etag).
//   input:  state, user_id, version; room_id, session_id — Some to scope, per the
//           three PUT route variants; body — JSON per the shape above
//   output: Ok((count, etag)) on success; Err(HsError) — 404 if the version does
//           not exist, 400 if the body doesn't match the expected scoped shape
//   sideEffects: mutates state.e2ee.room_key_data; bumps room_key_versions[..].etag;
//                persists one "put" journal record per session stored
// put_keys_scoped:end
fn put_keys_scoped(
    state: &Arc<AppState>,
    user_id: &str,
    version: &str,
    room_id: Option<&str>,
    session_id: Option<&str>,
    body: Value,
) -> Result<(u64, u64), HsError> {
    if !state.room_key_version_exists(user_id, version) {
        return Err(HsError::NotFound(format!(
            "no such backup version: {version}"
        )));
    }

    let mut last_etag = 0u64;
    let mut store_one = |rid: &str, sid: &str, data: Value| -> Result<(), HsError> {
        state
            .put_room_key_session(user_id, version, rid, sid, data.clone())
            .map_err(HsError::Internal)?;
        state.persist_room_key_put(user_id, version, rid, sid, &data);
        last_etag = state
            .bump_room_key_etag(user_id, version)
            .map_err(HsError::Internal)?
            .unwrap_or(last_etag);
        Ok(())
    };

    match (room_id, session_id) {
        (Some(rid), Some(sid)) => {
            store_one(rid, sid, body)?;
        }
        (Some(rid), None) => {
            let sessions = body
                .get("sessions")
                .and_then(|v| v.as_object())
                .ok_or_else(|| HsError::BadRequest("missing 'sessions' object".to_string()))?;
            for (sid, data) in sessions {
                store_one(rid, sid, data.clone())?;
            }
        }
        (None, _) => {
            let rooms = body
                .get("rooms")
                .and_then(|v| v.as_object())
                .ok_or_else(|| HsError::BadRequest("missing 'rooms' object".to_string()))?;
            for (rid, room_val) in rooms {
                let sessions = room_val
                    .get("sessions")
                    .and_then(|v| v.as_object())
                    .ok_or_else(|| {
                        HsError::BadRequest(format!("room {rid}: missing 'sessions' object"))
                    })?;
                for (sid, data) in sessions {
                    store_one(rid, sid, data.clone())?;
                }
            }
        }
    }

    let count = state.room_key_count(user_id, version);
    Ok((count, last_etag))
}

// put_room_keys_all:start
//   purpose: PUT /room_keys/keys?version=<v> — store sessions for possibly many
//            rooms in one request. See put_keys_scoped for the body shape.
//   input:  Query(VersionQuery); JSON body {"rooms": {...}};
//           Authorization: Bearer <signed mxt_ token>
//   output: JSON {"count","etag"}; 401/400/404 as documented on put_keys_scoped
//   sideEffects: see put_keys_scoped
// put_room_keys_all:end
pub async fn put_room_keys_all(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(q): Query<VersionQuery>,
    body: Option<Json<Value>>,
) -> Result<Json<Value>, HsError> {
    let (user_id, _device_id) = extract_caller(&headers, &state)
        .ok_or_else(|| HsError::UnknownToken("missing or invalid token".to_string()))?;
    let version = resolve_version(&state, &user_id, &q)?;
    let body = body.map(|b| b.0).unwrap_or_else(|| json!({}));
    let (count, etag) = put_keys_scoped(&state, &user_id, &version, None, None, body)?;
    Ok(Json(json!({ "count": count, "etag": etag.to_string() })))
}

// put_room_keys_room:start
//   purpose: PUT /room_keys/keys/{roomId}?version=<v> — store sessions for one
//            room. See put_keys_scoped for the body shape.
//   input:  Path(room_id); Query(VersionQuery); JSON body {"sessions": {...}};
//           Authorization: Bearer <signed mxt_ token>
//   output: JSON {"count","etag"}; 401/400/404 as documented on put_keys_scoped
//   sideEffects: see put_keys_scoped
// put_room_keys_room:end
pub async fn put_room_keys_room(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(room_id): Path<String>,
    Query(q): Query<VersionQuery>,
    body: Option<Json<Value>>,
) -> Result<Json<Value>, HsError> {
    let (user_id, _device_id) = extract_caller(&headers, &state)
        .ok_or_else(|| HsError::UnknownToken("missing or invalid token".to_string()))?;
    let version = resolve_version(&state, &user_id, &q)?;
    let body = body.map(|b| b.0).unwrap_or_else(|| json!({}));
    let (count, etag) = put_keys_scoped(&state, &user_id, &version, Some(&room_id), None, body)?;
    Ok(Json(json!({ "count": count, "etag": etag.to_string() })))
}

// put_room_keys_session:start
//   purpose: PUT /room_keys/keys/{roomId}/{sessionId}?version=<v> — store one
//            session's KeyBackupData directly as the request body.
//   input:  Path((room_id, session_id)); Query(VersionQuery);
//           JSON body = the KeyBackupData blob itself;
//           Authorization: Bearer <signed mxt_ token>
//   output: JSON {"count","etag"}; 401/404 as documented on put_keys_scoped
//   sideEffects: see put_keys_scoped
// put_room_keys_session:end
pub async fn put_room_keys_session(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((room_id, session_id)): Path<(String, String)>,
    Query(q): Query<VersionQuery>,
    body: Option<Json<Value>>,
) -> Result<Json<Value>, HsError> {
    let (user_id, _device_id) = extract_caller(&headers, &state)
        .ok_or_else(|| HsError::UnknownToken("missing or invalid token".to_string()))?;
    let version = resolve_version(&state, &user_id, &q)?;
    let body = body.map(|b| b.0).unwrap_or_else(|| json!({}));
    let (count, etag) = put_keys_scoped(
        &state,
        &user_id,
        &version,
        Some(&room_id),
        Some(&session_id),
        body,
    )?;
    Ok(Json(json!({ "count": count, "etag": etag.to_string() })))
}

// ── Keys read: GET, scoped by (room_id?, session_id?) ─────────────────────────

// get_room_keys_all:start
//   purpose: GET /room_keys/keys?version=<v> — return every stored session
//            across all rooms for that version.
//   input:  Query(VersionQuery); Authorization: Bearer <signed mxt_ token>
//   output: JSON {"rooms": {room_id: {"sessions": {session_id: data}}}}
//           401 M_UNKNOWN_TOKEN; 404 M_NOT_FOUND if no version resolves
//   sideEffects: none (read-only)
// get_room_keys_all:end
pub async fn get_room_keys_all(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(q): Query<VersionQuery>,
) -> Result<Json<Value>, HsError> {
    let (user_id, _device_id) = extract_caller(&headers, &state)
        .ok_or_else(|| HsError::UnknownToken("missing or invalid token".to_string()))?;
    let version = resolve_version(&state, &user_id, &q)?;
    Ok(Json(
        state.get_room_key_data(&user_id, &version, None, None),
    ))
}

// get_room_keys_room:start
//   purpose: GET /room_keys/keys/{roomId}?version=<v> — return every stored
//            session for one room.
//   input:  Path(room_id); Query(VersionQuery); Authorization: Bearer <token>
//   output: JSON {"sessions": {session_id: data}}
//           401 M_UNKNOWN_TOKEN; 404 M_NOT_FOUND if no version resolves
//   sideEffects: none (read-only)
// get_room_keys_room:end
pub async fn get_room_keys_room(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(room_id): Path<String>,
    Query(q): Query<VersionQuery>,
) -> Result<Json<Value>, HsError> {
    let (user_id, _device_id) = extract_caller(&headers, &state)
        .ok_or_else(|| HsError::UnknownToken("missing or invalid token".to_string()))?;
    let version = resolve_version(&state, &user_id, &q)?;
    Ok(Json(state.get_room_key_data(
        &user_id,
        &version,
        Some(&room_id),
        None,
    )))
}

// get_room_keys_session:start
//   purpose: GET /room_keys/keys/{roomId}/{sessionId}?version=<v> — return one
//            stored session's KeyBackupData directly.
//   input:  Path((room_id, session_id)); Query(VersionQuery);
//           Authorization: Bearer <signed mxt_ token>
//   output: JSON the KeyBackupData blob, or `null` if absent
//           401 M_UNKNOWN_TOKEN; 404 M_NOT_FOUND if no version resolves
//   sideEffects: none (read-only)
// get_room_keys_session:end
pub async fn get_room_keys_session(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((room_id, session_id)): Path<(String, String)>,
    Query(q): Query<VersionQuery>,
) -> Result<Json<Value>, HsError> {
    let (user_id, _device_id) = extract_caller(&headers, &state)
        .ok_or_else(|| HsError::UnknownToken("missing or invalid token".to_string()))?;
    let version = resolve_version(&state, &user_id, &q)?;
    Ok(Json(state.get_room_key_data(
        &user_id,
        &version,
        Some(&room_id),
        Some(&session_id),
    )))
}

// ── Keys delete, scoped by (room_id?, session_id?) ────────────────────────────

// delete_keys_scoped:start
//   purpose: Shared implementation for DELETE .../keys[/{roomId}[/{sessionId}]].
//            Removes the matching sub-tree from room_key_data and bumps the
//            version's etag once (a single delete call, however broad its scope,
//            is one mutation for etag purposes — mirrors the persisted journal
//            granularity in replay_room_key_data).
//   input:  state, user_id, version, room_id, session_id — same scoping as
//           put_keys_scoped
//   output: Ok(etag) on success; Err(HsError::NotFound) if the version does not
//           exist
//   sideEffects: mutates state.e2ee.room_key_data; bumps room_key_versions[..].etag;
//                persists one delete journal record
// delete_keys_scoped:end
fn delete_keys_scoped(
    state: &Arc<AppState>,
    user_id: &str,
    version: &str,
    room_id: Option<&str>,
    session_id: Option<&str>,
) -> Result<u64, HsError> {
    if !state.room_key_version_exists(user_id, version) {
        return Err(HsError::NotFound(format!(
            "no such backup version: {version}"
        )));
    }
    state
        .delete_room_key_data(user_id, version, room_id, session_id)
        .map_err(HsError::Internal)?;
    state.persist_room_key_delete(user_id, version, room_id, session_id);
    let etag = state
        .bump_room_key_etag(user_id, version)
        .map_err(HsError::Internal)?
        .unwrap_or(0);
    Ok(etag)
}

// delete_room_keys_all:start
//   purpose: DELETE /room_keys/keys?version=<v> — delete every stored session for
//            that version (across all rooms).
//   input:  Query(VersionQuery); Authorization: Bearer <signed mxt_ token>
//   output: JSON {"count","etag"}; 401/404 as documented on delete_keys_scoped
//   sideEffects: see delete_keys_scoped
// delete_room_keys_all:end
pub async fn delete_room_keys_all(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(q): Query<VersionQuery>,
) -> Result<Json<Value>, HsError> {
    let (user_id, _device_id) = extract_caller(&headers, &state)
        .ok_or_else(|| HsError::UnknownToken("missing or invalid token".to_string()))?;
    let version = resolve_version(&state, &user_id, &q)?;
    let etag = delete_keys_scoped(&state, &user_id, &version, None, None)?;
    let count = state.room_key_count(&user_id, &version);
    Ok(Json(json!({ "count": count, "etag": etag.to_string() })))
}

// delete_room_keys_room:start
//   purpose: DELETE /room_keys/keys/{roomId}?version=<v> — delete every stored
//            session for one room.
//   input:  Path(room_id); Query(VersionQuery); Authorization: Bearer <token>
//   output: JSON {"count","etag"}; 401/404 as documented on delete_keys_scoped
//   sideEffects: see delete_keys_scoped
// delete_room_keys_room:end
pub async fn delete_room_keys_room(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(room_id): Path<String>,
    Query(q): Query<VersionQuery>,
) -> Result<Json<Value>, HsError> {
    let (user_id, _device_id) = extract_caller(&headers, &state)
        .ok_or_else(|| HsError::UnknownToken("missing or invalid token".to_string()))?;
    let version = resolve_version(&state, &user_id, &q)?;
    let etag = delete_keys_scoped(&state, &user_id, &version, Some(&room_id), None)?;
    let count = state.room_key_count(&user_id, &version);
    Ok(Json(json!({ "count": count, "etag": etag.to_string() })))
}

// delete_room_keys_session:start
//   purpose: DELETE /room_keys/keys/{roomId}/{sessionId}?version=<v> — delete one
//            stored session.
//   input:  Path((room_id, session_id)); Query(VersionQuery);
//           Authorization: Bearer <signed mxt_ token>
//   output: JSON {"count","etag"}; 401/404 as documented on delete_keys_scoped
//   sideEffects: see delete_keys_scoped
// delete_room_keys_session:end
pub async fn delete_room_keys_session(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((room_id, session_id)): Path<(String, String)>,
    Query(q): Query<VersionQuery>,
) -> Result<Json<Value>, HsError> {
    let (user_id, _device_id) = extract_caller(&headers, &state)
        .ok_or_else(|| HsError::UnknownToken("missing or invalid token".to_string()))?;
    let version = resolve_version(&state, &user_id, &q)?;
    let etag = delete_keys_scoped(
        &state,
        &user_id,
        &version,
        Some(&room_id),
        Some(&session_id),
    )?;
    let count = state.room_key_count(&user_id, &version);
    Ok(Json(json!({ "count": count, "etag": etag.to_string() })))
}
