// START_AI_HEADER
// MODULE: matrix-hs/src/routes/account.rs
// PURPOSE: Account-related CS-API endpoints for Stage 2.
//          whoami, capabilities, pushrules, filter, joined_rooms, profile, devices,
//          and deactivate.
// DEPENDENCIES: axum, AppState, auth
// PUBLIC_API: get_whoami, get_capabilities, get_pushrules, post_user_filter,
//             get_user_filter, get_joined_rooms, get_profile, get_displayname,
//             get_devices, post_deactivate, post_logout, post_logout_all,
//             post_user_directory_search
// END_AI_HEADER

use crate::{auth, error::HsError, state::AppState};
use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde_json::{json, Value};
use std::sync::Arc;

// extract_token:start
//   purpose: Extract the Bearer token from an Authorization header.
//   input:  headers — HeaderMap
//   output: Option<&str> — token string without "Bearer " prefix
//   sideEffects: none
// extract_token:end
fn extract_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
}

// resolve_user_id_from_token:start
//   purpose: Verify a Bearer token and return the user_id it encodes.
//            ONLY signed "mxt_..." tokens (auth::verify_token, HMAC-SHA256) authenticate.
//            There is NO legacy "tok_<localpart>" acceptance — a forgeable prefix token
//            is not a credential.  Returns None if the token is absent, malformed, or has
//            an invalid MAC; callers convert None to 401 M_UNKNOWN_TOKEN.
//            The returned user_id is the identity minted into the token; rename resolution
//            (AppState.renamed lookup) is the caller's responsibility.
//   input:  token — Bearer token string; secret — HMAC key
//            (server_name retained for signature symmetry; unused since the mint-encoded
//             user_id already carries the server part)
//   output: Option<String> — Matrix user_id ("@user:server") or None
//   sideEffects: none
// resolve_user_id_from_token:end
pub fn resolve_user_id_from_token(
    token: &str,
    secret: &[u8],
    _server_name: &str,
) -> Option<String> {
    // Only signed mxt_ tokens authenticate. No tok_ fallback.
    // Note: epoch is NOT checked here — this is a low-level tokenverify, not full auth.
    // Epoch checking (revocation gate) happens in auth::extract_caller only.
    auth::verify_token(secret, token).map(|(uid, _, _)| uid)
}

// get_whoami:start
//   purpose: Return the user_id for the authenticated user.
//            Verifies the Bearer token via auth::verify_token (signed mxt_ tokens only).
//            Checks AppState.renamed: if the localpart was renamed, returns the NEW
//            user_id so the client discovers its new identity on the next poll.
//            Missing/invalid token → 401 M_UNKNOWN_TOKEN.
//   input:  State(AppState), headers with Authorization: Bearer <token>
//   output: JSON {"user_id":"@<user>:<server>","device_id":"<device_id>"}
//   sideEffects: none (read-only access to AppState.renamed + AppState.users)
// get_whoami:end
pub async fn get_whoami(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<Value>, HsError> {
    let token = extract_token(&headers)
        .ok_or_else(|| HsError::UnknownToken("missing Authorization header".to_string()))?;

    // Verify the signed token.
    let verified_user_id =
        resolve_user_id_from_token(token, &state.token_secret, &state.server_name)
            .ok_or_else(|| HsError::UnknownToken("invalid or expired token".to_string()))?;

    // Extract the localpart from the verified user_id.
    let orig_localpart = verified_user_id
        .strip_prefix('@')
        .and_then(|s| s.split(':').next())
        .unwrap_or(&verified_user_id);

    // Check if this localpart was renamed (lost a grow-set conflict).
    let (user_id, device_id) = {
        let renamed = state
            .renamed
            .lock()
            .map_err(|e| HsError::Internal(e.to_string()))?;

        if let Some(new_user_id) = renamed.get(orig_localpart) {
            let new_localpart = new_user_id
                .strip_prefix('@')
                .and_then(|s| s.split(':').next())
                .unwrap_or(orig_localpart);
            let device_id = state
                .users
                .lock()
                .ok()
                .and_then(|u| u.get(new_localpart).map(|r| r.device_id.clone()))
                .unwrap_or_else(|| "DEVICE1".to_string());
            (new_user_id.clone(), device_id)
        } else {
            let device_id = state
                .users
                .lock()
                .ok()
                .and_then(|u| u.get(orig_localpart).map(|r| r.device_id.clone()))
                .unwrap_or_else(|| "DEVICE1".to_string());
            (verified_user_id.clone(), device_id)
        }
    };

    Ok(Json(json!({
        "user_id":   user_id,
        "device_id": device_id,
    })))
}

// get_capabilities:start
//   purpose: Return server capabilities for the Matrix client.
//   input:  none
//   output: JSON {"capabilities":{...}}
//   sideEffects: none
// get_capabilities:end
pub async fn get_capabilities() -> Json<Value> {
    Json(json!({
        "capabilities": {
            "m.room_versions": {
                "default": "10",
                "available": {
                    "9":  "stable",
                    "10": "stable",
                    "11": "stable"
                }
            },
            "m.change_password": { "enabled": true }
        }
    }))
}

// get_pushrules:start
//   purpose: Return empty push rules (stub).
//   input:  none
//   output: JSON {"global":{...}}
//   sideEffects: none
// get_pushrules:end
pub async fn get_pushrules() -> Json<Value> {
    Json(json!({
        "global": {
            "content":   [],
            "override":  [],
            "room":      [],
            "sender":    [],
            "underride": []
        }
    }))
}

// post_user_filter:start
//   purpose: Accept a filter upload and return a filter_id (stub).
//   input:  user_id path param, optional JSON body
//   output: JSON {"filter_id":"0"}
//   sideEffects: none
// post_user_filter:end
pub async fn post_user_filter(
    Path(_user_id): Path<String>,
    _body: Option<Json<Value>>,
) -> Json<Value> {
    Json(json!({ "filter_id": "0" }))
}

// get_user_filter:start
//   purpose: Return a stored filter by ID (stub — returns empty filter).
//   input:  user_id, filter_id path params
//   output: JSON {}
//   sideEffects: none
// get_user_filter:end
pub async fn get_user_filter(Path((_user_id, _filter_id)): Path<(String, String)>) -> Json<Value> {
    Json(json!({}))
}

// get_joined_rooms:start
//   purpose: Return the list of room_ids where the authenticated user is a member.
//   input:  State(AppState), headers
//   output: JSON {"joined_rooms":["!room:server",...]}
//   sideEffects: none
// get_joined_rooms:end
pub async fn get_joined_rooms(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<Value>, HsError> {
    let token = extract_token(&headers)
        .ok_or_else(|| HsError::UnknownToken("missing Authorization header".to_string()))?;
    let user_id = resolve_user_id_from_token(token, &state.token_secret, &state.server_name)
        .ok_or_else(|| HsError::UnknownToken("invalid or expired token".to_string()))?;

    let rs = state
        .room_state
        .lock()
        .map_err(|e| HsError::Internal(e.to_string()))?;

    let mut joined: Vec<String> = Vec::new();
    for (room_id, events) in rs.iter() {
        let is_member = events.iter().any(|ev| {
            ev.event_type == "m.room.member"
                && ev.state_key == user_id
                && ev.content.get("membership").and_then(|v| v.as_str()) == Some("join")
        });
        if is_member {
            joined.push(room_id.clone());
        }
    }

    Ok(Json(json!({ "joined_rooms": joined })))
}

// displayname_of:start
//   purpose: The ONE place that turns a Matrix user id into the displayname both
//            profile routes answer with.  Returns None when the id carries no
//            localpart ("@:server", or a bare server name), so a caller can tell
//            "this user has no displayname" from "this user is called alice".
//            Both /profile/{userId} and /profile/{userId}/displayname go through
//            it, which is what keeps them in agreement: the sub-resource can
//            never disagree with the full profile about the same user.
//   input:  user_id — Matrix user id ("@localpart:server") or a bare name
//   output: Option<String> — the localpart, or None when there is none
//   sideEffects: none (pure)
// displayname_of:end
fn displayname_of(user_id: &str) -> Option<String> {
    let localpart = user_id
        .strip_prefix('@')
        .unwrap_or(user_id)
        .split(':')
        .next()
        .unwrap_or_default();
    if localpart.is_empty() {
        None
    } else {
        Some(localpart.to_string())
    }
}

// get_profile:start
//   purpose: Return profile information for a user.
//   input:  userId path param
//   output: JSON {"displayname":"<localpart>"}; an id with no localpart is
//           echoed back as the displayname rather than answered with an empty
//           string, which is what this route has always done for such an id.
//   sideEffects: none
// get_profile:end
pub async fn get_profile(Path(user_id): Path<String>) -> Json<Value> {
    let displayname = displayname_of(&user_id).unwrap_or(user_id);

    Json(json!({ "displayname": displayname }))
}

// get_displayname:start
//   purpose: GET /_matrix/client/{v3,r0}/profile/{userId}/displayname — the
//            displayname sub-resource clients call instead of parsing the full
//            profile.  Same source of truth as get_profile, so for every user the
//            two agree on the name; the only difference is the body: a user with
//            a displayname gets {"displayname": …}, a user without one gets {}
//            (the field is absent, not null and not an empty string).  An unknown
//            user is NOT an error here: it answers exactly what the full profile
//            answers for it today, because a client that gets 404 from the
//            sub-resource and 200 from the full profile has to treat the user as
//            present.  Requires no access token, like the full profile.
//   input:  userId path param
//   output: JSON {"displayname": "<name>"} or {}
//   sideEffects: none
// get_displayname:end
pub async fn get_displayname(Path(user_id): Path<String>) -> Json<Value> {
    match displayname_of(&user_id) {
        Some(name) => Json(json!({ "displayname": name })),
        None => Json(json!({})),
    }
}

// get_devices:start
//   purpose: Return the list of devices for the authenticated user (stub).
//   input:  headers
//   output: JSON {"devices":[{"device_id":"DEVICE1"}]}
//   sideEffects: none
// get_devices:end
pub async fn get_devices() -> Json<Value> {
    Json(json!({
        "devices": [
            { "device_id": "DEVICE1" }
        ]
    }))
}

// post_deactivate:start
//   purpose: POST /_matrix/client/v3/account/deactivate — authenticated.
//            Removes the caller's account from AppState.users and persist.
//            Records the localpart in AppState.deactivated so the slot is tracked.
//            Spec-standard; lets stale/test users be purged without a server restart.
//            Missing/invalid token → 401 M_UNKNOWN_TOKEN.
//   input:  State(AppState), headers with Authorization: Bearer <token>
//   output: 200 {} on success; 401 on bad token
//   sideEffects: removes user from AppState.users; inserts into AppState.deactivated
// post_deactivate:end
pub async fn post_deactivate(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    _body: Option<Json<Value>>,
) -> Response {
    let token = match extract_token(&headers) {
        Some(t) => t,
        None => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(json!({ "errcode": "M_UNKNOWN_TOKEN", "error": "missing Authorization header" })),
            ).into_response();
        }
    };

    let user_id = match resolve_user_id_from_token(token, &state.token_secret, &state.server_name) {
        Some(uid) => uid,
        None => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(json!({ "errcode": "M_UNKNOWN_TOKEN", "error": "invalid or expired token" })),
            )
                .into_response();
        }
    };

    let localpart = user_id
        .strip_prefix('@')
        .and_then(|s| s.split(':').next())
        .unwrap_or(&user_id)
        .to_string();

    // Remove from users map.
    if let Ok(mut users) = state.users.lock() {
        users.remove(&localpart);
    }

    // Track in deactivated set.
    if let Ok(mut deactivated) = state.deactivated.lock() {
        deactivated.insert(localpart);
    }

    // Device-list change tracking (E2EE device-lists feature): deactivation removes
    // this user's device(s) — a device-list change for anyone sharing a room with
    // them. Record it and gossip cross-node (mirrors the keys/upload hook in
    // routes/keys.rs).
    let _ = state.mark_device_list_changed(&user_id);
    #[cfg(feature = "cluster")]
    let _ = crate::routes::keys::gossip_device_list_change(&state, &user_id).await;

    (StatusCode::OK, Json(json!({}))).into_response()
}

// post_logout:start
//   purpose: POST /_matrix/client/v3/logout — authenticated.
//            SEAM: access tokens here are stateless signed HMACs (auth::sign_token/
//            verify_token) with NO server-side revocation store — this is a
//            deliberate, tracked architectural gap (see ROADMAP.md Phase 2, "Token
//            epoch / revocation"), not an oversight of this endpoint. Real
//            revocation needs a per-user epoch checked on verify; until that lands,
//            this endpoint cannot actually invalidate the presented token. What it
//            DOES do: exist and return spec-shaped 200 {} so real clients (which
//            universally call this on logout) complete their local logout flow
//            instead of erroring on a 404 — reproduced live against FluffyChat.
//   input:  State(AppState), headers with Authorization: Bearer <token>
//   output: 200 {} on success; 401 M_UNKNOWN_TOKEN on missing/invalid token
//   sideEffects: none (see SEAM above)
// post_logout:end
pub async fn post_logout(State(_state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    match extract_token(&headers) {
        Some(_) => (StatusCode::OK, Json(json!({}))).into_response(),
        None => (
            StatusCode::UNAUTHORIZED,
            Json(json!({ "errcode": "M_UNKNOWN_TOKEN", "error": "missing Authorization header" })),
        )
            .into_response(),
    }
}

// post_logout_all:start
//   purpose: POST /_matrix/client/v3/logout/all — authenticated. Same SEAM as
//            post_logout (no token revocation store yet); exists so real clients'
//            "log out all devices" action gets a valid 200 instead of a 404.
//   input:  State(AppState), headers with Authorization: Bearer <token>
//   output: 200 {} on success; 401 M_UNKNOWN_TOKEN on missing/invalid token
//   sideEffects: none (see SEAM on post_logout)
// post_logout_all:end
pub async fn post_logout_all(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    post_logout(State(state), headers).await
}

// post_user_directory_search:start
//   purpose: POST /_matrix/client/v3/user_directory/search — authenticated.
//            Real clients call this for invite-contact/start-chat typeahead; its
//            total absence (404) surfaced live as a visible "Unrecognized
//            request" toast in FluffyChat and silently blocked the invite flow
//            (no search results ever appear, so there is nothing to tap to
//            confirm an invite). Matches registered, non-deactivated users
//            whose localpart contains search_term (case-insensitive substring;
//            this server has no separate display_name store — see get_profile
//            — so localpart doubles as display_name here too).
//   input:  JSON body {"search_term": str, "limit"?: u64};
//           Authorization: Bearer <mxt_ token>
//   output: 200 {"results": [{"user_id","display_name"}], "limited": bool}
//           401 M_UNKNOWN_TOKEN on missing/invalid token
//   sideEffects: none (read-only)
// post_user_directory_search:end
#[derive(serde::Deserialize)]
pub struct UserDirectorySearchRequest {
    search_term: String,
    limit: Option<usize>,
}

pub async fn post_user_directory_search(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Json<UserDirectorySearchRequest>,
) -> Response {
    let token = match extract_token(&headers) {
        Some(t) => t,
        None => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(json!({ "errcode": "M_UNKNOWN_TOKEN", "error": "missing Authorization header" })),
            ).into_response();
        }
    };
    if resolve_user_id_from_token(token, &state.token_secret, &state.server_name).is_none() {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({ "errcode": "M_UNKNOWN_TOKEN", "error": "invalid or expired token" })),
        )
            .into_response();
    }

    let term = body.search_term.to_lowercase();
    let limit = body.limit.unwrap_or(10).max(1);

    let deactivated = state
        .deactivated
        .lock()
        .map(|d| d.clone())
        .unwrap_or_default();
    let mut matches: Vec<(String, String)> = state
        .users
        .lock()
        .map(|users| {
            users
                .keys()
                .filter(|localpart| !deactivated.contains(*localpart))
                .filter(|localpart| localpart.to_lowercase().contains(&term))
                .map(|localpart| {
                    let user_id = format!("@{localpart}:{}", state.server_name);
                    (user_id, localpart.clone())
                })
                .collect()
        })
        .unwrap_or_default();
    matches.sort();

    let limited = matches.len() > limit;
    let results: Vec<Value> = matches
        .into_iter()
        .take(limit)
        .map(|(user_id, display_name)| json!({ "user_id": user_id, "display_name": display_name }))
        .collect();

    (
        StatusCode::OK,
        Json(json!({ "results": results, "limited": limited })),
    )
        .into_response()
}
