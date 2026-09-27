// START_AI_HEADER
// MODULE: matrix-hs/src/routes/login.rs
// PURPOSE: POST /_matrix/client/v3/login — authentication.
//          Authenticates registered users against their Argon2id password hash
//          and returns a signed HMAC-SHA256 access token (mxt_ prefix).
//          Unregistered users get 403 M_FORBIDDEN — no lenient fallback.
//          Renamed users (losers of grow-set conflict): password verified against
//          the new localpart's record; response user_id reflects the new identity.
//          Stage 2: includes well_known field with homeserver base_url.
// DEPENDENCIES: axum, serde, serde_json, AppState, auth
// PUBLIC_API: post_login
// END_AI_HEADER

use crate::{auth, error::HsError, state::AppState};
use axum::{extract::State, http::HeaderMap, Json};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::Arc;

// LoginRequest:start
//   purpose: Deserialise the JSON body of POST /login.
//            Only `identifier.user` + `type` are inspected for routing.
//            password is validated against the Argon2id hash stored in AppState.users.
//   input:  JSON body from client
//   output: LoginRequest value
//   sideEffects: none
// LoginRequest:end
#[derive(Debug, Deserialize)]
pub struct LoginRequest {
    #[serde(rename = "type")]
    pub login_type: Option<String>,
    pub identifier: Option<Identifier>,
    pub user: Option<String>, // legacy flat field
    pub password: Option<String>,
    /// m.login.application_service: the MXID to mint a token for (spec field).
    pub user_id: Option<String>,
    /// Optional device for the new session. For the AS path this is the
    /// workers-as-devices knob: each fleet worker logs in with its own
    /// device_id and becomes a distinct device of the tenant account
    /// (AGENT-USE-CASES Case 2). Ignored by the password path (unchanged).
    pub device_id: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Identifier {
    #[serde(rename = "type")]
    pub id_type: Option<String>,
    pub user: Option<String>,
}

// post_login:start
//   purpose: Accept a login request and return a signed access_token + user_id.
//            Registered users: password is verified against the Argon2id hash in
//            AppState.users; mismatch → 403 M_FORBIDDEN.
//            Renamed users (losers of grow-set conflict): if username is in
//            AppState.renamed, password is verified against the NEW localpart's record.
//            The access_token encodes the NEW user_id; the response user_id is the
//            new identity so the client can update its stored state.
//            Unregistered users: 403 M_FORBIDDEN — the lenient fallback is removed.
//            Stage 2: includes well_known field with homeserver base_url.
//   input:  State(AppState), headers, JSON body with optional type/identifier/user fields
//   output: JSON {"access_token":"mxt_<...>","user_id":"@<user>:<server>",
//                 "device_id":"<device_id>","home_server":"<server>",
//                 "well_known":{"m.homeserver":{"base_url":"<url>"}}}
//           403 M_FORBIDDEN on unknown user or password mismatch
//   sideEffects: none (read-only access to AppState.users + AppState.renamed)
// post_login:end
pub async fn post_login(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<LoginRequest>,
) -> Result<Json<Value>, HsError> {
    // ── Application-service login (the agent socket, Case 2) ────────────────
    // type=m.login.application_service authenticates with the AS Bearer (NOT a
    // user token — extract_caller cannot be used) and mints a session for the
    // NAMED user without any password. Restricted to the AS's namespace, so a
    // valid AS token is never a password-bypass for human accounts. Workers-as-
    // devices: body.device_id names the worker's device; absent → generated.
    if body.login_type.as_deref() == Some("m.login.application_service") {
        let cfg = state.appservice.as_ref().ok_or_else(|| {
            HsError::Forbidden("application service logins are not enabled".to_string())
        })?;
        let bearer = crate::auth::bearer_token(&headers).ok_or_else(|| {
            HsError::Forbidden("application service login requires a Bearer token".to_string())
        })?;
        if !crate::auth::constant_time_eq(bearer.as_bytes(), cfg.token.as_bytes()) {
            return Err(HsError::Forbidden("invalid application service token".to_string()));
        }

        // Target user: spec's user_id field, or the generic identifier/user.
        let target_raw = body
            .user_id
            .as_deref()
            .or(body.identifier.as_ref().and_then(|id| id.user.as_deref()))
            .or(body.user.as_deref())
            .ok_or_else(|| {
                HsError::BadRequest("user_id is required for m.login.application_service".to_string())
            })?;
        let target = crate::state::localpart(target_raw);

        // Namespace: the AS may mint sessions ONLY for its own localparts.
        if !cfg.owns_localpart(target) {
            return Err(HsError::Forbidden(format!(
                "this application service may only log in users starting with '{}'",
                cfg.prefix
            )));
        }

        let (user_id, epoch) = {
            let users = state
                .users
                .lock()
                .map_err(|e| HsError::Internal(e.to_string()))?;
            let record = users.get(target).ok_or_else(|| {
                HsError::Forbidden("unknown user".to_string())
            })?;
            (format!("@{target}:{}", state.server_name), record.epoch)
        };

        let device_id = body
            .device_id
            .clone()
            .unwrap_or_else(|| format!("AS{}", crate::auth::random_device_suffix()));
        let access_token = auth::sign_token(&state.token_secret, &user_id, &device_id, epoch);
        let server_name = &state.server_name;

        return Ok(Json(json!({
            "access_token": access_token,
            "user_id":      user_id,
            "device_id":    device_id,
            "home_server":  server_name,
            "well_known": {
                "m.homeserver": {
                    "base_url": state.base_url_for_request(
                        headers.get(axum::http::header::HOST).and_then(|v| v.to_str().ok())
                    )
                }
            }
        })));
    }

    // Resolve username from either identifier.user or legacy flat user field.
    // The client may send a bare localpart ("tester") OR a full MXID
    // ("@tester:localhost") — matrix-nio/matrix-commander send the latter.
    // Normalise to the bare localpart.
    let username_raw = body
        .identifier
        .as_ref()
        .and_then(|id| id.user.as_deref())
        .or(body.user.as_deref())
        .unwrap_or("user");
    let username = crate::state::localpart(username_raw);

    let host = headers
        .get(axum::http::header::HOST)
        .and_then(|v| v.to_str().ok());
    let base_url = state.base_url_for_request(host);

    // ── Check renamed map first ───────────────────────────────────────────────
    // If this username was renamed (lost a grow-set conflict), the account lives
    // under a different localpart.  Verify password against the new record and
    // return the new user_id so the client updates its stored identity.
    let maybe_renamed_user_id: Option<String> = {
        let renamed = state
            .renamed
            .lock()
            .map_err(|e| HsError::Internal(e.to_string()))?;
        renamed.get(username).cloned()
    };

    if let Some(ref new_user_id) = maybe_renamed_user_id {
        // Resolve the new localpart from "@<new_localpart>:<server>".
        let new_localpart = new_user_id
            .strip_prefix('@')
            .and_then(|s| s.split(':').next())
            .unwrap_or(username);

        let users = state
            .users
            .lock()
            .map_err(|e| HsError::Internal(e.to_string()))?;

        let (device_id, hash, user_id, epoch) = if let Some(record) = users.get(new_localpart) {
            let supplied = body.password.as_deref().unwrap_or("");
            if !auth::verify_password(&record.password_hash, supplied) {
                return Err(HsError::Forbidden("invalid password".to_string()));
            }
            (
                record.device_id.clone(),
                record.password_hash.clone(),
                new_user_id.clone(),
                record.epoch,
            )
        } else {
            // Renamed but new record not found locally (shouldn't happen) — reject.
            return Err(HsError::Forbidden("unknown user".to_string()));
        };
        drop(users);
        let _ = hash;

        let access_token = auth::sign_token(&state.token_secret, &user_id, &device_id, epoch);
        let server_name = &state.server_name;

        return Ok(Json(json!({
            "access_token": access_token,
            "user_id":      user_id,
            "device_id":    device_id,
            "home_server":  server_name,
            "well_known": {
                "m.homeserver": {
                    "base_url": base_url
                }
            }
        })));
    }

    // ── Password validation for registered users ──────────────────────────────
    // If the username exists in the local store, validate the password.
    // If not registered, return 403 M_FORBIDDEN.
    let (device_id, user_id, epoch): (String, String, u32) = {
        let users = state
            .users
            .lock()
            .map_err(|e| HsError::Internal(e.to_string()))?;

        if let Some(record) = users.get(username) {
            // Registered user — verify password with Argon2.
            let supplied = body.password.as_deref().unwrap_or("");
            if !auth::verify_password(&record.password_hash, supplied) {
                return Err(HsError::Forbidden("invalid password".to_string()));
            }
            let uid = format!("@{username}:{}", state.server_name);
            (record.device_id.clone(), uid, record.epoch)
        } else {
            // Unregistered — reject.
            return Err(HsError::Forbidden("unknown user".to_string()));
        }
    };

    let access_token = auth::sign_token(&state.token_secret, &user_id, &device_id, epoch);
    let server_name = &state.server_name;

    Ok(Json(json!({
        "access_token": access_token,
        "user_id":      user_id,
        "device_id":    device_id,
        "home_server":  server_name,
        "well_known": {
            "m.homeserver": {
                "base_url": base_url
            }
        }
    })))
}
