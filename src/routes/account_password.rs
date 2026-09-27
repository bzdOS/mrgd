// START_AI_HEADER
// MODULE: matrix-hs/src/routes/account_password.rs
// PURPOSE: POST /_matrix/client/v3/account/password — User-Interactive Auth (UIA)
//          gated self-service password change. Was previously advertised as
//          disabled ({"m.change_password":{"enabled":false}} in routes/account.rs
//          get_capabilities) with no endpoint at all.
//
//          UIA handshake (two-step, mirrors routes/register.rs's m.login.dummy
//          flow, but with an m.login.password stage that re-verifies the
//          CALLER'S CURRENT password — this is the one property that actually
//          matters here: proving you still know the old password before being
//          allowed to set a new one):
//            Step 1 (no auth field): return HTTP 401 with UIA challenge
//                   {"flows":[{"stages":["m.login.password"]}],"params":{},"session":"uia_<N>"}
//            Step 2 (auth.type="m.login.password", auth.password=<current password>):
//                   verify caller's Bearer token identifies a registered local user,
//                   verify auth.password against that user's STORED hash (not the
//                   token — the token only proves session identity, not that the
//                   caller still knows the password), then hash+store new_password.
//
//          SEAM (documented, not fixed here): `logout_devices` (Matrix spec default
//          true) is accepted in the request body but NOT actually honored — this
//          server's access tokens (auth::sign_token/verify_token) are stateless
//          self-verifying HMACs with no revocation list or per-user epoch, so
//          there is currently no mechanism to invalidate any previously-issued
//          token for this user. A password change does NOT log out existing
//          sessions. Fixing this would need a token-revocation/epoch mechanism —
//          a separate, larger feature.
// DEPENDENCIES: axum, serde, serde_json, AppState, auth::{verify_password,hash_password}
// PUBLIC_API: post_change_password
// END_AI_HEADER

use crate::{routes::account::resolve_user_id_from_token, state::AppState};
use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;

// PasswordChangeAuthBlock:start
//   purpose: The auth sub-object inside a UIA password-change request body.
//            type: the auth stage being completed ("m.login.password").
//            session: the UIA session ID returned in the 401 challenge.
//            password: the caller's CURRENT plaintext password, re-verified
//            against their stored hash before the change is applied.
//   input:  JSON auth sub-object
//   output: PasswordChangeAuthBlock value
//   sideEffects: none
// PasswordChangeAuthBlock:end
#[derive(Debug, Deserialize)]
pub struct PasswordChangeAuthBlock {
    #[serde(rename = "type")]
    pub auth_type: Option<String>,
    pub session: Option<String>,
    pub password: Option<String>,
}

// PasswordChangeBody:start
//   purpose: Deserialise the JSON body of POST /account/password.
//            All fields optional to support both the initial (no auth) call and
//            the completing call.
//   input:  JSON body from client
//   output: PasswordChangeBody value
//   sideEffects: none
// PasswordChangeBody:end
#[derive(Debug, Deserialize, Default)]
pub struct PasswordChangeBody {
    pub new_password: Option<String>,
    pub auth: Option<PasswordChangeAuthBlock>,
    /// Accepted for spec compatibility; NOT honored — see module header SEAM note.
    pub logout_devices: Option<bool>,
}

// uia_challenge:start
//   purpose: Build the HTTP 401 UIA challenge response for POST /account/password.
//   input:  session_id — string ID to embed in the challenge
//   output: Response (axum IntoResponse)
//   sideEffects: none
// uia_challenge:end
fn uia_challenge(session_id: String) -> Response {
    (
        StatusCode::UNAUTHORIZED,
        Json(json!({
            "flows":   [{ "stages": ["m.login.password"] }],
            "params":  {},
            "session": session_id,
        })),
    )
        .into_response()
}

fn error_response(status: StatusCode, errcode: &str, error: &str) -> Response {
    (status, Json(json!({ "errcode": errcode, "error": error }))).into_response()
}

// post_change_password:start
//   purpose: POST /_matrix/client/v3/account/password — UIA-gated self-service
//            password change for the AUTHENTICATED caller's own account.
//
//            Flow:
//              1. Bearer token required → 401 M_UNKNOWN_TOKEN if missing/invalid.
//              2. No auth field → issue UIA session, return 401 challenge
//                 (stage m.login.password).
//              3. auth.type="m.login.password" + known session:
//                   a. auth.password must verify against the caller's CURRENT
//                      stored password_hash → 403 M_FORBIDDEN otherwise (does not
//                      distinguish "wrong password" from "unknown user" in the
//                      error message, matching login.rs's posture).
//                   b. new_password missing/empty → 400 M_MISSING_PARAM.
//                   c. Hash new_password, overwrite the in-memory UserRecord and
//                      append a new accounts.jsonl record (persist_user is
//                      last-write-wins per localpart — see persist.rs
//                      replay_accounts doc comment).
//              4. Unknown auth type or unknown session → re-issue 401 challenge.
//
//            logout_devices is accepted but not honored — see module header SEAM.
//   input:  State(AppState), headers (Authorization: Bearer), JSON body
//   output: 200 {} on success; 401 UIA challenge or M_UNKNOWN_TOKEN;
//           400/403 Matrix error JSON
//   sideEffects: may mutate AppState.users + AppState.uia_sessions; appends to
//                accounts.jsonl when persistence is enabled
// post_change_password:end
pub async fn post_change_password(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Option<Json<PasswordChangeBody>>,
) -> Response {
    let token = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "));

    let user_id = match token
        .and_then(|t| resolve_user_id_from_token(t, &state.token_secret, &state.server_name))
    {
        Some(uid) => uid,
        None => {
            return error_response(
                StatusCode::UNAUTHORIZED,
                "M_UNKNOWN_TOKEN",
                "missing or invalid token",
            )
        }
    };
    let localpart = user_id
        .strip_prefix('@')
        .and_then(|s| s.split(':').next())
        .unwrap_or(&user_id)
        .to_string();

    let body = body.map(|b| b.0).unwrap_or_default();

    // ── No auth block → issue UIA challenge ─────────────────────────────────
    let auth = match body.auth {
        None => {
            return match state.issue_uia_session() {
                Ok(id) => uia_challenge(id),
                Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, "M_UNKNOWN", &e),
            };
        }
        Some(a) => a,
    };

    let auth_type = auth.auth_type.as_deref().unwrap_or("");
    let session_id = auth.session.as_deref().unwrap_or("");

    if auth_type != "m.login.password" {
        return match state.issue_uia_session() {
            Ok(id) => uia_challenge(id),
            Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, "M_UNKNOWN", &e),
        };
    }

    let session_valid = match state.consume_uia_session(session_id) {
        Ok(v) => v,
        Err(e) => return error_response(StatusCode::INTERNAL_SERVER_ERROR, "M_UNKNOWN", &e),
    };
    if !session_valid {
        return match state.issue_uia_session() {
            Ok(id) => uia_challenge(id),
            Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, "M_UNKNOWN", &e),
        };
    }

    // ── Verify the supplied current password against the stored hash ────────
    let current_password = auth.password.as_deref().unwrap_or("");
    let (current_hash, device_id) = {
        let users = match state.users.lock() {
            Ok(g) => g,
            Err(e) => {
                return error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "M_UNKNOWN",
                    &e.to_string(),
                )
            }
        };
        match users.get(&localpart) {
            Some(rec) => (rec.password_hash.clone(), rec.device_id.clone()),
            None => {
                return error_response(
                    StatusCode::FORBIDDEN,
                    "M_FORBIDDEN",
                    "password does not match",
                )
            }
        }
    };
    if !crate::auth::verify_password(&current_hash, current_password) {
        return error_response(
            StatusCode::FORBIDDEN,
            "M_FORBIDDEN",
            "password does not match",
        );
    }

    let new_password = match body.new_password.as_deref() {
        Some(p) if !p.is_empty() => p,
        _ => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "M_MISSING_PARAM",
                "new_password is required",
            )
        }
    };
    let new_hash = match crate::auth::hash_password(new_password) {
        Ok(h) => h,
        Err(e) => {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "M_UNKNOWN",
                &format!("password hash error: {e}"),
            )
        }
    };

    {
        let mut users = match state.users.lock() {
            Ok(g) => g,
            Err(e) => {
                return error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "M_UNKNOWN",
                    &e.to_string(),
                )
            }
        };
        if let Some(rec) = users.get_mut(&localpart) {
            rec.password_hash = new_hash.clone();
        }
    }
    // Persist (last-write-wins per localpart — see persist.rs replay_accounts).
    state.persist_user(&localpart, &new_hash, &device_id);

    (StatusCode::OK, Json(json!({}))).into_response()
}
