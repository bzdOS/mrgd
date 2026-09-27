// START_AI_HEADER
// MODULE: matrix-hs/src/routes/register.rs
// PURPOSE: POST /_matrix/client/v3/register  — User-Interactive Auth (UIA) registration.
//          GET  /_matrix/client/v3/register/available — username availability check.
//
//          UIA handshake (two-step):
//            Step 1 (no auth field): return HTTP 401 with UIA challenge
//                   {"flows":[{"stages":["m.login.dummy"]}],"params":{},"session":"uia_<N>"}
//            Step 2 (auth.type="m.login.dummy"): validate + insert user → 200
//
//          Username charset: [a-z0-9._=\-/]+ (lowercase; see is_valid_username).
//          Guest registration (?kind=guest): always 403 M_FORBIDDEN.
//
//          Barrier wiring (cluster-wide username uniqueness):
//            If AppState.barrier_store is Some(store), the registration success path calls
//            crate::substrate::barrier::claim() BEFORE the local insert, using Policy::Optimistic:
//              ClaimOutcome::Claimed       → proceed with local insert (provisional=false).
//              ClaimOutcome::Rejected      → return 400 M_USER_IN_USE (same as local-dup).
//              ClaimOutcome::Provisional   → proceed with local insert (provisional=true);
//                                           log a note that reconcile is deferred on heal.
//              Err(_)                      → return 500 M_UNKNOWN (real store fault).
//            If barrier_store is None (single-node default) → local uniqueness only,
//            all existing tests pass unchanged.
//            See docs/DESIGN.md.6 for the full barrier model and deferred
//            multi-node claim-routing / consensus-backed CAS milestones.
//
// DEPENDENCIES: axum, serde, serde_json, AppState, crate::substrate::barrier
// PUBLIC_API: post_register, get_register_available
// END_AI_HEADER

use crate::state::AppState;
use axum::{
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use crate::substrate::barrier::{self, ClaimOutcome, Fence, Policy};
use serde::Deserialize;
use serde_json::json;
use std::sync::atomic::Ordering;
use std::sync::Arc;

// ── Query params ──────────────────────────────────────────────────────────────

// RegisterQuery:start
//   purpose: Parse the ?kind= query parameter on POST /register.
//            Only "guest" is inspected; anything else (including absent) is treated as
//            a normal user registration.
//   input:  URL query string
//   output: RegisterQuery value
//   sideEffects: none
// RegisterQuery:end
#[derive(Debug, Deserialize, Default)]
pub struct RegisterQuery {
    pub kind: Option<String>,
}

// AvailableQuery:start
//   purpose: Parse the ?username= query parameter on GET /register/available.
//   input:  URL query string
//   output: AvailableQuery value
//   sideEffects: none
// AvailableQuery:end
#[derive(Debug, Deserialize)]
pub struct AvailableQuery {
    pub username: Option<String>,
}

// ── Request body ──────────────────────────────────────────────────────────────

// RegisterBody:start
//   purpose: Deserialise the JSON body of POST /register.
//            All fields are optional to support both the initial call (no auth) and
//            the second call (with auth).
//   input:  JSON body from client
//   output: RegisterBody value
//   sideEffects: none
// RegisterBody:end
#[derive(Debug, Deserialize, Default)]
pub struct RegisterBody {
    pub username: Option<String>,
    pub password: Option<String>,
    pub auth: Option<AuthBlock>,
    pub inhibit_login: Option<bool>,
    pub device_id: Option<String>,
    pub initial_device_display_name: Option<String>,
    /// Invite/shared-secret gate (internal-task): must match AppState.registration_shared_secret
    /// when the server has one configured. Ignored (registration stays open) when the
    /// server has no shared secret set — see `AppState.registration_shared_secret`.
    pub registration_secret: Option<String>,
}

// AuthBlock:start
//   purpose: The auth sub-object inside a UIA request body.
//            type: the auth stage being completed (e.g. "m.login.dummy").
//            session: the UIA session ID returned in the 401 challenge.
//   input:  JSON auth sub-object
//   output: AuthBlock value
//   sideEffects: none
// AuthBlock:end
#[derive(Debug, Deserialize)]
pub struct AuthBlock {
    #[serde(rename = "type")]
    pub auth_type: Option<String>,
    pub session: Option<String>,
}

// ── Username validation ───────────────────────────────────────────────────────

// is_valid_username:start
//   purpose: Check that a username localpart contains only allowed characters.
//            Allowed: [a-z0-9._=\-/]+  (lowercase letters, digits, and ._=-/)
//            Empty strings are rejected.
//            Matrix spec §User Identifiers allows this charset for localparts.
//   input:  s — candidate localpart string
//   output: bool — true if valid
//   sideEffects: none
// is_valid_username:end
fn is_valid_username(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }
    s.chars()
        .all(|c| matches!(c, 'a'..='z' | '0'..='9' | '.' | '_' | '=' | '-' | '/'))
}

// ── UIA challenge helper ──────────────────────────────────────────────────────

// uia_challenge:start
//   purpose: Build the HTTP 401 UIA challenge response for POST /register.
//            The body is NOT an HsError shape — it is the Matrix UIA document
//            {"flows":[{"stages":["m.login.dummy"]}],"params":{},"session":"<id>"}.
//            This is built as an explicit (StatusCode, Json<Value>) pair so the
//            Content-Type stays application/json and the 401 body is correct.
//   input:  session_id — string ID to embed in the challenge
//   output: Response (axum IntoResponse)
//   sideEffects: none
// uia_challenge:end
fn uia_challenge(session_id: String) -> Response {
    (
        StatusCode::UNAUTHORIZED,
        Json(json!({
            "flows":   [{ "stages": ["m.login.dummy"] }],
            "params":  {},
            "session": session_id,
        })),
    )
        .into_response()
}

// ── Handlers ──────────────────────────────────────────────────────────────────

// post_register:start
//   purpose: POST /_matrix/client/v3/register — UIA-gated account creation.
//
//            Flow:
//              1. ?kind=guest → immediate 403 M_FORBIDDEN (guest registration disabled).
//              1b. If AppState.registration_shared_secret is Some, body.registration_secret
//                  must match (constant-time compare) → else 403 M_FORBIDDEN, before any
//                  UIA session is issued (internal-task invite/shared-secret gate). None configured
//                  → skipped entirely (open registration, pre-existing behaviour).
//              2. No auth field → issue UIA session, return 401 challenge.
//              3. auth.type="m.login.dummy" + known session:
//                   a. Validate username charset → 400 M_INVALID_USERNAME.
//                   b. Missing username → 400 M_MISSING_PARAM.
//                   c. Username taken → 400 M_USER_IN_USE.
//                   d. Consume session, insert user, return 200.
//              4. Unknown auth type or unknown session → re-issue 401 challenge.
//
//            Local uniqueness only; cluster-wide barrier deferred.
//            See AppState::register_user for the coordination barrier note.
//   input:  State(AppState), Query<?kind=>, optional JSON body
//   output: 200 {"user_id","device_id","home_server"[,"access_token"]}
//           401 UIA challenge
//           400/403 Matrix error JSON
//   sideEffects: may insert into AppState.users + AppState.uia_sessions
// post_register:end
pub async fn post_register(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<RegisterQuery>,
    body: Option<Json<RegisterBody>>,
) -> Response {
    // ── 1. Guest registration disabled ──────────────────────────────────────
    if query.kind.as_deref() == Some("guest") {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({
                "errcode": "M_FORBIDDEN",
                "error":   "guest registration is disabled on this server",
            })),
        )
            .into_response();
    }

    let body = body.map(|b| b.0).unwrap_or_default();

    // ── 1a. Application-service path (the agent socket, Case 2) ────────────
    // A Bearer on /register is never a user credential (normal registration is
    // unauthenticated); if this server has an AS configured, ANY bearer here is
    // read as an AS credential. Valid → UIA-free, invite-gate-free registration
    // under the AS namespace (the token IS the authorisation). Invalid → 403,
    // never a UIA challenge — an AS client cannot answer one. No AS configured
    // → the header is ignored and everything below is byte-for-byte as before.
    let as_authorized = match state.appservice.as_ref() {
        Some(cfg) => {
            match crate::auth::bearer_token(&headers) {
                Some(bearer) => {
                    if !crate::auth::constant_time_eq(
                        bearer.as_bytes(),
                        cfg.token.as_bytes(),
                    ) {
                        return (
                            StatusCode::FORBIDDEN,
                            Json(json!({
                                "errcode": "M_FORBIDDEN",
                                "error":   "invalid application service token",
                            })),
                        )
                            .into_response();
                    }
                    true
                }
                None => false,
            }
        }
        None => false,
    };

    // ── 1b. Invite/shared-secret gate (internal-task) ─────────────────────────────────
    // When the server has a registration_shared_secret configured, every call
    // (both the challenge probe and the completing call) must echo it back —
    // checked BEFORE a UIA session is issued, so an unauthorised caller never
    // gets as far as a session token. No secret configured → unchanged (open)
    // behaviour, so all pre-existing deployments/tests are unaffected.
    // AS-authorized calls skip this gate: the AS token is a strictly stronger
    // authorisation than the invite secret, and AS clients do not send it.
    if !as_authorized {
        if let Some(expected) = state.registration_shared_secret.as_deref() {
            let provided = body.registration_secret.as_deref().unwrap_or("");
            if !crate::auth::constant_time_eq(provided.as_bytes(), expected.as_bytes()) {
                return (
                    StatusCode::FORBIDDEN,
                    Json(json!({
                        "errcode": "M_FORBIDDEN",
                        "error":   "registration on this server requires a valid registration_secret",
                    })),
                )
                    .into_response();
            }
        }
    }

    // ── 2. No auth block → issue UIA challenge (never for an AS call) ───────
    let auth = if as_authorized {
        // The AS path carries no UIA session; skip challenge + validation.
        AuthBlock {
            auth_type: Some("m.login.application.service".to_string()),
            session: None,
        }
    } else {
        match body.auth {
            None => {
                let session_id = match state.issue_uia_session() {
                    Ok(id) => id,
                    Err(e) => {
                        return (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(json!({ "errcode": "M_UNKNOWN", "error": e })),
                        )
                            .into_response();
                    }
                };
                return uia_challenge(session_id);
            }
            Some(a) => a,
        }
    };

    // ── 3. Validate auth stage ───────────────────────────────────────────────
    let auth_type = auth.auth_type.as_deref().unwrap_or("");
    let session_id = auth.session.as_deref().unwrap_or("");

    if !as_authorized {
        if auth_type != "m.login.dummy" {
            // Unknown stage — re-challenge with a fresh session.
            let sid = match state.issue_uia_session() {
                Ok(id) => id,
                Err(e) => {
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({ "errcode": "M_UNKNOWN", "error": e })),
                    )
                        .into_response();
                }
            };
            return uia_challenge(sid);
        }

        // Verify the session was issued by us.
        let session_valid = match state.consume_uia_session(session_id) {
            Ok(v) => v,
            Err(e) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({ "errcode": "M_UNKNOWN", "error": e })),
                )
                    .into_response();
            }
        };

        if !session_valid {
            // Session unknown — re-challenge.
            let sid = match state.issue_uia_session() {
                Ok(id) => id,
                Err(e) => {
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({ "errcode": "M_UNKNOWN", "error": e })),
                    )
                        .into_response();
                }
            };
            return uia_challenge(sid);
        }
    }

    // ── 4. Username validation ───────────────────────────────────────────────
    let username = match body.username.as_deref() {
        Some(u) => u,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "errcode": "M_MISSING_PARAM",
                    "error":   "username is required",
                })),
            )
                .into_response();
        }
    };

    if !is_valid_username(username) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "errcode": "M_INVALID_USERNAME",
                "error":   "username contains invalid characters (allowed: [a-z0-9._=\\-/]+)",
            })),
        )
            .into_response();
    }

    // AS namespace: the service may ONLY create localparts under its prefix —
    // a valid AS token must not be a shortcut to arbitrary usernames.
    if as_authorized {
        let cfg = state.appservice.as_ref().expect("as_authorized implies configured");
        if !cfg.owns_localpart(username) {
            return (
                StatusCode::FORBIDDEN,
                Json(json!({
                    "errcode": "M_EXCLUSIVE",
                    "error":   format!("this application service may only register usernames starting with '{}'", cfg.prefix),
                })),
            )
                .into_response();
        }
    }

    // AS-created accounts have no password any client could present — their
    // only authentication is the AS token (see auth::unusable_password_hash).
    let password_hash = if as_authorized {
        crate::auth::unusable_password_hash()
    } else {
        let password_raw = body.password.as_deref().unwrap_or("");
        match crate::auth::hash_password(password_raw) {
            Ok(h) => h,
            Err(e) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(
                        json!({ "errcode": "M_UNKNOWN", "error": format!("password hash error: {e}") }),
                    ),
                )
                    .into_response();
            }
        }
    };
    let device_id = body.device_id.as_deref().unwrap_or("DEVICE1").to_string();

    // ── 5. Barrier claim + register ───────────────────────────────────────────
    //
    // If a barrier_store is configured (cluster mode), run crate::substrate::barrier::claim()
    // BEFORE the local insert to achieve cluster-wide at-most-once ownership.
    // If barrier_store is None (single-node mode), skip to local insert — preserving
    // all existing test behaviour.
    let provisional: bool;

    if let Some(ref store) = state.barrier_store {
        // Build a Fence for this claim attempt.
        // epoch: bump via uia_seq (already a monotonic counter on this node).
        // ts: milliseconds since UNIX epoch via SystemTime (real clock, Matrix-hs is a server).
        // node_id: MATRIX_HS_NODE_ID env if set, else server_name.
        let epoch = state.uia_seq.fetch_add(1, Ordering::Relaxed);
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let node_id =
            std::env::var("MATRIX_HS_NODE_ID").unwrap_or_else(|_| state.server_name.clone());

        let fence = Fence { epoch, ts, node_id };
        let claim_key = format!("mx:username:{username}");
        let user_id_str = format!("@{username}:{}", state.server_name);

        match barrier::claim(
            store.as_ref(),
            &claim_key,
            &user_id_str,
            fence,
            Policy::Optimistic,
        ) {
            Ok(ClaimOutcome::Claimed) => {
                // CP path: coordinator confirmed at-most-once ownership.
                provisional = false;
            }
            Ok(ClaimOutcome::Rejected { owner }) => {
                // Coordinator confirmed key is already owned.
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({
                        "errcode": "M_USER_IN_USE",
                        "error":   format!("username '{}' is already registered (barrier owner: {})", username, owner),
                    })),
                )
                    .into_response();
            }
            Ok(ClaimOutcome::Provisional { fence: pf }) => {
                // AP path: coordinator unreachable, optimistic grant.
                // Proceed with local insert; mark provisional for future reconcile.
                // Reconcile-on-heal is modeled by crate::substrate::barrier::reconcile() but is
                // NOT yet driven by any heal path — deferred ([see docs/DESIGN.md]).
                eprintln!(
                    "matrix-hs register: provisional claim for '{}' (fence ts={} node={}); \
                     reconcile deferred until partition heal",
                    username, pf.ts, pf.node_id
                );
                provisional = true;
            }
            Err(e) => {
                // Real store fault (not unavailable — Optimistic policy returns Provisional
                // on Unavailable, so Err here means a genuine error).
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({
                        "errcode": "M_UNKNOWN",
                        "error":   format!("barrier store error: {e}"),
                    })),
                )
                    .into_response();
            }
        }
    } else {
        // Single-node mode: no barrier store — local uniqueness only.
        provisional = false;
    }

    // Local insert (after barrier claim, or directly in single-node mode).
    if state
        .register_user(username, &password_hash, &device_id, provisional)
        .is_err()
    {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "errcode": "M_USER_IN_USE",
                "error":   format!("username '{}' is already registered on this server", username),
            })),
        )
            .into_response();
    }
    // Persist the new user registration (best-effort — after successful uniqueness check).
    state.persist_user(username, &password_hash, &device_id);

    // ── 6. Build success response ─────────────────────────────────────────────
    let user_id = format!("@{}:{}", username, state.server_name);
    let server_name = &state.server_name;

    // Device-list change tracking (E2EE device-lists feature): a new account means a
    // new device, which is a device-list change for anyone who later shares a room
    // with this user. Record it and gossip cross-node (mirrors the keys/upload hook
    // in routes/keys.rs — same AppState::mark_device_list_changed call).
    let _ = state.mark_device_list_changed(&user_id);
    #[cfg(feature = "cluster")]
    let _ = crate::routes::keys::gossip_device_list_change(&state, &user_id).await;

    let inhibit = body.inhibit_login.unwrap_or(false);

    let mut resp_body = json!({
        "user_id":     user_id,
        "home_server": server_name,
    });

    if !inhibit {
        let access_token = crate::auth::sign_token(&state.token_secret, &user_id, &device_id, 0);
        let obj = resp_body.as_object_mut().expect("json object");
        obj.insert("access_token".to_string(), json!(access_token));
        obj.insert("device_id".to_string(), json!(device_id));
    }

    (StatusCode::OK, Json(resp_body)).into_response()
}

// get_register_available:start
//   purpose: GET /_matrix/client/v3/register/available?username=X
//            Check whether a username is available for registration on this node.
//            Bad charset → 400 M_INVALID_USERNAME.
//            Taken       → 400 M_USER_IN_USE.
//            Free        → 200 {"available":true}.
//            Local check only — does not coordinate with other cluster nodes.
//   input:  State(AppState), Query(?username=X)
//   output: 200 {"available":true} | 400 Matrix error JSON
//   sideEffects: none (read-only)
// get_register_available:end
pub async fn get_register_available(
    State(state): State<Arc<AppState>>,
    Query(query): Query<AvailableQuery>,
) -> Response {
    let username = match query.username.as_deref() {
        Some(u) => u,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "errcode": "M_MISSING_PARAM",
                    "error":   "username query parameter is required",
                })),
            )
                .into_response();
        }
    };

    if !is_valid_username(username) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "errcode": "M_INVALID_USERNAME",
                "error":   "username contains invalid characters (allowed: [a-z0-9._=\\-/]+)",
            })),
        )
            .into_response();
    }

    let taken = match state.users.lock() {
        Ok(guard) => guard.contains_key(username),
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "errcode": "M_UNKNOWN", "error": e.to_string() })),
            )
                .into_response();
        }
    };

    if taken {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "errcode": "M_USER_IN_USE",
                "error":   format!("username '{}' is already registered on this server", username),
            })),
        )
            .into_response();
    }

    (StatusCode::OK, Json(json!({ "available": true }))).into_response()
}
