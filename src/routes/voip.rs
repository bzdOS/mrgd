// START_AI_HEADER
// MODULE: matrix-hs/src/routes/voip.rs
// PURPOSE: GET /_matrix/client/v3/voip/turnServer — hand the calling client the
//          credentials it needs to reach an (external) TURN server for WebRTC
//          NAT traversal. This is the ENTIRETY of a Matrix homeserver's VoIP
//          responsibility: the server never relays media, and the call
//          signaling (m.call.invite / m.call.candidates / m.call.answer /
//          m.call.hangup, or MSC3401 group-call to-device events) is just
//          ordinary room / to-device events that already flow through
//          routes/send.rs and routes/to_device.rs unchanged. There is nothing
//          VoIP-specific to do for signaling; only this credential handoff.
//
//          Credential scheme (standard TURN REST, coturn `use-auth-secret` +
//          `static-auth-secret`, identical to Synapse's turn_shared_secret):
//            expiry   = now_unix_secs + ttl
//            username = "<expiry>:<caller_mxid>"
//            password = base64_standard( HMAC-SHA1( shared_secret, username ) )
//          The expiry is embedded in the username so the TURN server can reject
//          stale credentials without the homeserver and TURN server sharing any
//          state beyond the secret — coordination-free, matching this project's
//          posture everywhere else.
//
//          Config comes from AppState.turn (TurnConfig, from MATRIX_HS_TURN_URIS
//          + MATRIX_HS_TURN_SHARED_SECRET + MATRIX_HS_TURN_TTL). When unset the
//          endpoint returns an empty {} object — exactly the previous stub
//          behavior, so a server with no TURN deployed simply advertises no
//          VoIP, breaking nothing.
//
//          DEPLOYMENT NOTE (not code — deliberately out of this module): this
//          endpoint is necessary but not sufficient for a working call. It is
//          inert until (1) a real TURN server (e.g. coturn) is running on a
//          publicly-reachable host with a matching static-auth-secret — NOT this
//          NAT'd edge box; a TURN relay must have a routable address — and
//          (2) both call parties run a Matrix client that actually speaks WebRTC.
// DEPENDENCIES: axum, serde_json, hmac, sha1, base64, AppState, auth
// PUBLIC_API: get_voip_turn_server
// END_AI_HEADER

use axum::{extract::State, http::HeaderMap, Json};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use hmac::{Hmac, Mac};
use serde_json::{json, Value};
use sha1::Sha1;
use std::sync::Arc;

use crate::{error::HsError, routes::account::resolve_user_id_from_token, state::AppState};

type HmacSha1 = Hmac<Sha1>;

// get_voip_turn_server:start
//   purpose: GET /_matrix/client/v3/voip/turnServer — return ephemeral TURN
//            credentials for the authenticated caller, or an empty object when
//            no TURN server is configured.
//   input:  State(AppState), Authorization: Bearer <mxt_ token> (required)
//   output: 200 {"username","password","uris","ttl"} when AppState.turn is Some;
//           200 {} when TURN is not configured (no VoIP advertised);
//           401 M_UNKNOWN_TOKEN when the Bearer token is missing/invalid
//   sideEffects: none (pure credential computation; reads the shared secret)
// get_voip_turn_server:end
pub async fn get_voip_turn_server(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<Value>, HsError> {
    // Matrix requires this endpoint to be authenticated — the caller's MXID is
    // bound into the TURN username, so an anonymous caller has no identity to mint.
    let user_id = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .and_then(|tok| resolve_user_id_from_token(tok, &state.token_secret, &state.server_name))
        .ok_or_else(|| HsError::UnknownToken("missing or invalid token".to_string()))?;

    // No TURN configured → empty response (no VoIP), unchanged from the old stub.
    let cfg = match &state.turn {
        Some(c) => c,
        None => return Ok(Json(json!({}))),
    };

    let expiry = crate::state::now_ms() / 1000 + cfg.ttl_secs;
    let username = format!("{expiry}:{user_id}");

    // password = base64( HMAC-SHA1( shared_secret, username ) ) — standard TURN
    // REST scheme. new_from_slice only errors on an impossible key length for
    // HMAC (it accepts any), so this is effectively infallible; map to 500 rather
    // than unwrap to keep the no-panic-in-prod invariant.
    let mut mac = HmacSha1::new_from_slice(cfg.shared_secret.as_bytes())
        .map_err(|e| HsError::Internal(format!("turn hmac init: {e}")))?;
    mac.update(username.as_bytes());
    let password = STANDARD.encode(mac.finalize().into_bytes());

    Ok(Json(json!({
        "username": username,
        "password": password,
        "uris":     cfg.uris.clone(),
        "ttl":      cfg.ttl_secs,
    })))
}
