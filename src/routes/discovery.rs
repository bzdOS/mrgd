// START_AI_HEADER
// MODULE: matrix-hs/src/routes/discovery.rs
// PURPOSE: Discovery endpoints for Matrix CS-API Stage 2.
//          GET /.well-known/matrix/client — advertise homeserver base URL.
//          GET /_matrix/client/v3/login — list supported login flows (GET variant).
// DEPENDENCIES: axum, AppState
// PUBLIC_API: get_well_known, get_login_flows
// END_AI_HEADER

use crate::state::AppState;
use axum::{extract::State, http::HeaderMap, Json};
use serde_json::{json, Value};
use std::sync::Arc;

// get_well_known:start
//   purpose: Return the .well-known Matrix client discovery document.
//            Advertises the homeserver base URL for client auto-discovery.
//   input:  State(AppState), headers (for Host header)
//   output: JSON {"m.homeserver":{"base_url":"<url>"}}
//   sideEffects: none
// get_well_known:end
pub async fn get_well_known(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Json<Value> {
    let host = headers
        .get(axum::http::header::HOST)
        .and_then(|v| v.to_str().ok());
    let base_url = state.base_url_for_request(host);

    Json(json!({
        "m.homeserver": {
            "base_url": base_url
        }
    }))
}

// get_login_flows:start
//   purpose: Return the list of supported login flows (GET variant).
//            Matrix clients query this to discover available authentication methods
//            before POSTing credentials.
//   input:  none
//   output: JSON {"flows":[{"type":"m.login.password"}]}
//   sideEffects: none
// get_login_flows:end
pub async fn get_login_flows() -> Json<Value> {
    Json(json!({
        "flows": [
            { "type": "m.login.password" }
        ]
    }))
}
