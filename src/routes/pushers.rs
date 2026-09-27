// START_AI_HEADER
// MODULE: matrix-hs/src/routes/pushers.rs
// PURPOSE: Pusher registration endpoints — POST /_matrix/client/v3/pushers/set and
//          GET /_matrix/client/v3/pushers. Replaces the old no-op stub
//          (routes::stubs::get_pushers, which always returned {"pushers":[]}).
//          Storage lives in AppState.push.pushers (state.rs); the actual outbound
//          notification POST to a pusher's data.url happens in routes/push.rs,
//          triggered from routes/send.rs when a message event is appended.
// DEPENDENCIES: axum, serde_json, AppState, auth
// PUBLIC_API: post_pushers_set, get_pushers
// END_AI_HEADER

use axum::{extract::State, http::HeaderMap, Json};
use serde_json::{json, Value};
use std::sync::Arc;

use super::keys::extract_caller;
use crate::{
    error::HsError,
    state::{AppState, PusherRecord},
};

// post_pushers_set:start
//   purpose: POST /_matrix/client/v3/pushers/set — register, update, or delete a
//            pusher for the authenticated user.
//            Per the Matrix spec: `kind: null` in the body means DELETE the pusher
//            identified by (app_id, pushkey) — no other fields are required in that
//            case. Any other `kind` (e.g. "http") registers/updates the pusher,
//            requiring app_id, pushkey, app_display_name, device_display_name, lang,
//            and data (opaque object; data.url is read later by routes/push.rs).
//   input:  Authorization: Bearer <signed mxt_ token>;
//           JSON body {"app_id","pushkey","kind","app_display_name",
//                      "device_display_name","lang","data":{"url","format"?}}
//   output: 200 {} on success;
//           401 M_UNKNOWN_TOKEN on missing/invalid token;
//           400 M_MISSING_PARAM if app_id/pushkey are absent, or (for non-null kind)
//           app_display_name/device_display_name/lang/data.url are absent
//   sideEffects: inserts into or removes from state.push.pushers
// post_pushers_set:end
pub async fn post_pushers_set(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Option<Json<Value>>,
) -> Result<Json<Value>, HsError> {
    let (user_id, _device_id) = extract_caller(&headers, &state)
        .ok_or_else(|| HsError::UnknownToken("missing or invalid token".to_string()))?;

    let body = body.map(|b| b.0).unwrap_or_else(|| json!({}));

    let app_id = body
        .get("app_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| HsError::BadRequest("missing app_id".to_string()))?
        .to_string();
    let pushkey = body
        .get("pushkey")
        .and_then(|v| v.as_str())
        .ok_or_else(|| HsError::BadRequest("missing pushkey".to_string()))?
        .to_string();

    // kind:null (explicit JSON null, or the key present-but-null) deletes the pusher.
    // A body that OMITS "kind" entirely is treated as a registration attempt (kind
    // is required by spec in that case) — .get() returns None for an absent key,
    // which we distinguish from Some(Value::Null) below.
    let kind_present = body.get("kind");
    let is_delete = matches!(kind_present, Some(Value::Null));

    if is_delete {
        state
            .delete_pusher(&user_id, &app_id, &pushkey)
            .map_err(HsError::Internal)?;
        return Ok(Json(json!({})));
    }

    let kind = kind_present
        .and_then(|v| v.as_str())
        .ok_or_else(|| HsError::BadRequest("missing kind".to_string()))?
        .to_string();
    let app_display_name = body
        .get("app_display_name")
        .and_then(|v| v.as_str())
        .ok_or_else(|| HsError::BadRequest("missing app_display_name".to_string()))?
        .to_string();
    let device_display_name = body
        .get("device_display_name")
        .and_then(|v| v.as_str())
        .ok_or_else(|| HsError::BadRequest("missing device_display_name".to_string()))?
        .to_string();
    let lang = body
        .get("lang")
        .and_then(|v| v.as_str())
        .ok_or_else(|| HsError::BadRequest("missing lang".to_string()))?
        .to_string();
    let data = body
        .get("data")
        .cloned()
        .ok_or_else(|| HsError::BadRequest("missing data".to_string()))?;
    if data.get("url").and_then(|v| v.as_str()).is_none() {
        return Err(HsError::BadRequest("missing data.url".to_string()));
    }

    let record = PusherRecord {
        app_id,
        pushkey,
        kind,
        app_display_name,
        device_display_name,
        lang,
        data,
        pushkey_ts: crate::state::now_ms(),
    };
    state
        .set_pusher(&user_id, record)
        .map_err(HsError::Internal)?;

    Ok(Json(json!({})))
}

// get_pushers:start
//   purpose: GET /_matrix/client/v3/pushers — list every pusher registered for the
//            authenticated caller.
//   input:  Authorization: Bearer <signed mxt_ token>
//   output: JSON {"pushers":[{"app_id","pushkey","kind","app_display_name",
//                              "device_display_name","lang","data"},...]}
//           401 M_UNKNOWN_TOKEN on missing/invalid token
//   sideEffects: none (read-only)
// get_pushers:end
pub async fn get_pushers(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<Value>, HsError> {
    let (user_id, _device_id) = extract_caller(&headers, &state)
        .ok_or_else(|| HsError::UnknownToken("missing or invalid token".to_string()))?;

    let pushers: Vec<Value> = state
        .pushers_for_user(&user_id)
        .into_iter()
        .map(|p| {
            json!({
                "app_id":              p.app_id,
                "pushkey":             p.pushkey,
                "kind":                p.kind,
                "app_display_name":    p.app_display_name,
                "device_display_name": p.device_display_name,
                "lang":                p.lang,
                "data":                p.data,
            })
        })
        .collect();

    Ok(Json(json!({ "pushers": pushers })))
}
