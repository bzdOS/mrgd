// START_AI_HEADER
// MODULE: matrix-hs/src/routes/sliding_sync.rs
// PURPOSE: MSC4186 Simplified Sliding Sync — POST /_matrix/client/unstable/
//          org.matrix.simplified_msc3575/sync (+ /_matrix/client/v1/sync).
//          Makes matrix-hs usable by sliding-sync-only clients (Element X, modern
//          FluffyChat). Reuses the SAME model as classic sync.rs (room_timeline /
//          room_state / stream_pos); reads the local converged replica only — adds
//          nothing to the multi-master coordination path.
// DEPENDENCIES: axum, serde_json, AppState, routes::rooms::state_event_to_json
// PUBLIC_API: post_sliding_sync
// END_AI_HEADER

use crate::{auth, error::HsError, routes::rooms::state_event_to_json, state::AppState};
use axum::{
    extract::{Query, State},
    http::HeaderMap,
    Json,
};
use serde::Deserialize;
use serde_json::{json, Map, Value};
use std::sync::atomic::Ordering;
use std::sync::Arc;

// extract_caller now lives in auth.rs (canonical implementation shared across route
// modules). Derives (user_id, device_id) from the signed Bearer token, to know whose
// to-device queue to drain.
use auth::extract_caller;

#[derive(Debug, Deserialize, Default)]
pub struct SsQuery {
    pub pos: Option<String>,
    pub timeout: Option<u64>,
    pub conn_id: Option<String>,
}

fn parse_pos(pos: &Option<String>) -> Option<u64> {
    pos.as_deref()
        .and_then(|s| s.strip_prefix('s'))
        .and_then(|n| n.parse::<u64>().ok())
}

// required_state spec matcher: each entry is [event_type, state_key]; "*" is wildcard.
fn state_matches(specs: &[Value], ev_type: &str, ev_key: &str) -> bool {
    specs.iter().any(|pair| {
        let t = pair.get(0).and_then(|v| v.as_str()).unwrap_or("");
        let k = pair.get(1).and_then(|v| v.as_str()).unwrap_or("");
        (t == "*" || t == ev_type) && (k == "*" || k == ev_key)
    })
}

// post_sliding_sync:start
//   purpose: MSC4186 simplified sliding sync. Returns all joined rooms (windowing is
//            a no-op for a small personal server) with timeline (last timeline_limit
//            message events), required_state filtered by the request specs, and a pos
//            token = "s<stream_pos>" (same token space as classic /sync).
//   input:  State(AppState), Query(pos,timeout,conn_id), optional JSON body (lists,
//           room_subscriptions, extensions).
//   output: JSON MSC4186 response {pos, lists, rooms, extensions}
//   sideEffects: (cluster) drains Zenoh inbox; may long-poll up to timeout ms
// post_sliding_sync:end
pub async fn post_sliding_sync(
    State(state): State<Arc<AppState>>,
    Query(q): Query<SsQuery>,
    headers: HeaderMap,
    body: Option<Json<Value>>,
) -> Result<Json<Value>, HsError> {
    let req = body.map(|Json(v)| v).unwrap_or_else(|| json!({}));

    #[cfg(feature = "cluster")]
    crate::routes::sync::drain_all_cluster(&state).await?;

    let since_pos = parse_pos(&q.pos);
    let timeout_ms = q.timeout.unwrap_or(0);

    if since_pos.is_some() && timeout_ms > 0 {
        let current = state.stream_pos.load(Ordering::SeqCst);
        if current <= since_pos.unwrap_or(0) {
            let wait_ms = timeout_ms.min(30_000);
            let notified = state.notify.notified();
            let _ = tokio::time::timeout(std::time::Duration::from_millis(wait_ms), notified).await;
            #[cfg(feature = "cluster")]
            crate::routes::sync::drain_all_cluster(&state).await?;
        }
    }

    let current_pos = state.stream_pos.load(Ordering::SeqCst);
    let initial = since_pos.is_none();

    // SECURITY: build_rooms below is scoped to this caller's own joined rooms only —
    // see its header comment. An unauthenticated request gets no rooms at all. Moved
    // up from further below (where a second, now-removed extract_caller call used to
    // live) so build_rooms can use it too.
    let caller = extract_caller(&headers, &state);

    let lists_req = req.get("lists").and_then(|v| v.as_object());

    // Union required_state across lists; timeline_limit = max requested (default 20).
    let mut specs: Vec<Value> = Vec::new();
    let mut timeline_limit: usize = 20;
    if let Some(lists) = lists_req {
        for (_n, l) in lists {
            if let Some(rs) = l.get("required_state").and_then(|v| v.as_array()) {
                specs.extend(rs.iter().cloned());
            }
            if let Some(tl) = l.get("timeline_limit").and_then(|v| v.as_u64()) {
                timeline_limit = timeline_limit.max(tl as usize);
            }
        }
    }
    if specs.is_empty() {
        specs = vec![
            json!(["m.room.create", ""]),
            json!(["m.room.name", ""]),
            json!(["m.room.topic", ""]),
            json!(["m.room.avatar", ""]),
            json!(["m.room.canonical_alias", ""]),
            json!(["m.room.join_rules", ""]),
            // m.room.encryption: needed so Element X (and other E2EE clients) can see
            // a room is encrypted from the sliding-sync default required_state alone,
            // without having to explicitly request it (mirrors createRoom's
            // apply_initial_state in routes/rooms.rs, which is how this state event
            // gets set in the first place).
            json!(["m.room.encryption", ""]),
            json!(["m.room.member", "*"]),
        ];
    }

    let caller_user_id = caller.as_ref().map(|(uid, _)| uid.as_str());
    let (rooms_map, room_count) =
        build_rooms(&state, &specs, timeline_limit, initial, caller_user_id)?;
    let (typing_rooms, receipts_rooms) = build_ephemeral_extensions(&state)?;

    let mut lists_resp = Map::new();
    if let Some(lists) = lists_req {
        for (name, _l) in lists {
            lists_resp.insert(name.clone(), json!({ "count": room_count }));
        }
    }

    // to_device / e2ee / account_data: only known when the caller authenticates
    // (Bearer token) — see extract_caller. Unauthenticated callers keep getting the
    // pre-existing empty defaults for to_device.events / device_one_time_keys_count /
    // account_data ({"global": [], "rooms": {}}).

    // account_data extension (MSC4186 "Extensions" account_data) — see
    // AppState::account_data_global_events / account_data_room_events.
    let (global_account_data, rooms_account_data) =
        build_account_data_extension(&state, caller.as_ref());

    let to_device_events = match &caller {
        Some((user_id, device_id)) => state
            .drain_to_device(user_id, device_id, since_pos)
            .map_err(HsError::Internal)?,
        None => Vec::new(),
    };

    // e2ee.device_one_time_keys_count: caller's own remaining OTK inventory (reuses
    // keys.rs's count_by_algorithm, same as classic sync.rs).
    let device_one_time_keys_count = match &caller {
        Some((user_id, device_id)) => {
            let otk_key = (user_id.clone(), device_id.clone());
            state
                .e2ee
                .device_otks
                .lock()
                .map_err(|e| HsError::Internal(e.to_string()))?
                .get(&otk_key)
                .map(crate::routes::keys::count_by_algorithm)
                .unwrap_or_else(|| json!({}))
        }
        None => json!({}),
    };

    Ok(Json(json!({
        "pos": format!("s{current_pos}"),
        "lists": lists_resp,
        "rooms": rooms_map,
        "extensions": {
            "to_device":    { "events": to_device_events, "next_batch": format!("s{current_pos}") },
            "e2ee":         { "device_one_time_keys_count": device_one_time_keys_count, "device_unused_fallback_key_types": [] },
            "account_data": { "global": global_account_data, "rooms": rooms_account_data },
            "receipts":     { "rooms": receipts_rooms },
            "typing":       { "rooms": typing_rooms }
        }
    })))
}

// build_ephemeral_extensions:start
//   purpose: Build the MSC4186 "typing" and "receipts" extension "rooms" maps from
//            the same AppState ephemeral helpers classic /sync uses (see
//            routes/sync.rs ephemeral_events_for_room) — one entry per room that
//            currently has anyone typing / any recorded receipts. Rooms with
//            neither are omitted (matches the "only send if changed/non-empty"
//            spirit of these extensions).
//   input:  state — Arc<AppState>
//   output: Result<(Map<String,Value>, Map<String,Value>), HsError> —
//           (typing.rooms, receipts.rooms)
//   sideEffects: none (read-only; briefly locks state.rooms to enumerate room_ids)
// build_ephemeral_extensions:end
type EphemeralExt = (Map<String, Value>, Map<String, Value>);
fn build_ephemeral_extensions(state: &Arc<AppState>) -> Result<EphemeralExt, HsError> {
    let room_ids: Vec<String> = {
        let guard = state
            .rooms
            .lock()
            .map_err(|e| HsError::Internal(e.to_string()))?;
        guard.keys().cloned().collect()
    };

    let mut typing_rooms = Map::new();
    let mut receipts_rooms = Map::new();

    for room_id in &room_ids {
        let typing_ids = state.typing_user_ids(room_id);
        if !typing_ids.is_empty() {
            typing_rooms.insert(room_id.clone(), json!({ "user_ids": typing_ids }));
        }
        if let Some(content) = state.receipt_event_content(room_id) {
            receipts_rooms.insert(
                room_id.clone(),
                json!({
                    "type":    "m.receipt",
                    "content": content
                }),
            );
        }
    }

    Ok((typing_rooms, receipts_rooms))
}

// build_account_data_extension:start
//   purpose: Build the MSC4186 "account_data" extension payload: the caller's
//            global account_data events, plus a "rooms" map of room_id -> its
//            account_data.events array (same per-room content classic /sync
//            returns — see AppState::account_data_room_events, which also folds
//            in a synthetic m.tag event when the user has tags on that room).
//            Rooms with no account_data/tags set are omitted from the map
//            (matches the "only send if non-empty" spirit of build_ephemeral_
//            extensions above).
//   input:  state — Arc<AppState>; caller — Some((user_id, device_id)) for an
//           authenticated request, None otherwise
//   output: (Vec<Value> global events, Map<String, Value> per-room events);
//           both empty when caller is None
//   sideEffects: none (read-only; briefly locks state.rooms to enumerate room_ids)
// build_account_data_extension:end
fn build_account_data_extension(
    state: &Arc<AppState>,
    caller: Option<&(String, String)>,
) -> (Vec<Value>, Map<String, Value>) {
    let Some((user_id, _device_id)) = caller else {
        return (Vec::new(), Map::new());
    };

    let global = state.account_data_global_events(user_id);

    let mut rooms = Map::new();
    if let Ok(guard) = state.rooms.lock() {
        for room_id in guard.keys() {
            let events = state.account_data_room_events(user_id, room_id);
            if !events.is_empty() {
                rooms.insert(room_id.clone(), json!({ "events": events }));
            }
        }
    }

    (global, rooms)
}

// build_rooms:start
//   purpose: Build the sliding-sync rooms map, scoped to caller_user_id's own
//            joined rooms only.
//            SECURITY: previously iterated every room in the server with no
//            membership check at all — any authenticated (or even
//            unauthenticated) caller got every other user's rooms, full
//            history included. Same live-confirmed bug as classic sync.rs's
//            build_join_rooms (fixed there identically); fixed here too.
//   input:  state, specs (required_state matcher), timeline_limit, initial;
//           caller_user_id — the authenticated caller's user_id, or None for
//           an unauthenticated request (returns no rooms)
//   output: (room_id -> room JSON, count of rooms actually included)
//   sideEffects: acquires rooms/room_state/room_timeline mutexes
// build_rooms:end
fn build_rooms(
    state: &Arc<AppState>,
    specs: &[Value],
    timeline_limit: usize,
    initial: bool,
    caller_user_id: Option<&str>,
) -> Result<(Map<String, Value>, usize), HsError> {
    let Some(caller_user_id) = caller_user_id else {
        return Ok((Map::new(), 0));
    };

    let rooms_guard = state
        .rooms
        .lock()
        .map_err(|e| HsError::Internal(e.to_string()))?;
    let room_state_guard = state
        .room_state
        .lock()
        .map_err(|e| HsError::Internal(e.to_string()))?;
    let rt_guard = state
        .room_timeline
        .lock()
        .map_err(|e| HsError::Internal(e.to_string()))?;

    let mut out = Map::new();
    let mut count = 0usize;
    for room_id in rooms_guard.keys() {
        let state_events = room_state_guard
            .get(room_id.as_str())
            .map(|v| v.as_slice())
            .unwrap_or(&[]);
        let caller_is_joined = state_events.iter().any(|ev| {
            ev.event_type == "m.room.member"
                && ev.state_key == caller_user_id
                && ev.content.get("membership").and_then(|v| v.as_str()) == Some("join")
        });
        // Scripting hook: on_room_visible may extend visibility beyond joined
        // rooms (mirror of routes/sync.rs::build_join_rooms).
        let visible = caller_is_joined
            || state
                .scripting
                .on_room_visible(caller_user_id, room_id.as_str());
        if !visible {
            continue;
        }

        count += 1;
        let empty_rt: &[(u64, Value)] = &[];
        let timeline_entries = rt_guard
            .get(room_id.as_str())
            .map(|v| v.as_slice())
            .unwrap_or(empty_rt);

        let mut msgs: Vec<Value> = timeline_entries
            .iter()
            .filter(|(_, ev)| ev.get("state_key").is_none())
            .map(|(_, ev)| state.apply_redaction(ev))
            .collect();
        let total = msgs.len();
        if msgs.len() > timeline_limit {
            msgs = msgs.split_off(msgs.len() - timeline_limit);
        }
        let limited = total > timeline_limit;

        let req_state: Vec<Value> = state_events
            .iter()
            .filter(|ev| state_matches(specs, &ev.event_type, &ev.state_key))
            .map(state_event_to_json)
            .collect();

        let name = state_events
            .iter()
            .find(|ev| ev.event_type == "m.room.name")
            .and_then(|ev| ev.content.get("name"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let joined_count = state_events
            .iter()
            .filter(|ev| {
                ev.event_type == "m.room.member"
                    && ev.content.get("membership").and_then(|v| v.as_str()) == Some("join")
            })
            .count();

        let mut room = json!({
            "initial": initial,
            "required_state": req_state,
            "timeline": msgs,
            "prev_batch": "",
            "limited": limited,
            "joined_count": joined_count,
            "invited_count": 0,
            "notification_count": 0,
            "highlight_count": 0,
            "num_live": 0,
            "bump_stamp": 0
        });
        if let Some(n) = name {
            room["name"] = json!(n);
        }
        out.insert(room_id.clone(), room);
    }
    Ok((out, count))
}
