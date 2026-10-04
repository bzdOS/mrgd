// START_AI_HEADER
// MODULE: matrix-hs/src/routes/room_state.rs
// PURPOSE: Room state management endpoints for Matrix CS-API Stage 2.
//          join, directory lookup, state read/write, members, messages.
//
//          Cross-node STATE replication (cluster feature) — CLOSES THE GAP where
//          room_state was previously LOCAL ONLY (separate from the RoomLog timeline,
//          which already replicated over Zenoh). Every local state-event write in
//          this module (join/leave membership via add_member_state_event, and
//          PUT .../state/{type}/{key} via put_room_state_event) now ALSO publishes a
//          state delta over a DEDICATED Zenoh channel — the room's existing
//          ZenohCrdtSink (crate::substrate::crdt::CrdtSink, see state::ClusterState::sink_for),
//          under the "state" routing key (state::ClusterState::state_crdt_key()) —
//          separate from "events" (the RoomLog/timeline PDU channel used by
//          routes/send.rs) so state and timeline drains are independent, mirroring
//          how routes/ephemeral.rs added "typing"/"receipt" keys on the SAME sink.
//          rooms.rs::post_create_room publishes each initial state event the same way.
//
//          Design — CRDT Last-Writer-Wins, NOT Matrix auth-chain state resolution:
//            Each room's current state is modeled as a map keyed by
//            (event_type, state_key) -> winning StateEvent, resolved by
//            AppState::apply_remote_state_event using a DETERMINISTIC tiebreak on
//            (origin_server_ts, event_id) — the same (ts, tiebreak-key) ordering
//            pattern used by crate::substrate::crdt::LwwRegister and crate::substrate::barrier::reconcile.
//            This is a deliberate simplification of full Matrix state resolution:
//              - NO power-level enforcement of who is allowed to set which state
//                (any accepted write from any node can win the LWW race — a
//                misbehaving/buggy node could overwrite power_levels or membership
//                it should not have been able to set; this module does not check).
//              - NO auth-chain / auth-events conflict resolution (Matrix's real
//                state-res v2 algorithm is explicitly out of scope — enormous, and
//                orthogonal to this codebase's coordination-free CRDT thesis).
//              - What IS guaranteed: every node that has drained the same set of
//                published deltas converges to the IDENTICAL current state for
//                every (event_type, state_key) slot — deterministically and
//                idempotently (redelivering an already-applied event_id is a no-op).
//                In particular, "users sharing a room" (AppState::users_sharing_room_with,
//                which scans room_state m.room.member entries) becomes cross-node
//                correct once membership deltas have propagated and been drained.
//          On the receive side, drain_cluster_state (below) drains "state" blobs from
//          every locally-known room's sink and merges each into room_state via
//          AppState::apply_remote_state_event. Wired into routes/sync.rs and
//          routes/sliding_sync.rs alongside the existing drain_cluster_deltas /
//          drain_to_device_gossip / drain_cluster_ephemeral / drain_device_list_gossip
//          calls, so state convergence runs on the same cadence as timeline replication.
// DEPENDENCIES: axum, AppState, StateEvent, (cluster) crate::substrate::crdt::CrdtSink
// PUBLIC_API: post_join_room_or_alias, post_join_room, get_directory_room,
//             get_room_state, get_room_state_event, get_room_members,
//             get_room_messages, put_room_state_event,
//             post_leave_room, post_invite_room, post_kick_room, post_ban_room,
//             post_unban_room, post_forget_room,
//             (cluster) publish_state_event, drain_cluster_state
// END_AI_HEADER

use crate::{
    error::HsError,
    routes::{account::resolve_user_id_from_token, rooms::state_event_to_json},
    state::{AppState, StateEvent},
};
use axum::{
    extract::{Path, Query, State},
    http::HeaderMap,
    Json,
};
use serde_json::{json, Value};
use std::sync::Arc;

// extract_token_user:start
//   purpose: Extract the verified user_id from the Authorization header.
//            Delegates to resolve_user_id_from_token, which accepts ONLY signed "mxt_..."
//            tokens (no legacy "tok_<localpart>" acceptance, no anonymous fallback).
//            Returns None when the header is absent, malformed, or the MAC is invalid;
//            callers convert None to 401 M_UNKNOWN_TOKEN.
//   input:  headers, secret — HMAC key, server_name
//   output: Option<String> — verified user_id, or None
//   sideEffects: none
// extract_token_user:end
fn extract_token_user(headers: &HeaderMap, secret: &[u8], server_name: &str) -> Option<String> {
    headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .and_then(|tok| resolve_user_id_from_token(tok, secret, server_name))
}

// add_member_state_event:start
//   purpose: Add (or update) an m.room.member state event in room_state.
//            Also appends to room_timeline for incremental sync.
//            Returns the constructed StateEvent so the caller can (cluster feature)
//            publish it to the room's ZenohCrdtSink — this fn itself stays sync/local
//            only, matching the rest of this module's style (async publish is the
//            caller's job, see post_join_room_or_alias / post_join_room below).
//            `actor` is the sender of the resulting event — the user whose OWN
//            action caused the transition (self, for join/leave/forget; the
//            inviter/kicker/banner for invite/kick/ban/unban). `reason`, when
//            given, is copied into content.reason (kick/ban/unban convention).
//   input:  state, room_id, target user_id, membership, actor, reason
//   output: Result<StateEvent, HsError> — the applied membership state event
//   sideEffects: mutates room_state, room_timeline; increments stream_pos; notifies
// add_member_state_event:end
pub(crate) fn add_member_state_event(
    state: &Arc<AppState>,
    room_id: &str,
    user_id: &str,
    membership: &str,
    actor: &str,
    reason: Option<&str>,
) -> Result<StateEvent, HsError> {
    let localpart = user_id
        .strip_prefix('@')
        .and_then(|s| s.split(':').next())
        .unwrap_or(user_id);

    let ts = state.hlc_now();
    let event_id = format!(
        "$join_{}_{}_{}",
        &room_id
            .chars()
            .filter(|c| c.is_alphanumeric())
            .take(8)
            .collect::<String>(),
        &user_id
            .chars()
            .filter(|c| c.is_alphanumeric())
            .take(8)
            .collect::<String>(),
        ts
    );

    let mut content = json!({
        "membership":  membership,
        "displayname": localpart
    });
    if let Some(r) = reason {
        content["reason"] = json!(r);
    }

    let new_ev = StateEvent {
        event_type: "m.room.member".to_string(),
        state_key: user_id.to_string(),
        sender: actor.to_string(),
        content,
        event_id: event_id.clone(),
        room_id: room_id.to_string(),
        origin_server_ts: ts,
    };

    {
        let mut rs = state
            .room_state
            .lock()
            .map_err(|e| HsError::Internal(e.to_string()))?;
        let room_vec = rs.entry(room_id.to_string()).or_default();
        // Remove existing member event for this user.
        room_vec.retain(|ev| !(ev.event_type == "m.room.member" && ev.state_key == user_id));
        room_vec.push(new_ev.clone());
    }

    {
        let ev_json = state_event_to_json(&new_ev);
        // Persist the member state event to the room journal (best-effort).
        state.persist_room_event(room_id, &ev_json);
        state.append_room_timeline(room_id, ev_json);
    }

    state.notify.notify_waiters();
    Ok(new_ev)
}

// post_join_room_or_alias:start
//   purpose: POST /_matrix/client/v3/join/{roomIdOrAlias}
//            Resolve alias if needed, add join member event, return room_id.
//   input:  State(AppState), room_id_or_alias path param, headers
//   output: JSON {"room_id":"!room:server"}
//   sideEffects: adds m.room.member join event to room_state
// post_join_room_or_alias:end
pub async fn post_join_room_or_alias(
    State(state): State<Arc<AppState>>,
    Path(room_id_or_alias): Path<String>,
    headers: HeaderMap,
    _body: Option<Json<Value>>,
) -> Result<Json<Value>, HsError> {
    let user_id = extract_token_user(&headers, &state.token_secret, &state.server_name)
        .ok_or_else(|| HsError::UnknownToken("missing or invalid token".to_string()))?;
    let room_id = resolve_room_id(&state, &room_id_or_alias)?;
    // Same rule as the room-id path: a resolvable alias is not a licence
    // to invent the room behind it.
    if !room_is_known(&state, &room_id) {
        return Err(HsError::RoomNotFound(room_id));
    }
    state.ensure_room_state(&room_id);
    #[cfg_attr(not(feature = "cluster"), allow(unused_variables))]
    let ev = add_member_state_event(&state, &room_id, &user_id, "join", &user_id, None)?;
    #[cfg(feature = "cluster")]
    publish_state_event(&state, &ev).await?;
    Ok(Json(json!({ "room_id": room_id })))
}

// post_join_room:start
//   purpose: POST /_matrix/client/v3/rooms/{roomId}/join
//            Add join member event, return room_id.
//   input:  State(AppState), room_id path param, headers
//   output: JSON {"room_id":"!room:server"}
//   sideEffects: adds m.room.member join event to room_state
// post_join_room:end
pub async fn post_join_room(
    State(state): State<Arc<AppState>>,
    Path(room_id): Path<String>,
    headers: HeaderMap,
    _body: Option<Json<Value>>,
) -> Result<Json<Value>, HsError> {
    let user_id = extract_token_user(&headers, &state.token_secret, &state.server_name)
        .ok_or_else(|| HsError::UnknownToken("missing or invalid token".to_string()))?;
    // The spec answer for a room this node has never heard of is 404
    // M_NOT_FOUND. The ensure_room_state() below used to create it instead, so a
    // join for a typo or a not-yet-arrived room id silently produced an
    // empty shell: one membership event, no content, forever in the room list.
    if !room_is_known(&state, &room_id) {
        return Err(HsError::RoomNotFound(room_id));
    }
    state.ensure_room_state(&room_id);
    #[cfg_attr(not(feature = "cluster"), allow(unused_variables))]
    let ev = add_member_state_event(&state, &room_id, &user_id, "join", &user_id, None)?;
    #[cfg(feature = "cluster")]
    publish_state_event(&state, &ev).await?;
    Ok(Json(json!({ "room_id": room_id })))
}

// current_membership:start
//   purpose: Read the caller's/target's current m.room.member membership value for
//            a room, if any m.room.member state event exists for that state_key.
//            Used by the invite/kick/ban/unban/forget handlers below for the
//            minimal auth/precondition checks documented on each handler.
//   input:  state, room_id, user_id
//   output: Result<Option<String>, HsError> — None if no member event exists yet
//   sideEffects: none (read-only lock of room_state)
// current_membership:end
fn current_membership(
    state: &Arc<AppState>,
    room_id: &str,
    user_id: &str,
) -> Result<Option<String>, HsError> {
    let rs = state
        .room_state
        .lock()
        .map_err(|e| HsError::Internal(e.to_string()))?;
    let membership = rs.get(room_id).and_then(|events| {
        events
            .iter()
            .find(|ev| ev.event_type == "m.room.member" && ev.state_key == user_id)
            .and_then(|ev| ev.content.get("membership"))
            .and_then(|v| v.as_str())
            .map(str::to_string)
    });
    Ok(membership)
}

// require_room_exists:start
//   purpose: Return 404 M_NOT_FOUND unless room_id has a room_state entry
//            (i.e. has been created / ensure_room_state'd at least once).
//   input:  state, room_id
//   output: Result<(), HsError>
//   sideEffects: none (read-only lock of room_state)
// require_room_exists:end
fn require_room_exists(state: &Arc<AppState>, room_id: &str) -> Result<(), HsError> {
    let rs = state
        .room_state
        .lock()
        .map_err(|e| HsError::Internal(e.to_string()))?;
    if rs.contains_key(room_id) {
        Ok(())
    } else {
        Err(HsError::RoomNotFound(room_id.to_string()))
    }
}

// require_caller_joined:start
//   purpose: Minimal sane authorization seam for invite/kick/ban/unban: the caller
//            must currently be a joined member of the room. This alone does NOT
//            implement Matrix power-level enforcement — kick/ban additionally call
//            require_outranks (below) for that; invite/unban still rely on this
//            check alone, matching this module's existing "NO power-level
//            enforcement" design note above for everything but kick/ban.
//   input:  state, room_id, caller user_id
//   output: Result<(), HsError> — Forbidden if caller is not a joined member
//   sideEffects: none
// require_caller_joined:end
fn require_caller_joined(
    state: &Arc<AppState>,
    room_id: &str,
    caller: &str,
) -> Result<(), HsError> {
    match current_membership(state, room_id, caller)? {
        Some(m) if m == "join" => Ok(()),
        _ => Err(HsError::Forbidden(format!(
            "{caller} is not a joined member of {room_id}"
        ))),
    }
}

// power_level_of / named_threshold:start
//   purpose: Read a user's power_levels level, and a named action threshold
//            ("kick"/"ban", default 50 for both, matching Matrix's own
//            defaults) — the two pieces require_outranks needs and this
//            module previously had no equivalent of at all.
//   input:  events — the room's state; user_id, or field name + default
//   output: the level / threshold (0 / the given default if unknown)
//   sideEffects: none
// power_level_of / named_threshold:end
fn power_level_of(events: &[StateEvent], user_id: &str) -> i64 {
    events
        .iter()
        .find(|ev| ev.event_type == "m.room.power_levels")
        .and_then(|pl| {
            pl.content
                .get("users")
                .and_then(|u| u.get(user_id))
                .and_then(|v| v.as_i64())
                .or_else(|| pl.content.get("users_default").and_then(|v| v.as_i64()))
        })
        .unwrap_or(0)
}

fn named_threshold(events: &[StateEvent], field: &str, default: i64) -> i64 {
    events
        .iter()
        .find(|ev| ev.event_type == "m.room.power_levels")
        .and_then(|pl| pl.content.get(field))
        .and_then(|v| v.as_i64())
        .unwrap_or(default)
}

// require_outranks:start
//   purpose: May `sender` kick/ban `target` in this room? require_caller_joined's
//            own doc comment used to say plainly "a misbehaving-but-joined member
//            can currently kick/ban anyone" — this closes that gap. `sender` must
//            reach power_levels' named "kick"/"ban" threshold (default 50, Matrix's
//            own default) AND strictly outrank `target`'s own level — the same
//            "reach the threshold and outrank" shape real Matrix uses, so two
//            equal-power members can never kick/ban each other and a low-power
//            member can never touch the room's admins. No "power_levels not known
//            -> allow" exemption: power_level_of already reads 0 for both sides
//            when no power_levels event exists, so the outrank half (0 > 0) is
//            false and this refuses by construction — deliberately the opposite
//            default from, say, invite, because an unauthenticated ability to
//            remove a member is a materially worse failure mode than a room the
//            server cannot yet police.
//   input:  state, room_id; action — "kick" or "ban" (the power_levels field name);
//           sender, target — user_ids
//   output: Result<(), HsError> — Forbidden if sender may not do this to target
//   sideEffects: none (read-only lock of room_state)
// require_outranks:end
fn require_outranks(
    state: &Arc<AppState>,
    room_id: &str,
    action: &str,
    sender: &str,
    target: &str,
) -> Result<(), HsError> {
    let rs = state
        .room_state
        .lock()
        .map_err(|e| HsError::Internal(e.to_string()))?;
    let events: &[StateEvent] = rs.get(room_id).map(Vec::as_slice).unwrap_or(&[]);
    let sender_level = power_level_of(events, sender);
    let target_level = power_level_of(events, target);
    let required = named_threshold(events, action, 50);
    if sender_level >= required && sender_level > target_level {
        Ok(())
    } else {
        Err(HsError::Forbidden(format!(
            "{sender} does not outrank {target} in {room_id} (needs >= {required} \
             and > {target_level}, has {sender_level})"
        )))
    }
}

// InviteBody / KickBanBody / UnbanBody:start
//   purpose: Request bodies for invite/kick/ban/unban.
//   input:  JSON {"user_id":"@target:server"[, "reason":"..."]}
//   output: n/a
//   sideEffects: none
// InviteBody / KickBanBody / UnbanBody:end
#[derive(serde::Deserialize)]
pub struct InviteBody {
    pub user_id: String,
}

#[derive(serde::Deserialize)]
pub struct KickBanBody {
    pub user_id: String,
    pub reason: Option<String>,
}

#[derive(serde::Deserialize)]
pub struct UnbanBody {
    pub user_id: String,
}

// post_leave_room:start
//   purpose: POST /_matrix/client/v3/rooms/{roomId}/leave
//            Set the caller's OWN membership to "leave". Self-action: no
//            power-level or membership precondition — a user may always leave
//            (this also covers rejecting a pending invite, per Matrix semantics).
//   input:  State(AppState), room_id path param, headers
//   output: JSON {} on success
//   sideEffects: adds m.room.member leave event to room_state (actor == caller)
// post_leave_room:end
pub async fn post_leave_room(
    State(state): State<Arc<AppState>>,
    Path(room_id): Path<String>,
    headers: HeaderMap,
    _body: Option<Json<Value>>,
) -> Result<Json<Value>, HsError> {
    let user_id = extract_token_user(&headers, &state.token_secret, &state.server_name)
        .ok_or_else(|| HsError::UnknownToken("missing or invalid token".to_string()))?;
    let room_id = state.resolve_room_id(&room_id);
    require_room_exists(&state, &room_id)?;
    #[cfg_attr(not(feature = "cluster"), allow(unused_variables))]
    let ev = add_member_state_event(&state, &room_id, &user_id, "leave", &user_id, None)?;
    #[cfg(feature = "cluster")]
    publish_state_event(&state, &ev).await?;
    Ok(Json(json!({})))
}

// post_invite_room:start
//   purpose: POST /_matrix/client/v3/rooms/{roomId}/invite  body {"user_id"}
//            Set the target user's membership to "invite". Auth: caller must be
//            a joined member of the room (require_caller_joined) — see that
//            function's contract for the documented power-level seam.
//   input:  State(AppState), room_id path param, headers, JSON {"user_id"}
//   output: JSON {} on success
//   sideEffects: adds m.room.member invite event to room_state (actor == caller)
// post_invite_room:end
pub async fn post_invite_room(
    State(state): State<Arc<AppState>>,
    Path(room_id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<InviteBody>,
) -> Result<Json<Value>, HsError> {
    let caller = extract_token_user(&headers, &state.token_secret, &state.server_name)
        .ok_or_else(|| HsError::UnknownToken("missing or invalid token".to_string()))?;
    let room_id = state.resolve_room_id(&room_id);
    require_room_exists(&state, &room_id)?;
    require_caller_joined(&state, &room_id, &caller)?;
    if body.user_id.trim().is_empty() {
        return Err(HsError::BadRequest("missing user_id".to_string()));
    }
    #[cfg_attr(not(feature = "cluster"), allow(unused_variables))]
    let ev = add_member_state_event(&state, &room_id, &body.user_id, "invite", &caller, None)?;
    #[cfg(feature = "cluster")]
    publish_state_event(&state, &ev).await?;
    Ok(Json(json!({})))
}

// post_kick_room:start
//   purpose: POST /_matrix/client/v3/rooms/{roomId}/kick  body {"user_id","reason"?}
//            Set the target user's membership to "leave", sender == caller
//            (distinguishing a kick from a self-leave in the resulting event).
//            Auth: caller must be a joined member (require_caller_joined) AND
//            reach the "kick" power_levels threshold while strictly outranking
//            the target (require_outranks) — closing the gap the two seams'
//            own doc comments used to flag: any joined member could kick anyone.
//   input:  State(AppState), room_id path param, headers, JSON {"user_id","reason"?}
//   output: JSON {} on success
//   sideEffects: adds m.room.member leave event to room_state (actor == caller)
// post_kick_room:end
pub async fn post_kick_room(
    State(state): State<Arc<AppState>>,
    Path(room_id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<KickBanBody>,
) -> Result<Json<Value>, HsError> {
    let caller = extract_token_user(&headers, &state.token_secret, &state.server_name)
        .ok_or_else(|| HsError::UnknownToken("missing or invalid token".to_string()))?;
    let room_id = state.resolve_room_id(&room_id);
    require_room_exists(&state, &room_id)?;
    require_caller_joined(&state, &room_id, &caller)?;
    require_outranks(&state, &room_id, "kick", &caller, &body.user_id)?;
    if body.user_id.trim().is_empty() {
        return Err(HsError::BadRequest("missing user_id".to_string()));
    }
    #[cfg_attr(not(feature = "cluster"), allow(unused_variables))]
    let ev = add_member_state_event(
        &state,
        &room_id,
        &body.user_id,
        "leave",
        &caller,
        body.reason.as_deref(),
    )?;
    #[cfg(feature = "cluster")]
    publish_state_event(&state, &ev).await?;
    Ok(Json(json!({})))
}

// post_ban_room:start
//   purpose: POST /_matrix/client/v3/rooms/{roomId}/ban  body {"user_id","reason"?}
//            Set the target user's membership to "ban".
//            Auth: caller must be a joined member (require_caller_joined) AND
//            reach the "ban" power_levels threshold while strictly outranking
//            the target (require_outranks) — same shape as post_kick_room.
//   input:  State(AppState), room_id path param, headers, JSON {"user_id","reason"?}
//   output: JSON {} on success
//   sideEffects: adds m.room.member ban event to room_state (actor == caller)
// post_ban_room:end
pub async fn post_ban_room(
    State(state): State<Arc<AppState>>,
    Path(room_id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<KickBanBody>,
) -> Result<Json<Value>, HsError> {
    let caller = extract_token_user(&headers, &state.token_secret, &state.server_name)
        .ok_or_else(|| HsError::UnknownToken("missing or invalid token".to_string()))?;
    let room_id = state.resolve_room_id(&room_id);
    require_room_exists(&state, &room_id)?;
    require_caller_joined(&state, &room_id, &caller)?;
    require_outranks(&state, &room_id, "ban", &caller, &body.user_id)?;
    if body.user_id.trim().is_empty() {
        return Err(HsError::BadRequest("missing user_id".to_string()));
    }
    #[cfg_attr(not(feature = "cluster"), allow(unused_variables))]
    let ev = add_member_state_event(
        &state,
        &room_id,
        &body.user_id,
        "ban",
        &caller,
        body.reason.as_deref(),
    )?;
    #[cfg(feature = "cluster")]
    publish_state_event(&state, &ev).await?;
    Ok(Json(json!({})))
}

// post_unban_room:start
//   purpose: POST /_matrix/client/v3/rooms/{roomId}/unban  body {"user_id"}
//            Set a currently-banned target user's membership to "leave"
//            (Matrix unban semantics: the user is not re-invited/joined, only
//            un-banned). Auth: caller must be a joined member (require_caller_joined
//            seam). Precondition: target's current membership must be "ban" —
//            otherwise 400 M_BAD_JSON, a minimal sanity check (not a power-level
//            check).
//   input:  State(AppState), room_id path param, headers, JSON {"user_id"}
//   output: JSON {} on success
//   sideEffects: adds m.room.member leave event to room_state (actor == caller)
// post_unban_room:end
pub async fn post_unban_room(
    State(state): State<Arc<AppState>>,
    Path(room_id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<UnbanBody>,
) -> Result<Json<Value>, HsError> {
    let caller = extract_token_user(&headers, &state.token_secret, &state.server_name)
        .ok_or_else(|| HsError::UnknownToken("missing or invalid token".to_string()))?;
    let room_id = state.resolve_room_id(&room_id);
    require_room_exists(&state, &room_id)?;
    require_caller_joined(&state, &room_id, &caller)?;
    if body.user_id.trim().is_empty() {
        return Err(HsError::BadRequest("missing user_id".to_string()));
    }
    match current_membership(&state, &room_id, &body.user_id)? {
        Some(ref m) if m == "ban" => {}
        _ => {
            return Err(HsError::BadRequest(format!(
                "{} is not banned",
                body.user_id
            )))
        }
    }
    #[cfg_attr(not(feature = "cluster"), allow(unused_variables))]
    let ev = add_member_state_event(&state, &room_id, &body.user_id, "leave", &caller, None)?;
    #[cfg(feature = "cluster")]
    publish_state_event(&state, &ev).await?;
    Ok(Json(json!({})))
}

// post_forget_room:start
//   purpose: POST /_matrix/client/v3/rooms/{roomId}/forget
//            Minimal "forget" — drops the caller's view of a room they have
//            already left. Per Matrix semantics this should error if the caller
//            is still joined; that precondition is enforced here. Beyond that,
//            this server does not maintain a separate per-user "forgotten rooms"
//            list (room_state / room_timeline stay server-global, not per-user),
//            so there is nothing further to mutate — documented minimal seam:
//            a fuller implementation would hide the room from this user's
//            /sync and /joined_rooms views.
//   input:  State(AppState), room_id path param, headers
//   output: JSON {} on success
//   sideEffects: none (read-only precondition check; no state mutation)
// post_forget_room:end
pub async fn post_forget_room(
    State(state): State<Arc<AppState>>,
    Path(room_id): Path<String>,
    headers: HeaderMap,
    _body: Option<Json<Value>>,
) -> Result<Json<Value>, HsError> {
    let user_id = extract_token_user(&headers, &state.token_secret, &state.server_name)
        .ok_or_else(|| HsError::UnknownToken("missing or invalid token".to_string()))?;
    let room_id = state.resolve_room_id(&room_id);
    require_room_exists(&state, &room_id)?;
    match current_membership(&state, &room_id, &user_id)? {
        Some(ref m) if m == "join" => {
            return Err(HsError::Forbidden(
                "cannot forget a room while still joined".to_string(),
            ));
        }
        _ => {}
    }
    Ok(Json(json!({})))
}

// resolve_room_id:start
//   purpose: Resolve a room_id_or_alias to a room_id.
//            If it starts with '!', return as-is.
//            If it starts with '#', look up in aliases map.
//   input:  state, room_id_or_alias
//   output: Result<String, HsError>
//   sideEffects: acquires aliases mutex
// resolve_room_id:end
fn resolve_room_id(state: &Arc<AppState>, room_id_or_alias: &str) -> Result<String, HsError> {
    if room_id_or_alias.starts_with('!') {
        return Ok(room_id_or_alias.to_string());
    }
    if room_id_or_alias.starts_with('#') {
        let aliases = state
            .aliases
            .lock()
            .map_err(|e| HsError::Internal(e.to_string()))?;
        return aliases
            .get(room_id_or_alias)
            .cloned()
            .ok_or_else(|| HsError::NotFound(format!("alias not found: {room_id_or_alias}")));
    }
    Err(HsError::BadRequest(format!(
        "invalid room identifier: {room_id_or_alias}"
    )))
}

// get_directory_room:start
//   purpose: GET /_matrix/client/v3/directory/room/{roomAlias}
//            Look up a room alias and return the room_id.
//   input:  State(AppState), roomAlias path param
//   output: JSON {"room_id":"!room:server","servers":["server"]}
//   sideEffects: none
// get_directory_room:end
pub async fn get_directory_room(
    State(state): State<Arc<AppState>>,
    Path(room_alias): Path<String>,
) -> Result<Json<Value>, HsError> {
    let aliases = state
        .aliases
        .lock()
        .map_err(|e| HsError::Internal(e.to_string()))?;

    let room_id = aliases
        .get(&room_alias)
        .cloned()
        .ok_or_else(|| HsError::NotFound(format!("alias not found: {room_alias}")))?;

    Ok(Json(json!({
        "room_id":  room_id,
        "servers":  [state.server_name]
    })))
}

// get_room_state:start
//   purpose: GET /_matrix/client/v3/rooms/{roomId}/state
//            Return all current state events for a room.
//   input:  State(AppState), room_id path param
//   output: JSON array of state events
//   sideEffects: none
// get_room_state:end
pub async fn get_room_state(
    State(state): State<Arc<AppState>>,
    Path(room_id): Path<String>,
) -> Result<Json<Value>, HsError> {
    let room_id = state.resolve_room_id(&room_id);
    let rs = state
        .room_state
        .lock()
        .map_err(|e| HsError::Internal(e.to_string()))?;

    let events = rs
        .get(&room_id)
        .ok_or_else(|| HsError::RoomNotFound(room_id.clone()))?;

    let event_jsons: Vec<Value> = events.iter().map(state_event_to_json).collect();
    Ok(Json(Value::Array(event_jsons)))
}

// get_room_state_event:start
//   purpose: GET /_matrix/client/v3/rooms/{roomId}/state/{eventType}/{stateKey}
//            Return the content of a specific state event.
//   input:  State(AppState), room_id, event_type, state_key path params
//   output: JSON content of the state event, or 404
//   sideEffects: none
// get_room_state_event:end
pub async fn get_room_state_event(
    State(state): State<Arc<AppState>>,
    Path((room_id, event_type, state_key)): Path<(String, String, String)>,
) -> Result<Json<Value>, HsError> {
    let room_id = state.resolve_room_id(&room_id);
    let rs = state
        .room_state
        .lock()
        .map_err(|e| HsError::Internal(e.to_string()))?;

    let events = rs
        .get(&room_id)
        .ok_or_else(|| HsError::RoomNotFound(room_id.clone()))?;

    let ev = events
        .iter()
        .find(|ev| ev.event_type == event_type && ev.state_key == state_key)
        .ok_or_else(|| HsError::NotFound(format!("{event_type}/{state_key} not found")))?;

    Ok(Json(ev.content.clone()))
}

// get_room_state_event_empty_key:start
//   purpose: GET /_matrix/client/v3/rooms/{roomId}/state/{eventType} — the empty-
//            state_key form of get_room_state_event. Matrix state events with
//            state_key="" (m.room.create, m.room.name, m.room.encryption, ...) are
//            addressed by clients WITHOUT a trailing path segment (no trailing
//            slash) — axum's router (matchit) does not match an empty final path
//            segment to a `{param}` capture, so the 3-segment route
//            (.../state/{event_type}/{state_key}) can never itself serve a
//            state_key="" lookup. This route exists purely to make that reachable;
//            it delegates to get_room_state_event with state_key hardcoded to "".
//   input:  State(AppState), room_id, event_type path params
//   output: same as get_room_state_event(..., state_key="")
//   sideEffects: none
// get_room_state_event_empty_key:end
pub async fn get_room_state_event_empty_key(
    state: State<Arc<AppState>>,
    Path((room_id, event_type)): Path<(String, String)>,
) -> Result<Json<Value>, HsError> {
    get_room_state_event(state, Path((room_id, event_type, String::new()))).await
}

// get_room_members:start
//   purpose: GET /_matrix/client/v3/rooms/{roomId}/members
//            Return all m.room.member state events for a room.
//   input:  State(AppState), room_id path param
//   output: JSON {"chunk":[...m.room.member events...]}
//   sideEffects: none
// get_room_members:end
pub async fn get_room_members(
    State(state): State<Arc<AppState>>,
    Path(room_id): Path<String>,
) -> Result<Json<Value>, HsError> {
    let room_id = state.resolve_room_id(&room_id);
    let rs = state
        .room_state
        .lock()
        .map_err(|e| HsError::Internal(e.to_string()))?;

    let events = rs
        .get(&room_id)
        .ok_or_else(|| HsError::RoomNotFound(room_id.clone()))?;

    let members: Vec<Value> = events
        .iter()
        .filter(|ev| ev.event_type == "m.room.member")
        .map(state_event_to_json)
        .collect();

    Ok(Json(json!({ "chunk": members })))
}

// get_joined_members:start
//   purpose: GET /_matrix/client/v3/rooms/{roomId}/joined_members — a simpler,
//            DISTINCT endpoint from GET .../members: returns only CURRENTLY
//            JOINED users (membership=="join"), shaped as {user_id: {display_name,
//            avatar_url}} rather than full m.room.member event objects. Some
//            clients (e.g. simple bot/notification flows) call this instead of
//            /members for a quick roster.
//   input:  State(AppState), room_id path param
//   output: JSON {"joined": {user_id: {"display_name","avatar_url"}}}
//   sideEffects: none
// get_joined_members:end
pub async fn get_joined_members(
    State(state): State<Arc<AppState>>,
    Path(room_id): Path<String>,
) -> Result<Json<Value>, HsError> {
    let room_id = state.resolve_room_id(&room_id);
    let rs = state
        .room_state
        .lock()
        .map_err(|e| HsError::Internal(e.to_string()))?;

    let events = rs
        .get(&room_id)
        .ok_or_else(|| HsError::RoomNotFound(room_id.clone()))?;

    let mut joined = serde_json::Map::new();
    for ev in events.iter().filter(|ev| ev.event_type == "m.room.member") {
        if ev.content.get("membership").and_then(Value::as_str) != Some("join") {
            continue;
        }
        joined.insert(
            ev.state_key.clone(),
            json!({
                "display_name": ev.content.get("displayname").cloned().unwrap_or(Value::Null),
                "avatar_url":   ev.content.get("avatar_url").cloned().unwrap_or(Value::Null),
            }),
        );
    }

    Ok(Json(json!({ "joined": Value::Object(joined) })))
}

// MessagesParams:start
//   purpose: Query parameters for GET /rooms/{roomId}/messages.
//   input:  query string from client
//   output: MessagesParams struct
//   sideEffects: none
// MessagesParams:end
#[derive(Debug, serde::Deserialize, Default)]
pub struct MessagesParams {
    pub from: Option<String>,
    pub dir: Option<String>,
    pub limit: Option<usize>,
    pub filter: Option<String>,
}

// parse_messages_token:start
//   purpose: Parse a pagination token of the form "t<N>" into a usize index into
//            the room's chronological message list. Returns None if absent or
//            malformed (caller substitutes a direction-appropriate default).
//   input:  token — optional string from the `from` query param
//   output: Option<usize>
//   sideEffects: none
// parse_messages_token:end
fn parse_messages_token(token: &Option<String>) -> Option<usize> {
    token
        .as_deref()
        .and_then(|s| s.strip_prefix('t'))
        .and_then(|n| n.parse::<usize>().ok())
}

// get_room_messages:start
//   purpose: GET /_matrix/client/v3/rooms/{roomId}/messages — paginate message
//            events for a room (non-state events from room_timeline).
//            Tokens are "t<N>" indices into the room's chronological message
//            list (N in 0..=total). dir=b (default) walks backwards (older
//            events) from `from` (default: total, i.e. "now"); dir=f walks
//            forwards from `from` (default: 0). `end` is OMITTED once the walk
//            reaches the boundary in that direction (start of history for
//            dir=b, present for dir=f) — per spec, this is exactly the signal
//            a real client's pagination loop relies on to stop requesting more.
//            The prior version ignored from/dir/limit entirely and always
//            returned a non-empty `end`, so any real client's backfill (e.g.
//            FluffyChat's "Load more") looped forever re-requesting the same
//            page — reproduced live and fixed here.
//   input:  State(AppState), room_id path param, Query(MessagesParams)
//   output: JSON {"chunk":[...],"start":"t<N>","end"?:"t<M>"}
//   sideEffects: none
// get_room_messages:end
pub async fn get_room_messages(
    State(state): State<Arc<AppState>>,
    Path(room_id): Path<String>,
    Query(params): Query<MessagesParams>,
) -> Result<Json<Value>, HsError> {
    let room_id = state.resolve_room_id(&room_id);
    // Check room exists.
    {
        let rooms = state
            .rooms
            .lock()
            .map_err(|e| HsError::Internal(e.to_string()))?;
        if !rooms.contains_key(&room_id) {
            return Err(HsError::RoomNotFound(room_id.clone()));
        }
    }

    let rt = state
        .room_timeline
        .lock()
        .map_err(|e| HsError::Internal(e.to_string()))?;

    let entries = rt.get(&room_id).map(|v| v.as_slice()).unwrap_or(&[]);
    let projected: Vec<Value> = entries
        .iter()
        .filter(|(_, ev)| ev.get("state_key").is_none())
        .map(|(_, ev)| state.apply_redaction(ev))
        .collect();
    drop(rt);

    // When the timeline cap has drained this room, the projection is only the TAIL
    // of the history. Pagination tokens are positions in the room's chronological
    // message list, and sync's prev_batch is a position in that same list — so the
    // dropped head has to be part of the list, or the token points past the events
    // the client is trying to reach and the backfill silently yields nothing.
    //
    // The RoomLog still holds them (only roomlog_max_events deletes, and that is
    // irreversible by design), so they are rebuilt here rather than lost.
    let dropped_head = if state.timeline_max_events > 0 {
        let projection_first = projected
            .first()
            .and_then(|ev| ev.get("event_id"))
            .and_then(|v| v.as_str());
        state.dropped_head_events(&room_id, projection_first)
    } else {
        Vec::new()
    };
    let messages: Vec<Value> = if dropped_head.is_empty() {
        projected
    } else {
        dropped_head.into_iter().chain(projected.into_iter()).collect()
    };

    let total = messages.len();
    let backwards = params.dir.as_deref() != Some("f");
    let limit = params.limit.unwrap_or(10).max(1);

    let (chunk, start_pos, end_pos): (Vec<Value>, usize, usize) = if backwards {
        let from_pos = parse_messages_token(&params.from)
            .unwrap_or(total)
            .min(total);
        let end_index = from_pos;
        let start_index = end_index.saturating_sub(limit);
        let mut page = messages[start_index..end_index].to_vec();
        page.reverse(); // dir=b: newest-first within the chunk, per spec.
        (page, end_index, start_index)
    } else {
        let from_pos = parse_messages_token(&params.from).unwrap_or(0).min(total);
        let start_index = from_pos;
        let end_index = (start_index + limit).min(total);
        (
            messages[start_index..end_index].to_vec(),
            start_index,
            end_index,
        )
    };

    let mut body = json!({
        "chunk": chunk,
        "start": format!("t{start_pos}"),
    });
    // Omit `end` once this walk has reached the boundary in its direction —
    // the client-visible "no more history to load" signal.
    let reached_boundary = if backwards {
        end_pos == 0
    } else {
        end_pos == total
    };
    if !reached_boundary {
        body["end"] = json!(format!("t{end_pos}"));
    }

    Ok(Json(body))
}

// put_room_state_event:start
//   purpose: PUT /_matrix/client/v3/rooms/{roomId}/state/{eventType}/{stateKey}
//            Create or replace a state event in the room.
//   input:  State(AppState), room_id, event_type, state_key path params, headers, JSON body
//   output: JSON {"event_id":"$..."}
//   sideEffects: mutates room_state; appends to room_timeline; notifies waiters
// put_room_state_event:end
pub async fn put_room_state_event(
    State(state): State<Arc<AppState>>,
    Path((room_id, event_type, state_key)): Path<(String, String, String)>,
    headers: HeaderMap,
    Json(content): Json<Value>,
) -> Result<Json<Value>, HsError> {
    let room_id = state.resolve_room_id(&room_id);
    let sender = extract_token_user(&headers, &state.token_secret, &state.server_name)
        .ok_or_else(|| HsError::UnknownToken("missing or invalid token".to_string()))?;

    // Ensure room exists.
    {
        let rooms = state
            .rooms
            .lock()
            .map_err(|e| HsError::Internal(e.to_string()))?;
        if !rooms.contains_key(&room_id) {
            return Err(HsError::RoomNotFound(room_id.clone()));
        }
    }

    let ts = state.hlc_now();
    let type_slug: String = event_type.replace('.', "_");
    let key_slug: String = state_key
        .chars()
        .filter(|c| c.is_alphanumeric() || *c == '-' || *c == '_')
        .take(8)
        .collect();
    let room_slug: String = room_id
        .chars()
        .filter(|c| c.is_alphanumeric())
        .take(8)
        .collect();
    let event_id = format!("$put_{type_slug}_{room_slug}_{key_slug}_{ts}");

    let new_ev = StateEvent {
        event_type: event_type.clone(),
        state_key: state_key.clone(),
        sender,
        content: content.clone(),
        event_id: event_id.clone(),
        room_id: room_id.clone(),
        origin_server_ts: ts,
    };

    {
        let mut rs = state
            .room_state
            .lock()
            .map_err(|e| HsError::Internal(e.to_string()))?;
        let room_vec = rs.entry(room_id.clone()).or_default();
        room_vec.retain(|ev| !(ev.event_type == event_type && ev.state_key == state_key));
        room_vec.push(new_ev.clone());
    }

    {
        let ev_json = state_event_to_json(&new_ev);
        // Persist the state event to the room journal (best-effort).
        state.persist_room_event(&room_id, &ev_json);
        state.append_room_timeline(&room_id, ev_json);
    }

    state.notify.notify_waiters();

    #[cfg(feature = "cluster")]
    publish_state_event(&state, &new_ev).await?;

    Ok(Json(json!({ "event_id": event_id })))
}

// put_room_state_event_empty_key:start
//   purpose: PUT /_matrix/client/v3/rooms/{roomId}/state/{eventType} — the
//            empty-state_key form of put_room_state_event. See
//            get_room_state_event_empty_key's contract comment for why the
//            3-segment route cannot itself serve a state_key="" write (axum/matchit
//            does not match an empty final segment to a `{param}` capture).
//            Delegates to put_room_state_event with state_key hardcoded to "".
//   input:  State(AppState), room_id, event_type path params, headers, JSON body
//   output: same as put_room_state_event(..., state_key="")
//   sideEffects: same as put_room_state_event
// put_room_state_event_empty_key:end
pub async fn put_room_state_event_empty_key(
    state: State<Arc<AppState>>,
    Path((room_id, event_type)): Path<(String, String)>,
    headers: HeaderMap,
    body: Json<Value>,
) -> Result<Json<Value>, HsError> {
    put_room_state_event(
        state,
        Path((room_id, event_type, String::new())),
        headers,
        body,
    )
    .await
}

// ── Cluster replication (feature = "cluster") ─────────────────────────────────

// StateDeltaMsg:start
//   purpose: Wire format for one room-state delta broadcast over the room's
//            ZenohCrdtSink under the "state" routing key (mirrors ToDeviceGossipMsg
//            in routes/to_device.rs). Serialised with serde_json — a control-plane
//            message, not a hot loop.
//   input:  constructed by publish_state_event (From<&StateEvent>)
//   output: round-trips through serde_json::to_vec / from_slice; converts back to
//           StateEvent (Into<StateEvent>) for AppState::apply_remote_state_event
//   sideEffects: none
// StateDeltaMsg:end
#[cfg(feature = "cluster")]
#[derive(serde::Serialize, serde::Deserialize)]
struct StateDeltaMsg {
    event_type: String,
    state_key: String,
    sender: String,
    content: Value,
    event_id: String,
    room_id: String,
    origin_server_ts: u64,
}

// StateCatchupMsg:start
//   purpose: Wire format for the FULL current state of one room during catch-up
//            (Phase 1 P1.1). Contains ALL state events currently known to the
//            responding node, serialized as Vec<StateDeltaMsg>. The catch-up
//            receiver applies each via AppState::apply_remote_state_event,
//            which converges via LWW (origin_server_ts, event_id) per
//            (event_type, state_key) — the same logic as live replication.
//   input:  constructed by the state queryable from AppState.room_state
//   output: round-trips through serde_json; converts back to StateEvent via
//           Into<StateEvent> for apply_remote_state_event
//   sideEffects: none
// StateCatchupMsg:end
#[cfg(feature = "cluster")]
#[derive(serde::Serialize, serde::Deserialize)]
pub struct StateCatchupMsg {
    events: Vec<StateDeltaMsg>,
}

#[cfg(feature = "cluster")]
impl From<&Vec<StateEvent>> for StateCatchupMsg {
    fn from(events: &Vec<StateEvent>) -> Self {
        StateCatchupMsg {
            events: events.iter().map(|ev| ev.into()).collect(),
        }
    }
}

#[cfg(feature = "cluster")]
impl StateCatchupMsg {
    pub fn into_events(self) -> Vec<StateEvent> {
        self.events.into_iter().map(|msg| msg.into()).collect()
    }
}

#[cfg(feature = "cluster")]
impl From<&StateEvent> for StateDeltaMsg {
    fn from(ev: &StateEvent) -> Self {
        StateDeltaMsg {
            event_type: ev.event_type.clone(),
            state_key: ev.state_key.clone(),
            sender: ev.sender.clone(),
            content: ev.content.clone(),
            event_id: ev.event_id.clone(),
            room_id: ev.room_id.clone(),
            origin_server_ts: ev.origin_server_ts,
        }
    }
}

#[cfg(feature = "cluster")]
impl From<StateDeltaMsg> for StateEvent {
    fn from(msg: StateDeltaMsg) -> Self {
        StateEvent {
            event_type: msg.event_type,
            state_key: msg.state_key,
            sender: msg.sender,
            content: msg.content,
            event_id: msg.event_id,
            room_id: msg.room_id,
            origin_server_ts: msg.origin_server_ts,
        }
    }
}

// publish_state_event:start
//   purpose: Publish one room-state event to its room's ZenohCrdtSink under the
//            "state" routing key (state::ClusterState::state_crdt_key()). Best-effort:
//            a no-op in single-node mode (state.cluster is None) — the event already
//            landed in local room_state via the caller's direct write, so single-node
//            behaviour is unaffected.
//   input:  state — Arc<AppState>; ev — the StateEvent just written locally
//   output: Result<(), HsError>
//   sideEffects: opens/reuses ev.room_id's ZenohCrdtSink lazily; publishes one Zenoh
//                sample under the "state" key
// publish_state_event:end
#[cfg(feature = "cluster")]
pub(crate) async fn publish_state_event(
    state: &Arc<AppState>,
    ev: &StateEvent,
) -> Result<(), HsError> {
    use crate::substrate::crdt::CrdtSink as _;

    let cluster = match &state.cluster {
        Some(c) => c,
        None => return Ok(()), // single-node mode — local write already happened
    };

    let msg: StateDeltaMsg = ev.into();
    let bytes = serde_json::to_vec(&msg).map_err(|e| HsError::Internal(e.to_string()))?;

    let sink = cluster
        .sink_for(&ev.room_id)
        .await
        .map_err(HsError::Internal)?;
    sink.publish(crate::state::ClusterState::state_crdt_key(), bytes)
        .map_err(|e| HsError::Internal(e.to_string()))?;

    Ok(())
}

// drain_cluster_state:start
//   purpose: For every room currently known locally (state.rooms — mirrors
//            routes/sync.rs::drain_cluster_deltas' room enumeration), drain any
//            pending "state" blobs from its ZenohCrdtSink and merge each into
//            room_state via AppState::apply_remote_state_event (LWW by
//            (origin_server_ts, event_id) per (event_type, state_key) — see module
//            header for the full convergence design and what it does NOT guarantee).
//            Called from routes/sync.rs and routes/sliding_sync.rs alongside
//            drain_cluster_deltas / drain_to_device_gossip / drain_cluster_ephemeral /
//            drain_device_list_gossip, so state convergence runs on the same cadence
//            as the room timeline. Malformed blobs (should not happen — only this
//            module ever publishes on the "state" key) are skipped rather than
//            treated as a hard error, so one bad message cannot fail a live /sync call.
//   input:  state — Arc<AppState> with cluster layer active
//   output: Result<(), HsError>
//   sideEffects: may mutate room_state / room_timeline / stream_pos (via
//                apply_remote_state_event); calls state.notify.notify_waiters() once
//                if any delta was actually applied (batched, not per-delta)
// drain_cluster_state:end
#[cfg(feature = "cluster")]
pub(crate) async fn drain_cluster_state(state: &Arc<AppState>) -> Result<(), HsError> {
    use crate::substrate::crdt::CrdtSink as _;

    let cluster = match &state.cluster {
        Some(c) => c,
        None => return Ok(()),
    };

    // Same union as drain_cluster_deltas: rooms learned only via the wildcard discovery
    // subscriber have a sink but no `rooms` entry, and draining just `state.rooms` would
    // leave them without membership/name/power_levels. That gap is not hypothetical —
    // a room created on a peer with no message sent yet publishes on the state channel
    // ONLY, so without this union such a room would never converge here at all.
    let mut room_id_set: std::collections::HashSet<String> = {
        let guard = state
            .rooms
            .lock()
            .map_err(|e| HsError::Internal(e.to_string()))?;
        guard.keys().cloned().collect()
    };
    room_id_set.extend(cluster.list_room_ids());
    let room_ids: Vec<String> = room_id_set.into_iter().collect();

    let state_key = crate::state::ClusterState::state_crdt_key();
    let mut any_applied = false;

    for room_id in &room_ids {
        let sink = cluster.sink_for(room_id).await.map_err(HsError::Internal)?;
        let blobs = sink
            .drain(state_key)
            .map_err(|e| HsError::Internal(e.to_string()))?;

        for bytes in blobs {
            let msg: StateDeltaMsg = match serde_json::from_slice(&bytes) {
                Ok(m) => m,
                Err(_) => continue, // malformed sample — skip, never panic on network input
            };
            let ev: StateEvent = msg.into();
            if state
                .apply_remote_state_event(ev)
                .map_err(HsError::Internal)?
            {
                any_applied = true;
            }
        }
    }

    if any_applied {
        state.notify.notify_waiters();
    }

    Ok(())
}

// room_is_known:start
//   purpose: Whether this node can answer a join for `room_id` at all. Three sources count
//            as known: local state events, a local RoomLog (a room pulled in by catch-up
//            has a log but no member state yet), and a room the cluster has discovered
//            from a peer. This is what stops join from inventing rooms.
//   input:  state, room_id
//   output: bool
//   sideEffects: none — read-only locks, no mutation
//
//   Shared, not copied: `pub(crate)` because the send route asks the same question before
//   its own lazy-create (a request must not bring a room into existence, whichever request
//   it is). Two copies of "known" would drift, and the drift would be invisible.
//   input:  state, room_id
//   output: bool
//
//   Known gap, stated rather than hidden: a room that exists ONLY on a peer and has not
//   been pulled yet is not known here, so join answers 404 until the next catch-up brings
//   it. Pulling on demand is a separate feature — not something to fake by creating an
//   empty shell, which is the defect this replaces.
// room_is_known:end
pub(crate) fn room_is_known(state: &Arc<AppState>, room_id: &str) -> bool {
    if state.room_state.lock().map(|rs| rs.contains_key(room_id)).unwrap_or(false) {
        return true;
    }
    if state.rooms.lock().map(|r| r.contains_key(room_id)).unwrap_or(false) {
        return true;
    }
    #[cfg(feature = "cluster")]
    if let Some(c) = state.cluster.as_ref() {
        if c.list_room_ids().iter().any(|r| r == room_id) {
            return true;
        }
    }
    false
}
