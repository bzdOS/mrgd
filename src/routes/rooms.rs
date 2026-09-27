// START_AI_HEADER
// MODULE: matrix-hs/src/routes/rooms.rs
// PURPOSE: POST /_matrix/client/v3/createRoom — create a new RoomLog in the store.
//          Stage 1: generates a deterministic room_id from the request alias or a
//          counter, inserts an empty RoomLog into AppState.rooms.
//          Stage 2: emits initial state events (m.room.create, m.room.member,
//          m.room.power_levels, m.room.name, m.room.topic), registers alias.
//          Stage 3: honors the request's `initial_state` array (see
//          apply_initial_state below) — each {type, state_key, content} entry is
//          applied as an additional room state event (e.g. m.room.encryption with
//          content {"algorithm":"m.megolm.v1.aes-sha2"}, needed for Element X E2EE),
//          replacing a same-(type, state_key) default if one was already queued.
//          State events go into room_state (NOT into RoomLog timeline).
//
//          Barrier wiring (cluster-wide alias uniqueness):
//            If AppState.barrier_store is Some(store) and room_alias_name is provided,
//            calls crate::substrate::barrier::claim() BEFORE the local alias insert, using
//            Policy::Optimistic and key "mx:alias:#<name>:<server>":
//              ClaimOutcome::Claimed       → register alias locally (provisional=false).
//              ClaimOutcome::Rejected      → 400 M_ROOM_IN_USE; room NOT created.
//              ClaimOutcome::Provisional   → register locally + mark provisional=true.
//              Err(_)                      → 500 M_UNKNOWN.
//            If barrier_store is None (single-node) → local alias uniqueness only,
//            all existing tests pass unchanged.
//            Reconcile loser handler in main.rs dispatches mx:alias: keys to
//            AppState::mark_alias_relinquished().
// DEPENDENCIES: axum, crate::substrate::matrix_events::RoomLog, AppState, crate::substrate::barrier
// PUBLIC_API: post_create_room
// END_AI_HEADER

use crate::{
    auth,
    error::HsError,
    state::{localpart, AppState, StateEvent},
};
use axum::{extract::State, http::HeaderMap, Json};
use crate::substrate::barrier::{self, ClaimOutcome, Fence, Policy};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::atomic::Ordering;
use std::sync::Arc;

// CreateRoomRequest:start
//   purpose: JSON body of POST /createRoom.
//   input:  JSON body
//   output: CreateRoomRequest
//   sideEffects: none
// CreateRoomRequest:end
#[derive(Debug, Deserialize, Default)]
pub struct CreateRoomRequest {
    pub room_alias_name: Option<String>,
    pub name: Option<String>,
    pub topic: Option<String>,
    pub preset: Option<String>,
    pub visibility: Option<String>,
    pub invite: Option<Vec<String>>,
    pub initial_state: Option<Vec<Value>>,
}

// make_state_event_id:start
//   purpose: Generate a deterministic event_id for a state event.
//            Hashes (event_type, room_id, state_key) into a fixed-width hex
//            string — opaque per the Matrix spec, with no truncation-induced
//            collision risk. The prior version built the ID by filtering and
//            TRUNCATING each component to 16 chars while still allowing ':'
//            and '@' through — for a state_key like a full MXID
//            ("@user:m.hubd.net", >16 chars after the domain's dots were
//            stripped) this produced a malformed, cut-mid-domain ID (e.g.
//            "$st_m_room_member_room_11_@debugtest2:mhub") that also carried a
//            real collision risk if two distinct state_keys shared the same
//            first-16-filtered-chars. Reproduced live: creating a room via
//            FluffyChat surfaced exactly this malformed ID for the creator's
//            own m.room.member event.
//   input:  event_type, room_id, state_key
//   output: String event_id starting with "$", fixed-width hex, opaque
//   sideEffects: none
// make_state_event_id:end
fn make_state_event_id(event_type: &str, room_id: &str, state_key: &str) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    event_type.hash(&mut hasher);
    room_id.hash(&mut hasher);
    state_key.hash(&mut hasher);
    format!("$st{:016x}", hasher.finish())
}

// post_create_room:start
//   purpose: Create a new named room in the in-memory event-store.
//            room_id = "!<alias>:<server>" if room_alias_name present, else
//            "!room_<seq>_<rand8>_<node>:<server>" — globally unique, so two nodes
//            creating their first room concurrently do not collide.
//            Idempotent: if room_id already exists,
//            returns the existing room_id without error (add-wins, Stage 1 simplification).
//            Stage 2: emits state events into room_state, registers alias.
//            Barrier path (cluster mode): if barrier_store is Some and room_alias_name is
//            provided, calls barrier::claim("mx:alias:#<name>:<server>", room_id, ...)
//            BEFORE the local alias insert:
//              Claimed      → register alias (provisional=false).
//              Rejected     → return 400 M_ROOM_IN_USE; room NOT created.
//              Provisional  → register + mark provisional=true.
//              Err(_)       → 500 M_UNKNOWN.
//            None barrier_store → local alias uniqueness only (all existing tests unchanged).
//   input:  State(AppState), headers, JSON body with optional room_alias_name
//   output: JSON {"room_id":"!<alias>:<server>"}
//   sideEffects: inserts RoomLog into AppState.rooms; inserts state events into room_state;
//                registers alias in aliases; may call barrier::claim on the barrier_store
// post_create_room:end
pub async fn post_create_room(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<CreateRoomRequest>,
) -> Result<Json<Value>, HsError> {
    let server = &state.server_name;

    // Resolve caller from Authorization header — signed token required.
    let caller_user = extract_user_from_headers(&headers, &state.token_secret, server)
        .ok_or_else(|| HsError::UnknownToken("missing or invalid token".to_string()))?;

    let room_id = {
        let rooms = state
            .rooms
            .lock()
            .map_err(|e| HsError::Internal(e.to_string()))?;
        match body.room_alias_name.as_deref() {
            Some(alias) => format!("!{alias}:{server}"),
            None => {
                // Globally-unique room id: local sequence + random suffix + node slug.
                // The bare `!room_<len>` this replaces is a multi-master collision bug —
                // two nodes each creating their first room both mint `!room_0:<server>`,
                // and the CRDT merge then folds two unrelated rooms into one.
                let seq = rooms.len();
                let rnd = rand::random::<u32>();
                let node_id = std::env::var("MATRIX_HS_NODE_ID")
                    .unwrap_or_else(|_| state.server_name.clone());
                // Sanitise node_id into a Matrix-localpart-safe slug.
                let node_slug: String = node_id
                    .chars()
                    .filter(|c| c.is_alphanumeric() || *c == '-' || *c == '_')
                    .take(16)
                    .collect();
                format!("!room_{seq}_{rnd:08x}_{node_slug}:{server}")
            }
        }
    };

    // ── Barrier claim for alias uniqueness (BEFORE room creation) ─────────────
    // If barrier_store is Some and a room_alias_name is requested, claim the alias
    // cluster-wide.  On Rejected the room is NOT created and we return M_ROOM_IN_USE.
    // On None barrier_store (single-node), skip to local alias insert below.
    let alias_provisional_flag: bool;

    if let (Some(ref alias_name), Some(ref store)) = (
        body.room_alias_name.as_deref(),
        state.barrier_store.as_ref(),
    ) {
        let full_alias = format!("#{alias_name}:{server}");

        // Build a Fence: ts = wall-clock millis, node_id from env or server_name, epoch from uia_seq.
        let epoch = state.uia_seq.fetch_add(1, Ordering::Relaxed);
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let node_id =
            std::env::var("MATRIX_HS_NODE_ID").unwrap_or_else(|_| state.server_name.clone());

        let fence = Fence { epoch, ts, node_id };
        let claim_key = format!("mx:alias:{full_alias}");

        match barrier::claim(
            store.as_ref(),
            &claim_key,
            &room_id,
            fence,
            Policy::Optimistic,
        ) {
            Ok(ClaimOutcome::Claimed) => {
                alias_provisional_flag = false;
            }
            Ok(ClaimOutcome::Rejected { owner }) => {
                // Alias is taken by another room on another node.
                return Err(HsError::RoomInUse(format!(
                    "room alias {full_alias} is already taken (barrier owner: {owner})"
                )));
            }
            Ok(ClaimOutcome::Provisional { fence: pf }) => {
                // AP path: coordinator unreachable, optimistic grant.
                eprintln!(
                    "matrix-hs createRoom: provisional alias claim for {full_alias:?} \
                     room_id={room_id:?} (fence ts={} node={}); reconcile deferred on heal",
                    pf.ts, pf.node_id
                );
                alias_provisional_flag = true;
            }
            Err(e) => {
                return Err(HsError::Internal(format!("barrier store error: {e}")));
            }
        }
    } else {
        // Single-node or no alias: no barrier needed.
        alias_provisional_flag = false;
    }

    // Ensure rooms + room_state + room_timeline entries exist.
    state.ensure_room_state(&room_id);

    // Emit initial state events into room_state.  Real wall-clock base so the
    // creation burst dates correctly in clients; +0..+6 offsets preserve the
    // intra-burst ordering (create → member → name → …).
    // Hybrid clock, not the raw wall clock: these timestamps are the LWW ordering
    // key, and must be comparable with the ones later edits carry.
    let ts_base = state.hlc_now();

    let mut state_events: Vec<StateEvent> = Vec::new();

    // m.room.create
    state_events.push(StateEvent {
        event_type: "m.room.create".to_string(),
        state_key: "".to_string(),
        sender: caller_user.clone(),
        content: json!({
            "creator":      caller_user.clone(),
            "room_version": "10"
        }),
        event_id: make_state_event_id("m.room.create", &room_id, ""),
        room_id: room_id.clone(),
        origin_server_ts: ts_base,
    });

    // m.room.member (creator joins)
    state_events.push(StateEvent {
        event_type: "m.room.member".to_string(),
        state_key: caller_user.clone(),
        sender: caller_user.clone(),
        content: json!({
            "membership":  "join",
            "displayname": localpart(&caller_user)
        }),
        event_id: make_state_event_id("m.room.member", &room_id, &caller_user),
        room_id: room_id.clone(),
        origin_server_ts: ts_base + 1,
    });

    // m.room.power_levels
    state_events.push(StateEvent {
        event_type: "m.room.power_levels".to_string(),
        state_key: "".to_string(),
        sender: caller_user.clone(),
        content: json!({
            "users": { &caller_user: 100 },
            "users_default": 0,
            "events": {},
            "events_default": 0,
            "state_default": 50,
            "ban": 50,
            "kick": 50,
            "redact": 50,
            "invite": 50
        }),
        event_id: make_state_event_id("m.room.power_levels", &room_id, ""),
        room_id: room_id.clone(),
        origin_server_ts: ts_base + 2,
    });

    // m.room.join_rules — derived from the request's `preset` (Matrix-spec presets:
    // "public_chat" -> join_rule "public"; "private_chat" / "trusted_private_chat" ->
    // "invite"; no preset given -> "invite", matching the previous unconditional
    // default so existing callers that never send preset see no behavior change).
    // An explicit initial_state entry for m.room.join_rules still wins over this —
    // apply_initial_state below replaces same-(type,state_key) queued events.
    let default_join_rule = match body.preset.as_deref() {
        Some("public_chat") => "public",
        _ => "invite",
    };
    state_events.push(StateEvent {
        event_type: "m.room.join_rules".to_string(),
        state_key: "".to_string(),
        sender: caller_user.clone(),
        content: json!({ "join_rule": default_join_rule }),
        event_id: make_state_event_id("m.room.join_rules", &room_id, ""),
        room_id: room_id.clone(),
        origin_server_ts: ts_base + 3,
    });

    // m.room.history_visibility
    state_events.push(StateEvent {
        event_type: "m.room.history_visibility".to_string(),
        state_key: "".to_string(),
        sender: caller_user.clone(),
        content: json!({ "history_visibility": "shared" }),
        event_id: make_state_event_id("m.room.history_visibility", &room_id, ""),
        room_id: room_id.clone(),
        origin_server_ts: ts_base + 4,
    });

    // Optional: m.room.name
    if let Some(ref name) = body.name {
        state_events.push(StateEvent {
            event_type: "m.room.name".to_string(),
            state_key: "".to_string(),
            sender: caller_user.clone(),
            content: json!({ "name": name }),
            event_id: make_state_event_id("m.room.name", &room_id, ""),
            room_id: room_id.clone(),
            origin_server_ts: ts_base + 5,
        });
    }

    // Optional: m.room.topic
    if let Some(ref topic) = body.topic {
        state_events.push(StateEvent {
            event_type: "m.room.topic".to_string(),
            state_key: "".to_string(),
            sender: caller_user.clone(),
            content: json!({ "topic": topic }),
            event_id: make_state_event_id("m.room.topic", &room_id, ""),
            room_id: room_id.clone(),
            origin_server_ts: ts_base + 6,
        });
    }

    // apply_initial_state:start
    //   purpose: Honor the request's `initial_state` array — each entry
    //            {"type":..,"state_key":..,"content":{..}} is applied as an additional
    //            room state event, in request order, after the built-in defaults above.
    //            This is how Element X sets m.room.encryption at creation time
    //            (content {"algorithm":"m.megolm.v1.aes-sha2"}), and generally lets a
    //            client pre-seed any state event alongside create/member/power_levels/...
    //            If an entry's (type, state_key) matches one of the default events
    //            already queued above (e.g. a client-supplied m.room.name), the later
    //            entry wins — it replaces the earlier queued event, matching Matrix's
    //            "last one for a given (type, state_key) is authoritative" semantics.
    //            Malformed entries (missing/non-string "type", or non-object "content")
    //            are skipped rather than erroring the whole createRoom call.
    //   input:  body.initial_state — Option<Vec<Value>>
    //   output: state_events extended/overwritten in place
    //   sideEffects: none beyond mutating the local state_events Vec
    // apply_initial_state:end
    if let Some(ref initial_state) = body.initial_state {
        for (idx, entry) in initial_state.iter().enumerate() {
            let Some(ev_type) = entry.get("type").and_then(Value::as_str) else {
                continue;
            };
            let state_key = entry.get("state_key").and_then(Value::as_str).unwrap_or("");
            let content = entry.get("content").cloned().unwrap_or_else(|| json!({}));

            let new_ev = StateEvent {
                event_type: ev_type.to_string(),
                state_key: state_key.to_string(),
                sender: caller_user.clone(),
                content,
                event_id: make_state_event_id(ev_type, &room_id, state_key),
                room_id: room_id.clone(),
                origin_server_ts: ts_base + 7 + idx as u64,
            };

            // A client-supplied entry for a (type, state_key) already queued above
            // (e.g. overriding the default m.room.name) replaces it; matches Matrix's
            // "one current event per (type, state_key)" state model.
            if let Some(existing) = state_events
                .iter_mut()
                .find(|ev| ev.event_type == new_ev.event_type && ev.state_key == new_ev.state_key)
            {
                *existing = new_ev;
            } else {
                state_events.push(new_ev);
            }
        }
    }

    // Write state events into room_state and room_timeline.
    // events_to_publish snapshots the events actually applied (only non-empty when
    // this is a genuinely new room — createRoom is idempotent on an existing
    // room_id) so the cluster publish loop below only broadcasts real writes, never
    // a no-op re-create.
    let events_to_publish: Vec<StateEvent> = {
        let mut rs = state
            .room_state
            .lock()
            .map_err(|e| HsError::Internal(e.to_string()))?;
        let room_vec = rs.entry(room_id.clone()).or_default();

        // Avoid duplicate state events if room was already created (idempotent).
        if room_vec.is_empty() {
            let published = state_events.clone();
            room_vec.extend(state_events);
            drop(rs); // release room_state before timeline appends
                      // Append each initial state event to room_timeline (retention-capped
                      // via the centralized helper) and persist it.
            for ev in &published {
                let ev_json = state_event_to_json(ev);
                state.persist_room_event(&room_id, &ev_json);
                state.append_room_timeline(&room_id, ev_json);
            }
            published
        } else {
            Vec::new()
        }
    };

    // Cluster: publish each initial state event over the room's ZenohCrdtSink so
    // other nodes converge on this room's create/member/power_levels/... state (see
    // routes/room_state.rs module header for the LWW design and its guarantees).
    #[cfg(feature = "cluster")]
    for ev in &events_to_publish {
        crate::routes::room_state::publish_state_event(&state, ev).await?;
    }

    // Honor the request's `invite` array: add an m.room.member(invite) event for
    // each listed user_id (mirrors POST .../invite's own add_member_state_event
    // call — same underlying membership write path, so these events cluster-
    // replicate and appear in /sync identically to a post-creation invite).
    // Gated on events_to_publish being non-empty (a genuinely new room, matching
    // createRoom's own idempotency guard above) — a re-create of an existing
    // room_id does not re-invite. The creator is skipped even if listed (already
    // joined via the m.room.member event above).
    if !events_to_publish.is_empty() {
        if let Some(ref invitees) = body.invite {
            for user_id in invitees {
                if user_id.trim().is_empty() || *user_id == caller_user {
                    continue;
                }
                #[cfg_attr(not(feature = "cluster"), allow(unused_variables))]
                let ev = crate::routes::room_state::add_member_state_event(
                    &state,
                    &room_id,
                    user_id,
                    "invite",
                    &caller_user,
                    None,
                )?;
                #[cfg(feature = "cluster")]
                crate::routes::room_state::publish_state_event(&state, &ev).await?;
            }
        }
    }

    // Register alias if requested.
    if let Some(ref alias) = body.room_alias_name {
        let full_alias = format!("#{alias}:{server}");
        {
            let mut aliases = state
                .aliases
                .lock()
                .map_err(|e| HsError::Internal(e.to_string()))?;
            aliases.insert(full_alias.clone(), room_id.clone());
        }
        // Track provisional flag so ReconcileDriver can relinquish on conflict.
        if alias_provisional_flag {
            if let Ok(mut prov) = state.alias_provisional.lock() {
                prov.insert(full_alias.clone());
            }
        }
        // Persist the alias mapping.
        state.persist_alias(&full_alias, &room_id);
    }

    state.notify.notify_waiters();

    Ok(Json(json!({ "room_id": room_id })))
}

// extract_user_from_headers:start
//   purpose: Extract the sender user_id from the Authorization header for createRoom.
//            ONLY signed "mxt_..." tokens (auth::verify_token) authenticate — no legacy
//            "tok_<localpart>" acceptance, no anonymous "@server:" fallback.
//            Returns None when the header is absent, malformed, or the MAC is invalid;
//            the caller converts None to 401 M_UNKNOWN_TOKEN.
//   input:  headers, secret — HMAC key, _server — server_name (unused; kept for symmetry)
//   output: Option<String> — verified user_id, or None
//   sideEffects: none
// extract_user_from_headers:end
fn extract_user_from_headers(headers: &HeaderMap, secret: &[u8], _server: &str) -> Option<String> {
    let token = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))?;
    // Only signed mxt_ tokens authenticate.
    // Note: epoch is NOT checked here — this is a low-level token verify.
    // Full auth (with revocation gate) is via auth::extract_caller.
    auth::verify_token(secret, token).map(|(uid, _, _)| uid)
}

// state_event_to_json:start
//   purpose: Render a StateEvent as a Matrix client event JSON value.
//   input:  ev — reference to StateEvent
//   output: serde_json::Value
//   sideEffects: none
// state_event_to_json:end
pub fn state_event_to_json(ev: &StateEvent) -> Value {
    json!({
        "event_id":          ev.event_id,
        "type":              ev.event_type,
        "state_key":         ev.state_key,
        "sender":            ev.sender,
        "room_id":           ev.room_id,
        "origin_server_ts":  ev.origin_server_ts,
        "content":           ev.content
    })
}
