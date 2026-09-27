// START_AI_HEADER
// MODULE: matrix-hs/src/routes/sync.rs
// PURPOSE: GET /_matrix/client/v3/sync — return the event timeline for joined rooms.
//          This is the "pull" half of the push/pull proof.
//
//          Stage 1 sync model:
//            - No filtering, no since-token pagination (full snapshot every time).
//            - All rooms in the store are returned as "joined" rooms.
//            - Timeline events = message events from RoomLog.ordered() only.
//            - State events go in room.state.events, NOT timeline.events.
//
//          Stage 2 additions:
//            - SyncParams: since, timeout, filter, full_state, set_presence
//            - since="s<N>": incremental sync — only events with stream_pos > N
//            - Long-poll: if since given, no new events, and timeout > 0, wait
//            - next_batch = "s<stream_pos>"
//            - State events in state.events (initial) or state.events (changed, incremental)
//            - Message events in timeline.events only
//            - Full response envelope: account_data, presence, to_device, device_lists, etc.
//
//          cluster feature: drain deltas, assign stream positions to new PDUs.
//
// DEPENDENCIES: axum, crate::substrate::matrix_events::RoomLog, AppState
// PUBLIC_API: get_sync
// END_AI_HEADER

use crate::{auth, error::HsError, routes::rooms::state_event_to_json, state::AppState};
use axum::{
    extract::{Query, State},
    http::HeaderMap,
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::atomic::Ordering;
use std::sync::Arc;

// extract_caller now lives in auth.rs (canonical implementation shared across route
// modules — see routes/keys.rs, routes/sliding_sync.rs, routes/to_device.rs). Derives
// (user_id, device_id) from the signed Bearer token, to know whose to-device queue to
// drain. Returns None when the header is absent or the token is invalid — callers
// treat that as "no known device", so to_device.events stays empty (unchanged
// behaviour for any caller that doesn't authenticate, e.g. existing tests).
use auth::extract_caller;

// SyncParams:start
//   purpose: Query parameters for GET /sync.
//   input:  query string from client
//   output: SyncParams struct
//   sideEffects: none
// SyncParams:end
#[derive(Debug, Deserialize, Default)]
pub struct SyncParams {
    pub since: Option<String>,
    pub timeout: Option<u64>,
    pub filter: Option<String>,
    pub full_state: Option<bool>,
    pub set_presence: Option<String>,
}

// parse_since:start
//   purpose: Parse a since token of the form "s<N>" into u64.
//            Returns None if absent or malformed.
//   input:  since — optional string from query param
//   output: Option<u64>
//   sideEffects: none
// parse_since:end
fn parse_since(since: &Option<String>) -> Option<u64> {
    since
        .as_deref()
        .and_then(|s| s.strip_prefix('s'))
        .and_then(|n| n.parse::<u64>().ok())
}

// get_sync:start
//   purpose: Return a sync response.
//            Without since: full state snapshot + all timeline message events.
//            With since=sN: incremental — only events with stream_pos > N.
//            Long-poll: if since present, no new events, timeout > 0: wait up to timeout ms.
//            State events go in room.state.events; message events in timeline.events.
//            cluster mode: drain pending Zenoh deltas into each room's RoomLog first.
//   input:  State(AppState), Query(SyncParams), headers
//   output: JSON sync response
//   sideEffects: (cluster) drains Zenoh inbox; may block up to timeout ms
// get_sync:end
pub async fn get_sync(
    State(state): State<Arc<AppState>>,
    Query(params): Query<SyncParams>,
    headers: HeaderMap,
) -> Result<Json<Value>, HsError> {
    // cluster: run the unified drain sequence (timeline deltas, to-device gossip,
    // ephemeral gossip, room-state deltas, device-list gossip) before building the
    // response — see drain_all_cluster for the fixed order and why it matters.
    #[cfg(feature = "cluster")]
    drain_all_cluster(&state).await?;

    let since_pos = parse_since(&params.since);
    let timeout_ms = params.timeout.unwrap_or(0);

    // Long-poll: if incremental sync with no new events and timeout > 0, wait.
    if since_pos.is_some() && timeout_ms > 0 {
        let current = state.stream_pos.load(Ordering::SeqCst);
        let since_n = since_pos.unwrap_or(0);

        if current <= since_n {
            // Wait for up to timeout_ms (capped at 30s).
            let wait_ms = timeout_ms.min(30_000);
            let notified = state.notify.notified();
            let _ =
                tokio::time::timeout(tokio::time::Duration::from_millis(wait_ms), notified).await;
            // After wakeup or timeout, drain cluster deltas again.
            #[cfg(feature = "cluster")]
            drain_all_cluster(&state).await?;
        }
    }

    let current_pos = state.stream_pos.load(Ordering::SeqCst);
    let next_batch = format!("s{current_pos}");

    // to_device / device_lists / device_one_time_keys_count: only known when the caller
    // authenticates (Bearer token) — see extract_caller. Unauthenticated callers (and all
    // pre-existing tests that never send a token) keep getting the same empty defaults as
    // before this feature (device_lists.changed=[], device_one_time_keys_count={}).
    let caller = extract_caller(&headers, &state);

    // account_data (account_data feature): global events are only known for an
    // authenticated caller — see AppState::account_data_global_events. Per-room
    // account_data is filled in per-room below via account_data_room_events
    // (needs the caller's user_id, so build_join_rooms takes it too).
    let account_data_user = caller.as_ref().map(|(uid, _)| uid.as_str());
    let global_account_data: Vec<Value> = account_data_user
        .map(|uid| state.account_data_global_events(uid))
        .unwrap_or_default();

    let join_rooms = build_join_rooms(&state, since_pos, account_data_user)?;
    let invite_rooms = account_data_user
        .map(|uid| build_invite_rooms(&state, uid))
        .unwrap_or_default();

    let to_device_events = match &caller {
        Some((user_id, device_id)) => state
            .drain_to_device(user_id, device_id, since_pos)
            .map_err(HsError::Internal)?,
        None => Vec::new(),
    };

    // device_lists.changed: Matrix-correct only on an INCREMENTAL sync (since given) — an
    // initial sync's client is expected to run its own full keys/query instead (see
    // module header). Candidates = every user whose device list changed after since_pos,
    // filtered down to users who share a room with the caller (simplest correct
    // approximation — see AppState::users_sharing_room_with for the encrypted-room seam).
    // device_lists.left is not populated: computing "no longer shares any room" needs a
    // membership-history query this server does not keep (only current room_state) — a
    // clearly-marked seam, left as an empty array (spec-legal: an empty array just means
    // "nothing to report", never wrong, only incomplete).
    let device_lists_changed: Vec<String> = match (&caller, since_pos) {
        (Some((caller_user_id, _)), Some(since_n)) => {
            let shared = state.users_sharing_room_with(caller_user_id);
            // Include a changed user if they share a room with the caller OR they
            // ARE the caller. A user must always learn about their OWN device-list /
            // cross-signing changes via device_lists.changed so the client re-queries
            // its own keys — without this, a freshly-registered user (not yet in any
            // room) never sees their own cross-signing key upload, and e.g.
            // matrix-dart-sdk's bootstrap spins forever in its "waiting for master to
            // be created" oneShotSync loop. Self is always relevant to self.
            let mut changed: Vec<String> = state
                .device_list_changes_since(since_n)
                .into_iter()
                .filter(|u| u == caller_user_id || shared.contains(u))
                .collect();
            changed.sort();
            changed
        }
        _ => Vec::new(),
    };

    // device_one_time_keys_count: the CALLER's own remaining OTK inventory (reuses
    // keys.rs's count_by_algorithm — the same tally keys/upload and keys/query return).
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

    // org.mrgd.renamed: server-push counterpart to GET /whoami's pull-based rename
    // discovery (AppState.renamed — see apply_username_loss's doc comment). Before
    // this, a renamed user's existing long-polling /sync loop never saw anything
    // different: only an explicit GET /whoami or POST /login re-check ever consulted
    // the renamed map, so a client that just kept syncing could go arbitrarily long
    // without noticing its username was reassigned after losing a grow-set conflict.
    // Not a real Matrix field — there is no spec concept of a server-initiated
    // rename — so it rides on the same "clients ignore unknown top-level keys"
    // tolerance every unstable MSC field already depends on. The caller's token
    // still authenticates as the ORIGINAL identity regardless (tokens are not
    // re-signed, and already-sent events keep their original sender); this only
    // gives the client somewhere new to log in, it does not change what already
    // happened.
    let renamed_field = caller.as_ref().and_then(|(user_id, _)| {
        let orig_localpart = user_id
            .strip_prefix('@')
            .and_then(|s| s.split(':').next())
            .unwrap_or(user_id.as_str());
        state
            .renamed
            .lock()
            .ok()
            .and_then(|renamed| renamed.get(orig_localpart).cloned())
    });

    let mut body = json!({
        "next_batch": next_batch,
        "rooms": {
            "join":   join_rooms,
            "invite": invite_rooms,
            "leave":  {}
        },
        "account_data":             { "events": global_account_data },
        "presence":                 { "events": [] },
        "to_device":                { "events": to_device_events },
        "device_lists":             { "changed": device_lists_changed, "left": [] },
        "device_one_time_keys_count": device_one_time_keys_count
    });
    if let Some(new_user_id) = renamed_field {
        body["org.mrgd.renamed"] = json!({ "user_id": new_user_id });
    }

    Ok(Json(body))
}

// build_join_rooms:start
//   purpose: Build the rooms.join map for the sync response.
//            For each room: state events in state.events, message events in timeline.events.
//            If since_pos is Some(N), only include events with stream_pos > N.
//            State events from room_state are only included in initial sync (since_pos=None)
//            OR if they appear in room_timeline with pos > N (for state changes after join).
//            SECURITY: only includes a room if account_data_user is Some (authenticated)
//            AND that user's own m.room.member event in the room has membership=="join".
//            Previously this iterated EVERY room in the server unconditionally — any
//            caller, including a brand-new registration or a fully unauthenticated
//            request, got every other user's rooms back with full history. Reproduced
//            live: a freshly-registered account appeared already "joined" to 14
//            unrelated rooms via /sync. Fixed here; the mirror bug in
//            routes/sliding_sync.rs::build_rooms is fixed the same way.
//   input:  state — Arc<AppState>, since_pos — parsed since token,
//           account_data_user — the authenticated caller's user_id (None if
//           unauthenticated — returns an empty map), used both for the membership
//           check and to fill each room's account_data.events block (see
//           AppState::account_data_room_events)
//   output: Result<serde_json::Map<String, Value>, HsError>
//   sideEffects: acquires room_state, room_timeline mutexes
// build_join_rooms:end
fn build_join_rooms(
    state: &Arc<AppState>,
    since_pos: Option<u64>,
    account_data_user: Option<&str>,
) -> Result<serde_json::Map<String, Value>, HsError> {
    let Some(caller_user_id) = account_data_user else {
        return Ok(serde_json::Map::new());
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

    let mut join_rooms = serde_json::Map::new();

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
        // Scripting hook (DC++-style): a Lua on_room_visible(user, room) may
        // grant visibility into rooms this caller is NOT joined to (e.g. a
        // monitoring account). Membership is the default; this can only extend.
        let visible = caller_is_joined
            || state
                .scripting
                .on_room_visible(caller_user_id, room_id.as_str());
        if !visible {
            continue;
        }

        let timeline_entries = rt_guard
            .get(room_id.as_str())
            .map(|v| v.as_slice())
            .unwrap_or(&[]);

        let (timeline_events, state_json_events): (Vec<Value>, Vec<Value>) = match since_pos {
            None => {
                // Initial sync: all message events from timeline (filter out state events).
                // State events are included in state.events array.
                let msgs: Vec<Value> = timeline_entries
                    .iter()
                    .filter(|(_, ev)| {
                        // Only include events that are NOT state events (no state_key field).
                        ev.get("state_key").is_none()
                    })
                    .map(|(_, ev)| state.apply_redaction(ev))
                    .collect();

                let state_evs: Vec<Value> = state_events.iter().map(state_event_to_json).collect();

                (msgs, state_evs)
            }
            Some(n) => {
                // Incremental sync: events with pos > n.
                // Split into state events (have state_key) and message events (no state_key).
                let new_entries: Vec<&Value> = timeline_entries
                    .iter()
                    .filter(|(pos, _)| *pos >= n)
                    .map(|(_, ev)| ev)
                    .collect();

                let mut msgs: Vec<Value> = Vec::new();
                let mut state_evs: Vec<Value> = Vec::new();
                for ev in new_entries {
                    if ev.get("state_key").is_some() {
                        state_evs.push(ev.clone());
                    } else {
                        msgs.push(state.apply_redaction(ev));
                    }
                }

                // JOIN BACKFILL: if this sync carries the caller's own join
                // membership for the room, the room is NEW to them — every
                // message event sits below their since-token and `msgs` would
                // be empty, handing a newly-joined client an invisible empty
                // room until unrelated traffic arrives. Synapse sends the
                // recent timeline on join; mirror that by falling back to the
                // initial-sync message selection. room_timeline is already
                // retention-capped, so "all messages" is bounded by the same
                // cap an initial sync would return. Found live 2026-08-24
                // driving BareChat's real matrix-rust-sdk against this server.
                let joined_now = state_evs.iter().any(|ev| {
                    ev.get("event_type").and_then(|v| v.as_str()) == Some("m.room.member")
                        || ev.get("type").and_then(|v| v.as_str()) == Some("m.room.member")
                }) && state_evs.iter().any(|ev| {
                    ev.get("state_key").and_then(|v| v.as_str()) == Some(caller_user_id)
                        && ev.pointer("/content/membership").and_then(|v| v.as_str()) == Some("join")
                });
                if joined_now && msgs.is_empty() {
                    msgs = timeline_entries
                        .iter()
                        .filter(|(_, ev)| ev.get("state_key").is_none())
                        .map(|(_, ev)| state.apply_redaction(ev))
                        .collect();
                }
                (msgs, state_evs)
            }
        };

        let ephemeral_events = ephemeral_events_for_room(state, room_id);
        let room_account_data: Vec<Value> = account_data_user
            .map(|uid| state.account_data_room_events(uid, room_id))
            .unwrap_or_default();

        // For incremental sync, only include rooms that have new timeline/state
        // events, a non-empty ephemeral block (typing/receipts are level-triggered,
        // not stream-position-gated, so a typing-only update must not be dropped
        // here), or any account_data/tags set for this room (also level-triggered,
        // same reasoning). For initial sync, include all rooms.
        if since_pos.is_some()
            && timeline_events.is_empty()
            && state_json_events.is_empty()
            && ephemeral_events.is_empty()
            && room_account_data.is_empty()
        {
            continue;
        }

        join_rooms.insert(
            room_id.clone(),
            json!({
                "timeline": {
                    "events":     timeline_events,
                    "limited":    false,
                    "prev_batch": ""
                },
                "state":     { "events": state_json_events },
                "ephemeral": { "events": ephemeral_events },
                "account_data": { "events": room_account_data }
            }),
        );
    }

    Ok(join_rooms)
}

// build_invite_rooms:start
//   purpose: Build the rooms.invite map for the sync response — rooms where
//            caller_user_id has a PENDING invite (membership=="invite") but has
//            not joined. Per spec this uses "stripped state" (a small, unsigned
//            subset of state events — just enough for a client to render an
//            invite: create, name, join_rules, and the invite's own
//            m.room.member event), not the full state/timeline a joined member
//            gets. Previously hardcoded to {} — combined with build_join_rooms
//            having no membership filter at all, an invited-but-not-joined user
//            saw the room as already fully joined instead of a pending invite
//            (reproduced live via FluffyChat's invite flow).
//   input:  state — Arc<AppState>, caller_user_id — the authenticated caller's user_id
//   output: serde_json::Map<String, Value> — room_id -> {"invite_state":{"events":[...]}}
//   sideEffects: acquires room_state mutex
// build_invite_rooms:end
fn build_invite_rooms(
    state: &Arc<AppState>,
    caller_user_id: &str,
) -> serde_json::Map<String, Value> {
    let mut invite_rooms = serde_json::Map::new();
    let Ok(room_state_guard) = state.room_state.lock() else {
        return invite_rooms;
    };

    for (room_id, state_events) in room_state_guard.iter() {
        let invite_event = state_events.iter().find(|ev| {
            ev.event_type == "m.room.member"
                && ev.state_key == caller_user_id
                && ev.content.get("membership").and_then(|v| v.as_str()) == Some("invite")
        });
        let Some(invite_event) = invite_event else {
            continue;
        };

        let stripped: Vec<Value> = state_events
            .iter()
            .filter(|ev| {
                matches!(
                    ev.event_type.as_str(),
                    "m.room.create" | "m.room.name" | "m.room.join_rules"
                ) || std::ptr::eq(*ev, invite_event)
            })
            .map(state_event_to_json)
            .collect();

        invite_rooms.insert(
            room_id.clone(),
            json!({ "invite_state": { "events": stripped } }),
        );
    }

    invite_rooms
}

// ephemeral_events_for_room:start
//   purpose: Build the room's "ephemeral.events" array: an "m.typing" event when
//            anyone is currently (non-expired) typing, and an "m.receipt" event
//            when any read receipts have been recorded.  Both are computed fresh
//            on every call (ephemeral EDUs are level-triggered state, not part of
//            the stream_pos-ordered timeline) — cheap in-memory reads, no locks
//            held across the call boundary.
//   input:  state — Arc<AppState>; room_id
//   output: Vec<Value> — 0, 1, or 2 ephemeral ClientEvent-shaped JSON values
//   sideEffects: none (typing_user_ids prunes expired local entries as a side benefit)
// ephemeral_events_for_room:end
fn ephemeral_events_for_room(state: &Arc<AppState>, room_id: &str) -> Vec<Value> {
    let mut events = Vec::new();

    let typing_ids = state.typing_user_ids(room_id);
    if !typing_ids.is_empty() {
        events.push(json!({
            "type":    "m.typing",
            "content": { "user_ids": typing_ids }
        }));
    }

    if let Some(content) = state.receipt_event_content(room_id) {
        events.push(json!({
            "type":    "m.receipt",
            "content": content
        }));
    }

    events
}

// drain_all_cluster:start
//   purpose: Run the full set of cluster-drain steps in the fixed order every
//            sync-style endpoint needs before building its response: room timeline
//            deltas, to-device gossip, ephemeral (typing/receipts) gossip, room-state
//            deltas, then device-list-change gossip. Unifies the ~4 previously
//            scattered call sites (routes/sync.rs and routes/sliding_sync.rs, each at
//            an initial-drain site and a post-long-poll-wakeup site) into one call.
//            Order matters and is preserved exactly as before: drain_cluster_state
//            must run before device_list_changes_since is read by callers (see
//            drain_cluster_state's own doc), so drain_device_list_gossip stays last.
//   input:  state — Arc<AppState> with cluster layer active
//   output: Result<(), HsError> — Err on the first drain step that fails; the
//           remaining steps are skipped (same short-circuiting `?` behaviour the
//           scattered call sites already had, since they ran sequentially with `?`).
//   sideEffects: see each individual drain_* fn's sideEffects; runs them in sequence
// drain_all_cluster:end
#[cfg(feature = "cluster")]
pub(crate) async fn drain_all_cluster(state: &Arc<AppState>) -> Result<(), HsError> {
    // cluster: drain incoming deltas for every known room before building the response.
    drain_cluster_deltas(state).await?;
    // cluster: drain any to-device gossip received from other nodes before this device's
    // queue is read — see routes/to_device.rs::drain_to_device_gossip.
    crate::routes::to_device::drain_to_device_gossip(state).await?;
    crate::routes::ephemeral::drain_cluster_ephemeral(state).await?;
    // cluster: drain any room-STATE deltas received from other nodes (createRoom /
    // membership / PUT state on a different node) — see routes/room_state.rs
    // drain_cluster_state. Must run before device_lists.changed is computed below
    // (users_sharing_room_with reads room_state).
    crate::routes::room_state::drain_cluster_state(state).await?;
    // cluster: drain any device-list-change gossip received from other nodes (a
    // keys/upload, register, or deactivate that happened on a different node) — see
    // routes/keys.rs::drain_device_list_gossip.
    crate::routes::keys::drain_device_list_gossip(state).await?;
    Ok(())
}

// drain_cluster_deltas:start
//   purpose: For every room currently in the AppState, drain any pending CRDT delta
//            blobs from its ZenohCrdtSink and apply them to the local RoomLog WITH
//            signature verification (P1.1 internal-task — apply_delta_verified, not the
//            unverified apply_delta: this is the network receive path, so every
//            incoming PDU must carry a valid signature from a known signer_node AND
//            have sender's domain equal signer_node, or it is rejected).
//            After applying, assign fresh stream positions to any new event_ids not
//            yet in room_timeline, then notify waiters.  Rejected PDUs are counted
//            and logged (observ if enabled, else eprintln) but never enter the log.
//            internal-task: also persists the pdumeta.jsonl sidecar (sig/signer_node/prev_events/
//            depth) for each newly-accepted remote PDU, so THIS node's own restart later
//            replays it as verifiable rather than falling back to an unsigned Pdu.
//   input:  state — Arc<AppState> with cluster layer active
//   output: Result<(), HsError>
//   sideEffects: may mutate rooms in state via apply_delta_verified; acquires rooms
//                Mutex; appends to room_timeline + pdumeta sidecar; notifies waiters;
//                logs rejections
// drain_cluster_deltas:end
#[cfg(feature = "cluster")]
pub(crate) async fn drain_cluster_deltas(state: &Arc<AppState>) -> Result<(), HsError> {
    use crate::substrate::crdt::CrdtSink as _;
    use crate::substrate::matrix_events::delta_from_bytes;

    let cluster = match &state.cluster {
        Some(c) => c,
        None => return Ok(()),
    };

    // Drain both locally-known rooms and rooms learned via the wildcard discovery
    // subscriber — the latter have a sink but no local `rooms` entry yet, so draining
    // only `state.rooms` would leave a discovered room permanently unapplied.
    let mut room_id_set: std::collections::HashSet<String> = {
        let guard = state
            .rooms
            .lock()
            .map_err(|e| HsError::Internal(e.to_string()))?;
        guard.keys().cloned().collect()
    };
    room_id_set.extend(cluster.list_room_ids());
    let room_ids: Vec<String> = room_id_set.into_iter().collect();

    let crdt_key = crate::state::ClusterState::crdt_key();
    let mut any_new = false;

    for room_id in &room_ids {
        let sink = cluster.sink_for(room_id).await.map_err(HsError::Internal)?;

        let blobs = sink
            .drain(crdt_key)
            .map_err(|e| HsError::Internal(e.to_string()))?;

        if blobs.is_empty() {
            continue;
        }

        // Collect known event_ids in room_timeline.
        let known_ids: std::collections::HashSet<String> = {
            let rt = state
                .room_timeline
                .lock()
                .map_err(|e| HsError::Internal(e.to_string()))?;
            rt.get(room_id.as_str())
                .map(|v| {
                    v.iter()
                        .filter_map(|(_, ev)| {
                            ev.get("event_id")
                                .and_then(|id| id.as_str())
                                .map(|s| s.to_string())
                        })
                        .collect()
                })
                .unwrap_or_default()
        };

        // Apply deltas to local RoomLog — verified (P1.1 internal-task): every incoming PDU must
        // carry a valid signature from a known signer_node with sender domain ==
        // signer_node, or it is rejected and never enters the log.
        let new_pdus: Vec<crate::routes::sync::ClusterPdu>;
        {
            let mut rooms = state
                .rooms
                .lock()
                .map_err(|e| HsError::Internal(e.to_string()))?;
            let log = rooms.entry(room_id.clone()).or_default();

            let before: std::collections::HashSet<String> =
                log.ordered().iter().map(|p| p.event_id.clone()).collect();

            let mut total_accepted = 0usize;
            let mut total_rejected = 0usize;
            for bytes in &blobs {
                // Untrusted bytes straight off the Zenoh inbox, decoded before any
                // signature check — a blob that will not parse is counted as a
                // rejection and dropped, never allowed to abort this request.
                let Some(delta) = delta_from_bytes(bytes) else {
                    eprintln!(
                        "[matrix-hs] drain {room_id}: malformed delta ({} bytes), skipped",
                        bytes.len()
                    );
                    total_rejected += 1;
                    continue;
                };
                let (accepted, rejected) = log.apply_delta_verified(&delta, &state.key_store);
                total_accepted += accepted;
                total_rejected += rejected;
            }
            if total_rejected > 0 {
                if crate::substrate::observ::enabled() {
                    crate::substrate::observ::emit(
                        "pdu.verify_reject",
                        &[
                            ("room_id", room_id.as_str()),
                            ("accepted", &total_accepted.to_string()),
                            ("rejected", &total_rejected.to_string()),
                        ],
                    );
                } else {
                    eprintln!(
                        "[matrix-hs] drain_cluster_deltas room={room_id}: \
                         accepted={total_accepted} rejected={total_rejected} \
                         (rejected PDUs failed signature/sender-binding verification)"
                    );
                }
            }

            let after = log.ordered();
            new_pdus = after
                .iter()
                .filter(|p| !before.contains(&p.event_id) && !known_ids.contains(&p.event_id))
                .map(|p| ClusterPdu {
                    event_id: p.event_id.clone(),
                    kind: p.kind.clone(),
                    sender: p.sender.clone(),
                    room_id: p.room_id.clone(),
                    ts: p.ts,
                    content: p.content.clone(),
                    // internal-task: carried through so the persist_room_pdu_meta call below can
                    // write the sidecar for this REMOTE (verified) PDU — without this a
                    // restart of THIS node would replay it unsigned even though it
                    // arrived here with a valid signature.
                    sig: p.sig.clone(),
                    signer_node: p.signer_node.clone(),
                    prev_events: p.prev_events.clone(),
                    depth: p.depth,
                })
                .collect();
        }

        // Add new PDUs to room_timeline.
        // Redactions arriving here are collected and recorded AFTER the timeline lock
        // is dropped: read paths take room_timeline then redactions, so acquiring them
        // in the other order underneath this lock would risk a deadlock.
        let mut arrived_redactions: Vec<(String, Value)> = Vec::new();
        if !new_pdus.is_empty() {
            any_new = true;
            let mut rt = state
                .room_timeline
                .lock()
                .map_err(|e| HsError::Internal(e.to_string()))?;
            let timeline_vec = rt.entry(room_id.clone()).or_default();
            for pdu in &new_pdus {
                let pos = state.stream_pos.fetch_add(1, Ordering::SeqCst);
                let content_val: Value =
                    serde_json::from_slice(&pdu.content).unwrap_or_else(|_| json!({}));
                let mut ev = json!({
                    "event_id":         pdu.event_id,
                    "type":             pdu.kind,
                    "sender":           pdu.sender,
                    "room_id":          pdu.room_id,
                    "origin_server_ts": pdu.ts,
                    "content":          content_val
                });
                // A remote redaction names its target in content (that is all a Pdu
                // carries); this lifts it to the top level and tells us what to mask.
                if let Some(target) = AppState::redaction_target(&mut ev) {
                    arrived_redactions.push((target, ev.clone()));
                }
                // Persist remote PDU to the room journal (best-effort).
                state.persist_room_event(&pdu.room_id, &ev);
                // internal-task: persist the signed-PDU meta sidecar for this remote PDU too, so a
                // restart of THIS node replays it as verifiable, not unsigned.
                state.persist_room_pdu_meta(
                    &pdu.room_id,
                    &pdu.event_id,
                    &pdu.sig,
                    &pdu.signer_node,
                    &pdu.prev_events,
                    pdu.depth,
                    &pdu.content,
                );
                timeline_vec.push((pos, ev));
            }
            // Phase 1 GC: apply retention cap to the batch just appended.
            let cap = state.timeline_max_events;
            if cap > 0 && timeline_vec.len() > cap {
                timeline_vec.drain(0..timeline_vec.len() - cap);
            }
        }
        // Phase 1 GC: cap the RoomLog for the room we just merged into.
        state.collect_room_log(&room_id[..]);

        // Timeline lock released — safe to touch redactions now.
        for (target, redaction_ev) in arrived_redactions {
            if let Err(e) = state.mark_redacted(&target, redaction_ev) {
                eprintln!("[matrix-hs] remote redaction of {target}: {e}");
            }
        }
    }

    if any_new {
        state.notify.notify_waiters();
    }

    Ok(())
}

// ClusterPdu is a temporary struct to hold PDU data extracted while holding the rooms lock,
// before we release it to write to room_timeline.
// internal-task: sig/signer_node/prev_events/depth added so the caller can persist the pdumeta
// sidecar for verified remote PDUs (without these, a later restart of THIS node would
// replay a remote-but-verified PDU as unsigned).
#[cfg(feature = "cluster")]
struct ClusterPdu {
    event_id: String,
    kind: String,
    sender: String,
    room_id: String,
    ts: u64,
    content: Vec<u8>,
    sig: Vec<u8>,
    signer_node: String,
    prev_events: Vec<String>,
    depth: u64,
}

// pdu_to_client_event:start
//   purpose: Render a Pdu as a minimal Matrix ClientEvent JSON value.
//            content is parsed from Pdu.content bytes; if not valid JSON, wraps as
//            {"raw": "<hex>"} so the response is always valid JSON.
//   input:  pdu — reference to a Pdu from RoomLog.ordered()
//   output: serde_json::Value representing the Matrix client event
//   sideEffects: none
// pdu_to_client_event:end
#[allow(dead_code)]
fn pdu_to_client_event(pdu: &crate::substrate::matrix_events::Pdu) -> Value {
    let content: Value = serde_json::from_slice(&pdu.content)
        .unwrap_or_else(|_| json!({ "raw": hex_encode(&pdu.content) }));

    json!({
        "event_id":          pdu.event_id,
        "type":              pdu.kind,
        "sender":            pdu.sender,
        "room_id":           pdu.room_id,
        "origin_server_ts":  pdu.ts,
        "content":           content
    })
}

// hex_encode:start
//   purpose: Encode bytes as hex string (avoids extra dep just for error paths).
//   input:  bytes slice
//   output: hex-encoded String
//   sideEffects: none
// hex_encode:end
fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
