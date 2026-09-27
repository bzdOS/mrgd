// START_AI_HEADER
// MODULE: matrix-hs/src/routes/send.rs
// PURPOSE: PUT /_matrix/client/v3/rooms/{roomId}/send/{eventType}/{txnId}
//          Converts the request body into a Pdu and inserts it into the room's RoomLog.
//          This is the "push" half of the push/pull proof.
//
//          Pdu construction (P1.1 internal-task — signed, not Pdu::new):
//            sender     = verified access token subject (auth::verify_token; NOT a raw
//                         header heuristic — see resolve_sender)
//            kind       = eventType path param (e.g. "m.room.message")
//            content    = raw JSON body bytes
//            prev_events = event_ids of ordered() tail (last event in room's timeline)
//            depth      = prev_depth + 1
//            ts         = monotone counter (room event count * 1000, deterministic for tests)
//            event_id   = Pdu::compute_id(...) (inside Pdu::signed)
//            sig / signer_node = ed25519 signature via state.signer (NodeSigner) —
//                         every locally-created PDU is signed before it enters the
//                         RoomLog, so remote replicas can verify it via
//                         apply_delta_verified / Pdu::verify_sig instead of trusting a
//                         self-asserted sender.
//
//          Stage 2: after inserting to RoomLog, also increments stream_pos,
//          appends to room_timeline, and notifies long-poll waiters.
//
//          Returns {"event_id": "$..."} on success.
//
//          cluster feature: after inserting the PDU into the local RoomLog,
//          immediately publish an INCREMENTAL delta (just this one PDU) to the
//          ZenohCrdtSink for this room. This makes the new event visible to all
//          peer instances within one Zenoh gossip round-trip (~200 ms loopback);
//          the receiving side verifies the signature before accepting
//          (routes/sync.rs drain_cluster_deltas). A node that is actually behind
//          (was offline) does not rely on this live pub/sub path to catch up — it
//          backfills via the separate full-history queryable in main.rs
//          (merge_catchup_delta), so this path only ever needs to carry what
//          just changed, not the room's entire history.
//
//          Push notifications: after commit, calls routes::push::dispatch_push to
//          fan out a Push Gateway API notification to joined members' registered
//          pushers (routes/pushers.rs). dispatch_push does its filtering
//          synchronously (in-memory) and spawns the actual outbound HTTP POSTs, so
//          a slow/dead gateway never blocks this handler.
// DEPENDENCIES: axum, crate::substrate::matrix_events::Pdu, AppState, routes::push
// PUBLIC_API: put_send_event
// END_AI_HEADER

use crate::{auth, error::HsError, state::AppState};
use axum::{
    extract::{Path, State},
    http::HeaderMap,
    Json,
};
use crate::substrate::matrix_events::Pdu;
use serde_json::{json, Value};
use std::sync::Arc;

// put_send_event:start
//   purpose: Accept a Matrix send-event request, build a Pdu from the path params and body,
//            insert it into the room's RoomLog via RoomLog.add(), and return the event_id.
//            Room is created lazily if it does not yet exist.
//            This is the CRDT "add" operation: idempotent, add-wins.
//            Also appends to room_timeline and notifies long-poll waiters.
//            cluster mode: publish a delta to the ZenohCrdtSink immediately after add so
//            remote peers receive the new PDU without waiting for a background tick.
//   input:  room_id, event_type, txn_id (path params);
//           Authorization header (optional, "Bearer tok_<user>");
//           JSON body (raw event content)
//   output: JSON {"event_id":"$<hash>"}
//   sideEffects: inserts Pdu into AppState.rooms[room_id]; creates room if absent;
//                appends to room_timeline; notifies waiters;
//                (cluster) publishes CRDT delta to Zenoh
// put_send_event:end
pub async fn put_send_event(
    State(state): State<Arc<AppState>>,
    Path((room_id, event_type, _txn_id)): Path<(String, String, String)>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<Json<Value>, HsError> {
    // Accept an alias in the {roomId} slot (some clients don't pre-resolve); resolve
    // to the canonical room_id BEFORE the lazy-create below, else we'd create a bogus
    // room keyed by the alias string.
    let room_id = state.resolve_room_id(&room_id);
    // Resolve sender from Authorization header — signed token required.
    let sender = resolve_sender(&headers, &state.token_secret, &state.server_name)
        .ok_or_else(|| HsError::UnknownToken("missing or invalid token".to_string()))?;

    // Ensure room exists (lazy create).
    state.ensure_room(&room_id);

    // Insert PDU into local RoomLog; return event_id and (cluster) serialised delta.
    let (event_id, _client_event) = insert_pdu(
        &state,
        &room_id,
        sender,
        event_type,
        body.to_vec(),
        json!({}),
    )
    .await?;

    Ok(Json(json!({ "event_id": event_id })))
}

// insert_pdu:start
//   purpose: Lock the rooms map, build and insert a Pdu for the given room, then
//            append to room_timeline, persist the client event + signed-PDU meta
//            sidecar (internal-task — pdumeta.jsonl, so a post-restart replay can reconstruct a
//            verifiable Pdu), notify waiters, and (cluster mode) publish an
//            incremental delta (just this PDU) to the ZenohCrdtSink.  Returns the
//            new event_id and the full client-event JSON that was written to
//            room_timeline.
//            Extracts the lock+publish logic from put_send_event to keep cfg-gating clean.
//            pub(crate): also called by routes/redact.rs::put_redact_event to send the
//            m.room.redaction event through the SAME path as any other message event
//            (RoomLog insert, timeline append, persistence, push, cluster publish) —
//            redactions replicate exactly like ordinary messages.
//   input:  state, room_id, sender, event_type, content bytes,
//           extra_fields — a JSON object merged into the top level of the client
//           event after the standard fields are built (e.g. {"redacts": "$id"} for
//           m.room.redaction); pass json!({}) for no extra fields.
//   output: Result<(String, Value), HsError> — (new event_id, client_event JSON)
//   sideEffects: mutates AppState.rooms; appends to room_timeline; persists client event +
//                pdumeta; notifies; (cluster) publishes
// insert_pdu:end
pub(crate) async fn insert_pdu(
    state: &Arc<AppState>,
    room_id: &str,
    sender: String,
    event_type: String,
    content: Vec<u8>,
    extra_fields: Value,
) -> Result<(String, Value), HsError> {
    // Scope the mutex guard to before any await points.
    let event_id;
    let client_event: Value;
    // internal-task: captured BEFORE log.add(pdu) moves the Pdu, so we can persist the signed-PDU
    // meta sidecar (pdumeta.jsonl) after the lock is released — needed so a post-restart
    // replay can reconstruct a VERIFIABLE Pdu instead of an unsigned synthetic one.
    let pdu_sig: Vec<u8>;
    let pdu_signer_node: String;
    let pdu_prev_events: Vec<String>;
    let pdu_depth: u64;
    #[cfg(feature = "cluster")]
    let delta_bytes: Vec<u8>;

    {
        let mut rooms = state
            .rooms
            .lock()
            .map_err(|e| HsError::Internal(e.to_string()))?;
        let log = rooms
            .get_mut(room_id)
            .ok_or_else(|| HsError::RoomNotFound(room_id.to_string()))?;

        // Determine prev_events + depth from the current tail of ordered().
        let ordered = log.ordered();
        let (prev_events, depth) = if ordered.is_empty() {
            (vec![], 0u64)
        } else {
            let tail = ordered.last().expect("non-empty ordered");
            (vec![tail.event_id.clone()], tail.depth + 1)
        };

        // Real wall-clock ts so clients (Element) date and order the timeline
        // correctly.  event_id stays distinct via depth + prev_events (see
        // Pdu::compute_id), so this does not collide even for identical bodies.
        let ts = crate::state::now_ms();

        // Sign every locally-created PDU (P1.1 internal-task) — closes the self-asserted-sender
        // forgery gap.  sender's domain (part after the first ':') MUST equal
        // state.signer.node_id (== state.server_name) for Pdu::verify_sig's
        // sender-binding check to accept it; resolve_sender() always returns a MXID
        // homed on this server (see routes/send.rs resolve_sender / auth::verify_token),
        // so this invariant holds for every request that reaches this point.
        let pdu = Pdu::signed(
            room_id.to_string(),
            sender.clone(),
            event_type.clone(),
            content.clone(),
            prev_events,
            depth,
            ts,
            &state.signer,
        );
        event_id = pdu.event_id.clone();

        // Build the client event JSON before adding, while we still have the data.
        let content_val: Value = serde_json::from_slice(&content)
            .unwrap_or_else(|_| json!({"raw": format!("{:02x?}", &content)}));
        let mut client_event_val = json!({
            "event_id":         event_id,
            "type":             event_type,
            "sender":           sender,
            "room_id":          room_id,
            "origin_server_ts": ts,
            "content":          content_val
        });
        // Merge caller-supplied extra top-level fields (e.g. m.room.redaction's
        // "redacts") — no-op when extra_fields is an empty object.
        if let (Value::Object(extra_map), Some(obj)) =
            (extra_fields, client_event_val.as_object_mut())
        {
            for (k, v) in extra_map {
                obj.insert(k, v);
            }
        }
        client_event = client_event_val;

        // internal-task: capture the signed-PDU meta fields before the move into log.add(pdu).
        pdu_sig = pdu.sig.clone();
        pdu_signer_node = pdu.signer_node.clone();
        pdu_prev_events = pdu.prev_events.clone();
        pdu_depth = pdu.depth;

        // Publish only the PDU just added, not the whole room log. log.delta() would
        // serialise EVERY PDU the room has ever had, so publishing it on every send
        // costs O(total room history) bytes/CPU — O(n^2) over a room's lifetime.
        // Peers merge a single-PDU delta exactly like any other one via
        // apply_delta_verified, which has no notion of "this delta is complete"; a
        // node that's genuinely behind backfills via the full-history queryable
        // catch-up (main.rs merge_catchup_delta), not this live pub/sub path.
        #[cfg(feature = "cluster")]
        {
            delta_bytes = crate::substrate::matrix_events::delta_to_bytes(&crate::substrate::matrix_events::RoomLogDelta {
                pdus: vec![pdu.clone()],
                // Carry this room's GC watermark on every send, so peers adopt it
                // from ordinary traffic instead of waiting for a catch-up pass.
                collected_depth: log.collected_depth(),
            });
        }

        log.add(pdu);
    } // mutex released here

    // Phase 1 GC: apply the RoomLog cap now the rooms mutex is free. No-op unless
    // MATRIX_HS_ROOMLOG_MAX_EVENTS is set.
    state.collect_room_log(room_id);

    // Append to room_timeline and notify.
    state.append_room_timeline(room_id, client_event.clone());
    // Persist: append the client event to the room journal (best-effort).
    state.persist_room_event(room_id, &client_event);
    // internal-task: persist the signed-PDU meta sidecar so a post-restart replay can reconstruct
    // a VERIFIABLE Pdu (a fresh peer's apply_delta_verified would otherwise reject the
    // unsigned synthetic Pdu that replay produces without this).
    state.persist_room_pdu_meta(
        room_id,
        &event_id,
        &pdu_sig,
        &pdu_signer_node,
        &pdu_prev_events,
        pdu_depth,
        &content,
    );
    state.notify.notify_waiters();

    // Push notifications: fan out to joined members' registered pushers. This is a
    // synchronous, in-memory scan (no I/O) that spawns background tasks for the
    // actual gateway POSTs — see routes/push.rs::dispatch_push for the
    // non-blocking design and the minimal push-rule scope note.
    crate::routes::push::dispatch_push(
        state,
        room_id,
        &event_id,
        &event_type,
        &sender,
        client_event.get("content").unwrap_or(&Value::Null),
    );

    // cluster: publish the delta outside the lock so Zenoh's async runtime can run freely.
    #[cfg(feature = "cluster")]
    publish_delta(state, room_id, delta_bytes).await?;

    Ok((event_id, client_event))
}

// publish_delta:start
//   purpose: Publish a serialised RoomLog delta to the ZenohCrdtSink for `room_id`.
//            Called by insert_pdu after the mutex is released so Zenoh's async I/O
//            does not contend with the rooms lock.
//   input:  state, room_id, bytes — serialised delta from delta_to_bytes
//   output: Result<(), HsError>
//   sideEffects: opens Zenoh sink lazily (first call per room); publishes to Zenoh network
// publish_delta:end
#[cfg(feature = "cluster")]
async fn publish_delta(
    state: &Arc<AppState>,
    room_id: &str,
    bytes: Vec<u8>,
) -> Result<(), HsError> {
    use crate::substrate::crdt::CrdtSink as _;

    let cluster = match &state.cluster {
        Some(c) => c,
        None => return Ok(()), // single-node mode even with feature compiled in
    };

    let sink = cluster.sink_for(room_id).await.map_err(HsError::Internal)?;

    let key = crate::state::ClusterState::crdt_key();
    // publish() uses block_in_place — safe in multi-thread tokio runtime.
    sink.publish(key, bytes)
        .map_err(|e| HsError::Internal(e.to_string()))?;

    Ok(())
}

// resolve_sender:start
//   purpose: Extract the sender user_id from the Authorization header.
//            ONLY signed "mxt_..." tokens (auth::verify_token) authenticate — no legacy
//            "tok_<username>" acceptance, no anonymous "@anon:" fallback.
//            Returns None when the header is absent, malformed, or the MAC is invalid;
//            the caller converts None to 401 M_UNKNOWN_TOKEN.
//   input:  HeaderMap from the request; secret — HMAC key; _server_name (unused; symmetry)
//   output: Option<String> — verified sender user_id, or None
//   sideEffects: none
// resolve_sender:end
fn resolve_sender(headers: &HeaderMap, secret: &[u8], _server_name: &str) -> Option<String> {
    let token = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))?;
    // Only signed mxt_ tokens authenticate.
    // Note: epoch is NOT checked here — this is a low-level token verify.
    // Full auth (with revocation gate) is via auth::extract_caller.
    auth::verify_token(secret, token).map(|(uid, _, _)| uid)
}
