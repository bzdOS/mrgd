// START_AI_HEADER
// MODULE: matrix-hs/src/routes/push.rs
// PURPOSE: Push Gateway API CLIENT — the outbound side of push notifications.
//          Called from routes/send.rs immediately after a PDU is appended to a
//          room's RoomLog. For every joined member of the room (other than the
//          sender) who has at least one registered pusher (routes/pushers.rs,
//          AppState.push.pushers), POSTs a notification to that pusher's data.url per
//          the Matrix Push Gateway API:
//            POST <data.url>
//            { "notification": { "event_id", "room_id", "type", "sender", "content",
//                                 "counts": {"unread": N},
//                                 "devices": [{"app_id","pushkey","pushkey_ts","data"}] } }
//
//          PUSH-RULE SCOPE (deliberately minimal — see task spec):
//            EVALUATED:     event_type == "m.room.message", and the joined member is
//                            not the sender of the event (nobody is notified of their
//                            own message).
//            OUT OF SCOPE:  keyword/content rules, .m.rule.* overrides, per-room
//                            mutes/highlights, tweaks (sound/highlight), event_match
//                            conditions — anything from the full push-rules engine
//                            (see routes/account.rs::get_pushrules, which still
//                            returns the empty default rule set). A real client-
//                            facing push-rules engine is a separate, larger feature;
//                            this dispatch only implements the "notify on message"
//                            baseline needed to drive a Push Gateway end to end.
//
//          NON-BLOCKING: dispatch_push() does a small amount of synchronous,
//          in-memory work (room_state / pushers Mutex scans — no I/O) and then
//          tokio::spawn's one task PER (member, pusher) that performs the actual
//          HTTP POST. It returns immediately without awaiting any network I/O, so
//          a slow or dead gateway cannot stall the /send request path. Each spawned
//          task independently times out (5s) and swallows its own error — a
//          gateway failure is logged to stderr and never propagates (no panics,
//          no unwraps).
//
//          The external gateway itself (e.g. sygnal, translating this call into an
//          APNs/FCM push) is explicitly NOT this module's concern — data.url is
//          whatever the client registered, and this module is only responsible for
//          making the documented Push Gateway API call to it.
// DEPENDENCIES: axum (via caller), reqwest, serde_json, AppState, tokio::spawn
// PUBLIC_API: dispatch_push
// END_AI_HEADER

use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;

use crate::state::AppState;

// dispatch_push:start
//   purpose: Fan out a "message sent" notification to every joined room member's
//            registered pushers, except the sender. Called by
//            routes/send.rs::insert_pdu right after the PDU is committed to the
//            room's RoomLog and room_timeline.
//            Minimal push-rule evaluation: only event_type == "m.room.message"
//            triggers a notification (see module header SCOPE note); the sender is
//            always excluded.
//   input:  state — Arc<AppState>; room_id — canonical room_id;
//           event_id, event_type, sender — PDU fields;
//           content — the raw event content (echoed into the notification body)
//   output: () — always returns immediately; all network I/O happens in spawned
//           tasks whose results are never awaited by the caller
//   sideEffects: tokio::spawn's zero or more background HTTP POST tasks; no
//                mutation of AppState (read-only scan of room_state + pushers)
// dispatch_push:end
pub fn dispatch_push(
    state: &Arc<AppState>,
    room_id: &str,
    event_id: &str,
    event_type: &str,
    sender: &str,
    content: &Value,
) {
    // Minimal push-rule scope: only m.room.message triggers a notification.
    if event_type != "m.room.message" {
        return;
    }

    let members = state.joined_members(room_id);
    for member in members {
        // Never notify the sender of their own message.
        if member == sender {
            continue;
        }

        let pushers = state.pushers_for_user(&member);
        if pushers.is_empty() {
            continue;
        }

        for pusher in pushers {
            let url = match pusher.data.get("url").and_then(|v| v.as_str()) {
                Some(u) => u.to_string(),
                None => continue, // malformed pusher data — nothing to POST to
            };

            let notification = json!({
                "notification": {
                    "event_id": event_id,
                    "room_id":  room_id,
                    "type":     event_type,
                    "sender":   sender,
                    "content":  content,
                    // Unread-count tracking per recipient is out of scope for this
                    // minimal dispatch (would require per-user read-state accounting
                    // beyond the existing fully_read/receipts EDUs); a fixed 1
                    // signals "there is at least one unread notification" without
                    // over-claiming precision.
                    "counts":   { "unread": 1 },
                    "devices": [{
                        "app_id":     pusher.app_id,
                        "pushkey":    pusher.pushkey,
                        "pushkey_ts": pusher.pushkey_ts,
                        "data":       pusher.data,
                    }],
                }
            });

            // Spawn — the HTTP POST (including DNS/connect/TLS) never blocks the
            // /send request path. Errors are logged and swallowed; a dead gateway
            // must never panic or back-pressure message sending.
            tokio::spawn(async move {
                // no_proxy(): a gateway URL is dialled directly. This also sidesteps a
                // host-specific gotcha (documented in this deployment's operating
                // notes) where an ambient http_proxy/https_proxy env cannot route to
                // loopback/LAN targets — relevant for locally co-located gateways and
                // for tests that point data.url at a mock server on 127.0.0.1.
                let client = match reqwest::Client::builder()
                    .timeout(Duration::from_secs(5))
                    .no_proxy()
                    .build()
                {
                    Ok(c) => c,
                    Err(e) => {
                        eprintln!("[matrix-hs] push dispatch: reqwest client build failed: {e}");
                        return;
                    }
                };

                match client.post(&url).json(&notification).send().await {
                    Ok(resp) if !resp.status().is_success() => {
                        eprintln!(
                            "[matrix-hs] push dispatch: gateway {url:?} returned {}",
                            resp.status()
                        );
                    }
                    Err(e) => {
                        eprintln!("[matrix-hs] push dispatch: POST to {url:?} failed: {e}");
                    }
                    Ok(_) => {}
                }
            });
        }
    }
}
