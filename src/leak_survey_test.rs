// START_AI_HEADER
// MODULE: matrix-hs/src/leak_survey_test.rs
// PURPOSE: Measurement harness for the soak finding of ~15 MB/h growth. A soak number says
//          SOMETHING grows; it does not say what. This test names the candidates and
//          measures each one directly: it performs N real client operations through the
//          HTTP routes and prints the size of every growing structure in AppState before
//          and after. What survives N iterations with no bound is the suspect; what stays
//          flat was already capped and needs no attention.
//
//          Scope note, stated so the numbers are not read as more than they are: this
//          measures GROWTH PER OPERATION on the test tree, in-memory, single node. It does
//          not attribute the soak's MB/h to any of these — heap bytes per structure were
//          not taken, and a 15 MB/h RSS figure includes allocator behaviour this test
//          cannot see. What it does give is the multiplier: X bytes-of-entries per Y
//          operations, which is what decides whether a leak is minutes or months old.
//
//          Ignored by default on purpose: it is a survey, not a gate, and it must not slow
//          the suite. Run it with --ignored --nocapture.
// DEPENDENCIES: axum-test, serde_json, matrix_hs::{router, AppState}
// END_AI_HEADER

#[cfg(test)]
mod tests {
    use crate::{router, AppState};
    use axum::http::{HeaderName, HeaderValue};
    use axum_test::TestServer;
    use serde_json::{json, Value};

    const N: usize = 200;

    async fn register_and_bearer(server: &TestServer, username: &str) -> (HeaderName, HeaderValue) {
        let ch: Value = server
            .post("/_matrix/client/v3/register")
            .json(&json!({ "username": username, "password": "pw" }))
            .await
            .json();
        let sess = ch["session"]
            .as_str()
            .unwrap_or_else(|| panic!("no session for {username}; got {ch}"))
            .to_string();
        let reg: Value = server
            .post("/_matrix/client/v3/register")
            .json(&json!({
                "username": username,
                "password": "pw",
                "auth": { "type": "m.login.dummy", "session": sess }
            }))
            .await
            .json();
        let token = reg["access_token"]
            .as_str()
            .unwrap_or_else(|| panic!("no token for {username}; got {reg}"))
            .to_string();
        (
            HeaderName::from_static("authorization"),
            HeaderValue::from_str(&format!("Bearer {token}")).expect("header value"),
        )
    }

    // leak_survey:start
    //   purpose: Run N iterations of each growth-prone client operation and print the size
    //            of every candidate structure before and after. The verdict is read off the
    //            deltas: flat = already bounded, linear = unbounded, sub-linear = bounded
    //            by a cap that is merely large.
    //   input:  none — N = 200 per candidate
    //   output: a table on stdout; no assertion, because a survey that fails the build is
    //            a gate, and this is not one
    //   sideEffects: in-memory AppState only
    // leak_survey:end
    #[tokio::test]
    #[ignore = "survey, not a gate: run with --ignored --nocapture"]
    async fn leak_survey_growth_per_operation() {
        let state = AppState::new();
        let app = router(state.clone());
        let server = TestServer::new(app);
        let alice = register_and_bearer(&server, "alice_leak").await;

        let created: Value = server
            .post("/_matrix/client/v3/createRoom")
            .add_header(alice.0.clone(), alice.1.clone())
            .json(&json!({}))
            .await
            .json();
        let room = created["room_id"]
            .as_str()
            .expect("room_id from createRoom")
            .to_string();

        // One event to redact, so the redaction path has a real target.
        let sent: Value = server
            .put(&format!("/_matrix/client/v3/rooms/{room}/send/m.room.message/txn-0"))
            .add_header(alice.0.clone(), alice.1.clone())
            .bytes(axum::body::Bytes::from_static(b"redact me"))
            .await
            .json();
        let victim_event = sent["event_id"]
            .as_str()
            .expect("event_id from send")
            .to_string();

        let sizes = |tag: &str| {
            let to_device_seen = state.to_device.to_device_seen.lock().expect("seen").len();
            let to_device_queue: usize = state
                .to_device
                .to_device_queue
                .lock()
                .expect("queue")
                .iter()
                .map(|(_, v)| v.len())
                .sum();
            let redactions = state.redactions.lock().expect("redactions").len();
            let room_state = state
                .room_state
                .lock()
                .expect("room_state")
                .get(&room)
                .map(|v| v.len())
                .unwrap_or(0);
            let roomlog = state
                .rooms
                .lock()
                .expect("rooms")
                .get(&room)
                .map(|l| l.ordered().len())
                .unwrap_or(0);
            let timeline = state
                .room_timeline
                .lock()
                .expect("timeline")
                .get(&room)
                .map(|v| v.len())
                .unwrap_or(0);
            let typing_rooms = state.ephemeral.typing.lock().expect("typing").len();
            let typing_users: usize = state
                .ephemeral
                .typing
                .lock()
                .expect("typing")
                .values()
                .map(|m| m.len())
                .sum();
            let delivered = state.to_device.delivered.lock().expect("delivered").len();
            let users = state.users.lock().expect("users").len();
            let uia = state.uia_sessions.lock().expect("uia").len();
            println!(
                "  {tag:<10} to_device_seen={to_device_seen:<6} to_device_queue={to_device_queue:<6} \
                 redactions={redactions:<5} room_state={room_state:<5} roomlog={roomlog:<5} \
                 timeline={timeline:<5} typing_rooms={typing_rooms:<3} typing_users={typing_users:<4} \
                 delivered={delivered:<4} users={users:<3} uia_sessions={uia}"
            );
        };

        println!(
            "\n  === leak survey: N = {N} per candidate; caps: roomlog_max_events={} timeline_max_events={} ===",
            state.roomlog_max_events, state.timeline_max_events
        );
        sizes("before");

        // (1) sendToDevice — one enqueue per iteration, distinct msg_id each time, which
        // is what a real client fleet does and what the dedup set is supposed to hold.
        for i in 0..N {
            server
                .put(&format!("/_matrix/client/v3/sendToDevice/m.leak.probe/txn-t{i}"))
                .add_header(alice.0.clone(), alice.1.clone())
                .json(&json!({
                    "messages": { "@bob:localhost": { "leakprobe": { "msg_id": format!("m{i}") } } }
                }))
                .await;
        }
        sizes("toDevice");

        // (2) state events — a DISTINCT state_key per iteration. Re-writing one key is
        // replaced in place (the handler retains same type+key first), so it cannot show a
        // leak; distinct keys are what a room with many members or many custom keys does.
        for i in 0..N {
            server
                .put(&format!(
                    "/_matrix/client/v3/rooms/{room}/state/m.leak.probe/key{i}"
                ))
                .add_header(alice.0.clone(), alice.1.clone())
                .json(&json!({ "n": i }))
                .await;
        }
        sizes("state");

        // (3) timeline — the classic bounded-by-cap candidate: roomlog_max_events and
        // timeline_max_events should hold it flat.
        for i in 0..N {
            server
                .put(&format!(
                    "/_matrix/client/v3/rooms/{room}/send/m.room.message/txn-s{i}"
                ))
                .add_header(alice.0.clone(), alice.1.clone())
                .bytes(axum::body::Bytes::from_static(b"filler"))
                .await;
        }
        sizes("send");

        // (4) typing — ephemeral, and the interesting part is the OUTER map: the inner
        // user entries expire by timestamp, but does the room key itself ever go away?
        for i in 0..N {
            server
                .put(&format!(
                    "/_matrix/client/v3/rooms/{room}/typing/@alice:localhost?timeout=1"
                ))
                .add_header(alice.0.clone(), alice.1.clone())
                .json(&json!({ "typing": true }))
                .await;
        }
        sizes("typing");

        // (5) redactions — one per iteration against the same event id.
        for i in 0..N {
            server
                .put(&format!(
                    "/_matrix/client/v3/rooms/{room}/redact/{victim_event}/txn-r{i}"
                ))
                .add_header(alice.0.clone(), alice.1.clone())
                .json(&json!({ "reason": "leak survey" }))
                .await;
        }
        sizes("redact");

        // (6) UIA sessions — register starts one per call and abandons it, which is what
        // a client that gives up looks like from the server's side.
        for _ in 0..N {
            server
                .post("/_matrix/client/v3/register")
                .json(&json!({ "username": "leakprobe_uia", "password": "pw" }))
                .await;
        }
        sizes("uia");

        println!("  === end survey ===\n");
    }
}
