// START_AI_HEADER
// MODULE: matrix-hs/src/soak_caps_test.rs
// PURPOSE: Cheap proof that the retention caps actually flatten the two structures that
//          the leak survey (fb517f3) measured as linear-in-time, BEFORE anyone spends six
//          hours of a stand's window finding out. The survey ran with the default caps,
//          which are 0, and 0 means unlimited — so its numbers (roomlog 1 → 201,
//          timeline 6 → 406 over 200 sends) describe a run with no bound at all.
//
//          Caps are supplied through `AppState::with_server_name_and_caps`, NOT through
//          environment variables, and that is the whole design of this test: env is
//          process-global, so a test that sets one to pin a cap silently changes it for
//          every other test running in parallel beside it. This file therefore proves the
//          MECHANISM (a positive cap trims oldest-first on the write path), while the
//          proposed values for a real deployment live in deploy/soak-caps.env, where they
//          belong — as a fragment nobody applies by accident.
//
//          Caps used here are deliberately tiny (50/50) so the plateau is visible in a
//          fraction of a second and cannot be mistaken for "it nearly flattened".
// DEPENDENCIES: axum-test, serde_json, matrix_hs::{router, AppState}
// END_AI_HEADER

#[cfg(test)]
mod tests {
    use crate::{router, AppState};
    use axum::http::{HeaderName, HeaderValue};
    use axum_test::TestServer;
    use serde_json::{json, Value};

    const CAP_TIMELINE: usize = 50;
    const CAP_ROOMLOG: usize = 50;
    const N_SENDS: usize = 300;

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

    // caps_flatten_roomlog_and_timeline:start
    //   purpose: The cheap proof. With positive caps, drive N_SENDS (6× the cap) through
    //            the real send route and show both structures plateau AT the cap instead of
    //            climbing. Also prints the same two numbers with the caps removed, because
    //            the comparison is the point: a cap that does nothing looks identical to a
    //            cap that works if you only ever look at one of the two runs.
    //   input:  none — caps 50/50 and a control run with 0/0, 300 sends each
    //   output: () — prints a checkpoint table and asserts the capped run is bounded
    //   sideEffects: in-memory AppState only; no env vars touched, no files, no network
    // caps_flatten_roomlog_and_timeline:end
    #[tokio::test]
    #[ignore = "survey, not a gate: run with --ignored --nocapture"]
    async fn caps_flatten_roomlog_and_timeline() {
        println!(
            "\n  === soak caps proof: caps timeline={CAP_TIMELINE} roomlog={CAP_ROOMLOG}, \
             N_SENDS={N_SENDS}; control run with caps 0/0 (the default) ==="
        );

        async fn run(timeline_cap: usize, roomlog_cap: usize, label: &str) -> (usize, usize) {
            let state = AppState::with_server_name_and_caps(
                "localhost".to_string(),
                timeline_cap,
                roomlog_cap,
            );
            let app = router(state.clone());
            let server = TestServer::new(app);
            let alice = register_and_bearer(&server, &format!("alice_{label}")).await;

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

            let sizes = |tag: &str| {
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
                println!(
                    "  {label:<8} caps t/r={timeline_cap}/{roomlog_cap}  {tag:<12} \
                     roomlog={roomlog:<5} timeline={timeline}"
                );
                (roomlog, timeline)
            };

            sizes("after-create");
            for i in 0..N_SENDS {
                server
                    .put(&format!(
                        "/_matrix/client/v3/rooms/{room}/send/m.room.message/txn-c{i}"
                    ))
                    .add_header(alice.0.clone(), alice.1.clone())
                    .bytes(axum::body::Bytes::from_static(b"caps proof"))
                    .await;
                if i == 9 || i == 49 {
                    sizes(&format!("after-{}", i + 1));
                }
            }
            sizes("final")
        }

        let (capped_log, capped_timeline) = run(CAP_TIMELINE, CAP_ROOMLOG, "capped").await;
        let (free_log, free_timeline) = run(0, 0, "uncapped").await;

        println!(
            "  RESULT capped: roomlog={capped_log} timeline={capped_timeline} (caps {CAP_ROOMLOG}/{CAP_TIMELINE})"
        );
        println!(
            "  RESULT default: roomlog={free_log} timeline={free_timeline} (caps 0/0 = unlimited)"
        );

        assert!(
            capped_log <= CAP_ROOMLOG,
            "the room log grew past its cap: {capped_log} > {CAP_ROOMLOG}"
        );
        assert!(
            capped_timeline <= CAP_TIMELINE,
            "the timeline grew past its cap: {capped_timeline} > {CAP_TIMELINE}"
        );
        // The control is the contrast: without caps the same 300 sends keep everything.
        assert!(
            free_log > CAP_ROOMLOG && free_timeline > CAP_TIMELINE,
            "the uncapped control did NOT outgrow the caps ({free_log}/{free_timeline}) — \
             then this test proves nothing about the caps"
        );
        println!(
            "  VERDICT: a positive cap holds both structures at the cap across {N_SENDS} \
             sends, while the default (0) keeps all of them — the difference the 6-hour run \
             would have had to discover the slow way."
        );
    }
}
