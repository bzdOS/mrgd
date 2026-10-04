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
    // fragment_variable_names_match_the_code:start
    //   purpose: Close the risk my own artifact names out loud. The reporter script warns
    //            that if the fragment's variable names and the names the code reads ever
    //            disagree, a soak run silently tests a different configuration than the
    //            one on disk — and the fragment's names were, until now, transcribed by
    //            reading the source with my eyes. A rename in state.rs, or a typo in the
    //            fragment, would leave both files looking fine and the run meaningless.
    //            This reads both and compares, so the drift fails the suite instead of
    //            failing a six-hour window.
    //
    //            Not ignored: it touches no environment variable, no process state and no
    //            network — it only compares two files that are already in the tree.
    //   input:  none — reads src/state.rs and deploy/soak-caps.env.example
    //   output: () — asserts the two variable-name sets are identical
    //   sideEffects: none (two file reads)
    // fragment_variable_names_match_the_code:end
    #[test]
    fn fragment_variable_names_match_the_code() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));

        let source = std::fs::read_to_string(root.join("src/state.rs"))
            .expect("readable src/state.rs");
        let fragment = std::fs::read_to_string(root.join("deploy/soak-caps.env.example"))
            .expect("readable deploy/soak-caps.env.example");

        // Names the code actually reads, from the two *_from_env helpers.
        let mut from_code: Vec<String> = Vec::new();
        for line in source.lines() {
            let line = line.trim();
            if let Some(rest) = line.strip_prefix("std::env::var(\"") {
                if let Some(end) = rest.find('"') {
                    let name = &rest[..end];
                    if name.ends_with("_MAX_EVENTS") {
                        from_code.push(name.to_string());
                    }
                }
            }
        }
        from_code.sort();
        from_code.dedup();

        // Names the fragment sets. Owned Strings on purpose — the obvious shorter version
        // of this borrows from a local and has to Box::leak it, which would make a test
        // about leaks the one that leaks.
        let mut from_fragment: Vec<String> = fragment
            .lines()
            .filter_map(|l| l.trim().split('=').next())
            .map(|name| name.to_string())
            .filter(|name| name.starts_with("MATRIX_HS_") && name.ends_with("_MAX_EVENTS"))
            .collect();
        from_fragment.sort();
        from_fragment.dedup();

        println!(
            "  caps variables — code reads {from_code:?}, fragment sets {from_fragment:?}"
        );

        assert!(
            !from_code.is_empty(),
            "no *_MAX_EVENTS env reads found in src/state.rs — this test is no longer \
             looking at what it thinks it is"
        );
        assert_eq!(
            from_code, from_fragment,
            "the fragment and the code disagree on the cap variable names: a soak run would \
             test a different configuration than the fragment on disk claims"
        );
    }

    // env_path_feeds_the_caps:start
    //   purpose: Prove the second half of the same risk: that those names, when present in
    //            the environment, actually reach AppState::new()'s caps. The mechanism proof
    //            (caps_flatten_roomlog_and_timeline) goes through with_server_name_and_caps
    //            precisely because env is process-global — which leaves the env path itself
    //            unproven, and the fragment is nothing but env.
    //
    //            IGNORED on purpose: setting an env variable here changes it for every test
    //            running in parallel in this process, and plenty of them build an AppState
    //            whose caps would silently become 50 instead of 0. A test that can turn a
    //            green suite red by racing its neighbours does not belong in the default run.
    //            Run it alone: cargo test --offline --release env_path_feeds -- --ignored
    //   input:  none — sets both cap variables, builds AppState, restores the environment
    //   output: () — asserts the caps arrived, and that the environment is left as found
    //   sideEffects: sets and removes two environment variables for the duration
    // env_path_feeds_the_caps:end
    #[test]
    #[ignore = "sets process-global env vars; run it alone, never as part of the suite"]
    fn env_path_feeds_the_caps() {
        const T: &str = "MATRIX_HS_TIMELINE_MAX_EVENTS";
        const R: &str = "MATRIX_HS_ROOMLOG_MAX_EVENTS";
        const WANT_T: usize = 1234;
        const WANT_R: usize = 5678;

        let prev_t = std::env::var(T).ok();
        let prev_r = std::env::var(R).ok();
        std::env::set_var(T, WANT_T.to_string());
        std::env::set_var(R, WANT_R.to_string());

        let state = AppState::new();

        // Restore before asserting, so a failure below cannot leave the environment of
        // whatever runs next in this process altered.
        match prev_t {
            Some(v) => std::env::set_var(T, v),
            None => std::env::remove_var(T),
        }
        match prev_r {
            Some(v) => std::env::set_var(R, v),
            None => std::env::remove_var(R),
        }

        println!(
            "  env path: timeline_max_events={} roomlog_max_events={} (asked for {WANT_T}/{WANT_R})",
            state.timeline_max_events, state.roomlog_max_events
        );
        assert_eq!(
            state.timeline_max_events, WANT_T,
            "{T} did not reach AppState::new() — the fragment would be read by a run that \
             silently keeps the default"
        );
        assert_eq!(
            state.roomlog_max_events, WANT_R,
            "{R} did not reach AppState::new() — the fragment would be read by a run that \
             silently keeps the default"
        );
    }

}
