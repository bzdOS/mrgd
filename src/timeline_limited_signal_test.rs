// START_AI_HEADER
// MODULE: matrix-hs/src/timeline_limited_signal_test.rs
// PURPOSE: Regression test for the /sync limited + prev_batch contract when the
//          room timeline has been trimmed by MATRIX_HS_TIMELINE_MAX_EVENTS.
//
//          What the contract says (.env.example, TIMELINE_MAX_EVENTS block):
//          "classic /sync signals `limited:true` + `prev_batch` so clients can
//          backfill the dropped tail via /rooms/{id}/messages".
//
//          What a live window measured (02.10, window #7, room at 20016 events
//          with cap 20000): /sync returned 20000 events with `limited:false` and
//          `prev_batch:""`, and the 16 dropped oldest events were unreachable
//          through BOTH /sync and /rooms/{id}/messages -- the client is never
//          told and has no token to backfill from.
//
//          The test therefore pins the contract, not the implementation:
//            1. a room of 5 events under cap 3 → /sync returns exactly 3 events
//               (the newest), `limited:true`, and a NON-EMPTY prev_batch token;
//            2. GET /rooms/{id}/messages from that token serves the 2 dropped
//               oldest events, so a client can actually reach them.
//
//          RED on current main: classic /sync hardcodes "limited": false and
//          "prev_batch": "" (src/routes/sync.rs), and /messages paginates over
//          the already-trimmed projection, so the dropped head is unreachable.
// DEPENDENCIES: axum-test, serde_json, matrix_hs::{router, AppState}
// END_AI_HEADER

#[cfg(test)]
mod tests {
    use crate::{router, AppState};
    use axum::http::{HeaderName, HeaderValue};
    use axum_test::TestServer;
    use serde_json::{json, Value};

    /// Cap 3, 5 events sent → 2 events dropped from the head of the projection.
    const CAP: usize = 3;
    const N_MESSAGES: usize = 5;

    fn capped_server(cap: usize) -> TestServer {
        let mut state = AppState::new();
        std::sync::Arc::get_mut(&mut state)
            .expect("state must be uniquely owned before it is wrapped")
            .timeline_max_events = cap;
        TestServer::new(router(state))
    }

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

    async fn create_room(
        server: &TestServer,
        auth: &(HeaderName, HeaderValue),
        alias: &str,
    ) -> String {
        let resp = server
            .post("/_matrix/client/v3/createRoom")
            .add_header(auth.0.clone(), auth.1.clone())
            .json(&json!({ "room_alias_name": alias }))
            .await;
        resp.assert_status_ok();
        let body: Value = resp.json();
        body["room_id"].as_str().expect("room_id").to_string()
    }

    async fn send_message(
        server: &TestServer,
        auth: &(HeaderName, HeaderValue),
        room_id: &str,
        txn_id: &str,
        body: &str,
    ) {
        let resp = server
            .put(&format!(
                "/_matrix/client/v3/rooms/{room_id}/send/m.room.message/{txn_id}"
            ))
            .add_header(auth.0.clone(), auth.1.clone())
            .json(&json!({ "msgtype": "m.text", "body": body }))
            .await;
        resp.assert_status_ok();
    }

    #[tokio::test]
    async fn trimmed_timeline_signals_limited_and_prev_batch_backfills_dropped_head() {
        let server = capped_server(CAP);
        let auth = register_and_bearer(&server, "erin").await;
        let room_id = create_room(&server, &auth, "tlcap").await;

        for i in 0..N_MESSAGES {
            send_message(
                &server,
                &auth,
                &room_id,
                &format!("txn{i}"),
                &format!("msg {i}"),
            )
            .await;
        }

        // ── 1. /sync must bound the timeline and say so ──────────────────────
        let sync: Value = server
            .get("/_matrix/client/v3/sync")
            .add_header(auth.0.clone(), auth.1.clone())
            .await
            .json();
        let tl = &sync["rooms"]["join"][&room_id]["timeline"];
        let events = tl["events"].as_array().expect("timeline events array");

        assert_eq!(
            events.len(),
            CAP,
            "with cap {CAP} and {N_MESSAGES} events, /sync must return the newest {CAP}; got {} in {tl}",
            events.len()
        );
        assert_eq!(
            tl["limited"],
            json!(true),
            "a TRIMMED timeline must signal limited:true -- .env.example promises it, and the client \
             cannot backfill a hole it was never told about; got {tl}"
        );

        let prev_batch = tl["prev_batch"].as_str().unwrap_or("");
        assert!(
            !prev_batch.is_empty(),
            "limited:true must come with a usable prev_batch token; got empty string in {tl}"
        );

        // ── 2. /messages from that token must reach the dropped head ─────────
        let resp: Value = server
            .get(&format!("/_matrix/client/v3/rooms/{room_id}/messages"))
            .add_header(auth.0.clone(), auth.1.clone())
            .add_query_param("from", prev_batch)
            .add_query_param("dir", "b")
            .add_query_param("limit", "10")
            .await
            .json();

        let bodies: Vec<String> = resp["chunk"]
            .as_array()
            .unwrap_or_else(|| panic!("chunk must be an array; got {resp}"))
            .iter()
            .map(|ev| ev["content"]["body"].as_str().unwrap_or("").to_string())
            .collect();

        for dropped in ["msg 0", "msg 1"] {
            assert!(
                bodies.iter().any(|b| b == dropped),
                "prev_batch '{prev_batch}' must let a client backfill the dropped event '{dropped}'; \
                 /messages returned {bodies:?}"
            );
        }
    }
}