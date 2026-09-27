// START_AI_HEADER
// MODULE: matrix-hs/src/messages_test.rs
// PURPOSE: Regression test for GET /rooms/{roomId}/messages pagination.
//          Reproduces a live bug: the endpoint ignored from/dir/limit entirely
//          and always returned a non-empty "end" token, so a real client's
//          backward-pagination loop (e.g. FluffyChat's "Load more" on opening
//          a room) never terminated — it kept re-requesting the same page
//          forever. Confirmed live: FluffyChat hammered
//          GET /messages?from=t1&dir=b&limit=100 in an infinite loop against a
//          real matrix-hs instance.
//
//          Tests prove:
//            1. Paginating backward from the end with a small limit,
//               following each returned "end" token, terminates within a
//               bounded number of steps (does not loop forever) and covers
//               every message exactly once.
//            2. Once the start of history is reached, "end" is omitted.
//            3. A single-message room paginated with from=t1&dir=b (the exact
//               shape observed in the live repro) returns that one message
//               and omits "end" (the client-visible stop signal).
// DEPENDENCIES: axum-test, serde_json, matrix_hs::{router, AppState}
// END_AI_HEADER

#[cfg(test)]
mod tests {
    use crate::{router, AppState};
    use axum::http::{HeaderName, HeaderValue};
    use axum_test::TestServer;
    use serde_json::{json, Value};

    fn test_server() -> TestServer {
        let state = AppState::new();
        let app = router(state);
        TestServer::new(app)
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
    async fn backward_pagination_terminates_and_omits_end_at_start() {
        let server = test_server();
        let auth = register_and_bearer(&server, "dave").await;
        let room_id = create_room(&server, &auth, "msgpage").await;

        for i in 0..5 {
            send_message(
                &server,
                &auth,
                &room_id,
                &format!("txn{i}"),
                &format!("msg {i}"),
            )
            .await;
        }

        // Paginate backward with a limit smaller than the total message count,
        // following the server's own "end" token each time — exactly what a
        // real client's backfill loop does.
        let mut from: Option<String> = None;
        let mut seen_bodies: Vec<String> = Vec::new();
        let mut steps = 0;

        loop {
            steps += 1;
            assert!(
                steps <= 20,
                "pagination did not terminate within 20 steps — infinite loop bug reproduced"
            );

            let mut req = server
                .get(&format!("/_matrix/client/v3/rooms/{room_id}/messages"))
                .add_header(auth.0.clone(), auth.1.clone())
                .add_query_param("dir", "b")
                .add_query_param("limit", "2");
            if let Some(f) = &from {
                req = req.add_query_param("from", f);
            }
            let resp: Value = req.await.json();

            for ev in resp["chunk"].as_array().expect("chunk array") {
                seen_bodies.push(ev["content"]["body"].as_str().unwrap_or("").to_string());
            }

            match resp.get("end").and_then(|v| v.as_str()) {
                Some(next) => from = Some(next.to_string()),
                None => break, // reached the start of history — must happen eventually
            }
        }

        seen_bodies.sort();
        let mut expected: Vec<String> = (0..5).map(|i| format!("msg {i}")).collect();
        expected.sort();
        assert_eq!(
            seen_bodies, expected,
            "backward pagination must cover every message exactly once"
        );
    }

    #[tokio::test]
    async fn single_message_room_from_t1_dir_b_omits_end() {
        // Exact shape observed in the live FluffyChat repro: a room with one
        // message, client requests from=t1&dir=b&limit=100. Before the fix,
        // this always returned a non-empty "end", causing FluffyChat to loop
        // forever re-requesting the identical page.
        let server = test_server();
        let auth = register_and_bearer(&server, "erin").await;
        let room_id = create_room(&server, &auth, "singlemsg").await;
        send_message(&server, &auth, &room_id, "txn0", "only message").await;

        let resp: Value = server
            .get(&format!("/_matrix/client/v3/rooms/{room_id}/messages"))
            .add_header(auth.0.clone(), auth.1.clone())
            .add_query_param("from", "t1")
            .add_query_param("dir", "b")
            .add_query_param("limit", "100")
            .await
            .json();

        assert!(
            resp.get("end").is_none(),
            "must omit 'end' once the start of history is reached, or a real client's \
             pagination loop never terminates; got {resp:?}"
        );
        let chunk = resp["chunk"].as_array().expect("chunk array");
        assert_eq!(
            chunk.len(),
            1,
            "must return the single existing message; got {resp:?}"
        );
        assert_eq!(chunk[0]["content"]["body"].as_str(), Some("only message"));
    }
}
