// START_AI_HEADER
// MODULE: matrix-hs/src/redact_test.rs
// PURPOSE: Integration test for PUT /rooms/{roomId}/redact/{eventId}/{txnId}
//          (routes/redact.rs). Proves the end-to-end contract: a redacted
//          message's content is masked to {} and carries unsigned.
//          redacted_because on every timeline-read path, while a NEW message
//          in the same room is unaffected.
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
        body: Value,
    ) -> String {
        let resp = server
            .put(&format!(
                "/_matrix/client/v3/rooms/{room_id}/send/m.room.message/{txn_id}"
            ))
            .add_header(auth.0.clone(), auth.1.clone())
            .json(&body)
            .await;
        resp.assert_status_ok();
        let resp_body: Value = resp.json();
        resp_body["event_id"]
            .as_str()
            .expect("event_id")
            .to_string()
    }

    async fn sync_timeline_events(
        server: &TestServer,
        auth: &(HeaderName, HeaderValue),
        room_id: &str,
    ) -> Vec<Value> {
        let resp = server
            .get("/_matrix/client/v3/sync")
            .add_header(auth.0.clone(), auth.1.clone())
            .await;
        resp.assert_status_ok();
        let body: Value = resp.json();
        body["rooms"]["join"][room_id]["timeline"]["events"]
            .as_array()
            .cloned()
            .unwrap_or_default()
    }

    // redact_masks_content_and_leaves_others_intact:start
    //   purpose: alice sends two messages in a room she and bob share; alice redacts
    //            the first. bob's /sync timeline must show the redacted event with
    //            content={} and unsigned.redacted_because set, an m.room.redaction
    //            event present, and the SECOND message untouched.
    // redact_masks_content_and_leaves_others_intact:end
    #[tokio::test]
    async fn redact_masks_content_and_leaves_others_intact() {
        let server = test_server();
        let alice = register_and_bearer(&server, "alice").await;
        let bob = register_and_bearer(&server, "bob").await;
        let room_id = create_room(&server, &alice, "redact-room").await;

        server
            .post(&format!("/_matrix/client/v3/rooms/{room_id}/join"))
            .add_header(bob.0.clone(), bob.1.clone())
            .json(&json!({}))
            .await
            .assert_status_ok();

        let target_event_id = send_message(
            &server,
            &alice,
            &room_id,
            "txn1",
            json!({ "msgtype": "m.text", "body": "oops, secret" }),
        )
        .await;
        let kept_event_id = send_message(
            &server,
            &alice,
            &room_id,
            "txn2",
            json!({ "msgtype": "m.text", "body": "unrelated message" }),
        )
        .await;

        let redact_resp = server
            .put(&format!(
                "/_matrix/client/v3/rooms/{room_id}/redact/{target_event_id}/redact-txn1"
            ))
            .add_header(alice.0.clone(), alice.1.clone())
            .json(&json!({ "reason": "posted by mistake" }))
            .await;
        redact_resp.assert_status_ok();
        let redact_body: Value = redact_resp.json();
        let redaction_event_id = redact_body["event_id"]
            .as_str()
            .unwrap_or_else(|| panic!("redact response missing event_id: {redact_body}"))
            .to_string();

        let events = sync_timeline_events(&server, &bob, &room_id).await;

        let redacted = events
            .iter()
            .find(|e| e["event_id"] == target_event_id)
            .unwrap_or_else(|| {
                panic!(
                "redacted event {target_event_id} must still appear in timeline; got {events:?}"
            )
            });
        assert_eq!(
            redacted["content"],
            json!({}),
            "redacted event content must be masked to {{}}; got {redacted}"
        );
        assert_eq!(
            redacted["unsigned"]["redacted_because"]["event_id"].as_str(),
            Some(redaction_event_id.as_str()),
            "redacted event must carry unsigned.redacted_because pointing at the redaction; got {redacted}"
        );

        let redaction_ev = events
            .iter()
            .find(|e| e["event_id"] == redaction_event_id)
            .unwrap_or_else(|| {
                panic!(
                    "the m.room.redaction event itself must appear in the timeline; got {events:?}"
                )
            });
        assert_eq!(redaction_ev["type"], "m.room.redaction");
        assert_eq!(redaction_ev["redacts"], target_event_id);

        let kept = events
            .iter()
            .find(|e| e["event_id"] == kept_event_id)
            .unwrap_or_else(|| panic!("unrelated event must be untouched; got {events:?}"));
        assert_eq!(
            kept["content"]["body"].as_str(),
            Some("unrelated message"),
            "non-redacted event content must be unchanged; got {kept}"
        );
    }

    // redact_requires_membership:start
    //   purpose: a non-member cannot redact an event in a room they never joined.
    // redact_requires_membership:end
    #[tokio::test]
    async fn redact_requires_membership() {
        let server = test_server();
        let alice = register_and_bearer(&server, "alice2").await;
        let mallory = register_and_bearer(&server, "mallory2").await;
        let room_id = create_room(&server, &alice, "redact-room-2").await;

        let event_id = send_message(
            &server,
            &alice,
            &room_id,
            "txn1",
            json!({ "msgtype": "m.text", "body": "hello" }),
        )
        .await;

        let resp = server
            .put(&format!(
                "/_matrix/client/v3/rooms/{room_id}/redact/{event_id}/redact-txn-x"
            ))
            .add_header(mallory.0.clone(), mallory.1.clone())
            .json(&json!({}))
            .await;

        assert_ne!(
            resp.status_code().as_u16(),
            200,
            "a non-member must not be able to redact; got 200"
        );
    }
    // redaction_survives_restart:start
    //   purpose: A redaction must outlive the process that issued it. The mask lives
    //            in AppState.redactions, which is NOT persisted as its own file — it
    //            is rebuilt on replay from the m.room.redaction events in the room
    //            journal. Before that rebuild existed, restarting a node silently
    //            un-redacted every message anyone had ever deleted on it.
    //   input:  none (temp data dir, removed at the end)
    //   output: assertion that the target is still masked after replay
    //   sideEffects: writes journals under a temp dir
    // redaction_survives_restart:end
    #[tokio::test]
    async fn redaction_survives_restart() {
        use crate::persist::replay_from_dir;
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "matrix_hs_redact_restart_{}_{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).expect("temp dir");

        let room_id;
        let target_event_id;
        {
            let state = AppState::with_data_dir(dir.clone());
            let server = TestServer::new(router(state));
            let alice = register_and_bearer(&server, "alice").await;
            room_id = create_room(&server, &alice, "restart-redact-room").await;
            target_event_id = send_message(
                &server,
                &alice,
                &room_id,
                "tx1",
                json!({ "msgtype": "m.text", "body": "secret" }),
            )
            .await;
            send_message(
                &server,
                &alice,
                &room_id,
                "tx2",
                json!({ "msgtype": "m.text", "body": "kept" }),
            )
            .await;

            server
                .put(&format!(
                    "/_matrix/client/v3/rooms/{room_id}/redact/{target_event_id}/tx-redact"
                ))
                .add_header(alice.0.clone(), alice.1.clone())
                .json(&json!({ "reason": "mistake" }))
                .await
                .assert_status_ok();

            // Masked before the restart — otherwise the test proves nothing after it.
            let events = sync_timeline_events(&server, &alice, &room_id).await;
            let target = events
                .iter()
                .find(|e| e["event_id"] == target_event_id.as_str())
                .expect("target present pre-restart");
            assert_eq!(
                target["content"],
                json!({}),
                "precondition: content must already be masked before the restart"
            );
        }

        // ── restart: fresh AppState, same journals ────────────────────────────
        let state2 = AppState::with_data_dir(dir.clone());
        replay_from_dir(&state2, &dir).expect("replay");
        let server2 = TestServer::new(router(state2));
        let alice2 = login_bearer(&server2, "alice").await;

        let events = sync_timeline_events(&server2, &alice2, &room_id).await;
        let target = events
            .iter()
            .find(|e| e["event_id"] == target_event_id.as_str())
            .expect("target event present after replay");
        assert_eq!(
            target["content"],
            json!({}),
            "the redaction must be rebuilt from the journal on replay, not lost with \
             the process that issued it. Got: {target}"
        );
        assert!(
            target["unsigned"]["redacted_because"].is_object(),
            "masked events must still carry redacted_because after replay"
        );

        let kept = events
            .iter()
            .find(|e| e["content"]["body"] == "kept")
            .expect("the other message must survive replay untouched");
        assert_eq!(kept["content"]["body"], "kept");

        let _ = std::fs::remove_dir_all(&dir);
    }

    // login_bearer:start
    //   purpose: Log in as an already-registered user (the restart test cannot
    //            re-register: the account came back from the journal).
    //   input:  server; username
    //   output: Authorization header pair
    //   sideEffects: none beyond the login call
    // login_bearer:end
    async fn login_bearer(server: &TestServer, username: &str) -> (HeaderName, HeaderValue) {
        let body: Value = server
            .post("/_matrix/client/v3/login")
            .json(&json!({
                "type": "m.login.password",
                "identifier": { "type": "m.id.user", "user": username },
                "password": "pw"
            }))
            .await
            .json();
        let token = body["access_token"]
            .as_str()
            .unwrap_or_else(|| panic!("login for {username} returned no token: {body}"))
            .to_string();
        (
            HeaderName::from_static("authorization"),
            HeaderValue::from_str(&format!("Bearer {token}")).expect("header"),
        )
    }
}
