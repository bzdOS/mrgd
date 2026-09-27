// START_AI_HEADER
// MODULE: matrix-hs/src/ephemeral_test.rs
// PURPOSE: Integration tests for the real ephemeral EDU implementation (typing,
//          receipts, read_markers — see routes/ephemeral.rs, state.rs "Ephemeral
//          EDUs" section). Node-local: two distinct authenticated users (alice,
//          bob) share one AppState/TestServer, with bob explicitly joining
//          alice's room (see join_room below) — /sync only returns rooms the
//          caller has actually joined (routes/sync.rs::build_join_rooms), so
//          this is required, not optional, for bob's /sync calls to see
//          anything. Alice's PUT typing / POST receipt calls must then be
//          visible in bob's GET /sync ephemeral block for that room.
//
//          Scenarios:
//            1. typing_visible_in_sync — alice PUTs typing=true; bob's /sync shows
//               an m.typing event listing alice's user_id.
//            2. typing_stop_clears — alice PUTs typing=false; the next /sync no
//               longer lists her.
//            3. typing_expires_after_timeout — alice PUTs typing=true with a short
//               timeout; after the timeout elapses (lazy expiry at read time, no
//               explicit stop), /sync no longer lists her.
//            4. receipt_visible_in_sync — alice POSTs an m.read receipt; bob's
//               /sync shows an m.receipt event with alice's user_id under that
//               event_id/m.read.
//            5. read_markers_accepted — POST read_markers with m.fully_read (and
//               m.read) returns 200 and the m.read receipt propagates to /sync
//               exactly like scenario 4.
//            6. typing_wrong_user_forbidden — a client cannot set another user's
//               typing state (403 M_FORBIDDEN).
//            7. typing_unknown_room_not_found — typing on a nonexistent room_id
//               returns 404 M_NOT_FOUND.
// DEPENDENCIES: axum-test, serde_json, matrix_hs::{router, AppState}
// END_AI_HEADER

#[cfg(test)]
mod tests {
    use crate::{router, AppState};
    use axum::http::{HeaderName, HeaderValue};
    use axum_test::TestServer;
    use serde_json::{json, Value};

    // test_server:start
    //   purpose: Build a fresh TestServer with an empty in-memory AppState (no
    //            cluster, no persistence — mirrors the other *_test.rs helpers).
    //   input:  none
    //   output: TestServer
    //   sideEffects: none
    // test_server:end
    fn test_server() -> TestServer {
        let state = AppState::new();
        let app = router(state);
        TestServer::new(app)
    }

    // auth:start
    //   purpose: Register a user via two-step UIA and return an Authorization
    //            header carrying the signed mxt_ token. Duplicated across
    //            *_test.rs modules by repo convention (see keys_test.rs, cluster_test.rs).
    //   input:  server, user — localpart
    //   output: (HeaderName, HeaderValue)
    //   sideEffects: inserts the user into AppState via /register
    // auth:end
    async fn auth(server: &TestServer, user: &str) -> (HeaderName, HeaderValue) {
        let challenge: Value = server
            .post("/_matrix/client/v3/register")
            .json(&json!({ "username": user, "password": "pw" }))
            .await
            .json();
        let session = challenge["session"]
            .as_str()
            .unwrap_or_else(|| panic!("auth: missing session for {user}; got {challenge}"))
            .to_string();

        let reg: Value = server
            .post("/_matrix/client/v3/register")
            .json(&json!({
                "username": user,
                "password": "pw",
                "auth": { "type": "m.login.dummy", "session": session }
            }))
            .await
            .json();
        let token = reg["access_token"]
            .as_str()
            .unwrap_or_else(|| panic!("auth: missing access_token for {user}; got {reg}"))
            .to_string();

        (
            HeaderName::from_static("authorization"),
            HeaderValue::from_str(&format!("Bearer {token}")).expect("header value"),
        )
    }

    // create_room:start
    //   purpose: Create a room as the given caller and return its room_id.
    //   input:  server, auth_header, alias — room_alias_name
    //   output: String room_id (e.g. "!alias:localhost")
    //   sideEffects: room created in AppState
    // create_room:end
    async fn create_room(
        server: &TestServer,
        auth_header: &(HeaderName, HeaderValue),
        alias: &str,
    ) -> String {
        let resp = server
            .post("/_matrix/client/v3/createRoom")
            .add_header(auth_header.0.clone(), auth_header.1.clone())
            .json(&json!({ "room_alias_name": alias }))
            .await;
        resp.assert_status_ok();
        let body: Value = resp.json();
        body["room_id"].as_str().expect("room_id").to_string()
    }

    // join_room:start
    //   purpose: Join a room as the given caller (this server's join is fully
    //            permissive — no invite/join_rules gate — so any authenticated
    //            user can call this directly). /sync is now scoped to the
    //            caller's own joined rooms (see routes/sync.rs::build_join_rooms
    //            — previously it leaked every room to every caller regardless
    //            of membership), so bob must actually join before his /sync
    //            calls can see alice's room at all.
    //   input:  server, auth_header, room_id
    //   output: none (asserts 200)
    //   sideEffects: adds a join m.room.member event for the caller in room_id
    // join_room:end
    async fn join_room(
        server: &TestServer,
        auth_header: &(HeaderName, HeaderValue),
        room_id: &str,
    ) {
        server
            .post(&format!("/_matrix/client/v3/rooms/{room_id}/join"))
            .add_header(auth_header.0.clone(), auth_header.1.clone())
            .await
            .assert_status_ok();
    }

    // sync_ephemeral_events:start
    //   purpose: GET /sync as the given caller and return the room's
    //            ephemeral.events array (empty Vec if the room is absent from
    //            the response, e.g. nobody typing and no receipts yet).
    //   input:  server, auth_header, room_id
    //   output: Vec<Value> — ephemeral.events for room_id
    //   sideEffects: none (read-only HTTP call)
    // sync_ephemeral_events:end
    async fn sync_ephemeral_events(
        server: &TestServer,
        auth_header: &(HeaderName, HeaderValue),
        room_id: &str,
    ) -> Vec<Value> {
        let resp = server
            .get("/_matrix/client/v3/sync")
            .add_header(auth_header.0.clone(), auth_header.1.clone())
            .await;
        resp.assert_status_ok();
        let body: Value = resp.json();
        body["rooms"]["join"][room_id]["ephemeral"]["events"]
            .as_array()
            .cloned()
            .unwrap_or_default()
    }

    // typing_visible_in_sync:start
    //   purpose: alice PUTs typing=true in a room she and bob share; bob's /sync
    //            must show an m.typing event whose content.user_ids contains alice.
    //   input:  none
    //   output: assertion on bob's sync ephemeral events
    //   sideEffects: none beyond in-memory state
    // typing_visible_in_sync:end
    #[tokio::test]
    async fn typing_visible_in_sync() {
        let server = test_server();
        let alice = auth(&server, "alice").await;
        let bob = auth(&server, "bob").await;
        let room_id = create_room(&server, &alice, "typing-room").await;
        join_room(&server, &bob, &room_id).await;

        let resp = server
            .put(&format!(
                "/_matrix/client/v3/rooms/{room_id}/typing/@alice:localhost"
            ))
            .add_header(alice.0.clone(), alice.1.clone())
            .json(&json!({ "typing": true, "timeout": 30000 }))
            .await;
        resp.assert_status_ok();

        let events = sync_ephemeral_events(&server, &bob, &room_id).await;
        let typing_ev = events
            .iter()
            .find(|ev| ev["type"] == "m.typing")
            .unwrap_or_else(|| panic!("bob's sync must contain an m.typing event; got {events:?}"));

        let user_ids = typing_ev["content"]["user_ids"]
            .as_array()
            .expect("m.typing content.user_ids must be an array");
        assert!(
            user_ids
                .iter()
                .any(|v| v.as_str() == Some("@alice:localhost")),
            "m.typing user_ids must contain alice; got {user_ids:?}"
        );
    }

    // typing_stop_clears:start
    //   purpose: After alice explicitly stops typing (typing=false), the next
    //            /sync must no longer list her in m.typing (or omit the event
    //            entirely if she was the only one typing).
    //   input:  none
    //   output: assertion that alice is absent from any subsequent m.typing event
    //   sideEffects: none beyond in-memory state
    // typing_stop_clears:end
    #[tokio::test]
    async fn typing_stop_clears() {
        let server = test_server();
        let alice = auth(&server, "alice").await;
        let bob = auth(&server, "bob").await;
        let room_id = create_room(&server, &alice, "typing-stop-room").await;
        join_room(&server, &bob, &room_id).await;

        server
            .put(&format!(
                "/_matrix/client/v3/rooms/{room_id}/typing/@alice:localhost"
            ))
            .add_header(alice.0.clone(), alice.1.clone())
            .json(&json!({ "typing": true, "timeout": 30000 }))
            .await
            .assert_status_ok();

        server
            .put(&format!(
                "/_matrix/client/v3/rooms/{room_id}/typing/@alice:localhost"
            ))
            .add_header(alice.0.clone(), alice.1.clone())
            .json(&json!({ "typing": false }))
            .await
            .assert_status_ok();

        let events = sync_ephemeral_events(&server, &bob, &room_id).await;
        let still_typing = events
            .iter()
            .find(|ev| ev["type"] == "m.typing")
            .and_then(|ev| ev["content"]["user_ids"].as_array().cloned())
            .unwrap_or_default()
            .iter()
            .any(|v| v.as_str() == Some("@alice:localhost"));
        assert!(
            !still_typing,
            "alice must be cleared from m.typing after typing:false; got {events:?}"
        );
    }

    // typing_expires_after_timeout:start
    //   purpose: A typing indicator with a short timeout and no explicit stop must
    //            lazily expire — after the timeout elapses, /sync must not list
    //            the user any more, proving expiry does not depend on a background
    //            sweep thread.
    //   input:  none
    //   output: assertion that alice is absent after sleeping past the timeout
    //   sideEffects: sleeps 60ms (test only)
    // typing_expires_after_timeout:end
    #[tokio::test]
    async fn typing_expires_after_timeout() {
        let server = test_server();
        let alice = auth(&server, "alice").await;
        let bob = auth(&server, "bob").await;
        let room_id = create_room(&server, &alice, "typing-expiry-room").await;
        join_room(&server, &bob, &room_id).await;

        server
            .put(&format!(
                "/_matrix/client/v3/rooms/{room_id}/typing/@alice:localhost"
            ))
            .add_header(alice.0.clone(), alice.1.clone())
            // timeout is clamped to a minimum of 1ms server-side; use a small
            // but real value so the test doesn't depend on the clamp floor.
            .json(&json!({ "typing": true, "timeout": 20 }))
            .await
            .assert_status_ok();

        tokio::time::sleep(std::time::Duration::from_millis(80)).await;

        let events = sync_ephemeral_events(&server, &bob, &room_id).await;
        let still_typing = events
            .iter()
            .find(|ev| ev["type"] == "m.typing")
            .and_then(|ev| ev["content"]["user_ids"].as_array().cloned())
            .unwrap_or_default()
            .iter()
            .any(|v| v.as_str() == Some("@alice:localhost"));
        assert!(
            !still_typing,
            "alice's typing indicator must have expired; got {events:?}"
        );
    }

    // receipt_visible_in_sync:start
    //   purpose: alice POSTs an m.read receipt for some event_id; bob's /sync must
    //            show an m.receipt event whose content[event_id]["m.read"] contains
    //            alice's user_id with a "ts" field.
    //   input:  none
    //   output: assertion on bob's sync ephemeral events
    //   sideEffects: none beyond in-memory state
    // receipt_visible_in_sync:end
    #[tokio::test]
    async fn receipt_visible_in_sync() {
        let server = test_server();
        let alice = auth(&server, "alice").await;
        let bob = auth(&server, "bob").await;
        let room_id = create_room(&server, &alice, "receipt-room").await;
        join_room(&server, &bob, &room_id).await;

        let event_id = "$some-event-alice-read";
        let resp = server
            .post(&format!(
                "/_matrix/client/v3/rooms/{room_id}/receipt/m.read/{event_id}"
            ))
            .add_header(alice.0.clone(), alice.1.clone())
            .json(&json!({}))
            .await;
        resp.assert_status_ok();

        let events = sync_ephemeral_events(&server, &bob, &room_id).await;
        let receipt_ev = events
            .iter()
            .find(|ev| ev["type"] == "m.receipt")
            .unwrap_or_else(|| {
                panic!("bob's sync must contain an m.receipt event; got {events:?}")
            });

        let ts = receipt_ev["content"][event_id]["m.read"]["@alice:localhost"]["ts"].as_u64();
        assert!(
            ts.is_some(),
            "m.receipt content must contain alice's m.read ts for {event_id}; got {receipt_ev}"
        );
    }

    // read_markers_accepted:start
    //   purpose: POST /read_markers with both m.fully_read and m.read must return
    //            200, and the m.read receipt embedded in the same call must
    //            propagate to /sync exactly like a standalone POST /receipt call.
    //   input:  none
    //   output: 200 response + m.receipt event visible in bob's /sync
    //   sideEffects: none beyond in-memory state
    // read_markers_accepted:end
    #[tokio::test]
    async fn read_markers_accepted() {
        let server = test_server();
        let alice = auth(&server, "alice").await;
        let bob = auth(&server, "bob").await;
        let room_id = create_room(&server, &alice, "read-markers-room").await;
        join_room(&server, &bob, &room_id).await;

        let event_id = "$some-event-fully-read";
        let resp = server
            .post(&format!("/_matrix/client/v3/rooms/{room_id}/read_markers"))
            .add_header(alice.0.clone(), alice.1.clone())
            .json(&json!({
                "m.fully_read": event_id,
                "m.read":       event_id
            }))
            .await;
        resp.assert_status_ok();
        let body: Value = resp.json();
        assert_eq!(body, json!({}), "read_markers must return an empty object");

        let events = sync_ephemeral_events(&server, &bob, &room_id).await;
        let receipt_ev = events
            .iter()
            .find(|ev| ev["type"] == "m.receipt")
            .unwrap_or_else(|| {
                panic!("bob's sync must contain an m.receipt event; got {events:?}")
            });
        let ts = receipt_ev["content"][event_id]["m.read"]["@alice:localhost"]["ts"].as_u64();
        assert!(
            ts.is_some(),
            "read_markers' embedded m.read must propagate to /sync; got {receipt_ev}"
        );
    }

    // typing_wrong_user_forbidden:start
    //   purpose: A caller may only set THEIR OWN typing state — PUT typing for a
    //            different {userId} than the authenticated caller must be rejected.
    //   input:  none
    //   output: 403 M_FORBIDDEN
    //   sideEffects: none
    // typing_wrong_user_forbidden:end
    #[tokio::test]
    async fn typing_wrong_user_forbidden() {
        let server = test_server();
        let alice = auth(&server, "alice").await;
        let bob = auth(&server, "bob").await;
        let room_id = create_room(&server, &alice, "typing-forbidden-room").await;
        join_room(&server, &bob, &room_id).await;

        let resp = server
            .put(&format!(
                "/_matrix/client/v3/rooms/{room_id}/typing/@bob:localhost"
            ))
            .add_header(alice.0.clone(), alice.1.clone())
            .json(&json!({ "typing": true, "timeout": 30000 }))
            .await;
        assert_eq!(
            resp.status_code(),
            403,
            "must be 403; got {}",
            resp.status_code()
        );
        let body: Value = resp.json();
        assert_eq!(body["errcode"].as_str(), Some("M_FORBIDDEN"));
    }

    // typing_unknown_room_not_found:start
    //   purpose: PUT typing against a room_id that does not exist must return 404
    //            M_NOT_FOUND rather than silently creating ephemeral state for it.
    //   input:  none
    //   output: 404 M_NOT_FOUND
    //   sideEffects: none
    // typing_unknown_room_not_found:end
    #[tokio::test]
    async fn typing_unknown_room_not_found() {
        let server = test_server();
        let alice = auth(&server, "alice").await;

        let resp = server
            .put("/_matrix/client/v3/rooms/!does-not-exist:localhost/typing/@alice:localhost")
            .add_header(alice.0.clone(), alice.1.clone())
            .json(&json!({ "typing": true, "timeout": 30000 }))
            .await;
        assert_eq!(
            resp.status_code(),
            404,
            "must be 404; got {}",
            resp.status_code()
        );
        let body: Value = resp.json();
        assert_eq!(body["errcode"].as_str(), Some("M_NOT_FOUND"));
    }
}
