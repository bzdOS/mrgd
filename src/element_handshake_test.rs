// START_AI_HEADER
// MODULE: matrix-hs/src/element_handshake_test.rs
// PURPOSE: Integration test simulating the Element client handshake sequence.
//          Tests the full Matrix CS-API Stage 2 flow: versions discovery,
//          well-known, login flows, authentication, room creation, state,
//          incremental sync with since tokens.
//          All requests go through an in-process axum-test server — no network.
// DEPENDENCIES: axum-test, serde_json, matrix_hs::{router, AppState}
// END_AI_HEADER

#[cfg(test)]
mod tests {
    use crate::{router, AppState};
    use axum::http::{HeaderName, HeaderValue};
    use axum_test::TestServer;
    use serde_json::{json, Value};

    // element_handshake_test_server:start
    //   purpose: Build a fresh TestServer backed by a new in-memory AppState.
    //   input:  none
    //   output: TestServer
    //   sideEffects: none
    // element_handshake_test_server:end
    fn test_server() -> TestServer {
        let state = AppState::new();
        let app = router(state);
        TestServer::new(app)
    }

    // element_handshake:start
    //   purpose: Simulate the Element client handshake — 15 sequential steps covering
    //            versions, well-known, login flows, authentication, room creation,
    //            state, joined_rooms, directory, sync, send, incremental sync.
    //   input:  none (in-process server with fresh AppState)
    //   output: all assertions pass — full Element handshake succeeds
    //   sideEffects: room + events + state in AppState
    // element_handshake:end
    #[tokio::test]
    async fn element_handshake() {
        let server = test_server();

        // ── Step 1: GET versions ─────────────────────────────────────────────
        let resp = server.get("/_matrix/client/versions").await;
        resp.assert_status_ok();
        let body: Value = resp.json();
        let versions = body["versions"].as_array().expect("versions array");
        assert!(
            versions.iter().any(|v| v.as_str() == Some("r0.6.0")),
            "r0.6.0 must be in versions"
        );

        // ── Step 2: GET .well-known ──────────────────────────────────────────
        let resp = server.get("/.well-known/matrix/client").await;
        resp.assert_status_ok();
        let body: Value = resp.json();
        assert!(
            body["m.homeserver"]["base_url"].as_str().is_some(),
            "m.homeserver.base_url must be present"
        );

        // ── Step 3: GET login flows ──────────────────────────────────────────
        let resp = server.get("/_matrix/client/v3/login").await;
        resp.assert_status_ok();
        let body: Value = resp.json();
        let flows = body["flows"].as_array().expect("flows array");
        assert!(
            flows
                .iter()
                .any(|f| f["type"].as_str() == Some("m.login.password")),
            "m.login.password must be in flows"
        );

        // ── Step 3.5: Register alice (required before login under HMAC auth) ────
        // UIA two-step: challenge then complete.
        let challenge: Value = server
            .post("/_matrix/client/v3/register")
            .json(&json!({ "username": "alice", "password": "secret" }))
            .await
            .json();
        let uia_session = challenge["session"]
            .as_str()
            .expect("UIA session")
            .to_string();
        server
            .post("/_matrix/client/v3/register")
            .json(&json!({
                "username": "alice",
                "password": "secret",
                "auth": { "type": "m.login.dummy", "session": uia_session }
            }))
            .await
            .assert_status_ok();

        // ── Step 4: POST login ────────────────────────────────────────────────
        let resp = server
            .post("/_matrix/client/v3/login")
            .json(&json!({
                "type": "m.login.password",
                "identifier": { "type": "m.id.user", "user": "alice" },
                "password": "secret"
            }))
            .await;
        resp.assert_status_ok();
        let body: Value = resp.json();
        let access_token = body["access_token"]
            .as_str()
            .expect("access_token")
            .to_string();
        let user_id = body["user_id"].as_str().expect("user_id").to_string();
        assert!(
            access_token.starts_with("mxt_"),
            "access_token must start with mxt_; got {access_token}"
        );
        assert_eq!(user_id, "@alice:localhost");

        // Helper to attach Bearer token.
        let auth_header_name = HeaderName::from_static("authorization");
        let auth_header_value =
            HeaderValue::from_str(&format!("Bearer {access_token}")).expect("header value");

        // ── Step 5: GET whoami ────────────────────────────────────────────────
        let resp = server
            .get("/_matrix/client/v3/account/whoami")
            .add_header(auth_header_name.clone(), auth_header_value.clone())
            .await;
        resp.assert_status_ok();
        let body: Value = resp.json();
        assert_eq!(
            body["user_id"].as_str(),
            Some("@alice:localhost"),
            "whoami user_id"
        );

        // ── Step 6: GET capabilities ──────────────────────────────────────────
        let resp = server.get("/_matrix/client/v3/capabilities").await;
        resp.assert_status_ok();
        let body: Value = resp.json();
        assert!(
            body["capabilities"].is_object(),
            "capabilities must be object"
        );

        // ── Step 7: GET pushrules ─────────────────────────────────────────────
        let resp = server.get("/_matrix/client/v3/pushrules/").await;
        resp.assert_status_ok();
        let body: Value = resp.json();
        assert!(body["global"].is_object(), "global must be object");

        // ── Step 8: POST filter ───────────────────────────────────────────────
        let filter_path = format!("/_matrix/client/v3/user/{}/filter", user_id);
        let resp = server.post(&filter_path).json(&json!({})).await;
        resp.assert_status_ok();
        let body: Value = resp.json();
        assert!(
            body["filter_id"].as_str().is_some(),
            "filter_id must be present"
        );

        // ── Step 9: POST createRoom ───────────────────────────────────────────
        let resp = server
            .post("/_matrix/client/v3/createRoom")
            .add_header(auth_header_name.clone(), auth_header_value.clone())
            .json(&json!({
                "name": "Test Room",
                "room_alias_name": "test-room-handshake"
            }))
            .await;
        resp.assert_status_ok();
        let body: Value = resp.json();
        let room_id = body["room_id"].as_str().expect("room_id").to_string();
        assert_eq!(room_id, "!test-room-handshake:localhost");

        // ── Step 10: GET room state ───────────────────────────────────────────
        let state_path = format!("/_matrix/client/v3/rooms/{}/state", room_id);
        let resp = server
            .get(&state_path)
            .add_header(auth_header_name.clone(), auth_header_value.clone())
            .await;
        resp.assert_status_ok();
        let body: Value = resp.json();
        let state_events = body.as_array().expect("state events array");

        let has_create = state_events.iter().any(|ev| ev["type"] == "m.room.create");
        assert!(has_create, "state must contain m.room.create");

        let has_member = state_events
            .iter()
            .any(|ev| ev["type"] == "m.room.member" && ev["content"]["membership"] == "join");
        assert!(has_member, "state must contain m.room.member with join");

        let has_power_levels = state_events
            .iter()
            .any(|ev| ev["type"] == "m.room.power_levels");
        assert!(has_power_levels, "state must contain m.room.power_levels");

        // ── Step 11: GET joined_rooms ─────────────────────────────────────────
        let resp = server
            .get("/_matrix/client/v3/joined_rooms")
            .add_header(auth_header_name.clone(), auth_header_value.clone())
            .await;
        resp.assert_status_ok();
        let body: Value = resp.json();
        let joined = body["joined_rooms"].as_array().expect("joined_rooms array");
        assert!(
            joined.iter().any(|r| r.as_str() == Some(&room_id)),
            "joined_rooms must contain the created room"
        );

        // ── Step 12: GET directory alias ──────────────────────────────────────
        let alias = "#test-room-handshake:localhost";
        let dir_path = format!("/_matrix/client/v3/directory/room/{}", alias);
        let resp = server.get(&dir_path).await;
        resp.assert_status_ok();
        let body: Value = resp.json();
        assert_eq!(
            body["room_id"].as_str(),
            Some(room_id.as_str()),
            "directory room_id must match"
        );

        // ── Step 13: GET initial sync ─────────────────────────────────────────
        let resp = server
            .get("/_matrix/client/v3/sync")
            .add_header(auth_header_name.clone(), auth_header_value.clone())
            .await;
        resp.assert_status_ok();
        let sync_body: Value = resp.json();

        let next_batch = sync_body["next_batch"]
            .as_str()
            .expect("next_batch")
            .to_string();
        assert!(
            next_batch.starts_with('s'),
            "next_batch must start with 's'; got {next_batch}"
        );

        // Room must appear in rooms.join.
        assert!(
            sync_body["rooms"]["join"].get(&room_id).is_some(),
            "room must appear in rooms.join after initial sync"
        );

        // ── Step 14: PUT send message ─────────────────────────────────────────
        let send_path = format!(
            "/_matrix/client/v3/rooms/{}/send/m.room.message/txnE",
            room_id
        );
        let resp = server
            .put(&send_path)
            .add_header(auth_header_name.clone(), auth_header_value.clone())
            .json(&json!({ "msgtype": "m.text", "body": "hello from element handshake" }))
            .await;
        resp.assert_status_ok();
        let body: Value = resp.json();
        let event_id = body["event_id"].as_str().expect("event_id").to_string();
        assert!(
            event_id.starts_with('$'),
            "event_id must start with '$'; got {event_id}"
        );

        // ── Step 15: GET incremental sync ─────────────────────────────────────
        let since_path = format!("/_matrix/client/v3/sync?since={}", next_batch);
        let resp = server
            .get(&since_path)
            .add_header(auth_header_name.clone(), auth_header_value.clone())
            .await;
        resp.assert_status_ok();
        let inc_body: Value = resp.json();

        let inc_next_batch = inc_body["next_batch"]
            .as_str()
            .expect("incremental next_batch")
            .to_string();

        // next_batch must have advanced.
        assert_ne!(
            inc_next_batch, next_batch,
            "incremental next_batch must differ from initial next_batch"
        );

        // Message must appear in timeline.events.
        let timeline_events = inc_body["rooms"]["join"][&room_id]["timeline"]["events"]
            .as_array()
            .unwrap_or_else(|| {
                panic!(
                    "incremental timeline.events missing; room_id={room_id}; inc_body={inc_body}"
                )
            });

        let found = timeline_events
            .iter()
            .any(|ev| ev["event_id"].as_str() == Some(event_id.as_str()));
        assert!(
            found,
            "message event_id {event_id} must appear in incremental sync timeline; got {timeline_events:?}"
        );
    }
}
