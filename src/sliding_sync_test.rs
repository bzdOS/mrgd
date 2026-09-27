// START_AI_HEADER
// MODULE: matrix-hs/src/sliding_sync_test.rs
// PURPOSE: Autonomous in-process test for MSC4186 Simplified Sliding Sync — the
//          console self-test loop (no real client/phone needed). Mirrors
//          element_handshake_test: register -> login -> createRoom -> send ->
//          POST sliding sync -> assert MSC4186 response shape/content.
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
        TestServer::new(router(state))
    }

    async fn auth(server: &TestServer) -> (HeaderName, HeaderValue) {
        let ch: Value = server
            .post("/_matrix/client/v3/register")
            .json(&json!({ "username": "alice", "password": "secret" }))
            .await
            .json();
        let sess = ch["session"].as_str().expect("uia session").to_string();
        server
            .post("/_matrix/client/v3/register")
            .json(&json!({ "username": "alice", "password": "secret",
                "auth": { "type": "m.login.dummy", "session": sess } }))
            .await;
        let login: Value = server
            .post("/_matrix/client/v3/login")
            .json(&json!({ "type": "m.login.password",
                "identifier": { "type": "m.id.user", "user": "alice" },
                "password": "secret" }))
            .await
            .json();
        let token = login["access_token"].as_str().expect("token").to_string();
        let hn = HeaderName::from_static("authorization");
        let hv = HeaderValue::from_str(&format!("Bearer {token}")).unwrap();
        (hn, hv)
    }

    #[tokio::test]
    async fn sliding_sync_advertised_and_serves() {
        let server = test_server();

        // 1) versions advertises sliding sync (else Element X won't try it)
        let v: Value = server.get("/_matrix/client/versions").await.json();
        assert_eq!(
            v["unstable_features"]["org.matrix.simplified_msc3575"],
            json!(true),
            "must advertise org.matrix.simplified_msc3575"
        );

        let (hn, hv) = auth(&server).await;

        // 2) create a room + send a message
        let cr: Value = server
            .post("/_matrix/client/v3/createRoom")
            .add_header(hn.clone(), hv.clone())
            .json(&json!({ "name": "SS Room", "room_alias_name": "ss-room" }))
            .await
            .json();
        let room_id = cr["room_id"].as_str().expect("room_id").to_string();
        server
            .put(&format!(
                "/_matrix/client/v3/rooms/{room_id}/send/m.room.message/txn1"
            ))
            .add_header(hn.clone(), hv.clone())
            .json(&json!({ "msgtype": "m.text", "body": "hello sliding" }))
            .await
            .assert_status_ok();

        // 3) initial sliding sync
        let resp = server
            .post("/_matrix/client/unstable/org.matrix.simplified_msc3575/sync")
            .add_header(hn.clone(), hv.clone())
            .json(&json!({ "lists": { "all": {
                "ranges": [[0, 10]], "required_state": [["*", "*"]], "timeline_limit": 10 } } }))
            .await;
        resp.assert_status_ok();
        let b: Value = resp.json();
        assert!(
            b["pos"]
                .as_str()
                .map(|s| s.starts_with('s'))
                .unwrap_or(false),
            "pos must be s<N>, got {:?}",
            b["pos"]
        );
        assert!(
            b["lists"]["all"]["count"].as_u64().unwrap_or(0) >= 1,
            "list count >= 1"
        );
        let room = b["rooms"]
            .get(room_id.as_str())
            .expect("room present in rooms map");
        assert_eq!(room["initial"], json!(true), "initial true on first sync");
        let tl = room["timeline"].as_array().expect("timeline array");
        assert!(
            tl.iter().any(|e| e["content"]["body"] == "hello sliding"),
            "timeline must contain the sent message"
        );
        let rs = room["required_state"]
            .as_array()
            .expect("required_state array");
        assert!(
            rs.iter().any(|e| e["type"] == "m.room.create"),
            "required_state must include m.room.create"
        );
        assert!(
            b["extensions"]["to_device"].is_object(),
            "extensions.to_device present"
        );

        // 4) incremental
        let pos = b["pos"].as_str().unwrap().to_string();
        let resp2 = server
            .post(&format!(
                "/_matrix/client/unstable/org.matrix.simplified_msc3575/sync?pos={pos}"
            ))
            .add_header(hn.clone(), hv.clone())
            .json(&json!({ "lists": { "all": {
                "ranges": [[0, 10]], "required_state": [["*", "*"]], "timeline_limit": 10 } } }))
            .await;
        resp2.assert_status_ok();
        let b2: Value = resp2.json();
        assert_eq!(
            b2["rooms"].get(room_id.as_str()).expect("room")["initial"],
            json!(false),
            "initial false on incremental"
        );

        // 5) /v1/sync alias also serves
        server
            .post("/_matrix/client/v1/sync")
            .add_header(hn, hv)
            .json(&json!({ "lists": { "all": { "ranges": [[0, 10]], "timeline_limit": 5 } } }))
            .await
            .assert_status_ok();
    }
}
