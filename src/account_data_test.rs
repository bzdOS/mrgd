// START_AI_HEADER
// MODULE: matrix-hs/src/account_data_test.rs
// PURPOSE: Integration tests for account data + room tags (routes/account_data.rs).
//          Scenarios: global account_data roundtrip + /sync surfacing; per-room
//          account_data roundtrip; room tag put/get/delete; ownership enforcement
//          (a caller may not touch another user's account data).
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

    async fn whoami(server: &TestServer, auth: &(HeaderName, HeaderValue)) -> String {
        let body: Value = server
            .get("/_matrix/client/v3/account/whoami")
            .add_header(auth.0.clone(), auth.1.clone())
            .await
            .json();
        body["user_id"]
            .as_str()
            .unwrap_or_else(|| panic!("whoami missing user_id: {body}"))
            .to_string()
    }

    // global_account_data_roundtrip_and_sync:start
    //   purpose: PUT global account_data → GET returns it verbatim, and it appears
    //            in /sync's top-level account_data.events.
    // global_account_data_roundtrip_and_sync:end
    #[tokio::test]
    async fn global_account_data_roundtrip_and_sync() {
        let server = test_server();
        let alice = register_and_bearer(&server, "alice").await;
        let user_id = whoami(&server, &alice).await;

        server
            .put(&format!(
                "/_matrix/client/v3/user/{user_id}/account_data/m.direct"
            ))
            .add_header(alice.0.clone(), alice.1.clone())
            .json(&json!({ "@bob:localhost": ["!dmroom:localhost"] }))
            .await
            .assert_status_ok();

        let got: Value = server
            .get(&format!(
                "/_matrix/client/v3/user/{user_id}/account_data/m.direct"
            ))
            .add_header(alice.0.clone(), alice.1.clone())
            .await
            .json();
        assert_eq!(
            got["@bob:localhost"][0].as_str(),
            Some("!dmroom:localhost"),
            "GET must return exactly what was PUT; got {got}"
        );

        let sync_body: Value = server
            .get("/_matrix/client/v3/sync")
            .add_header(alice.0.clone(), alice.1.clone())
            .await
            .json();
        let events = sync_body["account_data"]["events"]
            .as_array()
            .unwrap_or_else(|| panic!("sync missing account_data.events: {sync_body}"));
        let found = events.iter().any(|e| {
            e["type"] == "m.direct" && e["content"]["@bob:localhost"][0] == "!dmroom:localhost"
        });
        assert!(
            found,
            "m.direct must appear in /sync account_data.events; got {events:?}"
        );
    }

    // room_account_data_roundtrip:start
    //   purpose: PUT per-room account_data → GET returns it; does not require the
    //            room to exist locally.
    // room_account_data_roundtrip:end
    #[tokio::test]
    async fn room_account_data_roundtrip() {
        let server = test_server();
        let alice = register_and_bearer(&server, "alice2").await;
        let user_id = whoami(&server, &alice).await;
        let room_id = "!someroom:localhost";

        server
            .put(&format!(
                "/_matrix/client/v3/user/{user_id}/rooms/{room_id}/account_data/m.fully_read.marker"
            ))
            .add_header(alice.0.clone(), alice.1.clone())
            .json(&json!({ "note": "custom client state" }))
            .await
            .assert_status_ok();

        let got: Value = server
            .get(&format!(
                "/_matrix/client/v3/user/{user_id}/rooms/{room_id}/account_data/m.fully_read.marker"
            ))
            .add_header(alice.0.clone(), alice.1.clone())
            .await
            .json();
        assert_eq!(got["note"].as_str(), Some("custom client state"));
    }

    // room_tags_put_get_delete:start
    //   purpose: PUT a room tag, GET shows it under "tags", DELETE removes it
    //            (and DELETE is idempotent on an absent tag).
    // room_tags_put_get_delete:end
    #[tokio::test]
    async fn room_tags_put_get_delete() {
        let server = test_server();
        let alice = register_and_bearer(&server, "alice3").await;
        let user_id = whoami(&server, &alice).await;
        let room_id = "!tagroom:localhost";

        server
            .put(&format!(
                "/_matrix/client/v3/user/{user_id}/rooms/{room_id}/tags/m.favourite"
            ))
            .add_header(alice.0.clone(), alice.1.clone())
            .json(&json!({ "order": 0.5 }))
            .await
            .assert_status_ok();

        let tags: Value = server
            .get(&format!(
                "/_matrix/client/v3/user/{user_id}/rooms/{room_id}/tags"
            ))
            .add_header(alice.0.clone(), alice.1.clone())
            .await
            .json();
        assert_eq!(
            tags["tags"]["m.favourite"]["order"].as_f64(),
            Some(0.5),
            "GET tags must reflect the PUT tag; got {tags}"
        );

        server
            .delete(&format!(
                "/_matrix/client/v3/user/{user_id}/rooms/{room_id}/tags/m.favourite"
            ))
            .add_header(alice.0.clone(), alice.1.clone())
            .await
            .assert_status_ok();

        let tags_after: Value = server
            .get(&format!(
                "/_matrix/client/v3/user/{user_id}/rooms/{room_id}/tags"
            ))
            .add_header(alice.0.clone(), alice.1.clone())
            .await
            .json();
        assert!(
            tags_after["tags"]
                .as_object()
                .map(|m| m.is_empty())
                .unwrap_or(false),
            "tag must be gone after DELETE; got {tags_after}"
        );

        // Idempotent: deleting an already-absent tag still returns 200.
        server
            .delete(&format!(
                "/_matrix/client/v3/user/{user_id}/rooms/{room_id}/tags/m.favourite"
            ))
            .add_header(alice.0.clone(), alice.1.clone())
            .await
            .assert_status_ok();
    }

    // cannot_access_another_users_account_data:start
    //   purpose: alice's token must not grant read/write access to bob's account
    //            data, even for a syntactically valid {userId} path param.
    // cannot_access_another_users_account_data:end
    #[tokio::test]
    async fn cannot_access_another_users_account_data() {
        let server = test_server();
        let alice = register_and_bearer(&server, "alice4").await;
        let bob = register_and_bearer(&server, "bob4").await;
        let bob_user_id = whoami(&server, &bob).await;

        let resp = server
            .put(&format!(
                "/_matrix/client/v3/user/{bob_user_id}/account_data/m.direct"
            ))
            .add_header(alice.0.clone(), alice.1.clone())
            .json(&json!({ "hijacked": true }))
            .await;

        assert_ne!(
            resp.status_code().as_u16(),
            200,
            "alice must not be able to write bob's account data via bob's userId path param"
        );
    }
}
