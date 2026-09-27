// START_AI_HEADER
// MODULE: matrix-hs/src/account_password_test.rs
// PURPOSE: Integration tests for POST /_matrix/client/v3/account/password
//          (routes/account_password.rs) — the UIA-gated password-change endpoint
//          that replaces the previously-disabled m.change_password capability.
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

    async fn register_and_bearer(
        server: &TestServer,
        username: &str,
        password: &str,
    ) -> (HeaderName, HeaderValue) {
        let ch: Value = server
            .post("/_matrix/client/v3/register")
            .json(&json!({ "username": username, "password": password }))
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
                "password": password,
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

    async fn login_ok(server: &TestServer, username: &str, password: &str) -> bool {
        server
            .post("/_matrix/client/v3/login")
            .json(&json!({ "type": "m.login.password", "user": username, "password": password }))
            .await
            .status_code()
            == 200
    }

    // capabilities_advertises_change_password_enabled:start
    //   purpose: GET /capabilities must now report m.change_password.enabled:true.
    // capabilities_advertises_change_password_enabled:end
    #[tokio::test]
    async fn capabilities_advertises_change_password_enabled() {
        let server = test_server();
        let alice = register_and_bearer(&server, "alice_caps", "oldpw123").await;
        let caps: Value = server
            .get("/_matrix/client/v3/capabilities")
            .add_header(alice.0.clone(), alice.1.clone())
            .await
            .json();
        assert_eq!(
            caps["capabilities"]["m.change_password"]["enabled"].as_bool(),
            Some(true),
            "got {caps}"
        );
    }

    // no_auth_returns_uia_challenge:start
    //   purpose: POST /account/password with no auth field must return a 401 UIA
    //            challenge with stage m.login.password.
    // no_auth_returns_uia_challenge:end
    #[tokio::test]
    async fn no_auth_returns_uia_challenge() {
        let server = test_server();
        let alice = register_and_bearer(&server, "alice_uia", "oldpw123").await;

        let resp = server
            .post("/_matrix/client/v3/account/password")
            .add_header(alice.0.clone(), alice.1.clone())
            .json(&json!({ "new_password": "newpw456" }))
            .await;
        assert_eq!(resp.status_code().as_u16(), 401);
        let body: Value = resp.json();
        assert_eq!(
            body["flows"][0]["stages"][0].as_str(),
            Some("m.login.password")
        );
        assert!(body["session"].as_str().is_some());
    }

    // wrong_current_password_rejected:start
    //   purpose: Completing the UIA flow with the WRONG current password must be
    //            rejected (403) and must NOT change the stored password.
    // wrong_current_password_rejected:end
    #[tokio::test]
    async fn wrong_current_password_rejected() {
        let server = test_server();
        let alice = register_and_bearer(&server, "alice_wrong", "oldpw123").await;

        let challenge: Value = server
            .post("/_matrix/client/v3/account/password")
            .add_header(alice.0.clone(), alice.1.clone())
            .json(&json!({ "new_password": "newpw456" }))
            .await
            .json();
        let session = challenge["session"].as_str().expect("session").to_string();

        let resp = server
            .post("/_matrix/client/v3/account/password")
            .add_header(alice.0.clone(), alice.1.clone())
            .json(&json!({
                "new_password": "newpw456",
                "auth": { "type": "m.login.password", "session": session, "password": "TOTALLY_WRONG" }
            }))
            .await;
        assert_eq!(resp.status_code().as_u16(), 403);

        // Old password must still work; new one must NOT.
        assert!(
            login_ok(&server, "alice_wrong", "oldpw123").await,
            "old password must still work"
        );
        assert!(
            !login_ok(&server, "alice_wrong", "newpw456").await,
            "new password must NOT have been set"
        );
    }

    // full_change_flow_updates_password:start
    //   purpose: The complete UIA flow (correct current password + new_password)
    //            must actually change the password: old password stops working,
    //            new password works for a fresh /login.
    // full_change_flow_updates_password:end
    #[tokio::test]
    async fn full_change_flow_updates_password() {
        let server = test_server();
        let alice = register_and_bearer(&server, "alice_change", "oldpw123").await;

        let challenge: Value = server
            .post("/_matrix/client/v3/account/password")
            .add_header(alice.0.clone(), alice.1.clone())
            .json(&json!({ "new_password": "newpw456" }))
            .await
            .json();
        let session = challenge["session"].as_str().expect("session").to_string();

        let resp = server
            .post("/_matrix/client/v3/account/password")
            .add_header(alice.0.clone(), alice.1.clone())
            .json(&json!({
                "new_password": "newpw456",
                "auth": { "type": "m.login.password", "session": session, "password": "oldpw123" }
            }))
            .await;
        resp.assert_status_ok();

        assert!(
            !login_ok(&server, "alice_change", "oldpw123").await,
            "old password must stop working"
        );
        assert!(
            login_ok(&server, "alice_change", "newpw456").await,
            "new password must now work"
        );
    }

    // missing_bearer_token_rejected:start
    //   purpose: No Authorization header at all must be rejected (401
    //            M_UNKNOWN_TOKEN), never silently proceeding as some anonymous
    //            or wrong account.
    // missing_bearer_token_rejected:end
    #[tokio::test]
    async fn missing_bearer_token_rejected() {
        let server = test_server();
        let resp = server
            .post("/_matrix/client/v3/account/password")
            .json(&json!({ "new_password": "whatever" }))
            .await;
        assert_eq!(resp.status_code().as_u16(), 401);
        let body: Value = resp.json();
        assert_eq!(body["errcode"].as_str(), Some("M_UNKNOWN_TOKEN"));
    }
}
