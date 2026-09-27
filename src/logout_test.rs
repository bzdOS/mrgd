// START_AI_HEADER
// MODULE: matrix-hs/src/logout_test.rs
// PURPOSE: Regression test for POST /_matrix/client/v3/logout and /logout/all.
//          Reproduces a live bug: these endpoints did not exist at all (no route
//          registered), so a real client's logout action (FluffyChat, and every
//          other real Matrix client) got a 404 instead of the spec-shaped 200 {}.
//          Tests prove: an authenticated logout call returns 200 {}; an
//          unauthenticated call returns 401 M_UNKNOWN_TOKEN.
// DEPENDENCIES: axum-test, serde_json, matrix_hs::{router, AppState}
// END_AI_HEADER

#[cfg(test)]
mod tests {
    use crate::{router, AppState};
    use axum_test::TestServer;
    use serde_json::{json, Value};

    fn test_server() -> TestServer {
        let state = AppState::new();
        let app = router(state);
        TestServer::new(app)
    }

    async fn register_and_token(server: &TestServer, username: &str) -> String {
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
        reg["access_token"]
            .as_str()
            .unwrap_or_else(|| panic!("no token for {username}; got {reg}"))
            .to_string()
    }

    #[tokio::test]
    async fn logout_with_valid_token_returns_ok() {
        let server = test_server();
        let token = register_and_token(&server, "frank").await;

        let resp = server
            .post("/_matrix/client/v3/logout")
            .add_header(
                axum::http::HeaderName::from_static("authorization"),
                axum::http::HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
            )
            .await;
        resp.assert_status_ok();
    }

    #[tokio::test]
    async fn logout_all_with_valid_token_returns_ok() {
        let server = test_server();
        let token = register_and_token(&server, "grace").await;

        let resp = server
            .post("/_matrix/client/v3/logout/all")
            .add_header(
                axum::http::HeaderName::from_static("authorization"),
                axum::http::HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
            )
            .await;
        resp.assert_status_ok();
    }

    #[tokio::test]
    async fn logout_without_token_returns_401() {
        let server = test_server();
        let resp: Value = server.post("/_matrix/client/v3/logout").await.json();
        assert_eq!(resp["errcode"].as_str(), Some("M_UNKNOWN_TOKEN"));
    }
}
