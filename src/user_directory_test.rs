// START_AI_HEADER
// MODULE: matrix-hs/src/user_directory_test.rs
// PURPOSE: Regression test for POST /_matrix/client/v3/user_directory/search.
//          Reproduces a live bug: this endpoint did not exist at all, so
//          FluffyChat's "Invite contact" typeahead got a 404 (surfaced to the
//          user as an "Unrecognized request" toast) and never showed any
//          results — silently blocking the invite flow entirely (nothing to
//          tap to confirm an invite without a search result).
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

    #[tokio::test]
    async fn search_finds_registered_user_by_substring() {
        let server = test_server();
        let auth = register_and_bearer(&server, "harold").await;
        register_and_bearer(&server, "invitetarget").await;

        let resp: Value = server
            .post("/_matrix/client/v3/user_directory/search")
            .add_header(auth.0.clone(), auth.1.clone())
            .json(&json!({ "search_term": "invitetarg" }))
            .await
            .json();

        let results = resp["results"].as_array().expect("results array");
        assert!(
            results
                .iter()
                .any(|r| r["user_id"] == "@invitetarget:localhost"),
            "must find invitetarget by substring; got {resp:?}"
        );
    }

    #[tokio::test]
    async fn search_without_token_returns_401() {
        let server = test_server();
        let resp: Value = server
            .post("/_matrix/client/v3/user_directory/search")
            .json(&json!({ "search_term": "x" }))
            .await
            .json();
        assert_eq!(resp["errcode"].as_str(), Some("M_UNKNOWN_TOKEN"));
    }

    #[tokio::test]
    async fn search_no_match_returns_empty_results() {
        let server = test_server();
        let auth = register_and_bearer(&server, "iris").await;

        let resp: Value = server
            .post("/_matrix/client/v3/user_directory/search")
            .add_header(auth.0.clone(), auth.1.clone())
            .json(&json!({ "search_term": "nobody_matches_this_zzz" }))
            .await
            .json();

        assert_eq!(resp["results"].as_array().map(|a| a.len()), Some(0));
    }
}
