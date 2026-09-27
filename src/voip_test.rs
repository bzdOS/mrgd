// START_AI_HEADER
// MODULE: matrix-hs/src/voip_test.rs
// PURPOSE: Integration tests for GET /_matrix/client/v3/voip/turnServer
//          (routes/voip.rs) — the real TURN credential endpoint that replaced
//          the empty stub.
// DEPENDENCIES: axum-test, hmac, sha1, base64, matrix_hs::{router, AppState, state::TurnConfig}
// END_AI_HEADER

#[cfg(test)]
mod tests {
    use crate::{router, state::TurnConfig, AppState};
    use axum::http::{HeaderName, HeaderValue};
    use axum_test::TestServer;
    use base64::{engine::general_purpose::STANDARD, Engine as _};
    use hmac::{Hmac, Mac};
    use serde_json::{json, Value};
    use sha1::Sha1;

    type HmacSha1 = Hmac<Sha1>;

    async fn register_and_bearer(server: &TestServer, username: &str) -> (HeaderName, HeaderValue) {
        let ch: Value = server
            .post("/_matrix/client/v3/register")
            .json(&json!({ "username": username, "password": "pw" }))
            .await
            .json();
        let sess = ch["session"].as_str().expect("session").to_string();
        let reg: Value = server
            .post("/_matrix/client/v3/register")
            .json(&json!({
                "username": username, "password": "pw",
                "auth": { "type": "m.login.dummy", "session": sess }
            }))
            .await
            .json();
        let token = reg["access_token"].as_str().expect("token").to_string();
        (
            HeaderName::from_static("authorization"),
            HeaderValue::from_str(&format!("Bearer {token}")).expect("header"),
        )
    }

    // unconfigured_returns_empty:start
    //   purpose: With no TurnConfig, the endpoint returns {} (no VoIP), unchanged
    //            from the old stub — an authenticated caller just gets an empty object.
    // unconfigured_returns_empty:end
    #[tokio::test]
    async fn unconfigured_returns_empty() {
        let server = TestServer::new(router(AppState::new()));
        let auth = register_and_bearer(&server, "alice_noturn").await;
        let resp = server
            .get("/_matrix/client/v3/voip/turnServer")
            .add_header(auth.0.clone(), auth.1.clone())
            .await;
        resp.assert_status_ok();
        let body: Value = resp.json();
        assert_eq!(
            body,
            json!({}),
            "unconfigured TURN must return empty object; got {body}"
        );
    }

    // missing_token_rejected:start
    //   purpose: No Authorization header → 401 (the caller's MXID is bound into the
    //            TURN username, so an anonymous request has no identity to mint).
    // missing_token_rejected:end
    #[tokio::test]
    async fn missing_token_rejected() {
        let cfg = TurnConfig {
            uris: vec!["turn:turn.example.com:3478?transport=udp".to_string()],
            shared_secret: "s3cr3t".to_string(),
            ttl_secs: 3600,
        };
        let state = AppState::with_turn_config(AppState::new(), cfg);
        let server = TestServer::new(router(state));
        let resp = server.get("/_matrix/client/v3/voip/turnServer").await;
        assert_eq!(resp.status_code().as_u16(), 401);
    }

    // configured_returns_valid_ephemeral_credentials:start
    //   purpose: With a TurnConfig, the endpoint returns uris + ttl + a username of
    //            the form "<expiry>:<mxid>" whose password is exactly
    //            base64(HMAC-SHA1(secret, username)) — verified by recomputing it.
    // configured_returns_valid_ephemeral_credentials:end
    #[tokio::test]
    async fn configured_returns_valid_ephemeral_credentials() {
        let secret = "test-static-auth-secret";
        let cfg = TurnConfig {
            uris: vec![
                "turn:turn.example.com:3478?transport=udp".to_string(),
                "turns:turn.example.com:5349?transport=tcp".to_string(),
            ],
            shared_secret: secret.to_string(),
            ttl_secs: 3600,
        };
        let state = AppState::with_turn_config(AppState::new(), cfg);
        let server = TestServer::new(router(state));
        let auth = register_and_bearer(&server, "alice_turn").await;

        let body: Value = server
            .get("/_matrix/client/v3/voip/turnServer")
            .add_header(auth.0.clone(), auth.1.clone())
            .await
            .json();

        assert_eq!(body["ttl"].as_u64(), Some(3600), "got {body}");
        assert_eq!(
            body["uris"].as_array().map(|a| a.len()),
            Some(2),
            "got {body}"
        );

        let username = body["username"].as_str().expect("username");
        let password = body["password"].as_str().expect("password");

        // username = "<expiry>:<mxid>"; the mxid tail must be the caller's id.
        let (expiry_str, mxid) = username
            .split_once(':')
            .expect("username has expiry:mxid form");
        assert!(
            expiry_str.parse::<u64>().is_ok(),
            "expiry must be numeric; got {username}"
        );
        assert!(
            mxid.starts_with("@alice_turn:"),
            "username must bind caller mxid; got {username}"
        );

        // password must be exactly base64(HMAC-SHA1(secret, username)).
        let mut mac = HmacSha1::new_from_slice(secret.as_bytes()).expect("hmac");
        mac.update(username.as_bytes());
        let expected = STANDARD.encode(mac.finalize().into_bytes());
        assert_eq!(
            password, expected,
            "password must match the TURN REST HMAC-SHA1 scheme"
        );
    }
}
