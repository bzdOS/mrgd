// START_AI_HEADER
// MODULE: matrix-hs/src/as_socket_test.rs
// PURPOSE: Integration tests for the application-service agent socket
//          (ROADMAP Phase 3 item 3, docs/AGENT-USE-CASES.md Case 2):
//            - POST /register with an AS Bearer → UIA-free account creation
//              under the AS namespace only
//            - POST /login type=m.login.application_service → passwordless
//              per-device session minting (workers-as-devices)
//          Negative paths: bad token, out-of-namespace target, password login
//          on an AS-created account, AS login when no AS is configured.
// DEPENDENCIES: axum-test, serde_json, matrix_hs::{router, AppState, state::AppServiceConfig}
// END_AI_HEADER

#[cfg(test)]
mod tests {
    use crate::state::AppServiceConfig;
    use crate::{router, AppState};
    use axum_test::TestServer;
    use serde_json::{json, Value};

    // as_server:start
    //   purpose: TestServer with an AS configured: token "test-as-token", prefix "agent_".
    //            Injected via with_appservice — never process env (parallel-test race).
    //   input:  none
    //   output: (TestServer, token, prefix)
    //   sideEffects: none
    // as_server:end
    fn as_server() -> (TestServer, &'static str, &'static str) {
        let state = AppState::with_appservice(
            AppState::new(),
            AppServiceConfig {
                token: "test-as-token".to_string(),
                prefix: "agent_".to_string(),
            },
        );
        let app = router(state);
        (TestServer::new(app), "test-as-token", "agent_")
    }

    // register_as_user:start
    //   purpose: Helper — AS-register a user, return the minted access_token.
    //   input:  server, bearer, username
    //   output: (status, body)
    //   sideEffects: creates the account on success
    // register_as_user:end
    async fn register_as(server: &TestServer, bearer: &str, username: &str) -> (u16, Value) {
        let resp = server
            .post("/_matrix/client/v3/register")
            .add_header(axum::http::HeaderName::from_static("authorization"),
                axum::http::HeaderValue::from_str(&format!("Bearer {bearer}")).unwrap())
            .json(&json!({ "username": username }))
            .await;
        (resp.status_code().as_u16(), resp.json())
    }

    // test:as_register_no_uia:start
    //   purpose: The core ergonomics claim — ONE request creates the account
    //            and returns a usable token. No 401, no session dance.
    //   input:  valid AS bearer, in-namespace username
    //   output: 200 with user_id + access_token + device_id
    //   sideEffects: none
    // test:as_register_no_uia:end
    #[tokio::test]
    async fn as_register_no_uia() {
        let (server, tok, prefix) = as_server();
        let (status, body) = register_as(&server, tok, &format!("{prefix}worker1")).await;

        assert_eq!(status, 200, "AS register must succeed in one call: {body}");
        assert_eq!(
            body["user_id"], "@agent_worker1:localhost",
            "user_id must reflect the requested localpart"
        );
        assert!(
            body["access_token"].as_str().unwrap_or("").starts_with("mxt_"),
            "register must mint a token: {body}"
        );
        assert!(
            body["device_id"].as_str().is_some(),
            "register must return a device_id: {body}"
        );

        // The minted token authenticates as the new user.
        let who = server
            .get("/_matrix/client/v3/account/whoami")
            .add_header(
                axum::http::HeaderName::from_static("authorization"),
                axum::http::HeaderValue::from_str(&format!(
                    "Bearer {}",
                    body["access_token"].as_str().unwrap()
                ))
                .unwrap(),
            )
            .await;
        assert_eq!(who.status_code().as_u16(), 200);
        let wv: Value = who.json();
        assert_eq!(wv["user_id"], "@agent_worker1:localhost");
    }

    // test:as_register_bad_token:start
    //   purpose: An invalid AS bearer gets 403, not a UIA challenge — an AS
    //            client cannot answer a challenge, and the caller must not
    //            learn anything past "forbidden".
    //   input:  wrong bearer
    //   output: 403 M_FORBIDDEN
    //   sideEffects: none
    // test:as_register_bad_token:end
    #[tokio::test]
    async fn as_register_bad_token() {
        let (server, _tok, prefix) = as_server();
        let (status, body) = register_as(&server, "wrong-token", &format!("{prefix}x")).await;
        assert_eq!(status, 403, "bad AS token must be 403: {body}");
        assert_eq!(body["errcode"], "M_FORBIDDEN");
    }

    // test:as_register_namespace_enforced:start
    //   purpose: A valid AS token must NOT be a shortcut to arbitrary
    //            usernames — only the configured prefix is registerable.
    //   input:  valid bearer, out-of-namespace username
    //   output: 403 M_EXCLUSIVE
    //   sideEffects: none
    // test:as_register_namespace_enforced:end
    #[tokio::test]
    async fn as_register_namespace_enforced() {
        let (server, tok, _p) = as_server();
        let (status, body) = register_as(&server, tok, "humanaccount").await;
        assert_eq!(status, 403, "out-of-namespace register must fail: {body}");
        assert_eq!(body["errcode"], "M_EXCLUSIVE");
    }

    // test:as_login_passwordless_per_device:start
    //   purpose: Case 2's workers-as-devices — the same tenant account logs
    //            in repeatedly with different device_ids, no password ever,
    //            and each session carries its own device.
    //   input:  AS-registered account + two AS logins (explicit devices)
    //   output: both 200; distinct tokens; whoami shows the right user; the
    //           device_ids round-trip in the responses
    //   sideEffects: none
    // test:as_login_passwordless_per_device:end
    #[tokio::test]
    async fn as_login_passwordless_per_device() {
        let (server, tok, prefix) = as_server();
        let name = format!("{prefix}fleet");
        let (_, reg) = register_as(&server, tok, &name).await;
        assert!(reg["access_token"].as_str().is_some());

        let mut tokens = Vec::new();
        for dev in ["WORKER-A", "WORKER-B"] {
            let resp = server
                .post("/_matrix/client/v3/login")
                .add_header(axum::http::HeaderName::from_static("authorization"),
                axum::http::HeaderValue::from_str(&format!("Bearer {tok}")).unwrap())
                .json(&json!({
                    "type": "m.login.application_service",
                    "user_id": "@agent_fleet:localhost",
                    "device_id": dev,
                }))
                .await;
            let lv: Value = resp.json();
            assert_eq!(resp.status_code().as_u16(), 200, "AS login: {lv}");
            let body: Value = resp.json();
            assert_eq!(body["device_id"], dev, "session must carry the worker's device");
            tokens.push(body["access_token"].as_str().unwrap().to_string());
        }
        assert_ne!(tokens[0], tokens[1], "each worker session is its own token");

        // Both tokens authenticate as the same tenant account.
        for t in &tokens {
            let who = server
                .get("/_matrix/client/v3/account/whoami")
                .add_header(
                    axum::http::HeaderName::from_static("authorization"),
                    axum::http::HeaderValue::from_str(&format!("Bearer {t}")).unwrap(),
                )
                .await;
            assert_eq!(who.status_code().as_u16(), 200);
            let wv: Value = who.json();
            assert_eq!(wv["user_id"], "@agent_fleet:localhost");
        }
    }

    // test:as_login_rejects_out_of_namespace:start
    //   purpose: The namespace rule applies on login too — a valid AS token
    //            must not mint sessions for human accounts (password bypass).
    //   input:  human account registered normally + AS login targeting it
    //   output: 403
    //   sideEffects: none
    // test:as_login_rejects_out_of_namespace:end
    #[tokio::test]
    async fn as_login_rejects_out_of_namespace() {
        let (server, tok, _p) = as_server();

        // Create a human account the normal way (UIA).
        let ch = server
            .post("/_matrix/client/v3/register")
            .json(&json!({ "username": "human1", "password": "pw" }))
            .await;
        let cv: Value = ch.json();
        let sess = cv["session"].as_str().unwrap().to_string();
        server
            .post("/_matrix/client/v3/register")
            .json(&json!({
                "username": "human1", "password": "pw",
                "auth": { "type": "m.login.dummy", "session": sess }
            }))
            .await;

        let resp = server
            .post("/_matrix/client/v3/login")
            .add_header(axum::http::HeaderName::from_static("authorization"),
                axum::http::HeaderValue::from_str(&format!("Bearer {tok}")).unwrap())
            .json(&json!({
                "type": "m.login.application_service",
                "user_id": "human1",
            }))
            .await;
        assert_eq!(
            resp.status_code().as_u16(),
            403,
            "AS must not mint sessions outside its namespace"
        );
    }

    // test:as_account_no_password_login:start
    //   purpose: AS-created accounts are un-loginable by password — the hash
    //            is a random value nobody can present. This is what makes the
    //            AS token the ONLY way in.
    //   input:  password login against an AS-created account (several guesses)
    //   output: 403 for every guess
    //   sideEffects: none
    // test:as_account_no_password_login:end
    #[tokio::test]
    async fn as_account_no_password_login() {
        let (server, tok, prefix) = as_server();
        let (_, reg) = register_as(&server, tok, &format!("{prefix}locked")).await;
        assert!(reg["access_token"].as_str().is_some(), "register ok: {reg}");

        for guess in ["", "password", "test-as-token", "as_"] {
            let resp = server
                .post("/_matrix/client/v3/login")
                .json(&json!({
                    "type": "m.login.password",
                    "user": "agent_locked",
                    "password": guess,
                }))
                .await;
            assert_eq!(
                resp.status_code().as_u16(),
                403,
                "guess {guess:?} must not authenticate against an AS account"
            );
        }
    }

    // test:as_login_disabled_without_config:start
    //   purpose: With no AS configured, m.login.application_service is simply
    //            not a login type — it falls to the password path and fails
    //            as unknown user. No capability exists to abuse.
    //   input:  plain server, AS-style login
    //   output: 403
    //   sideEffects: none
    // test:as_login_disabled_without_config:end
    #[tokio::test]
    async fn as_login_disabled_without_config() {
        let state = AppState::new();
        let app = router(state);
        let server = TestServer::new(app);

        let resp = server
            .post("/_matrix/client/v3/login")
            .add_header(axum::http::HeaderName::from_static("authorization"),
                axum::http::HeaderValue::from_str("Bearer anything").unwrap())
            .json(&json!({
                "type": "m.login.application_service",
                "user_id": "@someone:localhost",
            }))
            .await;
        assert_eq!(resp.status_code().as_u16(), 403);

        // And a bearer on /register with no AS configured changes nothing:
        // the UIA challenge still comes back (header ignored).
        let resp = server
            .post("/_matrix/client/v3/register")
            .add_header(axum::http::HeaderName::from_static("authorization"),
                axum::http::HeaderValue::from_str("Bearer anything").unwrap())
            .json(&json!({ "username": "x1" }))
            .await;
        assert_eq!(resp.status_code().as_u16(), 401, "UIA challenge unchanged");
    }

    // test:as_register_respects_invite_gate:start
    //   purpose: The invite gate (internal-task) must NOT be bypassable by AS-STYLE
    //            calls — but a VALID AS token legitimately skips it. What is
    //            tested here is the boundary: a server with BOTH an invite
    //            secret and an AS still serves the AS path in one call, while
    //            a normal caller still needs the secret.
    //   input:  server with both gates configured
    //   output: AS register 200 without registration_secret; normal register
    //           403 without it
    //   sideEffects: none
    // test:as_register_respects_invite_gate:end
    #[tokio::test]
    async fn as_register_respects_invite_gate() {
        let state = AppState::with_appservice(
            AppState::with_registration_shared_secret(AppState::new(), "invite-secret".to_string()),
            AppServiceConfig {
                token: "test-as-token".to_string(),
                prefix: "agent_".to_string(),
            },
        );
        let app = router(state);
        let server = TestServer::new(app);

        // Normal caller without the invite secret → 403.
        let resp = server
            .post("/_matrix/client/v3/register")
            .json(&json!({ "username": "plainuser" }))
            .await;
        assert_eq!(resp.status_code().as_u16(), 403);

        // AS caller: the token IS the authorisation — no invite secret needed.
        let (status, body) = register_as(&server, "test-as-token", "agent_both").await;
        assert_eq!(status, 200, "AS path skips the invite gate: {body}");
    }
}
