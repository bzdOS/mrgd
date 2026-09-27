// START_AI_HEADER
// MODULE: matrix-hs/src/register_test.rs
// PURPOSE: Integration tests for the UIA registration flow (POST /register,
//          GET /register/available) and the login password-validation behaviour
//          introduced in Stage 2.5.
//
//          Tests replay the real HTTP registration sequence:
//            1. Initial POST /register (no auth) → 401 UIA challenge
//            2. POST /register with m.login.dummy + session → 200 success
//            3. Duplicate registration → 400 M_USER_IN_USE
//            4. GET /register/available?username=taken → 400 M_USER_IN_USE
//               GET /register/available?username=free  → 200 {"available":true}
//            5. Login with correct password after registration → 200
//               Login with wrong password → 403 M_FORBIDDEN
//            6. Login as unregistered user → 200 (lenient fallback preserved)
//            7. ?kind=guest → 403 M_FORBIDDEN
//            8. Barrier injection tests (new, 2026-07-05):
//               barrier_claimed_proceeds  — inject MemClaimStore; register alice → Claimed → 200
//               barrier_rejected_returns_user_in_use — inject with alice pre-claimed; second
//                                           register alice → Rejected → 400 M_USER_IN_USE
//               existing tests run with barrier_store=None (local path, unchanged)
//
// DEPENDENCIES: axum-test, serde_json, matrix_hs::{router, AppState}, crate::substrate::barrier
// END_AI_HEADER

#[cfg(test)]
mod tests {
    use crate::{router, AppState};
    use axum_test::TestServer;
    use crate::substrate::barrier::MemClaimStore;
    use serde_json::{json, Value};
    use std::sync::Arc;

    // reg_test_server:start
    //   purpose: Build a fresh TestServer backed by a new in-memory AppState.
    //   input:  none
    //   output: TestServer
    //   sideEffects: none
    // reg_test_server:end
    fn test_server() -> TestServer {
        let state = AppState::new();
        let app = router(state);
        TestServer::new(app)
    }

    // test:initial_register_returns_401_uia_challenge:start
    //   purpose: Initial POST /register (no auth field) must return HTTP 401
    //            with a UIA challenge body containing flows, params, and session.
    //   input:  POST /register with username + password but no auth
    //   output: HTTP 401; body.flows contains m.login.dummy stage; body.session present
    //   sideEffects: UIA session created in AppState
    // test:initial_register_returns_401_uia_challenge:end
    #[tokio::test]
    async fn initial_register_returns_401_uia_challenge() {
        let server = test_server();

        let resp = server
            .post("/_matrix/client/v3/register")
            .json(&json!({
                "username": "newbie",
                "password": "pw"
            }))
            .await;

        resp.assert_status(axum::http::StatusCode::UNAUTHORIZED);
        let body: Value = resp.json();

        let flows = body["flows"].as_array().expect("flows array");
        let has_dummy = flows.iter().any(|f| {
            f["stages"]
                .as_array()
                .map(|stages| stages.iter().any(|s| s.as_str() == Some("m.login.dummy")))
                .unwrap_or(false)
        });
        assert!(
            has_dummy,
            "flows must contain m.login.dummy stage; got {body}"
        );

        let session = body["session"].as_str().expect("session string");
        assert!(
            session.starts_with("uia_"),
            "session must have uia_ prefix; got {session}"
        );
    }

    // test:full_registration_succeeds:start
    //   purpose: Two-step UIA registration:
    //              Step 1: POST /register (no auth) → 401, extract session.
    //              Step 2: POST /register with m.login.dummy + session → 200.
    //            Response must include user_id, signed access_token (mxt_ prefix),
    //            device_id, home_server.
    //   input:  username "newbie", password "pw"
    //   output: user_id = "@newbie:localhost", access_token starts with "mxt_"
    //   sideEffects: user inserted into AppState
    // test:full_registration_succeeds:end
    #[tokio::test]
    async fn full_registration_succeeds() {
        let server = test_server();

        // Step 1: challenge
        let challenge_resp = server
            .post("/_matrix/client/v3/register")
            .json(&json!({ "username": "newbie", "password": "pw" }))
            .await;
        challenge_resp.assert_status(axum::http::StatusCode::UNAUTHORIZED);
        let challenge_body: Value = challenge_resp.json();
        let session = challenge_body["session"]
            .as_str()
            .expect("session")
            .to_string();

        // Step 2: complete
        let reg_resp = server
            .post("/_matrix/client/v3/register")
            .json(&json!({
                "username": "newbie",
                "password": "pw",
                "auth": {
                    "type":    "m.login.dummy",
                    "session": session
                }
            }))
            .await;
        reg_resp.assert_status_ok();
        let body: Value = reg_resp.json();

        assert_eq!(
            body["user_id"].as_str(),
            Some("@newbie:localhost"),
            "user_id"
        );
        let token = body["access_token"].as_str().expect("access_token present");
        assert!(
            token.starts_with("mxt_"),
            "access_token must be a signed mxt_ token; got {token}"
        );
        assert!(body["device_id"].as_str().is_some(), "device_id present");
        assert!(
            body["home_server"].as_str().is_some(),
            "home_server present"
        );
    }

    // test:duplicate_registration_rejected:start
    //   purpose: Registering the same username twice returns 400 M_USER_IN_USE.
    //   input:  register "newbie" twice (each with a fresh UIA session)
    //   output: second attempt returns 400 M_USER_IN_USE
    //   sideEffects: user "newbie" inserted on first registration
    // test:duplicate_registration_rejected:end
    #[tokio::test]
    async fn duplicate_registration_rejected() {
        let server = test_server();

        // ── Helper closure that does a full two-step registration ──────────────
        // (inlined as sequential awaits because async closures are tricky in tests)

        // First registration — must succeed
        let c1: Value = server
            .post("/_matrix/client/v3/register")
            .json(&json!({ "username": "newbie", "password": "pw" }))
            .await
            .json();
        let s1 = c1["session"].as_str().expect("session 1").to_string();

        let r1 = server
            .post("/_matrix/client/v3/register")
            .json(&json!({
                "username": "newbie",
                "password": "pw",
                "auth": { "type": "m.login.dummy", "session": s1 }
            }))
            .await;
        r1.assert_status_ok();

        // Second registration — must fail
        let c2: Value = server
            .post("/_matrix/client/v3/register")
            .json(&json!({ "username": "newbie", "password": "other" }))
            .await
            .json();
        let s2 = c2["session"].as_str().expect("session 2").to_string();

        let r2 = server
            .post("/_matrix/client/v3/register")
            .json(&json!({
                "username": "newbie",
                "password": "other",
                "auth": { "type": "m.login.dummy", "session": s2 }
            }))
            .await;
        r2.assert_status(axum::http::StatusCode::BAD_REQUEST);
        let body: Value = r2.json();
        assert_eq!(
            body["errcode"].as_str(),
            Some("M_USER_IN_USE"),
            "errcode must be M_USER_IN_USE; got {body}"
        );
    }

    // test:availability_check:start
    //   purpose: GET /register/available returns correct status for taken vs free usernames.
    //            After registering "newbie":
    //              ?username=newbie → 400 M_USER_IN_USE
    //              ?username=free1  → 200 {"available":true}
    //   input:  register "newbie" first, then two availability queries
    //   output: see above
    //   sideEffects: user "newbie" registered in AppState
    // test:availability_check:end
    #[tokio::test]
    async fn availability_check() {
        let server = test_server();

        // Register "newbie"
        let c: Value = server
            .post("/_matrix/client/v3/register")
            .json(&json!({ "username": "newbie", "password": "pw" }))
            .await
            .json();
        let sess = c["session"].as_str().expect("session").to_string();
        server
            .post("/_matrix/client/v3/register")
            .json(&json!({
                "username": "newbie",
                "password": "pw",
                "auth": { "type": "m.login.dummy", "session": sess }
            }))
            .await
            .assert_status_ok();

        // Taken
        let taken = server
            .get("/_matrix/client/v3/register/available?username=newbie")
            .await;
        taken.assert_status(axum::http::StatusCode::BAD_REQUEST);
        let taken_body: Value = taken.json();
        assert_eq!(
            taken_body["errcode"].as_str(),
            Some("M_USER_IN_USE"),
            "taken errcode; got {taken_body}"
        );

        // Free
        let free = server
            .get("/_matrix/client/v3/register/available?username=free1")
            .await;
        free.assert_status_ok();
        let free_body: Value = free.json();
        assert_eq!(
            free_body["available"].as_bool(),
            Some(true),
            "free availability; got {free_body}"
        );
    }

    // test:login_validates_password_for_registered_user:start
    //   purpose: After registering "bob2"/"pw":
    //              POST /login with correct pw → 200, access_token starts with "mxt_"
    //              POST /login with WRONG pw   → 403 M_FORBIDDEN
    //   input:  register "bob2" first, then two login attempts
    //   output: see above
    //   sideEffects: user "bob2" registered in AppState
    // test:login_validates_password_for_registered_user:end
    #[tokio::test]
    async fn login_validates_password_for_registered_user() {
        let server = test_server();

        // Register bob2
        let c: Value = server
            .post("/_matrix/client/v3/register")
            .json(&json!({ "username": "bob2", "password": "pw" }))
            .await
            .json();
        let sess = c["session"].as_str().expect("session").to_string();
        server
            .post("/_matrix/client/v3/register")
            .json(&json!({
                "username": "bob2",
                "password": "pw",
                "auth": { "type": "m.login.dummy", "session": sess }
            }))
            .await
            .assert_status_ok();

        // Correct password
        let ok_resp = server
            .post("/_matrix/client/v3/login")
            .json(&json!({
                "type": "m.login.password",
                "identifier": { "type": "m.id.user", "user": "bob2" },
                "password": "pw"
            }))
            .await;
        ok_resp.assert_status_ok();
        let ok_body: Value = ok_resp.json();
        let ok_token = ok_body["access_token"]
            .as_str()
            .expect("access_token present");
        assert!(
            ok_token.starts_with("mxt_"),
            "access_token after correct login must be mxt_ token; got {ok_token}"
        );

        // Wrong password
        let err_resp = server
            .post("/_matrix/client/v3/login")
            .json(&json!({
                "type": "m.login.password",
                "identifier": { "type": "m.id.user", "user": "bob2" },
                "password": "WRONG"
            }))
            .await;
        err_resp.assert_status(axum::http::StatusCode::FORBIDDEN);
        let err_body: Value = err_resp.json();
        assert_eq!(
            err_body["errcode"].as_str(),
            Some("M_FORBIDDEN"),
            "errcode on wrong password; got {err_body}"
        );
    }

    // test:login_rejects_unregistered_user:start
    //   purpose: POST /login for an unregistered user "stranger" must return 403 M_FORBIDDEN.
    //            The lenient fallback was removed as part of AUTH-MINIMUM hardening — only
    //            users that went through the UIA registration flow can login.
    //   input:  POST /login as "stranger" (never registered)
    //   output: 403 M_FORBIDDEN
    //   sideEffects: none
    // test:login_rejects_unregistered_user:end
    #[tokio::test]
    async fn login_lenient_for_unregistered_user() {
        let server = test_server();

        let resp = server
            .post("/_matrix/client/v3/login")
            .json(&json!({
                "type": "m.login.password",
                "identifier": { "type": "m.id.user", "user": "stranger" },
                "password": "anything"
            }))
            .await;
        resp.assert_status(axum::http::StatusCode::FORBIDDEN);
        let body: Value = resp.json();
        assert_eq!(
            body["errcode"].as_str(),
            Some("M_FORBIDDEN"),
            "unregistered user login must return M_FORBIDDEN; got {body}"
        );
    }

    // test:guest_registration_forbidden:start
    //   purpose: POST /register?kind=guest must return 403 M_FORBIDDEN immediately.
    //            Guest registration is disabled in this homeserver.
    //   input:  POST /register?kind=guest
    //   output: 403 {"errcode":"M_FORBIDDEN",...}
    //   sideEffects: none
    // test:guest_registration_forbidden:end
    #[tokio::test]
    async fn guest_registration_forbidden() {
        let server = test_server();

        let resp = server
            .post("/_matrix/client/v3/register?kind=guest")
            .json(&json!({}))
            .await;

        resp.assert_status(axum::http::StatusCode::FORBIDDEN);
        let body: Value = resp.json();
        assert_eq!(
            body["errcode"].as_str(),
            Some("M_FORBIDDEN"),
            "errcode must be M_FORBIDDEN for guest; got {body}"
        );
    }

    // ── Barrier injection tests ───────────────────────────────────────────────

    // barrier_test_server:start
    //   purpose: Build a TestServer backed by AppState with a MemClaimStore injected.
    //            Proves register→barrier path without needing zenoh or a real KvStore.
    //   input:  none
    //   output: (TestServer, Arc<MemClaimStore>)
    //   sideEffects: none
    // barrier_test_server:end
    fn barrier_test_server() -> (TestServer, Arc<MemClaimStore>) {
        let mem_store = Arc::new(MemClaimStore::new());
        let state = AppState::new();
        let state = AppState::with_barrier_store(
            state,
            mem_store.clone() as Arc<dyn crate::substrate::barrier::ClaimStore + Send + Sync>,
        );
        let app = router(state);
        (TestServer::new(app), mem_store)
    }

    // test:barrier_claimed_proceeds:start
    //   purpose: With a MemClaimStore injected, registering "alice" results in barrier
    //            returning Claimed → the registration succeeds (200) and the user is
    //            present in the store.
    //            This proves the register→barrier path end-to-end without zenoh.
    //   input:  AppState with MemClaimStore; two-step UIA for "alice"
    //   output: 200 with user_id = "@alice:localhost"
    //   sideEffects: "alice" inserted into AppState.users; key "mx:username:alice" set in MemClaimStore
    // test:barrier_claimed_proceeds:end
    #[tokio::test]
    async fn barrier_claimed_proceeds() {
        let (server, _store) = barrier_test_server();

        // Step 1: challenge
        let c: Value = server
            .post("/_matrix/client/v3/register")
            .json(&json!({ "username": "alice", "password": "pw" }))
            .await
            .json();
        let sess = c["session"].as_str().expect("session").to_string();

        // Step 2: complete
        let resp = server
            .post("/_matrix/client/v3/register")
            .json(&json!({
                "username": "alice",
                "password": "pw",
                "auth": { "type": "m.login.dummy", "session": sess }
            }))
            .await;
        resp.assert_status_ok();
        let body: Value = resp.json();
        assert_eq!(
            body["user_id"].as_str(),
            Some("@alice:localhost"),
            "barrier-claimed registration must return correct user_id; got {body}"
        );
    }

    // test:barrier_rejected_returns_user_in_use:start
    //   purpose: With a MemClaimStore pre-seeded so "alice" is already claimed,
    //            a second registration attempt returns 400 M_USER_IN_USE (barrier Rejected).
    //            This proves the Rejected path in the barrier→register wiring.
    //   input:  AppState with MemClaimStore; "alice" pre-claimed in store; UIA for "alice"
    //   output: 400 M_USER_IN_USE
    //   sideEffects: none (local users map is NOT mutated because barrier rejects first)
    // test:barrier_rejected_returns_user_in_use:end
    #[tokio::test]
    async fn barrier_rejected_returns_user_in_use() {
        let (server, store) = barrier_test_server();

        // Pre-seed the MemClaimStore so "alice" is already claimed.
        // cas_claim returns Set on first call, AlreadySet on subsequent.
        // Call via the trait to reach the impl on MemClaimStore.
        use crate::substrate::barrier::ClaimStore as _;
        store
            .cas_claim("mx:username:alice", "@alice:localhost")
            .expect("pre-seed alice in MemClaimStore");

        // Now attempt to register "alice" — barrier returns Rejected.
        let c: Value = server
            .post("/_matrix/client/v3/register")
            .json(&json!({ "username": "alice", "password": "pw" }))
            .await
            .json();
        let sess = c["session"].as_str().expect("session").to_string();

        let resp = server
            .post("/_matrix/client/v3/register")
            .json(&json!({
                "username": "alice",
                "password": "pw",
                "auth": { "type": "m.login.dummy", "session": sess }
            }))
            .await;
        resp.assert_status(axum::http::StatusCode::BAD_REQUEST);
        let body: Value = resp.json();
        assert_eq!(
            body["errcode"].as_str(),
            Some("M_USER_IN_USE"),
            "barrier-rejected registration must return M_USER_IN_USE; got {body}"
        );
    }

    // test:deactivate_removes_user_and_rejects_token:start
    //   purpose: POST /account/deactivate with a valid token removes the caller's account
    //            from AppState.users and inserts the localpart into AppState.deactivated.
    //            After deactivation, GET /whoami with the same token returns 200 or 401
    //            (documented: HMAC token still cryptographically valid; user slot gone).
    //            Unknown/missing token → 401 M_UNKNOWN_TOKEN without modifying any state.
    //   input:  register "deact-user"; login → signed token; POST /deactivate; GET /whoami
    //   output: POST /deactivate → 200 {}; AppState.deactivated contains "deact-user"
    //   sideEffects: user removed from AppState.users; localpart in AppState.deactivated
    // test:deactivate_removes_user_and_rejects_token:end
    #[tokio::test]
    async fn deactivate_removes_user_and_rejects_token() {
        // Use AppState directly so we can inspect it after deactivation.
        let state = crate::AppState::new();
        let server = axum_test::TestServer::new(crate::router(state.clone()));

        // Step 1: register "deact-user".
        let c: Value = server
            .post("/_matrix/client/v3/register")
            .json(&json!({ "username": "deact-user", "password": "pw" }))
            .await
            .json();
        let sess = c["session"].as_str().expect("session").to_string();
        server
            .post("/_matrix/client/v3/register")
            .json(&json!({
                "username": "deact-user",
                "password": "pw",
                "auth": { "type": "m.login.dummy", "session": sess }
            }))
            .await
            .assert_status_ok();

        // Step 2: login to get a signed token.
        let login_resp = server
            .post("/_matrix/client/v3/login")
            .json(&json!({
                "type": "m.login.password",
                "identifier": { "type": "m.id.user", "user": "deact-user" },
                "password": "pw"
            }))
            .await;
        login_resp.assert_status_ok();
        let login_body: Value = login_resp.json();
        let token = login_body["access_token"]
            .as_str()
            .expect("access_token")
            .to_string();
        assert!(
            token.starts_with("mxt_"),
            "login must return mxt_ token; got {token}"
        );

        // Step 3: POST /account/deactivate — must succeed.
        let deact_resp = server
            .post("/_matrix/client/v3/account/deactivate")
            .add_header(
                axum::http::HeaderName::from_static("authorization"),
                axum::http::HeaderValue::from_str(&format!("Bearer {token}")).expect("hv"),
            )
            .json(&json!({}))
            .await;
        deact_resp.assert_status_ok();
        let deact_body: Value = deact_resp.json();
        assert_eq!(deact_body, json!({}), "deactivate must return empty object");

        // Step 4: user must be absent from AppState.users after deactivation.
        {
            let users = state.users.lock().expect("users lock");
            assert!(
                !users.contains_key("deact-user"),
                "deact-user must be absent from AppState.users after deactivation"
            );
        }

        // Step 5: localpart must be in AppState.deactivated.
        {
            let deactivated = state.deactivated.lock().expect("deactivated lock");
            assert!(
                deactivated.contains("deact-user"),
                "deact-user must be in AppState.deactivated after deactivation"
            );
        }

        // Step 6: unknown token → 401 M_UNKNOWN_TOKEN.
        let bad_resp = server
            .post("/_matrix/client/v3/account/deactivate")
            .add_header(
                axum::http::HeaderName::from_static("authorization"),
                axum::http::HeaderValue::from_str("Bearer bad_token_xyz").expect("hv"),
            )
            .json(&json!({}))
            .await;
        bad_resp.assert_status(axum::http::StatusCode::UNAUTHORIZED);
        let bad_body: Value = bad_resp.json();
        assert_eq!(
            bad_body["errcode"].as_str(),
            Some("M_UNKNOWN_TOKEN"),
            "invalid token must return M_UNKNOWN_TOKEN; got {bad_body}"
        );
    }

    // test:forgeable_tok_prefix_rejected_on_whoami:start
    //   purpose: SECURITY REGRESSION GUARD. The legacy forgeable "tok_<localpart>" bearer
    //            token must NOT authenticate on any authed endpoint. GET /account/whoami with
    //            "Authorization: Bearer tok_alice" → 401 M_UNKNOWN_TOKEN, and a garbage token
    //            → 401 M_UNKNOWN_TOKEN. Only signed mxt_ tokens (auth::sign_token) authenticate.
    //   input:  GET /account/whoami with "Bearer tok_alice"; then with "Bearer garbage.xyz"
    //   output: both → 401 {"errcode":"M_UNKNOWN_TOKEN"}
    //   sideEffects: none
    // test:forgeable_tok_prefix_rejected_on_whoami:end
    #[tokio::test]
    async fn forgeable_tok_prefix_rejected_on_whoami() {
        let server = test_server();

        // A forgeable "tok_<localpart>" token must be rejected — this is the exact backdoor
        // that AUTH-MINIMUM closes. No account is even registered; the point is the prefix
        // itself must never grant identity.
        let forged = server
            .get("/_matrix/client/v3/account/whoami")
            .add_header(
                axum::http::HeaderName::from_static("authorization"),
                axum::http::HeaderValue::from_str("Bearer tok_alice").expect("hv"),
            )
            .await;
        forged.assert_status(axum::http::StatusCode::UNAUTHORIZED);
        let forged_body: Value = forged.json();
        assert_eq!(
            forged_body["errcode"].as_str(),
            Some("M_UNKNOWN_TOKEN"),
            "Bearer tok_alice must be rejected with M_UNKNOWN_TOKEN; got {forged_body}"
        );

        // A garbage token must also be rejected.
        let garbage = server
            .get("/_matrix/client/v3/account/whoami")
            .add_header(
                axum::http::HeaderName::from_static("authorization"),
                axum::http::HeaderValue::from_str("Bearer not-a-real-token.xyz").expect("hv"),
            )
            .await;
        garbage.assert_status(axum::http::StatusCode::UNAUTHORIZED);
        let garbage_body: Value = garbage.json();
        assert_eq!(
            garbage_body["errcode"].as_str(),
            Some("M_UNKNOWN_TOKEN"),
            "garbage token must be rejected with M_UNKNOWN_TOKEN; got {garbage_body}"
        );
    }

    // secret_test_server:start
    //   purpose: Build a TestServer backed by an AppState with a registration_shared_secret
    //            injected directly (NOT via env var — avoids a process-global env race
    //            across parallel `cargo test` threads).
    //   input:  secret — the shared secret to require
    //   output: TestServer
    //   sideEffects: none
    // secret_test_server:end
    fn secret_test_server(secret: &str) -> TestServer {
        let state = AppState::new();
        let state = AppState::with_registration_shared_secret(state, secret.to_string());
        let app = router(state);
        TestServer::new(app)
    }

    // test:registration_gate_missing_secret_forbidden:start
    //   purpose: SEC internal-task. When registration_shared_secret is configured, a register call
    //            with no registration_secret field must be rejected BEFORE a UIA session
    //            is even issued — 403 M_FORBIDDEN, not the usual 401 UIA challenge.
    //   input:  server with secret "letmein"; POST /register without registration_secret
    //   output: 403 {"errcode":"M_FORBIDDEN"}
    //   sideEffects: none (no UIA session created)
    // test:registration_gate_missing_secret_forbidden:end
    #[tokio::test]
    async fn registration_gate_missing_secret_forbidden() {
        let server = secret_test_server("letmein");

        let resp = server
            .post("/_matrix/client/v3/register")
            .json(&json!({ "username": "newbie", "password": "pw" }))
            .await;

        resp.assert_status(axum::http::StatusCode::FORBIDDEN);
        let body: Value = resp.json();
        assert_eq!(body["errcode"].as_str(), Some("M_FORBIDDEN"), "got {body}");
    }

    // test:registration_gate_wrong_secret_forbidden:end
    #[tokio::test]
    async fn registration_gate_wrong_secret_forbidden() {
        let server = secret_test_server("letmein");

        let resp = server
            .post("/_matrix/client/v3/register")
            .json(&json!({
                "username": "newbie",
                "password": "pw",
                "registration_secret": "wrong-guess",
            }))
            .await;

        resp.assert_status(axum::http::StatusCode::FORBIDDEN);
        let body: Value = resp.json();
        assert_eq!(body["errcode"].as_str(), Some("M_FORBIDDEN"), "got {body}");
    }

    // test:registration_gate_correct_secret_succeeds:start
    //   purpose: With the correct registration_secret on both the challenge probe and the
    //            completing call, registration proceeds exactly as in the open (no-secret)
    //            case — the gate must not break the legitimate flow.
    //   input:  server with secret "letmein"; both calls carry "registration_secret":"letmein"
    //   output: step 1 → 401 UIA challenge; step 2 → 200 with user_id
    //   sideEffects: "newbie" inserted into AppState.users
    // test:registration_gate_correct_secret_succeeds:end
    #[tokio::test]
    async fn registration_gate_correct_secret_succeeds() {
        let server = secret_test_server("letmein");

        let challenge: Value = server
            .post("/_matrix/client/v3/register")
            .json(&json!({
                "username": "newbie",
                "password": "pw",
                "registration_secret": "letmein",
            }))
            .await
            .json();
        let session = challenge["session"].as_str().expect("session").to_string();

        let resp = server
            .post("/_matrix/client/v3/register")
            .json(&json!({
                "username": "newbie",
                "password": "pw",
                "registration_secret": "letmein",
                "auth": { "type": "m.login.dummy", "session": session }
            }))
            .await;

        resp.assert_status_ok();
        let body: Value = resp.json();
        assert_eq!(
            body["user_id"].as_str(),
            Some("@newbie:localhost"),
            "got {body}"
        );
    }
}
