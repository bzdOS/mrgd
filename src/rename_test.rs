// START_AI_HEADER
// MODULE: matrix-hs/src/rename_test.rs
// PURPOSE: Unit and integration tests for the coordination-free username rename flow
//          (grow-set loser handler, Stage 3.1).
//
//          Tests cover:
//            1. deterministic_new_localpart — pure scheme, no I/O.
//            2. apply_username_loss (loser side):
//               - users map: orig removed, new localpart inserted.
//               - renamed map: orig → new full user_id.
//               - whoami with old token returns the NEW user_id.
//               - register_user(orig) succeeds after rename (slot freed).
//            3. Winner side: account not in users as orig → no rename (skip, Ok).
//            4. Existing 23 tests pass (this module adds new ones only).
//
// DEPENDENCIES: matrix_hs::{AppState, router}, axum_test, serde_json
// END_AI_HEADER

#[cfg(test)]
mod tests {
    use crate::{router, AppState};
    use axum_test::TestServer;
    use serde_json::{json, Value};

    // rename_test_server:start
    //   purpose: Build a fresh TestServer backed by a new in-memory AppState
    //            with server_name "localhost".
    //   input:  none
    //   output: (TestServer, Arc<AppState>)
    //   sideEffects: none
    // rename_test_server:end
    fn test_server_with_state() -> (TestServer, std::sync::Arc<AppState>) {
        let state = AppState::new();
        let server = TestServer::new(router(state.clone()));
        (server, state)
    }

    // Helper: perform a full two-step UIA registration via HTTP.
    // Returns the signed mxt_ access token from the registration response.
    async fn register_via_http(server: &TestServer, username: &str, password: &str) -> String {
        let c: Value = server
            .post("/_matrix/client/v3/register")
            .json(&json!({ "username": username, "password": password }))
            .await
            .json();
        let sess = c["session"].as_str().expect("session").to_string();
        let resp: Value = server
            .post("/_matrix/client/v3/register")
            .json(&json!({
                "username": username,
                "password": password,
                "auth": { "type": "m.login.dummy", "session": sess }
            }))
            .await
            .json();
        resp["access_token"]
            .as_str()
            .expect("access_token from register")
            .to_string()
    }

    // test:deterministic_new_localpart:start
    //   purpose: deterministic_new_localpart is a pure function that produces the same
    //            result for the same inputs on every node (coordination-free rename scheme).
    //            Scheme: "<orig>--<server_part_of_loser_claimant>".
    //   input:  various (orig, loser_claimant) pairs
    //   output: expected new localpart
    //   sideEffects: none
    // test:deterministic_new_localpart:end
    #[test]
    fn deterministic_new_localpart_scheme() {
        // Standard MXID: "@alice:node-42" → "alice--node-42"
        assert_eq!(
            AppState::deterministic_new_localpart("alice", "@alice:node-42"),
            "alice--node-42",
            "standard MXID"
        );

        // server_name = localhost
        assert_eq!(
            AppState::deterministic_new_localpart("bob", "@bob:localhost"),
            "bob--localhost",
            "localhost server"
        );

        // Malformed MXID (no '@') → fallback "lost"
        assert_eq!(
            AppState::deterministic_new_localpart("carol", "carol"),
            "carol--lost",
            "malformed MXID no @ → fallback"
        );

        // Malformed MXID (no ':') → fallback "lost"
        assert_eq!(
            AppState::deterministic_new_localpart("dave", "@dave"),
            "dave--lost",
            "malformed MXID no : → fallback"
        );

        // Idempotent: same inputs always produce the same result.
        let r1 = AppState::deterministic_new_localpart("alice", "@alice:node-42");
        let r2 = AppState::deterministic_new_localpart("alice", "@alice:node-42");
        assert_eq!(r1, r2, "deterministic: same inputs → same output");
    }

    // test:apply_username_loss_moves_record_and_records_rename:start
    //   purpose: apply_username_loss:
    //              - removes orig localpart from users;
    //              - inserts new localpart into users;
    //              - records orig → "@<new>:<server>" in renamed map;
    //              - clears rename_required on the new record.
    //   input:  AppState with "alice" registered; apply_username_loss("alice", ...)
    //   output: users["alice"] absent; users["alice--other-node"] present;
    //           renamed["alice"] = "@alice--other-node:localhost"
    //   sideEffects: mutates AppState.users + AppState.renamed
    // test:apply_username_loss_moves_record_and_records_rename:end
    #[tokio::test]
    async fn apply_username_loss_moves_record_and_records_rename() {
        let state = AppState::new();

        // Register alice locally.
        state
            .register_user("alice", "pw", "DEV1", false)
            .expect("register alice");

        // Simulate the ReconcileDriver firing: alice on this node (@alice:localhost)
        // lost to @alice:other-node.
        state
            .apply_username_loss(
                "alice",
                "@alice:localhost",  // loser_claimant (this node)
                "@alice:other-node", // winner_claimant
            )
            .expect("apply_username_loss");

        // ── Assert users map ─────────────────────────────────────────────────
        {
            let users = state.users.lock().expect("users lock");
            assert!(
                !users.contains_key("alice"),
                "orig localpart 'alice' must be absent after rename"
            );
            let new_key = "alice--localhost";
            assert!(
                users.contains_key(new_key),
                "new localpart '{new_key}' must be present after rename"
            );
            let rec = users.get(new_key).expect("new record");
            assert!(
                !rec.rename_required,
                "rename_required must be false on the new record"
            );
            assert_eq!(rec.password_hash, "pw", "password_hash must be preserved");
            assert_eq!(rec.device_id, "DEV1", "device_id must be preserved");
        }

        // ── Assert renamed map ───────────────────────────────────────────────
        {
            let renamed = state.renamed.lock().expect("renamed lock");
            let new_user_id = renamed.get("alice").expect("renamed['alice'] must be set");
            assert_eq!(
                new_user_id, "@alice--localhost:localhost",
                "renamed map value"
            );
        }
    }

    // test:whoami_returns_new_user_id_after_rename:start
    //   purpose: After apply_username_loss fires, GET /whoami with the original signed token
    //            returns the NEW user_id (via the renamed map lookup), not the old one.
    //            This is the client discovery path (no server-push for user_id changes).
    //   input:  register "alice", get signed token, apply rename, GET /whoami with token
    //   output: user_id = "@alice--localhost:localhost" (the new identity)
    //   sideEffects: none after setup
    // test:whoami_returns_new_user_id_after_rename:end
    #[tokio::test]
    async fn whoami_returns_new_user_id_after_rename() {
        let (server, state) = test_server_with_state();

        // Register alice via HTTP (full UIA flow) — get signed token.
        let token = register_via_http(&server, "alice", "pw").await;

        // Simulate the loser handler: alice on this node lost.
        state
            .apply_username_loss("alice", "@alice:localhost", "@alice:other-node")
            .expect("apply_username_loss");

        // GET /whoami with the original token (still valid HMAC, but user_id lookup
        // hits the renamed map and returns the new user_id).
        let resp = server
            .get("/_matrix/client/v3/account/whoami")
            .add_header(
                axum::http::HeaderName::from_static("authorization"),
                axum::http::HeaderValue::from_str(&format!("Bearer {token}")).expect("hv"),
            )
            .await;
        resp.assert_status_ok();

        let body: Value = resp.json();
        assert_eq!(
            body["user_id"].as_str(),
            Some("@alice--localhost:localhost"),
            "whoami must return the NEW user_id after rename; got {body}"
        );
    }

    // test:register_orig_name_succeeds_after_rename:start
    //   purpose: After the rename, the original localpart slot is free.
    //            A fresh register_user("alice") must succeed (returns Ok(())).
    //            This proves the rename freed the slot for the winner side to claim.
    //   input:  AppState with "alice" registered + renamed; register_user("alice") again
    //   output: Ok(())
    //   sideEffects: inserts alice back under users (test isolation; not a real use-case)
    // test:register_orig_name_succeeds_after_rename:end
    #[tokio::test]
    async fn register_orig_name_succeeds_after_rename() {
        let state = AppState::new();

        state
            .register_user("alice", "pw", "DEV1", false)
            .expect("initial register");
        state
            .apply_username_loss("alice", "@alice:localhost", "@alice:other-node")
            .expect("apply_username_loss");

        // The orig slot must be free now — re-registration must succeed.
        let result = state.register_user("alice", "pw2", "DEV2", false);
        assert!(
            result.is_ok(),
            "register_user('alice') must succeed after rename frees the slot"
        );
    }

    // test:winner_side_no_rename:start
    //   purpose: If the orig localpart is NOT in users (this node is the winner, or
    //            the account was never registered locally), apply_username_loss is a
    //            no-op — it returns Ok(()) without touching users or renamed.
    //   input:  AppState with NO "bob" account; apply_username_loss("bob", ...)
    //   output: Ok(()); users unchanged; renamed["bob"] absent
    //   sideEffects: none
    // test:winner_side_no_rename:end
    #[tokio::test]
    async fn winner_side_no_rename_when_account_not_local() {
        let state = AppState::new();

        // Bob is NOT registered locally (this node is the winner — it has
        // the winning claim, the losing node has bob in its users map).
        let result = state.apply_username_loss(
            "bob",
            "@bob:other-node", // loser is on other-node
            "@bob:localhost",  // winner is localhost (this node)
        );
        assert!(
            result.is_ok(),
            "apply_username_loss on absent account must return Ok"
        );

        // users and renamed must be untouched.
        let users = state.users.lock().expect("users lock");
        assert!(
            !users.contains_key("bob"),
            "users must not contain 'bob' after no-op"
        );
        drop(users);

        let renamed = state.renamed.lock().expect("renamed lock");
        assert!(
            !renamed.contains_key("bob"),
            "renamed must not contain 'bob' after no-op"
        );
    }

    // test:whoami_unchanged_for_non_renamed_user:start
    //   purpose: For a user that was NOT renamed, GET /whoami continues to return
    //            the original user_id (regression guard for existing behaviour).
    //   input:  register "carol", NO rename applied, GET /whoami with signed token
    //   output: user_id = "@carol:localhost"
    //   sideEffects: none
    // test:whoami_unchanged_for_non_renamed_user:end
    #[tokio::test]
    async fn whoami_unchanged_for_non_renamed_user() {
        let (server, _state) = test_server_with_state();

        let token = register_via_http(&server, "carol", "pw").await;

        let resp = server
            .get("/_matrix/client/v3/account/whoami")
            .add_header(
                axum::http::HeaderName::from_static("authorization"),
                axum::http::HeaderValue::from_str(&format!("Bearer {token}")).expect("hv"),
            )
            .await;
        resp.assert_status_ok();

        let body: Value = resp.json();
        assert_eq!(
            body["user_id"].as_str(),
            Some("@carol:localhost"),
            "non-renamed user must keep original user_id; got {body}"
        );
    }

    // test:sync_gains_rename_push_field_after_rename:start
    //   purpose: Server-push counterpart to whoami_returns_new_user_id_after_rename:
    //            after apply_username_loss fires, the caller's NEXT /sync response
    //            (using the original token — tokens are never re-signed) carries a
    //            top-level "org.mrgd.renamed":{"user_id": <new>} vendor field, so a
    //            client that only long-polls /sync (never re-checks /whoami) still
    //            discovers the rename.
    //   input:  register "alice", get signed token, apply rename, GET /sync with token
    //   output: body["org.mrgd.renamed"]["user_id"] == "@alice--localhost:localhost"
    //   sideEffects: none after setup
    // test:sync_gains_rename_push_field_after_rename:end
    #[tokio::test]
    async fn sync_gains_rename_push_field_after_rename() {
        let (server, state) = test_server_with_state();
        let token = register_via_http(&server, "alice", "pw").await;

        state
            .apply_username_loss("alice", "@alice:localhost", "@alice:other-node")
            .expect("apply_username_loss");

        let resp = server
            .get("/_matrix/client/v3/sync")
            .add_header(
                axum::http::HeaderName::from_static("authorization"),
                axum::http::HeaderValue::from_str(&format!("Bearer {token}")).expect("hv"),
            )
            .await;
        resp.assert_status_ok();

        let body: Value = resp.json();
        assert_eq!(
            body["org.mrgd.renamed"]["user_id"].as_str(),
            Some("@alice--localhost:localhost"),
            "sync must push the new user_id via org.mrgd.renamed after rename; got {body}"
        );
    }

    // test:sync_has_no_rename_field_for_non_renamed_user:start
    //   purpose: A caller who was never renamed must not see the org.mrgd.renamed
    //            field at all (not present, not null) — its presence alone is what a
    //            client would treat as a signal to re-authenticate.
    //   input:  register "carol" (no rename); GET /sync with her token
    //   output: body has no "org.mrgd.renamed" key
    //   sideEffects: none
    // test:sync_has_no_rename_field_for_non_renamed_user:end
    #[tokio::test]
    async fn sync_has_no_rename_field_for_non_renamed_user() {
        let (server, _state) = test_server_with_state();
        let token = register_via_http(&server, "carol", "pw").await;

        let resp = server
            .get("/_matrix/client/v3/sync")
            .add_header(
                axum::http::HeaderName::from_static("authorization"),
                axum::http::HeaderValue::from_str(&format!("Bearer {token}")).expect("hv"),
            )
            .await;
        resp.assert_status_ok();

        let body: Value = resp.json();
        assert!(
            body.get("org.mrgd.renamed").is_none(),
            "a non-renamed caller must not get an org.mrgd.renamed field at all; got {body}"
        );
    }
}
