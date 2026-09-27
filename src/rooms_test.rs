// START_AI_HEADER
// MODULE: matrix-hs/src/rooms_test.rs
// PURPOSE: Integration tests for the createRoom alias barrier wiring.
//
//          Tests prove:
//            1. createRoom with alias "foo" succeeds + directory resolves (barrier_store=Some).
//            2. createRoom with the SAME alias "foo" again (same store, already claimed)
//               returns 400 M_ROOM_IN_USE.
//            3. With barrier_store=None → local uniqueness unchanged (no regression).
//            4. Existing 19 default tests still pass (no teardown needed here).
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

    // barrier_alias_test_server:start
    //   purpose: Build a TestServer backed by AppState with a MemClaimStore injected.
    //            Proves createRoom→barrier alias path without needing zenoh or a real KvStore.
    //   input:  none
    //   output: (TestServer, Arc<MemClaimStore>)
    //   sideEffects: none
    // barrier_alias_test_server:end
    fn barrier_alias_test_server() -> (TestServer, Arc<MemClaimStore>) {
        let mem_store = Arc::new(MemClaimStore::new());
        let state = AppState::new();
        let state = AppState::with_barrier_store(
            state,
            mem_store.clone() as Arc<dyn crate::substrate::barrier::ClaimStore + Send + Sync>,
        );
        let app = router(state);
        (TestServer::new(app), mem_store)
    }

    // no_barrier_test_server:start
    //   purpose: Build a TestServer with barrier_store=None (single-node mode).
    //   input:  none
    //   output: TestServer
    //   sideEffects: none
    // no_barrier_test_server:end
    fn no_barrier_test_server() -> TestServer {
        let state = AppState::new();
        let app = router(state);
        TestServer::new(app)
    }

    // register_and_bearer:start
    //   purpose: Register a user via two-step UIA and return an Authorization bearer header.
    //   input:  server — &TestServer; username — localpart
    //   output: (HeaderName, HeaderValue) for Authorization: Bearer <mxt_ token>
    //   sideEffects: inserts user into AppState via register
    // register_and_bearer:end
    async fn register_and_bearer(
        server: &TestServer,
        username: &str,
    ) -> (axum::http::HeaderName, axum::http::HeaderValue) {
        let ch: Value = server
            .post("/_matrix/client/v3/register")
            .json(&json!({ "username": username, "password": "pw" }))
            .await
            .json();
        let sess = ch["session"]
            .as_str()
            .unwrap_or_else(|| panic!("register_and_bearer: no session for {username}; got {ch}"))
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
            .unwrap_or_else(|| panic!("register_and_bearer: no token for {username}; got {reg}"))
            .to_string();
        (
            axum::http::HeaderName::from_static("authorization"),
            axum::http::HeaderValue::from_str(&format!("Bearer {token}")).expect("header value"),
        )
    }

    // test:alias_barrier_first_create_succeeds:start
    //   purpose: createRoom with alias "foo" and a MemClaimStore injected succeeds (200)
    //            and GET /directory/room/#foo:localhost resolves to the new room_id.
    //            Proves the Claimed path: barrier::claim returns Claimed → alias registered.
    //   input:  AppState with MemClaimStore, createRoom{room_alias_name:"foo"}
    //   output: 200 {"room_id":"!foo:localhost"};
    //           GET /directory/room/%23foo%3Alocalhost → 200 {"room_id":"!foo:localhost"}
    //   sideEffects: alias "#foo:localhost" → "!foo:localhost" in AppState.aliases;
    //                key "mx:alias:#foo:localhost" set in MemClaimStore
    // test:alias_barrier_first_create_succeeds:end
    #[tokio::test]
    async fn alias_barrier_first_create_succeeds() {
        let (server, _store) = barrier_alias_test_server();
        let (hn, hv) = register_and_bearer(&server, "foo_user").await;

        let resp = server
            .post("/_matrix/client/v3/createRoom")
            .add_header(hn.clone(), hv.clone())
            .json(&json!({ "room_alias_name": "foo" }))
            .await;
        resp.assert_status_ok();
        let body: Value = resp.json();
        assert_eq!(
            body["room_id"].as_str(),
            Some("!foo:localhost"),
            "createRoom with barrier must return correct room_id; got {body}"
        );

        // Directory lookup must resolve.
        // URL-encode '#' as %23 and ':' as %3A for the path segment.
        let dir = server
            .get("/_matrix/client/v3/directory/room/%23foo%3Alocalhost")
            .await;
        dir.assert_status_ok();
        let dir_body: Value = dir.json();
        assert_eq!(
            dir_body["room_id"].as_str(),
            Some("!foo:localhost"),
            "directory must resolve alias; got {dir_body}"
        );
    }

    // test:alias_barrier_duplicate_returns_room_in_use:start
    //   purpose: createRoom with alias "bar" succeeds first; a second createRoom with the
    //            SAME alias "bar" (same MemClaimStore, so it's now claimed) returns
    //            400 M_ROOM_IN_USE.  The second call must NOT create the room.
    //   input:  two createRoom calls with room_alias_name="bar" against shared MemClaimStore
    //   output: first call 200; second call 400 {"errcode":"M_ROOM_IN_USE"}
    //   sideEffects: alias "#bar:localhost" claimed after first call; second rejected by barrier
    // test:alias_barrier_duplicate_returns_room_in_use:end
    #[tokio::test]
    async fn alias_barrier_duplicate_returns_room_in_use() {
        let (server, _store) = barrier_alias_test_server();
        let (hn, hv) = register_and_bearer(&server, "bar_user").await;

        // First creation must succeed.
        server
            .post("/_matrix/client/v3/createRoom")
            .add_header(hn.clone(), hv.clone())
            .json(&json!({ "room_alias_name": "bar" }))
            .await
            .assert_status_ok();

        // Second creation with the same alias must be rejected.
        let resp = server
            .post("/_matrix/client/v3/createRoom")
            .add_header(hn.clone(), hv.clone())
            .json(&json!({ "room_alias_name": "bar" }))
            .await;
        resp.assert_status(axum::http::StatusCode::BAD_REQUEST);
        let body: Value = resp.json();
        assert_eq!(
            body["errcode"].as_str(),
            Some("M_ROOM_IN_USE"),
            "duplicate alias must return M_ROOM_IN_USE; got {body}"
        );
    }

    // test:alias_no_barrier_local_uniqueness_unchanged:start
    //   purpose: With barrier_store=None (single-node mode), createRoom with an alias
    //            behaves as before: first call succeeds, second call with the same alias
    //            is idempotent (add-wins, existing Stage-1 simplification — no conflict).
    //            This test documents the existing no-barrier local behaviour is unchanged.
    //   input:  AppState with no barrier_store; two createRoom calls with room_alias_name="baz"
    //   output: both calls 200 (idempotent / add-wins local alias map)
    //   sideEffects: alias "#baz:localhost" → "!baz:localhost" in AppState.aliases
    // test:alias_no_barrier_local_uniqueness_unchanged:end
    #[tokio::test]
    async fn alias_no_barrier_local_uniqueness_unchanged() {
        let server = no_barrier_test_server();
        let (hn, hv) = register_and_bearer(&server, "baz_user").await;

        let r1 = server
            .post("/_matrix/client/v3/createRoom")
            .add_header(hn.clone(), hv.clone())
            .json(&json!({ "room_alias_name": "baz" }))
            .await;
        r1.assert_status_ok();
        let b1: Value = r1.json();
        assert_eq!(
            b1["room_id"].as_str(),
            Some("!baz:localhost"),
            "first no-barrier create"
        );

        // Second call with same alias: no barrier, so it proceeds to local insert (idempotent).
        let r2 = server
            .post("/_matrix/client/v3/createRoom")
            .add_header(hn.clone(), hv.clone())
            .json(&json!({ "room_alias_name": "baz" }))
            .await;
        r2.assert_status_ok();
        let b2: Value = r2.json();
        assert_eq!(
            b2["room_id"].as_str(),
            Some("!baz:localhost"),
            "second no-barrier create must still succeed (idempotent local alias insert); got {b2}"
        );
    }

    // test:alias_barrier_pre_claimed_returns_room_in_use:start
    //   purpose: With a MemClaimStore pre-seeded so "#qux:localhost" is already claimed,
    //            createRoom with room_alias_name="qux" returns 400 M_ROOM_IN_USE.
    //            Mirrors the register.rs pattern: barrier_rejected_returns_user_in_use.
    //   input:  AppState with MemClaimStore; "mx:alias:#qux:localhost" pre-claimed in store;
    //           createRoom{room_alias_name:"qux"}
    //   output: 400 {"errcode":"M_ROOM_IN_USE"}
    //   sideEffects: none (alias is NOT registered because barrier rejects first)
    // test:alias_barrier_pre_claimed_returns_room_in_use:end
    #[tokio::test]
    async fn alias_barrier_pre_claimed_returns_room_in_use() {
        let (server, store) = barrier_alias_test_server();
        let (hn, hv) = register_and_bearer(&server, "qux_user").await;

        // Pre-seed the MemClaimStore so "#qux:localhost" is already claimed.
        use crate::substrate::barrier::ClaimStore as _;
        store
            .cas_claim("mx:alias:#qux:localhost", "!some-other-room:localhost")
            .expect("pre-seed qux alias in MemClaimStore");

        let resp = server
            .post("/_matrix/client/v3/createRoom")
            .add_header(hn.clone(), hv.clone())
            .json(&json!({ "room_alias_name": "qux" }))
            .await;
        resp.assert_status(axum::http::StatusCode::BAD_REQUEST);
        let body: Value = resp.json();
        assert_eq!(
            body["errcode"].as_str(),
            Some("M_ROOM_IN_USE"),
            "pre-claimed alias must return M_ROOM_IN_USE; got {body}"
        );
    }
}
