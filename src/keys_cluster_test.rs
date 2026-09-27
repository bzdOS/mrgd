// START_AI_HEADER
// MODULE: matrix-hs/src/keys_cluster_test.rs
// PURPOSE: Cross-node convergence test for OTK claim routing (see
//          routes/keys.rs::serve_otk_claims / try_claim_local / post_keys_claim,
//          state.rs::ClusterState::fetch_otk) — mirrors device_lists_cluster_test.rs's
//          two-in-process-axum-servers-over-real-Zenoh pattern, but this feature needs
//          the queryable itself, so each side also calls serve_otk_claims directly
//          (main.rs's build_state, where the OTHER three queryables get declared, is
//          in the bin target and unreachable from a lib test).
//
//          Proves: alice uploads an OTK on node-a; bob's keys/claim HTTP call on
//          node-b (which does not own alice's device) is routed over the mesh to
//          node-a, pops the key there, and returns it — and a second claim for the
//          same (user,device,algorithm), with no OTK left, resolves to absent
//          (never hangs, never fabricates a key) rather than being routed forever.
// DEPENDENCIES: axum-test, zenoh, matrix_hs::{router, AppState, state::ClusterConfig, routes::keys::serve_otk_claims}
// END_AI_HEADER

#[cfg(all(test, feature = "cluster"))]
mod tests {
    use crate::{router, state::ClusterConfig, AppState};
    use axum_test::TestServer;
    use serde_json::{json, Value};

    // register_and_bearer:start
    //   purpose: Register a user via two-step UIA and return an Authorization
    //            bearer header (duplicated per-module by repo convention).
    //   input:  server, username
    //   output: (HeaderName, HeaderValue)
    //   sideEffects: inserts the user into the server's AppState via /register
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

    // cluster:cross_node_otk_claim_routes_to_the_owning_node:start
    //   purpose: Two in-process axum servers (node-a, node-b) sharing a real Zenoh
    //            peer mesh on loopback, each with serve_otk_claims declared. alice
    //            uploads one OTK on node-a. bob's keys/claim on node-b — which does
    //            not own alice's device — must return that exact key, routed over
    //            the mesh, not absent.
    //   input:  none (all resources constructed in-test)
    //   output: node-b's claim response contains alice's uploaded key
    //   sideEffects: opens two real Zenoh sessions; background tokio tasks per queryable
    // cluster:cross_node_otk_claim_routes_to_the_owning_node:end
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cross_node_otk_claim_routes_to_the_owning_node() {
        let _zg = crate::test_util::ZENOH_TEST_LOCK.acquire().await.unwrap();
        let [sess_a, sess_b] = crate::test_util::open_mesh().await;

        let prefix = crate::test_util::unique_prefix("mrgd/matrix/room/keys-cluster-test-0");

        let state_a = AppState::with_cluster(ClusterConfig {
            session: sess_a,
            key_prefix: prefix.to_string(),
            server_name: "node-a".to_string(),
        });
        let state_b = AppState::with_cluster(ClusterConfig {
            session: sess_b,
            key_prefix: prefix.to_string(),
            server_name: "node-b".to_string(),
        });

        crate::routes::keys::serve_otk_claims(state_a.clone())
            .await
            .expect("serve_otk_claims on node-a");
        crate::routes::keys::serve_otk_claims(state_b.clone())
            .await
            .expect("serve_otk_claims on node-b");
        // Let both queryables finish declaring before either side queries.
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let server_a = TestServer::new(router(state_a.clone()));
        let server_b = TestServer::new(router(state_b.clone()));

        let (auth_a_name, auth_a_val) = register_and_bearer(&server_a, "alice").await;
        let (auth_b_name, auth_b_val) = register_and_bearer(&server_b, "bob").await;

        server_a
            .post("/_matrix/client/v3/keys/upload")
            .add_header(auth_a_name, auth_a_val)
            .json(&json!({
                "one_time_keys": {
                    "signed_curve25519:AAAAAA": { "key": "ALICEOTK1" }
                }
            }))
            .await
            .assert_status_ok();

        let claim: Value = server_b
            .post("/_matrix/client/v3/keys/claim")
            .add_header(auth_b_name.clone(), auth_b_val.clone())
            .json(&json!({
                "one_time_keys": { "@alice:node-a": { "DEVICE1": "signed_curve25519" } }
            }))
            .await
            .json();

        let device_keys = &claim["one_time_keys"]["@alice:node-a"]["DEVICE1"];
        assert_eq!(
            device_keys.get("signed_curve25519:AAAAAA"),
            Some(&json!({ "key": "ALICEOTK1" })),
            "node-b must receive alice's OTK, routed from node-a over the mesh; got {claim}"
        );

        // The key was popped — a second claim for the same (user,device,algorithm)
        // must resolve to absent (not hang, not fabricate a key), because there is
        // nothing left to route to.
        let claim_again: Value = server_b
            .post("/_matrix/client/v3/keys/claim")
            .add_header(auth_b_name, auth_b_val)
            .json(&json!({
                "one_time_keys": { "@alice:node-a": { "DEVICE1": "signed_curve25519" } }
            }))
            .await
            .json();
        assert!(
            claim_again["one_time_keys"]
                .as_object()
                .map(|m| m.is_empty())
                .unwrap_or(true),
            "a second claim after the only key was popped must be empty, not re-served or hung; got {claim_again}"
        );
    }
}
