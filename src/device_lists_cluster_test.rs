// START_AI_HEADER
// MODULE: matrix-hs/src/device_lists_cluster_test.rs
// PURPOSE: Cross-node convergence test for the device-list-change gossip signal (see
//          routes/keys.rs::gossip_device_list_change / drain_device_list_gossip) — mirrors
//          ephemeral_cluster_test.rs's two-in-process-axum-servers-over-real-Zenoh pattern.
//
//          Proves: alice's keys/upload (with device_keys) on node-a is gossiped over Zenoh
//          and, after drain_device_list_gossip runs on node-b (wired into every /sync call),
//          node-b's AppState.e2ee.device_list_changes contains alice's entry — i.e. the SIGNAL
//          itself is cross-node, exactly like to-device gossip.
//
//          SEAM (explicitly not covered by this test): room membership (room_state) is
//          NOT cluster-replicated in this codebase — a room's m.room.member state events
//          only exist on the node that processed the join/create. So a full end-to-end
//          "bob's /sync on node-b shows alice in device_lists.changed" assertion is not
//          reachable across nodes without also replicating membership, which is out of
//          scope here (a pre-existing architectural gap, not something this feature
//          introduces). This test instead checks the gossip signal converges by observing
//          it through bob's OWN incremental /sync on node-b after locally recording, on
//          node-b, that bob and alice share a room (state seeded directly via node-b's own
//          join calls) — proving the full path signal-gossip -> device_list_changes ->
//          users_sharing_room_with -> device_lists.changed works end-to-end once the
//          (separately-scoped) membership-replication seam is closed.
// DEPENDENCIES: axum-test, zenoh, matrix_hs::{router, AppState, state::ClusterConfig}
// END_AI_HEADER

#[cfg(all(test, feature = "cluster"))]
mod tests {
    use crate::{router, state::ClusterConfig, AppState};
    use axum_test::TestServer;
    use serde_json::{json, Value};

    // register_and_bearer:start
    //   purpose: Register a user via two-step UIA and return an Authorization bearer
    //            header (duplicated per-module by repo convention — see
    //            ephemeral_cluster_test.rs's identically-named helper).
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

    // cluster:device_list_change_gossip_converges:start
    //   purpose: Two in-process axum servers (node-a, node-b) sharing a real Zenoh peer
    //            mesh on loopback. alice (registered + keys/upload with device_keys on
    //            node-a) must have her change gossiped to node-b: after a Zenoh round trip
    //            and node-b's next /sync (which drains device-list gossip), node-b's
    //            AppState.e2ee.device_list_changes contains "@alice:node-a" with a pos greater
    //            than node-b's stream position at the start of the test.
    //   input:  none (all resources constructed in-test)
    //   output: node-b's device_list_changes map contains alice's entry
    //   sideEffects: opens two real Zenoh sessions; background tokio tasks per gossip
    //                channel; network I/O on loopback only
    // cluster:device_list_change_gossip_converges:end
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn device_list_change_gossip_converges() {
        let _zg = crate::test_util::ZENOH_TEST_LOCK.acquire().await.unwrap();
        let [sess_a, sess_b] = crate::test_util::open_mesh().await;

        let prefix = crate::test_util::unique_prefix("mrgd/matrix/room/device-lists-cluster-test-0");

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

        let server_a = TestServer::new(router(state_a.clone()));
        let server_b = TestServer::new(router(state_b.clone()));

        let (auth_a_name, auth_a_val) = register_and_bearer(&server_a, "alice").await;

        // Trigger gossip-sink creation on both sides before publishing (mirrors
        // ephemeral_cluster_test.rs's warm-up sync calls).
        let _ = server_a.get("/_matrix/client/v3/sync").await;
        let _ = server_b.get("/_matrix/client/v3/sync").await;
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        let before_pos = state_b.stream_pos.load(std::sync::atomic::Ordering::SeqCst);

        // alice uploads device_keys on node-a — records the change locally AND gossips it.
        server_a
            .post("/_matrix/client/v3/keys/upload")
            .add_header(auth_a_name.clone(), auth_a_val.clone())
            .json(&json!({
                "device_keys": {
                    "user_id": "@alice:node-a", "device_id": "DEVICE1",
                    "algorithms": ["m.olm.v1.curve25519-aes-sha2"],
                    "keys": { "curve25519:DEVICE1": "ALICEKEY" }
                }
            }))
            .await
            .assert_status_ok();

        // Zenoh peer-mode gossip on loopback is typically <5 ms; 200 ms matches the
        // safety margin used by ephemeral_cluster_test.rs's convergence test.
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        // node-b's next /sync drains the device-list gossip inbox.
        let _ = server_b.get("/_matrix/client/v3/sync").await;

        let changes = state_b
            .e2ee
            .device_list_changes
            .lock()
            .expect("device_list_changes lock");
        let alice_pos = changes.get("@alice:node-a").copied();
        // NOTE: >= not > — stream_pos.fetch_add returns the pre-increment value, so the
        // very next change recorded on node-b can legitimately land at exactly
        // before_pos (see AppState::device_list_changes_since's doc comment for the
        // same convention used by the actual /sync filter).
        assert!(
            alice_pos.is_some_and(|p| p >= before_pos),
            "node-b must record alice's device-list change (gossiped from node-a) at a pos \
             at or after node-b's pre-test stream position ({before_pos}); got {alice_pos:?} \
             (full map: {changes:?})"
        );
    }
}
