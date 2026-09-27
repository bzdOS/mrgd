// START_AI_HEADER
// MODULE: matrix-hs/src/ephemeral_cluster_test.rs
// PURPOSE: Cross-node convergence test for ephemeral EDUs (typing + receipts)
//          under the `cluster` feature — mirrors cluster_test.rs's two-in-process-
//          axum-servers-over-real-Zenoh pattern, but exercises routes/ephemeral.rs
//          instead of routes/send.rs.
//
//          Proves:
//            1. alice (on node-a) PUTs typing=true → after a Zenoh gossip round
//               trip, bob's GET /sync on node-b shows an m.typing event listing
//               alice's user_id (routes::ephemeral::publish_typing →
//               ZenohCrdtSink "typing" key → drain_cluster_ephemeral →
//               AppState::merge_typing_remote → typing_user_ids union).
//            2. alice POSTs an m.read receipt on node-a → bob's /sync on node-b
//               shows the corresponding m.receipt event (publish_receipt →
//               "receipt" key → drain_cluster_ephemeral → AppState::set_receipt
//               last-writer-wins merge).
//
//          Reuses the SAME per-room ZenohCrdtSink as PDU replication (send.rs) —
//          just two additional routing keys ("typing", "receipt") under the same
//          Zenoh key prefix; no second Zenoh session/subscriber is opened.
// DEPENDENCIES: axum-test, zenoh, matrix_hs::{router, AppState, state::ClusterConfig}
// END_AI_HEADER

#[cfg(all(test, feature = "cluster"))]
mod tests {
    use crate::{router, state::ClusterConfig, AppState};
    use axum_test::TestServer;
    use serde_json::{json, Value};

    // register_and_bearer:start
    //   purpose: Register a user via two-step UIA and return an Authorization
    //            bearer header (duplicated per-module by repo convention — see
    //            cluster_test.rs's identically-named helper).
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

    // cluster:ephemeral_typing_and_receipt_converge:start
    //   purpose: Two in-process axum servers (node-a, node-b) sharing a real Zenoh
    //            peer mesh on loopback. alice (node-a) sets typing + posts a
    //            receipt; both must converge to bob's /sync on node-b within one
    //            gossip round trip (~200 ms), reusing the same per-room
    //            ZenohCrdtSink as PDU replication (just different routing keys).
    //   input:  none (all resources constructed in-test)
    //   output: bob's GET /sync on node-b shows alice's m.typing + m.receipt
    //   sideEffects: opens two real Zenoh sessions; background tokio tasks per
    //                room sink; network I/O on loopback only
    // cluster:ephemeral_typing_and_receipt_converge:end
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ephemeral_typing_and_receipt_converge() {
        let _zg = crate::test_util::ZENOH_TEST_LOCK.acquire().await.unwrap();
        // ── Two Zenoh peer sessions ────────────────────────────────────────────
        let [sess_a, sess_b] = crate::test_util::open_mesh().await;

        let prefix = crate::test_util::unique_prefix("mrgd/matrix/room/ephemeral-cluster-test-0");

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

        // Ephemeral EDUs carry no PDU signature, so no TOFU key exchange is
        // needed here (unlike cluster_test.rs's PDU convergence test).

        let server_a = TestServer::new(router(state_a.clone()));
        let server_b = TestServer::new(router(state_b.clone()));

        let (auth_a_name, auth_a_val) = register_and_bearer(&server_a, "alice").await;
        let (auth_b_name, auth_b_val) = register_and_bearer(&server_b, "bob").await;

        // Room created on node-a only; node-b joins by room_id (same posture as
        // cluster_test.rs — a room has one home server).
        let room_alias = "ephemeral-cluster-room-0";
        server_a
            .post("/_matrix/client/v3/createRoom")
            .add_header(auth_a_name.clone(), auth_a_val.clone())
            .json(&json!({ "room_alias_name": room_alias }))
            .await
            .assert_status_ok();

        let room_id = format!("!{room_alias}:node-a");
        state_b.ensure_room_state(&room_id);

        // bob must actually be a JOINED member in node-b's own room_state — /sync
        // is scoped to the caller's own joined rooms (routes/sync.rs::
        // build_join_rooms), so without this bob's later /sync on node-b would see
        // no rooms at all regardless of what typing/receipt state converges.
        // This server's join is fully permissive (no invite/join_rules gate), so
        // bob can join node-b's local (empty-but-for-this) room_state directly —
        // no need to wait on state replication from node-a for this test's purpose.
        server_b
            .post(&format!("/_matrix/client/v3/rooms/{room_id}/join"))
            .add_header(auth_b_name.clone(), auth_b_val.clone())
            .await
            .assert_status_ok();

        // Trigger sink creation (opens the Zenoh subscriber) on both sides before
        // publishing, same as cluster_test.rs.
        let _ = server_a.get("/_matrix/client/v3/sync").await;
        let _ = server_b.get("/_matrix/client/v3/sync").await;
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // ── alice sets typing=true on node-a ───────────────────────────────────
        let typing_path = format!("/_matrix/client/v3/rooms/{room_id}/typing/@alice:node-a");
        server_a
            .put(&typing_path)
            .add_header(auth_a_name.clone(), auth_a_val.clone())
            .json(&json!({ "typing": true, "timeout": 30000 }))
            .await
            .assert_status_ok();

        // ── alice posts an m.read receipt on node-a ────────────────────────────
        let event_id = "$cluster-ephemeral-event-0";
        let receipt_path = format!("/_matrix/client/v3/rooms/{room_id}/receipt/m.read/{event_id}");
        server_a
            .post(&receipt_path)
            .add_header(auth_a_name.clone(), auth_a_val.clone())
            .json(&json!({}))
            .await
            .assert_status_ok();

        // Zenoh peer-mode gossip on loopback is typically <5 ms; 200 ms matches
        // the safety margin used by cluster_test.rs's PDU convergence test.
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        // ── bob's /sync on node-b must show both ───────────────────────────────
        let sync_resp_b = server_b
            .get("/_matrix/client/v3/sync")
            .add_header(auth_b_name.clone(), auth_b_val.clone())
            .await;
        sync_resp_b.assert_status_ok();
        let sync_body_b: Value = sync_resp_b.json();

        let events_b = sync_body_b["rooms"]["join"][&room_id]["ephemeral"]["events"]
            .as_array()
            .unwrap_or_else(|| {
                panic!(
                    "node-b /sync has no ephemeral events for room {room_id}; body: {sync_body_b}"
                )
            });

        let typing_ev = events_b
            .iter()
            .find(|ev| ev["type"] == "m.typing")
            .unwrap_or_else(|| {
                panic!(
                "node-b /sync must show alice's m.typing after Zenoh convergence; got {events_b:?}"
            )
            });
        let user_ids = typing_ev["content"]["user_ids"]
            .as_array()
            .expect("m.typing content.user_ids must be an array");
        assert!(
            user_ids.iter().any(|v| v.as_str() == Some("@alice:node-a")),
            "m.typing user_ids on node-b must contain alice; got {user_ids:?}"
        );

        let receipt_ev = events_b
            .iter()
            .find(|ev| ev["type"] == "m.receipt")
            .unwrap_or_else(|| {
                panic!(
                "node-b /sync must show alice's m.receipt after Zenoh convergence; got {events_b:?}"
            )
            });
        let ts = receipt_ev["content"][event_id]["m.read"]["@alice:node-a"]["ts"].as_u64();
        assert!(
            ts.is_some(),
            "m.receipt content on node-b must contain alice's m.read ts for {event_id}; got {receipt_ev}"
        );
    }
}
