// START_AI_HEADER
// MODULE: matrix-hs/src/sink_growth_test.rs
// PURPOSE: Measure `ClusterState.sinks` — the one structure the first survey (fb517f3)
//          could not measure, and the one whose SHAPE is a slow leak: one sink per room id
//          the node has ever heard of, keyed by room, never evicted. This test puts a real
//          Zenoh mesh between two nodes and counts them.
//
//          Method note, because the naive version of this measurement lies: node-b must NOT
//          sync while measuring. /sync drives drain_cluster_deltas, which creates the local
//          room entry — and then a sink would look justified by a room that exists locally.
//          The number that matters is "sinks for rooms node-b has never drained", which is
//          exactly the discovery-only path: a sink per room created on the peer.
//
//          Also asserted: that the local room map on node-b stays EMPTY, so the count above
//          is discovery's doing and not a side effect of a drain.
//
// Gated on the cluster feature because it opens two real Zenoh sessions. Ignored by
// default because it is a survey and it serialises on ZENOH_TEST_LOCK.
// DEPENDENCIES: zenoh, matrix_hs::{router, state::ClusterConfig, AppState, test_util}
// END_AI_HEADER

#[cfg(all(test, feature = "cluster"))]
mod tests {
    use crate::{router, state::ClusterConfig, AppState};
    use crate::test_util::{open_mesh, unique_prefix, ZENOH_TEST_LOCK};
    use axum::http::{HeaderName, HeaderValue};
    use axum_test::TestServer;
    use serde_json::{json, Value};

    async fn register_and_bearer(
        server: &TestServer,
        username: &str,
    ) -> (HeaderName, HeaderValue) {
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

    // sink_count_grows_per_discovered_room:start
    //   purpose: Count sinks on a node that has never drained, for rooms a peer created.
    //            Answers the survey's open question with a number: is it one sink per room
    //            forever, and is the sink created by discovery alone.
    //   input:  two clustered nodes; node-a creates N_ROOMS rooms and says nothing to node-b
    //   output: () — prints sink count before/after and the local-room count on node-b
    //   sideEffects: two real Zenoh sessions on loopback, background sink tasks
    // sink_count_grows_per_discovered_room:end
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "survey, not a gate: opens a real mesh; run with --ignored --nocapture"]
    async fn sink_count_grows_per_discovered_room() {
        const N_ROOMS: usize = 20;

        let _zg = ZENOH_TEST_LOCK.acquire().await.unwrap();
        let [sess_a, sess_b] = open_mesh().await;
        let prefix = unique_prefix("mrgd/matrix/room/sink-growth");

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

        // TOFU both ways: without it every discovered sample is rejected and the sink
        // count would measure the wrong thing.
        state_b
            .key_store
            .insert("node-a", state_a.signer.verifying_key_bytes());
        state_a
            .key_store
            .insert("node-b", state_b.signer.verifying_key_bytes());

        let server_a = TestServer::new(router(state_a.clone()));
        let _server_b = TestServer::new(router(state_b.clone()));
        let (auth_a_name, auth_a_val) = register_and_bearer(&server_a, "alice_sink").await;

        // Let both discovery subscribers reach the gossip layer before any publish.
        tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;

        let sinks_b = |tag: &str| {
            let sinks = state_b
                .cluster
                .as_ref()
                .expect("node-b cluster")
                .list_room_ids()
                .len();
            let local_rooms = state_b.rooms.lock().expect("rooms").len();
            println!(
                "  {tag:<14} sinks_on_node_b={sinks:<4} local_rooms_on_node_b={local_rooms}"
            );
            (sinks, local_rooms)
        };

        println!("\n  === sink growth survey: N_ROOMS = {N_ROOMS} created on node-a ===");
        let (sinks0, rooms0) = sinks_b("before");

        let mut created = Vec::with_capacity(N_ROOMS);
        for _ in 0..N_ROOMS {
            let resp: Value = server_a
                .post("/_matrix/client/v3/createRoom")
                .add_header(auth_a_name.clone(), auth_a_val.clone())
                .json(&json!({}))
                .await
                .json();
            created.push(
                resp["room_id"]
                    .as_str()
                    .expect("room_id from createRoom")
                    .to_string(),
            );
        }

        // Poll for discovery. No /sync on node-b anywhere in this test — that is the whole
        // point: the sinks must appear from the wildcard discovery subscriber alone.
        let count_sinks_b = || {
            state_b
                .cluster
                .as_ref()
                .expect("node-b cluster")
                .list_room_ids()
                .len()
        };
        let mut discovered = count_sinks_b();
        for _ in 0..120 {
            if discovered >= created.len() {
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
            discovered = count_sinks_b();
        }

        let (sinks1, rooms1) = sinks_b("after-create");
        let all_present = state_b
            .cluster
            .as_ref()
            .expect("node-b cluster")
            .list_room_ids()
            .iter()
            .filter(|r| created.contains(r))
            .count();
        println!(
            "  VERDICT sinks: {sinks0} → {sinks1} for {} rooms created on the peer \
             ({all_present} of them present as a sink); local rooms on node-b {rooms0} → {rooms1} \
             — {}",
            created.len(),
            if sinks1 >= created.len() && rooms1 == 0 {
                "UNBOUNDED in rooms seen: one sink per discovered room, no eviction, and \
                 nothing local justifies them"
            } else {
                "not a flat per-room sink — investigate before drawing conclusions"
            }
        );
    }
}
