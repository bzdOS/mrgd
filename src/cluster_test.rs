// START_AI_HEADER
// MODULE: matrix-hs/src/cluster_test.rs
// PURPOSE: End-to-end multi-master test for the `cluster` feature.
//          Proves: POST /send to instance-A → GET /sync on instance-B sees the message
//          (and vice-versa), AND that node→node PDU forgery is rejected (P1.1 internal-task).
//          Two axum servers run in-process on different ports (no network — axum-test
//          uses the tower Service directly).  Two Zenoh sessions run in peer mode on
//          loopback; Zenoh's multicast/gossip discovers them within ~50 ms.
//          The convergence window is 200 ms (mirrors the mrgd Zenoh CRDT test).
//
//          Test flow (multimaster_a_to_b_via_http_and_zenoh):
//            1. Open two Zenoh sessions (default config, peer/scouting on loopback).
//            2. Build AppState-A ("node-a") and AppState-B ("node-b") — DISTINCT
//               server_names (== NodeSigner node_id), since one process cannot get two
//               values from the process-global MATRIX_HS_SERVER_NAME env var.
//            3. Simulate completed TOFU key distribution: each side inserts the OTHER
//               node's real pubkey into its own key_store.  (The real announce/drain
//               loop lives in the matrix-hs BINARY main.rs, not the library, so it is
//               not reachable from this lib-crate test; node_auth.rs unit-tests cover
//               the wire format directly. NOT sound until the mesh is authenticated —
//               see internal-task — but item 5's sender-binding, exercised below, is sound
//               standalone regardless of how the key arrived.)
//            4. register→login→signed mxt_ token on EACH node (createRoom/send now
//               require a signed token — unauth 401s since commit 9788942).
//            5. createRoom on node-a ONLY (room_id = "!cluster-room-0:node-a") — a room
//               has one home server; node-b joins it the way a federating server would,
//               via ensure_room_state with the SAME room_id string (no independent
//               room_id derivation from node-b's own server_name).
//            6. PUT /send on A (alice@node-a) → propagate → GET /sync on B: event present.
//            7. PUT /send on B (bob@node-b) → propagate → GET /sync on A: event present.
//               (a) proves convergence both directions.
//            8. Forgery: build a Pdu with sender "@x:node-a" but SIGNED BY node-b's
//               signer, feed it to node-a's RoomLog::apply_delta_verified directly.
//               (b) asserts accepted=0, rejected>=1 — the crypto check would pass
//               (node-a trusts node-b's real key from step 3) but the sender-binding
//               check in Pdu::verify_sig rejects it: node-b may only sign senders on
//               ITS OWN domain, not "@x:node-a".
//
//          This is a real Zenoh end-to-end test: serialise → Zenoh put → background
//          subscriber recv → inbox → drain (triggered by GET /sync) → apply_delta_verified
//          → RoomLog convergence → HTTP response.
// DEPENDENCIES: axum-test, zenoh, matrix_hs::{AppState, router, state::ClusterConfig},
//               crate::substrate::matrix_events::{Pdu, RoomLogDelta}
// END_AI_HEADER

#[cfg(all(test, feature = "cluster"))]
mod tests {
    use crate::{router, state::ClusterConfig, AppState};
    use axum_test::TestServer;
    use serde_json::{json, Value};

    // register_and_bearer:start
    //   purpose: Register a user via two-step UIA and return an Authorization bearer
    //            header carrying a signed mxt_ token (mirrors rooms_test.rs's helper —
    //            duplicated here because cluster_test.rs is a separate test module).
    //   input:  server — &TestServer; username — localpart
    //   output: (HeaderName, HeaderValue) for Authorization: Bearer <mxt_ token>
    //   sideEffects: inserts user into the server's AppState via /register
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

    // cluster:multimaster_a_to_b:start
    //   purpose: Prove (a) a message sent to instance-A appears in instance-B's /sync
    //            after Zenoh gossip propagates the delta (~200 ms loopback window) AND
    //            vice-versa, AND (b) a forged cross-node PDU (sender claims node-a's
    //            namespace but is signed by node-b) is rejected by
    //            RoomLog::apply_delta_verified — P1.1 internal-task items 2/3/5.
    //            Two in-process axum servers share a Zenoh peer mesh on localhost.
    //            No external dependencies: Zenoh uses multicast/gossip scouting on loopback.
    //   input:  none (all resources constructed in-test)
    //   output: (a) GET /sync on B contains A's event_id and vice-versa;
    //            (b) apply_delta_verified on the forged PDU returns (accepted=0, rejected>=1)
    //   sideEffects: opens two real Zenoh sessions; background tokio tasks per room sink;
    //                network I/O on loopback only
    // cluster:multimaster_a_to_b:end
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn multimaster_a_to_b_via_http_and_zenoh() {
        let _zg = crate::test_util::ZENOH_TEST_LOCK.acquire().await.unwrap();
        // ── Step 1: open two Zenoh peer sessions ──────────────────────────────
        // Default config = peer mode with multicast/gossip scouting on loopback.
        // No listen/connect needed — Zenoh discovers peers automatically.
        let [sess_a, sess_b] = crate::test_util::open_mesh().await;

        // ── Step 2: build two AppState instances with DISTINCT server_names ───
        // server_name doubles as the NodeSigner node_id (P1.1 internal-task item 1); node-a and
        // node-b MUST differ for the sender-binding forgery check below to be meaningful.
        let prefix = crate::test_util::unique_prefix("mrgd/matrix/room/cluster-test-0");

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

        // ── Step 3: simulate completed TOFU key distribution ──────────────────
        // See the module doc comment above for why this is manual here rather than
        // going through the real announce/drain loop (that lives in main.rs, the bin).
        state_a
            .key_store
            .insert("node-b", state_b.signer.verifying_key_bytes());
        state_b
            .key_store
            .insert("node-a", state_a.signer.verifying_key_bytes());

        let server_a = TestServer::new(router(state_a.clone()));
        let server_b = TestServer::new(router(state_b.clone()));

        // ── Step 4: register + signed token on each node ───────────────────────
        // createRoom/send now require a signed mxt_ token (unauth 401s since 9788942).
        let (auth_a_name, auth_a_val) = register_and_bearer(&server_a, "alice").await;
        let (auth_b_name, auth_b_val) = register_and_bearer(&server_b, "bob").await;

        // ── Step 5: create the room on node-a ONLY; node-b joins by room_id ────
        // A room has one home server (room_id domain suffix reflects the creator).
        // node-a and node-b now have DIFFERENT server_names, so node-b must NOT
        // independently call createRoom (that would derive "!cluster-room-0:node-b" —
        // a different room_id, and the two nodes would never converge). Instead
        // node-b learns about the room the way a federating server would: same
        // room_id, added directly to its local state.
        let room_alias = "cluster-room-0";

        server_a
            .post("/_matrix/client/v3/createRoom")
            .add_header(auth_a_name.clone(), auth_a_val.clone())
            .json(&json!({ "room_alias_name": room_alias }))
            .await
            .assert_status_ok();

        let room_id = format!("!{room_alias}:node-a");
        state_b.ensure_room_state(&room_id);

        // bob must actually JOIN in node-b's own local room_state — /sync is
        // scoped to the caller's own joined rooms (routes/sync.rs::
        // build_join_rooms; previously it leaked every room to every caller
        // regardless of membership). This server's join is fully permissive
        // (no invite/join_rules gate), so bob can join node-b's local room_state
        // directly without waiting on state replication from node-a.
        server_b
            .post(&format!("/_matrix/client/v3/rooms/{room_id}/join"))
            .add_header(auth_b_name.clone(), auth_b_val.clone())
            .await
            .assert_status_ok();

        // Trigger sink creation on both sides by sending a dummy sync on each.
        // This causes ClusterState::sink_for to open the Zenoh subscriber for this room.
        // After this, both sinks are subscribed and the 50 ms pause lets the subscription
        // propagate to the Zenoh gossip layer before we publish.
        let _ = server_a.get("/_matrix/client/v3/sync").await;
        let _ = server_b.get("/_matrix/client/v3/sync").await;

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // ── Step 6: PUT /send on instance-A ────────────────────────────────────
        let send_path =
            format!("/_matrix/client/v3/rooms/{room_id}/send/m.room.message/txn-cluster-0");
        let send_resp = server_a
            .put(&send_path)
            .add_header(auth_a_name.clone(), auth_a_val.clone())
            .json(&json!({
                "msgtype": "m.text",
                "body":    "hello from instance-A (cluster test)"
            }))
            .await;
        send_resp.assert_status_ok();

        let send_body: Value = send_resp.json();
        let event_id = send_body["event_id"]
            .as_str()
            .expect("event_id from A send")
            .to_string();
        assert!(
            event_id.starts_with('$'),
            "event_id must start with '$'; got {event_id}"
        );

        // Zenoh peer-mode gossip on loopback is typically <5 ms; 200 ms is a
        // conservative safety margin matching the mrgd CRDT test.
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        // ── Step 7: GET /sync on instance-B — A's event must have converged ───
        let sync_resp_b = server_b
            .get("/_matrix/client/v3/sync")
            .add_header(auth_b_name.clone(), auth_b_val.clone())
            .await;
        sync_resp_b.assert_status_ok();

        let sync_body_b: Value = sync_resp_b.json();
        let events_b = sync_body_b["rooms"]["join"][&room_id]["timeline"]["events"]
            .as_array()
            .unwrap_or_else(|| {
                panic!("instance-B /sync has no timeline for room {room_id}; body: {sync_body_b}")
            });

        let found_on_b = events_b
            .iter()
            .any(|ev| ev["event_id"].as_str() == Some(event_id.as_str()));
        assert!(
            found_on_b,
            "event_id {event_id} sent on instance-A must appear in instance-B /sync timeline.\n\
             Got events: {events_b:?}"
        );

        let ev_on_b = events_b
            .iter()
            .find(|ev| ev["event_id"].as_str() == Some(event_id.as_str()))
            .expect("event in B timeline");
        assert_eq!(
            ev_on_b["content"]["body"].as_str(),
            Some("hello from instance-A (cluster test)"),
            "message body must survive Zenoh round-trip"
        );
        assert_eq!(
            ev_on_b["sender"].as_str(),
            Some("@alice:node-a"),
            "sender must be preserved through the verified drain"
        );

        // ── (a) reverse direction: PUT /send on instance-B → GET /sync on A ────
        let send_resp_b = server_b
            .put(&send_path)
            .add_header(auth_b_name.clone(), auth_b_val.clone())
            .json(&json!({
                "msgtype": "m.text",
                "body":    "hello from instance-B (cluster test)"
            }))
            .await;
        send_resp_b.assert_status_ok();

        let send_body_b: Value = send_resp_b.json();
        let event_id_b = send_body_b["event_id"]
            .as_str()
            .expect("event_id from B send")
            .to_string();

        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        let sync_resp_a = server_a
            .get("/_matrix/client/v3/sync")
            .add_header(auth_a_name.clone(), auth_a_val.clone())
            .await;
        sync_resp_a.assert_status_ok();
        let sync_body_a: Value = sync_resp_a.json();
        let events_a = sync_body_a["rooms"]["join"][&room_id]["timeline"]["events"]
            .as_array()
            .unwrap_or_else(|| {
                panic!("instance-A /sync has no timeline for room {room_id}; body: {sync_body_a}")
            });
        let found_on_a = events_a
            .iter()
            .any(|ev| ev["event_id"].as_str() == Some(event_id_b.as_str()));
        assert!(
            found_on_a,
            "event_id {event_id_b} sent on instance-B must appear in instance-A /sync timeline \
             (reverse-direction convergence).\nGot events: {events_a:?}"
        );

        // ── (b) forgery: sender claims node-a's namespace, signed by node-b ────
        // The crypto check alone would PASS (node-a already trusts node-b's real key
        // from step 3) — only the sender-binding check (item 5) can catch this.
        use crate::substrate::matrix_events::{Pdu, RoomLogDelta};

        let forged = Pdu::signed(
            room_id.clone(),
            "@x:node-a".to_string(), // claims node-a's namespace
            "m.room.message".to_string(),
            b"{\"msgtype\":\"m.text\",\"body\":\"forged\"}".to_vec(),
            vec![],
            0,
            999_000,
            &state_b.signer, // but signed by node-b
        );
        assert_eq!(
            forged.signer_node, "node-b",
            "sanity: forged PDU is signed by node-b"
        );

        let forged_delta = RoomLogDelta {
            pdus: vec![forged.clone()],
            collected_depth: 0,
        };
        let (accepted, rejected) = {
            let mut rooms = state_a.rooms.lock().expect("rooms lock A (forgery check)");
            let log = rooms.entry(room_id.clone()).or_default();
            log.apply_delta_verified(&forged_delta, &state_a.key_store)
        };
        assert_eq!(accepted, 0, "forged cross-node PDU must NOT be accepted");
        assert!(
            rejected >= 1,
            "forged cross-node PDU must be counted as rejected; got {rejected}"
        );

        // Confirm it never entered node-a's RoomLog.
        let rooms_a = state_a.rooms.lock().expect("rooms lock A (forgery verify)");
        let log_a = rooms_a.get(&room_id).expect("room exists on A");
        assert!(
            log_a
                .ordered()
                .iter()
                .all(|p| p.event_id != forged.event_id),
            "forged event_id must never appear in node-a's RoomLog"
        );
    }

    // cluster:send_publishes_incremental_delta:start
    //   purpose: Prove that PUT /send publishes an INCREMENTAL delta (just the new
    //            PDU) to Zenoh, not the room's entire history. Before this fix,
    //            insert_pdu (routes/send.rs) serialised log.delta() — EVERY PDU the
    //            room has ever had — on every single send, so publish cost grew
    //            O(total room history) per send and O(n^2) over a room's lifetime.
    //            Subscribes directly to the room's raw "events" Zenoh key (bypassing
    //            AppState/ClusterState entirely) so the assertion sees exactly the
    //            bytes insert_pdu published, and checks that every one of N sends
    //            carries exactly 1 PDU — never i+1, which is what the old
    //            log.delta()-based publish would have produced on the i-th send.
    //   input:  none (all resources constructed in-test)
    //   output: each of N published deltas contains exactly 1 PDU
    //   sideEffects: opens two real Zenoh sessions; one raw subscriber on the room's
    //                events key; network I/O on loopback only
    // cluster:send_publishes_incremental_delta:end
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn send_publishes_incremental_delta_not_full_history() {
        let _zg = crate::test_util::ZENOH_TEST_LOCK.acquire().await.unwrap();
        let [sess_a, sess_spy] = crate::test_util::open_mesh().await;

        let prefix = crate::test_util::unique_prefix("mrgd/matrix/room/cluster-test-incr");
        let state_a = AppState::with_cluster(ClusterConfig {
            session: sess_a,
            key_prefix: prefix.to_string(),
            server_name: "node-incr".to_string(),
        });
        let server_a = TestServer::new(router(state_a.clone()));

        let (auth_name, auth_val) = register_and_bearer(&server_a, "carol").await;

        let room_alias = "incr-room-0";
        server_a
            .post("/_matrix/client/v3/createRoom")
            .add_header(auth_name.clone(), auth_val.clone())
            .json(&json!({ "room_alias_name": room_alias }))
            .await
            .assert_status_ok();
        let room_id = format!("!{room_alias}:node-incr");

        // Trigger sink creation (same warm-up pattern as multimaster_a_to_b) so the
        // publish path is fully set up before we start snooping on it.
        let _ = server_a.get("/_matrix/client/v3/sync").await;
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // Subscribe directly to the room's "events" CRDT key from an independent
        // session — bypassing AppState/ClusterState so this sees exactly what
        // insert_pdu published, not a re-derived view.
        // Mirror what the publisher actually writes: the room segment is percent-encoded, so
        // a spy subscribed to the raw id would see nothing and blame the publisher.
        let events_key = format!(
            "{prefix}/{}/events",
            crate::substrate::keyexpr::encode_segment(&room_id)
        );
        let subscriber = sess_spy
            .declare_subscriber(&events_key)
            .await
            .expect("declare_subscriber on events key");
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        const N: usize = 5;
        for i in 0..N {
            let send_path =
                format!("/_matrix/client/v3/rooms/{room_id}/send/m.room.message/txn-incr-{i}");
            server_a
                .put(&send_path)
                .add_header(auth_name.clone(), auth_val.clone())
                .json(&json!({ "msgtype": "m.text", "body": format!("msg{i}") }))
                .await
                .assert_status_ok();

            let sample = tokio::time::timeout(
                tokio::time::Duration::from_millis(500),
                subscriber.recv_async(),
            )
            .await
            .unwrap_or_else(|_| panic!("no publish observed for send #{i}"))
            .expect("subscriber closed unexpectedly");

            let bytes = sample.payload().to_bytes().to_vec();
            let delta = crate::substrate::matrix_events::delta_from_bytes(&bytes)
                .expect("a delta we published ourselves must parse");

            assert_eq!(
                delta.pdus.len(),
                1,
                "send #{i}: published delta must contain exactly 1 PDU (the new \
                 one), got {} — publish is not incremental (this is exactly what \
                 the old log.delta()-based publish would produce: {} PDUs by the \
                 {i}-th send)",
                delta.pdus.len(),
                i + 1,
            );
        }
    }

    // cluster:history_catchup:start
    //   purpose: Prove that a restarted node (node B, starting with no local state)
    //            catches up missed room history from a live peer (node A) via the
    //            room history Zenoh queryable.
    //
    //            Protocol:
    //              Node A has a room with events and declares a history queryable.
    //              Node B (empty) calls the merge_catchup path directly (mirroring what
    //              build_state does at startup) by doing a Zenoh GET to
    //              "<prefix>/<room_id>/history" and merging the reply into its AppState.
    //
    //            The test uses the same loopback session clone pattern as barrier_net.rs
    //            to avoid scouting delays: sess_b = sess_a.clone().
    //
    //   input:  none (all resources constructed in-test)
    //   output: node B's RoomLog and room_timeline contain node A's events after catch-up
    //   sideEffects: opens one Zenoh session (shared via clone); spawns one background task
    // cluster:history_catchup:end
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn history_queryable_catchup() {
        let _zg = crate::test_util::ZENOH_TEST_LOCK.acquire().await.unwrap();
        use crate::substrate::matrix_events::{delta_from_bytes, delta_to_bytes, Pdu};
        use std::time::Duration;

        let prefix = crate::test_util::unique_prefix("mrgd/matrix/room/catchup-test-1");
        let room_id = "!catchup-room-1:localhost";

        // ── Open one Zenoh session, clone for both sides ──────────────────────
        // Cloning shares the same Arc<SessionInner> so queryables declared by A
        // are immediately visible to B's GET (no scouting delay).
        let [sess_root] = crate::test_util::open_mesh().await;
        let sess_a = sess_root.clone();
        let sess_b = sess_root.clone();

        // ── Node A: build a RoomLog with two events ───────────────────────────
        let state_a = AppState::with_cluster(ClusterConfig {
            session: sess_a,
            key_prefix: prefix.to_string(),
            server_name: "catchup-node-a".to_string(),
        });

        // Ensure the room exists on A and add two synthetic PDUs directly.
        {
            state_a.ensure_room_state(room_id);
            let pdu1 = Pdu {
                event_id: "$catchup-ev1".to_string(),
                room_id: room_id.to_string(),
                sender: "@alice:localhost".to_string(),
                kind: "m.room.message".to_string(),
                content: b"\"hello\"".to_vec(),
                prev_events: vec![],
                depth: 0,
                ts: 1000,
                sig: Vec::new(),
                signer_node: String::new(),
            };
            let pdu2 = Pdu {
                event_id: "$catchup-ev2".to_string(),
                room_id: room_id.to_string(),
                sender: "@alice:localhost".to_string(),
                kind: "m.room.message".to_string(),
                content: b"\"world\"".to_vec(),
                prev_events: vec!["$catchup-ev1".to_string()],
                depth: 1,
                ts: 2000,
                sig: Vec::new(),
                signer_node: String::new(),
            };
            let mut rooms = state_a.rooms.lock().expect("rooms lock A");
            let log = rooms.entry(room_id.to_string()).or_default();
            log.add(pdu1);
            log.add(pdu2);
        }

        // ── Node A: declare a history queryable ───────────────────────────────
        let hist_key = format!("{prefix}/{room_id}/history");
        let qable = sess_root
            .declare_queryable(&hist_key)
            .await
            .expect("declare history queryable");

        let handler = qable.handler().clone();
        let state_a2 = state_a.clone();
        let rid_owned = room_id.to_string();
        let hk_owned = hist_key.clone();

        tokio::spawn(async move {
            while let Ok(query) = handler.recv_async().await {
                let delta_bytes: Vec<u8> = {
                    match state_a2.rooms.lock() {
                        Ok(rooms) => {
                            if let Some(log) = rooms.get(&rid_owned) {
                                delta_to_bytes(&log.delta())
                            } else {
                                vec![0u8; 4]
                            }
                        }
                        Err(_) => vec![0u8; 4],
                    }
                };
                let _ = query.reply(&hk_owned, delta_bytes).await;
            }
        });

        // Keep the queryable alive for the test duration.
        let _qable_guard = qable;

        // Brief pause to let the queryable declaration propagate within the shared session.
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // ── Node B: start empty, run catch-up ─────────────────────────────────
        let state_b = AppState::with_cluster(ClusterConfig {
            session: sess_b,
            key_prefix: prefix.to_string(),
            server_name: "catchup-node-b".to_string(),
        });

        // Ensure the room structure exists on B (a restarted node that knew about
        // this room from a previous run but lost its in-memory state).
        state_b.ensure_room_state(room_id);

        // B's RoomLog is empty — verify this.
        {
            let rooms = state_b.rooms.lock().expect("rooms lock B (before)");
            let log = rooms.get(room_id).expect("room exists on B");
            assert!(log.is_empty(), "node B must start with empty RoomLog");
        }

        // Run catch-up: GET the history queryable key from A, merge reply.
        let catchup_timeout = Duration::from_secs(3);
        let replies = sess_root
            .get(&hist_key)
            .timeout(catchup_timeout)
            .await
            .expect("GET history queryable");

        let mut merged = false;
        while let Ok(Ok(reply)) = tokio::time::timeout(catchup_timeout, replies.recv_async()).await
        {
            let sample = match reply.result() {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("catchup reply error: {e}");
                    continue;
                }
            };
            let bytes = sample.payload().to_bytes();
            let Some(delta) = delta_from_bytes(&bytes) else {
                continue;
            };
            if delta.pdus.is_empty() {
                continue;
            }

            // Merge delta into B.
            {
                let mut rooms = state_b.rooms.lock().expect("rooms lock B (merge)");
                let log = rooms.entry(room_id.to_string()).or_default();
                log.apply_delta(&delta);
            }

            // Add to room_timeline.
            {
                use std::sync::atomic::Ordering;
                let mut rt = state_b.room_timeline.lock().expect("rt lock B");
                let timeline = rt.entry(room_id.to_string()).or_default();
                for pdu in &delta.pdus {
                    let already = timeline.iter().any(|(_, ev)| {
                        ev.get("event_id").and_then(|v| v.as_str()) == Some(&pdu.event_id)
                    });
                    if !already {
                        let pos = state_b.stream_pos.fetch_add(1, Ordering::SeqCst);
                        let ev = serde_json::json!({
                            "event_id":         pdu.event_id,
                            "type":             pdu.kind,
                            "sender":           pdu.sender,
                            "room_id":          pdu.room_id,
                            "origin_server_ts": pdu.ts,
                            "content":          serde_json::from_slice::<serde_json::Value>(&pdu.content)
                                                    .unwrap_or(serde_json::json!({}))
                        });
                        timeline.push((pos, ev));
                    }
                }
            }

            merged = true;
            break;
        }

        assert!(
            merged,
            "node B must have received at least one history reply from node A"
        );

        // ── Verify B now has A's events ───────────────────────────────────────
        let rooms_b = state_b.rooms.lock().expect("rooms lock B (verify)");
        let log_b = rooms_b.get(room_id).expect("room in B after catch-up");
        let ordered = log_b.ordered();

        assert_eq!(
            ordered.len(),
            2,
            "node B must have 2 events after catch-up; got {}",
            ordered.len()
        );
        assert!(
            ordered.iter().any(|p| p.event_id == "$catchup-ev1"),
            "node B must have $catchup-ev1 after catch-up"
        );
        assert!(
            ordered.iter().any(|p| p.event_id == "$catchup-ev2"),
            "node B must have $catchup-ev2 after catch-up"
        );

        // Also verify room_timeline reflects the catch-up.
        drop(rooms_b);
        let rt_b = state_b.room_timeline.lock().expect("rt lock B (verify)");
        let timeline = rt_b.get(room_id).expect("timeline in B after catch-up");
        assert!(
            timeline.len() >= 2,
            "node B room_timeline must have ≥2 entries after catch-up; got {}",
            timeline.len()
        );
    }

    // cluster:to_device_cross_node:start
    //   purpose: Prove the real sendToDevice relay (routes/to_device.rs) works CROSS-NODE:
    //            alice on node-a PUTs a to-device message targeting bob's device on
    //            node-b; bob's device is actually synced from node-b (a different node/
    //            AppState/Zenoh identity than alice's). The message must reach bob via the
    //            "__to_device__" Zenoh gossip channel (mirrors the room-event convergence
    //            proof above, same transport primitive — ClusterState::sink_for — just a
    //            different pseudo key). Also proves the since-token GC: a second /sync on
    //            node-b using the first response's next_batch must NOT return the message
    //            again (exactly-once-per-since, per the task's Matrix semantics
    //            requirement), even though the message physically arrived via a different
    //            node than the one serving bob.
    //
    //            Protocol:
    //              1. Two independent Zenoh peer sessions (real loopback scouting, same
    //                 pattern as multimaster_a_to_b_via_http_and_zenoh) back node-a and
    //                 node-b's AppState/ClusterState.
    //              2. register alice on node-a, bob on node-b — distinct server_names, so
    //                 this is a genuine cross-node test, not two names on one node.
    //              3. A throwaway /sync on each server forces ClusterState::sink_for to
    //                 declare the "__to_device__" Zenoh subscriber on BOTH sides before any
    //                 publish happens (Zenoh pub/sub does not replay to late subscribers —
    //                 same reason the history-queryable and multimaster tests warm up
    //                 sinks/queryables first).
    //              4. alice (node-a) PUTs sendToDevice targeting bob's device (node-b).
    //                 node-a's handler enqueues locally (irrelevant here — alice is not the
    //                 target) AND publishes the gossip sample.
    //              5. After the gossip convergence window, bob's first /sync (against
    //                 node-b) drains the gossip sample into node-b's local to_device_queue
    //                 and returns it in to_device.events.
    //              6. bob's second /sync, using since=<first response's next_batch>, gets
    //                 an empty to_device.events — the message was GC'd by drain_to_device.
    //   input:  none (all resources constructed in-test)
    //   output: (5) to_device.events on node-b's first sync contains alice's message with
    //           the expected sender/type/content; (6) node-b's second sync (past the
    //           token) returns to_device.events == []
    //   sideEffects: opens two real Zenoh sessions; background tokio tasks per gossip sink;
    //                network I/O on loopback only
    // cluster:to_device_cross_node:end
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn to_device_cross_node_delivery_and_since_gc() {
        let _zg = crate::test_util::ZENOH_TEST_LOCK.acquire().await.unwrap();
        // ── Two independent Zenoh peer sessions (real scouting, like multimaster test) ──
        let [sess_a, sess_b] = crate::test_util::open_mesh().await;

        let prefix = crate::test_util::unique_prefix("mrgd/matrix/room/cluster-to-device-0");

        let state_a = AppState::with_cluster(ClusterConfig {
            session: sess_a,
            key_prefix: prefix.to_string(),
            server_name: "td-node-a".to_string(),
        });
        let state_b = AppState::with_cluster(ClusterConfig {
            session: sess_b,
            key_prefix: prefix.to_string(),
            server_name: "td-node-b".to_string(),
        });

        let server_a = TestServer::new(router(state_a.clone()));
        let server_b = TestServer::new(router(state_b.clone()));

        // ── register alice on node-a, bob on node-b ────────────────────────────
        let (auth_a_name, auth_a_val) = register_and_bearer(&server_a, "alice").await;
        let (auth_b_name, auth_b_val) = register_and_bearer(&server_b, "bob").await;

        // ── Warm up both "__to_device__" gossip sinks BEFORE publishing ────────
        // Mirrors the multimaster test's "dummy sync on each" warm-up: a subscriber
        // declared after the publish would miss it (Zenoh pub/sub does not replay).
        let _ = server_a
            .get("/_matrix/client/v3/sync")
            .add_header(auth_a_name.clone(), auth_a_val.clone())
            .await;
        let _ = server_b
            .get("/_matrix/client/v3/sync")
            .add_header(auth_b_name.clone(), auth_b_val.clone())
            .await;
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // ── alice (node-a) sends a to-device message to bob's device (node-b) ──
        let send_resp = server_a
            .put("/_matrix/client/v3/sendToDevice/m.room_key/txn-e2ee-0")
            .add_header(auth_a_name.clone(), auth_a_val.clone())
            .json(&json!({
                "messages": {
                    "@bob:td-node-b": {
                        "DEVICE1": {
                            "algorithm":   "m.megolm.v1.aes-sha2",
                            "room_id":     "!somewhere:td-node-a",
                            "session_id":  "sess-xyz",
                            "session_key": "top-secret-session-key"
                        }
                    }
                }
            }))
            .await;
        send_resp.assert_status_ok();

        // Zenoh peer-mode gossip on loopback is typically <5 ms; 200 ms matches the
        // safety margin used by the room-event convergence test above.
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        // ── bob's FIRST /sync (against node-b) — message must have arrived via gossip ──
        let sync1 = server_b
            .get("/_matrix/client/v3/sync")
            .add_header(auth_b_name.clone(), auth_b_val.clone())
            .await;
        sync1.assert_status_ok();
        let sync1_body: Value = sync1.json();

        let events1 = sync1_body["to_device"]["events"]
            .as_array()
            .unwrap_or_else(|| {
                panic!("node-b sync #1 to_device.events must be an array; body: {sync1_body}")
            });
        assert_eq!(
            events1.len(),
            1,
            "node-b sync #1 must contain exactly the one cross-node to-device message; \
             got: {events1:?}"
        );
        assert_eq!(events1[0]["sender"].as_str(), Some("@alice:td-node-a"));
        assert_eq!(events1[0]["type"].as_str(), Some("m.room_key"));
        assert_eq!(
            events1[0]["content"]["session_id"].as_str(),
            Some("sess-xyz"),
            "content must survive the cross-node gossip round-trip intact"
        );

        let next_batch = sync1_body["next_batch"]
            .as_str()
            .expect("next_batch present in sync #1")
            .to_string();

        // ── bob's SECOND /sync, since=<next_batch from #1> — message must be gone ──
        let sync2 = server_b
            .get(&format!("/_matrix/client/v3/sync?since={next_batch}"))
            .add_header(auth_b_name.clone(), auth_b_val.clone())
            .await;
        sync2.assert_status_ok();
        let sync2_body: Value = sync2.json();
        let events2 = sync2_body["to_device"]["events"]
            .as_array()
            .unwrap_or_else(|| {
                panic!("node-b sync #2 to_device.events must be an array; body: {sync2_body}")
            });
        assert!(
            events2.is_empty(),
            "node-b sync #2 (since past the token that already delivered the message) \
             must NOT redeliver it — exactly-once-per-since. Got: {events2:?}"
        );
    }

    // cluster:state_catchup:start
    //   purpose: Prove that a restarted node (node B, starting with no local state)
    //            catches up missed room STATE from a live peer (node A) via the
    //            state Zenoh queryable (Phase 1 P1.1).
    //
    //            Protocol mirrors history_queryable_catchup: node A has room state
    //            and declares a state queryable; node B does a Zenoh GET to
    //            "<prefix>/<room_id>/state", deserialises StateCatchupMsg, and
    //            applies each event via merge_state_catchup → apply_remote_state_event.
    //
    //   input:  none (all resources constructed in-test)
    //   output: node B's room_state contains node A's state events after catch-up
    // cluster:state_catchup:end
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn state_queryable_catchup() {
        let _zg = crate::test_util::ZENOH_TEST_LOCK.acquire().await.unwrap();
        use crate::routes::room_state::StateCatchupMsg;
        use crate::state::StateEvent;
        use serde_json::json;
        use std::time::Duration;

        let prefix = crate::test_util::unique_prefix("mrgd/matrix/room/state-catchup-test");
        let room_id = "!state-catchup-room:localhost";

        let [sess_root] = crate::test_util::open_mesh().await;
        let sess_b = sess_root.clone();

        // ── Node A: room with two state events (m.room.name + m.room.member) ──
        let state_a = AppState::new();
        {
            state_a.ensure_room_state(room_id);
            let name_ev = StateEvent {
                event_type: "m.room.name".to_string(),
                state_key: String::new(),
                sender: "@alice:localhost".to_string(),
                content: json!({"name": "Catch-Up Room"}),
                event_id: "$state-ev-name".to_string(),
                room_id: room_id.to_string(),
                origin_server_ts: 1000,
            };
            let member_ev = StateEvent {
                event_type: "m.room.member".to_string(),
                state_key: "@bob:localhost".to_string(),
                sender: "@bob:localhost".to_string(),
                content: json!({"membership": "join"}),
                event_id: "$state-ev-member".to_string(),
                room_id: room_id.to_string(),
                origin_server_ts: 2000,
            };
            let mut rs = state_a.room_state.lock().expect("room_state lock A");
            rs.insert(room_id.to_string(), vec![name_ev, member_ev]);
        }

        // ── Node A: declare a state queryable ─────────────────────────────────
        let state_key = format!("{prefix}/{room_id}/state");
        let state_for_qable = state_a.clone();
        let rid_for_qable = room_id.to_string();
        let sk_clone = state_key.clone();
        let qable = sess_root
            .declare_queryable(&state_key)
            .await
            .expect("declare state queryable");
        let handler = qable.handler().clone();
        let _qable_task = tokio::spawn(async move {
            while let Ok(query) = handler.recv_async().await {
                let bytes = {
                    let rs = state_for_qable.room_state.lock().expect("room_state qable lock");
                    match rs.get(&rid_for_qable) {
                        Some(events) => {
                            let msg: StateCatchupMsg = events.into();
                            serde_json::to_vec(&msg).unwrap_or_default()
                        }
                        None => Vec::new(),
                    }
                };
                let _ = query.reply(&sk_clone, bytes).await;
            }
        });

        // ── Node B: empty, GET state from A ───────────────────────────────────
        let state_b = AppState::new();
        let replies = sess_b
            .get(&state_key)
            .timeout(Duration::from_secs(3))
            .await
            .expect("GET state queryable");

        let mut merged = false;
        while let Ok(Ok(reply)) =
            tokio::time::timeout(Duration::from_secs(3), replies.recv_async()).await
        {
            let sample = match reply.result() {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("state catchup reply error: {e}");
                    continue;
                }
            };
            let bytes = sample.payload().to_bytes();
            if bytes.is_empty() {
                continue;
            }
            let msg: StateCatchupMsg = serde_json::from_slice(&bytes).expect("parse StateCatchupMsg");
            // Mirror merge_state_catchup: apply each event via apply_remote_state_event.
            for ev in msg.into_events() {
                state_b.apply_remote_state_event(ev).expect("apply_remote_state_event B");
            }
            merged = true;
            break;
        }
        assert!(merged, "node B must have received a state reply from node A");

        // ── Assert node B now has both state events ───────────────────────────
        let rs_b = state_b.room_state.lock().expect("room_state lock B");
        let events_b = rs_b
            .get(room_id)
            .expect("node B must have room_state for the room after catch-up");
        let has_name = events_b.iter().any(|e| {
            e.event_type == "m.room.name"
                && e.content.get("name").and_then(|v| v.as_str()) == Some("Catch-Up Room")
        });
        let has_member = events_b.iter().any(|e| {
            e.event_type == "m.room.member"
                && e.state_key == "@bob:localhost"
                && e.content.get("membership").and_then(|v| v.as_str()) == Some("join")
        });
        assert!(has_name, "node B must have m.room.name after state catch-up; got {events_b:?}");
        assert!(
            has_member,
            "node B must have m.room.member join after state catch-up; got {events_b:?}"
        );
    }

    // cluster:edge_node_three_way:start
    //   purpose: Validate edge-node mode (three independent nodes form a cluster).
    //            This is the Phase 1.2 gate: three nodes send events, all nodes see
    //            convergence. This proves multi-master clustering scales beyond two nodes
    //            and is essential for validating the substrate works across three deployed nodes.
    //
    //   scenario:
    //     1. Three independent Zenoh sessions (node-a, node-b, node-c) on loopback.
    //     2. Three distinct AppState instances with distinct server_names.
    //     3. Key TOFU setup: each node trusts the other two.
    //     4. Register alice@node-a, bob@node-b, charlie@node-c.
    //     5. Create room on node-a; node-b and node-c join the same room_id.
    //     6. Send from node-a, node-b, node-c; verify all nodes see all three events.
    //     7. Verify convergence time is reasonable (~200 ms, Zenoh gossip latency).
    //
    //   validation gate: after 200ms, all three nodes have all three events.
    //
    //   This test directly validates the ROADMAP Phase 1.2 gate:
    //   "power a node off for an hour, bring it back — room, membership, and media
    //   all re-converge. Until this test passes, multi-master ships with an asterisk."
    //   (Not the off/on part, but the same convergence proof across three live nodes.)
    // cluster:edge_node_three_way:end
    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn edge_node_three_way_convergence() {
        let _zg = crate::test_util::ZENOH_TEST_LOCK.acquire().await.unwrap();
        use std::time::Duration;

        let prefix = crate::test_util::unique_prefix("mrgd/matrix/room/edge-node-3way");
        let room_id = "!edge-room:node-a";

        // ── Three independent Zenoh sessions (real scouting on loopback) ────────
        let [sess_a, sess_b, sess_c] = crate::test_util::open_mesh().await;

        // ── Three AppState instances with distinct server_names ──────────────────
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
        let state_c = AppState::with_cluster(ClusterConfig {
            session: sess_c,
            key_prefix: prefix.to_string(),
            server_name: "node-c".to_string(),
        });

        // ── TOFU: each node trusts the other two ──────────────────────────────────
        let key_a = state_a.signer.verifying_key_bytes();
        let key_b = state_b.signer.verifying_key_bytes();
        let key_c = state_c.signer.verifying_key_bytes();

        // Node A trusts B and C
        state_a.key_store.insert("node-b", key_b);
        state_a.key_store.insert("node-c", key_c);

        // Node B trusts A and C
        state_b.key_store.insert("node-a", key_a);
        state_b.key_store.insert("node-c", key_c);

        // Node C trusts A and B
        state_c.key_store.insert("node-a", key_a);
        state_c.key_store.insert("node-b", key_b);

        // ── Build three HTTP servers ─────────────────────────────────────────────
        let server_a = TestServer::new(router(state_a.clone()));
        let server_b = TestServer::new(router(state_b.clone()));
        let server_c = TestServer::new(router(state_c.clone()));

        // ── Register alice, bob, charlie (each on their own node) ────────────────
        let (auth_a_name, auth_a_val) = register_and_bearer(&server_a, "alice").await;
        let (auth_b_name, auth_b_val) = register_and_bearer(&server_b, "bob").await;
        let (auth_c_name, auth_c_val) = register_and_bearer(&server_c, "charlie").await;

        // ── Create room on node-a ────────────────────────────────────────────────
        let create = server_a
            .post("/_matrix/client/v3/createRoom")
            .add_header(auth_a_name.clone(), auth_a_val.clone())
            .json(&json!({
                "room_alias_name": "edge-room",
                "visibility": "public"
            }))
            .await;
        create.assert_status_ok();

        // ── Node-b and node-c join the same room (simulating federation) ──────────
        // ensure_room_state creates the room locally; then /join adds the user to membership.
        state_b.ensure_room_state(room_id);
        state_c.ensure_room_state(room_id);

        // Alice (on node-a) joins her own room.
        server_a
            .post(&format!("/_matrix/client/v3/rooms/{room_id}/join"))
            .add_header(auth_a_name.clone(), auth_a_val.clone())
            .await
            .assert_status_ok();

        // Bob (on node-b) joins via node-b.
        server_b
            .post(&format!("/_matrix/client/v3/rooms/{room_id}/join"))
            .add_header(auth_b_name.clone(), auth_b_val.clone())
            .await
            .assert_status_ok();

        // Charlie (on node-c) joins via node-c.
        server_c
            .post(&format!("/_matrix/client/v3/rooms/{room_id}/join"))
            .add_header(auth_c_name.clone(), auth_c_val.clone())
            .await
            .assert_status_ok();

        // ── Warm up all three nodes' Zenoh subscribers ──────────────────────────
        // Zenoh pub/sub does not replay, so we must have subscribers active BEFORE
        // the first publish. A dummy /sync on each node ensures the room's Zenoh
        // subscriber is opened.
        let _ = server_a
            .get("/_matrix/client/v3/sync")
            .add_header(auth_a_name.clone(), auth_a_val.clone())
            .await;
        let _ = server_b
            .get("/_matrix/client/v3/sync")
            .add_header(auth_b_name.clone(), auth_b_val.clone())
            .await;
        let _ = server_c
            .get("/_matrix/client/v3/sync")
            .add_header(auth_c_name.clone(), auth_c_val.clone())
            .await;

        tokio::time::sleep(Duration::from_millis(50)).await;

        // ── Send from node-a ────────────────────────────────────────────────────
        server_a
            .put(&format!(
                "/_matrix/client/v3/rooms/{room_id}/send/m.room.message/txn-edge-a"
            ))
            .add_header(auth_a_name.clone(), auth_a_val.clone())
            .json(&json!({ "msgtype": "m.text", "body": "from alice on node-a" }))
            .await
            .assert_status_ok();

        // ── Send from node-b ────────────────────────────────────────────────────
        server_b
            .put(&format!(
                "/_matrix/client/v3/rooms/{room_id}/send/m.room.message/txn-edge-b"
            ))
            .add_header(auth_b_name.clone(), auth_b_val.clone())
            .json(&json!({ "msgtype": "m.text", "body": "from bob on node-b" }))
            .await
            .assert_status_ok();

        // ── Send from node-c ────────────────────────────────────────────────────
        server_c
            .put(&format!(
                "/_matrix/client/v3/rooms/{room_id}/send/m.room.message/txn-edge-c"
            ))
            .add_header(auth_c_name.clone(), auth_c_val.clone())
            .json(&json!({ "msgtype": "m.text", "body": "from charlie on node-c" }))
            .await
            .assert_status_ok();

        // ── Wait for Zenoh gossip convergence (200 ms safety margin) ────────────
        tokio::time::sleep(Duration::from_millis(200)).await;

        // ── Verify node-a sees all three events ──────────────────────────────────
        let sync_a = server_a
            .get("/_matrix/client/v3/sync")
            .add_header(auth_a_name.clone(), auth_a_val.clone())
            .await;
        sync_a.assert_status_ok();
        let body_a: Value = sync_a.json();
        let events_a = body_a["rooms"]["join"][room_id]["timeline"]["events"]
            .as_array()
            .expect("node-a must have timeline events");
        assert!(
            events_a.len() >= 3,
            "node-a must see at least 3 events (alice, bob, charlie), got {}",
            events_a.len()
        );

        // ── Verify node-b sees all three events ──────────────────────────────────
        let sync_b = server_b
            .get("/_matrix/client/v3/sync")
            .add_header(auth_b_name.clone(), auth_b_val.clone())
            .await;
        sync_b.assert_status_ok();
        let body_b: Value = sync_b.json();
        let events_b = body_b["rooms"]["join"][room_id]["timeline"]["events"]
            .as_array()
            .expect("node-b must have timeline events");
        assert!(
            events_b.len() >= 3,
            "node-b must see at least 3 events (alice, bob, charlie), got {}",
            events_b.len()
        );

        // ── Verify node-c sees all three events ──────────────────────────────────
        let sync_c = server_c
            .get("/_matrix/client/v3/sync")
            .add_header(auth_c_name.clone(), auth_c_val.clone())
            .await;
        sync_c.assert_status_ok();
        let body_c: Value = sync_c.json();
        let events_c = body_c["rooms"]["join"][room_id]["timeline"]["events"]
            .as_array()
            .expect("node-c must have timeline events");
        assert!(
            events_c.len() >= 3,
            "node-c must see at least 3 events (alice, bob, charlie), got {}",
            events_c.len()
        );

        // ── Sanity check: verify the bodies are correct ──────────────────────────
        let messages_a: Vec<String> = events_a
            .iter()
            .filter_map(|e| {
                e["content"]["body"]
                    .as_str()
                    .map(|s| s.to_string())
            })
            .collect();
        assert!(
            messages_a.iter().any(|m| m.contains("alice")),
            "node-a must see alice's message"
        );
        assert!(
            messages_a.iter().any(|m| m.contains("bob")),
            "node-a must see bob's message"
        );
        assert!(
            messages_a.iter().any(|m| m.contains("charlie")),
            "node-a must see charlie's message"
        );
    }

    // cluster:room_discovery_fresh_node:start
    //   purpose: Prove ROADMAP Phase 1 room discovery — a node that has NEVER heard of a
    //            room picks it up from Zenoh traffic alone, with no room-list gossip and
    //            no local bootstrap.
    //
    //            This is the case every other cluster test skips: they all call
    //            `ensure_room_state` / `/join` on the second node first, so that node
    //            already knows the room_id before any delta arrives. Here node-b does
    //            neither — its only path to the room is
    //            `ClusterState::start_discovery`'s wildcard subscriber, which sees a
    //            sample on `<prefix>/<room_id>/<crdt_key>` for an unknown room, lazily
    //            creates the per-room sink, and `inject`s the payload the sink's own
    //            (too-late) subscriber never saw.
    //
    //            Regression value: without `inject` the first sample for an unknown room
    //            is silently dropped, and without `list_room_ids` in the sync drain the
    //            discovered sink is never drained. Either omission reverts node-b to
    //            permanently blind, which is exactly the pre-port behaviour.
    //   input:  none (all resources constructed in-test)
    //   output: node-b's `rooms` map contains the room_id created on node-a, carrying
    //           node-a's event — despite node-b never being told the room exists
    //   sideEffects: opens two real Zenoh sessions; background tokio tasks; loopback only
    // cluster:room_discovery_fresh_node:end
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn room_discovery_fresh_node_learns_unknown_room() {
        let _zg = crate::test_util::ZENOH_TEST_LOCK.acquire().await.unwrap();

        let [sess_a, sess_b] = crate::test_util::open_mesh().await;

        let prefix = crate::test_util::unique_prefix("mrgd/matrix/room/discovery-test-0");

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

        // TOFU: node-b must trust node-a's signing key, or apply_delta_verified rejects
        // every discovered PDU and the test would pass/fail for the wrong reason.
        state_b
            .key_store
            .insert("node-a", state_a.signer.verifying_key_bytes());
        state_a
            .key_store
            .insert("node-b", state_b.signer.verifying_key_bytes());

        let server_a = TestServer::new(router(state_a.clone()));
        let server_b = TestServer::new(router(state_b.clone()));

        let (auth_a_name, auth_a_val) = register_and_bearer(&server_a, "alice").await;

        // Let both discovery subscribers reach the Zenoh gossip layer before any publish.
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // ── node-a creates a room. node-b is told NOTHING. ────────────────────────
        // No ensure_room_state, no /join, no room_id handed over — node-b's only
        // possible source is the wildcard discovery subscriber.
        let create: Value = server_a
            .post("/_matrix/client/v3/createRoom")
            .add_header(auth_a_name.clone(), auth_a_val.clone())
            .json(&json!({ "name": "discovery-room" }))
            .await
            .json();
        let room_id = create["room_id"]
            .as_str()
            .expect("room_id from createRoom")
            .to_string();

        // Sanity-check the ported unique-id format while we are here: a bare
        // "!room_0:node-a" would be the collision-prone pre-port shape.
        assert!(
            room_id.starts_with("!room_0_") && room_id.ends_with(":node-a"),
            "expected globally-unique room id !room_<seq>_<rnd>_<node>:<server>; got {room_id}"
        );

        // Confirm the premise: node-b genuinely does not know this room yet.
        assert!(
            !state_b
                .rooms
                .lock()
                .expect("rooms lock")
                .contains_key(&room_id),
            "precondition violated: node-b already knows {room_id} before any Zenoh traffic"
        );

        let send_path =
            format!("/_matrix/client/v3/rooms/{room_id}/send/m.room.message/txn-discovery-0");
        let sent: Value = server_a
            .put(&send_path)
            .add_header(auth_a_name.clone(), auth_a_val.clone())
            .json(&json!({
                "msgtype": "m.text",
                "body":    "hello from a room node-b has never heard of"
            }))
            .await
            .json();
        let event_id = sent["event_id"].as_str().expect("event_id").to_string();

        // ── node-b: discover, then drain ──────────────────────────────────────────
        // GET /sync drives drain_cluster_deltas, which unions locally-known rooms with
        // list_room_ids() — the discovered sink is only reachable through the latter.
        //
        // Scope note (measured, not assumed): this asserts on the *timeline* event, which
        // is the room's SECOND publication — createRoom already published state on
        // `<prefix>/<room_id>/state`, and it is that earlier sample which causes node-b
        // to create the sink. So this test covers the discovery subscriber and the
        // list_room_ids() union, but NOT `ZenohCrdtSink::inject` — stubbing inject out
        // leaves this test green. The first-publication path inject exists for is covered
        // by `room_discovery_state_only_room` below (verified: no-oping inject makes that
        // one fail). No separate unit test for inject — it would need its own Zenoh
        // session, and every such test serialises on ZENOH_TEST_LOCK.
        let mut found = false;
        for _ in 0..40 {
            tokio::time::sleep(tokio::time::Duration::from_millis(25)).await;
            let _ = server_b.get("/_matrix/client/v3/sync").await;
            let rooms_b = state_b.rooms.lock().expect("rooms lock");
            if let Some(log) = rooms_b.get(&room_id) {
                if log.ordered().iter().any(|p| p.event_id == event_id) {
                    found = true;
                    break;
                }
            }
        }

        assert!(
            found,
            "node-b never discovered {room_id} / event {event_id} — the wildcard \
             discovery subscriber or the list_room_ids() union in drain_cluster_deltas \
             is not wired up"
        );
    }

    // cluster:room_discovery_state_only:start
    //   purpose: Cover the case the timeline test above cannot: a room whose ONLY Zenoh
    //            traffic is its very first publication. node-a creates a room and sends
    //            nothing, so the single sample for that room is the createRoom state
    //            delta on `<prefix>/<room_id>/state`.
    //
    //            That sample arrives at node-b's wildcard discovery subscriber at a
    //            moment when no per-room sink exists. The sink is created while handling
    //            it — too late for its own subscriber to have seen it — so the payload
    //            survives only if `ZenohCrdtSink::inject` hands it over. Nothing
    //            re-publishes it: with no message ever sent, a dropped first sample means
    //            node-b never learns the room exists.
    //
    //            Also covers the matching union in `drain_cluster_state`, which the
    //            original patch omitted (only the sync drain had it): without it a
    //            discovered room is drained for timeline but never for state.
    //   input:  none
    //   output: node-b's room_state contains the room, with m.room.create from node-a
    //   sideEffects: two real Zenoh sessions; loopback only
    // cluster:room_discovery_state_only:end
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn room_discovery_state_only_room() {
        let _zg = crate::test_util::ZENOH_TEST_LOCK.acquire().await.unwrap();

        let [sess_a, sess_b] = crate::test_util::open_mesh().await;

        let prefix = crate::test_util::unique_prefix("mrgd/matrix/room/discovery-test-1");

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

        state_b
            .key_store
            .insert("node-a", state_a.signer.verifying_key_bytes());
        state_a
            .key_store
            .insert("node-b", state_b.signer.verifying_key_bytes());

        let server_a = TestServer::new(router(state_a.clone()));
        let server_b = TestServer::new(router(state_b.clone()));

        let (auth_a_name, auth_a_val) = register_and_bearer(&server_a, "alice").await;

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // node-a creates a room and sends NOTHING. One publication, on the state channel.
        let create: Value = server_a
            .post("/_matrix/client/v3/createRoom")
            .add_header(auth_a_name.clone(), auth_a_val.clone())
            .json(&json!({ "name": "state-only-room" }))
            .await
            .json();
        let room_id = create["room_id"]
            .as_str()
            .expect("room_id from createRoom")
            .to_string();

        assert!(
            !state_b
                .room_state
                .lock()
                .expect("room_state lock")
                .contains_key(&room_id),
            "precondition violated: node-b already knows {room_id}"
        );

        let mut found = false;
        for _ in 0..40 {
            tokio::time::sleep(tokio::time::Duration::from_millis(25)).await;
            let _ = server_b.get("/_matrix/client/v3/sync").await;
            let rs_b = state_b.room_state.lock().expect("room_state lock");
            if let Some(events) = rs_b.get(&room_id) {
                if events.iter().any(|e| e.event_type == "m.room.create") {
                    found = true;
                    break;
                }
            }
        }

        assert!(
            found,
            "node-b never learned state-only room {room_id}. Its single Zenoh sample \
             arrived before any per-room sink existed, so this fails if \
             ZenohCrdtSink::inject drops it, or if drain_cluster_state does not union \
             cluster.list_room_ids() into the rooms it drains."
        );
    }

    // cluster:catchup_key_parsing:start
    //   purpose: ClusterState::room_from_key is what lets a node name a room it has
    //            never heard of — it reads the room_id off a catch-up REPLY key. If it
    //            mis-parses, a discovered room is silently dropped, so pin the shapes.
    //   input:  none (pure function)
    //   output: assertions on accepted and rejected key shapes
    // cluster:catchup_key_parsing:end
    #[test]
    fn catchup_key_parsing() {
        use crate::state::ClusterState;
        let prefix = "mrgd/matrix/room";

        // The shape a queryable actually replies with, room_id included verbatim.
        assert_eq!(
            ClusterState::room_from_key(
                "mrgd/matrix/room/!abc_1f2e3d4c_node-a:localhost/history",
                prefix,
                "history"
            ),
            Some("!abc_1f2e3d4c_node-a:localhost".to_string()),
            "a room_id may contain '!' ':' '_' '-' and must survive round-tripping"
        );
        assert_eq!(
            ClusterState::room_from_key("mrgd/matrix/room/!r:localhost/state", prefix, "state"),
            Some("!r:localhost".to_string())
        );

        // Wildcards come back as the literal segment; the caller decides what they mean.
        assert_eq!(
            ClusterState::room_from_key("mrgd/matrix/room/*/state", prefix, "state"),
            Some("*".to_string())
        );

        // A key that arrives percent-encoded decodes back to the raw room_id: the
        // serving side compares against its own map, which stores raw ids.
        assert_eq!(
            ClusterState::room_from_key(
                "mrgd/matrix/room/%21%23seed433-chatlong%3Alocalhost/history",
                prefix,
                "history"
            ),
            Some("!#seed433-chatlong:localhost".to_string()),
            "an encoded room segment must decode to the raw room_id it came from"
        );

        // Rejected shapes.
        assert_eq!(
            ClusterState::room_from_key("mrgd/matrix/room/!r:localhost/state", prefix, "history"),
            None,
            "the state channel must not be read as history"
        );
        assert_eq!(
            ClusterState::room_from_key("other/prefix/!r:localhost/state", prefix, "state"),
            None,
            "a key from a different cluster prefix is not ours"
        );
        assert_eq!(
            ClusterState::room_from_key("mrgd/matrix/room/a/b/state", prefix, "state"),
            None,
            "a room_id is exactly one chunk — 'a/b' is a different key layout"
        );
        assert_eq!(
            ClusterState::room_from_key("mrgd/matrix/room//state", prefix, "state"),
            None,
            "empty room_id"
        );
    }

    // cluster:catchup_wildcard_query_scope:start
    //   purpose: ClusterState::rooms_for_query decides what a catch-up queryable answers.
    //            The wildcard case is the fix: it is evaluated against live state, so a
    //            room created after this node started is still served. Per-room
    //            queryables declared from a startup snapshot could not do that.
    //   input:  none (pure function)
    //   output: assertions on wildcard, concrete-hit, concrete-miss and malformed keys
    // cluster:catchup_wildcard_query_scope:end
    #[test]
    fn catchup_wildcard_query_scope() {
        use crate::state::ClusterState;
        let prefix = "mrgd/matrix/room";
        let local = || vec!["!a:localhost".to_string(), "!b:localhost".to_string()];

        // Wildcard: everything this node has, whenever it was created.
        assert_eq!(
            ClusterState::rooms_for_query("mrgd/matrix/room/*/history", prefix, "history", local()),
            local(),
            "a wildcard query must be answered for every locally-known room"
        );

        // Concrete and known: just that room.
        assert_eq!(
            ClusterState::rooms_for_query(
                "mrgd/matrix/room/!b:localhost/history",
                prefix,
                "history",
                local()
            ),
            vec!["!b:localhost".to_string()]
        );

        // Concrete and unknown: nothing — do not invent a reply for a room we lack.
        assert!(ClusterState::rooms_for_query(
            "mrgd/matrix/room/!zz:localhost/history",
            prefix,
            "history",
            local()
        )
        .is_empty());

        // Unparseable: answer generously. The reply key names the room, so a querier
        // can always tell what it got; staying silent would just stall convergence.
        assert_eq!(
            ClusterState::rooms_for_query("garbage", prefix, "history", local()),
            local()
        );
    }

    // cluster:catchup_room_created_after_startup:start
    //   purpose: Prove the seam between discovery and catch-up is closed: a node
    //            recovers a room that was created on a peer AFTER that peer started,
    //            and that has produced no live traffic since.
    //
    //            This is the case neither half used to cover. Startup catch-up asked
    //            only about rooms it already knew, and the peer only ever declared
    //            queryables for rooms present in its own startup replay — so a room
    //            created later was not served to anyone, whatever the querier asked.
    //            Live discovery does not help either: there is no traffic to observe.
    //
    //            Node A declares ONE wildcard state queryable, backed by the real
    //            ClusterState::rooms_for_query, and only THEN creates the room. Node B
    //            starts empty, is never told the room_id, and issues one wildcard GET,
    //            naming what comes back with the real ClusterState::room_from_key.
    //
    //            Caveat, same as state_queryable_catchup: the Zenoh plumbing is
    //            mirrored from main.rs rather than called, because it lives in the
    //            binary. The two decision functions are the production ones.
    //
    //   input:  none (all resources constructed in-test)
    //   output: node B holds the room's state, keyed by a room_id it learned from the
    //           reply itself
    // cluster:catchup_room_created_after_startup:end
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn catchup_room_created_after_startup() {
        let _zg = crate::test_util::ZENOH_TEST_LOCK.acquire().await.unwrap();
        use crate::routes::room_state::StateCatchupMsg;
        use crate::state::{ClusterState, StateEvent};
        use serde_json::json;
        use std::time::Duration;

        let prefix = crate::test_util::unique_prefix("mrgd/matrix/room/wildcard-catchup-test");
        let room_id = "!created-after-startup:node-a";

        let [sess_root] = crate::test_util::open_mesh().await;
        let sess_b = sess_root.clone();

        // ── Node A: running, and at this point knows no rooms at all ──────────
        let state_a = AppState::new();
        assert!(
            state_a
                .room_state
                .lock()
                .expect("room_state lock A")
                .is_empty(),
            "precondition: node A must start with no rooms, so the room below is \
             unambiguously created after its queryable exists"
        );

        // ── Node A: one wildcard state queryable, declared BEFORE the room ────
        let state_wild = format!("{prefix}/*/state");
        let qable = sess_root
            .declare_queryable(&state_wild)
            .await
            .expect("declare wildcard state queryable");
        let handler = qable.handler().clone();
        let state_for_qable = state_a.clone();
        let prefix_for_qable = prefix.to_string();
        let _qable_task = tokio::spawn(async move {
            while let Ok(query) = handler.recv_async().await {
                let known: Vec<String> = {
                    let rs = state_for_qable
                        .room_state
                        .lock()
                        .expect("room_state qable lock");
                    rs.keys().cloned().collect()
                };
                let wanted = ClusterState::rooms_for_query(
                    query.key_expr().as_str(),
                    &prefix_for_qable,
                    "state",
                    known,
                );
                for rid in wanted {
                    let bytes = {
                        let rs = state_for_qable
                            .room_state
                            .lock()
                            .expect("room_state qable lock");
                        match rs.get(&rid) {
                            Some(events) => {
                                let msg: StateCatchupMsg = events.into();
                                serde_json::to_vec(&msg).unwrap_or_default()
                            }
                            None => Vec::new(),
                        }
                    };
                    let reply_key = format!("{prefix_for_qable}/{rid}/state");
                    let _ = query.reply(&reply_key, bytes).await;
                }
            }
        });

        // ── Only NOW does the room exist on node A ────────────────────────────
        state_a.ensure_room_state(room_id);
        {
            let name_ev = StateEvent {
                event_type: "m.room.name".to_string(),
                state_key: String::new(),
                sender: "@alice:node-a".to_string(),
                content: json!({"name": "Created After Startup"}),
                event_id: "$after-startup-name".to_string(),
                room_id: room_id.to_string(),
                origin_server_ts: 1000,
            };
            let mut rs = state_a.room_state.lock().expect("room_state lock A");
            rs.insert(room_id.to_string(), vec![name_ev]);
        }

        // ── Node B: empty, and never told the room_id ─────────────────────────
        let state_b = AppState::new();
        assert!(
            !state_b
                .room_state
                .lock()
                .expect("room_state lock B")
                .contains_key(room_id),
            "precondition: node B must not already know {room_id}"
        );

        let replies = sess_b
            .get(&state_wild)
            .timeout(Duration::from_secs(3))
            .await
            .expect("wildcard GET");

        let mut learned: Vec<String> = Vec::new();
        while let Ok(Ok(reply)) =
            tokio::time::timeout(Duration::from_secs(3), replies.recv_async()).await
        {
            let sample = match reply.result() {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("wildcard catch-up reply error: {e}");
                    continue;
                }
            };
            // The room names itself here — B had no way to know it otherwise.
            let reply_key = sample.key_expr().as_str().to_string();
            let Some(rid) = ClusterState::room_from_key(&reply_key, &prefix, "state") else {
                continue;
            };
            let bytes = sample.payload().to_bytes();
            if bytes.is_empty() {
                continue;
            }
            let msg: StateCatchupMsg = serde_json::from_slice(&bytes).expect("parse");
            for ev in msg.into_events() {
                state_b
                    .apply_remote_state_event(ev)
                    .expect("apply_remote_state_event B");
            }
            learned.push(rid.to_string());
        }

        assert!(
            learned.iter().any(|r| r == room_id),
            "node B never learned {room_id}. It was created after node A's queryable \
             was declared, so this fails if rooms_for_query stops answering wildcards \
             from live state, or if room_from_key cannot name a room from a reply key. \
             Learned: {learned:?}"
        );

        let rs_b = state_b.room_state.lock().expect("room_state lock B");
        let events = rs_b
            .get(room_id)
            .unwrap_or_else(|| panic!("node B has no state for {room_id}"));
        assert!(
            events
                .iter()
                .any(|e| e.event_type == "m.room.name"
                    && e.content.get("name").and_then(|v| v.as_str())
                        == Some("Created After Startup")),
            "node B learned the room_id but not its state: {events:?}"
        );
    }
    // cluster:redaction_replicates:start
    //   purpose: A redaction issued on one node must take effect on every node. It
    //            did not: `redacts` was attached to the client event AFTER the Pdu
    //            was built, and a Pdu carries only content — so the redaction
    //            replicated as an m.room.redaction that named no target, and every
    //            other node went on serving the original message body. "Delete this
    //            message" worked on whichever node you happened to be talking to.
    //
    //            Now the target rides in content as well (room version 11 puts it
    //            there anyway), and the receiving drain records it.
    //
    //   input:  none (all resources constructed in-test)
    //   output: node B's /sync shows the target masked (content {}, with
    //           redacted_because) while an unrelated message in the room is untouched
    //   sideEffects: opens two real Zenoh sessions
    // cluster:redaction_replicates:end
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn redaction_replicates_to_other_nodes() {
        let _zg = crate::test_util::ZENOH_TEST_LOCK.acquire().await.unwrap();

        let prefix = crate::test_util::unique_prefix("mrgd/matrix/room/redaction-cluster-test");
        let [sess_a, sess_b] = crate::test_util::open_mesh().await;

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
        state_a
            .key_store
            .insert("node-b", state_b.signer.verifying_key_bytes());
        state_b
            .key_store
            .insert("node-a", state_a.signer.verifying_key_bytes());

        let server_a = TestServer::new(router(state_a.clone()));
        let server_b = TestServer::new(router(state_b.clone()));
        let (an, av) = register_and_bearer(&server_a, "alice").await;
        let (bn, bv) = register_and_bearer(&server_b, "bob").await;

        let room_alias = "redaction-cluster-room";
        server_a
            .post("/_matrix/client/v3/createRoom")
            .add_header(an.clone(), av.clone())
            .json(&json!({ "room_alias_name": room_alias }))
            .await
            .assert_status_ok();
        let room_id = format!("!{room_alias}:node-a");
        state_b.ensure_room_state(&room_id);
        server_b
            .post(&format!("/_matrix/client/v3/rooms/{room_id}/join"))
            .add_header(bn.clone(), bv.clone())
            .await
            .assert_status_ok();

        // Open both sinks before publishing anything.
        let _ = server_a.get("/_matrix/client/v3/sync").await;
        let _ = server_b.get("/_matrix/client/v3/sync").await;
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // alice sends two messages on node-a, then redacts the first.
        let mut ids = Vec::new();
        for (i, body) in ["secret", "kept"].iter().enumerate() {
            let resp = server_a
                .put(&format!(
                    "/_matrix/client/v3/rooms/{room_id}/send/m.room.message/txn-red-{i}"
                ))
                .add_header(an.clone(), av.clone())
                .json(&json!({ "msgtype": "m.text", "body": body }))
                .await;
            resp.assert_status_ok();
            let b: Value = resp.json();
            ids.push(b["event_id"].as_str().expect("event_id").to_string());
        }
        let target = ids[0].clone();

        server_a
            .put(&format!(
                "/_matrix/client/v3/rooms/{room_id}/redact/{target}/txn-red-x"
            ))
            .add_header(an.clone(), av.clone())
            .json(&json!({ "reason": "cross-node" }))
            .await
            .assert_status_ok();

        // Let the message deltas AND the redaction delta reach node-b.
        let mut masked = false;
        let mut last_seen = Value::Null;
        for _ in 0..40 {
            tokio::time::sleep(tokio::time::Duration::from_millis(25)).await;
            let body: Value = server_b
                .get("/_matrix/client/v3/sync")
                .add_header(bn.clone(), bv.clone())
                .await
                .json();
            let events = body["rooms"]["join"][&room_id]["timeline"]["events"]
                .as_array()
                .cloned()
                .unwrap_or_default();
            if let Some(ev) = events.iter().find(|e| e["event_id"] == target.as_str()) {
                last_seen = ev.clone();
                if ev["content"] == json!({}) {
                    masked = true;
                    break;
                }
            }
        }

        assert!(
            masked,
            "node B must mask the event alice redacted on node A. A redaction only \
             carries its target in content over the wire, so this fails if redact.rs \
             stops putting `redacts` there, or if the cluster drain stops recording it. \
             Last seen on B: {last_seen}"
        );
        assert!(
            last_seen["unsigned"]["redacted_because"].is_object(),
            "the masked event must carry redacted_because on node B too: {last_seen}"
        );

        // The other message must be untouched — a redaction masks one event, not a room.
        let body: Value = server_b
            .get("/_matrix/client/v3/sync")
            .add_header(bn.clone(), bv.clone())
            .await
            .json();
        let events = body["rooms"]["join"][&room_id]["timeline"]["events"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        let kept = events
            .iter()
            .find(|e| e["event_id"] == ids[1].as_str())
            .expect("the second message must have replicated to B");
        assert_eq!(
            kept["content"]["body"], "kept",
            "an unrelated message must not be masked: {kept}"
        );
    }
    // cluster:gc_not_refilled_by_peer:start
    //   purpose: The reason garbage collection on a grow-only set is hard: the peer
    //            that has not collected still holds everything, and hands it back at
    //            the first opportunity — a live delta, or a catch-up reply carrying the
    //            whole room. Without the depth watermark a collected node refills as
    //            fast as it prunes, and with the mid-life re-query now running on a
    //            timer it would refill on a schedule.
    //
    //            Node B collects; node A (which has not) keeps talking to it. B must
    //            take the NEW event and refuse the old ones.
    //
    //   input:  none (all resources constructed in-test)
    //   output: B stays collected across a live delta and a full-history delta, while
    //           still accepting the event sent after collection
    //   sideEffects: opens two real Zenoh sessions
    // cluster:gc_not_refilled_by_peer:end
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn gc_not_refilled_by_peer() {
        let _zg = crate::test_util::ZENOH_TEST_LOCK.acquire().await.unwrap();

        let prefix = crate::test_util::unique_prefix("mrgd/matrix/room/gc-cluster-test");
        let [sess_a, sess_b] = crate::test_util::open_mesh().await;

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
        state_a
            .key_store
            .insert("node-b", state_b.signer.verifying_key_bytes());
        state_b
            .key_store
            .insert("node-a", state_a.signer.verifying_key_bytes());

        let server_a = TestServer::new(router(state_a.clone()));
        let server_b = TestServer::new(router(state_b.clone()));
        let (an, av) = register_and_bearer(&server_a, "alice").await;
        let (bn, bv) = register_and_bearer(&server_b, "bob").await;

        let alias = "gc-cluster-room";
        server_a
            .post("/_matrix/client/v3/createRoom")
            .add_header(an.clone(), av.clone())
            .json(&json!({ "room_alias_name": alias }))
            .await
            .assert_status_ok();
        let room_id = format!("!{alias}:node-a");
        state_b.ensure_room_state(&room_id);
        server_b
            .post(&format!("/_matrix/client/v3/rooms/{room_id}/join"))
            .add_header(bn.clone(), bv.clone())
            .await
            .assert_status_ok();

        let _ = server_a.get("/_matrix/client/v3/sync").await;
        let _ = server_b.get("/_matrix/client/v3/sync").await;
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // A sends six; wait for B to have them all.
        for i in 0..6 {
            server_a
                .put(&format!(
                    "/_matrix/client/v3/rooms/{room_id}/send/m.room.message/gc-tx-{i}"
                ))
                .add_header(an.clone(), av.clone())
                .json(&json!({ "msgtype": "m.text", "body": format!("old {i}") }))
                .await
                .assert_status_ok();
        }
        let mut b_len = 0usize;
        for _ in 0..40 {
            tokio::time::sleep(tokio::time::Duration::from_millis(25)).await;
            let _ = server_b
                .get("/_matrix/client/v3/sync")
                .add_header(bn.clone(), bv.clone())
                .await;
            b_len = state_b
                .rooms
                .lock()
                .unwrap()
                .get(&room_id)
                .map(|l| l.len())
                .unwrap_or(0);
            if b_len >= 6 {
                break;
            }
        }
        assert!(
            b_len >= 6,
            "precondition: node B must first receive the six events, got {b_len}"
        );

        // ── B collects, A does not ────────────────────────────────────────────
        let dropped = state_b.collect_room_log_to(&room_id, 2);
        assert!(dropped > 0, "B must actually collect something");
        let after_collect = state_b.rooms.lock().unwrap().get(&room_id).unwrap().len();
        let watermark = state_b
            .rooms
            .lock()
            .unwrap()
            .get(&room_id)
            .unwrap()
            .collected_depth();
        assert!(watermark > 0, "collecting must raise the watermark");

        // ── A sends one more; B must take it and only it ──────────────────────
        server_a
            .put(&format!(
                "/_matrix/client/v3/rooms/{room_id}/send/m.room.message/gc-tx-new"
            ))
            .add_header(an.clone(), av.clone())
            .json(&json!({ "msgtype": "m.text", "body": "after the collection" }))
            .await
            .assert_status_ok();

        let mut got_new = false;
        for _ in 0..40 {
            tokio::time::sleep(tokio::time::Duration::from_millis(25)).await;
            let _ = server_b
                .get("/_matrix/client/v3/sync")
                .add_header(bn.clone(), bv.clone())
                .await;
            let rooms = state_b.rooms.lock().unwrap();
            let log = rooms.get(&room_id).unwrap();
            if log
                .ordered()
                .iter()
                .any(|p| String::from_utf8_lossy(&p.content).contains("after the collection"))
            {
                got_new = true;
                break;
            }
        }
        assert!(got_new, "B must still accept events sent AFTER it collected");

        let len_now = state_b.rooms.lock().unwrap().get(&room_id).unwrap().len();
        assert_eq!(
            len_now,
            after_collect + 1,
            "B gained exactly the one new event. Anything more means node A's traffic              put collected history back — the failure the watermark exists to prevent."
        );

        // ── and the hard case: A offers its ENTIRE history, as catch-up does ───
        let full = state_a.rooms.lock().unwrap().get(&room_id).unwrap().delta();
        assert!(
            full.pdus.len() > len_now,
            "precondition: A still holds more than B does"
        );
        {
            let mut rooms_b = state_b.rooms.lock().unwrap();
            let log_b = rooms_b.get_mut(&room_id).unwrap();
            log_b.apply_delta_verified(&full, &state_b.key_store);
        }
        let len_after_full = state_b.rooms.lock().unwrap().get(&room_id).unwrap().len();
        assert_eq!(
            len_after_full, len_now,
            "a full-history delta from an uncollected peer — exactly what a catch-up \
             pass delivers — must not refill node B"
        );
    }

    // obfs:locator_is_dispatchable_and_psk_gates_the_link:start
    //   purpose: Guard the [patch.crates-io] entries in Cargo.toml that swap in the
    //            bsdOS zenoh-link fork. Zenoh 1.x has no runtime link registry, so a
    //            new transport is only reachable if the dispatcher was compiled in;
    //            drop those two lines and every `obfs/` locator silently becomes an
    //            unknown scheme. Also asserts the property the scope-separation record (kept private) §5 leans
    //            on: the PSK, not the address, is what admits a peer.
    //   input:  a session listening on an obfs locator with an inline PSK, then a
    //           peer dialling the same address with a DIFFERENT PSK
    //   output: the listener opens (dispatcher present); the mismatched peer
    //           exchanges nothing
    //   sideEffects: binds a loopback port; opens real Zenoh sessions
    // obfs:locator_is_dispatchable_and_psk_gates_the_link:end
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn obfs_locator_is_dispatchable_and_psk_gates_the_link() {
        let _zg = crate::test_util::ZENOH_TEST_LOCK.acquire().await.unwrap();

        // Two distinct 32-byte PSKs, base64 as the endpoint config expects.
        use base64::{engine::general_purpose::STANDARD, Engine as _};
        let psk_ok = STANDARD.encode([7u8; 32]);
        let psk_bad = STANDARD.encode([9u8; 32]);
        let port = 7473;
        let key = "obfstest/dispatch";

        // NOTE the `#`, not `?`: zenoh separates endpoint *config* with `#` and
        // endpoint *metadata* with `?`, and the PSK is config. With `?` the
        // listener binds and then fails PSK load at first use — which looks like
        // a working obfs endpoint right up until nothing can connect to it.
        let listen = format!("obfs/127.0.0.1:{port}#obfs_psk_base64={psk_ok}");
        let mut cfg_a = zenoh::Config::default();
        cfg_a
            .insert_json5("listen/endpoints", &format!("[\"{listen}\"]"))
            .expect("obfs listen endpoint must be accepted — if this fails, the \
                     zenoh-link patch in Cargo.toml is missing");
        // Scouting off so the only possible path is the obfs link under test;
        // with it on, a peer can converge over a loopback link we are not testing.
        cfg_a.insert_json5("scouting/multicast/enabled", "false").unwrap();
        cfg_a.insert_json5("scouting/gossip/enabled", "false").unwrap();
        let sess_a = zenoh::open(cfg_a)
            .await
            .expect("session with an obfs listener must open");

        // A peer with the WRONG PSK: same address, same key expression.
        let mut cfg_bad = zenoh::Config::default();
        cfg_bad
            .insert_json5(
                "connect/endpoints",
                &format!("[\"obfs/127.0.0.1:{port}#obfs_psk_base64={psk_bad}\"]"),
            )
            .unwrap();
        cfg_bad.insert_json5("scouting/multicast/enabled", "false").unwrap();
        cfg_bad.insert_json5("scouting/gossip/enabled", "false").unwrap();
        let sess_bad = zenoh::open(cfg_bad).await.expect("session opens");

        let sub = sess_a.declare_subscriber(key).await.expect("subscriber");
        tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
        sess_bad.put(key, b"from the wrong key".to_vec()).await.ok();
        tokio::time::sleep(std::time::Duration::from_millis(1500)).await;

        assert!(
            sub.try_recv().ok().flatten().is_none(),
            "a peer holding the wrong PSK must not be able to publish into this \
             session — the PSK is the scope boundary (the scope-separation record (kept private) §6 interlock 5)"
        );

        sess_bad.close().await.ok();
        sess_a.close().await.ok();
    }

}
