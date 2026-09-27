// START_AI_HEADER
// MODULE: matrix-hs/src/two_pool_demo_test.rs
// PURPOSE: The two-pool demo (ROADMAP Phase 3 item 4) as a live integration
//          test: TWO operators, one on each node, each with an application-
//          service namespace (the agent socket, Case 2), sharing ONE room
//          across the mesh. Proves the server-side E2EE story between pools:
//            - tenants created + workers-as-devices via AS (no passwords)
//            - cross-pool OTK bootstrap: pool B's worker claims pool A
//              worker's OTK over the mesh (routed claim)
//            - m.room.encrypted timeline traffic converges to both nodes
//            - sendToDevice m.room.encrypted delivers A→B
//            - device-list changes of A's workers are visible on B (gossip)
//            - trust boundary: each pool's AS token is worthless on the other
//              node, and worker tokens are not AS tokens
//          node_auth between the pools is the mesh's own signing layer —
//          proven separately (forgery + anchor tests in cluster_test.rs);
//          this test runs on top of it.
//          Known limit exercised honestly: device_keys themselves are
//          node-local (ROADMAP multi-master asterisk) — keys/query for the
//          other pool's user returns empty HERE, which is why the OTK route
//          (a different, replicated path) is what E2EE bootstrap leans on.
// DEPENDENCIES: axum-test, zenoh, matrix_hs::{router, AppState, state::AppServiceConfig}
// END_AI_HEADER

#[cfg(all(test, feature = "cluster"))]
mod tests {
    use crate::state::{AppServiceConfig, ClusterConfig};
    use crate::{router, AppState};
    use axum_test::TestServer;
    use serde_json::{json, Value};

    // twopool:helpers:start
    //   purpose: Shared helpers for the two-pool demo: bearer header builder,
    //            AS register (one call), AS worker login (per device).
    //   input:  see per-fn docs
    //   output: see per-fn docs
    //   sideEffects: AS calls mutate server state (create users/sessions)
    // twopool:helpers:end
    fn hdr(token: &str) -> (axum::http::HeaderName, axum::http::HeaderValue) {
        (
            axum::http::HeaderName::from_static("authorization"),
            axum::http::HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
        )
    }

    // as_register_tenant:start
    //   purpose: One-call AS registration of a pool's tenant account.
    //   input:  server, as_token, username (must be in the AS namespace)
    //   output: the minted user access_token
    //   sideEffects: creates the account
    // as_register_tenant:end
    async fn as_register_tenant(server: &TestServer, as_token: &str, username: &str) -> String {
        let resp = server
            .post("/_matrix/client/v3/register")
            .add_header(hdr(as_token).0, hdr(as_token).1)
            .json(&json!({ "username": username }))
            .await;
        let body: Value = resp.json();
        assert_eq!(
            body["access_token"].as_str().map(|s| s.starts_with("mxt_")),
            Some(true),
            "tenant register must mint a token: {body}"
        );
        body["access_token"].as_str().unwrap().to_string()
    }

    // as_worker_login:start
    //   purpose: Passwordless per-device session — the workers-as-devices call.
    //   input:  server, as_token, mxid, device_id
    //   output: worker session token
    //   sideEffects: none (session minted from the existing account)
    // as_worker_login:end
    async fn as_worker_login(
        server: &TestServer,
        as_token: &str,
        user_id: &str,
        device: &str,
    ) -> String {
        let resp = server
            .post("/_matrix/client/v3/login")
            .add_header(hdr(as_token).0, hdr(as_token).1)
            .json(&json!({
                "type": "m.login.application_service",
                "user_id": user_id,
                "device_id": device,
            }))
            .await;
        let body: Value = resp.json();
        assert_eq!(
            body["device_id"].as_str(),
            Some(device),
            "worker session must carry its device: {body}"
        );
        body["access_token"].as_str().unwrap().to_string()
    }

    // twopool:main:start
    //   purpose: The two-pool demo end-to-end (see module header). Two nodes,
    //            two AS namespaces, one shared room, E2EE bootstrap + encrypted
    //            traffic + trust-boundary negatives.
    //   input:  none (all resources constructed in-test)
    //   output: assertions (see steps)
    //   sideEffects: opens two real Zenoh sessions; background tasks per room sink
    // twopool:main:end
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn two_pools_one_room_e2ee_and_trust_boundary() {
        let _zg = crate::test_util::ZENOH_TEST_LOCK.acquire().await.unwrap();
        let [sess_a, sess_b] = crate::test_util::open_mesh().await;
        let prefix = crate::test_util::unique_prefix("mrgd/matrix/room/two-pool-demo");

        // ── Two operators, two namespaces, one node each ──────────────────────
        let state_a = AppState::with_appservice(
            AppState::with_cluster(ClusterConfig {
                session: sess_a,
                key_prefix: prefix.clone(),
                server_name: "poola".to_string(),
            }),
            AppServiceConfig {
                token: "poola-as-token".to_string(),
                prefix: "poola_".to_string(),
            },
        );
        let state_b = AppState::with_appservice(
            AppState::with_cluster(ClusterConfig {
                session: sess_b,
                key_prefix: prefix.clone(),
                server_name: "poolb".to_string(),
            }),
            AppServiceConfig {
                token: "poolb-as-token".to_string(),
                prefix: "poolb_".to_string(),
            },
        );

        // Simulate completed TOFU key distribution (same as the multimaster
        // test: the announce/drain loop lives in the binary, not the lib).
        state_a
            .key_store
            .insert("poolb", state_b.signer.verifying_key_bytes());
        state_b
            .key_store
            .insert("poola", state_a.signer.verifying_key_bytes());

        // The OTK-claim queryables (what makes cross-pool E2EE bootstrap routable).
        crate::routes::keys::serve_otk_claims(state_a.clone())
            .await
            .expect("serve_otk_claims poola");
        crate::routes::keys::serve_otk_claims(state_b.clone())
            .await
            .expect("serve_otk_claims poolb");
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let server_a = TestServer::new(router(state_a.clone()));
        let server_b = TestServer::new(router(state_b.clone()));

        // ── Step 1: tenants, one call each, no passwords anywhere ─────────────
        let tenant_a = as_register_tenant(&server_a, "poola-as-token", "poola_fleet").await;
        let tenant_b = as_register_tenant(&server_b, "poolb-as-token", "poolb_fleet").await;

        // ── Step 2: two workers per pool = devices of the tenant account ─────
        let worker_a1 = as_worker_login(&server_a, "poola-as-token", "@poola_fleet:poola", "PA-WORKER-1").await;
        let worker_b1 = as_worker_login(&server_b, "poolb-as-token", "@poolb_fleet:poolb", "PB-WORKER-1").await;

        // Same account, same identity — the worker IS a device.
        for (srv, tok, expect) in [
            (&server_a, &worker_a1, "@poola_fleet:poola"),
            (&server_b, &worker_b1, "@poolb_fleet:poolb"),
        ] {
            let who: Value = srv
                .get("/_matrix/client/v3/account/whoami")
                .add_header(hdr(tok).0, hdr(tok).1)
                .await
                .json();
            assert_eq!(who["user_id"], expect, "worker speaks for its tenant: {who}");
        }

        // ── Step 3: workers publish key material (OTKs; device_keys local) ───
        server_a
            .post("/_matrix/client/v3/keys/upload")
            .add_header(hdr(&worker_a1).0, hdr(&worker_a1).1)
            .json(&json!({
                "one_time_keys": {
                    "signed_curve25519:POOLA1": { "key": "POOLA-OTK-1" }
                }
            }))
            .await
            .assert_status_ok();
        server_b
            .post("/_matrix/client/v3/keys/upload")
            .add_header(hdr(&worker_b1).0, hdr(&worker_b1).1)
            .json(&json!({
                "one_time_keys": {
                    "signed_curve25519:POOLB1": { "key": "POOLB-OTK-1" }
                }
            }))
            .await
            .assert_status_ok();

        // ── Step 4: the shared room — created by pool A, joined by pool B ────
        server_a
            .post("/_matrix/client/v3/createRoom")
            .add_header(hdr(&tenant_a).0, hdr(&tenant_a).1)
            .json(&json!({ "room_alias_name": "twopool" }))
            .await
            .assert_status_ok();
        let room_id = "!twopool:poola";
        state_b.ensure_room_state(room_id);
        server_b
            .post(&format!("/_matrix/client/v3/rooms/{room_id}/join"))
            .add_header(hdr(&tenant_b).0, hdr(&tenant_b).1)
            .await
            .assert_status_ok();

        // Warm-up syncs (sink creation on both sides — the documented pattern).
        let _ = server_a.get("/_matrix/client/v3/sync").await;
        let _ = server_b.get("/_matrix/client/v3/sync").await;
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // ── Step 5: cross-pool OTK bootstrap over the mesh ────────────────────
        // pool B's worker claims pool A's worker OTK FROM NODE B: the claim is
        // routed to the owning node and the exact key comes back. This is the
        // server-side half of "pool B encrypts to pool A".
        let claim: Value = server_b
            .post("/_matrix/client/v3/keys/claim")
            .add_header(hdr(&worker_b1).0, hdr(&worker_b1).1)
            .json(&json!({
                "one_time_keys": { "@poola_fleet:poola": { "PA-WORKER-1": "signed_curve25519" } }
            }))
            .await
            .json();
        assert_eq!(
            claim["one_time_keys"]["@poola_fleet:poola"]["PA-WORKER-1"]
                ["signed_curve25519:POOLA1"],
            json!({ "key": "POOLA-OTK-1" }),
            "pool B must bootstrap E2EE with pool A's OTK, routed over the mesh: {claim}"
        );

        // ── Step 6: encrypted room traffic converges to both pools ────────────
        // The server relays m.room.encrypted content opaque; convergence is the
        // claim under test (node_auth signatures cover it on the wire). Sync
        // retries: the first sync drains the mesh inbox AFTER assembling its
        // own response, so the event lands on the second poll (same retry
        // pattern as the multimaster tests).
        server_a
            .put(&format!(
                "/_matrix/client/v3/rooms/{room_id}/send/m.room.encrypted/t1"
            ))
            .add_header(hdr(&worker_a1).0, hdr(&worker_a1).1)
            .json(&json!({ "algorithm": "m.megolm.v1.aes-sha2", "ciphertext": "TWOPOOL-CIPHERTEXT-A1" }))
            .await
            .assert_status_ok();
        let mut body_b = false;
        for _ in 0..10 {
            tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;
            let sync_b: Value = server_b
                .get("/_matrix/client/v3/sync")
                .add_header(hdr(&worker_b1).0, hdr(&worker_b1).1)
                .await
                .json();
            body_b = sync_b["rooms"]["join"][&room_id]["timeline"]["events"]
                .as_array()
                .map(|evs| {
                    evs.iter()
                        .any(|e| e["content"]["ciphertext"] == "TWOPOOL-CIPHERTEXT-A1")
                })
                .unwrap_or(false);
            if body_b {
                break;
            }
        }
        assert!(
            body_b,
            "pool B's worker must see pool A's encrypted event in the shared room"
        );

        // ── Step 7: sendToDevice between pools (the Megolm-session channel) ──
        server_a
            .put("/_matrix/client/v3/sendToDevice/m.room.encrypted/t2")
            .add_header(hdr(&worker_a1).0, hdr(&worker_a1).1)
            .json(&json!({
                "messages": {
                    "@poolb_fleet:poolb": { "PB-WORKER-1": { "ciphertext": "TO-DEVICE-A1-TO-B1" } }
                }
            }))
            .await
            .assert_status_ok();
        let mut todev = false;
        for _ in 0..10 {
            tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;
            let sync_b2: Value = server_b
                .get("/_matrix/client/v3/sync")
                .add_header(hdr(&worker_b1).0, hdr(&worker_b1).1)
                .await
                .json();
            todev = sync_b2["to_device"]["events"]
                .as_array()
                .map(|evs| {
                    evs.iter()
                        .any(|e| e["content"]["ciphertext"] == "TO-DEVICE-A1-TO-B1")
                })
                .unwrap_or(false);
            if todev {
                break;
            }
        }
        assert!(
            todev,
            "pool B's worker must receive pool A's to-device message"
        );

        // ── Step 8: device-list visibility across pools (gossip) ──────────────
        // Pool B must know pool A's worker appeared (refresh keys for it) —
        // the device_lists.changed signal, drained by the sync above.
        // Scoped so the guard never lives past this block (clippy: held
        // across await — the Step 9 probes below are async).
        {
            let changes = state_b
                .e2ee
                .device_list_changes
                .lock()
                .expect("device_list_changes lock");
            assert!(
                changes.contains_key("@poola_fleet:poola"),
                "pool B must record pool A's device-list change (gossiped): {changes:?}"
            );
        }

        // ── Step 9: the trust boundary — tokens do not cross pools ────────────
        // Pool A's AS token is worthless on pool B's node (unknown credential),
        // a worker session token is NOT an AS token, and neither AS can name
        // the other pool's users. Three probes, all must be 403.
        let cross_as = server_b
            .post("/_matrix/client/v3/register")
            .add_header(hdr("poola-as-token").0, hdr("poola-as-token").1)
            .json(&json!({ "username": "poolb_intruder" }))
            .await;
        assert_eq!(cross_as.status_code().as_u16(), 403, "foreign AS token must not register");

        let worker_as_as = server_a
            .post("/_matrix/client/v3/register")
            .add_header(hdr(&worker_a1).0, hdr(&worker_a1).1)
            .json(&json!({ "username": "poola_escalate" }))
            .await;
        assert_eq!(
            worker_as_as.status_code().as_u16(),
            403,
            "a worker session token is not its pool's AS token"
        );

        let cross_login = server_a
            .post("/_matrix/client/v3/login")
            .add_header(hdr("poolb-as-token").0, hdr("poolb-as-token").1)
            .json(&json!({
                "type": "m.login.application_service",
                "user_id": "@poola_fleet:poola",
            }))
            .await;
        assert_eq!(
            cross_login.status_code().as_u16(),
            403,
            "pool B's AS token must not mint sessions on pool A's node"
        );
    }
}
