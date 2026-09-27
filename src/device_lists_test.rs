// START_AI_HEADER
// MODULE: matrix-hs/src/device_lists_test.rs
// PURPOSE: Integration tests for E2EE device-list change tracking (device_lists.changed)
//          and OTK-count wiring in /sync's device_one_time_keys_count.
//
//          Scenarios:
//            1. device_list_changed_and_own_otk_count (same-node): alice and bob share a
//               room; alice uploads device_keys via keys/upload; bob's INCREMENTAL /sync
//               shows alice in device_lists.changed; bob's own device_one_time_keys_count
//               reflects OTKs bob uploaded.
//            2. device_list_initial_sync_stays_empty: an initial sync (no since) never
//               populates device_lists.changed, even after a device-list change — per the
//               spec, a fresh client is expected to run its own full keys/query instead.
//            3. device_list_changed_requires_shared_room: alice's device-list change is NOT
//               reported to carol, who does not share a room with alice.
// DEPENDENCIES: axum-test, serde_json, matrix_hs::{router, AppState}
// END_AI_HEADER

#[cfg(test)]
mod tests {
    use crate::{router, AppState};
    use axum::http::{HeaderName, HeaderValue};
    use axum_test::TestServer;
    use serde_json::{json, Value};

    // test_server:start
    //   purpose: Build a fresh TestServer with an empty in-memory AppState.
    //   input:  none
    //   output: TestServer
    //   sideEffects: none
    // test_server:end
    fn test_server() -> TestServer {
        let state = AppState::new();
        let app = router(state);
        TestServer::new(app)
    }

    // auth:start
    //   purpose: Register a user via two-step UIA and return an Authorization: Bearer
    //            header (duplicated per-module by repo convention — see keys_test.rs's
    //            identically-named helper).
    //   input:  server — &TestServer; user — localpart string
    //   output: (HeaderName, HeaderValue) for Authorization: Bearer <mxt_ token>
    //   sideEffects: inserts user into AppState via register
    // auth:end
    async fn auth(server: &TestServer, user: &str) -> (HeaderName, HeaderValue) {
        let challenge: Value = server
            .post("/_matrix/client/v3/register")
            .json(&json!({ "username": user, "password": "pw" }))
            .await
            .json();
        let session = challenge["session"]
            .as_str()
            .unwrap_or_else(|| panic!("auth helper: missing session for {user}; got {challenge}"))
            .to_string();

        let reg: Value = server
            .post("/_matrix/client/v3/register")
            .json(&json!({
                "username": user,
                "password": "pw",
                "auth": { "type": "m.login.dummy", "session": session }
            }))
            .await
            .json();

        let token = reg["access_token"]
            .as_str()
            .unwrap_or_else(|| panic!("auth helper: missing access_token for {user}; got {reg}"))
            .to_string();
        assert!(
            token.starts_with("mxt_"),
            "token must start with mxt_ for {user}; got {token}"
        );

        (
            HeaderName::from_static("authorization"),
            HeaderValue::from_str(&format!("Bearer {token}")).expect("header value"),
        )
    }

    // device_list_changed_and_own_otk_count:start
    //   purpose: alice and bob share a room. alice uploads device_keys (a device-list
    //            change). bob's incremental /sync (since=his own next_batch from a prior
    //            sync) must list "@alice:localhost" in device_lists.changed. bob's own
    //            device_one_time_keys_count must reflect the 2 curve25519 OTKs he
    //            uploaded (reusing keys.rs's count_by_algorithm).
    //   input:  none
    //   output: assertions on bob's second /sync response
    //   sideEffects: creates a room, registers 2 users, uploads keys
    // device_list_changed_and_own_otk_count:end
    #[tokio::test]
    async fn device_list_changed_and_own_otk_count() {
        let server = test_server();
        let (alice_hn, alice_hv) = auth(&server, "alice").await;
        let (bob_hn, bob_hv) = auth(&server, "bob").await;

        // alice creates a room; bob joins it — they now share a room.
        let create_resp: Value = server
            .post("/_matrix/client/v3/createRoom")
            .add_header(alice_hn.clone(), alice_hv.clone())
            .json(&json!({ "room_alias_name": "device-lists-room" }))
            .await
            .json();
        let room_id = create_resp["room_id"]
            .as_str()
            .unwrap_or_else(|| panic!("createRoom: missing room_id; got {create_resp}"))
            .to_string();

        server
            .post(&format!("/_matrix/client/v3/join/{room_id}"))
            .add_header(bob_hn.clone(), bob_hv.clone())
            .await
            .assert_status_ok();

        // bob uploads 2 curve25519 OTKs (no device_keys — must NOT itself trigger a
        // device-list change for bob).
        let bob_upload: Value = server
            .post("/_matrix/client/v3/keys/upload")
            .add_header(bob_hn.clone(), bob_hv.clone())
            .json(&json!({
                "one_time_keys": {
                    "curve25519:BKEY0": { "key": "AAAA" },
                    "curve25519:BKEY1": { "key": "BBBB" },
                }
            }))
            .await
            .json();
        assert_eq!(
            bob_upload["one_time_key_counts"]["curve25519"].as_u64(),
            Some(2),
            "bob's keys/upload response must show 2 curve25519 OTKs; got {bob_upload}"
        );

        // bob's FIRST /sync — establishes a since-token baseline (initial sync: no
        // since param). device_lists.changed must be empty on an initial sync even
        // though bob's OWN keys/upload just happened (initial sync never populates it).
        let first_sync: Value = server
            .get("/_matrix/client/v3/sync")
            .add_header(bob_hn.clone(), bob_hv.clone())
            .await
            .json();
        assert!(
            first_sync["device_lists"]["changed"]
                .as_array()
                .map(|a| a.is_empty())
                .unwrap_or(false),
            "initial sync must have empty device_lists.changed; got {first_sync}"
        );
        let since = first_sync["next_batch"]
            .as_str()
            .unwrap_or_else(|| panic!("first sync: missing next_batch; got {first_sync}"))
            .to_string();

        // alice uploads device_keys — a device-list change for alice.
        server
            .post("/_matrix/client/v3/keys/upload")
            .add_header(alice_hn.clone(), alice_hv.clone())
            .json(&json!({
                "device_keys": {
                    "user_id":    "@alice:localhost",
                    "device_id":  "DEVICE1",
                    "algorithms": ["m.olm.v1.curve25519-aes-sha2"],
                    "keys": { "curve25519:DEVICE1": "ALICEKEY" }
                }
            }))
            .await
            .assert_status_ok();

        // bob's SECOND (incremental) /sync must show alice in device_lists.changed, and
        // his own device_one_time_keys_count must still show the 2 OTKs he uploaded.
        let second_sync: Value = server
            .get("/_matrix/client/v3/sync")
            .add_header(bob_hn.clone(), bob_hv.clone())
            .add_query_param("since", &since)
            .await
            .json();

        let changed = second_sync["device_lists"]["changed"]
            .as_array()
            .unwrap_or_else(|| {
                panic!("incremental sync: device_lists.changed must be an array; got {second_sync}")
            });
        assert!(
            changed
                .iter()
                .any(|v| v.as_str() == Some("@alice:localhost")),
            "bob's incremental sync must list alice in device_lists.changed; got {changed:?}"
        );

        let otk_count = second_sync["device_one_time_keys_count"]["curve25519"].as_u64();
        assert_eq!(
            otk_count,
            Some(2),
            "bob's device_one_time_keys_count.curve25519 must be 2; got {:?}",
            second_sync["device_one_time_keys_count"]
        );
    }

    // device_list_initial_sync_stays_empty:start
    //   purpose: A device-list change that happened BEFORE a user's very first /sync call
    //            must NOT appear in that initial sync's device_lists.changed — a fresh
    //            client always runs its own full keys/query instead (per the module
    //            contract: "Initial sync (no since): changed stays empty").
    //   input:  none
    //   output: assertion on the initial /sync response
    //   sideEffects: creates a room, registers 2 users, uploads a device key
    // device_list_initial_sync_stays_empty:end
    #[tokio::test]
    async fn device_list_initial_sync_stays_empty() {
        let server = test_server();
        let (alice_hn, alice_hv) = auth(&server, "alice2").await;
        let (bob_hn, bob_hv) = auth(&server, "bob2").await;

        let create_resp: Value = server
            .post("/_matrix/client/v3/createRoom")
            .add_header(alice_hn.clone(), alice_hv.clone())
            .json(&json!({ "room_alias_name": "device-lists-room-2" }))
            .await
            .json();
        let room_id = create_resp["room_id"]
            .as_str()
            .expect("room_id")
            .to_string();

        server
            .post(&format!("/_matrix/client/v3/join/{room_id}"))
            .add_header(bob_hn.clone(), bob_hv.clone())
            .await
            .assert_status_ok();

        // alice's device-list change happens BEFORE bob ever calls /sync.
        server
            .post("/_matrix/client/v3/keys/upload")
            .add_header(alice_hn.clone(), alice_hv.clone())
            .json(&json!({
                "device_keys": {
                    "user_id": "@alice2:localhost", "device_id": "DEVICE1",
                    "algorithms": [], "keys": {}
                }
            }))
            .await
            .assert_status_ok();

        // bob's very first /sync — no since param.
        let initial: Value = server
            .get("/_matrix/client/v3/sync")
            .add_header(bob_hn.clone(), bob_hv.clone())
            .await
            .json();
        assert!(
            initial["device_lists"]["changed"]
                .as_array()
                .map(|a| a.is_empty())
                .unwrap_or(false),
            "initial sync must have empty device_lists.changed even though alice changed \
             beforehand; got {initial}"
        );
    }

    // device_list_changed_requires_shared_room:start
    //   purpose: alice's device-list change must NOT be reported to carol, who does not
    //            share any room with alice — proves the shared-room filter
    //            (AppState::users_sharing_room_with) actually restricts the result, not
    //            just returns every changed user unconditionally.
    //   input:  none
    //   output: assertion on carol's incremental /sync response
    //   sideEffects: registers 2 users (no shared room), uploads a device key
    // device_list_changed_requires_shared_room:end
    #[tokio::test]
    async fn device_list_changed_requires_shared_room() {
        let server = test_server();
        let (alice_hn, alice_hv) = auth(&server, "alice3").await;
        let (carol_hn, carol_hv) = auth(&server, "carol3").await;

        // carol establishes a since-token baseline (no shared room with alice at all).
        let first: Value = server
            .get("/_matrix/client/v3/sync")
            .add_header(carol_hn.clone(), carol_hv.clone())
            .await
            .json();
        let since = first["next_batch"]
            .as_str()
            .expect("next_batch")
            .to_string();

        // alice uploads device_keys — a device-list change, but carol shares no room.
        server
            .post("/_matrix/client/v3/keys/upload")
            .add_header(alice_hn.clone(), alice_hv.clone())
            .json(&json!({
                "device_keys": {
                    "user_id": "@alice3:localhost", "device_id": "DEVICE1",
                    "algorithms": [], "keys": {}
                }
            }))
            .await
            .assert_status_ok();

        let second: Value = server
            .get("/_matrix/client/v3/sync")
            .add_header(carol_hn.clone(), carol_hv.clone())
            .add_query_param("since", &since)
            .await
            .json();
        let changed = second["device_lists"]["changed"]
            .as_array()
            .expect("changed array");
        assert!(
            !changed
                .iter()
                .any(|v| v.as_str() == Some("@alice3:localhost")),
            "carol must NOT see alice's device-list change without a shared room; got {changed:?}"
        );
    }

    // own_device_change_appears_in_changed:start
    //   purpose: Regression for the E2EE-bootstrap hang. A freshly-registered user
    //            (not yet in any room) uploads device keys / cross-signing keys,
    //            then runs an incremental /sync. Their OWN user_id MUST appear in
    //            device_lists.changed so the client re-queries its own keys and
    //            populates userDeviceKeys[self] — without this, matrix-dart-sdk's
    //            bootstrap spins forever in its "waiting for master to be created"
    //            oneShotSync loop. The old filter (`u != caller_user_id`) excluded
    //            self; this test pins the fix (`u == caller_user_id || shared`).
    // own_device_change_appears_in_changed:end
    #[tokio::test]
    async fn own_device_change_appears_in_changed() {
        let server = test_server();
        // alice: freshly registered, NOT a member of any room.
        let (hn, hv) = auth(&server, "alice_self").await;

        // Establish a since-token baseline.
        let first: Value = server
            .get("/_matrix/client/v3/sync")
            .add_header(hn.clone(), hv.clone())
            .await
            .json();
        let since = first["next_batch"]
            .as_str()
            .expect("first sync: missing next_batch")
            .to_string();

        // alice uploads device_keys → mark_device_list_changed(alice_self).
        server
            .post("/_matrix/client/v3/keys/upload")
            .add_header(hn.clone(), hv.clone())
            .json(&json!({
                "device_keys": {
                    "user_id": "@alice_self:localhost",
                    "device_id": "DEVICE1",
                    "algorithms": ["m.olm.v1.curve25519-aes-sha2"],
                    "keys": {"ed25519:DEVICE1": "AAAA"},
                    "signatures": {"@alice_self:localhost": {"ed25519:DEVICE1": "sig"}}
                }
            }))
            .await
            .assert_status_ok();

        // alice's incremental sync MUST list her own user_id in device_lists.changed.
        let second: Value = server
            .get("/_matrix/client/v3/sync")
            .add_header(hn, hv)
            .add_query_param("since", &since)
            .await
            .json();
        let changed = second["device_lists"]["changed"]
            .as_array()
            .expect("changed array");
        assert!(
            changed
                .iter()
                .any(|v| v.as_str() == Some("@alice_self:localhost")),
            "a user must see their OWN device-list change in device_lists.changed \
             (bootstrap depends on it); got {changed:?}"
        );
    }
}
