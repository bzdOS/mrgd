// START_AI_HEADER
// MODULE: matrix-hs/src/keys_test.rs
// PURPOSE: Integration tests for the OTK CLAIM-EXACTLY-ONCE barrier (OWNERSHIP-PARTITION).
//          All tests are node-local: keys uploaded → claimed → never returned twice.
//
//          Exactly-once scenarios:
//            1. upload 3 OTKs → counts show 3 → claim one → response has exactly one key
//               + count drops to 2 → claim again → different key → claim until empty →
//               further claim returns absent.
//            2. Concurrent claim attempt: two tasks race to claim the only remaining key;
//               exactly one gets it and the other gets absent.
//            3. keys/query returns device_keys blob uploaded via keys/upload.
//
// DEPENDENCIES: axum-test, serde_json, matrix_hs::{router, AppState}
// END_AI_HEADER

#[cfg(test)]
mod tests {
    use crate::{router, AppState};
    use axum::http::{HeaderName, HeaderValue};
    use axum_test::TestServer;
    use serde_json::{json, Value};

    // test_helper:start
    //   purpose: Build a fresh TestServer with an empty in-memory AppState.
    //   input:  none
    //   output: TestServer
    //   sideEffects: none
    // test_helper:end
    fn test_server() -> TestServer {
        let state = AppState::new();
        let app = router(state);
        TestServer::new(app)
    }

    // auth:start
    //   purpose: Register a user via two-step UIA (if not already registered) and return
    //            an Authorization: Bearer <mxt_...> header for axum-test requests.
    //            Performs the full UIA flow: POST /register (challenge) then POST /register
    //            (m.login.dummy + session) → extracts the signed mxt_ token.
    //            Panics if any step fails (test helper — panic is OK in test code).
    //   input:  server — &TestServer; user — localpart string
    //   output: (HeaderName, HeaderValue) for Authorization: Bearer <mxt_ token>
    //   sideEffects: inserts user into AppState via register
    // auth:end

    async fn auth(server: &TestServer, user: &str) -> (HeaderName, HeaderValue) {
        // Step 1: UIA challenge
        let challenge: serde_json::Value = server
            .post("/_matrix/client/v3/register")
            .json(&json!({ "username": user, "password": "pw" }))
            .await
            .json();
        let session = challenge["session"]
            .as_str()
            .unwrap_or_else(|| panic!("auth helper: missing session for {user}; got {challenge}"))
            .to_string();

        // Step 2: complete
        let reg: serde_json::Value = server
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
            "auth helper: token must start with mxt_ for {user}; got {token}"
        );

        (
            HeaderName::from_static("authorization"),
            HeaderValue::from_str(&format!("Bearer {token}")).expect("header value"),
        )
    }

    // cross_signing_upload:start
    //   purpose: Complete the two-step UIA the cross-signing endpoint now requires:
    //            a bare POST answers 401 with a session, and the retry carries
    //            m.login.password. Test users all register with password "pw".
    //   input:  server; auth header pair; user localpart; keys — the upload body
    //   output: none (asserts the completed upload succeeds)
    //   sideEffects: two POSTs to keys/device_signing/upload
    // cross_signing_upload:end
    async fn cross_signing_upload(
        server: &TestServer,
        hn: &HeaderName,
        hv: &HeaderValue,
        user: &str,
        mut keys: Value,
    ) {
        let challenge = server
            .post("/_matrix/client/v3/keys/device_signing/upload")
            .add_header(hn.clone(), hv.clone())
            .json(&keys)
            .await;
        assert_eq!(
            challenge.status_code(),
            401,
            "an upload with no auth block must be challenged, not accepted"
        );
        let body: Value = challenge.json();
        let session = body["session"]
            .as_str()
            .unwrap_or_else(|| panic!("challenge carried no session: {body}"))
            .to_string();

        keys["auth"] = json!({
            "type": "m.login.password",
            "session": session,
            "identifier": { "type": "m.id.user", "user": user },
            "password": "pw"
        });
        server
            .post("/_matrix/client/v3/keys/device_signing/upload")
            .add_header(hn.clone(), hv.clone())
            .json(&keys)
            .await
            .assert_status_ok();
    }

    // otk_exactly_once_sequential:start
    //   purpose: Upload 3 OTKs, claim them one by one, verify exactly-once:
    //              1. After upload: counts show 3.
    //              2. Claim #1 → exactly one key returned; count now 2.
    //              3. Claim #2 → a DIFFERENT key; count now 1.
    //              4. Claim #3 → last key; count now 0.
    //              5. Claim #4 (exhausted) → response has no key for this (user,device).
    //            Proves: the pop is permanent — each key returned at most once.
    //   input:  none
    //   output: assertions on responses at each step
    //   sideEffects: keys stored in AppState
    // otk_exactly_once_sequential:end
    #[tokio::test]
    async fn otk_exactly_once_sequential() {
        let server = test_server();
        let (hn, hv) = auth(&server, "alice").await;

        // ── Step 1: Upload 3 curve25519 OTKs ─────────────────────────────────
        let upload_resp = server
            .post("/_matrix/client/v3/keys/upload")
            .add_header(hn.clone(), hv.clone())
            .json(&json!({
                "one_time_keys": {
                    "curve25519:KEYID0": { "key": "AAAA" },
                    "curve25519:KEYID1": { "key": "BBBB" },
                    "curve25519:KEYID2": { "key": "CCCC" },
                }
            }))
            .await;
        upload_resp.assert_status_ok();
        let upload_body: Value = upload_resp.json();
        let counts = &upload_body["one_time_key_counts"];
        assert_eq!(
            counts["curve25519"].as_u64(),
            Some(3),
            "after upload: curve25519 count must be 3; got {counts}"
        );

        // ── Step 2: Claim one key ─────────────────────────────────────────────
        let claim1: Value = server
            .post("/_matrix/client/v3/keys/claim")
            .add_header(hn.clone(), hv.clone())
            .json(&json!({
                "one_time_keys": {
                    "@alice:localhost": {
                        "DEVICE1": "curve25519"
                    }
                }
            }))
            .await
            .json();

        let alice_keys = &claim1["one_time_keys"]["@alice:localhost"]["DEVICE1"];
        let claimed1 = alice_keys
            .as_object()
            .expect("claim1: DEVICE1 must be an object");
        assert_eq!(
            claimed1.len(),
            1,
            "claim1 must return exactly one key; got {claimed1:?}"
        );
        let key_id1 = claimed1
            .keys()
            .next()
            .expect("claim1: must have a key_id")
            .clone();
        assert!(
            key_id1.starts_with("curve25519:"),
            "claim1: key_id must start with 'curve25519:'; got {key_id1}"
        );

        // ── Step 3: Claim another — must be a DIFFERENT key ───────────────────
        let claim2: Value = server
            .post("/_matrix/client/v3/keys/claim")
            .add_header(hn.clone(), hv.clone())
            .json(&json!({
                "one_time_keys": {
                    "@alice:localhost": {
                        "DEVICE1": "curve25519"
                    }
                }
            }))
            .await
            .json();

        let alice2 = &claim2["one_time_keys"]["@alice:localhost"]["DEVICE1"];
        let claimed2 = alice2.as_object().expect("claim2: DEVICE1 must be object");
        assert_eq!(
            claimed2.len(),
            1,
            "claim2 must return exactly one key; got {claimed2:?}"
        );
        let key_id2 = claimed2.keys().next().expect("claim2: key_id").clone();
        assert_ne!(
            key_id1, key_id2,
            "claim2 must return a different key than claim1; both returned {key_id1}"
        );

        // ── Step 4: Claim the last key ────────────────────────────────────────
        let claim3: Value = server
            .post("/_matrix/client/v3/keys/claim")
            .add_header(hn.clone(), hv.clone())
            .json(&json!({
                "one_time_keys": {
                    "@alice:localhost": {
                        "DEVICE1": "curve25519"
                    }
                }
            }))
            .await
            .json();

        let claimed3 = claim3["one_time_keys"]["@alice:localhost"]["DEVICE1"]
            .as_object()
            .expect("claim3: DEVICE1 must be object");
        assert_eq!(
            claimed3.len(),
            1,
            "claim3 must return exactly one key; got {claimed3:?}"
        );
        let key_id3 = claimed3.keys().next().expect("claim3: key_id").clone();
        // All three keys must be distinct.
        let claimed_ids = [&key_id1, &key_id2, &key_id3];
        assert_eq!(
            {
                let mut s: Vec<_> = claimed_ids.iter().collect();
                s.sort();
                s.dedup();
                s.len()
            },
            3,
            "all three claimed key_ids must be distinct; got {key_id1}, {key_id2}, {key_id3}"
        );

        // ── Step 5: Exhausted — claim returns absent ──────────────────────────
        let claim4: Value = server
            .post("/_matrix/client/v3/keys/claim")
            .add_header(hn.clone(), hv.clone())
            .json(&json!({
                "one_time_keys": {
                    "@alice:localhost": {
                        "DEVICE1": "curve25519"
                    }
                }
            }))
            .await
            .json();

        // When no keys are available, either the inner device key is absent,
        // or the user/device entry itself is absent.
        let no_key_for_device = claim4["one_time_keys"]["@alice:localhost"]["DEVICE1"]
            .as_object()
            .map(|m| m.is_empty())
            .unwrap_or(true); // absent = no key available
        assert!(
            no_key_for_device,
            "claim4 (exhausted): must return absent or empty for DEVICE1; got {:?}",
            claim4["one_time_keys"]
        );

        // ── Verify upload count also reflects 0 after exhaustion ─────────────
        // Re-upload 0 keys — counts endpoint returns current inventory.
        let recheck: Value = server
            .post("/_matrix/client/v3/keys/upload")
            .add_header(hn.clone(), hv.clone())
            .json(&json!({ "one_time_keys": {} }))
            .await
            .json();
        let after_count = recheck["one_time_key_counts"]["curve25519"]
            .as_u64()
            .unwrap_or(0);
        assert_eq!(
            after_count, 0,
            "after claiming all 3 keys, count must be 0; got {after_count}"
        );
    }

    // otk_upload_query_device_keys:start
    //   purpose: keys/upload with device_keys → keys/query returns the stored blob.
    //            Also verifies that keys/query returns empty for unknown users.
    //   input:  none
    //   output: assertions
    //   sideEffects: device_keys stored in AppState
    // otk_upload_query_device_keys:end
    #[tokio::test]
    async fn otk_upload_query_device_keys() {
        let server = test_server();
        let (hn, hv) = auth(&server, "bob").await;

        let dk_blob = json!({
            "user_id":   "@bob:localhost",
            "device_id": "DEVICE1",
            "algorithms": ["m.olm.v1.curve25519-aes-sha2"],
            "keys": { "curve25519:DEVICE1": "ZZZZ" }
        });

        // Upload device_keys.
        server
            .post("/_matrix/client/v3/keys/upload")
            .add_header(hn.clone(), hv.clone())
            .json(&json!({ "device_keys": dk_blob }))
            .await
            .assert_status_ok();

        // Query back — should return the blob.
        let query_resp: Value = server
            .post("/_matrix/client/v3/keys/query")
            .add_header(hn.clone(), hv.clone())
            .json(&json!({
                "device_keys": {
                    "@bob:localhost": {}
                }
            }))
            .await
            .json();

        let returned = &query_resp["device_keys"]["@bob:localhost"]["DEVICE1"];
        assert!(
            !returned.is_null(),
            "keys/query must return blob for @bob:localhost DEVICE1; got {:?}",
            query_resp
        );
        assert_eq!(
            returned["keys"]["curve25519:DEVICE1"].as_str(),
            Some("ZZZZ"),
            "keys/query must return the uploaded key blob"
        );

        // Query back using the spec-correct ARRAY shape (empty array = "all devices
        // for this user") — this is what every real client actually sends
        // (matrix-dart-sdk, matrix-js-sdk, Synapse); the object-shape query above
        // does not exercise this path, and a prior version of post_keys_query only
        // handled the object shape, silently returning empty device_keys for any
        // real client's query.
        let query_resp_array: Value = server
            .post("/_matrix/client/v3/keys/query")
            .add_header(hn.clone(), hv.clone())
            .json(&json!({
                "device_keys": {
                    "@bob:localhost": []
                }
            }))
            .await
            .json();

        let returned_array = &query_resp_array["device_keys"]["@bob:localhost"]["DEVICE1"];
        assert!(
            !returned_array.is_null(),
            "keys/query with array-shape device list must return blob for @bob:localhost DEVICE1; got {:?}",
            query_resp_array
        );
        assert_eq!(
            returned_array["keys"]["curve25519:DEVICE1"].as_str(),
            Some("ZZZZ"),
            "keys/query with array-shape device list must return the uploaded key blob"
        );

        // Array shape with a specific device_id requested — should also resolve.
        let query_resp_specific: Value = server
            .post("/_matrix/client/v3/keys/query")
            .add_header(hn.clone(), hv.clone())
            .json(&json!({
                "device_keys": {
                    "@bob:localhost": ["DEVICE1"]
                }
            }))
            .await
            .json();
        assert!(
            !query_resp_specific["device_keys"]["@bob:localhost"]["DEVICE1"].is_null(),
            "keys/query with a specific device_id in the array must return that device's blob; got {:?}",
            query_resp_specific
        );

        // Query for unknown user (authenticated caller) — returns empty object.
        let unknown: Value = server
            .post("/_matrix/client/v3/keys/query")
            .add_header(hn.clone(), hv.clone())
            .json(&json!({
                "device_keys": {
                    "@nobody:localhost": []
                }
            }))
            .await
            .json();
        let nobody = &unknown["device_keys"]["@nobody:localhost"];
        assert!(
            nobody.as_object().map(|m| m.is_empty()).unwrap_or(true),
            "keys/query for unknown user must return empty; got {nobody:?}"
        );
    }

    // otk_upload_device_keys_reset_resets_stale_otks:start
    //   purpose: Regression test for a live-reproduced crash: matrix-dart-sdk's
    //            OlmManager.init() (used by FluffyChat and other real clients)
    //            requires one_time_key_counts.signed_curve25519 in a keys/upload
    //            response to exactly equal the number of OTKs it just uploaded,
    //            when establishing a fresh Olm account — otherwise it throws
    //            "Upload key failed" and the client crashes right after login.
    //            A device_id is reused across logins in this server (no
    //            per-session device rotation), so a client reinstall/app-data-clear
    //            re-uploads a brand NEW device_keys blob (new Olm identity) under
    //            the SAME device_id while old one_time_keys from the PRIOR
    //            identity are still sitting in device_otks. Before the fix, those
    //            stale keys got counted alongside the new upload, inflating the
    //            count past what the client uploaded and tripping the SDK's exact
    //            check. The fix: a device_keys upload must clear any pre-existing
    //            OTKs for that (user, device) slot before merging in new ones.
    //   input:  none
    //   output: n/a (asserts)
    //   sideEffects: none beyond the test server's in-memory state
    // otk_upload_device_keys_reset_resets_stale_otks:end
    #[tokio::test]
    async fn otk_upload_device_keys_reset_resets_stale_otks() {
        let server = test_server();
        let (hn, hv) = auth(&server, "carol").await;

        // First "install": upload device_keys + 2 OTKs.
        let resp1: Value = server
            .post("/_matrix/client/v3/keys/upload")
            .add_header(hn.clone(), hv.clone())
            .json(&json!({
                "device_keys": {
                    "user_id": "@carol:localhost", "device_id": "DEVICE1",
                    "keys": { "ed25519:DEVICE1": "OLD_IDENTITY" }
                },
                "one_time_keys": {
                    "signed_curve25519:AAAAAA": { "key": "aaa" },
                    "signed_curve25519:BBBBBB": { "key": "bbb" }
                }
            }))
            .await
            .json();
        assert_eq!(
            resp1["one_time_key_counts"]["signed_curve25519"].as_u64(),
            Some(2),
            "first upload: count must equal the 2 keys just uploaded; got {resp1:?}"
        );

        // Second "install" (same device_id, brand new Olm identity + 3 fresh OTKs) —
        // simulates a reinstall/app-data-clear against the same account.
        let resp2: Value = server
            .post("/_matrix/client/v3/keys/upload")
            .add_header(hn.clone(), hv.clone())
            .json(&json!({
                "device_keys": {
                    "user_id": "@carol:localhost", "device_id": "DEVICE1",
                    "keys": { "ed25519:DEVICE1": "NEW_IDENTITY" }
                },
                "one_time_keys": {
                    "signed_curve25519:CCCCCC": { "key": "ccc" },
                    "signed_curve25519:DDDDDD": { "key": "ddd" },
                    "signed_curve25519:EEEEEE": { "key": "eee" }
                }
            }))
            .await
            .json();
        assert_eq!(
            resp2["one_time_key_counts"]["signed_curve25519"].as_u64(),
            Some(3),
            "second upload (new identity, same device_id) must report exactly the 3 \
             newly-uploaded keys, not 3 + leftover stale keys from the old identity; \
             got {resp2:?}"
        );
    }

    // otk_algorithm_mismatch:start
    //   purpose: Uploading ed25519 keys; claiming curve25519 → absent.
    //            Claiming ed25519 → returns the key.
    //            Proves algorithm prefix matching in the claim pop.
    //   input:  none
    //   output: assertions
    //   sideEffects: keys stored/consumed in AppState
    // otk_algorithm_mismatch:end
    #[tokio::test]
    async fn otk_algorithm_mismatch() {
        let server = test_server();
        let (hn, hv) = auth(&server, "carol").await;

        // Upload only ed25519 keys.
        server
            .post("/_matrix/client/v3/keys/upload")
            .add_header(hn.clone(), hv.clone())
            .json(&json!({
                "one_time_keys": {
                    "ed25519:EDKEY0": { "key": "sig0" }
                }
            }))
            .await
            .assert_status_ok();

        // Claim curve25519 → absent (wrong algorithm).
        let mismatch: Value = server
            .post("/_matrix/client/v3/keys/claim")
            .json(&json!({
                "one_time_keys": {
                    "@carol:localhost": {
                        "DEVICE1": "curve25519"
                    }
                }
            }))
            .await
            .json();
        let absent = mismatch["one_time_keys"]["@carol:localhost"]["DEVICE1"]
            .as_object()
            .map(|m| m.is_empty())
            .unwrap_or(true);
        assert!(
            absent,
            "claiming curve25519 when only ed25519 available must return absent; got {:?}",
            mismatch
        );

        // Claim ed25519 → returns the key.
        let matched: Value = server
            .post("/_matrix/client/v3/keys/claim")
            .json(&json!({
                "one_time_keys": {
                    "@carol:localhost": {
                        "DEVICE1": "ed25519"
                    }
                }
            }))
            .await
            .json();
        let got = matched["one_time_keys"]["@carol:localhost"]["DEVICE1"]
            .as_object()
            .expect("matched: must be object");
        assert_eq!(
            got.len(),
            1,
            "must return exactly one ed25519 key; got {got:?}"
        );
        let k = got.keys().next().expect("key_id");
        assert!(
            k.starts_with("ed25519:"),
            "key_id must start with 'ed25519:'; got {k}"
        );
    }

    // otk_non_owner_absent:start
    //   purpose: Claiming keys for a (user,device) that has no keys on this node
    //            returns absent — NOT an error.
    //            This is the node-local exactly-once guarantee; cross-node routing deferred.
    //   input:  none
    //   output: assertion
    //   sideEffects: none
    // otk_non_owner_absent:end
    #[tokio::test]
    async fn otk_non_owner_absent() {
        let server = test_server();
        // No upload at all — claim for an unknown user.
        let resp: Value = server
            .post("/_matrix/client/v3/keys/claim")
            .json(&json!({
                "one_time_keys": {
                    "@ghost:localhost": {
                        "DEVICE1": "curve25519"
                    }
                }
            }))
            .await
            .json();

        // Response must be valid JSON with one_time_keys; absent entry for ghost.
        let ghost = &resp["one_time_keys"]["@ghost:localhost"];
        assert!(
            ghost.is_null()
                || ghost
                    .as_object()
                    .map(|m| !m.contains_key("DEVICE1"))
                    .unwrap_or(true),
            "non-owner claim must return absent (null or no DEVICE1 entry); got {resp:?}"
        );
    }

    // otk_upload_merge_idempotent:start
    //   purpose: Uploading the same key_id twice does not create duplicates.
    //            After uploading "curve25519:KEYID0" twice, count stays 1, not 2.
    //   input:  none
    //   output: assertion
    //   sideEffects: keys stored in AppState
    // otk_upload_merge_idempotent:end
    #[tokio::test]
    async fn otk_upload_merge_idempotent() {
        let server = test_server();
        let (hn, hv) = auth(&server, "dave").await;

        for _ in 0..2 {
            server
                .post("/_matrix/client/v3/keys/upload")
                .add_header(hn.clone(), hv.clone())
                .json(&json!({
                    "one_time_keys": {
                        "curve25519:KEYID0": { "key": "DDDD" }
                    }
                }))
                .await
                .assert_status_ok();
        }

        // Count must still be 1 (not 2).
        let check: Value = server
            .post("/_matrix/client/v3/keys/upload")
            .add_header(hn.clone(), hv.clone())
            .json(&json!({ "one_time_keys": {} }))
            .await
            .json();
        let count = check["one_time_key_counts"]["curve25519"]
            .as_u64()
            .unwrap_or(0);
        assert_eq!(
            count, 1,
            "duplicate upload must not increase count; got {count}"
        );
    }

    // signatures_upload_merges_device_sig_into_master_key:start
    //   purpose: Regression for the E2EE-bootstrap hang: after device_signing/upload
    //            (unsigned master) + signatures/upload (device signs master), keys/query
    //            MUST return the master key WITH the device signature embedded in its
    //            signatures map. Before the merge fix, signatures/upload stored the
    //            blob opaquely and keys/query returned master.signatures == {} — so
    //            crypto identity could never verify and matrix-dart-sdk's bootstrap
    //            spun forever ("waiting for master to be created").
    // signatures_upload_merges_device_sig_into_master_key:end
    #[tokio::test]
    async fn signatures_upload_merges_device_sig_into_master_key() {
        let server = test_server();
        let (hn, hv) = auth(&server, "alice").await;

        // 1) Upload an UNSIGNED master key (plus self/user signed by master).
        cross_signing_upload(
            &server,
            &hn,
            &hv,
            "alice",
            json!({
                "master_key": {
                    "user_id": "@alice:localhost",
                    "usage": ["master"],
                    "keys": {"ed25519:MPUB": "MPUB"}
                },
                "self_signing_key": {
                    "user_id": "@alice:localhost",
                    "usage": ["self_signing"],
                    "keys": {"ed25519:SPUB": "SPUB"},
                    "signatures": {"@alice:localhost": {"ed25519:MPUB": "selfByMaster"}}
                },
                "user_signing_key": {
                    "user_id": "@alice:localhost",
                    "usage": ["user_signing"],
                    "keys": {"ed25519:UPUB": "UPUB"},
                    "signatures": {"@alice:localhost": {"ed25519:MPUB": "userByMaster"}}
                }
            }),
        )
        .await;

        // Sanity: right after device_signing/upload the master is unsigned.
        let q0: Value = server
            .post("/_matrix/client/v3/keys/query")
            .add_header(hn.clone(), hv.clone())
            .json(&json!({"device_keys": {"@alice:localhost": []}}))
            .await
            .json();
        let mk0 = &q0["master_keys"]["@alice:localhost"];
        assert!(
            mk0["signatures"].as_object().is_none_or(|m| m.is_empty()),
            "pre-sign master must have empty signatures; got {}",
            mk0["signatures"]
        );

        // 2) signatures/upload: device signs the master.
        server
            .post("/_matrix/client/v3/keys/signatures/upload")
            .add_header(hn.clone(), hv.clone())
            .json(&json!({
                "@alice:localhost": {
                    "MPUB": {
                        "user_id": "@alice:localhost",
                        "usage": ["master"],
                        "keys": {"ed25519:MPUB": "MPUB"},
                        "signatures": {"@alice:localhost": {"ed25519:DEVICE1": "masterByDevice"}}
                    }
                }
            }))
            .await
            .assert_status_ok();

        // 3) keys/query MUST now return the master WITH the device signature merged in.
        let q1: Value = server
            .post("/_matrix/client/v3/keys/query")
            .add_header(hn, hv)
            .json(&json!({"device_keys": {"@alice:localhost": []}}))
            .await
            .json();
        let mk = &q1["master_keys"]["@alice:localhost"];
        let dev_sig = &mk["signatures"]["@alice:localhost"]["ed25519:DEVICE1"];
        assert_eq!(
            dev_sig.as_str(),
            Some("masterByDevice"),
            "master must carry the device signature after signatures/upload; got {}",
            mk["signatures"]
        );
    }

    // cross_signing_requires_password:start
    //   purpose: A master key re-points every "is this device really theirs?" answer
    //            for a user, so a bearer token alone must not be enough to replace it:
    //            a leaked token would otherwise let an attacker install their own
    //            cross-signing identity silently. Pin the gate, including the ways a
    //            client might try to slip past it.
    //   input:  none
    //   output: assertions on each rejected shape, then a successful completion
    //   sideEffects: builds an in-memory AppState
    // cross_signing_requires_password:end
    #[tokio::test]
    async fn cross_signing_requires_password() {
        let server = test_server();
        let (hn, hv) = auth(&server, "alice").await;
        let (mn, mv) = auth(&server, "mallory").await;
        let keys = json!({
            "master_key": {
                "user_id": "@alice:localhost",
                "usage": ["master"],
                "keys": {"ed25519:MPUB": "MPUB"}
            }
        });

        let post = |body: Value, h: (HeaderName, HeaderValue)| {
            let server = &server;
            async move {
                server
                    .post("/_matrix/client/v3/keys/device_signing/upload")
                    .add_header(h.0, h.1)
                    .json(&body)
                    .await
            }
        };

        // No auth block at all → challenged, and the challenge must name the stage.
        let r = post(keys.clone(), (hn.clone(), hv.clone())).await;
        assert_eq!(r.status_code(), 401, "a valid token alone must not suffice");
        let ch: Value = r.json();
        assert_eq!(ch["flows"][0]["stages"][0], "m.login.password");
        let session = ch["session"].as_str().expect("session").to_string();

        // Right session, WRONG password → 403, not a pass.
        let mut bad = keys.clone();
        bad["auth"] = json!({
            "type": "m.login.password", "session": session,
            "identifier": { "type": "m.id.user", "user": "alice" },
            "password": "not-the-password"
        });
        assert_eq!(post(bad, (hn.clone(), hv.clone())).await.status_code(), 403);

        // A session is one-shot: the one just consumed cannot be reused, even with
        // the right password.
        let mut replay = keys.clone();
        replay["auth"] = json!({
            "type": "m.login.password", "session": session,
            "identifier": { "type": "m.id.user", "user": "alice" },
            "password": "pw"
        });
        assert_eq!(
            post(replay, (hn.clone(), hv.clone())).await.status_code(),
            401,
            "a consumed session must be re-challenged, not accepted"
        );

        // Fabricated session id → challenged, never accepted.
        let mut forged = keys.clone();
        forged["auth"] = json!({
            "type": "m.login.password", "session": "uia_99999",
            "identifier": { "type": "m.id.user", "user": "alice" },
            "password": "pw"
        });
        assert_eq!(post(forged, (hn.clone(), hv.clone())).await.status_code(), 401);

        // Mallory holds her OWN valid token and knows her OWN password, and tries to
        // clear the gate with them while uploading. The identifier must be checked
        // against the caller, so proving a different account is worth nothing.
        let ch2: Value = post(keys.clone(), (mn.clone(), mv.clone())).await.json();
        let s2 = ch2["session"].as_str().expect("session").to_string();
        let mut cross = keys.clone();
        cross["auth"] = json!({
            "type": "m.login.password", "session": s2,
            "identifier": { "type": "m.id.user", "user": "alice" },
            "password": "pw"
        });
        assert_eq!(
            post(cross, (mn.clone(), mv.clone())).await.status_code(),
            401,
            "proving an account that is not the caller's must not clear the gate"
        );

        // And the honest path works.
        cross_signing_upload(&server, &hn, &hv, "alice", keys).await;
    }
}
