// START_AI_HEADER
// MODULE: matrix-hs/src/room_keys_test.rs
// PURPOSE: Integration tests for E2EE durability — key backup (/room_keys) and
//          cross-signing (keys/device_signing/upload surfaced via keys/query).
//          All node-local (per-user storage): backup version create → put keys →
//          get keys roundtrip with count/etag; cross-signing upload → query returns
//          master_keys/self_signing_keys.
// DEPENDENCIES: axum-test, serde_json, matrix_hs::{router, AppState}
// END_AI_HEADER

#[cfg(test)]
mod tests {
    use crate::{router, AppState};
    use axum::http::{HeaderName, HeaderValue};
    use axum_test::TestServer;
    use serde_json::{json, Value};

    // auth:start
    //   purpose: Two-step UIA register a user and return an Authorization: Bearer
    //            header (mirrors keys_test.rs::auth). Panic on failure (test code).
    //   input:  server — &TestServer; user — localpart
    //   output: (HeaderName, HeaderValue)
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
            .unwrap_or_else(|| panic!("missing session for {user}; got {challenge}"))
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
            .unwrap_or_else(|| panic!("missing access_token for {user}; got {reg}"))
            .to_string();
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

    // room_key_backup_roundtrip:start
    //   purpose: Prove the /room_keys backup happy path — create a version, store a
    //            session key under it, read it back, and see count/etag reflect it.
    //   input:  none
    //   output: none (asserts)
    //   sideEffects: builds an in-memory AppState
    // room_key_backup_roundtrip:end
    #[tokio::test]
    async fn room_key_backup_roundtrip() {
        let server = TestServer::new(router(AppState::new()));
        let (an, av) = auth(&server, "alice").await;

        // Create a backup version.
        let create: Value = server
            .post("/_matrix/client/v3/room_keys/version")
            .add_header(an.clone(), av.clone())
            .json(&json!({
                "algorithm": "m.megolm_backup.v1.curve25519-aes-sha2",
                "auth_data": { "public_key": "abcdef" }
            }))
            .await
            .json();
        let version = create["version"]
            .as_str()
            .unwrap_or_else(|| panic!("create version response missing version: {create}"))
            .to_string();

        // GET version metadata reflects algorithm + a zero count.
        let meta: Value = server
            .get("/_matrix/client/v3/room_keys/version")
            .add_header(an.clone(), av.clone())
            .await
            .json();
        assert_eq!(meta["version"].as_str(), Some(version.as_str()));
        assert_eq!(
            meta["algorithm"].as_str(),
            Some("m.megolm_backup.v1.curve25519-aes-sha2")
        );
        assert_eq!(
            meta["count"].as_u64(),
            Some(0),
            "fresh version has 0 keys; got {meta}"
        );

        // PUT one session key under (room, session).
        let room = "!room:state-node-a";
        let session = "sess1";
        let put: Value = server
            .put(&format!(
                "/_matrix/client/v3/room_keys/keys/{room}/{session}?version={version}"
            ))
            .add_header(an.clone(), av.clone())
            .json(&json!({
                "first_message_index": 0,
                "forwarded_count": 0,
                "is_verified": true,
                "session_data": { "ciphertext": "ENCRYPTED", "ephemeral": "e", "mac": "m" }
            }))
            .await
            .json();
        assert_eq!(
            put["count"].as_u64(),
            Some(1),
            "after one put, count=1; got {put}"
        );
        let etag_after_put = put["etag"].as_str().map(str::to_string);
        assert!(etag_after_put.is_some(), "put returns an etag; got {put}");

        // GET the session back — session_data must survive verbatim.
        let got: Value = server
            .get(&format!(
                "/_matrix/client/v3/room_keys/keys/{room}/{session}?version={version}"
            ))
            .add_header(an.clone(), av.clone())
            .await
            .json();
        assert_eq!(
            got["session_data"]["ciphertext"].as_str(),
            Some("ENCRYPTED"),
            "stored session_data must round-trip; got {got}"
        );

        // GET the whole backup — the room/session must be present.
        let all: Value = server
            .get(&format!(
                "/_matrix/client/v3/room_keys/keys?version={version}"
            ))
            .add_header(an.clone(), av.clone())
            .await
            .json();
        assert_eq!(
            all["rooms"][room]["sessions"][session]["session_data"]["ciphertext"].as_str(),
            Some("ENCRYPTED"),
            "whole-backup GET must include the stored session; got {all}"
        );
    }

    // room_key_backup_requires_auth:start
    //   purpose: Unauthenticated backup version creation must be rejected.
    // room_key_backup_requires_auth:end
    #[tokio::test]
    async fn room_key_backup_requires_auth() {
        let server = TestServer::new(router(AppState::new()));
        let resp = server
            .post("/_matrix/client/v3/room_keys/version")
            .json(
                &json!({ "algorithm": "m.megolm_backup.v1.curve25519-aes-sha2", "auth_data": {} }),
            )
            .await;
        assert_ne!(
            resp.status_code().as_u16(),
            200,
            "no-auth version create must not 200"
        );
    }

    // cross_signing_upload_and_query:start
    //   purpose: Uploaded cross-signing master/self-signing keys must be returned by
    //            keys/query for that user.
    // cross_signing_upload_and_query:end
    #[tokio::test]
    async fn cross_signing_upload_and_query() {
        let server = TestServer::new(router(AppState::new()));
        let (an, av) = auth(&server, "alice").await;
        // Alice's real user_id depends on AppState::new()'s server_name — ask whoami
        // rather than hardcoding the domain.
        let whoami: Value = server
            .get("/_matrix/client/v3/account/whoami")
            .add_header(an.clone(), av.clone())
            .await
            .json();
        let alice = whoami["user_id"]
            .as_str()
            .unwrap_or_else(|| panic!("whoami missing user_id: {whoami}"))
            .to_string();
        let alice = alice.as_str();

        cross_signing_upload(
            &server,
            &an,
            &av,
            "alice",
            json!({
                "master_key": {
                    "user_id": alice,
                    "usage": ["master"],
                    "keys": { "ed25519:MK": "master_pub" }
                },
                "self_signing_key": {
                    "user_id": alice,
                    "usage": ["self_signing"],
                    "keys": { "ed25519:SSK": "self_pub" }
                }
            }),
        )
        .await;

        let q: Value = server
            .post("/_matrix/client/v3/keys/query")
            .add_header(an.clone(), av.clone())
            .json(&json!({ "device_keys": { alice: [] } }))
            .await
            .json();

        assert_eq!(
            q["master_keys"][alice]["keys"]["ed25519:MK"].as_str(),
            Some("master_pub"),
            "keys/query must return the uploaded master_key; got {q}"
        );
        assert_eq!(
            q["self_signing_keys"][alice]["keys"]["ed25519:SSK"].as_str(),
            Some("self_pub"),
            "keys/query must return the uploaded self_signing_key; got {q}"
        );
    }
}
