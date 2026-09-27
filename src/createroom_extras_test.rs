// START_AI_HEADER
// MODULE: matrix-hs/src/createroom_extras_test.rs
// PURPOSE: Integration tests for three createRoom/room gaps found and reported by
//          other agents against a deployed matrix-hs build (via the hubd "matrix-hs"
//          queue, 2026-07-20) while wiring up a real bot use case:
//            1. m.room.join_rules honoring `preset` ("public_chat" -> "public";
//               anything else / absent -> "invite", unchanged default).
//            2. createRoom's `invite: [user_id, ...]` array actually inviting those
//               users (m.room.member(invite) events), not silently ignored.
//            3. GET /rooms/{roomId}/joined_members — a distinct, simpler endpoint
//               from /members that was missing entirely.
// DEPENDENCIES: axum-test, serde_json, matrix_hs::{router, AppState}
// END_AI_HEADER

#[cfg(test)]
mod tests {
    use crate::{router, AppState};
    use axum::http::{HeaderName, HeaderValue};
    use axum_test::TestServer;
    use serde_json::{json, Value};

    fn test_server() -> TestServer {
        let state = AppState::new();
        let app = router(state);
        TestServer::new(app)
    }

    async fn register_and_bearer(server: &TestServer, username: &str) -> (HeaderName, HeaderValue) {
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

    async fn whoami(server: &TestServer, auth: &(HeaderName, HeaderValue)) -> String {
        let body: Value = server
            .get("/_matrix/client/v3/account/whoami")
            .add_header(auth.0.clone(), auth.1.clone())
            .await
            .json();
        body["user_id"]
            .as_str()
            .unwrap_or_else(|| panic!("whoami missing user_id: {body}"))
            .to_string()
    }

    // public_chat_preset_sets_public_join_rule:start
    //   purpose: createRoom with preset:"public_chat" must result in an actual
    //            join_rule:"public" state event, not the previous hardcoded "invite".
    // public_chat_preset_sets_public_join_rule:end
    #[tokio::test]
    async fn public_chat_preset_sets_public_join_rule() {
        let server = test_server();
        let alice = register_and_bearer(&server, "alice_preset").await;

        let create: Value = server
            .post("/_matrix/client/v3/createRoom")
            .add_header(alice.0.clone(), alice.1.clone())
            .json(&json!({ "preset": "public_chat" }))
            .await
            .json();
        let room_id = create["room_id"].as_str().expect("room_id").to_string();

        let content: Value = server
            .get(&format!(
                "/_matrix/client/v3/rooms/{room_id}/state/m.room.join_rules"
            ))
            .add_header(alice.0.clone(), alice.1.clone())
            .await
            .json();
        assert_eq!(
            content["join_rule"].as_str(),
            Some("public"),
            "preset:public_chat must set join_rule:public; got {content}"
        );
    }

    // no_preset_defaults_to_invite:start
    //   purpose: createRoom with no preset at all must be unchanged from before this
    //            fix — join_rule defaults to "invite" (no regression for existing
    //            callers that never send preset).
    // no_preset_defaults_to_invite:end
    #[tokio::test]
    async fn no_preset_defaults_to_invite() {
        let server = test_server();
        let alice = register_and_bearer(&server, "alice_nopreset").await;

        let create: Value = server
            .post("/_matrix/client/v3/createRoom")
            .add_header(alice.0.clone(), alice.1.clone())
            .json(&json!({}))
            .await
            .json();
        let room_id = create["room_id"].as_str().expect("room_id").to_string();

        let content: Value = server
            .get(&format!(
                "/_matrix/client/v3/rooms/{room_id}/state/m.room.join_rules"
            ))
            .add_header(alice.0.clone(), alice.1.clone())
            .await
            .json();
        assert_eq!(content["join_rule"].as_str(), Some("invite"));
    }

    // create_room_invite_array_invites_users:start
    //   purpose: createRoom's `invite:[user_id,...]` array must actually produce
    //            m.room.member(invite) events for those users — bob (invited at
    //            creation, never having joined) must show membership:"invite" and
    //            appear in GET /members; the creator must NOT be double-invited
    //            even if listed in the array.
    // create_room_invite_array_invites_users:end
    #[tokio::test]
    async fn create_room_invite_array_invites_users() {
        let server = test_server();
        let alice = register_and_bearer(&server, "alice_invite").await;
        let bob = register_and_bearer(&server, "bob_invite").await;
        let alice_id = whoami(&server, &alice).await;
        let bob_id = whoami(&server, &bob).await;

        let create: Value = server
            .post("/_matrix/client/v3/createRoom")
            .add_header(alice.0.clone(), alice.1.clone())
            .json(&json!({ "invite": [bob_id, alice_id] }))
            .await
            .json();
        let room_id = create["room_id"].as_str().expect("room_id").to_string();

        let members: Value = server
            .get(&format!("/_matrix/client/v3/rooms/{room_id}/members"))
            .add_header(alice.0.clone(), alice.1.clone())
            .await
            .json();
        let chunk = members["chunk"].as_array().expect("chunk array");

        let bob_member = chunk
            .iter()
            .find(|ev| ev["state_key"] == bob_id)
            .unwrap_or_else(|| panic!("bob must have a member event; got {chunk:?}"));
        assert_eq!(
            bob_member["content"]["membership"].as_str(),
            Some("invite"),
            "bob listed in createRoom's invite array must be invited; got {bob_member}"
        );

        let alice_members: Vec<&Value> = chunk
            .iter()
            .filter(|ev| ev["state_key"] == alice_id)
            .collect();
        assert_eq!(
            alice_members.len(), 1,
            "the creator must not be double-invited even when listed in invite:[]; got {alice_members:?}"
        );
        assert_eq!(
            alice_members[0]["content"]["membership"].as_str(),
            Some("join")
        );
    }

    // joined_members_lists_only_joined_users:start
    //   purpose: GET /joined_members (distinct from /members) must return ONLY
    //            currently-joined users, shaped as {joined: {user_id: {display_name,
    //            avatar_url}}} — an invited-but-not-joined user must be absent.
    // joined_members_lists_only_joined_users:end
    #[tokio::test]
    async fn joined_members_lists_only_joined_users() {
        let server = test_server();
        let alice = register_and_bearer(&server, "alice_jm").await;
        let bob = register_and_bearer(&server, "bob_jm").await;
        let alice_id = whoami(&server, &alice).await;
        let bob_id = whoami(&server, &bob).await;

        let create: Value = server
            .post("/_matrix/client/v3/createRoom")
            .add_header(alice.0.clone(), alice.1.clone())
            .json(&json!({ "invite": [bob_id.clone()] }))
            .await
            .json();
        let room_id = create["room_id"].as_str().expect("room_id").to_string();

        let joined: Value = server
            .get(&format!(
                "/_matrix/client/v3/rooms/{room_id}/joined_members"
            ))
            .add_header(alice.0.clone(), alice.1.clone())
            .await
            .json();

        let joined_map = joined["joined"]
            .as_object()
            .unwrap_or_else(|| panic!("joined_members missing joined object: {joined}"));
        assert!(
            joined_map.contains_key(&alice_id),
            "creator (joined) must be in joined_members; got {joined_map:?}"
        );
        assert!(
            !joined_map.contains_key(&bob_id),
            "invited-but-not-joined bob must NOT be in joined_members; got {joined_map:?}"
        );

        // bob actually joins — now he must appear.
        server
            .post(&format!("/_matrix/client/v3/rooms/{room_id}/join"))
            .add_header(bob.0.clone(), bob.1.clone())
            .json(&json!({}))
            .await
            .assert_status_ok();

        let joined_after: Value = server
            .get(&format!(
                "/_matrix/client/v3/rooms/{room_id}/joined_members"
            ))
            .add_header(alice.0.clone(), alice.1.clone())
            .await
            .json();
        assert!(
            joined_after["joined"]
                .as_object()
                .expect("object")
                .contains_key(&bob_id),
            "bob must appear in joined_members after actually joining; got {joined_after}"
        );
    }
}
