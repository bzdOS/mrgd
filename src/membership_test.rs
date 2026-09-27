// START_AI_HEADER
// MODULE: matrix-hs/src/membership_test.rs
// PURPOSE: Integration tests for the room membership operations added in
//          routes/room_state.rs: leave, invite, kick, ban, unban, forget.
//
//          Tests prove the primary scenario end-to-end (alice creates a room,
//          invites bob, bob joins, alice kicks bob, alice bans bob) plus the
//          auth seam (a non-member cannot invite/kick/ban) and the remaining
//          endpoints (unban, leave, forget) individually.
// DEPENDENCIES: axum-test, serde_json, matrix_hs::{router, AppState}
// END_AI_HEADER

#[cfg(test)]
mod tests {
    use crate::{router, AppState};
    use axum_test::TestServer;
    use serde_json::{json, Value};

    fn test_server() -> TestServer {
        let state = AppState::new();
        let app = router(state);
        TestServer::new(app)
    }

    // register_and_bearer:start
    //   purpose: Register a user via two-step UIA and return an Authorization bearer header.
    //   input:  server — &TestServer; username — localpart
    //   output: (HeaderName, HeaderValue) for Authorization: Bearer <mxt_ token>
    //   sideEffects: inserts user into AppState via register
    // register_and_bearer:end
    async fn register_and_bearer(
        server: &TestServer,
        username: &str,
    ) -> (axum::http::HeaderName, axum::http::HeaderValue, String) {
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
        let user_id = reg["user_id"]
            .as_str()
            .unwrap_or_else(|| panic!("register_and_bearer: no user_id for {username}; got {reg}"))
            .to_string();
        (
            axum::http::HeaderName::from_static("authorization"),
            axum::http::HeaderValue::from_str(&format!("Bearer {token}")).expect("header value"),
            user_id,
        )
    }

    // member_event:start
    //   purpose: Fetch GET /rooms/{roomId}/members and return the m.room.member
    //            content for the given user_id, if present.
    //   input:  server, room_id, user_id
    //   output: Option<Value> — the member event's "content" object
    //   sideEffects: none
    // member_event:end
    async fn member_content(server: &TestServer, room_id: &str, user_id: &str) -> Option<Value> {
        let resp = server
            .get(&format!("/_matrix/client/v3/rooms/{room_id}/members"))
            .await;
        resp.assert_status_ok();
        let body: Value = resp.json();
        body["chunk"]
            .as_array()?
            .iter()
            .find(|ev| ev["state_key"].as_str() == Some(user_id))
            .map(|ev| ev["content"].clone())
    }

    // test:membership_full_lifecycle:start
    //   purpose: alice creates a room, invites bob (membership=invite); bob joins
    //            (membership=join); alice kicks bob (membership=leave); alice
    //            re-invites+bob rejoins so ban has a joined target; alice bans bob
    //            (membership=ban). Proves the primary scenario from the feature spec.
    //   input:  createRoom, invite, join, kick, invite, join, ban — all over HTTP
    //   output: membership content transitions as listed above
    //   sideEffects: room_state mutations in AppState
    // test:membership_full_lifecycle:end
    #[tokio::test]
    async fn membership_full_lifecycle() {
        let server = test_server();
        let (a_hn, a_hv, _alice_id) = register_and_bearer(&server, "alice_mem").await;
        let (_b_hn, b_hv, bob_id) = register_and_bearer(&server, "bob_mem").await;

        let create: Value = server
            .post("/_matrix/client/v3/createRoom")
            .add_header(a_hn.clone(), a_hv.clone())
            .json(&json!({}))
            .await
            .json();
        let room_id = create["room_id"].as_str().expect("room_id").to_string();

        // alice invites bob.
        server
            .post(&format!("/_matrix/client/v3/rooms/{room_id}/invite"))
            .add_header(a_hn.clone(), a_hv.clone())
            .json(&json!({ "user_id": bob_id }))
            .await
            .assert_status_ok();
        let content = member_content(&server, &room_id, &bob_id)
            .await
            .unwrap_or_else(|| panic!("expected bob member event after invite"));
        assert_eq!(
            content["membership"].as_str(),
            Some("invite"),
            "bob must be invited"
        );

        // bob joins.
        server
            .post(&format!("/_matrix/client/v3/rooms/{room_id}/join"))
            .add_header(
                axum::http::HeaderName::from_static("authorization"),
                b_hv.clone(),
            )
            .await
            .assert_status_ok();
        let content = member_content(&server, &room_id, &bob_id)
            .await
            .unwrap_or_else(|| panic!("expected bob member event after join"));
        assert_eq!(
            content["membership"].as_str(),
            Some("join"),
            "bob must be joined"
        );

        // alice kicks bob.
        server
            .post(&format!("/_matrix/client/v3/rooms/{room_id}/kick"))
            .add_header(a_hn.clone(), a_hv.clone())
            .json(&json!({ "user_id": bob_id, "reason": "testing kick" }))
            .await
            .assert_status_ok();
        let content = member_content(&server, &room_id, &bob_id)
            .await
            .unwrap_or_else(|| panic!("expected bob member event after kick"));
        assert_eq!(
            content["membership"].as_str(),
            Some("leave"),
            "bob must be left after kick"
        );
        assert_eq!(content["reason"].as_str(), Some("testing kick"));

        // alice re-invites + bob rejoins so ban has a real joined target.
        server
            .post(&format!("/_matrix/client/v3/rooms/{room_id}/invite"))
            .add_header(a_hn.clone(), a_hv.clone())
            .json(&json!({ "user_id": bob_id }))
            .await
            .assert_status_ok();
        server
            .post(&format!("/_matrix/client/v3/rooms/{room_id}/join"))
            .add_header(
                axum::http::HeaderName::from_static("authorization"),
                b_hv.clone(),
            )
            .await
            .assert_status_ok();

        // alice bans bob.
        server
            .post(&format!("/_matrix/client/v3/rooms/{room_id}/ban"))
            .add_header(a_hn.clone(), a_hv.clone())
            .json(&json!({ "user_id": bob_id, "reason": "testing ban" }))
            .await
            .assert_status_ok();
        let content = member_content(&server, &room_id, &bob_id)
            .await
            .unwrap_or_else(|| panic!("expected bob member event after ban"));
        assert_eq!(
            content["membership"].as_str(),
            Some("ban"),
            "bob must be banned"
        );
    }

    // test:low_power_member_cannot_kick_or_ban_a_higher_power_member:start
    //   purpose: Closes the gap require_caller_joined's own doc comment used to flag
    //            ("a misbehaving-but-joined member can currently kick/ban anyone").
    //            bob joins at users_default (0); alice (the creator, power 100) is
    //            far above the kick/ban threshold (50) and above bob either way, so
    //            bob's kick/ban of alice must be refused on power grounds alone —
    //            even though bob passes require_caller_joined (he IS a member).
    //            Then alice promotes bob to power 50 (== the ban/kick threshold
    //            itself) and bob tries again: he now REACHES the threshold but does
    //            NOT outrank alice (50 is not > 100) — must still be refused. This
    //            is the case a "reach the threshold" check alone (without the
    //            outrank half) would wrongly allow.
    //   input:  alice creates+bob joins; bob kicks/bans alice (403 both times);
    //           alice sets bob's power to 50; bob kicks/bans alice again (403 both)
    //   output: every kick/ban attempt by bob returns 403, never mutates alice's membership
    //   sideEffects: room_state mutations (bob's power_levels entry) in AppState
    // test:low_power_member_cannot_kick_or_ban_a_higher_power_member:end
    #[tokio::test]
    async fn low_power_member_cannot_kick_or_ban_a_higher_power_member() {
        let server = test_server();
        let (a_hn, a_hv, alice_id) = register_and_bearer(&server, "alice_pow").await;
        let (b_hn, b_hv, bob_id) = register_and_bearer(&server, "bob_pow").await;

        let create: Value = server
            .post("/_matrix/client/v3/createRoom")
            .add_header(a_hn.clone(), a_hv.clone())
            .json(&json!({}))
            .await
            .json();
        let room_id = create["room_id"].as_str().expect("room_id").to_string();

        server
            .post(&format!("/_matrix/client/v3/rooms/{room_id}/invite"))
            .add_header(a_hn.clone(), a_hv.clone())
            .json(&json!({ "user_id": bob_id }))
            .await
            .assert_status_ok();
        server
            .post(&format!("/_matrix/client/v3/rooms/{room_id}/join"))
            .add_header(b_hn.clone(), b_hv.clone())
            .await
            .assert_status_ok();

        // bob (power 0) cannot reach the kick/ban threshold (50) at all.
        server
            .post(&format!("/_matrix/client/v3/rooms/{room_id}/kick"))
            .add_header(b_hn.clone(), b_hv.clone())
            .json(&json!({ "user_id": alice_id }))
            .await
            .assert_status(axum::http::StatusCode::FORBIDDEN);
        server
            .post(&format!("/_matrix/client/v3/rooms/{room_id}/ban"))
            .add_header(b_hn.clone(), b_hv.clone())
            .json(&json!({ "user_id": alice_id }))
            .await
            .assert_status(axum::http::StatusCode::FORBIDDEN);

        // alice promotes bob to exactly the ban/kick threshold (50) — reaches it,
        // but still does not OUTRANK alice (100).
        server
            .put(&format!(
                "/_matrix/client/v3/rooms/{room_id}/state/m.room.power_levels"
            ))
            .add_header(a_hn.clone(), a_hv.clone())
            .json(&json!({
                "users": { &alice_id: 100, &bob_id: 50 },
                "users_default": 0,
                "events": {},
                "events_default": 0,
                "state_default": 50,
                "ban": 50,
                "kick": 50,
                "redact": 50,
                "invite": 50
            }))
            .await
            .assert_status_ok();

        server
            .post(&format!("/_matrix/client/v3/rooms/{room_id}/kick"))
            .add_header(b_hn.clone(), b_hv.clone())
            .json(&json!({ "user_id": alice_id }))
            .await
            .assert_status(axum::http::StatusCode::FORBIDDEN);
        server
            .post(&format!("/_matrix/client/v3/rooms/{room_id}/ban"))
            .add_header(b_hn.clone(), b_hv.clone())
            .json(&json!({ "user_id": alice_id }))
            .await
            .assert_status(axum::http::StatusCode::FORBIDDEN);

        // alice's membership must still be untouched throughout.
        let content = member_content(&server, &room_id, &alice_id)
            .await
            .unwrap_or_else(|| panic!("expected alice's own member event"));
        assert_eq!(content["membership"].as_str(), Some("join"));
    }

    // test:unban_requires_prior_ban:start
    //   purpose: unban on a target who is NOT currently banned returns 400 M_BAD_JSON
    //            (minimal precondition, documented in post_unban_room's contract);
    //            unban on an actually-banned target succeeds and sets membership=leave.
    //   input:  invite bob (not banned) -> unban rejected; ban bob -> unban succeeds
    //   output: first unban 400 M_BAD_JSON; second unban 200, membership=leave
    //   sideEffects: room_state mutations in AppState
    // test:unban_requires_prior_ban:end
    #[tokio::test]
    async fn unban_requires_prior_ban() {
        let server = test_server();
        let (a_hn, a_hv, _alice_id) = register_and_bearer(&server, "alice_unban").await;
        let (_b_hn, _b_hv, bob_id) = register_and_bearer(&server, "bob_unban").await;

        let create: Value = server
            .post("/_matrix/client/v3/createRoom")
            .add_header(a_hn.clone(), a_hv.clone())
            .json(&json!({}))
            .await
            .json();
        let room_id = create["room_id"].as_str().expect("room_id").to_string();

        server
            .post(&format!("/_matrix/client/v3/rooms/{room_id}/invite"))
            .add_header(a_hn.clone(), a_hv.clone())
            .json(&json!({ "user_id": bob_id }))
            .await
            .assert_status_ok();

        // Not banned yet: unban must be rejected.
        let resp = server
            .post(&format!("/_matrix/client/v3/rooms/{room_id}/unban"))
            .add_header(a_hn.clone(), a_hv.clone())
            .json(&json!({ "user_id": bob_id }))
            .await;
        resp.assert_status(axum::http::StatusCode::BAD_REQUEST);

        // Ban then unban: must succeed and set membership=leave.
        server
            .post(&format!("/_matrix/client/v3/rooms/{room_id}/ban"))
            .add_header(a_hn.clone(), a_hv.clone())
            .json(&json!({ "user_id": bob_id }))
            .await
            .assert_status_ok();
        server
            .post(&format!("/_matrix/client/v3/rooms/{room_id}/unban"))
            .add_header(a_hn.clone(), a_hv.clone())
            .json(&json!({ "user_id": bob_id }))
            .await
            .assert_status_ok();
        let content = member_content(&server, &room_id, &bob_id)
            .await
            .unwrap_or_else(|| panic!("expected bob member event after unban"));
        assert_eq!(
            content["membership"].as_str(),
            Some("leave"),
            "bob must be leave after unban"
        );
    }

    // test:leave_and_forget:start
    //   purpose: alice creates a room + leaves it (self-action, membership=leave);
    //            forget while still joined is forbidden (403); forget after leaving
    //            succeeds (200 {}).
    //   input:  createRoom, leave, forget (before/after leave)
    //   output: leave 200, membership=leave; forget-before-leave 403; forget-after-leave 200
    //   sideEffects: room_state mutations in AppState
    // test:leave_and_forget:end
    #[tokio::test]
    async fn leave_and_forget() {
        let server = test_server();
        let (a_hn, a_hv, alice_id) = register_and_bearer(&server, "alice_leave").await;

        let create: Value = server
            .post("/_matrix/client/v3/createRoom")
            .add_header(a_hn.clone(), a_hv.clone())
            .json(&json!({}))
            .await
            .json();
        let room_id = create["room_id"].as_str().expect("room_id").to_string();

        // Forgetting while still joined must be forbidden.
        let resp = server
            .post(&format!("/_matrix/client/v3/rooms/{room_id}/forget"))
            .add_header(a_hn.clone(), a_hv.clone())
            .await;
        resp.assert_status(axum::http::StatusCode::FORBIDDEN);

        // Leave.
        server
            .post(&format!("/_matrix/client/v3/rooms/{room_id}/leave"))
            .add_header(a_hn.clone(), a_hv.clone())
            .await
            .assert_status_ok();
        let content = member_content(&server, &room_id, &alice_id)
            .await
            .unwrap_or_else(|| panic!("expected alice member event after leave"));
        assert_eq!(
            content["membership"].as_str(),
            Some("leave"),
            "alice must have left"
        );

        // Forget after leaving must succeed.
        server
            .post(&format!("/_matrix/client/v3/rooms/{room_id}/forget"))
            .add_header(a_hn.clone(), a_hv.clone())
            .await
            .assert_status_ok();
    }

    // test:non_member_cannot_invite:start
    //   purpose: A registered user who has never joined the room gets 403 Forbidden
    //            when trying to invite another user — proves require_caller_joined.
    //   input:  eve (never joined room) POST invite {"user_id": carol}
    //   output: 403 Forbidden
    //   sideEffects: none (rejected before any state mutation)
    // test:non_member_cannot_invite:end
    #[tokio::test]
    async fn non_member_cannot_invite() {
        let server = test_server();
        let (a_hn, a_hv, _alice_id) = register_and_bearer(&server, "alice_seam").await;
        let (e_hn, e_hv, _eve_id) = register_and_bearer(&server, "eve_seam").await;
        let (_c_hn, _c_hv, carol_id) = register_and_bearer(&server, "carol_seam").await;

        let create: Value = server
            .post("/_matrix/client/v3/createRoom")
            .add_header(a_hn.clone(), a_hv.clone())
            .json(&json!({}))
            .await
            .json();
        let room_id = create["room_id"].as_str().expect("room_id").to_string();

        let resp = server
            .post(&format!("/_matrix/client/v3/rooms/{room_id}/invite"))
            .add_header(e_hn.clone(), e_hv.clone())
            .json(&json!({ "user_id": carol_id }))
            .await;
        resp.assert_status(axum::http::StatusCode::FORBIDDEN);
    }
}
