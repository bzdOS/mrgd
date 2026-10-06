// START_AI_HEADER
// MODULE: matrix-hs/src/member_gate_test.rs
// PURPOSE: Regression tests for the membership gate on client writes that append to a room
//            (send / state / redact). Before it, the three routes each asked a DIFFERENT
//            question: send checked only that the room existed — a non-member's message went
//            through with 200, measured — redact checked only membership and so could not
//            404 for an unknown room, and the state route had a third room test written
//            inline against state.rooms only. One predicate, require_joined_room, now
//            answers for all three: unknown room → 404 M_NOT_FOUND, known room + non-member →
//            403 M_FORBIDDEN.
//
//            Both directions are pinned per class, because a gate that only refuses also
//            passes a test suite where rooms stopped accepting their own members:
//              1. non-member's write to an EXISTING room → 403 M_FORBIDDEN and nothing lands
//                 in the room (send, state and redact, one test each);
//              2. the creator, who auto-joins at creation, still gets 200 — otherwise the
//                 gate would have broken room creation and the walkthrough;
//              3. the older characterization (unknown room → 404) stays green, owned by
//                 join_not_found_test.rs and re-run in the same suite.
//
//            Deliberately NOT asserted here: power levels. Membership is "joined"; whether an
//            event type needs a power level to post is a separate rule that this gate
//            deliberately does not decide.
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

    /// A room that really exists, plus a stranger's bearer token. The room is created by
    /// one user, so it is known AND has a joined member — exactly the case where the old
    /// send route answered 200 to anybody.
    async fn existing_room_and_stranger(
        server: &TestServer,
    ) -> (String, HeaderName, HeaderValue) {
        let owner = register_and_bearer(server, "gate_owner").await;
        let created: Value = server
            .post("/_matrix/client/v3/createRoom")
            .add_header(owner.0.clone(), owner.1.clone())
            .json(&json!({}))
            .await
            .json();
        let room = created["room_id"]
            .as_str()
            .unwrap_or_else(|| panic!("no room_id in {created}"))
            .to_string();
        let stranger = register_and_bearer(server, "gate_stranger").await;
        (room, stranger.0, stranger.1)
    }

    async fn room_event_count(state: &AppState, room: &str) -> usize {
        state
            .rooms
            .lock()
            .expect("rooms lock")
            .get(room)
            .map(|log| log.ordered().len())
            .unwrap_or(0)
    }

    // send_as_non_member_into_existing_room_is_403:start
    //   purpose: The measured defect — a stranger's message was accepted into a room they
    //            never joined. Must now be 403 M_FORBIDDEN, and the room must not grow.
    //   input:  a fresh server, an existing room, one non-member device token
    //   output: ()
    //   sideEffects: in-memory AppState only
    // send_as_non_member_into_existing_room_is_403:end
    #[tokio::test]
    async fn send_as_non_member_into_existing_room_is_403() {
        let state = AppState::new();
        let app = router(state.clone());
        let server = TestServer::new(app);
        let (room, h, v) = existing_room_and_stranger(&server).await;
        let before = room_event_count(&state, &room).await;

        let resp = server
            .put(&format!(
                "/_matrix/client/v3/rooms/{room}/send/m.room.message/txn-gate"
            ))
            .add_header(h.clone(), v.clone())
            .bytes(axum::body::Bytes::from_static(b"not mine"))
            .await;

        assert_eq!(
            resp.status_code(),
            403,
            "a non-member's send into an existing room must be refused: {:?}",
            resp.text()
        );
        let body: Value = resp.json();
        assert_eq!(body["errcode"], "M_FORBIDDEN", "body was {body}");

        let after = room_event_count(&state, &room).await;
        assert_eq!(
            after, before,
            "a refused send must leave the room's timeline exactly as it was ({before} -> {after})"
        );
    }

    // state_write_as_non_member_into_existing_room_is_403:start
    //   purpose: Same question asked by the state route, which used to have its own
    //            inline room test and no membership test at all.
    //   input:  a fresh server, an existing room, one non-member device token
    //   output: ()
    //   sideEffects: in-memory AppState only
    // state_write_as_non_member_into_existing_room_is_403:end
    #[tokio::test]
    async fn state_write_as_non_member_into_existing_room_is_403() {
        let state = AppState::new();
        let app = router(state.clone());
        let server = TestServer::new(app);
        let (room, h, v) = existing_room_and_stranger(&server).await;

        let resp = server
            .put(&format!(
                "/_matrix/client/v3/rooms/{room}/state/m.room.topic"
            ))
            .add_header(h.clone(), v.clone())
            .json(&json!({ "topic": "not mine" }))
            .await;

        assert_eq!(
            resp.status_code(),
            403,
            "a non-member's state write must be refused: {:?}",
            resp.text()
        );
        let body: Value = resp.json();
        assert_eq!(body["errcode"], "M_FORBIDDEN", "body was {body}");

        let topic = state
            .room_state
            .lock()
            .expect("room_state lock")
            .get(&room)
            .map(|evs| evs.iter().any(|e| e.event_type == "m.room.topic"))
            .unwrap_or(false);
        assert!(
            !topic,
            "a refused state write must not leave an m.room.topic behind"
        );
    }

    // redact_as_non_member_into_existing_room_is_403:start
    //   purpose: The third class. Redact already refused non-members — but inline, and with
    //            no room check, so it could never answer 404. Now it asks the same gate, and
    //            a non-member is still refused.
    //   input:  a fresh server, an existing room with one event, one non-member token
    //   output: ()
    //   sideEffects: in-memory AppState only
    // redact_as_non_member_into_existing_room_is_403:end
    #[tokio::test]
    async fn redact_as_non_member_into_existing_room_is_403() {
        let state = AppState::new();
        let app = router(state.clone());
        let server = TestServer::new(app);
        let owner = register_and_bearer(&server, "redact_owner").await;
        let created: Value = server
            .post("/_matrix/client/v3/createRoom")
            .add_header(owner.0.clone(), owner.1.clone())
            .json(&json!({}))
            .await
            .json();
        let room = created["room_id"]
            .as_str()
            .unwrap_or_else(|| panic!("no room_id in {created}"))
            .to_string();
        let sent = server
            .put(&format!(
                "/_matrix/client/v3/rooms/{room}/send/m.room.message/txn-redact-me"
            ))
            .add_header(owner.0.clone(), owner.1.clone())
            .bytes(axum::body::Bytes::from_static(b"mine"))
            .await
            .json::<Value>();
        let target = sent["event_id"]
            .as_str()
            .unwrap_or_else(|| panic!("no event_id in {sent}"))
            .to_string();
        let stranger = register_and_bearer(&server, "redact_stranger").await;

        let resp = server
            .put(&format!(
                "/_matrix/client/v3/rooms/{room}/redact/{target}/txn-redact"
            ))
            .add_header(stranger.0.clone(), stranger.1.clone())
            .json(&json!({}))
            .await;

        assert_eq!(
            resp.status_code(),
            403,
            "a non-member's redact must be refused: {:?}",
            resp.text()
        );
        let body: Value = resp.json();
        assert_eq!(body["errcode"], "M_FORBIDDEN", "body was {body}");
        assert!(
            !state.redactions.lock().expect("redactions lock").contains_key(&target),
            "a refused redact must not be recorded"
        );
    }

    // member_still_writes_and_unknown_room_is_still_404:start
    //   purpose: The other direction, in one place: the gate must not have become a door
    //            that nobody can use. The creator auto-joins at creation, so create-then-send
    //            is 200; and the older characterization (unknown room → 404) still holds, so
    //            the order of the two questions inside the gate is pinned too.
    //   input:  a fresh server, one registered user, one created room, one unknown room id
    //   output: ()
    //   sideEffects: in-memory AppState only
    // member_still_writes_and_unknown_room_is_still_404:end
    #[tokio::test]
    async fn member_still_writes_and_unknown_room_is_still_404() {
        let server = test_server();
        let alice = register_and_bearer(&server, "gate_member").await;
        let created: Value = server
            .post("/_matrix/client/v3/createRoom")
            .add_header(alice.0.clone(), alice.1.clone())
            .json(&json!({}))
            .await
            .json();
        let room = created["room_id"]
            .as_str()
            .unwrap_or_else(|| panic!("no room_id in {created}"))
            .to_string();

        let ok = server
            .put(&format!(
                "/_matrix/client/v3/rooms/{room}/send/m.room.message/txn-ok"
            ))
            .add_header(alice.0.clone(), alice.1.clone())
            .bytes(axum::body::Bytes::from_static(b"mine"))
            .await;
        assert_eq!(
            ok.status_code(),
            200,
            "a member's send must keep working — the creator auto-joins: {:?}",
            ok.text()
        );

        let gone = server
            .put(
                "/_matrix/client/v3/rooms/!nosuch:local.host/send/m.room.message/txn-gone",
            )
            .add_header(alice.0.clone(), alice.1.clone())
            .bytes(axum::body::Bytes::from_static(b"hello"))
            .await;
        assert_eq!(
            gone.status_code(),
            404,
            "an unknown room must still be 404, not 403: {:?}",
            gone.text()
        );
        assert_eq!(gone.json::<Value>()["errcode"], "M_NOT_FOUND");
    }
}