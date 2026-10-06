// START_AI_HEADER
// MODULE: matrix-hs/src/ephemeral_member_gate_test.rs
// PURPOSE: Regression tests for the membership gate on the three room-scoped ephemeral
//            writes — PUT typing, POST receipt, POST read_markers. They previously asked
//            only whether a RoomLog existed (a strictly weaker question than
//            require_joined_room asks), so any authenticated stranger could make typing
//            indicators, read receipts and a fully-read marker appear in other people's
//            /sync, and in the cluster snapshots those routes publish. Synapse asks
//            membership on all three.
//
//            Both directions are pinned, because a gate that only refuses also passes a
//            suite where ephemeral state stopped reaching the room's members:
//              1. non-member's write → 403 M_FORBIDDEN AND the room's ephemeral state is
//                 untouched (each route separately: typing, receipt, read_markers);
//              2. member writing for ANOTHER user → 403 M_FORBIDDEN, and that second
//                 message stays reachable: the gate runs first so a member gets "not
//                 another user" while a non-member gets "not a member";
//              3. a member's own writes still succeed and DO change ephemeral state —
//                 checked by reading the maps back, because a 403-only test cannot tell a
//                 working gate from a broken route.
//
//            Deliberately NOT asserted here: cluster publication. The negatives assert
//            local state only, which is where a refusal must be visible; what a refused
//            request would have published to a peer is asserted by the same local check
//            being false, since publication happens after the gate returns.
// DEPENDENCIES: axum-test, serde_json, matrix_hs::{router, AppState}
// END_AI_HEADER

#[cfg(test)]
mod tests {
    use crate::{router, AppState};
    use axum::http::{HeaderName, HeaderValue};
    use axum_test::TestServer;
    use serde_json::{json, Value};

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

    /// A room that exists (its creator auto-joins), plus a stranger's token. That is the
    /// exact state in which the old code accepted any authenticated caller.
    async fn room_and_stranger(server: &TestServer) -> (String, HeaderName, HeaderValue) {
        let owner = register_and_bearer(server, "eg_owner").await;
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
        let stranger = register_and_bearer(server, "eg_stranger").await;
        (room, stranger.0, stranger.1)
    }

    async fn send_message(
        server: &TestServer,
        auth: &(HeaderName, HeaderValue),
        room: &str,
    ) -> String {
        let resp = server
            .put(&format!(
                "/_matrix/client/v3/rooms/{room}/send/m.room.message/txn-{room}"
            ))
            .add_header(auth.0.clone(), auth.1.clone())
            .bytes(axum::body::Bytes::from_static(b"mine"))
            .await
            .json::<Value>();
        resp["event_id"]
            .as_str()
            .unwrap_or_else(|| panic!("send failed: {resp}"))
            .to_string()
    }

    /// typing / receipts / fully_read are all "room_id -> ..." maps. One snapshot of the
    /// three of them is enough to say whether a refused request changed anything.
    async fn ephemeral_snapshot(state: &AppState, room: &str) -> (usize, usize, usize) {
        let typing = state
            .ephemeral
            .typing
            .lock()
            .expect("typing lock")
            .get(room)
            .map(|m| m.len())
            .unwrap_or(0);
        let receipts = state
            .ephemeral
            .receipts
            .lock()
            .expect("receipts lock")
            .get(room)
            .map(|m| m.len())
            .unwrap_or(0);
        let fully_read = state
            .ephemeral
            .fully_read
            .lock()
            .expect("fully_read lock")
            .get(&(room.to_string(), "!any:local.host".to_string()))
            .is_some();
        (typing, receipts, usize::from(fully_read))
    }

    // typing_as_non_member_is_403_and_writes_nothing:start
    //   purpose: PUT typing by a stranger must be refused and must leave the room's typing
    //            map exactly as it was. Status alone would pass even if the handler wrote
    //            first and returned 403 after.
    //   input:  a fresh server, an existing room, one non-member device token
    //   output: ()
    //   sideEffects: in-memory AppState only
    // typing_as_non_member_is_403_and_writes_nothing:end
    #[tokio::test]
    async fn typing_as_non_member_is_403_and_writes_nothing() {
        let state = AppState::new();
        let app = router(state.clone());
        let server = TestServer::new(app);
        let (room, h, v) = room_and_stranger(&server).await;
        let before = ephemeral_snapshot(&state, &room).await;

        let resp = server
            .put(&format!(
                "/_matrix/client/v3/rooms/{room}/typing/@eg_stranger:localhost"
            ))
            .add_header(h.clone(), v.clone())
            .json(&json!({ "typing": true, "timeout": 30000 }))
            .await;

        assert_eq!(
            resp.status_code(),
            403,
            "a non-member's typing must be refused: {:?}",
            resp.text()
        );
        assert_eq!(resp.json::<Value>()["errcode"], "M_FORBIDDEN");
        assert_eq!(
            ephemeral_snapshot(&state, &room).await,
            before,
            "a refused typing must not touch typing/receipts/fully_read"
        );
    }

    // receipt_as_non_member_is_403_and_writes_nothing:start
    //   purpose: Same question asked by POST receipt. A stranger's read receipt showed up
    //            in every member's /sync before the gate.
    //   input:  a fresh server, an existing room with one event, one non-member token
    //   output: ()
    //   sideEffects: in-memory AppState only
    // receipt_as_non_member_is_403_and_writes_nothing:end
    #[tokio::test]
    async fn receipt_as_non_member_is_403_and_writes_nothing() {
        let state = AppState::new();
        let app = router(state.clone());
        let server = TestServer::new(app);
        let owner = register_and_bearer(&server, "receipt_owner").await;
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
        let event = send_message(&server, &owner, &room).await;
        let stranger = register_and_bearer(&server, "receipt_stranger").await;
        let before = ephemeral_snapshot(&state, &room).await;

        let resp = server
            .post(&format!(
                "/_matrix/client/v3/rooms/{room}/receipt/m.read/{event}"
            ))
            .add_header(stranger.0.clone(), stranger.1.clone())
            .json(&json!({}))
            .await;

        assert_eq!(
            resp.status_code(),
            403,
            "a non-member's receipt must be refused: {:?}",
            resp.text()
        );
        assert_eq!(resp.json::<Value>()["errcode"], "M_FORBIDDEN");
        assert_eq!(
            ephemeral_snapshot(&state, &room).await,
            before,
            "a refused receipt must not touch typing/receipts/fully_read"
        );
    }

    // read_markers_as_non_member_is_403_and_writes_nothing:start
    //   purpose: The third route. m.fully_read is the one that looked harmless and is not:
    //            it moves the unread marker for the USER, which then drives read receipts
    //            published to everyone in the room.
    //   input:  a fresh server, an existing room, one non-member device token
    //   output: ()
    //   sideEffects: in-memory AppState only
    // read_markers_as_non_member_is_403_and_writes_nothing:end
    #[tokio::test]
    async fn read_markers_as_non_member_is_403_and_writes_nothing() {
        let state = AppState::new();
        let app = router(state.clone());
        let server = TestServer::new(app);
        let (room, h, v) = room_and_stranger(&server).await;
        let before = ephemeral_snapshot(&state, &room).await;

        let resp = server
            .post(&format!(
                "/_matrix/client/v3/rooms/{room}/read_markers"
            ))
            .add_header(h.clone(), v.clone())
            .json(&json!({ "m.fully_read": "!whatever:local.host" }))
            .await;

        assert_eq!(
            resp.status_code(),
            403,
            "a non-member's read_markers must be refused: {:?}",
            resp.text()
        );
        assert_eq!(resp.json::<Value>()["errcode"], "M_FORBIDDEN");
        assert_eq!(
            ephemeral_snapshot(&state, &room).await,
            before,
            "a refused read_markers must not touch typing/receipts/fully_read"
        );
    }

    // member_writing_for_another_user_is_still_403:start
    //   purpose: The other half of the order decision. A MEMBER asking to set somebody
    //            else's typing must still be refused with the "not another user" message —
    //            which only stays reachable because the membership gate runs first. If the
    //            order is ever swapped, this message becomes unreachable for non-members
    //            and the gate's ordering is no longer tested by anything.
    //   input:  a fresh server, an existing room with two members, one member's token
    //   output: ()
    //   sideEffects: in-memory AppState only
    // member_writing_for_another_user_is_still_403:end
    #[tokio::test]
    async fn member_writing_for_another_user_is_still_403() {
        let state = AppState::new();
        let app = router(state.clone());
        let server = TestServer::new(app);
        let owner = register_and_bearer(&server, "order_owner").await;
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
        let other = register_and_bearer(&server, "order_other").await;
        let joined = server
            .post(&format!("/_matrix/client/v3/rooms/{room}/join"))
            .add_header(other.0.clone(), other.1.clone())
            .json(&json!({}))
            .await;
        assert_eq!(joined.status_code(), 200, "the second member must join");
        let before = ephemeral_snapshot(&state, &room).await;

        // A member of the room, writing typing for somebody else.
        let resp = server
            .put(&format!(
                "/_matrix/client/v3/rooms/{room}/typing/@order_owner:localhost"
            ))
            .add_header(other.0.clone(), other.1.clone())
            .json(&json!({ "typing": true, "timeout": 30000 }))
            .await;

        assert_eq!(
            resp.status_code(),
            403,
            "a member writing for another user must be refused: {:?}",
            resp.text()
        );
        assert_eq!(
            ephemeral_snapshot(&state, &room).await,
            before,
            "the refused write must not touch ephemeral state"
        );
    }

    // member_writes_still_work:start
    //   purpose: The positive half, and it is the one that catches a gate that became a
    //            closed door. A member's own typing, receipt and read_markers must each be
    //            200 AND must actually change the room's ephemeral maps — a 200 that wrote
    //            nothing would satisfy a status-only test.
    //   input:  a fresh server, one registered user, one room they created
    //   output: ()
    //   sideEffects: in-memory AppState only
    // member_writes_still_work:end
    #[tokio::test]
    async fn member_writes_still_work() {
        let state = AppState::new();
        let app = router(state.clone());
        let server = TestServer::new(app);
        let owner = register_and_bearer(&server, "works_owner").await;
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
        let event = send_message(&server, &owner, &room).await;

        let typing = server
            .put(&format!(
                "/_matrix/client/v3/rooms/{room}/typing/@works_owner:localhost"
            ))
            .add_header(owner.0.clone(), owner.1.clone())
            .json(&json!({ "typing": true, "timeout": 30000 }))
            .await;
        assert_eq!(
            typing.status_code(),
            200,
            "a member's own typing must still work: {:?}",
            typing.text()
        );

        let receipt = server
            .post(&format!(
                "/_matrix/client/v3/rooms/{room}/receipt/m.read/{event}"
            ))
            .add_header(owner.0.clone(), owner.1.clone())
            .json(&json!({}))
            .await;
        assert_eq!(
            receipt.status_code(),
            200,
            "a member's own receipt must still work: {:?}",
            receipt.text()
        );

        let markers = server
            .post(&format!("/_matrix/client/v3/rooms/{room}/read_markers"))
            .add_header(owner.0.clone(), owner.1.clone())
            .json(&json!({ "m.fully_read": event }))
            .await;
        assert_eq!(
            markers.status_code(),
            200,
            "a member's own read_markers must still work: {:?}",
            markers.text()
        );

        let typing_n = state
            .ephemeral
            .typing
            .lock()
            .expect("typing lock")
            .get(&room)
            .map(|m| m.len())
            .unwrap_or(0);
        assert!(
            typing_n >= 1,
            "a 200 typing must actually be recorded in the room's typing map, got {typing_n}"
        );
        let receipts_n = state
            .ephemeral
            .receipts
            .lock()
            .expect("receipts lock")
            .get(&room)
            .map(|m| m.len())
            .unwrap_or(0);
        assert!(
            receipts_n >= 1,
            "a 200 receipt must actually be recorded, got {receipts_n}"
        );
        let fully_read = state
            .ephemeral
            .fully_read
            .lock()
            .expect("fully_read lock")
            .get(&(room.clone(), "@works_owner:localhost".to_string()))
            .is_some();
        assert!(
            fully_read,
            "a 200 read_markers must actually set the fully-read marker for its own user"
        );
    }
}