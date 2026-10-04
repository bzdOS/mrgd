// START_AI_HEADER
// MODULE: matrix-hs/src/join_not_found_test.rs
// PURPOSE: Regression tests for the join defect found on the stand 2026-10-04:
//            `POST /_matrix/client/v3/rooms/{roomId}/join` answered 200 for a room the
//            node had never heard of and created it on the spot — `ensure_room_state()`
//            ran before anything checked whether the room existed, so the result was an
//            empty shell: one m.room.member event, no content, and a room id that then
//            looked real in the room list and in every later /sync. The Matrix spec's
//            answer for an unknown room is 404 M_NOT_FOUND, and creating the room is
//            exactly what must NOT happen.
//
//            Both directions are pinned here, because a fix that only adds the 404 would
//            also pass a test that checks "no longer creates" while breaking real joins:
//              1. join of a nonexistent room → 404 M_NOT_FOUND, and the room is absent
//                 from state afterwards (no shell, no state entry, no timeline);
//              2. join of an existing room → 200 and the room_id comes back unchanged.
//
//            Deliberately NOT asserted here: anything about the empty shells already in
//            the stand's store. Removing them is a data decision, not a code fix.
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

    // join_nonexistent_room_is_404_and_creates_nothing:start
    //   purpose: The defect itself. Joining a room the node never heard of must answer
    //            404 M_NOT_FOUND and must leave NO trace — the old behaviour answered
    //            200 and left a shell that looked like a real room forever.
    //   input:  a fresh server and one registered user
    //   output: ()
    //   sideEffects: in-memory AppState only; no files, no network, no listeners
    // join_nonexistent_room_is_404_and_creates_nothing:end
    #[tokio::test]
    async fn join_nonexistent_room_is_404_and_creates_nothing() {
        let server = test_server();
        let alice = register_and_bearer(&server, "alice_join_404").await;
        let ghost = "!never-existed:localhost";

        let resp = server
            .post(&format!("/_matrix/client/v3/rooms/{ghost}/join"))
            .add_header(alice.0.clone(), alice.1.clone())
            .await;
        assert_eq!(
            resp.status_code(),
            404,
            "an unknown room must not answer 200: {:?}",
            resp.text()
        );
        let body: Value = resp.json();
        assert_eq!(body["errcode"], "M_NOT_FOUND", "body was {body}");

        // No shell: the room must not appear in the user's joined rooms, and a second
        // attempt must still be a 404 (i.e. nothing was quietly created in between).
        let joined: Value = server
            .get("/_matrix/client/v3/joined_rooms")
            .add_header(alice.0.clone(), alice.1.clone())
            .await
            .json();
        let list: Vec<String> = serde_json::from_value(joined["joined_rooms"].clone()).unwrap_or_default();
        assert!(
            !list.iter().any(|r| r == ghost),
            "the room must not exist after a 404 join: {list:?}"
        );

        let again = server
            .post(&format!("/_matrix/client/v3/rooms/{ghost}/join"))
            .add_header(alice.0.clone(), alice.1.clone())
            .await;
        assert_eq!(again.status_code(), 404, "and it stays 404 on a retry");
    }

    // join_existing_room_is_200:start
    //   purpose: The other half, and the reason the fix cannot be "always 404": a room
    //            that really exists must still be joinable and must answer with the
    //            room_id that was asked for. A fix that broke this would be invisible to
    //            a test that only checks the refusal.
    //   input:  a fresh server, one registered user, one room created through the API
    //   output: ()
    //   sideEffects: in-memory AppState only
    // join_existing_room_is_200:end
    #[tokio::test]
    async fn join_existing_room_is_200() {
        let server = test_server();
        let alice = register_and_bearer(&server, "alice_join_200").await;

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

        let resp = server
            .post(&format!("/_matrix/client/v3/rooms/{room}/join"))
            .add_header(alice.0.clone(), alice.1.clone())
            .await;
        assert_eq!(resp.status_code(), 200, "an existing room must stay joinable");
        let body: Value = resp.json();
        assert_eq!(
            body["room_id"], room,
            "the answer must name the room that was asked for: {body}"
        );

        // And it really is a room now, not a shell: it shows up in joined_rooms.
        let joined: Value = server
            .get("/_matrix/client/v3/joined_rooms")
            .add_header(alice.0.clone(), alice.1.clone())
            .await
            .json();
        let list: Vec<String> = serde_json::from_value(joined["joined_rooms"].clone()).unwrap_or_default();
        assert!(list.iter().any(|r| r == &room), "joined_rooms was {list:?}");
    }

    // join_via_alias_of_unknown_room_is_404:start
    //   purpose: The alias path resolves an alias and then ran the same
    //            ensure_room_state() as the room-id path. An alias that resolves to a
    //            room this node has never heard of must also be a 404 — otherwise the
    //            defect is only half fixed and the alias route is a way around it.
    //   input:  a fresh server and one registered user; an alias pointing at a room that
    //            does not exist here (registered directly in the alias map)
    //   output: ()
    //   sideEffects: in-memory AppState only
    // join_via_alias_of_unknown_room_is_404:end
    #[tokio::test]
    async fn join_via_alias_of_unknown_room_is_404() {
        let state = AppState::new();
        let app = router(state.clone());
        let server = TestServer::new(app);
        let alice = register_and_bearer(&server, "alice_join_alias").await;

        // Point an alias at a room that has no state here — the shape a stale alias has
        // after its room was never replicated.
        let ghost = "!ghost-via-alias:localhost";
        state
            .aliases
            .lock()
            .expect("aliases lock")
            .insert("#ghost-alias:localhost".to_string(), ghost.to_string());

        let resp = server
            .post("/_matrix/client/v3/join/%23ghost-alias%3Alocalhost")
            .add_header(alice.0.clone(), alice.1.clone())
            .await;
        assert_eq!(
            resp.status_code(),
            404,
            "a resolvable alias must not license creating the room: {:?}",
            resp.text()
        );
        let body: Value = resp.json();
        assert_eq!(body["errcode"], "M_NOT_FOUND", "body was {body}");
    }
    // characterization_send_to_unknown_room_writes_a_room:start
    //   purpose: MEASUREMENT, not a fix. `PUT /rooms/{roomId}/send/{eventType}/{txnId}`
    //            (src/routes/send.rs:73) calls `state.ensure_room(&room_id)` with the
    //            comment "lazy create" and then inserts the PDU — with NO existence check
    //            and NO membership check. So an authenticated client that PUTs one message
    //            into a room id nobody ever heard of gets 200 back, and the node is left
    //            holding a fabricated room WITH a timeline event in it. That is a wider hole
    //            than the one the 404-join fix closed (join created an empty shell; this
    //            writes content into it), and `sync` unions `state.rooms`, so the room
    //            becomes reachable for the drain path as well.
    //
    //            This test asserts TODAY's behaviour on purpose, so the vector is a number
    //            in the suite rather than a suspicion in prose. The next dispatch that
    //            closes it will turn this red — that is the intended way for it to die.
    //            Nothing is fixed here: the stand is frozen inside somebody else's
    //            regression run, and the join dispatch named that endpoint only — so this
    //            branch must not smuggle a behaviour change into a deploy.
    //   input:  a fresh server and one registered user; a room id that does not exist
    //   output: ()
    //   sideEffects: in-memory AppState only
    // characterization_send_to_unknown_room_writes_a_room:end
    #[tokio::test]
    async fn characterization_send_to_unknown_room_writes_a_room() {
        let state = AppState::new();
        let app = router(state.clone());
        let server = TestServer::new(app);
        let alice = register_and_bearer(&server, "alice_send_unknown").await;
        let ghost = "!never-existed-send:localhost";

        assert!(
            !state.rooms.lock().expect("rooms lock").contains_key(ghost),
            "premise: {ghost} must not exist before the probe"
        );

        let resp = server
            .put(&format!(
                "/_matrix/client/v3/rooms/{ghost}/send/m.room.message/txn-1"
            ))
            .add_header(alice.0.clone(), alice.1.clone())
            .bytes(axum::body::Bytes::from_static(b"hello"))
            .await;

        let status = resp.status_code();
        let created = state.rooms.lock().expect("rooms lock").contains_key(ghost);
        let events = state
            .rooms
            .lock()
            .expect("rooms lock")
            .get(ghost)
            .map(|log| log.ordered().len())
            .unwrap_or(0);

        println!(
            "send to unknown room: status={status} room_log_created={created} timeline_events={events}"
        );

        // Assert the measured behaviour, so a change in either direction shows up as a
        // failure here rather than as a surprise on the stand.
        assert_eq!(
            status, 200,
            "MEASURED behaviour changed: send to an unknown room no longer answers 200 \
             (room_log_created={created}, timeline_events={events}). If this is now a fix, \
             flip this test to assert the refusal and the absence of the room."
        );
        assert!(
            created,
            "MEASURED behaviour changed: the room log is no longer created ({ghost})."
        );
        assert!(
            events >= 1,
            "MEASURED behaviour changed: the fabricated room no longer holds the event."
        );
    }

}
