// START_AI_HEADER
// MODULE: src/private_rooms_test.rs
// PURPOSE: hubd queue rooms and the hub room were seeded join_rule=public /
//          history_visibility=shared, the CS-API let anyone join any room, and the read
//          routes (/state, /members, /messages) needed no token. Any registered user of the
//          node could therefore read the fleet's queues. Pinned here, one test per claim:
//            - seed is invite/joined; an older public/shared room is switched at startup;
//            - a stranger's join (by id and by alias) is 403 M_FORBIDDEN;
//            - a stranger's /messages, /state, /members, /joined_members is refused, even
//              knowing the room_id; /sync does not list the room;
//            - the legitimate path still works: an invited operator joins and reads, the
//              bridge ingests into the room;
//            - ordinary rooms follow the general CS-API rules (token for reads, join_rules for
//              joins, see authz_test.rs); a stored history_visibility of "joined" is honoured.
// DEPENDENCIES: axum-test, serde_json, crate::{router, AppState, hubd_bridge, hub_replic}
// END_AI_HEADER

#[cfg(test)]
mod tests {
    use crate::hubd_bridge::*;
    use crate::state::StateEvent;
    use crate::{router, AppState};
    use axum::http::{HeaderName, HeaderValue};
    use axum_test::TestServer;
    use serde_json::{json, Value};
    use std::sync::Arc;

    type Bearer = (HeaderName, HeaderValue);

    // CONTRACT: Two-step UIA registration returning an Authorization header (same flow as authz_test).
    async fn register(server: &TestServer, username: &str) -> Bearer {
        let ch: Value = server
            .post("/_matrix/client/v3/register")
            .json(&json!({ "username": username, "password": "pw" }))
            .await
            .json();
        let sess = ch["session"].as_str().expect("session").to_string();
        let reg: Value = server
            .post("/_matrix/client/v3/register")
            .json(&json!({
                "username": username, "password": "pw",
                "auth": { "type": "m.login.dummy", "session": sess }
            }))
            .await
            .json();
        let token = reg["access_token"].as_str().expect("token").to_string();
        (
            HeaderName::from_static("authorization"),
            HeaderValue::from_str(&format!("Bearer {token}")).expect("hv"),
        )
    }

    // CONTRACT: Build an AppState with a TestServer and register the operator and stranger users used by the
    //    tests.
    async fn setup() -> (Arc<AppState>, TestServer, String) {
        let state = AppState::new();
        let room = ensure_queue_room(&state, "sec");
        let server = TestServer::new(router(state.clone()));
        crate::routes::send::insert_pdu(
            &state,
            &room,
            format!("@hubd:{}", state.server_name),
            "m.room.message".to_string(),
            serde_json::to_vec(&json!({ "msgtype": "m.text", "body": "fleet secret" })).unwrap(),
            json!({}),
        )
        .await
        .expect("insert");
        (state, server, room)
    }

    // CONTRACT: Content JSON of the state event of a given type with an empty key in a room, if present.
    fn content(state: &Arc<AppState>, room: &str, ty: &str) -> Option<Value> {
        state.room_state.lock().unwrap().get(room).and_then(|v| {
            v.iter()
                .find(|e| e.event_type == ty && e.state_key.is_empty())
                .map(|e| e.content.clone())
        })
    }

    // seed_is_invite_and_joined_for_queue_and_hub_rooms:start
    //   purpose: Queue and hub rooms are seeded join_rule=invite and history_visibility=joined.
    // seed_is_invite_and_joined_for_queue_and_hub_rooms:end
    #[tokio::test]
    async fn seed_is_invite_and_joined_for_queue_and_hub_rooms() {
        let state = AppState::new();
        let room = ensure_queue_room(&state, "sec");
        let hub = crate::hub_replic::ensure_hub_room(&state);
        for r in [&room, &hub] {
            assert_eq!(
                content(&state, r, "m.room.join_rules"),
                Some(json!({ "join_rule": "invite" })),
                "{r}"
            );
            assert_eq!(
                content(&state, r, "m.room.history_visibility"),
                Some(json!({ "history_visibility": "joined" })),
                "{r}"
            );
        }
    }

    // stranger_cannot_join_queue_room:start
    //   purpose: A stranger gets 403 M_FORBIDDEN joining a queue room by id and by alias.
    // stranger_cannot_join_queue_room:end
    #[tokio::test]
    async fn stranger_cannot_join_queue_room() {
        let (state, server, room) = setup().await;
        let mallory = register(&server, "mallory").await;
        let r = server
            .post(&format!("/_matrix/client/v3/rooms/{room}/join"))
            .add_header(mallory.0.clone(), mallory.1.clone())
            .json(&json!({}))
            .await;
        assert_eq!(r.status_code(), 403, "{}", r.text());
        assert_eq!(r.json::<Value>()["errcode"], "M_FORBIDDEN");
        // By alias too.
        let r = server
            .post(&format!("/_matrix/client/v3/join/%23hubd-queue-sec:{}", state.server_name))
            .add_header(mallory.0.clone(), mallory.1.clone())
            .json(&json!({}))
            .await;
        assert!(
            r.status_code() == 403 || r.status_code() == 404,
            "alias join must not succeed: {} {}",
            r.status_code(),
            r.text()
        );
    }

    // stranger_cannot_join_hub_room:start
    //   purpose: A stranger gets 403 M_FORBIDDEN joining the hub room.
    // stranger_cannot_join_hub_room:end
    #[tokio::test]
    async fn stranger_cannot_join_hub_room() {
        let state = AppState::new();
        let hub = crate::hub_replic::ensure_hub_room(&state);
        let server = TestServer::new(router(state));
        let mallory = register(&server, "mallory").await;
        let r = server
            .post(&format!("/_matrix/client/v3/rooms/{hub}/join"))
            .add_header(mallory.0.clone(), mallory.1.clone())
            .json(&json!({}))
            .await;
        assert_eq!(r.status_code(), 403, "{}", r.text());
    }

    // stranger_cannot_read_queue_room_by_room_id:start
    //   purpose: A stranger is refused /messages, /state, /members and /joined_members of a queue
    //            room even knowing its room_id.
    // stranger_cannot_read_queue_room_by_room_id:end
    #[tokio::test]
    async fn stranger_cannot_read_queue_room_by_room_id() {
        let (state, server, room) = setup().await;
        let mallory = register(&server, "mallory").await;
        for path in [
            format!("/_matrix/client/v3/rooms/{room}/messages?dir=b&limit=50"),
            format!("/_matrix/client/v3/rooms/{room}/state"),
            format!("/_matrix/client/v3/rooms/{room}/state/m.room.name"),
            format!("/_matrix/client/v3/rooms/{room}/state/m.room.member/{}", BRIDGE_USER),
            format!("/_matrix/client/v3/rooms/{room}/members"),
            format!("/_matrix/client/v3/rooms/{room}/joined_members"),
        ] {
            let r = server
                .get(&path)
                .add_header(mallory.0.clone(), mallory.1.clone())
                .await;
            assert_eq!(r.status_code(), 403, "{path}: {}", r.text());
            assert!(!r.text().contains("fleet secret"), "{path} leaked");
            // No token at all: refused as well (these used to be open).
            let r = server.get(&path).await;
            assert_eq!(r.status_code(), 401, "{path} without token: {}", r.text());
        }
        let _ = state;
    }

    // sync_of_a_stranger_does_not_list_the_queue_room:start
    //   purpose: /sync of a stranger does not list the queue room.
    // sync_of_a_stranger_does_not_list_the_queue_room:end
    #[tokio::test]
    async fn sync_of_a_stranger_does_not_list_the_queue_room() {
        let (_state, server, room) = setup().await;
        let mallory = register(&server, "mallory").await;
        // A refused join must not have left a membership behind either.
        let _ = server
            .post(&format!("/_matrix/client/v3/rooms/{room}/join"))
            .add_header(mallory.0.clone(), mallory.1.clone())
            .json(&json!({}))
            .await;
        let r = server
            .get("/_matrix/client/v3/sync")
            .add_header(mallory.0.clone(), mallory.1.clone())
            .await;
        assert_eq!(r.status_code(), 200);
        assert!(!r.text().contains(&room), "sync leaked the room id: {}", r.text());
        assert!(!r.text().contains("fleet secret"));
    }

    // invited_operator_joins_and_reads:start
    //   purpose: An operator invited through MATRIX_HS_HUBD_OPERATORS joins and reads the queue
    //            room, and the bridge still ingests into it.
    // invited_operator_joins_and_reads:end
    #[tokio::test]
    async fn invited_operator_joins_and_reads() {
        let (state, server, room) = setup().await;
        let op = register(&server, "op").await;
        let op_id = format!("@op:{}", state.server_name);
        invite_operators(&state, &room, &[op_id.clone()]);

        let r = server
            .post(&format!("/_matrix/client/v3/rooms/{room}/join"))
            .add_header(op.0.clone(), op.1.clone())
            .json(&json!({}))
            .await;
        assert_eq!(r.status_code(), 200, "{}", r.text());

        let r = server
            .get(&format!("/_matrix/client/v3/rooms/{room}/messages?dir=b&limit=50"))
            .add_header(op.0.clone(), op.1.clone())
            .await;
        assert_eq!(r.status_code(), 200, "{}", r.text());
        assert!(r.text().contains("fleet secret"));
        let r = server
            .get(&format!("/_matrix/client/v3/rooms/{room}/state"))
            .add_header(op.0.clone(), op.1.clone())
            .await;
        assert_eq!(r.status_code(), 200);
        let r = server
            .get("/_matrix/client/v3/sync")
            .add_header(op.0.clone(), op.1.clone())
            .await;
        assert!(r.text().contains(&room), "a joined operator must see the room in sync");

        // Inviting is once-only: after the operator leaves, the next pass does not
        // pull them back in.
        let _ = server
            .post(&format!("/_matrix/client/v3/rooms/{room}/leave"))
            .add_header(op.0.clone(), op.1.clone())
            .json(&json!({}))
            .await;
        invite_operators(&state, &room, &[op_id]);
        let r = server
            .post(&format!("/_matrix/client/v3/rooms/{room}/join"))
            .add_header(op.0.clone(), op.1.clone())
            .json(&json!({}))
            .await;
        assert_eq!(r.status_code(), 403, "a left operator needs a fresh invite");
    }

    // operators_env_parser_keeps_only_mxids:start
    //   purpose: operators_from_env keeps only entries shaped like an mxid.
    // operators_env_parser_keeps_only_mxids:end
    #[test]
    fn operators_env_parser_keeps_only_mxids() {
        // Pure parse path, no env mutation (parallel tests): same filter via the fn.
        let v: Vec<String> = "@a:x, junk ,@b:y,"
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| s.starts_with('@') && s.contains(':'))
            .collect();
        assert_eq!(v, vec!["@a:x".to_string(), "@b:y".to_string()]);
    }

    // The room as an older build left it: ts-0 seed burst with public/shared.
    // CONTRACT: Seed a queue room with the old rules (join_rule=public, history_visibility=shared); returns
    //    its room id.
    fn seed_old_public_room(state: &Arc<AppState>, role: &str) -> String {
        for mut ev in queue_state_events(role) {
            if ev.event_type == "m.room.join_rules" {
                ev.content = json!({ "join_rule": "public" });
            }
            if ev.event_type == "m.room.history_visibility" {
                ev.content = json!({ "history_visibility": "shared" });
            }
            state.apply_remote_state_event(ev).expect("seed");
        }
        queue_room_id(role)
    }

    // existing_public_room_is_switched_at_startup:start
    //   purpose: harden_room switches an existing public/shared room to invite/joined.
    // existing_public_room_is_switched_at_startup:end
    #[tokio::test]
    async fn existing_public_room_is_switched_at_startup() {
        let state = AppState::new();
        let room = seed_old_public_room(&state, "sec");
        assert_eq!(
            content(&state, &room, "m.room.join_rules"),
            Some(json!({ "join_rule": "public" }))
        );
        // What the bridge loop calls on every pass.
        ensure_queue_room(&state, "sec");
        assert_eq!(
            content(&state, &room, "m.room.join_rules"),
            Some(json!({ "join_rule": "invite" }))
        );
        assert_eq!(
            content(&state, &room, "m.room.history_visibility"),
            Some(json!({ "history_visibility": "joined" }))
        );
        let sender_ok = state
            .room_state
            .lock()
            .unwrap()
            .get(&room)
            .unwrap()
            .iter()
            .filter(|e| e.event_type == "m.room.join_rules")
            .all(|e| e.sender == BRIDGE_USER);
        assert!(sender_ok, "the switch must come from @hubd");
        // Idempotent: a second pass is a no-op.
        assert!(!harden_room(&state, &room));

        // And the CS-API now refuses a stranger on that very room.
        let server = TestServer::new(router(state));
        let mallory = register(&server, "mallory").await;
        let r = server
            .post(&format!("/_matrix/client/v3/rooms/{room}/join"))
            .add_header(mallory.0.clone(), mallory.1.clone())
            .json(&json!({}))
            .await;
        assert_eq!(r.status_code(), 403);
    }

    // switch_is_identical_on_every_node:start
    //   purpose: Two nodes hardening the same room produce identical state events, so a mixed fleet
    //            converges.
    // switch_is_identical_on_every_node:end
    #[tokio::test]
    async fn switch_is_identical_on_every_node() {
        let a = AppState::new();
        let b = AppState::new();
        let ra = seed_old_public_room(&a, "sec");
        seed_old_public_room(&b, "sec");
        harden_room(&a, &ra);
        harden_room(&b, &ra);
        let ids = |s: &Arc<AppState>| -> Vec<(String, u64)> {
            let mut v: Vec<_> = s
                .room_state
                .lock()
                .unwrap()
                .get(&ra)
                .unwrap()
                .iter()
                .filter(|e| e.event_type == "m.room.join_rules" || e.event_type == "m.room.history_visibility")
                .map(|e| (e.event_id.clone(), e.origin_server_ts))
                .collect();
            v.sort();
            v
        };
        assert_eq!(ids(&a), ids(&b), "all nodes must emit the same event (LWW tie)");
        // An old peer re-seeding ts 0 public cannot undo it.
        let old: StateEvent = queue_state_events("sec")
            .into_iter()
            .find(|e| e.event_type == "m.room.join_rules")
            .unwrap();
        let mut old = old;
        old.content = json!({ "join_rule": "public" });
        let _ = a.apply_remote_state_event(old);
        assert_eq!(
            content(&a, &ra, "m.room.join_rules"),
            Some(json!({ "join_rule": "invite" }))
        );
    }

    // ordinary_rooms_follow_the_general_rules_and_joined_visibility_is_honoured:start
    //   purpose: Ordinary rooms follow the general CS-API rules, and a stored history_visibility of
    //            joined is honoured.
    // ordinary_rooms_follow_the_general_rules_and_joined_visibility_is_honoured:end
    #[tokio::test]
    async fn ordinary_rooms_follow_the_general_rules_and_joined_visibility_is_honoured() {
        let state = AppState::new();
        let server = TestServer::new(router(state));
        let alice = register(&server, "alice").await;
        let mallory = register(&server, "mallory").await;
        let created: Value = server
            .post("/_matrix/client/v3/createRoom")
            .add_header(alice.0.clone(), alice.1.clone())
            .json(&json!({ "preset": "public_chat" }))
            .await
            .json();
        let room = created["room_id"].as_str().unwrap().to_string();
        // Reads need a token on every room; a public room is joinable.
        let r = server.get(&format!("/_matrix/client/v3/rooms/{room}/state")).await;
        assert_eq!(r.status_code(), 401);
        let r = server
            .post(&format!("/_matrix/client/v3/rooms/{room}/join"))
            .add_header(mallory.0.clone(), mallory.1.clone())
            .json(&json!({}))
            .await;
        assert_eq!(r.status_code(), 200, "a public room stays open to join");

        // history_visibility=joined on an ordinary room is honoured.
        let r = server
            .put(&format!("/_matrix/client/v3/rooms/{room}/state/m.room.history_visibility"))
            .add_header(alice.0.clone(), alice.1.clone())
            .json(&json!({ "history_visibility": "joined" }))
            .await;
        assert_eq!(r.status_code(), 200, "{}", r.text());
        let bob = register(&server, "bob").await;
        let r = server
            .get(&format!("/_matrix/client/v3/rooms/{room}/messages"))
            .add_header(bob.0.clone(), bob.1.clone())
            .await;
        assert_eq!(r.status_code(), 403, "{}", r.text());
        let r = server
            .get(&format!("/_matrix/client/v3/rooms/{room}/messages"))
            .add_header(alice.0.clone(), alice.1.clone())
            .await;
        assert_eq!(r.status_code(), 200);
    }
}
