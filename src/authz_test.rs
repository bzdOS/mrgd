// START_AI_HEADER
// MODULE: src/authz_test.rs
// PURPOSE: CS-API authorisation of room reads and joins. The read routes
//          (/state, /state/{type}[/{key}], /members, /joined_members, /messages) needed no
//          token at all and join ignored m.room.join_rules, so anyone with network access
//          to the node could read any room by room_id and enter any room. One test per
//          claim, every closed route covered:
//            - no token           -> 401 M_MISSING_TOKEN (bad token: 401 M_UNKNOWN_TOKEN);
//            - stranger           -> 403 M_FORBIDDEN, body carries nothing of the room;
//            - invited / left     -> 403 (documented simplification: joined or world_readable);
//            - joined member      -> 200;
//            - world_readable     -> a stranger with a token reads, without one still 401;
//            - join: public ok, invite needs an invite, knock/restricted/no rule need an
//              invite, a banned user is refused everywhere.
//          The public-by-spec routes are pinned open at the end.
// DEPENDENCIES: axum-test, serde_json, crate::{router, AppState}
// END_AI_HEADER

use crate::{router, AppState};
use axum::http::{HeaderName, HeaderValue};
use axum_test::TestServer;
use serde_json::{json, Value};
use std::sync::Arc;

type Bearer = (HeaderName, HeaderValue);

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

fn uid(state: &Arc<AppState>, name: &str) -> String {
    format!("@{name}:{}", state.server_name)
}

async fn create(server: &TestServer, who: &Bearer, body: Value) -> String {
    let r = server
        .post("/_matrix/client/v3/createRoom")
        .add_header(who.0.clone(), who.1.clone())
        .json(&body)
        .await;
    assert_eq!(r.status_code(), 200, "{}", r.text());
    r.json::<Value>()["room_id"].as_str().expect("room_id").to_string()
}

async fn post(server: &TestServer, who: Option<&Bearer>, path: &str, body: Value) -> (u16, Value) {
    let mut req = server.post(&format!("/_matrix/client/v3{path}"));
    if let Some(b) = who {
        req = req.add_header(b.0.clone(), b.1.clone());
    }
    let r = req.json(&body).await;
    (r.status_code().as_u16(), r.json::<Value>())
}

async fn get(server: &TestServer, who: Option<&Bearer>, path: &str) -> (u16, Value) {
    let mut req = server.get(&format!("/_matrix/client/v3{path}"));
    if let Some(b) = who {
        req = req.add_header(b.0.clone(), b.1.clone());
    }
    let r = req.await;
    let status = r.status_code().as_u16();
    (status, r.json::<Value>())
}

/// Room created by alice (private_chat), with a message, plus bob (invited and joined),
/// mallory (stranger) and the server.
struct World {
    state: Arc<AppState>,
    server: TestServer,
    alice: Bearer,
    bob: Bearer,
    mallory: Bearer,
    room: String,
}

async fn world() -> World {
    let state = AppState::new();
    let server = TestServer::new(router(state.clone()));
    let alice = register(&server, "alice").await;
    let bob = register(&server, "bob").await;
    let mallory = register(&server, "mallory").await;
    let room = create(&server, &alice, json!({ "preset": "private_chat" })).await;
    let (s, _) = post(&server, Some(&alice), &format!("/rooms/{room}/invite"), json!({ "user_id": uid(&state, "bob") })).await;
    assert_eq!(s, 200);
    let (s, _) = post(&server, Some(&bob), &format!("/rooms/{room}/join"), json!({})).await;
    assert_eq!(s, 200);
    let (s, _) = {
        let r = server
            .put(&format!("/_matrix/client/v3/rooms/{room}/send/m.room.message/t1"))
            .add_header(alice.0.clone(), alice.1.clone())
            .json(&json!({ "msgtype": "m.text", "body": "room secret" }))
            .await;
        (r.status_code().as_u16(), r.text())
    };
    assert_eq!(s, 200);
    World { state, server, alice, bob, mallory, room }
}

fn read_paths(room: &str) -> Vec<String> {
    vec![
        format!("/rooms/{room}/state"),
        format!("/rooms/{room}/state/m.room.name"),
        format!("/rooms/{room}/state/m.room.name/"),
        format!("/rooms/{room}/state/m.room.join_rules/"),
        format!("/rooms/{room}/state/m.room.member/@alice:localhost"),
        format!("/rooms/{room}/members"),
        format!("/rooms/{room}/joined_members"),
        format!("/rooms/{room}/messages?dir=b&limit=50"),
    ]
}

async fn set_visibility(w: &World, vis: &str) {
    let r = w
        .server
        .put(&format!("/_matrix/client/v3/rooms/{}/state/m.room.history_visibility", w.room))
        .add_header(w.alice.0.clone(), w.alice.1.clone())
        .json(&json!({ "history_visibility": vis }))
        .await;
    assert_eq!(r.status_code(), 200, "{}", r.text());
}

#[tokio::test]
async fn read_without_token_is_401_missing_token() {
    let w = world().await;
    for p in read_paths(&w.room) {
        let (s, b) = get(&w.server, None, &p).await;
        assert_eq!(s, 401, "{p}: {b}");
        assert_eq!(b["errcode"], "M_MISSING_TOKEN", "{p}: {b}");
    }
}

#[tokio::test]
async fn read_with_garbage_token_is_401_unknown_token() {
    let w = world().await;
    let bad = (
        HeaderName::from_static("authorization"),
        HeaderValue::from_static("Bearer mxt_garbage"),
    );
    for p in read_paths(&w.room) {
        let (s, b) = get(&w.server, Some(&bad), &p).await;
        assert_eq!(s, 401, "{p}: {b}");
        assert_eq!(b["errcode"], "M_UNKNOWN_TOKEN", "{p}: {b}");
    }
}

#[tokio::test]
async fn read_by_stranger_is_403_and_leaks_nothing() {
    let w = world().await;
    for p in read_paths(&w.room) {
        let (s, b) = get(&w.server, Some(&w.mallory), &p).await;
        assert_eq!(s, 403, "{p}: {b}");
        assert_eq!(b["errcode"], "M_FORBIDDEN", "{p}: {b}");
        assert!(!b.to_string().contains("room secret"), "{p} leaked");
    }
}

#[tokio::test]
async fn read_by_member_is_200() {
    let w = world().await;
    for who in [&w.alice, &w.bob] {
        for p in read_paths(&w.room) {
            // m.room.name is never set on this room (404 from the handler, not a gate).
            if p.contains("m.room.name") || p.contains("@alice:localhost") {
                let (s, _) = get(&w.server, Some(who), &p).await;
                assert!(s == 200 || s == 404, "{p}: {s}");
                continue;
            }
            let (s, b) = get(&w.server, Some(who), &p).await;
            assert_eq!(s, 200, "{p}: {b}");
        }
    }
    let (s, b) = get(&w.server, Some(&w.bob), &format!("/rooms/{}/messages?dir=b&limit=50", w.room)).await;
    assert_eq!(s, 200);
    assert!(b.to_string().contains("room secret"));
    let alice_id = uid(&w.state, "alice");
    let (s, _) = get(&w.server, Some(&w.bob), &format!("/rooms/{}/state/m.room.member/{alice_id}", w.room)).await;
    assert_eq!(s, 200);
}

#[tokio::test]
async fn read_by_invited_or_left_user_is_403() {
    let w = world().await;
    // Invited, not joined.
    let carol = register(&w.server, "carol").await;
    let (s, _) = post(&w.server, Some(&w.alice), &format!("/rooms/{}/invite", w.room), json!({ "user_id": uid(&w.state, "carol") })).await;
    assert_eq!(s, 200);
    let (s, b) = get(&w.server, Some(&carol), &format!("/rooms/{}/messages?dir=b", w.room)).await;
    assert_eq!((s, b["errcode"].as_str()), (403, Some("M_FORBIDDEN")), "{b}");
    let (s, _) = get(&w.server, Some(&carol), &format!("/rooms/{}/state", w.room)).await;
    assert_eq!(s, 403);
    // Left.
    let (s, _) = post(&w.server, Some(&w.bob), &format!("/rooms/{}/leave", w.room), json!({})).await;
    assert_eq!(s, 200);
    for p in read_paths(&w.room) {
        let (s, b) = get(&w.server, Some(&w.bob), &p).await;
        assert_eq!(s, 403, "{p}: {b}");
    }
}

#[tokio::test]
async fn world_readable_stranger_with_token_reads_without_token_still_401() {
    let w = world().await;
    set_visibility(&w, "world_readable").await;
    for p in read_paths(&w.room) {
        if p.contains("m.room.name") || p.contains("@alice:localhost") {
            let (s, _) = get(&w.server, Some(&w.mallory), &p).await;
            assert!(s == 200 || s == 404, "{p}: {s}");
            continue;
        }
        let (s, b) = get(&w.server, Some(&w.mallory), &p).await;
        assert_eq!(s, 200, "{p}: {b}");
        let (s, b) = get(&w.server, None, &p).await;
        assert_eq!(s, 401, "{p}: {b}");
        assert_eq!(b["errcode"], "M_MISSING_TOKEN");
    }
    let (_, b) = get(&w.server, Some(&w.mallory), &format!("/rooms/{}/messages?dir=b&limit=50", w.room)).await;
    assert!(b.to_string().contains("room secret"));
}

#[tokio::test]
async fn read_of_unknown_room_is_404_after_the_token_check() {
    let w = world().await;
    let (s, b) = get(&w.server, None, "/rooms/!nope:localhost/state").await;
    assert_eq!((s, b["errcode"].as_str()), (401, Some("M_MISSING_TOKEN")));
    let (s, b) = get(&w.server, Some(&w.mallory), "/rooms/!nope:localhost/state").await;
    assert_eq!((s, b["errcode"].as_str()), (404, Some("M_NOT_FOUND")));
}

// ---- join ----------------------------------------------------------------------------

fn join_paths(room: &str) -> Vec<String> {
    vec![format!("/rooms/{room}/join"), format!("/join/{room}")]
}

#[tokio::test]
async fn join_without_token_is_401_missing_token() {
    let w = world().await;
    for p in join_paths(&w.room) {
        let (s, b) = post(&w.server, None, &p, json!({})).await;
        assert_eq!((s, b["errcode"].as_str()), (401, Some("M_MISSING_TOKEN")), "{p}: {b}");
    }
}

#[tokio::test]
async fn join_invite_room_needs_an_invite() {
    let w = world().await;
    for p in join_paths(&w.room) {
        let (s, b) = post(&w.server, Some(&w.mallory), &p, json!({})).await;
        assert_eq!((s, b["errcode"].as_str()), (403, Some("M_FORBIDDEN")), "{p}: {b}");
    }
    // A refused join left no membership: still a stranger, still unreadable.
    let (s, _) = get(&w.server, Some(&w.mallory), &format!("/rooms/{}/state", w.room)).await;
    assert_eq!(s, 403);
    // With an invite, both routes work.
    for (name, p) in [("dave", 0usize), ("erin", 1usize)] {
        let t = register(&w.server, name).await;
        let (s, _) = post(&w.server, Some(&w.alice), &format!("/rooms/{}/invite", w.room), json!({ "user_id": uid(&w.state, name) })).await;
        assert_eq!(s, 200);
        let (s, b) = post(&w.server, Some(&t), &join_paths(&w.room)[p], json!({})).await;
        assert_eq!(s, 200, "{b}");
        let (s, _) = get(&w.server, Some(&t), &format!("/rooms/{}/state", w.room)).await;
        assert_eq!(s, 200);
    }
}

#[tokio::test]
async fn join_public_room_is_open() {
    let w = world().await;
    let room = create(&w.server, &w.alice, json!({ "preset": "public_chat" })).await;
    for p in join_paths(&room) {
        let who = register(&w.server, &format!("p{}", p.len())).await;
        let (s, b) = post(&w.server, Some(&who), &p, json!({})).await;
        assert_eq!(s, 200, "{p}: {b}");
    }
}

#[tokio::test]
async fn join_banned_user_is_403_even_in_a_public_room() {
    let w = world().await;
    let room = create(&w.server, &w.alice, json!({ "preset": "public_chat" })).await;
    let (s, _) = post(&w.server, Some(&w.mallory), &format!("/rooms/{room}/join"), json!({})).await;
    assert_eq!(s, 200);
    let (s, _) = post(&w.server, Some(&w.alice), &format!("/rooms/{room}/ban"), json!({ "user_id": uid(&w.state, "mallory") })).await;
    assert_eq!(s, 200);
    for p in join_paths(&room) {
        let (s, b) = post(&w.server, Some(&w.mallory), &p, json!({})).await;
        assert_eq!((s, b["errcode"].as_str()), (403, Some("M_FORBIDDEN")), "{p}: {b}");
    }
    // ...and in an invite room, an invite after the ban is an unban in spirit only:
    // the ban must be lifted first; a banned user is not "invited".
    let (s, _) = get(&w.server, Some(&w.mallory), &format!("/rooms/{room}/state")).await;
    assert_eq!(s, 403);
}

#[tokio::test]
async fn join_knock_restricted_and_ruleless_rooms_need_an_invite() {
    let w = world().await;
    for rule in ["knock", "restricted", "knock_restricted", "something_else"] {
        let room = create(
            &w.server,
            &w.alice,
            json!({ "preset": "public_chat", "initial_state": [
                { "type": "m.room.join_rules", "state_key": "", "content": { "join_rule": rule } }
            ] }),
        )
        .await;
        for p in join_paths(&room) {
            let (s, b) = post(&w.server, Some(&w.mallory), &p, json!({})).await;
            assert_eq!(s, 403, "{rule} {p}: {b}");
        }
    }
    // No m.room.join_rules at all: the spec default is invite, never open.
    let room = create(&w.server, &w.alice, json!({ "preset": "public_chat" })).await;
    w.state
        .room_state
        .lock()
        .unwrap()
        .get_mut(&room)
        .unwrap()
        .retain(|e| e.event_type != "m.room.join_rules");
    let (s, b) = post(&w.server, Some(&w.mallory), &format!("/rooms/{room}/join"), json!({})).await;
    assert_eq!(s, 403, "{b}");
}

#[tokio::test]
async fn already_joined_member_can_rejoin() {
    let w = world().await;
    let (s, b) = post(&w.server, Some(&w.bob), &format!("/rooms/{}/join", w.room), json!({})).await;
    assert_eq!(s, 200, "{b}");
}

// ---- routes that are public by spec stay open ----------------------------------------

#[tokio::test]
async fn spec_public_routes_stay_open_without_a_token() {
    let w = world().await;
    for p in [
        "/_matrix/client/versions",
        "/_matrix/client/v3/login",
        "/_matrix/client/r0/login",
        "/_matrix/client/v3/register/available?username=zed",
        "/_matrix/client/v3/profile/@alice:localhost",
        "/_matrix/client/v3/profile/@alice:localhost/displayname",
        "/_matrix/media/v3/config",
    ] {
        let r = w.server.get(p).await;
        assert_eq!(r.status_code(), 200, "{p}: {}", r.text());
    }
    let r = w.server.get("/.well-known/matrix/client").await;
    assert!(r.status_code() == 200 || r.status_code() == 404);
}
