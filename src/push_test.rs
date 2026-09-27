// START_AI_HEADER
// MODULE: matrix-hs/src/push_test.rs
// PURPOSE: Integration tests for push notifications (routes/pushers.rs, routes/push.rs).
//          Scenarios:
//            1. pusher_set_then_get — POST /pushers/set followed by GET /pushers
//               returns the registered pusher.
//            2. pusher_delete_via_kind_null — POST /pushers/set with kind:null
//               removes a previously-registered pusher.
//            3. dispatch_posts_to_mock_gateway — the real end-to-end proof: a LOCAL
//               mock Push Gateway (a tiny axum server bound to 127.0.0.1:0) records
//               every notify POST it receives into a shared Arc<Mutex<Vec<Value>>>.
//               bob registers a pusher whose data.url points at that mock. alice and
//               bob join a room; alice sends a message; after a short grace period
//               (dispatch_push spawns the POST in a background task — see that
//               module's non-blocking design) the mock gateway must have recorded a
//               notification whose event_id/room_id match the sent message, and the
//               sender (alice) must NOT have received a copy (nobody is notified of
//               their own message — see routes/push.rs's minimal push-rule scope).
// DEPENDENCIES: axum-test, axum (mock gateway), tokio, serde_json, matrix_hs::{router, AppState}
// END_AI_HEADER

#[cfg(test)]
mod tests {
    use crate::{router, AppState};
    use axum::http::{HeaderName, HeaderValue};
    use axum_test::TestServer;
    use serde_json::{json, Value};
    use std::sync::{Arc, Mutex};

    // test_server:start
    //   purpose: Build a fresh TestServer with an empty in-memory AppState (mirrors
    //            the other *_test.rs helpers in this crate).
    //   input:  none
    //   output: TestServer
    //   sideEffects: none
    // test_server:end
    fn test_server() -> TestServer {
        let state = AppState::new();
        let app = router(state);
        TestServer::new(app)
    }

    // register_and_bearer:start
    //   purpose: Register a user via two-step UIA and return an Authorization bearer
    //            header carrying a signed mxt_ token (duplicated per repo convention —
    //            see cluster_test.rs::register_and_bearer).
    //   input:  server, username
    //   output: (HeaderName, HeaderValue)
    //   sideEffects: inserts the user into AppState via /register
    // register_and_bearer:end
    async fn register_and_bearer(server: &TestServer, username: &str) -> (HeaderName, HeaderValue) {
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
        (
            HeaderName::from_static("authorization"),
            HeaderValue::from_str(&format!("Bearer {token}")).expect("header value"),
        )
    }

    // pusher_set_then_get:start
    //   purpose: POST /pushers/set registers a pusher; GET /pushers must then return
    //            it with all fields intact.
    // pusher_set_then_get:end
    #[tokio::test]
    async fn pusher_set_then_get() {
        let server = test_server();
        let (name, val) = register_and_bearer(&server, "alice").await;

        server.post("/_matrix/client/v3/pushers/set")
            .add_header(name.clone(), val.clone())
            .json(&json!({
                "app_id": "com.example.app",
                "pushkey": "pk-abc123",
                "kind": "http",
                "app_display_name": "Example App",
                "device_display_name": "Alice's Phone",
                "lang": "en",
                "data": { "url": "https://gateway.example.com/_matrix/push/v1/notify", "format": "event_id_only" }
            }))
            .await
            .assert_status_ok();

        let resp = server
            .get("/_matrix/client/v3/pushers")
            .add_header(name, val)
            .await;
        resp.assert_status_ok();
        let body: Value = resp.json();
        let pushers = body["pushers"].as_array().expect("pushers array");
        assert_eq!(
            pushers.len(),
            1,
            "expected exactly one registered pusher; got {body}"
        );
        assert_eq!(pushers[0]["app_id"], "com.example.app");
        assert_eq!(pushers[0]["pushkey"], "pk-abc123");
        assert_eq!(pushers[0]["kind"], "http");
        assert_eq!(
            pushers[0]["data"]["url"],
            "https://gateway.example.com/_matrix/push/v1/notify"
        );
    }

    // pusher_delete_via_kind_null:start
    //   purpose: A pusher registered with kind:"http" is removed by a subsequent
    //            POST /pushers/set for the same (app_id, pushkey) with kind:null.
    // pusher_delete_via_kind_null:end
    #[tokio::test]
    async fn pusher_delete_via_kind_null() {
        let server = test_server();
        let (name, val) = register_and_bearer(&server, "bob").await;

        server
            .post("/_matrix/client/v3/pushers/set")
            .add_header(name.clone(), val.clone())
            .json(&json!({
                "app_id": "com.example.app",
                "pushkey": "pk-xyz",
                "kind": "http",
                "app_display_name": "Example App",
                "device_display_name": "Bob's Phone",
                "lang": "en",
                "data": { "url": "https://gateway.example.com/_matrix/push/v1/notify" }
            }))
            .await
            .assert_status_ok();

        // Sanity: it is present before deletion.
        let body: Value = server
            .get("/_matrix/client/v3/pushers")
            .add_header(name.clone(), val.clone())
            .await
            .json();
        assert_eq!(body["pushers"].as_array().expect("array").len(), 1);

        // kind:null deletes.
        server
            .post("/_matrix/client/v3/pushers/set")
            .add_header(name.clone(), val.clone())
            .json(&json!({
                "app_id": "com.example.app",
                "pushkey": "pk-xyz",
                "kind": Value::Null,
            }))
            .await
            .assert_status_ok();

        let body: Value = server
            .get("/_matrix/client/v3/pushers")
            .add_header(name, val)
            .await
            .json();
        assert_eq!(
            body["pushers"].as_array().expect("array").len(),
            0,
            "pusher must be gone after kind:null delete; got {body}"
        );
    }

    // start_mock_gateway:start
    //   purpose: Bind a real tiny axum server to 127.0.0.1:0 (ephemeral port) that
    //            accepts POST / (any path — the test only registers one pusher
    //            pointing at the returned base URL) and records the JSON body of
    //            every request into the returned Arc<Mutex<Vec<Value>>>. Always
    //            replies 200 {"rejected":[]} per the Push Gateway API contract.
    //   input:  none
    //   output: (base_url — "http://127.0.0.1:<port>/notify", Arc<Mutex<Vec<Value>>>
    //           of recorded notification bodies)
    //   sideEffects: binds a TCP listener; spawns a background tokio task serving it
    //                for the lifetime of the test process (test-only, never used in
    //                production code)
    // start_mock_gateway:end
    async fn start_mock_gateway() -> (String, Arc<Mutex<Vec<Value>>>) {
        use axum::{routing::post, Router};

        let received: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
        let received_for_handler = received.clone();

        async fn notify_handler(
            axum::extract::State(store): axum::extract::State<Arc<Mutex<Vec<Value>>>>,
            axum::Json(body): axum::Json<Value>,
        ) -> axum::Json<Value> {
            if let Ok(mut guard) = store.lock() {
                guard.push(body);
            }
            axum::Json(json!({ "rejected": [] }))
        }

        let app = Router::new()
            .route("/notify", post(notify_handler))
            .with_state(received_for_handler);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("mock gateway: bind 127.0.0.1:0");
        let addr = listener.local_addr().expect("mock gateway: local_addr");

        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        (format!("http://127.0.0.1:{}/notify", addr.port()), received)
    }

    // dispatch_posts_to_mock_gateway:start
    //   purpose: End-to-end proof of the push dispatch path: alice sends a message
    //            in a room bob has joined; bob has a pusher registered pointing at a
    //            local mock gateway; the mock gateway must receive a notify POST
    //            whose notification.event_id/room_id match the sent message, and
    //            alice (the sender) must not be notified of her own message.
    // dispatch_posts_to_mock_gateway:end
    #[tokio::test]
    async fn dispatch_posts_to_mock_gateway() {
        let server = test_server();
        let (alice_name, alice_val) = register_and_bearer(&server, "alice").await;
        let (bob_name, bob_val) = register_and_bearer(&server, "bob").await;

        let (gateway_url, received) = start_mock_gateway().await;

        // bob registers a pusher pointing at the mock gateway.
        server
            .post("/_matrix/client/v3/pushers/set")
            .add_header(bob_name.clone(), bob_val.clone())
            .json(&json!({
                "app_id": "com.example.app",
                "pushkey": "bob-pushkey",
                "kind": "http",
                "app_display_name": "Example App",
                "device_display_name": "Bob's Phone",
                "lang": "en",
                "data": { "url": gateway_url, "format": "event_id_only" }
            }))
            .await
            .assert_status_ok();

        // alice creates a room; bob joins it.
        let resp = server
            .post("/_matrix/client/v3/createRoom")
            .add_header(alice_name.clone(), alice_val.clone())
            .json(&json!({ "name": "Push Test Room", "room_alias_name": "push-test-room" }))
            .await;
        resp.assert_status_ok();
        let room_id = resp.json::<Value>()["room_id"]
            .as_str()
            .expect("room_id")
            .to_string();

        server
            .post(&format!("/_matrix/client/v3/rooms/{room_id}/join"))
            .add_header(bob_name.clone(), bob_val.clone())
            .json(&json!({}))
            .await
            .assert_status_ok();

        // alice sends a message.
        let send_path =
            format!("/_matrix/client/v3/rooms/{room_id}/send/m.room.message/txn-push-1");
        let resp = server
            .put(&send_path)
            .add_header(alice_name.clone(), alice_val.clone())
            .json(&json!({ "msgtype": "m.text", "body": "hello bob" }))
            .await;
        resp.assert_status_ok();
        let event_id = resp.json::<Value>()["event_id"]
            .as_str()
            .expect("event_id")
            .to_string();

        // Grace period for the spawned background POST (dispatch_push is
        // non-blocking — see routes/push.rs).
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        let notifications = received.lock().expect("received lock").clone();
        assert!(
            !notifications.is_empty(),
            "mock gateway received no notifications within the grace period"
        );

        let matched = notifications.iter().find(|n| {
            n["notification"]["event_id"] == event_id && n["notification"]["room_id"] == room_id
        });
        assert!(
            matched.is_some(),
            "no notification matched event_id={event_id} room_id={room_id}; got {notifications:?}"
        );
        let matched = matched.expect("checked above");
        assert_eq!(matched["notification"]["sender"], "@alice:localhost");
        assert_eq!(matched["notification"]["type"], "m.room.message");
        let devices = matched["notification"]["devices"]
            .as_array()
            .expect("devices array");
        assert_eq!(devices.len(), 1);
        assert_eq!(devices[0]["app_id"], "com.example.app");
        assert_eq!(devices[0]["pushkey"], "bob-pushkey");

        // Exactly one notification for this event (only bob has a pusher; alice —
        // the sender — must never be notified of her own message).
        let count_for_event = notifications
            .iter()
            .filter(|n| n["notification"]["event_id"] == event_id)
            .count();
        assert_eq!(
            count_for_event, 1,
            "expected exactly one notification (bob only); got {notifications:?}"
        );
    }
}
