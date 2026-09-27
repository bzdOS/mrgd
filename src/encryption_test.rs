// START_AI_HEADER
// MODULE: matrix-hs/src/encryption_test.rs
// PURPOSE: Integration tests for createRoom's initial_state handling of
//          m.room.encryption (required for Element X / E2EE rooms).
//
//          Tests prove:
//            1. createRoom with initial_state:[{type:"m.room.encryption",
//               state_key:"", content:{algorithm:"m.megolm.v1.aes-sha2"}}] stores the
//               state event, readable via GET /rooms/{id}/state/m.room.encryption/.
//            2. The event appears in classic GET /sync room.state.events.
//            3. Sliding sync's default required_state (no explicit request) surfaces
//               m.room.encryption in the room's required_state array.
// DEPENDENCIES: axum-test, serde_json, matrix_hs::{router, AppState}
// END_AI_HEADER

#[cfg(test)]
mod tests {
    use crate::{router, AppState};
    use axum_test::TestServer;
    use serde_json::{json, Value};

    // register_and_bearer:start
    //   purpose: Register a user via two-step UIA and return an Authorization bearer header.
    //   input:  server — &TestServer; username — localpart
    //   output: (HeaderName, HeaderValue) for Authorization: Bearer <mxt_ token>
    //   sideEffects: inserts user into AppState via register
    // register_and_bearer:end
    async fn register_and_bearer(
        server: &TestServer,
        username: &str,
    ) -> (axum::http::HeaderName, axum::http::HeaderValue) {
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
            axum::http::HeaderName::from_static("authorization"),
            axum::http::HeaderValue::from_str(&format!("Bearer {token}")).expect("header value"),
        )
    }

    // test:create_room_with_encryption_initial_state:start
    //   purpose: createRoom with initial_state containing m.room.encryption must store
    //            the event so it is readable directly, appears in classic /sync
    //            state.events, and appears in sliding sync's default required_state.
    //   input:  createRoom{initial_state:[{type:"m.room.encryption", state_key:"",
    //           content:{algorithm:"m.megolm.v1.aes-sha2"}}]}
    //   output: GET .../state/m.room.encryption/ → 200 {"algorithm":"m.megolm.v1.aes-sha2"};
    //           classic /sync → room.state.events contains the m.room.encryption event;
    //           sliding sync (no explicit required_state) → room's required_state
    //           contains an m.room.encryption event.
    //   sideEffects: none beyond room creation
    // test:create_room_with_encryption_initial_state:end
    #[tokio::test]
    async fn create_room_with_encryption_initial_state() {
        let state = AppState::new();
        let server = TestServer::new(router(state));
        let (hn, hv) = register_and_bearer(&server, "enc_user").await;

        let create_resp = server
            .post("/_matrix/client/v3/createRoom")
            .add_header(hn.clone(), hv.clone())
            .json(&json!({
                "name": "Encrypted Room",
                "initial_state": [
                    {
                        "type": "m.room.encryption",
                        "state_key": "",
                        "content": { "algorithm": "m.megolm.v1.aes-sha2" }
                    }
                ]
            }))
            .await;
        create_resp.assert_status_ok();
        let create_body: Value = create_resp.json();
        let room_id = create_body["room_id"]
            .as_str()
            .unwrap_or_else(|| panic!("createRoom: no room_id; got {create_body}"))
            .to_string();

        // 1. Direct state read. No trailing slash: axum/matchit does not match an
        // empty final path segment to a {state_key} capture, so the empty state_key
        // ("") of m.room.encryption is served via the 2-segment route
        // (.../state/{event_type}, no third segment) — see
        // routes::room_state::get_room_state_event_empty_key.
        let state_url = format!(
            "/_matrix/client/v3/rooms/{}/state/m.room.encryption",
            urlencoding_path(&room_id)
        );
        let state_resp = server.get(&state_url).await;
        state_resp.assert_status_ok();
        let content: Value = state_resp.json();
        assert_eq!(
            content["algorithm"].as_str(),
            Some("m.megolm.v1.aes-sha2"),
            "GET state/m.room.encryption must return the stored algorithm; got {content}"
        );

        // 2. Classic /sync must surface it in room.state.events.
        let sync_resp = server
            .get("/_matrix/client/v3/sync")
            .add_header(hn.clone(), hv.clone())
            .await;
        sync_resp.assert_status_ok();
        let sync_body: Value = sync_resp.json();
        let room_sync = &sync_body["rooms"]["join"][&room_id];
        let state_events = room_sync["state"]["events"].as_array().unwrap_or_else(|| {
            panic!("sync: no state.events array for {room_id}; got {sync_body}")
        });
        let found = state_events.iter().any(|ev| {
            ev.get("type").and_then(Value::as_str) == Some("m.room.encryption")
                && ev
                    .get("content")
                    .and_then(|c| c.get("algorithm"))
                    .and_then(Value::as_str)
                    == Some("m.megolm.v1.aes-sha2")
        });
        assert!(
            found,
            "m.room.encryption must appear in classic /sync state.events; got {sync_body}"
        );

        // 3. Sliding sync's default required_state (no explicit request) must surface it.
        let ss_resp = server
            .post("/_matrix/client/unstable/org.matrix.simplified_msc3575/sync")
            .add_header(hn.clone(), hv.clone())
            .json(&json!({ "lists": { "all": { "ranges": [[0, 9]], "timeline_limit": 1 } } }))
            .await;
        ss_resp.assert_status_ok();
        let ss_body: Value = ss_resp.json();
        let ss_room = &ss_body["rooms"][&room_id];
        let required_state = ss_room["required_state"].as_array().unwrap_or_else(|| {
            panic!("sliding sync: no required_state for {room_id}; got {ss_body}")
        });
        let ss_found = required_state
            .iter()
            .any(|ev| ev.get("type").and_then(Value::as_str) == Some("m.room.encryption"));
        assert!(
            ss_found,
            "m.room.encryption must appear in sliding sync's default required_state; got {ss_body}"
        );
    }

    // urlencoding_path:start
    //   purpose: Percent-encode a room_id ("!foo:bar") for use as a single URL path
    //            segment ('!' is left as-is — allowed unreserved-ish in practice for
    //            these test server routes; ':' must be encoded so axum treats the
    //            whole room_id as one path segment).
    //   input:  s — raw room_id
    //   output: percent-escaped String safe for one path segment
    //   sideEffects: none
    // urlencoding_path:end
    fn urlencoding_path(s: &str) -> String {
        s.chars()
            .map(|c| {
                if c == ':' {
                    "%3A".to_string()
                } else {
                    c.to_string()
                }
            })
            .collect()
    }
}
