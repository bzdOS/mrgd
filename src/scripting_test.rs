// START_AI_HEADER
// MODULE: matrix-hs/src/scripting_test.rs
// PURPOSE: Tests for the DC++-style Lua scripting hook `on_room_visible`.
//          Proves:
//            (1) Scripting::default() is a no-op (no script => spec default:
//                a user never sees rooms they are not joined to).
//            (2) load_dir dispatches on_room_visible: the granted user becomes
//                visible, everyone else stays spec-default.
//            (3) Full /sync round-trip: with a script granting @aki visibility,
//                aki's /sync returns a room aki is NOT a member of; a control
//                user (carol) still does NOT see it. This guards the hook wiring
//                in routes/sync.rs::build_join_rooms.
// DEPENDENCIES: axum-test, serde_json, matrix_hs::{router, AppState, scripting}
// END_AI_HEADER

#[cfg(test)]
mod tests {
    use axum_test::TestServer;
    use serde_json::{json, Value};
    use std::fs;

    use crate::scripting::Scripting;
    use crate::{router, AppState};

    // ---- unit: the Scripting engine itself ----

    #[test]
    fn default_is_noop_spec_default() {
        let s = Scripting::default();
        assert!(
            !s.on_room_visible("@aki:localhost", "!room:localhost"),
            "no script loaded => on_room_visible must return spec default (false)"
        );
    }

    #[test]
    fn load_dir_dispatches_hook_for_granted_user_only() {
        let dir = "/tmp/mhs_scripting_unit_test";
        let _ = fs::remove_dir_all(dir);
        fs::create_dir_all(dir).unwrap();
        fs::write(
            format!("{dir}/vis.lua"),
            "function on_room_visible(u, r) return u == \"@aki:localhost\" end\n",
        )
        .unwrap();

        let s = Scripting::default();
        s.load_dir(dir);

        assert!(
            s.on_room_visible("@aki:localhost", "!anything:localhost"),
            "aki must be granted visibility by the script"
        );
        assert!(
            !s.on_room_visible("@other:localhost", "!anything:localhost"),
            "non-aki must keep spec default (not visible)"
        );

        let _ = fs::remove_dir_all(dir);
    }

    // ---- integration: the hook is honoured by /sync ----

    fn server_with_script(server_name: &str, lua_body: &str) -> TestServer {
        let state = AppState::with_server_name(server_name.to_string());
        let dir = format!("/tmp/mhs_scripting_itest_{server_name}");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        fs::write(format!("{dir}/hook.lua"), lua_body).unwrap();
        state.scripting.load_dir(&dir);
        TestServer::new(router(state))
    }

    async fn register(
        server: &TestServer,
        user: &str,
    ) -> (axum::http::HeaderName, axum::http::HeaderValue) {
        let ch: Value = server
            .post("/_matrix/client/v3/register")
            .json(&json!({ "username": user, "password": "pw" }))
            .await
            .json();
        let sess = ch["session"].as_str().expect("uia session").to_string();
        let reg: Value = server
            .post("/_matrix/client/v3/register")
            .json(&json!({
                "username": user,
                "password": "pw",
                "auth": { "type": "m.login.dummy", "session": sess }
            }))
            .await
            .json();
        let token = reg["access_token"].as_str().expect("access_token");
        (
            axum::http::HeaderName::from_static("authorization"),
            axum::http::HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
        )
    }

    // test:sync_visibility_hook_grants_unjoined_room:start
    //   purpose: alice creates a room. aki (NOT a member) must see it in /sync
    //            because the Lua on_room_visible grants @aki:localhost visibility.
    //            carol (control, not granted) must NOT see it — proves the hook
    //            only extends visibility, never narrows, and only for granted users.
    // test:sync_visibility_hook_grants_unjoined_room:end
    #[tokio::test]
    async fn sync_visibility_hook_grants_unjoined_room() {
        let server = server_with_script(
            "localhost",
            "function on_room_visible(u, r) return u == \"@aki:localhost\" end\n",
        );

        // alice creates a room (alice is joined to it).
        let (a_hn, a_hv) = register(&server, "alice").await;
        let create: Value = server
            .post("/_matrix/client/v3/createRoom")
            .add_header(a_hn, a_hv)
            .json(&json!({}))
            .await
            .json();
        let room_id = create["room_id"].as_str().expect("room_id").to_string();

        // aki is NOT a member of alice's room, but the script grants visibility.
        let (aki_hn, aki_hv) = register(&server, "aki").await;
        let sync: Value = server
            .get("/_matrix/client/v3/sync")
            .add_header(aki_hn, aki_hv)
            .await
            .json();
        let joined = sync["rooms"]["join"].as_object().expect("rooms.join map");
        assert!(
            joined.contains_key(&room_id),
            "aki must SEE alice's room via on_room_visible; got keys: {:?}",
            joined.keys().collect::<Vec<_>>()
        );

        // carol (no grant) must NOT see alice's room — spec default preserved.
        let (c_hn, c_hv) = register(&server, "carol").await;
        let sync_c: Value = server
            .get("/_matrix/client/v3/sync")
            .add_header(c_hn, c_hv)
            .await
            .json();
        let joined_c = sync_c["rooms"]["join"].as_object().expect("rooms.join map");
        assert!(
            !joined_c.contains_key(&room_id),
            "carol must NOT see alice's room (spec default); got keys: {:?}",
            joined_c.keys().collect::<Vec<_>>()
        );

        let _ = fs::remove_dir_all("/tmp/mhs_scripting_itest_localhost");
    }
}
