// START_AI_HEADER
// MODULE: matrix-hs/src/profile_displayname_test.rs
// PURPOSE: Integration tests for the displayname sub-resource
//          GET /_matrix/client/{v3,r0}/profile/{userId}/displayname.
//
//          Scenarios:
//            1. displayname_subresource_returns_the_name: a user id with a localpart
//               answers 200 {"displayname": "<localpart>"} — the body a client that
//               asks for one field expects, not the whole profile object.
//            2. displayname_subresource_is_empty_without_a_name: an id that carries no
//               localpart answers 200 {} — the field is ABSENT, not null and not an
//               empty string, so a client can tell "no name" from "named empty".
//            3. displayname_subresource_matches_the_full_profile_for_an_unknown_user:
//               an id nobody registered still answers exactly what the full profile
//               answers for it (same status, same name).  This is the compatibility
//               rule: a client that got 404 here and 200 from /profile/{userId}
//               would have to treat the user as present, so the sub-resource must
//               not be the stricter of the two.
//            4. v3_and_r0_agree_on_every_case: the two prefixes are mounted from one
//               table, and this pins that for all three cases above — a client on
//               the legacy prefix gets the same bytes, not a 404.
// DEPENDENCIES: axum-test, serde_json, matrix_hs::{router, AppState}
// END_AI_HEADER

#[cfg(test)]
mod tests {
    use crate::{router, AppState};
    use axum_test::TestServer;
    use serde_json::Value;

    // test_server:start
    //   purpose: Build a fresh TestServer with an empty in-memory AppState.
    //   input:  none
    //   output: TestServer
    //   sideEffects: none
    // test_server:end
    fn test_server() -> TestServer {
        TestServer::new(router(AppState::new()))
    }

    // prefixes:start
    //   purpose: The two CS-API prefixes the same route table is mounted under.
    //   input:  none
    //   output: [&str; 2] — the legacy and current client prefixes
    //   sideEffects: none
    // prefixes:end
    const PREFIXES: [&str; 2] = ["/_matrix/client/v3", "/_matrix/client/r0"];

    // profile_displayname_returns_the_name:start
    //   purpose: A user with a localpart gets that localpart as the displayname.
    //   input:  none (server-local)
    //   output: asserts 200 and {"displayname":"alice"}
    //   sideEffects: none
    // profile_displayname_returns_the_name:end
    #[tokio::test]
    async fn displayname_subresource_returns_the_name() {
        let server = test_server();

        let res = server
            .get("/_matrix/client/v3/profile/@alice:localhost/displayname")
            .await;

        assert_eq!(res.status_code(), 200);
        let body: Value = res.json();
        assert_eq!(body, serde_json::json!({ "displayname": "alice" }));
    }

    // profile_displayname_is_empty_without_a_name:start
    //   purpose: An id with no localpart answers an empty object, not a 404 and not
    //            a null/empty displayname.
    //   input:  none (server-local)
    //   output: asserts 200, an empty object, and no "displayname" key at all
    //   sideEffects: none
    // profile_displayname_is_empty_without_a_name:end
    #[tokio::test]
    async fn displayname_subresource_is_empty_without_a_name() {
        let server = test_server();

        let res = server
            .get("/_matrix/client/v3/profile/@:localhost/displayname")
            .await;

        assert_eq!(res.status_code(), 200);
        let body: Value = res.json();
        assert_eq!(body, serde_json::json!({}), "no name must be an empty object");
        assert!(
            body.get("displayname").is_none(),
            "the field must be absent, not null: {body}"
        );
    }

    // profile_displayname_matches_full_profile_for_unknown_user:start
    //   purpose: For an id that was never registered, the sub-resource and the full
    //            profile must be indistinguishable — same status, same name.
    //   input:  none (server-local)
    //   output: asserts the two responses agree on status and displayname
    //   sideEffects: none
    // profile_displayname_matches_full_profile_for_unknown_user:end
    #[tokio::test]
    async fn displayname_subresource_matches_the_full_profile_for_an_unknown_user() {
        let server = test_server();

        let sub = server
            .get("/_matrix/client/v3/profile/@ghost:localhost/displayname")
            .await;
        let full = server.get("/_matrix/client/v3/profile/@ghost:localhost").await;

        assert_eq!(sub.status_code(), full.status_code());
        assert_eq!(
            sub.json::<Value>()["displayname"],
            full.json::<Value>()["displayname"],
            "the sub-resource must not be stricter than the full profile"
        );
    }

    // v3_and_r0_agree_on_every_case:start
    //   purpose: Both client prefixes answer every case the same way.
    //   input:  none (server-local)
    //   output: asserts identical status and displayname across the two prefixes
    //           for a named user, a user without a localpart, and an unknown user
    //   sideEffects: none
    // v3_and_r0_agree_on_every_case:end
    #[tokio::test]
    async fn v3_and_r0_agree_on_every_case() {
        let server = test_server();

        for (user_id, expected) in [
            ("@alice:localhost", Some("alice")),
            ("@:localhost", None),
            ("@ghost:localhost", Some("ghost")),
        ] {
            let mut seen: Option<(u16, Value)> = None;
            for prefix in PREFIXES {
                let res = server
                    .get(format!("{prefix}/profile/{user_id}/displayname").as_str())
                    .await;
                let status = res.status_code().as_u16();
                let body: Value = res.json();

                match expected {
                    Some(name) => assert_eq!(body["displayname"], Value::from(name)),
                    None => assert_eq!(body, serde_json::json!({})),
                }

                let this = (status, body);
                if let Some(first) = &seen {
                    assert_eq!(
                        *first, this,
                        "{user_id}: {prefix} answered differently from the other prefix"
                    );
                } else {
                    seen = Some(this);
                }
            }
        }
    }
}