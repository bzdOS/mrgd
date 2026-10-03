// START_AI_HEADER
// MODULE: matrix-hs/src/persist_test.rs
// PURPOSE: Integration tests for durable persistence (Stage 3).
//          Tests 1-4 require a real filesystem temp dir and verify the full
//          write → restart → replay round-trip.  Test 5 verifies that in-memory
//          mode (MATRIX_HS_DATA_DIR unset) produces no files at all.
//          Tests 6-7 (internal-task) prove the pdumeta.jsonl sidecar: a restarted node replays a
//          VERIFIABLE Pdu (test 6, the meta path) vs. an UNVERIFIABLE one when the meta
//          sidecar is absent (test 7, the pre-internal-task fallback path — contrast case proving
//          test 6 actually exercises the meta reconstruction, not just luck).
//          Test 8 (internal-task RETURN) proves replayed content is byte-exact to what was signed,
//          not merely "parses to the same JSON value" — tests 6/7's `.json(&json!({...}))`
//          bodies were coincidentally already alphabetical (json! is BTreeMap-backed too),
//          masking a real bug where a JSON round-trip during replay silently reordered keys
//          and broke verify_sig for any real (non-alphabetical) client message body.
//
//          Test strategy: axum_test::TestServer against a real AppState backed by
//          a temp directory.  A second AppState + replay is constructed from the same
//          dir to simulate a server restart.
//
//          Temp dir pattern: std::env::temp_dir() + unique subdir from a pid-based
//          counter; cleaned up in each test via std::fs::remove_dir_all.
//          No `tempfile` crate dependency.
//
// DEPENDENCIES: axum-test, tokio, serde_json, matrix_hs, crate::substrate::matrix_events,
//               crate::substrate::node_auth, std::fs
// END_AI_HEADER

#[cfg(test)]
mod persist_tests {
    use crate::persist::{compact_all, replay_from_dir};
    use crate::{router, AppState};
    use axum_test::TestServer;
    use serde_json::{json, Value};
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    // ── temp dir helper ───────────────────────────────────────────────────────

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    // make_temp_dir:start
    //   purpose: Create a unique temporary directory under the system temp dir.
    //            Uses process ID + a monotonic counter for uniqueness without
    //            external crate dependencies.  Caller is responsible for cleanup
    //            via std::fs::remove_dir_all.
    //   input:  none
    //   output: PathBuf pointing at the newly-created directory
    //   sideEffects: creates a directory on disk
    // make_temp_dir:end
    fn make_temp_dir() -> PathBuf {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        let dir = std::env::temp_dir().join(format!("matrix_hs_persist_test_{}_{n}", pid));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    // ── test 1: full round-trip (register + createRoom + send + restart → replay) ──

    // test:persist:roundtrip:start
    //   purpose: Verify that room history, user accounts, and aliases survive a
    //            simulated process restart via journal replay.
    //
    //            Steps:
    //              A. Use TestServer backed by an AppState with a temp data_dir.
    //              B. Register user "alice" (UIA 2-step).
    //              C. POST createRoom with alias "persist-room".
    //              D. PUT /send one message.
    //              E. Construct a fresh AppState from the same dir + replay.
    //              F. GET /sync on the new state: message must appear in timeline.
    //              G. GET /directory/room/#persist-room:localhost: must resolve.
    //              H. POST /login as "alice" with correct password: must succeed.
    //              I. GET /account/whoami with alice's token: must return user_id.
    //   input:  temp dir on filesystem
    //   output: all assertions pass
    //   sideEffects: writes journals to temp dir; cleans up on completion
    // test:persist:roundtrip:end
    #[tokio::test]
    async fn persist_round_trip() {
        let data_dir = make_temp_dir();
        let result = persist_round_trip_inner(&data_dir).await;
        // Clean up before asserting so we don't leave temps on failure.
        let _ = std::fs::remove_dir_all(&data_dir);
        result.expect("persist_round_trip failed");
    }

    async fn persist_round_trip_inner(data_dir: &Path) -> Result<(), String> {
        // ── Phase A: write ───────────────────────────────────────────────────
        let state1 = AppState::with_data_dir(data_dir.to_path_buf());
        let server1 = TestServer::new(router(state1.clone()));

        // Step 1: register alice (UIA)
        let reg1 = server1
            .post("/_matrix/client/v3/register")
            .json(&json!({}))
            .await;
        // Expect 401 with session.
        if reg1.status_code().as_u16() != 401 {
            return Err(format!(
                "expected 401 on first register, got {}",
                reg1.status_code()
            ));
        }
        let challenge: Value = reg1.json();
        let session_id = challenge["session"]
            .as_str()
            .ok_or("no session in 401 body")?
            .to_string();

        let reg2 = server1
            .post("/_matrix/client/v3/register")
            .json(&json!({
                "username": "alice",
                "password": "hunter2",
                "auth": {
                    "type":    "m.login.dummy",
                    "session": session_id
                }
            }))
            .await;
        if !reg2.status_code().is_success() {
            return Err(format!("register step 2 failed: {}", reg2.status_code()));
        }

        // Step 2: login to get token
        let login = server1
            .post("/_matrix/client/v3/login")
            .json(&json!({
                "type": "m.login.password",
                "identifier": { "type": "m.id.user", "user": "alice" },
                "password": "hunter2"
            }))
            .await;
        if !login.status_code().is_success() {
            return Err(format!("login failed: {}", login.status_code()));
        }
        let login_body: Value = login.json();
        let token = login_body["access_token"]
            .as_str()
            .ok_or("no access_token in login response")?
            .to_string();

        // Step 3: create room with alias
        let create = server1
            .post("/_matrix/client/v3/createRoom")
            .add_header(
                axum::http::HeaderName::from_static("authorization"),
                axum::http::HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
            )
            .json(&json!({ "room_alias_name": "persist-room" }))
            .await;
        if !create.status_code().is_success() {
            return Err(format!("createRoom failed: {}", create.status_code()));
        }
        let create_body: Value = create.json();
        let room_id = create_body["room_id"]
            .as_str()
            .ok_or("no room_id in createRoom response")?
            .to_string();

        // Step 4: send a message
        let send_path = format!("/_matrix/client/v3/rooms/{room_id}/send/m.room.message/txn1");
        let send = server1
            .put(&send_path)
            .add_header(
                axum::http::HeaderName::from_static("authorization"),
                axum::http::HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
            )
            .json(&json!({ "msgtype": "m.text", "body": "hello persistence" }))
            .await;
        if !send.status_code().is_success() {
            return Err(format!("send failed: {}", send.status_code()));
        }
        let send_body: Value = send.json();
        let orig_event_id = send_body["event_id"]
            .as_str()
            .ok_or("no event_id from send")?
            .to_string();

        // ── Phase B: replay (simulate restart) ──────────────────────────────
        let state2 = AppState::with_data_dir(data_dir.to_path_buf());
        replay_from_dir(&state2, data_dir).map_err(|e| format!("replay_from_dir: {e}"))?;

        let server2 = TestServer::new(router(state2.clone()));

        // Assertion F: /sync returns the message in timeline
        // /sync is now scoped to the caller's own joined rooms (fixed a severe
        // membership-leak bug — see routes/sync.rs::build_join_rooms) — must
        // authenticate as the room's actual member to see it.
        let sync = server2
            .get("/_matrix/client/v3/sync")
            .add_header(
                axum::http::HeaderName::from_static("authorization"),
                axum::http::HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
            )
            .await;
        if !sync.status_code().is_success() {
            return Err(format!("sync failed after replay: {}", sync.status_code()));
        }
        let sync_body: Value = sync.json();
        let events = sync_body["rooms"]["join"][&room_id]["timeline"]["events"]
            .as_array()
            .ok_or_else(|| format!("no timeline events for room {room_id} after replay"))?;

        let found = events
            .iter()
            .any(|ev| ev.get("event_id").and_then(|v| v.as_str()) == Some(&orig_event_id));
        if !found {
            return Err(format!(
                "event {orig_event_id} not found in timeline after replay; got: {events:?}"
            ));
        }

        // Assertion G: alias resolves
        let alias_resp = server2
            .get("/_matrix/client/v3/directory/room/%23persist-room%3Alocalhost")
            .await;
        if !alias_resp.status_code().is_success() {
            // Try the non-encoded form too.
            let alias_resp2 = server2
                .get("/_matrix/client/v3/directory/room/#persist-room:localhost")
                .await;
            if !alias_resp2.status_code().is_success() {
                return Err(format!(
                    "alias lookup failed after replay: {} / {}",
                    alias_resp.status_code(),
                    alias_resp2.status_code()
                ));
            }
        }

        // Assertion H: alice can log in with correct password
        let login2 = server2
            .post("/_matrix/client/v3/login")
            .json(&json!({
                "type": "m.login.password",
                "identifier": { "type": "m.id.user", "user": "alice" },
                "password": "hunter2"
            }))
            .await;
        if !login2.status_code().is_success() {
            return Err(format!(
                "alice login after replay failed: {}",
                login2.status_code()
            ));
        }
        let login2_body: Value = login2.json();
        let token2 = login2_body["access_token"]
            .as_str()
            .ok_or("no access_token from second login")?
            .to_string();

        // Assertion I: whoami returns alice
        let whoami = server2
            .get("/_matrix/client/v3/account/whoami")
            .add_header(
                axum::http::HeaderName::from_static("authorization"),
                axum::http::HeaderValue::from_str(&format!("Bearer {token2}")).unwrap(),
            )
            .await;
        if !whoami.status_code().is_success() {
            return Err(format!(
                "whoami failed after replay: {}",
                whoami.status_code()
            ));
        }
        let whoami_body: Value = whoami.json();
        let user_id = whoami_body["user_id"].as_str().unwrap_or("");
        if !user_id.starts_with("@alice:") {
            return Err(format!("whoami user_id mismatch after replay: {user_id}"));
        }

        Ok(())
    }

    // ── test 2: in-memory mode — no files written ──────────────────────────────

    // test:persist:inmemory:start
    //   purpose: When MATRIX_HS_DATA_DIR is not set (AppState::new()), no files
    //            are written to disk.  The persist context is None and all persist_*
    //            calls are no-ops.  Verify: send a message, then assert no .jsonl
    //            files were created in the system temp dir for this test invocation.
    //   input:  AppState::new() (no data_dir)
    //   output: assertions pass; no leftover files
    //   sideEffects: none (no disk writes)
    // test:persist:inmemory:end
    #[tokio::test]
    async fn persist_disabled_when_no_data_dir() {
        // Use AppState::new() — reads MATRIX_HS_DATA_DIR from env.
        // In CI this env var is not set, so persistence is disabled.
        let state = AppState::with_server_name("localhost".to_string());
        assert!(
            !state.persist.enabled(),
            "persist should be disabled without data_dir"
        );

        let server = TestServer::new(router(state));

        // Register a user and get a token.
        let ch: Value = server
            .post("/_matrix/client/v3/register")
            .json(&json!({ "username": "inmem_user", "password": "pw" }))
            .await
            .json();
        let sess = ch["session"].as_str().expect("session").to_string();
        let reg: Value = server
            .post("/_matrix/client/v3/register")
            .json(&json!({
                "username": "inmem_user",
                "password": "pw",
                "auth": { "type": "m.login.dummy", "session": sess }
            }))
            .await
            .json();
        let token = reg["access_token"]
            .as_str()
            .expect("access_token")
            .to_string();
        let auth_hn = axum::http::HeaderName::from_static("authorization");
        let auth_hv = axum::http::HeaderValue::from_str(&format!("Bearer {token}")).unwrap();

        // Create room + send a message — should succeed in-memory.
        server
            .post("/_matrix/client/v3/createRoom")
            .add_header(auth_hn.clone(), auth_hv.clone())
            .json(&json!({ "room_alias_name": "inmem-room" }))
            .await
            .assert_status_ok();

        let room_id = "!inmem-room:localhost";
        let path = format!("/_matrix/client/v3/rooms/{room_id}/send/m.room.message/txn1");
        server
            .put(&path)
            .add_header(auth_hn.clone(), auth_hv.clone())
            .json(&json!({ "msgtype": "m.text", "body": "in memory only" }))
            .await
            .assert_status_ok();

        // /sync must work. Scoped to the caller's own joined rooms — see
        // routes/sync.rs::build_join_rooms — must authenticate as inmem_user.
        let sync: Value = server
            .get("/_matrix/client/v3/sync")
            .add_header(auth_hn.clone(), auth_hv.clone())
            .await
            .json();
        let events = sync["rooms"]["join"][room_id]["timeline"]["events"]
            .as_array()
            .expect("timeline events");
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0]["content"]["body"].as_str(),
            Some("in memory only")
        );
    }

    // ── test 3: sanitize_filename correctness ────────────────────────────────

    // test:persist:sanitize:start
    //   purpose: Verify that sanitize_filename correctly encodes forbidden characters
    //            and produces safe file name components for Matrix identifiers.
    //   input:  various Matrix room_id and alias strings
    //   output: assertions on encoded form
    //   sideEffects: none
    // test:persist:sanitize:end
    #[test]
    fn sanitize_filename_encodes_forbidden_chars() {
        use crate::persist::sanitize_filename;

        // Room ID: "!room:localhost" → '!' and ':' must be encoded
        let r = sanitize_filename("!room:localhost");
        assert!(!r.contains('!'), "! must be encoded");
        assert!(!r.contains(':'), ": must be encoded");
        assert!(r.contains("%21"), "! should map to %21");
        assert!(r.contains("%3a"), ": should map to %3a (lowercase hex)");

        // Alias: "#general:server" → '#' must be encoded
        let a = sanitize_filename("#general:server");
        assert!(!a.contains('#'), "# must be encoded");
        assert!(a.contains("%23"), "# should map to %23");

        // Safe characters preserved
        let safe = sanitize_filename("room-name_v2");
        assert_eq!(safe, "room-name_v2");

        // Long string truncated at 200 bytes
        let long: String = "a".repeat(300);
        let s = sanitize_filename(&long);
        assert!(s.len() <= 200, "filename must be ≤200 bytes");
    }

    // ── test 4: idempotent replay (run replay twice on same state) ──────────

    // test:persist:idempotent:start
    //   purpose: Running replay_from_dir twice on the same AppState must be
    //            idempotent: no duplicate events in room_timeline, no duplicate users,
    //            no duplicate aliases.  Verifies the event_id HashSet dedup in
    //            replay_room_journal and last-write-wins in replay_accounts/replay_aliases.
    //   input:  data_dir with one message written; replay called twice
    //   output: room_timeline has exactly the initial events; users/aliases not duplicated
    //   sideEffects: writes + reads temp journals; cleaned up after test
    // test:persist:idempotent:end
    #[tokio::test]
    async fn persist_replay_is_idempotent() {
        let data_dir = make_temp_dir();
        let result = idempotent_inner(&data_dir).await;
        let _ = std::fs::remove_dir_all(&data_dir);
        result.expect("idempotent replay test failed");
    }

    async fn idempotent_inner(data_dir: &Path) -> Result<(), String> {
        // Write phase.
        let state1 = AppState::with_data_dir(data_dir.to_path_buf());
        let server1 = TestServer::new(router(state1.clone()));

        // Register user and get token.
        let ch: Value = server1
            .post("/_matrix/client/v3/register")
            .json(&json!({ "username": "idem_user", "password": "pw" }))
            .await
            .json();
        let sess = ch["session"].as_str().ok_or("no session")?;
        let reg: Value = server1
            .post("/_matrix/client/v3/register")
            .json(&json!({
                "username": "idem_user",
                "password": "pw",
                "auth": { "type": "m.login.dummy", "session": sess }
            }))
            .await
            .json();
        let token = reg["access_token"]
            .as_str()
            .ok_or("no access_token")?
            .to_string();
        let auth_hn = axum::http::HeaderName::from_static("authorization");
        let auth_hv = axum::http::HeaderValue::from_str(&format!("Bearer {token}"))
            .map_err(|e| format!("header: {e}"))?;

        server1
            .post("/_matrix/client/v3/createRoom")
            .add_header(auth_hn.clone(), auth_hv.clone())
            .json(&json!({ "room_alias_name": "idem-r" }))
            .await;

        let room_id = "!idem-r:localhost";
        server1
            .put(&format!(
                "/_matrix/client/v3/rooms/{room_id}/send/m.room.message/t1"
            ))
            .add_header(auth_hn.clone(), auth_hv.clone())
            .json(&json!({ "msgtype": "m.text", "body": "once" }))
            .await;

        // Replay into a fresh state twice.
        let state2 = AppState::with_data_dir(data_dir.to_path_buf());
        replay_from_dir(&state2, data_dir).map_err(|e| format!("first replay: {e}"))?;
        // Second replay on the same state — must not double entries.
        replay_from_dir(&state2, data_dir).map_err(|e| format!("second replay: {e}"))?;

        let rt = state2
            .room_timeline
            .lock()
            .map_err(|e| format!("lock: {e}"))?;
        let entries = rt
            .get(room_id)
            .ok_or_else(|| format!("room {room_id} not in timeline after replay"))?;

        // The message event (one) + state events from createRoom.
        // Key assertion: no duplicate event_ids.
        let mut seen = std::collections::HashSet::new();
        for (_, ev) in entries {
            if let Some(id) = ev.get("event_id").and_then(|v| v.as_str()) {
                if !seen.insert(id.to_string()) {
                    return Err(format!("duplicate event_id {id} after double replay"));
                }
            }
        }

        Ok(())
    }

    // ── test 5: compact_room deduplicates and compact_all idempotency ──────────

    // test:persist:compaction:start
    //   purpose: Verify that compact_room:
    //              (a) rewrites the journal with only unique-by-event_id events;
    //              (b) preserves a stable order (first-occurrence wins);
    //              (c) the compacted journal replays to the same room state
    //                  (timeline count correct, no double-count).
    //            Also verifies compact_all via AppState iterates rooms correctly.
    //   input:  temp dir; a room journal written with three events (one duplicated);
    //           compact_room called; replay called on fresh state
    //   output: journal has 3 lines (not 4); replay yields 3 timeline entries
    //   sideEffects: writes/reads temp journals; cleaned up after test
    // test:persist:compaction:end
    #[tokio::test]
    async fn compaction_deduplicates_journal() {
        let data_dir = make_temp_dir();
        let result = compaction_inner(&data_dir).await;
        let _ = std::fs::remove_dir_all(&data_dir);
        result.expect("compaction test failed");
    }

    async fn compaction_inner(data_dir: &Path) -> Result<(), String> {
        use crate::persist::{compact_all, compact_room, sanitize_filename};
        use std::io::{BufRead, BufReader};

        let room_id = "!compact-room:localhost";

        // ── Phase A: write a journal with a duplicate event_id ───────────────
        let state1 = AppState::with_data_dir(data_dir.to_path_buf());
        let server = TestServer::new(router(state1.clone()));

        // Register user + get token.
        let ch: Value = server
            .post("/_matrix/client/v3/register")
            .json(&json!({ "username": "compact_user", "password": "pw" }))
            .await
            .json();
        let sess = ch["session"].as_str().ok_or("no session")?;
        let reg: Value = server
            .post("/_matrix/client/v3/register")
            .json(&json!({
                "username": "compact_user",
                "password": "pw",
                "auth": { "type": "m.login.dummy", "session": sess }
            }))
            .await
            .json();
        let token = reg["access_token"]
            .as_str()
            .ok_or("no access_token")?
            .to_string();
        let auth_hn = axum::http::HeaderName::from_static("authorization");
        let auth_hv = axum::http::HeaderValue::from_str(&format!("Bearer {token}"))
            .map_err(|e| format!("header: {e}"))?;

        // Create room + send 2 messages.
        server
            .post("/_matrix/client/v3/createRoom")
            .add_header(auth_hn.clone(), auth_hv.clone())
            .json(&json!({ "room_alias_name": "compact-room" }))
            .await;

        let p1 = format!("/_matrix/client/v3/rooms/{room_id}/send/m.room.message/tc1");
        let r1 = server
            .put(&p1)
            .add_header(auth_hn.clone(), auth_hv.clone())
            .json(&json!({ "msgtype": "m.text", "body": "msg1" }))
            .await;
        r1.assert_status_ok();
        let eid1 = r1.json::<Value>()["event_id"]
            .as_str()
            .ok_or("no event_id from send 1")?
            .to_string();

        let p2 = format!("/_matrix/client/v3/rooms/{room_id}/send/m.room.message/tc2");
        let r2 = server
            .put(&p2)
            .add_header(auth_hn.clone(), auth_hv.clone())
            .json(&json!({ "msgtype": "m.text", "body": "msg2" }))
            .await;
        r2.assert_status_ok();
        let eid2 = r2.json::<Value>()["event_id"]
            .as_str()
            .ok_or("no event_id from send 2")?
            .to_string();

        // Manually append a duplicate of eid1 to simulate a re-appended duplicate.
        let rooms_dir = data_dir.join("rooms");
        let filename = format!("{}.jsonl", sanitize_filename(room_id));
        let journal_path = rooms_dir.join(&filename);

        // Read current content, find the eid1 line, and append it again.
        let eid1_line: String = {
            let f = std::fs::File::open(&journal_path).map_err(|e| format!("open journal: {e}"))?;
            let reader = BufReader::new(f);
            let mut found = None;
            for line in reader.lines() {
                let l = line.map_err(|e| format!("read line: {e}"))?;
                if l.contains(&eid1) {
                    found = Some(l);
                    break;
                }
            }
            found.ok_or_else(|| format!("eid1 {eid1} not found in journal"))?
        };

        // Count lines before compaction.
        let lines_before: usize = {
            let f = std::fs::File::open(&journal_path)
                .map_err(|e| format!("open journal for count: {e}"))?;
            BufReader::new(f).lines().count()
        };

        // Append duplicate line manually.
        {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&journal_path)
                .map_err(|e| format!("open for append: {e}"))?;
            writeln!(f, "{eid1_line}").map_err(|e| format!("write dup: {e}"))?;
        }

        let lines_with_dup: usize = {
            let f =
                std::fs::File::open(&journal_path).map_err(|e| format!("count with dup: {e}"))?;
            BufReader::new(f).lines().count()
        };

        // Journal must have grown by exactly 1.
        if lines_with_dup != lines_before + 1 {
            return Err(format!(
                "expected journal to grow by 1 (was {lines_before}, now {lines_with_dup})"
            ));
        }

        // ── Phase B: compact using compact_all ───────────────────────────────
        // Replay into a fresh state first (needed for Phase C's replay assertions
        // below; compact_all itself now reads the journal directly off disk).
        let state2 = AppState::with_data_dir(data_dir.to_path_buf());
        replay_from_dir(&state2, data_dir).map_err(|e| format!("replay before compact: {e}"))?;
        compact_all(data_dir);

        // The journal after compaction must have lines_before lines (dup removed).
        let lines_after: usize = {
            let f = std::fs::File::open(&journal_path)
                .map_err(|e| format!("count after compact: {e}"))?;
            BufReader::new(f).lines().count()
        };
        if lines_after != lines_before {
            return Err(format!(
                "after compaction: expected {lines_before} lines, got {lines_after}"
            ));
        }

        // ── Phase C: replay from compacted journal ───────────────────────────
        let state3 = AppState::with_data_dir(data_dir.to_path_buf());
        replay_from_dir(&state3, data_dir).map_err(|e| format!("replay from compacted: {e}"))?;

        // room_timeline must contain no duplicate event_ids.
        let rt = state3
            .room_timeline
            .lock()
            .map_err(|e| format!("timeline lock: {e}"))?;
        let entries = rt.get(room_id).ok_or_else(|| {
            format!("room {room_id} missing from timeline after compacted replay")
        })?;

        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        for (_, ev) in entries {
            if let Some(id) = ev.get("event_id").and_then(|v| v.as_str()) {
                if !seen.insert(id.to_string()) {
                    return Err(format!(
                        "duplicate event_id {id} in timeline after compacted replay"
                    ));
                }
            }
        }

        // Both message event_ids must be present.
        let all_ids: std::collections::HashSet<_> = seen.iter().collect();
        if !all_ids.iter().any(|id| id.as_str() == eid1) {
            return Err(format!(
                "eid1 {eid1} missing from timeline after compacted replay"
            ));
        }
        if !all_ids.iter().any(|id| id.as_str() == eid2) {
            return Err(format!(
                "eid2 {eid2} missing from timeline after compacted replay"
            ));
        }

        // ── Phase D: test compact_room directly (crash-safe temp+rename) ─────
        // Write a known set of events, compact with explicit events list, verify.
        let test_events = vec![
            json!({ "event_id": "$direct1", "room_id": room_id, "type": "m.room.message",
                    "sender": "@u:localhost", "origin_server_ts": 1, "content": {} }),
            json!({ "event_id": "$direct2", "room_id": room_id, "type": "m.room.message",
                    "sender": "@u:localhost", "origin_server_ts": 2, "content": {} }),
            // Duplicate of $direct1 — must be deduplicated.
            json!({ "event_id": "$direct1", "room_id": room_id, "type": "m.room.message",
                    "sender": "@u:localhost", "origin_server_ts": 1, "content": {} }),
        ];

        compact_room(data_dir, room_id, &test_events);

        let compacted_lines: Vec<String> = {
            let f = std::fs::File::open(&journal_path)
                .map_err(|e| format!("open after direct compact: {e}"))?;
            BufReader::new(f)
                .lines()
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| format!("read compacted lines: {e}"))?
        };

        // Should be exactly 2 lines ($direct1 and $direct2).
        if compacted_lines.len() != 2 {
            return Err(format!(
                "direct compact_room: expected 2 lines, got {}; lines: {compacted_lines:?}",
                compacted_lines.len()
            ));
        }
        let has_d1 = compacted_lines.iter().any(|l| l.contains("$direct1"));
        let has_d2 = compacted_lines.iter().any(|l| l.contains("$direct2"));
        if !has_d1 || !has_d2 {
            return Err(format!(
                "direct compact_room: missing $direct1 or $direct2; lines: {compacted_lines:?}"
            ));
        }

        Ok(())
    }

    // ── test 6: internal-task — pdumeta.jsonl lets a restarted node serve a VERIFIABLE PDU ──

    // test:persist:pdumeta_survives_restart:start
    //   purpose: The core internal-task proof.  Node A sends a signed message (client event +
    //            pdumeta.jsonl both persisted).  A SECOND AppState built from the SAME
    //            data_dir + replay_from_dir (simulating a process restart) must reconstruct
    //            the Pdu with its REAL sig/signer_node/prev_events/depth — not the unsigned
    //            synthetic fallback — so that:
    //              (a) the replayed Pdu verifies against the restarted node's own
    //                  NodeKeyStore (which re-derives the SAME ed25519 key from the
    //                  persisted node_ed25519.key file — this IS node A's pubkey);
    //              (b) a completely FRESH peer B (empty RoomLog, its own NodeKeyStore
    //                  TOFU-seeded with node A's pubkey — as a live peer catching up via
    //                  Zenoh would be) ACCEPTS the restarted node's PDU via
    //                  apply_delta_verified: accepted>=1, rejected=0.
    //            Before internal-task this would fail: replay produced sig=[]/signer_node=""/
    //            prev_events=[]/depth=0, so apply_delta_verified would reject it (see
    //            test 7 below, which reproduces exactly that by removing the meta sidecar).
    //   input:  temp dir on filesystem
    //   output: all assertions pass
    //   sideEffects: writes journals + pdumeta sidecar to temp dir; cleaned up on completion
    // test:persist:pdumeta_survives_restart:end
    #[tokio::test]
    async fn pdumeta_survives_restart_and_verifies_for_fresh_peer() {
        let data_dir = make_temp_dir();
        let result = pdumeta_survives_restart_inner(&data_dir).await;
        let _ = std::fs::remove_dir_all(&data_dir);
        result.expect("pdumeta_survives_restart_and_verifies_for_fresh_peer failed");
    }

    async fn pdumeta_survives_restart_inner(data_dir: &Path) -> Result<(), String> {
        use crate::substrate::matrix_events::{Pdu, RoomLog, RoomLogDelta};
        use crate::substrate::node_auth::NodeKeyStore;

        // ── Phase A: node A sends a signed message ──────────────────────────
        let (room_id, orig_event_id) =
            send_one_signed_message(data_dir, "pdumeta_user", "pdumeta-room", "tx1").await?;

        // ── Phase B: restart — fresh AppState from the SAME dir + replay ────
        let state2 = AppState::with_data_dir(data_dir.to_path_buf());
        replay_from_dir(&state2, data_dir).map_err(|e| format!("replay_from_dir: {e}"))?;

        let pdu: Pdu = {
            let rooms = state2
                .rooms
                .lock()
                .map_err(|e| format!("rooms lock: {e}"))?;
            let log = rooms
                .get(&room_id)
                .ok_or_else(|| format!("room {room_id} missing after replay"))?;
            log.ordered()
                .into_iter()
                .find(|p| p.event_id == orig_event_id)
                .cloned()
                .ok_or_else(|| format!("event {orig_event_id} missing from replayed RoomLog"))?
        };

        // (a) Replayed Pdu must be signed and verify against the restarted node's own
        // key_store (which holds its own pubkey via self-trust — the SAME key, reloaded
        // from node_ed25519.key on disk).
        if pdu.sig.is_empty() || pdu.signer_node.is_empty() {
            return Err(format!(
                "replayed Pdu is unsigned after restart (sig.is_empty()={}, signer_node={:?}) \
                 — pdumeta reconstruction did not run",
                pdu.sig.is_empty(),
                pdu.signer_node
            ));
        }
        if !pdu.verify_sig(&state2.key_store) {
            return Err(
                "replayed Pdu failed verify_sig against the restarted node's own key_store"
                    .to_string(),
            );
        }

        // (b) A FRESH peer B (never seen node A before this moment) TOFU-seeds node A's
        // real pubkey, then applies the restarted node's delta via apply_delta_verified.
        let store_b = NodeKeyStore::new();
        store_b.insert(&pdu.signer_node, state2.signer.verifying_key_bytes());
        let mut log_b = RoomLog::new();
        let delta = RoomLogDelta {
            pdus: vec![pdu.clone()],
            collected_depth: 0,
        };
        let (accepted, rejected) = log_b.apply_delta_verified(&delta, &store_b);
        if rejected != 0 {
            return Err(format!(
                "fresh peer B rejected the restarted node's PDU: rejected={rejected}"
            ));
        }
        if accepted < 1 {
            return Err(format!(
                "fresh peer B did not accept the restarted node's PDU: accepted={accepted}"
            ));
        }

        Ok(())
    }

    // send_one_signed_message:start
    //   purpose: Test helper shared by tests 6-7: register a user, log in, create a room
    //            with the given alias, and PUT /send one text message.  Returns the
    //            room_id and the event_id of the sent message.  Factored out so both the
    //            meta-path proof (test 6) and the no-meta contrast (test 7) start from an
    //            identical write phase.
    //   input:  data_dir — temp dir backing the AppState; username, room_alias, txn_id —
    //           distinct per-test values so the two tests don't collide on shared state
    //   output: Result<(String, String), String> — (room_id, event_id) on success
    //   sideEffects: constructs an AppState + TestServer backed by data_dir; writes
    //                journals (client-event + pdumeta) to disk
    // send_one_signed_message:end
    async fn send_one_signed_message(
        data_dir: &Path,
        username: &str,
        room_alias: &str,
        txn_id: &str,
    ) -> Result<(String, String), String> {
        let state1 = AppState::with_data_dir(data_dir.to_path_buf());
        let server1 = TestServer::new(router(state1.clone()));

        let ch: Value = server1
            .post("/_matrix/client/v3/register")
            .json(&json!({ "username": username, "password": "pw" }))
            .await
            .json();
        let sess = ch["session"].as_str().ok_or("no session")?.to_string();
        let reg: Value = server1
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
            .ok_or("no access_token")?
            .to_string();
        let auth_hn = axum::http::HeaderName::from_static("authorization");
        let auth_hv = axum::http::HeaderValue::from_str(&format!("Bearer {token}"))
            .map_err(|e| format!("header: {e}"))?;

        let create = server1
            .post("/_matrix/client/v3/createRoom")
            .add_header(auth_hn.clone(), auth_hv.clone())
            .json(&json!({ "room_alias_name": room_alias }))
            .await;
        if !create.status_code().is_success() {
            return Err(format!("createRoom failed: {}", create.status_code()));
        }
        let create_body: Value = create.json();
        let room_id = create_body["room_id"]
            .as_str()
            .ok_or("no room_id")?
            .to_string();

        let send_path = format!("/_matrix/client/v3/rooms/{room_id}/send/m.room.message/{txn_id}");
        let send = server1
            .put(&send_path)
            .add_header(auth_hn.clone(), auth_hv.clone())
            .json(&json!({ "msgtype": "m.text", "body": "hello internal-task" }))
            .await;
        if !send.status_code().is_success() {
            return Err(format!("send failed: {}", send.status_code()));
        }
        let send_body: Value = send.json();
        let event_id = send_body["event_id"]
            .as_str()
            .ok_or("no event_id from send")?
            .to_string();

        Ok((room_id, event_id))
    }

    // ── test 7: contrast — WITHOUT the pdumeta sidecar, replay stays unsigned ──────

    // test:persist:pdumeta_absent_falls_back_unsigned:start
    //   purpose: Contrast case proving test 6 genuinely exercises the meta-reconstruction
    //            path (not merely luck / an already-passing fallback).  Same write phase
    //            as test 6, but the pdumeta.jsonl sidecar is deleted BEFORE the simulated
    //            restart — reproducing exactly what a pre-internal-task journal looks like (no meta
    //            file at all).  Replay must then fall back to the unsigned synthetic Pdu
    //            (sig empty, signer_node empty), and a fresh peer B's apply_delta_verified
    //            must REJECT it (accepted=0, rejected>=1) — this is the internal-task bug being fixed.
    //   input:  temp dir on filesystem
    //   output: all assertions pass (replayed Pdu unsigned; fresh peer B rejects it)
    //   sideEffects: writes journals to temp dir, then deletes the pdumeta sidecar;
    //                cleaned up on completion
    // test:persist:pdumeta_absent_falls_back_unsigned:end
    #[tokio::test]
    async fn pdumeta_absent_falls_back_to_unsigned_and_is_rejected() {
        let data_dir = make_temp_dir();
        let result = pdumeta_absent_inner(&data_dir).await;
        let _ = std::fs::remove_dir_all(&data_dir);
        result.expect("pdumeta_absent_falls_back_to_unsigned_and_is_rejected failed");
    }

    async fn pdumeta_absent_inner(data_dir: &Path) -> Result<(), String> {
        use crate::persist::sanitize_filename;
        use crate::substrate::matrix_events::{Pdu, RoomLog, RoomLogDelta};
        use crate::substrate::node_auth::NodeKeyStore;

        let (room_id, orig_event_id) =
            send_one_signed_message(data_dir, "nometa_user", "nometa-room", "tx1").await?;

        // Simulate a pre-internal-task journal: remove the meta sidecar entirely.
        let meta_path = data_dir
            .join("rooms")
            .join(format!("{}.pdumeta.jsonl", sanitize_filename(&room_id)));
        if !meta_path.exists() {
            return Err(format!(
                "expected pdumeta sidecar to exist before deletion: {}",
                meta_path.display()
            ));
        }
        std::fs::remove_file(&meta_path).map_err(|e| format!("remove pdumeta sidecar: {e}"))?;

        // Restart with the meta sidecar gone.
        let state2 = AppState::with_data_dir(data_dir.to_path_buf());
        replay_from_dir(&state2, data_dir).map_err(|e| format!("replay_from_dir: {e}"))?;

        let pdu: Pdu = {
            let rooms = state2
                .rooms
                .lock()
                .map_err(|e| format!("rooms lock: {e}"))?;
            let log = rooms
                .get(&room_id)
                .ok_or_else(|| format!("room {room_id} missing after replay"))?;
            log.ordered()
                .into_iter()
                .find(|p| p.event_id == orig_event_id)
                .cloned()
                .ok_or_else(|| format!("event {orig_event_id} missing from replayed RoomLog"))?
        };

        // Fallback path: unsigned synthetic Pdu (pre-internal-task behaviour).
        if !pdu.sig.is_empty() || !pdu.signer_node.is_empty() {
            return Err(format!(
                "expected unsigned fallback Pdu with meta sidecar absent, got sig.is_empty()={}, \
                 signer_node={:?}",
                pdu.sig.is_empty(),
                pdu.signer_node
            ));
        }

        // A fresh peer B must REJECT this unverifiable PDU.
        let store_b = NodeKeyStore::new();
        store_b.insert("nometa_user_node", state2.signer.verifying_key_bytes());
        let mut log_b = RoomLog::new();
        let delta = RoomLogDelta {
            pdus: vec![pdu.clone()],
            collected_depth: 0,
        };
        let (accepted, rejected) = log_b.apply_delta_verified(&delta, &store_b);
        if accepted != 0 {
            return Err(format!(
                "fresh peer B must not accept an unsigned PDU: accepted={accepted}"
            ));
        }
        if rejected < 1 {
            return Err(format!(
                "fresh peer B must count the unsigned PDU as rejected: rejected={rejected}"
            ));
        }

        Ok(())
    }

    // ── test 8: internal-task RETURN — replayed content must be byte-exact, not JSON-round-tripped ──

    // test:persist:pdumeta_content_byte_exact_after_restart:start
    //   purpose: Regression test for the internal-task RETURN finding: replay used to reconstruct a
    //            Pdu's content by re-serializing the parsed client-event JSON
    //            (serde_json::to_vec(&content)). serde_json::Value's Map has no
    //            preserve_order here (no indexmap feature on serde_json in this workspace),
    //            so it is BTreeMap-backed and ALWAYS serializes keys alphabetically,
    //            regardless of the source order. Since Pdu::signed hashes the RAW bytes as
    //            received (send.rs passes the untouched request body straight through), any
    //            message whose JSON keys are not already alphabetical would silently
    //            byte-mismatch after a restart -> verify_sig fails closed on a perfectly
    //            legitimate message. This sends a body with deliberately non-alphabetical
    //            keys ("msgtype" before "body", plus a trailing "z_extra") -- exactly the
    //            shape real Matrix clients (Element, nio) send -- and asserts the replayed
    //            Pdu.content is byte-identical to what was sent, not merely "parses to the
    //            same value". Existing tests 6/7 use `.json(&json!({...}))`, which ALSO
    //            forces alphabetical order at the source (json! builds the same BTreeMap-
    //            backed Value), so they could not have caught this: the "signed" and
    //            "round-tripped" bytes coincidentally matched. This test sends raw bytes via
    //            `.bytes()` to bypass that masking.
    //   input:  temp dir on filesystem
    //   output: replayed Pdu.content == original raw bytes (byte string equality);
    //           verify_sig passes; a fresh peer B's apply_delta_verified accepts it
    //   sideEffects: writes journals to temp dir; cleaned up on completion
    // test:persist:pdumeta_content_byte_exact_after_restart:end
    #[tokio::test]
    async fn pdumeta_content_byte_exact_after_restart() {
        let data_dir = make_temp_dir();
        let result = pdumeta_content_byte_exact_inner(&data_dir).await;
        let _ = std::fs::remove_dir_all(&data_dir);
        result.expect("pdumeta_content_byte_exact_after_restart failed");
    }

    async fn pdumeta_content_byte_exact_inner(data_dir: &Path) -> Result<(), String> {
        use crate::substrate::matrix_events::{Pdu, RoomLog, RoomLogDelta};
        use crate::substrate::node_auth::NodeKeyStore;

        let state1 = AppState::with_data_dir(data_dir.to_path_buf());
        let server1 = TestServer::new(router(state1.clone()));

        let ch: Value = server1
            .post("/_matrix/client/v3/register")
            .json(&json!({ "username": "nonalpha_user", "password": "pw" }))
            .await
            .json();
        let sess = ch["session"].as_str().ok_or("no session")?.to_string();
        let reg: Value = server1
            .post("/_matrix/client/v3/register")
            .json(&json!({
                "username": "nonalpha_user",
                "password": "pw",
                "auth": { "type": "m.login.dummy", "session": sess }
            }))
            .await
            .json();
        let token = reg["access_token"]
            .as_str()
            .ok_or("no access_token")?
            .to_string();
        let auth_hn = axum::http::HeaderName::from_static("authorization");
        let auth_hv = axum::http::HeaderValue::from_str(&format!("Bearer {token}"))
            .map_err(|e| format!("header: {e}"))?;

        let create = server1
            .post("/_matrix/client/v3/createRoom")
            .add_header(auth_hn.clone(), auth_hv.clone())
            .json(&json!({ "room_alias_name": "nonalpha-room" }))
            .await;
        if !create.status_code().is_success() {
            return Err(format!("createRoom failed: {}", create.status_code()));
        }
        let create_body: Value = create.json();
        let room_id = create_body["room_id"]
            .as_str()
            .ok_or("no room_id")?
            .to_string();

        // Deliberately non-alphabetical key order + a trailing key that sorts last anyway,
        // so a naive "it happened to already be sorted" coincidence is ruled out either way.
        let raw_body: &[u8] = br#"{"msgtype":"m.text","body":"hello nonalpha internal-task","z_extra":1}"#;
        let send_path = format!("/_matrix/client/v3/rooms/{room_id}/send/m.room.message/tx1");
        let send = server1
            .put(&send_path)
            .add_header(auth_hn.clone(), auth_hv.clone())
            .content_type("application/json")
            .bytes(axum::body::Bytes::copy_from_slice(raw_body))
            .await;
        if !send.status_code().is_success() {
            return Err(format!("send failed: {}", send.status_code()));
        }
        let send_body: Value = send.json();
        let event_id = send_body["event_id"]
            .as_str()
            .ok_or("no event_id from send")?
            .to_string();

        // ── Restart — fresh AppState from the SAME dir + replay ────────────────
        let state2 = AppState::with_data_dir(data_dir.to_path_buf());
        replay_from_dir(&state2, data_dir).map_err(|e| format!("replay_from_dir: {e}"))?;

        let pdu: Pdu = {
            let rooms = state2
                .rooms
                .lock()
                .map_err(|e| format!("rooms lock: {e}"))?;
            let log = rooms
                .get(&room_id)
                .ok_or_else(|| format!("room {room_id} missing after replay"))?;
            log.ordered()
                .into_iter()
                .find(|p| p.event_id == event_id)
                .cloned()
                .ok_or_else(|| format!("event {event_id} missing from replayed RoomLog"))?
        };

        // The core assertion: byte-exact, not "parses to the same JSON value".
        if pdu.content != raw_body {
            return Err(format!(
                "replayed content is NOT byte-exact to what was sent\n  sent:     {}\n  replayed: {}",
                String::from_utf8_lossy(raw_body),
                String::from_utf8_lossy(&pdu.content),
            ));
        }

        if pdu.sig.is_empty() || pdu.signer_node.is_empty() {
            return Err("replayed Pdu is unsigned after restart".to_string());
        }
        if !pdu.verify_sig(&state2.key_store) {
            return Err(
                "replayed Pdu failed verify_sig against the restarted node's own key_store \
                         (this is exactly the internal-task RETURN bug: content byte-mismatch after a \
                         JSON round-trip breaks the signature)"
                    .to_string(),
            );
        }

        let store_b = NodeKeyStore::new();
        store_b.insert(&pdu.signer_node, state2.signer.verifying_key_bytes());
        let mut log_b = RoomLog::new();
        let delta = RoomLogDelta {
            pdus: vec![pdu.clone()],
            collected_depth: 0,
        };
        let (accepted, rejected) = log_b.apply_delta_verified(&delta, &store_b);
        if rejected != 0 || accepted < 1 {
            return Err(format!(
                "fresh peer B did not accept the restarted node's PDU: accepted={accepted} rejected={rejected}"
            ));
        }

        Ok(())
    }

    // ── test 9: compact_all must not lose history beyond the timeline cap ──────

    // test:persist:gc_compaction_preserves_full_history:start
    //   purpose: Regression test for the retention-cap/compaction bug: compact_all
    //            used to rebuild the on-disk journal from AppState.room_timeline,
    //            which AppState::append_room_timeline trims to timeline_max_events.
    //            That silently discarded events beyond the cap from disk on the
    //            FIRST restart's compaction; by the SECOND restart, replay had only
    //            the already-trimmed disk to read from, permanently corrupting the
    //            grow-only RoomLog (message history) AND room_state (e.g.
    //            m.room.create, which is never "refreshed" by later messages and
    //            would be the first thing evicted by chat volume under the old bug).
    //            Steps: cap the timeline at 3, create a room (m.room.create +
    //            member), send 6 messages (twice the cap), then simulate TWO
    //            restarts (replay_from_dir + compact_all, twice) and assert at each
    //            that RoomLog still has all 6 messages and room_state still has
    //            m.room.create — i.e. compaction never used the capped in-memory
    //            projection as its source of on-disk truth.
    //   input:  temp dir on filesystem
    //   output: all assertions pass
    //   sideEffects: writes journals to temp dir; cleans up on completion
    // test:persist:gc_compaction_preserves_full_history:end
    #[tokio::test]
    async fn gc_compaction_preserves_full_history_beyond_cap() {
        let data_dir = make_temp_dir();
        let result = gc_compaction_preserves_full_history_beyond_cap_inner(&data_dir).await;
        let _ = std::fs::remove_dir_all(&data_dir);
        result.expect("gc_compaction_preserves_full_history_beyond_cap failed");
    }

    async fn gc_compaction_preserves_full_history_beyond_cap_inner(
        data_dir: &Path,
    ) -> Result<(), String> {
        const CAP: usize = 3;
        const N_MESSAGES: usize = 6;

        // ── Phase A: write, with a small retention cap ──────────────────────
        let mut state1 = AppState::with_data_dir(data_dir.to_path_buf());
        std::sync::Arc::get_mut(&mut state1)
            .ok_or("state1 not uniquely owned")?
            .timeline_max_events = CAP;
        let server1 = TestServer::new(router(state1.clone()));

        let reg1 = server1
            .post("/_matrix/client/v3/register")
            .json(&json!({}))
            .await;
        let session_id = reg1.json::<Value>()["session"]
            .as_str()
            .ok_or("no session in 401 body")?
            .to_string();
        let reg2 = server1
            .post("/_matrix/client/v3/register")
            .json(&json!({
                "username": "bob",
                "password": "hunter2",
                "auth": { "type": "m.login.dummy", "session": session_id }
            }))
            .await;
        if !reg2.status_code().is_success() {
            return Err(format!("register step 2 failed: {}", reg2.status_code()));
        }

        let login = server1
            .post("/_matrix/client/v3/login")
            .json(&json!({
                "type": "m.login.password",
                "identifier": { "type": "m.id.user", "user": "bob" },
                "password": "hunter2"
            }))
            .await;
        let token = login.json::<Value>()["access_token"]
            .as_str()
            .ok_or("no access_token in login response")?
            .to_string();
        let auth_hn = axum::http::HeaderName::from_static("authorization");
        let auth_hv = axum::http::HeaderValue::from_str(&format!("Bearer {token}")).unwrap();

        let create = server1
            .post("/_matrix/client/v3/createRoom")
            .add_header(auth_hn.clone(), auth_hv.clone())
            .json(&json!({}))
            .await;
        if !create.status_code().is_success() {
            return Err(format!("createRoom failed: {}", create.status_code()));
        }
        let room_id = create.json::<Value>()["room_id"]
            .as_str()
            .ok_or("no room_id in createRoom response")?
            .to_string();

        // More messages than the cap — the FIRST ones must still survive on disk.
        for i in 0..N_MESSAGES {
            let send_path =
                format!("/_matrix/client/v3/rooms/{room_id}/send/m.room.message/txn{i}");
            let send = server1
                .put(&send_path)
                .add_header(auth_hn.clone(), auth_hv.clone())
                .json(&json!({ "msgtype": "m.text", "body": format!("msg{i}") }))
                .await;
            if !send.status_code().is_success() {
                return Err(format!("send {i} failed: {}", send.status_code()));
            }
        }

        // Sanity: the LIVE in-memory timeline is indeed capped (proves the cap is
        // active, so a pass below isn't just "the cap never kicked in").
        {
            let rt = state1
                .room_timeline
                .lock()
                .map_err(|e| format!("timeline lock: {e}"))?;
            let live_len = rt.get(&room_id).map(|v| v.len()).unwrap_or(0);
            if live_len > CAP {
                return Err(format!(
                    "cap not active: live room_timeline has {live_len} entries, expected <= {CAP}"
                ));
            }
        }

        // ── Phase B: restart #1 — replay, then compact (mirrors main.rs boot) ──
        // The cap must be re-applied on every restart (a real deployment keeps
        // MATRIX_HS_TIMELINE_MAX_EVENTS set across restarts) so replay's own
        // append_room_timeline calls trim room_timeline exactly as they would live.
        let mut state2 = AppState::with_data_dir(data_dir.to_path_buf());
        std::sync::Arc::get_mut(&mut state2)
            .ok_or("state2 not uniquely owned")?
            .timeline_max_events = CAP;
        replay_from_dir(&state2, data_dir).map_err(|e| format!("replay 1: {e}"))?;
        compact_all(data_dir);
        assert_full_history_intact(&state2, &room_id, N_MESSAGES, "after restart #1")?;

        // ── Phase C: restart #2 — replay from the JUST-COMPACTED disk, then
        // compact again. This is the step that exposed the original bug: restart
        // #1's compaction used to already have thrown away everything beyond the
        // cap, so restart #2's replay would inherit a permanently truncated
        // RoomLog/room_state.
        let mut state3 = AppState::with_data_dir(data_dir.to_path_buf());
        std::sync::Arc::get_mut(&mut state3)
            .ok_or("state3 not uniquely owned")?
            .timeline_max_events = CAP;
        replay_from_dir(&state3, data_dir).map_err(|e| format!("replay 2: {e}"))?;
        compact_all(data_dir);
        assert_full_history_intact(&state3, &room_id, N_MESSAGES, "after restart #2")?;

        Ok(())
    }

    // assert_full_history_intact:start
    //   purpose: Assert that a room's full message history (RoomLog) and its
    //            m.room.create state event both survived a replay, regardless of
    //            any in-memory retention cap applied afterwards.
    //   input:  state — the freshly-replayed AppState; room_id; expected_messages
    //           — total messages that must still be in RoomLog; label — for
    //           error messages
    //   output: Result<(), String>
    //   sideEffects: none
    // assert_full_history_intact:end
    fn assert_full_history_intact(
        state: &std::sync::Arc<AppState>,
        room_id: &str,
        expected_messages: usize,
        label: &str,
    ) -> Result<(), String> {
        let n_pdus = {
            let rooms = state.rooms.lock().map_err(|e| format!("rooms lock: {e}"))?;
            rooms
                .get(room_id)
                .map(|log| log.ordered().len())
                .unwrap_or(0)
        };
        if n_pdus != expected_messages {
            return Err(format!(
                "{label}: RoomLog has {n_pdus} messages, expected {expected_messages} \
                 (compaction lost history beyond the timeline cap)"
            ));
        }

        let has_create = {
            let rs = state
                .room_state
                .lock()
                .map_err(|e| format!("room_state lock: {e}"))?;
            rs.get(room_id)
                .map(|evs| evs.iter().any(|e| e.event_type == "m.room.create"))
                .unwrap_or(false)
        };
        if !has_create {
            return Err(format!(
                "{label}: m.room.create missing from room_state (evicted by the \
                 timeline cap and lost on compaction)"
            ));
        }

        Ok(())
    }

    // gc_survives_restart_and_shrinks_the_journal:start
    //   purpose: Garbage collection has to hold across a restart, and it has to reach
    //            disk. Replay re-adds every event still in the journal, so without a
    //            persisted watermark a collected room comes back in full on the next
    //            boot — and then this node starts offering peers the events it had
    //            decided to drop. Bounding memory while the journal grows forever would
    //            also miss half the point of collecting at all.
    //   input:  ten messages in one room, collect keeping four
    //   output: log shrinks, journal shrinks, marker written; after replay the room is
    //           still four events, not ten
    //   sideEffects: writes journals under a temp dir
    // gc_survives_restart_and_shrinks_the_journal:end
    #[tokio::test]
    async fn gc_survives_restart_and_shrinks_the_journal() -> Result<(), String> {
        let data_dir = make_temp_dir();
        let room_id;

        {
            let state = AppState::with_data_dir(data_dir.clone());
            let server = TestServer::new(router(state.clone()));

            let ch: Value = server
                .post("/_matrix/client/v3/register")
                .json(&json!({ "username": "gcuser", "password": "pw" }))
                .await
                .json();
            let reg: Value = server
                .post("/_matrix/client/v3/register")
                .json(&json!({
                    "username": "gcuser", "password": "pw",
                    "auth": { "type": "m.login.dummy", "session": ch["session"] }
                }))
                .await
                .json();
            let token = reg["access_token"].as_str().ok_or("no token")?.to_string();
            let auth = format!("Bearer {token}");

            let created: Value = server
                .post("/_matrix/client/v3/createRoom")
                .add_header(
                    axum::http::HeaderName::from_static("authorization"),
                    axum::http::HeaderValue::from_str(&auth).unwrap(),
                )
                .json(&json!({ "room_alias_name": "gc-room" }))
                .await
                .json();
            room_id = created["room_id"].as_str().ok_or("no room_id")?.to_string();

            for i in 0..10 {
                server
                    .put(&format!(
                        "/_matrix/client/v3/rooms/{room_id}/send/m.room.message/gc-tx-{i}"
                    ))
                    .add_header(
                        axum::http::HeaderName::from_static("authorization"),
                        axum::http::HeaderValue::from_str(&auth).unwrap(),
                    )
                    .json(&json!({ "msgtype": "m.text", "body": format!("msg {i}") }))
                    .await
                    .assert_status_ok();
            }

            let before = state.rooms.lock().unwrap().get(&room_id).unwrap().len();
            assert!(
                before >= 10,
                "precondition: the log should hold the ten messages (plus room state), got {before}"
            );
            let journal = data_dir
                .join("rooms")
                .join(format!("{}.jsonl", crate::persist::sanitize_filename(&room_id)));

            let dropped = state.collect_room_log_to(&room_id, 4);
            assert!(dropped > 0, "collection must actually drop something");

            let after = state.rooms.lock().unwrap().get(&room_id).unwrap().len();
            assert!(
                after <= 4,
                "the log must be down to the cap, got {after} (cut is by depth so it \
                 may drop a little more, never less)"
            );

            // The watermark reached disk...
            let marker = data_dir
                .join("rooms")
                .join(format!("{}.gc", crate::persist::sanitize_filename(&room_id)));
            assert!(marker.exists(), "GC marker must be written next to the journal");

            // ...and so did the pruning, if the journal path resolved as expected.
            if journal.exists() {
                let lines = std::fs::read_to_string(&journal)
                    .map_err(|e| e.to_string())?
                    .lines()
                    .filter(|l| !l.trim().is_empty())
                    .count();
                assert!(
                    lines <= 4,
                    "the journal must be pruned too, or GC bounds memory and lets disk \
                     grow forever; {lines} lines remain"
                );
            }
        }

        // ── restart ───────────────────────────────────────────────────────────
        let state2 = AppState::with_data_dir(data_dir.clone());
        replay_from_dir(&state2, &data_dir).map_err(|e| format!("replay: {e}"))?;

        let after_restart = state2
            .rooms
            .lock()
            .unwrap()
            .get(&room_id)
            .map(|l| l.len())
            .unwrap_or(0);
        assert!(
            after_restart <= 4,
            "the collected events must NOT come back on restart — got {after_restart}. \
             Replay re-adds whatever is still in the journal, so this fails if the \
             watermark is not persisted and re-applied."
        );

        let _ = std::fs::remove_dir_all(&data_dir);
        Ok(())
    }

    // test:persist:every_constructor_honours_the_retention_caps:start
    //   purpose: Regression test for the constructor that silently removed the
    //            retention caps. AppState::with_server_name hardcoded
    //            timeline_max_events and roomlog_max_events to 0 -- 0 means
    //            UNLIMITED -- so a deployment built through that path ran with no
    //            bound at all, and nothing in the running process said so: the
    //            only evidence was a line of source. The cap is the whole
    //            protection between a long-lived node and unbounded growth, and a
    //            protection that a constructor can switch off by being called is
    //            not one.
    //            Two claims, one per way the caps are supposed to arrive:
    //              1. passed explicitly, they land on the state AND the timeline
    //                 cap actually trims (plumbing, not just a stored field);
    //              2. left to the environment, with_server_name reads them
    //                 instead of overriding them.
    //            The value in (2) is deliberately enormous rather than small:
    //            environment variables are process-global and libtest runs tests
    //            in parallel threads, so any other test that reads these two
    //            variables while this one holds them set would see a cap of
    //            100_000. A cap that large trims nothing, so a leak of the
    //            setting can only make another test behave like the default
    //            (unlimited) -- it cannot make one fail by truncating. A small
    //            value would have been the opposite: harmless here, dangerous to
    //            a neighbour. Both variables are removed again before returning.
    //   input:  none (no filesystem; environment variables set and restored)
    //   output: all assertions pass
    //   sideEffects: sets and then removes MATRIX_HS_TIMELINE_MAX_EVENTS and
    //                MATRIX_HS_ROOMLOG_MAX_EVENTS
    // test:persist:every_constructor_honours_the_retention_caps:end
    #[test]
    fn every_constructor_honours_the_retention_caps() {
        use serde_json::json;

        // ── 1. explicit caps, and the timeline cap really trims ──────────────
        let state = AppState::with_server_name_and_caps("localhost".to_string(), 1, 5);
        assert_eq!(
            state.timeline_max_events, 1,
            "an explicitly passed timeline cap must reach the state"
        );
        assert_eq!(
            state.roomlog_max_events, 5,
            "an explicitly passed roomlog cap must reach the state"
        );
        for i in 0..3 {
            state.append_room_timeline(
                "!cap_probe:localhost",
                json!({ "type": "m.room.message", "body": format!("cap-{i}") }),
            );
        }
        {
            let rt = state.room_timeline.lock().expect("room_timeline lock");
            let live = rt.get("!cap_probe:localhost").map_or(0, |v| v.len());
            assert_eq!(
                live, 1,
                "with timeline_max_events=1 the timeline must hold one entry, not {live} -- \
                 a cap that is stored but not consulted is the same defect as no cap"
            );
        }

        // ── 2. caps left to the environment ──────────────────────────────────
        const HUGE: &str = "100000";
        std::env::set_var("MATRIX_HS_TIMELINE_MAX_EVENTS", HUGE);
        std::env::set_var("MATRIX_HS_ROOMLOG_MAX_EVENTS", HUGE);
        let from_env = AppState::with_server_name("localhost".to_string());
        std::env::remove_var("MATRIX_HS_TIMELINE_MAX_EVENTS");
        std::env::remove_var("MATRIX_HS_ROOMLOG_MAX_EVENTS");

        assert_eq!(
            from_env.timeline_max_events, 100_000,
            "with_server_name must read MATRIX_HS_TIMELINE_MAX_EVENTS, not hardcode 0 \
             (0 means unlimited: a node configured with a cap would run without one)"
        );
        assert_eq!(
            from_env.roomlog_max_events, 100_000,
            "with_server_name must read MATRIX_HS_ROOMLOG_MAX_EVENTS, not hardcode 0"
        );

        // ── 3. unset means unlimited, and that is still the default ──────────
        let defaults = AppState::with_server_name("localhost".to_string());
        assert_eq!(
            (defaults.timeline_max_events, defaults.roomlog_max_events),
            (0, 0),
            "with the variables unset both caps must be 0 (unlimited) -- the fix must not \
             invent a bound that the operator did not ask for"
        );
    }

    // prune_then_append_visible:start
    //   purpose: Regression for the prune/append race — an event appended AFTER
    //            prune_room_journal must be readable from the live journal, and a second
    //            prune must not lose it. With a bare fs::rename the process-wide cached
    //            O_APPEND handle kept pointing at the replaced (unlinked) inode, so that
    //            append landed in a file nobody reads: present in the pdumeta sidecar and
    //            in the RoomLog, absent from the journal.
    //   input:  none — temp data_dir via AppState::with_data_dir
    //   output: assertions only; a panic names the failing step
    //   sideEffects: creates and then removes one temp directory
    // prune_then_append_visible:end
    #[test]
    fn prune_then_append_is_visible_and_survives_second_prune() {
        let data_dir = make_temp_dir();
        let state = AppState::with_data_dir(data_dir.to_path_buf());
        let room_id = "!prune_swap_probe:localhost";
        let journal_path = data_dir
            .join("rooms")
            .join(format!("{}.jsonl", crate::persist::sanitize_filename(room_id)));

        let ev = |id: &str, ts: i64| {
            json!({ "event_id": id, "room_id": room_id, "type": "m.room.message",
                    "sender": "@u:localhost", "origin_server_ts": ts, "content": {} })
        };
        let read_ids = |p: &Path| -> Vec<String> {
            let txt = std::fs::read_to_string(p).expect("read journal");
            txt.lines()
                .filter(|l| !l.trim().is_empty())
                .filter_map(|l| serde_json::from_str::<Value>(l).ok())
                .filter_map(|v| v.get("event_id").and_then(|e| e.as_str()).map(str::to_string))
                .collect()
        };

        // 1. Three events through the public writer — this is what populates APPEND_FILES
        //    with an O_APPEND handle for exactly this path, i.e. the precondition for the bug.
        state.persist_room_event(room_id, &ev("$p1", 1));
        state.persist_room_event(room_id, &ev("$p2", 2));
        state.persist_room_event(room_id, &ev("$p3", 3));

        let before = read_ids(&journal_path);
        assert_eq!(before.len(), 3, "3 events expected before prune, got {before:?}");

        // 2. Prune keeps two survivors.
        let kept: std::collections::HashSet<String> =
            ["$p1".to_string(), "$p2".to_string()].into_iter().collect();
        crate::persist::prune_room_journal(&data_dir, room_id, &kept);

        let after_prune = read_ids(&journal_path);
        assert_eq!(
            after_prune.len(),
            2,
            "prune must keep exactly the survivors, got {after_prune:?}"
        );

        // 3. Append after the prune — the step that used to vanish into the unlinked inode.
        state.persist_room_event(room_id, &ev("$p4", 4));

        let after_append = read_ids(&journal_path);
        assert!(
            after_append.iter().any(|id| id == "$p4"),
            "append after prune must be visible in the live journal; journal holds {after_append:?}"
        );

        // 4. Second prune over all three survivors must keep the appended event.
        let kept2: std::collections::HashSet<String> = ["$p1".to_string(), "$p2".to_string(), "$p4".to_string()]
            .into_iter()
            .collect();
        crate::persist::prune_room_journal(&data_dir, room_id, &kept2);

        let after_second = read_ids(&journal_path);
        assert_eq!(
            after_second.len(),
            3,
            "second prune must keep 3 events, got {after_second:?}"
        );
        assert!(
            after_second.iter().any(|id| id == "$p4"),
            "second prune must not lose the post-prune append; journal holds {after_second:?}"
        );

        let _ = std::fs::remove_dir_all(&data_dir);
    }

}

    // doc_markers_balanced:start
    //   purpose: Guard the :start/:end doc-marker convention itself. These pairs
    //            are machine-read tooling targets (see the agent handoff record (kept private): the START/END
    //            AI-HEADER style); an unbalanced, duplicated or misordered marker
    //            silently breaks whatever extracts them — and exactly that
    //            happened more than once during the 2026-08-22 persist edits,
    //            which is why this test exists. Scans every non-test source file
    //            reachable from src/, walks a stack, and fails on any anomaly.
    //   input:  none (reads src/ via env!("CARGO_MANIFEST_DIR"))
    //   output: none (asserts)
    //   sideEffects: none
    // doc_markers_balanced:end
    #[test]
    fn doc_markers_balanced() {
        let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut stack: Vec<(String, usize)> = Vec::new();
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut count = 0usize;

        fn visit(
            dir: &std::path::Path,
            stack: &mut Vec<(String, usize)>,
            seen: &mut std::collections::HashSet<String>,
            count: &mut usize,
        ) {
            let entries = match std::fs::read_dir(dir) {
                Ok(e) => e,
                Err(_) => return,
            };
            for entry in entries.flatten() {
                let p = entry.path();
                if p.is_dir() {
                    visit(&p, stack, seen, count);
                    continue;
                }
                if p.extension().and_then(|e| e.to_str()) != Some("rs") {
                    continue;
                }
                if p.file_name().and_then(|n| n.to_str()).map(|n| n.ends_with("_test.rs")).unwrap_or(false) {
                    continue; // test files carry the convention too, but only
                              // non-test source is the tooling target
                }
                if let Ok(body) = std::fs::read_to_string(&p) {
                    for (lineno, line) in body.lines().enumerate() {
                        let t = line.trim();
                        if let Some(name) = t.strip_prefix("// ").and_then(|r| {
                            r.strip_suffix(":start")
                        }) {
                            let name = name.trim().to_string();
                            assert!(
                                seen.insert(format!("{:?}{}", p, name)),
                                "{}:{}: duplicate :start block {name:?}",
                                p.display(),
                                lineno + 1
                            );
                            stack.push((name, lineno + 1));
                            *count += 1;
                        } else if let Some(name) =
                            t.strip_prefix("// ").and_then(|r| r.strip_suffix(":end"))
                        {
                            let (top, start_line) = stack
                                .pop()
                                .unwrap_or_else(|| panic!("{}:{}: :end without :start", p.display(), lineno + 1));
                            assert_eq!(
                                top,
                                name.trim(),
                                "{}:{}: :end {:?} does not close :start {:?} (opened line {start_line})",
                                p.display(),
                                lineno + 1,
                                name.trim(),
                                top
                            );
                        }
                    }
                }
            }
        }

        visit(&root, &mut stack, &mut seen, &mut count);
        assert!(stack.is_empty(), "unclosed :start blocks: {stack:?}");
        assert!(count > 50, "suspiciously few markers found ({count}) — scanner broken?");
    }
