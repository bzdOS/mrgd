// START_AI_HEADER
// MODULE: matrix-hs/src/state_replication_cluster_test.rs
// PURPOSE: Cross-node convergence tests for ROOM STATE replication under the
//          `cluster` feature — mirrors cluster_test.rs's / ephemeral_cluster_test.rs's
//          two-in-process-axum-servers-over-real-Zenoh pattern, but exercises the
//          "state" ZenohCrdtSink key added in routes/room_state.rs (publish_state_event /
//          drain_cluster_state) instead of "events" (timeline PDUs) or
//          "typing"/"receipt" (ephemeral EDUs).
//
//          Proves:
//            1. state_replication_membership_converges: a room created on node-a,
//               with bob (a node-a-local user) joining it, converges to node-b's
//               room_state — m.room.create AND bob's m.room.member(join) are both
//               visible on node-b after one Zenoh gossip round trip, both via the raw
//               room_state map AND via node-b's /sync response's state.events. This
//               closes the gap the task describes: room_state was previously a
//               per-node-LOCAL structure with no cross-node replication at all.
//            2. state_lww_conflict_resolution_deterministic: a PURE (no Zenoh) test
//               of the LWW merge itself (AppState::apply_remote_state_event) — two
//               conflicting writes to the SAME (event_type,state_key) are fed to two
//               independent AppState replicas in OPPOSITE orders; both replicas must
//               converge to the IDENTICAL winner, proving the (origin_server_ts,
//               event_id) tiebreak is a deterministic, order-independent function
//               (mirrors the style of mrgd/src/crdt.rs's own convergence tests).
// DEPENDENCIES: axum-test, zenoh, matrix_hs::{router, AppState, state::{ClusterConfig, StateEvent}}
// END_AI_HEADER

#[cfg(all(test, feature = "cluster"))]
mod tests {
    use crate::{router, state::ClusterConfig, AppState};
    use axum_test::TestServer;
    use serde_json::{json, Value};

    // register_and_bearer:start
    //   purpose: Register a user via two-step UIA and return an Authorization
    //            bearer header (duplicated per-module by repo convention — see
    //            cluster_test.rs's identically-named helper).
    //   input:  server, username
    //   output: (HeaderName, HeaderValue)
    //   sideEffects: inserts the user into the server's AppState via /register
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

    // cluster:state_replication_membership_converges:start
    //   purpose: Two in-process axum servers (node-a, node-b) sharing a real Zenoh
    //            peer mesh on loopback. A room (with its initial m.room.create /
    //            m.room.member / power_levels / ... state) is created on node-a;
    //            bob (also local to node-a) then joins it. Both the createRoom burst
    //            and bob's join must converge to node-b's room_state within one
    //            Zenoh gossip round trip (~200 ms) — proving cross-node room-state
    //            replication actually works, not just the timeline.
    //   input:  none (all resources constructed in-test)
    //   output: node-b's room_state contains m.room.create for the room AND bob's
    //           m.room.member(join); node-b's /sync response's
    //           rooms.join[room_id].state.events also contains both
    //   sideEffects: opens two real Zenoh sessions; background tokio tasks per room
    //                sink; network I/O on loopback only
    // cluster:state_replication_membership_converges:end
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn state_replication_membership_converges() {
        let _zg = crate::test_util::ZENOH_TEST_LOCK.acquire().await.unwrap();
        // ── Two Zenoh peer sessions ────────────────────────────────────────────
        let sess_a = zenoh::open(zenoh::Config::default())
            .await
            .expect("zenoh session A");
        let sess_b = zenoh::open(zenoh::Config::default())
            .await
            .expect("zenoh session B");

        let prefix = "mrgd/matrix/room/state-cluster-test-0";

        let state_a = AppState::with_cluster(ClusterConfig {
            session: sess_a,
            key_prefix: prefix.to_string(),
            server_name: "state-node-a".to_string(),
        });
        let state_b = AppState::with_cluster(ClusterConfig {
            session: sess_b,
            key_prefix: prefix.to_string(),
            server_name: "state-node-b".to_string(),
        });

        let server_a = TestServer::new(router(state_a.clone()));
        let server_b = TestServer::new(router(state_b.clone()));

        // alice creates the room; bob joins it — both local to node-a (membership
        // convergence is what we're proving here, not cross-node registration).
        let (auth_a_name, auth_a_val) = register_and_bearer(&server_a, "alice").await;
        let (bob_name, bob_val) = register_and_bearer(&server_a, "bob").await;
        // node-b needs SOME authenticated caller for /sync's device_lists path, but
        // that is not exercised here — no registration needed on node-b for this test.

        let room_alias = "state-cluster-room-0";
        let room_id = format!("!{room_alias}:state-node-a");

        // node-b learns of the room the way a federating server would — same
        // room_id, added directly (a room has one home server; room DISCOVERY is
        // out of scope, only state CONVERGENCE for an already-known room_id).
        // This MUST happen BEFORE node-a creates & publishes: Zenoh pub/sub has no
        // replay for late subscribers, and state catch-up for offline/late nodes
        // is a documented seam. Warm up both subscribers (the "state" key shares
        // the room's sink) FIRST, then create on node-a so the create-state deltas
        // land on an already-listening node-b.
        state_b.ensure_room_state(&room_id);
        let _ = server_a.get("/_matrix/client/v3/sync").await;
        let _ = server_b.get("/_matrix/client/v3/sync").await;
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        server_a
            .post("/_matrix/client/v3/createRoom")
            .add_header(auth_a_name.clone(), auth_a_val.clone())
            .json(&json!({ "room_alias_name": room_alias, "name": "State Cluster Room" }))
            .await
            .assert_status_ok();

        // let the create-state deltas propagate to node-b before bob joins
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        // ── bob joins the room on node-a ────────────────────────────────────────
        server_a
            .post(&format!("/_matrix/client/v3/rooms/{room_id}/join"))
            .add_header(bob_name.clone(), bob_val.clone())
            .json(&json!({}))
            .await
            .assert_status_ok();

        // Zenoh peer-mode gossip on loopback is typically <5 ms; 200 ms matches the
        // safety margin used by cluster_test.rs's PDU convergence test.
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        // ── node-b's /sync must drain_cluster_state and show the converged state ──
        // /sync is scoped to the caller's own joined rooms (routes/sync.rs::
        // build_join_rooms) — bob's join membership converged to node-b's
        // room_state above, so his token (verified against the shared global
        // HMAC secret; device_id falls back to DEVICE1 since he's not
        // registered locally on node-b, which is fine — extract_caller doesn't
        // require that) resolves to a genuine join on node-b.
        let sync_resp_b = server_b
            .get("/_matrix/client/v3/sync")
            .add_header(bob_name.clone(), bob_val.clone())
            .await;
        sync_resp_b.assert_status_ok();
        let sync_body_b: Value = sync_resp_b.json();

        let state_events_b = sync_body_b["rooms"]["join"][&room_id]["state"]["events"]
            .as_array()
            .unwrap_or_else(|| {
                panic!("node-b /sync has no state.events for room {room_id}; body: {sync_body_b}")
            });

        let has_create = state_events_b
            .iter()
            .any(|ev| ev["type"] == "m.room.create");
        assert!(
            has_create,
            "node-b /sync state.events must contain m.room.create after convergence; got {state_events_b:?}"
        );

        let bob_member = state_events_b
            .iter()
            .find(|ev| ev["type"] == "m.room.member" && ev["state_key"] == "@bob:state-node-a");
        assert!(
            bob_member.is_some(),
            "node-b /sync state.events must contain bob's m.room.member(join) after \
             convergence; got {state_events_b:?}"
        );
        assert_eq!(
            bob_member.unwrap()["content"]["membership"].as_str(),
            Some("join"),
            "bob's replicated membership must be 'join'"
        );

        // ── Also verify directly against room_state (not just the /sync JSON) ───
        // AND that users_sharing_room_with (the basis of cross-node device-list
        // gossip targeting) is now correct on node-b for this room.
        {
            let rs = state_b
                .room_state
                .lock()
                .expect("room_state lock (node-b verify)");
            let events = rs
                .get(&room_id)
                .expect("room_id present in node-b room_state");
            assert!(
                events.iter().any(|ev| ev.event_type == "m.room.create"),
                "node-b room_state must contain m.room.create directly"
            );
            assert!(
                events.iter().any(|ev| {
                    ev.event_type == "m.room.member"
                        && ev.state_key == "@bob:state-node-a"
                        && ev.content.get("membership").and_then(|v| v.as_str()) == Some("join")
                }),
                "node-b room_state must contain bob's join membership directly"
            );
        }

        let shared = state_b.users_sharing_room_with("@alice:state-node-a");
        assert!(
            shared.contains("@bob:state-node-a"),
            "users_sharing_room_with on node-b must see bob as sharing the room with \
             alice now that membership has replicated; got {shared:?}"
        );
    }

    // cluster:state_lww_conflict_resolution_deterministic:start
    //   purpose: PURE test of the LWW merge (AppState::apply_remote_state_event) with
    //            NO Zenoh involved — proves the deterministic (origin_server_ts,
    //            event_id) tiebreak is order-independent: two conflicting writes to
    //            the SAME (event_type,state_key) slot, applied to two independent
    //            replicas in OPPOSITE orders, converge to the SAME winner on both
    //            sides. This is the "two nodes set the same (type,state_key)
    //            concurrently" requirement — modeled directly against the merge
    //            function rather than over real Zenoh (mrgd/src/crdt.rs's own laws
    //            tests use the same direct-apply style for LWW/OrSet/GCounter).
    //   input:  none
    //   output: both replica orderings pick the same winning content
    //           ("topic-from-node-b", the higher ts); redelivering the loser after
    //           the winner already applied is confirmed idempotent (no-op, winner
    //           unchanged)
    //   sideEffects: none beyond two throwaway in-memory AppState instances
    // cluster:state_lww_conflict_resolution_deterministic:end
    #[tokio::test]
    async fn state_lww_conflict_resolution_deterministic() {
        use crate::state::StateEvent;

        let room_id = "!lww-room:localhost".to_string();

        // Two concurrent writers set m.room.topic with different timestamps —
        // node-b's write (ts=200) must beat node-a's write (ts=100) everywhere.
        let ev_from_a = StateEvent {
            event_type: "m.room.topic".to_string(),
            state_key: "".to_string(),
            sender: "@alice:node-a".to_string(),
            content: json!({ "topic": "topic-from-node-a" }),
            event_id: "$topic-a".to_string(),
            room_id: room_id.clone(),
            origin_server_ts: 100,
        };
        let ev_from_b = StateEvent {
            event_type: "m.room.topic".to_string(),
            state_key: "".to_string(),
            sender: "@bob:node-b".to_string(),
            content: json!({ "topic": "topic-from-node-b" }),
            event_id: "$topic-b".to_string(),
            room_id: room_id.clone(),
            origin_server_ts: 200,
        };

        // Replica 1: applies A then B.
        let replica1 = AppState::new();
        replica1.ensure_room_state(&room_id);
        let applied1a = replica1
            .apply_remote_state_event(ev_from_a.clone())
            .expect("replica1 apply A");
        let applied1b = replica1
            .apply_remote_state_event(ev_from_b.clone())
            .expect("replica1 apply B");
        assert!(applied1a, "first write to an empty slot always applies");
        assert!(applied1b, "higher-ts write must beat the earlier one");

        // Replica 2: applies B then A (REVERSED order).
        let replica2 = AppState::new();
        replica2.ensure_room_state(&room_id);
        let applied2b = replica2
            .apply_remote_state_event(ev_from_b.clone())
            .expect("replica2 apply B");
        let applied2a = replica2
            .apply_remote_state_event(ev_from_a.clone())
            .expect("replica2 apply A");
        assert!(applied2b, "first write to an empty slot always applies");
        assert!(
            !applied2a,
            "lower-ts write arriving AFTER the higher-ts winner must be a no-op"
        );

        // ── Both replicas must converge to the SAME winning content ────────────
        let topic1 = {
            let rs = replica1.room_state.lock().expect("replica1 room_state");
            rs.get(&room_id)
                .expect("room in replica1")
                .iter()
                .find(|ev| ev.event_type == "m.room.topic")
                .expect("topic in replica1")
                .content
                .get("topic")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string()
        };
        let topic2 = {
            let rs = replica2.room_state.lock().expect("replica2 room_state");
            rs.get(&room_id)
                .expect("room in replica2")
                .iter()
                .find(|ev| ev.event_type == "m.room.topic")
                .expect("topic in replica2")
                .content
                .get("topic")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string()
        };
        assert_eq!(
            topic1, "topic-from-node-b",
            "replica1 must converge on node-b's write (higher ts)"
        );
        assert_eq!(
            topic2, "topic-from-node-b",
            "replica2 must converge on node-b's write (higher ts)"
        );
        assert_eq!(
            topic1, topic2,
            "both replicas must converge on the IDENTICAL winner regardless of arrival order"
        );

        // ── Idempotency: redelivering the already-applied winner is a no-op ─────
        let redelivered = replica1
            .apply_remote_state_event(ev_from_b.clone())
            .expect("replica1 redeliver B");
        assert!(
            !redelivered,
            "redelivering the identical event_id must be a no-op (idempotent)"
        );
    }

    // ── Phase 2: power-level gate over LWW ─────────────────────────────────────

    // pl_room:start
    //   purpose: A room's current state with alice at 100 and everyone else at the
    //            default 0 — the shape createRoom actually produces.
    //   input:  none
    //   output: Vec<StateEvent> usable as `current` for may_set_state
    //   sideEffects: none
    // pl_room:end
    fn pl_room() -> Vec<crate::state::StateEvent> {
        use crate::state::StateEvent;
        vec![StateEvent {
            event_type: "m.room.power_levels".to_string(),
            state_key: "".to_string(),
            sender: "@alice:node-a".to_string(),
            content: json!({
                "users": { "@alice:node-a": 100, "@mod:node-a": 50 },
                "users_default": 0,
                "events": {},
                "events_default": 0,
                "state_default": 50,
                "ban": 50, "kick": 50, "redact": 50, "invite": 50
            }),
            event_id: "$pl".to_string(),
            room_id: "!r:node-a".to_string(),
            origin_server_ts: 1,
        }]
    }

    fn pl_ev(
        event_type: &str,
        state_key: &str,
        sender: &str,
        content: Value,
    ) -> crate::state::StateEvent {
        crate::state::StateEvent {
            event_type: event_type.to_string(),
            state_key: state_key.to_string(),
            sender: sender.to_string(),
            content,
            event_id: format!("${event_type}-{sender}"),
            room_id: "!r:node-a".to_string(),
            origin_server_ts: 1000,
        }
    }

    // power_gate_rules:start
    //   purpose: Pin the receive-side authorisation rules. Before this gate existed,
    //            LWW alone decided, so any node could rewrite any membership or any
    //            power_levels in any room it could reach — the headline hole in
    //            sharing a room with a second operator.
    //   input:  none (pure function)
    //   output: assertions on each rule, including the two that must NOT be blocked
    //   sideEffects: none
    // power_gate_rules:end
    #[test]
    fn power_gate_rules() {
        let room = pl_room();
        let may = |ev: &crate::state::StateEvent| AppState::may_set_state(&room, ev);

        // The hole this closes: a nobody rewriting somebody else's membership.
        assert!(
            !may(&pl_ev(
                "m.room.member",
                "@alice:node-a",
                "@nobody:node-b",
                json!({ "membership": "leave" })
            )),
            "a user at users_default must not be able to kick the room admin"
        );

        // ...but managing your OWN membership has to keep working, or a remote join
        // could never land: a joining user is at users_default, under state_default.
        assert!(
            may(&pl_ev(
                "m.room.member",
                "@nobody:node-b",
                "@nobody:node-b",
                json!({ "membership": "join" })
            )),
            "self-membership is self-service; blocking it would break joining"
        );

        // Ordinary state writes need state_default.
        assert!(
            !may(&pl_ev(
                "m.room.name",
                "",
                "@nobody:node-b",
                json!({ "name": "hijacked" })
            )),
            "a user below state_default must not rename the room"
        );
        assert!(
            may(&pl_ev(
                "m.room.name",
                "",
                "@alice:node-a",
                json!({ "name": "fine" })
            )),
            "the admin may rename it"
        );
        assert!(
            may(&pl_ev(
                "m.room.name",
                "",
                "@mod:node-a",
                json!({ "name": "also fine" })
            )),
            "50 >= state_default 50"
        );

        // Escalation: the check that stops "promote self, then act legitimately".
        assert!(
            !may(&pl_ev(
                "m.room.power_levels",
                "",
                "@mod:node-a",
                json!({ "users": { "@mod:node-a": 100 }, "users_default": 0, "state_default": 50 })
            )),
            "a moderator must not grant anyone a level above its own"
        );
        assert!(
            !may(&pl_ev(
                "m.room.power_levels",
                "",
                "@mod:node-a",
                json!({ "users": { "@mod:node-a": 50 }, "users_default": 100, "state_default": 50 })
            )),
            "nor raise users_default above its own level — the same escalation by \
             another route"
        );
        assert!(
            may(&pl_ev(
                "m.room.power_levels",
                "",
                "@alice:node-a",
                json!({ "users": { "@alice:node-a": 100, "@mod:node-a": 100 }, "users_default": 0, "state_default": 50 })
            )),
            "the admin may promote someone to its own level"
        );

        // Bootstrap: a room whose power_levels this node has not seen yet cannot be
        // judged, and refusing would make the room unreplicable — its own
        // power_levels event would be the first thing turned away.
        assert!(
            AppState::may_set_state(
                &[],
                &pl_ev("m.room.name", "", "@anyone:node-z", json!({ "name": "x" }))
            ),
            "with no power_levels known, the write must be allowed through"
        );

        // A per-event override in `events` beats state_default in both directions.
        let mut relaxed = pl_room();
        relaxed[0].content = json!({
            "users": { "@alice:node-a": 100 },
            "users_default": 0,
            "events": { "m.room.topic": 0 },
            "state_default": 50
        });
        assert!(
            AppState::may_set_state(
                &relaxed,
                &pl_ev("m.room.topic", "", "@nobody:node-b", json!({ "topic": "hi" }))
            ),
            "events[\"m.room.topic\"]=0 must let a default-level user set the topic"
        );
    }

    // power_gate_beats_a_newer_timestamp:start
    //   purpose: Authorisation must be checked BEFORE the LWW compare. A denied event
    //            arriving with a newer clock must not take the slot — otherwise a node
    //            could overwrite anything simply by claiming a later timestamp, which
    //            is precisely the attack LWW invites.
    //   input:  a room with power_levels; an unauthorised write with a much newer ts
    //   output: the write is refused and the existing state is untouched
    //   sideEffects: none beyond a throwaway AppState
    // power_gate_beats_a_newer_timestamp:end
    #[tokio::test]
    async fn power_gate_beats_a_newer_timestamp() {
        let room_id = "!pl-lww:node-a".to_string();
        let state = AppState::new();
        state.ensure_room_state(&room_id);

        for ev in pl_room() {
            let mut ev = ev;
            ev.room_id = room_id.clone();
            state
                .apply_remote_state_event(ev)
                .expect("power_levels bootstraps");
        }
        // A legitimate name, set by the admin.
        let mut good = pl_ev("m.room.name", "", "@alice:node-a", json!({ "name": "real" }));
        good.room_id = room_id.clone();
        good.origin_server_ts = 1000;
        assert!(state.apply_remote_state_event(good).expect("admin write"));

        // An unauthorised overwrite, with a far newer timestamp.
        let mut hijack = pl_ev(
            "m.room.name",
            "",
            "@nobody:node-b",
            json!({ "name": "hijacked" }),
        );
        hijack.room_id = room_id.clone();
        hijack.event_id = "$hijack".to_string();
        hijack.origin_server_ts = 9_999_999;
        assert!(
            !state
                .apply_remote_state_event(hijack)
                .expect("hijack attempt"),
            "a newer timestamp must not buy authority"
        );

        let rs = state.room_state.lock().expect("room_state");
        let name = rs
            .get(&room_id)
            .expect("room")
            .iter()
            .find(|e| e.event_type == "m.room.name")
            .expect("name event");
        assert_eq!(
            name.content["name"], "real",
            "the admin's value must still stand: {:?}",
            name.content
        );
    }

    // hlc_orders_state_writes:start
    //   purpose: The LWW ordering key must be a clock, and it must be one clock. Room
    //            state writes used to carry `stream_pos * 1000` — a node-local event
    //            counter — while createRoom carried the wall clock. Two consequences,
    //            both pinned here: values from different nodes were incomparable (the
    //            busier node won regardless of when anything happened), and a counter
    //            value (~1e4) could never beat a wall-clock one (~1.7e12), so an edit
    //            to state set at room creation applied locally and lost on every peer.
    //   input:  none
    //   output: a later edit outranks room-creation state; the clock advances past a
    //           peer's timestamp; an absurd peer timestamp is refused
    //   sideEffects: none beyond throwaway AppStates
    // hlc_orders_state_writes:end
    #[tokio::test]
    async fn hlc_orders_state_writes() {
        use crate::state::{StateEvent, HLC_MAX_DRIFT_MS};

        let state = AppState::new();

        // Monotonic, and never behind the wall clock.
        let a = state.hlc_now();
        let b = state.hlc_now();
        assert!(b > a, "must be strictly increasing: {a} then {b}");
        assert!(
            a > 1_600_000_000_000,
            "must be wall-clock-scaled, not a counter — got {a}. A counter here is \
             what made edits lose to room-creation state on peers."
        );

        // A peer that is ahead pulls us past it, so our next write is ordered later.
        let peer = b + 10_000;
        state.hlc_observe(peer);
        assert!(
            state.hlc_now() > peer,
            "after observing a peer, our next timestamp must outrank theirs"
        );

        // ...but only within reason. A dead RTC must not freeze ordering for everyone.
        let absurd = crate::state::now_ms() + HLC_MAX_DRIFT_MS * 100;
        state.hlc_observe(absurd);
        assert!(
            state.hlc_now() < absurd,
            "a peer clock far in the future must be ignored, not adopted"
        );

        // The scenario that was actually broken: state set at creation, edited later.
        let room_id = "!hlc:node-a".to_string();
        let replica = AppState::new();
        replica.ensure_room_state(&room_id);
        let creation = StateEvent {
            event_type: "m.room.name".to_string(),
            state_key: "".to_string(),
            sender: "@alice:node-a".to_string(),
            content: json!({ "name": "original" }),
            event_id: "$create".to_string(),
            room_id: room_id.clone(),
            origin_server_ts: replica.hlc_now(),
        };
        assert!(replica
            .apply_remote_state_event(creation)
            .expect("creation applies"));

        let rename = StateEvent {
            event_type: "m.room.name".to_string(),
            state_key: "".to_string(),
            sender: "@alice:node-a".to_string(),
            content: json!({ "name": "renamed" }),
            event_id: "$rename".to_string(),
            room_id: room_id.clone(),
            origin_server_ts: replica.hlc_now(),
        };
        assert!(
            replica
                .apply_remote_state_event(rename)
                .expect("rename applies"),
            "a later edit must outrank the room's own creation state — with the old \
             counter-vs-wall-clock mismatch it could not"
        );

        let rs = replica.room_state.lock().expect("room_state");
        let name = rs
            .get(&room_id)
            .expect("room")
            .iter()
            .find(|e| e.event_type == "m.room.name")
            .expect("name");
        assert_eq!(name.content["name"], "renamed");
    }
}
