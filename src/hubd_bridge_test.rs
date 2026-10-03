// START_AI_HEADER
// MODULE: src/hubd_bridge_test.rs
// PURPOSE: Tests for the hubd queue bridge. Two groups:
//          (1) pure — block parse/render round-trip, well-known room determinism,
//              origin routing, timestamp formatting;
//          (2) end-to-end over a temp directory — a block written the way hubd's
//              queueSend writes it appears in the room, and a room event from
//              another node appears as a per-host file the MCP tools can read.
//
//          The invariants under test are the ones that break hubd if they slip:
//          files stay append-only (byte offsets), no file is both ingested and
//          materialised on one host (loop prevention), and two nodes seed the same
//          room rather than two.
// DEPENDENCIES: serde_json, mrgd::{hubd_bridge, AppState}
// END_AI_HEADER

#[cfg(test)]
mod tests {
    use crate::hubd_bridge::*;
    use crate::AppState;
    use serde_json::{json, Value};
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    // tmpdir:start
    //   purpose: A unique scratch directory under the OS temp dir. No tempfile dep in
    //            this crate, and these tests want a real path a second "node" can see.
    //   input:  tag — a name fragment
    //   output: created PathBuf
    //   sideEffects: creates a directory
    // tmpdir:end
    fn tmpdir(tag: &str) -> PathBuf {
        static N: AtomicU32 = AtomicU32::new(0);
        let n = N.fetch_add(1, Ordering::SeqCst);
        let p = std::env::temp_dir().join(format!("mrgd-hubd-{tag}-{}-{n}", std::process::id()));
        std::fs::create_dir_all(p.join("queues")).expect("mkdir");
        p
    }

    fn cfg_for(root: &Path, node: &str) -> BridgeConfig {
        BridgeConfig {
            queues_dir: root.join("queues"),
            state_dir: root.join(".mxstate"),
            node: node.to_string(),
            poll_ms: 1000,
        }
    }

    // Exactly what hubd's queueSend writes: leading \n, "## <ts> · from <who>", body, \n.
    fn hubd_send(root: &Path, role: &str, node: &str, ts: &str, from: &str, text: &str) {
        use std::io::Write as _;
        let p = root.join("queues").join(format!("{role}.{node}.queue.md"));
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&p)
            .expect("open queue file");
        writeln!(f, "\n## {ts} · from {from}\n{text}").expect("append");
    }

    // ── Pure: block format ───────────────────────────────────────────────────

    // block_roundtrip_is_byte_identical:start
    //   purpose: A block rendered back out must match what hubd wrote, byte for byte.
    //            Anything else and a block that travels through a room comes home
    //            altered, which is visible to every consumer downstream.
    // block_roundtrip_is_byte_identical:end
    #[test]
    fn block_roundtrip_is_byte_identical() {
        let original = "\n## 2026-08-05 11:30 · from alpha\nship it\n";
        let blocks = parse_blocks(original);
        assert_eq!(blocks.len(), 1, "one block; got {blocks:?}");
        assert_eq!(blocks[0].ts, "2026-08-05 11:30");
        assert_eq!(blocks[0].from, "alpha");
        assert_eq!(blocks[0].body, "ship it");
        assert_eq!(
            render_block(&blocks[0]),
            original,
            "render must reproduce hubd's bytes exactly"
        );
    }

    // parse_blocks_keeps_multiline_bodies:start
    //   purpose: Queue entries are markdown and routinely span lines; the parser must
    //            not stop at the first blank line.
    // parse_blocks_keeps_multiline_bodies:end
    #[test]
    fn parse_blocks_keeps_multiline_bodies() {
        let text = "\n## 2026-08-05 11:30 · from a\nline one\n\nline two\n\n## 2026-08-05 11:31 · from b\nsecond\n";
        let blocks = parse_blocks(text);
        assert_eq!(blocks.len(), 2, "two blocks; got {blocks:?}");
        assert_eq!(blocks[0].body, "line one\n\nline two");
        assert_eq!(blocks[1].body, "second");
    }

    // parse_blocks_ignores_headings_inside_a_body:start
    //   purpose: A body may itself contain "## ..." markdown. Only a line with hubd's
    //            exact timestamp shape may start a new block, or a message quoting a
    //            queue header would be split into two.
    // parse_blocks_ignores_headings_inside_a_body:end
    #[test]
    fn parse_blocks_ignores_headings_inside_a_body() {
        let text = "\n## 2026-08-05 11:30 · from a\n## Notes · from the docs\nstill the same block\n";
        let blocks = parse_blocks(text);
        assert_eq!(
            blocks.len(),
            1,
            "a heading without a timestamp is body text; got {blocks:?}"
        );
        assert!(
            blocks[0].body.contains("## Notes · from the docs"),
            "the inner heading must survive in the body; got {:?}",
            blocks[0].body
        );
    }

    // parse_blocks_skips_text_before_the_first_header:start
    //   purpose: Resuming from a byte offset hands us a slice that may start mid-block.
    //            That leading text belongs to an entry already consumed and must not
    //            become a headerless block.
    // parse_blocks_skips_text_before_the_first_header:end
    #[test]
    fn parse_blocks_skips_text_before_the_first_header() {
        let text = "tail of a consumed body\n\n## 2026-08-05 11:30 · from a\nnew\n";
        let blocks = parse_blocks(text);
        assert_eq!(blocks.len(), 1, "got {blocks:?}");
        assert_eq!(blocks[0].body, "new");
    }

    // format_ts_ms_matches_hubd_shape:start
    //   purpose: Events with no hubd stamp get one generated; it must be the same
    //            "YYYY-MM-DD HH:MM" shape, or the block we write back is unparseable.
    // format_ts_ms_matches_hubd_shape:end
    #[test]
    fn format_ts_ms_matches_hubd_shape() {
        // 2026-08-05T11:30:00Z
        assert_eq!(format_ts_ms(1_785_929_400_000), "2026-08-05 11:30");
        assert_eq!(format_ts_ms(0), "1970-01-01 00:00");
        // A leap day, which is where a hand-rolled civil-date conversion goes wrong.
        assert_eq!(format_ts_ms(1_709_164_800_000), "2024-02-29 00:00");
        let generated = format_ts_ms(1_785_929_400_000);
        let round = parse_blocks(&format!("\n## {generated} · from x\nbody\n"));
        assert_eq!(
            round.len(),
            1,
            "a generated stamp must parse back as a header; got {round:?}"
        );
    }

    // ── Pure: well-known rooms ───────────────────────────────────────────────

    // room_id_is_node_independent:start
    //   purpose: The whole point of a well-known room: the id depends on the role and
    //            nothing else, so two partitioned nodes seed one room, not two.
    // room_id_is_node_independent:end
    #[test]
    fn room_id_is_node_independent() {
        assert_eq!(queue_room_id("sec"), "!hubd-queue-sec:hubd");
        assert_eq!(queue_room_id("sec"), queue_room_id("sec"));
        assert_ne!(queue_room_id("sec"), queue_room_id("devops"));
        assert_eq!(role_from_room_id("!hubd-queue-sec:hubd"), Some("sec"));
        assert_eq!(role_from_room_id("!room_0_aaaa_x:example.org"), None);
    }

    // role_slug_keeps_odd_roles_distinct:start
    //   purpose: Sanitising a role must not merge two roles into one room. A role
    //            needing rewrite carries a digest of the original.
    // role_slug_keeps_odd_roles_distinct:end
    #[test]
    fn role_slug_keeps_odd_roles_distinct() {
        assert_eq!(role_slug("opencode-gamma"), "opencode-gamma");
        let a = role_slug("a/b");
        let b = role_slug("a:b");
        assert_ne!(a, b, "two roles that sanitize alike must not share a room");
        for s in [&a, &b] {
            assert!(
                s.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'),
                "slug must be keyexpr/localpart safe; got {s}"
            );
        }
    }

    // seed_burst_is_byte_identical_across_nodes:start
    //   purpose: Every field of the seed state must be node-independent, so a
    //            concurrent seed in a partition is an LWW tie on every key instead of
    //            a race one node loses.
    // seed_burst_is_byte_identical_across_nodes:end
    #[test]
    fn seed_burst_is_byte_identical_across_nodes() {
        let a = queue_state_events("sec");
        let b = queue_state_events("sec");
        assert!(!a.is_empty());
        for (x, y) in a.iter().zip(b.iter()) {
            assert_eq!(x.event_id, y.event_id, "event_id must be deterministic");
            assert_eq!(x.sender, y.sender);
            assert_eq!(x.content, y.content);
            assert_eq!(
                x.origin_server_ts, y.origin_server_ts,
                "a wall-clock ts here would start an LWW race between identical events"
            );
            assert_eq!(x.origin_server_ts, 0, "seed ts must be fixed");
        }
    }

    // seed_burst_passes_its_own_power_level_gate:start
    //   purpose: The burst has to survive arriving from a peer in ANY order. If the
    //            creator sat below state_default, power_levels landing first would
    //            make the room reject its own m.room.create — the room would exist
    //            with no create event and no name on every node but the first.
    // seed_burst_passes_its_own_power_level_gate:end
    #[test]
    fn seed_burst_passes_its_own_power_level_gate() {
        let burst = queue_state_events("sec");
        let pl = burst
            .iter()
            .find(|e| e.event_type == "m.room.power_levels")
            .expect("burst has power_levels")
            .clone();

        // Worst case: power_levels is already applied, everything else arrives after.
        let current = vec![pl.clone()];
        for ev in &burst {
            assert!(
                AppState::may_set_state(&current, ev),
                "{} must be allowed once power_levels is in place",
                ev.event_type
            );
        }

        // And a passing stranger must not be able to reshape the room.
        let intruder = crate::state::StateEvent {
            event_type: "m.room.name".to_string(),
            state_key: String::new(),
            sender: "@mallory:example.org".to_string(),
            content: json!({ "name": "hijacked" }),
            event_id: "$x".to_string(),
            room_id: queue_room_id("sec"),
            origin_server_ts: 99_999_999_999,
        };
        assert!(
            !AppState::may_set_state(&current, &intruder),
            "a user at users_default must not rename a machine-managed room"
        );
    }

    // ── Pure: origin routing (this is the loop prevention) ───────────────────

    // origin_routes_file_and_client_events_apart:start
    //   purpose: A block ingested from a file keeps its origin node, so it lands in
    //            that node's file everywhere. A message from a Matrix client gets
    //            "mx-<signer>", a name no ingest half reads — which is what stops it
    //            from bouncing out of the file it was just written into.
    // origin_routes_file_and_client_events_apart:end
    #[test]
    fn origin_routes_file_and_client_events_apart() {
        let bridged = json!({ "body": "hi", "net.hubd.queue": { "origin": "Alpha" } });
        assert_eq!(origin_of(&bridged, "alpha.example"), "Alpha");

        let from_client = json!({ "body": "hi" });
        let o = origin_of(&from_client, "alpha.example");
        assert_eq!(
            o, "mx-alpha-example",
            "a client event must get a synthetic origin, not a real node name"
        );
        assert_ne!(
            o, "Alpha",
            "if a client event could take a node's own origin it would loop"
        );
        assert!(
            !o.contains('.'),
            "an origin with a dot adds a filename segment and hides the file from hubd"
        );
    }

    // block_from_event_preserves_hubd_metadata:start
    //   purpose: A bridged block must come back out with the sender and timestamp
    //            hubd recorded, not the Matrix ones — otherwise every hop rewrites
    //            "from" to the bridge user and the audit trail is lost.
    // block_from_event_preserves_hubd_metadata:end
    #[test]
    fn block_from_event_preserves_hubd_metadata() {
        let content = json!({
            "body": "deploy done",
            "net.hubd.queue": { "from": "devops", "ts": "2026-08-05 09:15", "origin": "gamma" }
        });
        let b = block_from_event(&content, "@hubd:alpha.example", 1_785_929_400_000);
        assert_eq!(b.from, "devops", "hubd's sender must survive the round trip");
        assert_eq!(b.ts, "2026-08-05 09:15", "hubd's stamp must survive");
        assert_eq!(b.body, "deploy done");

        // A client message has neither, so both are derived.
        let plain = json!({ "body": "typed in Element" });
        let b2 = block_from_event(&plain, "@andrey:alpha.example", 1_785_929_400_000);
        assert_eq!(b2.from, "andrey");
        assert_eq!(b2.ts, "2026-08-05 11:30");
    }

    // role_from_file_name_matches_hubd_regex:start
    //   purpose: Role discovery must read filenames the same way queue.mjs does, or
    //            the bridge and the MCP tools disagree about which file is whose.
    // role_from_file_name_matches_hubd_regex:end
    #[test]
    fn role_from_file_name_matches_hubd_regex() {
        assert_eq!(role_from_file_name("sec.Alpha.queue.md").as_deref(), Some("sec"));
        assert_eq!(role_from_file_name("sec.queue.md").as_deref(), Some("sec"));
        assert_eq!(
            role_from_file_name("opencode-gamma.m.queue.md").as_deref(),
            Some("opencode-gamma")
        );
        assert_eq!(role_from_file_name("notes.md"), None);
        assert_eq!(role_from_file_name(".queue.md"), None);
    }

    // ── End to end over a real directory ─────────────────────────────────────

    fn queue_messages(state: &Arc<AppState>, role: &str) -> Vec<Value> {
        let rooms = state.rooms.lock().expect("rooms");
        let Some(log) = rooms.get(&queue_room_id(role)) else {
            return Vec::new();
        };
        log.ordered()
            .iter()
            .filter(|p| p.kind == "m.room.message")
            .filter_map(|p| serde_json::from_slice::<Value>(&p.content).ok())
            .collect()
    }

    // ingested_block_becomes_a_room_message:start
    //   purpose: The north-south half: what hubd appends to this node's own file
    //            shows up in the well-known room, carrying its hubd metadata.
    // ingested_block_becomes_a_room_message:end
    #[tokio::test]
    async fn ingested_block_becomes_a_room_message() {
        let root = tmpdir("ingest");
        let cfg = cfg_for(&root, "Alpha");
        let state = AppState::new();

        hubd_send(&root, "sec", "Alpha", "2026-08-05 11:30", "andrey", "rotate the key");
        let n = ingest_role_for_test(&state, &cfg, "sec").await;
        assert_eq!(n, 1, "one appended block must ingest as one message");

        let msgs = queue_messages(&state, "sec");
        assert_eq!(msgs.len(), 1, "got {msgs:?}");
        assert_eq!(msgs[0]["body"].as_str(), Some("rotate the key"));
        assert_eq!(msgs[0]["net.hubd.queue"]["from"].as_str(), Some("andrey"));
        assert_eq!(msgs[0]["net.hubd.queue"]["origin"].as_str(), Some("Alpha"));
        assert_eq!(msgs[0]["net.hubd.queue"]["role"].as_str(), Some("sec"));

        // The room must be seeded and joinable, not a bare timeline.
        let rs = state.room_state.lock().expect("room_state");
        let evs = rs.get(&queue_room_id("sec")).expect("seeded room state");
        assert!(
            evs.iter().any(|e| e.event_type == "m.room.create"),
            "an ingested queue must seed a real room"
        );
    }

    // ingest_is_incremental_across_passes:start
    //   purpose: The byte offset must persist, and a second pass over an unchanged
    //            file must ingest nothing. Without this a restart replays the file
    //            and duplicates the queue into the room — the replayed blocks get new
    //            depths, so content addressing cannot dedup them.
    // ingest_is_incremental_across_passes:end
    #[tokio::test]
    async fn ingest_is_incremental_across_passes() {
        let root = tmpdir("incremental");
        let cfg = cfg_for(&root, "Alpha");
        let state = AppState::new();

        hubd_send(&root, "sec", "Alpha", "2026-08-05 11:30", "a", "first");
        assert_eq!(ingest_role_for_test(&state, &cfg, "sec").await, 1);
        assert_eq!(
            ingest_role_for_test(&state, &cfg, "sec").await,
            0,
            "an unchanged file must ingest nothing on the next pass"
        );

        hubd_send(&root, "sec", "Alpha", "2026-08-05 11:31", "b", "second");
        assert_eq!(
            ingest_role_for_test(&state, &cfg, "sec").await,
            1,
            "only the newly appended block"
        );
        assert_eq!(queue_messages(&state, "sec").len(), 2);

        // A fresh AppState with the SAME state dir is the restart case.
        let restarted = AppState::new();
        assert_eq!(
            ingest_role_for_test(&restarted, &cfg, "sec").await,
            0,
            "a persisted offset must survive restart, or the queue is replayed whole"
        );
    }

    // half_written_block_waits_for_the_next_pass:start
    //   purpose: hubd appends a whole block in one write, but a reader can still
    //            catch a partial one. Ingesting it would put a truncated body in the
    //            room permanently — a grow-only log has no edit.
    // half_written_block_waits_for_the_next_pass:end
    #[tokio::test]
    async fn half_written_block_waits_for_the_next_pass() {
        use std::io::Write as _;
        let root = tmpdir("partial");
        let cfg = cfg_for(&root, "Alpha");
        let state = AppState::new();

        let p = root.join("queues").join("sec.Alpha.queue.md");
        let mut f = std::fs::File::create(&p).expect("create");
        write!(f, "\n## 2026-08-05 11:30 · from a\nhalf a bo").expect("write");
        drop(f);

        assert_eq!(
            ingest_role_for_test(&state, &cfg, "sec").await,
            0,
            "a chunk with no trailing newline is still being written"
        );

        let mut f = std::fs::OpenOptions::new().append(true).open(&p).expect("open");
        writeln!(f, "dy").expect("write");
        drop(f);

        assert_eq!(ingest_role_for_test(&state, &cfg, "sec").await, 1);
        let msgs = queue_messages(&state, "sec");
        assert_eq!(msgs[0]["body"].as_str(), Some("half a body"));
    }

    // remote_event_materialises_into_a_peer_owned_file:start
    //   purpose: The other half: an event that came from node "gamma" is written on
    //            this node into gamma's file — a file this node never ingests — and
    //            in a shape hubd's reader parses.
    // remote_event_materialises_into_a_peer_owned_file:end
    #[tokio::test]
    async fn remote_event_materialises_into_a_peer_owned_file() {
        let root_a = tmpdir("mat-a");
        let root_b = tmpdir("mat-b");
        let cfg_a = cfg_for(&root_a, "Alpha");
        let cfg_b = cfg_for(&root_b, "gamma");

        // Node "gamma" ingests a block of its own.
        let gamma = AppState::new();
        hubd_send(&root_b, "sec", "gamma", "2026-08-05 12:00", "devops", "deploy done");
        assert_eq!(ingest_role_for_test(&gamma, &cfg_b, "sec").await, 1);

        // The same event reaches node "Alpha" (stand-in for substrate replication).
        let alpha = AppState::new();
        copy_room(&gamma, &alpha, &queue_room_id("sec"));

        let mut counts = Default::default();
        let n = materialize_role_for_test(&alpha, &cfg_a, "sec", &mut counts);
        assert_eq!(n, 1, "the peer's event must materialise");

        let path = root_a.join("queues").join("sec.gamma.queue.md");
        let text = std::fs::read_to_string(&path).expect("gamma's file on Alpha");
        assert_eq!(
            text, "\n## 2026-08-05 12:00 · from devops\ndeploy done\n",
            "the file must be byte-identical to what hubd would have written"
        );

        // Alpha must NOT have written its own file: that one is hubd's to write, and
        // writing it here is exactly the loop.
        assert!(
            !root_a.join("queues").join("sec.Alpha.queue.md").exists(),
            "materialising into this node's own file would feed the ingest half"
        );

        // Idempotent: a second pass appends nothing.
        assert_eq!(
            materialize_role_for_test(&alpha, &cfg_a, "sec", &mut counts),
            0,
            "re-materialising must not duplicate blocks"
        );
        assert_eq!(std::fs::read_to_string(&path).expect("reread"), text);
    }

    // own_origin_never_materialises_back_into_the_ingest_half:start
    //   purpose: The loop, directly. A block this node ingested is in the room with
    //            this node's own origin; materialising it would append it back into
    //            the very file the ingest half reads, which would then ingest it
    //            again, forever. Assert the file is untouched and the second ingest
    //            pass finds nothing.
    //
    //            The earlier materialise tests do NOT cover this — their rooms only
    //            ever hold a peer's events, so the own-origin branch is never reached.
    //
    //            Nor does the steady state cover it: when the file and the room agree,
    //            the block-count cursor happens to make the append a no-op anyway. The
    //            guard only earns its keep when the room holds MORE of our own history
    //            than our file does — a trimmed or re-provisioned node whose peers
    //            still have its old events. So that is the case set up here.
    // own_origin_never_materialises_back_into_the_ingest_half:end
    #[tokio::test]
    async fn own_origin_never_materialises_back_into_the_ingest_half() {
        let root = tmpdir("noloop");
        let cfg = cfg_for(&root, "Alpha");
        let state = AppState::new();
        let own = root.join("queues").join("sec.Alpha.queue.md");

        hubd_send(&root, "sec", "Alpha", "2026-08-05 11:30", "andrey", "mine");
        hubd_send(&root, "sec", "Alpha", "2026-08-05 11:31", "andrey", "also mine");
        assert_eq!(ingest_role_for_test(&state, &cfg, "sec").await, 2);

        // The file is trimmed back to one block while the room keeps both — what a
        // node rebuilt from git looks like once catch-up returns its own history.
        std::fs::write(&own, "\n## 2026-08-05 11:30 · from andrey\nmine\n").expect("trim");
        let before = std::fs::read_to_string(&own).expect("own file");

        let mut counts = Default::default();
        assert_eq!(
            materialize_role_for_test(&state, &cfg, "sec", &mut counts),
            0,
            "this node's own history must never be written back into the file it ingests"
        );
        assert_eq!(
            std::fs::read_to_string(&own).expect("reread"),
            before,
            "the ingest half's file must be untouched by materialise"
        );
        assert_eq!(
            queue_messages(&state, "sec").len(),
            2,
            "and the room must not have grown"
        );
    }

    // materialise_never_rewrites_only_appends:start
    //   purpose: hubd's readers track byte offsets. If materialising ever rewrote a
    //            file rather than appending, every live consumer would either replay
    //            it or skip past new content. Assert the prefix is untouched.
    // materialise_never_rewrites_only_appends:end
    #[tokio::test]
    async fn materialise_never_rewrites_only_appends() {
        let root_a = tmpdir("append-a");
        let root_b = tmpdir("append-b");
        let cfg_a = cfg_for(&root_a, "Alpha");
        let cfg_b = cfg_for(&root_b, "gamma");

        let gamma = AppState::new();
        hubd_send(&root_b, "sec", "gamma", "2026-08-05 12:00", "devops", "one");
        ingest_role_for_test(&gamma, &cfg_b, "sec").await;

        let alpha = AppState::new();
        copy_room(&gamma, &alpha, &queue_room_id("sec"));
        let mut counts = Default::default();
        materialize_role_for_test(&alpha, &cfg_a, "sec", &mut counts);

        let path = root_a.join("queues").join("sec.gamma.queue.md");
        let first = std::fs::read_to_string(&path).expect("read");

        // gamma sends a second block; it reaches Alpha.
        hubd_send(&root_b, "sec", "gamma", "2026-08-05 12:05", "devops", "two");
        ingest_role_for_test(&gamma, &cfg_b, "sec").await;
        copy_room(&gamma, &alpha, &queue_room_id("sec"));
        assert_eq!(
            materialize_role_for_test(&alpha, &cfg_a, "sec", &mut counts),
            1,
            "only the new block"
        );

        let second = std::fs::read_to_string(&path).expect("reread");
        assert!(
            second.starts_with(&first),
            "the existing prefix must be untouched — hubd readers hold byte offsets into it"
        );
        assert_eq!(parse_blocks(&second).len(), 2);
    }

    // materialise_treats_the_file_as_its_own_cursor:start
    //   purpose: There is no sidecar cursor: the blocks already in the file are the
    //            cursor. That is what makes the bridge idempotent next to hubd's git
    //            mesh-sync, which may deliver the same blocks first.
    // materialise_treats_the_file_as_its_own_cursor:end
    #[tokio::test]
    async fn materialise_treats_the_file_as_its_own_cursor() {
        let root_a = tmpdir("cursor-a");
        let root_b = tmpdir("cursor-b");
        let cfg_a = cfg_for(&root_a, "Alpha");
        let cfg_b = cfg_for(&root_b, "gamma");

        let gamma = AppState::new();
        hubd_send(&root_b, "sec", "gamma", "2026-08-05 12:00", "devops", "one");
        ingest_role_for_test(&gamma, &cfg_b, "sec").await;
        let alpha = AppState::new();
        copy_room(&gamma, &alpha, &queue_room_id("sec"));

        // Pretend git mesh-sync already delivered gamma's file to Alpha.
        hubd_send(&root_a, "sec", "gamma", "2026-08-05 12:00", "devops", "one");
        let before = std::fs::read_to_string(root_a.join("queues").join("sec.gamma.queue.md"))
            .expect("read");

        let mut counts = Default::default();
        assert_eq!(
            materialize_role_for_test(&alpha, &cfg_a, "sec", &mut counts),
            0,
            "a block git already delivered must not be appended a second time"
        );
        assert_eq!(
            std::fs::read_to_string(root_a.join("queues").join("sec.gamma.queue.md"))
                .expect("reread"),
            before
        );
    }

    // client_message_materialises_but_never_re_ingests:start
    //   purpose: A human typing in Element X must reach hubd's consumers, and must not
    //            come back around. It goes into "mx-<signer>", which no ingest half
    //            reads on any node — including the one it was typed on.
    // client_message_materialises_but_never_re_ingests:end
    #[tokio::test]
    async fn client_message_materialises_but_never_re_ingests() {
        let root = tmpdir("client");
        let cfg = cfg_for(&root, "Alpha");
        let state = AppState::new();
        let room_id = ensure_queue_room(&state, "sec");

        // A message with no hubd metadata — what the CS-API send path produces.
        crate::routes::send::insert_pdu(
            &state,
            &room_id,
            format!("@andrey:{}", state.server_name),
            "m.room.message".to_string(),
            serde_json::to_vec(&json!({ "msgtype": "m.text", "body": "typed in Element" }))
                .expect("encode"),
            json!({}),
        )
        .await
        .expect("insert");

        let mut counts = Default::default();
        assert_eq!(
            materialize_role_for_test(&state, &cfg, "sec", &mut counts),
            1,
            "a client message must reach hubd's consumers"
        );

        // It must land in a file this node does not ingest.
        let own = root.join("queues").join("sec.Alpha.queue.md");
        assert!(!own.exists(), "a client message must not land in the ingest half");
        let names: Vec<String> = std::fs::read_dir(root.join("queues"))
            .expect("readdir")
            .flatten()
            .filter_map(|e| e.file_name().to_str().map(str::to_string))
            .collect();
        let target = names
            .iter()
            .find(|n| n.starts_with("sec.mx-"))
            .unwrap_or_else(|| panic!("expected a sec.mx-*.queue.md; got {names:?}"));
        let text =
            std::fs::read_to_string(root.join("queues").join(target)).expect("read materialised");
        assert!(text.contains("from andrey"), "got {text:?}");
        assert!(text.contains("typed in Element"), "got {text:?}");

        // And the ingest half sees nothing to do — no loop.
        assert_eq!(
            ingest_role_for_test(&state, &cfg, "sec").await,
            0,
            "the ingest half must not pick its own materialised output back up"
        );
        assert_eq!(queue_messages(&state, "sec").len(), 1, "still one message");
    }

    // ── Helpers that reach into the module under test ────────────────────────

    // copy_room:start
    //   purpose: Move a room's PDUs from one AppState to another, standing in for
    //            substrate replication. These tests are about the bridge, not about
    //            Zenoh, which cluster_test.rs already covers end to end.
    //   input:  from, to, room_id
    //   output: none
    //   sideEffects: inserts PDUs into `to`
    // copy_room:end
    fn copy_room(from: &Arc<AppState>, to: &Arc<AppState>, room_id: &str) {
        to.ensure_room(room_id);
        let src = from.rooms.lock().expect("src rooms");
        let Some(log) = src.get(room_id) else { return };
        let pdus: Vec<_> = log.ordered().into_iter().cloned().collect();
        drop(src);
        let mut dst = to.rooms.lock().expect("dst rooms");
        let target = dst.entry(room_id.to_string()).or_default();
        for p in pdus {
            target.add(p);
        }
    }

    async fn ingest_role_for_test(
        state: &Arc<AppState>,
        cfg: &BridgeConfig,
        role: &str,
    ) -> usize {
        // A fresh content-id cache per call, exactly like a process that has just
        // started: the cache is an optimisation, and the dedup itself comes from
        // scanning the room, so every call must stand on its own.
        let mut seen = std::collections::HashMap::new();
        crate::hubd_bridge::ingest_role(state, cfg, role, &mut seen).await
    }

    fn materialize_role_for_test(
        state: &Arc<AppState>,
        cfg: &BridgeConfig,
        role: &str,
        counts: &mut std::collections::HashMap<PathBuf, (u64, usize)>,
    ) -> usize {
        crate::hubd_bridge::materialize_role(state, cfg, role, counts)
    }
}
