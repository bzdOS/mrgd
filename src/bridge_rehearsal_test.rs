// START_AI_HEADER
// MODULE: matrix-hs/src/bridge_rehearsal_test.rs
// PURPOSE: The idempotency half of the hub-transport rehearsal, as
//          deterministic in-process tests: what the 10 000-block rig measured at
//          scale is pinned here at a size that runs in milliseconds.
//
//          The defect these tests exist for: the ingest cursor is a byte offset
//          written AFTER the chunk is published, and insert_pdu mints event_id from
//          wall-clock ts, so a node killed between the two re-publishes the same
//          blocks under fresh ids.  The grow-set cannot dedup that and every
//          consumer appends them twice.  Measured on the rig: 12 250 duplicate
//          blocks out of 10 000 unique ones.
//
//          Scenarios:
//            1. ingest_publishes_every_block_once: a clean pass turns each block in
//               the file into exactly one room event, in file order.
//            2. lost_cursor_does_not_duplicate: the byte offset is deleted — the
//               state after an ungraceful stop between publishing and persisting it
//               — and the next pass re-reads the whole file. The room must not grow.
//            3. materialised_file_is_identical_after_a_reingest: the bytes a consumer
//               ends up with must be the same whether the producer was interrupted
//               or not. This is the property hubd's MCP tools depend on.
//            4. half_written_trailing_block_is_left_alone: a block hubd is still
//               appending has no trailing newline yet; it must not be ingested, and
//               the next pass must take it once it is complete.
//            5. two_ingest_halves_stay_disjoint: blocks for the same role that arrive
//               in two files (the ingest halves) are both published, each once.
// DEPENDENCIES: serde_json, matrix_hs::{state::AppState, hubd_bridge}
// END_AI_HEADER

#[cfg(test)]
mod tests {
    use crate::hubd_bridge::{
        block_content_id, ensure_queue_room, ingest_role, materialize_role, queue_file_name,
        BridgeConfig, CONTENT_KEY,
    };
    use crate::state::AppState;
    use serde_json::{json, Value};
    use std::collections::{HashMap, HashSet};
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    // rig_dir:start
    //   purpose: A fresh directory tree per test, under the OS temp dir, so no test
    //            reads another test's cursors or journals and nothing points at a
    //            real hub.
    //   input:  name — unique suffix for this test
    //   output: (queues_dir, state_dir) pair the bridge is configured with
    //   sideEffects: creates directories under the temp dir
    // rig_dir:end
    fn rig_dir(name: &str) -> (PathBuf, PathBuf) {
        let base = std::env::temp_dir().join(format!(
            "bridge-rehearsal-{name}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&base);
        let queues = base.join("queues");
        std::fs::create_dir_all(&queues).expect("create queues dir");
        (queues, base.join(".mxstate"))
    }

    // cfg_for:start
    //   purpose: A BridgeConfig wired to the test's directories and a node name.
    //   input:  queues, state_dir, node
    //   output: BridgeConfig
    //   sideEffects: none
    // cfg_for:end
    fn cfg_for(queues: &Path, state_dir: &Path, node: &str) -> BridgeConfig {
        BridgeConfig {
            queues_dir: queues.to_path_buf(),
            state_dir: state_dir.to_path_buf(),
            node: node.to_string(),
            poll_ms: 100,
        }
    }

    // write_blocks:start
    //   purpose: Append blocks to a node's own queue file in hubd's on-disk form.
    //   input:  cfg, from, bodies — one body per block
    //   output: the file's byte length after the append
    //   sideEffects: appends to <queues_dir>/<role>.<node>.queue.md
    // write_blocks:end
    fn write_blocks(cfg: &BridgeConfig, role: &str, bodies: &[&str]) -> u64 {
        let mut text = String::new();
        for (i, body) in bodies.iter().enumerate() {
            text.push_str(&format!(
                "\n## 2026-10-03 17:20 · from {}\nEVENT-{i:04} {body}\n",
                cfg.node
            ));
        }
        let path = cfg.queues_dir.join(queue_file_name(role, &cfg.node));
        std::fs::create_dir_all(&cfg.queues_dir).expect("queues dir");
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .expect("open queue file");
        use std::io::Write;
        f.write_all(text.as_bytes()).expect("append blocks");
        path.metadata().expect("stat queue file").len()
    }

    // room_messages:start
    //   purpose: The queue room's message events, in room order, as parsed content.
    //   input:  state, role
    //   output: Vec<Value> of event contents
    //   sideEffects: none (reads under the rooms lock)
    // room_messages:end
    fn room_messages(state: &Arc<AppState>, role: &str) -> Vec<Value> {
        let room_id = crate::hubd_bridge::queue_room_id(role);
        let rooms = state.rooms.lock().expect("rooms lock");
        // No room yet means nothing has ever been ingested — not a failure.
        let Some(log) = rooms.get(&room_id) else {
            return Vec::new();
        };
        log.ordered()
            .iter()
            .filter(|p| p.kind == "m.room.message")
            .filter_map(|p| serde_json::from_slice::<Value>(&p.content).ok())
            .collect()
    }

    // bodies_in:start
    //   purpose: The bodies of a list of event contents, in order.
    //   input:  events
    //   output: Vec<String>
    //   sideEffects: none
    // bodies_in:end
    fn bodies_in(events: &[Value]) -> Vec<String> {
        events
            .iter()
            .map(|c| c.get("body").and_then(|v| v.as_str()).unwrap_or("").to_string())
            .collect()
    }

    // ingest_publishes_every_block_once:start
    //   purpose: One clean pass publishes each block exactly once, in file order.
    //   input:  none (fresh tree, fresh state)
    //   output: asserts the ingest count and the room contents
    //   sideEffects: writes under the temp dir
    // ingest_publishes_every_block_once:end
    #[tokio::test]
    async fn ingest_publishes_every_block_once() {
        let (queues, state_dir) = rig_dir("clean");
        let cfg = cfg_for(&queues, &state_dir, "rig-a");
        write_blocks(&cfg, "rehearsal", &["one", "two", "three"]);

        let state = Arc::new(AppState::new());
        let mut seen: HashMap<String, HashSet<String>> = HashMap::new();
        let n = ingest_role(&state, &cfg, "rehearsal", &mut seen).await;

        assert_eq!(n, 3, "every block must be ingested");
        let bodies = bodies_in(&room_messages(&state, "rehearsal"));
        assert_eq!(bodies.len(), 3, "room must hold one event per block");
        for (i, expected) in ["EVENT-0000 one", "EVENT-0001 two", "EVENT-0002 three"]
            .iter()
            .enumerate()
        {
            assert_eq!(&bodies[i], expected, "order and body must survive");
        }
    }

    // lost_cursor_does_not_duplicate:start
    //   purpose: The ungraceful-stop case. Deleting the offset file is exactly the
    //            state a kill -9 between publishing and persisting the cursor leaves
    //            behind: the next pass re-reads the file from zero.
    //   input:  none (fresh tree, fresh state)
    //   output: asserts the second pass inserts nothing and the room is unchanged
    //   sideEffects: writes under the temp dir
    // lost_cursor_does_not_duplicate:end
    #[tokio::test]
    async fn lost_cursor_does_not_duplicate() {
        let (queues, state_dir) = rig_dir("lost-cursor");
        let cfg = cfg_for(&queues, &state_dir, "rig-a");
        write_blocks(&cfg, "rehearsal", &["alpha", "beta", "gamma"]);

        let state = Arc::new(AppState::new());
        let mut seen: HashMap<String, HashSet<String>> = HashMap::new();
        assert_eq!(
            ingest_role(&state, &cfg, "rehearsal", &mut seen).await,
            3,
            "first pass ingests everything"
        );
        let after_first = bodies_in(&room_messages(&state, "rehearsal"));

        // The crash: the cursor never reached the disk.
        for entry in std::fs::read_dir(&state_dir).expect("state dir") {
            let path = entry.expect("dir entry").path();
            let _ = std::fs::remove_file(path);
        }

        // A fresh process: an empty cache, and the whole file ahead of it again.
        let mut seen_after: HashMap<String, HashSet<String>> = HashMap::new();
        let again = ingest_role(&state, &cfg, "rehearsal", &mut seen_after).await;

        assert_eq!(again, 0, "a re-read of the same file must publish nothing");
        let after_second = bodies_in(&room_messages(&state, "rehearsal"));
        assert_eq!(
            after_first, after_second,
            "the room must not grow a second copy of the same blocks"
        );
    }

    // materialised_file_is_identical_after_a_reingest:start
    //   purpose: The consumer's bytes must not depend on whether the producer was
    //            interrupted — the property hubd's byte-offset readers rely on.
    //   input:  none (two fresh trees, one producer state each)
    //   output: asserts the two materialised files are byte-identical
    //   sideEffects: writes under the temp dir
    // materialised_file_is_identical_after_a_reingest:end
    #[tokio::test]
    async fn materialised_file_is_identical_after_a_reingest() {
        let bodies = ["a", "b", "c", "d", "e"];

        let mut rendered: Vec<String> = Vec::new();
        for interrupt in [false, true] {
            let (queues, state_dir) = rig_dir(if interrupt { "dup-yes" } else { "dup-no" });
            let cfg = cfg_for(&queues, &state_dir, "rig-a");
            write_blocks(&cfg, "rehearsal", &bodies);

            let state = Arc::new(AppState::new());
            let mut seen: HashMap<String, HashSet<String>> = HashMap::new();
            ingest_role(&state, &cfg, "rehearsal", &mut seen).await;
            if interrupt {
                for entry in std::fs::read_dir(&state_dir).expect("state dir") {
                    let _ = std::fs::remove_file(entry.expect("dir entry").path());
                }
                let mut seen2: HashMap<String, HashSet<String>> = HashMap::new();
                ingest_role(&state, &cfg, "rehearsal", &mut seen2).await;
            }

            // A second node materialises what the room holds, into its own directory.
            let (consumer_queues, consumer_state) =
                rig_dir(if interrupt { "dup-yes-b" } else { "dup-no-b" });
            let ccfg = cfg_for(&consumer_queues, &consumer_state, "rig-b");
            let mut counts: HashMap<PathBuf, (u64, usize)> = HashMap::new();
            ensure_queue_room(&state, "rehearsal");
            let written = materialize_role(&state, &ccfg, "rehearsal", &mut counts);
            assert_eq!(written, bodies.len(), "consumer must write every block");
            let out = consumer_queues.join(queue_file_name("rehearsal", "rig-a"));
            rendered.push(std::fs::read_to_string(&out).expect("read materialised file"));
        }

        assert_eq!(
            rendered[0], rendered[1],
            "an interrupted producer must produce the same bytes as an uninterrupted one"
        );
    }

    // half_written_trailing_block_is_left_alone:start
    //   purpose: A block hubd is still appending has no trailing newline, and the
    //            whole pass is deferred rather than ingesting a half-written body —
    //            one poll of latency for the file, never a torn block. This pins that
    //            coarse guard, because it is the other half of the restart story:
    //            a crash cannot be papered over by a torn read either.
    //   input:  none (fresh tree, fresh state)
    //   output: asserts 0 events while the block is half-written, then 2 once whole
    //   sideEffects: writes under the temp dir
    // half_written_trailing_block_is_left_alone:end
    #[tokio::test]
    async fn half_written_trailing_block_is_left_alone() {
        let (queues, state_dir) = rig_dir("half");
        let cfg = cfg_for(&queues, &state_dir, "rig-a");
        let path = queues.join(queue_file_name("rehearsal", "rig-a"));
        let complete = "\n## 2026-10-03 17:20 · from rig-a\nEVENT-0000 one\n";
        let partial = "\n## 2026-10-03 17:20 · from rig-a\nEVENT-0001 tw";
        std::fs::write(&path, format!("{complete}{partial}")).expect("seed queue file");

        let state = Arc::new(AppState::new());
        let mut seen: HashMap<String, HashSet<String>> = HashMap::new();
        assert_eq!(
            ingest_role(&state, &cfg, "rehearsal", &mut seen).await,
            0,
            "a file ending mid-block is left entirely for the next pass"
        );
        assert!(
            room_messages(&state, "rehearsal").is_empty(),
            "nothing may be ingested from a torn file"
        );

        // hubd finishes the block it was writing.
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .expect("open queue file");
        use std::io::Write;
        f.write_all(b"o\n").expect("finish block");

        let mut seen2: HashMap<String, HashSet<String>> = HashMap::new();
        assert_eq!(
            ingest_role(&state, &cfg, "rehearsal", &mut seen2).await,
            2,
            "once the file is whole, every block is taken exactly once"
        );
        let events = room_messages(&state, "rehearsal");
        assert_eq!(events.len(), 2);
        let second = events[1].get("body").and_then(|v| v.as_str()).unwrap_or("");
        assert_eq!(second, "EVENT-0001 two", "body must be whole");
        assert_eq!(
            events[1].get(CONTENT_KEY).and_then(|q| q.get("from")),
            Some(&json!("rig-a")),
            "the block's own from must survive"
        );
    }

    // two_ingest_halves_stay_disjoint:start
    //   purpose: Blocks for one role that reach the node through two different files
    //            are both published, each exactly once.
    //   input:  none (fresh tree, fresh state)
    //   output: asserts six events, no duplicates
    //   sideEffects: writes under the temp dir
    // two_ingest_halves_stay_disjoint:end
    #[tokio::test]
    async fn two_ingest_halves_stay_disjoint() {
        let (queues, state_dir) = rig_dir("halves");
        let cfg = cfg_for(&queues, &state_dir, "rig-a");
        let other = cfg_for(&queues, &state_dir, "rig-b");
        write_blocks(&cfg, "rehearsal", &["x1", "x2", "x3"]);
        write_blocks(&other, "rehearsal", &["y1", "y2", "y3"]);

        let state = Arc::new(AppState::new());
        let mut seen: HashMap<String, HashSet<String>> = HashMap::new();
        assert_eq!(ingest_role(&state, &cfg, "rehearsal", &mut seen).await, 3);
        assert_eq!(ingest_role(&state, &other, "rehearsal", &mut seen).await, 3);

        let bodies = bodies_in(&room_messages(&state, "rehearsal"));
        assert_eq!(bodies.len(), 6, "both halves, each block once");
        let mut unique = bodies.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(unique.len(), 6, "no block may appear twice");
    }

    // content_id_separates_fields:start
    //   purpose: The id must not be forgeable by moving text across a field boundary
    //            — that is the whole reason the fields are NUL-separated.
    //   input:  none
    //   output: asserts two different field splits produce different ids
    //   sideEffects: none
    // content_id_separates_fields:end
    #[test]
    fn content_id_separates_fields() {
        let a = block_content_id("role", "node", "ts", "from", "body");
        let b = block_content_id("role", "node", "ts", "frombo", "dy");
        assert_ne!(a, b, "field boundaries must not be forgeable");
        let c = block_content_id("role", "node", "ts", "from", "body");
        assert_eq!(a, c, "the same content must always give the same id");
    }
}