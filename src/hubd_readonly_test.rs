// START_AI_HEADER
// MODULE: src/hubd_readonly_test.rs
// PURPOSE: Tests for MATRIX_HS_HUBD_READONLY=1 — the bridge to a LIVE hub that only
//          reads. (a) a node on a filesystem-read-only hub starts and ingests;
//          (b) the hub directory manifest (names, sizes, mtimes, sha256) does not move
//          across ingest cycles; (c) a room message reaches the hub exactly once via
//          the external send command, also across a restart; (d) without the flag the
//          old behaviour (cursors beside the hub, materialise writes) is unchanged;
//          (e) eco-loop: what came out of a hub file never goes back through send.
//          Only tmp copies and a stub command are used — never a real hub.
// END_AI_HEADER

#[cfg(test)]
mod tests {
    use crate::hubd_bridge::*;
    use crate::hub_replic::{self, HubReplicConfig};
    use crate::AppState;
    use serde_json::{json, Value};
    use sha2::{Digest, Sha256};
    use std::collections::{BTreeMap, HashMap};
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    // CONTRACT: Unique per-test temp dir (pid + counter) holding a copy of the hub layout.
    fn tmp(tag: &str) -> PathBuf {
        static N: AtomicU32 = AtomicU32::new(0);
        let p = std::env::temp_dir().join(format!(
            "mrgd-hubro-{tag}-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::SeqCst)
        ));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(p.join("srv/hub/queues")).unwrap();
        std::fs::create_dir_all(p.join("srv/hub/projects")).unwrap();
        std::fs::create_dir_all(p.join("data")).unwrap();
        p
    }

    // CONTRACT: Path of the hub copy inside a test root.
    fn hub(root: &Path) -> PathBuf {
        root.join("srv/hub")
    }

    // seed_hub:start
    //   purpose: Seed a hub copy the way hubd would have left it: a queue file, a journal, a
    //            project card, sections.json.
    //   input:  test root
    //   output: none
    //   sideEffects: writes files under the root hub copy
    // seed_hub:end
    /// Seed a hub copy the way hubd would have left it.
    fn seed_hub(root: &Path) {
        let h = hub(root);
        std::fs::write(
            h.join("queues/sec.Alpha.queue.md"),
            "\n## 2026-08-05 11:30 · from andrey\nrotate the key\n",
        )
        .unwrap();
        std::fs::write(h.join("journal.alpha.jsonl"), "{\"e\":1}\n").unwrap();
        std::fs::write(h.join("projects/p.md"), "# card\n").unwrap();
        std::fs::write(h.join("sections.json"), "{\"next\":1}").unwrap();
    }

    // CONTRACT: Env lookup map pointing the bridge at the test hub, with optional extra overrides.
    fn lookup(root: &Path, extra: &[(&str, String)]) -> HashMap<String, String> {
        let h = hub(root);
        let mut m: HashMap<String, String> = HashMap::new();
        m.insert("MATRIX_HS_HUBD_QUEUES_DIR".into(), h.join("queues").display().to_string());
        m.insert("MATRIX_HS_HUBD_HUB_DIR".into(), h.display().to_string());
        m.insert("MATRIX_HS_HUBD_NODE".into(), "Alpha".into());
        m.insert("MATRIX_HS_HUBD_POLL_MS".into(), "50".into());
        m.insert("MATRIX_HS_DATA_DIR".into(), root.join("data").display().to_string());
        for (k, v) in extra {
            m.insert((*k).to_string(), v.clone());
        }
        m
    }

    // CONTRACT: Env lookup map with MATRIX_HS_HUBD_READONLY=1.
    fn ro_env(root: &Path) -> HashMap<String, String> {
        lookup(root, &[("MATRIX_HS_HUBD_READONLY", "1".into())])
    }

    // CONTRACT: BridgeConfig built from a lookup map; panics when none results.
    fn bcfg(m: &HashMap<String, String>) -> BridgeConfig {
        BridgeConfig::from_lookup(&|k| m.get(k).cloned()).expect("bridge cfg")
    }

    // CONTRACT: HubReplicConfig built from a lookup map; panics when none results.
    fn rcfg(m: &HashMap<String, String>) -> HubReplicConfig {
        HubReplicConfig::from_lookup(&|k| m.get(k).cloned()).expect("replic cfg")
    }

    // manifest:start
    //   purpose: Fingerprint a directory tree: name -> "kind size mtime_ns sha256" for every entry,
    //            directories included.
    //   input:  dir
    //   output: BTreeMap of relative name to fingerprint
    //   sideEffects: reads the tree
    // manifest:end
    /// name -> "kind size mtime_ns sha256" for every entry under `dir`, dirs included.
    fn manifest(dir: &Path) -> BTreeMap<String, String> {
        // CONTRACT: Recursive helper of manifest(); fills the map for one directory level.
        fn walk(base: &Path, cur: &Path, out: &mut BTreeMap<String, String>) {
            use std::os::unix::fs::MetadataExt;
            for e in std::fs::read_dir(cur).unwrap().flatten() {
                let p = e.path();
                let md = std::fs::symlink_metadata(&p).unwrap();
                let rel = p.strip_prefix(base).unwrap().display().to_string();
                if md.is_dir() {
                    out.insert(rel.clone(), format!("dir {} {}", md.mtime(), md.mtime_nsec()));
                    walk(base, &p, out);
                } else {
                    let h = Sha256::digest(std::fs::read(&p).unwrap_or_default());
                    out.insert(
                        rel,
                        format!("file {} {} {} {:x}", md.len(), md.mtime(), md.mtime_nsec(), h),
                    );
                }
            }
        }
        let mut out = BTreeMap::new();
        walk(dir, dir, &mut out);
        out
    }

    /// chmod -R a-w (or chflags uchg when running as root, which ignores modes).
    struct Locked(PathBuf);
    impl Locked {
        // CONTRACT: Lock a path read-only (apply with lock=true) and remember it for unlocking on drop.
        fn new(p: &Path) -> Self {
            Self::apply(p, true);
            Locked(p.to_path_buf())
        }
        // CONTRACT: Lock or unlock recursively: chmod for ordinary users, chflags uchg/nouchg when running as
        //    root.
        fn apply(p: &Path, lock: bool) {
            let root = unsafe { libc::geteuid() } == 0;
            let (prog, args): (&str, Vec<&str>) = match (root, lock) {
                (false, true) => ("chmod", vec!["-R", "a-w"]),
                (false, false) => ("chmod", vec!["-R", "u+w"]),
                (true, true) => ("chflags", vec!["-R", "uchg"]),
                (true, false) => ("chflags", vec!["-R", "nouchg"]),
            };
            let st = std::process::Command::new(prog).args(&args).arg(p).status().expect(prog);
            assert!(st.success(), "{prog} failed");
        }
    }
    impl Drop for Locked {
        // CONTRACT: Unlock the path again so the temp tree can be removed.
        fn drop(&mut self) {
            Self::apply(&self.0, false);
        }
    }

    // CONTRACT: Decoded m.room.message contents of a room in timeline order.
    fn room_msgs(state: &Arc<AppState>, room_id: &str) -> Vec<Value> {
        let rooms = state.rooms.lock().unwrap();
        rooms
            .get(room_id)
            .map(|l| {
                l.ordered()
                    .iter()
                    .filter(|p| p.kind == "m.room.message")
                    .filter_map(|p| serde_json::from_slice(&p.content).ok())
                    .collect()
            })
            .unwrap_or_default()
    }

    // CONTRACT: Poll a condition every 50 ms for up to 10 s; returns its final value.
    async fn wait_until(mut f: impl FnMut() -> bool) -> bool {
        for _ in 0..200 {
            if f() {
                return true;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        f()
    }

    // client_say:start
    //   purpose: Post a client message into a role queue room as a local user, with optional extra
    //            content keys.
    //   input:  state, role, user name, text, extra JSON object
    //   output: none
    //   sideEffects: inserts a PDU through the send route
    // client_say:end
    async fn client_say(state: &Arc<AppState>, role: &str, who: &str, text: &str, extra: Value) {
        let room_id = ensure_queue_room(state, role);
        let mut c = json!({ "msgtype": "m.text", "body": text });
        if let (Some(o), Some(e)) = (c.as_object_mut(), extra.as_object()) {
            for (k, v) in e {
                o.insert(k.clone(), v.clone());
            }
        }
        crate::routes::send::insert_pdu(
            state,
            &room_id,
            format!("@{who}:{}", state.server_name),
            "m.room.message".to_string(),
            serde_json::to_vec(&c).unwrap(),
            json!({}),
        )
        .await
        .expect("insert");
    }

    // stub:start
    //   purpose: Create a stub `hub queue send` script that logs "role|body|from" to a file.
    //   input:  test root
    //   output: (script path, log path)
    //   sideEffects: writes an executable script under the root
    // stub:end
    /// A stub `hub queue send`: logs "role|body|from" to a file under the test dir.
    fn stub(root: &Path) -> (String, PathBuf) {
        use std::os::unix::fs::PermissionsExt;
        let log = root.join("stub.log");
        let sh = root.join("stub.sh");
        std::fs::write(
            &sh,
            format!(
                "#!/bin/sh\n[ \"$3\" = \"--from\" ] || exit 3\nprintf '%s|%s|%s\\n' \"$1\" \"$2\" \"$4\" >> '{}'\n",
                log.display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&sh, std::fs::Permissions::from_mode(0o755)).unwrap();
        (sh.display().to_string(), log)
    }

    // CONTRACT: Lines of the stub log, empty when it does not exist.
    fn log_lines(log: &Path) -> Vec<String> {
        std::fs::read_to_string(log)
            .unwrap_or_default()
            .lines()
            .map(str::to_string)
            .collect()
    }

    // (a) ─────────────────────────────────────────────────────────────────────
    // readonly_node_starts_and_ingests_on_a_read_only_hub:start
    //   purpose: (a) A node on a filesystem-read-only hub starts and ingests; cursors land in the
    //            data dir only.
    // readonly_node_starts_and_ingests_on_a_read_only_hub:end
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn readonly_node_starts_and_ingests_on_a_read_only_hub() {
        let root = tmp("a");
        seed_hub(&root);
        let m = ro_env(&root);
        let (b, r) = (bcfg(&m), rcfg(&m));
        assert!(b.readonly && r.readonly);
        assert!(b.state_dir.starts_with(root.join("data")), "{:?}", b.state_dir);
        assert!(r.state_dir.starts_with(root.join("data")), "{:?}", r.state_dir);

        let _lock = Locked::new(&hub(&root));
        let state = AppState::new();
        assert!(spawn_with(state.clone(), b.clone()));
        assert!(hub_replic::spawn_with(state.clone(), r.clone()));

        let rid = queue_room_id("sec");
        assert!(
            wait_until(|| !room_msgs(&state, &rid).is_empty()
                && !room_msgs(&state, &hub_replic::hub_room_id()).is_empty())
            .await,
            "ingest must work on a read-only hub"
        );
        let msgs = room_msgs(&state, &rid);
        assert_eq!(msgs[0]["body"].as_str(), Some("rotate the key"));
        // cursors landed in the data dir, and only there
        assert!(
            wait_until(|| b.state_dir.join("sec.Alpha.queue.md.offset").exists()).await,
            "queue cursor must be persisted in the data dir"
        );
        assert!(!hub(&root).join(".mxstate").exists());
        assert!(!hub(&root).join("queues/../.mxstate").exists());
    }

    // (b) ─────────────────────────────────────────────────────────────────────
    // hub_manifest_is_identical_after_many_ingest_cycles:start
    //   purpose: (b) The hub directory manifest does not change across many ingest cycles.
    // hub_manifest_is_identical_after_many_ingest_cycles:end
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn hub_manifest_is_identical_after_many_ingest_cycles() {
        let root = tmp("b");
        seed_hub(&root);
        let before = manifest(&root.join("srv"));
        let m = ro_env(&root);
        let state = AppState::new();
        assert!(spawn_with(state.clone(), bcfg(&m)));
        assert!(hub_replic::spawn_with(state.clone(), rcfg(&m)));
        let rid = queue_room_id("sec");
        assert!(wait_until(|| !room_msgs(&state, &rid).is_empty()).await);
        // poll is 50 ms: this is a few dozen further cycles
        tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
        let after = manifest(&root.join("srv"));
        assert_eq!(before, after, "the hub tree (and its parent) must not change at all");
        assert_eq!(room_msgs(&state, &rid).len(), 1, "no duplicate ingest across cycles");
    }

    // (c) ─────────────────────────────────────────────────────────────────────
    // room_message_goes_out_once_through_the_command_and_not_again_after_restart:start
    //   purpose: (c) A client message reaches the send command exactly once, also after a node
    //            restart, and nothing is written into the hub tree.
    // room_message_goes_out_once_through_the_command_and_not_again_after_restart:end
    #[tokio::test]
    async fn room_message_goes_out_once_through_the_command_and_not_again_after_restart() {
        let root = tmp("c");
        let (cmd, log) = stub(&root);
        let m = lookup(
            &root,
            &[("MATRIX_HS_HUBD_READONLY", "1".into()), ("MATRIX_HS_HUBD_SEND_CMD", cmd)],
        );
        let cfg = bcfg(&m);
        assert!(cfg.send_cmd.is_some());
        let state = AppState::new();

        client_say(&state, "sec", "old", "history before the bridge", json!({})).await;
        let mut sent = HashMap::new();
        assert_eq!(send_role(&state, &cfg, "sec", &mut sent), 0, "first pass is a baseline");

        client_say(&state, "sec", "andrey", "reply from the room", json!({})).await;
        assert_eq!(send_role(&state, &cfg, "sec", &mut sent), 1);
        assert_eq!(log_lines(&log), vec!["sec|reply from the room|andrey"]);
        assert_eq!(send_role(&state, &cfg, "sec", &mut sent), 0, "same process: no repeat");

        // node restart: empty in-memory cache, same data dir
        let mut sent2 = HashMap::new();
        assert_eq!(send_role(&state, &cfg, "sec", &mut sent2), 0, "restart: no repeat");
        assert_eq!(log_lines(&log).len(), 1);

        client_say(&state, "sec", "andrey", "second", json!({})).await;
        assert_eq!(send_role(&state, &cfg, "sec", &mut sent2), 1);
        assert_eq!(log_lines(&log).len(), 2);

        // nothing in the hub tree: no queue file was written
        assert_eq!(std::fs::read_dir(hub(&root).join("queues")).unwrap().count(), 0);
    }

    // without_a_send_command_nothing_is_sent_and_nothing_written:start
    //   purpose: (c) With no send command configured nothing is sent and nothing is written.
    // without_a_send_command_nothing_is_sent_and_nothing_written:end
    #[tokio::test]
    async fn without_a_send_command_nothing_is_sent_and_nothing_written() {
        let root = tmp("c2");
        let m = ro_env(&root);
        let cfg = bcfg(&m);
        assert!(cfg.send_cmd.is_none());
        let state = AppState::new();
        let mut sent = HashMap::new();
        assert_eq!(send_role(&state, &cfg, "sec", &mut sent), 0);
        client_say(&state, "sec", "andrey", "nobody listens", json!({})).await;
        assert_eq!(send_role(&state, &cfg, "sec", &mut sent), 0);
        assert!(!cfg.state_dir.exists(), "no cursor either: enabling the command later starts clean");
        assert_eq!(std::fs::read_dir(hub(&root).join("queues")).unwrap().count(), 0);
    }

    // (d) ─────────────────────────────────────────────────────────────────────
    // without_the_flag_behaviour_is_unchanged:start
    //   purpose: (d) Without exactly MATRIX_HS_HUBD_READONLY=1 the old behaviour holds: cursors
    //            beside the hub, peer blocks materialise.
    // without_the_flag_behaviour_is_unchanged:end
    #[tokio::test]
    async fn without_the_flag_behaviour_is_unchanged() {
        let root = tmp("d");
        seed_hub(&root);
        let m = lookup(&root, &[("MATRIX_HS_HUBD_SEND_CMD", "/nonexistent/cmd".into())]);
        let cfg = bcfg(&m);
        assert!(!cfg.readonly);
        assert_eq!(cfg.state_dir, hub(&root).join(".mxstate"), "cursor beside queues/, as before");
        let r = rcfg(&m);
        assert!(!r.readonly);
        assert_eq!(r.state_dir, hub(&root).join(".mxstate"));
        // "0", "true", "" are not the flag
        for v in ["0", "true", ""] {
            let m2 = lookup(&root, &[("MATRIX_HS_HUBD_READONLY", v.into())]);
            assert!(!bcfg(&m2).readonly, "{v:?} must not enable readonly");
        }

        let state = AppState::new();
        let mut seen = HashMap::new();
        assert_eq!(ingest_role(&state, &cfg, "sec", &mut seen).await, 1);
        assert!(cfg.state_dir.join("sec.Alpha.queue.md.offset").exists());

        // a peer's block still materialises into a peer-owned file
        let rid = queue_room_id("sec");
        crate::routes::send::insert_pdu(
            &state,
            &rid,
            "@hubd:x".into(),
            "m.room.message".into(),
            serde_json::to_vec(&json!({"msgtype":"m.text","body":"from beta",
                CONTENT_KEY: {"role":"sec","from":"b","ts":"2026-08-05 12:00","origin":"Beta"}}))
            .unwrap(),
            json!({}),
        )
        .await
        .unwrap();
        let mut counts = HashMap::new();
        assert_eq!(materialize_role(&state, &cfg, "sec", &mut counts), 1);
        assert!(hub(&root).join("queues/sec.Beta.queue.md").exists());
    }

    // readonly_refuses_unsafe_cursor_locations_and_never_materialises:start
    //   purpose: Readonly refuses a missing data dir or one inside the hub, and materialize_role is
    //            inert.
    // readonly_refuses_unsafe_cursor_locations_and_never_materialises:end
    #[tokio::test]
    async fn readonly_refuses_unsafe_cursor_locations_and_never_materialises() {
        let root = tmp("d2");
        // no data dir at all
        let mut m = ro_env(&root);
        m.remove("MATRIX_HS_DATA_DIR");
        assert!(BridgeConfig::from_lookup(&|k| m.get(k).cloned()).is_none());
        assert!(HubReplicConfig::from_lookup(&|k| m.get(k).cloned()).is_none());
        // data dir inside the hub, or beside queues/ in the hub's parent
        for bad in [hub(&root).join("data"), hub(&root), root.join("srv/other")] {
            let mut m = ro_env(&root);
            m.insert("MATRIX_HS_DATA_DIR".into(), bad.display().to_string());
            assert!(BridgeConfig::from_lookup(&|k| m.get(k).cloned()).is_none(), "{bad:?}");
            assert!(HubReplicConfig::from_lookup(&|k| m.get(k).cloned()).is_none(), "{bad:?}");
        }
        // materialize_role itself is inert in readonly
        let cfg = bcfg(&ro_env(&root));
        let state = AppState::new();
        let rid = queue_room_id("sec");
        ensure_queue_room(&state, "sec");
        crate::routes::send::insert_pdu(
            &state, &rid, "@hubd:x".into(), "m.room.message".into(),
            serde_json::to_vec(&json!({"msgtype":"m.text","body":"x",
                CONTENT_KEY: {"role":"sec","from":"b","ts":"2026-08-05 12:00","origin":"Beta"}})).unwrap(),
            json!({}),
        ).await.unwrap();
        let mut counts = HashMap::new();
        assert_eq!(materialize_role(&state, &cfg, "sec", &mut counts), 0);
        assert_eq!(std::fs::read_dir(hub(&root).join("queues")).unwrap().count(), 0);
    }

    // (e) ─────────────────────────────────────────────────────────────────────
    // eco_loop_a_mirrored_hub_block_never_goes_back_through_send:start
    //   purpose: (e) A block mirrored from a hub file or a peer hub, or a forged hub-looking
    //            message, never goes through send; a real client message still does.
    // eco_loop_a_mirrored_hub_block_never_goes_back_through_send:end
    #[tokio::test]
    async fn eco_loop_a_mirrored_hub_block_never_goes_back_through_send() {
        let root = tmp("e");
        let (cmd, log) = stub(&root);
        let m = lookup(
            &root,
            &[("MATRIX_HS_HUBD_READONLY", "1".into()), ("MATRIX_HS_HUBD_SEND_CMD", cmd)],
        );
        let cfg = bcfg(&m);
        let state = AppState::new();
        ensure_queue_room(&state, "sec");
        let mut sent = HashMap::new();
        assert_eq!(send_role(&state, &cfg, "sec", &mut sent), 0); // baseline: empty

        // the hub appends a block (e.g. the very effect of an earlier send) and ingest mirrors it
        std::fs::write(
            hub(&root).join("queues/sec.Alpha.queue.md"),
            "\n## 2026-08-05 11:30 · from andrey\nreply from the room\n",
        )
        .unwrap();
        let mut seen = HashMap::new();
        assert_eq!(ingest_role(&state, &cfg, "sec", &mut seen).await, 1);
        // a peer hub's block replicated here, and a forged hub-looking client message
        crate::routes::send::insert_pdu(
            &state, &queue_room_id("sec"), "@hubd:x".into(), "m.room.message".into(),
            serde_json::to_vec(&json!({"msgtype":"m.text","body":"peer block",
                CONTENT_KEY: {"role":"sec","from":"b","ts":"2026-08-05 12:00","origin":"Beta"}})).unwrap(),
            json!({}),
        ).await.unwrap();
        client_say(&state, "sec", "mallory", "forged", json!({CONTENT_KEY: {"origin":"Alpha","role":"sec"}})).await;

        assert_eq!(send_role(&state, &cfg, "sec", &mut sent), 0, "nothing hub-born may be sent");
        assert!(log_lines(&log).is_empty(), "stub must not have been called: {:?}", log_lines(&log));

        // while a real client message still goes
        client_say(&state, "sec", "andrey", "a genuine reply", json!({})).await;
        assert_eq!(send_role(&state, &cfg, "sec", &mut sent), 1);
        assert_eq!(log_lines(&log), vec!["sec|a genuine reply|andrey"]);
    }
}
