// START_AI_HEADER
// MODULE: matrix-hs/src/hub_replic_materialise_test.rs
// PURPOSE: Mesh-free tests of hub_replic::materialise (applied-id set, cursor migration).
// END_AI_HEADER

// Regression for the cursor-position bug in materialise(): ordered() is a
// (depth, ts, id) linearisation, so an event from a peer branch that forks
// BELOW our cursor sorts before it and a `skip(cursor_idx)` pass never applies
// it. Pure RoomLog + disk, no mesh, so it runs in every feature set.
#[cfg(test)]
mod materialise_tests {
    use crate::hub_replic::{hub_room_id, materialise_for_test, HubReplicConfig, HUB_CONTENT_KEY};
    use crate::substrate::matrix_events::Pdu;
    use crate::AppState;
    use serde_json::json;
    use std::path::PathBuf;
    use std::sync::Arc;

    fn tmp(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("mhs_mat_{tag}_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(d.join(".mxstate")).unwrap();
        d
    }

    fn cfg(dir: &PathBuf) -> HubReplicConfig {
        HubReplicConfig {
            hub_dir: dir.clone(),
            node: "me".to_string(),
            poll_ms: 250,
            state_dir: dir.join(".mxstate"),
            readonly: false,
        }
    }

    /// Add one peer append event ("journal.peer.jsonl") with an explicit DAG position.
    fn add(state: &Arc<AppState>, body: &str, prev: &[&str], depth: u64, ts: u64) -> String {
        let content = serde_json::to_vec(&json!({
            "msgtype": "m.text",
            "body": body,
            HUB_CONTENT_KEY: {"kind": "append", "path": "journal.peer.jsonl", "node": "peer", "hash": ""}
        }))
        .unwrap();
        let pdu = Pdu::signed(
            hub_room_id(),
            "@hubd:peer".to_string(),
            "m.room.message".to_string(),
            content,
            prev.iter().map(|s| s.to_string()).collect(),
            depth,
            ts,
            &state.signer,
        );
        let id = pdu.event_id.clone();
        state.rooms.lock().unwrap().entry(hub_room_id()).or_default().add(pdu);
        id
    }

    fn read(dir: &PathBuf) -> String {
        std::fs::read_to_string(dir.join("journal.peer.jsonl")).unwrap_or_default()
    }

    // test:peer_branch_below_cursor_is_materialised_once:start
    //   purpose: A peer event whose depth/ts sorts BEFORE the cursor event must
    //            still reach disk, exactly once, and a further pass must not
    //            re-apply anything.
    // test:peer_branch_below_cursor_is_materialised_once:end
    #[tokio::test]
    async fn peer_branch_below_cursor_is_materialised_once() {
        let dir = tmp("fork");
        let (state, cfg) = (AppState::new(), cfg(&dir));
        let e1 = add(&state, "A\n", &[], 1, 100);
        let e2 = add(&state, "B\n", &[&e1], 2, 200);
        let _e3 = add(&state, "C\n", &[&e2], 3, 300);
        materialise_for_test(&state, &cfg).await;
        assert_eq!(read(&dir), "A\nB\nC\n");

        // Peer restarted and forked off e1: depth 2, ts earlier than e2 ⇒ it
        // sorts below the cursor (e3).
        add(&state, "D\n", &[&e1], 2, 150);
        materialise_for_test(&state, &cfg).await;
        let got = read(&dir);
        assert_eq!(got.matches("D\n").count(), 1, "fork event must be applied exactly once: {got:?}");
        assert_eq!(got.len(), "A\nB\nC\nD\n".len(), "nothing else re-applied: {got:?}");

        materialise_for_test(&state, &cfg).await;
        materialise_for_test(&state, &cfg).await;
        assert_eq!(read(&dir), got, "idempotent: no re-application on later passes");
        let _ = std::fs::remove_dir_all(&dir);
    }

    // test:migration_without_applied_file:start
    //   purpose: A node upgraded from the cursor-only state (no hubrep-applied
    //            file) must treat everything up to its cursor as applied — not
    //            replay the log onto disk a second time — and still pick up
    //            new events.
    // test:migration_without_applied_file:end
    #[tokio::test]
    async fn migration_without_applied_file() {
        let dir = tmp("migr");
        let (state, cfg) = (AppState::new(), cfg(&dir));
        let e1 = add(&state, "A\n", &[], 1, 100);
        let e2 = add(&state, "B\n", &[&e1], 2, 200);
        materialise_for_test(&state, &cfg).await;
        assert_eq!(read(&dir), "A\nB\n");
        // Old binary: only the cursor existed.
        let _ = std::fs::remove_file(dir.join(".mxstate/hubrep-applied"));
        assert_eq!(std::fs::read_to_string(dir.join(".mxstate/hubrep-cursor")).unwrap().trim(), e2);

        add(&state, "E\n", &[&e2], 3, 300);
        materialise_for_test(&state, &cfg).await;
        assert_eq!(read(&dir), "A\nB\nE\n", "only the new event is applied after migration");
        assert!(dir.join(".mxstate/hubrep-applied").exists(), "applied set is persisted after migration");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
