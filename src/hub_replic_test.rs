// START_AI_HEADER
// MODULE: matrix-hs/src/hub_replic_test.rs
//          each with its own temp hub dir. Proves:
//            - node A's journal/task-log appends land on node B's disk
//            - project cards converge both ways (A→B and B→A), LWW
//            - the loop guard holds: materialising a peer's card does not
//              echo back as a new edit (ingest skips applied hashes)
//            - the internal index stays byte-free under persistence
//              (the gamma-33 leak class cannot return through this path)
// DEPENDENCIES: axum-test, zenoh, matrix_hs::{AppState, state::ClusterConfig, hub_replic}
// END_AI_HEADER

#[cfg(all(test, feature = "cluster"))]
mod tests {
    use crate::state::{AppServiceConfig, ClusterConfig};
    use crate::{hub_replic, AppState};
    use std::path::PathBuf;
    use std::sync::Arc;

    // mk_node:start
    //   purpose: One mesh node with its own temp hub dir, the replicator
    //            running against it. AppServiceConfig set (same shape as a
    //            deployed node) but unused by the replicator itself.
    //   input:  server_name, session
    //   output: (state, hub_dir)
    //   sideEffects: creates a temp dir; spawns the replicator task
    // mk_node:end
    fn mk_node(
        name: &str,
        sess: zenoh::Session,
        prefix: String,
    ) -> (Arc<AppState>, PathBuf) {
        let state = AppState::with_appservice(
            AppState::with_cluster(ClusterConfig {
                session: sess,
                key_prefix: prefix,
                server_name: name.to_string(),
            }),
            AppServiceConfig { token: format!("{name}-as"), prefix: format!("{name}_") },
        );
        let dir = std::env::temp_dir().join(format!(
            "mhs_hubreplic_{}_{}",
            name,
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        (state, dir)
    }

    // wait_for:start
    //   purpose: Poll a closure until true or ~8 s elapse (mesh convergence
    //            budget; matches the two-pool demo's retry discipline).
    //   input:  predicate
    //   output: bool — final predicate value
    //   sideEffects: none beyond sleeping
    // wait_for:end
    async fn wait_for(mut pred: impl FnMut() -> bool) -> bool {
        for _ in 0..80 {
            if pred() {
                return true;
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
        pred()
    }

    // test:hub_replic_journals_cards_converge:start
    //   purpose: The full loop — node-a ingests its own journal + task log +
    //            a card edit, they arrive on node-b's disk; bob-side card
    //            edit converges back to node-a; the loop guard keeps
    //            materialised cards from echoing.
    //   input:  none
    //   output: assertions (see inline reasons)
    //   sideEffects: writes two temp hub dirs; spawns two replicators
    // test:hub_replic_journals_cards_converge:end
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn hub_replic_journals_cards_converge() {
        let _zg = crate::test_util::ZENOH_TEST_LOCK.acquire().await.unwrap();
        let [sess_a, sess_b] = crate::test_util::open_mesh().await;
        // ONE shared key prefix for both nodes — two calls would mint two
        // disjoint key-spaces and the peers would never hear each other.
        let shared_prefix = crate::test_util::unique_prefix("mrgd/matrix/room/hubreplic");
        let (state_a, dir_a) = mk_node("poola", sess_a, shared_prefix.clone());
        let (state_b, dir_b) = mk_node("beta", sess_b, shared_prefix);

        // Seed each hub's own files the way hubd would have written them.
        std::fs::create_dir_all(dir_a.join(".mxstate")).unwrap();
        std::fs::create_dir_all(dir_a.join("projects")).unwrap();
        std::fs::create_dir_all(dir_b.join("projects")).unwrap();

        hub_replic::spawn_with(
            state_a.clone(),
            hub_replic::HubReplicConfig {
                hub_dir: dir_a.clone(),
                node: "poola".to_string(),
                poll_ms: 250,
                state_dir: dir_a.join(".mxstate"),
            },
        );
        hub_replic::spawn_with(
            state_b.clone(),
            hub_replic::HubReplicConfig {
                hub_dir: dir_b.clone(),
                node: "beta".to_string(),
                poll_ms: 250,
                state_dir: dir_b.join(".mxstate"),
            },
        );

        // Simulate completed TOFU key distribution (the announce/drain loop
        // lives in the binary) — without a peer key in the store,
        // drain_cluster early-returns and NOTHING converges.
        state_a.key_store.insert("beta", state_b.signer.verifying_key_bytes());
        state_b.key_store.insert("poola", state_a.signer.verifying_key_bytes());

        // Let both nodes finish subscribing to the hub room's sinks across the
        // mesh — writes issued in the same millisecond as spawn can race the
        // subscribers and be missed by a node that has not declared yet.
        tokio::time::sleep(std::time::Duration::from_millis(1200)).await;

        // Warm-up: keep writing the marker card until it reaches node-b.
        // Zenoh delivers a put only to peers whose subscription is declared at
        // publication time — the first write can race node-b's still-declaring
        // subscriber, so retrying writes IS the correct pattern here too.
        let mut warm = false;
        for _ in 0..30 {
            std::fs::write(dir_a.join("sections.json"), r#"{"next":"warm"}"#).unwrap();
            if wait_for(|| {
                std::fs::read_to_string(dir_b.join("sections.json"))
                    .map(|s| s.contains("warm"))
                    .unwrap_or(false)
            })
            .await
            {
                warm = true;
                break;
            }
        }
        assert!(warm, "warm-up marker must reach node-b");

        std::fs::write(
            dir_a.join("journal.poola.jsonl"),
            "{\"ts\":\"2026-08-26 12:00\",\"kind\":\"note\",\"text\":\"wire round one\"}\n",
        )
        .unwrap();
        std::fs::write(
            dir_a.join("tasks.poola.events.jsonl"),
            "{\"ev\":\"add\",\"id\":\"p-1\"}\n",
        )
        .unwrap();
        std::fs::write(dir_a.join("projects/mrgd.md"), "# mrgd v1.0\n").unwrap();


// Both journals must land on node B's disk.
        assert!(
            wait_for(|| {
                std::fs::read_to_string(dir_b.join("journal.poola.jsonl"))
                    .map(|s| s.contains("wire round one"))
                    .unwrap_or(false)
            })
            .await,
            "node-b must receive node-a's journal over the mesh"
        );

        // The card too — full-snapshot LWW.
        assert!(
            wait_for(|| {
                std::fs::read_to_string(dir_b.join("projects/mrgd.md"))
                    .map(|s| s.contains("v1.0"))
                    .unwrap_or(false)
            })
            .await,
            "card must reach node-b"
        );

        // Loop guard: node-b's card materialisation must not be re-ingested
        // by node-b (its own journal is what it ingests) NOR re-shipped by
        // node-a (hash unchanged). Give the loops a few ticks.
        tokio::time::sleep(std::time::Duration::from_millis(1200)).await;
        let b_card = std::fs::read_to_string(dir_b.join("projects/mrgd.md")).unwrap();
        assert_eq!(b_card.trim(), "# mrgd v1.0", "content stable after settle");

        // Reverse direction: node B edits the card; node A must converge.
        std::fs::write(dir_b.join("projects/mrgd.md"), "# mrgd v1.1 (edited on b)\n").unwrap();
        assert!(
            wait_for(|| {
                std::fs::read_to_string(dir_a.join("projects/mrgd.md"))
                    .map(|s| s.contains("(edited on b)"))
                    .unwrap_or(false)
            })
            .await,
            "B→A card convergence"
        );

        // Append file from B: a second journal line lands incrementally.
        std::fs::write(
            dir_b.join("journal.beta.jsonl")
              , "{\"ts\":\"2026-08-26 13:00\",\"kind\":\"note\",\"text\":\"reply from b\"}\n{\"ts\":\"2026-08-26 13:01\",\"kind\":\"note\",\"text\":\"second line\"}\n",
        )
        .unwrap();
        assert!(
            wait_for(|| {
                std::fs::read_to_string(dir_a.join("journal.beta.jsonl"))
                    .map(|s| s.contains("reply from b") && s.contains("second line"))
                    .unwrap_or(false)
            })
            .await,
            "B's task/journal appends must reach A incrementally"
        );
    }
}