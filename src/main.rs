// START_AI_HEADER
// MODULE: matrix-hs/src/main.rs
// PURPOSE: Binary entry-point.  Binds the axum router on the address from
//          MATRIX_HS_LISTEN (default 127.0.0.1:8448, the Matrix default port).
//
//          Persistence: when MATRIX_HS_DATA_DIR is set, build_state calls
//          persist::replay_from_dir to restore rooms/accounts/aliases from the
//          append-only journals before starting the listener.  Unset → pure in-memory.
//          After replay, compact_all is called to collapse duplicate re-appends and
//          keep journal size O(unique events).
//
//          cluster feature: opens a Zenoh session and enables multi-master delta-CRDT
//          sync.  Endpoints (both comma-separated) come from:
//            MATRIX_HS_ZENOH_CONNECT — connect/endpoints (e.g. "tcp/peer-host:7448")
//            MATRIX_HS_ZENOH_LISTEN  — listen/endpoints  (e.g. "tcp/0.0.0.0:7448")
//          A cross-host peer needs one side to LISTEN on a known port and the other to
//          CONNECT to it (multicast/gossip scouting does not cross subnets).  With neither
//          var set the server starts in peer/scouting mode (loopback gossip only).
//          MATRIX_HS_ZENOH_PREFIX controls the key prefix (default "mrgd/matrix/room").
//
//          Distributed barrier (cluster feature) — coordination-free grow-set:
//            Username AND alias uniqueness is implemented WITHOUT a distributed lock or
//            coordinator.  Each node uses GrowSetClaimStore (mrgd::substrate::barrier_growset):
//            local optimistic claim + publish to a Zenoh grow-set
//            (mrgd/coupling/barrier/claims) + deterministic ReconcileDriver resolves the
//            rare concurrent-claim conflict identically on every node (min ts, node_id
//            winner).
//
//            Loser handler dispatches by key prefix:
//              mx:username: → AppState::mark_rename_required (username account flagged)
//              mx:alias:    → AppState::mark_alias_relinquished (alias removed locally)
//
//            The CP path (RoutedClaimStore + BarrierCoordinator) is PARKED (not wired in):
//            the types are compiled and tested but not started here.  It can be re-enabled
//            for the OTK barrier (Policy::Strict) in a future milestone.
//
//            MATRIX_HS_BARRIER_KEY — CRDT key prefix for grow-set transport
//                                    (default "mrgd/coupling/barrier").
//            MATRIX_HS_NODE_ID     — stable node identifier for tiebreaking
//                                    (default: "node-<pid>").
//
//          Catch-up (cluster feature) — Zenoh pub/sub does NOT replay to subscribers
//          that were absent, so anything published while a node was unreachable is
//          lost to it unless it asks. This node both serves and asks, over wildcards:
//            Serving: two queryables, "<prefix>/*/history" (a room's RoomLog delta)
//              and "<prefix>/*/state" (its current state), each answering from live
//              state at query time. One per node, not one per room — a per-room
//              queryable declared at startup could never serve a room created later.
//            Asking: one GET per channel, covering every room every peer has. The
//              room_id is read off each REPLY key, so a room this node has never
//              heard of identifies itself. See catchup_pass.
//            When: once after replay+compaction (startup), and thereafter from a
//              background task whenever the peer set grows or a backstop timer
//              elapses — that is what recovers a node that stayed UP through a
//              partition. Safe to repeat because a pass is idempotent.
//            Startup waits for a peer AND for that peer's signing key before asking:
//              caught-up PDUs are signature-verified, so querying before the key
//              lands recovers rooms with every event in them rejected.
//            Best-effort throughout: no peer, or a timeout, → proceed with local
//              state, no error.
// DEPENDENCIES: tokio, axum, matrix_hs
// END_AI_HEADER

use mrgd::persist::{compact_all, replay_from_dir};
#[cfg(feature = "cluster")]
use mrgd::persist::{compact_room, compact_room_pdumeta};
#[cfg(feature = "cluster")]
use mrgd::state::{ClusterConfig, ClusterState};
use mrgd::{router, AppState};
#[cfg(feature = "cluster")]
use mrgd::substrate::barrier_growset::{GrowSetClaimStore, ReconcileDriver};
#[cfg(feature = "cluster")]
use mrgd::substrate::crdt::ZenohCrdtSink;
#[cfg(feature = "cluster")]
use mrgd::substrate::matrix_events::{delta_from_bytes, delta_to_bytes};
use std::{net::SocketAddr, path::PathBuf};
#[cfg(feature = "cluster")]
use std::{sync::Arc, time::Duration};

#[cfg(target_os = "freebsd")]
mod memprobe;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Allocator statistics on SIGUSR2, when MATRIX_HS_MEMPROBE_LOG is set.
    // Inert otherwise — see src/memprobe.rs. Installed before anything is
    // built or served so the first sample is the startup baseline.
    #[cfg(target_os = "freebsd")]
    memprobe::install();

    let listen: SocketAddr = std::env::var("MATRIX_HS_LISTEN")
        .unwrap_or_else(|_| "127.0.0.1:8448".to_string())
        .parse()?;

    let state = build_state().await?;

    // Load DC++-style Lua server scripts (on_room_visible, ...). Missing dir or
    // no scripts → no-op (server runs fully spec-compliant). Override path with
    // MATRIX_HS_SCRIPTS_DIR. Hot-reload: SIGHUP-friendly via re-calling load_dir.
    // The default is relative to the working directory, so a checkout runs without
    // configuration; set the env var when the scripts live elsewhere.
    let scripts_dir = std::env::var("MATRIX_HS_SCRIPTS_DIR")
        .unwrap_or_else(|_| "scripts".to_string());
    state.scripting.load_dir(&scripts_dir);

    // hubd queue bridge: mirror queues/<role>.<node>.queue.md into well-known rooms
    // and remote rooms back into per-host files. No-op unless
    // MATRIX_HS_HUBD_QUEUES_DIR is set — see src/hubd_bridge.rs.
    mrgd::hubd_bridge::spawn(state.clone());

    // hub state replication: journals, task event logs and project cards ride
    // the mesh in place of git mesh-sync. No-op unless MATRIX_HS_HUBD_HUB_DIR
    // is set — see src/hub_replic.rs.
    mrgd::hub_replic::spawn(state.clone());

    let app = router(state);

    println!("matrix-hs listening on http://{listen}");

    // Periodic RSS/swap self-report (gamma-33): matrix-hs went silent for two
    // days on a node while leaking 20 GiB, and the journal held exactly one
    // line — nothing to correlate a regression against. Ten minutes of
    // quiet-node memory numbers are a few kB of journal and turn "it got slow
    // sometime last week" into a curve. Linux-only path; other OSes skip.
    tokio::spawn(async_loop_report_rss());

    let listener = tokio::net::TcpListener::bind(listen).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

// async_shutdown_signal:start
//   purpose: resolve when the operator asks the process to stop, so it leaves
//            through main() instead of being stopped at the signal.
//   why: a process that has no handler for SIGTERM/SIGINT is killed by the
//        kernel at the signal — nothing the process registered as an exit hook
//        runs, so MALLOC_CONF=stats_print printed no dump at all and a stopped
//        service could never show a final snapshot. This resolves on the first
//        TERM or INT and hands control back to serve(), which then finishes the
//        requests it already accepted.
//   kills: (1) the first TERM or INT wins, so a second Ctrl-C is not a question
//            an unattended stop has to answer; (2) nothing here escalates to
//            SIGKILL — the allocator has to be able to finish its statistics
//            before the process exits, and that is the whole point.
//   exposes: nothing. The future resolves exactly once.
//   todo: the USR2 allocator sampler is a separate handler and is untouched.
async fn shutdown_signal() {
    use tokio::signal::unix::{signal, SignalKind};

    let mut term = match signal(SignalKind::terminate()) {
        Ok(s) => Some(s),
        Err(e) => {
            eprintln!("[matrix-hs] shutdown: SIGTERM handler unavailable: {e}");
            None
        }
    };
    let mut int = match signal(SignalKind::interrupt()) {
        Ok(s) => Some(s),
        Err(e) => {
            eprintln!("[matrix-hs] shutdown: SIGINT handler unavailable: {e}");
            None
        }
    };

    let which = match (term.as_mut(), int.as_mut()) {
        (Some(t), Some(i)) => tokio::select! {
            _ = t.recv() => "SIGTERM",
            _ = i.recv() => "SIGINT",
        },
        (Some(t), None) => {
            t.recv().await;
            "SIGTERM"
        }
        (None, Some(i)) => {
            i.recv().await;
            "SIGINT"
        }
        // Neither signal can be caught: behave as before rather than inventing
        // a shutdown nobody asked for.
        (None, None) => {
            std::future::pending::<()>().await;
            unreachable!()
        }
    };

    println!(
        "[matrix-hs] {which} received — draining accepted requests, then exiting through main()"
    );
}
// async_shutdown_signal:end

// async_loop_report_rss:start
//   purpose: Log VmRSS/VmSwap/VmHWM from /proc/self/status every 10 minutes,
//            forever. Cheap (one small file read per tick) and the only
//            steady-state telemetry this server emits. Best-effort: if
//            /proc is unavailable the loop logs nothing and keeps ticking.
//   input:  none
//   output: none (logs to stderr)
//   sideEffects: none
// async_loop_report_rss:end
async fn async_loop_report_rss() {
    loop {
        if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
            let pick = |key: &str| -> String {
                status
                    .lines()
                    .find(|l| l.starts_with(key))
                    .and_then(|l| l.split_whitespace().nth(1))
                    .map(|v| format!("{v} kB"))
                    .unwrap_or_else(|| "?".to_string())
            };
            eprintln!(
                "[matrix-hs] mem: rss={} swap={} peak={}",
                pick("VmRSS:"),
                pick("VmSwap:"),
                pick("VmHWM:")
            );
        }
        tokio::time::sleep(std::time::Duration::from_secs(600)).await;
    }
}

// build_state:start
//   purpose: Construct AppState, optionally replaying from on-disk journals, and
//            optionally enabling the cluster layer when MATRIX_HS_ZENOH_CONNECT is
//            set (cluster feature only).
//            Without the feature flag or the env var, returns a plain single-node state.
//            After replay (default or cluster), compact_all is called to eliminate
//            duplicate re-appends in the on-disk journals.
//            Cluster-only: after replay+compact, declares the two wildcard catch-up
//            queryables, runs one catch-up pass, and spawns the background re-query
//            task (see the module header).
//   input:  MATRIX_HS_DATA_DIR env  — data directory for journals (optional)
//           MATRIX_HS_ZENOH_CONNECT env — comma-separated connect endpoints (optional)
//           MATRIX_HS_ZENOH_LISTEN  env — comma-separated listen endpoints (optional)
//           MATRIX_HS_ZENOH_PREFIX  env — CRDT key prefix (optional, default "mrgd/matrix/room")
//           MATRIX_HS_BARRIER_KEY env — grow-set key prefix for barrier CRDT transport
//                                       (default "mrgd/coupling/barrier")
//           MATRIX_HS_NODE_ID env  — stable node identifier for tiebreaking (default: "node-<pid>")
//   output: Arc<AppState>
//   sideEffects: may read/write journal files from MATRIX_HS_DATA_DIR;
//                (cluster) opens a zenoh::Session; prints startup info to stdout;
//                (cluster) opens ZenohCrdtSink + GrowSetClaimStore + ReconcileDriver;
//                (cluster) spawns loser handler task;
//                (cluster) declares two wildcard queryables (Box::leaked);
//                (cluster) waits for a peer + its signing key, then runs a catch-up
//                pass, then spawns the mid-life re-query task (Box::leaked)
// build_state:end
async fn build_state() -> Result<std::sync::Arc<AppState>, Box<dyn std::error::Error + Send + Sync>>
{
    let data_dir: Option<PathBuf> = std::env::var("MATRIX_HS_DATA_DIR").ok().map(PathBuf::from);

    #[cfg(feature = "cluster")]
    {
        let prefix = std::env::var("MATRIX_HS_ZENOH_PREFIX")
            .unwrap_or_else(|_| "mrgd/matrix/room".to_string());

        // server_name doubles as this node's signing node_id (AppState::with_cluster
        // uses ClusterConfig.server_name, NOT the env var directly, so that two nodes
        // sharing one process — e.g. cluster_test.rs — can have distinct identities;
        // in this single-node-per-process production binary reading the env once here
        // is equivalent to the previous behaviour).
        let server_name =
            std::env::var("MATRIX_HS_SERVER_NAME").unwrap_or_else(|_| "localhost".to_string());

        // Parse MATRIX_HS_ZENOH_CONNECT="tcp/ip:port,tcp/ip2:port" into Zenoh config.
        // Format: JSON5 array of endpoint strings, e.g. ["tcp/192.0.2.1:7447"].
        // If unset: use default config (peer mode, multicast/gossip scouting).
        // Build a JSON5 array ["ep1","ep2"] from comma-separated "ep1, ep2".
        let to_json5 = |s: &str| -> String {
            let quoted: Vec<String> = s.split(',').map(|e| format!("\"{}\"", e.trim())).collect();
            format!("[{}]", quoted.join(","))
        };
        let connect = std::env::var("MATRIX_HS_ZENOH_CONNECT").ok();
        let listen = std::env::var("MATRIX_HS_ZENOH_LISTEN").ok();
        // MATRIX_HS_ZENOH_SCOUTING=off disables multicast discovery, so the only
        // peers are the ones named above. Three reasons it exists, all measured:
        //   - it is the difference between "these two nodes are meshed the way I
        //     configured" and "they found each other some other way". Without it a
        //     carrier test can pass over a link it was not testing (this is how a
        //     first obfs verification produced a false positive, 2026-08-20).
        //   - on a host that also runs a node, scouting makes the cluster test
        //     suite discover it and ~4 tests fail per run with a shifting set.
        //   - two unrelated clusters on one host TOFU-trust each other's key
        //     announcements (the deployment topology record (kept private) §3).
        // Default stays on: LAN peer mode is ladder rung 1 and depends on it.
        let scouting_off = std::env::var("MATRIX_HS_ZENOH_SCOUTING")
            .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "off" | "0" | "false"))
            .unwrap_or(false);
        // MATRIX_HS_ZENOH_MODE=client switches the session to zenoh CLIENT mode —
        // the topology a ROUTER carrier is built for. Peer-mode sessions behind a
        // router relay unreliably between each other: measured 2026-08-21 on the
        // three-node mesh (Alpha router + beta/delta via ssh -R), a peer-mode
        // publish from one tunnel client reached the direct client but NOT the
        // other tunnel client, both directions, while catch-up QUERIES worked —
        // exactly the "peer-mode routing flakiness" the router's own source
        // names when it says nodes should connect as clients. Default stays
        // peer: LAN peer mode (ladder rung 1) has no router and needs it.
        let client_mode = std::env::var("MATRIX_HS_ZENOH_MODE")
            .map(|v| v.trim().eq_ignore_ascii_case("client"))
            .unwrap_or(false);
        let session = if connect.is_some() || listen.is_some() || scouting_off || client_mode {
            let mut cfg = zenoh::Config::default();
            if client_mode {
                cfg.insert_json5("mode", "\"client\"")
                    .map_err(|e| format!("zenoh mode config: {e}"))?;
            }
            if let Some(c) = connect.as_deref() {
                cfg.insert_json5("connect/endpoints", &to_json5(c))
                    .map_err(|e| format!("zenoh connect config: {e}"))?;
            }
            if let Some(l) = listen.as_deref() {
                cfg.insert_json5("listen/endpoints", &to_json5(l))
                    .map_err(|e| format!("zenoh listen config: {e}"))?;
            }
            if scouting_off {
                cfg.insert_json5("scouting/multicast/enabled", "false")
                    .map_err(|e| format!("zenoh scouting config: {e}"))?;
                cfg.insert_json5("scouting/gossip/enabled", "false")
                    .map_err(|e| format!("zenoh gossip config: {e}"))?;
            }
            println!(
                "cluster mode: connect={connect:?} listen={listen:?} \
                 mode={} scouting={} prefix={prefix}",
                if client_mode { "client" } else { "peer" },
                if scouting_off { "off" } else { "on" }
            );
            zenoh::open(cfg).await?
        } else {
            println!("cluster mode: peer/scouting (no connect/listen env) prefix={prefix}");
            zenoh::open(zenoh::Config::default()).await?
        };

        // ── Coordination-free grow-set barrier ────────────────────────────────
        // No coordinator, no distributed lock.  Each node claims locally and
        // publishes to a Zenoh grow-set.  ReconcileDriver resolves conflicts
        // deterministically (min ts, node_id) and flags the loser.
        //
        // CP path (RoutedClaimStore + BarrierCoordinator) is PARKED — compiled but
        // not started.  Re-enable for OTK/alias barrier (Policy::Strict) in future.
        let barrier_key_prefix = std::env::var("MATRIX_HS_BARRIER_KEY")
            .unwrap_or_else(|_| "mrgd/coupling/barrier".to_string());

        let node_id = std::env::var("MATRIX_HS_NODE_ID")
            .unwrap_or_else(|_| format!("node-{}", std::process::id()));

        // Propagate node_id into MRGD_NODE_ID so observ::node_id() picks it up.
        // set_var before any observ::emit call so the OnceLock reads the right value.
        // Safety: single-threaded at this point in main; no concurrent env readers.
        // SAFETY: called once before tokio tasks are spawned from this path.
        unsafe {
            std::env::set_var("MRGD_NODE_ID", &node_id);
        }

        // Construct the grow-set sink (subscribes to barrier_key_prefix/**).
        let barrier_sink = Arc::new(
            ZenohCrdtSink::new(session.clone(), &barrier_key_prefix)
                .await
                .map_err(|e| format!("barrier grow-set sink: {e}"))?,
        );

        // Observability: emit connected Zenoh peers at startup.
        // session.info().peers_zid().await returns Box<dyn Iterator<Item = ZenohId>>.
        // This must run before session is moved into ClusterConfig.
        if mrgd::substrate::observ::enabled() {
            let peers_iter = session.info().peers_zid().await;
            let mut peer_strs: Vec<String> = Vec::new();
            for zid in peers_iter {
                peer_strs.push(zid.to_string());
            }
            let peers_joined = if peer_strs.is_empty() {
                "(none)".to_string()
            } else {
                peer_strs.join(",")
            };
            mrgd::substrate::observ::emit("zenoh.peers", &[("peers", &peers_joined)]);
        }

        let (growset_store, synced_view) =
            GrowSetClaimStore::new(barrier_sink.clone(), node_id.clone());

        let barrier_store: Arc<dyn mrgd::substrate::barrier::ClaimStore + Send + Sync> =
            Arc::new(growset_store);

        println!(
            "cluster mode: grow-set barrier started \
             node_id={node_id} prefix={barrier_key_prefix}"
        );

        // ── ReconcileDriver — loser handler ───────────────────────────────────
        // Spawns background reconcile loop.  When a loser is detected, sets
        // UserRecord::rename_required=true.  Full rename flow is deferred.
        let (loser_tx, mut loser_rx) =
            tokio::sync::mpsc::unbounded_channel::<mrgd::substrate::barrier_growset::LostClaim>();

        let driver = ReconcileDriver::new(
            barrier_sink,
            synced_view,
            node_id.clone(),
            loser_tx,
            Duration::from_secs(1),
        );
        // Leak the driver join-handle for process lifetime.
        Box::leak(Box::new(driver.spawn()));

        // ── TOFU key distribution (P1.1 internal-task item 4) ──────────────────────────
        // Open the key-announcement sink BEFORE `session` is moved into ClusterConfig
        // below (session.clone() is cheap — Arc<SessionInner> refcount bump).
        let keys_prefix = std::env::var("MATRIX_HS_KEYS_PREFIX")
            .unwrap_or_else(|_| "mrgd/matrix/keys".to_string());
        let keys_sink_base = Arc::new(
            ZenohCrdtSink::new(session.clone(), &keys_prefix)
                .await
                .map_err(|e| format!("key-announcement sink: {e}"))?,
        );

        // Encrypt the key announcements when a PSK is configured: the scope key is
        // derived from it, so a node outside this deployment cannot read what the
        // key sink carries.  Unset (the default) keeps the sink in the clear, which
        // is the pre-existing behaviour.
        let keys_sink: Arc<dyn mrgd::substrate::crdt::CrdtSink + Send + Sync> =
            match std::env::var("MATRIX_HS_KEYS_PSK") {
                Ok(psk) if !psk.trim().is_empty() => Arc::new(
                    mrgd::substrate::encrypted_crdt::EncryptedCrdtSink::new(
                        keys_sink_base,
                        mrgd::substrate::encrypted_crdt::derive_scope_key(psk.trim()),
                    ),
                ),
                _ => keys_sink_base,
            };

        let state = AppState::with_cluster(ClusterConfig {
            session,
            key_prefix: prefix.clone(),
            server_name: server_name.clone(),
        });
        let state = AppState::with_barrier_store(state, barrier_store);

        // Publish this node's own (node_id=server_name, pubkey) once immediately, then
        // periodically re-publish (anti-entropy — Zenoh pub/sub has no replay, so a
        // late-joining peer only learns our key from a re-publish after its subscriber
        // comes up) AND drain incoming announcements, TOFU-inserting each into
        // state.key_store.
        // ⚠ This distribution channel is UNAUTHENTICATED (the announcement itself
        // carries no signature) — it is sound only once the mesh itself is
        // authenticated (mTLS / PSK / out-of-band trust anchor). See internal-task (mesh auth
        // is owner/infra scope, off-limits here). Until then, the sender-binding check
        // in Pdu::verify_sig (mrgd::substrate::matrix_events) limits blast radius: a race-claimed
        // key can only sign PDUs whose sender domain equals the claimed node_id, so it
        // cannot forge senders on OTHER domains.
        let own_announcement = mrgd::substrate::node_auth::encode_key_announcement(
            &server_name,
            &state.signer.verifying_key_bytes(),
        );
        if let Err(e) = keys_sink.publish("announce", own_announcement.clone()) {
            eprintln!("[matrix-hs] key announcement publish failed: {e}");
        }
        {
            let keys_sink_bg = keys_sink.clone();
            let key_store_bg = state.key_store.clone();
            let announcement_bg = own_announcement.clone();
            let bg_task = tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(5));
                loop {
                    interval.tick().await;
                    if let Err(e) = keys_sink_bg.publish("announce", announcement_bg.clone()) {
                        eprintln!("[matrix-hs] key announcement re-publish failed: {e}");
                    }
                    match keys_sink_bg.drain("announce") {
                        Ok(blobs) => {
                            for blob in blobs {
                                match mrgd::substrate::node_auth::decode_key_announcement(&blob) {
                                    Some((peer_node_id, pubkey)) => {
                                        key_store_bg.insert(&peer_node_id, pubkey);
                                    }
                                    None => eprintln!(
                                        "[matrix-hs] key announcement: malformed blob \
                                         ({} bytes) — skipped",
                                        blob.len()
                                    ),
                                }
                            }
                        }
                        Err(e) => eprintln!("[matrix-hs] key announcement drain error: {e}"),
                    }
                }
            });
            // Leak the join-handle for process lifetime (same pattern as the barrier driver).
            Box::leak(Box::new(bg_task));
        }

        // Spawn the loser handler: drains the ReconcileDriver's loser channel and
        // dispatches by key prefix:
        //   mx:username:<localpart>  → apply_username_loss (rename the losing account
        //                              to a deterministic new localpart; record in renamed map;
        //                              whoami/login resolve old tokens to new user_id)
        //   mx:alias:<full_alias>    → mark_alias_relinquished (alias removed locally)
        //   (unknown prefix)         → log and ignore
        // The handler runs for the process lifetime; state is Arc-cloned cheaply.
        {
            let state_for_loser = state.clone();
            tokio::spawn(async move {
                while let Some(lost) = loser_rx.recv().await {
                    eprintln!(
                        "[matrix-hs] grow-set loser: key={:?} \
                         winner={:?} loser={:?}",
                        lost.username, lost.winner_claimant, lost.loser_claimant,
                    );
                    if let Some(localpart) = lost.username.strip_prefix("mx:username:") {
                        // Username conflict: apply deterministic rename to the losing account.
                        // loser_claimant is the full MXID "@<localpart>:<server>".
                        if let Err(e) = state_for_loser.apply_username_loss(
                            localpart,
                            &lost.loser_claimant,
                            &lost.winner_claimant,
                        ) {
                            eprintln!("[matrix-hs] apply_username_loss error: {e}");
                        }
                    } else if let Some(alias) = lost.username.strip_prefix("mx:alias:") {
                        // Alias conflict: relinquish the losing alias locally.
                        if let Err(e) =
                            state_for_loser.mark_alias_relinquished(alias, &lost.winner_claimant)
                        {
                            eprintln!("[matrix-hs] mark_alias_relinquished error: {e}");
                        }
                    } else {
                        eprintln!(
                            "[matrix-hs] grow-set loser: unrecognised key prefix {:?} — ignored",
                            lost.username
                        );
                    }
                }
            });
        }

        if let Some(ref dir) = data_dir {
            println!("persistence: replaying from {}", dir.display());
            replay_from_dir(&state, dir).map_err(|e| format!("replay failed: {e}"))?;
            println!("persistence: compacting journals after replay");
            compact_all(dir);
        }

        // ── Catch-up queryables + startup catch-up ────────────────────────────
        //
        // Two queryables serve peers:
        //   "<prefix>/*/history"  → a room's RoomLog delta (binary, same wire
        //                           format as CRDT delta pub/sub)
        //   "<prefix>/*/state"    → its current state as a StateCatchupMsg
        //
        // ONE queryable each, wildcard over the room segment, answering from
        // whatever this node knows AT QUERY TIME.
        //
        // They used to be declared per room, over the set replayed at startup,
        // each on its own Zenoh session. That had two consequences worth naming,
        // because both looked like "replication is flaky" rather than like bugs:
        //   1. A room created after startup got no queryable at all, so no peer
        //      could ever recover it — including a peer that was simply offline
        //      when the room was created and saw no traffic for it afterwards.
        //   2. N rooms meant 2N+2 Zenoh sessions.
        // A wildcard queryable reads live state per query, which fixes (1), and
        // needs one session for all rooms, which fixes (2).
        // Share the cluster's own session rather than opening a sibling from the same
        // env. A sibling re-applies MATRIX_HS_ZENOH_LISTEN and so tries to bind a port
        // this process already holds — on a listening node that fails with "Address
        // already in use" and the whole catch-up subsystem below is skipped. See
        // ClusterState::session. The queryables are declared allowed_origin(Remote)
        // so a shared session never answers this node's own catch-up queries.
        let qsession = match state.cluster.as_ref() {
            Some(c) => c.session(),
            None => match open_cluster_session().await {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("[matrix-hs] startup: queryable session: {e}; skipping catch-up");
                    return Ok(state);
                }
            },
        };

        // History queryable.
        {
            let hist_wild = format!("{prefix}/*/history");
            let qable = match qsession
                .clone()
                .declare_queryable(&hist_wild)
                .allowed_origin(zenoh::sample::Locality::Remote)
                .await
            {
                Ok(q) => q,
                Err(e) => {
                    eprintln!("[matrix-hs] startup: declare_queryable({hist_wild}): {e}");
                    return Ok(state);
                }
            };
            let handler = qable.handler().clone();
            let state_clone = state.clone();
            let prefix_clone = prefix.clone();
            let qable_task = tokio::spawn(async move {
                while let Ok(query) = handler.recv_async().await {
                    let known: Vec<String> = match state_clone.rooms.lock() {
                        Ok(rooms) => rooms.keys().cloned().collect(),
                        Err(e) => {
                            eprintln!("[matrix-hs] history queryable: rooms lock: {e}");
                            continue;
                        }
                    };
                    let wanted = ClusterState::rooms_for_query(
                        query.key_expr().as_str(),
                        &prefix_clone,
                        "history",
                        known,
                    );
                    for room_id in wanted {
                        let delta_bytes: Vec<u8> = match state_clone.rooms.lock() {
                            Ok(rooms) => rooms
                                .get(&room_id)
                                .map(|log| delta_to_bytes(&log.delta()))
                                .unwrap_or_default(),
                            Err(e) => {
                                eprintln!("[matrix-hs] history queryable: rooms lock: {e}");
                                continue;
                            }
                        };
                        // A known room with no events still replies, with an empty
                        // delta (0 PDUs = 4 zero bytes).
                        let payload = if delta_bytes.is_empty() {
                            vec![0u8; 4]
                        } else {
                            delta_bytes
                        };
                        let reply_key = format!("{prefix_clone}/{room_id}/history");
                        let _ = query.reply(&reply_key, payload).await;
                    }
                }
            });
            // Hold the queryable struct + task for process lifetime by leaking them.
            Box::leak(Box::new(qable));
            Box::leak(Box::new(qable_task));
        }

        // State queryable (Phase 1 P1.1).
        {
            let state_wild = format!("{prefix}/*/state");
            let qable_state = match qsession
                .clone()
                .declare_queryable(&state_wild)
                .allowed_origin(zenoh::sample::Locality::Remote)
                .await
            {
                Ok(q) => q,
                Err(e) => {
                    eprintln!("[matrix-hs] startup: declare_queryable({state_wild}): {e}");
                    return Ok(state);
                }
            };
            let handler_state = qable_state.handler().clone();
            let state_clone = state.clone();
            let prefix_clone = prefix.clone();
            let qable_state_task = tokio::spawn(async move {
                while let Ok(query) = handler_state.recv_async().await {
                    let known: Vec<String> = match state_clone.room_state.lock() {
                        Ok(rs) => rs.keys().cloned().collect(),
                        Err(e) => {
                            eprintln!("[matrix-hs] state queryable: room_state lock: {e}");
                            continue;
                        }
                    };
                    let wanted = ClusterState::rooms_for_query(
                        query.key_expr().as_str(),
                        &prefix_clone,
                        "state",
                        known,
                    );
                    for room_id in wanted {
                        let state_bytes: Vec<u8> = match state_clone.room_state.lock() {
                            Ok(rs) => match rs.get(&room_id) {
                                Some(events) => {
                                    let msg: mrgd::routes::room_state::StateCatchupMsg =
                                        events.into();
                                    serde_json::to_vec(&msg).unwrap_or_default()
                                }
                                None => Vec::new(),
                            },
                            Err(e) => {
                                eprintln!("[matrix-hs] state queryable: room_state lock: {e}");
                                continue;
                            }
                        };
                        let reply_key = format!("{prefix_clone}/{room_id}/state");
                        let _ = query.reply(&reply_key, state_bytes).await;
                    }
                }
            });
            Box::leak(Box::new(qable_state));
            Box::leak(Box::new(qable_state_task));
        }

        // Media queryable: "<prefix>/media/*" → the blob, if this node holds it.
        // Media is never gossiped (blobs would swamp the channel that carries room
        // events); a node that lacks a blob fetches it on the first request that
        // needs it, and that request lands here.
        {
            let media_wild = format!("{prefix}/media/*");
            let qable_media = match qsession
                .clone()
                .declare_queryable(&media_wild)
                .allowed_origin(zenoh::sample::Locality::Remote)
                .await
            {
                Ok(q) => q,
                Err(e) => {
                    eprintln!("[matrix-hs] startup: declare_queryable({media_wild}): {e}");
                    return Ok(state);
                }
            };
            let handler_media = qable_media.handler().clone();
            let state_clone = state.clone();
            let prefix_clone = prefix.clone();
            let qable_media_task = tokio::spawn(async move {
                while let Ok(query) = handler_media.recv_async().await {
                    let qkey = query.key_expr().as_str().to_string();
                    let Some(media_id) = ClusterState::media_from_key(&qkey, &prefix_clone) else {
                        continue;
                    };
                    // Stay SILENT when we do not have it. An empty reply would be
                    // indistinguishable from a real one at the far end, and every
                    // node in the mesh would send one for every miss.
                    let Some(entry) = state_clone.get_media(media_id) else {
                        continue;
                    };
                    let payload = ClusterState::encode_media_reply(
                        &entry.content_type,
                        &entry.owner_node,
                        &entry.bytes,
                    );
                    let reply_key = ClusterState::media_key(&prefix_clone, media_id);
                    let _ = query.reply(&reply_key, payload).await;
                }
            });
            Box::leak(Box::new(qable_media));
            Box::leak(Box::new(qable_media_task));
        }

        // OTK claim queryable: "<prefix>/keys/claim/**" → a claimed one-time-key,
        // if this node owns the targeted device's keys. Mirrors the media
        // queryable above (concrete key, silent on a miss); extracted into
        // routes/keys.rs::serve_otk_claims rather than kept inline like the
        // other three, so a lib test can declare the same queryable directly
        // (this function, build_state, lives in the bin target).
        if let Err(e) = mrgd::routes::keys::serve_otk_claims(state.clone()).await {
            eprintln!("[matrix-hs] startup: serve_otk_claims: {e}");
            return Ok(state);
        }

        // Keep the queryable session open for the process lifetime.
        Box::leak(Box::new(qsession));

        // ── Startup catch-up: one wildcard GET per channel ────────────────────
        // Ask peers for EVERY room they have, instead of asking only about rooms
        // already known locally. That is what makes a node with no local state —
        // fresh, or one that missed a room entirely while offline — converge on
        // startup rather than waiting for live traffic that may never come.
        // The room_id is read off each REPLY key, so an unknown room identifies
        // itself. Timeout 2 s per receive; no reply → proceed.
        let catchup_timeout = Duration::from_secs(2);
        // Same shared session as the queryables above, and for the same reason: a
        // sibling opened from the env re-binds MATRIX_HS_ZENOH_LISTEN and dies with
        // "Address already in use" on any node that listens. Asking over the cluster's
        // own session cannot collide, and the queryables' allowed_origin(Remote) keeps
        // this node from answering its own query.
        let catchup_session = match state.cluster.as_ref() {
            Some(c) => c.session(),
            None => match open_cluster_session().await {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("[matrix-hs] startup: catch-up session: {e}; skipping catch-up");
                    return Ok(state);
                }
            },
        };

        // Peer discovery is asynchronous, and catch-up is the one thing that cannot
        // simply be retried later — it runs once, at startup. On a cold start the
        // session normally has no peers for the first moment, so a GET issued right
        // away matches no queryable anywhere and comes back empty: catch-up would
        // report success having converged nothing. Wait for the mesh before asking.
        //
        // Two things must be true before asking, not one:
        //   a) a peer is connected, otherwise the GET matches nothing;
        //   b) we hold that peer's signing key, because caught-up PDUs are verified
        //      against key_store (apply_delta_verified). The key is announced over
        //      the same mesh and lands asynchronously — query before it arrives and
        //      catch-up recovers the ROOM but rejects every EVENT in it, which reads
        //      in the log as a forgery warning rather than as a race.
        //
        // The two conditions get different deadlines, because they fail differently.
        //   - No peer at all is the ordinary single-node case. Notice it quickly and
        //     stop waiting: there is nothing to catch up from.
        //     (MATRIX_HS_CATCHUP_PEER_WAIT_MS, default 3 s; 0 disables the wait.)
        //   - A peer whose key has not arrived yet is a slow start, not an absence.
        //     Key announcements re-publish on a 5 s interval and we joined after the
        //     peer's first publish, so this legitimately takes longer than one tick;
        //     anything under ~10 s would time out every time.
        //     (MATRIX_HS_CATCHUP_KEY_WAIT_MS, default 12 s.)
        let peer_wait_ms = std::env::var("MATRIX_HS_CATCHUP_PEER_WAIT_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(3000);
        let key_wait_ms = std::env::var("MATRIX_HS_CATCHUP_KEY_WAIT_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(12000);
        if peer_wait_ms > 0 {
            let started = tokio::time::Instant::now();
            let mut peers_seen = false;
            loop {
                if !peers_seen && catchup_session.info().peers_zid().await.count() > 0 {
                    peers_seen = true;
                }
                // Any node_id other than our own means a peer key has landed.
                let have_peer_key = state
                    .key_store
                    .snapshot()
                    .keys()
                    .any(|k| k.as_str() != server_name.as_str());
                if peers_seen && have_peer_key {
                    // Connected and trusted; give queryable declarations a moment to
                    // propagate before querying them.
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    break;
                }
                let waited = started.elapsed().as_millis() as u64;
                if !peers_seen && waited >= peer_wait_ms {
                    println!(
                        "cluster mode: no Zenoh peers after {peer_wait_ms} ms — \
                         nothing to catch up from"
                    );
                    break;
                }
                if peers_seen && waited >= key_wait_ms {
                    println!(
                        "cluster mode: peers found but no peer signing key after \
                         {key_wait_ms} ms — history catch-up will reject their events"
                    );
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }

        let mut startup_stats = mrgd::requery_backoff::CatchupStats::default();
        let converged = catchup_pass(
            &state,
            &catchup_session,
            &prefix,
            data_dir.as_deref(),
            catchup_timeout,
            "startup catch-up",
            &mut startup_stats,
        )
        .await;
        if !converged.is_empty() {
            println!(
                "cluster mode: catch-up complete for {} rooms",
                converged.len()
            );
        }

        // ── Mid-life re-query ─────────────────────────────────────────────────
        // Everything above happens once, at startup. A node that is already UP
        // when a partition heals has no reason to ask again: it resumes seeing
        // live traffic, but nothing replays what it missed while the link was
        // down, so those events are lost to it permanently. Re-run the pass:
        //   - when the peer set GROWS, because a peer (re)appearing is what a
        //     healed partition looks like from this side;
        //   - and on a slow backstop timer, because a link can drop samples
        //     without the transport ever going away, and no peer event fires.
        // Repeating is safe because a pass is idempotent — see catchup_pass.
        {
            let state_bg = state.clone();
            let session_bg = catchup_session.clone();
            let prefix_bg = prefix.clone();
            let dir_bg = data_dir.clone();
            let backstop_secs = std::env::var("MATRIX_HS_CATCHUP_INTERVAL_SECS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(300);
            let settle_ms = std::env::var("MATRIX_HS_CATCHUP_SETTLE_MS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(6000);
            let poll = Duration::from_secs(5);
            let bg_task = tokio::spawn(async move {
                let mut seen_peers: std::collections::HashSet<_> =
                    session_bg.info().peers_zid().await.collect();
                let mut since_backstop = Duration::ZERO;
                // Re-query backoff: a peer whose PDUs are systematically rejected
                // (TOFU key mismatch, bad signature) must not cost a full-history
                // re-query every MATRIX_HS_CATCHUP_INTERVAL_SECS forever.
                let mut backoff =
                    mrgd::requery_backoff::RequeryBackoff::new(backstop_secs);
                let mut stats = mrgd::requery_backoff::CatchupStats::default();
                loop {
                    tokio::time::sleep(poll).await;
                    since_backstop += poll;

                    let peers: std::collections::HashSet<_> =
                        session_bg.info().peers_zid().await.collect();
                    let grew = peers.difference(&seen_peers).next().is_some();
                    seen_peers = peers;

                    let backstop_due = backstop_secs > 0
                        && since_backstop >= Duration::from_secs(backoff.delay_secs());
                    if !grew && !backstop_due {
                        continue;
                    }
                    since_backstop = Duration::ZERO;

                    if grew && settle_ms > 0 {
                        // A peer that just appeared may not have announced its
                        // signing key yet (5 s republish interval). Querying before
                        // it does would reject every PDU it sends back.
                        tokio::time::sleep(Duration::from_millis(settle_ms)).await;
                    }

                    let label = if grew {
                        "re-query (peer appeared)"
                    } else {
                        "re-query (periodic)"
                    };
                    stats.reset();
                    let converged = catchup_pass(
                        &state_bg,
                        &session_bg,
                        &prefix_bg,
                        dir_bg.as_deref(),
                        catchup_timeout,
                        label,
                        &mut stats,
                    )
                    .await;
                    backoff.note_pass(*stats);
                    if backoff.delay_secs() > backstop_secs {
                        println!(
                            "cluster mode: {label}: systematic reject, next re-query in {} s",
                            backoff.delay_secs()
                        );
                    }
                    if !converged.is_empty() {
                        println!(
                            "cluster mode: {label}: {} room(s) answered",
                            converged.len()
                        );
                    }
                }
            });
            // Runs for the process lifetime.
            Box::leak(Box::new(bg_task));
        }

        Ok(state)
    }

    #[cfg(not(feature = "cluster"))]
    {
        if let Some(ref dir) = data_dir {
            println!("persistence enabled: data_dir={}", dir.display());
            let state = AppState::with_data_dir(dir.clone());
            replay_from_dir(&state, dir).map_err(|e| format!("replay failed: {e}"))?;
            println!("persistence: compacting journals after replay");
            compact_all(dir);
            return Ok(state);
        }
        println!("single-node mode (build with --features cluster for multi-master)");
        Ok(AppState::new())
    }
}

// open_cluster_session:start
//   purpose: Open a sibling Zenoh session using the same MATRIX_HS_ZENOH_CONNECT /
//            MATRIX_HS_ZENOH_LISTEN configuration as the main cluster session.
//            The catch-up queryables and the startup catch-up queries each need a
//            session, and the main one was consumed into ClusterState.
//   input:  none (reads MATRIX_HS_ZENOH_CONNECT / MATRIX_HS_ZENOH_LISTEN)
//   output: Result<zenoh::Session, String> — Err carries the Zenoh error as text
//   sideEffects: opens a Zenoh session (caller is responsible for keeping it alive)
// open_cluster_session:end
#[cfg(feature = "cluster")]
async fn open_cluster_session() -> Result<zenoh::Session, String> {
    let to_json5 = |s: &str| -> String {
        let quoted: Vec<String> = s.split(',').map(|e| format!("\"{}\"", e.trim())).collect();
        format!("[{}]", quoted.join(","))
    };
    let mut cfg = zenoh::Config::default();
    if let Ok(c) = std::env::var("MATRIX_HS_ZENOH_CONNECT") {
        let _ = cfg.insert_json5("connect/endpoints", &to_json5(&c));
    }
    if let Ok(l) = std::env::var("MATRIX_HS_ZENOH_LISTEN") {
        let _ = cfg.insert_json5("listen/endpoints", &to_json5(&l));
    }
    zenoh::open(cfg).await.map_err(|e| e.to_string())
}

// catchup_pass:start
//   purpose: One full catch-up round. Asks every peer for every room it has, over
//            both channels ("<prefix>/*/history" and "<prefix>/*/state"), and merges
//            whatever comes back. Each reply carries its room_id in its own key, so
//            a room this node has never heard of identifies itself.
//
//            Idempotent, which is what makes it safe to repeat on a live node:
//            merge_catchup_delta only appends PDUs absent from the RoomLog, so a
//            second pass adds nothing to a retention-capped timeline, and
//            merge_state_catchup resolves by LWW, so a state event that does not win
//            is skipped. Repeat cost is bandwidth, not duplicate events.
//
//   input:  state; session — Zenoh session to query over; prefix — cluster key
//           prefix; data_dir — persistence directory or None; timeout — per-receive
//           budget; label — how this pass names itself in log lines
//   output: the set of room_ids that answered. A room answering does not mean it
//           changed — a pass over already-converged rooms returns them all and
//           merges nothing.
//   sideEffects: two Zenoh queries; mutates RoomLogs, room_timeline and room_state;
//                may append to journals; logs per-room merge results
// catchup_pass:end
#[cfg(feature = "cluster")]
async fn catchup_pass(
    state: &Arc<AppState>,
    session: &zenoh::Session,
    prefix: &str,
    data_dir: Option<&std::path::Path>,
    timeout: Duration,
    label: &str,
    stats: &mut mrgd::requery_backoff::CatchupStats,
) -> std::collections::HashSet<String> {
    let mut converged: std::collections::HashSet<String> = std::collections::HashSet::new();

    // History.
    let hist_wild = format!("{prefix}/*/history");
    match session.get(&hist_wild).timeout(timeout).await {
        Ok(replies) => {
            while let Ok(Ok(reply)) = tokio::time::timeout(timeout, replies.recv_async()).await {
                let sample = match reply.result() {
                    Ok(s) => s,
                    Err(e) => {
                        eprintln!("[matrix-hs] {label}: reply error: {e}");
                        continue;
                    }
                };
                let reply_key = sample.key_expr().as_str().to_string();
                let Some(room_id) = ClusterState::room_from_key(&reply_key, prefix, "history")
                else {
                    eprintln!("[matrix-hs] {label}: unexpected reply key {reply_key}");
                    continue;
                };
                let bytes = sample.payload().to_bytes();
                // A reply we cannot parse is dropped rather than trusted: this is a
                // peer's payload, and delta_from_bytes runs before any signature is
                // checked. The empty-room sentinel (4 zero bytes) parses fine and
                // falls out at the is_empty() check below.
                let Some(delta) = delta_from_bytes(&bytes) else {
                    eprintln!(
                        "[matrix-hs] {label}: malformed delta on {reply_key} ({} bytes), skipped",
                        bytes.len()
                    );
                    continue;
                };
                if delta.pdus.is_empty() {
                    continue;
                }
                // May be a room this node has never heard of: merge_catchup_delta
                // creates the RoomLog and room state for it.
                merge_catchup_delta(state, room_id, &delta, data_dir, stats);
                converged.insert(room_id.to_string());
            }
        }
        Err(e) => eprintln!("[matrix-hs] {label}: GET {hist_wild}: {e}"),
    }

    // State (Phase 1 P1.1).
    let state_wild = format!("{prefix}/*/state");
    match session.get(&state_wild).timeout(timeout).await {
        Ok(replies) => {
            while let Ok(Ok(reply)) = tokio::time::timeout(timeout, replies.recv_async()).await {
                let sample = match reply.result() {
                    Ok(s) => s,
                    Err(e) => {
                        eprintln!("[matrix-hs] {label} (state): reply error: {e}");
                        continue;
                    }
                };
                let reply_key = sample.key_expr().as_str().to_string();
                let Some(room_id) = ClusterState::room_from_key(&reply_key, prefix, "state") else {
                    eprintln!("[matrix-hs] {label} (state): unexpected reply key {reply_key}");
                    continue;
                };
                let bytes = sample.payload().to_bytes();
                if bytes.is_empty() {
                    continue;
                }
                match serde_json::from_slice::<mrgd::routes::room_state::StateCatchupMsg>(&bytes) {
                    Ok(msg) => {
                        merge_state_catchup(state, room_id, msg);
                        converged.insert(room_id.to_string());
                    }
                    Err(e) => {
                        eprintln!("[matrix-hs] {label} (state): parse for {room_id}: {e}")
                    }
                }
            }
        }
        Err(e) => eprintln!("[matrix-hs] {label} (state): GET {state_wild}: {e}"),
    }

    converged
}

// merge_catchup_delta:start
//   purpose: Merge a RoomLogDelta received from a history catch-up query into the
//            local AppState: updates the RoomLog, room_timeline, and (if persistence
//            is enabled) appends new events to the on-disk journal.
//            Only events with event_ids not already in room_timeline are added.
//            Idempotent: calling with the same delta twice is a no-op.
//            P1.1 internal-task: this is a network receive path (a peer's history queryable
//            reply) so the merge uses apply_delta_verified, not apply_delta — every
//            PDU must carry a valid signature from a known signer_node with matching
//            sender-domain binding, or it is rejected and excluded from both the
//            RoomLog AND room_timeline/persistence (a rejected PDU must never surface
//            to clients even via the catch-up path).
//            internal-task: also persists the pdumeta.jsonl sidecar (sig/signer_node/prev_events/
//            depth) for each merged PDU, and keeps it compacted alongside the client-event
//            journal, so a LATER restart of this node replays these catch-up PDUs as
//            verifiable rather than falling back to an unsigned synthetic Pdu.
//   input:  state    — Arc<AppState>;
//           room_id  — the room these PDUs belong to;
//           delta    — RoomLogDelta received from a peer history queryable;
//           data_dir — optional data directory for persistence (None = no-op persist)
//   output: none (errors printed to stderr)
//   sideEffects: mutates state.rooms (verified), room_timeline; may append to room
//                journal + pdumeta sidecar; logs rejected count (observ if enabled,
//                else eprintln)
// merge_catchup_delta:end
#[cfg(feature = "cluster")]
fn merge_catchup_delta(
    state: &std::sync::Arc<AppState>,
    room_id: &str,
    delta: &mrgd::substrate::matrix_events::RoomLogDelta,
    data_dir: Option<&std::path::Path>,
    stats: &mut mrgd::requery_backoff::CatchupStats,
) {
    use std::sync::atomic::Ordering;

    // Collect existing event_ids from room_timeline.
    let known_ids: std::collections::HashSet<String> = {
        match state.room_timeline.lock() {
            Ok(rt) => rt
                .get(room_id)
                .map(|v| {
                    v.iter()
                        .filter_map(|(_, ev)| {
                            ev.get("event_id")
                                .and_then(|id| id.as_str())
                                .map(|s| s.to_string())
                        })
                        .collect()
                })
                .unwrap_or_default(),
            Err(e) => {
                eprintln!("[matrix-hs] catch-up merge: timeline lock: {e}");
                return;
            }
        }
    };

    // Ensure room structures exist.
    state.ensure_room_state(room_id);

    // Merge PDUs into RoomLog (verified — P1.1 internal-task) and collect the new, ACCEPTED ones
    // (not previously known AND actually present in the log after verification — a
    // rejected PDU must never surface to room_timeline/persistence).
    let (new_pdus, rejected): (Vec<&mrgd::substrate::matrix_events::Pdu>, usize) = {
        let before_ids: std::collections::HashSet<String> = {
            match state.rooms.lock() {
                Ok(r) => r
                    .get(room_id)
                    .map(|log| log.ordered().iter().map(|p| p.event_id.clone()).collect())
                    .unwrap_or_default(),
                Err(e) => {
                    eprintln!("[matrix-hs] catch-up merge: rooms lock (before): {e}");
                    return;
                }
            }
        };

        let rejected_count = match state.rooms.lock() {
            Ok(mut rooms) => {
                let log = rooms.entry(room_id.to_string()).or_default();
                let (_accepted, rejected) = log.apply_delta_verified(delta, &state.key_store);
                rejected
            }
            Err(e) => {
                eprintln!("[matrix-hs] catch-up merge: rooms lock (apply): {e}");
                return;
            }
        };

        // Re-check which event_ids are actually present now — verification may have
        // rejected some PDUs from `delta.pdus`, and those must be excluded here too.
        let after_ids: std::collections::HashSet<String> = match state.rooms.lock() {
            Ok(r) => r
                .get(room_id)
                .map(|log| log.ordered().iter().map(|p| p.event_id.clone()).collect())
                .unwrap_or_default(),
            Err(e) => {
                eprintln!("[matrix-hs] catch-up merge: rooms lock (after): {e}");
                return;
            }
        };

        let new_pdus = delta
            .pdus
            .iter()
            .filter(|p| {
                !before_ids.contains(&p.event_id)
                    && !known_ids.contains(&p.event_id)
                    && after_ids.contains(&p.event_id)
            })
            .collect();
        (new_pdus, rejected_count)
    };
    stats.rejected += rejected;
    stats.applied += new_pdus.len();

    if rejected > 0 {
        if mrgd::substrate::observ::enabled() {
            mrgd::substrate::observ::emit(
                "pdu.verify_reject",
                &[
                    ("room_id", room_id),
                    ("path", "catchup"),
                    ("rejected", &rejected.to_string()),
                ],
            );
        } else {
            eprintln!(
                "[matrix-hs] catch-up merge room={room_id}: rejected={rejected} PDU(s) \
                 failed signature/sender-binding verification"
            );
        }
    }

    if new_pdus.is_empty() {
        return;
    }

    // Add new PDUs to room_timeline and optionally persist them.
    let mut rt = match state.room_timeline.lock() {
        Ok(g) => g,
        Err(e) => {
            eprintln!("[matrix-hs] catch-up merge: timeline lock (write): {e}");
            return;
        }
    };
    let timeline_vec = rt.entry(room_id.to_string()).or_default();

    // Redactions are recorded after this lock is dropped — read paths take
    // room_timeline then redactions, so taking them the other way round here could
    // deadlock.
    let mut arrived_redactions: Vec<(String, serde_json::Value)> = Vec::new();

    for pdu in &new_pdus {
        let pos = state.stream_pos.fetch_add(1, Ordering::SeqCst);
        let content_val: serde_json::Value =
            serde_json::from_slice(&pdu.content).unwrap_or_else(|_| serde_json::json!({}));
        let mut ev = serde_json::json!({
            "event_id":         pdu.event_id,
            "type":             pdu.kind,
            "sender":           pdu.sender,
            "room_id":          pdu.room_id,
            "origin_server_ts": pdu.ts,
            "content":          content_val
        });
        // A caught-up redaction names its target in content; lift it and remember it.
        if let Some(target) = AppState::redaction_target(&mut ev) {
            arrived_redactions.push((target, ev.clone()));
        }
        // Persist new event to journal (best-effort).
        state.persist_room_event(room_id, &ev);
        // internal-task: persist the signed-PDU meta sidecar for this catch-up PDU too, so a
        // later restart of THIS node replays it as verifiable rather than falling back
        // to an unsigned synthetic Pdu.
        state.persist_room_pdu_meta(
            room_id,
            &pdu.event_id,
            &pdu.sig,
            &pdu.signer_node,
            &pdu.prev_events,
            pdu.depth,
            &pdu.content,
        );
        timeline_vec.push((pos, ev));
    }

    // Phase 1 GC: apply retention cap to the batch just appended.
    let cap = state.timeline_max_events;
    if cap > 0 && timeline_vec.len() > cap {
        timeline_vec.drain(0..timeline_vec.len() - cap);
    }

    // Notify any waiters (long-poll sync).
    state.notify.notify_waiters();

    // If we added events, compact the room journal so the newly-merged events
    // are deduplicated on disk immediately.
    if let Some(dir) = data_dir {
        let events: Vec<serde_json::Value> =
            timeline_vec.iter().map(|(_, ev)| ev.clone()).collect();
        drop(rt); // release lock before compact_room
        compact_room(dir, room_id, &events);
        // internal-task: keep the pdumeta sidecar in sync with the compacted timeline.
        let ids_kept: std::collections::HashSet<String> = events
            .iter()
            .filter_map(|ev| {
                ev.get("event_id")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            })
            .collect();
        compact_room_pdumeta(dir, room_id, &ids_kept);
    } else {
        drop(rt);
    }

    // Phase 1 GC: cap the RoomLog after a catch-up merge too — a node that has been
    // away can otherwise pull back far more history than its cap allows.
    state.collect_room_log(room_id);

    // Timeline lock released — safe to take the redactions lock now.
    for (target, redaction_ev) in arrived_redactions {
        if let Err(e) = state.mark_redacted(&target, redaction_ev) {
            eprintln!("[matrix-hs] caught-up redaction of {target}: {e}");
        }
    }
}

// merge_state_catchup:start
//   purpose: Merge state events received from a state catch-up query into the
//            local AppState via apply_remote_state_event (LWW convergence).
//            Phase 1 P1.1: network receive path for late-joining/offline nodes
//            to recover current state of rooms missed while disconnected.
//   input:  state — Arc<AppState>; room_id — room id; msg — StateCatchupMsg
//   output: none (errors printed to stderr)
//   sideEffects: mutates room_state (via apply_remote_state_event); logs counts
// merge_state_catchup:end
#[cfg(feature = "cluster")]
fn merge_state_catchup(
    state: &std::sync::Arc<AppState>,
    room_id: &str,
    msg: mrgd::routes::room_state::StateCatchupMsg,
) {
    let mut applied = 0usize;
    let mut skipped = 0usize;

    for ev in msg.into_events() {
        match state.apply_remote_state_event(ev) {
            Ok(true) => applied += 1,
            Ok(false) => skipped += 1,
            Err(e) => eprintln!("[matrix-hs] state catch-up error for {room_id}: {e}"),
        }
    }

    if applied > 0 || skipped > 0 {
        eprintln!("[matrix-hs] state catch-up for {room_id}: applied {applied}, skipped {skipped}");
    }
}
