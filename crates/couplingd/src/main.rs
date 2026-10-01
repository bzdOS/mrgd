// START_AI_HEADER
// MODULE: couplingd/src/main.rs
// PURPOSE: bsdOS coupling-store daemon entry point — Ярус 2, SPEC_coupling_v1 §3.
//          Resolves socket path from COUPLINGD_SOCK env, binds a UnixListener,
//          constructs Stores, loads coupling jail declarations from COUPLINGD_JAILS_DIR,
//          and delegates to server::serve_ext.
//          All dispatch + cascade logic lives in server.rs; this file owns only
//          socket lifecycle, daemon startup, and jail-config loading.
//
//          Jail loading (M2.5):
//            COUPLINGD_JAILS_DIR — directory of *.coupling files (jail.conf stanza format).
//            Each .coupling file is parsed via jailspec::parse_block.
//            The filename stem is used as the fallback jail name when "name =" is absent.
//            On parse error: log warning and skip (daemon must not refuse to start).
//            If the env var is unset or the dir is missing/empty: zero jails — no-op reconcile.
//
//          JailManager selection:
//            On FreeBSD (cfg target_os = "freebsd"): FreeBsdJailManager.
//            On host (Linux/macOS): MemJailManager (safe, no OS calls).
//
// INTENT: M2.5 — ReconcileLoop wired; CRDT verbs live; raft/Zenoh deferred.
// DEPENDENCIES: tokio, std::fs, couplingd::{server, server::{Stores,ReconcileConfig}, jailspec, reconcile}
// PUBLIC_API: main
// END_AI_HEADER

use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;
use tokio::net::UnixListener;
use couplingd::{
    jailspec::parse_block,
    reconcile::MemJailManager,
    server::{serve_ext, ReconcileConfig, Stores},
};

// load_jails:start
//   purpose: Load CouplingJail descriptors from all *.coupling files in `dir`.
//            Each file is read as text and parsed with jailspec::parse_block.
//            The filename stem (without extension) is used as the fallback jail name.
//            Files that fail to parse are logged to stderr and skipped — the daemon
//            must not refuse to start due to a single malformed file.
//   input:  dir — directory path string (from COUPLINGD_JAILS_DIR env)
//   output: Vec<couplingd::jailspec::CouplingJail> — successfully parsed descriptors
//   sideEffects: reads files from disk; logs warnings to stderr on parse errors
// load_jails:end
fn load_jails(dir: &str) -> Vec<couplingd::jailspec::CouplingJail> {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(err) => {
            eprintln!("[couplingd] COUPLINGD_JAILS_DIR '{dir}' not readable: {err} — no jails loaded");
            return Vec::new();
        }
    };

    let mut jails = Vec::new();

    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("coupling") {
            continue;
        }

        // Fallback jail name = filename stem (e.g. "pg-matrix" from "pg-matrix.coupling").
        let stem = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown");

        let content = match std::fs::read_to_string(&path) {
            Ok(s) => s,
            Err(err) => {
                eprintln!("[couplingd] skipping {}: read error: {err}", path.display());
                continue;
            }
        };

        match parse_block(&content, stem) {
            Ok(jail) => {
                eprintln!("[couplingd] loaded jail '{}' role={} svc={}", jail.name, jail.role, jail.svc);
                jails.push(jail);
            }
            Err(err) => {
                eprintln!("[couplingd] skipping {}: parse error: {err}", path.display());
            }
        }
    }

    jails
}

// main:start
//   purpose: Daemon entry — resolve socket path from COUPLINGD_SOCK env
//            (default /tmp/bsdos/couplingd.sock), bind Unix socket, set 0o660
//            permissions, load coupling jails from COUPLINGD_JAILS_DIR (if set),
//            construct Stores, build ReconcileConfig, and call server::serve_ext.
//   input:  env COUPLINGD_SOCK (optional), COUPLINGD_JAILS_DIR (optional),
//           COUPLINGD_NODE_ID (optional u64, default 1),
//           COUPLINGD_RECONCILE_MS (optional u64, default 1000)
//   output: Result<(), Box<dyn std::error::Error>>
//   sideEffects: creates Unix socket at COUPLINGD_SOCK path; loops accepting connections;
//                spawns background expiry + reconcile tasks
// main:end
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let sock_path = std::env::var("COUPLINGD_SOCK")
        .unwrap_or_else(|_| "/tmp/bsdos/couplingd.sock".to_string());

    // Ensure parent directory exists.
    if let Some(parent) = std::path::Path::new(&sock_path).parent() {
        std::fs::create_dir_all(parent)?;
    }

    // Remove stale socket if present.
    let _ = std::fs::remove_file(&sock_path);

    let listener = UnixListener::bind(&sock_path)?;
    std::fs::set_permissions(&sock_path, std::fs::Permissions::from_mode(0o660))?;

    eprintln!("[couplingd] coupling-store daemon on {sock_path}");
    eprintln!("[couplingd] echo 'PING' | nc -U {sock_path}");

    // Load coupling jails from COUPLINGD_JAILS_DIR (empty list = no-op reconcile).
    let jails = match std::env::var("COUPLINGD_JAILS_DIR") {
        Ok(dir) => load_jails(&dir),
        Err(_)  => {
            eprintln!("[couplingd] COUPLINGD_JAILS_DIR not set — reconcile disabled (no-op)");
            Vec::new()
        }
    };

    let node_id: u64 = std::env::var("COUPLINGD_NODE_ID")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);

    let tick_ms: u64 = std::env::var("COUPLINGD_RECONCILE_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1_000);

    eprintln!("[couplingd] node_id={node_id} jails={} reconcile_tick={tick_ms}ms",
              jails.len());

    // Select JailManager: FreeBsdJailManager on FreeBSD, MemJailManager on host.
    #[cfg(target_os = "freebsd")]
    let mgr: Arc<dyn couplingd::reconcile::JailManager> =
        Arc::new(couplingd::reconcile::FreeBsdJailManager::new());

    #[cfg(not(target_os = "freebsd"))]
    let mgr: Arc<dyn couplingd::reconcile::JailManager> =
        Arc::new(MemJailManager::new());

    let cfg = ReconcileConfig {
        jails,
        mgr,
        node_id,
        ttl_ms:  5_000,
        tick_ms,
    };

    let stores = Stores::new();
    serve_ext(listener, stores, cfg).await.map_err(|e| format!("{e}").into())
}
