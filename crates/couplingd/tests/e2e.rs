// START_AI_HEADER
// MODULE: couplingd/tests/e2e.rs
// PURPOSE: End-to-end integration tests for couplingd text-protocol over Unix socket.
//          Each test: binds a temp Unix socket, calls server::serve in a background
//          task, connects with tokio async I/O, sends text-protocol commands, asserts
//          wire responses.  Tests verify PING, SESSION OPEN/CLOSE, LOCK ACQ exclusive
//          (fence returned), lock BUSY, SESSION CLOSE cascade (auto-release),
//          KV PUT→CAS conflict, SVC REG→RESOLVE, and CRDT GET/MERGE.
//          ReconcileLoop tests verify: daemon with no jails starts and ticks without
//          error; daemon with one singleton jail reconciles via MemJailManager.
// INTENT: Prove end-to-end wiring of dispatch + daemon-level cascade + reconcile tick.
// DEPENDENCIES: tokio (rt-multi-thread, macros, net, io-util), couplingd (lib)
// PUBLIC_API: (test functions only)
// END_AI_HEADER

use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use couplingd::server::{serve, serve_ext, ReconcileConfig, Stores};

// ── Test helpers ───────────────────────────────────────────────────────────────

/// Generate a unique temporary socket path for each test.
fn temp_sock_path() -> PathBuf {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    PathBuf::from(format!("/tmp/couplingd-test-{}-{}.sock", std::process::id(), ts))
}

/// Bind a temporary Unix socket, spawn server::serve in background, return
/// (socket_path, abort_handle).  The caller drops `_guard` to cancel the server.
async fn start_server() -> (PathBuf, tokio::task::JoinHandle<()>) {
    let path = temp_sock_path();

    // Remove stale socket if present.
    let _ = std::fs::remove_file(&path);

    let listener = UnixListener::bind(&path).expect("bind test socket");
    let stores   = Stores::new();
    let p        = path.clone();

    let handle = tokio::spawn(async move {
        let _ = serve(listener, stores).await;
        let _ = std::fs::remove_file(&p);
    });

    // Give the server task a moment to enter the accept loop.
    tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;

    (path, handle)
}

/// Bind a temp socket and start a daemon with the given ReconcileConfig.
async fn start_server_with_reconcile(cfg: ReconcileConfig) -> (PathBuf, tokio::task::JoinHandle<()>) {
    let path = temp_sock_path();
    let _ = std::fs::remove_file(&path);

    let listener = UnixListener::bind(&path).expect("bind test socket");
    let stores   = Stores::new();
    let p        = path.clone();

    let handle = tokio::spawn(async move {
        let _ = serve_ext(listener, stores, cfg).await;
        let _ = std::fs::remove_file(&p);
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(30)).await;
    (path, handle)
}

/// Open a connection to the test socket, returning a (writer, line-reader) pair.
async fn connect(path: &PathBuf) -> (
    tokio::net::unix::OwnedWriteHalf,
    tokio::io::Lines<BufReader<tokio::net::unix::OwnedReadHalf>>,
) {
    let stream = UnixStream::connect(path).await.expect("connect to test server");
    let (r, w) = stream.into_split();
    let lines  = BufReader::new(r).lines();
    (w, lines)
}

/// Send a command and return the server's one-line response (stripped of trailing \n).
async fn cmd(
    w:     &mut tokio::net::unix::OwnedWriteHalf,
    lines: &mut tokio::io::Lines<BufReader<tokio::net::unix::OwnedReadHalf>>,
    line:  &str,
) -> String {
    w.write_all(format!("{line}\n").as_bytes()).await.expect("write cmd");
    lines.next_line().await.expect("read response").expect("server closed")
}

/// Assert response starts with "+OK" — provides clear failure messages.
fn assert_ok(resp: &str, ctx: &str) {
    assert!(
        resp.starts_with("+OK"),
        "{ctx}: expected +OK, got: {resp:?}"
    );
}

/// Assert response starts with "-ERR" — provides clear failure messages.
fn assert_err(resp: &str, ctx: &str) {
    assert!(
        resp.starts_with("-ERR"),
        "{ctx}: expected -ERR, got: {resp:?}"
    );
}

// ── Tests ──────────────────────────────────────────────────────────────────────

/// PING → +OK PONG
#[tokio::test]
async fn e2e_ping() {
    let (path, _srv) = start_server().await;
    let (mut w, mut lines) = connect(&path).await;

    let resp = cmd(&mut w, &mut lines, "PING").await;
    assert_ok(&resp, "PING");
    assert!(resp.contains("PONG"), "PING response must contain PONG, got: {resp:?}");
}

/// SESSION OPEN → returns sid=<u64>
#[tokio::test]
async fn e2e_session_open() {
    let (path, _srv) = start_server().await;
    let (mut w, mut lines) = connect(&path).await;

    let resp = cmd(&mut w, &mut lines, "SESSION OPEN ttl=5000").await;
    assert_ok(&resp, "SESSION OPEN");
    assert!(resp.contains("sid="), "SESSION OPEN must return sid=…, got: {resp:?}");
}

/// LOCK ACQ exclusive → +OK fence=<N>  (fence is a positive integer)
#[tokio::test]
async fn e2e_lock_acq_exclusive_returns_fence() {
    let (path, _srv) = start_server().await;
    let (mut w, mut lines) = connect(&path).await;

    // Open a session first (sid doesn't matter for lock in M1, but good form).
    let sr = cmd(&mut w, &mut lines, "SESSION OPEN ttl=30000").await;
    assert_ok(&sr, "SESSION OPEN before lock");

    let resp = cmd(&mut w, &mut lines, "LOCK ACQ /db/primary mode=exclusive sid=1").await;
    assert_ok(&resp, "LOCK ACQ exclusive");
    assert!(resp.contains("fence="), "ACQ response must carry fence=…, got: {resp:?}");

    // Extract fence value and verify it is a positive integer.
    let fence: u64 = resp
        .split_whitespace()
        .find(|t| t.starts_with("fence="))
        .and_then(|t| t["fence=".len()..].parse().ok())
        .expect("fence= must be a valid u64");
    assert!(fence > 0, "fence must be positive, got {fence}");
}

/// LOCK ACQ exclusive on a key held exclusively by another session → -ERR BUSY
#[tokio::test]
async fn e2e_lock_busy_on_exclusive_conflict() {
    let (path, _srv) = start_server().await;
    let (mut w, mut lines) = connect(&path).await;

    // First holder takes the lock.
    let r1 = cmd(&mut w, &mut lines, "LOCK ACQ /db/writer mode=exclusive sid=10").await;
    assert_ok(&r1, "first LOCK ACQ");

    // Second holder on same key → must be BUSY.
    let r2 = cmd(&mut w, &mut lines, "LOCK ACQ /db/writer mode=exclusive sid=11").await;
    assert_err(&r2, "second LOCK ACQ on held key");
    assert!(
        r2.to_ascii_lowercase().contains("busy"),
        "error must mention 'busy', got: {r2:?}"
    );
}

/// SESSION CLOSE cascades lock release: after CLOSE, the same key can be acquired again.
/// This is the key correctness test for lease-cascade (§8 requirement).
#[tokio::test]
async fn e2e_session_close_cascades_lock_release() {
    let (path, _srv) = start_server().await;

    // Connection A: open session, acquire exclusive lock.
    let (mut wa, mut la) = connect(&path).await;
    let sr_a = cmd(&mut wa, &mut la, "SESSION OPEN ttl=30000").await;
    assert_ok(&sr_a, "SESSION OPEN A");
    let sid_a: u64 = sr_a
        .split_whitespace()
        .find(|t| t.starts_with("sid="))
        .and_then(|t| t["sid=".len()..].parse().ok())
        .expect("sid= must be a valid u64");

    let lock_r = cmd(&mut wa, &mut la, &format!("LOCK ACQ /resource/x mode=exclusive sid={sid_a}")).await;
    assert_ok(&lock_r, "LOCK ACQ by A");

    // Connection B: verify the lock is busy.
    let (mut wb, mut lb) = connect(&path).await;
    let busy_r = cmd(&mut wb, &mut lb, "LOCK ACQ /resource/x mode=exclusive sid=99").await;
    assert_err(&busy_r, "lock must be BUSY before close");

    // Connection A: close session — cascade must release the lock.
    let close_r = cmd(&mut wa, &mut la, &format!("SESSION CLOSE sid={sid_a}")).await;
    assert_ok(&close_r, "SESSION CLOSE");

    // Connection B: now the lock must be available.
    let acq_r = cmd(&mut wb, &mut lb, "LOCK ACQ /resource/x mode=exclusive sid=99").await;
    assert_ok(&acq_r, "LOCK ACQ after session close cascade — proves auto-release");
}

/// KV PUT then CAS with wrong version → -ERR conflict
#[tokio::test]
async fn e2e_kv_put_then_cas_conflict() {
    let (path, _srv) = start_server().await;
    let (mut w, mut lines) = connect(&path).await;

    // PUT creates v1.
    let r1 = cmd(&mut w, &mut lines, "KV PUT /cfg/port val=38343939").await; // hex "8899"
    assert_ok(&r1, "KV PUT");
    assert!(r1.contains("ver=1"), "first PUT must create ver=1, got: {r1:?}");

    // PUT again → v2.
    let r2 = cmd(&mut w, &mut lines, "KV PUT /cfg/port val=3939").await;
    assert_ok(&r2, "second KV PUT");
    assert!(r2.contains("ver=2"), "second PUT must create ver=2, got: {r2:?}");

    // CAS against ver=1 (stale) must fail — conflict.
    let r3 = cmd(&mut w, &mut lines, "KV CAS /cfg/port val=4141 ver=1").await;
    assert_err(&r3, "CAS with wrong ver must fail");
    assert!(
        r3.to_ascii_lowercase().contains("conflict"),
        "CAS error must mention 'conflict', got: {r3:?}"
    );

    // CAS against ver=2 (current) must succeed → v3.
    let r4 = cmd(&mut w, &mut lines, "KV CAS /cfg/port val=4242 ver=2").await;
    assert_ok(&r4, "CAS with correct ver must succeed");
    assert!(r4.contains("ver=3"), "successful CAS must produce ver=3, got: {r4:?}");
}

/// SVC REG then RESOLVE returns the registered provider.
#[tokio::test]
async fn e2e_svc_reg_then_resolve() {
    let (path, _srv) = start_server().await;
    let (mut w, mut lines) = connect(&path).await;

    // Open a session for the svc registration.
    let sr = cmd(&mut w, &mut lines, "SESSION OPEN ttl=30000").await;
    assert_ok(&sr, "SESSION OPEN");
    let sid: u64 = sr
        .split_whitespace()
        .find(|t| t.starts_with("sid="))
        .and_then(|t| t["sid=".len()..].parse().ok())
        .expect("sid= must be a valid u64");

    // Register service.
    let reg_r = cmd(&mut w, &mut lines, &format!("SVC REG pg-matrix node=7 sid={sid}")).await;
    assert_ok(&reg_r, "SVC REG");

    // Resolve must return node=7.
    let res_r = cmd(&mut w, &mut lines, "SVC RESOLVE pg-matrix").await;
    assert_ok(&res_r, "SVC RESOLVE");
    assert!(
        res_r.contains("node=7"),
        "RESOLVE must return node=7, got: {res_r:?}"
    );
    assert!(
        res_r.contains(&format!("sid={sid}")),
        "RESOLVE must return correct sid, got: {res_r:?}"
    );
}

/// SESSION CLOSE also cascades svc expiry: after CLOSE, the svc registration is gone.
#[tokio::test]
async fn e2e_session_close_cascades_svc_expiry() {
    let (path, _srv) = start_server().await;
    let (mut w, mut lines) = connect(&path).await;

    // Open session.
    let sr = cmd(&mut w, &mut lines, "SESSION OPEN ttl=30000").await;
    assert_ok(&sr, "SESSION OPEN");
    let sid: u64 = sr
        .split_whitespace()
        .find(|t| t.starts_with("sid="))
        .and_then(|t| t["sid=".len()..].parse().ok())
        .expect("sid= must be a valid u64");

    // Register svc.
    let reg_r = cmd(&mut w, &mut lines, &format!("SVC REG myservice node=42 sid={sid}")).await;
    assert_ok(&reg_r, "SVC REG");

    // Resolve confirms registration.
    let res_r = cmd(&mut w, &mut lines, "SVC RESOLVE myservice").await;
    assert_ok(&res_r, "SVC RESOLVE before close");

    // Close session → cascade must remove svc.
    let close_r = cmd(&mut w, &mut lines, &format!("SESSION CLOSE sid={sid}")).await;
    assert_ok(&close_r, "SESSION CLOSE");

    // Resolve must now fail.
    let res2_r = cmd(&mut w, &mut lines, "SVC RESOLVE myservice").await;
    assert_err(&res2_r, "SVC RESOLVE after session close — must be NotFound");
}

/// MEMBERS → +OK (stub, at least does not error)
#[tokio::test]
async fn e2e_members() {
    let (path, _srv) = start_server().await;
    let (mut w, mut lines) = connect(&path).await;

    let resp = cmd(&mut w, &mut lines, "MEMBERS").await;
    assert_ok(&resp, "MEMBERS");
}

// ── ReconcileLoop integration tests ────────────────────────────────────────────

// e2e_reconcile_empty_jails_no_error:start
//   purpose: A daemon started with an empty jails list (no COUPLINGD_JAILS_DIR)
//            starts successfully and responds to PING after at least one reconcile tick.
//            Proves that the reconcile background task runs harmlessly when jails is empty.
//   input:  ReconcileConfig::default() (empty jails, MemJailManager, tick_ms=50)
//   output: PING → +OK PONG after two tick intervals; no panics or errors
//   sideEffects: daemon started; two reconcile ticks elapsed
// e2e_reconcile_empty_jails_no_error:end
#[tokio::test]
async fn e2e_reconcile_empty_jails_no_error() {
    let cfg = ReconcileConfig {
        tick_ms: 50, // fast tick for test speed
        ..ReconcileConfig::default()
    };

    let (path, _srv) = start_server_with_reconcile(cfg).await;

    // Wait 2+ ticks to ensure the reconcile loop has fired at least twice.
    tokio::time::sleep(tokio::time::Duration::from_millis(130)).await;

    // Daemon must still be live and responsive.
    let (mut w, mut lines) = connect(&path).await;
    let resp = cmd(&mut w, &mut lines, "PING").await;
    assert_ok(&resp, "PING after reconcile ticks with empty jails");
    assert!(resp.contains("PONG"), "got: {resp:?}");
}

// e2e_reconcile_singleton_jail_starts:start
//   purpose: A daemon with one singleton jail reconciles it on the first tick:
//            MemJailManager receives a Start event, the jail is marked running.
//            Verifies the reconcile-loop-to-serve integration (not just unit test).
//   input:  ReconcileConfig with one singleton jail, MemJailManager, fast tick
//   output: after two tick intervals: MemJailManager.is_running("test-jail") == true;
//           daemon still responds to PING
//   sideEffects: MemJailManager.start("test-jail") called by reconcile task
// e2e_reconcile_singleton_jail_starts:end
#[tokio::test]
async fn e2e_reconcile_singleton_jail_starts() {
    use couplingd::{
        jailspec::{CouplingJail, JailRole},
        reconcile::MemJailManager,
    };

    let mgr = Arc::new(MemJailManager::new());

    let jail = CouplingJail {
        name:       "test-jail".to_string(),
        role:       JailRole::Singleton,
        svc:        "test-svc".to_string(),
        dataset:    String::new(),
        on_promote: String::new(),
        on_demote:  String::new(),
    };

    let cfg = ReconcileConfig {
        jails:   vec![jail],
        mgr:     Arc::clone(&mgr) as Arc<dyn couplingd::reconcile::JailManager>,
        node_id: 1,
        ttl_ms:  30_000,
        tick_ms: 50, // fast tick for test speed
    };

    let (path, _srv) = start_server_with_reconcile(cfg).await;

    // Wait for at least two reconcile ticks so the loop has time to win the election.
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // The singleton jail must have been started by the reconcile loop.
    assert!(
        mgr.is_running("test-jail"),
        "reconcile loop must have started the singleton jail via MemJailManager"
    );

    // Daemon must still be live.
    let (mut w, mut lines) = connect(&path).await;
    let resp = cmd(&mut w, &mut lines, "PING").await;
    assert_ok(&resp, "PING after reconcile started singleton jail");
}

// ── CRDT verb tests ────────────────────────────────────────────────────────────

// e2e_crdt_get_absent_key_returns_zero:start
//   purpose: CRDT GET on a key that has never been written returns val=0 (PnCounter default).
//   input:  CRDT GET /counters/hits on fresh daemon
//   output: +OK key=/counters/hits val=0
//   sideEffects: none
// e2e_crdt_get_absent_key_returns_zero:end
#[tokio::test]
async fn e2e_crdt_get_absent_key_returns_zero() {
    let (path, _srv) = start_server().await;
    let (mut w, mut lines) = connect(&path).await;

    let resp = cmd(&mut w, &mut lines, "CRDT GET /counters/hits").await;
    assert_ok(&resp, "CRDT GET absent key");
    assert!(
        resp.contains("val=0"),
        "absent CRDT key must return val=0, got: {resp:?}"
    );
}

// e2e_crdt_merge_increments_counter:start
//   purpose: CRDT MERGE applies a PnCounterDelta; subsequent GET returns updated value.
//            Verifies the wire round-trip: delta serialised → base64 → MERGE command → GET.
//   input:  PnCounterDelta(p={node1: +5}, n={}) encoded as base64; then GET
//   output: first GET returns val=0; after MERGE, GET returns val=5
//   sideEffects: CrdtStore mutated for /counters/page_views
// e2e_crdt_merge_increments_counter:end
#[tokio::test]
async fn e2e_crdt_merge_increments_counter() {
    use couplingd::server::{base64_encode, pn_delta_to_bytes};
    use couplingd::crdt::{GCounterDelta, PnCounterDelta};
    use std::collections::HashMap;

    let (path, _srv) = start_server().await;
    let (mut w, mut lines) = connect(&path).await;

    // Initial GET → 0.
    let resp0 = cmd(&mut w, &mut lines, "CRDT GET /counters/page_views").await;
    assert_ok(&resp0, "CRDT GET initial");
    assert!(resp0.contains("val=0"), "initial val must be 0, got: {resp0:?}");

    // Build a PnCounterDelta with p={node 1: 5}, n={}.
    let mut p_slots = HashMap::new();
    p_slots.insert(1u64, 5u64);
    let delta = PnCounterDelta {
        p: GCounterDelta { slots: p_slots },
        n: GCounterDelta { slots: HashMap::new() },
    };

    let bytes  = pn_delta_to_bytes(&delta);
    let b64    = base64_encode(&bytes);
    let merge_cmd = format!("CRDT MERGE /counters/page_views delta={b64}");

    let resp1 = cmd(&mut w, &mut lines, &merge_cmd).await;
    assert_ok(&resp1, "CRDT MERGE");
    assert!(
        resp1.contains("val=5"),
        "after MERGE +5, val must be 5, got: {resp1:?}"
    );

    // GET confirms the stored value.
    let resp2 = cmd(&mut w, &mut lines, "CRDT GET /counters/page_views").await;
    assert_ok(&resp2, "CRDT GET after merge");
    assert!(
        resp2.contains("val=5"),
        "GET after MERGE must return val=5, got: {resp2:?}"
    );
}

// e2e_crdt_merge_idempotent:start
//   purpose: Merging the same delta twice (idempotency) leaves the value unchanged
//            on the second application — verifies CRDT join-semilattice property
//            end-to-end through the wire protocol.
//   input:  two identical MERGE commands with same delta
//   output: value after first MERGE == value after second MERGE
//   sideEffects: CrdtStore mutated once (second merge is no-op)
// e2e_crdt_merge_idempotent:end
#[tokio::test]
async fn e2e_crdt_merge_idempotent() {
    use couplingd::server::{base64_encode, pn_delta_to_bytes};
    use couplingd::crdt::{GCounterDelta, PnCounterDelta};
    use std::collections::HashMap;

    let (path, _srv) = start_server().await;
    let (mut w, mut lines) = connect(&path).await;

    let mut p_slots = HashMap::new();
    p_slots.insert(1u64, 10u64);
    let delta = PnCounterDelta {
        p: GCounterDelta { slots: p_slots },
        n: GCounterDelta { slots: HashMap::new() },
    };
    let bytes = pn_delta_to_bytes(&delta);
    let b64   = base64_encode(&bytes);
    let merge_cmd = format!("CRDT MERGE /counters/events delta={b64}");

    // First MERGE → val=10.
    let r1 = cmd(&mut w, &mut lines, &merge_cmd).await;
    assert_ok(&r1, "first MERGE");
    assert!(r1.contains("val=10"), "first MERGE must yield val=10, got: {r1:?}");

    // Second MERGE (same delta) → still val=10 (idempotent).
    let r2 = cmd(&mut w, &mut lines, &merge_cmd).await;
    assert_ok(&r2, "second MERGE");
    assert!(
        r2.contains("val=10"),
        "idempotent MERGE must not change val, got: {r2:?}"
    );
}
