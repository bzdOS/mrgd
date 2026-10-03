// START_AI_HEADER
// MODULE: hubd-queue-repl/src/main.rs
// PURPOSE: Native Zenoh replication of hubd team queues. `hub queue send` appends a
//          block to <teamroot>/queues/<role>.queue.md; this daemon watches that dir,
//          publishes every locally-originated append over Zenoh, and applies remote
//          appends received from peers — so a queue entry written on one node appears
//          on every other node within ~1s, WITHOUT git mesh-sync.
//
//          Transport follows the ZenohCrdtSink pattern (couplingd::crdt): a stock
//          zenoh::Session with connect/endpoints + a wildcard subscriber on a keyexpr
//          prefix. The queue key layout encodes routing metadata in the keyexpr so the
//          payload is the raw appended bytes (no envelope encoding):
//
//              hubd/queues/<role>/<origin_node>/<seq>
//
//          - <role>        queue file role (filename minus .queue.md)
//          - <origin_node> the node that originated the append (loopback echo filter)
//          - <seq>         monotonic per-origin counter (best-effort dedup)
//
//          Loop prevention: when THIS daemon applies a remote append to its local file,
//          it records the chunk's sha256 in a suppress set first; the local dir-watch
//          then sees the resulting append, finds its hash in the suppress set, and
//          skips re-publishing it. Locally-originated appends (`hub queue send`) are not
//          in the set and therefore get published. A receiver-side suffix check makes
//          delivery idempotent across restarts and re-deliveries (append only if the
//          file does not already end with the chunk).
//
// ENV:
//   HUBD_TEAM_DIR            team root containing queues/ (default: walk-up from CWD)
//   HUBD_QUEUE_NODE_ID       origin tag (default: /etc/hostname or node-<pid>)
//   HUBD_QUEUE_ZENOH_LISTEN  zenoh listen endpoints, e.g. tcp/127.0.0.1:7449 (default: none)
//   HUBD_QUEUE_ZENOH_CONNECT zenoh connect endpoints, e.g. tcp/127.0.0.1:7449 (default: none)
//   HUBD_QUEUE_KEY_PREFIX    zenoh keyexpr prefix (default: hubd/queues)
//   HUBD_QUEUE_POLL_MS       dir-watch poll interval (default: 1000)
//
// SECURITY MODEL (mandatory): nodes live on UNTRUSTED networks (build-host public IP,
// offsite behind NAT, laptop, internet). Zenoh MUST NOT carry plaintext on public
// interfaces. Both LISTEN/CONNECT default to empty (peer/scouting, localhost/LAN
// only). For cross-node replication use ONE of:
//   (A) ssh -L tunnel (autossh): each node binds Zenoh on 127.0.0.1 only; a peer
//       forwards its localhost port over `autossh -N -L 7449:127.0.0.1:7449 <peer>`
//       and the local daemon CONNECTs to tcp/127.0.0.1:7449. Traffic rides ssh (TLS).
//   (B) Zenoh TLS/QUIC locators (tls/<host>:<port>) with mutual certs/PSK — reuse the
//       zenoh transport_tls feature already in the workspace (cf. the F2 mTLS notes).
// CAVEAT: on ТСПУ/DPI-filtered ISPs `ssh -L` may be cut — fall back to (B) or the
// obfs link; see see the DPI notes below (Zenoh-over-ssh-exec) for the extreme-DPI path.
// END_AI_HEADER

use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

const DEFAULT_PREFIX: &str = "hubd/queues";
const DEFAULT_POLL_MS: u64 = 1000;

fn env_or(k: &str, d: &str) -> String {
    std::env::var(k).unwrap_or_else(|_| d.to_string())
}

// resolve_team_root:start
//   purpose: Find the team root (dir containing queues/), mirroring hubd queue.mjs
//            resolveQueueRoot: HUBD_TEAM_DIR/HUBD_QUEUE_DIR env, else walk up from CWD
//            (max 8 levels) to the first dir with queues/ or .git, else CWD.
//   output: PathBuf to the team root
// resolve_team_root:end
fn resolve_team_root() -> PathBuf {
    if let Ok(t) = std::env::var("HUBD_TEAM_DIR")
        .or_else(|_| std::env::var("HUBD_QUEUE_DIR"))
    {
        return PathBuf::from(t);
    }
    let mut d = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    for _ in 0..8 {
        if d.join("queues").exists() || d.join(".git").exists() {
            return d;
        }
        if !d.pop() {
            break;
        }
    }
    std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
}

fn node_id() -> String {
    if let Ok(n) = std::env::var("HUBD_QUEUE_NODE_ID") {
        if !n.is_empty() {
            return n;
        }
    }
    let h = std::fs::read_to_string("/etc/hostname").unwrap_or_default();
    let h = h.trim();
    if !h.is_empty() {
        return h.to_string();
    }
    format!("node-{}", std::process::id())
}

fn sha(bytes: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(bytes);
    h.finalize().into()
}

struct Repl {
    node: String,
    queues_dir: PathBuf,
    prefix: String,
    offsets: Mutex<HashMap<String, (u64, u64)>>, // (inode, byte_offset) per queue file
    suppress: Mutex<HashSet<[u8; 32]>>,
    seq: AtomicU64,
}

impl Repl {
    fn new(node: String, queues_dir: PathBuf, prefix: String) -> Self {
        Repl {
            node,
            queues_dir,
            prefix,
            offsets: Mutex::new(HashMap::new()),
            suppress: Mutex::new(HashSet::new()),
            seq: AtomicU64::new(0),
        }
    }

    // scan queues dir for *.queue.md; returns (filename, full_path) pairs.
    fn list_queue_files(&self) -> Vec<(String, PathBuf)> {
        let Ok(rd) = std::fs::read_dir(&self.queues_dir) else {
            return Vec::new();
        };
        let mut out = Vec::new();
        for e in rd.flatten() {
            let name = e.file_name().to_string_lossy().into_owned();
            if name.ends_with(".queue.md") {
                out.push((name, e.path()));
            }
        }
        out
    }
}

// watch_loop:start
//   purpose: Poll queues/*.queue.md every POLL_MS; for each file grown since last poll,
//            publish the appended chunk unless it was a remote-apply (in suppress set).
//            Offsets are initialized to the current file size (tail-from-now: no history flood).
// watch_loop:end
async fn watch_loop(repl: Arc<Repl>, session: zenoh::Session, poll_ms: u64) {
    loop {
        let mut jobs: Vec<(String, Vec<u8>)> = Vec::new();
        {
            let mut offsets = repl.offsets.lock().await;
            for (fname, path) in repl.list_queue_files() {
                let Ok(meta) = std::fs::metadata(&path) else {
                    continue;
                };
                let ino = std::os::unix::fs::MetadataExt::ino(&meta);
                let data = std::fs::read(&path).unwrap_or_default();
                let new_size = data.len() as u64;
                // entry: (last_inode, last_offset). Default (ino, 0) for a file unseen
                // until now (appeared after startup → publish its full content).
                let entry = offsets.entry(fname.clone()).or_insert((ino, 0));
                // If the inode changed the file was recreated/rotated → re-publish from 0.
                let start = if entry.0 != ino { 0 } else { entry.1 } as usize;
                entry.0 = ino;
                entry.1 = new_size;
                if new_size as usize > start {
                    let chunk = data[start..].to_vec();
                    if chunk.is_empty() {
                        continue;
                    }
                    let hash = sha(&chunk);
                    if repl.suppress.lock().await.remove(&hash) {
                        continue; // we applied this remotely → don't republish
                    }
                    let role = fname.trim_end_matches(".queue.md");
                    let s = repl.seq.fetch_add(1, Ordering::SeqCst);
                    let key = format!("{}/{}/{}/{}", repl.prefix, role, repl.node, s);
                    jobs.push((key, chunk));
                }
            }
        }
        for (key, chunk) in jobs {
            if let Err(e) = session.put(&key, chunk).await {
                eprintln!("[hubd-queue-repl] zenoh put {key} failed: {e}");
            }
        }
        tokio::time::sleep(Duration::from_millis(poll_ms)).await;
    }
}

// file_ends_with: append-idempotency check — true if `path` already ends with `chunk`.
fn file_ends_with(path: &std::path::Path, chunk: &[u8]) -> bool {
    let data = match std::fs::read(path) {
        Ok(d) => d,
        Err(_) => return false,
    };
    data.len() >= chunk.len() && data[data.len() - chunk.len()..] == *chunk
}

// sub_loop:start
//   purpose: Subscribe to <prefix>/**; on each sample, extract role/origin/seq from the
//            keyexpr, skip own echoes, skip chunks already at the file tail, else record
//            the chunk hash in the suppress set and append it to the local queue file.
//            The suppress-set entry makes the local watcher treat the resulting file
//            growth as a remote-apply (skip republish), breaking the feedback loop.
// sub_loop:end
async fn sub_loop(repl: Arc<Repl>, session: zenoh::Session) {
    let sub_key = format!("{}/**", repl.prefix);
    let subscriber = match session.declare_subscriber(&sub_key).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("[hubd-queue-repl] subscriber declare on {sub_key} failed: {e}");
            return;
        }
    };
    let node = repl.node.clone();
    while let Ok(sample) = subscriber.recv_async().await {
        let ke = sample.key_expr().as_str();
        // Expected layout: <prefix>/<role>/<origin>/<seq>  (prefix has no '/')
        let role_origin_seq: Vec<&str> = ke
            .strip_prefix(&format!("{}/", repl.prefix))
            .unwrap_or(ke)
            .splitn(3, '/')
            .collect();
        if role_origin_seq.len() != 3 {
            continue;
        }
        let role = role_origin_seq[0];
        let origin = role_origin_seq[1];
        if origin == node {
            continue; // own echo
        }
        let chunk = sample.payload().to_bytes().to_vec();
        if chunk.is_empty() {
            continue;
        }
        let qfile = repl.queues_dir.join(format!("{role}.queue.md"));
        if file_ends_with(&qfile, &chunk) {
            continue; // already applied (idempotent)
        }
        // Mark suppress BEFORE append so the watcher does not republish this growth.
        let hash = sha(&chunk);
        repl.suppress.lock().await.insert(hash);
        match std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&qfile)
        {
            Ok(mut f) => {
                if let Err(e) = f.write_all(&chunk) {
                    eprintln!("[hubd-queue-repl] append {qfile:?} failed: {e}");
                }
            }
            Err(e) => eprintln!("[hubd-queue-repl] open {qfile:?} failed: {e}"),
        }
        eprintln!(
            "[hubd-queue-repl] applied remote append role={role} origin={origin} bytes={}",
            chunk.len()
        );
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Initialize a tracing subscriber so RUST_LOG=zenoh=trace reveals session
    // handshake / interest propagation / routing (debug aid for cross-node delivery).
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_writer(std::io::stderr)
        .try_init();

    let team_root = resolve_team_root();
    let queues_dir = team_root.join("queues");
    std::fs::create_dir_all(&queues_dir)?;
    let node = node_id();
    // SECURITY (mandatory): NO plaintext Zenoh on public interfaces. Defaults are
    // empty → peer/scouting (localhost/LAN only). For inter-node transport, bind
    // LISTEN on 127.0.0.1 and carry traffic over an encrypted ssh -L tunnel
    // (autossh) or use Zenoh TLS/QUIC locators with certs. See module header.
    let connect = env_or("HUBD_QUEUE_ZENOH_CONNECT", "");
    let listen = env_or("HUBD_QUEUE_ZENOH_LISTEN", "");
    let prefix = env_or("HUBD_QUEUE_KEY_PREFIX", DEFAULT_PREFIX);
    let poll_ms = env_or("HUBD_QUEUE_POLL_MS", &DEFAULT_POLL_MS.to_string())
        .parse()
        .unwrap_or(DEFAULT_POLL_MS);

    let to_json5 = |s: &str| -> String {
        let quoted: Vec<String> = s.split(',').map(|e| format!("\"{}\"", e.trim())).collect();
        format!("[{}]", quoted.join(","))
    };
    let mut cfg = zenoh::Config::default();
    if !connect.is_empty() {
        cfg.insert_json5("connect/endpoints", &to_json5(&connect))?;
    }
    if !listen.is_empty() {
        cfg.insert_json5("listen/endpoints", &to_json5(&listen))?;
    }
    if connect.is_empty() && listen.is_empty() {
        eprintln!(
            "[hubd-queue-repl] WARNING: no connect/listen set — peer/scouting (localhost/LAN only); \
             cross-node needs ssh -L tunnel or TLS locators (see module header)"
        );
    }
    // Optional zenoh session mode (default peer). Set HUBD_QUEUE_MODE=client to connect
    // to a dedicated hubd-queue-router (src/bin/router.rs) — the reliable broker topology.
    if let Ok(mode) = std::env::var("HUBD_QUEUE_MODE") {
        if !mode.is_empty() {
            cfg.insert_json5("mode", &format!("\"{}\"", mode))?;
            eprintln!("[hubd-queue-repl] zenoh mode={mode}");
        }
    }

    eprintln!(
        "[hubd-queue-repl] node={node} queues={queues_dir:?} prefix={prefix} listen={listen} connect={connect} poll={poll_ms}ms"
    );
    let session = zenoh::open(cfg).await?;

    let repl = Arc::new(Repl::new(node, queues_dir, prefix.clone()));

    // Snapshot pre-existing queue files: their current content is treated as
    // already-known (offset = size, tail-from-now → no history flood). Files that
    // appear AFTER startup are unknown → offset stays 0 → their content is published.
    {
        let mut offsets = repl.offsets.lock().await;
        for (fname, path) in repl.list_queue_files() {
            let (sz, ino) = std::fs::metadata(&path)
                .map(|m| (m.len(), std::os::unix::fs::MetadataExt::ino(&m)))
                .unwrap_or((0, 0));
            offsets.insert(fname, (ino, sz));
        }
        eprintln!(
            "[hubd-queue-repl] tail-from-now for {} pre-existing queue file(s)",
            offsets.len()
        );
    }

    let r1 = repl.clone();
    let s1 = session.clone();
    let watch = tokio::spawn(async move { watch_loop(r1, s1, poll_ms).await; });

    let r2 = repl.clone();
    let s2 = session.clone();
    let sub = tokio::spawn(async move { sub_loop(r2, s2).await; });

    let _ = tokio::join!(watch, sub);
    Ok(())
}
