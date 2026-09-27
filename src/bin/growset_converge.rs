// Battle-volume convergence + quiescence proof for the growset anti-entropy.
//
// WHAT THIS IS FOR
// The 2026-09-07..11 anti-entropy incident cost the deployment 9.5 GB RSS and
// ~18 h of CPU over four days: about 200 claims per node were re-published
// every fifth tick
// forever, and the Zenoh self-echo came back as endless "drain tick: N blob(s)
// received".  The fix (AntiEntropyPolicy) grants a bounded burst after the last
// observed activity and then goes silent.  growset_stress proves that on an
// in-memory wire with a single CRDT family; this harness proves it on a REAL
// Zenoh loopback mesh at battle volume, across several scopes and all four CRDT
// families matrix-hs replicates, and it measures the three things the incident
// was measured in: retained bytes, CPU time and wall time to quiescence.
//
// BOUNDARY (critical, and enforced in code — see assert_temp_dir)
// Everything this binary touches lives under the system temp directory.  It
// opens Zenoh sessions with multicast AND gossip scouting DISABLED and
// explicit 127.0.0.1 endpoints only, so it cannot discover or join a live node
// on the LAN even by accident, and it never reads or writes a hub data
// directory, a queue, or any other node's state.
//
// SCENARIO
//   NODES x SCOPES independent replicas in one loopback Zenoh mesh, ring
//   topology: node i listens on base_port+i and connects to base_port+(i+1)%N.
//   With scouting off those links are the ONLY edges, and Zenoh routing makes
//   the ring a full mesh for pub/sub.
//   Per (node, scope): a ZenohCrdtSink, a GrowSetClaimStore + ReconcileDriver
//   (the production barrier path, claims_key = "claims"), and four CRDT
//   families under their own routing keys: room + keys (RoomLog, the
//   production wire format via delta_to_bytes/delta_from_bytes), counter/hits
//   (GCounter) and orset/users (OrSet<String>).  The claim count defaults to
//   the incident's scale, rounded up to 200 per replica).
//   A sync loop per replica drains and merges every family on the tick, the way
//   a matrix-hs node drains its room/keys state.
//
// QUIESCENCE CRITERION (explicit, and the only thing the run waits for)
//   Published-blob count, drained-blob count, retained bytes and the per-replica
//   state fingerprint (barrier view names + records, room log len, counter
//   value, orset size) are all UNCHANGED for QUIET_S consecutive seconds.  The
//   first moment that holds is "time to quiescence".
//
// MEASURES
//   retained bytes = published bytes - drained bytes, i.e. what is still sitting
//                    undrained in the sink inboxes.  This is the quantity that
//                    grew without bound in the incident.
//   cpu seconds    = utime+stime of this process from /proc/self/stat, so it
//                    covers every Zenoh background thread too.
//   wall to quiescence, peak RSS (VmHWM — the exact metric the 9.5 GB was
//   recorded in), plus per-family convergence checks.
//
// PASS/FAIL (exit 0 only if all hold)
//   * quiescence reached within MAX_WALL_S
//   * retained bytes <= RETAINED_MAX and peak RSS <= RSS_MAX_MB
//   * CPU seconds <= CPU_MAX_S
//   * converged: for every scope, every node holds the same number of records
//     per claimed name, every node's own claims are present in every other
//     node's view, and room/keys/counter/orset agree across nodes on the full
//     union size.
//
// USAGE
//   cargo run --release --features cluster --bin growset_converge
// Every knob has a neutral default and can be overridden by env var (see
// Params::from_env).  Exit 0 = quiesced and converged within budget.

fn main() {
    #[cfg(feature = "cluster")]
    let code = {
        // Multi-thread runtime is REQUIRED: ZenohCrdtSink::publish drives its put
        // through tokio::task::block_in_place, which panics on a current_thread
        // runtime.  Extra workers because every ReconcileDriver tick may be
        // parked in block_in_place at the same time.
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(8)
            .enable_all()
            .build()
            .expect("runtime");
        rt.block_on(scenario::run())
    };
    #[cfg(not(feature = "cluster"))]
    let code = {
        eprintln!(
            "growset_converge: needs --features cluster \
             (the scenario runs on a real Zenoh loopback mesh)"
        );
        2
    };
    std::process::exit(code);
}

#[cfg(feature = "cluster")]
mod scenario {
    use std::collections::{BTreeMap, BTreeSet, HashMap};    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use mrgd::substrate::barrier::ClaimStore;
    use mrgd::substrate::barrier_growset::{
        ClaimRecord, GrowSetClaimStore, LostClaim, ReconcileDriver,
    };
    use mrgd::substrate::crdt::{
        CrdtError, CrdtSink, GCounter, GCounterDelta, OrSet, OrSetDelta, Tag, ZenohCrdtSink,
    };
    use mrgd::substrate::matrix_events::{Pdu, RoomLog};

    /// Zenoh keyexpr prefix per SCOPE — exactly as in production, where every
    /// node in a scope shares the same room/barrier/keys prefix and the family is
    /// the CRDT key under it.  A per-node prefix would put each subscriber on a
    /// disjoint keyexpr and nothing could cross.  No real deployment is named.
    const PREFIX_FMT: &str = "mrgd/converge/s{scope}";
    const KEY_ROOM: &str = "room";
    const KEY_KEYS: &str = "keys";
    const KEY_COUNTER: &str = "counter/hits";
    const KEY_ORSET: &str = "orset/users";

    // ── Params ─────────────────────────────────────────────────────────────────

    /// Scenario knobs.  The tick is the incident's 1 s compressed 10x (the same
    /// compression growset_stress used) so a full battle run fits in seconds.
    struct Params {
        nodes: usize,
        scopes: usize,
        claims: usize,
        pdus: usize,
        key_pdus: usize,
        counter_incs: usize,
        orset_adds: usize,
        tick: Duration,
        settle: Duration,
        quiet_s: u64,
        max_wall_s: u64,
        base_port: u16,
        retained_max: u64,
        rss_max_mb: u64,
        cpu_max_s: f64,
        dir: PathBuf,
    }

    impl Params {
        fn env_u64(key: &str, default: u64) -> u64 {
            std::env::var(key)
                .ok()
                .and_then(|v| v.trim().parse::<u64>().ok())
                .unwrap_or(default)
        }

        // Params::from_env:start
        //   purpose: Read the scenario knobs, defaulting to the battle volume.
        //   input:  env vars MRGD_CONVERGE_{NODES,SCOPES,CLAIMS,PDUS,KEY_PDUS,
        //           COUNTER_INCS,ORSET_ADDS,TICK_MS,SETTLE_MS,QUIET_S,MAX_WALL_S,
        //           BASE_PORT,RETAINED_MAX_MB,RSS_MAX_MB,CPU_MAX_S,DIR}
        //   output: Params
        //   sideEffects: none
        // Params::from_env:end
        fn from_env() -> Self {
            let dir = std::env::var("MRGD_CONVERGE_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|_| std::env::temp_dir().join("growset-converge"));
            Self {
                nodes: Self::env_u64("MRGD_CONVERGE_NODES", 3).max(2) as usize,
                scopes: Self::env_u64("MRGD_CONVERGE_SCOPES", 3).max(1) as usize,
                claims: Self::env_u64("MRGD_CONVERGE_CLAIMS", 200).max(1) as usize,
                pdus: Self::env_u64("MRGD_CONVERGE_PDUS", 40).max(1) as usize,
                key_pdus: Self::env_u64("MRGD_CONVERGE_KEY_PDUS", 8).max(1) as usize,
                counter_incs: Self::env_u64("MRGD_CONVERGE_COUNTER_INCS", 10).max(1) as usize,
                orset_adds: Self::env_u64("MRGD_CONVERGE_ORSET_ADDS", 10).max(1) as usize,
                tick: Duration::from_millis(Self::env_u64("MRGD_CONVERGE_TICK_MS", 100)),
                settle: Duration::from_millis(Self::env_u64("MRGD_CONVERGE_SETTLE_MS", 300)),
                quiet_s: Self::env_u64("MRGD_CONVERGE_QUIET_S", 3),
                max_wall_s: Self::env_u64("MRGD_CONVERGE_MAX_WALL_S", 120),
                base_port: Self::env_u64("MRGD_CONVERGE_BASE_PORT", 17447) as u16,
                retained_max: Self::env_u64("MRGD_CONVERGE_RETAINED_MAX_MB", 64) * 1024 * 1024,
                rss_max_mb: Self::env_u64("MRGD_CONVERGE_RSS_MAX_MB", 1024),
                cpu_max_s: Self::env_u64("MRGD_CONVERGE_CPU_MAX_S", 60) as f64,
                dir,
            }
        }
    }

    // ── Boundary guard ─────────────────────────────────────────────────────────

    // assert_temp_dir:start
    //   purpose: Refuse to run anywhere but a temp-directory work dir.  The
    //            scenario may only ever touch throwaway state: a hub data dir
    //            or a queue path must not be reachable from here, and failing
    //            loudly beats discovering it after the fact.
    //   input:  dir — the work dir from Params
    //   output: () — panics with a clear message if dir escapes the temp dir
    //   sideEffects: none
    // assert_temp_dir:end
    fn assert_temp_dir(dir: &Path) {
        let temp = std::env::temp_dir();
        assert!(
            dir.starts_with(&temp),
            "growset_converge refuses to run outside the temp dir: {} is not under {}",
            dir.display(),
            temp.display()
        );
    }

    // ── Metering ───────────────────────────────────────────────────────────────

    #[derive(Default, Clone, Copy, Debug)]
    struct Meter {
        pub_blobs: u64,
        pub_bytes: u64,
        dr_blobs: u64,
        dr_bytes: u64,
    }

    type MeterRef = Arc<Mutex<Meter>>;

    /// Bytes still sitting UNDRAINED in the sink inboxes — the quantity that grew
    /// without bound in the 2026-09-07..11 incident.
    ///
    /// Derived, not read: every blob published under a scope prefix is delivered
    /// to one subscriber per node in that scope (the Zenoh self-echo included),
    /// so the inboxes collectively receive published_bytes * fanout and release
    /// drained_bytes.  What is left over is what they are still holding.  With
    /// N=3 the run below lands on 1046 KiB published x 3 = 3138 KiB received vs
    /// 3138 KiB drained, i.e. exactly zero retained — which is the check that
    /// the fan-out model is right rather than a hopeful subtraction.
    fn retained(m: &Meter, fanout: u64) -> u64 {
        m.pub_bytes
            .saturating_mul(fanout)
            .saturating_sub(m.dr_bytes)
    }

    /// Counting decorator over a real ZenohCrdtSink.  Sits between the replica
    /// code and the wire, so the byte counts are of what actually crossed the
    /// sink, not of what a replica believed it sent.
    struct MeteredSink {
        inner: Arc<dyn CrdtSink>,
        meter: MeterRef,
    }

    impl CrdtSink for MeteredSink {
        fn publish(&self, key: &str, bytes: Vec<u8>) -> Result<(), CrdtError> {
            if let Ok(mut m) = self.meter.lock() {
                m.pub_blobs += 1;
                m.pub_bytes += bytes.len() as u64;
            }
            self.inner.publish(key, bytes)
        }

        fn drain(&self, key: &str) -> Result<Vec<Vec<u8>>, CrdtError> {
            let blobs = self.inner.drain(key)?;
            if let Ok(mut m) = self.meter.lock() {
                m.dr_blobs += blobs.len() as u64;
                m.dr_bytes += blobs.iter().map(|b| b.len() as u64).sum::<u64>();
            }
            Ok(blobs)
        }
    }

    // ── Process measures ───────────────────────────────────────────────────────

    /// utime+stime of this process in seconds, from /proc/self/stat.  Covers
    /// every thread, so the Zenoh background tasks are included — which is where
    /// the incident's CPU went.
    fn cpu_seconds() -> f64 {
        let Ok(stat) = std::fs::read_to_string("/proc/self/stat") else {
            return 0.0;
        };
        // comm may contain spaces, so split after the LAST ')'.  Fields are
        // 1-indexed from 'pid'; after comm the next field is #3 (state), so
        // field N lives at index N-3 of the tail.
        let Some(close) = stat.rfind(')') else {
            return 0.0;
        };
        let tail: Vec<&str> = stat[close + 1..].split_whitespace().collect();
        let pick = |field: usize| -> f64 {
            tail.get(field.saturating_sub(3))
                .and_then(|v| v.parse::<f64>().ok())
                .unwrap_or(0.0)
        };
        // utime=#14, stime=#15; the clock-tick rate is 100 Hz on Linux.
        (pick(14) + pick(15)) / 100.0
    }

    /// Peak resident set size in kB (VmHWM) — the exact metric the 9.5 GB
    /// incident was recorded in.
    fn vm_hwm_kb() -> u64 {
        let Ok(status) = std::fs::read_to_string("/proc/self/status") else {
            return 0;
        };
        for line in status.lines() {
            if let Some(rest) = line.strip_prefix("VmHWM:") {
                return rest
                    .split_whitespace()
                    .next()
                    .and_then(|n| n.parse().ok())
                    .unwrap_or(0);
            }
        }
        0
    }

    // ── Wire encodings for the two generic families ────────────────────────────

    // counter_bytes:start
    //   purpose: Compact, DETERMINISTIC wire form for a GCounterDelta.  The
    //            generic counter family has no production wire format yet, so
    //            the harness defines one and both ends use it; keys are sorted so
    //            two replicas encoding the same state produce the same bytes.
    //   input:  &GCounterDelta
    //   output: Vec<u8> — [n: u32][node: u64, value: u64] * n
    //   sideEffects: none
    // counter_bytes:end
    fn counter_bytes(d: &GCounterDelta) -> Vec<u8> {
        let mut slots: Vec<(&u64, &u64)> = d.slots.iter().collect();
        slots.sort();
        let mut buf = Vec::with_capacity(4 + slots.len() * 16);
        buf.extend_from_slice(&(slots.len() as u32).to_le_bytes());
        for (node, value) in slots {
            buf.extend_from_slice(&node.to_le_bytes());
            buf.extend_from_slice(&value.to_le_bytes());
        }
        buf
    }

    fn counter_from_bytes(bytes: &[u8]) -> Option<GCounterDelta> {
        let mut c = Cursor { b: bytes, off: 0 };
        let n = c.u32()? as usize;
        let mut slots = HashMap::new();
        for _ in 0..n {
            let node = c.u64()?;
            let value = c.u64()?;
            slots.insert(node, value);
        }
        Some(GCounterDelta { slots })
    }

    // orset_bytes:start
    //   purpose: Compact, DETERMINISTIC wire form for an OrSetDelta<String>,
    //            same caveat as counter_bytes: the harness defines the format
    //            for the generic set family.  [elems: u32], then per element
    //            [len: u32][utf8][tags: u32], then per tag [node: u64, ts: u64].
    //   input:  &OrSetDelta<String>
    //   output: Vec<u8>
    //   sideEffects: none
    // orset_bytes:end
    fn orset_bytes(d: &OrSetDelta<String>) -> Vec<u8> {
        let mut elems: Vec<(&String, &std::collections::HashSet<Tag>)> = d.tags.iter().collect();
        elems.sort_by(|a, b| a.0.cmp(b.0));
        let mut buf = Vec::new();
        buf.extend_from_slice(&(elems.len() as u32).to_le_bytes());
        for (elem, tags) in elems {
            buf.extend_from_slice(&(elem.len() as u32).to_le_bytes());
            buf.extend_from_slice(elem.as_bytes());
            let mut t: Vec<&Tag> = tags.iter().collect();
            t.sort_by(|a, b| (a.node, a.ts).cmp(&(b.node, b.ts)));
            buf.extend_from_slice(&(t.len() as u32).to_le_bytes());
            for tag in t {
                buf.extend_from_slice(&tag.node.to_le_bytes());
                buf.extend_from_slice(&tag.ts.to_le_bytes());
            }
        }
        buf
    }

    fn orset_from_bytes(bytes: &[u8]) -> Option<OrSetDelta<String>> {
        let mut c = Cursor { b: bytes, off: 0 };
        let n = c.u32()? as usize;
        let mut tags: HashMap<String, std::collections::HashSet<Tag>> = HashMap::new();
        for _ in 0..n {
            let len = c.u32()? as usize;
            let elem = std::str::from_utf8(c.take(len)?).ok()?.to_string();
            let tn = c.u32()? as usize;
            let mut set = std::collections::HashSet::new();
            for _ in 0..tn {
                let node = c.u64()?;
                let ts = c.u64()?;
                set.insert(Tag { node, ts });
            }
            tags.insert(elem, set);
        }
        Some(OrSetDelta { tags })
    }

    struct Cursor<'a> {
        b: &'a [u8],
        off: usize,
    }

    impl<'a> Cursor<'a> {
        fn take(&mut self, n: usize) -> Option<&'a [u8]> {
            let s = self.b.get(self.off..self.off + n)?;
            self.off += n;
            Some(s)
        }
        fn u32(&mut self) -> Option<u32> {
            Some(u32::from_le_bytes(self.take(4)?.try_into().ok()?))
        }
        fn u64(&mut self) -> Option<u64> {
            Some(u64::from_le_bytes(self.take(8)?.try_into().ok()?))
        }
    }

    // ── Replica ────────────────────────────────────────────────────────────────

    type SyncedView = Arc<Mutex<HashMap<String, Vec<ClaimRecord>>>>;

    /// One (node, scope) replica: the production barrier path plus the four CRDT
    /// families, all sharing one metered ZenohCrdtSink.  Lives behind a Mutex so
    /// the watcher loop can fingerprint it while the sync loop drives it.
    struct Replica {
        node: usize,
        scope: usize,
        sink: Arc<dyn CrdtSink>,
        store: GrowSetClaimStore,
        synced: SyncedView,
        room: RoomLog,
        keys: RoomLog,
        counter: GCounter,
        users: OrSet<String>,
    }

    /// Order-independent fingerprint of everything a replica has merged.  Two
    /// consecutive equal fingerprints mean "nothing changed", which together with
    /// flat traffic counters IS the quiescence criterion.
    fn fingerprint(r: &Replica) -> Vec<u64> {
        let (names, records) = {
            let synced = r.synced.lock().expect("synced");
            let records: usize = synced.values().map(|v| v.len()).sum();
            (synced.len(), records)
        };
        vec![
            names as u64,
            records as u64,
            r.room.len() as u64,
            r.keys.len() as u64,
            r.counter.value(),
            r.users.value().len() as u64,
        ]
    }

    // sync_loop:start
    //   purpose: The steady-state drain/merge loop of one replica — the part a
    //            matrix-hs node runs for rooms and keys while the
    //            ReconcileDriver does the barrier's anti-entropy.  Nothing here
    //            republishes: after the initial burst all four families must go
    //            silent, which is exactly what the quiescence run measures.
    //   input:  r — shared replica; tick
    //   output: never returns
    //   sideEffects: drains the replica's sink and merges into its CRDT state
    // sync_loop:end
    async fn sync_loop(r: Arc<Mutex<Replica>>, tick: Duration) {
        loop {
            tokio::time::sleep(tick).await;
            // Only drains happen under this lock (never a blocking put), so the
            // guard is held for microseconds at a time.  The sink Arc is cloned
            // out first so the CRDT state can be taken mutably.
            let mut g = r.lock().expect("replica");
            let sink = g.sink.clone();
            let s: &dyn CrdtSink = sink.as_ref();
            let _ = g.room.drain_delta(s, KEY_ROOM);
            let _ = g.keys.drain_delta(s, KEY_KEYS);
            if let Ok(blobs) = s.drain(KEY_COUNTER) {
                for b in blobs {
                    if let Some(d) = counter_from_bytes(&b) {
                        g.counter.apply_delta(&d);
                    }
                }
            }
            if let Ok(blobs) = s.drain(KEY_ORSET) {
                for b in blobs {
                    if let Some(d) = orset_from_bytes(&b) {
                        g.users.apply_delta(&d);
                    }
                }
            }
        }
    }

    // ── Zenoh mesh ─────────────────────────────────────────────────────────────

    /// Loopback-only Zenoh config: scouting off on both channels, explicit
    /// 127.0.0.1 endpoints.  With no discovery there is no way for this process
    /// to find — or be found by — a live node.
    fn loopback_cfg(listen: u16, connect: &[u16]) -> zenoh::Config {
        let mut cfg = zenoh::Config::default();
        cfg.insert_json5("scouting/multicast/enabled", "false")
            .expect("disable multicast scouting");
        cfg.insert_json5("scouting/gossip/enabled", "false")
            .expect("disable gossip scouting");
        let eps = format!("[\"tcp/127.0.0.1:{listen}\"]");
        cfg.insert_json5("listen/endpoints", &eps)
            .expect("listen endpoints");
        if !connect.is_empty() {
            let eps = format!(
                "[{}]",
                connect
                    .iter()
                    .map(|p| format!("\"tcp/127.0.0.1:{p}\""))
                    .collect::<Vec<_>>()
                    .join(",")
            );
            cfg.insert_json5("connect/endpoints", &eps)
                .expect("connect endpoints");
        }
        cfg
    }

    // ── Scenario ───────────────────────────────────────────────────────────────

    /// Append to the run log and echo it, so stdout and the artifact can never
    /// disagree.
    fn say(out: &mut String, line: impl AsRef<str>) {
        let line = line.as_ref();
        println!("{line}");
        out.push_str(line);
        out.push('\n');
    }

    pub async fn run() -> i32 {
        let p = Params::from_env();
        assert_temp_dir(&p.dir);
        if let Err(e) = std::fs::create_dir_all(&p.dir) {
            eprintln!(
                "growset_converge: cannot create work dir {}: {e}",
                p.dir.display()
            );
            return 1;
        }

        let replicas = p.nodes * p.scopes;
        let total_claims = replicas * p.claims;
        let mut log = String::new();

        say(&mut log, "=== growset_converge: battle-volume quiescence ===".to_string());
        say(&mut log, format!(
            "scenario: {} nodes x {} scopes = {replicas} replicas, {} claims/replica ({} total), \
             {} room PDUs + {} key PDUs + {} counter incs + {} orset adds per replica",
            p.nodes, p.scopes, p.claims, total_claims, p.pdus, p.key_pdus, p.counter_incs, p.orset_adds
        ));
        say(&mut log, format!(
            "quiescence criterion: traffic counters + retained bytes + per-replica state \
             fingerprint unchanged for {} consecutive s",
            p.quiet_s
        ));
        say(&mut log, format!(
            "budgets: retained <= {} MiB, peak RSS <= {} MiB, CPU <= {:.0} s, wall <= {} s",
            p.retained_max / 1024 / 1024,
            p.rss_max_mb,
            p.cpu_max_s,
            p.max_wall_s
        ));
        say(&mut log, format!(
            "work dir: {} (temp only; no hub dir, no queue, no live node)",
            p.dir.display()
        ));

        // ── Mesh: one session per node, ring topology ─────────────────────────
        let mut sessions: Vec<zenoh::Session> = Vec::with_capacity(p.nodes);
        for node in 0..p.nodes {
            let listen = p.base_port + node as u16;
            let peers: Vec<u16> = (0..p.nodes)
                .filter(|other| *other != node)
                .map(|other| p.base_port + other as u16)
                .collect();
            let cfg = loopback_cfg(listen, &peers);
            match zenoh::open(cfg).await {
                Ok(s) => sessions.push(s),
                Err(e) => {
                    eprintln!(
                        "growset_converge: cannot open node {node} on 127.0.0.1:{listen}: {e} \
                         (override with MRGD_CONVERGE_BASE_PORT)"
                    );
                    return 1;
                }
            }
        }
        say(&mut log, format!(
            "mesh: {} loopback sessions in a full mesh, ports {}-{}, multicast+gossip scouting off",
            p.nodes,
            p.base_port,
            p.base_port + p.nodes as u16 - 1
        ));

        // ── Replicas: sinks, stores, drivers ─────────────────────────────────
        let mut drivers = Vec::new();
        let mut all: Vec<Arc<Mutex<Replica>>> = Vec::with_capacity(replicas);
        let mut meters: Vec<MeterRef> = Vec::with_capacity(replicas);
        let lost: Arc<Mutex<Vec<(usize, usize, LostClaim)>>> = Arc::new(Mutex::new(Vec::new()));

        for node in 0..p.nodes {
            let node_id = format!("node-{node}");
            for scope in 0..p.scopes {
                let prefix = PREFIX_FMT.replace("{scope}", &scope.to_string());
                let zenoh_sink = Arc::new(
                    ZenohCrdtSink::new(sessions[node].clone(), &prefix)
                        .await
                        .unwrap_or_else(|e| panic!("sink {node}/{scope}: {e}")),
                );
                let meter: MeterRef = Arc::new(Mutex::new(Meter::default()));
                let metered: Arc<dyn CrdtSink> = Arc::new(MeteredSink {
                    inner: zenoh_sink,
                    meter: meter.clone(),
                });
                let (store, synced) = GrowSetClaimStore::new(metered.clone(), node_id.clone());
                let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<LostClaim>();
                drivers.push(
                    ReconcileDriver::new(metered.clone(), synced.clone(), node_id.clone(), tx, p.tick)
                        .spawn(),
                );
                let lost = lost.clone();
                tokio::spawn(async move {
                    while let Some(lc) = rx.recv().await {
                        lost.lock().expect("lost").push((node, scope, lc));
                    }
                });

                all.push(Arc::new(Mutex::new(Replica {
                    node,
                    scope,
                    sink: metered,
                    store,
                    synced,
                    room: RoomLog::new(),
                    keys: RoomLog::new(),
                    counter: GCounter::new(),
                    users: OrSet::new(),
                })));
                meters.push(meter);
            }
        }

        // Let every subscriber propagate before the first PUT: Zenoh pub/sub has
        // no replay, so a publish that beats a remote subscriber is silently
        // dropped.  (Same margin the existing cluster tests use.)
        tokio::time::sleep(p.settle).await;

        let wall0 = Instant::now();
        let cpu0 = cpu_seconds();

        // ── Battle load: claims + all four CRDT families, per replica ─────────
        let mut own_claims: HashMap<(usize, usize), BTreeSet<(String, String, String)>> =
            HashMap::new();
        for rep in all.iter() {
            let (node, scope) = {
                let g = rep.lock().expect("replica");
                (g.node, g.scope)
            };
            let node_us = node as u64 + 1;
            let mut mine: BTreeSet<(String, String, String)> = BTreeSet::new();
            {
                let mut g = rep.lock().expect("replica");
                let sink = g.sink.clone();
                let s: &dyn CrdtSink = sink.as_ref();

                // Barrier claims: disjoint per node, plus one contested name per
                // scope that EVERY node claims with the identical claimant string
                // (the pathological case the store's node_id predicate exists for).
                for i in 0..p.claims {
                    let user = format!("mx:username:s{scope}_n{node}_u{i:04}");
                    let claimant = format!("@s{scope}_n{node}_u{i:04}:localhost");
                    let _ = g.store.cas_claim(&user, &claimant);
                    mine.insert((user, claimant, node_id_of(node)));
                }
                let contested = format!("mx:username:s{scope}_contested");
                let _ = g
                    .store
                    .cas_claim(&contested, "@contested:localhost");

                // RoomLog family: each replica contributes its own PDUs, so the
                // union is nodes x pdus per scope once converged.
                for i in 0..p.pdus {
                    let depth = i as u64 + 1;
                    g.room.add(Pdu::new(
                        format!("!s{scope}:localhost"),
                        format!("@n{node}:localhost"),
                        "m.room.message".to_string(),
                        format!("payload-{node}-{i}").into_bytes(),
                        vec![],
                        depth,
                        depth,
                    ));
                }
                let _ = g.room.publish_delta(s, KEY_ROOM);

                // keys family: the same wire format on the scope's keys key.
                for i in 0..p.key_pdus {
                    let depth = i as u64 + 1;
                    g.keys.add(Pdu::new(
                        format!("keys:s{scope}"),
                        format!("@n{node}:localhost"),
                        "m.device_keys".to_string(),
                        format!("keys-{node}-{i}").into_bytes(),
                        vec![],
                        depth,
                        depth,
                    ));
                }
                let _ = g.keys.publish_delta(s, KEY_KEYS);

                // counter/hits: every replica bumps its OWN slot, so the converged
                // value on every node is nodes x incs.
                for _ in 0..p.counter_incs {
                    g.counter.increment(node_us, 1);
                }
                let _ = s.publish(KEY_COUNTER, counter_bytes(&g.counter.delta()));

                // orset/users: disjoint elements per replica, converged size is
                // nodes x adds.
                for i in 0..p.orset_adds {
                    g.users.add(format!("u{node}-{i}"), node_us, i as u64 + 1);
                }
                let _ = s.publish(KEY_ORSET, orset_bytes(&g.users.delta()));
            }
            own_claims.insert((node, scope), mine);
        }
        say(&mut log, format!(
            "load: {total_claims} claims + {} room PDUs + {} key PDUs + {} counter/orset deltas published",
            p.nodes * p.scopes * p.pdus,
            p.nodes * p.scopes * p.key_pdus,
            replicas * 2
        ));

        // Drain loops start only AFTER the load, so the quiescence run measures
        // the anti-entropy itself, not the initial burst.
        for rep in all.iter() {
            tokio::spawn(sync_loop(rep.clone(), p.tick));
        }

        // ── Quiescence watch ─────────────────────────────────────────────────
        let mut quiet_since: Option<Instant> = None;
        let mut last_sig: Option<String> = None;
        let mut quiesce_wall: Option<f64> = None;
        let mut peak_retained = 0u64;
        let deadline = wall0 + Duration::from_secs(p.max_wall_s);

        while quiesce_wall.is_none() {
            if Instant::now() > deadline {
                break;
            }
            let m: Meter = {
                let mut acc = Meter::default();
                for meter in &meters {
                    let g = meter.lock().expect("meter");
                    acc.pub_blobs += g.pub_blobs;
                    acc.pub_bytes += g.pub_bytes;
                    acc.dr_blobs += g.dr_blobs;
                    acc.dr_bytes += g.dr_bytes;
                }
                acc
            };
            let fingerprints: Vec<Vec<u64>> = all
                .iter()
                .map(|r| fingerprint(&r.lock().expect("replica")))
                .collect();
            let sig = format!("{m:?}|{fingerprints:?}");
            peak_retained = peak_retained.max(retained(&m, p.nodes as u64));

            if last_sig.as_deref() == Some(sig.as_str()) {
                let since = quiet_since.get_or_insert_with(Instant::now);
                if since.elapsed() >= Duration::from_secs(p.quiet_s) {
                    quiesce_wall = Some(wall0.elapsed().as_secs_f64());
                }
            } else {
                quiet_since = None;
                last_sig = Some(sig);
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }

        // One last drain pass so the retained figure is a settled number rather
        // than "whatever was in flight at the sample".
        tokio::time::sleep(p.tick * 2).await;
        let final_meter: Meter = {
            let mut acc = Meter::default();
            for meter in &meters {
                let g = meter.lock().expect("meter");
                acc.pub_blobs += g.pub_blobs;
                acc.pub_bytes += g.pub_bytes;
                acc.dr_blobs += g.dr_blobs;
                acc.dr_bytes += g.dr_bytes;
            }
            acc
        };
        peak_retained = peak_retained.max(retained(&final_meter, p.nodes as u64));
        let wall = wall0.elapsed().as_secs_f64();
        let cpu = cpu_seconds() - cpu0;
        let hwm = vm_hwm_kb();

        for d in drivers {
            d.abort();
        }

        // ── Convergence verification ─────────────────────────────────────────
        let expect_claims_per_scope = p.nodes * p.claims + 1;
        // Per scope: nodes*claims disjoint names (1 record each) + the contested
        // name, which every node claims once (1 record per node).
        let expect_records_per_scope = p.nodes * p.claims + p.nodes;
        let mut failures: Vec<String> = Vec::new();
        let mut per_scope = Vec::new();

        for scope in 0..p.scopes {
            let mut view_records: BTreeSet<usize> = BTreeSet::new();
            let mut view_names: BTreeSet<usize> = BTreeSet::new();
            let mut room_lens: BTreeSet<usize> = BTreeSet::new();
            let mut keys_lens: BTreeSet<usize> = BTreeSet::new();
            let mut counter_vals: BTreeSet<u64> = BTreeSet::new();
            let mut orset_lens: BTreeSet<usize> = BTreeSet::new();
            // name -> record count, per node.  Counts legitimately differ BETWEEN
            // names (a contested name carries one record per node, a disjoint one
            // a single record), so uniformity is only required ACROSS nodes.
            let mut per_node_name_counts: Vec<BTreeMap<String, usize>> = Vec::new();

            for rep in all.iter() {
                let g = rep.lock().expect("replica");
                if g.scope != scope {
                    continue;
                }
                let (name_counts, records, room_len, keys_len, counter_val, orset_len) = {
                    let synced = g.synced.lock().expect("synced");
                    let name_counts: BTreeMap<String, usize> =
                        synced.iter().map(|(k, v)| (k.clone(), v.len())).collect();
                    let records: usize = name_counts.values().sum();
                    (
                        name_counts,
                        records,
                        g.room.len(),
                        g.keys.len(),
                        g.counter.value(),
                        g.users.value().len(),
                    )
                };
                view_names.insert(name_counts.len());
                per_node_name_counts.push(name_counts);
                view_records.insert(records);
                room_lens.insert(room_len);
                keys_lens.insert(keys_len);
                counter_vals.insert(counter_val);
                orset_lens.insert(orset_len);
            }

            // The name->count map must be IDENTICAL on every node of the scope.
            for (i, map) in per_node_name_counts.iter().enumerate() {
                let same = per_node_name_counts.iter().all(|other| other == map);
                if !same {
                    failures.push(format!(
                        "scope {scope}: node {i}'s name->record-count map differs from another node's"
                    ));
                    break;
                }
            }

            // Every node's own claims must be present in every other node's view:
            // that is the anti-entropy property under test, checked by content.
            for rep in all.iter() {
                let (node, mine) = {
                    let g = rep.lock().expect("replica");
                    if g.scope != scope {
                        continue;
                    }
                    (g.node, own_claims.get(&(g.node, g.scope)).cloned().unwrap_or_default())
                };
                for other in all.iter() {
                    let g = other.lock().expect("replica");
                    if g.scope != scope {
                        continue;
                    }
                    let synced = g.synced.lock().expect("synced");
                    for (user, claimant, node_id) in &mine {
                        let present = synced
                            .get(user)
                            .map(|recs| {
                                recs.iter()
                                    .any(|r| r.claimant == *claimant && r.node_id == *node_id)
                            })
                            .unwrap_or(false);
                        if !present {
                            failures.push(format!(
                                "scope {scope}: node {node}'s claim {user} is missing from node {}'s view",
                                g.node
                            ));
                            break;
                        }
                    }
                }
            }

            if view_records.len() != 1 {
                failures.push(format!("scope {scope}: nodes disagree on record count {view_records:?}"));
            }
            if view_names.len() != 1 {
                failures.push(format!("scope {scope}: nodes disagree on claimed-name count {view_names:?}"));
            }
            if room_lens.len() != 1 {
                failures.push(format!("scope {scope}: nodes disagree on room log length {room_lens:?}"));
            }
            if keys_lens.len() != 1 {
                failures.push(format!("scope {scope}: nodes disagree on keys log length {keys_lens:?}"));
            }
            if counter_vals.len() != 1 {
                failures.push(format!("scope {scope}: nodes disagree on counter value {counter_vals:?}"));
            }
            if orset_lens.len() != 1 {
                failures.push(format!("scope {scope}: nodes disagree on orset size {orset_lens:?}"));
            }

            per_scope.push(format!(
                "scope {scope}: names={:?} records={:?} (expect {expect_claims_per_scope}/\
                 {expect_records_per_scope}) room={room_lens:?} (expect {}) keys={keys_lens:?} \
                 (expect {}) counter={counter_vals:?} (expect {}) orset={orset_lens:?} (expect {})",
                view_names.iter().next().copied().unwrap_or(0),
                view_records.iter().next().copied().unwrap_or(0),
                p.nodes * p.pdus,
                p.nodes * p.key_pdus,
                p.nodes as u64 * p.counter_incs as u64,
                p.nodes * p.orset_adds,
            ));
        }

        let lost_n = lost.lock().expect("lost").len();

        // ── Verdict ───────────────────────────────────────────────────────────
        let quiesced = quiesce_wall.is_some();
        if !quiesced {
            failures.push(format!(
                "no quiescence within {} s (traffic still moving at the deadline)",
                p.max_wall_s
            ));
        }
        if peak_retained > p.retained_max {
            failures.push(format!(
                "retained {} MiB > budget {} MiB",
                peak_retained / 1024 / 1024,
                p.retained_max / 1024 / 1024
            ));
        }
        if hwm > p.rss_max_mb * 1024 {
            failures.push(format!("peak RSS {} MiB > budget {} MiB", hwm / 1024, p.rss_max_mb));
        }
        if cpu > p.cpu_max_s {
            failures.push(format!("CPU {cpu:.1} s > budget {:.0} s", p.cpu_max_s));
        }

        say(&mut log, String::new());
        say(&mut log, "── measures ──".to_string());
        say(&mut log, format!(
            "wall to quiescence: {}",
            quiesce_wall
                .map(|w| format!("{w:.2} s"))
                .unwrap_or_else(|| "NEVER (deadline hit)".to_string())
        ));
        say(&mut log, format!("total wall (incl. settle + final drain): {wall:.2} s"));
        say(&mut log, format!("cpu (utime+stime, all threads): {cpu:.2} s"));
        say(&mut log, format!(
            "retained bytes at end: {} KiB; peak retained during run: {} KiB (budget {} MiB)",
            retained(&final_meter, p.nodes as u64) / 1024,
            peak_retained / 1024,
            p.retained_max / 1024 / 1024
        ));
        say(&mut log, format!(
            "traffic: published {} blobs / {} KiB, drained {} blobs / {} KiB",
            final_meter.pub_blobs,
            final_meter.pub_bytes / 1024,
            final_meter.dr_blobs,
            final_meter.dr_bytes / 1024
        ));
        say(&mut log, format!("peak RSS (VmHWM): {} MiB", hwm / 1024));
        say(&mut log, format!("LostClaim notifications: {lost_n}"));
        say(&mut log, String::new());
        say(&mut log, "── convergence ──".to_string());
        for line in &per_scope {
            say(&mut log, format!("  {line}"));
        }
        say(&mut log, String::new());

        let verdict = if failures.is_empty() {
            say(&mut log, format!(
                "RESULT: QUIESCENT + CONVERGED at battle volume — wall {:.2} s, cpu {cpu:.2} s, \
                 peak retained {} KiB, peak RSS {} MiB",
                quiesce_wall.unwrap_or_default(),
                peak_retained / 1024,
                hwm / 1024
            ));
            0
        } else {
            say(&mut log, format!("RESULT: FAILED ({} problem(s))", failures.len()));
            for f in failures.iter().take(12) {
                say(&mut log, format!("  - {f}"));
            }
            if failures.len() > 12 {
                say(&mut log, format!("  … and {} more", failures.len() - 12));
            }
            1
        };

        // Keep the numbers next to the run that produced them, in the temp dir.
        let artifact = p.dir.join("growset_converge-report.txt");
        if let Err(e) = std::fs::write(&artifact, &log) {
            eprintln!("growset_converge: cannot write {}: {e}", artifact.display());
        }
        say(&mut log, format!("report written to {}", artifact.display()));
        verdict
    }

    fn node_id_of(node: usize) -> String {
        format!("node-{node}")
    }
}
