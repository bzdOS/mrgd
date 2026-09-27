// START_AI_HEADER
// MODULE: matrix-hs/src/hub_replic.rs
// PURPOSE: Replicate a hubd HUB (project cards, journals, task event logs)
//          over the mrgd mesh — the bus replacing hubd's git mesh-sync for
//          nodes that already ride the mesh. Queues are NOT handled here
//          (hubd_bridge.rs does those); this module carries everything else
//          a hubd peer needs to see the shared state:
//            - journal.<node>.jsonl        — append-only, incremental events
//            - tasks.<node>.events.jsonl   — append-only, incremental events
//            - projects/*.md, resources/*.md, sections.json — full-snapshot
//              LWW events (latest event in the room wins)
//          Deliberately NOT replicated: tasks.json (hubd rebuilds it from
//          event logs — verified in hubd core.mjs loadTasks), HUBD.md
//          (generated per node), presence/ (ephemeral, TTL), claims.json
//          (TTL), queues/ (hubd_bridge), .qstate/.mxstate (local cursors).
// INTENT: mrgd was built to BE the transport for this operator's agent
//          coordination (ROADMAP "What this is for"); the git mesh-sync was
//          the interim. This module is the second consumer of that intent
//          after the queue bridge — same wire semantics (content-addressed
//          events, signatures, catch-up after downtime) inherited for free.
// DEPENDENCIES: crate::routes::send::insert_pdu, crate::state::{AppState, StateEvent}
// PUBLIC_API: spawn, hub_room_id, hub_state_events, HUB_CONTENT_KEY
// END_AI_HEADER

use crate::routes::send::insert_pdu;
use crate::state::{AppState, StateEvent};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};

/// Content key for hub-replication events. Mirrors hubd_bridge's
/// net.hubd.queue — a vendor-namespaced bag inside m.room.message content.
pub const HUB_CONTENT_KEY: &str = "net.hubd.hub";

/// The synthetic user hub events are sent by (same as the queue bridge).
pub const HUB_USER: &str = "@hubd:hubd";

/// Fixed room domain (same rationale as the queue rooms).
pub const HUB_ROOM_DOMAIN: &str = "hubd";

/// One room carries the whole hub. Append files key by path; card files take
/// the latest full snapshot per path. A single room means a fresh node
/// catches up with one pass and one history.
pub const HUB_ROOM_ID: &str = "!hubd-hub:hubd";

/// Refuse to ship absurdly large payloads as one event (cards are KB-scale;
/// append chunks are bounded by how much a poll interval can add).
const MAX_EVENT_BODY_BYTES: usize = 4 * 1024 * 1024;

// hub_room_id:start
//   purpose: The well-known room_id for hub replication. Fixed constant —
//            every node derives the same room, like the queue rooms do.
//   input:  none
//   output: room_id string
//   sideEffects: none
// hub_room_id:end
pub fn hub_room_id() -> String {
    HUB_ROOM_ID.to_string()
}

// sha256_hex:start
//   purpose: Hex sha256 of a byte slice — content hashes for card snapshots
//            (loop guard + cheap compare) and state-event ids.
//   input:  bytes
//   output: 64-char lowercase hex
//   sideEffects: none
// sha256_hex:end
fn sha256_hex(bytes: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(bytes);
    h.finalize().iter().map(|b| format!("{b:02x}")).collect()
}

// hub_event_id:start
//   purpose: Deterministic event_id for a seed state event (sha256 over the
//            room + type + state_key) — identical on every node so
//            concurrent seeding converges to a tie, same trick as the queue
//            rooms' state_event_id.
//   input:  event_type, state_key
//   output: "$" + base64url(sha256)
//   sideEffects: none
// hub_event_id:end
fn hub_event_id(event_type: &str, state_key: &str) -> String {
    let mut h = Sha256::new();
    h.update(b"hubd-hub-state\0");
    h.update(event_type.as_bytes());
    h.update([0u8]);
    h.update(HUB_ROOM_ID.as_bytes());
    h.update([0u8]);
    h.update(state_key.as_bytes());
    format!("${}", URL_SAFE_NO_PAD.encode(h.finalize()))
}

// hub_state_events:start
//   purpose: The seed state burst for the hub room — byte-identical on every
//            node (fixed sender, ts 0, sha256 ids, power 100 for HUB_USER)
//            for the same convergence reasons as queue_state_events; see
//            that function's doc comment for the power_levels reasoning.
//   input:  none
//   output: the seed burst
//   sideEffects: none
// hub_state_events:end
pub fn hub_state_events() -> Vec<StateEvent> {
    let mk = |event_type: &str, state_key: &str, content: Value| StateEvent {
        event_type: event_type.to_string(),
        state_key: state_key.to_string(),
        sender: HUB_USER.to_string(),
        content,
        event_id: hub_event_id(event_type, state_key),
        room_id: HUB_ROOM_ID.to_string(),
        // Fixed, not a clock — see queue_state_events.
        origin_server_ts: 0,
    };
    vec![
        mk(
            "m.room.create",
            "",
            json!({
                "creator": HUB_USER,
                "room_version": "10",
                HUB_CONTENT_KEY: { "kind": "hub" }
            }),
        ),
        mk(
            "m.room.member",
            HUB_USER,
            json!({ "membership": "join", "displayname": "hubd hub replication" }),
        ),
        mk(
            "m.room.power_levels",
            "",
            json!({
                "users": { HUB_USER: 100 },
                "users_default": 0,
                "state_default": 50,
                "events_default": 0,
                "ban": 50, "kick": 50, "redact": 50, "invite": 50
            }),
        ),
        mk("m.room.join_rules", "", json!({ "join_rule": "invite" })),
        mk("m.room.history_visibility", "", json!({ "history_visibility": "shared" })),
    ]
}

// ensure_hub_room:start
//   purpose: Find-or-seed the hub room on this node (the queue rooms'
//          ensure_queue_room pattern: idempotent, seeds only when the room
//          has no m.room.create yet).
//   input:  state
//   output: the room_id
//   sideEffects: may seed state; registers an alias
// ensure_hub_room:end
pub fn ensure_hub_room(state: &Arc<AppState>) -> String {
    let room_id = hub_room_id();
    state.ensure_room_state(&room_id);

    let already = state
        .room_state
        .lock()
        .map(|rs| {
            rs.get(&room_id)
                .map(|v| {
                    v.iter()
                        .any(|e| e.event_type == "m.room.create" && e.state_key.is_empty())
                })
                .unwrap_or(false)
        })
        .unwrap_or(false);

    if !already {
        for ev in hub_state_events() {
            if let Err(e) = state.apply_remote_state_event(ev) {
                eprintln!("[matrix-hs] hub-replic: seed state: {e}");
            }
        }
        eprintln!("[matrix-hs] hub-replic: seeded hub room {room_id}");
    }

    if let Ok(mut aliases) = state.aliases.lock() {
        aliases
            .entry(format!("#hubd-hub:{}", state.server_name))
            .or_insert_with(|| room_id.clone());
    }
    room_id
}

// ───────────────────────── config ────────────────────────────────────────────

// HubReplicConfig:start
//   purpose: Everything the replicator needs, resolved once from env.
//            MATRIX_HS_HUBD_HUB_DIR is the on-switch (unset ⇒ module inert).
//            node MUST match the hubd node name on this host (HUBD_NODE or
//            hostname — hubd core.mjs JOURNAL_NODE) because it decides which
//            journal.<node>.jsonl / tasks.<node>.events.jsonl are "ours" to
//            ingest: the per-host files hubd itself writes here.
//   input:  process env
//   output: Option<HubReplicConfig> (None = off)
//   sideEffects: none
// HubReplicConfig:end
pub struct HubReplicConfig {
    pub hub_dir: PathBuf,
    pub node: String,
    pub poll_ms: u64,
    /// Cursor/offset state lives beside the bridge's (.mxstate sibling trick):
    /// <hub_dir>/.mxstate/ — gitignored by the hub, never travels.
    pub state_dir: PathBuf,
}

impl HubReplicConfig {
    // HubReplicConfig::from_env:start
    //   purpose: MATRIX_HS_HUBD_HUB_DIR (on-switch) + MATRIX_HS_HUBD_NODE
    //            (falls back to MATRIX_HS_HUBD_NODE / HUBD_QUEUE_NODE_ID /
    //            hostname, mirroring hubd_bridge's chain) + poll interval
    //            shared with the bridge's MATRIX_HS_HUBD_POLL_MS.
    //   input:  env
    //   output: Option<config>
    //   sideEffects: none
    // HubReplicConfig::from_env:end
    pub fn from_env() -> Option<Self> {
        let hub_dir = std::env::var("MATRIX_HS_HUBD_HUB_DIR").ok()?;
        if hub_dir.trim().is_empty() {
            return None;
        }
        let hub_dir = PathBuf::from(hub_dir);
        // hubd's JOURNAL_NODE (core.mjs) is the hostname lowercased and
        // sanitised — NOT the same name the queue layer uses (nodeName() is
        // case-preserving; on Alpha the queues say "Alpha" while the
        // journals say "alpha"). Ingesting journal.<node>.jsonl /
        // tasks.<node>.events.jsonl keys on JOURNAL_NODE, so apply the same
        // normalisation here or we look for files hubd never wrote.
        let node = crate::hubd_bridge::node_name()
            .split('.')
            .next()
            .unwrap_or("node")
            .to_lowercase()
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '-' })
            .collect::<String>()
            .trim_matches('-')
            .chars()
            .take(40)
            .collect::<String>();
        let node = if node.is_empty() { "node".to_string() } else { node };
        let poll_ms = std::env::var("MATRIX_HS_HUBD_POLL_MS")
            .ok()
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or(1000);
        let state_dir = hub_dir.join(".mxstate");
        Some(Self { hub_dir, node, poll_ms, state_dir })
    }
}

// ───────────────────────── ingest ────────────────────────────────────────────

// append_files_for:start
//   purpose: The per-host append-only files this node ingests — its own
//            journal and its own task event log. Names follow hubd's
//            core.mjs (journal.<JOURNAL_NODE>.jsonl,
//            tasks.<JOURNAL_NODE>.events.jsonl), where JOURNAL_NODE is
//            exactly our cfg.node by construction.
//   input:  cfg
//   output: relative path strings
//   sideEffects: none
// append_files_for:end
fn append_files_for(cfg: &HubReplicConfig) -> Vec<String> {
    vec![
        format!("journal.{}.jsonl", cfg.node),
        format!("tasks.{}.events.jsonl", cfg.node),
    ]
}

// card_targets:start
//   purpose: Enumerate the LWW snapshot files: projects/*.md,
//            resources/*.md, sections.json. Relative paths returned;
//            missing dirs are simply skipped.
//   input:  cfg
//   output: relative path strings
//   sideEffects: none (filesystem reads only)
// card_targets:end
fn card_targets(cfg: &HubReplicConfig) -> Vec<String> {
    let mut out = Vec::new();
    for dir in ["projects", "resources"] {
        if let Ok(rd) = std::fs::read_dir(cfg.hub_dir.join(dir)) {
            for e in rd.flatten() {
                if e.path().extension().and_then(|x| x.to_str()) == Some("md") {
                    if let Ok(rel) = e.path().strip_prefix(&cfg.hub_dir) {
                        out.push(rel.to_string_lossy().replace('\\', "/"));
                    }
                }
            }
        }
    }
    if cfg.hub_dir.join("sections.json").exists() {
        out.push("sections.json".to_string());
    }
    out.sort();
    out
}

// read_off/write_off + card hash state:start
//   purpose: Persisted cursors. Offsets: byte position per append file.
//            Cards: a small JSON map path → {hash, applied} where hash is
//            the last content hash WE ingested or materialised — the loop
//            guard: a file whose hash equals the recorded one is skipped on
//            ingest, so materialising a peer's card never echoes it back.
//   input:  state_dir
//   output: offset u64 / HashMap<String, CardState>
//   sideEffects: writes on update
// read_off/write_off + card hash state:end
#[derive(Default)]
struct CardMem {
    cards: HashMap<String, CardState>,
    dirty: bool,
}

#[derive(serde::Deserialize, serde::Serialize, Clone, Default)]
struct CardState {
    hash: String,
    /// true when the recorded hash came from MATERIALIZING a peer event
    /// (not our own edit) — ingest must skip it; false/absent = our own.
    applied: bool,
}

fn read_off(state_dir: &Path, name: &str) -> u64 {
    std::fs::read_to_string(state_dir.join(format!("hubrep-{name}.offset")))
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(0)
}

fn write_off(state_dir: &Path, name: &str, off: u64) {
    let _ = std::fs::create_dir_all(state_dir);
    let _ = std::fs::write(state_dir.join(format!("hubrep-{name}.offset")), off.to_string());
}

fn card_state_path(state_dir: &Path) -> PathBuf {
    state_dir.join("hubrep-cards.json")
}

fn load_card_mem(state_dir: &Path) -> CardMem {
    match std::fs::read_to_string(card_state_path(state_dir)) {
        Ok(s) => serde_json::from_str::<HashMap<String, CardState>>(&s)
            .map(|cards| CardMem { cards, dirty: false })
            .unwrap_or_default(),
        Err(_) => CardMem::default(),
    }
}

fn save_card_mem(state_dir: &Path, mem: &CardMem) {
    let _ = std::fs::create_dir_all(state_dir);
    if let Ok(s) = serde_json::to_string(&mem.cards) {
        let _ = std::fs::write(card_state_path(state_dir), s);
    }
}

// publish_event:start
//   purpose: Insert one m.room.message carrying the hub payload — the same
//            insert_pdu path the queue bridge uses, so persistence, cluster
//            publish, notify and signing all behave identically.
//   input:  state, room_id, kind, path, body bytes, cfg.node
//   output: bool (true on success)
//   sideEffects: inserts a PDU (persists; publishes in cluster mode)
// publish_event:end
async fn publish_event(
    state: &Arc<AppState>,
    room_id: &str,
    kind: &str,
    path: &str,
    body: &str,
    node: &str,
) -> bool {
    let content = json!({
        "msgtype": "m.text",
        "body": body,
        HUB_CONTENT_KEY: {
            "kind": kind,
            "path": path,
            "node": node,
            "hash": sha256_hex(body.as_bytes()),
        }
    });
    // Timeline PDUs are signed by THIS node and verified with sender-binding on
    // peers (Pdu::verify_sig: domain(sender) == signer_node), so the message
    // sender must be homed on this server — the same pattern hubd_bridge uses
    // ("@hubd:<server_name>"). HUB_USER ("@hubd:hubd") is only valid for the
    // seed STATE burst, which never passes through PDU verification.
    let sender = format!("@hubd:{}", state.server_name);
    match serde_json::to_vec(&content) {
        Ok(bytes) => match insert_pdu(
            state,
            room_id,
            sender,
            "m.room.message".to_string(),
            bytes,
            json!({}),
        )
        .await
        {
            Ok(_) => true,
            Err(e) => {
                eprintln!("[matrix-hs] hub-replic: publish {path}: {e}");
                false
            }
        },
        Err(e) => {
            eprintln!("[matrix-hs] hub-replic: encode {path}: {e}");
            false
        }
    }
}

// ingest_append_files:start
//   purpose: For each own append-only file: read past the persisted byte
//            offset and publish the new tail as ONE append event (a poll's
//            worth of journal lines is one event — fewer PDUs, and the lines
//            inside stay ordered because a single origin's events form a
//            causal chain). Truncation (offset > size) resets to 0 like
//            hubd's own reader. Only complete trailing newline ships: a
//            half-written line waits for the next tick.
//   input:  state, room_id, cfg
//   output: none
//   sideEffects: publishes events; advances offsets
// ingest_append_files:end
async fn ingest_append_files(state: &Arc<AppState>, room_id: &str, cfg: &HubReplicConfig) {
    for rel in append_files_for(cfg) {
        let path = cfg.hub_dir.join(&rel);
        let Ok(meta) = std::fs::metadata(&path) else { continue };
        let size = meta.len();
        let mut off = read_off(&cfg.state_dir, &rel);
        if size < off {
            off = 0;
        }
        if size == off {
            continue;
        }
        let Ok(text) = std::fs::read_to_string(&path) else { continue };
        let start = off as usize;
        if start > text.len() || !text.is_char_boundary(start) {
            write_off(&cfg.state_dir, &rel, 0);
            continue;
        }
        let chunk = &text[start..];
        if !chunk.ends_with('\n') {
            continue; // half-written tail — next tick
        }
        if chunk.len() > MAX_EVENT_BODY_BYTES {
            // A tail this large (a bulk import, a backfill) ships as several
            // line-aligned append events of at most MAX bytes each, order
            // preserved; the offset only advances once every part is out.
            let mut batch = String::new();
            let mut ok = true;
            for line in chunk.split_inclusive('\n') {
                batch.push_str(line);
                if batch.len() >= MAX_EVENT_BODY_BYTES {
                    if !publish_event(state, room_id, "append", &rel, &batch, &cfg.node).await {
                        ok = false;
                        break;
                    }
                    batch.clear();
                }
            }
            if ok && !batch.is_empty() {
                ok = publish_event(state, room_id, "append", &rel, &batch, &cfg.node).await;
            }
            if ok {
                write_off(&cfg.state_dir, &rel, size);
            }
            continue;
        }
        if publish_event(state, room_id, "append", &rel, chunk, &cfg.node).await {
            write_off(&cfg.state_dir, &rel, size);
        }
    }
}

// ingest_cards:start
//   purpose: For each LWW snapshot file: hash it; skip when the hash equals
//            the recorded one (loop guard — includes hashes we merely
//            materialised from a peer); publish a full event on change and
//            record the hash as OURS (applied=false), so a peer echoing it
//            back is dropped by materialise's own node check while our next
//            tick sees no change.
//   input:  state, room_id, cfg, card memory (persisted on change)
//   output: none
//   sideEffects: publishes events; updates card memory
// ingest_cards:end
async fn ingest_cards(
    state: &Arc<AppState>,
    room_id: &str,
    cfg: &HubReplicConfig,
    mem: &mut CardMem,
) {
    let mut changed = false;
    for rel in card_targets(cfg) {
        let Ok(bytes) = std::fs::read(cfg.hub_dir.join(&rel)) else { continue };
        if bytes.len() > MAX_EVENT_BODY_BYTES {
            eprintln!(
                "[matrix-hs] hub-replic: {rel} is {} bytes — over the {} snapshot cap, skipped",
                bytes.len(),
                MAX_EVENT_BODY_BYTES
            );
            continue;
        }
        let hash = sha256_hex(&bytes);
        let known = mem.cards.get(&rel);
        if let Some(cs) = known {
            if cs.hash == hash {
                // Unchanged since we last shipped/applied it. If the recorded
                // state was merely "applied from peer", flip it to ours-now:
                // the file content has survived a full round trip, so future
                // edits (hash change) must ship again. applied stays for the
                // unchanged case to keep the guard active.
                continue;
            }
        }
        let Ok(body) = String::from_utf8(bytes) else {
            eprintln!("[matrix-hs] hub-replic: {rel} is not valid UTF-8, skipped");
            continue;
        };
        if publish_event(state, room_id, "full", &rel, &body, &cfg.node).await {
            mem.cards.insert(rel, CardState { hash, applied: false });
            changed = true;
        }
    }
    if changed {
        save_card_mem(&cfg.state_dir, mem);
    }
}

// ───────────────────────── materialise ───────────────────────────────────────

// HubEventView:start
//   purpose: Decoded view of one hub event from the room timeline.
//   input:  client-event JSON value
//   output: Some(HubEventView) for hub events, None for foreign messages
//   sideEffects: none
// HubEventView:end
#[derive(Debug, Clone)]
pub struct HubEventView {
    pub kind: String, // "append" | "full"
    pub path: String,
    pub node: String,
    pub hash: String,
    pub body: String,
}

// decode_hub_event:start
//   purpose: Parse a timeline event into a HubEventView (checks our content
//            key; anything else — including human chat in the room — is not
//            ours to apply).
//   input:  event JSON
//   output: Option<HubEventView>
//   sideEffects: none
// decode_hub_event:end
pub fn decode_hub_event(ev: &Value) -> Option<HubEventView> {
    let meta = ev.get(HUB_CONTENT_KEY)?;
    Some(HubEventView {
        kind: meta.get("kind")?.as_str()?.to_string(),
        path: meta.get("path")?.as_str()?.to_string(),
        node: meta.get("node")?.as_str()?.to_string(),
        hash: meta.get("hash").and_then(|h| h.as_str()).unwrap_or("").to_string(),
        body: ev.get("body")?.as_str()?.to_string(),
    })
}

// sanitize_rel_path:start
//   purpose: A replicated path must stay inside the hub dir: no absolute
//            paths, no "..", no leading '/'. Peers are trusted nodes, but a
//            buggy one must not be able to make us write outside the hub
//            (defence in depth on top of node_auth).
//   input:  relative path from an event
//   output: Some(normalised relative path) or None (rejected)
//   sideEffects: none
// sanitize_rel_path:end
pub fn sanitize_rel_path(rel: &str) -> Option<String> {
    if rel.is_empty() || rel.starts_with('/') || rel.contains("..") || rel.contains('\\') {
        return None;
    }
    Some(rel.to_string())
}

// apply_event_to_disk:start
//   purpose: Apply one decoded hub event to the local hub dir.
//            append → append body to the file (parents created);
//            full → atomically replace the file (tmp + rename).
//   input:  hub_dir, event
//   output: Ok(()) / Err(String)
//   sideEffects: writes files under hub_dir
// apply_event_to_disk:end
fn apply_event_to_disk(hub_dir: &Path, ev: &HubEventView) -> Result<(), String> {
    let rel = sanitize_rel_path(&ev.path).ok_or_else(|| format!("bad path {:?}", ev.path))?;
    let target = hub_dir.join(&rel);
    match ev.kind.as_str() {
        "append" => {
            if let Some(parent) = target.parent() {
                std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
            }
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&target)
                .map_err(|e| e.to_string())?;
            f.write_all(ev.body.as_bytes()).map_err(|e| e.to_string())?;
            Ok(())
        }
        "full" => {
            if let Some(parent) = target.parent() {
                std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
            }
            let tmp = target.with_extension("mrgdhub.tmp");
            std::fs::write(&tmp, ev.body.as_bytes()).map_err(|e| e.to_string())?;
            std::fs::rename(&tmp, &target).map_err(|e| e.to_string())?;
            Ok(())
        }
        other => Err(format!("unknown kind {other:?}")),
    }
}

// room_events:start
//   purpose: The hub room's timeline events in DAG order (RoomLog::ordered),
//            as client-event JSON values.
//   input:  state
//   output: Vec<Value>
//   sideEffects: none
// room_events:end
fn room_events(state: &Arc<AppState>) -> Vec<Value> {
    let rooms = match state.rooms.lock() {
        Ok(g) => g,
        Err(_) => return Vec::new(),
    };
    let Some(log) = rooms.get(&hub_room_id()) else {
        return Vec::new();
    };
    log.ordered()
        .iter()
        .filter_map(|p| {
            let mut ev: Value = serde_json::from_slice(&p.content).ok()?;
            if let Some(obj) = ev.as_object_mut() {
                obj.insert("event_id".to_string(), Value::String(p.event_id.clone()));
                obj.insert("sender".to_string(), Value::String(p.sender.clone()));
                obj.insert("ts".to_string(), Value::from(p.ts));
            }
            Some(ev)
        })
        .collect()
}

// materialise:start
//   purpose: Bring the local hub dir in line with the room.
//
//            Cursor: the event_id of the last applied event
//            (.mxstate/hubrep-cursor). Missing/stale (an id we cannot find
//            in the room) ⇒ full rebuild from event 0: fold every foreign
//            event in order — appends concatenate per path, fulls replace —
//            then write each touched path once. Rebuild is idempotent and
//            duplicate-free because it is driven by event ORDER, not content
//            matching.
//
//            Incremental pass: apply each foreign event after the cursor to
//            disk directly (append appends; full replaces), then advance the
//            cursor. Own-origin events (node == self) advance the cursor but
//            never touch disk — that is the loop prevention, same asymmetry
//            as the queue bridge.
//
//            Card loop-guard bookkeeping: after applying a foreign full
//            event, record its hash as applied so OUR ingest tick does not
//            immediately re-publish the file we just wrote.
//
//            LWW conflict note (deliberate v1 semantics): when two nodes
//            edit the same card between syncs, the LATER event in room order
//            wins and the earlier edit is lost. Git mesh-sync would have
//            refused the merge; the bus converges silently. Cards are
//            edited where they live (overwhelmingly one node); concurrent
//            same-card edits across nodes are a workflow error either way.
//            Recorded here so nobody is surprised.
//   input:  state, cfg, card memory
//   output: none
//   sideEffects: writes hub files, cursor, card memory
// materialise:end
async fn materialise(state: &Arc<AppState>, cfg: &HubReplicConfig, mem: &mut CardMem) {
    let events = room_events(state);
    if events.is_empty() {
        return;
    }
    let cursor_path = cfg.state_dir.join("hubrep-cursor");
    let last = std::fs::read_to_string(&cursor_path).ok().map(|s| s.trim().to_string());

    // Resolve cursor position in the room (index of the event AFTER it).
    let start_idx = match &last {
        Some(id) => match events.iter().position(|e| e.get("event_id").and_then(|v| v.as_str()) == Some(id.as_str())) {
            Some(i) => i + 1,
            None => 0, // cursor unknown here — rebuild
        },
        None => 0,
    };

    let rebuild = start_idx == 0
        && last.as_ref().is_some_and(|id| !id.is_empty())
        || last.is_none();
    let foreign = |ev: &Value| -> bool {
        decode_hub_event(ev).is_some_and(|h| h.node != cfg.node)
    };

    if rebuild {
        // Fold the whole room: per-path final state for foreign events, in order.
        let mut appends: HashMap<String, String> = HashMap::new();
        let mut fulls: HashMap<String, (String, String)> = HashMap::new(); // path → (hash, body)
        let mut order: Vec<String> = Vec::new(); // paths touched, first-touch order
        let note = |m: &mut Vec<String>, p: String| {
            if !m.contains(&p) {
                m.push(p);
            }
        };
        for ev in &events {
            let Some(h) = decode_hub_event(ev) else { continue };
            if h.node == cfg.node {
                continue;
            }
            let Some(rel) = sanitize_rel_path(&h.path) else { continue };
            match h.kind.as_str() {
                "append" => {
                    note(&mut order, rel.clone());
                    appends.entry(rel.clone()).or_default().push_str(&h.body);
                }
                "full" => {
                    note(&mut order, rel.clone());
                    fulls.insert(rel.clone(), (h.hash.clone(), h.body.clone()));
                    appends.remove(&rel); // a later full supersedes earlier appends
                }
                _ => {}
            }
        }
        for rel in order {
            if let Some((hash, body)) = fulls.get(&rel) {
                let view = HubEventView {
                    kind: "full".into(),
                    path: rel.clone(),
                    node: "peer".into(),
                    hash: hash.clone(),
                    body: body.clone(),
                };
                if apply_event_to_disk(&cfg.hub_dir, &view).is_ok() {
                    mem.cards.insert(rel.clone(), CardState { hash: hash.clone(), applied: true });
                }
            } else if let Some(body) = appends.get(&rel) {
                let view = HubEventView {
                    kind: "append".into(),
                    path: rel.clone(),
                    node: "peer".into(),
                    hash: String::new(),
                    body: body.clone(),
                };
                let _ = apply_event_to_disk(&cfg.hub_dir, &view);
            }
        }
        mem.dirty = true;
    } else {
        for ev in events.iter().skip(start_idx) {
            if !foreign(ev) {
                // own or non-hub event: just advance past it
                continue;
            }
            let Some(h) = decode_hub_event(ev) else { continue };
            if apply_event_to_disk(&cfg.hub_dir, &h).is_ok() && h.kind == "full" {
                mem.cards
                    .insert(h.path.clone(), CardState { hash: h.hash.clone(), applied: true });
                mem.dirty = true;
            }
        }
    }

    // Advance the cursor to the last event id we considered.
    if let Some(id) = events
        .last()
        .and_then(|e| e.get("event_id"))
        .and_then(|v| v.as_str())
    {
        let _ = std::fs::create_dir_all(&cfg.state_dir);
        let _ = std::fs::write(&cursor_path, id);
    }
    if mem.dirty {
        save_card_mem(&cfg.state_dir, mem);
        mem.dirty = false;
    }
}

// ───────────────────────── driver ───────────────────────────────────────────

// spawn:start
//   purpose: Start the hub replicator when MATRIX_HS_HUBD_HUB_DIR is set.
//            One loop: drain the cluster (a client-less node must apply
//            inbound deltas itself — the same reason the queue bridge
//            drains), materialise, then ingest. Order matters: materialise
//            first so a card a peer just shipped doesn't race our echo.
//   input:  state
//   output: bool (true = running)
//   sideEffects: spawns a tokio task; reads/writes hub files
// spawn:end
pub fn spawn(state: Arc<AppState>) -> bool {
    let Some(cfg) = HubReplicConfig::from_env() else {
        return false;
    };
    spawn_with(state, cfg)
}

// spawn_with:start
//   purpose: Same driver as spawn but taking an explicit config — the
//            test/programmatic entry point (process env would race across
//            parallel cargo-test threads).
//   input:  state, cfg
//   output: bool (true = running)
//   sideEffects: spawns a tokio task; reads/writes hub files
// spawn_with:end
pub fn spawn_with(state: Arc<AppState>, cfg: HubReplicConfig) -> bool {
    eprintln!(
        "[matrix-hs] hub-replic: hub {} as node '{}' (poll {} ms)",
        cfg.hub_dir.display(),
        cfg.node,
        cfg.poll_ms
    );
    tokio::spawn(async move {
        // Subscribe to the hub room BEFORE anything else. The room's sink is
        // created lazily (discovery only materialises it on the first sample
        // received), so without this every event published before this
        // node's first inbound sample is lost forever — precisely what made
        // the integration test flaky and would make a freshly joined node
        // miss whatever shipped during its first seconds.
        #[cfg(feature = "cluster")]
        if let Some(cluster) = state.cluster.as_ref() {
            match cluster.sink_for(&hub_room_id()).await {
                Ok(_) => {}
                Err(e) => eprintln!("[matrix-hs] hub-replic: subscribe failed: {e}"),
            }
        }
        let mut mem = load_card_mem(&cfg.state_dir);
        loop {
            #[cfg(feature = "cluster")]
            crate::hubd_bridge::drain_cluster(&state).await;
            let room = ensure_hub_room(&state);
            materialise(&state, &cfg, &mut mem).await;
            ingest_cards(&state, &room, &cfg, &mut mem).await;
            ingest_append_files(&state, &room, &cfg).await;
            tokio::time::sleep(std::time::Duration::from_millis(cfg.poll_ms)).await;
        }
    });
    true
}
