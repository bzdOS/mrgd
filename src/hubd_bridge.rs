// START_AI_HEADER
// MODULE: src/hubd_bridge.rs
// PURPOSE: Bridge hubd's per-host queue FILES onto matrix-hs rooms, so a queue entry
//          replicates over the substrate (signed, catch-up, GC, HLC — all of Phase 1/2)
//          instead of over a bespoke fire-and-forget pub/sub with a hand-rolled dedup
//          set. Replaces bsdOS/hubd-queue-repl.
//
//          Off unless MATRIX_HS_HUBD_QUEUES_DIR is set — same shape as
//          MATRIX_HS_SCRIPTS_DIR: unset means the whole module is a no-op.
//
//          ── Why this does not break hubd's MCP tools ──────────────────────────
//          hub_queue_wait tracks a BYTE OFFSET per source file, so a queue file must
//          only ever grow by clean append. hubd already relies on that and gets it by
//          giving every host its own file:
//              queues/<role>.<node>.queue.md   — written ONLY by <node>
//              queues/<role>.queue.md          — legacy shared file, read, never written
//          A reader merges across every file matching ^<role>(\.[^.]+)?\.queue\.md$.
//
//          The bridge keeps that invariant exactly, by splitting the directory into
//          two disjoint halves:
//            INGEST      reads  <role>.<self>.queue.md   (hubd writes it, we only read)
//            MATERIALIZE writes <role>.<other>.queue.md  (we write it, hubd only reads)
//          No file is ever written by both hubd and the bridge on the same host, and
//          every write is an append of a whole block. Byte offsets stay valid, so
//          hub_queue_wait / hub_queue_wait_all / the MCP surface need no changes at
//          all — MCP still talks to files, it just now has files it did not have.
//
//          ── Loop prevention is structural, not a flag ────────────────────────
//          A block ingested on node A is materialised on B, C, … into
//          `<role>.A.queue.md` — a file whose ingest half belongs to A alone. B never
//          reads it, so it cannot re-publish it. There is no suppress set, no content
//          hash, no echo filter: the two halves simply do not overlap. Events that
//          entered from a Matrix client rather than a file (a human typing in
//          Element X) carry no file origin, so they get the synthetic origin
//          "mx-<signer_node>" — again a file nobody ingests, on any node.
//
//          ── Well-known rooms ─────────────────────────────────────────────────
//          A queue's room_id is derived from its role: "!hubd-queue-<slug>:hubd".
//          Every node computes the same id and seeds the same, byte-identical state
//          burst (queue_state_events) — same synthetic sender, ts 0, and sha256
//          event_ids. So two nodes that create the room while partitioned have not
//          collided: they created THE SAME room, and the LWW merge is a tie on every
//          key. That is why this needs no alias barrier and no state replication of
//          its own — convergence is by construction.
//
//          Power levels in that burst are load-bearing: the synthetic creator holds
//          100 so every node's seed passes AppState::may_set_state whatever order it
//          arrives in, and everyone else sits at 0 under state_default 50 so a human
//          who joins can talk but cannot reshape a machine-managed room. (Their own
//          m.room.member is allowed by may_set_state's self-membership carve-out —
//          that is what lets them join at all.)
//
//          ── Cursors ──────────────────────────────────────────────────────────
//          INGEST keeps a byte offset per own-file under <queues_dir>/../.mxstate/,
//          mirroring hubd's .qstate. It MUST persist: re-reading a file from 0 after
//          a restart would mint fresh event_ids (ts/depth differ) and duplicate the
//          whole queue into the room.
//          MATERIALIZE keeps no cursor at all — the count of blocks already in the
//          target file IS the cursor. That makes the file self-describing (delete it
//          and it rebuilds from the room), and makes the bridge idempotent alongside
//          hubd's own git mesh-sync: if git delivered a peer's blocks first, the count
//          already covers them and nothing is appended twice.
//
//          That cursor is NOT what prevents the loop, though it looks like it could:
//          in the steady state our own file and the room agree, so an own-origin
//          append would be a no-op anyway. It stops being a no-op the moment the room
//          holds more of our own history than our file does — a trimmed queue, or a
//          node rebuilt from git whose peers hand its old events back on catch-up.
//          Then, without the origin skip, materialise appends our history into the
//          file we ingest, ingest re-reads it as new (fresh depth ⇒ fresh event_id,
//          so content addressing cannot dedup it), and the room grows without bound.
//          The skip is the guard; the cursor merely hides its absence most of the time.
//
// ENV:
//   MATRIX_HS_HUBD_QUEUES_DIR  hubd queues/ directory (unset ⇒ bridge disabled)
//   MATRIX_HS_HUBD_NODE        this host's hubd node name
//                              (default: HUBD_QUEUE_NODE_ID, else /etc/hostname)
//   MATRIX_HS_HUBD_POLL_MS     poll interval in ms (default 1000)
//
// DEPENDENCIES: crate::state::{AppState, StateEvent}, crate::routes::send::insert_pdu,
//               crate::substrate::matrix_events::Pdu
// PUBLIC_API: BridgeConfig, spawn, queue_room_id, queue_state_events, parse_blocks,
//             render_block, Block
// NOTE: file I/O here is std::fs on the async task, matching persist.rs. The files are
//       a few KB and the poll is 1 s; if queues ever get big this is the thing to move
//       to spawn_blocking.
// END_AI_HEADER

use crate::routes::send::insert_pdu;
use crate::state::{AppState, StateEvent};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Synthetic creator of every queue room. Node-independent on purpose: the seed
/// state burst must be byte-identical on every node, and a sender homed on the
/// local server_name would differ per node and turn a tie into an LWW race.
pub const BRIDGE_USER: &str = "@hubd:hubd";
/// Domain half of a queue room_id. Fixed, for the same reason as BRIDGE_USER.
pub const ROOM_DOMAIN: &str = "hubd";
/// Localpart prefix of a queue room_id.
pub const ROOM_PREFIX: &str = "hubd-queue-";
/// Content key carrying hubd metadata on a bridged message.
pub const CONTENT_KEY: &str = "net.hubd.queue";
/// Filename suffix of a hubd queue file.
pub const QUEUE_SUFFIX: &str = ".queue.md";
/// Separator inside a block header: "## <ts> · from <sender>". The middle dot is
/// U+00B7 and is part of hubd's on-disk format — do not "normalise" it.
const BLOCK_SEP: &str = " · from ";
const DEFAULT_POLL_MS: u64 = 1000;

// ═══════════════════════════════════════════════════════════════════════════════
// Config
// ═══════════════════════════════════════════════════════════════════════════════

// BridgeConfig:start
//   purpose: Everything the bridge needs to run, resolved once at startup.
//   input:  built by from_env()
//   output: BridgeConfig
//   sideEffects: none
// BridgeConfig:end
#[derive(Debug, Clone)]
pub struct BridgeConfig {
    /// hubd `queues/` directory — the one holding `<role>.<node>.queue.md`.
    pub queues_dir: PathBuf,
    /// Where ingest byte offsets live. Sibling of queues/, never inside it: hubd's
    /// mesh-sync commits queues/, and a node-local cursor must not travel.
    pub state_dir: PathBuf,
    /// This host's hubd node name — the `<node>` segment of the file we ingest.
    pub node: String,
    pub poll_ms: u64,
}

impl BridgeConfig {
    // BridgeConfig::from_env:start
    //   purpose: Resolve config from the environment; None disables the bridge.
    //   input:  MATRIX_HS_HUBD_QUEUES_DIR / MATRIX_HS_HUBD_NODE / MATRIX_HS_HUBD_POLL_MS,
    //           falling back to HUBD_QUEUE_NODE_ID then /etc/hostname for the node name
    //   output: Some(BridgeConfig) iff MATRIX_HS_HUBD_QUEUES_DIR is set
    //   sideEffects: reads /etc/hostname when no node name is configured
    // BridgeConfig::from_env:end
    pub fn from_env() -> Option<Self> {
        let queues_dir = PathBuf::from(std::env::var("MATRIX_HS_HUBD_QUEUES_DIR").ok()?);
        let state_dir = queues_dir
            .parent()
            .map(|p| p.join(".mxstate"))
            .unwrap_or_else(|| PathBuf::from(".mxstate"));
        let node = std::env::var("MATRIX_HS_HUBD_NODE")
            .or_else(|_| std::env::var("HUBD_QUEUE_NODE_ID"))
            .ok()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| {
                std::fs::read_to_string("/etc/hostname")
                    .map(|s| s.trim().to_string())
                    .ok()
                    .filter(|s| !s.is_empty())
                    .unwrap_or_else(|| "node".to_string())
            });
        let poll_ms = std::env::var("MATRIX_HS_HUBD_POLL_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_POLL_MS);
        Some(BridgeConfig {
            queues_dir,
            state_dir,
            // hubd node names carry no dots (see queue.mjs's ^role(\.[^.]+)?\. regex);
            // a dotted one would split the filename into an extra segment and make the
            // file invisible to every reader.
            node: sanitize_node(&node),
            poll_ms,
        })
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Pure helpers — room identity
// ═══════════════════════════════════════════════════════════════════════════════

// sanitize_node:start
//   purpose: Reduce a host/node name to a single filename segment: alphanumerics,
//            '-' and '_' survive, everything else (dots above all) becomes '-'.
//            A dot here would add a segment to `<role>.<node>.queue.md` and hide the
//            file from hubd's per-role regex.
//   input:  raw node name
//   output: sanitized segment ("node" if nothing survives)
//   sideEffects: none
// sanitize_node:end
pub fn sanitize_node(raw: &str) -> String {
    let s: String = raw
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '-'
            }
        })
        .collect();
    let s = s.trim_matches('-').to_string();
    if s.is_empty() {
        "node".to_string()
    } else {
        s
    }
}

// role_slug:start
//   purpose: Turn a role name into the localpart chunk of its room_id. Roles in
//            practice are already alphanumeric+dash, and those pass through verbatim
//            so the room_id stays readable. A role that needed rewriting gets an
//            8-hex digest of the ORIGINAL appended, so two roles that sanitize to the
//            same string cannot land in the same room.
//   input:  role name
//   output: slug safe as a Matrix localpart chunk and as a Zenoh keyexpr chunk
//   sideEffects: none
// role_slug:end
pub fn role_slug(role: &str) -> String {
    let clean: String = role
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '-'
            }
        })
        .collect();
    if clean == role && !clean.is_empty() {
        clean
    } else {
        let d = Sha256::digest(role.as_bytes());
        let short: String = d.iter().take(4).map(|b| format!("{b:02x}")).collect();
        let base = clean.trim_matches('-');
        if base.is_empty() {
            format!("q-{short}")
        } else {
            format!("{base}-{short}")
        }
    }
}

// queue_room_id:start
//   purpose: The well-known room_id for a role. Deterministic and node-independent —
//            this is the whole point: two partitioned nodes seeding the same queue
//            produce the same room rather than two rooms to reconcile later.
//   input:  role name
//   output: "!hubd-queue-<slug>:hubd"
//   sideEffects: none
// queue_room_id:end
pub fn queue_room_id(role: &str) -> String {
    format!("!{ROOM_PREFIX}{}:{ROOM_DOMAIN}", role_slug(role))
}

// role_from_room_id:start
//   purpose: Recognise a queue room and recover its role from the id alone. Exact
//            only for roles that needed no sanitising (the normal case); callers
//            treat the result as a hint and prefer a role read out of event content.
//   input:  room_id
//   output: Some(slug) for "!hubd-queue-<slug>:hubd", else None
//   sideEffects: none
// role_from_room_id:end
pub fn role_from_room_id(room_id: &str) -> Option<&str> {
    room_id
        .strip_prefix('!')?
        .strip_suffix(ROOM_DOMAIN)?
        .strip_suffix(':')?
        .strip_prefix(ROOM_PREFIX)
        .filter(|s| !s.is_empty())
}

// state_event_id:start
//   purpose: Deterministic event_id for a seeded state event. sha256, NOT the
//            DefaultHasher that routes/rooms.rs uses: DefaultHasher's output is not
//            guaranteed stable across Rust versions, and these ids have to match
//            byte-for-byte between nodes that may be running different builds.
//   input:  event_type, room_id, state_key
//   output: "$" + base64url-nopad(sha256)
//   sideEffects: none
// state_event_id:end
fn state_event_id(event_type: &str, room_id: &str, state_key: &str) -> String {
    let mut h = Sha256::new();
    h.update(b"hubd-queue-state\0");
    h.update(event_type.as_bytes());
    h.update([0u8]);
    h.update(room_id.as_bytes());
    h.update([0u8]);
    h.update(state_key.as_bytes());
    format!("${}", URL_SAFE_NO_PAD.encode(h.finalize()))
}

// queue_state_events:start
//   purpose: The seed state burst for a queue room. Byte-identical on every node —
//            same sender, ts 0, sha256 ids — so seeding it concurrently in a
//            partition converges to a tie instead of an LWW race.
//
//            The power_levels entry is the part to be careful with. BRIDGE_USER holds
//            100 so that this same burst arriving from a peer passes may_set_state
//            regardless of which event lands first (a burst whose creator sat below
//            state_default would deny its own m.room.create when power_levels won the
//            race). Everyone else is users_default 0 against state_default 50: they
//            can join (self-membership carve-out) and send messages (timeline is not
//            power-gated) but cannot rewrite a machine-managed room.
//   input:  role name
//   output: the full seed burst, in no particular order (each is its own LWW key)
//   sideEffects: none
// queue_state_events:end
pub fn queue_state_events(role: &str) -> Vec<StateEvent> {
    let room_id = queue_room_id(role);
    let mk = |event_type: &str, state_key: &str, content: Value| StateEvent {
        event_type: event_type.to_string(),
        state_key: state_key.to_string(),
        sender: BRIDGE_USER.to_string(),
        content,
        event_id: state_event_id(event_type, &room_id, state_key),
        room_id: room_id.clone(),
        // Fixed, not a clock: any node-local timestamp would make these events differ
        // per node and start an LWW race between identical content.
        origin_server_ts: 0,
    };
    vec![
        mk(
            "m.room.create",
            "",
            json!({
                "creator": BRIDGE_USER,
                "room_version": "10",
                CONTENT_KEY: { "role": role }
            }),
        ),
        mk(
            "m.room.power_levels",
            "",
            json!({
                "users": { BRIDGE_USER: 100 },
                "users_default": 0,
                "events": {},
                "events_default": 0,
                "state_default": 50,
                "ban": 50,
                "kick": 50,
                "redact": 50,
                "invite": 50
            }),
        ),
        mk(
            "m.room.member",
            BRIDGE_USER,
            json!({ "membership": "join", "displayname": "hubd" }),
        ),
        mk("m.room.join_rules", "", json!({ "join_rule": "public" })),
        mk(
            "m.room.history_visibility",
            "",
            json!({ "history_visibility": "shared" }),
        ),
        mk("m.room.name", "", json!({ "name": format!("hubd: {role}") })),
        mk(
            "m.room.topic",
            "",
            json!({ "topic": format!("hubd queue '{role}' — bridged from queues/{role}.*{QUEUE_SUFFIX}") }),
        ),
    ]
}

// ═══════════════════════════════════════════════════════════════════════════════
// Pure helpers — block format
// ═══════════════════════════════════════════════════════════════════════════════

// Block:start
//   purpose: One hubd queue entry: "\n## <ts> · from <sender>\n<body>\n".
//   input:  parsed from a queue file, or rendered back into one
//   output: Block value
//   sideEffects: none
// Block:end
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Block {
    /// hubd's own timestamp string, "YYYY-MM-DD HH:MM". Carried verbatim in both
    /// directions so a block that round-trips through a room is byte-identical.
    pub ts: String,
    pub from: String,
    pub body: String,
}

// looks_like_ts:start
//   purpose: Recognise hubd's "YYYY-MM-DD HH:MM" stamp. Deliberately strict: a
//            message body may legitimately contain a markdown heading with " · from "
//            in it, and only the timestamp shape tells a real block header apart from
//            a line of user text.
//   input:  candidate string
//   output: true iff it is exactly that shape
//   sideEffects: none
// looks_like_ts:end
fn looks_like_ts(s: &str) -> bool {
    let b = s.as_bytes();
    if b.len() != 16 {
        return false;
    }
    let digit = |i: usize| b[i].is_ascii_digit();
    digit(0)
        && digit(1)
        && digit(2)
        && digit(3)
        && b[4] == b'-'
        && digit(5)
        && digit(6)
        && b[7] == b'-'
        && digit(8)
        && digit(9)
        && b[10] == b' '
        && digit(11)
        && digit(12)
        && b[13] == b':'
        && digit(14)
        && digit(15)
}

// parse_header:start
//   purpose: Split a block header line into (ts, from).
//   input:  one line
//   output: Some((ts, from)) iff the line is a well-formed block header
//   sideEffects: none
// parse_header:end
fn parse_header(line: &str) -> Option<(String, String)> {
    let rest = line.strip_prefix("## ")?;
    let (ts, from) = rest.split_once(BLOCK_SEP)?;
    if !looks_like_ts(ts) {
        return None;
    }
    Some((ts.to_string(), from.trim().to_string()))
}

// parse_blocks:start
//   purpose: Parse queue-file text into blocks. Text before the first header is
//            ignored — that is what a mid-file byte offset hands us when a reader
//            resumes, and it belongs to a block already consumed.
//   input:  file text (whole file, or the slice past a byte offset)
//   output: blocks in file order
//   sideEffects: none
// parse_blocks:end
pub fn parse_blocks(text: &str) -> Vec<Block> {
    let mut out: Vec<Block> = Vec::new();
    let mut cur: Option<(String, String, Vec<&str>)> = None;
    for line in text.lines() {
        if let Some((ts, from)) = parse_header(line) {
            if let Some((t, f, body)) = cur.take() {
                out.push(Block {
                    ts: t,
                    from: f,
                    body: body.join("\n").trim().to_string(),
                });
            }
            cur = Some((ts, from, Vec::new()));
        } else if let Some((_, _, body)) = cur.as_mut() {
            body.push(line);
        }
    }
    if let Some((t, f, body)) = cur {
        out.push(Block {
            ts: t,
            from: f,
            body: body.join("\n").trim().to_string(),
        });
    }
    out
}

// render_block:start
//   purpose: Render a block back into hubd's on-disk form, byte-identical to what
//            queueSend writes — leading newline, trailing newline, same separator.
//   input:  block
//   output: the text to append
//   sideEffects: none
// render_block:end
pub fn render_block(b: &Block) -> String {
    format!("\n## {}{}{}\n{}\n", b.ts, BLOCK_SEP, b.from, b.body.trim())
}

// format_ts_ms:start
//   purpose: Render a unix-millis timestamp as hubd's "YYYY-MM-DD HH:MM" in UTC.
//            Used only for events that entered from a Matrix client and so have no
//            hubd stamp of their own. Hand-rolled (Howard Hinnant's civil_from_days)
//            because the crate has no date dependency and this is not worth one.
//   input:  ms since the unix epoch
//   output: "YYYY-MM-DD HH:MM"
//   sideEffects: none
// format_ts_ms:end
pub fn format_ts_ms(ms: u64) -> String {
    let secs = (ms / 1000) as i64;
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    let (y, m, d) = civil_from_days(days);
    let (hh, mm) = (rem / 3600, (rem % 3600) / 60);
    format!("{y:04}-{m:02}-{d:02} {hh:02}:{mm:02}")
}

fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

// queue_file_name:start
//   purpose: The per-host queue filename hubd expects: "<role>.<node>.queue.md".
//   input:  role, node
//   output: filename
//   sideEffects: none
// queue_file_name:end
pub fn queue_file_name(role: &str, node: &str) -> String {
    format!("{role}.{node}{QUEUE_SUFFIX}")
}

// role_from_file_name:start
//   purpose: Recover the role from a queue filename, accepting both hubd shapes:
//            "<role>.<node>.queue.md" and the legacy shared "<role>.queue.md".
//            Mirrors queue.mjs's ^<role>(\.[^.]+)?\.queue\.md$ — a node segment is
//            exactly one dotless chunk, so a role containing dots is unambiguous.
//   input:  file name
//   output: Some(role) for a queue file, else None
//   sideEffects: none
// role_from_file_name:end
pub fn role_from_file_name(name: &str) -> Option<String> {
    let stem = name.strip_suffix(QUEUE_SUFFIX)?;
    if stem.is_empty() {
        return None;
    }
    match stem.rsplit_once('.') {
        // Trailing dotless chunk = the node segment; everything before it is the role.
        Some((role, node)) if !role.is_empty() && !node.is_empty() => Some(role.to_string()),
        _ => Some(stem.to_string()),
    }
}

// origin_of:start
//   purpose: Decide which per-host file an event belongs in. A bridged block carries
//            the hubd node it was ingested from; anything else came in through the
//            CS-API and gets "mx-<signer_node>", a name no ingest half ever reads —
//            which is what keeps a client-sent message from bouncing back out of the
//            file it lands in.
//   input:  content — parsed event content; signer_node — the PDU's signing node
//   output: origin tag, safe as a filename segment
//   sideEffects: none
// origin_of:end
pub fn origin_of(content: &Value, signer_node: &str) -> String {
    if let Some(o) = content
        .get(CONTENT_KEY)
        .and_then(|q| q.get("origin"))
        .and_then(|v| v.as_str())
        .filter(|s| !s.trim().is_empty())
    {
        return sanitize_node(o);
    }
    let node = if signer_node.trim().is_empty() {
        "unknown"
    } else {
        signer_node
    };
    format!("mx-{}", sanitize_node(node))
}

// block_from_event:start
//   purpose: Render one room event as the queue block it should appear as. A block
//            that came from a file round-trips verbatim (its own ts and from are
//            carried in content); one from a Matrix client gets a formatted ts and
//            the sender's localpart.
//   input:  content — parsed event content; sender — MXID; ts — origin_server_ts
//   output: Block
//   sideEffects: none
// block_from_event:end
pub fn block_from_event(content: &Value, sender: &str, ts: u64) -> Block {
    let q = content.get(CONTENT_KEY);
    let hub_ts = q
        .and_then(|q| q.get("ts"))
        .and_then(|v| v.as_str())
        .filter(|s| looks_like_ts(s))
        .map(str::to_string)
        .unwrap_or_else(|| format_ts_ms(ts));
    let from = q
        .and_then(|q| q.get("from"))
        .and_then(|v| v.as_str())
        .filter(|s| !s.trim().is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| crate::state::localpart(sender).to_string());
    let body = content
        .get("body")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    Block {
        ts: hub_ts,
        from,
        body,
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Impure — room seeding, ingest, materialise
// ═══════════════════════════════════════════════════════════════════════════════

// ensure_queue_room:start
//   purpose: Seed a role's well-known room if it is not already seeded, and register
//            a local alias so a human can /join #hubd-queue-<slug>:<server>.
//            Idempotent: re-applying the burst is an LWW tie on every key, so the
//            second call changes nothing. Not published to peers — every node
//            computes the identical burst locally, which is cheaper than replicating
//            it and cannot diverge.
//   input:  state, role
//   output: the room_id
//   sideEffects: inserts room/room_state/room_timeline entries; inserts an alias
// ensure_queue_room:end
pub fn ensure_queue_room(state: &Arc<AppState>, role: &str) -> String {
    let room_id = queue_room_id(role);
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
        for ev in queue_state_events(role) {
            if let Err(e) = state.apply_remote_state_event(ev) {
                eprintln!("[matrix-hs] hubd-bridge: seed state for {role}: {e}");
            }
        }
        eprintln!("[matrix-hs] hubd-bridge: seeded queue room {room_id} for role '{role}'");
    }

    if let Ok(mut aliases) = state.aliases.lock() {
        let alias = format!(
            "#{ROOM_PREFIX}{}:{}",
            role_slug(role),
            state.server_name.as_str()
        );
        aliases.entry(alias).or_insert_with(|| room_id.clone());
    }

    room_id
}

// read_offset / write_offset:start
//   purpose: Persist the ingest byte offset for one own-file. Losing it would
//            re-ingest the file from 0 and duplicate the room's whole history: a
//            replayed block gets a new depth and ts, so its event_id differs and the
//            content address cannot dedup it.
//   input:  state_dir, file name (+ value for write)
//   output: offset (0 when unknown)
//   sideEffects: write_offset creates state_dir and writes a file
// read_offset / write_offset:end
fn read_offset(state_dir: &Path, file: &str) -> u64 {
    std::fs::read_to_string(state_dir.join(format!("{file}.offset")))
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(0)
}

fn write_offset(state_dir: &Path, file: &str, off: u64) {
    if let Err(e) = std::fs::create_dir_all(state_dir) {
        eprintln!("[matrix-hs] hubd-bridge: create {}: {e}", state_dir.display());
        return;
    }
    if let Err(e) = std::fs::write(state_dir.join(format!("{file}.offset")), off.to_string()) {
        eprintln!("[matrix-hs] hubd-bridge: write offset for {file}: {e}");
    }
}

// discover_roles:start
//   purpose: Every role this node should bridge — those with a queue file on disk,
//            plus those it only knows as a room (learned from a peer via catch-up,
//            with no local file yet). The second half is what lets a node materialise
//            a queue it has never seen locally.
//   input:  state, cfg
//   output: sorted, de-duplicated role names
//   sideEffects: reads the queues directory
// discover_roles:end
fn discover_roles(state: &Arc<AppState>, cfg: &BridgeConfig) -> Vec<String> {
    let mut roles: Vec<String> = Vec::new();
    if let Ok(rd) = std::fs::read_dir(&cfg.queues_dir) {
        for entry in rd.flatten() {
            if let Some(name) = entry.file_name().to_str() {
                if let Some(role) = role_from_file_name(name) {
                    roles.push(role);
                }
            }
        }
    }

    // Rooms this node holds that are queue rooms. Prefer the role recorded in event
    // content — the id only carries the slug, which equals the role for every role
    // that needed no sanitising but not for one that did.
    if let Ok(rooms) = state.rooms.lock() {
        for (room_id, log) in rooms.iter() {
            let Some(slug) = role_from_room_id(room_id) else {
                continue;
            };
            let from_content = log.ordered().iter().find_map(|p| {
                serde_json::from_slice::<Value>(&p.content)
                    .ok()
                    .and_then(|c| {
                        c.get(CONTENT_KEY)
                            .and_then(|q| q.get("role"))
                            .and_then(|v| v.as_str())
                            .map(str::to_string)
                    })
            });
            roles.push(from_content.unwrap_or_else(|| slug.to_string()));
        }
    }

    roles.sort();
    roles.dedup();
    roles
}

// block_content_id:start
//   purpose: The identity of a hubd block by its CONTENT, so re-publishing the same
//            block is recognisable as the same block.  Needed because the ingest
//            cursor is a byte offset written AFTER the chunk is published: a node
//            killed in that window comes back, re-reads the same blocks, and
//            insert_pdu — which mints event_id from wall-clock ts + depth +
//            prev_events — gives every one of them a NEW id.  The grow-set cannot
//            dedup that, the room grows a second copy, and every consumer appends
//            the block twice.  Keyed by role, origin, hubd ts, from and body:
//            exactly the fields block_from_event reads back, so the id computed on
//            ingest and the id computed from a room event are the same string.
//            NUL-separated with a domain prefix, so no combination of fields can
//            imitate another.
//   input:  role, origin, ts, from, body
//   output: "$" + base64url-nopad(sha256)
//   sideEffects: none
// block_content_id:end
pub fn block_content_id(role: &str, origin: &str, ts: &str, from: &str, body: &str) -> String {
    let mut h = Sha256::new();
    h.update(b"hubd-queue-block\0");
    for field in [role, origin, ts, from, body] {
        h.update(field.as_bytes());
        h.update([0u8]);
    }
    format!("${}", URL_SAFE_NO_PAD.encode(h.finalize()))
}

// room_block_ids:start
//   purpose: Every queue-block id already present in a role's room, computed the
//            same way ingest computes it.  Built once per room per process and then
//            extended in memory, so the dedup costs one room scan at start-up and
//            O(1) per block afterwards.
//   input:  state, room_id
//   output: HashSet of block content ids
//   sideEffects: none (reads under the rooms lock, releases it before returning)
// room_block_ids:end
fn room_block_ids(state: &Arc<AppState>, room_id: &str) -> HashSet<String> {
    let mut ids = HashSet::new();
    let Ok(rooms) = state.rooms.lock() else {
        return ids;
    };
    let Some(log) = rooms.get(room_id) else {
        return ids;
    };
    for pdu in log.ordered() {
        if pdu.kind != "m.room.message" {
            continue;
        }
        let Ok(content) = serde_json::from_slice::<Value>(&pdu.content) else {
            continue;
        };
        let q = content.get(CONTENT_KEY);
        let Some(role) = q.and_then(|q| q.get("role")).and_then(|v| v.as_str()) else {
            continue;
        };
        let origin = origin_of(&content, &pdu.signer_node);
        let ts = q
            .and_then(|q| q.get("ts"))
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let from = q
            .and_then(|q| q.get("from"))
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let body = content
            .get("body")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        ids.insert(block_content_id(role, &origin, ts, from, body));
    }
    ids
}

// ingest_role:start
//   purpose: Read whatever hubd appended to THIS node's queue file since the last
//            pass and turn each new block into a room message. Only this node's own
//            file is ever read here — that asymmetry is the loop prevention.
//            A block whose content id is already in the room is skipped rather than
//            re-published: the byte-offset cursor is written after the chunk, so an
//            ungraceful stop between the two makes this pass see the same blocks
//            again, and without the check every one of them would land twice.
//   input:  state, cfg, role, seen — per-room content ids, carried across passes
//   output: number of blocks ingested
//   sideEffects: inserts PDUs (which persist, notify and, in cluster mode, publish);
//                advances the persisted byte offset
// ingest_role:end
pub(crate) async fn ingest_role(
    state: &Arc<AppState>,
    cfg: &BridgeConfig,
    role: &str,
    seen: &mut HashMap<String, HashSet<String>>,
) -> usize {
    let file = queue_file_name(role, &cfg.node);
    let path = cfg.queues_dir.join(&file);
    let Ok(meta) = std::fs::metadata(&path) else {
        return 0; // hubd has not created this node's file for the role yet
    };
    let size = meta.len();
    let mut off = read_offset(&cfg.state_dir, &file);
    if size < off {
        // Truncated or recreated — same recovery hubd's own reader performs.
        off = 0;
    }
    if size == off {
        return 0;
    }

    let Ok(text) = std::fs::read_to_string(&path) else {
        return 0;
    };
    // Byte offset into a &str: only slice on a char boundary, else a multi-byte
    // character straddling the offset would panic. hubd counts bytes, so the offset
    // is a byte index by definition.
    let start = off as usize;
    if start > text.len() || !text.is_char_boundary(start) {
        eprintln!("[matrix-hs] hubd-bridge: offset {start} is not a char boundary in {file}; rescanning from 0");
        write_offset(&cfg.state_dir, &file, 0);
        return 0;
    }
    let chunk = &text[start..];
    // A block hubd is still writing has no trailing newline yet. Leaving it for the
    // next pass costs one poll and avoids ingesting a half-written body.
    if !chunk.ends_with('\n') {
        return 0;
    }

    let blocks = parse_blocks(chunk);
    if blocks.is_empty() {
        write_offset(&cfg.state_dir, &file, size);
        return 0;
    }

    let room_id = ensure_queue_room(state, role);
    let sender = format!("@hubd:{}", state.server_name);
    let ids = seen
        .entry(room_id.clone())
        .or_insert_with(|| room_block_ids(state, &room_id));
    let mut n = 0usize;
    for b in &blocks {
        let content_id = block_content_id(role, &cfg.node, &b.ts, &b.from, &b.body);
        if ids.contains(&content_id) {
            continue; // already in the room — re-read after an ungraceful stop
        }
        let content = json!({
            "msgtype": "m.text",
            "body": b.body,
            CONTENT_KEY: {
                "role":   role,
                "from":   b.from,
                "ts":     b.ts,
                "origin": cfg.node,
            }
        });
        let bytes = match serde_json::to_vec(&content) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("[matrix-hs] hubd-bridge: encode block for {role}: {e}");
                continue;
            }
        };
        match insert_pdu(
            state,
            &room_id,
            sender.clone(),
            "m.room.message".to_string(),
            bytes,
            json!({}),
        )
        .await
        {
            Ok(_) => {
                ids.insert(content_id);
                n += 1;
            }
            Err(e) => {
                // Stop at the first failure and leave the offset where it is, so the
                // whole remaining chunk is retried rather than silently dropped.
                eprintln!("[matrix-hs] hubd-bridge: insert block for {role}: {e}");
                return n;
            }
        }
    }

    write_offset(&cfg.state_dir, &file, size);
    if n > 0 {
        eprintln!("[matrix-hs] hubd-bridge: ingested {n} block(s) from {file}");
    }
    n
}

// materialize_role:start
//   purpose: Write the room's events back out as per-origin queue files, so hubd's
//            MCP tools see remote traffic as ordinary local queue content.
//
//            The cursor is the number of blocks already in the target file — no
//            sidecar. Per-origin files make that sound: one origin's events form a
//            causal chain (each takes the previous head as prev_event), so ordered()
//            can never emit them out of order or leave a gap in the middle, and "the
//            first k are already written" is therefore always true.
//   input:  state, cfg, role, counts — cache of (file length, block count) per path
//   output: number of blocks appended
//   sideEffects: appends to queue files; creates the queues directory
// materialize_role:end
pub(crate) fn materialize_role(
    state: &Arc<AppState>,
    cfg: &BridgeConfig,
    role: &str,
    counts: &mut HashMap<PathBuf, (u64, usize)>,
) -> usize {
    let room_id = queue_room_id(role);

    // Collect per-origin blocks under the rooms lock, then release it before any I/O.
    let mut by_origin: HashMap<String, Vec<Block>> = HashMap::new();
    {
        let Ok(rooms) = state.rooms.lock() else {
            return 0;
        };
        let Some(log) = rooms.get(&room_id) else {
            return 0;
        };
        for pdu in log.ordered() {
            if pdu.kind != "m.room.message" {
                continue;
            }
            let Ok(content) = serde_json::from_slice::<Value>(&pdu.content) else {
                continue;
            };
            let origin = origin_of(&content, &pdu.signer_node);
            // Our own file is hubd's to write; we only ever read it.
            if origin == cfg.node {
                continue;
            }
            by_origin
                .entry(origin)
                .or_default()
                .push(block_from_event(&content, &pdu.sender, pdu.ts));
        }
    }

    if by_origin.is_empty() {
        return 0;
    }
    if let Err(e) = std::fs::create_dir_all(&cfg.queues_dir) {
        eprintln!(
            "[matrix-hs] hubd-bridge: create {}: {e}",
            cfg.queues_dir.display()
        );
        return 0;
    }

    let mut appended = 0usize;
    for (origin, blocks) in by_origin {
        let path = cfg.queues_dir.join(queue_file_name(role, &origin));
        let len = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        let have = match counts.get(&path) {
            Some((cached_len, cached)) if *cached_len == len => *cached,
            _ => std::fs::read_to_string(&path)
                .map(|t| parse_blocks(&t).len())
                .unwrap_or(0),
        };
        if blocks.len() <= have {
            counts.insert(path, (len, have));
            continue;
        }
        let text: String = blocks[have..].iter().map(render_block).collect();
        match append(&path, &text) {
            Ok(new_len) => {
                appended += blocks.len() - have;
                counts.insert(path, (new_len, blocks.len()));
            }
            Err(e) => {
                eprintln!(
                    "[matrix-hs] hubd-bridge: append to {}: {e}",
                    path.display()
                );
            }
        }
    }
    if appended > 0 {
        eprintln!("[matrix-hs] hubd-bridge: materialised {appended} block(s) for role '{role}'");
    }
    appended
}

// append:start
//   purpose: Append text to a queue file, returning the new length. Append-only is
//            not a style choice here: hubd's readers track byte offsets, and any
//            rewrite would either replay the file or skip past live content.
//   input:  path, text
//   output: new file length
//   sideEffects: creates/extends the file
// append:end
fn append(path: &Path, text: &str) -> std::io::Result<u64> {
    use std::io::Write as _;
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    f.write_all(text.as_bytes())?;
    f.flush()?;
    Ok(f.metadata()?.len())
}

// spawn:start
//   purpose: Start the bridge loop if MATRIX_HS_HUBD_QUEUES_DIR is set; otherwise do
//            nothing at all, exactly like an absent MATRIX_HS_SCRIPTS_DIR.
//   input:  state
//   output: true iff the bridge was started
//   sideEffects: spawns a tokio task that reads and writes the queues directory
// spawn:end
pub fn spawn(state: Arc<AppState>) -> bool {
    let Some(cfg) = BridgeConfig::from_env() else {
        return false;
    };
    eprintln!(
        "[matrix-hs] hubd-bridge: watching {} as node '{}' (poll {} ms)",
        cfg.queues_dir.display(),
        cfg.node,
        cfg.poll_ms
    );
    tokio::spawn(async move {
        let mut counts: HashMap<PathBuf, (u64, usize)> = HashMap::new();
        let mut seen: HashMap<String, HashSet<String>> = HashMap::new();
        loop {
            drain_cluster(&state).await;
            for role in discover_roles(&state, &cfg) {
                ensure_queue_room(&state, &role);
                ingest_role(&state, &cfg, &role, &mut seen).await;
                materialize_role(&state, &cfg, &role, &mut counts);
            }
            tokio::time::sleep(std::time::Duration::from_millis(cfg.poll_ms)).await;
        }
    });
    true
}

// drain_cluster:start
//   purpose: Apply whatever the substrate has delivered, before reading the rooms.
//
//            This is not an optimisation, it is the difference between working and
//            not. Zenoh delivers a peer's delta into a per-room inbox; the inbox is
//            emptied by drain_all_cluster, and the ONLY thing that calls it is a
//            sync-style CS-API request. A homeserver whose users are Matrix clients
//            always has one in flight, so the inbox is always being emptied and the
//            dependency is invisible. A node whose users are hubd agents may have no
//            client at all — replicated events then sit in the inbox forever and the
//            bridge materialises an empty room. Found exactly that way: two live
//            nodes, a block ingested on one, nothing on the other, and no error
//            anywhere.
//
//            Scoped to the bridge on purpose. Every cluster node arguably wants a
//            drain that does not depend on a client showing up, but making it
//            unconditional changes behaviour for deployments that are working today,
//            and that is a call for the operator, not for this module.
//   input:  state
//   output: none (drain failures are logged; the next pass retries)
//   sideEffects: applies pending remote deltas into rooms/room_state/timeline
// drain_cluster:end
// node_name:start
//   purpose: Resolve THIS host's hubd node name exactly the way
//            BridgeConfig does (MATRIX_HS_HUBD_NODE -> HUBD_QUEUE_NODE_ID ->
//            /etc/hostname -> "node"). Shared with hub_replic, whose
//            per-file ownership (journal.<node>.jsonl,
//            tasks.<node>.events.jsonl) must key on the same name hubd
//            itself writes under (core.mjs JOURNAL_NODE).
//   input:  process env + /etc/hostname
//   output: node name string
//   sideEffects: none
// node_name:end
pub fn node_name() -> String {
    std::env::var("MATRIX_HS_HUBD_NODE")
        .or_else(|_| std::env::var("HUBD_QUEUE_NODE_ID"))
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| {
            std::fs::read_to_string("/etc/hostname")
                .map(|s| s.trim().to_string())
                .ok()
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "node".to_string())
        })
}

#[cfg(feature = "cluster")]
pub(crate) async fn drain_cluster(state: &Arc<AppState>) {
    if state.cluster.is_none() {
        return;
    }
    // Wait for a peer's signing key before draining anything. Incoming PDUs are
    // verified against key_store, and a delta drained before its sender's key has
    // landed is not deferred — every PDU in it is rejected outright and only the next
    // catch-up pass brings them back, up to MATRIX_HS_CATCHUP_INTERVAL_SECS later.
    // Observed live on a restart: "REJECT PDU ... bad/unknown sig from node a.test",
    // then the same block arriving intact one backstop interval afterwards.
    //
    // This narrows the window rather than closing it: with three or more nodes the
    // first key can land before the second, and that peer's events still get rejected.
    // Catch-up remains the backstop for the rest — this only stops the case that
    // happens on every single startup.
    let have_peer_key = state
        .key_store
        .snapshot()
        .keys()
        .any(|k| k.as_str() != state.server_name.as_str());
    if !have_peer_key {
        return;
    }
    if let Err(e) = crate::routes::sync::drain_all_cluster(state).await {
        eprintln!("[matrix-hs] hubd-bridge: cluster drain: {e}");
    }
}

#[cfg(not(feature = "cluster"))]
pub(crate) async fn drain_cluster(_state: &Arc<AppState>) {}
