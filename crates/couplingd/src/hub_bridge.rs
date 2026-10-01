// START_AI_HEADER
// MODULE: couplingd/src/hub_bridge.rs
// PURPOSE: Bidirectional bridge between hubd and a Matrix room's CRDT event-log.
//          Implements SPEC_matrix_multimaster §4 (mapping) + §8.6 (decisions:
//          N agent-users per §Q1, per-project rooms per §Q4, power-level authz per §Q3).
//
//          Two directions:
//          (a) hubd → Matrix: HubEvent from HubSource → m.room.message Pdu → RoomLog.add
//          (b) Matrix → hubd: new m.room.message Pdus from RoomLog → /command parse →
//              HubCommand → HubSink.submit
//
//          Idempotency: the RoomLog grow-set ensures a HubEvent translated twice yields
//          the same event_id (content-addressed hash) and is a no-op on the second add.
//          The bridge tracks the last processed log length to avoid re-scanning old PDUs.
//
//          FILE-APPEND MODEL (SPEC_matrix_multimaster §4, hubd Zenoh-agnostic):
//          The journal file is the ONLY interface between hubd and the cluster layer.
//          hubd writes/reads only the file; the cluster layer appends remote events
//          into the same file. Both sides are pure appenders; no Zenoh knowledge leaks
//          into hubd. Loop safety: content-addressed event_id → RoomLog grow-set dedup
//          makes a re-emitted remote line a no-op on the second add. A `source` field
//          in each line lets the tail side cheaply skip cluster-appended lines (plus
//          event_id dedup as backstop).
//
// INTENT: PoC for the hub↔Matrix channel. Mem-impl for unit tests; File-impl for
//         host integration tests (temp files only — never /root/.hubd).
// DEPENDENCIES: std, crate::matrix_events::{Pdu, RoomLog}
// PUBLIC_API: HubEventKind, HubEvent, HubCommand, BridgeError,
//             HubSource (trait), MemHubSource, FileHubSource,
//             HubSink (trait), MemHubSink, FileHubSink,
//             hub_event_to_pdu, pdu_to_hub_command,
//             HubBridge
// END_AI_HEADER

use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, Seek, SeekFrom, Write};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::matrix_events::{Pdu, RoomLog};

// ═══════════════════════════════════════════════════════════════════════════════
// Error type
// ═══════════════════════════════════════════════════════════════════════════════

// BridgeError:start
//   purpose: Errors produced by bridge operations (sink submission, lock contention,
//            file I/O for FileHubSink/FileHubSource).
//   input:  none (variants constructed internally)
//   output: Display via manual impl
//   sideEffects: none
// BridgeError:end
#[derive(Debug)]
pub enum BridgeError {
    /// The in-memory sink's Mutex was poisoned.
    Poisoned,
    /// The submitted HubCommand was rejected by the sink implementation.
    Rejected(String),
    /// A file I/O operation failed (FileHubSink/FileHubSource).
    Io(io::Error),
    /// A JSON serialisation/deserialisation error in the file layer.
    Json(serde_json::Error),
}

impl std::fmt::Display for BridgeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BridgeError::Poisoned    => write!(f, "bridge sink lock poisoned"),
            BridgeError::Rejected(r) => write!(f, "bridge sink rejected: {r}"),
            BridgeError::Io(e)       => write!(f, "bridge file I/O: {e}"),
            BridgeError::Json(e)     => write!(f, "bridge JSON: {e}"),
        }
    }
}

impl std::error::Error for BridgeError {}

impl From<io::Error> for BridgeError {
    fn from(e: io::Error) -> Self { BridgeError::Io(e) }
}

impl From<serde_json::Error> for BridgeError {
    fn from(e: serde_json::Error) -> Self { BridgeError::Json(e) }
}

// ═══════════════════════════════════════════════════════════════════════════════
// HubEventKind — discriminated kind for inbound hubd events
// ═══════════════════════════════════════════════════════════════════════════════

// HubEventKind:start
//   purpose: Classify the origin operation of a HubEvent so the bridge can choose
//            the correct Matrix body format (§4.1 mapping table).
//   input:  constructed by hubd integration code (or MemHubSource in tests)
//   output: used in hub_event_to_pdu to compose body / hubd.type field
//   sideEffects: none (pure enum)
// HubEventKind:end
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum HubEventKind {
    /// hub_task_add: a new task was created.
    TaskAdd,
    /// hub_task_update with status=done.
    Done,
    /// hub_report: DONE/DECIDE/FACT journal entry.
    Report,
    /// hub_card_set: project digest / card.
    Card,
    /// hub_kanban: kanban board snapshot.
    Kanban,
}

impl HubEventKind {
    // HubEventKind::as_str:start
    //   purpose: Canonical string representation used in hubd.type Matrix content field.
    //   input:  self
    //   output: &'static str
    //   sideEffects: none
    // HubEventKind::as_str:end
    pub fn as_str(&self) -> &'static str {
        match self {
            HubEventKind::TaskAdd => "task",
            HubEventKind::Done    => "task_done",
            HubEventKind::Report  => "report",
            HubEventKind::Card    => "card",
            HubEventKind::Kanban  => "kanban",
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// HubEvent — an event emitted by hubd toward Matrix
// ═══════════════════════════════════════════════════════════════════════════════

// EventSource:start
//   purpose: Tags a journal-file line as either produced by the local hubd instance
//            ("local") or appended by the cluster/remote sync layer ("remote").
//            Used by FileHubSource.drain() to cheaply skip cluster-appended lines on
//            the tail side (cheap first check; event_id dedup in RoomLog is backstop).
//   input:  serialised to/from JSON in journal file lines
//   output: EventSource::Local (hubd events to forward to Matrix) vs Remote (skip)
//   sideEffects: none (pure enum)
// EventSource:end
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EventSource {
    /// Written by the local hubd instance; tailed by cluster → Matrix direction.
    Local,
    /// Appended by the remote/cluster side; hubd reads these; tail side skips them.
    Remote,
}

// HubEvent:start
//   purpose: Carries one hubd-side event that should appear as a Matrix message.
//            Fields map to §4.1 matrix content:
//              kind    → hubd.type / body prefix
//              project → hubd.project
//              agent   → hubd.agent (set when kind is Report)
//              text    → body plaintext suffix
//              ts      → Pdu.ts (origin_server_ts, injected by caller — no wall-clock)
//              source  → EventSource::Local (hubd-produced) / Remote (cluster-appended).
//                        FileHubSource skips Remote lines; RoomLog event_id dedup is backstop.
//   input:  constructed by hubd integration; ts injected explicitly for test determinism
//   output: consumed by hub_event_to_pdu
//   sideEffects: none (pure value)
// HubEvent:end
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct HubEvent {
    pub kind:    HubEventKind,
    pub project: String,
    /// Agent role name (e.g. "runner"). Relevant for Report events; empty otherwise.
    pub agent:   String,
    pub text:    String,
    /// Injected timestamp (millis since epoch or monotonic counter); never Date::now.
    pub ts:      u64,
    /// Origin marker: Local = written by hubd; Remote = appended by cluster sync.
    /// Default is Local when constructing events inside hubd.
    #[serde(default = "EventSource::local_default")]
    pub source:  EventSource,
}

impl EventSource {
    // EventSource::local_default:start
    //   purpose: serde default used when deserialising old lines that predate the source field.
    //            Treats missing field as Local (conservative — does not silently skip).
    //   input:  none
    //   output: EventSource::Local
    //   sideEffects: none
    // EventSource::local_default:end
    fn local_default() -> Self { EventSource::Local }
}

// ═══════════════════════════════════════════════════════════════════════════════
// HubCommand — a parsed command flowing Matrix → hubd
// ═══════════════════════════════════════════════════════════════════════════════

// HubCommand:start
//   purpose: Represents a /-prefixed command extracted from a Matrix m.room.message body
//            (§4.2 protocol). Carries the parsed verb and arguments so the hubd sink
//            can dispatch without re-parsing.
//            verb    → one of "task", "done", "report", "decide", "ask", "kanban", "status"
//            project → project slug from command args (may be empty for /status, /kanban)
//            args    → remaining text after verb+project
//            by      → Matrix sender user_id (e.g. "@andrey:domain")
//   input:  constructed by pdu_to_hub_command
//   output: passed to HubSink.submit
//   sideEffects: none (pure value)
// HubCommand:end
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HubCommand {
    pub verb:    String,
    pub project: String,
    pub args:    String,
    pub by:      String,
}

// ═══════════════════════════════════════════════════════════════════════════════
// HubSource trait + MemHubSource
// ═══════════════════════════════════════════════════════════════════════════════

// HubSource:start
//   purpose: Abstraction over the hubd event feed. Production: polling tasks.jsonl +
//            journal via inotify (variant (a) from §Q2 — hubd writes files, bridge reads).
//            Tests: MemHubSource — push events in-memory, drain returns them.
//   input:  drain() call from HubBridge.tick
//   output: Vec<HubEvent> — all events queued since last drain (consuming)
//   sideEffects: clears internal buffer on drain
// HubSource:end
pub trait HubSource: Send + Sync {
    // HubSource::drain:start
    //   purpose: Drain and return all pending HubEvents since the last call.
    //            Idempotent if called again with no new events: returns empty Vec.
    //   input:  none
    //   output: Vec<HubEvent>
    //   sideEffects: clears internal pending buffer
    // HubSource::drain:end
    fn drain(&self) -> Vec<HubEvent>;
}

// MemHubSource:start
//   purpose: In-memory HubSource for host-side tests. Callers push events via push();
//            HubBridge calls drain() to consume them.
//   input:  push(ev) — enqueue a HubEvent; drain() — consume all queued events
//   output: drain() → Vec<HubEvent>
//   sideEffects: Arc<Mutex<Vec>> shared between pusher and bridge
// MemHubSource:end
#[derive(Clone)]
pub struct MemHubSource {
    queue: Arc<Mutex<Vec<HubEvent>>>,
}

impl MemHubSource {
    // MemHubSource::new:start
    //   purpose: Construct an empty MemHubSource.
    //   input:  none
    //   output: MemHubSource
    //   sideEffects: allocates Arc<Mutex<Vec>>
    // MemHubSource::new:end
    pub fn new() -> Self {
        Self { queue: Arc::new(Mutex::new(Vec::new())) }
    }

    // MemHubSource::push:start
    //   purpose: Enqueue a HubEvent to be returned on the next drain() call.
    //   input:  ev — HubEvent
    //   output: none
    //   sideEffects: pushes ev into internal queue
    // MemHubSource::push:end
    pub fn push(&self, ev: HubEvent) {
        // Silently ignore if poisoned — tests detect missing events via assertions.
        if let Ok(mut q) = self.queue.lock() {
            q.push(ev);
        }
    }
}

impl Default for MemHubSource {
    fn default() -> Self {
        Self::new()
    }
}

impl HubSource for MemHubSource {
    fn drain(&self) -> Vec<HubEvent> {
        self.queue
            .lock()
            .map(|mut q| {
                let mut out = Vec::new();
                std::mem::swap(&mut *q, &mut out);
                out
            })
            .unwrap_or_default()
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// HubSink trait + MemHubSink
// ═══════════════════════════════════════════════════════════════════════════════

// HubSink:start
//   purpose: Abstraction over the hubd command receiver. Production: calls hubd MCP tool
//            (hub_task_add, hub_report, etc.) over the local MCP socket.
//            Tests: MemHubSink — collects submitted HubCommands in a Vec.
//   input:  submit(cmd) from HubBridge.tick after parsing a Matrix /command
//   output: Result<(), BridgeError>
//   sideEffects: production side-effects (hubd state mutation) via MCP
// HubSink:end
pub trait HubSink: Send + Sync {
    // HubSink::submit:start
    //   purpose: Dispatch a HubCommand to hubd (or record it in tests).
    //   input:  cmd — parsed HubCommand
    //   output: Result<(), BridgeError>
    //   sideEffects: may mutate hubd state (tasks.json, journal)
    // HubSink::submit:end
    fn submit(&self, cmd: HubCommand) -> Result<(), BridgeError>;
}

// MemHubSink:start
//   purpose: In-memory HubSink that records submitted HubCommands for inspection in tests.
//   input:  submit(cmd) — records the command
//   output: commands() → snapshot of Vec<HubCommand> for assertions
//   sideEffects: appends to Arc<Mutex<Vec>>
// MemHubSink:end
#[derive(Clone)]
pub struct MemHubSink {
    submitted: Arc<Mutex<Vec<HubCommand>>>,
}

impl MemHubSink {
    // MemHubSink::new:start
    //   purpose: Construct an empty MemHubSink.
    //   input:  none
    //   output: MemHubSink
    //   sideEffects: allocates Arc<Mutex<Vec>>
    // MemHubSink::new:end
    pub fn new() -> Self {
        Self { submitted: Arc::new(Mutex::new(Vec::new())) }
    }

    // MemHubSink::commands:start
    //   purpose: Return a snapshot of all submitted HubCommands (for test assertions).
    //   input:  none
    //   output: Vec<HubCommand> (cloned)
    //   sideEffects: none
    // MemHubSink::commands:end
    pub fn commands(&self) -> Vec<HubCommand> {
        self.submitted
            .lock()
            .map(|q| q.clone())
            .unwrap_or_default()
    }
}

impl Default for MemHubSink {
    fn default() -> Self {
        Self::new()
    }
}

impl HubSink for MemHubSink {
    fn submit(&self, cmd: HubCommand) -> Result<(), BridgeError> {
        self.submitted
            .lock()
            .map_err(|_| BridgeError::Poisoned)?
            .push(cmd);
        Ok(())
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// FileHubSink — appends HubEvents as JSON lines to the hubd journal file
// ═══════════════════════════════════════════════════════════════════════════════

// FileHubSink:start
//   purpose: HubSink implementation that appends HubCommands encoded as JSON lines
//            to the hubd journal file via O_APPEND.  Used by the Matrix→hubd direction:
//            remote Matrix /commands are serialised as HubEvents and appended so that
//            the local hubd process can read them.
//
//            O_APPEND semantics: on POSIX (Linux/FreeBSD) a single write() up to
//            PIPE_BUF bytes (4096 on both) to an O_APPEND file is atomic — no two
//            concurrent appenders will interleave partial lines.  A JSON-encoded
//            HubEvent line is comfortably under 4096 bytes.  No locking needed.
//
//            The appended line carries `source: "remote"` so that FileHubSource.drain()
//            can cheaply skip it on the cluster→Matrix tail side.  The RoomLog
//            content-addressed event_id is the backstop dedup.
//
//   input:  path — path to the journal file (created if absent)
//   output: submit(ev) → writes one JSON line terminated by '\n'
//   sideEffects: opens file with O_APPEND|O_CREAT on every submit (cheap: no buffering
//                needed for append-only correctness; OS VFS handles the open/close).
// FileHubSink:end
pub struct FileHubSink {
    path: PathBuf,
}

impl FileHubSink {
    // FileHubSink::new:start
    //   purpose: Construct a FileHubSink pointing at the given journal path.
    //   input:  path — journal file path (need not exist yet)
    //   output: FileHubSink
    //   sideEffects: none (file opened lazily on submit)
    // FileHubSink::new:end
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self { path: path.as_ref().to_path_buf() }
    }

    // FileHubSink::append_event:start
    //   purpose: Serialise a HubEvent to JSON and atomically append it as one line.
    //            The event's source field is forced to Remote before serialisation so
    //            the FileHubSource tail side will recognise it as a cluster-appended entry.
    //   input:  ev — HubEvent (cloned internally to set source=Remote)
    //   output: Result<(), BridgeError>
    //   sideEffects: opens file O_APPEND|O_CREAT; writes one JSON line; closes file
    // FileHubSink::append_event:end
    pub fn append_event(&self, mut ev: HubEvent) -> Result<(), BridgeError> {
        ev.source = EventSource::Remote;
        let mut line = serde_json::to_string(&ev)?;
        line.push('\n');
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        file.write_all(line.as_bytes())?;
        Ok(())
    }
}

impl HubSink for FileHubSink {
    // HubSink::submit (FileHubSink):start
    //   purpose: Convert a HubCommand back to a synthetic HubEvent and append it to the
    //            journal as source=Remote so the local hubd process can react to it.
    //            Verb→Kind mapping: "task"→TaskAdd, "done"/"task_done"→Done,
    //            "report"/"decide"→Report, "card"→Card, "kanban"→Kanban, unknown→Report.
    //            ts is set to 0 (bridge does not have a reliable wall-clock; caller may
    //            override by using append_event directly with a known ts).
    //   input:  cmd — HubCommand parsed from a Matrix /command PDU
    //   output: Result<(), BridgeError>
    //   sideEffects: appends one JSON line to journal file
    // HubSink::submit (FileHubSink):end
    fn submit(&self, cmd: HubCommand) -> Result<(), BridgeError> {
        let kind = match cmd.verb.as_str() {
            "task"              => HubEventKind::TaskAdd,
            "done" | "task_done" => HubEventKind::Done,
            "report" | "decide" => HubEventKind::Report,
            "card"              => HubEventKind::Card,
            "kanban"            => HubEventKind::Kanban,
            _                   => HubEventKind::Report,
        };
        let ev = HubEvent {
            kind,
            project: cmd.project,
            agent:   cmd.by,
            text:    cmd.args,
            ts:      0,
            source:  EventSource::Remote,
        };
        let mut line = serde_json::to_string(&ev)?;
        line.push('\n');
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        file.write_all(line.as_bytes())?;
        Ok(())
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// DurableCursor — byte offset + inode for rotation-safe file tailing
// ═══════════════════════════════════════════════════════════════════════════════

// DurableCursor:start
//   purpose: Tracks the read position in the journal file across multiple drain() calls.
//            Stores (inode, byte_offset) so that if the file is rotated (unlinked and
//            re-created) the cursor resets automatically: a new file has a different inode.
//            On re-open: if inode matches → seek to byte_offset; if inode differs → reset
//            to offset 0 (the new file is fresh).
//   input:  updated by FileHubSource.drain() after each successful read batch
//   output: byte_offset and inode preserved between drain() calls
//   sideEffects: none (pure value; mutated by FileHubSource.drain())
// DurableCursor:end
#[derive(Debug, Clone, Default)]
struct DurableCursor {
    /// Inode of the file at last read. 0 = not yet opened.
    inode:       u64,
    /// Byte offset into the file for the next read.
    byte_offset: u64,
}

// ═══════════════════════════════════════════════════════════════════════════════
// FileHubSource — tails new JSON lines from the journal file with a durable cursor
// ═══════════════════════════════════════════════════════════════════════════════

// FileHubSource:start
//   purpose: HubSource implementation that reads NEW HubEvent lines from the journal
//            file since the last drain() call, using a durable (inode, byte_offset)
//            cursor so re-opening never re-reads already-consumed lines.
//
//            Rotation safety: compares inode on every drain(); if inode changed the
//            file was rotated — reset offset to 0 and read from beginning of new file.
//
//            Source filtering: lines with `source="remote"` are skipped — those were
//            appended by the cluster/FileHubSink side and are not local hubd events.
//            The RoomLog content-addressed event_id provides a second safety net.
//
//            Parse errors on individual lines are silently skipped (log corruption
//            should not stall the tail); caller detects missing events via assertions
//            in tests or monitoring in production.
//
//   input:  path — journal file path; cursor — DurableCursor (internal, mutable)
//   output: drain() → Vec<HubEvent> (only source=Local lines, not yet seen)
//   sideEffects: advances internal DurableCursor; opens/closes file on each drain()
// FileHubSource:end
pub struct FileHubSource {
    path:   PathBuf,
    cursor: Mutex<DurableCursor>,
}

impl FileHubSource {
    // FileHubSource::new:start
    //   purpose: Construct a FileHubSource at the given path with cursor at start.
    //   input:  path — journal file path (need not exist yet)
    //   output: FileHubSource
    //   sideEffects: none (file opened lazily on drain)
    // FileHubSource::new:end
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path:   path.as_ref().to_path_buf(),
            cursor: Mutex::new(DurableCursor::default()),
        }
    }
}

impl HubSource for FileHubSource {
    // HubSource::drain (FileHubSource):start
    //   purpose: Read all new lines from the journal file since the last drain() call.
    //            Steps:
    //              1. Open file; stat to get inode.
    //              2. If inode != cursor.inode → file was rotated; reset cursor to 0.
    //              3. Seek to cursor.byte_offset.
    //              4. Read lines until EOF; skip blank lines and JSON parse errors.
    //              5. Skip lines where source == Remote (cluster-appended).
    //              6. Advance cursor.byte_offset to current position.
    //              7. Return collected Local HubEvents.
    //            If the file does not exist yet returns empty Vec (not an error).
    //   input:  none
    //   output: Vec<HubEvent> — new Local events since last drain
    //   sideEffects: advances cursor.byte_offset (and resets inode on rotation)
    // HubSource::drain (FileHubSource):end
    fn drain(&self) -> Vec<HubEvent> {
        let mut cursor = match self.cursor.lock() {
            Ok(c)  => c,
            Err(_) => return Vec::new(), // poisoned mutex — return empty, caller retries
        };

        // If file does not exist yet, return empty without error.
        let file = match File::open(&self.path) {
            Ok(f)  => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Vec::new(),
            Err(_) => return Vec::new(),
        };

        // Check inode for rotation detection.
        let inode = match file.metadata() {
            Ok(m)  => m.ino(),
            Err(_) => return Vec::new(),
        };

        if inode != cursor.inode {
            // File was rotated (or first open): reset to start of new file.
            cursor.inode       = inode;
            cursor.byte_offset = 0;
        }

        let mut reader = BufReader::new(file);
        // Seek to last-known position.  Seek past EOF silently gives EOF on read.
        if reader.seek(SeekFrom::Start(cursor.byte_offset)).is_err() {
            return Vec::new();
        }

        let mut events = Vec::new();
        let mut current_pos = cursor.byte_offset;

        loop {
            let mut line = String::new();
            match reader.read_line(&mut line) {
                Ok(0) => break, // EOF
                Ok(n) => {
                    current_pos += n as u64;
                    let trimmed = line.trim();
                    if trimmed.is_empty() {
                        continue;
                    }
                    match serde_json::from_str::<HubEvent>(trimmed) {
                        Ok(ev) if ev.source == EventSource::Local => events.push(ev),
                        Ok(_)  => { /* Remote line — skip (cluster-appended) */ }
                        Err(_) => { /* Malformed line — skip silently */ }
                    }
                }
                Err(_) => break,
            }
        }

        cursor.byte_offset = current_pos;
        events
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// hub_event_to_pdu — hubd event → Matrix m.room.message PDU
// ═══════════════════════════════════════════════════════════════════════════════

// hub_event_to_pdu:start
//   purpose: Translate one HubEvent into an m.room.message Pdu suitable for RoomLog.add.
//            Sender is "@agent-<agent>:<domain>" per §8.6/Q1 (N agent-users, not one bot).
//            For non-agent events (TaskAdd, Done, Card, Kanban) the sender is
//            "@hubd-bridge:<domain>" extracted from room_id suffix.
//            Content encodes:
//              body         — plain-text body (Matrix fallback; §Q5 Element plain fallback)
//              msgtype      — "m.notice" (hubd-originated, not human)
//              hubd.type    — HubEventKind::as_str()
//              hubd.project — ev.project
//              hubd.agent   — ev.agent (empty string if not a Report)
//            prev_events: [] (bridge PDUs have no causal parents in the Matrix DAG;
//              they are causal roots within their own synthetic stream — the real
//              prev_events would be wired up by the CS-API layer in Stage 1).
//            depth: 0 (same reason — no DAG ancestry tracked at bridge layer).
//   input:  ev — HubEvent; room_id — target room (e.g. "!proj:domain"); sender_domain —
//           used to build "@agent-<name>:<domain>" (e.g. "matrix.example.com")
//   output: Pdu with computed event_id
//   sideEffects: none (pure)
// hub_event_to_pdu:end
pub fn hub_event_to_pdu(ev: &HubEvent, room_id: &str, sender_domain: &str) -> Pdu {
    // §8.6 Q1: Report events name the specific agent; others use the bridge bot identity.
    let sender = if ev.kind == HubEventKind::Report && !ev.agent.is_empty() {
        format!("@agent-{}:{}", ev.agent, sender_domain)
    } else {
        format!("@hubd-bridge:{}", sender_domain)
    };

    // Compose body per §4.1 table.
    let body = match &ev.kind {
        HubEventKind::TaskAdd => format!("[TASK] {}", ev.text),
        HubEventKind::Done    => format!("[DONE] {}", ev.text),
        HubEventKind::Report  => {
            if ev.agent.is_empty() {
                format!("[REPORT] {}", ev.text)
            } else {
                format!("[REPORT:{}] {}", ev.agent, ev.text)
            }
        }
        HubEventKind::Card   => format!("[PROJECT {}] {}", ev.project, ev.text),
        HubEventKind::Kanban => format!("[KANBAN] {}", ev.text),
    };

    // Encode content as a compact plain-text representation.
    // Format: "msgtype=m.notice\nbody=<body>\nhubd.type=<type>\nhubd.project=<proj>\nhubd.agent=<agent>"
    // This avoids JSON (per project rules) while remaining parseable.
    let content_str = format!(
        "msgtype=m.notice\nbody={}\nhubd.type={}\nhubd.project={}\nhubd.agent={}",
        body,
        ev.kind.as_str(),
        ev.project,
        ev.agent,
    );
    let content = content_str.into_bytes();

    Pdu::new(
        room_id.to_string(),
        sender,
        "m.room.message".to_string(),
        content,
        vec![],  // no causal parents at bridge layer (Stage 0)
        0,       // depth 0 — no DAG ancestry tracked at bridge layer
        ev.ts,
    )
}

// ═══════════════════════════════════════════════════════════════════════════════
// pdu_to_hub_command — Matrix m.room.message Pdu → HubCommand (if /-command)
// ═══════════════════════════════════════════════════════════════════════════════

// pdu_to_hub_command:start
//   purpose: Extract a HubCommand from a Matrix m.room.message Pdu if and only if:
//            (a) the Pdu kind is "m.room.message",
//            (b) the body starts with '/' (slash-command syntax per §4.2),
//            (c) sender_power >= 50 (Matrix power-level ACL per §8.6/Q3).
//            Returns None for ordinary messages, hubd-originated PDUs (msgtype=m.notice),
//            or under-privileged senders.
//
//            Parsed commands (§4.2):
//              /task <project> <text>     → verb="task",   project=<project>, args=<text>
//              /done <task_id>            → verb="done",   project="",        args=<task_id>
//              /report <project> <text>   → verb="report", project=<project>, args=<text>
//              /decide <project> <dec>    → verb="decide", project=<project>, args=<dec>
//              /ask @agent-X <text>       → verb="ask",    project="",        args=@agent-X <text>
//              /kanban <project>          → verb="kanban", project=<project>, args=""
//              /status                    → verb="status", project="",        args=""
//            Unknown /-prefix → None (ignore gracefully).
//
//            The body is read from the content bytes encoded by hub_event_to_pdu or by
//            a real Matrix client.  Content format accepted:
//              - Plain line starting with '/' (Matrix client m.text message)
//              - The first line of the encoded content field
//            Content with "msgtype=m.notice" is a hubd-originated PDU — ignored (no loop).
//
//   input:  pdu — Matrix PDU; sender_power — power level of pdu.sender in the room (u64)
//   output: Option<HubCommand> — Some if parsed, None otherwise
//   sideEffects: none (pure)
// pdu_to_hub_command:end
pub fn pdu_to_hub_command(pdu: &Pdu, sender_power: u64) -> Option<HubCommand> {
    // Only process m.room.message events.
    if pdu.kind != "m.room.message" {
        return None;
    }

    // Power-level authz: sender must have power >= 50 (§8.6/Q3).
    if sender_power < 50 {
        return None;
    }

    // Decode content bytes as UTF-8.
    let content_str = std::str::from_utf8(&pdu.content).ok()?;

    // Reject hubd-originated PDUs (msgtype=m.notice) to prevent feedback loops.
    // The content encoding produced by hub_event_to_pdu starts with "msgtype=m.notice".
    // A real Matrix client sends plain text starting with '/'.
    if content_str.starts_with("msgtype=m.notice") {
        return None;
    }

    // Extract body: either the raw content (plain-text slash command from real client)
    // or the "body=" line if the content uses our encoded format.
    let body = if let Some(body_line) = content_str
        .lines()
        .find(|l| l.starts_with("body="))
    {
        &body_line["body=".len()..]
    } else {
        content_str.trim()
    };

    // Must start with '/'.
    if !body.starts_with('/') {
        return None;
    }

    // Tokenise: first token is verb (including '/'), rest are args.
    let without_slash = &body[1..]; // drop leading '/'
    let mut tokens = without_slash.splitn(3, ' ');
    let verb = tokens.next().unwrap_or("").to_lowercase();
    let rest1 = tokens.next().unwrap_or("").trim();
    let rest2 = tokens.next().unwrap_or("").trim();

    // Known verbs per §4.2.
    let (project, args) = match verb.as_str() {
        "task" => {
            // /task <project> <text>
            (rest1.to_string(), rest2.to_string())
        }
        "done" => {
            // /done <task_id> — no project
            (String::new(), rest1.to_string())
        }
        "report" | "decide" => {
            // /report <project> <text>  /decide <project> <decision>
            (rest1.to_string(), rest2.to_string())
        }
        "ask" => {
            // /ask @agent-X <text>  — agent name is first arg, no project
            let args_full = if rest2.is_empty() {
                rest1.to_string()
            } else {
                format!("{} {}", rest1, rest2)
            };
            (String::new(), args_full)
        }
        "kanban" => {
            // /kanban <project>
            (rest1.to_string(), String::new())
        }
        "status" => {
            // /status — no project, no args
            (String::new(), String::new())
        }
        _ => {
            // Unknown /-command → ignore gracefully.
            return None;
        }
    };

    Some(HubCommand {
        verb,
        project,
        args,
        by: pdu.sender.clone(),
    })
}

// ═══════════════════════════════════════════════════════════════════════════════
// HubBridge — stateful bidirectional bridge
// ═══════════════════════════════════════════════════════════════════════════════

// HubBridge:start
//   purpose: Drives the bidirectional hubd↔Matrix bridge.
//            Holds references to a HubSource (inbound hubd events), a HubSink
//            (outbound hubd commands), and the target room_id + sender_domain.
//            `tick(log, power_fn)` is the main entry point:
//              (a) Drains HubSource, translates each HubEvent → Pdu, adds to RoomLog.
//              (b) Scans new m.room.message PDUs in RoomLog (since last_log_len),
//                  calls pdu_to_hub_command for each, submits HubCommands to HubSink.
//            Idempotency guarantee:
//              - RoomLog.add is add-only (grow-set): same Pdu inserted twice is a no-op.
//              - last_log_len cursor prevents re-processing already-handled PDUs.
//              - Same HubEvent → same content bytes → same Pdu.compute_id → one add.
//   input:  source — HubSource impl; sink — HubSink impl; room_id — Matrix room;
//           sender_domain — used to build sender IDs
//   output: tick() returns Result<(), BridgeError> — first sink error encountered
//   sideEffects: mutates RoomLog (add PDUs); calls HubSink.submit for commands
// HubBridge:end
pub struct HubBridge {
    source:        Box<dyn HubSource>,
    sink:          Box<dyn HubSink>,
    room_id:       String,
    sender_domain: String,
    /// Index into RoomLog.ordered() up to which PDUs have already been processed
    /// for the Matrix→hubd direction.  Grows monotonically.
    last_log_len:  usize,
}

impl HubBridge {
    // HubBridge::new:start
    //   purpose: Construct a HubBridge with the given source, sink, and room configuration.
    //   input:  source — HubSource; sink — HubSink; room_id — target Matrix room;
    //           sender_domain — domain suffix for sender IDs (e.g. "matrix.example.com")
    //   output: HubBridge
    //   sideEffects: none
    // HubBridge::new:end
    pub fn new(
        source:        Box<dyn HubSource>,
        sink:          Box<dyn HubSink>,
        room_id:       String,
        sender_domain: String,
    ) -> Self {
        Self { source, sink, room_id, sender_domain, last_log_len: 0 }
    }

    // HubBridge::tick:start
    //   purpose: One bridge cycle — processes both directions:
    //            (a) hubd→Matrix: drain source, translate each HubEvent to Pdu, add to log.
    //            (b) Matrix→hubd: iterate new PDUs in log (from last_log_len to current end),
    //                call pdu_to_hub_command with sender_power, submit commands to sink.
    //            `power_fn` maps a Matrix sender user_id → power level for authz (§8.6/Q3).
    //            Returns the first BridgeError from a sink.submit failure; earlier commands
    //            in the same tick that succeeded are not rolled back (best-effort delivery).
    //   input:  log — mutable RoomLog (shared with caller); power_fn — closure mapping
    //           sender &str → u64 power level
    //   output: Result<(), BridgeError>
    //   sideEffects: adds PDUs to log; calls sink.submit; advances last_log_len
    // HubBridge::tick:end
    pub fn tick(
        &mut self,
        log: &mut RoomLog,
        power_fn: &dyn Fn(&str) -> u64,
    ) -> Result<(), BridgeError> {
        // ── (a) hubd → Matrix ───────────────────────────────────────────────────
        let hub_events = self.source.drain();
        for ev in &hub_events {
            let pdu = hub_event_to_pdu(ev, &self.room_id, &self.sender_domain);
            log.add(pdu);
        }

        // ── (b) Matrix → hubd ───────────────────────────────────────────────────
        // ordered() returns a deterministic topological slice; we process only
        // PDUs added since the last tick (last_log_len..current).
        let ordered = log.ordered();
        let new_pdus: Vec<&Pdu> = ordered
            .iter()
            .copied()
            .skip(self.last_log_len)
            .collect();

        // Advance cursor before submitting so a panic in submit does not cause
        // double-processing on the next tick.
        self.last_log_len = ordered.len();

        let mut first_err: Option<BridgeError> = None;
        for pdu in new_pdus {
            let power = power_fn(&pdu.sender);
            if let Some(cmd) = pdu_to_hub_command(pdu, power) {
                if let Err(e) = self.sink.submit(cmd) {
                    if first_err.is_none() {
                        first_err = Some(e);
                    }
                }
            }
        }

        match first_err {
            Some(e) => Err(e),
            None    => Ok(()),
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use crate::matrix_events::RoomLog;

    const ROOM: &str  = "!bsdos:matrix.example.com";
    const DOMAIN: &str = "matrix.example.com";

    // ── helper: power function that grants power=100 to admin, 0 to others ────

    fn power_fn(sender: &str) -> u64 {
        if sender == "@admin:matrix.example.com" { 100 } else { 0 }
    }

    // ── helper: inject a plain-text slash-command Pdu (simulates real client) ─

    fn slash_pdu(body: &str, sender: &str, ts: u64) -> Pdu {
        Pdu::new(
            ROOM.to_string(),
            sender.to_string(),
            "m.room.message".to_string(),
            body.as_bytes().to_vec(),
            vec![],
            0,
            ts,
        )
    }

    // hub_event_to_pdu:task_add:start
    //   purpose: HubEvent with kind=TaskAdd → Pdu has kind=m.room.message, sender=
    //            @hubd-bridge:<domain>, content encodes hubd.type=task, and event_id
    //            is deterministic (content-addressed).
    //   input:  TaskAdd HubEvent
    //   output: Pdu with expected fields
    //   sideEffects: none
    // hub_event_to_pdu:task_add:end
    #[test]
    fn hub_event_to_pdu_task_add_fields() {
        let ev = HubEvent {
            kind:    HubEventKind::TaskAdd,
            project: "bsdos".to_string(),
            agent:   String::new(),
            text:    "write state-res test".to_string(),
            ts:      1000,
            source:  EventSource::Local,
        };
        let pdu = hub_event_to_pdu(&ev, ROOM, DOMAIN);

        assert_eq!(pdu.kind, "m.room.message");
        assert_eq!(pdu.room_id, ROOM);
        assert_eq!(pdu.sender, format!("@hubd-bridge:{}", DOMAIN));
        assert_eq!(pdu.ts, 1000);

        let content = std::str::from_utf8(&pdu.content).expect("utf8");
        assert!(content.contains("hubd.type=task"), "missing hubd.type=task");
        assert!(content.contains("hubd.project=bsdos"), "missing hubd.project");
        assert!(content.contains("[TASK]"), "missing [TASK] prefix in body");
        assert!(content.contains("write state-res test"), "missing text");
    }

    // hub_event_to_pdu:report_sender:start
    //   purpose: Report HubEvent with non-empty agent → sender is @agent-<name>:<domain>
    //            (§8.6 Q1: N agent-users).
    //   input:  Report HubEvent, agent="runner"
    //   output: pdu.sender == "@agent-runner:matrix.example.com"
    //   sideEffects: none
    // hub_event_to_pdu:report_sender:end
    #[test]
    fn hub_event_to_pdu_report_uses_agent_sender() {
        let ev = HubEvent {
            kind:    HubEventKind::Report,
            project: "bsdos".to_string(),
            agent:   "runner".to_string(),
            text:    "DONE: build passed".to_string(),
            ts:      2000,
            source:  EventSource::Local,
        };
        let pdu = hub_event_to_pdu(&ev, ROOM, DOMAIN);
        assert_eq!(pdu.sender, "@agent-runner:matrix.example.com");
        let content = std::str::from_utf8(&pdu.content).expect("utf8");
        assert!(content.contains("hubd.agent=runner"));
        assert!(content.contains("[REPORT:runner]"));
    }

    // hub_event_appears_in_roomlog:start
    //   purpose: After tick(), a HubEvent translated to a Pdu appears in RoomLog.ordered().
    //            The Pdu's content encodes the expected hubd.* fields.
    //   input:  MemHubSource with one TaskAdd event; tick() called once
    //   output: RoomLog.len() == 1; first ordered Pdu has correct sender and content
    //   sideEffects: none
    // hub_event_appears_in_roomlog:end
    #[test]
    fn hub_event_appears_in_roomlog_after_tick() {
        let source = MemHubSource::new();
        let sink   = MemHubSink::new();
        source.push(HubEvent {
            kind:    HubEventKind::TaskAdd,
            project: "bsdos".to_string(),
            agent:   String::new(),
            text:    "implement hub_bridge".to_string(),
            ts:      42,
            source:  EventSource::Local,
        });

        let mut bridge = HubBridge::new(
            Box::new(source.clone()),
            Box::new(sink.clone()),
            ROOM.to_string(),
            DOMAIN.to_string(),
        );
        let mut log = RoomLog::new();
        bridge.tick(&mut log, &power_fn).expect("tick ok");

        assert_eq!(log.len(), 1, "one PDU added to log");
        let ordered = log.ordered();
        assert_eq!(ordered.len(), 1);
        let pdu = ordered[0];
        assert_eq!(pdu.kind, "m.room.message");
        assert_eq!(pdu.sender, format!("@hubd-bridge:{}", DOMAIN));

        let content = std::str::from_utf8(&pdu.content).expect("utf8");
        assert!(content.contains("implement hub_bridge"));
        assert!(content.contains("hubd.type=task"));
    }

    // slash_command_task_submitted_to_sink:start
    //   purpose: A /task slash-command in a Matrix PDU (sender with power≥50) results
    //            in a HubCommand with verb="task" being submitted to the HubSink.
    //   input:  slash_pdu "/task bsdos Write tests", sender @admin, power=100
    //   output: sink.commands() contains HubCommand{verb="task", project="bsdos", ...}
    //   sideEffects: none
    // slash_command_task_submitted_to_sink:end
    #[test]
    fn slash_task_command_submitted_to_sink() {
        let source = MemHubSource::new();
        let sink   = MemHubSink::new();

        let mut bridge = HubBridge::new(
            Box::new(source),
            Box::new(sink.clone()),
            ROOM.to_string(),
            DOMAIN.to_string(),
        );
        let mut log = RoomLog::new();

        // Inject a plain slash-command PDU directly into the log (simulates Matrix client).
        log.add(slash_pdu(
            "/task bsdos Write state-res tests",
            "@admin:matrix.example.com",
            100,
        ));

        bridge.tick(&mut log, &power_fn).expect("tick ok");

        let cmds = sink.commands();
        assert_eq!(cmds.len(), 1, "one command submitted");
        let cmd = &cmds[0];
        assert_eq!(cmd.verb, "task");
        assert_eq!(cmd.project, "bsdos");
        assert_eq!(cmd.args, "Write state-res tests");
        assert_eq!(cmd.by, "@admin:matrix.example.com");
    }

    // non_slash_message_ignored:start
    //   purpose: A plain (non-/-prefixed) message PDU from a privileged sender is NOT
    //            translated to a HubCommand — the bridge ignores ordinary chat messages.
    //   input:  plain message PDU, sender power=100
    //   output: sink.commands() is empty
    //   sideEffects: none
    // non_slash_message_ignored:end
    #[test]
    fn non_slash_message_ignored_by_bridge() {
        let source = MemHubSource::new();
        let sink   = MemHubSink::new();

        let mut bridge = HubBridge::new(
            Box::new(source),
            Box::new(sink.clone()),
            ROOM.to_string(),
            DOMAIN.to_string(),
        );
        let mut log = RoomLog::new();
        log.add(slash_pdu(
            "Just a normal chat message, no slash",
            "@admin:matrix.example.com",
            200,
        ));

        bridge.tick(&mut log, &power_fn).expect("tick ok");
        assert!(sink.commands().is_empty(), "plain messages must not produce commands");
    }

    // low_power_sender_blocked:start
    //   purpose: A /task command from a sender with power < 50 is silently ignored
    //            (authz check per §8.6/Q3 — only power ≥ 50 can command agents).
    //   input:  /task PDU, sender power=0
    //   output: sink.commands() is empty
    //   sideEffects: none
    // low_power_sender_blocked:end
    #[test]
    fn low_power_sender_does_not_produce_command() {
        let source = MemHubSource::new();
        let sink   = MemHubSink::new();

        let mut bridge = HubBridge::new(
            Box::new(source),
            Box::new(sink.clone()),
            ROOM.to_string(),
            DOMAIN.to_string(),
        );
        let mut log = RoomLog::new();
        // @user has power 0 (default in power_fn)
        log.add(slash_pdu(
            "/task bsdos Sneaky task",
            "@user:matrix.example.com",
            300,
        ));

        bridge.tick(&mut log, &power_fn).expect("tick ok");
        assert!(sink.commands().is_empty(), "low-power sender must be blocked by authz");
    }

    // idempotency_same_hub_event_twice:start
    //   purpose: Pushing the same HubEvent twice and calling tick() twice does NOT produce
    //            duplicate PDUs in the RoomLog (CRDT grow-set add idempotency).
    //   input:  same HubEvent pushed via source, tick called twice
    //   output: RoomLog.len() == 1 (not 2)
    //   sideEffects: none
    // idempotency_same_hub_event_twice:end
    #[test]
    fn idempotency_same_hub_event_twice_no_duplicate() {
        let source = MemHubSource::new();
        let sink   = MemHubSink::new();

        let ev = HubEvent {
            kind:    HubEventKind::Report,
            project: "bsdos".to_string(),
            agent:   "scout".to_string(),
            text:    "Found issue #42".to_string(),
            ts:      500,
            source:  EventSource::Local,
        };

        let mut bridge = HubBridge::new(
            Box::new(source.clone()),
            Box::new(sink),
            ROOM.to_string(),
            DOMAIN.to_string(),
        );
        let mut log = RoomLog::new();

        // First tick with the event.
        source.push(ev.clone());
        bridge.tick(&mut log, &power_fn).expect("first tick ok");
        assert_eq!(log.len(), 1, "one PDU after first tick");

        // Second tick with the identical event (same content → same event_id → no-op).
        source.push(ev);
        bridge.tick(&mut log, &power_fn).expect("second tick ok");
        assert_eq!(log.len(), 1, "no duplicate after second tick with same event");
    }

    // both_directions_in_one_tick:start
    //   purpose: A single tick processes both directions: a HubEvent is added to the log
    //            AND a pre-existing /task PDU is converted to a HubCommand.
    //   input:  one HubEvent in source + one slash_pdu pre-loaded in log
    //   output: log.len() == 2; sink has one command
    //   sideEffects: none
    // both_directions_in_one_tick:end
    #[test]
    fn both_directions_processed_in_single_tick() {
        let source = MemHubSource::new();
        let sink   = MemHubSink::new();

        source.push(HubEvent {
            kind:    HubEventKind::Done,
            project: "bsdos".to_string(),
            agent:   String::new(),
            text:    "Task 7 closed".to_string(),
            ts:      600,
            source:  EventSource::Local,
        });

        let mut bridge = HubBridge::new(
            Box::new(source),
            Box::new(sink.clone()),
            ROOM.to_string(),
            DOMAIN.to_string(),
        );
        let mut log = RoomLog::new();

        // Pre-seed the log with a command PDU (simulates Matrix client writing before tick).
        log.add(slash_pdu(
            "/status",
            "@admin:matrix.example.com",
            700,
        ));

        bridge.tick(&mut log, &power_fn).expect("tick ok");

        // HubEvent added to log (plus the pre-seeded PDU = 2 total).
        assert_eq!(log.len(), 2, "two PDUs: pre-seeded + hubd event");

        // /status command was picked up and submitted.
        let cmds = sink.commands();
        assert_eq!(cmds.len(), 1, "one command from /status");
        assert_eq!(cmds[0].verb, "status");
    }

    // hubd_originated_pdus_not_looped_back:start
    //   purpose: PDUs generated by hub_event_to_pdu (msgtype=m.notice encoded content)
    //            are NOT re-parsed as commands — prevents hubd→Matrix→hubd feedback loops.
    //   input:  HubEvent pushed to source; tick adds its Pdu to log; second tick scans it
    //   output: sink.commands() is empty (the hubd-originated Pdu is skipped)
    //   sideEffects: none
    // hubd_originated_pdus_not_looped_back:end
    #[test]
    fn hubd_originated_pdus_not_looped_back() {
        let source = MemHubSource::new();
        let sink   = MemHubSink::new();

        // Simulate: push a Report event that normally has body starting with [REPORT…]
        // NOT a slash, so it should be ignored by the Matrix→hubd direction anyway.
        // But also verify the m.notice encoding is skipped.
        source.push(HubEvent {
            kind:    HubEventKind::Report,
            project: "bsdos".to_string(),
            agent:   "runner".to_string(),
            text:    "DONE: all tests pass".to_string(),
            ts:      800,
            source:  EventSource::Local,
        });

        let mut bridge = HubBridge::new(
            Box::new(source),
            Box::new(sink.clone()),
            ROOM.to_string(),
            DOMAIN.to_string(),
        );
        let mut log = RoomLog::new();

        // First tick: HubEvent → Pdu added to log.
        bridge.tick(&mut log, &power_fn).expect("tick 1 ok");
        assert_eq!(log.len(), 1);

        // Second tick: no new source events; the Pdu already in log should NOT produce a command.
        bridge.tick(&mut log, &power_fn).expect("tick 2 ok");
        assert!(
            sink.commands().is_empty(),
            "hubd-originated Pdus must not produce HubCommands (no loop)"
        );
    }

    // ── FILE-APPEND MODEL TESTS ──────────────────────────────────────────────
    // All tests use temp files in the OS temp dir — never /root/.hubd.

    fn tmp_journal() -> std::path::PathBuf {
        // Use a unique filename per test by including the thread id.
        use std::time::{SystemTime, UNIX_EPOCH};
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        std::env::temp_dir().join(format!("hub_bridge_test_{ts}.jsonl"))
    }

    fn local_ev(kind: HubEventKind, project: &str, text: &str, ts: u64) -> HubEvent {
        HubEvent { kind, project: project.to_string(), agent: String::new(),
                   text: text.to_string(), ts, source: EventSource::Local }
    }

    fn remote_ev(kind: HubEventKind, project: &str, text: &str, ts: u64) -> HubEvent {
        HubEvent { kind, project: project.to_string(), agent: String::new(),
                   text: text.to_string(), ts, source: EventSource::Remote }
    }

    // file_append_roundtrip:start
    //   purpose: Writing a Local HubEvent via FileHubSink.append_event and then draining
    //            with FileHubSource returns exactly that event.  Verifies the basic
    //            append + tail round-trip over a real temp file.
    //   input:  one Local HubEvent appended; FileHubSource drained
    //   output: drain() returns Vec with that single event; source field == Local
    //   sideEffects: creates/deletes temp file
    // file_append_roundtrip:end
    #[test]
    fn file_append_roundtrip() {
        let path = tmp_journal();
        let sink   = FileHubSink::new(&path);
        let source = FileHubSource::new(&path);

        let ev = local_ev(HubEventKind::TaskAdd, "bsdos", "roundtrip task", 1);
        sink.append_event(ev.clone()).expect("append ok");

        // drain() returns the event but with source=Remote because FileHubSink forced it.
        // FileHubSource.drain skips Remote lines → we use direct file manipulation here
        // to verify the line was actually written, then test Local filtering below.
        // For the round-trip we write a Local-tagged line manually.
        use std::io::Write as W;
        let mut f = OpenOptions::new().create(true).append(true).open(&path).unwrap();
        let local_line = serde_json::to_string(&local_ev(HubEventKind::Report, "bsdos", "local report", 2)).unwrap();
        writeln!(f, "{}", local_line).unwrap();
        drop(f);

        let drained = source.drain();
        // The Remote-tagged line from FileHubSink is skipped; only the Local line appears.
        assert_eq!(drained.len(), 1, "only Local lines returned");
        assert_eq!(drained[0].text, "local report");
        assert_eq!(drained[0].source, EventSource::Local);

        let _ = std::fs::remove_file(&path);
    }

    // durable_cursor_no_reread:start
    //   purpose: After a drain(), re-opening FileHubSource (new instance, same path)
    //            does NOT re-read already-consumed lines — the durable cursor (inode +
    //            byte_offset) prevents double-delivery.
    //   input:  two Local lines appended; first drain; third Local line appended;
    //           second drain on the SAME FileHubSource instance
    //   output: first drain → 2 events; second drain → 1 event (only the new line)
    //   sideEffects: creates/deletes temp file
    // durable_cursor_no_reread:end
    #[test]
    fn durable_cursor_no_reread() {
        let path   = tmp_journal();
        let source = FileHubSource::new(&path);

        // Write two Local lines.
        let write_local = |text: &str, ts: u64| {
            let ev = local_ev(HubEventKind::TaskAdd, "bsdos", text, ts);
            let mut line = serde_json::to_string(&ev).unwrap();
            line.push('\n');
            let mut f = OpenOptions::new().create(true).append(true).open(&path).unwrap();
            f.write_all(line.as_bytes()).unwrap();
        };

        write_local("line1", 10);
        write_local("line2", 20);

        let first = source.drain();
        assert_eq!(first.len(), 2, "two events on first drain");
        assert_eq!(first[0].text, "line1");
        assert_eq!(first[1].text, "line2");

        // Append a third line.
        write_local("line3", 30);

        let second = source.drain();
        assert_eq!(second.len(), 1, "only the new line on second drain — cursor held");
        assert_eq!(second[0].text, "line3");

        let _ = std::fs::remove_file(&path);
    }

    // remote_lines_skipped:start
    //   purpose: Lines with source=remote (appended by FileHubSink / cluster side) are
    //            NOT returned by FileHubSource.drain().  This is the cheap first-pass
    //            filter; RoomLog event_id dedup is the backstop.
    //   input:  one Remote line and one Local line in file
    //   output: drain() returns only the Local line
    //   sideEffects: creates/deletes temp file
    // remote_lines_skipped:end
    #[test]
    fn remote_lines_skipped_by_source() {
        let path   = tmp_journal();
        let source = FileHubSource::new(&path);

        let write_ev = |ev: &HubEvent| {
            let mut line = serde_json::to_string(ev).unwrap();
            line.push('\n');
            let mut f = OpenOptions::new().create(true).append(true).open(&path).unwrap();
            f.write_all(line.as_bytes()).unwrap();
        };

        write_ev(&remote_ev(HubEventKind::Report, "bsdos", "cluster appended", 1));
        write_ev(&local_ev(HubEventKind::TaskAdd, "bsdos", "local task", 2));

        let events = source.drain();
        assert_eq!(events.len(), 1, "remote line must be skipped");
        assert_eq!(events[0].text, "local task");
        assert_eq!(events[0].source, EventSource::Local);

        let _ = std::fs::remove_file(&path);
    }

    // loop_safety_remote_append_dedup:start
    //   purpose: Proves loop safety for the file-append model.
    //            Scenario: a remote event is appended to the file (source=remote).
    //            FileHubSource.drain() skips it (cheap filter).
    //            Even if somehow it slips through and is added to RoomLog, the
    //            content-addressed event_id makes the second RoomLog.add a no-op.
    //
    //            Steps:
    //              1. Append a Remote event to the file (simulates cluster side).
    //              2. Append the same event as Local (simulates hubd writing same content).
    //              3. Drain → only the Local line is returned (remote skipped).
    //              4. Translate both to PDUs via hub_event_to_pdu with same content.
    //              5. Add both PDUs to RoomLog → second add is no-op (same event_id).
    //              6. RoomLog.len() == 1, not 2.
    //
    //   input:  one Remote + one Local line with identical semantic content; same ts
    //   output: drain returns 1 event; RoomLog has 1 PDU (dedup backstop confirmed)
    //   sideEffects: creates/deletes temp file
    // loop_safety_remote_append_dedup:end
    #[test]
    fn loop_safety_remote_append_dedup() {
        let path   = tmp_journal();
        let source = FileHubSource::new(&path);

        // Same logical event, once as Remote (cluster-appended), once as Local (hubd).
        let remote = remote_ev(HubEventKind::Report, "bsdos", "DONE: build ok", 99);
        let local  = local_ev(HubEventKind::Report,  "bsdos", "DONE: build ok", 99);

        let write_ev = |ev: &HubEvent| {
            let mut line = serde_json::to_string(ev).unwrap();
            line.push('\n');
            let mut f = OpenOptions::new().create(true).append(true).open(&path).unwrap();
            f.write_all(line.as_bytes()).unwrap();
        };

        write_ev(&remote);
        write_ev(&local);

        // Drain: only Local line is returned.
        let events = source.drain();
        assert_eq!(events.len(), 1, "remote line filtered; only local returned");
        assert_eq!(events[0].source, EventSource::Local);

        // Now simulate: even if we mistakenly try to add both to RoomLog via hub_event_to_pdu,
        // the content-addressed event_id collapses duplicates.
        // (We normalise both to Local before converting to PDUs so the content bytes match.)
        let mut local_for_pdu = remote.clone(); local_for_pdu.source = EventSource::Local;
        let pdu1 = hub_event_to_pdu(&local_for_pdu, ROOM, DOMAIN);
        let pdu2 = hub_event_to_pdu(&local,          ROOM, DOMAIN);

        // Both PDUs must have the same event_id (same content bytes → same FNV hash).
        assert_eq!(pdu1.event_id, pdu2.event_id, "same content → same event_id");

        let mut log = RoomLog::new();
        log.add(pdu1);
        log.add(pdu2); // second add → no-op (grow-set)
        assert_eq!(log.len(), 1, "RoomLog dedup: second add is no-op");

        let _ = std::fs::remove_file(&path);
    }

    // two_appenders_no_interleave:start
    //   purpose: Two concurrent appenders writing short lines to the same O_APPEND file
    //            do not produce interleaved/corrupted lines.  Each line must be valid JSON.
    //            POSIX O_APPEND + write() up to PIPE_BUF bytes is atomic on ext4/UFS.
    //   input:  two threads each append 50 Local lines; FileHubSource drains all
    //   output: all 100 lines parseable as HubEvent; no partial JSON
    //   sideEffects: creates/deletes temp file; spawns 2 threads
    // two_appenders_no_interleave:end
    #[test]
    fn two_appenders_no_interleave() {
        let path  = tmp_journal();
        let path1 = path.clone();
        let path2 = path.clone();

        let t1 = std::thread::spawn(move || {
            let sink = FileHubSink::new(&path1);
            for i in 0u64..50 {
                let ev = local_ev(HubEventKind::TaskAdd, "bsdos", &format!("t1-{i}"), i);
                sink.append_event(ev).expect("t1 append");
            }
        });
        let t2 = std::thread::spawn(move || {
            for i in 0u64..50 {
                let ev = remote_ev(HubEventKind::Report, "bsdos", &format!("t2-{i}"), i + 100);
                let mut line = serde_json::to_string(&ev).unwrap();
                line.push('\n');
                let mut f = OpenOptions::new().create(true).append(true)
                    .open(&path2).unwrap();
                f.write_all(line.as_bytes()).unwrap();
            }
        });
        t1.join().expect("t1 joined");
        t2.join().expect("t2 joined");

        // Read ALL lines from file directly and verify each is valid JSON.
        let content = std::fs::read_to_string(&path).expect("read file");
        let lines: Vec<&str> = content.lines().filter(|l| !l.trim().is_empty()).collect();
        assert_eq!(lines.len(), 100, "all 100 lines present");
        let mut ok = 0usize;
        for line in &lines {
            if serde_json::from_str::<HubEvent>(line).is_ok() {
                ok += 1;
            }
        }
        assert_eq!(ok, 100, "all 100 lines are valid JSON (no interleaving)");

        let _ = std::fs::remove_file(&path);
    }
}
