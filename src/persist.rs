// START_AI_HEADER
// MODULE: matrix-hs/src/persist.rs
// PURPOSE: Durable append-only journal for rooms, users, and aliases.
//          When MATRIX_HS_DATA_DIR is set, every admitted event is fsynced to a
//          per-room JSON-lines file; on startup the journals are replayed to restore
//          full state without any extra coordination.
//
//          Design: CRDT-native append-log + replay.
//            - RoomLog is a grow-only set of immutable PDUs → journal = one line per PDU.
//            - Restart = replay = re-merge: idempotent by event_id (RoomLog.add is no-op
//              for duplicates, users/aliases use last-wins).
//            - Durability model: best-effort-but-loud.  On any fs error we log to stderr
//              and CONTINUE — never crash or fail the client request over a disk hiccup.
//              Rationale: losing one write beats crashing the server; the admin can
//              inspect stderr, fix the fs issue, and replay from disk at the next restart.
//            - Data dir: MATRIX_HS_DATA_DIR env.  Unset → persistence disabled (no-ops).
//
//          Journal layout under <data_dir>:
//            rooms/<sanitized_room_id>.jsonl          — one JSON line per room event
//            rooms/<sanitized_room_id>.pdumeta.jsonl  — signed-PDU metadata (internal-task), keyed
//                                                        by event_id: sig/signer_node/
//                                                        prev_events/depth.  SEPARATE from
//                                                        the client-event journal above:
//                                                        that JSON feeds room_timeline/`/sync`
//                                                        directly and is rewritten from the
//                                                        clean timeline on compaction, so it
//                                                        must never carry these extra keys.
//                                                        Absent for journals written before
//                                                        internal-task — replay falls back to the
//                                                        unsigned synthetic Pdu in that case.
//            accounts.jsonl                     — user registrations
//            aliases.jsonl                      — alias → room_id mappings
//            media/<sanitized_media_id>         — raw uploaded media bytes
//            media/<sanitized_media_id>.ct      — sibling Content-Type sidecar
//            room_key_versions.jsonl            — E2EE key-backup version lifecycle
//                                                  ops (create/update/delete), one line
//                                                  per op, replayed IN ORDER at startup —
//                                                  see AppState::persist_room_key_version_*
//                                                  and replay_room_key_versions.
//            room_key_data.jsonl                 — E2EE key-backup session-data ops
//                                                  (put/delete), same replay-in-order
//                                                  event-log model as the version journal
//                                                  above — see persist_room_key_data_*
//                                                  and replay_room_key_data. Replaying
//                                                  both journals in order reproduces the
//                                                  same etag counters as the live run
//                                                  (etag is just "count of mutations
//                                                  applied so far" per version).
//
//          Sanitization: room_id strings ("!name:server") and alias strings ("#name:server")
//          contain characters that are illegal in file names on common FS.  We percent-encode
//          the forbidden set {!, #, :, /, \, *, ?, <, >, |, "} plus NUL, and truncate at
//          200 bytes to stay well within FS limits.
//
//          Compaction: compact_room rewrites a room journal atomically (temp-file + rename)
//          as the deduped set of current events (unique by event_id, stable order).
//          compact_room_pdumeta (internal-task) does the same for the sibling pdumeta journal, keeping
//          only meta lines whose event_id is still present after compaction.
//          compact_all reads each <dir>/rooms/*.jsonl file straight off disk (NOT from
//          AppState) and compacts each one (+ its pdumeta). Triggered at startup AFTER
//          replay so that re-appended duplicates accumulated from previous runs are
//          collapsed — keeps journal growth O(unique events). Deliberately independent
//          of AppState.room_timeline's retention cap (timeline_max_events): that cap
//          bounds only the in-memory /sync read projection, never the on-disk journal —
//          coupling compaction to it would silently truncate RoomLog and room_state
//          (membership, power_levels, ...) on the second restart after enabling the cap.
//
// DEPENDENCIES: std::fs, std::io, serde_json, base64, crate::substrate::matrix_events::{Pdu,RoomLog},
//               crate::state::{AppState,StateEvent,UserRecord}
// PUBLIC_API: PersistCtx, replay_from_dir (standalone), compact_room, compact_room_pdumeta,
//             compact_all, AppState persist_* helpers (incl. persist_room_pdu_meta,
//             persist_media_blob)
// END_AI_HEADER

use std::{
    collections::{HashMap, HashSet},
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
};

use crate::state::{AppState, StateEvent, UserRecord};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use crate::substrate::matrix_events::Pdu;
use serde_json::{json, Value};

// ── filename sanitiser ─────────────────────────────────────────────────────────

// sanitize_filename:start
//   purpose: Percent-encode characters that are illegal or problematic in file names
//            on common file-systems (UFS, ZFS, ext4, APFS) so that Matrix room_ids
//            like "!name:server" and aliases like "#name:server" map to safe paths.
//            Forbidden set: ! # : / \ * ? < > | " and NUL (0x00).
//            Truncated to 200 bytes to stay well within FS NAME_MAX limits.
//   input:  s — arbitrary Matrix identifier string
//   output: String — safe filename component (no path separators, ≤200 bytes)
//   sideEffects: none
pub fn sanitize_filename(s: &str) -> String {
    let mut out = String::with_capacity(s.len() * 2);
    for b in s.bytes() {
        match b {
            // Percent-encode forbidden/problematic characters.
            b'!' | b'#' | b':' | b'/' | b'\\' | b'*' | b'?' | b'<' | b'>' | b'|' | b'"' | 0 => {
                out.push('%');
                out.push(char::from_digit((b >> 4) as u32, 16).unwrap_or('0'));
                out.push(char::from_digit((b & 0xf) as u32, 16).unwrap_or('0'));
            }
            // Pass safe ASCII and valid UTF-8 continuation bytes as-is.
            _ => out.push(b as char),
        }
    }
    // Truncate at 200 bytes (safe well below typical NAME_MAX=255).
    if out.len() > 200 {
        out.truncate(200);
    }
    out
}
// sanitize_filename:end

// ── PersistCtx — the context held in AppState ─────────────────────────────────

// PersistCtx:start
//   purpose: Opaque persistence context stored inside AppState.
//            When data_dir is Some(_), journal writes are enabled.
//            When None, all persist_* methods are no-ops (pure in-memory mode,
//            preserving the behaviour of all 13 existing tests which run without
//            MATRIX_HS_DATA_DIR set).
//   input:  constructed by PersistCtx::new(data_dir: Option<PathBuf>)
//   output: PersistCtx value
//   sideEffects: if data_dir is Some, creates the directory tree on construction
pub struct PersistCtx {
    pub data_dir: Option<PathBuf>,
}
// PersistCtx:end

impl PersistCtx {
    // PersistCtx::new:start
    //   purpose: Create a PersistCtx from an optional data directory path.
    //            If data_dir is Some, creates subdirectories rooms/ and the two
    //            flat journal files (accounts.jsonl, aliases.jsonl) on-demand.
    //   input:  data_dir — Some(path) to enable persistence; None for in-memory mode
    //   output: PersistCtx
    //   sideEffects: may create directories and files if data_dir is Some
    pub fn new(data_dir: Option<PathBuf>) -> Self {
        if let Some(ref dir) = data_dir {
            // Best-effort: create directory structure.  Errors are printed but not fatal.
            if let Err(e) = fs::create_dir_all(dir.join("rooms")) {
                eprintln!(
                    "matrix-hs persist: create_dir_all {}/rooms: {e}",
                    dir.display()
                );
            }
        }
        PersistCtx { data_dir }
    }
    // PersistCtx::new:end

    // PersistCtx::enabled:start
    //   purpose: Return true when persistence is active (data_dir is set).
    //   input:  none
    //   output: bool
    //   sideEffects: none
    pub fn enabled(&self) -> bool {
        self.data_dir.is_some()
    }
    // PersistCtx::enabled:end
}

// ── low-level append helpers ───────────────────────────────────────────────────

// append_line:start
//   purpose: Append a JSON-lines record to a file with O_APPEND + fsync semantics.
//            O_APPEND makes the seek-then-write atomic at the OS level; fsync
//            ensures the bytes reach stable storage before we return.
//            File handles are CACHED across calls (one open per path, not one per
//            line) — measured 2026-08-22: open+write+fsync+close per event capped
//            ingest at ~312 ev/s on a decent SSD, and on the weak household node
//            the syscall pair dominates. The cache is invalidated by compaction
//            (which replaces files by rename) under the same mutex, so no handle
//            ever points at a replaced inode. That mutex also makes
//            append-during-compaction safe, which it was NOT before (a rename
//            between another thread's open and write silently lost the line).
//            On any error: print to stderr and return (best-effort, never panic).
//   input:  path — file path to open or create; value — JSON value to serialise as one line
//   output: none (errors logged to stderr, not propagated)
//   sideEffects: opens/creates file (cached), appends one line, calls fsync
fn append_line(path: &Path, value: &Value) {
    let line = match serde_json::to_string(value) {
        Ok(mut s) => {
            s.push('\n');
            s
        }
        Err(e) => {
            eprintln!("matrix-hs persist: encode for {}: {e}", path.display());
            return;
        }
    };
    let result = (|| -> Result<(), Box<dyn std::error::Error>> {
        let mut files = append_files().lock().map_err(|_| "append cache poisoned")?;
        use std::collections::hash_map::Entry;
        let file = match files.entry(path.to_path_buf()) {
            Entry::Occupied(o) => o.into_mut(),
            Entry::Vacant(v) => v.insert(
                OpenOptions::new().create(true).append(true).open(path)?,
            ),
        };
        file.write_all(line.as_bytes())?;
        file.flush()?;
        // Durability policy. Default: fsync every line (the journal is the ONLY
        // copy of an accepted event on a single-node deployment — e.g. the
        // household anchor — so losing the tail means losing messages).
        // MATRIX_HS_PERSIST_FSYNC_INTERVAL_MS=N (>0) batches instead: sync when
        // N ms have passed since the last sync, trading a bounded tail-loss
        // window (N ms of events, on power loss only) for far fewer disk
        // syncs. Measured 2026-08-22: per-event fsync costs ~9 KB of physical
        // writes per ~250 B event on ext4 (journal commits) — the dominant
        // ingest cost on a weak disk.
        let interval = fsync_interval_ms();
        let sync_now = if interval == 0 {
            true
        } else {
            fsync_due(interval)
        };
        if sync_now {
            file.sync_data()?;
        }
        Ok(())
    })();
    if let Err(e) = result {
        eprintln!("matrix-hs persist: append_line {}: {e}", path.display());
    }
}
// append_line:end

// fsync_interval_ms:start
//   purpose: Read MATRIX_HS_PERSIST_FSYNC_INTERVAL_MS once per process.
//            0/absent (the default) = fsync every appended line — the
//            durability this module has always promised. >0 = batch syncs with
//            that many milliseconds between them (bounded loss window).
//   input:  none (env)
//   output: u64 milliseconds
//   sideEffects: none (cached after first read)
fn fsync_interval_ms() -> u64 {
    static V: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("MATRIX_HS_PERSIST_FSYNC_INTERVAL_MS")
            .ok()
            .and_then(|s| s.trim().parse().ok())
            .unwrap_or(0)
    })
}
// fsync_interval_ms:end

// LAST_FSYNC:start
//   purpose: Timestamp of the last batched fsync, shared across all journal
//            files. A Mutex<Option<Instant>>, NOT a OnceLock: OnceLock is
//            write-once, so .set() would silently fail after the first success
//            and batching would degrade to per-event fsync without any error.
//   input:   none (static)
//   output:  none
//   sideEffects: none
static LAST_FSYNC: std::sync::Mutex<Option<std::time::Instant>> =
    std::sync::Mutex::new(None);
// LAST_FSYNC:end

// fsync_due:start
//   purpose: Decide (and record) whether a batched sync is due now, under the
//            LAST_FSYNC lock so concurrent appenders cannot both see "due" and
//            double-sync the interval.
//   input:  interval — milliseconds between syncs
//   output: true when the caller should fsync
//   sideEffects: updates LAST_FSYNC when returning true
fn fsync_due(interval: u64) -> bool {
    let now = std::time::Instant::now();
    let Ok(mut last) = LAST_FSYNC.lock() else {
        return true; // poisoned — fall back to syncing every line
    };
    let due = last
        .map(|t| now.duration_since(t).as_millis() as u64 >= interval)
        .unwrap_or(true);
    if due {
        *last = Some(now);
    }
    due
}
// fsync_due:end

// APPEND_FILES:start
//   purpose: Process-wide cache of O_APPEND handles for journal files, plus the
//            lock that serialises appends against compaction renames (see
//            append_line). Invalidated per-path by compact_room/_pdumeta in the
//            SAME critical section as the rename — a handle opened before the
//            rename still refers to the replaced inode, and an append slipping
//            between rename and cache-drop would write into the unlinked file
//            and vanish.
//   input:   none (static)
//   output:  none
//   sideEffects: none
static APPEND_FILES: std::sync::OnceLock<
    std::sync::Mutex<std::collections::HashMap<PathBuf, std::fs::File>>,
> = std::sync::OnceLock::new();
// APPEND_FILES:end

// append_files:start
//   purpose: Accessor for the APPEND_FILES singleton (OnceLock indirection,
//            because Mutex::new of a HashMap is not const).
//   input:  none
//   output: &'static Mutex<HashMap<PathBuf, File>>
//   sideEffects: initialises on first call
fn append_files(
) -> &'static std::sync::Mutex<std::collections::HashMap<PathBuf, std::fs::File>> {
    APPEND_FILES.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}
// append_files:end

// swap_journal_file:start
//   purpose: Atomically replace a journal file AND drop its cached append
//            handle under one hold of the append lock — the only correct shape
//            for compaction's temp-then-rename. If the cache-drop were outside
//            the critical section, an appender could take the stale handle
//            between rename and removal and its line would be written to the
//            unlinked inode, i.e. silently lost. Errors from remove are ignored:
//            a missing entry is the desired end state anyway.
//   input:  tmp_path — the written temp file; final_path — the live journal path
//   output: Result<(), io::Error> from the rename
//   sideEffects: renames tmp→final; removes final_path from the append cache
fn swap_journal_file(tmp_path: &Path, final_path: &Path) -> std::io::Result<()> {
    let mut files = append_files()
        .lock()
        .map_err(|_| std::io::Error::other("append cache poisoned"))?;
    fs::rename(tmp_path, final_path)?;
    files.remove(final_path);
    Ok(())
}
// swap_journal_file:end

// ── Compaction ─────────────────────────────────────────────────────────────────

// compact_room:start
//   purpose: Atomically rewrite the room journal at <dir>/rooms/<sanitized>.jsonl
//            as the DEDUPED set of current events (unique by event_id), in a stable
//            order (same as they appear when iterating the provided events slice).
//            Uses write-to-temp then rename so a crash mid-compaction cannot corrupt
//            the live journal: the old journal remains intact until rename succeeds.
//            Best-effort: any fs error is printed to stderr and the function returns
//            (old journal untouched).  No-op when events is empty.
//   input:  dir    — data directory root (<data_dir>);
//           room_id — Matrix room identifier (used to derive file name);
//           events — ordered slice of client-event JSON values for this room
//   output: none (errors printed to stderr, not propagated)
//   sideEffects: may create/rename files inside <dir>/rooms/
pub fn compact_room(dir: &Path, room_id: &str, events: &[Value]) {
    if events.is_empty() {
        return;
    }
    let filename = format!("{}.jsonl", sanitize_filename(room_id));
    let final_path = dir.join("rooms").join(&filename);
    let tmp_path = dir.join("rooms").join(format!("{filename}.compact.tmp"));

    // Deduplicate by event_id while preserving first-occurrence order.
    let mut seen_ids: HashSet<&str> = HashSet::new();
    let mut lines: Vec<String> = Vec::with_capacity(events.len());
    for ev in events {
        let event_id = ev.get("event_id").and_then(|v| v.as_str()).unwrap_or("");
        // Skip events without an event_id (malformed) or already seen.
        if event_id.is_empty() || !seen_ids.insert(event_id) {
            continue;
        }
        let mut line = match serde_json::to_string(ev) {
            Ok(l) => l,
            Err(e) => {
                eprintln!("matrix-hs compact_room: {room_id}: encode: {e}");
                return;
            }
        };
        line.push('\n');
        lines.push(line);
    }

    // SKIP-WHEN-CLEAN: compaction exists to remove duplicate event_ids. If the
    // on-disk journal has exactly one line per deduped event, there is nothing
    // to remove and the rewrite would be a byte-for-byte no-op — so don't do
    // it. Measured 2026-08-22: without this check every startup and every
    // post-catch-up merge rewrote the ENTIRE journal set (8 MB written to boot a
    // 7.9 MB store with zero duplicates); on the household node (789 MB store,
    // weak disk) that is minutes of pure write amplification per restart, and
    // it is what "the system hangs" there reduced to. Reading the file to count
    // newlines is cheap next to write+fsync+rename; if the count does not match
    // (dups present, or malformed lines read_room_journal_events skipped), we
    // fall through to the real rewrite — the safe direction.
    if count_newlines(&final_path) == Some(lines.len()) {
        return;
    }

    let result = (|| -> Result<(), Box<dyn std::error::Error>> {
        // Write the deduped content to a temp file.
        let mut tmp = File::create(&tmp_path)?;
        for line in &lines {
            tmp.write_all(line.as_bytes())?;
        }
        tmp.flush()?;
        tmp.sync_data()?;
        drop(tmp);

        // Atomically swap AND drop the stale cached handle in one critical
        // section (see swap_journal_file for why they must be atomic together).
        swap_journal_file(&tmp_path, &final_path)?;
        Ok(())
    })();

    if let Err(e) = result {
        eprintln!("matrix-hs compact_room: {room_id}: {e} (journal untouched)");
        // Best-effort cleanup of orphaned temp file — ignore errors.
        let _ = fs::remove_file(&tmp_path);
    }
}
// compact_room:end

// count_newlines:start
//   purpose: Count '\n' bytes in a file (= its line count for well-formed
//            JSON-lines journals), streaming in 64 KiB chunks. Used by
//            compaction's skip-when-clean check; allocation-free.
//   input:  path — file to scan
//   output: Some(count), or None when the file cannot be read (treated as
//           "unknown" by callers, who then take the safe rewrite path)
//   sideEffects: none
fn count_newlines(path: &Path) -> Option<usize> {
    use std::io::Read;
    let mut f = File::open(path).ok()?;
    let mut buf = [0u8; 65536];
    let mut n = 0usize;
    loop {
        let read = f.read(&mut buf).ok()?;
        if read == 0 {
            return Some(n);
        }
        n += buf[..read].iter().filter(|&&b| b == b'\n').count();
    }
}
// count_newlines:end

// compact_room_pdumeta:start
//   purpose: Atomically rewrite <dir>/rooms/<sanitized>.pdumeta.jsonl (internal-task) so it
//            contains only the FIRST occurrence of each event_id that is also present
//            in `event_ids_kept` — dedup + drop meta for event_ids that no longer exist
//            in the room's timeline.  Mirrors compact_room's write-temp-then-rename
//            atomicity: a crash mid-compaction leaves the old meta journal intact.
//            No-op when the meta file does not exist yet (nothing to compact — e.g.
//            a pre-internal-task journal with no pdumeta sidecar) or when event_ids_kept is empty.
//            Malformed meta lines are dropped silently (best-effort; replay already
//            tolerates a missing/short meta map via its unsigned fallback).
//   input:  dir            — data directory root (<data_dir>);
//           room_id        — Matrix room identifier (derives the sidecar file name);
//           event_ids_kept — set of event_ids that should survive compaction (normally:
//                            every event_id currently in room_timeline for this room)
//   output: none (errors printed to stderr, not propagated)
//   sideEffects: may create/rename <dir>/rooms/<sanitized>.pdumeta.jsonl
pub fn compact_room_pdumeta(dir: &Path, room_id: &str, event_ids_kept: &HashSet<String>) {
    if event_ids_kept.is_empty() {
        return;
    }
    let filename = format!("{}.pdumeta.jsonl", sanitize_filename(room_id));
    let final_path = dir.join("rooms").join(&filename);
    if !final_path.exists() {
        return; // no meta sidecar yet (pre-internal-task journal, or nothing signed) — nothing to do.
    }
    let tmp_path = dir.join("rooms").join(format!("{filename}.compact.tmp"));

    let result = (|| -> Result<(), Box<dyn std::error::Error>> {
        let file = File::open(&final_path)?;
        let reader = BufReader::new(file);

        let mut seen_ids: HashSet<String> = HashSet::new();
        let mut lines: Vec<String> = Vec::new();
        let mut total_parsed = 0usize;
        for line_result in reader.lines() {
            let line = line_result?;
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            let rec: Value = match serde_json::from_str(trimmed) {
                Ok(v) => v,
                Err(_) => continue, // malformed meta line — drop on compaction (best-effort)
            };
            total_parsed += 1;
            let event_id = rec
                .get("event_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if event_id.is_empty()
                || !event_ids_kept.contains(&event_id)
                || !seen_ids.insert(event_id.clone())
            {
                continue;
            }
            let mut out_line = trimmed.to_string();
            out_line.push('\n');
            lines.push(out_line);
        }

        // SKIP-WHEN-CLEAN, same reasoning as compact_room: if every parsed line
        // survived filtering and dedup, the rewrite would reproduce the file's
        // content byte-for-byte (out_line is the original trimmed line) — so
        // the write is pure amplification. The pdumeta sidecar carries the
        // base64'd PDU content and is roughly the size of the journal itself,
        // so skipping matters here as much as there. Measured boot cost without
        // either skip: the entire store rewritten on every startup.
        if total_parsed == lines.len() {
            return Ok(());
        }

        let mut tmp = File::create(&tmp_path)?;
        for line in &lines {
            tmp.write_all(line.as_bytes())?;
        }
        tmp.flush()?;
        tmp.sync_data()?;
        drop(tmp);

        swap_journal_file(&tmp_path, &final_path)?;
        Ok(())
    })();

    if let Err(e) = result {
        eprintln!("matrix-hs compact_room_pdumeta: {room_id}: {e} (pdumeta journal untouched)");
        let _ = fs::remove_file(&tmp_path);
    }
}
// compact_room_pdumeta:end

// persist_room_gc:start
//   purpose: Record a room's GC watermark at <dir>/rooms/<sanitized>.gc.
//            Without this, collection is undone by the next restart: replay re-adds
//            every event still in the journal and the in-memory watermark is back to
//            zero, so a peer that never collected refills the room on its next
//            catch-up. The same shape of bug that made redactions vanish on restart.
//   input:  dir — data dir; room_id; depth — the watermark
//   output: none (best-effort; errors logged)
//   sideEffects: writes <dir>/rooms/<sanitized>.gc
pub fn persist_room_gc(dir: &Path, room_id: &str, depth: u64) {
    let path = dir
        .join("rooms")
        .join(format!("{}.gc", sanitize_filename(room_id)));
    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    if let Err(e) = fs::write(&path, depth.to_string()) {
        eprintln!("matrix-hs persist_room_gc {room_id}: {e}");
    }
}
// persist_room_gc:end

// read_room_gc:start
//   purpose: Read back a room's GC watermark. Absent, empty or unparseable all mean
//            "nothing collected" — a missing marker must never be read as a deep cut,
//            which would silently erase a room on the next replay.
//   input:  dir — data dir; room_id
//   output: the watermark, or 0
//   sideEffects: none
pub fn read_room_gc(dir: &Path, room_id: &str) -> u64 {
    let path = dir
        .join("rooms")
        .join(format!("{}.gc", sanitize_filename(room_id)));
    fs::read_to_string(&path)
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
        .unwrap_or(0)
}
// read_room_gc:end

// prune_room_journal:start
//   purpose: Drop collected events from a room's on-disk journal, keeping only
//            `event_ids_kept`. Memory is bounded by the watermark alone; this is what
//            bounds DISK, which is the other half of what garbage collection is for.
//            Mirrors compact_room_pdumeta: rewrite to a temp file, fsync, rename, and
//            on any error leave the original untouched.
//   input:  dir; room_id; event_ids_kept — ids surviving collection
//   output: none (best-effort; errors logged)
//   sideEffects: rewrites <dir>/rooms/<sanitized>.jsonl
pub fn prune_room_journal(dir: &Path, room_id: &str, event_ids_kept: &HashSet<String>) {
    let filename = format!("{}.jsonl", sanitize_filename(room_id));
    let final_path = dir.join("rooms").join(&filename);
    if !final_path.exists() {
        return;
    }
    let tmp_path = dir.join("rooms").join(format!("{filename}.prune.tmp"));

    let result = (|| -> Result<(), Box<dyn std::error::Error>> {
        let reader = BufReader::new(File::open(&final_path)?);
        let mut seen: HashSet<String> = HashSet::new();
        let mut lines: Vec<String> = Vec::new();
        for line_result in reader.lines() {
            let line = line_result?;
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            let rec: Value = match serde_json::from_str(trimmed) {
                Ok(v) => v,
                Err(_) => continue,
            };
            let event_id = rec
                .get("event_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if event_id.is_empty()
                || !event_ids_kept.contains(&event_id)
                || !seen.insert(event_id)
            {
                continue;
            }
            let mut out = trimmed.to_string();
            out.push('\n');
            lines.push(out);
        }

        let mut tmp = File::create(&tmp_path)?;
        for line in &lines {
            tmp.write_all(line.as_bytes())?;
        }
        tmp.flush()?;
        tmp.sync_data()?;
        drop(tmp);
        fs::rename(&tmp_path, &final_path)?;
        Ok(())
    })();

    if let Err(e) = result {
        eprintln!("matrix-hs prune_room_journal: {room_id}: {e} (journal untouched)");
        let _ = fs::remove_file(&tmp_path);
    }
}
// prune_room_journal:end

// read_room_journal_events:start
//   purpose: Read every event line from a room journal file directly off disk, in
//            file order. Deliberately independent of AppState / room_timeline: the
//            in-memory room_timeline may be trimmed by AppState::append_room_timeline's
//            retention cap (timeline_max_events), but that cap is a bound on the live
//            /sync read projection only — the on-disk journal remains the full durable
//            history until real CRDT tombstones exist (ROADMAP Phase 1 layer b).
//            compact_all uses this so compaction only DEDUPES re-appended lines and
//            never enforces the memory cap on disk.
//   input:  path — journal file path
//   output: Vec<Value> — every parseable client-event JSON line, in file order
//   sideEffects: none (errors logged to stderr)
fn read_room_journal_events(path: &Path) -> Vec<Value> {
    let file = match File::open(path) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("matrix-hs compact_all: open {}: {e}", path.display());
            return Vec::new();
        }
    };
    let reader = BufReader::new(file);
    let mut events = Vec::new();
    for (line_no, line_result) in reader.lines().enumerate() {
        let line = match line_result {
            Ok(l) => l,
            Err(e) => {
                eprintln!(
                    "matrix-hs compact_all: read line {} of {}: {e}",
                    line_no + 1,
                    path.display()
                );
                continue;
            }
        };
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        match serde_json::from_str(line) {
            Ok(v) => events.push(v),
            Err(e) => eprintln!(
                "matrix-hs compact_all: parse line {} of {}: {e}",
                line_no + 1,
                path.display()
            ),
        }
    }
    events
}
// read_room_journal_events:end

// compact_all:start
//   purpose: Compact every room journal in the data directory. Called once after
//            startup replay so that duplicate re-appends from previous un-compacted
//            runs are eliminated on-disk before any new events are written.
//            Reads each <dir>/rooms/*.jsonl file straight off disk (NOT from
//            AppState/room_timeline — see read_room_journal_events) so compaction
//            never drops events that fell outside the in-memory retention cap; it
//            only removes byte-identical duplicate lines for the same event_id.
//            No-op when the rooms directory does not exist yet (nothing persisted).
//   input:  dir — data directory root
//   output: none (errors printed to stderr per room)
//   sideEffects: may rewrite room journals (+ pdumeta sidecars, internal-task) on disk
pub fn compact_all(dir: &Path) {
    let rooms_dir = dir.join("rooms");
    let entries = match fs::read_dir(&rooms_dir) {
        Ok(e) => e,
        Err(_) => return, // nothing persisted yet
    };

    for entry_result in entries {
        let entry = match entry_result {
            Ok(e) => e,
            Err(e) => {
                eprintln!("matrix-hs compact_all: read_dir entry error: {e}");
                continue;
            }
        };
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("jsonl") {
            continue;
        }
        // <sanitized>.pdumeta.jsonl also ends in ".jsonl" — it is the sidecar
        // compacted below via compact_room_pdumeta, never a room journal itself.
        let is_pdumeta = path
            .file_name()
            .and_then(|n| n.to_str())
            .map(|n| n.ends_with(".pdumeta.jsonl"))
            .unwrap_or(false);
        if is_pdumeta {
            continue;
        }

        let events = read_room_journal_events(&path);
        let room_id = match events
            .first()
            .and_then(|ev| ev.get("room_id"))
            .and_then(|v| v.as_str())
        {
            Some(id) => id.to_string(),
            None => continue, // empty or malformed journal — nothing to compact
        };

        compact_room(dir, &room_id, &events);
        // internal-task: keep the pdumeta sidecar in sync — dedup, keep only ids still present
        // in the (full, uncapped) journal just read.
        let ids_kept: HashSet<String> = events
            .iter()
            .filter_map(|ev| {
                ev.get("event_id")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            })
            .collect();
        compact_room_pdumeta(dir, &room_id, &ids_kept);
    }
}
// compact_all:end

// ── AppState persist helpers ───────────────────────────────────────────────────

impl AppState {
    // AppState::persist_room_event:start
    //   purpose: Append a room event (message or state) to the per-room journal.
    //            The record is the full client-event JSON (state events carry state_key).
    //            No-op when persistence is disabled.
    //   input:  room_id — Matrix room identifier; ev — client event JSON value
    //   output: none
    //   sideEffects: appends one JSON line to rooms/<sanitized_room_id>.jsonl
    pub fn persist_room_event(&self, room_id: &str, ev: &Value) {
        let ctx = &self.persist;
        let dir = match &ctx.data_dir {
            Some(d) => d,
            None => return,
        };
        let filename = format!("{}.jsonl", sanitize_filename(room_id));
        let path = dir.join("rooms").join(filename);
        append_line(&path, ev);
    }
    // AppState::persist_room_event:end

    // AppState::persist_room_pdu_meta:start
    //   purpose: Append the signed-PDU metadata (sig, signer_node, prev_events, depth) for
    //            one event to a per-room meta journal, SEPARATE from the client-event
    //            journal written by persist_room_event (internal-task).  This is what lets replay
    //            reconstruct a VERIFIABLE Pdu after restart — the client-event JSON alone
    //            cannot carry these fields because it feeds room_timeline/`/sync` directly
    //            and is rewritten from the clean timeline on compaction, so any extra keys
    //            here would either leak to clients or be silently dropped.
    //            Format: {"event_id","sig":<base64url-nopad>,"signer_node",
    //                     "prev_events":[...],"depth":<u64>,"content":<base64url-nopad>}.
    //            `content` is the Pdu's RAW content bytes (exactly what was fed into
    //            canonical_bytes for signing) — NOT re-derived from the client-event JSON
    //            on replay. serde_json::Value uses an unordered/BTreeMap-backed Map (no
    //            preserve_order feature in this workspace), so parsing the client-event's
    //            "content" and re-serializing it does NOT reproduce the original byte
    //            sequence when the source has non-alphabetical key order — a near-certainty
    //            for real client bodies (Element/nio send `{"msgtype":...,"body":...}`, not
    //            alphabetical). Storing the raw bytes here is what makes replay byte-exact,
    //            so verify_sig's canonical_bytes hash matches post-restart (internal-task RETURN).
    //            No-op when persistence is disabled (data_dir is None).
    //   input:  room_id — Matrix room identifier; event_id — the Pdu's event_id;
    //           sig — ed25519 signature bytes (may be empty if the Pdu is unsigned);
    //           signer_node — signing node_id (may be empty if unsigned);
    //           prev_events — Pdu.prev_events at insertion time; depth — Pdu.depth;
    //           content — Pdu.content raw bytes at insertion time (exactly as signed)
    //   output: none
    //   sideEffects: appends one JSON line to rooms/<sanitized_room_id>.pdumeta.jsonl
    #[allow(clippy::too_many_arguments)]
    pub fn persist_room_pdu_meta(
        &self,
        room_id: &str,
        event_id: &str,
        sig: &[u8],
        signer_node: &str,
        prev_events: &[String],
        depth: u64,
        content: &[u8],
    ) {
        let ctx = &self.persist;
        let dir = match &ctx.data_dir {
            Some(d) => d,
            None => return,
        };
        let filename = format!("{}.pdumeta.jsonl", sanitize_filename(room_id));
        let path = dir.join("rooms").join(filename);
        let rec = json!({
            "event_id":    event_id,
            "sig":         URL_SAFE_NO_PAD.encode(sig),
            "signer_node": signer_node,
            "prev_events": prev_events,
            "depth":       depth,
            "content":     URL_SAFE_NO_PAD.encode(content),
        });
        append_line(&path, &rec);
    }
    // AppState::persist_room_pdu_meta:end

    // AppState::persist_user:start
    //   purpose: Append a user registration record to accounts.jsonl.
    //            Format: {"localpart":"<u>","password_hash":"<h>","device_id":"<d>"}.
    //            The stored value is the Argon2id PHC hash, never plaintext.
    //            No-op when persistence is disabled.
    //   input:  localpart, password_hash (Argon2id PHC string), device_id
    //   output: none
    //   sideEffects: appends one JSON line to accounts.jsonl
    pub fn persist_user(&self, localpart: &str, password_hash: &str, device_id: &str) {
        let ctx = &self.persist;
        let dir = match &ctx.data_dir {
            Some(d) => d,
            None => return,
        };
        let path = dir.join("accounts.jsonl");
        let rec = json!({
            "localpart":     localpart,
            "password_hash": password_hash,
            "device_id":     device_id,
        });
        append_line(&path, &rec);
    }
    // AppState::persist_user:end

    // AppState::persist_alias:start
    //   purpose: Append an alias → room_id mapping to aliases.jsonl.
    //            Format: {"alias":"#name:srv","room_id":"!id:srv"}.
    //            No-op when persistence is disabled.
    //   input:  alias — full Matrix alias string; room_id — room identifier
    //   output: none
    //   sideEffects: appends one JSON line to aliases.jsonl
    pub fn persist_alias(&self, alias: &str, room_id: &str) {
        let ctx = &self.persist;
        let dir = match &ctx.data_dir {
            Some(d) => d,
            None => return,
        };
        let path = dir.join("aliases.jsonl");
        let rec = json!({ "alias": alias, "room_id": room_id });
        append_line(&path, &rec);
    }
    // AppState::persist_alias:end

    // AppState::persist_media_blob:start
    //   purpose: Write one uploaded media blob to <data_dir>/media/<sanitized_media_id>
    //            plus a sibling "<sanitized_media_id>.ct" sidecar carrying the raw
    //            Content-Type string, so replay_media can restore both on restart.
    //            Best-effort like every other persist_* helper here: any fs error is
    //            printed to stderr and the call returns — never panics, never fails the
    //            upload response over a disk hiccup. No-op when persistence is disabled.
    //   input:  media_id — the id the blob was stored under; content_type — Content-Type
    //           string as received/defaulted at upload; bytes — raw file contents
    //   output: none (errors logged to stderr)
    //   sideEffects: creates <data_dir>/media/ (if absent) and writes two files
    pub fn persist_media_blob(&self, media_id: &str, content_type: &str, bytes: &[u8]) {
        let ctx = &self.persist;
        let dir = match &ctx.data_dir {
            Some(d) => d,
            None => return,
        };
        let media_dir = dir.join("media");
        let result = (|| -> Result<(), Box<dyn std::error::Error>> {
            fs::create_dir_all(&media_dir)?;
            let filename = sanitize_filename(media_id);
            let blob_path = media_dir.join(&filename);
            fs::write(&blob_path, bytes)?;
            let ct_path = media_dir.join(format!("{filename}.ct"));
            fs::write(&ct_path, content_type.as_bytes())?;
            Ok(())
        })();
        if let Err(e) = result {
            eprintln!("matrix-hs persist: persist_media_blob {media_id}: {e}");
        }
    }
    // AppState::persist_media_blob:end

    // AppState::persist_room_key_version_create:start
    //   purpose: Append a "create" record to room_key_versions.jsonl for one new
    //            E2EE key-backup version. No-op when persistence is disabled.
    //   input:  user_id, version, algorithm, auth_data — exactly what was stored
    //           in room_key_versions by create_room_key_version
    //   output: none
    //   sideEffects: appends one JSON line to room_key_versions.jsonl
    pub fn persist_room_key_version_create(
        &self,
        user_id: &str,
        version: &str,
        algorithm: &Value,
        auth_data: &Value,
    ) {
        let Some(dir) = &self.persist.data_dir else {
            return;
        };
        let path = dir.join("room_key_versions.jsonl");
        let rec = json!({
            "op": "create", "user_id": user_id, "version": version,
            "algorithm": algorithm, "auth_data": auth_data,
        });
        append_line(&path, &rec);
    }
    // AppState::persist_room_key_version_create:end

    // AppState::persist_room_key_version_update:start
    //   purpose: Append an "update" record to room_key_versions.jsonl. Only the
    //            fields actually changed (Some) are included in the record so
    //            replay can distinguish "left unchanged" from "explicitly cleared" —
    //            mirrors update_room_key_version's own Option semantics.
    //            No-op when persistence is disabled.
    //   input:  user_id, version; algorithm, auth_data — same Option<Value>
    //           semantics as update_room_key_version's parameters
    //   output: none
    //   sideEffects: appends one JSON line to room_key_versions.jsonl
    pub fn persist_room_key_version_update(
        &self,
        user_id: &str,
        version: &str,
        algorithm: Option<&Value>,
        auth_data: Option<&Value>,
    ) {
        let Some(dir) = &self.persist.data_dir else {
            return;
        };
        let path = dir.join("room_key_versions.jsonl");
        let mut rec = serde_json::Map::new();
        rec.insert("op".to_string(), json!("update"));
        rec.insert("user_id".to_string(), json!(user_id));
        rec.insert("version".to_string(), json!(version));
        if let Some(a) = algorithm {
            rec.insert("algorithm".to_string(), a.clone());
        }
        if let Some(ad) = auth_data {
            rec.insert("auth_data".to_string(), ad.clone());
        }
        append_line(&path, &Value::Object(rec));
    }
    // AppState::persist_room_key_version_update:end

    // AppState::persist_room_key_version_delete:start
    //   purpose: Append a "delete" record to room_key_versions.jsonl.
    //            No-op when persistence is disabled.
    //   input:  user_id, version
    //   output: none
    //   sideEffects: appends one JSON line to room_key_versions.jsonl
    pub fn persist_room_key_version_delete(&self, user_id: &str, version: &str) {
        let Some(dir) = &self.persist.data_dir else {
            return;
        };
        let path = dir.join("room_key_versions.jsonl");
        let rec = json!({ "op": "delete", "user_id": user_id, "version": version });
        append_line(&path, &rec);
    }
    // AppState::persist_room_key_version_delete:end

    // AppState::persist_room_key_put:start
    //   purpose: Append a "put" record to room_key_data.jsonl for one stored
    //            session's KeyBackupData. No-op when persistence is disabled.
    //   input:  user_id, version, room_id, session_id, data — exactly what was
    //           passed to put_room_key_session
    //   output: none
    //   sideEffects: appends one JSON line to room_key_data.jsonl
    pub fn persist_room_key_put(
        &self,
        user_id: &str,
        version: &str,
        room_id: &str,
        session_id: &str,
        data: &Value,
    ) {
        let Some(dir) = &self.persist.data_dir else {
            return;
        };
        let path = dir.join("room_key_data.jsonl");
        let rec = json!({
            "op": "put", "user_id": user_id, "version": version,
            "room_id": room_id, "session_id": session_id, "data": data,
        });
        append_line(&path, &rec);
    }
    // AppState::persist_room_key_put:end

    // AppState::persist_room_key_delete:start
    //   purpose: Append a delete record to room_key_data.jsonl, scoped exactly like
    //            delete_room_key_data (room_id/session_id optional — None means
    //            "all"). No-op when persistence is disabled.
    //   input:  user_id, version; room_id, session_id — same scoping as
    //           delete_room_key_data
    //   output: none
    //   sideEffects: appends one JSON line to room_key_data.jsonl
    pub fn persist_room_key_delete(
        &self,
        user_id: &str,
        version: &str,
        room_id: Option<&str>,
        session_id: Option<&str>,
    ) {
        let Some(dir) = &self.persist.data_dir else {
            return;
        };
        let path = dir.join("room_key_data.jsonl");
        let rec = match (room_id, session_id) {
            (Some(rid), Some(sid)) => json!({
                "op": "delete_session", "user_id": user_id, "version": version,
                "room_id": rid, "session_id": sid,
            }),
            (Some(rid), None) => json!({
                "op": "delete_room", "user_id": user_id, "version": version, "room_id": rid,
            }),
            (None, _) => json!({
                "op": "delete_all", "user_id": user_id, "version": version,
            }),
        };
        append_line(&path, &rec);
    }
    // AppState::persist_room_key_delete:end
}

// ── Startup replay ─────────────────────────────────────────────────────────────

// replay_from_dir:start
//   purpose: Replay all on-disk journals into a fresh AppState to restore durable state.
//            Called by build_state() in main.rs when MATRIX_HS_DATA_DIR is set.
//            Also used directly in tests to verify round-trip persistence.
//
//            Algorithm:
//              1. Scan rooms/<file>.jsonl for each file, SKIPPING <file>.pdumeta.jsonl
//                 sidecars (internal-task — they are loaded per-room by load_pdu_meta() from inside
//                 replay_room_journal, not iterated as their own room journal here; their
//                 lines have no "room_id"/"event_id" in the client-event shape).  Each
//                 remaining line is a client-event JSON.
//                 Events without "state_key" go into RoomLog (as Pdus) + room_timeline.
//                 Events with "state_key" go into room_state + room_timeline.
//                 Dedup: RoomLog.add() is idempotent by event_id; for room_state we replace
//                 by (event_type, state_key) last-wins; for room_timeline we skip event_ids
//                 already present (HashSet dedup).
//              2. Read accounts.jsonl — insert into users; last-write-wins per localpart.
//              3. Read aliases.jsonl  — insert into aliases; last-write-wins per alias.
//              4. Set stream_pos to the total number of timeline entries restored.
//
//            Resilience: malformed JSON lines are skipped with a stderr warning; missing
//            files are treated as empty (not an error).
//
//   input:  state — Arc<AppState> to populate (should be freshly constructed);
//           data_dir — path to the data directory
//   output: Result<(), String> — Ok or error message describing a fatal setup failure
//   sideEffects: populates state.rooms, room_state, room_timeline, users, aliases;
//                updates stream_pos
pub fn replay_from_dir(state: &std::sync::Arc<AppState>, data_dir: &Path) -> Result<(), String> {
    let rooms_dir = data_dir.join("rooms");

    // ── 1. Room journals ─────────────────────────────────────────────────────
    if rooms_dir.exists() {
        let entries = fs::read_dir(&rooms_dir)
            .map_err(|e| format!("replay: read_dir {}: {e}", rooms_dir.display()))?;

        for entry_result in entries {
            let entry = match entry_result {
                Ok(e) => e,
                Err(e) => {
                    eprintln!("matrix-hs replay: read_dir entry error: {e}");
                    continue;
                }
            };
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("jsonl") {
                continue;
            }
            // internal-task: <sanitized>.pdumeta.jsonl also ends in ".jsonl" (extension() only sees
            // the LAST component) — it is a sidecar loaded by load_pdu_meta() from inside
            // replay_room_journal for its matching <sanitized>.jsonl file, never a room
            // journal in its own right.  Skip it here or its meta lines (no "room_id"/
            // "event_id" in the client-event shape) would be mis-parsed as malformed events.
            let is_pdumeta = path
                .file_name()
                .and_then(|n| n.to_str())
                .map(|n| n.ends_with(".pdumeta.jsonl"))
                .unwrap_or(false);
            if is_pdumeta {
                continue;
            }
            replay_room_journal(state, &path);
        }
    }

    // ── 2. Accounts journal ──────────────────────────────────────────────────
    let accounts_path = data_dir.join("accounts.jsonl");
    if accounts_path.exists() {
        replay_accounts(state, &accounts_path);
    }

    // ── 3. Aliases journal ───────────────────────────────────────────────────
    let aliases_path = data_dir.join("aliases.jsonl");
    if aliases_path.exists() {
        replay_aliases(state, &aliases_path);
    }

    // ── 4. Media blobs ───────────────────────────────────────────────────────
    let media_dir = data_dir.join("media");
    if media_dir.exists() {
        replay_media(state, &media_dir);
    }

    // ── 5. E2EE key-backup journals ──────────────────────────────────────────
    // Versions MUST replay before data: delete_room_key_data (§ replay_room_key_data)
    // consults room_key_versions to decide whether a "put" targets a version that
    // still exists in the FINAL post-replay state (a put for a since-deleted
    // version is simply dropped, mirroring how a live delete_room_key_version
    // eagerly clears that version's room_key_data).
    let versions_path = data_dir.join("room_key_versions.jsonl");
    if versions_path.exists() {
        replay_room_key_versions(state, &versions_path);
    }
    let data_path = data_dir.join("room_key_data.jsonl");
    if data_path.exists() {
        replay_room_key_data(state, &data_path);
    }

    Ok(())
}
// replay_from_dir:end

// replay_room_key_versions:start
//   purpose: Replay room_key_versions.jsonl (create/update/delete ops, in file
//            order) to reconstruct state.e2ee.room_key_versions, room_key_backup_seq,
//            and room_key_current_version exactly as they were at the moment the
//            journal was last written. This is an EVENT LOG replay (not a
//            snapshot): applying the same ops in the same order reproduces the
//            same final maps, including room_key_backup_seq (derived as the max
//            numeric version seen per user across all "create" ops) so a restart
//            never reuses a version number.
//   input:  state — Arc<AppState> to populate; path — room_key_versions.jsonl path
//   output: none (errors logged to stderr per line)
//   sideEffects: mutates state.e2ee.room_key_versions, room_key_backup_seq,
//                room_key_current_version
fn replay_room_key_versions(state: &std::sync::Arc<AppState>, path: &Path) {
    let file = match File::open(path) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("matrix-hs replay: open {}: {e}", path.display());
            return;
        }
    };
    let reader = BufReader::new(file);
    for (line_no, line_result) in reader.lines().enumerate() {
        let line = match line_result {
            Ok(l) => l,
            Err(e) => {
                eprintln!(
                    "matrix-hs replay: room_key_versions line {}: {e}",
                    line_no + 1
                );
                continue;
            }
        };
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let rec: Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(e) => {
                eprintln!(
                    "matrix-hs replay: room_key_versions parse line {}: {e}",
                    line_no + 1
                );
                continue;
            }
        };
        let op = rec.get("op").and_then(|v| v.as_str()).unwrap_or("");
        let user_id = match rec.get("user_id").and_then(|v| v.as_str()) {
            Some(u) => u.to_string(),
            None => continue,
        };
        let version = match rec.get("version").and_then(|v| v.as_str()) {
            Some(v) => v.to_string(),
            None => continue,
        };

        match op {
            "create" => {
                let algorithm = rec.get("algorithm").cloned().unwrap_or(Value::Null);
                let auth_data = rec.get("auth_data").cloned().unwrap_or(Value::Null);
                if let Ok(mut versions) = state.e2ee.room_key_versions.lock() {
                    versions.insert(
                        (user_id.clone(), version.clone()),
                        crate::state::RoomKeyBackupVersion {
                            algorithm,
                            auth_data,
                            etag: 0,
                        },
                    );
                }
                if let Ok(mut cur) = state.e2ee.room_key_current_version.lock() {
                    cur.insert(user_id.clone(), version.clone());
                }
                if let Ok(n) = version.parse::<u64>() {
                    if let Ok(mut seq) = state.e2ee.room_key_backup_seq.lock() {
                        let slot = seq.entry(user_id.clone()).or_insert(0);
                        if n > *slot {
                            *slot = n;
                        }
                    }
                }
            }
            "update" => {
                if let Ok(mut versions) = state.e2ee.room_key_versions.lock() {
                    if let Some(meta) = versions.get_mut(&(user_id.clone(), version.clone())) {
                        if let Some(a) = rec.get("algorithm") {
                            meta.algorithm = a.clone();
                        }
                        if let Some(ad) = rec.get("auth_data") {
                            meta.auth_data = ad.clone();
                        }
                    }
                }
            }
            "delete" => {
                let key = (user_id.clone(), version.clone());
                if let Ok(mut versions) = state.e2ee.room_key_versions.lock() {
                    versions.remove(&key);
                }
                if let Ok(mut data) = state.e2ee.room_key_data.lock() {
                    data.remove(&key);
                }
                if let Ok(mut cur) = state.e2ee.room_key_current_version.lock() {
                    if cur.get(&user_id).map(|v| v.as_str()) == Some(version.as_str()) {
                        cur.remove(&user_id);
                    }
                }
            }
            other => {
                eprintln!(
                    "matrix-hs replay: room_key_versions line {} unknown op {other:?} — skipped",
                    line_no + 1
                );
            }
        }
    }
}
// replay_room_key_versions:end

// replay_room_key_data:start
//   purpose: Replay room_key_data.jsonl (put/delete_session/delete_room/delete_all
//            ops, in file order) to reconstruct state.e2ee.room_key_data and each
//            touched version's etag counter. A "put" for a (user_id, version) that
//            no longer exists in state.e2ee.room_key_versions (because a LATER "delete"
//            op in room_key_versions.jsonl removed it) is dropped — this mirrors
//            the live behaviour where delete_room_key_version eagerly clears that
//            version's data, so an orphaned put from before the delete must not
//            resurrect it. MUST be called after replay_room_key_versions.
//   input:  state — Arc<AppState> (room_key_versions already replayed);
//           path — room_key_data.jsonl path
//   output: none (errors logged to stderr per line)
//   sideEffects: mutates state.e2ee.room_key_data; bumps room_key_versions[..].etag
fn replay_room_key_data(state: &std::sync::Arc<AppState>, path: &Path) {
    let file = match File::open(path) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("matrix-hs replay: open {}: {e}", path.display());
            return;
        }
    };
    let reader = BufReader::new(file);
    for (line_no, line_result) in reader.lines().enumerate() {
        let line = match line_result {
            Ok(l) => l,
            Err(e) => {
                eprintln!("matrix-hs replay: room_key_data line {}: {e}", line_no + 1);
                continue;
            }
        };
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let rec: Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(e) => {
                eprintln!(
                    "matrix-hs replay: room_key_data parse line {}: {e}",
                    line_no + 1
                );
                continue;
            }
        };
        let op = rec.get("op").and_then(|v| v.as_str()).unwrap_or("");
        let user_id = match rec.get("user_id").and_then(|v| v.as_str()) {
            Some(u) => u.to_string(),
            None => continue,
        };
        let version = match rec.get("version").and_then(|v| v.as_str()) {
            Some(v) => v.to_string(),
            None => continue,
        };
        let key = (user_id.clone(), version.clone());

        // Skip ops targeting a version that no longer exists in the final state
        // (it was deleted by a later room_key_versions.jsonl "delete" op).
        let version_exists = state
            .e2ee
            .room_key_versions
            .lock()
            .map(|v| v.contains_key(&key))
            .unwrap_or(false);
        if !version_exists {
            continue;
        }

        match op {
            "put" => {
                let room_id = match rec.get("room_id").and_then(|v| v.as_str()) {
                    Some(r) => r.to_string(),
                    None => continue,
                };
                let session_id = match rec.get("session_id").and_then(|v| v.as_str()) {
                    Some(s) => s.to_string(),
                    None => continue,
                };
                let data = rec.get("data").cloned().unwrap_or(Value::Null);
                if let Ok(mut store) = state.e2ee.room_key_data.lock() {
                    store
                        .entry(key.clone())
                        .or_default()
                        .entry(room_id)
                        .or_default()
                        .insert(session_id, data);
                }
                bump_etag_replay(state, &key);
            }
            "delete_session" => {
                let room_id = rec
                    .get("room_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let session_id = rec
                    .get("session_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                if let Ok(mut store) = state.e2ee.room_key_data.lock() {
                    if let Some(rooms) = store.get_mut(&key) {
                        if let Some(sessions) = rooms.get_mut(&room_id) {
                            sessions.remove(&session_id);
                        }
                    }
                }
                bump_etag_replay(state, &key);
            }
            "delete_room" => {
                let room_id = rec
                    .get("room_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                if let Ok(mut store) = state.e2ee.room_key_data.lock() {
                    if let Some(rooms) = store.get_mut(&key) {
                        rooms.remove(&room_id);
                    }
                }
                bump_etag_replay(state, &key);
            }
            "delete_all" => {
                if let Ok(mut store) = state.e2ee.room_key_data.lock() {
                    store.remove(&key);
                }
                bump_etag_replay(state, &key);
            }
            other => {
                eprintln!(
                    "matrix-hs replay: room_key_data line {} unknown op {other:?} — skipped",
                    line_no + 1
                );
            }
        }
    }
}
// replay_room_key_data:end

// bump_etag_replay:start
//   purpose: Replay-time equivalent of AppState::bump_room_key_etag — increments
//            the etag for (user_id, version) if that version still exists.
//            Kept as a free function (not calling the AppState method) purely to
//            avoid an extra lock/unlock round-trip inside this module; behaviour
//            is identical to bump_room_key_etag.
//   input:  state — Arc<AppState>; key — (user_id, version)
//   output: none (mutex-poison errors are silently ignored — best-effort replay)
//   sideEffects: mutates state.e2ee.room_key_versions[key].etag
fn bump_etag_replay(state: &std::sync::Arc<AppState>, key: &(String, String)) {
    if let Ok(mut versions) = state.e2ee.room_key_versions.lock() {
        if let Some(meta) = versions.get_mut(key) {
            meta.etag += 1;
        }
    }
}
// bump_etag_replay:end

// replay_media:start
//   purpose: Restore the media INDEX from <data_dir>/media/ into state.media.media —
//            metadata only, NOT blob contents (gamma-33, 2026-08-25: this loop used
//            to fs::read every blob into RAM, so a restart on a node with 20 GiB of
//            media re-inflated the process by 20 GiB before serving anything; the
//            empty-bytes sentinel marks entries as disk-backed and get_media loads
//            them on demand). Each blob is a plain file <sanitized_media_id> with a
//            sibling "<sanitized_media_id>.ct" sidecar holding its Content-Type; the
//            sidecars are skipped when iterating since they are not blobs themselves.
//            NOTE: media_id is recovered from the SANITIZED filename, not reversed back
//            to the original id. This is safe in practice because media_id is always
//            generated by AppState::new_media_id() as base64url (URL_SAFE_NO_PAD) —
//            an alphabet with no characters in sanitize_filename's forbidden set — so
//            sanitization is a no-op round-trip for every id this server itself mints.
//            owner_node is set to state.server_name (this node) for every restored
//            entry — a restart never changes which node "owns" media it originally
//            stored.
//   input:  state — Arc<AppState> to populate; media_dir — <data_dir>/media path
//   output: none (errors logged to stderr per file)
//   sideEffects: populates state.media.media with disk-backed index entries
// replay_media:end
fn replay_media(state: &std::sync::Arc<AppState>, media_dir: &Path) {
    let entries = match fs::read_dir(media_dir) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("matrix-hs replay: read_dir {}: {e}", media_dir.display());
            return;
        }
    };

    for entry_result in entries {
        let entry = match entry_result {
            Ok(e) => e,
            Err(e) => {
                eprintln!("matrix-hs replay: media read_dir entry error: {e}");
                continue;
            }
        };
        let path = entry.path();
        let file_name = match path.file_name().and_then(|n| n.to_str()) {
            Some(n) => n.to_string(),
            None => continue,
        };
        if file_name.ends_with(".ct") {
            continue; // sidecar — loaded alongside its blob below, not its own entry.
        }

        let ct_path = media_dir.join(format!("{file_name}.ct"));
        let content_type =
            fs::read_to_string(&ct_path).unwrap_or_else(|_| "application/octet-stream".to_string());

        if let Ok(mut media) = state.media.media.lock() {
            media.insert(
                file_name,
                crate::state::MediaEntry {
                    content_type,
                    bytes: std::sync::Arc::new(Vec::new()), // disk-backed — see purpose
                    owner_node: state.server_name.clone(),
                },
            );
        }
    }
}

// PduMeta:start
//   purpose: In-memory representation of one signed-PDU metadata record loaded from a
//            room's <sanitized>.pdumeta.jsonl sidecar (internal-task).  Used by replay to
//            reconstruct a VERIFIABLE Pdu (sig/signer_node/prev_events/depth/content)
//            instead of the unsigned synthetic fallback that was the only option before
//            internal-task.  `content` is the RAW bytes as originally signed (internal-task RETURN) — NOT
//            re-derived by re-serializing the client-event JSON, which would silently
//            reorder keys (serde_json::Value's Map has no preserve_order here) and break
//            the signature's byte-exact match.
//   input:  constructed field-by-field by load_pdu_meta while parsing one JSON line
//   output: PduMeta value
//   sideEffects: none
struct PduMeta {
    sig: Vec<u8>,
    signer_node: String,
    prev_events: Vec<String>,
    depth: u64,
    content: Vec<u8>,
}
// PduMeta:end

// load_pdu_meta:start
//   purpose: Load a room's <sanitized>.pdumeta.jsonl (the sidecar of the room's
//            client-event .jsonl journal, internal-task) into a HashMap keyed by event_id, for use
//            during replay to reconstruct signed Pdus.  Returns an EMPTY map (not an
//            error) when the meta file does not exist — back-compat with journals written
//            before internal-task, which have no meta sidecar at all; callers fall back to the
//            unsigned synthetic Pdu for any event_id missing from the returned map.
//            base64: sig is stored URL_SAFE_NO_PAD-encoded (same engine as
//            matrix_events::Pdu::compute_id / node_auth); a bad-base64 line is treated as
//            "no sig" for that one event_id (logged, not fatal) rather than aborting replay.
//   input:  journal_path — the room's <sanitized>.jsonl path (NOT the meta path itself;
//           this function derives the sibling "<sanitized>.pdumeta.jsonl" path from it)
//   output: HashMap<String, PduMeta> — event_id → meta; empty if the sidecar is absent,
//           unreadable, or journal_path has no parent/stem (defensive, should not happen)
//   sideEffects: none (read-only; errors logged to stderr)
fn load_pdu_meta(journal_path: &Path) -> HashMap<String, PduMeta> {
    let mut out = HashMap::new();

    let stem = match journal_path.file_stem().and_then(|s| s.to_str()) {
        Some(s) => s,
        None => return out,
    };
    let dir = match journal_path.parent() {
        Some(d) => d,
        None => return out,
    };
    let meta_path = dir.join(format!("{stem}.pdumeta.jsonl"));
    if !meta_path.exists() {
        return out; // back-compat: pre-internal-task journal — caller falls back to unsigned synthesis.
    }

    let file = match File::open(&meta_path) {
        Ok(f) => f,
        Err(e) => {
            eprintln!(
                "matrix-hs replay: open pdumeta {}: {e}",
                meta_path.display()
            );
            return out;
        }
    };
    let reader = BufReader::new(file);
    for (line_no, line_result) in reader.lines().enumerate() {
        let line = match line_result {
            Ok(l) => l,
            Err(e) => {
                eprintln!(
                    "matrix-hs replay: read pdumeta line {} of {}: {e}",
                    line_no + 1,
                    meta_path.display()
                );
                continue;
            }
        };
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let rec: Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(e) => {
                eprintln!(
                    "matrix-hs replay: parse pdumeta line {} of {}: {e}",
                    line_no + 1,
                    meta_path.display()
                );
                continue;
            }
        };
        let event_id = match rec.get("event_id").and_then(|v| v.as_str()) {
            Some(id) => id.to_string(),
            None => continue,
        };
        let sig_b64 = rec.get("sig").and_then(|v| v.as_str()).unwrap_or("");
        let sig = match URL_SAFE_NO_PAD.decode(sig_b64) {
            Ok(b) => b,
            Err(e) => {
                eprintln!(
                    "matrix-hs replay: pdumeta line {} of {} ({event_id}): bad base64 sig: {e} — treating as unsigned",
                    line_no + 1, meta_path.display()
                );
                Vec::new()
            }
        };
        let signer_node = rec
            .get("signer_node")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let prev_events: Vec<String> = rec
            .get("prev_events")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|e| e.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let depth = rec.get("depth").and_then(|v| v.as_u64()).unwrap_or(0);
        // internal-task RETURN: raw content bytes, exactly as signed. Absent on journals written by
        // the pre-fix internal-task code (no "content" key) or bad base64 — falls back to an empty
        // Vec, which simply fails verify_sig same as any other content mismatch (not a panic).
        let content_b64 = rec.get("content").and_then(|v| v.as_str()).unwrap_or("");
        let content = URL_SAFE_NO_PAD.decode(content_b64).unwrap_or_default();

        out.insert(
            event_id,
            PduMeta {
                sig,
                signer_node,
                prev_events,
                depth,
                content,
            },
        );
    }
    out
}
// load_pdu_meta:end

// replay_room_journal:start
//   purpose: Read one room journal file and merge its events into AppState.
//            Events are client-event JSON objects as written by persist_room_event.
//            Message events (no state_key) → RoomLog, as a Pdu reconstructed from the
//            room's pdumeta.jsonl sidecar when available (internal-task: real sig/signer_node/
//            prev_events/depth, so a fresh peer's apply_delta_verified accepts it after
//            this node restarts), else the unsigned synthetic fallback (pre-internal-task
//            journals, or events that were never signed) — both go to room_timeline too.
//            State events (has state_key) → room_state + room_timeline.
//            Dedup by event_id: room_timeline tracks seen ids to avoid double-appending.
//   input:  state — Arc<AppState>; path — path to the .jsonl file
//   output: none (errors logged to stderr)
//   sideEffects: mutates state.rooms, room_state, room_timeline; increments stream_pos
fn replay_room_journal(state: &std::sync::Arc<AppState>, path: &Path) {
    let file = match File::open(path) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("matrix-hs replay: open {}: {e}", path.display());
            return;
        }
    };
    let reader = BufReader::new(file);
    // internal-task: load this room's signed-PDU metadata sidecar (event_id → sig/signer_node/
    // prev_events/depth), if present, so message Pdus below can be reconstructed as
    // VERIFIABLE instead of the unsigned synthetic fallback.
    let pdu_meta = load_pdu_meta(path);
    // Which room this journal belongs to — needed after the loop to re-apply the GC
    // watermark. A journal file holds exactly one room, so the last line's room_id is
    // the file's room_id.
    let mut journal_room: Option<String> = None;
    // Collect already-present event_ids from room_timeline so double-replay is idempotent.
    // We gather this once before scanning the journal lines, then extend it as we add.
    // Using a local HashSet avoids holding the Mutex across the BufReader loop.
    let mut seen_ids: std::collections::HashSet<String> = {
        match state.room_timeline.lock() {
            Ok(rt) => rt
                .values()
                .flat_map(|v| v.iter())
                .filter_map(|(_, ev)| {
                    ev.get("event_id")
                        .and_then(|id| id.as_str())
                        .map(|s| s.to_string())
                })
                .collect(),
            Err(_) => std::collections::HashSet::new(),
        }
    };

    for (line_no, line_result) in reader.lines().enumerate() {
        let line = match line_result {
            Ok(l) => l,
            Err(e) => {
                eprintln!(
                    "matrix-hs replay: read line {} of {}: {e}",
                    line_no + 1,
                    path.display()
                );
                continue;
            }
        };
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let ev: Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(e) => {
                eprintln!(
                    "matrix-hs replay: parse line {} of {}: {e}",
                    line_no + 1,
                    path.display()
                );
                continue;
            }
        };

        let event_id = match ev.get("event_id").and_then(|v| v.as_str()) {
            Some(id) => id.to_string(),
            None => {
                eprintln!(
                    "matrix-hs replay: line {} of {} missing event_id — skipped",
                    line_no + 1,
                    path.display()
                );
                continue;
            }
        };
        let room_id = match ev.get("room_id").and_then(|v| v.as_str()) {
            Some(id) => id.to_string(),
            None => {
                eprintln!(
                    "matrix-hs replay: line {} of {} missing room_id — skipped",
                    line_no + 1,
                    path.display()
                );
                continue;
            }
        };
        let event_type = ev
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let has_state_key = ev.get("state_key").is_some();

        // Ensure room structures exist.
        state.ensure_room_state(&room_id);

        if has_state_key {
            // State event → room_state (last-wins by type+key) + room_timeline.
            let state_key = ev
                .get("state_key")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let sender = ev
                .get("sender")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let content = ev.get("content").cloned().unwrap_or(Value::Null);
            let ts = ev
                .get("origin_server_ts")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);

            let new_ev = StateEvent {
                event_type: event_type.clone(),
                state_key: state_key.clone(),
                sender,
                content,
                event_id: event_id.clone(),
                room_id: room_id.clone(),
                origin_server_ts: ts,
            };

            if let Ok(mut rs) = state.room_state.lock() {
                let room_vec = rs.entry(room_id.clone()).or_default();
                // Last-wins: replace matching (event_type, state_key).
                room_vec.retain(|e| !(e.event_type == event_type && e.state_key == state_key));
                room_vec.push(new_ev);
            }
        } else {
            // Message event → RoomLog, as a Pdu.
            let sender = ev
                .get("sender")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let ts = ev
                .get("origin_server_ts")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let content = ev.get("content").cloned().unwrap_or(Value::Null);
            let content_bytes = serde_json::to_vec(&content).unwrap_or_default();

            // internal-task: if this event_id has a pdumeta.jsonl record, reconstruct the REAL
            // sig/signer_node/prev_events/depth — this is what makes the replayed Pdu
            // pass verify_sig / apply_delta_verified for a fresh peer catching up after
            // this node restarts.  Otherwise (pre-internal-task journal, or an event that was
            // never signed) fall back to the unsigned synthetic Pdu: prev_events=[],
            // depth=0.  The RoomLog CRDT still accepts and orders these events correctly
            // (events with no prev_events sit at depth 0 by ordered()); since we replay
            // in journal order and event_id is content-addressed, convergence on THIS
            // node is preserved either way — only cross-node verification differs.
            //
            // internal-task RETURN: the signed branch uses meta.content (the RAW bytes captured at
            // signing time), NOT content_bytes (re-serialized from the client-event JSON).
            // serde_json::Value's Map has no preserve_order here, so re-serializing silently
            // reorders keys alphabetically — a byte-for-byte mismatch against what
            // canonical_bytes hashed at signing time, which made verify_sig fail-closed on
            // every real (non-alphabetical) message body after a restart. content_bytes is
            // only used in the unsigned fallback below, where there is no signature to match.
            let pdu = match pdu_meta.get(&event_id) {
                Some(meta) => Pdu {
                    event_id: event_id.clone(),
                    room_id: room_id.clone(),
                    sender: sender.clone(),
                    kind: event_type.clone(),
                    content: meta.content.clone(),
                    prev_events: meta.prev_events.clone(),
                    depth: meta.depth,
                    ts,
                    sig: meta.sig.clone(),
                    signer_node: meta.signer_node.clone(),
                },
                None => Pdu {
                    event_id: event_id.clone(),
                    room_id: room_id.clone(),
                    sender: sender.clone(),
                    kind: event_type.clone(),
                    content: content_bytes,
                    prev_events: vec![],
                    depth: 0,
                    ts,
                    sig: Vec::new(),
                    signer_node: String::new(),
                },
            };

            if let Ok(mut rooms) = state.rooms.lock() {
                let log = rooms.entry(room_id.clone()).or_default();
                log.add(pdu);
            }
        }

        // Append to room_timeline (dedup by event_id).
        if !seen_ids.contains(&event_id) {
            seen_ids.insert(event_id.clone());
            state.append_room_timeline(&room_id, ev);
        }
        journal_room = Some(room_id);
    }

    // Re-apply the GC watermark. Replay has just re-added every event still on disk,
    // so without this a collected room comes back in full — and then a peer that never
    // collected sees a node offering the old events again, which is the resurrection
    // the watermark exists to prevent. The marker is what carries the decision across
    // a restart.
    if let (Some(room_id), Some(dir)) = (journal_room, path.parent().and_then(|p| p.parent())) {
        let watermark = read_room_gc(dir, &room_id);
        if watermark > 0 {
            if let Ok(mut rooms) = state.rooms.lock() {
                if let Some(log) = rooms.get_mut(&room_id) {
                    let dropped = log.collect_below(watermark);
                    if dropped > 0 {
                        eprintln!(
                            "matrix-hs replay: {room_id}: re-applied GC watermark \
                             {watermark}, dropped {dropped} replayed event(s)"
                        );
                    }
                }
            }
        }
    }
}
// replay_room_journal:end

// replay_accounts:start
//   purpose: Read accounts.jsonl and populate state.users.
//            Each line: {"localpart":"...","password":"...","device_id":"..."}.
//            Last-write-wins per localpart (last line for a localpart takes effect).
//   input:  state — Arc<AppState>; path — accounts.jsonl path
//   output: none (errors logged to stderr)
//   sideEffects: mutates state.users
fn replay_accounts(state: &std::sync::Arc<AppState>, path: &Path) {
    let file = match File::open(path) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("matrix-hs replay: open accounts {}: {e}", path.display());
            return;
        }
    };
    let reader = BufReader::new(file);
    for (line_no, line_result) in reader.lines().enumerate() {
        let line = match line_result {
            Ok(l) => l,
            Err(e) => {
                eprintln!("matrix-hs replay: accounts line {}: {e}", line_no + 1);
                continue;
            }
        };
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let rec: Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("matrix-hs replay: accounts parse line {}: {e}", line_no + 1);
                continue;
            }
        };
        let localpart = match rec.get("localpart").and_then(|v| v.as_str()) {
            Some(u) => u.to_string(),
            None => continue,
        };
        // Accept both "password_hash" (new format) and legacy "password" field.
        // If neither is present, fall back to an empty string (an invalid hash that
        // will never verify — user effectively has no password but can be re-registered).
        let password_hash = rec
            .get("password_hash")
            .or_else(|| rec.get("password"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let device_id = rec
            .get("device_id")
            .and_then(|v| v.as_str())
            .unwrap_or("DEVICE1")
            .to_string();

        if let Ok(mut users) = state.users.lock() {
            // Last-write-wins: overwrite any existing entry for this localpart.
            users.insert(
                localpart,
                UserRecord {
                    password_hash,
                    device_id,
                    provisional: false,
                    rename_required: false,
                    epoch: 0,
                },
            );
        }
    }
}
// replay_accounts:end

// replay_aliases:start
//   purpose: Read aliases.jsonl and populate state.aliases.
//            Each line: {"alias":"#name:srv","room_id":"!id:srv"}.
//            Last-write-wins per alias.
//   input:  state — Arc<AppState>; path — aliases.jsonl path
//   output: none (errors logged to stderr)
//   sideEffects: mutates state.aliases
fn replay_aliases(state: &std::sync::Arc<AppState>, path: &Path) {
    let file = match File::open(path) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("matrix-hs replay: open aliases {}: {e}", path.display());
            return;
        }
    };
    let reader = BufReader::new(file);
    for (line_no, line_result) in reader.lines().enumerate() {
        let line = match line_result {
            Ok(l) => l,
            Err(e) => {
                eprintln!("matrix-hs replay: aliases line {}: {e}", line_no + 1);
                continue;
            }
        };
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let rec: Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("matrix-hs replay: aliases parse line {}: {e}", line_no + 1);
                continue;
            }
        };
        let alias = match rec.get("alias").and_then(|v| v.as_str()) {
            Some(a) => a.to_string(),
            None => continue,
        };
        let room_id = match rec.get("room_id").and_then(|v| v.as_str()) {
            Some(r) => r.to_string(),
            None => continue,
        };
        if let Ok(mut aliases) = state.aliases.lock() {
            aliases.insert(alias, room_id);
        }
    }
}
// replay_aliases:end
