// START_AI_HEADER
// MODULE: src/sweep_alloc.rs
// PURPOSE: Per-site allocation accounting for the cluster sweep path — the bytes that
//          `catchup_pass` (src/main.rs) and `merge_catchup_delta` allocate while walking
//          every known room, so the step amplitude of a sweep can be attributed to places
//          instead of guessed at. Window-D measurements showed the peak of a sweep growing
//          with the number of EVENTS in rooms while staying flat when twelve EMPTY rooms
//          were added, which pointed here: three `HashSet<String>` rebuilds per room per
//          sweep (`known_ids`, `before_ids`, `after_ids`) each clone every event_id.
//          This module only COUNTS; it changes no behaviour and allocates nothing itself.
//          Counters are plain relaxed atomics, so the cost on the hot path is a few adds.
// PUBLIC_API: Site, add, add_str_set, snapshot, reset, totals, format_summary
// END_AI_HEADER

use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};

/// Per-entry overhead of a `HashSet<String>` bucket: `String` header is 24 bytes on
/// 64-bit, hashbrown stores one control byte per bucket and load factor is 7/8, so a set
/// entry costs ~48 bytes of table on top of the string's own heap bytes. Used only for
/// estimation and stated as such in every summary this module prints.
pub const HASH_SET_ENTRY_OVERHEAD: u64 = 48;

/// One allocation site inside the sweep path.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Site {
    /// `catchup_pass`: `sample.key_expr().as_str().to_string()` per reply.
    ReplyKeyString,
    /// `catchup_pass`: `sample.payload().to_bytes()` per reply.
    PayloadBytes,
    /// `catchup_pass`: `converged.insert(room_id.to_string())` per room.
    ConvergedRoomId,
    /// `merge_catchup_delta`: the `known_ids` rebuild (one `event_id` clone per timeline entry).
    KnownIds,
    /// `merge_catchup_delta`: the `before_ids` rebuild (one `event_id` clone per log PDU).
    BeforeIds,
    /// `merge_catchup_delta`: the `after_ids` rebuild (one `event_id` clone per log PDU).
    AfterIds,
    /// `merge_catchup_delta`: `serde_json::from_slice(&pdu.content)` input size per new PDU.
    JsonContentParse,
}

impl Site {
    pub const ALL: [Site; 7] = [
        Site::ReplyKeyString,
        Site::PayloadBytes,
        Site::ConvergedRoomId,
        Site::KnownIds,
        Site::BeforeIds,
        Site::AfterIds,
        Site::JsonContentParse,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Site::ReplyKeyString => "catchup.reply_key_string",
            Site::PayloadBytes => "catchup.payload_bytes",
            Site::ConvergedRoomId => "catchup.converged_room_id",
            Site::KnownIds => "merge.known_ids",
            Site::BeforeIds => "merge.before_ids",
            Site::AfterIds => "merge.after_ids",
            Site::JsonContentParse => "merge.json_content_parse",
        }
    }

    fn counter(self) -> &'static AtomicU64 {
        match self {
            Site::ReplyKeyString => &REPLY_KEY,
            Site::PayloadBytes => &PAYLOAD,
            Site::ConvergedRoomId => &CONVERGED,
            Site::KnownIds => &KNOWN_IDS,
            Site::BeforeIds => &BEFORE_IDS,
            Site::AfterIds => &AFTER_IDS,
            Site::JsonContentParse => &JSON_PARSE,
        }
    }
}

static REPLY_KEY: AtomicU64 = AtomicU64::new(0);
static PAYLOAD: AtomicU64 = AtomicU64::new(0);
static CONVERGED: AtomicU64 = AtomicU64::new(0);
static KNOWN_IDS: AtomicU64 = AtomicU64::new(0);
static BEFORE_IDS: AtomicU64 = AtomicU64::new(0);
static AFTER_IDS: AtomicU64 = AtomicU64::new(0);
static JSON_PARSE: AtomicU64 = AtomicU64::new(0);

/// Rooms and replies seen since the last `reset`, so a summary can state per-cycle
/// denominators next to the byte totals.
static ROOMS: AtomicU64 = AtomicU64::new(0);
static REPLIES: AtomicU64 = AtomicU64::new(0);

/// add:start
///   purpose: Record `bytes` allocated at `site`. Relaxed ordering: these counters are
///            statistics for a report, never synchronisation.
///   input:  site — allocation site; bytes — estimated heap bytes
///   output: ()
///   sideEffects: increments one global counter
/// add:end
#[inline]
pub fn add(site: Site, bytes: u64) {
    site.counter().fetch_add(bytes, Ordering::Relaxed);
}

/// add_str_set:start
///   purpose: Record a `HashSet<String>` rebuild: `entries` cloned ids whose own bytes
///            are `string_bytes`, plus `HASH_SET_ENTRY_OVERHEAD` per entry for the table.
///   input:  site, entries, string_bytes
///   output: ()
///   sideEffects: increments one global counter
/// add_str_set:end
#[inline]
pub fn add_str_set(site: Site, entries: u64, string_bytes: u64) {
    add(
        site,
        string_bytes + entries.saturating_mul(HASH_SET_ENTRY_OVERHEAD),
    );
}

/// count_room:start
///   purpose: Note that one more room was merged in this cycle.
///   input:  ()
///   output: ()
///   sideEffects: increments ROOMS
/// count_room:end
#[inline]
pub fn count_room() {
    ROOMS.fetch_add(1, Ordering::Relaxed);
}

/// count_reply:start
///   purpose: Note one more cluster reply consumed in this cycle.
///   input:  ()
///   output: ()
///   sideEffects: increments REPLIES
/// count_reply:end
#[inline]
pub fn count_reply() {
    REPLIES.fetch_add(1, Ordering::Relaxed);
}

/// snapshot:start
///   purpose: Read all counters without resetting them.
///   input:  ()
///   output: Vec<(Site, u64)> in `Site::ALL` order
///   sideEffects: none
/// snapshot:end
pub fn snapshot() -> Vec<(Site, u64)> {
    Site::ALL
        .iter()
        .map(|s| (*s, s.counter().load(Ordering::Relaxed)))
        .collect()
}

/// reset:start
///   purpose: Zero every counter so the next measurement covers exactly one sweep cycle.
///   input:  ()
///   output: ()
///   sideEffects: clears all counters
/// reset:end
pub fn reset() {
    for s in Site::ALL {
        s.counter().store(0, Ordering::Relaxed);
    }
    ROOMS.store(0, Ordering::Relaxed);
    REPLIES.store(0, Ordering::Relaxed);
}

/// rooms:start
///   purpose: Rooms merged since the last `reset`.
///   input:  none
///   output: u64
///   sideEffects: none
/// rooms:end
pub fn rooms() -> u64 {
    ROOMS.load(Ordering::Relaxed)
}

/// replies:start
///   purpose: Replies consumed since the last `reset`.
///   input:  none
///   output: u64
///   sideEffects: none
/// replies:end
pub fn replies() -> u64 {
    REPLIES.load(Ordering::Relaxed)
}

/// totals:start
///   purpose: Sum of all site counters since the last `reset`.
///   input:  none
///   output: u64
///   sideEffects: none
/// totals:end
pub fn totals() -> u64 {
    snapshot().iter().map(|(_, v)| *v).sum()
}

/// format_summary:start
///   purpose: One line suitable for the node log: every site with its bytes, the total,
///            and the per-room / per-reply denominators. Deterministic site order so two
///            runs can be diffed line by line.
///   input:  label — what the cycle was (e.g. "sweep")
///   output: String, no trailing newline
///   sideEffects: none
/// format_summary:end
pub fn format_summary(label: &str) -> String {
    let snap = snapshot();
    let total: u64 = snap.iter().map(|(_, v)| *v).sum();
    let mut out = format!("[matrix-hs] sweep-alloc {label}: rooms={} replies={} total={}B", rooms(), replies(), total);
    for (site, bytes) in snap {
        let _ = write!(out, " {}={}B", site.as_str(), bytes);
    }
    out
}
