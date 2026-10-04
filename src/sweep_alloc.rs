// START_AI_HEADER
// MODULE: src/sweep_alloc.rs
// PURPOSE: Per-site allocation accounting for the cluster sweep path — the bytes that
//          `catchup_pass` (src/main.rs) and `merge_catchup_delta` allocate while walking
//          every known room, so the step amplitude of a sweep can be attributed to places
//          instead of guessed at. Window-D measurements showed the peak of a sweep growing
//          with the number of EVENTS in rooms while staying flat when twelve EMPTY rooms
//          were added, which pointed here: three `HashSet<String>` rebuilds per room per
//          sweep (`known_ids`, `before_ids`, `after_ids`) each clone every event_id.
//          Those three are gone (commit 1c78791); what the live window left unattributed is
//          now attributed too: the two sites the residual actually sits in —
//          `delta_from_bytes` (parsing every reply blob into owned PDUs) and
//          `apply_delta_verified` (re-hashing and re-cloning every PDU on the way in).
//          Each of those two sites also records an INPUT gauge, so a summary can print
//          bytes-in next to bytes-out: the residual was measured at 46-72x the reply
//          payload, which is an amplification claim, and an amplification claim needs a
//          denominator on the same line or it is just a big number.
//          This module only COUNTS; it changes no behaviour and allocates nothing itself.
//          Counters are plain relaxed atomics, so the cost on the hot path is a few adds.
// PUBLIC_API: Site, add, add_str_set, calibrate_input, input_bytes, snapshot, reset, totals, format_summary
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
    /// `merge_catchup_delta`: the small Vec of candidate PDU references kept across the
    /// merge (replaces the two full-room id sets; kept so the accounting stays complete).
    CandidateRefs,
    /// `delta_from_bytes`: owned heap built while parsing one reply blob — the `Vec<Pdu>`
    /// table plus every String/Vec field it moves out of the wire format. This and
    /// `ApplyDeltaVerified` are the two sites the post-1c78791 residual sits in.
    DeltaFromBytes,
    /// `apply_delta_verified`: per PDU — the canonical bytes hashed for `compute_id`, the
    /// base64 + `format!` id strings it returns, the `event_id` clone made for the entry
    /// API, and the full PDU clone when the event was not held yet.
    ApplyDeltaVerified,
}

impl Site {
    pub const ALL: [Site; 10] = [
        Site::ReplyKeyString,
        Site::PayloadBytes,
        Site::ConvergedRoomId,
        Site::KnownIds,
        Site::BeforeIds,
        Site::AfterIds,
        Site::JsonContentParse,
        Site::CandidateRefs,
        Site::DeltaFromBytes,
        Site::ApplyDeltaVerified,
    ];

    /// Sites that carry an INPUT gauge (bytes handed to the function, next to the bytes
    /// it allocated). Printed as `name=outB(in=inB)` so one token stays one site.
    pub const CALIBRATED: [Site; 2] = [Site::DeltaFromBytes, Site::ApplyDeltaVerified];

    pub fn as_str(self) -> &'static str {
        match self {
            Site::ReplyKeyString => "catchup.reply_key_string",
            Site::PayloadBytes => "catchup.payload_bytes",
            Site::ConvergedRoomId => "catchup.converged_room_id",
            Site::KnownIds => "merge.known_ids",
            Site::BeforeIds => "merge.before_ids",
            Site::AfterIds => "merge.after_ids",
            Site::JsonContentParse => "merge.json_content_parse",
            Site::CandidateRefs => "merge.candidate_refs",
            Site::DeltaFromBytes => "catchup.delta_from_bytes",
            Site::ApplyDeltaVerified => "merge.apply_delta_verified",
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
            Site::CandidateRefs => &CANDIDATE_REFS,
            Site::DeltaFromBytes => &DELTA_FROM_BYTES,
            Site::ApplyDeltaVerified => &APPLY_DELTA_VERIFIED,
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
static CANDIDATE_REFS: AtomicU64 = AtomicU64::new(0);
static DELTA_FROM_BYTES: AtomicU64 = AtomicU64::new(0);
static APPLY_DELTA_VERIFIED: AtomicU64 = AtomicU64::new(0);

/// INPUT gauges: bytes handed to the two calibrated sites since the last `reset`.
/// Kept out of `Site::ALL` on purpose — they are denominators, not allocations, and
/// adding them to `totals()` would double-count the very thing they calibrate.
static DELTA_FROM_BYTES_IN: AtomicU64 = AtomicU64::new(0);
static APPLY_DELTA_VERIFIED_IN: AtomicU64 = AtomicU64::new(0);

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

/// calibrate_input:start
///   purpose: Record the bytes handed to a calibrated site — the reply blob
///            `delta_from_bytes` parses, or the PDU bytes `apply_delta_verified` walks —
///            so the summary can print bytes-in next to the bytes it allocated. A
///            residual that reads "46x the payload" is an amplification claim; without
///            the denominator on the same line it is an unexplained number.
///   input:  site — must be one of `Site::CALIBRATED`; bytes — input bytes for this call
///   output: ()
///   sideEffects: increments one input gauge; ignored for uncalibrated sites
/// calibrate_input:end
#[inline]
pub fn calibrate_input(site: Site, bytes: u64) {
    match site {
        Site::DeltaFromBytes => DELTA_FROM_BYTES_IN.fetch_add(bytes, Ordering::Relaxed),
        Site::ApplyDeltaVerified => APPLY_DELTA_VERIFIED_IN.fetch_add(bytes, Ordering::Relaxed),
        _ => return,
    };
}

/// input_bytes:start
///   purpose: Input gauge of a calibrated site since the last `reset`; 0 for the others.
///   input:  site
///   output: u64
///   sideEffects: none
/// input_bytes:end
#[inline]
pub fn input_bytes(site: Site) -> u64 {
    match site {
        Site::DeltaFromBytes => DELTA_FROM_BYTES_IN.load(Ordering::Relaxed),
        Site::ApplyDeltaVerified => APPLY_DELTA_VERIFIED_IN.load(Ordering::Relaxed),
        _ => 0,
    }
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
    DELTA_FROM_BYTES_IN.store(0, Ordering::Relaxed);
    APPLY_DELTA_VERIFIED_IN.store(0, Ordering::Relaxed);
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
///            runs can be diffed line by line. A calibrated site prints as
///            `name=outB(in=inB)` — still one token and one site, with the input the
///            output can be divided by.
///   input:  label — what the cycle was (e.g. "sweep")
///   output: String, no trailing newline
///   sideEffects: none
/// format_summary:end
pub fn format_summary(label: &str) -> String {
    let snap = snapshot();
    let total: u64 = snap.iter().map(|(_, v)| *v).sum();
    let mut out = format!("[matrix-hs] sweep-alloc {label}: rooms={} replies={} total={}B", rooms(), replies(), total);
    for (site, bytes) in snap {
        if Site::CALIBRATED.contains(&site) {
            let _ = write!(
                out,
                " {}={}B(in={}B)",
                site.as_str(),
                bytes,
                input_bytes(site)
            );
        } else {
            let _ = write!(out, " {}={}B", site.as_str(), bytes);
        }
    }
    out
}
