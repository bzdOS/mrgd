// START_AI_HEADER
// MODULE: matrix-hs/src/memprobe.rs
// PURPOSE: On-demand allocator statistics for a RUNNING server, sampled by signal.
//
// WHY THIS EXISTS, and why it is a signal and not a log line. The question it
// answers cannot be answered from outside the process: RSS conflates the live
// set with pages the allocator still holds, and the allocator's own view of the
// live set is not visible to any external tool. Asking the allocator means
// calling mallctl from inside, which means code in the server.
//
//   MATRIX_HS_MEMPROBE_LOG=<path>   enable; unset → this module does nothing
//   kill -USR2 <pid>                 append one sample line
//
// The line is CSV, one row per sample:
//   epoch_s,seq,trigger,allocated,resident,mapped,retained,
//   arenas,dirty_purged,dirty_npurge,muzzy_purged,muzzy_npurge,
//   dirty_decay_ms,muzzy_decay_ms,metadata,active,dirty
// allocated/resident/mapped/retained are bytes, from the four top-level stats
// mallctls. The rest is per-arena, summed over the arenas that answer. trigger
// is "start" for the row written at install time and "usr2" afterwards, so a
// row can always be placed.
//
// THE LAST THREE COLUMNS, and why they are empty-able. metadata and active are
// the top-level stats mallctls of the same names; on this node both answer and
// both are BYTES, measured rather than assumed: a probe on this host read
// allocated 28672, active 32768, resident 7942144, mapped 8421376,
// metadata 7931888, which only orders correctly if active and metadata are
// bytes (active as pages would be 128 MiB against an 7.9 MiB resident, which is
// impossible). dirty is read as a top-level name on purpose even though this
// build does not have it -- the same probe returned ENOENT for stats.dirty, for
// stats.arenas.0.dirty and for stats.arenas.0.ndirty, while
// stats.arenas.0.pdirty does answer and is in PAGES. So the dirty cell is EMPTY
// here, never 0: a name this build lacks must not be indistinguishable from a
// real zero, which is the whole reason the per-arena walk above tolerates ENOENT.
// What the three columns are FOR: window #6 left a structural floor of roughly
// 0.9 MiB (mapped minus allocated, on a fresh room, with no replay) and spikes
// whose epochs are not periodic, and neither fact can be attributed to a part of
// the allocator from the four totals alone. metadata is what the allocator holds
// for its own bookkeeping, active is the extent set it is actively using, and
// dirty would be the pages awaiting decay -- the three candidates for a floor
// that does not shrink with the live set.
//
// These three are appended at the END, so columns 1-14 keep the positions they
// have in every row already recorded in the field; an older series file still
// lines up with a newer reader for those columns.
//
// THE COUNTER THAT IS MISSING, and it matters. "How many dirty pages is the
// allocator holding right now" is what a decay change is supposed to move, and
// this build exposes no mallctl name for it: stats.arenas.N.dirty, .ndirty,
// .purge, .decaying and .decay.dirty.npages are all absent (ENOENT), while
// .dirty_purged, .dirty_npurge, .muzzy_purged, .muzzy_npurge, .dirty_decay_ms
// and .muzzy_decay_ms all answer. So the row carries the purge counters and the
// decay settings, which is what a decay decision can actually be argued from:
// with muzzy_decay_ms=0 the clean extents are handed back with MADV_FREE, and
// muzzy_purged counts how much of that the kernel has taken. A name that this
// build does not have reads as absent and contributes nothing -- never as a
// zero, which would be indistinguishable from a real zero.
//
// WHY THE WORK HAPPENS ON A THREAD AND NOT IN THE HANDLER. The handler does one
// atomic store and returns. mallctl() takes allocator-internal locks, and a
// signal delivered while another thread holds one of those locks would make the
// handler wait for a lock that cannot be released until the handler returns —
// a self-deadlock, not a slow path. A store is async-signal-safe; mallctl is not.
// The thread polls the flag, so a signal that arrives while it is mid-sample is
// not lost: the flag is still set and the next poll samples again.
//
// MEASUREMENT ONLY. Nothing here changes a request path, an allocation site, or
// a configuration default, and with the variable unset the module is a single
// environment lookup that returns. There is no code path in which installing it
// alters what the server does.
//
// jemalloc is linked into libc on the platforms this binary targets, so mallctl
// is reached as a plain libc symbol. Declaring it here rather than through a
// crate keeps the delta to this one file and adds no dependency; a wrong or
// absent mallctl would fail the asserts below loudly at first sample instead of
// writing a plausible-looking row of zeroes.

use std::ffi::CString;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::raw::c_void;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

const MALLCTL_EPOCH_RENEW: u64 = 1;
/// Per-arena extents are read for this many arenas; more than this and a single
/// sample stops being cheap. A server with more arenas than this still reports
/// the four top-level numbers, and `arenas` in the row says how many were read.
const MAX_ARENAS: usize = 64;

static TRIGGERED: AtomicBool = AtomicBool::new(false);
static SEQ: AtomicU64 = AtomicU64::new(0);
static LOG: OnceLock<Mutex<File>> = OnceLock::new();

/// A NUL-terminated name for mallctl. The names are compile-time constants in
/// this file, so an interior NUL is a bug here, not an input to handle.
fn ctl_name(name: &str) -> CString {
    CString::new(name).expect("mallctl name has no interior NUL")
}

/// Read one size_t-valued mallctl. `None` when the platform has no such counter,
/// which is a normal outcome for the per-arena names rather than a failure.
fn size_stat(name: &str) -> Option<u64> {
    let cn = ctl_name(name);
    unsafe {
        // stats.* is cached: the epoch has to be renewed for a read to see the
        // current numbers rather than the last refreshed snapshot.
        let epoch = MALLCTL_EPOCH_RENEW;
        let elen = std::mem::size_of::<u64>();
        if libc::mallctl(
            ctl_name("epoch").as_ptr(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            &epoch as *const u64 as *mut c_void,
            elen,
        ) != 0
        {
            return None;
        }
        let mut out: u64 = 0;
        let mut len = std::mem::size_of::<u64>();
        if libc::mallctl(
            cn.as_ptr(),
            &mut out as *mut u64 as *mut c_void,
            &mut len,
            std::ptr::null_mut(),
            0,
        ) != 0
        {
            return None;
        }
        Some(out)
    }
}

fn required(name: &str) -> u64 {
    size_stat(name).unwrap_or_else(|| panic!("mallctl {name} is required by memprobe but unavailable"))
}

/// One CSV field for a counter that this build may not have: the number, or an
/// EMPTY field when the name is absent. Empty and 0 are different facts — a
/// missing counter is missing, and writing 0 for it would put a number in the
/// data that no allocation ever produced.
fn opt_field(v: Option<u64>) -> String {
    match v {
        Some(n) => n.to_string(),
        None => String::new(),
    }
}

extern "C" fn on_sigusr2(_sig: i32) {
    // The only thing that happens in signal context.
    TRIGGERED.store(true, Ordering::SeqCst);
}

fn sample(trigger: &str) {
    let log = match LOG.get() {
        Some(f) => f,
        None => return,
    };
    let allocated = required("stats.allocated");
    let resident = required("stats.resident");
    let mapped = required("stats.mapped");
    let retained = required("stats.retained");

    // Per-arena counters are summed over the arenas that answer; the first
    // arena that does not end the walk, because jemalloc numbers arenas
    // contiguously from zero.
    let (mut dirty_purged, mut dirty_npurge) = (0u64, 0u64);
    let (mut muzzy_purged, mut muzzy_npurge) = (0u64, 0u64);
    let (mut dirty_decay_ms, mut muzzy_decay_ms) = (0u64, 0u64);
    let mut arenas: u64 = 0;
    for i in 0..MAX_ARENAS {
        let probe = format!("stats.arenas.{i}.dirty_purged");
        if size_stat(&probe).is_none() {
            break;
        }
        arenas += 1;
        dirty_purged += size_stat(&format!("stats.arenas.{i}.dirty_purged")).unwrap_or(0);
        dirty_npurge += size_stat(&format!("stats.arenas.{i}.dirty_npurge")).unwrap_or(0);
        muzzy_purged += size_stat(&format!("stats.arenas.{i}.muzzy_purged")).unwrap_or(0);
        muzzy_npurge += size_stat(&format!("stats.arenas.{i}.muzzy_npurge")).unwrap_or(0);
        dirty_decay_ms = size_stat(&format!("stats.arenas.{i}.dirty_decay_ms")).unwrap_or(dirty_decay_ms);
        muzzy_decay_ms = size_stat(&format!("stats.arenas.{i}.muzzy_decay_ms")).unwrap_or(muzzy_decay_ms);
    }

    // The three top-level totals the four above cannot break down. Read through
    // size_stat (not `required`): each is a name this build may not carry, and
    // an absent one must reach the row as an empty cell rather than a zero or a
    // panic on a probe whose whole purpose is to keep running.
    let metadata = size_stat("stats.metadata");
    let active = size_stat("stats.active");
    let dirty = size_stat("stats.dirty");

    let epoch_s = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let seq = SEQ.fetch_add(1, Ordering::SeqCst);
    let line = format!(
        "{epoch_s},{seq},{trigger},{allocated},{resident},{mapped},{retained},\
{arenas},{dirty_purged},{dirty_npurge},{muzzy_purged},{muzzy_npurge},{dirty_decay_ms},{muzzy_decay_ms},\
{},{},{}\n",
        opt_field(metadata),
        opt_field(active),
        opt_field(dirty),
    );
    if let Ok(mut f) = log.lock() {
        let _ = f.write_all(line.as_bytes());
        let _ = f.flush();
    }
    eprintln!("[memprobe] {line}");
}

/// Install the handler and the sampler thread. A no-op unless
/// MATRIX_HS_MEMPROBE_LOG names a writable file; that variable is the only
/// switch, so a deployment that does not set it runs exactly the code it ran
/// before this module existed.
pub fn install() {
    let path = match std::env::var("MATRIX_HS_MEMPROBE_LOG") {
        Ok(p) if !p.is_empty() => p,
        _ => return,
    };
    let file = match OpenOptions::new().create(true).append(true).open(&path) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("[memprobe] disabled: cannot open {path}: {e}");
            return;
        }
    };
    if LOG.set(Mutex::new(file)).is_err() {
        return; // already installed
    }
    // SAFETY: on_sigusr2 performs one relaxed-ordered atomic store on a static,
    // which is async-signal-safe; signal() is called before any request is
    // served, from the main task, and its return value is only used to notice a
    // refusal to install.
    let prev = unsafe { libc::signal(libc::SIGUSR2, on_sigusr2 as *const () as libc::sighandler_t) };
    if prev == libc::SIG_ERR {
        eprintln!("[memprobe] disabled: cannot install SIGUSR2 handler");
        return;
    }
    std::thread::Builder::new()
        .name("memprobe".into())
        .spawn(|| {
            sample("start");
            loop {
                std::thread::sleep(std::time::Duration::from_millis(500));
                if TRIGGERED.swap(false, Ordering::SeqCst) {
                    sample("usr2");
                }
            }
        })
        .map(|_| ())
        .unwrap_or_else(|e| eprintln!("[memprobe] disabled: cannot spawn sampler: {e}"));
}
