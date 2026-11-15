// START_AI_HEADER
// MODULE: zenoh-link-obfs/src/reject.rs
// PURPOSE: Accounting, per-IP log rate limiting and pending-handshake caps for rejected inbound
//          obfs connections.
// DEPENDENCIES: std, tracing
// PUBLIC_API: HANDSHAKE_TIMEOUT_ENV, MAX_PENDING_ENV, MAX_PENDING_PER_IP_ENV, REJECT_LINE_PREFIX,
//              handshake_timeout, RejectReason, Counts, totals, LineLimiter, record, summary_line,
//              flush_summary, Pending, PendingGuard, pending
// END_AI_HEADER

//
// bsdOS — zenoh-link-obfs
//
// Accounting and log rate-limiting for rejected inbound obfs connections.
//
// Every rejection bumps a counter (always, never rate-limited). A log line
// `[obfs-accept] Handshake FAILED from <addr>: reason=<r> ...` is emitted at most
// once per source IP per second. Once a minute a summary line is emitted when
// anything was rejected: `[obfs-accept] rejected last 60s: N (timeout=.., ...)`.
// The summary carries the exact count; per-address lines may be sampled.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
use std::{
    collections::HashMap,
    net::IpAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex, OnceLock,
    },
    time::{Duration, Instant},
};

/// Env var overriding the accept-side handshake + first-frame deadline (seconds).
pub const HANDSHAKE_TIMEOUT_ENV: &str = "BSDOS_OBFS_HANDSHAKE_TIMEOUT_SECS";
const DEFAULT_HANDSHAKE_TIMEOUT_SECS: u64 = 10;

/// Env vars bounding concurrent *unfinished* handshakes (accept -> first
/// decrypted frame). Established sessions never count against these.
pub const MAX_PENDING_ENV: &str = "BSDOS_OBFS_MAX_PENDING";
pub const MAX_PENDING_PER_IP_ENV: &str = "BSDOS_OBFS_MAX_PENDING_PER_IP";
const DEFAULT_MAX_PENDING: usize = 256;
const DEFAULT_MAX_PENDING_PER_IP: usize = 16;

// CONTRACT: Positive usize from an env var, else the default.
fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(default)
}

/// Stable prefix of every per-connection rejection line (grep-able).
pub const REJECT_LINE_PREFIX: &str = "[obfs-accept] Handshake FAILED from";

// handshake_timeout:start
//   purpose: Accept-side deadline from ClientHello to the first decrypted AEAD record, read once
//            from BSDOS_OBFS_HANDSHAKE_TIMEOUT_SECS.
//   input:  env BSDOS_OBFS_HANDSHAKE_TIMEOUT_SECS
//   output: Duration (default 10 s)
//   sideEffects: reads env once, caches in a OnceLock
// handshake_timeout:end
/// Accept-side deadline from ClientHello to the first decrypted AEAD record.
pub fn handshake_timeout() -> Duration {
    static T: OnceLock<Duration> = OnceLock::new();
    *T.get_or_init(|| {
        let secs = std::env::var(HANDSHAKE_TIMEOUT_ENV)
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .filter(|n| *n > 0)
            .unwrap_or(DEFAULT_HANDSHAKE_TIMEOUT_SECS);
        Duration::from_secs(secs)
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectReason {
    Timeout,
    BadHello,
    Auth,
    Eof,
    Limit,
}

impl RejectReason {
    // RejectReason::as_str:start
    //   purpose: Stable lowercase name of a reject reason, used in log lines.
    //   input:  self
    //   output: &str: timeout, bad-hello, auth, eof or limit
    //   sideEffects: none
    // RejectReason::as_str:end
    pub fn as_str(self) -> &'static str {
        match self {
            RejectReason::Timeout => "timeout",
            RejectReason::BadHello => "bad-hello",
            RejectReason::Auth => "auth",
            RejectReason::Eof => "eof",
            RejectReason::Limit => "limit",
        }
    }
    // CONTRACT: Index of the reason into the Counts arrays.
    fn idx(self) -> usize {
        self as usize
    }
}

/// Counts per reason: [timeout, bad-hello, auth, eof, limit].
pub type Counts = [u64; 5];

const Z: AtomicU64 = AtomicU64::new(0);
static WINDOW: [AtomicU64; 5] = [Z; 5];
static TOTAL: [AtomicU64; 5] = [Z; 5];

// totals:start
//   purpose: Process-lifetime rejection counters per reason.
//   input:  none
//   output: Counts
//   sideEffects: none
// totals:end
/// Process-lifetime rejection counters (for tests / diagnostics).
pub fn totals() -> Counts {
    std::array::from_fn(|i| TOTAL[i].load(Ordering::Relaxed))
}

/// Per-IP "at most one line per interval" limiter.
pub struct LineLimiter {
    last: HashMap<IpAddr, Instant>,
    interval: Duration,
}

const MAX_TRACKED: usize = 4096;

impl LineLimiter {
    // LineLimiter::new:start
    //   purpose: Create a per-IP line limiter with the given interval.
    //   input:  interval
    //   output: LineLimiter
    //   sideEffects: none
    // LineLimiter::new:end
    pub fn new(interval: Duration) -> Self {
        Self { last: HashMap::new(), interval }
    }
    // LineLimiter::allow:start
    //   purpose: Permit at most one log line per IP per interval; bounded to MAX_TRACKED addresses
    //            (a flood from many addresses is left to the summary line).
    //   input:  &mut self, ip, now
    //   output: true when a line may be logged
    //   sideEffects: updates the last-seen map, may prune it
    // LineLimiter::allow:end
    pub fn allow(&mut self, ip: IpAddr, now: Instant) -> bool {
        if let Some(t) = self.last.get(&ip) {
            if now.saturating_duration_since(*t) < self.interval {
                return false;
            }
        }
        if self.last.len() >= MAX_TRACKED {
            let iv = self.interval;
            self.last.retain(|_, t| now.saturating_duration_since(*t) < iv);
            if self.last.len() >= MAX_TRACKED {
                return false; // flood from many addresses: summary line covers it
            }
        }
        self.last.insert(ip, now);
        true
    }
}

// CONTRACT: Process-wide LineLimiter with a 1 s interval.
fn limiter() -> &'static Mutex<LineLimiter> {
    static L: OnceLock<Mutex<LineLimiter>> = OnceLock::new();
    L.get_or_init(|| Mutex::new(LineLimiter::new(Duration::from_secs(1))))
}

// record:start
//   purpose: Record one rejected connection: bump window and total counters, log one rate-limited
//            line.
//   input:  peer addr, reason, detail
//   output: none
//   sideEffects: increments atomics, writes to stderr and the debug trace
// record:end
/// Record one rejected connection: bump counters, log (rate-limited per IP).
pub fn record(addr: std::net::SocketAddr, reason: RejectReason, detail: &dyn std::fmt::Display) {
    WINDOW[reason.idx()].fetch_add(1, Ordering::Relaxed);
    TOTAL[reason.idx()].fetch_add(1, Ordering::Relaxed);
    let allowed = limiter()
        .lock()
        .map(|mut l| l.allow(addr.ip(), Instant::now()))
        .unwrap_or(false);
    if allowed {
        eprintln!("{REJECT_LINE_PREFIX} {addr:?}: reason={} {detail}", reason.as_str());
        tracing::debug!("obfs handshake (accept) from {addr:?} failed: reason={} {detail}", reason.as_str());
    }
}

// summary_line:start
//   purpose: Format the 60 s summary line, or None when nothing was rejected.
//   input:  c: Counts
//   output: Some(line) when the sum is non-zero
//   sideEffects: none
// summary_line:end
/// Format the summary line for a window, or None when nothing was rejected.
pub fn summary_line(c: Counts) -> Option<String> {
    let n: u64 = c.iter().sum();
    if n == 0 {
        return None;
    }
    Some(format!(
        "[obfs-accept] rejected last 60s: {n} (timeout={}, auth={}, bad-hello={}, eof={}, limit={})",
        c[RejectReason::Timeout.idx()],
        c[RejectReason::Auth.idx()],
        c[RejectReason::BadHello.idx()],
        c[RejectReason::Eof.idx()],
        c[RejectReason::Limit.idx()],
    ))
}

// flush_summary:start
//   purpose: Take and reset the window counters; print the summary line if non-empty.
//   input:  none
//   output: none
//   sideEffects: resets the window counters, writes to stderr
// flush_summary:end
/// Take and reset the window counters; print the summary if non-empty.
pub fn flush_summary() {
    let c: Counts = std::array::from_fn(|i| WINDOW[i].swap(0, Ordering::Relaxed));
    if let Some(l) = summary_line(c) {
        eprintln!("{l}");
    }
}

/// Tracks unfinished handshakes: a global cap and a per-IP cap.
pub struct Pending {
    inner: Mutex<(usize, HashMap<IpAddr, usize>)>,
    max_total: usize,
    max_per_ip: usize,
}

/// Slot held from accept until the first AEAD frame is verified (or failure).
pub struct PendingGuard {
    pending: &'static Pending,
    ip: IpAddr,
}

impl Pending {
    // Pending::new:start
    //   purpose: Create a tracker with a global and a per-IP cap on unfinished handshakes.
    //   input:  max_total, max_per_ip
    //   output: Pending
    //   sideEffects: none
    // Pending::new:end
    pub fn new(max_total: usize, max_per_ip: usize) -> Self {
        Self { inner: Mutex::new((0, HashMap::new())), max_total, max_per_ip }
    }
    // Pending::try_acquire:start
    //   purpose: Take a handshake slot unless either cap is reached.
    //   input:  &self (static), ip
    //   output: Some(PendingGuard) or None
    //   sideEffects: increments the global and per-IP counts
    // Pending::try_acquire:end
    /// Try to take a slot; None when either cap is reached.
    pub fn try_acquire(&'static self, ip: IpAddr) -> Option<PendingGuard> {
        let mut g = self.inner.lock().ok()?;
        if g.0 >= self.max_total {
            return None;
        }
        let per = g.1.get(&ip).copied().unwrap_or(0);
        if per >= self.max_per_ip {
            return None;
        }
        g.0 += 1;
        g.1.insert(ip, per + 1);
        Some(PendingGuard { pending: self, ip })
    }
    // Pending::current:start
    //   purpose: Number of unfinished handshakes now.
    //   input:  &self
    //   output: count
    //   sideEffects: none
    // Pending::current:end
    pub fn current(&self) -> usize {
        self.inner.lock().map(|g| g.0).unwrap_or(0)
    }
}

impl Drop for PendingGuard {
    // PendingGuard::drop:start
    //   purpose: Release the slot: decrement the global count and the per-IP count, removing a zero
    //            entry.
    //   input:  &mut self
    //   output: none
    //   sideEffects: updates the tracker
    // PendingGuard::drop:end
    fn drop(&mut self) {
        if let Ok(mut g) = self.pending.inner.lock() {
            g.0 = g.0.saturating_sub(1);
            if let Some(n) = g.1.get_mut(&self.ip) {
                *n -= 1;
                if *n == 0 {
                    g.1.remove(&self.ip);
                }
            }
        }
    }
}

// pending:start
//   purpose: Process-wide pending-handshake tracker with limits read once from env.
//   input:  env BSDOS_OBFS_MAX_PENDING, BSDOS_OBFS_MAX_PENDING_PER_IP
//   output: &static Pending
//   sideEffects: reads env once, caches in a OnceLock
// pending:end
/// Process-wide pending-handshake tracker, limits read once from env.
pub fn pending() -> &'static Pending {
    static P: OnceLock<Pending> = OnceLock::new();
    P.get_or_init(|| {
        Pending::new(
            env_usize(MAX_PENDING_ENV, DEFAULT_MAX_PENDING),
            env_usize(MAX_PENDING_PER_IP_ENV, DEFAULT_MAX_PENDING_PER_IP),
        )
    })
}

#[cfg(test)]
mod tests {
    // pending_caps_total_and_per_ip_and_releases:start
    //   purpose: Per-IP and total caps refuse further slots; dropping a guard frees one.
    // pending_caps_total_and_per_ip_and_releases:end
    #[test]
    fn pending_caps_total_and_per_ip_and_releases() {
        use super::*;
        let p: &'static Pending = Box::leak(Box::new(Pending::new(5, 2)));
        let a: IpAddr = "10.0.0.1".parse().unwrap();
        let b: IpAddr = "10.0.0.2".parse().unwrap();
        let c: IpAddr = "10.0.0.3".parse().unwrap();
        let a1 = p.try_acquire(a).unwrap();
        let _a2 = p.try_acquire(a).unwrap();
        assert!(p.try_acquire(a).is_none(), "per-ip cap");
        let _b1 = p.try_acquire(b).unwrap();
        let _b2 = p.try_acquire(b).unwrap();
        let _c1 = p.try_acquire(c).unwrap();
        assert!(p.try_acquire(c).is_none(), "total cap (5)");
        assert_eq!(p.current(), 5);
        drop(a1);
        assert_eq!(p.current(), 4);
        assert!(p.try_acquire(c).is_some(), "slot freed");
    }

    use super::*;

    // limiter_one_line_per_ip_per_second:start
    //   purpose: One line per IP per interval; another IP is independent; allowed again after the
    //            interval.
    // limiter_one_line_per_ip_per_second:end
    #[test]
    fn limiter_one_line_per_ip_per_second() {
        let mut l = LineLimiter::new(Duration::from_secs(1));
        let a: IpAddr = "127.0.0.1".parse().unwrap();
        let b: IpAddr = "127.0.0.2".parse().unwrap();
        let t0 = Instant::now();
        assert!(l.allow(a, t0));
        assert!(!l.allow(a, t0 + Duration::from_millis(500)));
        assert!(l.allow(b, t0 + Duration::from_millis(500)));
        assert!(l.allow(a, t0 + Duration::from_millis(1001)));
    }

    // summary_only_when_nonzero:start
    //   purpose: summary_line is None for zero counts and formats the exact line otherwise.
    // summary_only_when_nonzero:end
    #[test]
    fn summary_only_when_nonzero() {
        assert_eq!(summary_line([0, 0, 0, 0, 0]), None);
        assert_eq!(
            summary_line([1, 2, 3, 4, 5]).unwrap(),
            "[obfs-accept] rejected last 60s: 15 (timeout=1, auth=3, bad-hello=2, eof=4, limit=5)"
        );
    }
}
