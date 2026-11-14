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

/// Stable prefix of every per-connection rejection line (grep-able).
pub const REJECT_LINE_PREFIX: &str = "[obfs-accept] Handshake FAILED from";

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
}

impl RejectReason {
    pub fn as_str(self) -> &'static str {
        match self {
            RejectReason::Timeout => "timeout",
            RejectReason::BadHello => "bad-hello",
            RejectReason::Auth => "auth",
            RejectReason::Eof => "eof",
        }
    }
    fn idx(self) -> usize {
        self as usize
    }
}

/// Counts per reason: [timeout, bad-hello, auth, eof].
pub type Counts = [u64; 4];

static WINDOW: [AtomicU64; 4] =
    [AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)];
static TOTAL: [AtomicU64; 4] =
    [AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)];

/// Process-lifetime rejection counters (for tests / diagnostics).
pub fn totals() -> Counts {
    [
        TOTAL[0].load(Ordering::Relaxed),
        TOTAL[1].load(Ordering::Relaxed),
        TOTAL[2].load(Ordering::Relaxed),
        TOTAL[3].load(Ordering::Relaxed),
    ]
}

/// Per-IP "at most one line per interval" limiter.
pub struct LineLimiter {
    last: HashMap<IpAddr, Instant>,
    interval: Duration,
}

const MAX_TRACKED: usize = 4096;

impl LineLimiter {
    pub fn new(interval: Duration) -> Self {
        Self { last: HashMap::new(), interval }
    }
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

fn limiter() -> &'static Mutex<LineLimiter> {
    static L: OnceLock<Mutex<LineLimiter>> = OnceLock::new();
    L.get_or_init(|| Mutex::new(LineLimiter::new(Duration::from_secs(1))))
}

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

/// Format the summary line for a window, or None when nothing was rejected.
pub fn summary_line(c: Counts) -> Option<String> {
    let n: u64 = c.iter().sum();
    if n == 0 {
        return None;
    }
    Some(format!(
        "[obfs-accept] rejected last 60s: {n} (timeout={}, auth={}, bad-hello={}, eof={})",
        c[RejectReason::Timeout.idx()],
        c[RejectReason::Auth.idx()],
        c[RejectReason::BadHello.idx()],
        c[RejectReason::Eof.idx()],
    ))
}

/// Take and reset the window counters; print the summary if non-empty.
pub fn flush_summary() {
    let c = [
        WINDOW[0].swap(0, Ordering::Relaxed),
        WINDOW[1].swap(0, Ordering::Relaxed),
        WINDOW[2].swap(0, Ordering::Relaxed),
        WINDOW[3].swap(0, Ordering::Relaxed),
    ];
    if let Some(l) = summary_line(c) {
        eprintln!("{l}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn summary_only_when_nonzero() {
        assert_eq!(summary_line([0, 0, 0, 0]), None);
        assert_eq!(
            summary_line([1, 2, 3, 4]).unwrap(),
            "[obfs-accept] rejected last 60s: 10 (timeout=1, auth=3, bad-hello=2, eof=4)"
        );
    }
}
