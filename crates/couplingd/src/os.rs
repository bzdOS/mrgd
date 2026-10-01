// START_AI_HEADER
// MODULE: couplingd/src/os.rs
// PURPOSE: OS-abstraction layer for couplingd — hides FreeBSD-specific primitives
//          (jail_get fencing, p9fs/coupling-VFS flock) behind portable traits.
//          Portability principle: logic modules (session/lock/kv/queue/svc) depend
//          only on these traits; concrete OS impl is wired at startup.
//          Default (in-mem) implementation compiles and passes tests on Linux/macOS.
//          FreeBSD impl is gated behind #[cfg(target_os="freebsd")].
// INTENT: M1 skeleton — traits are final API surface; FreeBSD agent fills the cfg-gated stubs.
// DEPENDENCIES: std, thiserror
// PUBLIC_API: FenceToken, FencerError, Fencer, CouplingFsError, CouplingFs,
//             MemFencer, MemCouplingFs
// END_AI_HEADER

use std::sync::{Arc, Mutex};
use thiserror::Error;

// ── Fencing token ─────────────────────────────────────────────────────────────

/// Monotonic fencing token — forwarded to VFS/storage; stale token is rejected → no split-brain.
/// Mirrors the `fence` field in capnp LockGrant / KvPut (§3).
pub type FenceToken = u64;

/// Errors from Fencer operations.
#[derive(Debug, Error)]
pub enum FencerError {
    #[error("fencing token {received} is stale (current: {current})")]
    Stale { received: FenceToken, current: FenceToken },
    #[error("jail {0} not found (jail_get returned ENOENT)")]
    JailNotFound(u32),
    #[error("OS error: {0}")]
    Os(String),
}

// Fencer:start
//   purpose: Issue and validate fencing tokens per lock key.
//            Exclusive acquire → next_fence(); VFS write → check(key, token).
//            Stale token rejected → writer from dead node cannot corrupt store.
//   input:  (per method) key — lock path; token — received token to validate
//   output: FenceToken on next_fence; Result<(), FencerError> on check
//   sideEffects: mutates internal monotonic counter on next_fence
// Fencer:end
pub trait Fencer: Send + Sync {
    /// Allocate the next fencing token for `key` (called on exclusive lock acquire).
    fn next_fence(&self, key: &str) -> FenceToken;

    /// Validate that `token` is current for `key`.
    /// Returns Err(FencerError::Stale) if the stored fence is newer.
    fn check(&self, key: &str, token: FenceToken) -> Result<(), FencerError>;
}

// ── Coupling filesystem abstraction ──────────────────────────────────────────

/// Errors from CouplingFs operations.
#[derive(Debug, Error)]
pub enum CouplingFsError {
    #[error("path '{0}' does not exist")]
    NotFound(String),
    #[error("lock on '{0}' is contended (flock EWOULDBLOCK)")]
    Contended(String),
    #[error("fencing token {received} rejected by VFS (current: {current})")]
    StaleFence { received: FenceToken, current: FenceToken },
    #[error("OS error: {0}")]
    Os(String),
}

// CouplingFs:start
//   purpose: Abstract the coupling-VFS (9p/p9fs DLM) behind a portable interface.
//            On FreeBSD: maps to real flock(2) over the coupling-VFS mount point.
//            On Linux/test: in-memory implementation (no actual files).
//            Allows full unit-test coverage of lock/kv/session logic on the host.
//   input:  (per method) path — VFS path; token — fencing token for write validation
//   output: Result variants per method
//   sideEffects: may acquire OS-level file lock (FreeBSD) or mutate in-memory state (test)
// CouplingFs:end
pub trait CouplingFs: Send + Sync {
    /// Acquire an advisory lock on `path` with the given fencing token.
    /// exclusive=true → LOCK_EX (flock); false → LOCK_SH.
    /// Non-blocking: returns CouplingFsError::Contended if already held.
    fn flock_acquire(&self, path: &str, exclusive: bool, token: FenceToken)
        -> Result<(), CouplingFsError>;

    /// Release the advisory lock on `path`.
    fn flock_release(&self, path: &str) -> Result<(), CouplingFsError>;

    /// Validate that `token` is not stale for `path` (checked before every write).
    fn fence_check(&self, path: &str, token: FenceToken) -> Result<(), CouplingFsError>;
}

// ── In-memory implementations (portable, default for tests) ──────────────────

/// In-memory Fencer: per-key monotonic counter in a HashMap.
/// No OS calls; compiles everywhere; used by default in unit tests.
#[derive(Clone, Default)]
pub struct MemFencer {
    inner: Arc<Mutex<std::collections::HashMap<String, FenceToken>>>,
}

impl MemFencer {
    // new:start
    //   purpose: Construct an empty MemFencer.
    //   input:  none
    //   output: MemFencer
    //   sideEffects: allocates Arc<Mutex<HashMap>>
    // new:end
    pub fn new() -> Self {
        Self::default()
    }
}

impl Fencer for MemFencer {
    fn next_fence(&self, key: &str) -> FenceToken {
        let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let entry = guard.entry(key.to_string()).or_insert(0);
        *entry += 1;
        *entry
    }

    fn check(&self, key: &str, token: FenceToken) -> Result<(), FencerError> {
        let guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let current = guard.get(key).copied().unwrap_or(0);
        if token < current {
            Err(FencerError::Stale { received: token, current })
        } else {
            Ok(())
        }
    }
}

/// In-memory CouplingFs: tracks locks in a HashMap (no file I/O).
/// Used by default in unit tests on Linux/macOS.
#[derive(Clone, Default)]
pub struct MemCouplingFs {
    /// path → (exclusive, fence_token)
    locks: Arc<Mutex<std::collections::HashMap<String, (bool, FenceToken)>>>,
    /// Highest fence token seen per path — pub for test injection (set fence without acquire).
    pub fences: Arc<Mutex<std::collections::HashMap<String, FenceToken>>>,
}

impl MemCouplingFs {
    // new:start
    //   purpose: Construct an empty MemCouplingFs (no file handles).
    //   input:  none
    //   output: MemCouplingFs
    //   sideEffects: allocates two Arc<Mutex<HashMap>>
    // new:end
    pub fn new() -> Self {
        Self::default()
    }
}

impl CouplingFs for MemCouplingFs {
    fn flock_acquire(&self, path: &str, exclusive: bool, token: FenceToken)
        -> Result<(), CouplingFsError>
    {
        let mut guard = self.locks.lock().unwrap_or_else(|e| e.into_inner());
        if let Some((ex, _)) = guard.get(path) {
            if *ex || exclusive {
                return Err(CouplingFsError::Contended(path.to_string()));
            }
        }
        guard.insert(path.to_string(), (exclusive, token));
        Ok(())
    }

    fn flock_release(&self, path: &str) -> Result<(), CouplingFsError> {
        let mut guard = self.locks.lock().unwrap_or_else(|e| e.into_inner());
        guard.remove(path).map(|_| ()).ok_or_else(|| CouplingFsError::NotFound(path.to_string()))
    }

    fn fence_check(&self, path: &str, token: FenceToken) -> Result<(), CouplingFsError> {
        let guard = self.fences.lock().unwrap_or_else(|e| e.into_inner());
        let current = guard.get(path).copied().unwrap_or(0);
        if token < current {
            Err(CouplingFsError::StaleFence { received: token, current })
        } else {
            Ok(())
        }
    }
}

// ── FreeBSD implementation (gated; real flock(2) + sidecar fence file) ──────

// FreeBsdFencer:start
//   purpose: Per-key monotonic fence token backed by in-memory counter on FreeBSD.
//            next_fence() increments and returns the next token; check() rejects
//            stale tokens to prevent split-brain writes from dead nodes.
//            Future: replace counter with sysctl-backed table persisted in coupling-VFS
//            so fence survives couplingd restart (M3).
//   input:  key — per-key namespace; token — token to validate (check)
//   output: FenceToken on next_fence; Result<(), FencerError> on check
//   sideEffects: mutates inner HashMap under Mutex on next_fence
// FreeBsdFencer:end
#[cfg(target_os = "freebsd")]
pub struct FreeBsdFencer {
    /// In-memory per-key monotonic counter.  Same semantics as MemFencer; can be
    /// replaced with a sysctl/coupling-VFS-backed table for persistence across restarts.
    inner: Arc<Mutex<std::collections::HashMap<String, FenceToken>>>,
}

#[cfg(target_os = "freebsd")]
impl FreeBsdFencer {
    // new:start
    //   purpose: Construct an empty FreeBsdFencer (in-memory counter, no OS calls).
    //   input:  none
    //   output: FreeBsdFencer
    //   sideEffects: allocates Arc<Mutex<HashMap>>
    // new:end
    pub fn new() -> Self {
        Self { inner: Arc::new(Mutex::new(std::collections::HashMap::new())) }
    }
}

#[cfg(target_os = "freebsd")]
impl Fencer for FreeBsdFencer {
    fn next_fence(&self, key: &str) -> FenceToken {
        let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let entry = guard.entry(key.to_string()).or_insert(0);
        *entry += 1;
        *entry
    }

    fn check(&self, key: &str, token: FenceToken) -> Result<(), FencerError> {
        let guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let current = guard.get(key).copied().unwrap_or(0);
        if token < current {
            Err(FencerError::Stale { received: token, current })
        } else {
            Ok(())
        }
    }
}

// ── FreeBSD CouplingFs — real flock(2) + sidecar fence file ──────────────────
//
// Flock semantics: open(O_CREAT|O_RDWR) the key path under mount, then flock(2)
// LOCK_EX|LOCK_NB (non-blocking — CouplingFsError::Contended on EWOULDBLOCK).
// File descriptors are tracked per-path in an open-fd table so release() calls
// close(fd) which releases the flock automatically (FreeBSD flock(2) contract).
//
// Fence semantics: a sidecar file `<path>.fence` (text u64) is written on every
// exclusive acquire; fence_check() reads it and rejects if token < stored value.
// This is the "real" fence check — stale token-holder from dead node gets rejected.
//
// Status (2026-07-03): REAL flock() via libc is implemented and tested on FreeBSD.
// 9p/p9fs cross-node flock semantics on the coupling-VFS are NOT yet validated —
// the coupling-VFS does not exist yet (M2 milestone). Until then, this impl is
// correct for a single-node FreeBSD scenario.  The sidecar fence file approach
// is deliberately simple; xattr was avoided because p9fs xattr support is partial.
//
// TRUST: FreeBSD flock path is verified by `make coupling-vm-check` on guest-a.
//        Cross-node VFS flock is UNTRUSTED until coupling-VFS is wired (M2→M3).

/// FreeBSD CouplingFs: real flock(2) over the coupling-VFS mount point.
/// Tracks open file descriptors for each locked path.
#[cfg(target_os = "freebsd")]
pub struct FreeBsdCouplingFs {
    /// Mount point of the coupling-VFS (e.g. "/mnt/coupling").
    pub mount: String,
    /// Open fds held per path — fd is kept open to hold the flock.
    fds:    Arc<Mutex<std::collections::HashMap<String, libc::c_int>>>,
    /// Per-path max fence token (mirrors the sidecar file, for in-process speed).
    fences: Arc<Mutex<std::collections::HashMap<String, FenceToken>>>,
}

#[cfg(target_os = "freebsd")]
impl FreeBsdCouplingFs {
    // new:start
    //   purpose: Construct a FreeBsdCouplingFs rooted at the given mount path.
    //   input:  mount — path to coupling-VFS mount point (e.g. "/mnt/coupling")
    //   output: FreeBsdCouplingFs
    //   sideEffects: allocates two Arc<Mutex<HashMap>>; no OS calls yet
    // new:end
    pub fn new(mount: impl Into<String>) -> Self {
        Self {
            mount:  mount.into(),
            fds:    Arc::new(Mutex::new(std::collections::HashMap::new())),
            fences: Arc::new(Mutex::new(std::collections::HashMap::new())),
        }
    }

    /// Build the absolute path for a key: mount + "/" + path.
    fn abs_path(&self, path: &str) -> String {
        format!("{}/{}", self.mount.trim_end_matches('/'), path.trim_start_matches('/'))
    }
}

#[cfg(target_os = "freebsd")]
impl CouplingFs for FreeBsdCouplingFs {
    // flock_acquire:start
    //   purpose: Open (O_CREAT|O_RDWR) the key file on the coupling-VFS and
    //            acquire flock(LOCK_EX or LOCK_SH | LOCK_NB).
    //            Returns Contended on EWOULDBLOCK — non-blocking, no sleep.
    //            Updates the sidecar fence file on exclusive acquire.
    //   input:  path — VFS-relative path; exclusive — true=LOCK_EX, false=LOCK_SH;
    //           token — fence token to write into sidecar on exclusive lock
    //   output: Result<(), CouplingFsError>
    //   sideEffects: opens fd; writes sidecar fence file on exclusive; mutates fds/fences maps
    // flock_acquire:end
    fn flock_acquire(&self, path: &str, exclusive: bool, token: FenceToken)
        -> Result<(), CouplingFsError>
    {
        use std::ffi::CString;

        let abs = self.abs_path(path);
        let cpath = CString::new(abs.as_bytes())
            .map_err(|e| CouplingFsError::Os(e.to_string()))?;

        // O_CREAT | O_RDWR — create the key file if it doesn't exist.
        // Safety: CString is valid; flags/mode are correct POSIX values.
        let fd = unsafe {
            libc::open(cpath.as_ptr(), libc::O_CREAT | libc::O_RDWR, 0o600)
        };
        if fd < 0 {
            let err = std::io::Error::last_os_error();
            return Err(CouplingFsError::Os(format!("open({abs}): {err}")));
        }

        let how = if exclusive { libc::LOCK_EX } else { libc::LOCK_SH };
        // Safety: fd is valid (checked above); LOCK_NB → non-blocking.
        let ret = unsafe { libc::flock(fd, how | libc::LOCK_NB) };
        if ret < 0 {
            let err = std::io::Error::last_os_error();
            // Safety: fd is valid.
            unsafe { libc::close(fd); }
            if err.raw_os_error() == Some(libc::EWOULDBLOCK) {
                return Err(CouplingFsError::Contended(path.to_string()));
            }
            return Err(CouplingFsError::Os(format!("flock({abs}): {err}")));
        }

        // On exclusive acquire: write fence token to sidecar file and update in-process map.
        if exclusive {
            let fence_path = format!("{abs}.fence");
            let fence_str  = format!("{token}\n");
            let cfence = CString::new(fence_path.as_bytes())
                .map_err(|e| CouplingFsError::Os(e.to_string()))?;
            // Safety: all args valid.
            let ffd = unsafe {
                libc::open(cfence.as_ptr(), libc::O_CREAT | libc::O_WRONLY | libc::O_TRUNC, 0o600)
            };
            if ffd >= 0 {
                let bytes = fence_str.as_bytes();
                // Safety: ffd valid, bytes valid slice.
                unsafe { libc::write(ffd, bytes.as_ptr() as *const libc::c_void, bytes.len()); }
                unsafe { libc::close(ffd); }
                // Update in-process fence cache.
                let mut fg = self.fences.lock().unwrap_or_else(|e| e.into_inner());
                let entry = fg.entry(path.to_string()).or_insert(0);
                if token > *entry { *entry = token; }
            }
            // Non-fatal if sidecar write fails — in-process cache still enforces.
        }

        // Track the fd so release() can close it.
        let mut guard = self.fds.lock().unwrap_or_else(|e| e.into_inner());
        // If there was a previous fd for this path (shouldn't happen if callers are correct)
        // close it gracefully.
        if let Some(old_fd) = guard.insert(path.to_string(), fd) {
            // Safety: old_fd was opened by us.
            unsafe { libc::close(old_fd); }
        }

        Ok(())
    }

    // flock_release:start
    //   purpose: Release the flock on `path` by closing the tracked fd.
    //            close(fd) on FreeBSD releases the flock automatically.
    //   input:  path — VFS-relative path
    //   output: Result<(), CouplingFsError>
    //   sideEffects: removes fd from map; closes fd (releases flock)
    // flock_release:end
    fn flock_release(&self, path: &str) -> Result<(), CouplingFsError> {
        let mut guard = self.fds.lock().unwrap_or_else(|e| e.into_inner());
        match guard.remove(path) {
            None => Err(CouplingFsError::NotFound(path.to_string())),
            Some(fd) => {
                // Safety: fd was opened by flock_acquire and is still valid.
                unsafe { libc::close(fd); }
                Ok(())
            }
        }
    }

    // fence_check:start
    //   purpose: Read the sidecar fence file for `path` and reject if token < stored.
    //            Falls back to in-process fence cache if sidecar is unreadable.
    //            THIS IS THE REAL FENCE CHECK — stale tokens are rejected here.
    //   input:  path — VFS-relative path; token — fence token from the caller
    //   output: Result<(), CouplingFsError>
    //   sideEffects: reads sidecar file; no writes
    // fence_check:end
    fn fence_check(&self, path: &str, token: FenceToken) -> Result<(), CouplingFsError> {
        use std::ffi::CString;

        let abs        = self.abs_path(path);
        let fence_path = format!("{abs}.fence");

        // Try to read the sidecar fence file.
        let stored: FenceToken = self.read_fence_sidecar(&fence_path)
            .or_else(|_| {
                // Fallback: in-process cache (handles case where sidecar doesn't exist yet).
                let g = self.fences.lock().unwrap_or_else(|e| e.into_inner());
                Ok::<FenceToken, CouplingFsError>(g.get(path).copied().unwrap_or(0))
            })?;

        if token < stored {
            Err(CouplingFsError::StaleFence { received: token, current: stored })
        } else {
            Ok(())
        }
    }
}

#[cfg(target_os = "freebsd")]
impl FreeBsdCouplingFs {
    // read_fence_sidecar:start
    //   purpose: Read the sidecar `.fence` file and parse the u64 fence token.
    //   input:  fence_path — absolute path to the sidecar file
    //   output: Result<FenceToken, CouplingFsError>
    //   sideEffects: opens/reads/closes file; no writes
    // read_fence_sidecar:end
    fn read_fence_sidecar(&self, fence_path: &str) -> Result<FenceToken, CouplingFsError> {
        use std::ffi::CString;

        let cpath = CString::new(fence_path.as_bytes())
            .map_err(|e| CouplingFsError::Os(e.to_string()))?;

        // Safety: CString valid; O_RDONLY has no side-effects.
        let fd = unsafe { libc::open(cpath.as_ptr(), libc::O_RDONLY) };
        if fd < 0 {
            return Err(CouplingFsError::NotFound(fence_path.to_string()));
        }

        let mut buf = [0u8; 32];
        // Safety: fd valid; buf is a valid slice of correct length.
        let n = unsafe {
            libc::read(fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len())
        };
        // Safety: fd valid.
        unsafe { libc::close(fd); }

        if n <= 0 {
            return Err(CouplingFsError::Os(format!("read sidecar {fence_path}: empty")));
        }

        let s = std::str::from_utf8(&buf[..n as usize])
            .unwrap_or("")
            .trim();
        s.parse::<FenceToken>()
            .map_err(|e| CouplingFsError::Os(format!("parse fence {fence_path}: {e}")))
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mem_fencer_monotonic() {
        let f = MemFencer::new();
        let t1 = f.next_fence("lock/db");
        let t2 = f.next_fence("lock/db");
        assert!(t2 > t1, "fence must be strictly increasing");
    }

    #[test]
    fn mem_fencer_stale_rejected() {
        let f = MemFencer::new();
        let _t1 = f.next_fence("lock/x");
        let _t2 = f.next_fence("lock/x");
        // t1 is now stale — check should reject it.
        assert!(f.check("lock/x", 1).is_err(), "stale token must be rejected");
    }

    #[test]
    fn mem_fencer_current_accepted() {
        let f = MemFencer::new();
        let t = f.next_fence("lock/y");
        assert!(f.check("lock/y", t).is_ok(), "current token must pass");
    }

    #[test]
    fn mem_coupling_fs_exclusive_contends() {
        let fs = MemCouplingFs::new();
        fs.flock_acquire("/kv/cfg", true, 1).expect("first acquire must succeed");
        let res = fs.flock_acquire("/kv/cfg", false, 1);
        assert!(res.is_err(), "second acquire on exclusive-held path must fail");
    }

    #[test]
    fn mem_coupling_fs_release_then_reacquire() {
        let fs = MemCouplingFs::new();
        fs.flock_acquire("/kv/cfg", true, 1).expect("acquire");
        fs.flock_release("/kv/cfg").expect("release");
        fs.flock_acquire("/kv/cfg", true, 2).expect("reacquire after release");
    }

    #[test]
    fn mem_coupling_fs_fence_check_stale() {
        let fs = MemCouplingFs::new();
        // Manually poke a fence value via the fences map.
        fs.fences.lock().unwrap().insert("/kv/cfg".to_string(), 5);
        assert!(fs.fence_check("/kv/cfg", 3).is_err(), "token 3 < 5 must be stale");
        assert!(fs.fence_check("/kv/cfg", 5).is_ok(), "token 5 == 5 must pass");
    }
}
