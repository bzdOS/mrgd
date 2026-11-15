// START_AI_HEADER
// MODULE: tests/sigusr2_safe.rs
// PURPOSE: Integration test: SIGUSR2 must not kill the matrix-hs binary when memprobe is not
//          active.
// DEPENDENCIES: matrix-hs binary (CARGO_BIN_EXE), std
// PUBLIC_API: sigusr2_without_memprobe_keeps_process_alive
// END_AI_HEADER

//! SIGUSR2 must not kill matrix-hs when memprobe is not active.
#![cfg(unix)]
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

// CONTRACT: GET /_matrix/client/versions on 127.0.0.1:port; returns the HTTP status line or None.
fn versions_status(port: u16) -> Option<String> {
    let mut s = TcpStream::connect(("127.0.0.1", port)).ok()?;
    s.set_read_timeout(Some(Duration::from_secs(2))).ok()?;
    s.write_all(b"GET /_matrix/client/versions HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n").ok()?;
    let mut b = String::new();
    let _ = s.read_to_string(&mut b);
    b.lines().next().map(|l| l.to_string())
}

// sigusr2_without_memprobe_keeps_process_alive:start
//   purpose: Start the server without MATRIX_HS_MEMPROBE_LOG, send SIGUSR2, and require that it
//            still answers /versions.
// sigusr2_without_memprobe_keeps_process_alive:end
#[test]
fn sigusr2_without_memprobe_keeps_process_alive() {
    let port = TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let dir = std::env::temp_dir().join(format!("mrgd-usr2-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_matrix-hs"))
        .env_remove("MATRIX_HS_MEMPROBE_LOG")
        .env("MATRIX_HS_LISTEN", format!("127.0.0.1:{port}"))
        .env("MATRIX_HS_DATA_DIR", &dir)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let t0 = Instant::now();
    while versions_status(port).is_none() {
        assert!(t0.elapsed() < Duration::from_secs(30), "server did not start");
        std::thread::sleep(Duration::from_millis(100));
    }
    let pid = child.id() as libc::pid_t;
    for _ in 0..3 {
        assert_eq!(unsafe { libc::kill(pid, libc::SIGUSR2) }, 0);
        std::thread::sleep(Duration::from_millis(300));
    }
    let alive = child.try_wait().unwrap().is_none();
    let status = if alive { versions_status(port) } else { None };
    unsafe { libc::kill(pid, libc::SIGTERM) };
    let _ = child.wait();
    let mut err = String::new();
    let _ = child.stderr.take().unwrap().read_to_string(&mut err);
    let _ = std::fs::remove_dir_all(&dir);
    assert!(alive, "process died on SIGUSR2");
    assert!(status.as_deref().is_some_and(|l| l.contains(" 200")), "versions: {status:?}");
    assert_eq!(err.matches("SIGUSR2 received").count(), 1, "stderr: {err}");
}
