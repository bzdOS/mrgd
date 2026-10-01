// START_AI_HEADER
// MODULE: src/bin/scope_canary.rs
// PURPOSE: Watch a scope prefix that must never appear on this host and shout if
//          it does — the observable form of the scope-separation record (kept private) §10.
// INTENT: Scope separation is enforced by config (prefixes, carriers) and by
//         crypto (the obfs PSK). Neither is self-reporting: if an interlock is
//         undone, nothing announces it and the first symptom is household data
//         sitting in a public node's store. One subscriber turns "the scopes are
//         separate" from a belief into a monitored property.
// DEPENDENCIES: zenoh (cluster feature only)
// PUBLIC_API: binary `scope-canary`
// END_AI_HEADER

//! Run it on a `bus` host, watching the `home` prefix:
//!
//! ```sh
//! scope-canary                                  # defaults below, runs forever
//! CANARY_SECONDS=30 scope-canary                # one-shot check, exits non-zero on a leak
//! CANARY_WATCH='home/**' CANARY_CONNECT=tcp/127.0.0.1:7449 scope-canary
//! CANARY_SCOUTING=on scope-canary               # opt back into multicast/gossip discovery
//! ```
//!
//! It never listens: `listen/endpoints` is forced empty and scouting is off
//! unless `CANARY_SCOUTING=on` says otherwise, so the only socket it can hold
//! is the outbound one in `CANARY_CONNECT`.
//!
//! Exit codes: 0 = nothing seen, 1 = a leak was observed, 2 = could not start.
//!
//! Silence is a weak signal on its own — a canary that is subscribed to nothing,
//! or wired to the wrong carrier, is also silent. Verify it by publishing one
//! sample on the watched prefix and seeing it scream; there is a test that does
//! exactly this (`scope_canary_sees_a_leak_on_the_watched_prefix`).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

const DEFAULT_WATCH: &str = "home/**";
const DEFAULT_CONNECT: &str = "tcp/127.0.0.1:7449";

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    let watch = std::env::var("CANARY_WATCH").unwrap_or_else(|_| DEFAULT_WATCH.to_string());
    let connect = std::env::var("CANARY_CONNECT").unwrap_or_else(|_| DEFAULT_CONNECT.to_string());
    let seconds: u64 = std::env::var("CANARY_SECONDS")
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(0);

    let mut cfg = zenoh::Config::default();

    // ── Egress-only, always ───────────────────────────────────────────────
    // The canary detects leaks; it serves nothing. Leaving zenoh's default
    // listen/endpoints in place gave this process a reachable port on every
    // interface (and, with multicast scouting, UDP discovery as well) — which
    // is exposure, not capability: the subscriber works fine on a session that
    // only dials out. So the listening side is emptied explicitly rather than
    // left to a default that changes under us.
    if let Err(e) = cfg.insert_json5("listen/endpoints", "[]") {
        eprintln!("scope-canary: cannot clear listen/endpoints: {e}");
        std::process::exit(2);
    }

    if !connect.is_empty() {
        let json5 = format!(
            "[{}]",
            connect
                .split(',')
                .map(|e| format!("\"{}\"", e.trim()))
                .collect::<Vec<_>>()
                .join(",")
        );
        if let Err(e) = cfg.insert_json5("connect/endpoints", &json5) {
            eprintln!("scope-canary: bad CANARY_CONNECT {connect:?}: {e}");
            std::process::exit(2);
        }
    }
    // Scouting is OFF unless asked for by name. It used to be on by default, on
    // the reasoning that a leak delivered by local discovery is still a leak —
    // true, but it costs this host multicast/UDP listeners to catch a case that
    // is reproduced deliberately far more often than it happens by accident.
    // Opt back in with CANARY_SCOUTING=on when working on that case; the
    // security-relevant default is off.
    if !matches!(
        std::env::var("CANARY_SCOUTING").unwrap_or_default().as_str(),
        "on" | "1" | "true"
    ) {
        let _ = cfg.insert_json5("scouting/multicast/enabled", "false");
        let _ = cfg.insert_json5("scouting/gossip/enabled", "false");
        let _ = cfg.insert_json5("scouting/automatic", "false");
    }

    let session = match zenoh::open(cfg).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("scope-canary: cannot open zenoh session: {e}");
            std::process::exit(2);
        }
    };
    let sub = match session.declare_subscriber(&watch).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("scope-canary: cannot subscribe to {watch:?}: {e}");
            std::process::exit(2);
        }
    };

    println!(
        "scope-canary: watching {watch:?} via {connect:?} \
         ({}). Any sample here is a scope violation.",
        if seconds == 0 {
            "forever".to_string()
        } else {
            format!("for {seconds}s")
        }
    );

    let seen = Arc::new(AtomicU64::new(0));
    let seen_task = seen.clone();
    let watcher = tokio::spawn(async move {
        while let Ok(sample) = sub.recv_async().await {
            let n = seen_task.fetch_add(1, Ordering::SeqCst) + 1;
            // stderr so it survives being piped, and loud enough to grep for.
            eprintln!(
                "SCOPE LEAK #{n}: key={} bytes={} — data from a scope that must \
                 not reach this host (the scope-separation record (kept private))",
                sample.key_expr().as_str(),
                sample.payload().len()
            );
        }
    });

    if seconds == 0 {
        let _ = watcher.await;
    } else {
        tokio::time::sleep(std::time::Duration::from_secs(seconds)).await;
        watcher.abort();
    }

    let total = seen.load(Ordering::SeqCst);
    if total > 0 {
        eprintln!("scope-canary: {total} leaked sample(s) observed on {watch:?}");
        std::process::exit(1);
    }
    println!("scope-canary: clean — nothing appeared on {watch:?}");
}
