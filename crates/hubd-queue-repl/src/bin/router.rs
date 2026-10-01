// START_AI_HEADER
// MODULE: hubd-queue-repl/src/bin/router.rs
// PURPOSE: A tiny Zenoh ROUTER for the hubd queue-replication fabric. Runs on one
//          node (build-host) listening on localhost; queue-repl instances connect to it
//          as CLIENTS (mode=client). The router brokers pub/sub between them — the
//          canonical reliable zenoh topology, avoiding peer-mode routing flakiness.
//
//          Cross-node encryption: the router binds 127.0.0.1 only. A remote node
//          (offsite, behind NAT) reaches it through an `ssh -L 7449:127.0.0.1:7449
//          build-host` tunnel, so the only internet-crossing zenoh link rides ssh (TLS).
//          No plaintext zenoh on any public interface.
//
// ENV:
//   HUBD_QROUTER_LISTEN  zenoh listen endpoint (default tcp/127.0.0.1:7449)
// END_AI_HEADER

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_writer(std::io::stderr)
        .try_init();

    let listen = std::env::var("HUBD_QROUTER_LISTEN")
        .unwrap_or_else(|_| "tcp/127.0.0.1:7449".to_string());

    let mut cfg = zenoh::Config::default();
    cfg.insert_json5("mode", "\"router\"")?;
    cfg.insert_json5("listen/endpoints", &format!("[\"{}\"]", listen))?;
    // A router should not auto-mesh with the bsdos zenoh fabric via scouting — it is a
    // private broker for queue-repl clients only.
    cfg.insert_json5("scouting/multicast/enabled", "false")?;
    cfg.insert_json5("scouting/gossip/enabled", "false")?;

    eprintln!("[hubd-queue-router] mode=router listen={listen}");
    let _session = zenoh::open(cfg).await?;
    eprintln!("[hubd-queue-router] router ready — brokering forever");
    std::future::pending::<()>().await;
    Ok(())
}
