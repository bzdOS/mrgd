# zenoh-link FreeBSD + DPI-bypass patches

## Overview

`mac-companion/zenoh-link-patched/`, `zenoh-link-tls-patched/`, and
`zenoh-link-commons-patched/` together form a bsdOS-local fork of three
upstream crates from `eclipse-zenoh/zenoh`.  The fork is pinned via
`[patch.crates-io]` in the workspace `Cargo.toml` so no upstream changes
are needed at build time.

The changes fall into three independent groups that could each become a
separate upstream PR.

---

## Group 1 — New transport: `zenoh-link-obfs` (bsdOS-specific, DPI bypass)

### Crate affected
`zenoh-link` (`zenoh-link-patched/src/lib.rs`)

### Problem
Upstream `zenoh-link` routes locator prefixes to transport implementations
via a match on feature flags.  There is no extension point for third-party
transports without forking.

### Change
`ObfsLocatorInspector` and `LinkManagerUnicastObfs` are wired in
**unconditionally** (no `transport_obfs` feature gate).  The `obfs/`
prefix is always available in the dispatcher.

```rust
// Before (upstream): no obfs lines at all

// After (patched):
pub use zenoh_link_obfs as obfs;
use zenoh_link_obfs::{LinkManagerUnicastObfs, ObfsLocatorInspector, OBFS_LOCATOR_PREFIX};
// ...
OBFS_LOCATOR_PREFIX => supported_links.push(LinkKind::Obfs),
// ...
LinkKind::Obfs => Ok(std::sync::Arc::new(LinkManagerUnicastObfs::new(_manager))),
```

### Upstream PR strategy
Open a feature-gated upstream PR:
```toml
[features]
transport_obfs = ["zenoh-link-obfs"]
```
The `zenoh-link-obfs` crate itself cannot be upstreamed (bsdOS-specific PSK
AEAD transport), but the dispatcher hook pattern could be accepted as an
extension mechanism.

---

## Group 2 — TLS: ALPN DPI bypass

### Crates affected
`zenoh-link-tls` (`zenoh-link-tls-patched/src/utils.rs`)

### Problem
Upstream TLS links send no ALPN extension.  Deep-packet inspection
appliances (e.g. ТСПУ) can distinguish Zenoh TLS from browser HTTPS by
the absence of the `h2` / `http/1.1` ALPN values.

### Change
Both `TlsServerConfig::new()` and `TlsClientConfig::new()` set ALPN after
building the rustls config:

```rust
// DPI bypass: set ALPN to mimic browser HTTPS traffic
sc.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
// ...
cc.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
```

| File | Location | Change | Reason |
|---|---|---|---|
| `zenoh-link-tls-patched/src/utils.rs` | `TlsServerConfig::new()`, after `ServerConfig` build | `sc.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()]` | Make Zenoh TLS handshake look like browser HTTPS to DPI |
| `zenoh-link-tls-patched/src/utils.rs` | `TlsClientConfig::new()`, after `ClientConfig` build | `cc.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()]` | Same reason, client side |

### Upstream PR strategy
This is a general-purpose hardening and could be upstreamed as-is.
ALPN does not change protocol semantics; it only affects the TLS handshake.
A config option `tls_alpn_protocols: Vec<String>` (defaulting to `["h2",
"http/1.1"]`) would be the cleanest interface.

---

## Group 3 — TLS: `skip_certificate_verification` and `NoCertVerifier`

### Crates affected
`zenoh-link-commons` (`tls.rs`), `zenoh-link-tls` (`utils.rs`)

### Problem
Upstream provides `WebPkiVerifierAnyServerName` (CA check, no name check)
but no way to skip CA verification entirely.  Development environments with
self-signed certificates that are not in any trust store require a full
skip.

### Change

**`zenoh-link-commons-patched/src/tls.rs`** — new `NoCertVerifier` struct:
```rust
pub struct NoCertVerifier;
impl ServerCertVerifier for NoCertVerifier {
    fn verify_server_cert(...) -> Result<ServerCertVerified, rustls::Error> {
        tracing::warn!("Certificate verification DISABLED ...");
        Ok(ServerCertVerified::assertion())
    }
    // tls12/tls13 signatures pass through unconditionally
}
```

**`zenoh-link-tls-patched/src/utils.rs`** — `TlsClientConfig::new()` reads
`ZENOH_TLS_SKIP_CERT_VERIFICATION` env var and routes to `NoCertVerifier`:
```rust
let tls_skip_cert_verification: bool = std::env::var("ZENOH_TLS_SKIP_CERT_VERIFICATION")
    .or_else(|_| std::env::var("SKIP_CERT_VERIFICATION"))
    .map(|v| v != "0" && v.to_lowercase() != "false")
    .unwrap_or(TLS_SKIP_CERTIFICATE_VERIFICATION_DEFAULT);
```

| File | Change | Reason |
|---|---|---|
| `zenoh-link-commons-patched/src/tls.rs` | Add `NoCertVerifier` struct + `TLS_SKIP_CERTIFICATE_VERIFICATION` const | Accept any cert without CA check (dev/test only) |
| `zenoh-link-tls-patched/src/utils.rs` | Read `ZENOH_TLS_SKIP_CERT_VERIFICATION` env; branch to `NoCertVerifier` | Wire skip-cert into config build path |

### Upstream PR strategy
Upstream as opt-in feature with prominent warnings and docs:
1. Add `NoCertVerifier` to `zenoh-link-commons/src/tls.rs`
2. Add `skip_certificate_verification: bool` field to `zenoh-config`'s
   `TLSConf` struct
3. Read from config (not env var) in `zenoh-link-tls/src/utils.rs`

See also `PATCH-PROGRESS.md` in this directory for the original
implementation notes.

---

## Group 4 — `SO_BINDTODEVICE` → FreeBSD stubs (in `zenoh-util`)

This group lives in a **separate crate**: `vendor/zenoh-util-freebsd/`.

See `vendor/zenoh-util-freebsd/UPSTREAM-PR.md` for the full write-up.

Short summary: `set_bind_to_device_tcp_socket` / `set_bind_to_device_udp_socket`
are only implemented for `linux|android` and `macos|ios|windows` upstream.
FreeBSD does not have `SO_BINDTODEVICE` (the equivalent is `IP_BOUND_IF`,
not exposed through `socket2`/`tokio`).  The bsdOS fork adds warn+no-op
stubs for `#[cfg(target_os = "freebsd")]`, matching the macOS/Windows
pattern.  This is the compile-time blocker for the entire zenoh-link stack
on FreeBSD: without `set_bind_to_device_*` stubs, `zenoh-link-commons`
refuses to compile.

---

## Status summary

| Group | Ready to PR? | Blocker |
|---|---|---|
| 1 — obfs transport hook | No | obfs crate is bsdOS-internal (PSK AEAD); only the dispatch hook mechanism is upstreamable |
| 2 — ALPN DPI bypass | Yes | None; general-purpose hardening |
| 3 — skip_cert_verification | Yes (with zenoh-config change) | Requires adding field to `zenoh-config::TLSConf` struct |
| 4 — FreeBSD SO_BINDTODEVICE stubs | Yes | PR target: `commons/zenoh-util/src/net/mod.rs` (see zenoh-util-freebsd/) |

## Repository targets

All PRs go to: https://github.com/eclipse-zenoh/zenoh

Relevant upstream files:
- `commons/zenoh-util/src/net/mod.rs` — Group 4
- `io/zenoh-link-commons/src/tls.rs` — Group 3
- `io/zenoh-link-tls/src/utils.rs` — Groups 2, 3
- `io/zenoh-link/src/lib.rs` — Group 1
