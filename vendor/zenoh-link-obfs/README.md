# zenoh-link-obfs

DPI-resistant Zenoh unicast transport for bsdOS. Implements
[`PLAN-dpi-transport.md`](../../PLAN-dpi-transport.md) variant **(a)**: a custom
Zenoh link that carries the Zenoh byte-stream over **AEAD-framed raw TCP** instead
of TLS. There is no TLS ClientHello, so there is **no JA3/JA4 TLS fingerprint** for a
passive DPI box (ТСПУ/GFW) to classify.

> **Handshake design:** The ephemeral public keys are encoded with **Elligator2**
> before being sent, so the handshake prefix is computationally indistinguishable
> from uniform random. See [§Crypto design](#crypto-design) and
> [§Residual limitations](#residual-limitations-plan-5).

## What it is

- Locator prefix: **`obfs/<host:port>`** (e.g. `obfs/0.0.0.0:443`).
- Handshake: **X25519** ephemeral ECDH on both sides, mixed with a **PSK**-bound
  static server key, run through **HKDF-SHA256** → session key (PLAN §4.2).
  Ephemeral pubkeys are sent as **Elligator2 representatives** (32 bytes +
  1-byte random tweak = 33 bytes per side), making the handshake prefix
  computationally indistinguishable from uniform random.
- Record framing (SS-AEAD style, PLAN §4.3):
  `[enc(u16 len)+16B tag][enc(payload)+16B tag]`, ChaCha20-Poly1305,
  96-bit little-endian **per-direction** counter nonce (separate c2s/s2c
  counters, no nonce reuse), payload ≤ `0x3FFF` bytes.
- No hand-rolled crypto. Primitives: `chacha20poly1305`, `x25519-dalek`,
  `curve25519-elligator2` (Elligator2 encoding), `hkdf` + `sha2`, `rand_core`.

## Crypto design

### Elligator2 handshake encoding

Raw X25519 ephemeral public keys are **not** uniformly random: they are canonical
Montgomery u-coordinates `< 2^255 − 19`, so the MSB of the last byte is **always
0** and the distribution is non-uniform. A passive DPI box can build a statistical
classifier on the first 32 bytes of every new connection.

This crate replaces the raw pubkey with its **Elligator2 representative**: a
32-byte value that is computationally indistinguishable from a uniform random
string. The `curve25519-elligator2` crate (a fork of `curve25519-dalek` pending
upstream inclusion) provides `representative_from_privkey(scalar, tweak)` for
encoding and `MontgomeryPoint::from_representative::<RFC9380>(repr)` for decoding.

**Wire format for each ephemeral message:**

```
[32 bytes — Elligator2 representative][1 byte — random tweak]
```

The tweak is a random byte chosen per connection; it is sent in the clear and
controls which of the two Elligator2 pre-images is used during encoding. The
decoder uses only the 32-byte representative (the decode is unique regardless of
the tweak value).

**Retry loop:** only ~50% of X25519 scalars are Elligator2-representable.
`gen_ephemeral_elligator()` retries with a fresh scalar+tweak until it finds one;
expected ≈1.4 attempts, capped at 64.

### Session key derivation

```
HKDF salt uses the *decoded pubkey bytes* (not the representatives) so both
sides compute the identical salt regardless of which representative was chosen.
```

### Per-direction nonce counters

`ObfsStream` holds separate `send_nonce` and `recv_nonce` counters. Client sends
with c2s, receives with s2c; server is the mirror. This prevents nonce reuse
across directions even if framing nonce values overlap.

## Crate layout

| File | Responsibility |
|---|---|
| `src/crypto.rs` | `PreSharedKey`, `StaticServerKey`, `ObfsStream<S>` — handshake + AEAD framing. |
| `src/unicast.rs` | `LinkUnicastObfs` (impl `LinkUnicastTrait`), `LinkManagerUnicastObfs` (impl `LinkManagerUnicastTrait`), connector + listener accept loop. |
| `src/lib.rs` | `OBFS_LOCATOR_PREFIX`, `ObfsLocatorInspector`, `ObfsConfigurator`, MTU/linger config, `load_psk`. |

Public surface intentionally mirrors `zenoh-link-tls`'s shape
(`LinkManagerUnicastObfs` ↔ `LinkManagerUnicastTls`, `ObfsConfigurator` ↔
`TlsConfigurator`, `ObfsLocatorInspector` ↔ `TlsLocatorInspector`,
`OBFS_LOCATOR_PREFIX` ↔ `TLS_LOCATOR_PREFIX`).

## Pre-shared key (PSK)

32 raw bytes, identical on client and server. Supplied out-of-band:

1. environment variable **`BSDOS_OBFS_PSK`** (base64), or
2. endpoint config parameter **`obfs_psk_base64`** (base64) — convenient for tests.

The static server keypair is derived deterministically from the PSK
(`HKDF(PSK, "bsdos-obfs-static-v1")`), so no second secret needs distribution.

```sh
openssl rand -base64 32        # generate a PSK
export BSDOS_OBFS_PSK="<that base64>"
```

## Wiring into Zenoh (REQUIRED — not done by this crate alone)

Zenoh 1.9.0's `zenoh-link` dispatcher is a **closed** crate: the protocol→manager
mapping is a hardcoded `enum LinkKind` plus `match` arms in
`LinkManagerBuilderUnicast::make` / `LinkKind::try_from`. A brand-new `obfs/`
prefix therefore **cannot** be activated merely by adding this crate to
`[patch.crates-io]` (and in fact a `[patch.crates-io]` entry for a non-existent
crates.io crate makes cargo error out — see the root `Cargo.toml` REVIEW note).

To activate, one of the following is needed (outside this crate's file ownership):

- **Option A (recommended): patch `zenoh-link`.** Fork it like the other
  `*-patched` crates, add `LinkKind::Obfs`, route `OBFS_LOCATOR_PREFIX` in
  `LinkKind::try_from` / `new_supported_links`, and add
  `LinkKind::Obfs => Arc::new(LinkManagerUnicastObfs::new(_manager))` in
  `LinkManagerBuilderUnicast::make`. Add `zenoh-link = { path = ... }` to
  `[patch.crates-io]`.
- **Option B: reuse the `tls` slot.** Point
  `[patch.crates-io] zenoh-link-tls = { path = "./mac-companion/zenoh-link-obfs" }`
  and re-export the obfs manager under the names Zenoh expects
  (`LinkManagerUnicastTls`, `TlsConfigurator`, `TlsLocatorInspector`,
  `TLS_LOCATOR_PREFIX`). Endpoints then use `tls/...` but speak obfs on the wire.
  This conflicts with keeping the real TLS link, so it is not the default here.

## Endpoint usage (once wired)

- Server (`bsdos-core`): listen on `obfs/0.0.0.0:443`.
- Client (`metal-viewer`): connect to `obfs/VPS_IP:443`.

## Residual limitations (PLAN §5)

### Passive-DPI: handshake prefix

The handshake prefix is now encoded with **Elligator2** and is designed to be
computationally indistinguishable from uniform random. The passive-DPI
distinguisher on raw X25519 pubkeys has been closed.

**Remaining caveat:** `curve25519-elligator2` version `0.1.0-alpha.2` is a
pre-release fork pending upstream integration into `curve25519-dalek`. It has not
received an independent third-party security audit. If the upstream integration is
completed (and the fork is yanked), the dependency should be updated to the
canonical `curve25519-dalek` API. See `REVIEW` comments in `Cargo.toml`.

**Wire length:** each ephemeral message is now 33 bytes (32-byte representative +
1-byte tweak) rather than 32. The total handshake exchange is 66 bytes instead of
64. This is not a security concern but must be noted for any interop or protocol
documentation (PLAN §4.2 will need a note update).

### No server authentication (PSK-binding only)

The static server keypair is derived **deterministically from the shared PSK**
(`derive_from_psk`), so anyone holding the PSK — including every legitimate client —
can recompute the server's static secret. The static-DH term
`X25519(client_eph, server_static)` is therefore purely a **PSK-binding mix**; it
provides **no server authentication** and **no forward-secrecy contribution beyond
the ephemeral DH**. A man-in-the-middle who knows the PSK is fully transparent.
Security rests entirely on **PSK confidentiality + ephemeral DH**. If real server
authentication is later required, ship a genuine server static key out-of-band
instead of deriving it from the PSK.

### No explicit key confirmation

There is no separate handshake-finished / key-confirmation frame. The first proof
that both sides share the PSK is the AEAD tag of the **first data record**, so a
mismatched PSK or on-path tampering is detected only on the first decrypt failure,
not at handshake completion time. See `crypto.rs` for details.

### Active probing (PLAN §5)

MVP does **not** defend against **active probing**. A probe that opens `:443` and
sends a bogus client ephemeral simply fails the handshake and the connection is
dropped (`accept_task` logs at debug and returns). Phase 2 (Reality-style silent
splice to a real HTTPS backend on handshake failure) is future work.

## Tests

`cargo test -p zenoh-link-obfs` runs an in-memory (`tokio::io::duplex`)
handshake + round-trip test in `src/crypto.rs` (no network, no VM needed).
