# mrgd — Design Notes

## Motivation

**The concrete need, first.** A fleet of machines and agents — Claude sessions
coordinating through hubd, plus devices and a few servers, spread over a public
host, two FreeBSD nodes, a NATed box and a laptop behind DPI — has to stay in
sync over whichever carrier is alive that hour, and re-converge afterwards for
whoever was down. That is the product;
[`../ROADMAP.md`](../ROADMAP.md) §"What this is for" is the normative statement
of it. Everything below is how that need becomes an architecture.

**The theory that makes it cheap.** Most distributed systems coordinate
unnecessarily. The CALM theorem (Consistency As Logical Monotonicity) proves
that any computation expressible as a monotone function over a join-semilattice
can be made eventually consistent without coordination.

`mrgd` pushes as much state as possible into CRDTs (coordination-free) and reserves
the single coordination point for genuinely non-monotone invariants (username
uniqueness, one-time key consumption).

### Why that buys transport independence

Coordination-freedom is usually sold on availability under partition. In this
system the payoff that matters more is **transport independence**.

Because events are content-addressed, idempotent and order-independent, the same
event can be delivered over several carriers at once and duplicates cost nothing.
There is no primary channel to fail over from, no ordering to preserve end-to-end,
no split-brain to reconcile. Whatever subset of carriers happens to be reachable
delivers convergence.

That property is what lets the deployment run a *ladder* of transports — LAN
gossip, Zenoh over an ssh tunnel, an obfuscated link, HTTPS — instead of betting
on one. It is also the reason Matrix federation cannot sit underneath this:
federation is HTTPS-only by spec and its state resolution is order-sensitive, so
it cannot survive the carrier diversity the deployment requires. See
[`../ARCHITECTURE-boundaries.md`](../ARCHITECTURE-boundaries.md) §"East–west vs
north–south" for the normative form of this rule.

## Module map (`substrate` module)

| Module | Role |
|---|---|
| `crdt` | Five CvRDT types: GCounter, PnCounter, OrSet, LwwRegister, MvRegister. CrdtSink trait (MemCrdtSink + ZenohCrdtSink behind `cluster` feature). |
| `barrier` | CALM barrier: `claim()` + `reconcile()`. MemClaimStore for tests. |
| `barrier_growset` | Coordination-free grow-set variant of the barrier backed by OrSet over Zenoh. |
| `reconcile` | Re-export shim for `barrier::reconcile`. |
| `observ` | FNV-1a-64 content hashing (`content_id`). Structured emit gated by `MRGD_OBSERV`. |
| `matrix_events` | Matrix PDU (event), RoomLog (grow-only set + Kahn BFS topological sort), delta serialisation. |

## Barrier design (CALM theorem applied)

The username claim (`POST /register`) is the only non-monotone operation:
"at most one user owns a given username" is not expressible as a CRDT.

`claim()` routes through a `ClaimStore`:
- Coordinator reachable → CP: compare-and-set → `Claimed` or `Rejected{owner}`.
- Coordinator unreachable, `Policy::Optimistic` → AP: `Provisional{fence}` (recorded locally).
- Coordinator unreachable, `Policy::Strict` → `Err(Unavailable)`.

On partition heal, `reconcile(&[ProvisionalClaim])` deterministically selects the
winner: `min_by(ts, then node_id)`. Pure, order-independent, idempotent — every node
arrives at the same result without further coordination.

## Transport (`cluster` feature)

Zenoh in peer mode (no broker). CRDT updates flow as:
- key: `<prefix>/<key>` (configurable)
- value: CRDT delta serialised as JSON

The `ZenohCrdtSink` in `crdt.rs` is the only Zenoh-aware type. Everything else is
pure Rust with no network dependency.

### Carrier selection is configuration, not code

`ZenohCrdtSink` is deliberately carrier-blind. The transport ladder (LAN peer →
Zenoh over `ssh -L` → obfs link) is **the same code with different Zenoh
locators**, selected via `MATRIX_HS_ZENOH_CONNECT` / `MATRIX_HS_ZENOH_LISTEN`.
Plural transports therefore cost no abstraction.

A second `CrdtSink` implementation alongside `ZenohCrdtSink` is where a real
abstraction tax would begin, and is only justified if Zenoh *itself* becomes the
thing being blocked — not merely one of its carriers. Until then, resist it.

Two standing rules, both normative in `ARCHITECTURE-boundaries.md`:
- Never route east–west replication over the CS-API. That places the most
  critical function on the least survivable transport.
- Plaintext Zenoh on a public interface is a bug, not a ladder rung. Cross-node
  links ride ssh, TLS, or the obfs link.

## Matrix homeserver (the `matrix-hs` binary)

The window, not the product ([`../ROADMAP.md`](../ROADMAP.md) §"What this is
for"): a real Matrix client is how a *human* reads rooms whose other occupants
are agents and devices. It is a thin axum 0.8 HTTP layer over `substrate`, in
the same crate. Implements:
- CS-API v3 (and legacy r0 alias) endpoints
- In-memory `RoomLog` (CRDT event store)
- Registration with CALM barrier (username uniqueness)
- E2EE key endpoints (keys/upload, keys/query, keys/claim)
- Persist/replay from disk (compact room log to JSON files)
- Multi-master CRDT delta catchup (cluster feature)

No state machine outside `AppState`. All mutation is append-only to the RoomLog.

Sliding sync (`routes/sliding_sync.rs`, MSC4186) is a read-only projection of the
same `RoomLog`/`stream_pos` model as classic `/sync` — it adds no state and no
coordination path of its own.

## FreeBSD builds (`cluster` feature)

`zenoh-util` 1.9.0 only implements `set_bind_to_device_{tcp,udp}_socket()` for
linux/android and macos/ios/windows — FreeBSD is left ungated, so
`zenoh-link-commons` fails to compile there (`E0425`).

**Corrected 2026-08-19:** earlier versions of this section said bsdOS "already
carries a fix" in an earlier private copy. It did not. That vendored
crate held the fix as an *unapplied* `freebsd-upstream.patch` sitting next to a
source tree that still lacked the two stubs — and it additionally gated the
`tokio::net::{TcpSocket, UdpSocket}` import behind
`#[cfg(not(target_os = "freebsd"))]`, so simply pasting the patch in produced a
second error rather than a build. Both are fixed in place now, and a real
FreeBSD `--features cluster` binary was built on beta and put into a live mesh
(the deployment topology record (kept private) §3b). If you are the first to build there after a re-vendor,
check those two things before anything else.

To build `--features cluster` on FreeBSD, the stubs are vendored at
`vendor/zenoh-util-freebsd` — add to this crate's root `Cargo.toml` (left
commented in the shipped file because the patch is global and would also
divert Linux builds onto the fork):

```toml
[patch.crates-io]
zenoh-util = { path = "vendor/zenoh-util-freebsd" }
```

Also drop `rust-toolchain.toml` on the FreeBSD copy if the box has a plain
rustc rather than rustup: the pin asks for 1.96.0 and cargo will refuse a
1.96.1-only host rather than fall back.

See `vendor/zenoh-util-freebsd/freebsd-upstream.patch` (and its README) for the full patch
rationale. Without this, `cargo build --release --features cluster` only
works on Linux/macOS.

## Live deployment

> **Operational state lives in the deployment topology record (kept private)**, which is
> re-measured and date-stamped. Do not duplicate node/transport tables here —
> they rot, and two copies rot differently.

Historical note, kept because older docs and configs still assume it: the
2026-07-11 design was a three-node mesh with **plaintext Zenoh on TCP 7448**
between beta, delta and the gamma edge node. As measured on 2026-08-03 that mesh
no longer exists — 7448 is filtered on both FreeBSD nodes while ssh stays open,
which is what motivated the transport ladder above. Any config still naming
`tcp/…:7448` is pointing at a dead carrier.

What remains accurate from that era:

`m.hubd.net` (Caddy on delta) reverse-proxies to `127.0.0.1:8448` and returns
403 for `/register*` (public registration stays disabled; the homeserver is
for the existing cluster only). MSC4186 sliding sync
(`org.matrix.simplified_msc3575` / `/_matrix/client/v1/sync`) is reachable
through this same public path as of the internal-task rollout — verify with:

```bash
curl -s https://m.hubd.net/_matrix/client/versions | grep simplified_msc3575
```

Rc.d service on FreeBSD nodes: `bsdos_matrix` (see
the bsdOS workspace rc.d scripts for per-node `sysrc` config and the
build/install steps). Binaries are staged through the existing
`artefacts/myvm-bin/` 9p-shared path, same as the rest of bsdOS.
