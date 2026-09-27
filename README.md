# mrgd

A coordination bus for machines and agents, with a Matrix homeserver as its
human-facing window. One Rust crate.

**Status: v1.0 (2026-08-21).** Deployed as a three-node mesh across Linux and
FreeBSD, with live delivery verified in all four directions, and its first
non-chat consumer (an agent-queue bridge) riding the bus. The measured,
date-stamped deployment records (topology, scope separation) are kept
private; this repo is the code, the protocol, and the reasoning.

## What it is for

**The bus is the product.** Durable, partition-tolerant state and message
exchange between nodes over whichever carrier happens to be alive — LAN gossip,
Zenoh over an ssh tunnel, an obfuscated link — converging afterwards for the
nodes that were down. Its occupants are agents (Claude sessions coordinating
through hubd) and devices.

**The homeserver is the window.** It is how a *human* looks into those same
rooms with a real Matrix client (Element X, FluffyChat). A genuine requirement,
not a demo — but the viewport, not the reason the bus exists.

Why not something off the shelf: Matrix federation is HTTPS-only by
specification and its state resolution is order-sensitive, so it cannot survive
that carrier diversity; brokers assume a reachable central node. The full
argument, and the test used to keep product apart from tax, is in
[`ROADMAP.md`](ROADMAP.md) §"What this is for".

## Layout

Single crate, package `mrgd`:

| Target | Type | Description |
|---|---|---|
| `mrgd` (lib) | lib | `src/substrate/` — CvRDT/Delta-CRDT types, CALM barrier, Matrix event-DAG. No axum dependency; kept as a module boundary (not a crate boundary) for engineering hygiene, since `matrix-hs` is currently its only consumer — see [`ARCHITECTURE-boundaries.md`](ARCHITECTURE-boundaries.md). |
| `matrix-hs` (bin) | bin | Matrix CS-API homeserver (axum 0.8) — everything under `src/` outside `substrate/`. |

## Build

```bash
cargo build                          # no-network, in-process only
cargo build --features cluster       # enables Zenoh peer transport
```

## Test

```bash
cargo test                            # unit + integration (no network)
cargo test --features cluster         # + cluster-gated tests
```

## Features

- `cluster` (off by default): enables Zenoh peer-mode transport for CRDT propagation.
  Requires `zenoh` and `tokio` — not needed for single-node or test builds.

## Design

| Doc | What it answers |
|---|---|
| [`ARCHITECTURE-boundaries.md`](ARCHITECTURE-boundaries.md) | **normative.** What may live in `substrate`; east–west vs north–south; the transport ladder; one replication semantic, many transports. |
| [`docs/DESIGN.md`](docs/DESIGN.md) | how it works — CALM/CRDT rationale, barrier, transport, module map. |
| [`ROADMAP.md`](ROADMAP.md) | **what this is for**, what to build next, and the "Conduit test" for deciding what is product vs tax. |
| [`docs/WIRE.md`](docs/WIRE.md) | **the east–west protocol.** What a non-Rust process must speak to join the mesh, with a test vector. |
| [`docs/AGENT-USE-CASES.md`](docs/AGENT-USE-CASES.md) | which parts of the CS-API agents actually need, by agent topology. |
| [`docs/SETUP.md`](docs/SETUP.md) | building and running it on a new machine. |

Deployment records (live topology, scope separation, migration runbooks) are
deliberately not part of the public repo: they describe a running deployment,
and a current map of one is worth more to an attacker than to a reader. No key,
token or password value is in them either way. Everything needed to build, run
and join a mesh is here — [`docs/WIRE.md`](docs/WIRE.md) in particular.

The bus's first non-chat consumer is hubd's agent queues, carried as room
traffic. That queue format is hubd's, not ours:
[hubd `docs/interop.md` → Transport](https://github.com/bzdOS/hubd/blob/main/docs/interop.md#transport-how-a-queue-crosses-machines)
documents it, including the fact that two transports may carry the same queue
directory concurrently.

The one-paragraph version: events are content-addressed, idempotent and
order-independent, so the same event may arrive over any number of carriers with
duplicates costing nothing. That buys **transport independence** — node↔node
replication runs over whichever carrier survives (LAN gossip → Zenoh over ssh →
obfuscated link), while the Matrix CS-API stays a client-facing surface only. It
is also why Matrix federation cannot sit underneath this: it is HTTPS-only by
spec and order-sensitive, so it cannot survive that diversity.
