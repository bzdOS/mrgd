# Architecture boundaries — `substrate` (module) vs the rest of `mrgd` (edge)

Status: normative. This document defines where the Matrix protocol is allowed to
touch the system, and where it must not. Read it before adding any
Matrix-shaped feature. Companion to [`docs/DESIGN.md`](docs/DESIGN.md).

**2026-08 update:** `mrgd` and `matrix-hs` used to be two crates in one
workspace. They are now one crate (package `mrgd`, binary `matrix-hs`),
because the "two consumers" story below turned out to be aspirational, not
code-sharing: `bsdOS/couplingd` never depended on the `mrgd` crate — it has its
own fork of the same modules (see "History" below). Carrying a
real crate boundary for a consumer that wasn't actually linking the crate was
paying an abstraction tax for nothing. The boundary this document describes is
now a **module** boundary (`src/substrate/` vs the rest of `src/`), enforced by
convention and code review, not by `cargo`. Re-extract it into its own crate
the day a second consumer actually adds `mrgd = { path/git = ... }` to its own
`Cargo.toml` — not before.

## One-line rule

**`substrate` (this crate's `src/substrate/` module) is a coordination-free
replication core. Everything else in this crate is a Matrix protocol adapter
that sits on top of it. The Matrix *protocol* (CS-API, E2EE, sync tokens,
media, push) lives only outside `substrate`. `substrate` never imports axum
and never will.**

This boundary is not hygiene for its own sake — it is the product line.
[`ROADMAP.md`](ROADMAP.md) §"What this is for" states the same split in product
terms: **the substrate is the bus, and the bus is the product; everything
outside it is the window humans look through.** The rule above is that sentence
expressed as a constraint on imports. So when a change would blur it, the
question is never "is this tidy" but "does this make the window load-bearing for
convergence" — and the answer has to stay no.

## East–west vs north–south

The single most load-bearing distinction in this system. Two directions of
traffic, two different transports, two different survivability requirements:

| | **east–west** (node ↔ node) | **north–south** (client ↔ node) |
|---|---|---|
| Carries | substrate CRDT deltas | Matrix CS-API |
| Over | Zenoh, by whatever carrier is alive | HTTPS |
| Serves | convergence | humans and UI (Element, FluffyChat, the hubd report bridge) |
| If it dies | nodes stop converging | **you lose the picture, not the convergence** |

Convergence must never be made to depend on the north–south path. The CS-API is
the least survivable link in the system (see the ladder below) and the most
visible one; those two facts together make it a tempting and wrong place to put
replication.

### The transport ladder

Ordered by measured survivability, most durable first. See
the deployment topology record (kept private) for the measurements.

| | Carrier | Status | Dies when |
|---|---|---|---|
| 1 | Zenoh peer on LAN | works, zero-config | you cross a NAT |
| 2 | **Zenoh over `ssh -L` (autossh)** | works — the queue path already uses it | the ISP filters ssh (rare; already observed on some paths) |
| 3 | Zenoh over obfs link (AEAD) | **wired and verified 2026-08-20** — ChaCha20-Poly1305 over raw TCP, X25519+PSK, no TLS ClientHello so no JA3/JA4. Two nodes converge over an `obfs/` locator; a wrong PSK gets nothing. Enabled by `[patch.crates-io]` in `Cargo.toml`; see the scope-separation record (kept private) §5 | ТСПУ-grade DPI defeats the obfuscation |
| 4 | HTTPS / CS-API | works | **first** — SNI/domain blocking, protocol fingerprinting |

Rung 2 is the default east–west carrier. Rung 4 is north–south only.

### One semantic, many transports

> **The mess is not the number of transports. The mess is the number of
> replication semantics. Keep transports plural; keep the semantic singular.**

Transport diversity is the resilience property this system is built for, and
**the CRDT is what makes it safe**: events are content-addressed, idempotent and
order-independent, so the same event may arrive over ssh *and* obfs *and* HTTPS
with duplicates costing nothing. No failover logic, no "primary channel", no
split-brain — whatever subset of carriers is up delivers convergence.

This is the real justification for putting the substrate at the bottom, and it
is stronger than "multi-master": **Matrix federation cannot be the bottom layer,
because it is HTTPS-only by specification and its state resolution is
order-sensitive.** It cannot survive transport diversity. The substrate can.

Where the mess actually starts: rungs 1–3 are *the same code* with different
Zenoh locators — plural transports at zero abstraction cost. Writing a second
`CrdtSink` alongside `ZenohCrdtSink` is where a real abstraction tax begins, and
that is only justified when Zenoh *itself* is what is being blocked. Not before.

Corollaries:
- Do **not** move queue/journal replication onto the CS-API. It would put the
  system's most critical function on its least survivable transport.
- Do **not** let a second component grow its own dedup scheme. Content-addressed
  `event_id` already solves it; a hand-rolled hash set is a worse version with
  the same replay gap (`hubd-queue-repl` is the existing instance of this —
  see the deployment topology record (kept private) §4). The queue bridge in `src/hubd_bridge.rs` is the
  answer to that instance: it carries the same queue blocks with no suppress set
  at all, and retiring the daemon removes the scheme rather than adding a third.
  See the hubd-bridge record (kept private).
- Plaintext Zenoh on a public interface is not a rung on the ladder. It is a
  bug.
- **A mesh boundary is a data boundary.** A node converges on everything its
  mesh carries, so which mesh a process joins decides which data it holds. That
  makes scope a deployment-time decision with no smaller unit than the process —
  normative rules and the five interlocks that enforce them are in
  the scope-separation record (kept private). Corollary worth stating separately: a host
  can carry a scope's traffic without being in it. Rung 3 above is what makes
  that real — AEAD keyed at the endpoints means a relay forwards bytes it cannot
  read, so **rendezvous and participant are different roles** and only the
  second one holds data.

## Why: how much of Matrix actually fits

Matrix is not adopted wholesale. It fits at some layers and is deliberately
rejected at others. Be precise about which:

| Layer | Matrix's design | This product | Verdict |
|-------|-----------------|--------------|---------|
| **Event model** | causal DAG of PDUs (`prev_events`, `depth`) | `matrix_events::RoomLog` as a CvRDT | **adopted** — lives in `substrate` |
| **Event authentication** | each home server signs its own PDUs; `sender` must be in the signing server's domain | `node_auth`: per-node ed25519 sign-on-publish + sender-domain binding | **adopted** (same trust model) — lives in `substrate` |
| **Consistency / conflict resolution** | server-authoritative auth-chain **state resolution** (power-levels decide who may set state) | **CRDT LWW** by `(origin_server_ts, event_id)` — coordination-free, no consensus | **rejected & replaced** — replacement lives outside `substrate` |
| **Transport** | federation HTTP (`/_matrix/federation`, `/_matrix/key/v2`) | Zenoh delta pub/sub over a ladder of carriers | **replaced** — Zenoh in `substrate`. Not a preference: federation is HTTPS-only and order-sensitive, so it cannot survive the transport diversity this system requires. |
| **Key bootstrap** | key-server + notary/perspectives | TOFU grow-set (`NodeKeyStore`), or a pinned closed set via `MATRIX_HS_NODE_KEYS` | **replaced** (simpler) — in `substrate`. TOFU when unpinned; the authenticated anchor once listed here as a gap landed 2026-08-04 |
| **Client protocol surface** | CS-API: register/login/sync/sliding-sync, E2EE key mgmt, media, push, receipts… | same endpoints, served for real Matrix clients | **client tax** — lives **only** outside `substrate` |

The through-line: the substrate borrows Matrix's *event model* and *event-auth
trust model* (both genuinely good coordination-free primitives). It does **not**
borrow Matrix's *consistency algorithm*, *transport*, or *client protocol* —
those are either replaced with coordination-free equivalents or confined to the
edge adapter.

### On `node_auth` specifically

`node_auth` is **not** a bespoke replacement for Matrix federation trust — it *is*
Matrix's federation trust model (per-server signing + `domain(sender) ==
signer_node`), ported onto Zenoh with a TOFU key store instead of a notary key
server. It therefore belongs **inside `substrate`**: it authenticates substrate
events, and its semantics are Matrix-federation-compatible by construction (which
also makes a future bridge to real Matrix federation a transport/bootstrap
problem, not a trust-model problem).

## The two areas of one crate

- **`src/substrate/`** — the replication core. Modules: `crdt` (CRDT primitives +
  `ZenohCrdtSink`), `barrier` / `barrier_growset` / `reconcile` (coordination-free
  active-passive fencing), `observ`, `matrix_events` (Pdu + RoomLog CvRDT),
  `node_auth` (federation-style event signing). **Has no `axum`/HTTP dependency
  and must never gain one** — this is enforced by review, since it is a module,
  not a separate crate that `cargo` could gate.
- **everything else under `src/`** — the Matrix homeserver: the axum HTTP layer,
  `AppState`, persistence, and every CS-API concern (auth tokens, sync, sliding
  sync, to-device, device lists, ephemeral, media, key backup, cross-signing,
  push). The CRDT-LWW room-state replication (`apply_remote_state_event`,
  `publish_state_event`, `drain_cluster_state`) — the *replacement* for Matrix
  state resolution — lives here, outside `substrate`, because it is a
  protocol-level decision, not a substrate primitive.

## History: one real consumer, not two

This document used to claim `mrgd` was consumed by two independent crates —
`matrix-hs` and `bsdOS/couplingd` — as proof the substrate was genuinely
protocol-independent. That claim did not survive a `Cargo.toml` check, and the
correction runs in the opposite direction from what was written:

- `couplingd` has **no** dependency on `mrgd`, and never had one.
- The real historical dependency was the reverse: `bsdOS/matrix-hs` depended on
  `couplingd` as a path dep. `mrgd` was created by copying the substrate modules
  *out of* `couplingd`.
- That `bsdOS/matrix-hs` is now a dead fork (last touched 2026-07-07) still
  listed in the bsdOS workspace `members`.

So today there is exactly **one** real consumer of `substrate`: the `matrix-hs`
binary in this same crate. The module boundary below is kept for engineering
hygiene, not because a second consumer is literally linking this code.

### Why the fork stays forked

The earlier text also claimed the two copies had "~1000+ lines diverged". That
was wrong — it counted test code and rustfmt differences. Measured properly
(2026-08-03, tests stripped, formatting normalised) the production cores are
**near-identical**: `crdt` and `barrier_growset` differ only cosmetically,
`barrier` differs by one store type couplingd needs and we don't, and our
`matrix_events` is a strict superset (it adds node_auth signing). See
the deployment topology record (kept private) §4 for the numbers.

This matters because "the fork is expensive to keep in sync" was the main
argument for merging the two, and it does not hold. The decision is therefore
**keep the fork, make it explicit**:

- `couplingd` is the **boot/OS path** — jails, locks, fencing, watchdog. It runs
  before the network exists. `matrix-hs` is the **application path**. A Cargo
  dependency would bind a FreeBSD jail supervisor's lifecycle to an axum
  homeserver's, for ~1000 lines of primitives that have not changed in a month.
  Different failure domains are a legitimate reason to duplicate.
- The sync mechanism should be a formatting-normalising diff check, not a crate
  boundary. Cheap precisely because there is no real drift to reconcile.

Re-extract `substrate` into its own crate on the day something actually adds
`mrgd = { path/git = … }` to its own `Cargo.toml` — re-extraction is cheap;
carrying a speculative crate boundary for years before that happens is not.

### What reuse actually looks like here

Twice, someone needed exactly what the substrate provides and declined to link
it — correctly, both times:

- `hubd-queue-repl` needed replicated append. It wrote 355 lines of Zenoh +
  sha256 rather than use `couplingd::crdt`.
- The hubd→Matrix bridge needed event delivery. It shipped 171 lines of Python
  over the CS-API rather than use the 1504 lines of already-written, already-
  tested `couplingd::hub_bridge`.

Linking a Rust crate demands one language, one build, one toolchain, one
lifecycle. This system is Node.js, Python, Rust, FreeBSD, Linux and macOS. A
crate cannot be the seam across that; a wire protocol can.

> **The unit of reuse in this system is the wire, not the crate.** The system's
> two reusable surfaces are the CS-API (north–south, for clients) and the Zenoh
> keyexpr convention (east–west, for peers) — not `pub` items.

Earlier wording called the CS-API "`substrate`'s reusable surface", which
contradicts the one-line rule above: the CS-API lives *outside* `substrate` and
`substrate` has no HTTP surface at all. The CS-API is the window; the keyexpr
convention is what an east–west consumer actually reuses.

That east–west convention was implicit in the Rust until 2026-08-19, which made
it reusable in principle and by nobody in practice. It is now written down as
[`docs/WIRE.md`](docs/WIRE.md) — normative for interoperability, with a
published test vector and a conformance script that decodes it from the document
alone. A second implementation is now a reading job rather than an archaeology
job, which is the whole content of the claim above.

The queue bridge (`src/hubd_bridge.rs`) is the third data point and it argues the
same way. It gives hubd's queues everything the substrate has — signing, catch-up,
GC — and hubd links nothing, learns nothing, changes nothing. The seam is hubd's
own on-disk block format, `\n## <ts> · from <sender>\n<body>\n`, which is a wire
that happens to be a file. Its append-only, one-writer-per-file discipline is what
made the seam usable at all; had `hub_queue_wait` not tracked byte offsets, the
bridge would have had to reach into hubd instead.

That is also why `hub_bridge.rs` should be considered superseded rather than
revived, and why no third component should grow its own replication logic.

## The rule (and the test that enforces it)

> **A Matrix protocol concern lives outside `substrate` and never leaks into it.**

Before adding anything Matrix-shaped, apply the test:

**"Does the coordination *log* need this, or does only an Element X *client* need
it?"**

- **Log needs it** (it's about replicating/merging/authenticating events) → it may
  live in `substrate`. Examples already there: the DAG model, CRDT merge, node signing.
- **Only a client needs it** (sync tokens, E2EE key management, media, push,
  receipts, the CS-API shape) → it lives outside `substrate`, full stop.

Corollary — things to actively resist:
- Do **not** push Matrix auth-chain **state resolution** into `substrate`; its
  convergence is CRDT/LWW by design (AP, not CP).
- Do **not** add E2EE to agent↔agent or OS coordination paths; E2EE is a
  human-client trust concern and stays outside `substrate`.
- Do **not** assume "one home server owns a resource." The substrate is
  multi-master; per-resource ownership is a Matrix protocol assumption that must
  not constrain substrate design.

## Known seams (deliberate, documented, edge-only)

Conscious limitations of the code outside `substrate`, not substrate gaps.

Four entries stood here through mid-2026 and are now **closed** — removed rather
than left to mislead a reader into re-solving them: state catch-up for
offline/late-joining nodes (wildcard catch-up plus mid-life re-query), cross-node
media fetch, UIA on cross-signing, and the authenticated key anchor. All landed
2026-08-04; [`ROADMAP.md`](ROADMAP.md) Phases 1–2 carry the detail.

What is genuinely still open:

- **No rung of the transport ladder is currently wired for matrix-hs
  east–west.** Rung 2 (Zenoh over `ssh -L`) is proven in this deployment, but by
  the hubd queue path — matrix-hs does not use it, and its deployed
  `MATRIX_HS_ZENOH_CONNECT` still names the plaintext `:7448` ports measured
  filtered on 2026-08-03 (the deployment topology record (kept private) §2). Rungs 1 and 3 exist as
  capabilities (Zenoh peer mode; `zenoh-link-obfs` in the bsdOS workspace) with
  nothing selecting between them automatically. Configuration and deployment
  debt, not a substrate gap — the substrate is already carrier-blind. It is also
  the largest single thing standing between here and v1.0.
- **Cluster inboxes are drained by sync-style CS-API requests only.** Zenoh
  delivers a delta into a per-room inbox that `drain_all_cluster` empties, and
  normally the only caller is `/sync`. Invisible on a chat server — some client
  is always long-polling — and fatal on a node whose occupants are agents, which
  is why `hubd_bridge` drains for itself. Any further client-less consumer needs
  the same, or the drain has to become unconditional. This is the seam most
  likely to bite the purpose in `ROADMAP.md` §"What this is for", because
  agent-only nodes are precisely what that purpose calls for.
- **Per-user E2EE stores are node-local.** device_keys and cross-signing do not
  replicate; timeline, room state, media and one-time-key claims do. A device is
  therefore reachable cluster-wide for the OTK it hands out, but not for its key
  material.
- **Minimal push rules** — notify on `m.room.message` only.

None of these are reasons to move logic across the boundary.
