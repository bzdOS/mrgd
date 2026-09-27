# Agent use cases — why the CS-API is (and isn't) overkill for agents

Companion to [`DESIGN.md`](DESIGN.md),
[`../ARCHITECTURE-boundaries.md`](../ARCHITECTURE-boundaries.md) and
[`../ROADMAP.md`](../ROADMAP.md) §"What this is for".

Agents are the bus's intended occupants, so "how much Matrix does an agent
need" is a product question here rather than a curiosity: it decides whether
the CS-API can stay a window for humans or has to grow an agent-shaped half.
The answer depends entirely on *what kind of agent* is talking, not on "agent
vs human" as a flat binary — which is why the three cases below reach three
different verdicts. Case 2 is the one the roadmap acts on (Phase 3 item 3, the
tenant gateway).

## Case 1 — one resident, trusted agent process

The simplest case: a single long-running agent process, one operator, always
running (or long-poll-connected via `/sync`). Here almost the entire CS-API is
dead weight:

- **device_lists / OTK / cross-signing** exist to sync E2EE keys across
  **multiple devices of one human user**. A single resident process is one
  device — nothing to sync.
- **Push** exists to wake a phone that went to sleep. A resident process that's
  already blocked on long-poll `/sync` doesn't need waking.
- **E2EE** protects against an untrusted server/other readers. If the agent and
  the server share an operator, there's no one to protect against.
- **Media, account_data, room tags** are UI conveniences for a human looking at
  a client. An agent reading structured `content` doesn't need them.

For this case the natural entry point isn't the CS-API at all — it is the
substrate itself, skipping HTTP and the Matrix protocol layer entirely. See
`ARCHITECTURE-boundaries.md`'s "does the log need it, or only an Element X
client?" test.

Two corrections to earlier versions of this paragraph, both load-bearing:

- It said `bsdOS/couplingd` already consumed `mrgd` this way. It never has.
  Nothing has ever linked this crate, and the historical dependency ran the
  other direction — `ARCHITECTURE-boundaries.md` §"History: one real consumer,
  not two".
- It named `mrgd`'s `RoomLog` — a Rust type — as the entry point. Wrong unit:
  the consumers here are Node.js, Python, Rust, FreeBSD, Linux and macOS. The
  entry point is the **wire** (Zenoh keyexpr + delta envelope), now written down
  as [`WIRE.md`](WIRE.md). The one instance of this case that exists today — the
  hubd queue bridge — is a module *inside* matrix-hs precisely because there was
  no documented wire to write it against; anything built after 2026-08-19 has no
  such excuse.

## Case 2 — an ephemeral worker pool behind one account

**First slice landed 2026-08-23: the application-service agent socket.**
`MATRIX_HS_AS_TOKEN` + `MATRIX_HS_AS_PREFIX` (off unless set) turn this case
from design into two calls — `POST /register` with the AS bearer creates the
tenant account (no UIA, unusable password, token returned), and
`POST /login type=m.login.application_service` mints a passwordless
per-device session for any worker (`device_id` = the worker). Namespace-
enforced both ways: the token can only touch its prefix, and nothing else is
granted by it. Tests: `src/as_socket_test.rs`. What is still future: key
backup as the handoff path and the two-pool demo (Case 3's integration test).

This is where the CS-API's device machinery becomes genuinely load-bearing
again, just not for the reason it was built (a human's phone + laptop). If one
logical identity (`@company-bot:example.com`) is backed by an **autoscaled pool
of stateless workers** — spinning up on demand, dying without notice — each
worker maps naturally onto a Matrix **device**:

- **device_lists** — a new worker booting is "a new device logged in"; other
  room participants get `device_lists.changed` and know to refresh keys for it.
  A worker dying needs no cleanup — the rest of the pool (other devices of the
  same account) keeps working.
- **Cross-signing** — the account's `self_signing_key` can vouch for every new
  worker-device automatically. New instances are trusted the moment they start,
  without per-instance manual verification — exactly the "trust new instances
  of my own service, not a stranger's" primitive a fleet needs.
- **Key backup (`/room_keys`)** — a worker that dies mid-task without handing
  off session keys doesn't strand them: the next worker restores from backup
  instead of needing a live device-to-device handoff from a process that no
  longer exists.
- **sendToDevice** — the actual mechanism for shipping Megolm session keys
  between workers of the same account (built in this repo's Ф1a).

## Case 3 — agents from a different company/operator in the same room

Here the identity boundary is a full account (and usually a different node),
not a device — this is the closest thing to real federation:

- **E2EE genuinely matters** — confidentiality between operators who don't
  share a trust root, not just "different devices of one trusted pool."
- **`node_auth`** (already built into `mrgd`) is the right primitive: each
  server signs its own PDUs, `sender`-domain is bound to the signing node, so
  one operator's node can't forge another's sender.

## Combining 2 + 3 — pools of pools

Operator A runs a worker pool (many devices, one account, cross-signing trust
inside the pool). Operator B runs their own pool. Both pools sit in the same
room. **Inside a pool**: device model, no E2EE needed (all workers trust their
own account). **Between pools**: E2EE + `node_auth` (a real trust boundary).
This is Matrix's original design point — "collaborate across organizations
without a shared trust root" — just with autoscaled agent fleets instead of
humans with phones.

**The reusable architectural idea:** an agent gateway (candidate for a future
Application-Service-style adapter, see `ARCHITECTURE-boundaries.md`) should
mint one MXID **per account/tenant, not per process**. Worker registration =
`keys/upload` (new device); worker death = it just stops syncing, no
deregistration needed. Under that model, the E2EE stack built in this repo
(Ф1a sendToDevice, Ф1b device_lists, Ф2 key backup + cross-signing) isn't a
human-only tax — it's the direct infrastructure for multi-tenant, autoscaled
agent fleets sharing rooms.

## What still doesn't need Matrix, regardless of swarm size

- **Media** — agents exchange structured data in `content`, not binaries for
  human viewing. Stays a client-only concern.
- **account_data / room tags** — better served by `mrgd`/`hubd` directly (the
  coordination substrate) than by Matrix's per-user client-settings store.

## What scales *up* in importance with swarm size (not down)

- **Push-as-wakeup** — relevant whenever workers are scaled-to-zero rather than
  always-resident; the push-gateway dispatch is a literal "wake this dormant
  worker" primitive, not just a phone convenience.
- **Redactions / moderation** — more workers means more chances one is
  compromised or buggy; the ability to redact/kick a specific device or
  identity matters more at scale, not less.
- **Membership (join/invite/kick/ban)** — the actual right analogy for "pool
  composition changes as workers come and go," not device_lists (which is
  about the identity/keys, not room presence).
