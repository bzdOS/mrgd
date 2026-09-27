# Roadmap — the path to v1.0

Status: normative intent, not a schedule. The schedule is
the v1.0 plan record (kept private) — step-by-step execution of the items below
that are still open, including the dependency between them that this file does
not state. Companion to
[`ARCHITECTURE-boundaries.md`](ARCHITECTURE-boundaries.md),
[`docs/DESIGN.md`](docs/DESIGN.md),
the deployment topology record (kept private), and
[`docs/AGENT-USE-CASES.md`](docs/AGENT-USE-CASES.md).

## What this is for

Fixed 2026-08-19. The question "what is this for, other than a personal Matrix
server?" was put directly, and the honest first answer was "not much" — which is
a symptom, not a fact about the code. Nothing below is new work; it names what
this repo has in fact been building, so that it can be prioritised against
instead of drifted through.

**Primary: a coordination bus for this operator's own machines and agents.** The
real fleet is Claude sessions coordinating through hubd, plus devices like the
EchoEar-2ST, spread over Alpha, two FreeBSD nodes, a NATed edge box and a laptop
behind DPI. What they need is durable, partition-tolerant message and state
exchange over whichever carrier is alive that hour, converging afterwards for the
nodes that were down. Nothing stock provides that: Matrix federation dies on the
first filtered HTTPS path *by specification*, and every broker worth the name
assumes a reachable central node. This is the product. It already has one real
consumer written — the hubd queue bridge — and zero deployed.

**The Matrix homeserver is the window, not the product.** Element X is how a
human looks into rooms whose other occupants are agents and devices. That is a
real requirement and it stays: the owner wants a personal homeserver, free of
anyone else's infrastructure, with its own conveniences on top. But it is the
viewport onto the bus, not the reason the bus exists. Read every "tax" verdict
below in that light — the CS-API is not overhead to be resented, it is the thing
you look through, and it has to keep working. It is simply not where the
differentiator lives.

**Secondary, and for readers who are not this operator: communication that
survives hostile networks.** A small cell — a family, a team — kept in sync
across filtered links where federation cannot go by construction. This needs no
separate work; it is the transport ladder plus Phases 1–2, described for a
different audience. It is written down because it is the story an outsider can
act on, and therefore the shape any eventual public release should take. It
ranks second on purpose: no such user exists today, and building for a
hypothetical one is how the primary goal gets starved.

## The Conduit test — how to prioritise anything here

bsdOS originally planned to run **Conduit** as its homeserver
(`bsdOS/DESIGN-matrix-homeserver.md`, `CONDUIT-DEPLOYMENT-STATUS.md`).
`matrix-hs` exists for exactly one reason: Conduit cannot do coordination-free
multi-master. That gives a sharp test for every item below:

> **Would running stock Conduit have solved this?**
> If yes, it is *tax* — implement the minimum that stops real clients breaking.
> If no, it is *the product* — that is where effort belongs.

By this test, Phase 1 and Phase 2 are product. Media, push, sliding-sync and UIA
as a client login flow are tax: keep them alive, do not invest in them. (Phase
2's "UIA on cross-signing" is not a counter-example. What is product there is
*requiring* a password before a key upload that crosses the trust boundary — not
the UIA machinery it reuses to ask for one.) "Alive" is the operative word —
they are the glass in the window, and a cracked window is a real defect even
though polishing it is not progress. The test also disqualifies the tempting
shortcut of putting replication on the CS-API — that would hand the
differentiator back to Conduit.

### The same test, applied to purposes

It filters *goals* as sharply as it filters features, which is what makes the
section above load-bearing rather than decorative:

| Candidate purpose | What already does it | Verdict |
|---|---|---|
| "a personal Matrix server" | Conduit, Synapse, Dendrite | **fails** — and this is precisely why the project can feel like a suitcase with no handle |
| "sync files between my nodes" | Syncthing | **fails** |
| "run the house" | MQTT + Home Assistant | **fails** |
| **"a bus my agents and devices coordinate over, on carriers federation cannot use"** | nothing | **passes** — the only candidate that does |

A goal a stock tool already serves will pull effort back into tax no matter how
carefully the phases below are worded. Note that the winning row is not a
rejection of the personal homeserver: it is what the homeserver is a window
onto.

## Where we actually are: v1.0 (tagged 2026-08-21)

The honest read as of 2026-08-19 was 0.x — foundation real, code healthy, but
not yet what its own thesis claimed. That gap is closed as of 2026-08-21: the
four reasons below are kept because each names what had to become true, with
its resolution.

Four specific reasons this was 0.x and not 1.0 (each resolved — see the v1.0
verdict above):

1. **The multi-master claim still has an asterisk, a smaller one.** Cross-node
   replication holds for the message timeline and (since `f349f26`) room state.
   A node that missed events now re-converges — on startup, and while running
   after a partition heals (both since 2026-08-04, Phase 1 below): it pulls every
   room every peer has, including rooms it never knew existed. Media is readable
   from any node too (pulled on demand), and (since 2026-08-18) so is a one-time
   key claim for a device owned by a different node. What remains node-local:
   device_keys and cross-signing stores.

2. **The security posture is no longer single-operator-only** (Phase 2, complete
   2026-08-04). Tokens carry a revocable epoch, node keys can be pinned to a
   closed set instead of TOFU, cross-node state writes are checked against the
   room's power_levels before LWW sees them, cross-signing needs a password, and
   ordering runs on a bounded hybrid clock rather than on whoever's wall clock is
   fastest. What is left is not a hole so much as a limit: this is a check against
   current state, not Matrix state resolution v2, and it does not enforce
   join_rules or per-transition membership rules.

3. **The differentiator has its first consumer — live on two nodes since
   2026-08-20.** The project's value (per §"What this is for") is the bus.
   Since 2026-08-05 something other than a chat client rides it: the hubd queue
   bridge (`src/hubd_bridge.rs`,
   the hubd-bridge record (kept private)), and since 2026-08-20 it runs
   on Alpha and beta — real hubd traffic ingested into rooms, materialised
   byte-identical across the wire both ways. The asterisk that remains:
   `hubd-queue-repl` still runs as gamma's feed (scope-blocked, see Phase 3
   item 2), and beta materialises passively because no hubd runs there. The
   wire convention that a second consumer would be written against is no
   longer missing ([`docs/WIRE.md`](docs/WIRE.md), 2026-08-19); the ergonomic
   agent socket — one identity per tenant, workers as devices — still is, and
   stays past the line.

   Correction to earlier versions of this file: they claimed agents already use
   the substrate directly "as `bsdOS/couplingd` proves". They do not, and it
   doesn't. Nothing has ever linked this crate. Twice, a component that needed
   exactly this functionality chose to re-implement rather than depend on it —
   which is evidence the reusable surface is the *wire*, not the crate. See
   `ARCHITECTURE-boundaries.md` §"What reuse actually looks like here". The
   agent socket is therefore specified as a protocol surface rather than a `pub`
   API — the surface itself being Phase 3 item 1 (write the wire down, before
   the v1.0 line), and the socket built on top of it item 3, after.

4. **The deployment is real, and durable since 2026-08-20.** "Live-deployed
   **multi-master**" has been a true statement since 2026-08-19: Alpha (Linux)
   and beta (FreeBSD) converge over Zenoh on an ssh tunnel, and the Phase 1 gate
   passes between them. The same evening's follow-up made it survive reboots on
   both ends — `mesh-tunnel-beta.service` on Alpha, `rc.d mrgd_node` on beta —
   so the former caveat ("a reboot on either side ends the mesh") is withdrawn.
   Separately, the homeserver the owner actually uses daily remains the
   **gamma** node (the deployment topology record (kept private) §3a) and is deliberately *not* in the
   mesh: it is a single non-cluster build carrying real household traffic, and
   joining it is the owner's call, not a deployment step. Measurements: the deployment topology record (kept private) §3b and §3c.

### Definition of v1.0

**Phases 1 and 2, plus one real consumer live on the bus.**

Moved deliberately on 2026-08-19 — by exactly one item — when §"What this is
for" was written down. Earlier versions of this line stopped at "Phase 1 +
Phase 2 complete" and justified that by calling `matrix-hs` a working bridge for
"humans (Element X) and agents (raw `mrgd`, already proven)". The parenthetical
was the same false claim corrected in point 3 above, and deleting it is most of
why the line had to move: with it gone, the old definition describes a 1.0 whose
only live consumer is a chat client. That is a personal Matrix server with an
unusual backend — the exact thing the Conduit test rejects.

One deployed non-chat consumer is the smallest honest proof that a bus is a bus,
and it costs no new code: the bridge is written and tested, it needs a config
flag on real hosts. So v1.0 is now four things, and **none of them is a coding
task**:

1. the Phase 1 carrier work and its live gate (power a node off for an hour) —
   deployment, and now the largest item left;
2. the Phase 2 gate — it already passes on paper;
3. ~~Phase 3 item 1, write the wire down~~ — **done 2026-08-19**
   ([`docs/WIRE.md`](docs/WIRE.md));
4. ~~Phase 3 item 2, turn the bridge on where it replaces `hubd-queue-repl`~~ —
   **live on Alpha and beta since 2026-08-20**; the residual `hubd-queue-repl`
   is gamma's feed and is scope-blocked, not work-blocked (Phase 3 item 2
   above records the facts for the owner's verdict on this line).

Still past the line: the ergonomic agent socket, the two-pool demo and OSS
extraction (Phase 3 items 3–5). Those make it *shine* and are the v1.x → v2 arc.
This line remains a judgment call, recorded so the next move is also deliberate
rather than drift.

### Operating rule across all phases

**Real clients are the bug source of record.** The hubd `matrix-hs` queue
(agents) and Element X (owner) surface what actually matters faster than any
backlog — this has already outperformed planning twice (the EchoEar
createRoom/invite gaps, 2026-07-20). Prioritize accordingly.

Those two sources are the two halves of §"What this is for" reporting in: the
queue is the bus, Element X is the window. Both count. The queue has been the
sharper of the two, which is consistent with where the product is.

---

## Phase 1 — Make multi-master true

Close the gap between "replicates between live nodes" and "converges, period."
This is correctness, and it affects even the single-owner case (a rebooted
personal node today comes back blind to state set while it was down).

- **State catch-up for offline / late-joining nodes.** The mechanism already
  exists for the timeline (history catch-up via a Zenoh queryable, see
  `main.rs::merge_catchup_delta`); build the equivalent for the state channel
  (`routes/room_state.rs`'s `"state"` key) so a node that missed deltas can pull
  them on startup instead of staying permanently behind.

  ✅ **Room-discovery half done (2026-08-03).** `ClusterState::start_discovery()`
  — a wildcard subscriber that lazily creates a per-room sink for rooms this node
  has never seen — plus `list_room_ids()`, `ZenohCrdtSink::inject()`, and the
  matching unions in both the timeline and state drains. Salvaged from
  uncommitted work orphaned in a private fork; see
  the deployment topology record (kept private) §4. Covered by
  `room_discovery_fresh_node_learns_unknown_room` and
  `room_discovery_state_only_room`.

  ✅ **Closed (2026-08-04): wildcard catch-up.** Discovery only helps a node that
  is *running* when a peer publishes; a node that was down misses the sample, and
  Zenoh has no replay. Startup catch-up was supposed to cover that, but could not:
  it queried only rooms it already knew, and peers declared queryables only for
  rooms present in their own startup replay — so a room created after the peer
  started was served to nobody, whatever the querier asked. Both sides are now
  wildcards (`<prefix>/*/history`, `<prefix>/*/state`): one queryable per node
  answering from live state, and one GET per channel that learns each room_id off
  the reply key. Side effect: 2N+2 Zenoh sessions became 2.

  Startup now also waits for the mesh before asking — for a peer, and for that
  peer's *signing key*. Without the key, caught-up PDUs fail verification and are
  dropped, so the room would converge with no events in it and the log would read
  as a forgery warning rather than a race. Tunable via
  `MATRIX_HS_CATCHUP_{PEER,KEY}_WAIT_MS`; it delays the HTTP listener, which is
  the deliberate trade (do not serve a room before its contents arrive).

  Covered by `catchup_room_created_after_startup` plus unit tests on the two key
  functions, and verified end-to-end against the real binary: a room created on
  node A after A started, with node B booting fresh and never told the room id,
  arrives on B complete with its state and its messages.

  ✅ **Closed (2026-08-04): mid-life re-query.** All of the above runs once, at
  startup, so a node that was *up* through a partition still lost whatever passed
  while the link was down. A background pass now re-asks on two triggers: when the
  Zenoh peer set grows (a peer reappearing is what a healed partition looks like
  from this side), and on a backstop timer (a link can drop samples without the
  transport dropping, which fires no peer event). `MATRIX_HS_CATCHUP_INTERVAL_SECS`
  (300 s) and `MATRIX_HS_CATCHUP_SETTLE_MS` (6 s, so a new peer's signing key lands
  before we ask it anything).

  Repeating is safe because a pass is idempotent, and specifically because
  `merge_catchup_delta` tests each PDU against the **RoomLog**, not against
  `room_timeline`. That matters once `MATRIX_HS_TIMELINE_MAX_EVENTS` is set: were
  the check against the timeline, every pass would re-add events the retention cap
  had just trimmed and hand them new stream positions, i.e. re-deliver old messages
  to clients forever.

  Verified live rather than by unit test — the loop lives in the binary, which has
  no test harness. Node B, already running and idle, learned a room from node A
  that A only replayed from disk and never published; with the loop stubbed out,
  the same scenario leaves B without it. The backstop was observed firing on its
  own schedule.
- ✅ **Globally-unique room ids (2026-08-03).** Was a live multi-master
  correctness bug: `routes/rooms.rs` minted `!room_<rooms.len()>:<server>`, so two
  nodes each creating their first room both produced `!room_0:localhost` and
  collided on merge. Now `!room_<seq>_<rnd8>_<node>:<server>`. Salvaged from the
  same place. Note existing rooms keep their old ids — this only affects newly
  created ones.
- **Resolve the edge-node mode.** ✅ CODE VALIDATED (commit 500d2e3, test
  `edge_node_three_way_convergence`). Proves three nodes converge within 200ms.
  ✅ **DEPLOYED 2026-08-19, ACROSS TWO MACHINES.** Every blocker named here is
  cleared. Alpha runs from a stable install path with the carrier at
  `tcp/127.0.0.1:7449` — the hubd queue router, the local end of exactly the ssh
  rung `hubd-queue-repl` already proves — and **beta (FreeBSD) joined over
  `ssh -R` to that same router**. Round trip 0.01 s each way, a verified outage
  recovered in full, and an unpinned peer refused. The FreeBSD build blocker was
  real and mis-described: the vendored `zenoh-util` crate carried its fix as an
  *unapplied* patch file, so nothing had ever been built there. Fixed, and a
  FreeBSD `--features cluster` binary now exists. Remaining: neither the tunnel
  nor beta's node survives a reboot — see the deployment topology record (kept private) §3b and §3c.
- **Wire the transport ladder.** Rung 2 (Zenoh over `ssh -L`) is the default
  east–west carrier and already works for the hubd queue path; matrix-hs does
  not use it. Rungs 1 and 3 (LAN peer, obfs link) exist as capabilities with
  nothing selecting between them. This is config/deployment work, not substrate
  work — `ZenohCrdtSink` is already carrier-blind — but until it is done,
  transport independence is a property of the design and not of the system.
  Ladder and rules: `ARCHITECTURE-boundaries.md` §"East–west vs north–south".
- ✅ **Redactions replicate and survive restart (2026-08-04).** Found while checking
  whether the new periodic re-query could resurrect deleted content. It could not —
  but redactions turned out not to work cross-node at all, for a smaller reason:
  `redacts` was attached to the client event AFTER the Pdu was built, and a Pdu
  carries only content, so a redaction replicated naming no target and every other
  node kept serving the original body. The redaction was also lost on restart, since
  the mask lived in a map nothing rebuilt. `redacts` now rides in content too (where
  room version 11 puts it anyway) and all four write paths record it — send, replay,
  live cluster drain, catch-up. Tests `redaction_survives_restart` and
  `redaction_replicates_to_other_nodes`, both negative-checked; the catch-up path is
  verified live. Note this is masking, not erasure: the original event remains in
  every node's log, as in Matrix generally.
- ✅ **Garbage collection / anti-entropy tombstones (2026-08-04)**
  (hubd task `matrix-hs#gamma-2`). Layer (a) already capped `room_timeline`, but
  that is only the read projection — the RoomLog underneath it, and the journal on
  disk, still grew forever.

  The hard part is that you cannot delete from a grow-only set: the peer that has
  not collected hands everything straight back on the next merge, and with the
  mid-life re-query now on a timer it would do so on a schedule. The tombstone here
  is a **per-room depth watermark** rather than a per-event marker, so its cost is
  O(1) per room instead of growing with what it deletes. It is itself grow-only
  (merges by max), rides along on every delta, and both sides reject anything at or
  below it — so a collect decision spreads exactly like an event, and two nodes that
  collected different amounts converge on the deeper cut instead of fighting.

  Depth works as the axis because `routes/send.rs` assigns `depth = parent + 1`, so
  every prev of an event at depth d sits at d-1. That makes the rule for the
  topological sort exact rather than heuristic: a missing prev is collected exactly
  when the child is at depth <= watermark+1, and anything deeper is still a genuine
  forward reference to defer. Without that, collecting the tail of a room would hide
  the entire surviving head behind it.

  Watermark and pruning are persisted (`rooms/<room>.gc`, plus a journal rewrite) and
  re-applied on replay — otherwise a restart re-adds everything from the journal and
  the node starts re-offering peers exactly what it had decided to drop.

  `MATRIX_HS_ROOMLOG_MAX_EVENTS` (0 = off, the default). Deliberately off by default:
  this is deletion, cluster-wide and irreversible, not the timeline cap's local trim.

  Seven tests, all negative-checked: ordering across a cut, anti-resurrection through
  both `merge` and `apply_delta`, max-convergence, wire round-trip, monotonicity,
  forward-references still deferring, plus `gc_not_refilled_by_peer` (two real Zenoh
  nodes: B collects, A keeps talking, B takes the new event and refuses the old ones
  even when handed A's entire history) and `gc_survives_restart_and_shrinks_the_journal`.

  Still uncollected: `room_state` (bounded by distinct state keys, so it does not
  grow with traffic) and media (every node that reads a blob keeps it — see the
  cross-node fetch entry).
- ✅ **Cross-node media fetch (2026-08-04).** A blob uploaded on one node is now
  readable from any node. Pulled on demand, never gossiped — blobs are megabytes
  and would ride the same channel as room events, to replicate files most nodes
  are never asked for. Each node serves one queryable on `<prefix>/media/*`, keyed
  by media_id rather than by node, so no media_id→owner mapping has to be gossiped
  for a fetch to find the holder; a node without the blob stays silent, so silence
  is the "no". First reader caches write-through, preserving the original
  `owner_node`, so the second read is local and the blob survives the uploader
  going away. Bounded by `max_media_upload_bytes()` (a peer's reply is untrusted
  input) and `MATRIX_HS_MEDIA_FETCH_TIMEOUT_MS`.

  Thumbnails share `resolve_media`, so a thumbnail request can be what drags a
  blob across — deliberate: it is the same resolution order for both.

  Covered by `media_fetched_from_peer_and_cached` (real Zenoh, asserts the blob
  arrives byte-for-byte and is kept), `media_miss_stays_a_miss`, and wire-format /
  key round-trips; the first two verified to fail when the code they cover is
  stubbed. Also verified live: 200 KB of random bytes uploaded to node A,
  downloaded byte-identical from node B, unknown ids 404 in ~1.5 ms.
- ✅ **Cross-node OTK claim routing (2026-08-18).** A `keys/claim` for a device
  NOT owned by the node that received it used to silently return absent even
  when the owning peer was up and had the key — no per-(user,device) owner
  map is gossiped; instead this mirrors the media fetch above exactly, one
  wildcard queryable per node on `<prefix>/keys/claim/**` (keyed by
  base64url-encoded user_id/device_id/algorithm — a raw user_id's `@` does
  not survive Zenoh key-expression matching intact), silent on a miss.
  `try_claim_local` (the Mutex-guarded pop) is the one function both the
  local HTTP path and the queryable handler ever call, so exactly-once holds
  cross-node for the same reason it always held locally. Live-tested
  (`keys_cluster_test.rs`): alice's OTK uploaded on node-a is claimed by bob
  on node-b, routed over the mesh; a second claim after the only key is
  popped resolves to absent rather than hanging or re-serving it.

**Gate:** power a node off for an hour, bring it back — room, membership, and
media all re-converge. Until this test passes, "multi-master" ships with the
asterisk spelled out.

✅ **Passed 2026-08-19, across two machines and two operating systems.** beta
(FreeBSD) was stopped — verified down, not merely signalled — Alpha (Linux)
wrote five messages and a 16 KiB blob while it was away, and on restart it
re-converged unaided: 5/5 messages, membership on both domains, blob
byte-identical, all over Zenoh on an ssh tunnel.

The asterisk is smaller but not gone. The outage lasted minutes rather than an
hour, so startup catch-up and the peer-reappearance re-query were exercised and
the 300 s backstop timer across a long gap was not. And the deployment is not
reboot-durable yet (the deployment topology record (kept private) §3c). A first run of this gate was a
false pass — `pgrep -f matrix-hs` matched the ssh command carrying the same
string, so the node never stopped — and was discarded and redone.

## Phase 2 — Make the trust boundary real

Everything needed before a *second operator* can share a room without "well, a
bad node can do anything" being the honest answer.

All five items were done as of 2026-08-04; a sixth, smaller gap on the same
boundary closed 2026-08-18, and a seventh 2026-08-19.

- ✅ **Token epoch / revocation** (`7cfbffe`). Per-user epoch in `UserRecord` and
  in the token payload, checked on verify. Makes `logout_devices` real and lets a
  password change actually end sessions.
- ✅ **Minimal power-level gate over LWW (2026-08-04).** A receive-side check in
  `apply_remote_state_event`, run BEFORE the LWW compare — winning on timestamp is
  not permission. Closes "any node overwrites any membership" without adopting
  state resolution v2. Two carve-outs worth knowing: self-membership stays
  self-service, or a remote join could never land (a joining user is below
  `state_default`), and `m.room.power_levels` may not grant anyone — or
  `users_default` — a level above the sender's own, without which the gate is
  bypassable in one step by self-promotion.
- ✅ **UIA on cross-signing (2026-08-04).** `keys/device_signing/upload` now
  requires `m.login.password`, with the identifier checked against the caller's own
  user_id (otherwise a stolen token plus any other account's password clears it).
  Sessions are one-shot; anything incomplete re-challenges rather than failing, so
  response shape does not distinguish an expired session from a wrong password.
- ✅ **TOFU → anchor (2026-08-04, internal-task).** `MATRIX_HS_NODE_KEYS` pins
  `node_id=<hex32>` pairs and closes the set: announcements from anyone else are
  refused rather than learned, which removes the race the unauthenticated
  announcement channel otherwise hands to whoever claims a node_id first. Unset →
  TOFU. A malformed list fails **closed**, trusting only ourselves — a typo in an
  allow-list must not quietly become no allow-list.
- ✅ **HLC instead of wall-clock ts (2026-08-04)**, and it turned out to matter more
  than "strengthens the tiebreak under clock skew". Room state writes were carrying
  `stream_pos * 1000` — a node-LOCAL event counter, not a clock. Values from
  different nodes were not comparable (the busier node won every race regardless of
  when anything happened), and since `createRoom` used the wall clock (~1.7e12)
  while later edits used the counter (~1e4), an edit to state set at creation
  applied locally and **lost on every peer** — a rename that silently did not
  converge. One hybrid clock now feeds both: `max(wall, last+1)`, advanced past any
  peer timestamp within `HLC_MAX_DRIFT_MS` (5 min). The bound matters as much as the
  clock: without it one node with a dead RTC would drag every node's timestamps into
  the future and nothing later could ever win again.
- ✅ **Kick/ban power-level enforcement on the LOCAL write path (2026-08-18).**
  The minimal power-level gate above (`may_set_state`) only ever ran on the
  cross-node *receive* side. `routes/room_state.rs`'s own kick/ban handlers
  checked nothing but room membership — any joined member, however low their
  power level, could kick or ban anyone else in the room, including its
  admins, straight through the local HTTP path the cross-node gate never
  sees. `require_outranks` closes it: sender must reach the "kick"/"ban"
  power_levels threshold (default 50) AND strictly outrank the target, so
  two equal-power members can never remove each other. Same "reach the
  threshold, outrank the target" shape as the existing cross-node gate, just
  applied where a local client actually calls in.
- ✅ **The receive path no longer trusts the sender's framing or its event_id
  (2026-08-19).** Two holes found while documenting the wire for Phase 3 item 1,
  both on the path a peer's bytes actually take.

  `delta_from_bytes` had no bounds checks — it sliced `buf[off..off+n]` directly
  and ended in `.expect("utf8")` — and it runs *before* verification on both the
  `/sync` drain and catch-up. A five-byte publish took down a handler with no
  key at all, which is a lower bar than every other item in this phase assumes.
  It now returns `Option` and every call site drops a malformed blob.

  And `apply_delta_verified` filed each PDU under the `event_id` the sender
  supplied without ever re-deriving it, though `event_id` is not part of the
  signed pre-image — so a node whose key we pinned could put a validly-signed
  event under any id, and dedup, ordering and the redaction table all key on
  that field. It is now compared against `Pdu::compute_id`. This costs no
  compatibility: the signature already covers every field the id is derived
  from, and unsigned PDUs are rejected before the check.

  Both negative-checked. Detail and the reasoning about historical data:
  the v1.0 plan record (kept private).

**Gate:** the "compromised/buggy node" scenario walks through on paper without
"it can do anything." A node that is not trusted by the anchor cannot be heard at
all; one that is can no longer rewrite state it has no power level for, nor win by
claiming a later clock.

Two holes in that gate were found on 2026-08-19 while the wire was being
documented, and both were closed the same day — see the seventh Phase 2 item
below and the v1.0 plan record (kept private) §"Two code findings, both closed
2026-08-19".

---

## Phase 3 — Make the bus a bus, and open it up

The differentiator's usable surface. Items 1–2 are on the v1.0 line (marker
below), and item 1 is done; items 3–5 are past it. Earlier versions of this section opened by saying
"agents already work via raw `mrgd`" — that was the `couplingd` claim again, and
it is false. Nothing has ever linked this crate.

### 1. ✅ Write the wire down — a spec, not a crate (2026-08-19)

[`docs/WIRE.md`](docs/WIRE.md). Every key expression in both its concrete and
wildcard form; the two payload families and which key uses which; the canonical
byte encoding that feeds both the content address and the signature; the
`node_auth` scheme and its sender-domain binding; the GC watermark; the two
clocks; the query plane with its three different ways of saying "I don't have
it"; and the receive-side rules in the order they must be applied. Ten named
asymmetries are listed separately, because each is a way an independent
implementation looks correct and is not — the endianness that is uniform except
for media, the length prefixes that are `u64` when signing and `u16` on the
wire, the reply key rather than the payload being the routing authority.

Not a `pub` API, and specifically not a crate. The consumers here are Node.js,
Python, Rust, FreeBSD, Linux and macOS; a Rust dependency demands one language,
one build and one lifecycle from all of them, and twice a component that needed
exactly this declined to link it and was right to
(`ARCHITECTURE-boundaries.md` §"What reuse actually looks like here").

**Verified rather than declared.** A spec checked against the implementation
that produced it proves nothing, so the document publishes a test vector
(§13) and [`scripts/wire_conformance.py`](scripts/wire_conformance.py) decodes
it from the document alone, with the Rust closed. It recovers every field,
recomputes the `event_id` from canonical bytes and gets the published value —
which is what actually proves the `u64` prefixes and the prev-sorting rule are
written down correctly — verifies the ed25519 signature over those same bytes,
and confirms its own decoder is total. The vector is pinned from the Rust side
by `golden_vector_matches_the_spec`, so encoder, spec and script are one
contract: change one, change all three, and know that the commit is a wire break
rather than a refactor.

Item 3 and every consumer after it get written against this. Item 2 is the
exception that proves the point: it was built before any spec existed, which is
exactly why it ended up as a module *inside* matrix-hs instead of a separate
process speaking the wire.

### 2. Put one real consumer on the wire — retire `hubd-queue-repl`

**Live since 2026-08-20 on Alpha and beta, both directions verified.**
`src/hubd_bridge.rs` carries hubd's agent queues as room traffic: signed,
caught up after an outage, deduped by content-addressed `event_id` with no
suppress set. Switched on via `MATRIX_HS_HUBD_QUEUES_DIR` on both nodes; at
turn-on 15 queue files (63 blocks) rode the wire to beta byte-identical, and
live probes round-tripped both ways. Steps and measurements:
the hubd-bridge record (kept private) §"Migrating off `hubd-queue-repl`".

**Item B is executed but not closed**, and the residue is a scope question,
not code: `hubd-queue-repl` keeps running on Alpha because gamma's hubd is
still fed by it over the ssh tunnel, and per the scope-separation record (kept private)
gamma must not join this mesh — so the last hand-rolled dedup scheme
(`ARCHITECTURE-boundaries.md` §"One semantic, many transports") survives until
gamma has a consumer path that does not cross scopes. Whether "one real
consumer live on the bus" is satisfied by a live producer (Alpha) plus a
passive materialising peer (beta, no hubd runs there) is the owner's call to
make against the v1.0 line; the facts are recorded here so that call is a
judgment, not a drift.

### ← v1.0 line

**Tagged 2026-08-21.** The verdict the facts were parked for (see Phase 3 item 2)
was given by the owner executing all recommendations: the line is **met**. What
"one real consumer live on the bus" resolved to: a live producer on Alpha
(real hubd traffic, every agent queue block on this host), two materialising
peers (beta, delta — one of which has since proven live *ingest* too, in the
four-direction probes of the three-node mesh), and a `hub_queue_wait` consumer
on Alpha reading bridge-materialised peer files. The gamma residue
(`hubd-queue-repl` as its feed) is recorded, deliberate, and scope-blocked —
an asterisk with an owner and a plan, not an unknown.

Earlier wording of this line, kept for the record: tag v1.0 when Phases 1 and
2 are done, their gates pass, and Phase 3 items 1–2 are live — the point at
which the README can describe the project without an asterisk.

1. the Phase 1 carrier work and its live gate (power a node off for an hour) —
   ~~deployment, and now the largest item left~~ **passed 2026-08-19 across two
   machines** (and the deployment is reboot-durable since 2026-08-20);
2. the Phase 2 gate — ~~it already passes on paper~~ **passed: every item
   closed in code, two receive-path holes found and fixed while documenting the
   wire (2026-08-19)**;
3. ~~Phase 3 item 1, write the wire down~~ — **done 2026-08-19**
   ([`docs/WIRE.md`](docs/WIRE.md));
4. ~~Phase 3 item 2, turn the bridge on where it replaces `hubd-queue-repl`~~ —
   **live on Alpha and beta since 2026-08-20, on delta since 2026-08-21**; the
   residual `hubd-queue-repl` is gamma's feed and is scope-blocked, not
   work-blocked.

### 3. Tenant gateway (Application-Service-shaped)

**First slice landed 2026-08-23: the agent socket is real.** `MATRIX_HS_AS_TOKEN`
+ `MATRIX_HS_AS_PREFIX` switch on AS-shaped registration and passwordless
per-device login (`m.login.application_service`) — the "one MXID per tenant,
workers as devices" surface, namespace-enforced, tested (`as_socket_test.rs`,
8 cases). See `docs/AGENT-USE-CASES.md` Case 2. Still open on this item: a
first live consumer through the socket (the hubd agents are the natural one),
key backup as the worker handoff path, and plural services.

One MXID per account/tenant, not per process; worker registration =
`keys/upload` (a new device), worker death = it just stops syncing. Turns the
E2EE stack already built (Ф1a sendToDevice, Ф1b device_lists, Ф2 key backup +
cross-signing) from "human-client tax" into direct autoscaled-fleet
infrastructure — exactly the model in
[`docs/AGENT-USE-CASES.md`](docs/AGENT-USE-CASES.md) Case 2.

### 4. Two-pool demo

**First form landed 2026-08-23 (`src/two_pool_demo_test.rs`): the demo as a
live integration test.** Two operators, one node each, an AS namespace per
pool, one shared room over a real Zenoh mesh — cross-pool OTK bootstrap
(routed claim), encrypted timeline convergence, sendToDevice delivery,
device-list gossip visibility, and three trust-boundary negatives (foreign AS
token, worker-token-as-AS, cross-pool AS login — all 403). Building it caught
a real bug: `extract_caller` read the device from the user record instead of
the token, so workers-as-devices never worked end-to-end (commit `db69887`
fixes it; the token's device field is HMAC-signed). Still future on this
item: the same scenario with real client-side crypto (vodozemac) as the
headline artifact for the outside audience — the server-side half is what is
proven today.

Pool A and Pool B under distinct operators in one room: device-trust inside each
pool, E2EE + `node_auth` between them. Doubles as the Phase 2 integration test
and as the headline artifact for the secondary audience named in §"What this is
for".

### 5. OSS extraction

The crate was deliberately collapsed (`8f535db`) because no second consumer was
linking it. Re-extract `substrate` on the day something actually adds
`mrgd = { path/git = … }` to its own `Cargo.toml` — not to publish, and not
before. Publishing the *protocol* (item 1) does not require splitting the crate,
and it is the release that would matter to anyone outside this operator.

## VoIP (hubd task `matrix-hs#gamma-5`) — server+relay proven end-to-end on LAN

Reclassified from non-goal to on-the-path once a concrete consumer appeared:
the EchoEar-2ST nursery-monitor speaker/mic (a companion project, not in this
repository). That
reclassification is the "devices" half of §"What this is for" doing its job —
a real machine on the bus asked for something, and that is the only thing that
has ever moved an item onto this path. Two of the three pieces are **done and
live-verified**:

1. **Homeserver credential endpoint** — `GET /voip/turnServer` (standard TURN
   REST / HMAC-SHA1 shared-secret scheme, `routes/voip.rs`, commit `311ef8b`).
   The server never touches media; call signaling is already generic event
   relay. Not just unit-tested — proven against the real deployed instance.
2. **TURN relay itself** — coturn installed and running on this LAN
   (`<lan-host>:3478`, `matrix-hs.env` wired with the matching
   `static-auth-secret`), firewalld-scoped to `<lan>/24` only via the
   same rich-rule pattern already used for other LAN-only ports. Verified with
   `turnutils_uclient` using credentials actually issued by the live
   `voip/turnServer` endpoint: real allocation, 0% packet loss, and — the
   security check that matters — a tampered password is rejected
   ("Cannot complete Allocation"). The full chain (matrix-hs mints credential →
   coturn accepts it → relay carries traffic) works today for anything on this
   LAN, no public exposure needed.

**Remaining, outside the matrix-hs codebase:** a Matrix client that actually
speaks WebRTC on each end. The EchoEar today streams raw HTTP audio, not
Matrix WebRTC — turning it into a real call participant (`m.call.*` /
MSC3401) is firmware work in the echoear-2st project, not here. Whether to
pursue that is a separate call driven by that consumer.

Not yet decided (deliberately, not an oversight): whether this TURN server
should ever be reachable beyond the LAN. `turnserver.conf` has no TLS
configured (plain UDP/TCP only, no `tls-listening-port`/cert) — fine for a
LAN relay, but exposing it publicly would want `turns:`/DTLS added first. LAN-
only is the right scope for the current consumer; revisit only if a
remote-caller use case actually shows up.

---

## Non-goals (deliberately NOT on the path to v1)

- **Matrix State Resolution v2** (hubd task `matrix-hs#gamma-1`). The project's
  position is "CRDT LWW + a minimal power-gate," fixed in
  `ARCHITECTURE-boundaries.md`; full auth-chain state-res contradicts the
  coordination-free thesis.

  Considered again, not just assumed, on 2026-08-18: an unrelated exploratory
  session built the algorithm's foundation primitives as pure, hand-tested
  functions (auth-chain transitive closure over `auth_events`, the
  power-event predicate, reverse-topological power ordering, mainline
  construction, and an iterative re-authorization driver) — validated line
  by line against Synapse's reference implementation, including two
  corrections to a common spec-prose misreading (kicks count as power
  events, not just bans; `m.room.create` counts too). It confirmed the
  non-goal rather than overturning it: real state-res-v2 needs an
  `auth_events` field on `Pdu` (a breaking wire-format change), a
  "conflicted state set" concept this codebase's incremental one-PDU-at-a-
  time fold has no equivalent of, and a "who cited what" dependents index
  for retracting an already-admitted event when a state event it cited
  later loses a fork elsewhere — none of which exist today, all real,
  ongoing architectural weight for a class of bug (a genuine 3+-way
  concurrent power-level fork, or a join_rules/per-transition violation the
  minimal gate above does not catch) no real client has hit yet. That
  prototype was not merged and was discarded, not kept in this repo — this
  paragraph is the record of the reasoning, not a pointer to the code.
- **Full CS-API spec parity.** The window has to be clear, not panoramic: only
  implement what real clients actually hit — the hubd queue filters this well
  (the EchoEar case is the pattern). This is a non-goal about *parity*, not about
  quality: an endpoint a real client depends on being broken is an ordinary bug
  at ordinary severity, because that window is the owner's own.
- **"Federation over Zenoh" as a standalone task** (hubd task
  `matrix-hs#gamma-4`). Too vague; it is effectively Phases 1 + 2.
- **Real Matrix federation as the east–west transport.** Would connect this to
  the wider Matrix ecosystem over boring HTTPS, but federation is HTTPS-only by
  spec and order-sensitive in its state resolution — it cannot survive the
  carrier diversity this deployment needs, and HTTPS is the *first* rung to be
  blocked on these paths, not the last. Reconsider only if the threat model
  changes. Not a non-goal because it is uninteresting; a non-goal because it
  inverts the layering.

  **Interop verdict, 2026-08-24 — not even as a gateway, for now.** The
  question "should mrgd grow a federation bridge (HTTPS outside, WIRE.md
  inside) so its users can join rooms on the wider network?" was put to the
  owner and answered **no**: participating in rooms on matrix.org et al. is
  solved by multi-accounting — the clients this server serves (BareChat,
  Element X) are multi-account already, and a second account on a public
  homeserver through the same client covers the need without a single line of
  federation code. The clean shape, if this is ever revisited, remains a
  separate gateway process (a rendezvous-pattern north-south adapter), NOT
  state-res-v2 inside the substrate — and the smallest honest spike would be
  read-only: pulling a public room's `/backfill` with state-res v2 on *foreign*
  events only. Recorded so the next session does not re-litigate it.
- **Moving hubd queue replication onto the CS-API.** Considered and rejected
  2026-08-03: it would place the system's most critical function on its least
  survivable transport. `hubd-queue-repl`'s hand-rolled dedup is still worth
  retiring in favour of content-addressed `event_id`, but over its existing
  ssh-tunnelled Zenoh carrier — the transport stays, the duplicate semantic goes.

## Known infra / process debt tracked elsewhere

- **One process per scope = a second process on gamma (hub task
  `alpha-67`; added 2026-08-27).** The four-node bus works
  because gamma runs TWO matrix-hs processes — `home` and `bus` — since
  the scope-separation record (kept private) §2's rule holds: a process converges on everything its mesh
  carries, so one process cannot sit in two scopes without handing each scope
  the other's replica. That is correct today and ugly forever: every
  dual-scope host pays double processes, stores, ports and envs. The debt this
  names is the **proper fix, in three layers**:
  1. **Scopes at the protocol level** — a mesh member declares which
     scope(s) it participates in, and the substrate routes/delivers per scope
     (a node holds replicas only for scopes it joined; discovery and catch-up
     answer per-scope, not "every room I hold"). Replaces the
     process-per-scope interlock with a wire-level one.
  2. **E2EE on replication** — encrypt the replicated payloads so a relaying
     node (Alpha for `home`, any bus host for a future scoped guest) carries
     ciphertext it cannot read; today only the obfs *transport* is AEAD, the
     payload plaintext lives on every mesh member by design.
  3. **Federation-shaped sharing on our protocol** — selective room/event
     publication between scopes (the scope-separation record (kept private) §9's "derived facts across scopes"
     generalised): what Matrix gets from federation, but coordination-free —
     typed, one-directional, deliberate, no server-to-server trust.
  Until then the second process stays; it is the interlock, not a workaround
  to delete casually.
- ~~`hubd#m-3` (high): `hub_queue_wait` drops a role's pre-existing backlog on
  the first-ever wait~~ — **fixed upstream 2026-07-31 and verified in the
  installed hubd on 2026-08-20**: `lib/queue.mjs`'s `readOff` defaults to 0
  when no offset file exists, so a first-ever wait replays the backlog from
  the top of the file.
- **hubd project cards: two cards, one project.** `matrix-hs` (which holds the
  task history and journal) and `mrgd` (a folder-name guess `hub_context`
  created) both describe this repo. Both digests were rewritten 2026-08-19 to
  carry the purpose above, the real HEAD and the real status — the previous ones
  still listed as open several things closed on 2026-08-04, and the `mrgd` one
  pointed at a stray clone deleted 2026-08-18. 2026-08-20
  evening: the `matrix-hs` card's `path:` was corrected to this repository's
  checkout via `hub_sync`. What still wants a human: collapsing the two cards
  into one — that moves or discards task history, so it is not something to
  do unattended.
- Doc debt outside this repo (bsdOS workspace still lists the dead `matrix-hs`
  fork in `members`; the Conduit design docs never say they were superseded;
  `hubd-queue-repl`'s stated transport-security rule contradicts the deployed
  matrix-hs env). Enumerated in the deployment topology record (kept private) §5.
