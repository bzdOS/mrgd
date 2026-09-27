# The east–west wire

Status: **normative for interoperability.** This is the protocol nodes use to
converge with each other. It is written so that something which is not this Rust
crate can join the mesh — that is the entire point of writing it down, and it is
[`../ROADMAP.md`](../ROADMAP.md) Phase 3 item 1.

Extracted from the implementation on 2026-08-19 at `44905ea`. Where this
document and the code disagree, the code is right and this document is a bug;
report it rather than working around it.

**Scope.** East–west only: node ↔ node replication over Zenoh. The north–south
surface (a Matrix client talking to one node over HTTPS) is the CS-API and is
not described here. The boundary between the two, and why it must not be
crossed, is [`../ARCHITECTURE-boundaries.md`](../ARCHITECTURE-boundaries.md).

**Not a `pub` API.** Reuse in this system is by wire, not by crate: consumers
here run on Node.js, Python, Rust, FreeBSD, Linux and macOS, and a Rust
dependency would demand one language, one build and one lifecycle from all of
them. Implement this document; do not link the crate.

---

## 1. Transport

Zenoh 1.x, **TCP only** (`transport_tcp`; no UDP, no QUIC, no shared memory).
A node runs either in peer mode with scouting, or connected to a Zenoh router —
the protocol is identical either way, and the carrier underneath (LAN, an
`ssh -L` tunnel, an obfuscated link) is invisible to it.

Two properties shape everything below:

- **Zenoh pub/sub has no replay.** A subscriber that was not connected when a
  sample was published never sees it. This is why §7's query plane exists, why
  key announcements repeat every 5 s, and why claims repeat every 5 ticks **only
  within a bounded AntiEntropyPolicy burst** (see §X — the 2026-09-23 fix limits
  re-publish to 3 eligible ticks after last activity, then silence).
- **Delivery is at-least-once and unordered.** Every payload here is therefore
  idempotent and order-independent. Receiving the same delta twice, or out of
  causal order, must converge to the same state; §8 is where that is made
  explicit.

## 2. Names

Three **independent** prefixes, not one. An implementation must take all three
as configuration, because a deployment that shares one prefix between unrelated
clusters cannot tell them apart.

| Prefix | Env var in the reference implementation | Default | Carries |
|---|---|---|---|
| `<P>` | `MATRIX_HS_ZENOH_PREFIX` | `mrgd/matrix/room` | rooms, media, one-time-key claims |
| `<B>` | `MATRIX_HS_BARRIER_KEY` | `mrgd/coupling/barrier` | grow-set claims (username, alias) |
| `<K>` | `MATRIX_HS_KEYS_PREFIX` | `mrgd/matrix/keys` | node signing-key announcements |

A node also has two identifiers, and **they are not the same thing**:

- **`server_name`** — the signing identity. It is what goes in `signer_node`,
  what a `sender`'s domain is checked against (§5.4), and what a key
  announcement claims. In the reference implementation this is
  `MATRIX_HS_SERVER_NAME`, default `localhost`.
- **`node_id`** — a tiebreaker for the claim barrier and a label for
  observability. It never appears in an authentication decision.

Using the default `server_name` on a shared prefix is the documented way to
create a mesh in which two unrelated nodes both announce as `localhost`.

## 3. Key expressions

### 3.1 Publish / subscribe

Publishers use concrete keys of the form `<P>/<room_id>/<channel>`. Subscribers
use `<P>/**` (discovery: learns rooms it has never heard of) or
`<P>/<room_id>/**` (one per known room). Both may receive the same sample; §8's
idempotence is what makes that harmless.

| Key | Payload | §|
|---|---|---|
| `<P>/<room_id>/events` | binary `RoomLogDelta` | 6 |
| `<P>/<room_id>/state` | JSON `StateDeltaMsg` | 9.1 |
| `<P>/<room_id>/typing` | JSON | 9.2 |
| `<P>/<room_id>/receipt` | JSON | 9.3 |
| `<P>/__to_device__/msgs` | JSON | 9.4 |
| `<P>/__device_list__/changes` | JSON | 9.5 |
| `<B>/claims` | JSON `ClaimRecord` | 9.6 |
| `<K>/announce` | binary | 5.5 |

`__to_device__` and `__device_list__` are **pseudo-rooms**: they occupy the same
slot as a `room_id` and a wildcard subscriber will parse them as one. A real
room whose id were literally `media` or `keys` would collide with §3.2; room ids
minted by the reference implementation cannot be.

`<room_id>` is embedded **raw and unencoded**, `!` and `:` included. See §11.1 —
this is inconsistent with §3.2 and the inconsistency is load-bearing to
reproduce.

### 3.2 Query plane

Four queryables. Every one of them is declared with Zenoh's
`allowed_origin(Locality::Remote)`, and that is a correctness requirement rather
than tuning: a node declares its queryables on the *same session* it queries
from, so without `Remote` it answers its own query and concludes it has
converged with itself.

| Queryable declares | Querier GETs | Reply key | Miss = |
|---|---|---|---|
| `<P>/*/history` | `<P>/*/history` (wildcard) | `<P>/<room_id>/history` | see §7.1 |
| `<P>/*/state` | `<P>/*/state` (wildcard) | `<P>/<room_id>/state` | see §7.1 |
| `<P>/media/*` | `<P>/media/<media_id>` (concrete) | same concrete key | **silence** |
| `<P>/keys/claim/**` | `<P>/keys/claim/<u>/<d>/<a>` (concrete) | same concrete key | **silence** |

No query carries a payload, selector parameters, or a target/consolidation
override. The key expression is the entire request.

### 3.3 Segment encoding

The one-time-key claim key, and only it, base64url-encodes its variable
segments with the **`URL_SAFE_NO_PAD`** alphabet (`A–Z a–z 0–9 - _`, no `=`):

```
<P>/keys/claim/<b64u(user_id)>/<b64u(device_id)>/<b64u(algorithm)>
```

because a raw `user_id` begins with `@` and contains `:`, which do not survive
Zenoh key-expression matching as literals. A decoder must reject a key with more
or fewer than three segments after `keys/claim/`, and reject segments that are
not valid base64url of valid UTF-8.

`media_id` is not encoded at key-build time, but the reference implementation
mints every `media_id` as base64url of random bytes, so it is safe by
construction. An implementation that mints media ids differently must ensure the
same.

## 4. Framing primitives

Two payload families. Which family a key uses is **not** guessable from its
name, so §3.1 and §3.2 state it per key.

**JSON** — `serde_json`'s default representation of the struct given in §9.
Field names are exactly as written; there are no renames, no omitted-if-null
fields, and no envelope. A receiver must skip a payload that does not parse, and
must not fail the surrounding operation because of it.

**Binary** — length-prefixed, little-endian **except where §5.5 and §7.3 say
big-endian**. This asymmetry is real; see §11.2. The primitives are:

| Notation | Meaning |
|---|---|
| `u16`, `u32`, `u64` | unsigned little-endian |
| `u16be` | unsigned **big**-endian |
| `str16` | `u16` byte length, then that many bytes of UTF-8 |
| `bin16` | `u16` byte length, then that many raw bytes |
| `bin32` | `u32` byte length, then that many raw bytes |

**A decoder must be total.** Every length prefix is attacker-controlled: these
bytes arrive from the network and are decoded *before* any signature is checked
(§8.1). A malformed payload must yield "not a valid message" and be dropped —
never a panic, an abort, or an allocation sized from a count read off the wire.
The reference implementation had exactly this bug until 2026-08-19: a five-byte
publish took down a request handler with no key required.

## 5. Identity and authentication

### 5.1 Canonical bytes

The single injective serialisation of a PDU's content fields. It is the input to
**both** the event id and the signature, which is why injectivity is required:
if two distinct field tuples could produce the same bytes, one signature would
authenticate both readings.

```
u64 len(room_id)  room_id
u64 len(sender)   sender
u64 len(kind)     kind
u64 len(content)  content
u64 count(prev_events)
    for each prev, in ASCENDING BYTE ORDER:  u64 len(prev)  prev
u64 depth
u64 ts
```

Three details are easy to get wrong and each breaks interoperability silently:

1. **Every length here is `u64`**, including the prev count — even though the
   *transport* framing in §6 prefixes the same fields with `u16`/`u32`. Reusing
   the transport widths here produces signatures no one can verify.
2. **`prev_events` are sorted** (plain ascending byte comparison of the UTF-8)
   before being written, so the ordering a sender happens to hold them in does
   not change the result.
3. **There are no delimiters.** Length prefixes are the only structure. A
   NUL-delimited variant is not compatible and is not injective.

`event_id`, `sig` and `signer_node` are **not** covered.

### 5.2 Event id

```
event_id = "$" + base64url_nopad( sha256( canonical_bytes ) )
```

43 characters after the `$`. It is a content address, and §8.1 requires
receivers to check that it actually is one.

### 5.3 Signature

Ed25519 over the canonical bytes of §5.1, 64 bytes, verified with
**`verify_strict`** semantics — non-canonical or malleable signatures and
small-order public keys must be rejected, not merely "verified".

The signing key is a 32-byte seed held by the node. It must not be derived from
`server_name`, which is public.

### 5.4 Sender-domain binding

A valid signature is not sufficient. A signing node may only assert senders
homed on its own domain:

```
domain(sender) == signer_node
```

where `domain(sender)` is everything after the **first** `:` in `sender`. A
`sender` with no `:` is rejected. So `@u:a:b` has domain `a:b`.

This is what limits the blast radius of §5.5's unauthenticated announcement
channel: a node that race-claims someone else's `server_name` can sign only for
that domain, and cannot forge senders on others.

### 5.5 Key announcement

Published repeatedly (every 5 s in the reference implementation, because there
is no replay) on `<K>/announce`:

```
u16 len(node_id)   node_id   [32 raw bytes: ed25519 public key]
```

Little-endian length. A decoder must require the total length to be **exactly**
`2 + len + 32` — trailing bytes are a reject, not a tolerance.

**This channel is unauthenticated by design.** An announcement carries no
signature, so on an unauthenticated mesh anyone can claim a `node_id` before its
legitimate owner first announces. Two mitigations exist and an implementation
should support both: §5.4's binding, and a pinned closed set of
`node_id → public key` (in the reference implementation `MATRIX_HS_NODE_KEYS`,
formatted `id=<64 hex>,id=<64 hex>`), which refuses announcements from anyone
else rather than learning them. A malformed pin list must fail **closed** —
trusting only oneself — because a typo in an allow-list must not silently become
no allow-list.

Without pinning, the store is trust-on-first-use: the first key seen for a
`node_id` is kept, and a later different key for the same id is refused.

## 6. The event channel: `RoomLogDelta`

Binary. The unit of timeline replication.

```
u32 n_pdus
repeat n_pdus times:
    str16  event_id
    str16  room_id
    str16  sender
    str16  kind
    bin32  content            -- opaque to this layer
    u16    n_prevs
        repeat n_prevs: str16 prev_event_id
    u64    depth
    u64    ts
    str16  signer_node        -- empty when unsigned
    bin16  sig                -- length 0 when unsigned
u64 collected_depth           -- see §6.1; may be ABSENT
```

Note the tail order: **`signer_node` precedes `sig` on the wire**, which is the
opposite of the order they appear in the reference implementation's struct.

A live publish carries **exactly one PDU** — the new one. A catch-up reply
(§7.1) carries the sender's whole log. Both use this same format.

`content` is opaque bytes at this layer. For Matrix events it is the event's
JSON body, and it must be forwarded byte-for-byte: re-serialising it changes the
canonical bytes and invalidates the signature. (Key order in a JSON object is
not preserved by every serialiser. This exact mistake broke verification for
every message body whose keys were not alphabetical.)

### 6.1 The garbage-collection watermark

`collected_depth` is appended **after** the PDU blocks, deliberately: a reader
that stops after `n_pdus` blocks simply never sees it, so an older peer keeps
working. **A reader that finds no trailing 8 bytes must treat the value as 0**,
meaning "this sender has collected nothing" — that is a valid old sender, not a
malformed payload.

Its meaning: every event at depth ≤ the watermark has been deleted and must
never be re-added. It is what makes deletion possible on a grow-only set — a
peer that had not collected would otherwise hand everything straight back on the
next merge, forever.

Three rules, all required for convergence:

1. It is itself grow-only: merge by `max`. A lower value is ignored, never
   applied, or two nodes take turns undoing each other.
2. Adopt the sender's cut **before** inserting any PDU from the same delta.
3. Reject any PDU at depth ≤ the watermark — but **do not count that as a
   rejection**. A peer resending history you chose to drop is not misbehaving.

There is a fourth rule in the topological sort. Because `depth = parent + 1`, a
PDU whose `prev_event` is missing is *collected* rather than *pending* when the
PDU sits at `watermark + 1` or below. Get this wrong and collecting a room's
tail hides its entire surviving head.

## 7. The query plane

### 7.1 Catch-up

Two GETs, history then state, each on the **wildcard** key. No payload.

A responder replies **once per room it knows**, with the room id in the reply
key. That is the whole of room discovery: a querier learns of rooms it has never
heard of by reading reply keys, and must create them locally on the strength of
that. The payload also contains a `room_id` field, but it is not what routing
uses — a responder whose payload disagrees with its reply key will have the
payload believed.

Because a reply exists per known room, "I don't have it" is expressed by
**absence from the reply set**, not by an error. Two sub-cases:

- A **known but empty** room replies on `history` with a 4-byte payload of
  zeros (a delta declaring zero PDUs), and on `state` with a zero-length
  payload. A querier must treat both as "nothing to merge" and must not count
  the room as converged.
- A node with **no rooms at all** sends no reply.

### 7.2 One-time key claims

Concrete key (§3.3). The reply is JSON:

```json
{"key_id": "<algorithm>:<id>", "key_value": <opaque>}
```

**A node that does not own the device stays silent.** This is the important
convention: an empty reply would be indistinguishable from a real one, and every
node in the mesh would send one for every miss. Silence is the "no".

Claiming is destructive and must be exactly-once. The reference implementation
gets this without a distributed lock because a device belongs to exactly one
node, and both the local path and the queryable handler funnel through one
mutex-guarded pop. An implementation must preserve that property: it is a
partition of ownership, not a consensus.

### 7.3 Media

Concrete key. Silent on a miss, for the same reason as §7.2. The reply is
binary, and **big-endian**:

```
u16be len(content_type)  content_type
u16be len(owner_node)    owner_node
blob                     -- to the end of the payload
```

`owner_node` is the node that originally accepted the upload, carried so that a
node which caches a fetched copy does not start claiming ownership of it.

A fetcher must bound the accepted reply size — this is an untrusted peer's
payload, and the natural bound is whatever the local upload limit is.

Media is **never** gossiped on the pub/sub plane. Blobs are megabytes and would
ride the same channel as room events.

## 8. Receive-side rules

These are not optional. A node that skips them converges to different state than
its peers, or accepts events it should not.

### 8.1 The event channel, in order

1. **Parse.** On failure, drop the payload. Never panic. (§4)
2. **Adopt the watermark**, then skip any PDU at or below it — not counted as a
   rejection. (§6.1)
3. **Reject unsigned PDUs** (`sig` or `signer_node` empty).
4. **Verify the signature** over freshly recomputed canonical bytes, then the
   sender-domain binding. (§5.3, §5.4)
5. **Re-derive `event_id`** from those same canonical bytes and reject a
   mismatch. The signature does not cover the id, so without this a node whose
   key you have pinned can file a validly-signed event under any id it likes —
   and dedup, ordering and redaction all key on that field.
6. **Insert, keyed on `event_id`, first writer wins.** Re-receiving a PDU you
   already hold is a no-op, not an update.

### 8.2 Ordering

Events form a DAG via `prev_events`. Emit them in a topological order, breaking
ties deterministically so that every node reaches the same sequence from the
same set. A PDU naming a `prev_event` nobody has is **deferred**, not dropped —
it is a forward reference and the parent may still arrive — except under §6.1's
fourth rule.

### 8.3 State, and why it is not the same as events

Room state does not use the event DAG. It is last-writer-wins per
`(event_type, state_key)`, ordered by `(origin_server_ts, event_id)`.

Winning the timestamp comparison is **not** permission. Before the LWW compare,
a receiver must check the write against the room's current `m.room.power_levels`:

- No `m.room.power_levels` in the room at all → allow. Otherwise a room's own
  power levels could never replicate in the first place.
- `m.room.member` where `state_key == sender` → allow unconditionally. A user
  joining is below `state_default`, so gating self-membership would break every
  remote join.
- Otherwise: required level is `content.events[<type>]`, else
  `content.state_default`, else **50**; the sender's level is
  `content.users[<sender>]`, else `content.users_default`, else **0**; allow when
  sender ≥ required.
- Additionally, an `m.room.power_levels` write with an empty `state_key` is
  rejected if it would grant any user — or `users_default` — a level strictly
  above the sender's own. Without this the gate is bypassable in one step by
  self-promotion.

### 8.4 Clocks

**There are two, and they are not interchangeable.**

State events carry a **hybrid logical clock** in `origin_server_ts`:
`max(wall_clock_ms, last_issued + 1)`, advanced to any peer value observed
within a bounded drift (5 minutes in the reference implementation) and ignoring
anything beyond it. The bound matters as much as the clock: without it one node
with a dead RTC drags every node into the future and nothing can ever win again.
A receiver must advance its clock from a peer's timestamp **before** deciding
whether to accept the event, so that even a rejected write is accounted for.

Timeline PDUs carry a plain wall clock in `ts`. It is not an ordering key —
§8.2's DAG is.

Using a wall clock where the HLC belongs is not a subtle degradation: a
node-local counter was once used here, and because room creation used the wall
clock (~1.7 × 10¹²) while later edits used the counter (~10⁴), every edit to
state set at creation lost on every peer. The rename applied locally and
silently failed to converge.

## 9. The remaining channels

All JSON. All idempotent.

### 9.1 State — `<P>/<room_id>/state`

```json
{"event_type": "...", "state_key": "...", "sender": "@u:node",
 "content": {...}, "event_id": "...", "room_id": "!r:node",
 "origin_server_ts": 1755000000123}
```

**The catch-up reply on the same key expression is a different type** — see
§11.3 — carrying `{"events": [ <the above>, ... ]}`.

### 9.2 Typing — `<P>/<room_id>/typing`

```json
{"node_id": "<server_name of the sender>", "users": {"@u:node": 1755000000123}}
```

`users` maps user id to expiry in epoch milliseconds. A receiver replaces that
sender's contribution wholesale rather than merging into it.

### 9.3 Receipts — `<P>/<room_id>/receipt`

```json
{"user_id": "@u:node", "receipt_type": "m.read", "event_id": "$...", "ts": 1755000000123}
```

`receipt_type` is `m.read` or `m.read.private`.

### 9.4 To-device — `<P>/__to_device__/msgs`

```json
{"target_user": "@u:node", "target_device": "DEV1", "sender": "@s:node",
 "event_type": "...", "content": {...}, "msg_id": "..."}
```

`msg_id` is the dedup key. Every node receives every message; a node keeps only
those for users it hosts.

### 9.5 Device lists — `<P>/__device_list__/changes`

```json
{"user_id": "@u:node"}
```

A bare invalidation: "this user's device list changed, re-query it."

### 9.6 Claims — `<B>/claims`

The coordination-free barrier for the few genuinely non-monotone invariants
(username uniqueness, alias ownership). A grow-only set of:

```json
{"username": "<namespaced key>", "claimant": "<identity>",
 "ts": 1755000000123, "node_id": "<tiebreak id>"}
```

`username` is a namespaced key, not a bare name — `mx:username:<localpart>` and
`mx:alias:<full alias>` are the two namespaces in use.

Resolution is a pure function every node computes independently: among claims
for the same key, the winner is the lowest `(ts, node_id)`. It is deterministic,
order-independent and idempotent, so no node has to ask another who won.

**Conflict is detected on `node_id`, not `claimant`.** Two nodes registering the
same username produce the identical `claimant` string, so comparing claimants
makes the conflict invisible.

Republished periodically, because there is no replay.

## 10a. E2EE envelope — per-scope ChaCha20-Poly1305 encryption

Encrypted payloads are so that replicated CRDT data for one scope remains opaque
to nodes not participating in that scope.

### E2EE key derivation — `src/substrate/encrypted_crdt.rs:25:derive_scope_key`_
`derive_scope_key(psk)` = `SHA256(PSK || "mrgd-scope-key")` → 32-byte key for
ChaCha20-Poly1305.  Each scope gets its own key; cross-scope reads see only
ciphertext.

### `EncryptedCrdtSink` — `src/substrate/encrypted_crdt.rs:38:new`_
Wraps a `CrdtSink` and encrypts/decrypts every payload with ChaCha20-Poly1305.
`encrypt(plaintext)` generates a random 12-byte nonce, encrypts, and prepends the
nonce to ciphertext for transport (`encrypted_crdt.rs:52-67`).  `decrypt`
extracts the nonce (first 12 bytes) and decrypts (`encrypted_crdt.rs:70-80`).
Different scope keys cannot decrypt each other's ciphertext
(`encrypted_crdt.rs:150:different_scope_keys_cannot_decrypt`).

### CrdtSink impl — `encrypted_crdt.rs:83:publish` / `drain`_
`publish(key, bytes)` encrypts `bytes` then delegates to the inner sink
(`encrypted_crdt.rs:84-87`).
`drain(key)` pulls blobs from the inner sink and decrypts each (`encrypted_crdt.rs:89-96`).

## 10b. Selective PublicationSink — cross-scope federation publication

Selective sharing of CRDT data from one scope to another, by forwarding chosen
keys from a source scope's CRDT sink to a target scope's CRDT sink.

### `PublicationRule` — `src/substrate/publication.rs:17`_
A rule defining which keys to publish from source to target scope.
`source_pattern` supports `*` wildcard at end (`publication.rs:19-20`).
`target_prefix` prepended when forwarding (`publication.rs:21-23`).
`filter_contains` optional substring filter (`publication.rs:24-26`).
`matches(key)` returns true when key starts with the pattern prefix or equals it
(`publication.rs:30-33`).  `transform_key(key)` maps source key to target form
(`publication.rs:47-61`).

### `PublicationPolicy` — `src/substrate/publication.rs:65`_
Ordered list of rules; first match wins (`publication.rs:67-69`).
`match_rule(key)` finds the first matching rule (`publication.rs:82-84`).

### `PublicationSink` — `src/substrate/publication.rs:90:CrtdSink impl`_
`publish(key, bytes)`:
1. Always publishes to the **inner** (source) sink first
   (`publication.rs:125-127`).
2. If a rule matches, transforms the key and forwards to the **target** sink
   (`publication.rs:130-133`).
3. `drain(key)` passes through only to the inner sink (`publication.rs:138-139`).

### Presets — `src/substrate/publication.rs:144:presets`_
- `publish_all(prefix)`: match `*`, forward with prefix
- `publish_counters(target_prefix)`: match `counter/*`, forward with prefix
- `publish_orset(prefix, target_prefix)`: match `{prefix}/*`, forward with prefix
  (`publication.rs:148-172`).

## 10c. Bounded AntiEntropyPolicy — quiescence fix (2026-09-23)

The fix for the 2026-09-07..11 incident where ~200 claims/5s were
re-published forever, costing 9.5 GB RSS.  The policy grants a **bounded burst**
of eligible ticks after last observed activity, then goes silent.

### Policy structure — `src/substrate/barrier_growset.rs:369:AntiEntropyPolicy`_
`max_rounds: u32 = 3` (`barrier_growset.rs:337`), `rounds_left`, `last_own_hash`,
`initialized: bool`, `known_peers: HashSet<String>` (`barrier_growset.rs:369-377`).

### Burst granting rules — `barrier_growset.rs:397-429`_
- `note_own(own_hash)`: if hash changes (new local claim), grant fresh burst:
  `rounds_left = max_rounds` (`barrier_growset.rs:397-402`).
- `note_remote(new_records, remote_node_ids)`: only genuinely new information
  extends the burst.  A never-before-seen `node_id` (unknown peer) grants a burst
  so late/restarted peers can converge.  **Duplicate echo** (including the Zenoh
  self-echo of our own re-publish) does **not** extend the burst
  (`barrier_growset.rs:411-420`).  This is what lets two nodes ping-pong into
  silence instead of forever.
- `should_republish()`: returns `rounds_left > 0` (`barrier_growset.rs:423`).
- `did_republish()`: decrements `rounds_left` by 1 (`barrier_growset.rs:428`).

### Tick cycle — `barrier_growset.rs:540:tick`_
Every `REPUBLISH_EVERY_TICKS = 5` ticks: call `note_own` (own hash change),
then `should_republish`.  If true, re-publish own claims and decrement
`rounds_left`.  After each drain, call `note_remote` with newly seen records and
node IDs (`barrier_growset.rs:577-580`).
With unchanged state the burst runs out and this tick publishes **nothing at
all**, guaranteeing quiescence rather than a forever flood
(`barrier_growset.rs:545-550`).

### Republish own claims — `barrier_growset.rs:593:republish_own_claims`_
Re-publishes every `ClaimRecord` in the synced view belonging to this node
(`barrier_growset.rs:593-618`).  Idempotent on the receiver side: dedup in
`drain_and_reconcile` prevents duplicate conflict detection.

The burst decays to exactly zero instead of flooding forever — the fix for the
2026-09-07..11 incident.  Late-starting or restarted peers catch up via: (a) the
initial burst at startup, and (b) a fresh burst triggered by any new activity
own publish or remote records seen.

Values from the reference implementation. An implementation may choose others;
these are what the deployed mesh currently expects.

| Parameter | Value | Configurable as |
|---|---|---|
| Catch-up GET and per-reply budget | 2 s | — |
| Startup wait for a peer | 3 s (`0` disables) | `MATRIX_HS_CATCHUP_PEER_WAIT_MS` |
| Startup wait for a peer's signing key | 12 s | `MATRIX_HS_CATCHUP_KEY_WAIT_MS` |
| Periodic re-query | 300 s (`0` disables) | `MATRIX_HS_CATCHUP_INTERVAL_SECS` |
| Settle delay after the peer set grows | 6 s | `MATRIX_HS_CATCHUP_SETTLE_MS` |
| Media fetch | 5 s | `MATRIX_HS_MEDIA_FETCH_TIMEOUT_MS` |
| One-time-key claim fetch | 800 ms | — |
| Key announcement republish | 5 s | — |
| Claim republish | 5 s | — |
| Max accepted HLC drift | 5 min | — |
| Max accepted media reply | 50 MiB | `MATRIX_HS_MEDIA_MAX_BYTES` |

**Only media has a size bound on an accepted reply.** A history or state
catch-up reply is currently accepted at any size. An implementation exposed to
untrusted peers should impose its own.

A node **must wait for a peer's signing key before its first catch-up**. A delta
merged before the key lands is not deferred — every PDU in it fails
verification and is dropped, and the room converges empty while the log reads
like a forgery warning rather than a race.

## 11. Asymmetries and traps

Each of these is a way an independent implementation fails while looking
correct. They are recorded because none is derivable from the rest of the
document.

1. **Only the claim key encodes its segments** (§3.3). `room_id` — which also
   begins with a sigil and contains `:` — is embedded raw. Both behaviours must
   be reproduced exactly; the stated reason for the first does not explain the
   second.
2. **Endianness is not uniform.** Little-endian everywhere except the media
   reply (§7.3), which is big-endian.
3. **Two different payload types share `<P>/<room_id>/state`.** A published
   sample is one state event; a query *reply* is an object wrapping an array
   (§9.1). Zenoh keeps them apart because subscribers do not see query replies —
   an implementation that unifies the two paths will mis-parse one.
4. **Signature length prefixes are `u64`; transport length prefixes are
   `u16`/`u32`** for the same fields (§5.1 vs §6).
5. **`signer_node` precedes `sig`** in the transport framing (§6).
6. **Silence means "no"** for media and claims, but **absence from a reply set**
   means "no" for catch-up, and a known-but-empty room replies with a sentinel
   rather than staying silent (§7.1).
7. **The reply key is the routing authority**, not the payload's own `room_id`
   (§7.1).
8. **`allowed_origin(Remote)` is required**, not an optimisation (§3.2).
9. **The GC watermark is absent, not zero, on old senders** — and absent must
   read as zero rather than as a parse error (§6.1).
10. **A rejected state write still advances the clock** (§8.4).

## 12. Conformance

An implementation is interoperable when, against a reference node:

1. It joins the mesh and its key announcement is accepted, including under a
   pinned key set.
2. Events it publishes are accepted: signature valid, sender domain bound,
   `event_id` equal to the content address.
3. Events it receives are ordered identically to the reference node's ordering
   of the same set, delivered in a different order.
4. It survives a catch-up round from a node holding rooms it has never seen, and
   creates those rooms from the reply keys alone.
5. It converges on room state through the power-level gate of §8.3, including
   both carve-outs.
6. It honours a GC watermark from a peer and does not hand back collected
   events.
7. Every decoder in it is total: fed truncations at every offset, lying length
   prefixes, invalid UTF-8 and absurd counts, it drops the payload and stays up.

The cheapest useful first step needs no network at all: decode the vector in
§13. [`../scripts/wire_conformance.py`](../scripts/wire_conformance.py) does
exactly that, written from this document with the Rust source closed — which is
the only way the exercise proves anything, since a decoder checked against the
implementation that produced it proves nothing. Run it:

```bash
python3 scripts/wire_conformance.py
```

If an independent decoder cannot be written from this document, the document has
a gap, and the gap is the finding.

## 13. Test vector

One signed PDU on `<P>/<room_id>/events`, with **deliberately unsorted**
`prev_events` and a non-zero GC watermark — so that an implementation which
misses §5.1's sorting rule computes a different `event_id` and finds out here
rather than in production.

```
room_id      !room:node-a
sender       @alice:node-a
kind         m.room.message
content      {"body":"hi"}          (13 bytes, verbatim)
prev_events  ["$zzz", "$aaa"]       (this order on the wire; sorted for signing)
depth        4
ts           1700000000000
signer_node  node-a
collected_depth  2
```

Signing key: ed25519 seed `07` repeated 32 times, whose public key is

```
ea4a6c63e29c520abef5507b132ec5f9954776aebebe7b92421eea691446d22c
```

Expected content address:

```
$0syn2lgeSX0g3-FK53G83mWXiTAINqN_Zx9o2cPz1xw
```

The complete delta as published:

```
010000002c00243073796e326c676553583067332d464b35334738336d5758695441494e714e5f
5a78396f3263507a3178770c0021726f6f6d3a6e6f64652d610d0040616c6963653a6e6f64652d
610e006d2e726f6f6d2e6d6573736167650d0000007b22626f6479223a226869227d02000400247a
7a7a04002461616104000000000000000068e5cf8b01000006006e6f64652d61400096b0b13b2e99
a1964ce38276b5be738cf30c761f9750743426921e3c43cdadc9eeaa3c288b35708a0d17728a58b8
6da91190ca95f9643d9e9c7481ce71f100080200000000000000
```

(concatenate the lines; the wrapping is for this page only).

This vector is asserted byte-for-byte by the Rust test
`golden_vector_matches_the_spec` and decoded by the conformance script above.
**All three are one contract:** if the encoder changes, this section and the
script change in the same commit, and that commit is a wire break rather than a
refactor.
