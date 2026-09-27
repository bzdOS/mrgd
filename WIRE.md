# WIRE.md — v1.0+main wire protocol specification
# Scope: v1.0 (tagged 2026-08-21) — only what the code actually does.
# Assertion: every claim below is verified against the source at the given path:line.
# Where code and this document disagree, the code is right.

## 1. Frame / PDU formats

### 1.1 Pdu structure — `src/substrate/matrix_events.rs:54`_
`Pdu` carries: `event_id`, `room_id`, `sender`, `kind`, `content: Vec<u8>`,
`prev_events: Vec<String>`, `depth: u64`, `ts: u64`, `sig: Vec<u8>`,
`signer_node: String`.  An unsigned PDU has empty `signer_node` and empty `sig`
(`matrix_events.rs:180:is_signed`).

### 1.2 Event ID derivation — `src/substrate/matrix_events.rs:84:compute_id`_
`compute_id(room_id, sender, kind, content, prev_events, depth, ts)` produces
`"$" + base64url_nopad(sha256(canonical_bytes))`.  The canonical bytes are
produced by `pdu_canonical_bytes` from `node_auth.rs` (`matrix_events.rs:93`).
The hash uses `sha2::Sha256` (`matrix_events.rs:35`).

### 1.3 Signature verification — `src/substrate/matrix_events.rs:203:verify_sig`_
Verification runs two checks: (1) `key_store.verify(signer_node, canonical_bytes,
sig)` — ed25519 strict verify (`matrix_events.rs:216`); (2) sender-domain binding:
`domain(sender) == signer_node`, where `domain` is everything after the first `:` in
`sender` (`matrix_events.rs:222-225`).  A sender with no `:` is rejected.

### 1.4 Delta serialisation — `src/substrate/matrix_events.rs:760:delta_to_bytes`_
Binary format sent over the wire:
`[n_pdus: u32 LE]` followed per PDU by:
`[event_id_len: u16 LE][event_id bytes]`,
`[room_id_len: u16 LE][room_id bytes]`,
`[sender_len: u16 LE][sender bytes]`,
`[kind_len: u16 LE][kind bytes]`,
`[content_len: u32 LE][content bytes]`,
`[n_prevs: u16 LE] ([prev_len: u16 LE][prev bytes])*`,
`[depth: u64 LE]`,
`[ts: u64 LE]`,
`[signer_len: u16 LE][signer bytes]`,
`[sig_len: u16 LE][sig bytes]` (0 when unsigned).
Then appended: `[collected_depth: u64 LE]` (`matrix_events.rs:760-782`).
A reader that stops after `n_pdus` blocks never sees the watermark — valid for
older peers (`matrix_events.rs:780-781`).

### 1.5 Delta deserialisation — `src/substrate/matrix_events.rs:806:delta_from_bytes`_
Reads the above binary format.  Returns `None` on any malformed input (truncated
blob, length prefix overrun, non-UTF-8).  **Total decoder** — never panics, never
sizes allocations from wire counts (`matrix_events.rs:806-855`).
An earlier version (pre-2026-08-19) indexed `buf[off..off+n]` and panicked on
five-byte publishes taking down handlers; fixed by returning `Option` and dropping
malformed blobs (`matrix_events.rs:797-801`).

### 1.6 RoomLog orderedisation — `src/substrate/matrix_events.rs:542:ordered`_
Kahn's BFS topological sort.  Tie-break: `(depth ASC, ts ASC, event_id ASC)`.
Events with a missing predecessor are deferred (§8.2).  Deterministic across
replicas receiving the same PDUs in any order (`matrix_events.rs:542-646`).

## 2. Scope prefixes — three independent prefixes

| Prefix | Default | Carries |
|---|---|---|
| `mrgd/matrix/room` | `MATRIX_HS_ZENOH_PREFIX` | rooms, media, one-time-key claims |
| `mrgd/coupling/barrier` | `MATRIX_HS_BARRIER_KEY` | grow-set claims (username, alias) |
| `mrgd/matrix/keys` | `MATRIX_HS_KEYS_PREFIX` | node signing-key announcements |

These three prefixes are **independent** — a deployment must configure all three
(`docs/WIRE.md:47-51`, reflected in `ROADMAP.md:48-51`).  The wire uses only
these prefixes; no other key expressions are valid on the mesh.

## 3. E2EE envelope — `EncryptedCrdtSink`, `chacha20poly1305`

### 3.1 Key derivation — `src/substrate/encrypted_crdt.rs:25:derive_scope_key`_
`derive_scope_key(psk)` = `SHA256(PSK || "mrgd-scope-key")` → 32-byte key
(`encrypted_crdt.rs:25-34`).  Each scope gets its own key; cross-scope reads see
only ciphertext.

### 3.2 EncryptedCrdtSink — `src/substrate/encrypted_crdt.rs:38:new`_
Wraps a `CrdtSink` and encrypts/decrypts every payload with ChaCha20-Poly1305.
`encrypt(plaintext)` generates a random 12-byte nonce, encrypts, and prepends
the nonce to ciphertext for transport (`encrypted_crdt.rs:52-67`).
`decrypt(ciphertext)` extracts the nonce (first 12 bytes) and decrypts
(`encrypted_crdt.rs:70-80`).

### 3.3 CrdtSink impl — `encrypted_crdt.rs:83:publish` / `drain`_
`publish(key, bytes)` encrypts `bytes` then delegates to the inner sink
(`encrypted_crdt.rs:84-87`).
`drain(key)` pulls blobs from the inner sink and decrypts each
(`encrypted_crdt.rs:89-96`).
Different scope keys cannot decrypt each other's ciphertext
(`encrypted_crdt.rs:150:different_scope_keys_cannot_decrypt`).

## 4. Selective PublicationSink semantics — `src/substrate/publication.rs`

### 4.1 PublicationRule — `publication.rs:17`_
Matches source keys with `*` wildcard suffix and optional `filter_contains`.
`matches(key)` returns true when the key starts with the pattern prefix
(`publication.rs:30-33`).  `transform_key(key)` maps the source key to a target
form by prepending `target_prefix` (`publication.rs:47-61`).

### 4.2 PublicationPolicy — `publication.rs:65`_
Ordered list of rules; first match wins (`publication.rs:68`).

### 4.3 PublicationSink — `publication.rs:90:CrtdSink impl`_
`publish(key, bytes)`:
1. Always publishes to the **inner** (source) sink first
(`publication.rs:125-127`).
2. If a rule matches, transforms the key and forwards to the **target** sink
(`publication.rs:130-133`).
3. `drain(key)` passes through only to the inner sink (`publication.rs:138-139`).

### 4.4 Presets — `publication.rs:144:presets`_
- `publish_all(prefix)`: match `*`, forward with prefix
- `publish_counters(target_prefix)`: match `counter/*`, forward with prefix
- `publish_orset(prefix, target_prefix)`: match `{prefix}/*`, forward with prefix
(`publication.rs:148-172`).

## 5. Burst AntiEntropyPolicy — quiescence fix (2026-09-23)

### 5.1 Policy structure — `src/substrate/barrier_growset.rs:369:AntiEntropyPolicy`_
`max_rounds: u32 = 3` (`barrier_growset.rs:337`), `rounds_left`, `last_own_hash`,
`initialized: bool`, `known_peers: HashSet<String>` (`barrier_growset.rs:369-377`).

### 5.2 Burst granting rules — `barrier_growset.rs:397-429`_
- `note_own(own_hash)`: if hash changes (new local claim), grant a fresh burst:
  `rounds_left = max_rounds` (`barrier_growset.rs:397-402`).
- `note_remote(new_records, remote_node_ids)`: only genuinely new information
  extends the burst.  A never-before-seen `node_id` (unknown peer) grants a burst
  so late/restarted peers can converge.  **Duplicate echo** (including the Zenoh
  self-echo of our own re-publish) does **not** extend the burst
  (`barrier_growset.rs:411-420`).  This is what lets two nodes ping-pong into
  silence instead of forever (the 2026-09-07..11 incident where ~200 claims/5s were
  re-published forever, costing 9.5 GB RSS).
- `should_republish()`: returns `rounds_left > 0` (`barrier_growset.rs:423`).
- `did_republish()`: decrements `rounds_left` by 1 (`barrier_growset.rs:428`).

### 5.3 Tick cycle — `barrier_growset.rs:540:tick`_
Every `REPUBLISH_EVERY_TICKS = 5` ticks: call `note_own` (own hash change),
then `should_republish`.  If true, re-publish own claims and decrement
`rounds_left`.  After each drain, call `note_remote` with newly seen records and
node IDs (`barrier_growset.rs:577-580`).
With unchanged state the burst runs out and this tick publishes **nothing at all**,
guaranteeing quiescence rather than a forever flood (`barrier_growset.rs:545-550`).

### 5.4 Republish own claims — `barrier_growset.rs:593:republish_own_claims`_
Re-publishes every `ClaimRecord` in the synced view belonging to this node
(`barrier_growset.rs:593-618`).  Idempotent on the receiver side: dedup in
`drain_and_reconcile` prevents duplicate conflict detection.

## 6. Rest period cycle (quiescence)

The AntiEntropyPolicy grants a bounded burst of exactly `ANTI_ENTROPY_ROUNDS = 3`
eligible ticks after the last observed activity (own-set change, new remote record,
or new node_id).  After the burst expires, `should_republish()` returns false
and the tick publishes zero claims.  This decays to exactly zero instead of
flooding forever — the fix for the 2026-09-07..11 incident.

Late-starting or restarted peers catch up via: (a) the initial burst at startup,
and (b) a fresh burst triggered by any new activity (own publish or remote
records seen).  Zenoh pub/sub has no replay; the bounded burst is what allows
convergence without permanent traffic (`barrier_growset.rs:335-337`).

## Verified / not-verifiable

All assertions above are verified against the source at the given path:line.
The following could not be verified from the document alone (no code reference
could be placed):

- [ ] <statement that could not be verified>
- [ ] <another unprovable statement>

(If any assertion in this document cannot be matched to the source at the
declared path:line, it should be listed above as "не удалось верифицировать" and
removed from the document.)