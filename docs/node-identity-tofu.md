# Node identity and TOFU across a key-rotation window

How to get a clean key exchange between two nodes when one side has to be
restarted with a rotated transport secret, and how to tell afterwards that the
exchange actually happened instead of merely looking quiet.

## 0. Scope, and why this file uses placeholders

This document is written for a public repository. Everything that identifies a
particular deployment — host names, addresses, user home paths, rotation ids, PSK
values — is therefore written as a placeholder, and the concrete values live in
the operator's private channel. The code references are exact; the deployment
values are not, by design.

Placeholders used below:

| Placeholder | Meaning |
| --- | --- |
| `LOCAL_NODE` | server_name of the single-node stand under test (`MATRIX_HS_SERVER_NAME`, default `localhost`) |
| `REMOTE_NODE` | server_name of the peer node that federates with it |
| `$DATA_DIR` | persistence directory of a node (`MATRIX_HS_DATA_DIR`) |
| `$ENV_FILE` | the env file the launcher sources (`MATRIX_HS_LISTEN`, `MATRIX_HS_DATA_DIR`, `MATRIX_HS_SERVER_NAME`, `MATRIX_HS_ZENOH_LISTEN`, …) |
| `$BIN` | the `matrix-hs` binary the launcher execs |
| `$PSK_FILE` | transport-secret store read by the obfs transport |
| `$STAND_LOG` | the stand's service log — stdout of the launched process |
| `<obfs-endpoint>` | the obfs listen address of the stand (`MATRIX_HS_ZENOH_LISTEN`) |

## 1. Identity is final

* `REMOTE_NODE` was renamed on 2026-10-02 and the name is final. The peer's rooms
  became visible under the new name on the day of the rename, which is how the
  rename was verified. Do not rename it again inside a window: the room id
  embeds the server_name (`!<localpart>:<server_name>`), and both the journal
  file names and the signed sender domain are derived from it.
* `LOCAL_NODE` keeps its current name (the `localhost` default). The stand is the
  server side of the client test suites, so its name is part of their fixtures.
* A node's identity is the ed25519 key in `$DATA_DIR/node_ed25519.key`, not the
  name. Renaming a node does not change its key; deleting the key file does.

## 2. Which files are involved, and what is actually on disk

### 2.1 There is no TOFU store file

The trust-on-first-use store of peer keys is **in memory only**
(`src/substrate/node_auth.rs`, `NodeKeyStore`: "pure in-memory storage +
verification logic"). Nothing reads or writes a peer-key file, so there is
nothing to archive, rename or delete. Consequences that matter for a window:

* every process start begins with an empty store and re-learns peers from their
  announcements;
* the rejection counter you see in the log belongs to the *running process*, not
  to the disk. A restart is the only thing that clears it;
* the only durable trust input an operator can set is the environment anchor
  `MATRIX_HS_NODE_KEYS`, which is parsed, not stored: comma-separated
  `node_id=<64 hex chars>` pairs, whitespace around a pair ignored, and **any**
  malformed entry fails the whole list (the store then trusts only the node
  itself and refuses every peer — fail-closed, `src/state.rs`).

### 2.2 Files on each side

Both sides have the same layout; only the values differ.

| Path (as `$DATA_DIR/…`) | Format | Role in this procedure |
| --- | --- | --- |
| `node_ed25519.key` | exactly 32 raw seed bytes, created mode 0600, written atomically via `node_ed25519.key.tmp` + rename (`src/substrate/node_auth.rs`) | the node's identity — **preserve** unless a new identity is intended |
| `rooms/<sanitized_room_id>.jsonl` | one JSON object per line, one line per room event | durable peer state; where the peer's events land |
| `rooms/<sanitized_room_id>.pdumeta.jsonl` | signed-PDU metadata keyed by `event_id`: `sig` / `signer_node` / `prev_events` / `depth` | what the sender-binding check runs against on replay |
| `accounts.jsonl`, `aliases.jsonl` | one record per line | local client state, unrelated to TOFU |
| `room_key_versions.jsonl`, `room_key_data.jsonl` | one op per line, replayed in order | local E2EE key-backup state |
| `media/<sanitized_media_id>` (+ `.ct`) | raw bytes + content-type sidecar | local uploads |

`<sanitized_room_id>` is the percent-encoded room id: the forbidden set
`{! # : / \ * ? < > | "}` and NUL are percent-encoded and the result is truncated
at 200 bytes (`src/persist.rs`, `sanitize_filename`). A peer room therefore lands
under a name that no longer contains `!` or `:`.

Transport side, outside `$DATA_DIR`:

| Path | Format | Role |
| --- | --- | --- |
| `$PSK_FILE` | one line per key, `<key-id>:<64 hex chars>` | the transport secret for the obfs listener; a rotation replaces the line, so the **key-id changes** |
| `$ENV_FILE` | shell env assignments | `MATRIX_HS_ZENOH_LISTEN` carries the locator `obfs/<host>:<port>#obfs_psk_base64=<base64>` |
| `$STAND_LOG` | text | launcher banner, obfs acceptor heartbeats, TOFU decisions, catch-up results |

### 2.3 The wire format of a key announcement

A node publishes `(node_id, pubkey)` on the zenoh key sink, subject `announce`,
immediately at startup and every 5 s afterwards (anti-entropy — the transport
gives no replay). The blob is: `u16` little-endian length of the id, the id
bytes, then the 32-byte verifying key (`encode_key_announcement` /
`decode_key_announcement` in `src/substrate/node_auth.rs`). The channel carries
**no signature**: whoever announces an id first owns it. That is the race the
environment anchor exists to remove.

## 3. Two ways to get a clean exchange

### Variant A — fresh store on both sides (restart, keep the key files)

Stop both nodes, start both again, change nothing on disk. Each process comes up
with an empty in-memory store and learns the peer's *current* key from its first
announcement; `first-seen pubkey … trusted` appears once per peer, and no
`presented a DIFFERENT pubkey` line can follow for that id in that process.

Note what this does **not** mean: do not delete `$DATA_DIR` to "clear the store".
Deleting it also deletes `node_ed25519.key`, so the node comes up with a brand-new
identity, and its peer will then have to learn that new key through the same
unauthenticated channel. If the local room journals are the only thing in the way,
archive `rooms/` alone.

### Variant B — anchor the peer by hand (restart + `MATRIX_HS_NODE_KEYS`)

Keep both key files, capture the peer's announcement off the wire (or derive the
verifying key from its seed file), and pin it in `MATRIX_HS_NODE_KEYS` as
`REMOTE_NODE=<64 hex chars>`. The store is then a closed set: startup prints
`node auth: anchored to N configured node key(s) (TOFU disabled)`, and an
announcement from anyone not in the list is refused with `anchored: node_id … is
not in the configured key list`. A key that no longer matches the pin is still
rejected, so this also detects rotation instead of silently re-learning it.

### Recommendation

**Variant A for the window, Variant B immediately after it passes.**

Variant A is the minimal step that unblocks the exchange, and it is the only one
that can be executed inside the window without a second source of truth: the
anchor value cannot be read from the staged config by hand, it is applied by the
restart itself. Variant A alone leaves the race open — with two nodes on an
isolated network it is acceptable for one window, but it is not a state to keep.
Once the exchange is verified, take the peer's key from the traffic you just
watched, pin it, and re-run the same verification: the `first-seen` line
disappears and the startup line shows the anchored count. From then on the
identity is bound to a value the operator chose, not to whoever announced first.

## 4. Order of operations inside the window

The restart is the first step and everything else is verification around it. The
launcher (`scripts/stand-lift.sh`) performs, in one pass: the obfs listener on
`<obfs-endpoint>`, the transport-secret rotation (a new key-id line in
`$PSK_FILE`), and the binary from the current main. Its own gates are worth
knowing, because they decide whether the restart is safe at all:

* already-running stand → hard no-op, nothing is touched;
* a pid file naming a live process is never restarted underneath it;
* `MATRIX_HS_LISTEN` bound to a wildcard address is refused before `exec`;
* if the env file names hubd directories, the launcher refuses to start — a stand
  must never be wired to a live hub queue tree;
* a busy port with no live pid file is refused (it is not ours to take).

### 4.1 Before the restart (stand side)

Record the baselines — after the restart the counters restart from these numbers,
so write them down rather than remember them:

```sh
grep -c 'presented a DIFFERENT pubkey' "$STAND_LOG"      # rejection base
grep -c 'first-seen pubkey'            "$STAND_LOG"      # TOFU learns so far
grep -n  'stand-lift start'            "$STAND_LOG" | tail -1   # last boot marker
grep -c ''                             "$PSK_FILE"       # key lines before rotation
grep    '^zenoh-obfs-'                 "$PSK_FILE"       # key-id before rotation
```

Do not edit `$DATA_DIR`, `$PSK_FILE` or `$ENV_FILE` by hand, and do not archive
anything yet — an archive taken after the restart proves nothing about what the
new process loaded.

### 4.2 After the restart

```sh
# 1. the new process is up and serves
curl -s -o /dev/null -w '%{http_code}\n' http://<listen-host>:<listen-port>/_matrix/client/versions
# 2. its own key was self-learned (expected on EVERY boot, not evidence of a wipe)
grep 'first-seen pubkey' "$STAND_LOG" | tail -2
# 3. the peer was learned exactly once, with no mismatch
grep 'first-seen pubkey for node "REMOTE_NODE"' "$STAND_LOG"
grep 'presented a DIFFERENT pubkey'  "$STAND_LOG" | awk -F: '$1 > <boot-marker-line>'
# 4. the exchange actually happened
grep 'state catch-up for ' "$STAND_LOG" | tail -5      # applied must be > 0
grep 'catch-up merge room=' "$STAND_LOG" | tail -5     # rejected must be 0
# 5. the rotation landed
grep '^zenoh-obfs-' "$PSK_FILE"                         # key-id differs from 4.1
nc -z <obfs-endpoint>                                  # exit 0
```

Reading the results:

* `applied > 0` is the only line that proves the peer's events were accepted
  under the current key. `applied 0, skipped N` with no rejection line means the
  peer had nothing new — quiet, not broken;
* `rejected=… failed signature/sender-binding verification` must be zero *after*
  the boot marker line. Older lines belong to the previous process and are the
  baseline, not a new fault;
* `first-seen pubkey for node "LOCAL_NODE"` on every boot is normal: a node
  always trusts its own key, and the store it inserts into is empty. Reading it
  as "the store was wiped" is a mistake — an empty store at boot is the normal
  case, not an event;
* the key-id in `$PSK_FILE` changing is the proof that the rotation was applied;
  the value itself is never logged, printed or committed.

### 4.3 If the exchange still does not happen

Stop and report; do not "fix" it by deleting state. The two discriminating cases:

* `presented a DIFFERENT pubkey` for `REMOTE_NODE` → both processes are running
  but disagree about the key: the peer rotated its `node_ed25519.key`, or two
  processes are sharing one `$DATA_DIR`. The anchor (Variant B) makes this
  explicit instead of intermittent;
* `anchored: node_id … refused` → the anchor list is in effect and does not
  contain the announcing id; the list, not the network, is what needs changing.

## 5. Boundaries

* Never run this against a live hub queue tree. The launcher refuses an env that
  names hubd directories; keep that refusal in place and never wire a stand to
  live queues.
* Never listen outward. The launcher's wildcard check stays; the obfs endpoint is
  reachable from the isolated lab network only.
* PSK values — the current one and the rotated one — never appear in this
  document, in a diff, in a commit message or in a log line. Only the key-id and
  the file format are shareable.
* The peer's key hex is not a secret in the same sense, but it is deployment
  identity: it belongs in the operator's channel until Variant B pins it.