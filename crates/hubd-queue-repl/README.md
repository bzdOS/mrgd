# hubd-queue-repl

Native Zenoh replication of hubd team queues. `hub queue send <role>` appends a
block to `<teamroot>/queues/<role>.queue.md`; this daemon watches that dir,
publishes every locally-originated append over Zenoh, and applies remote appends
received from peers — so a queue entry written on one node appears on every other
node within ~1s, **without git mesh-sync**.

Transport follows the `ZenohCrdtSink` pattern (`couplingd::crdt`): a stock
`zenoh::Session` + a wildcard subscriber on a keyexpr prefix. Queue routing
metadata is encoded in the keyexpr so the payload is the raw appended bytes:

```
hubd/queues/<role>/<origin_node>/<seq>
```

Loop prevention: when this daemon applies a remote append, it records the chunk's
sha256 in a suppress set *first*; the local dir-watch then sees the resulting file
growth, finds the hash in the set, and skips re-publishing. A receiver-side
suffix-check makes delivery idempotent across restarts and re-deliveries.

## Build

```
cargo build -p hubd-queue-repl --release   # → target/release/hubd-queue-repl
```

## Security model (task #8) — MANDATORY

> **This rule is system-wide, not local to this daemon.** It applies to every
> Zenoh-carrying component, `matrix-hs` included. Written up as the normative
> transport ladder in `/opt/mrgd/ARCHITECTURE-boundaries.md`
> §"East–west vs north–south"; measurements in `/opt/mrgd/docs/TOPOLOGY.md`.
>
> **Known violation (2026-08-03):** the deployed matrix-hs env
> (`/srv/bsdos/artefacts/matrix-hs-bridge.env`) sets
> `MATRIX_HS_ZENOH_CONNECT=tcp/203.0.113.11:7448,tcp/203.0.113.12:7448` —
> plaintext Zenoh aimed at public IPs, exactly what this section forbids. It is
> also moot in practice: both ports measure **filtered**, while ssh is open on
> both hosts. That measurement is the empirical basis for ranking `ssh -L` above
> HTTPS in the ladder — this daemon's transport choice was right, and matrix-hs
> should adopt it rather than the reverse.

Nodes live on **untrusted** networks (buildhost public IP, fedora behind NAT, mac,
internet). Zenoh **MUST NOT** carry plaintext on public interfaces. Both
`HUBD_QUEUE_ZENOH_LISTEN` and `HUBD_QUEUE_ZENOH_CONNECT` default to **empty**
(peer/scouting, localhost/LAN only). For cross-node replication use ONE of:

### (A) ssh `-L` tunnel (autossh) — recommended minimum

Each node binds Zenoh on `127.0.0.1` only; a peer forwards its localhost port over
an encrypted ssh tunnel and the local daemon CONNECTs to its own `127.0.0.1:<port>`
(the tunnel's near end).

**buildhost** (public, accepts inbound) — listen:
```
HUBD_QUEUE_ZENOH_LISTEN=tcp/127.0.0.1:7449   # infra/systemd/hubd-queue-repl.service
```

**fedora** (behind NAT, connects out) — tunnel + connect:
```
autossh -N -f -L 7449:127.0.0.1:7449 root@203.0.113.10     # fedora → buildhost
HUBD_QUEUE_ZENOH_CONNECT=tcp/127.0.0.1:7449
```
Zenoh traffic fedora→buildhost now rides ssh (encrypted); neither side exposes a
plaintext Zenoh port to the internet.

> **ТСПУ/DPI caveat:** on some ISPs `ssh -L` is filtered (see task #168,
> Zenoh-over-ssh-exec). Fall back to (B) or the obfs link.

### (B) Zenoh TLS/QUIC locators with certs

Use `tls/<host>:<port>` endpoints with mutual certs/PSK (the `transport_tls`
feature is already in the workspace; cf. `bsdos-core` F2 mTLS, task #87). No ssh
tunnel needed; Zenoh itself is encrypted.

## Config (env)

| var | default | note |
|---|---|---|
| `HUBD_TEAM_DIR` | walk-up from CWD | team root containing `queues/` |
| `HUBD_QUEUE_NODE_ID` | `/etc/hostname` | origin tag (must be unique per node) |
| `HUBD_QUEUE_ZENOH_LISTEN` | *(empty)* | zenoh listen endpoints (localhost!) |
| `HUBD_QUEUE_ZENOH_CONNECT` | *(empty)* | zenoh connect endpoints (localhost tunnel / tls) |
| `HUBD_QUEUE_KEY_PREFIX` | `hubd/queues` | zenoh keyexpr prefix |
| `HUBD_QUEUE_POLL_MS` | `1000` | dir-watch poll interval |

## Proof (two-instance, secure localhost)

Mechanism verified bidirectional + loop-free with two instances on one host over a
localhost Zenoh link (the ssh-tunnel pattern minus the ssh hop): `hub queue send`
on side A → block appears on side B within ~1s; reverse direction identical; files
stay stable after delivery (suppress-set breaks the feedback loop). Cross-node
physical run = same binary + the secure transport above.
