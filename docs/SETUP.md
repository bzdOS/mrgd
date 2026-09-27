# Setup — building & running matrix-hs / mrgd on a new server

This is the transfer-and-develop guide — it covers getting the thing to build
and run, and nothing about what it is for. For that, and for the plan, see
[`../ROADMAP.md`](../ROADMAP.md) §"What this is for"; for architecture see
[`../ARCHITECTURE-boundaries.md`](../ARCHITECTURE-boundaries.md). Runtime config
template: [`../.env.example`](../.env.example).

## 1. System prerequisites

- **Rust** ≥ 1.85 (edition 2021). `rust-toolchain.toml` pins `1.96.0` — if you
  use rustup, `cargo` auto-installs it on first run. Plain rustc must match or
  newer. Verify: `rustc --version`.
- **A C compiler** (`cc`/`gcc`/`clang`) + `make` — required because the Lua
  scripting layer (`mlua`) builds Lua 5.4 from source (`vendored` feature). No
  system `liblua` needed.
  - Fedora/RHEL: `dnf install gcc make`
  - Debian/Ubuntu: `apt install build-essential`
- No other system libraries. All crypto (argon2, sha2/1, ed25519), image
  decoding (jpeg/png), and HTTP are pure-Rust crates.

## 2. Build

```bash
cargo build                        # single-node binary (no network transport)
cargo build --features cluster     # multi-master: enables Zenoh CRDT replication
```

The first build downloads and compiles ~250 crates (incl. vendored Lua). Expect
a few minutes on a cold cache.

## 3. Verify

```bash
cargo test                         # substrate + HTTP handler tests (single crate)
cargo test --features cluster      # + cluster-gated tests
```

These must be fully green before any development. Baseline re-measured
2026-08-20: **256 tests** with `--features cluster`, **221** without.

On a host that is *also running* a matrix-hs node — Alpha is, since 2026-08-19 —
stop it first (`systemctl stop matrix-hs`). The cluster tests open Zenoh sessions
with default scouting and will otherwise discover the running node, which makes a
handful of them fail with a different set each run. See the agent handoff record (kept private).

## 4. Configure

```bash
cp .env.example .env
$EDITOR .env                       # at minimum set MATRIX_HS_TOKEN_SECRET
```

`MATRIX_HS_TOKEN_SECRET` should be generated (`openssl rand -hex 32`). Without
it the server starts but every restart invalidates all access tokens. For a
single-machine dev run, all other vars can stay at defaults.

## 5. Run

```bash
# in-memory, single-node (reads .env if you source it, or export the vars)
export $(grep -v '^#' .env | xargs) && cargo run --bin matrix-hs

# or, with explicit listen:
MATRIX_HS_LISTEN=127.0.0.1:8448 cargo run --bin matrix-hs
```

Server logs `matrix-hs listening on http://…`. Point a Matrix client at
`http://<host>:8448` (server_name `localhost` unless overridden). For a public
hostname, set `MATRIX_HS_SERVER_NAME` and put a TLS-terminating reverse proxy
(Caddy/nginx) in front — the CS-API is plain HTTP.

## 6. Paths that are machine-specific (transfer caveats)

These reference the original deploy host and will NOT exist on a new server —
they are harmless defaults, not requirements:

| Location | What | Effect if absent |
|---|---|---|
| `MATRIX_HS_SCRIPTS_DIR` default `./scripts` | legacy Lua scripts path, relative to the working directory | set the env var when the server runs from elsewhere |
| E2E scripts (`scripts/e2e_*.sh`) | require `ADB_DEVICE`, `E2E_PASSWORD`, `HOMESERVER`; `CADDY_LOG` defaults under `$MATRIX_HS_HOME` | only affect the FluffyChat E2E harness, not the build |
| `vendor/zenoh-util-freebsd/` | FreeBSD zenoh-util stubs (see DESIGN.md) | build-time, FreeBSD only |
| `ROADMAP.md`'s companion-project path | companion project (not in this repo) | documentation only |

## 7. Transfer methods

- **`git bundle`** (preserves full history, no remote needed):
  `git bundle create /tmp/mrgd.bundle --all` then on the new host
  `git clone /tmp/mrgd.bundle mrgd`.
- **Push to a remote** then clone.
- Either way `Cargo.lock` is committed (reproducible deps) and
  `.env` / data dirs are gitignored, so secrets never travel with the repo.
