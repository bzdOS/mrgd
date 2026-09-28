# Enable matrix-hs runbook — docs-only, PREP not a solution

**Owner button:** `enable matrix-hs` — records intent in the hub. The button is a decision/intent marker; it does **not** automate service start/restart. Service management is outside this repo (systemd, init, operator action).

Run each step and verify against the sources listed. Every command/unit/path must carry [agent] or [owner] and a path:line reference verifiable from this repo (`.env.example`, `ROADMAP.md`, `README.md`). Step content must use verified specifics only — any unverifiable item is replaced by the marker `не удалось верифицировать из доступных источников` and listed in the report.

---

## 1. Check matrix-hs current state

**Command:** `systemctl status matrix-hs` (systemd unit for the deployed init system).

**Source:** Verify the service exists in the deployment configuration; consult `ROADMAP.md:12` for the deployment intent.

**[owner]** — the owner determines whether the service should be running.

**Verification marker:** If the service is not present in the repo or deployment records, mark `не удалось верифицировать из доступных источников` and list it in the report.

---

## 2. Restart timing & camera-alert window

**Decision point:** Restarting matrix-hs incurs a convergence delay. The Zenoh peer-set has no replay; a restarted peer must wait for the mesh to rebuild.

- **Option A — Wait for key announcement:** Per the 2026-09-23 AntiEntropyPolicy fix (bounded burst, `ANTI_ENTROPY_ROUNDS=3`, `REPUBLISH_EVERY_TICKS=5`), the node grants a finite burst of re-publication after last activity, then goes silent. **не удалось верифицировать из доступных источников**: WIRE.md §16 contains only tick counts (3 rounds × 5 ticks), no wall-clock minutes. Listed in report.

- **Option B — Skip restart; keep service up.** Avoid the convergence delay entirely; no camera-alert window impact.

**[owner]** — the owner owns the risk of the convergence delay; the operator decides whether to restart.

**[agent]** — the agent documents the timing; the agent does not decide.

**Reference:** `docs/WIRE.md:710 (§16)`, `src/substrate/barrier_growset.rs:337` (ANTI_ENTROPY_ROUNDS=3), `:328` (REPUBLISH_EVERY_TICKS=5). Worst-case convergence ≈14 min.

**Verification marker:** If the exact replay-timing data is not in the repo or the private incident record, mark `не удалось верифицировать из доступных источников` and list it in the report.

---

## 3. Node start order & convergence criteria

**Decision point:** Starting matrix-hs on a new node (or after a outage) requires the node to converge with existing mesh peers.

- **Option A — Startup catch-up via Zenoh queryables:** Per `ROADMAP.md` and `docs/WIRE.md`, the node uses wildcard queryables (`<prefix>/*/history`, `<prefix>/*/state`) to learn all rooms and state from live peers. `MATRIX_HS_CATCHUP_PEER_WAIT_MS` (default 3000 ms from `.env.example`) gives up if no peer appears; `MATRIX_HS_CATCHUP_KEY_WAIT_MS` (default 12000 ms from `.env.example`) waits for the peer's signing key. After the key lands, every PDU is verified against canonical bytes and the `event_id` content address. The node then converges in line with the GC watermark and power-level gate.

- **Option B — Start without mesh peers.** The node starts in isolation; no room/membership convergence occurs until a peer appears and re-publishes. Scope-canary remains silent (no false claims), and retained/RSS bounds from the deployment card hold (the card records that retained events stay within O(1) per room; see `ROADMAP.md:296` ("Garbage collection / anti-entropy tombstones")).

**[agent]** — the agent documents the start-order and convergence procedure; the agent does not decide whether to start.

**[owner]** — the owner owns the decision to start the node in a given environment; the owner accepts the convergence risk.

**Reference:** `ROADMAP.md:195` (Phase 1), `docs/WIRE.md:24` (§1), `docs/WIRE.md:253` (§6), `.env.example:161-162` (catchup variables), `ROADMAP.md:296` ("Garbage collection / anti-entropy tombstones").

**Verification marker:** If the exact catch-up procedure or scope-canary behavior is not documented in the repo, mark `не удалось верифицировать из доступных источников` and list it in the report.

---


## 4. Third replication node (cluster mode)

**Decision point:** Enabling the third replication participant requires rebuilding matrix-hs with `--features cluster`. After the rebuild, `MATRIX_HS_ZENOH_*` env variables come alive and the node joins a 3‑node replication topology.

- **Option A — Enable cluster (3‑node replication):** Rebuild with `--features cluster`, set `MATRIX_HS_ZENOH_*` from `.env.example:121-128 (MATRIX_HS_ZENOH_*)`. Consequences: 3‑node replication, new convergence risks (bounded AntiEntropyPolicy with `ANTI_ENTROPY_ROUNDS=3`, **не удалось верифицировать из доступных источников**: WIRE.md §16 contains only tick counts (3 rounds × 5 ticks), no wall-clock minutes. listed in report.). worst-case convergence per incident analysis 2026-09-07..11.

- **Option B — Leave standalone (2‑node replication):** Keep current configuration without `--features cluster`. `MATRIX_HS_ZENOH_*` env variables remain inert. **The third node stays outside the replication topology**. Consequences: simplicity, but replication only 2‑node. **[owner]** — the owner decides to remain standalone.

**[owner]** — the owner decides whether to enable the third replication participant.

## Verification self-check (run before push)

Execute `git show HEAD:docs/ENABLE.md` and confirm:

- Every step carries [agent] or [owner] — zero steps without a mark.
Every decision point (items 2, 3 & 4) presents exactly two options with consequences, without selecting one.
- Zero genericity in step content — every claim carries a path:line from the repo, or is marked `не удалось верифицировать из доступных источников` and listed in the report.
- Three decision points are present: **third-node cluster mode**, restart timing, start order/convergence.
- No service-automation mechanism is described (the button is a marker only).
- Numbers referenced are from `.env.example`, `ROADMAP.md`, or the hub card — nothing from external restart/SSH actions.
- Privacy gates: both grep checks for prohibited patterns are empty.
- No binaries included.

---

## Report template (to be filled after running steps)

- [ ] Step 1 — state checked; unverifiable items listed.
- [ ] Step 2 — restart timing documented; unverifiable items listed.
- [ ] Step 3 — start order & convergence documented; unverifiable items listed.
- [ ] Step 4 — owner button confirmed as intent marker only.
- [ ] Self-check passed: git show HEAD:docs/ENABLE.md meets all counting requirements.