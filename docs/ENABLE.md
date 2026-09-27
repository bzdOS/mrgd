# Enable matrix-hs runbook

**PREP:** This is not a solution — it is a documentation placeholder.

**Purpose:** Enable/disable the matrix-hs service via owner-controlled button in the hub.

**Owner button:** `enable matrix-hs` — toggles the matrix-hs service state.

**How it works:** When pressed, the button sets the service enable/disable state in the deployment configuration. The actual service restart / start / stop is handled outside this repo (systemd, init, or operator‑initiated actions).

**Runbook:** 

1. Verify current matrix-hs status: `systemctl status matrix-hs` (or equivalent). 
2. Press the owner button `enable matrix-hs` in the hub to record the intent. 
3. Restart the service: `systemctl restart matrix-hs` (or equivalent). 
4. Verify convergence: check that the node joins/re-joins the mesh as expected.

**Gate:** matrix-hs must be running and mesh-converged after the restart. If the node was previously in the mesh, it must re-converge. If it was down, it must come up cleanly.

**Notes:** 
- This is a docs-only task — no code changes, no behavioral changes. 
- The owner button records the intent; the actual service management is ops‑outside this repo. 
- After pressing the button, always verify the service is running and the mesh convergence gate passes.