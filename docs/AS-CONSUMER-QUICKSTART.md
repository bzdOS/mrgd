# AS Consumer Quickstart — application-service socket

**For:** operator / integrator connecting agents via application-service socket  
**Facts verified against** 557f907; **no automation implied**  
**Source:** ROADMAP §3 "Tenant gateway (Application-Service-shaped)", AGENT-USE-CASES.md Case 2

## Prereqs

- `MATRIX_HS_AS_TOKEN` — set to a valid application-service token [.env.example:75]
- `MATRIX_HS_AS_PREFIX` — defaults to `as_` when only token is set [.env.example:76]; unset = socket off
- Both are **OPTIONAL**; if either is missing, the AS socket is non-functional (no registration, no login)

## Consumer steps (each anchored to code routes + test case)

### a. Register tenant-MXID via AS bearer (UIA‑bypass, unusable password)

- **Route:** `POST /_matrix/client/v3/register` with AS bearer token (test :48)
- **Anchor:** `as_register_no_uia` test case (`src/as_socket_test.rs:64`)
- **Flow:**
  1. Client sends `POST /register` with `Authorization: Bearer <AS_TOKEN>` and desired `@tenant:domain`
  2. Server creates tenant account with unusable password (random hash), returns access_token
  3. The AS token is now bound to the tenant's prefix (enforced by namespace)
- **Result:** Tenant-MXID exists; AS token is the only way in (password login returns 403)

### b. Login type=m.login.application_service → passwordless per-device session

- **Route:** `POST /_matrix/client/v3/login` with AS token (test :149)
- **Anchor:** `as_login_passwordless_per_device` test case (`src/as_socket_test.rs:140`)
- **Flow:**
  1. Client sends login with AS token and a `device_id` (worker identifier)
  2. Server mints a passwordless session; the device is registered under the tenant
- **Result:** Worker logs in as a device of the tenant MXID; no password ever used

### c. Sync / send

- **Route:** `GET /_matrix/client/v3/sync` (from `src/routes/sync.rs:3`, test end-to-end with steps a+b)
- **After steps a+b:** the agent/device is registered and can sync room state, send PDUs
- **Anchor:** verified by `as_socket_test.rs` flow end-to-end; uses the standard Matrix v3 sync path

## Negative cases (what is rejected)

- `as_register_bad_token` — invalid/expired AS token returns error on registration
- `as_login_rejects_out_of_namespace` — AS token cannot mint sessions for accounts outside its prefix
- `as_account_no_password_login` — password login against an AS-created account returns 403 (password is unusable)
- `as_login_disabled_without_config` — without `MATRIX_HS_AS_TOKEN`/`MATRIX_HS_AS_PREFIX`, `m.login.application_service` is not available; falls to password path and fails
- `as_register_respects_invite_gate` — registration respects the invite gate; invites may block registration

## Tail — next on the plan

- **Key backup as handoff:** exists in `src/routes/room_keys.rs` (not `src/room_keys.rs`); general key backup infrastructure present, but **no handoff workflow** for "worker dies → successor receives keys" yet
- **Plural services:** config is currently single (`one AS_TOKEN` / `one AS_PREFIX`); to support multiple services would require changes to env, routing namespace, and auth — all beyond this quickstart

---

**No automation implied.** This doc records the factual code-anchored path for a consumer to register, login, and sync via the application-service socket, based on the existing code in 557f907.