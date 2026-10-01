#!/usr/bin/env bash
# AS Consumer Walkthrough — application-service socket consumer, end to end.
# Every step is CHECKED (HTTP status + the field the step is about), not merely
# attempted, and the exit code names the step that failed.
#
# Steps, with the code each one is anchored to:
#   a.  register the tenant localpart under the AS namespace, no UIA
#       POST /_matrix/client/v3/register
#         body {"username":"<localpart>"} + Authorization: Bearer <AS_TOKEN>
#       [src/as_socket_test.rs:47-52 · src/routes/register.rs:199-229]
#       Checks: 200, and user_id == "@<localpart>:<server_name>".
#   b1. first worker device logs in, passwordless
#       POST /_matrix/client/v3/login
#         body {"type":"m.login.application_service","user_id":"@<lp>:<sn>",
#               "device_id":"<dev>"} + Bearer <AS_TOKEN>
#       [src/as_socket_test.rs:147-157 · src/routes/login.rs:79-124]
#       Checks: 200, non-empty access_token, device_id round-trips.
#   b2. second worker device, same account, same AS bearer → same shape.
#   c.  the two device tokens are DISTINCT — each worker is its own session,
#       which is the whole point of workers-as-devices
#       [src/as_socket_test.rs:164 assert_ne!(tokens[0], tokens[1])]
#   d.  create a room with a DEVICE token
#       POST /_matrix/client/v3/createRoom  [src/routes/rooms.rs:3 · src/lib.rs:333]
#       Checks: 200, non-empty room_id.
#   e.  sync with a device token
#       GET /_matrix/client/v3/sync  [src/routes/sync.rs:3 · src/lib.rs:340]
#       Checks: 200, parseable JSON, and the room from step d present in
#       .rooms.join — an unauthenticated caller gets an empty map
#       [src/routes/sync.rs:266-268].
#   f.  send a message
#       PUT /_matrix/client/v3/rooms/{roomId}/send/m.room.message/{txnId}
#       body {"msgtype":"m.text","body":"..."} — no "content" wrapper, no ?auth
#       [src/routes/send.rs:3 · src/lib.rs:335-338]
#       Checks: 200, event_id starting with "$".
#
# Auth rule (the first revision of this script broke it): the AS bearer is a
# credential for /register and /login ONLY [src/routes/register.rs:206-229,
# src/routes/login.rs:79-92]. From step d on the MINTED device tokens are the
# credentials, and the difference is measurable, not cosmetic:
#   · createRoom and send verify a signed mxt_ token — the AS bearer gets 401
#     [src/routes/rooms.rs:474-483 · src/routes/send.rs:318-327]
#   · sync does NOT 401 on a bad bearer: it answers as an anonymous caller and
#     returns an EMPTY rooms.join [src/routes/sync.rs:266-268], and a room is
#     listed only when the CALLER's own m.room.member is join [:288-295]. So a
#     walkthrough that syncs with the AS bearer gets a green 200 that shows the
#     consumer nothing at all — which is exactly the failure being fixed.
#
# server_name is NOT the host:port in BASE_URL. "127.0.0.1:8448" is a socket
# address, not a Matrix server name, and MXIDs are built from the server name.
# It is read from the environment the same way the server reads it:
# MATRIX_HS_SERVER_NAME, default "localhost" [src/main.rs:195].
#
# Usage:
#   MATRIX_HS_AS_TOKEN=my_token ./scripts/as_consumer_walkthrough.sh
#   MATRIX_HS_AS_TOKEN=my_token SERVER_NAME=hubd.net \
#     BASE_URL=http://127.0.0.1:8448 ./scripts/as_consumer_walkthrough.sh
#   MATRIX_HS_AS_TOKEN=my_token DRY_RUN=1 ./scripts/as_consumer_walkthrough.sh
#
# Exit codes (one per step, so a failure names itself):
#   0 — all steps passed
#   1 — step a  (register) failed
#   2 — step b1 (first device login) failed
#   3 — step b2 (second device login) failed
#   4 — step c  (device tokens are not distinct) failed
#   5 — step d  (createRoom) failed
#   6 — step e  (sync) failed
#   7 — step f  (send) failed
#   8 — usage / environment error
#   130 / 143 — stopped by SIGINT / SIGTERM; the traps end the run with the
#               signal's own status so an abort is not reported as a step
#               failure. These two sit outside 0-8 deliberately: they are
#               128+n, not a step.
#
# Env:
#   MATRIX_HS_AS_TOKEN  required — the AS bearer. Unset = agent socket off
#                      [.env.example:75]
#   MATRIX_HS_AS_PREFIX localpart namespace, default "as_" [.env.example:76]
#                      — must match the server's own value [src/state.rs:1290-1300]
#   AS_LOCALPART        localpart to register, default "<prefix>walkthrough<unix
#                      time>" so the walkthrough is repeatable against a
#                      long-lived server. Pin it to a fixed name and the second
#                      run stops at step a with M_USER_IN_USE / exit 1 —
#                      registration is once-per-localpart
#                      [src/routes/register.rs:470-482], and that is a fact about
#                      the server, not a walkthrough bug.
#   AS_DEVICE_1/2       device_ids for the two logins, default WORKER-A / WORKER-B
#   SERVER_NAME         Matrix server name, default $MATRIX_HS_SERVER_NAME,
#                      else "localhost"
#   BASE_URL            homeserver base, default http://127.0.0.1:8448
#   ROOM_NAME           room name for createRoom, default "AS consumer walkthrough"
#   MESSAGE_BODY        message text for step f
#   HTTP_TIMEOUT        per-request curl timeout in seconds, default 15
#   DRY_RUN=1           print every command exactly as it would run, execute nothing.
#                      Secrets print as <masked>; REVEAL_TOKENS=1 prints them.
#
# Requires: curl, jq, and the usual POSIX utilities (sed, head, mktemp, date).

set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:8448}"
# The server's own default [src/main.rs:195]; never derived from BASE_URL.
SERVER_NAME="${SERVER_NAME:-${MATRIX_HS_SERVER_NAME:-localhost}}"
AS_PREFIX="${MATRIX_HS_AS_PREFIX:-as_}"
# Unique per run by default: a live server refuses a second registration of the
# same localpart (M_USER_IN_USE), so a fixed name would make the walkthrough
# pass exactly once.
LOCALPART="${AS_LOCALPART:-${AS_PREFIX}walkthrough$(date +%s)}"
DEVICE_1="${AS_DEVICE_1:-WORKER-A}"
DEVICE_2="${AS_DEVICE_2:-WORKER-B}"
ROOM_NAME="${ROOM_NAME:-AS consumer walkthrough}"
MESSAGE_BODY="${MESSAGE_BODY:-AS consumer walkthrough message}"
TXN_ID="${TXN_ID:-as-walkthrough-1}"
HTTP_TIMEOUT="${HTTP_TIMEOUT:-15}"
DRY_RUN="${DRY_RUN:-0}"
REVEAL_TOKENS="${REVEAL_TOKENS:-0}"
MASK_LABEL='<masked>'

API="${BASE_URL}/_matrix/client/v3"
MXID="@${LOCALPART}:${SERVER_NAME}"

# --- output / exit helpers --------------------------------------------------
# Defined before the environment gate, which is the first thing that can fail
# and the first thing that wants to speak.

log() {
    printf '[as_consumer %s] %s\n' "$(date +%H:%M:%S)" "$*"
}

is_dry() {
    [ "$DRY_RUN" -eq 1 ]
}

# fail <exit-code> <what> — the header's exit-code table is the contract.
fail() {
    local code="$1"
    shift
    log "FAILED (exit $code): $*" >&2
    exit "$code"
}

die_env() {
    fail 8 "config error: $*"
}

# --- environment gate -------------------------------------------------------

for tool in curl jq; do
    command -v "$tool" >/dev/null 2>&1 || die_env "$tool is required but not on PATH"
done

if [ -z "${MATRIX_HS_AS_TOKEN:-}" ]; then
    die_env "MATRIX_HS_AS_TOKEN is required — the agent socket is off without it [.env.example:75]"
fi
AS_TOKEN="${MATRIX_HS_AS_TOKEN}"

# The server refuses any localpart outside its prefix [src/state.rs:1309-1311];
# catching it here turns a 403 from step a into a one-line config hint.
case "$LOCALPART" in
    "${AS_PREFIX}"*) ;;
    *) die_env "AS_LOCALPART must start with the AS prefix '${AS_PREFIX}' (got '${LOCALPART}')" ;;
esac

if [ "$DRY_RUN" != 0 ] && [ "$DRY_RUN" != 1 ]; then
    die_env "DRY_RUN must be 0 or 1 (got '${DRY_RUN}')"
fi

# Step outputs. TOKEN_1 / TOKEN_2 are the MINTED device tokens — the AS bearer
# is not accepted by createRoom/sync/send.
TOKEN_1=""
TOKEN_2=""
ROOM_ID=""
SENT_EVENT_ID=""
EXPECTED_MXID=""
HTTP_CODE=""

# --- plumbing ---------------------------------------------------------------

WORKDIR=""
RESP=""

cleanup() {
    [ -n "$WORKDIR" ] && [ -d "$WORKDIR" ] && rm -rf "$WORKDIR"
    return 0
}
# INT/TERM as well as EXIT: a walkthrough interrupted at the prompt should not
# leave a response-body temp dir behind.
#
# But a cleanup handler alone is not an interrupt. `trap cleanup EXIT INT TERM`
# ran the handler and then carried on into the next step, so an aborted run went
# on to fail on whatever it touched next and exited 1 blaming that — measured on a
# request to an unroutable address: SIGTERM at 3 s, the handler never even got to
# run until curl gave up at the 25 s timeout, and the run ended "FAILED (exit 1):
# register: transport error", which is exactly what a genuine network fault looks
# like. With the signal traps ending the script, the same experiment ends 143 with
# no invented failure line.
#
# MEASURED, both signals, on this script. TERM: exit 143. INT: exit 130. In both
# cases no failure line is invented for a run the operator stopped on purpose.
# One caveat for whoever re-tests this: a script launched in the BACKGROUND
# inherits SIGINT ignored, and bash cannot trap a signal that was ignored on
# entry — so INT must be exercised from a foreground run, or the measurement
# will be of the harness and not of this script.
#
# What neither fix does: interrupt the request already in flight. The handler runs
# only once the foreground command returns, so the wait is still bounded by
# HTTP_TIMEOUT. Killing the curl child would need its PID, which `$(curl ...)`
# does not expose. Flagged, not silently half-done.
trap cleanup EXIT
trap 'cleanup; exit 130' INT
trap 'cleanup; exit 143' TERM


# Replace any known secret with $MASK_LABEL so a DRY_RUN transcript — and an
# ordinary run's log — is safe to paste. REVEAL_TOKENS=1 opts out.
mask_secrets() {
    local s="$1" t
    for t in "${AS_TOKEN:-}" "$TOKEN_1" "$TOKEN_2"; do
        [ -n "$t" ] || continue
        s="${s//"$t"/$MASK_LABEL}"
    done
    # Any mxt_ token is a live signed credential [src/routes/login.rs:124], even
    # one this process has not stored in a variable yet.
    printf '%s' "$s" | sed -E "s/mxt_[[:alnum:]_.-]+/${MASK_LABEL}/g"
}

# body_for_log — the response body, made safe to paste.
#
# The three diagnostic dumps below fire exactly when the server misbehaved, which
# is exactly when the body is most likely to be carrying a live credential: a
# login that answered 200 while a field went missing still has the token sitting
# next to it. Those dumps called head directly and so bypassed mask_secrets,
# while the promise in the comment above is that an ordinary run's log is safe to
# paste. One path, one rule: everything printed goes through the masker, and
# REVEAL_TOKENS=1 opts out here exactly as it does in render().
body_for_log() {
    local b
    b=$(head -c 400 "$RESP" 2>/dev/null || true)
    if [ "$REVEAL_TOKENS" -eq 1 ]; then
        printf '%s' "$b"
    else
        mask_secrets "$b"
    fi
}

# render <argv...> — one copy-pasteable command line, exactly the argv used.
render() {
    local out="" a q
    for a in "$@"; do
        if [ "$REVEAL_TOKENS" -eq 1 ]; then
            q="$a"
        else
            q=$(mask_secrets "$a")
        fi
        printf -v q '%q' "$q"
        out+="$q "
    done
    printf '%s' "${out% }"
}

# req <method> <url> <bearer> <json-body | ->   — the single network chokepoint.
# Sets HTTP_CODE; on dry-run prints the command and returns 0 without running it.
req() {
    local method="$1" url="$2" bearer="$3" body="$4"
    local -a args
    args=(--noproxy '*' --silent --show-error --max-time "$HTTP_TIMEOUT"
          -X "$method" "$url" -H "Authorization: Bearer $bearer")
    if [ "$body" != "-" ]; then
        args+=(-H "Content-Type: application/json" --data-binary "$body")
    fi

    if is_dry; then
        log "DRY_RUN: $(render curl "${args[@]}" -o "$RESP" -w '%{http_code}')"
        return 0
    fi

    local rc=0
    HTTP_CODE=$(curl "${args[@]}" -o "$RESP" -w '%{http_code}') || rc=$?
    if [ "$rc" -ne 0 ]; then
        HTTP_CODE="curl-exit-$rc"
        return 1
    fi
    return 0
}

# expect_ok <exit-code> <label> — the status check every step owes the reader.
expect_ok() {
    local code="$1" label="$2"
    if is_dry; then
        log "DRY_RUN: would require HTTP 200 for $label"
        return 0
    fi
    if [ "$HTTP_CODE" != 200 ]; then
        log "  $label -> HTTP $HTTP_CODE" >&2
        log "  body: $(body_for_log)" >&2
        fail "$code" "$label returned HTTP $HTTP_CODE (expected 200)"
    fi
    log "  $label -> HTTP 200"
}

# field <json-path> — pull one value out of the last response.
field() {
    local path="$1"
    if is_dry; then
        printf '<dry-run>'
        return 0
    fi
    jq -r "$path // empty" "$RESP"
}

# expect_field <exit-code> <json-path> <prefix> <varname> <what>
# A missing field is the failure this script exists to catch, so an empty or
# null value is fatal rather than logged-and-continued.
expect_field() {
    local code="$1" path="$2" prefix="$3" varname="$4" what="$5"
    if is_dry; then
        log "DRY_RUN: would require $what in the response"
        return 0
    fi
    local value
    value=$(field "$path")
    if [ -z "$value" ] || [ "$value" = null ]; then
        log "  body: $(body_for_log)" >&2
        fail "$code" "$what missing from the response (jq path $path)"
    fi
    case "$value" in
        "$prefix"*) ;;
        *)
            fail "$code" "$what is '${value}', expected it to start with '${prefix}'"
            ;;
    esac
    log "  $what = $(mask_secrets "$value")"
    printf -v "$varname" '%s' "$value"
}

# --- step a: register -------------------------------------------------------

step_a() {
    log "--- step a: register '${LOCALPART}' via AS bearer (no UIA) ---"
    RESP="$WORKDIR/a_register.json"
    local payload
    payload=$(jq -nc --arg u "$LOCALPART" '{username: $u}')
    req POST "$API/register" "$AS_TOKEN" "$payload" \
        || fail 1 "register: transport error (see above)"
    expect_ok 1 "register"
    expect_field 1 '.user_id' '@' EXPECTED_MXID 'user_id'
    check_server_name
    log "  tenant MXID = $MXID"
}

# The register handler builds the MXID from the server's own server_name
# [src/routes/register.rs:487], so a mismatch here means SERVER_NAME is wrong
# for this server — exactly the BASE_URL host:port confusion this script is
# meant not to have.
check_server_name() {
    if is_dry; then
        log "DRY_RUN: would check user_id == '${MXID}' (from SERVER_NAME)"
        return 0
    fi
    if [ "$EXPECTED_MXID" != "$MXID" ]; then
        fail 1 "server reports user_id '${EXPECTED_MXID}' but SERVER_NAME='${SERVER_NAME}' \
implies '${MXID}' — set SERVER_NAME to the homeserver's Matrix server name"
    fi
}

# --- steps b1/b2: passwordless per-device login -----------------------------

# login_step <exit-code> <device_id> <out-varname>
login_step() {
    local code="$1" device="$2" outvar="$3"
    log "--- login device '${device}' (step code $code) ---"
    RESP="$WORKDIR/login_${device}.json"
    local payload
    payload=$(jq -nc --arg u "$MXID" --arg d "$device" \
        '{type: "m.login.application_service", user_id: $u, device_id: $d}')
    req POST "$API/login" "$AS_TOKEN" "$payload" \
        || fail "$code" "login ${device}: transport error (see above)"
    expect_ok "$code" "login ${device}"

    local token="" got_device=""
    expect_field "$code" '.access_token' 'mxt_' token "access_token (${device})"
    expect_field "$code" '.device_id' "$device" got_device "device_id (${device})"
    printf -v "$outvar" '%s' "$token"
}

# --- step c: the two sessions are separate tokens ---------------------------

step_c() {
    log "--- step c: the two worker sessions are distinct tokens ---"
    if is_dry; then
        log "DRY_RUN: would assert device tokens differ"
        return 0
    fi
    if [ "$TOKEN_1" = "$TOKEN_2" ]; then
        fail 4 "both logins returned the same access_token — workers are not separate sessions"
    fi
    log "  device tokens differ (as_socket_test.rs:164)"
}

# --- step d: createRoom with a device token ---------------------------------

step_d() {
    log "--- step d: createRoom with the device token ---"
    RESP="$WORKDIR/d_createroom.json"
    # The room_id the response must carry; a placeholder keeps the dry-run
    # transcript an illustration of a real request rather than an empty slot.
    if is_dry; then
        ROOM_ID="<dry-run-room-id>"
    fi
    local payload
    payload=$(jq -nc --arg n "$ROOM_NAME" '{name: $n}')
    req POST "$API/createRoom" "$TOKEN_1" "$payload" \
        || fail 5 "createRoom: transport error (see above)"
    expect_ok 5 "createRoom"
    expect_field 5 '.room_id' '!' ROOM_ID 'room_id'
}

# --- step e: sync with a device token ---------------------------------------

step_e() {
    log "--- step e: sync with the SECOND device token ---"
    RESP="$WORKDIR/e_sync.json"
    req GET "$API/sync" "$TOKEN_2" - \
        || fail 6 "sync: transport error (see above)"
    expect_ok 6 "sync"
    if is_dry; then
        log "DRY_RUN: would require .rooms.join to contain ${ROOM_ID}"
        return 0
    fi
    # A 200 from /sync proves nothing on its own: an unauthenticated caller gets
    # an empty rooms.join [src/routes/sync.rs:266-268]. The room from step d must
    # come back, which only happens for a caller whose own m.room.member is join
    # [:288-295] — here the other device of the same tenant account, which is
    # the workers-as-devices claim being walked through.
    if ! jq -e --arg r "$ROOM_ID" 'has("rooms") and (.rooms.join | has($r))' "$RESP" >/dev/null 2>&1; then
        log "  body: $(body_for_log)" >&2
        fail 6 "sync did not return room '${ROOM_ID}' in .rooms.join — joined rooms seen: \
$(jq -c '.rooms.join | keys' "$RESP" 2>/dev/null || echo '<none>')"
    fi
    log "  .rooms.join contains $ROOM_ID (second device sees the first's room)"
}

# --- step f: send -----------------------------------------------------------

step_f() {
    log "--- step f: send m.room.message into $ROOM_ID ---"
    RESP="$WORKDIR/f_send.json"
    local payload
    payload=$(jq -nc --arg b "$MESSAGE_BODY" '{msgtype: "m.text", body: $b}')
    req PUT "$API/rooms/$ROOM_ID/send/m.room.message/$TXN_ID" "$TOKEN_1" "$payload" \
        || fail 7 "send: transport error (see above)"
    expect_ok 7 "send"
    expect_field 7 '.event_id' '$' SENT_EVENT_ID 'event_id'
}

# --- main -------------------------------------------------------------------

log "=== AS Consumer Walkthrough ==="
log "BASE_URL    : $BASE_URL"
log "SERVER_NAME : $SERVER_NAME (MXIDs are built from this, never from BASE_URL)"
log "AS_PREFIX   : $AS_PREFIX"
log "localpart   : $LOCALPART"
log "devices     : $DEVICE_1 / $DEVICE_2"
log "DRY_RUN     : $DRY_RUN"
if is_dry; then
    log "=== DRY_RUN: every command below is printed exactly as it would run ==="
fi

WORKDIR=$(mktemp -d "${TMPDIR:-/tmp}/as-walkthrough.XXXXXX")

step_a
login_step 2 "$DEVICE_1" TOKEN_1
login_step 3 "$DEVICE_2" TOKEN_2
step_c
step_d
step_e
step_f

log "=== walkthrough passed ==="
log "  tenant   : $MXID"
log "  room_id  : $ROOM_ID"
log "  event_id : ${SENT_EVENT_ID:-<dry-run-event-id>}"
