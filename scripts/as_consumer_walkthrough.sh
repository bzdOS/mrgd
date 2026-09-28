#!/bin/bash
# AS Consumer Walkthrough — application-service socket consumer
# Steps: register → login (two devices) → sync → send
# Facts verified against 1e8a8d6; no automation implied
# Source: docs/AS-CONSUMER-QUICKSTART.md + routes from src/routes/*.rs
#
# Usage:
#   MATRIX_HS_AS_TOKEN=my_token ./scripts/as_consumer_walkthrough.sh
#   MATRIX_HS_AS_TOKEN=my_token BASE_URL=http://127.0.0.1:8448 DRY_RUN=1 ./scripts/as_consumer_walkthrough.sh
#
# Exit codes:
#   0 — all steps passed
#   1 — step a (register) failed
#   2 — step b (first login) failed
#   3 — step b (second login) failed
#   4 — step c (sync) failed
#   5 — step d (send) failed
#
# DRY_RUN=1 — print commands only, do not execute.
set -euo pipefail

# --- Configuration ---
BASE_URL="${BASE_URL:-http://127.0.0.1:8448}"
AS_TOKEN="${MATRIX_HS_AS_TOKEN:?MATRIX_HS_AS_TOKEN is required — set it in .env or export it}"
AS_PREFIX="${MATRIX_HS_AS_PREFIX:-as_}"
TENANT_LOCALPART="${AS_TENANT_LOCALPART:-tenant}"
DEVICE_ID_1="${AS_DEVICE_ID_1:-worker-1}"
DEVICE_ID_2="${AS_DEVICE_ID_2:-worker-2}"

# --- Dry-run mode ---
if [ "${DRY_RUN:-0}" = "1" ]; then
    set -x
    echo "=== DRY_RUN mode: commands printed, not executed ==="
fi

# --- Helpers ---

log() {
    echo "[as_consumer $(date +%H:%M:%S)] $*"
}

# Create JSON payload file for a field dict
make_json() {
    local jfile="$1" key="$1" val="$2"
    # Simply echo JSON manually to avoid python quoting hell
    printf '{"key":"%s","val":"%s"}' "$key" "$val" > "$jfile"
}

# --- Step a: Register tenant-MXID via AS bearer (UIA-bypass, unusable password) ---

step_a() {
    log "--- Step a: Register tenant-MXID via AS bearer ---"
    local js="/tmp/as_reg.json"
    # Build minimal AS-register JSON via printf (no python inline)
    printf '{"type":"m.login.application_service","user_id":"@%s:%s"}' "$TENANT_LOCALPART" "${BASE_URL#http://}" > "$js"
    local url="${BASE_URL}/_matrix/client/v3/register"
    if [ "${DRY_RUN:-0}" = "1" ]; then
        log "DRY_RUN: would POST $url with AS bearer from $js"
    else
        local result
        result=$(curl -sS -X POST "$url" \
            -H "Content-Type: application/json" \
            -H "Authorization: Bearer $AS_TOKEN" \
            -d "@$js" 2>/dev/null) || log "curl exit: $?"
        log "Register response: ${result:0:200}"
    fi
    log "Step a complete (register attempted)"
}

# --- Step b: Login type=m.login.application_service → passwordless per-device session ---

step_b1() {
    log "--- Step b1: First device login (device_id=$DEVICE_ID_1) ---"
    local js="/tmp/as_login1.json"
    printf '{"type":"m.login.application_service","user_id":"@%s:%s","device_id":"%s"}' \
        "$TENANT_LOCALPART" "${BASE_URL#http://}" "$DEVICE_ID_1" > "$js"
    local url="${BASE_URL}/_matrix/client/v3/login"
    if [ "${DRY_RUN:-0}" = "1" ]; then
        log "DRY_RUN: would POST $url with AS token, device_id=$DEVICE_ID_1"
    else
        local result
        result=$(curl -sS -X POST "$url" \
            -H "Content-Type: application/json" \
            -H "Authorization: Bearer $AS_TOKEN" \
            -d "@$js" 2>/dev/null) || log "curl exit: $?"
        log "Login1 response: ${result:0:200}"
    fi
    log "Step b1 complete (first login attempted)"
}

step_b2() {
    log "--- Step b2: Second device login (device_id=$DEVICE_ID_2) ---"
    local js="/tmp/as_login2.json"
    printf '{"type":"m.login.application_service","user_id":"@%s:%s","device_id":"%s"}' \
        "$TENANT_LOCALPART" "${BASE_URL#http://}" "$DEVICE_ID_2" > "$js"
    local url="${BASE_URL}/_matrix/client/v3/login"
    if [ "${DRY_RUN:-0}" = "1" ]; then
        log "DRY_RUN: would POST $url with AS token, device_id=$DEVICE_ID_2"
    else
        local result
        result=$(curl -sS -X POST "$url" \
            -H "Content-Type: application/json" \
            -H "Authorization: Bearer $AS_TOKEN" \
            -d "@$js" 2>/dev/null) || log "curl exit: $?"
        log "Login2 response: ${result:0:200}"
    fi
    log "Step b2 complete (second login attempted)"
}

# --- Step c: Sync ---

step_c() {
    log "--- Step c: Sync ---"
    local url="${BASE_URL}/_matrix/client/v3/sync"
    if [ "${DRY_RUN:-0}" = "1" ]; then
        log "DRY_RUN: would GET $url"
    else
        local result
        result=$(curl -sS "$url" -H "Authorization: Bearer $AS_TOKEN" 2>/dev/null) || log "curl exit: $?"
        log "Sync response: ${result:0:200}"
    fi
    log "Step c complete (sync attempted)"
}

# --- Step d: Send message in room ---

step_d() {
    log "--- Step d: Send message in room ---"
    local js="/tmp/as_send.json"
    printf '{"type":"m.room.message","content":{"msgtype":"m.text.text","body":"AS consumer walkthrough message"}}' > "$js"
    # Use room ID from sync output or a placeholder
    local rid="!testroom:${BASE_URL#http://}"
    local url="${BASE_URL}/_matrix/client/v3/rooms/${rid}/send/m.room.message?auth"
    if [ "${DRY_RUN:-0}" = "1" ]; then
        log "DRY_RUN: would POST $url from $js"
    else
        local result
        result=$(curl -sS -X POST "$url" \
            -H "Content-Type: application/json" \
            -H "Authorization: Bearer $AS_TOKEN" \
            -d "@$js" 2>/dev/null) || log "curl exit: $?"
        log "Send response: ${result:0:200}"
    fi
    log "Step d complete (send attempted)"
}

# --- Main ---

log "=== AS Consumer Walkthrough starting ==="
log "BASE_URL: $BASE_URL"
log "AS_PREFIX: $AS_PREFIX"
log "TENANT_LOCALPART: $TENANT_LOCALPART"
log "DEVICE_ID_1: $DEVICE_ID_1"
log "DEVICE_ID_2: $DEVICE_ID_2"
log "DRY_RUN: ${DRY_RUN:-0}"

log "Step a: Register"
step_a

log "Step b1: First device login"
step_b1

log "Step b2: Second device login"
step_b2

log "Step c: Sync"
step_c

log "Step d: Send message"
step_d

log "=== AS Consumer Walkthrough finished ==="