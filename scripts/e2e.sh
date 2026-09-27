#!/usr/bin/env bash
# e2e.sh — minimal smoke-test for matrix-hs
# Usage: ./scripts/e2e.sh [port]
#
# Starts matrix-hs on a random (or specified) port, runs a few CS-API calls,
# then tears down the server.
#
# Dependencies: curl, jq (optional but improves output)

set -euo pipefail

PORT=${1:-18448}
BIN="./target/debug/matrix-hs"

# All requests target loopback — never route them through an ambient HTTP proxy.
# `--noproxy '*'` is more reliable than exporting no_proxy for every curl.
CURL=(curl --noproxy '*' -sf)

if [[ ! -x "$BIN" ]]; then
  echo "Binary not found: $BIN — run 'cargo build' first" >&2
  exit 1
fi

# Start server in background.
# matrix-hs reads its bind address from MATRIX_HS_LISTEN (default 127.0.0.1:8448).
# The MATRIX_HS_ZENOH_PREFIX / MATRIX_HS_BARRIER_KEY vars only take effect in a
# `--features cluster` build; harmless (ignored) in the default single-node build.
MATRIX_HS_LISTEN="127.0.0.1:${PORT}" \
MATRIX_HS_ZENOH_PREFIX="mrgd/e2e/room" \
MATRIX_HS_BARRIER_KEY="mrgd/e2e/barrier" \
  "$BIN" &
SERVER_PID=$!
trap "kill $SERVER_PID 2>/dev/null || true" EXIT

# Wait for server to be ready
for i in $(seq 1 20); do
  if "${CURL[@]}" "http://127.0.0.1:${PORT}/_matrix/client/versions" >/dev/null 2>&1; then
    break
  fi
  sleep 0.2
done

BASE="http://127.0.0.1:${PORT}/_matrix/client/v3"

echo "=== versions ==="
"${CURL[@]}" "$BASE/../../../_matrix/client/versions" | (command -v jq >/dev/null && jq . || cat)

echo ""
echo "=== register (Matrix UIA: 401 challenge, then m.login.dummy) ==="
# Step 1: initial POST with no auth → HTTP 401 + a UIA session id.
# curl -f fails on 401, so drop -f for this one call and read the body directly.
CHALLENGE=$(curl --noproxy '*' -s -X POST "$BASE/register" \
  -H "Content-Type: application/json" \
  -d '{"username":"testuser","password":"s3cr3t"}')
SESSION=$(printf '%s' "$CHALLENGE" \
  | (command -v jq >/dev/null && jq -r '.session' || grep -o '"session":"[^"]*"' | cut -d'"' -f4))
echo "uia session: $SESSION"

# Step 2: resubmit with the dummy auth stage + session → HTTP 200 + access_token.
TOKEN=$("${CURL[@]}" -X POST "$BASE/register" \
  -H "Content-Type: application/json" \
  -d "{\"username\":\"testuser\",\"password\":\"s3cr3t\",\"auth\":{\"type\":\"m.login.dummy\",\"session\":\"$SESSION\"}}" \
  | (command -v jq >/dev/null && jq -r '.access_token' || grep -o '"access_token":"[^"]*"' | cut -d'"' -f4))
echo "access_token: $TOKEN"

echo ""
echo "=== whoami ==="
"${CURL[@]}" "$BASE/account/whoami" \
  -H "Authorization: Bearer $TOKEN" \
  | (command -v jq >/dev/null && jq . || cat)

echo ""
echo "=== create room ==="
ROOM_ID=$("${CURL[@]}" -X POST "$BASE/createRoom" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name":"test-room"}' \
  | (command -v jq >/dev/null && jq -r '.room_id' || grep -o '"room_id":"[^"]*"' | cut -d'"' -f4))
echo "room_id: $ROOM_ID"

echo ""
echo "=== send message ==="
"${CURL[@]}" -X PUT "$BASE/rooms/${ROOM_ID}/send/m.room.message/txn1" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"msgtype":"m.text","body":"hello mrgd"}' \
  | (command -v jq >/dev/null && jq . || cat)

echo ""
echo "=== sync ==="
"${CURL[@]}" "$BASE/sync" \
  -H "Authorization: Bearer $TOKEN" \
  | (command -v jq >/dev/null && jq '.rooms.join | keys' || cat)

echo ""
echo "ALL CHECKS PASSED"
