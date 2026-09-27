#!/bin/bash
# Automated E2E registration test for the patched native FluffyChat binary.
# Drives the full signUp flow against matrix-hs (m.hubd.net) and exits 0 only
# if POST /_matrix/client/v3/register returned 200 (user actually created).
#
# Run:  ./e2e_register_auto.sh
# Exit: 0 PASS (register->200 seen)  |  1 FAIL
# Forensics: $SCRATCH/*.png , /tmp/fluffychat.log , $CADDY

set -uo pipefail
export DISPLAY=":99"
export LIBGL_ALWAYS_SOFTWARE=1
FC_BIN="/usr/local/fluffychat"
SCRATCH="${SCRATCH:-/tmp/mrgd-e2e}"; mkdir -p "$SCRATCH"
CADDY="${CADDY_LOG:-${MATRIX_HS_HOME:-.}/caddy_443.log}"
HOMESERVER="${HOMESERVER:?set HOMESERVER, e.g. localhost:8448}"
PASSWORD="${E2E_PASSWORD:?set E2E_PASSWORD -- this script registers a real account}"
USER="e2e_$(date +%s | tail -c5)"
DIR="$(dirname "$(readlink -f "$0")")"

log()  { echo "[e2e $(date +%H:%M:%S)] $*"; }
tap()  { xdotool mousemove "$1" "$2" click 1 >/dev/null 2>&1; sleep "${D:-3}"; }
type_(){ xdotool type --delay 25 "$1" >/dev/null 2>&1; sleep 1; }
shot() { import -window root "$SCRATCH/$1.png" 2>/dev/null; }
fc_up(){ xdotool search --onlyvisible --class fluffychat >/dev/null 2>&1; }
# wait until the screen has real rendered content (PNG > 25KB = not blank)
wait_render() {
    local name="${1:-screen}" sz=0
    for i in $(seq 1 20); do
        shot "_probe"; sz=$(wc -c < "$SCRATCH/_probe.png" 2>/dev/null || echo 0)
        [ "$sz" -gt 25000 ] && { shot "$name"; return 0; }
        sleep 1
    done
    log "WARN: $name never rendered (last size ${sz}B)"; shot "$name"; return 1
}

# returns 0 if a POST /register with status 200 appears after the given baseline line
register_200_since() {
    local before="$1" total
    total=$(wc -l < "$CADDY" 2>/dev/null || echo 0)
    tail -n "$((total-before))" "$CADDY" 2>/dev/null | python3 -c '
import json,sys
ok=False
for l in sys.stdin:
    try:
        d=json.loads(l);r=d["request"];ua=r.get("headers",{}).get("User-Agent",[""])[0]
        if ("Dart" in ua or "fluffy" in ua.lower()) and r["method"]=="POST" and "/register" in r["uri"]:
            print("   POST",r["uri"].split("?")[0],"->",d.get("status"))
            if d.get("status")==200: ok=True
    except: pass
sys.exit(0 if ok else 1)
'
}

# 0. environment + fresh launch
"$DIR/e2e_fluffychat_native.sh" setup_env >/dev/null 2>&1
pkill -x fluffychat 2>/dev/null; sleep 2
# wipe persisted session so the app starts at the welcome screen (not resumed)
rm -rf "$HOME/.local/share/chat.fluffy.fluffychat" 2>/dev/null
"$FC_BIN" >/tmp/fluffychat.log 2>&1 &
for i in $(seq 1 25); do fc_up && break; sleep 1; done
fc_up || { log "FAIL: fluffychat window never appeared"; exit 1; }
wait_render 01_welcome || true
log "welcome up — target @$USER:$HOMESERVER"

# 1. Create Account entry (signUp=true)
tap 640 500; wait_render 02_signup_picker || true
# 2. homeserver picker → type → suggestion → Continue
tap 290 190; type_ "$HOMESERVER"; sleep 1
tap 327 256; sleep 1
mark=$(wc -l < "$CADDY")
tap 640 660; sleep 5              # Continue → discovery fires
wait_render 03_reg_form || true
if ! register_200_since "$mark" >/dev/null 2>&1; then
    # not expecting register yet, but discovery should have happened — sanity log
    log "discovery window (well-known/versions/login) check:"
    tail -n "$(( $(wc -l <"$CADDY") - mark ))" "$CADDY" 2>/dev/null | python3 -c '
import json,sys
for l in sys.stdin:
  try:
    d=json.loads(l);r=d["request"];ua=r.get("headers",{}).get("User-Agent",[""])[0]
    if ("Dart" in ua or "fluffy" in ua.lower()) and r["method"]=="GET": print("   GET",r["uri"][:45],"->",d.get("status"))
  except: pass
' | head -5
fi

# 3. fill registration form
tap 356 279; xdotool key BackSpace >/dev/null 2>&1; sleep 0.5; type_ "$USER"
tap 308 343; type_ "$PASSWORD"
tap 399 406; type_ "$PASSWORD"
wait_render 04_form_filled || true

# 4. submit + verify register->200
mark=$(wc -l < "$CADDY")
tap 640 470; sleep 6
wait_render 05_after_submit || true
log "POST /register calls observed:"
register_200_since "$mark"; rc=$?

if [ $rc -eq 0 ]; then
    log "PASS: @$USER:$HOMESERVER registered via patched native FluffyChat"
    echo "$USER" > "$SCRATCH/last_testuser.txt"
    exit 0
fi
log "FAIL: no POST /register -> 200 (see $SCRATCH/*.png , /tmp/fluffychat.log)"
exit 1
