#!/bin/bash
# E2E harness: drives the NATIVE Linux FluffyChat binary (/usr/local/fluffychat,
# patched with password-registration) through a flow against matrix-hs
# (m.hubd.net:8443), capturing a screenshot + the Caddy access-log slice after
# each step so a failure can be diagnosed from server-side evidence.
#
# Mirrors scripts/e2e_fluffychat.sh (which drives FluffyChat-on-Waydroid via
# adb). This one drives the native GTK binary via xdotool under Xvfb+openbox.
#
# Usage: e2e_fluffychat_native.sh <step-name> [args]
#   setup_env          - start Xvfb :99 + openbox + fluffychat (idempotent)
#   screenshot [name]  - capture current screen only
#   tap <x> <y>        - click at coords
#   type <text>        - type a string
#   key <keysym>       - press a key (Return, Tab, BackSpace, ...)
#   fresh_launch       - kill+relaunch fluffychat, screenshot welcome
#   caddy_since        - print matrix-hs requests since last marker
#
# Coordinates below are for the 1280x720 Xvfb screen and were found by
# OCR-ing screenshots (tesseract) — verify/adjust if the layout shifts.
#   welcome:    "Войти" (Sign in) button ......... (433, 577)
#   hs picker:  homeserver input field ........... (290, 190)
#   hs picker:  suggestion "m.hubd.net" ........... (327, 256)
#   hs picker:  Continue (icon, bottom-center) .... (640, 660)  → triggers discovery
# Discovery that lands on the login screen proves client→server E2E.
# Registration path (signUp=true entry) still to be wired — needs the
# "Create account" button on welcome/sign_in (not the "Войти" used above).

set -uo pipefail

DISPLAY_NUM=":99"
export DISPLAY="$DISPLAY_NUM"
export LIBGL_ALWAYS_SOFTWARE=1            # Xvfb has no GPU → software GL
SCREEN_W=1280; SCREEN_H=720
FC_BIN="/usr/local/fluffychat"
SCRATCH="${SCRATCH:-/tmp/mrgd-e2e}"
CADDY_LOG="${CADDY_LOG:-${MATRIX_HS_HOME:-.}/caddy_443.log}"
MARKER="$SCRATCH/caddy_marker.txt"
mkdir -p "$SCRATCH"

log() { echo "[$(date +%H:%M:%S)] $*"; }

# --- Caddy log correlation (server-side evidence) ---
mark_caddy() { wc -l < "$CADDY_LOG" > "$MARKER" 2>/dev/null || echo 0 > "$MARKER"; }
caddy_since() {
    local before=0 total
    [ -f "$MARKER" ] && before=$(cat "$MARKER")
    total=$(wc -l < "$CADDY_LOG" 2>/dev/null || echo 0)
    [ "$total" -le "$before" ] && { echo "(no new matrix-hs requests)"; return; }
    tail -n "$((total - before))" "$CADDY_LOG" | python3 -c '
import json, sys
for line in sys.stdin:
    try:
        d = json.loads(line); r = d["request"]
        ua = r.get("headers",{}).get("User-Agent",[""])[0]
        # FluffyChat/Dart UA contains "Dart" or "fluffy"; filter out Caddy/admin noise
        if "Dart" in ua or "fluffy" in ua.lower() or "Dart" in str(d):
            print("  {:6} {:60} -> {} ({}B)".format(r["method"], r["uri"][:60], d.get("status"), d.get("size")))
    except Exception:
        pass
'
}

# --- UI primitives (xdotool) ---
tap()      { timeout 8 xdotool mousemove "$1" "$2" click 1 >/dev/null 2>&1; sleep "${STEP_DELAY:-2}"; }
type_text(){ timeout 8 xdotool type --delay 20 "$1" >/dev/null 2>&1; sleep 1; }
press_key(){ timeout 8 xdotool key "$1" >/dev/null 2>&1; sleep 1; }
shot()     { local n="${1:-step}"; timeout 10 import -window root "$SCRATCH/${n}.png" 2>/dev/null
             echo "screenshot: $SCRATCH/${n}.png ($(wc -c < "$SCRATCH/${n}.png" 2>/dev/null)B)"; }
fc_window(){ xdotool search --onlyvisible --class fluffychat 2>/dev/null | head -1; }

# --- environment setup ---
setup_env() {
    log "ensuring Xvfb on $DISPLAY_NUM"
    if ! pgrep -x Xvfb >/dev/null; then
        Xvfb "$DISPLAY_NUM" -screen 0 ${SCREEN_W}x${SCREEN_H}x24 >/tmp/xvfb.log 2>&1 &
    fi
    for i in $(seq 1 6); do
        pgrep -x Xvfb >/dev/null && break
        sleep 1
    done
    pgrep -x Xvfb >/dev/null || { log "Xvfb FAILED"; tail -5 /tmp/xvfb.log; exit 1; }
    log "ensuring openbox WM"
    if ! pgrep -x openbox >/dev/null; then
        DISPLAY="$DISPLAY_NUM" openbox >/tmp/openbox.log 2>&1 &
    fi
    for i in $(seq 1 4); do pgrep -x openbox >/dev/null && break; sleep 1; done
    log "ensuring fluffychat running"
    if pgrep -x fluffychat >/dev/null; then log "already running"; return; fi
    DISPLAY="$DISPLAY_NUM" LIBGL_ALWAYS_SOFTWARE=1 "$FC_BIN" >/tmp/fluffychat.log 2>&1 &
    for i in $(seq 1 25); do
        [ -n "$(fc_window)" ] && { log "fluffychat window up after ${i}s"; return; }
        sleep 1
    done
    log "fluffychat window did NOT appear"; tail -5 /tmp/fluffychat.log
}

fresh_launch() {
    log "killing fluffychat for fresh launch"
    pkill -x fluffychat 2>/dev/null; sleep 2
    mark_caddy
    DISPLAY="$DISPLAY_NUM" LIBGL_ALWAYS_SOFTWARE=1 "$FC_BIN" >/tmp/fluffychat.log 2>&1 &
    for i in $(seq 1 20); do [ -n "$(fc_window)" ] && break; sleep 1; done
    sleep 3   # let welcome screen render
    shot "01_welcome"
    caddy_since
}

# --- dispatch ---
case "${1:-help}" in
    setup_env)    setup_env ;;
    screenshot)   shot "${2:-manual}" ;;
    tap)          tap "$2" "$3" ;;
    type)         type_text "$2" ;;
    key)          press_key "$2" ;;
    fresh_launch) fresh_launch ;;
    caddy_since)  caddy_since ;;
    window)       echo "wid=$(fc_window)" ;;
    *) cat <<EOF
Usage: $0 <step> [args]
  setup_env | fresh_launch | screenshot [name] | tap <x> <y> | type <text>
  | key <keysym> | caddy_since | window
Env: Xvfb $DISPLAY_NUM ${SCREEN_W}x${SCREEN_H}, openbox WM, software-GL.
Binary: $FC_BIN
EOF
       ;;
esac
