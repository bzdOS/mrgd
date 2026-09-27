#!/bin/bash
# Full, non-interactive E2E pass: registration flow + login flow (with a plain
# username instead of a full MXID, to test whether that avoids the localhost
# domain-rediscovery issue found earlier). Text-only output (no screenshots
# viewed inline) — screenshots are saved to disk for later inspection only if
# something looks wrong from the log correlation.
set -uo pipefail

SCRATCH="${SCRATCH:-/tmp/mrgd-e2e}"; mkdir -p "$SCRATCH"
DEVICE="${ADB_DEVICE:?set ADB_DEVICE, e.g. 127.0.0.1:5555 or <waydroid-ip>:5555}"
PKG="chat.fluffy.fluffychat"
CADDY_LOG="${CADDY_LOG:-${MATRIX_HS_HOME:-.}/caddy_443.log}"
export XDG_RUNTIME_DIR=/run/user/0
export WAYLAND_DISPLAY=wayland-waydroid

log() { echo "[$(date +%H:%M:%S)] $*"; }

ensure_session() {
    if ! pgrep -f 'waydroid show-full-ui' > /dev/null; then
        nohup waydroid show-full-ui > "$SCRATCH/showui_persist.log" 2>&1 &
        disown
        sleep 3
    fi
    adb connect "$DEVICE" > /dev/null 2>&1
}

mark() { wc -l < "$CADDY_LOG"; }

show_since() {
    local before="$1" total
    total=$(wc -l < "$CADDY_LOG")
    [ "$total" -le "$before" ] && return
    tail -n "$((total - before))" "$CADDY_LOG" | python3 -c '
import json, sys
for line in sys.stdin:
    try:
        d = json.loads(line)
        r = d["request"]
        method = r["method"]
        uri = r["uri"]
        status = d.get("status")
        size = d.get("size")
        ua = r["headers"].get("User-Agent", [""])[0]
        if "Dart" in ua or "fluffy" in ua.lower():
            print("    {:6} {:60} -> {} ({}B)".format(method, uri, status, size))
    except Exception:
        pass
'
}

tap() { timeout 8 adb -s "$DEVICE" shell input tap "$1" "$2" > /dev/null 2>&1; }
type_text() { timeout 8 adb -s "$DEVICE" shell input text "$1" > /dev/null 2>&1; }
shot() { timeout 15 adb -s "$DEVICE" exec-out screencap -p > "$SCRATCH/$1.png" 2>/dev/null; }
resumed_activity() { timeout 8 adb -s "$DEVICE" shell dumpsys activity activities 2>&1 | grep -oE "$PKG/[.][A-Za-z]+" | head -1; }

wait_for_app() {
    local tries=0
    while [ "$tries" -lt 10 ]; do
        if [ -n "$(resumed_activity)" ]; then return 0; fi
        sleep 1
        tries=$((tries+1))
    done
    return 1
}

restart_app() {
    timeout 8 adb -s "$DEVICE" shell am force-stop "$PKG" > /dev/null 2>&1
    sleep 1
    timeout 8 adb -s "$DEVICE" shell am start -n "$PKG/.MainActivity" > /dev/null 2>&1
    if wait_for_app; then
        log "app resumed: $(resumed_activity)"
    else
        log "WARNING: app did not report as resumed activity after start"
    fi
    sleep 3
}

# ── REGISTRATION FLOW ────────────────────────────────────────────────────────
run_registration() {
    local user="$1" pass="$2"
    log "=== REGISTRATION: user=$user ==="
    restart_app
    m=$(mark); tap 493 1093; sleep 3; show_since "$m"   # tap "Create new account"
    shot "reg_01_after_create_tap"

    m=$(mark); tap 536 875; sleep 1; type_text "m.hubd.net"; sleep 2; show_since "$m"
    shot "reg_02_homeserver_typed"

    m=$(mark); tap 536 950; sleep 2; show_since "$m"   # tap discovered suggestion
    shot "reg_03_suggestion_tapped"

    m=$(mark); tap 536 1347; sleep 3; show_since "$m"  # tap Continue
    shot "reg_04_after_continue"

    m=$(mark); tap 536 950; sleep 1; type_text "$user"; sleep 1; show_since "$m"
    shot "reg_05_username_typed"

    m=$(mark); tap 536 1100; sleep 1; type_text "$pass"; sleep 1; show_since "$m"
    shot "reg_06_password_typed"

    m=$(mark); tap 536 1350; sleep 5; show_since "$m"  # submit
    shot "reg_07_after_submit"
    log "=== REGISTRATION step sequence done — see reg_07_after_submit.png + logs above ==="
}

# ── LOGIN FLOW (plain username, not full MXID) ──────────────────────────────
run_login_plain_username() {
    local user="$1" pass="$2"
    log "=== LOGIN (plain username): user=$user ==="
    restart_app
    m=$(mark); tap 538 1271; sleep 3; show_since "$m"  # tap "Sign in"
    shot "login_01_signin_screen"

    m=$(mark); tap 536 875; sleep 1; type_text "m.hubd.net"; sleep 2; show_since "$m"
    shot "login_02_homeserver_typed"

    m=$(mark); tap 536 950; sleep 2; show_since "$m"
    shot "login_03_suggestion_tapped"

    m=$(mark); tap 536 1347; sleep 3; show_since "$m"  # Continue
    shot "login_04_credentials_screen"

    m=$(mark); type_text "$user"; sleep 1; show_since "$m"   # field already focused
    shot "login_05_username_typed"

    m=$(mark); tap 536 1100; sleep 1; type_text "$pass"; sleep 1; show_since "$m"
    shot "login_06_password_typed"

    m=$(mark); tap 536 1500; sleep 5; show_since "$m"
    shot "login_07_after_submit"
    log "=== LOGIN step sequence done — see login_07_after_submit.png + logs above ==="
}

ensure_session
run_registration "e2euser$(date +%s | tail -c 5)" "TestPass123"
run_login_plain_username "debugtest2" "testpw123"
log "ALL DONE"
