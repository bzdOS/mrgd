#!/bin/bash
# E2E test harness: drives FluffyChat (real Android client, via Waydroid+adb)
# through the login flow against a matrix-hs instance, capturing a screenshot
# and the corresponding Caddy access-log slice after each step so a failure
# can be diagnosed from server-side evidence, not just a screenshot.
#
# Usage: e2e_fluffychat.sh <step-name>
#   step-name: fresh_launch | tap_sign_in | enter_homeserver | tap_continue |
#              enter_username <user> | enter_password <pass> | tap_login |
#              screenshot (just capture, no action) | tap <x> <y>
#
# Requires: waydroid session RUNNING, adb connected to $ADB_DEVICE,
# a persistent `waydroid show-full-ui` process keeping the container unfrozen
# (this script does NOT manage that — see setup_session below).

set -uo pipefail

SCRATCH="${SCRATCH:-/tmp/mrgd-e2e}"; mkdir -p "$SCRATCH"
DEVICE="${ADB_DEVICE:?set ADB_DEVICE, e.g. 127.0.0.1:5555 or <waydroid-ip>:5555}"
PKG="chat.fluffy.fluffychat"
CADDY_LOG="${CADDY_LOG:-${MATRIX_HS_HOME:-.}/caddy_443.log}"

log_marker_file="$SCRATCH/e2e_caddy_marker.txt"

mark_caddy_log() {
    wc -l < "$CADDY_LOG" > "$log_marker_file"
}

show_caddy_since_marker() {
    local before=0
    [ -f "$log_marker_file" ] && before=$(cat "$log_marker_file")
    local total
    total=$(wc -l < "$CADDY_LOG")
    if [ "$total" -gt "$before" ]; then
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
        print("{:6} {:70} -> {} ({}B) ua={}".format(method, uri, status, size, ua))
    except Exception:
        pass
'
    fi
}

setup_session() {
    export XDG_RUNTIME_DIR=/run/user/0
    export WAYLAND_DISPLAY=wayland-waydroid
    if ! pgrep -f 'waydroid show-full-ui' > /dev/null; then
        nohup waydroid show-full-ui > "$SCRATCH/showui_persist.log" 2>&1 &
        disown
        sleep 3
    fi
    adb connect "$DEVICE" > /dev/null 2>&1
}

screenshot() {
    local name="$1"
    timeout 15 adb -s "$DEVICE" exec-out screencap -p > "$SCRATCH/e2e_$name.png"
    echo "screenshot: $SCRATCH/e2e_$name.png"
}

step_fresh_launch() {
    mark_caddy_log
    timeout 8 adb -s "$DEVICE" shell am force-stop "$PKG" 2>&1
    sleep 1
    timeout 8 adb -s "$DEVICE" shell am start -n "$PKG/.MainActivity" 2>&1
    sleep 6
    screenshot "01_launch"
    show_caddy_since_marker
}

step_tap_sign_in() {
    mark_caddy_log
    timeout 8 adb -s "$DEVICE" shell input tap 538 1271 2>&1
    sleep 3
    screenshot "02_login_screen"
    show_caddy_since_marker
}

step_tap_create_account() {
    mark_caddy_log
    timeout 8 adb -s "$DEVICE" shell input tap 538 1200 2>&1
    sleep 3
    screenshot "reg_02_after_create_account"
    show_caddy_since_marker
}

step_enter_homeserver() {
    mark_caddy_log
    timeout 8 adb -s "$DEVICE" shell input tap 536 875 2>&1
    sleep 1
    timeout 8 adb -s "$DEVICE" shell input text "m.hubd.net" 2>&1
    sleep 2
    screenshot "03_homeserver_typed"
    show_caddy_since_marker
}

step_tap_continue() {
    mark_caddy_log
    # tap the discovered homeserver suggestion first, then Continue
    timeout 8 adb -s "$DEVICE" shell input tap 536 950 2>&1
    sleep 2
    timeout 8 adb -s "$DEVICE" shell input tap 536 1347 2>&1
    sleep 3
    screenshot "04_after_continue"
    show_caddy_since_marker
}

step_enter_username() {
    local user="$1"
    mark_caddy_log
    # username field is expected near the top of the login-credentials form;
    # coordinates verified against a screenshot before first real use.
    timeout 8 adb -s "$DEVICE" shell input tap 536 950 2>&1
    sleep 1
    timeout 8 adb -s "$DEVICE" shell input text "$user" 2>&1
    sleep 1
    screenshot "05_username_typed"
    show_caddy_since_marker
}

step_enter_password() {
    local pass="$1"
    mark_caddy_log
    timeout 8 adb -s "$DEVICE" shell input tap 536 1100 2>&1
    sleep 1
    timeout 8 adb -s "$DEVICE" shell input text "$pass" 2>&1
    sleep 1
    screenshot "06_password_typed"
    show_caddy_since_marker
}

step_tap_login() {
    mark_caddy_log
    timeout 8 adb -s "$DEVICE" shell input tap 536 984 2>&1
    sleep 5
    screenshot "07_after_login"
    show_caddy_since_marker
}

# login_via_mxid: full, verified-working login flow using a complete Matrix ID
# (e.g. "@user:m.hubd.net"), which is the exact path that was broken before two
# fixes landed: (1) matrix-hs's empty-body 404/405 responses (commit adding
# fallback_unrecognized in lib.rs — a Matrix client's JSON error parser can
# choke on an empty body and surface a generic error instead of falling back
# gracefully), and (2) server_name being "localhost" (unresolvable by any real
# external client attempting MXID-domain-based homeserver rediscovery — fixed
# by migrating server_name to m.hubd.net, see the data migration of 2026-07-25).
step_login_via_mxid() {
    local mxid="$1" pass="$2"
    step_fresh_launch > /dev/null
    mark_caddy_log
    timeout 8 adb -s "$DEVICE" shell input tap 538 1271 2>&1   # Sign in
    sleep 3
    timeout 8 adb -s "$DEVICE" shell input tap 536 875 2>&1    # homeserver search field
    sleep 1
    timeout 8 adb -s "$DEVICE" shell input text "m.hubd.net" 2>&1
    sleep 2
    timeout 8 adb -s "$DEVICE" shell input tap 536 950 2>&1    # select suggestion
    sleep 2
    timeout 8 adb -s "$DEVICE" shell input tap 536 1347 2>&1   # Continue
    sleep 3
    timeout 8 adb -s "$DEVICE" shell input text "$mxid" 2>&1
    sleep 1
    timeout 8 adb -s "$DEVICE" shell input keyevent 66 2>&1    # ENTER -> password field
    sleep 3
    timeout 8 adb -s "$DEVICE" shell input tap 536 909 2>&1    # password field
    timeout 8 adb -s "$DEVICE" shell input text "$pass" 2>&1
    sleep 1
    timeout 8 adb -s "$DEVICE" shell input tap 536 984 2>&1    # Login
    sleep 5
    screenshot "login_via_mxid_result"
    show_caddy_since_marker
}

step_tap() {
    local x="$1" y="$2"
    mark_caddy_log
    timeout 8 adb -s "$DEVICE" shell input tap "$x" "$y" 2>&1
    sleep 3
    screenshot "tap_${x}_${y}"
    show_caddy_since_marker
}

step_screenshot() {
    mark_caddy_log
    screenshot "manual"
    show_caddy_since_marker
}

setup_session

case "${1:-}" in
    fresh_launch)      step_fresh_launch ;;
    tap_sign_in)        step_tap_sign_in ;;
    tap_create_account) step_tap_create_account ;;
    enter_homeserver)   step_enter_homeserver ;;
    tap_continue)       step_tap_continue ;;
    enter_username)     step_enter_username "${2:?username required}" ;;
    enter_password)     step_enter_password "${2:?password required}" ;;
    tap_login)          step_tap_login ;;
    login_via_mxid)     step_login_via_mxid "${2:?mxid required}" "${3:?password required}" ;;
    tap)                step_tap "${2:?x required}" "${3:?y required}" ;;
    screenshot)         step_screenshot ;;
    *)
        echo "Usage: $0 {fresh_launch|tap_sign_in|tap_create_account|enter_homeserver|tap_continue|enter_username <u>|enter_password <p>|tap_login|login_via_mxid <mxid> <password>|tap <x> <y>|screenshot}"
        echo ""
        echo "login_via_mxid is the recommended, fully-verified end-to-end regression"
        echo "check: fresh-launches FluffyChat and drives it through Sign in -> select"
        echo "m.hubd.net -> full Matrix ID -> password -> Login, exactly reproducing"
        echo "the real client flow that was broken (empty error bodies + server_name="
        echo "localhost). Example: $0 login_via_mxid '@debugtest2:m.hubd.net' testpw123"
        exit 1
        ;;
esac
