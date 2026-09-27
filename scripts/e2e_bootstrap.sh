#!/bin/bash
# Idempotent bootstrap for the Waydroid+FluffyChat E2E test environment.
# Safe to re-run after a host reboot: brings up weston (headless Wayland
# compositor), waydroid session, adb connection, and a persistent
# show-full-ui keep-alive (prevents the LXC container's freeze/idle timeout
# from silencing adb mid-test). No-ops any step already satisfied.
set -uo pipefail

export XDG_RUNTIME_DIR=/run/user/0
export WAYLAND_DISPLAY=wayland-waydroid
mkdir -p "$XDG_RUNTIME_DIR"
chmod 700 "$XDG_RUNTIME_DIR"

SCRATCH="${SCRATCH:-/tmp/mrgd-e2e}"; mkdir -p "$SCRATCH"
DEVICE="${ADB_DEVICE:?set ADB_DEVICE, e.g. 127.0.0.1:5555 or <waydroid-ip>:5555}"

# 1. weston headless compositor
if ! pgrep -f 'weston -B headless.*wayland-waydroid' > /dev/null; then
    echo "starting weston..."
    nohup weston -B headless --width=1080 --height=2220 --socket=wayland-waydroid \
        > "$SCRATCH/weston.log" 2>&1 &
    disown
    sleep 3
fi

# 2. pulse stub (mount-entry dependency for the container)
mkdir -p /run/user/0/pulse
touch /run/user/0/pulse/native

# 3. waydroid session
STATUS=$(waydroid status 2>&1 | grep -oP '(?<=Session:\t)\S+' || echo "STOPPED")
if [ "$STATUS" != "RUNNING" ]; then
    echo "starting waydroid session..."
    nohup waydroid session start > "$SCRATCH/waydroid_session.log" 2>&1 &
    disown
    for i in $(seq 1 30); do
        sleep 2
        if waydroid status 2>&1 | grep -q "RUNNING"; then break; fi
    done
fi
waydroid status 2>&1

# 4. adb connect (retry a few times — guest network/dnsmasq needs a moment)
for i in $(seq 1 10); do
    if adb connect "$DEVICE" 2>&1 | grep -q "connected to"; then break; fi
    sleep 3
done
adb devices -l

# 5. persistent show-full-ui to keep the container unfrozen for the whole test
if ! pgrep -f 'waydroid show-full-ui' > /dev/null; then
    echo "starting persistent show-full-ui..."
    nohup waydroid show-full-ui > "$SCRATCH/showui_persist.log" 2>&1 &
    disown
    sleep 3
fi

echo "=== bootstrap complete ==="
waydroid status 2>&1
adb -s "$DEVICE" get-state 2>&1
