#!/bin/sh
[ -r "${BSDOS_HOSTS_ENV:-/etc/bsdos/hosts.env}" ] && . "${BSDOS_HOSTS_ENV:-/etc/bsdos/hosts.env}"
# push-mrgd-185 — refresh the 185 mesh node's source, rebuild, restart.
#
# The tree at ~freebsd/mrgd on 185 is a plain copy, not a git repo (see
# /opt/mrgd docs/TOPOLOGY.md 3c "Building on 185"). This script is the one-
# command form of that procedure: rsync from Buildhost, re-apply the two local
# Cargo.toml fixups the copy needs, rebuild, restart the rc.d service.
#
# Fixups (idempotent):
#   1. comment out the [patch.crates-io] zenoh-link* entries — the obfs
#      transport is not used on 185 and its vendored fork is untested on
#      FreeBSD; the zenoh-util FreeBSD stubs patch is required instead
#   2. ensure zenoh-util = { path = "/home/freebsd/zenoh-util-freebsd" } —
#      the FreeBSD stubs (docs/DESIGN.md "FreeBSD builds")
#
# Usage (from anywhere on Buildhost):  sh deploy/bsdos/scripts/push-mrgd-185.sh
# Env:   BUILD=0 to skip the rebuild/restart (source refresh only)

set -eu

SSH="ssh -i ${BSDOS_SSH_KEY:?set BSDOS_SSH_KEY in /etc/bsdos/hosts.env} -o BatchMode=yes"
REMOTE="freebsd@${BSDOS_DEV_IP:?set BSDOS_DEV_IP in /etc/bsdos/hosts.env}"
SRC=/opt/mrgd

echo "==> [1/4] rsync source (excluding target/, .git/)"
rsync -a --delete -e "$SSH" \
  --exclude target/ --exclude .git/ --exclude Cargo.toml.bak \
  "$SRC/src" "$SRC/vendor" "$SRC/Cargo.toml" "$SRC/Cargo.lock" "$SRC/rust-toolchain.toml" \
  "$REMOTE:mrgd/"

echo "==> [2/4] re-apply Cargo.toml fixups on 185"
$SSH "$REMOTE" 'cd ~/mrgd
# drop any previous fixups for a clean idempotent pass
sed -i.bak "/^zenoh-util = { path = .\/home\/freebsd\/zenoh-util-freebsd. }$/d; s/^#obfs-patch-disabled-on-185: //; /^zenoh-link  *= *{ path *= *\"vendor\//s/^/#obfs-patch-disabled-on-185: /; /^zenoh-link-commons *= *{ path *= *\"vendor\//s/^/#obfs-patch-disabled-on-185: /" Cargo.toml
awk "/^\\[patch.crates-io\\]/ && !done {print; print \"zenoh-util = { path = \\\"/home/freebsd/zenoh-util-freebsd\\\" }\"; done=1; next} {print}" Cargo.toml > Cargo.toml.new && mv Cargo.toml.new Cargo.toml
grep -A3 "patch.crates-io" Cargo.toml'

if [ "${BUILD:-1}" = "0" ]; then
  echo "==> BUILD=0 — source refreshed, skipping rebuild"
  exit 0
fi

echo "==> [3/4] rebuild (cluster)"
$SSH "$REMOTE" 'cd ~/mrgd && cargo build --release --features cluster --bin matrix-hs --bin scope-canary 2>&1 | tail -1'

echo "==> [4/4] install canary + restart node"
timeout 90 $SSH -o ConnectTimeout=10 "$REMOTE" 'su -m root -c "install -m 755 /home/freebsd/mrgd/target/release/scope-canary /usr/local/bin/scope-canary && service mrgd_node restart"'
sleep 6
$SSH "$REMOTE" 'su -m root -c "service mrgd_node status"; tail -3 ~/mhs.log'

echo "done."
