#!/bin/sh
[ -r "${BSDOS_HOSTS_ENV:-/etc/fleet/hosts.env}" ] && . "${BSDOS_HOSTS_ENV:-/etc/fleet/hosts.env}"
# backup-mesh-stores — cross-host backups of the three bus node stores.
#
# Rationale (2026-08-23): the repo history lives in two places (build-host +
# offsite clone), but the STORES themselves — room journals, accounts, media,
# node signing keys — were backed up nowhere. One disk death = the mesh's
# data gone. Scope rule (SCOPES.md): backups of bus data stay on bus hosts;
# offsite (home) is never a target, build-host never holds a copy it doesn't
# already have.
#
# Topology — each store lands on a DIFFERENT bus host than its source:
#   build-host store -> guest-a      (the build host)
#   guest-a store    -> guest-b
#   guest-b store    -> guest-a
# All streams pipe through build-host (it holds the credentials; bytes transit,
# nothing is written here — the same rendezvous pattern as the mesh itself).
#
# Retention: 7 daily archives per store on each target (KEEP=7).
# Integrity: every archive is listed (tar -tzf) after writing; a zero-exit
# with entries counts as good. Sizes are logged for drift detection.
#
# Usage: run from cron/systemd timer on build-host. Exit non-zero if ANY leg
# fails, so the timer unit shows degraded.

set -u

KEY=${BSDOS_SSH_KEY:?set BSDOS_SSH_KEY in /etc/fleet/hosts.env}
SSH_A="ssh -i $KEY -o BatchMode=yes -o ConnectTimeout=20"
SSH_B="ssh -i $KEY -o BatchMode=yes -o ConnectTimeout=20"
KEEP=7
TS=$(date +%Y%m%d-%H%M%S)
HOST_STORE="${BSDOS_STORE_DIR:?set BSDOS_STORE_DIR (the node's store directory) in /etc/fleet/hosts.env}"
RC=0

backup() {
  # backup <label> <src-ssh-cmd|local> <src-path> <dst-ssh-cmd> <dst-dir>
  label=$1; srccmd=$2; srcpath=$3; dstcmd=$4; dstdir=$5
  echo "==> $label"
  # ensure target dir + retention on the destination host
  $dstcmd "mkdir -p $dstdir && ls -t $dstdir/*.tgz 2>/dev/null | tail -n +$((KEEP+1)) | xargs -r rm -f"
  # stream: src tar -> dst file
  if [ "$srccmd" = "local" ]; then
    tar czf - -C "$(dirname $srcpath)" "$(basename $srcpath)" \
      | $dstcmd "cat > $dstdir/$label-$TS.tgz"
  else
    $srccmd "tar czf - -C $(dirname $srcpath) $(basename $srcpath)" \
      | $dstcmd "cat > $dstdir/$label-$TS.tgz"
  fi
  # integrity: tar must list cleanly and have entries
  ENTRIES=$($dstcmd "tar -tzf $dstdir/$label-$TS.tgz 2>/dev/null | wc -l")
  SIZE=$($dstcmd "stat -f %z $dstdir/$label-$TS.tgz 2>/dev/null || stat -c %s $dstdir/$label-$TS.tgz 2>/dev/null")
  if [ "${ENTRIES:-0}" -gt 1 ] 2>/dev/null; then
    echo "    ok: $label-$TS.tgz ($SIZE bytes, $ENTRIES entries)"
  else
    echo "    FAIL: $label archive unreadable ($ENTRIES entries)"
    RC=1
  fi
}

backup build-host-store local "$HOST_STORE" "$SSH_A freebsd@${BSDOS_DEV_IP:?set BSDOS_DEV_IP in /etc/fleet/hosts.env}" ~/backups
backup store-guest-a "$SSH_A freebsd@${BSDOS_DEV_IP:?set BSDOS_DEV_IP in /etc/fleet/hosts.env}" ~/store-guest-a "$SSH_B freebsd@${BSDOS_GUEST_B_IP:?set BSDOS_GUEST_B_IP in /etc/fleet/hosts.env}" ~/backups
backup store-guest-b "$SSH_B freebsd@${BSDOS_GUEST_B_IP:?set BSDOS_GUEST_B_IP in /etc/fleet/hosts.env}" ~/store-guest-b "$SSH_A freebsd@${BSDOS_DEV_IP:?set BSDOS_DEV_IP in /etc/fleet/hosts.env}" ~/backups

echo "done rc=$RC"
exit $RC
