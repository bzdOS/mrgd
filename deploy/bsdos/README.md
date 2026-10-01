# Deploying the bsdOS mesh nodes

How matrix-hs runs on the bsdOS hosts: build-host (systemd), guest-a and guest-b
(FreeBSD rc.d), plus the mesh tunnels, the Zenoh router on 127.0.0.1:7449,
store backups and the canary. Moved here from the bsdOS monorepo on
2026-10-01.

| Path | Runs on | What |
|---|---|---|
| `rc.d/mrgd_node_guest-a`, `rc.d/mrgd_node_guest-b`, `rc.d/mrgd_canary_guest-b`, `rc.d/bsdos_matrix` | FreeBSD guests | matrix-hs node / canary |
| `systemd/matrix-hs*.service` | build-host | local edge node |
| `systemd/hubd-queue-router.service` | build-host | Zenoh router the tunnels forward 7449 to |
| `systemd/mesh-*.service` | build-host | ssh tunnels to guest-a / guest-b / offsite, home rendezvous |
| `systemd/mrgd-store-backup.*`, `scripts/backup-mesh-stores.sh` | build-host | store backups across nodes |
| `systemd/mrgd-sync-offsite.*` | build-host | pushes /srv/app to offsite |
| `systemd/scope-canary*.service` | build-host | canary |
| `scripts/push-mrgd-guest-a.sh` | build-host | build and install on guest-a |

Host values come from `/etc/fleet/hosts.env` (`BSDOS_DEV_IP`, `BSDOS_GUEST_B_IP`,
`BSDOS_SSH_KEY`, `BSDOS_ROOT`, ...); units read it with `EnvironmentFile=`,
scripts source it and stop if a value is missing. Unit env files go to
`/etc/fleet/` (`matrix-hs.env`); scripts are installed to
`/usr/local/libexec/fleet/` — units must not run files from a working copy.

As of 2026-10-01 the units deployed on build-host still point at the old
`/srv/mesh` paths; switching them over is stage 1 of the host-split plan.
`mrgd-sync-offsite` has been failing since `/srv/app` left build-host that day.
