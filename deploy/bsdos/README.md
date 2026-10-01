# Deploying the bsdOS mesh nodes

How matrix-hs runs on the bsdOS hosts: buildhost (systemd), the dev VM and myvm
(FreeBSD rc.d), plus the mesh tunnels, the Zenoh router on 127.0.0.1:7449,
store backups and the canary. Moved here from the bsdOS monorepo on
2026-10-01.

| Path | Runs on | What |
|---|---|---|
| `rc.d/mrgd_node_185`, `rc.d/mrgd_node_186`, `rc.d/mrgd_canary_186`, `rc.d/bsdos_matrix` | FreeBSD guests | matrix-hs node / canary |
| `systemd/matrix-hs*.service` | buildhost | local edge node |
| `systemd/hubd-queue-router.service` | buildhost | Zenoh router the tunnels forward 7449 to |
| `systemd/mesh-*.service` | buildhost | ssh tunnels to 185 / 186 / fedora, home rendezvous |
| `systemd/mrgd-store-backup.*`, `scripts/backup-mesh-stores.sh` | buildhost | store backups across nodes |
| `systemd/mrgd-sync-fedora.*` | buildhost | pushes /opt/mrgd to fedora |
| `systemd/scope-canary*.service` | buildhost | canary |
| `scripts/push-mrgd-185.sh` | buildhost | build and install on 185 |

Host values come from `/etc/bsdos/hosts.env` (`BSDOS_DEV_IP`, `BSDOS_MYVM_IP`,
`BSDOS_SSH_KEY`, `BSDOS_ROOT`, ...); units read it with `EnvironmentFile=`,
scripts source it and stop if a value is missing. Unit env files go to
`/etc/bsdos/` (`matrix-hs.env`); scripts are installed to
`/usr/local/libexec/bsdos/` — units must not run files from a working copy.

As of 2026-10-01 the units deployed on buildhost still point at the old
`/srv/bsdos` paths; switching them over is stage 1 of the host-split plan.
`mrgd-sync-fedora` has been failing since `/opt/mrgd` left buildhost that day.
