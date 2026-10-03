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
| `systemd/mrgd-sync-offsite.*` | build-host | pushes `<install-root>` to offsite |
| `systemd/scope-canary*.service` | build-host | canary |
| `scripts/push-mrgd-guest-a.sh` | build-host | build and install on guest-a |
| `scripts/stand-lift.sh` | FreeBSD guests | idempotent post-boot lift of a stand (see below) |

Host values come from `/etc/fleet/hosts.env` (`BSDOS_DEV_IP`, `BSDOS_GUEST_B_IP`,
`BSDOS_SSH_KEY`, `BSDOS_APP_ROOT`, `BSDOS_STORE_DIR`, ...); units read it with
`EnvironmentFile=`,
scripts source it and stop if a value is missing. Unit env files go to
`/etc/fleet/` (`matrix-hs.env`); scripts are installed to
`/usr/local/libexec/fleet/` — units must not run files from a working copy.

As of 2026-10-01 the units deployed on build-host still point at the old
`<mesh-root>` paths; switching them over is stage 1 of the host-split plan.
`mrgd-sync-offsite` has been failing since `<install-root>` left build-host that
day. The paths these units carry are deployment-specific and are written here as
placeholders: `<install-root>` (the build root), `<mesh-root>` (the old shared
root) and `<bus-root>` (the bus scope's own root).

## Post-boot stand lift on a FreeBSD guest

A guest reboot takes the stand down with it, and everything downstream —
profiling, measurement windows, client test runs — needs it back before it can
mean anything. The lift is therefore a script, not a remembered shell line.
Install it like any other script, never run it from a working copy:

```sh
install -m 755 scripts/stand-lift.sh /usr/local/libexec/fleet/stand-lift.sh
```

Run it as the unprivileged node user, not as root — it writes the pidfile and
the log into the node's own state directory:

```sh
sudo -n -u <node-user> env STAND_HOME=<state-dir> /usr/local/libexec/fleet/stand-lift.sh
```

`STAND_HOME` is the node's state directory, the one holding `mrgd.env`,
`bin/matrix-hs` and `soak/`. It is a parameter because the real path is
host-local and this file is published; the port is read out of
`MATRIX_HS_LISTEN` in that env file for the same reason. Everything else
(`STAND_BIN`, `STAND_ENV`, `STAND_LOG`, `STAND_PIDFILE`, `STAND_MEMPROBE`)
derives from `STAND_HOME` and rarely needs overriding.

What it does, in order, and what each outcome means:

| Situation | Result |
|---|---|
| pidfile holds a live pid | no-op, exit 0, nothing touched — the idempotence gate |
| pidfile is stale | removed, then continue |
| port listens, no pidfile | refuses (exit 1): not our process, and a second matrix-hs on one data_dir is never started |
| `MATRIX_HS_LISTEN` is a wildcard address | refuses before `exec` |
| env carries hubd queue dirs | refuses before `exec` — the hub bridge stays off |
| otherwise | `daemon(8)`, then poll up to 30 s for the port to really listen |

`daemon(8)` is load-bearing, not decoration: `exec "$BIN"` without it dies with
the operator's process group about a minute after the lift, which reads as "it
came up and fell over". `daemon -p` writes the child's pid (the server), not
the wrapper's — that is the pid a USR2 poke must reach, and the pid the
idempotence gate checks for liveness.

Verify the lift by what it promised, not by the exit code alone:

```sh
sockstat -4 -l -p <port> | grep ":<port>"     # exactly one socket, on the configured address
curl -s -o /dev/null -w '%{http_code}\n' \
    http://<listen-host>:<port>/_matrix/client/versions
```

A measurement window is **not** this script: a window starts its own launcher,
its own binary and its own allocator settings, on an explicit GO. The lift
exists so the stand can be brought up, checked and taken down again.

### Turning the lift into boot-time automation

Two options, and the order is not optional:

1. `rc.d/bsdos_matrix_enable=YES` — `rc.d/bsdos_matrix` already supervises
   matrix-hs the same way (pidfile, `daemon -f -o -p`, SIGTERM to the wrapper).
   It was left `NO` because an earlier grow-set attempt failed to converge;
   enabling it before convergence is measured puts that failure back in place
   unattended, on every reboot, where nobody is watching.
2. A post-boot step that runs the lift above.

Flip plan, once a full window has shown the grow-set converging — RSS flat and
`data_dir` bounded across the whole window, not only at its end:

1. Record the current run: pid from `<state-dir>/mrgd.pid`, `sockstat`, and
   `versions`. This is the rollback reference.
2. `sysrc bsdos_matrix_enable=YES` plus the per-node block from the rc script's
   own header. `bsdos_matrix_listen` must be the same address the stand used —
   never the wildcard default — and `node_id` and `data_dir` must match it too.
3. Stop the hand-lifted stand by the exact pid from step 1 (no `pkill`, no
   `killall`), then confirm the port is free.
4. `service bsdos_matrix start`, then run both verify lines above.
5. On any failure of 3-4: `sysrc bsdos_matrix_enable=NO`,
   `service bsdos_matrix stop`, run the lift script again. The hand-lifted path
   stays available precisely because it needs no root.

Steps 2-4 need root on the guest, so they are the host's button to press, not
the worker's.
