# Zenoh transport-security guard: startup verification

Confirms that the startup guard `validate_transport_security()` (`src/cluster_env.rs`)
refuses plaintext `tcp/` on a non-loopback address and lets the allowed cases through.

- Binary: `matrix-hs` built with `--features cluster` (release), sha256 `1517a143c366deec4e59653ef4f7b00fce865d22204a6bc74d0603d746a82e1d`
- Source: main at `f4443f6` (guard introduced in `111482c`)
- Environment for all cases: test server name, ephemeral data dir, `MATRIX_HS_LISTEN=127.0.0.1:<port>`,
  `MATRIX_HS_ZENOH_SCOUTING=off`, `MATRIX_HS_ZENOH_MODE=peer`. The PSK is a random 32-byte test value (base64), shown as `<psk>`.

Command template (per case, only `MATRIX_HS_ZENOH_LISTEN` differs):

```sh
MATRIX_HS_SERVER_NAME=test.invalid MATRIX_HS_DATA_DIR=<tmpdir> MATRIX_HS_LISTEN=127.0.0.1:<port> \
MATRIX_HS_ZENOH_SCOUTING=off MATRIX_HS_ZENOH_MODE=peer \
MATRIX_HS_ZENOH_LISTEN="<endpoint>" <bin>
```

## 1. Plaintext tcp/ on a non-loopback address: guard refuses

`<endpoint>` = `tcp/0.0.0.0:<port>` -> exit code 1:

```
Error: "refusing to start: MATRIX_HS_ZENOH_LISTEN: plaintext tcp/ endpoint on non-loopback address 0.0.0.0 is forbidden; use obfs/ or TLS instead (see .env.example rule)"
```

## 2. obfs/ on a non-loopback address: guard passes

`<endpoint>` = `obfs/192.0.2.10:<port>#obfs_psk_base64=<psk>` (RFC 5737 documentation address, not assigned
to the host). No refusal text; the process gets past the guard and fails only later at socket bind, which proves
the guard let it through. Exit code 1:

```
cluster mode: connect=None listen=Some("obfs/192.0.2.10:<port>#obfs_psk_base64=<psk>") mode=peer scouting=off prefix=mrgd/matrix/room
[obfs-new-listener] ENTER: 192.0.2.10:<port>
[obfs-new-listener] bind FAILED on 192.0.2.10:<port>: 192.0.2.10:<port>: Can't assign requested address (os error 49) at vendor/zenoh-link-commons-patched/src/tcp.rs:53.
```

## 3. Plaintext tcp/ on loopback: guard passes, node starts

`<endpoint>` = `tcp/127.0.0.1:<port>`. No refusal; the node started and kept running until stopped
with SIGTERM (clean shutdown, exit code 0):

```
cluster mode: connect=None listen=Some("tcp/127.0.0.1:<port>") mode=peer scouting=off prefix=mrgd/matrix/room
...
matrix-hs listening on http://127.0.0.1:<port>
[matrix-hs] SIGTERM received — draining accepted requests, then exiting through main()
```

Result: 3/3 as expected. Every test process was stopped and confirmed gone.
