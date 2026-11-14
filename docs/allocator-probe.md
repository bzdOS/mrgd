# Allocator metrics readability for future soak runs (design note)

## Context

The 2026-11-13 single-node soak recorded RSS and gate series but left allocator-level
gauges unreadable in that run's capture, so the ~15 MiB/h suspicion could not be
confirmed or refuted at allocator level. This note answers: how can the next soak
obtain allocator live-bytes (`mp_alloc`) and related metrics **without expanding
permissions** (rights frozen)? Code is not changed here.

## Recon (current codebase)

- **In-process probe exists (file + signal, no HTTP):** `src/memprobe.rs` samples
  allocator statistics from inside the running server and appends one CSV row per
  sample. It is inert unless `MATRIX_HS_MEMPROBE_LOG=<path>` is set; the sampler
  writes a row on start and on `SIGUSR2`. Rows carry epoch/seq/trigger plus
  allocated, resident, mapped, retained and per-arena counters, with structure gauges
  (to-device-seen size, typing outer room keys, UIA sessions) appended at the end.
  An unset/unregistered gauge is emitted as empty (not zero).
- **Recorder reads the file:** `scripts/soak-run.sh` requires `--memprobe <csv>` and,
  each checkpoint, pokes `SIGUSR2` and reads the last row's allocated/resident fields
  into `mp_alloc_mib`/`mp_resident_mib`; without a readable memprobe file `mp_alloc`
  stays unavailable and the growth gates are vacuous. The probe thread is registered
  in `src/main.rs` (`memprobe::install()` / `memprobe::watch(...)`) when the env var
  is set.
- **No HTTP/admin surface found:** there is no memprobe, admin, or metrics route in
  `src/routes/` and no allocator-exposing HTTP endpoint in the router; only the
  file+signal path above exists.

## Options (narrowest first)

### (a) Reuse existing file+signal probe — make the capture readable (no code change)

- **What it measures:** exactly what the probe already emits — allocator allocated/
  resident/mapped/retained, purge/decay counters, and the appended structure gauges
  (including `mp_alloc`, the series the growth gates read). Same semantics as today.
- **How (within frozen rights):** run the recorder in a context that can (1) read the
  memprobe log path and (2) deliver `SIGUSR2` to the server process, and ensure the
  log path is one that context can open. Concretely: launch/observe the pair so the
  sampling file lives at a path readable by the recorder's identity and the signal is
  deliverable between them — i.e. align ownership/mode of the log file and the
  poke/reader to identities that already have that access, without granting new rights.
  (If they cannot be aligned under frozen rights, this option is not viable and (b)/(c)
  must be chosen instead.)
- **What it does not solve:** cannot make an unreadable process environment or an
  unreadable/un-writable log path readable by adding privileges (rights frozen); does
  not create an HTTP view; does not help if the probe is not enabled for the run.
- **Risk / rights:** lowest — no new code path, no new surface, no privilege change;
  relies only on existing, already-deployed behavior and pre-existing file/signal
  access. Failure mode is "still unreadable" (observable, non-silent if checked).

### (b) Add a narrow read-only endpoint serving the latest probe sample (code change)

- **What it measures:** a read-only snapshot of the same allocator/structure counters
  the probe already computes (e.g. last sample / current stats), exposed over an
  authenticated or loopback-only route so the recorder can fetch it over HTTP instead
  of reading the log/signalling the process.
- **What it does not solve:** does not remove the need for the server to compute the
  counters; fetching still requires network reachability to that route from the
  recorder (under frozen rights/topology); does not retroactively fix past runs.
- **Risk / rights:** adds a new code path and exposed surface (must be strictly
  read-only, minimal fields, and access-controlled); requires code change + deploy to
  take effect; more moving parts than (a). Medium risk relative to (a).

### (c) Server writes a periodic snapshot to a readable location (code change)

- **What it measures:** the same counter set, written by the server on an interval or
  on-signal to a known file/location the recorder can read, decoupling capture from
  the recorder's ability to signal the process.
- **What it does not solve:** the write target must be writable by the server and
  readable by the recorder under existing rights (path/ownership constraints remain);
  still needs enablement; interval/clock and partial-write handling become concerns.
- **Risk / rights:** adds file-output behavior in the server (path handling, rotation/
  overwrite, partial writes); depends on a shared-readable location existing under
  frozen rights. Medium risk, and shifts complexity into durability of the snapshot.

## Recommended narrow route

**Prefer (a): reuse the existing file+signal probe, with the memprobe log placed at a
path the recorder's identity can already read and with the recorder able to send the
sample signal under existing (frozen) rights.**

- **What it measures:** allocator live-bytes and the companion counters/gauges the
  probe already emits (`mp_alloc` and peers) — the exact series the recorder and
  growth gates consume.
- **What it does not solve:** it yields nothing if the recorder cannot, under frozen
  rights, read that log path or signal the process, and it provides no HTTP view; in
  that case the blocked access is a rights/topology constraint to report, not to work
  around here. It also cannot recover allocator detail for runs where the probe was
  never enabled.

This is the narrowest path: zero code change, no new surface, uses the probe and
recorder logic that already exist; only the readability/alignment of the existing
capture path is required.
