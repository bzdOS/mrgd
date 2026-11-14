# Soak run verdict (single-node stand, 2026-11-13)

## Setup
- Single-node stand (isolated, not restarted during the run). No production data touched.
- Binary and memory cap per runbook in effect at launch (cap observed as unreadable from recorder context; treated as observation, not a stop condition).
- Window: 36 samples over ~5.86 h (`2026-11-13T07:26:26Z`–`2026-11-13T13:18:13Z`, 10 min interval). Gates did not trip; recorder finished with process still alive.

## Measurements (from run CSV)
- RSS (MiB): warmup 224.6 → 225.3 → 78.3 (drop/reclaim), then 78.7 → 100.9 across the measured window.
- Theil–Sen RSS slope (MiB/h), by window (same CSV):
  - full ok window (33 samples, `07:56:33Z`–`13:18:13Z`, 5.36 h): **+4.807**
  - last 3 h (18 samples, `10:27:05Z`–`13:18:13Z`, 2.85 h): **+2.391**
  - tail segment 74.4→100.9 MiB (22 samples, `09:46:56Z`–`13:18:13Z`, 3.52 h): **+3.450**

## Conclusion
- The previously suspected ~15 MiB/h growth is **not confirmed at RSS level** in this run (observed RSS slopes +2.39 to +4.81 MiB/h over the analyzed windows, no gate breach).
- Allocator-level view (`mp_alloc` and related gauges) was unreadable in this run; this run therefore **neither confirms nor refutes** leak/growth at the allocator level.

## Load (reported separately)
- In-run traffic generator: 56 successful `sent + synced` messages (`07:24:38Z`–`07:52:38Z`, counter stable in CSV), recorded separately from RSS/allocator columns.
- External unlabeled traffic: 8 short probe runs (each register/login/room/send/sync/e2ee, ~8 API calls) during `11:23Z`–`11:39Z`, no further probes observed; stand responded HTTP 200 on the versions endpoint at `13:43Z`. Not present in the in-run traffic counter; counted as separate stand load in interpretation.

## Reproducing the slope from the CSV

Exact command (reads the run CSV; computes Theil–Sen RSS slope for the three windows above). Replace `CSV` with the path to the run CSV:

```sh
python3 - "$CSV" <<'PY'
import csv, statistics, sys
rows = list(csv.DictReader(open(sys.argv[1])))
ok = [r for r in rows if r.get('gate') == 'ok']
def ts(rs):
    xs = [int(r['epoch']) for r in rs]; ys = [float(r['rss_mib']) for r in rs]
    s = []
    for i in range(len(xs)):
        for j in range(i+1, len(xs)):
            dx = (xs[j]-xs[i]) / 3600.0
            if dx > 0: s.append((ys[j]-ys[i]) / dx)
    return round(statistics.median(s), 3), len(rs), round((xs[-1]-xs[0])/3600.0, 3), rs[0]['iso'], rs[-1]['iso'], ys[0], ys[-1]
for name, rs in [('ok_window', ok), ('last_3h', ok[-18:]), ('tail_74.4_to_100.9', ok[11:])]:
    slope, n, hours, t0, t1, y0, y1 = ts(rs)
    print(f'{name}: n={n} hours={hours} slope_mib_per_h={slope} rss {y0}->{y1} ({t0}..{t1})')
PY
```

Expected output for this run's CSV:
```
ok_window: n=33 hours=5.361 slope_mib_per_h=4.807 rss 78.7->100.9 (2026-11-13T07:56:33Z..2026-11-13T13:18:13Z)
last_3h: n=18 hours=2.852 slope_mib_per_h=2.391 rss 94.4->100.9 (2026-11-13T10:27:05Z..2026-11-13T13:18:13Z)
tail_74.4_to_100.9: n=22 hours=3.521 slope_mib_per_h=3.45 rss 74.4->100.9 (2026-11-13T09:46:56Z..2026-11-13T13:18:13Z)
```
