#!/bin/sh
# soak-verdict.sh — one read-only verdict summary for a finished soak run.
#
# WHAT IT IS. The recorder writes a row every 300 s for six hours and appends gate lines
# to a separate log. That is the evidence; this is the reading of it. It prints one block
# and nothing else, because a verdict that takes a screen to say "nothing happened" is not
# a verdict — it is a second thing to read.
#
# WHAT IT DELIBERATELY DOES NOT DO. It has no thresholds and no verdict of its own. It
# counts and prints; the gate thresholds in soak-run.sh are the only thresholds in the
# project that may act on data, and duplicating them here would create a second place where
# a number becomes a decision. Two counters are reported because they are the two ways a
# run can lie about itself: gaps between rows mean the recorder stopped, and a big |d_rss|
# means a step that a min/max range hides. The owner reads the block and decides.
#
# The slope columns are printed as na until the 2-hour window has enough of the run in it
# (span >= 90% of win). That is a real state, not a missing value, and it is printed as-is.
#
# READ-ONLY. Opens two files and prints. Starts nothing, signals nothing, writes nothing,
# and takes no PID — the recorder is deliberately not part of this script's contract.
#
# Usage:
#   scripts/soak-verdict.sh [--csv PATH] [--gates PATH] [--gap-seconds N]
#
# Defaults match the recorder's own output names, resolved from this script's location so
# the command works from any directory.

set -eu

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
CSV="${SCRIPT_DIR}/../soak/soak-run-d.csv"
GATES="${SCRIPT_DIR}/../soak/soak-run-d.gates.log"
GAP_SECONDS=310

while [ $# -gt 0 ]; do
	case "$1" in
	--csv) CSV=${2:?--csv needs a path}; shift 2 ;;
	--gates) GATES=${2:?--gates needs a path}; shift 2 ;;
	--gap-seconds) GAP_SECONDS=${2:?--gap-seconds needs a number}; shift 2 ;;
	-h|--help) sed -n '2,24p' "$0" | cut -c3-; exit 0 ;;
	*) echo "soak-verdict: unknown argument: $1" >&2; exit 2 ;;
	esac
done

[ -r "$CSV" ] || { echo "soak-verdict: csv not readable: $CSV" >&2; exit 1; }

# Column indices are resolved from the header, not hardcoded, so a recorder that grows or
# reorders columns cannot make this report quietly about the wrong field. The names below
# are the ones this script prints; if the header loses one, that line says so instead of
# printing a zero.
col() { head -1 "$CSV" | awk -F, -v name="$1" '{ for (i=1; i<=NF; i++) if ($i==name) { print i; exit } }'; }

C_EPOCH=$(col epoch || true)
C_ISO=$(col iso || true)
C_RSS=$(col rss_mib || true)
C_DRSS=$(col d_rss_mib || true)
C_MP=$(col mp_alloc_mib || true)
C_SLOPE=$(col slope_theilsen_mib_h || true)
C_GATE=$(col gate || true)
[ -n "$C_EPOCH" ] && [ -n "$C_RSS" ] || {
	echo "soak-verdict: header lacks epoch/rss_mib — refusing to report on guessed columns" >&2
	exit 1
}

WARMUP_ROWS=$(awk -F, -v g="${C_GATE:-18}" 'NR>1 && $g=="warmup" {n++} END{print n+0}' "$CSV")
GATES_READABLE=0
[ -r "$GATES" ] && GATES_READABLE=1

echo "== soak verdict =="
echo "  csv:  $CSV"
[ "$GATES_READABLE" = "1" ] && echo "  gates: $GATES" || echo "  gates: (not readable)"

awk -F, \
	-v c_epoch="$C_EPOCH" -v c_iso="${C_ISO:-0}" -v c_rss="$C_RSS" \
	-v c_drss="${C_DRSS:-0}" -v c_mp="${C_MP:-0}" -v c_slope="${C_SLOPE:-0}" \
	-v c_gate="${C_GATE:-0}" -v gap="$GAP_SECONDS" -v gates_ok="$GATES_READABLE" -v gates="$GATES" '
function push_delta(x) { if (nd < 5000) { nd++; d[nd] = x } }
NR == 1 { next }
{
	n++
	if ($c_gate == "warmup") warm++
	if ($c_iso > "" && $c_iso != "0") { prev_iso = $c_iso }
	# gaps: the recorder promised 300 s; a bigger gap means it was not running
	if (n > 1) {
		dg = $c_epoch - prev_epoch
		if (dg > gap) gaps++
		if (dg > 0 && (step_min == "" || dg < step_min)) step_min = dg
		if (dg > step_max) step_max = dg
	}
	prev_epoch = $c_epoch
	r = $c_rss + 0
	if (n == 1 || r < rmin) rmin = r
	if (n == 1 || r > rmax) rmax = r
	rr[n] = r
	if ($c_drss != "" && $c_drss != "0" && $c_drss ~ /^-?[0-9]/) {
		push_delta($c_drss + 0)
		if (($c_drss + 0) > 150 || ($c_drss + 0) < -150) bigjump++
	}
	m = $c_mp + 0
	if (n == 1 || m < mpmin) mpmin = m
	if (n == 1 || m > mpmax) mpmax = m
	if ($c_slope != "" && $c_slope != "0" && $c_slope ~ /^[0-9.-]+$/ && $c_slope != "na") {
		sv = $c_slope + 0
		if (ns == 0) { slast = sv; smin = sv; smax = sv }
		else { if (sv < smin) smin = sv; if (sv > smax) smax = sv }
		slast = sv
		ns++
	}
}
END {
	# median of RSS: sort a copy, do not disturb the running minimum/maximum
	if (n > 0) {
		for (i = 1; i <= n; i++) s[i] = rr[i]
		for (i = 2; i <= n; i++) { v = s[i]; j = i - 1; while (j >= 1 && s[j] > v) { s[j + 1] = s[j]; j-- } s[j + 1] = v }
		med = (n % 2) ? s[(n + 1) / 2] : (s[n / 2] + s[n / 2 + 1]) / 2
	}
	printf "  рядов: %d всего / %d после warmup\n", n, n - warm
	printf "  шаг между рядами: min=%d s max=%d s | пробелов >%d s: %d\n", step_min, step_max, gap, gaps + 0
	printf "  rss_mib: min=%.1f median=%.1f max=%.1f | |d_rss|>150: %d\n", rmin, med, rmax, bigjump + 0
	if (ns > 0)
		printf "  slope_theilsen (Theil-Sen, mp_alloc, МиБ/ч): последний=%+.3f min=%+.3f max=%+.3f по %d рядах\n", slast, smin, smax, ns
	else
		printf "  slope_theilsen (Theil-Sen, mp_alloc, МиБ/ч): последний=na min=na max=na (окно 2ч ещё не набрано)\n"
	printf "  mp_alloc_mib: min=%.1f max=%.1f (размах %.1f)\n", mpmin, mpmax, mpmax - mpmin
	if (gates_ok == 1) {
		kills = 0; warns = 0; banner = 0
		while ((getline line < gates) > 0) {
			if (line ~ /^.*gates \(/) { banner = 1; continue }
			if (banner == 0) continue
			if (line ~ /killed pid/) kills++
			if (line ~ /\[warn\]/) warns++
		}
		close(gates)
		printf "  gates.log после баннера: KILL-строк=%d | rate [warn]=%d\n", kills, warns
	} else {
		printf "  gates.log: не читается — счётчики гейтов пропущены\n"
	}
}' "$CSV"
