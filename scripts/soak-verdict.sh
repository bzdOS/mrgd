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
# LOAD WINDOWS. A probe inside a declared window is still a probe, but it answers a
# different question: it measures the node under someone else's load, not the node's
# own trend. --windows points at a plain list of such windows, one per line:
#
#   <from-iso> <to-iso> <label...>
#   # comments and blank lines are ignored; ISO strings compare correctly as strings
#
# The block then prints what each window covered (probe count, RSS and mp_alloc range
# inside it) and the RSS trend over the probes OUTSIDE every window, next to the same
# trend over all of them, so the difference is visible rather than argued about. The
# RSS trend is REPORT-ONLY: it has no threshold here and gates nothing. mp_alloc
# remains what the gates read, unchanged and unretuned.
#
# Usage:
#   scripts/soak-verdict.sh [--csv PATH] [--gates PATH] [--gap-seconds N] [--windows PATH]
#
# Defaults match the recorder's own output names, resolved from this script's location so
# the command works from any directory.

set -eu

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
CSV="${SCRIPT_DIR}/../soak/soak-run-d.csv"
GATES="${SCRIPT_DIR}/../soak/soak-run-d.gates.log"
GAP_SECONDS=310
WINDOWS=""

while [ $# -gt 0 ]; do
	case "$1" in
	--csv) CSV=${2:?--csv needs a path}; shift 2 ;;
	--gates) GATES=${2:?--gates needs a path}; shift 2 ;;
	--gap-seconds) GAP_SECONDS=${2:?--gap-seconds needs a number}; shift 2 ;;
	--windows) WINDOWS=${2:?--windows needs a path}; shift 2 ;;
	-h|--help) sed -n '2,38p' "$0" | cut -c3-; exit 0 ;;
	*) echo "soak-verdict: unknown argument: $1" >&2; exit 2 ;;
	esac
done

[ -r "$CSV" ] || { echo "soak-verdict: csv not readable: $CSV" >&2; exit 1; }
if [ -n "$WINDOWS" ] && [ ! -r "$WINDOWS" ]; then
	echo "soak-verdict: windows file not readable: $WINDOWS" >&2
	exit 1
fi

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
	-v c_gate="${C_GATE:-0}" -v gap="$GAP_SECONDS" -v gates_ok="$GATES_READABLE" -v gates="$GATES" \
	-v windows="$WINDOWS" '
function push_delta(x) { if (nd < 5000) { nd++; d[nd] = x } }
# Theil-Sen over the pairs of the points in GKX/GKY (sel=0) or AKX/AKY (sel=1), in MiB
# per hour. The pairwise median, not least squares: RSS on this target swings on its
# own, and a least-squares line through it reports a slope for a saw-tooth. Report-only:
# it gates nothing, and mp_alloc stays what the gates read.
function theil(sel,   i, j, m, v, x1, y1, x2, y2, n) {
	n = (sel == 0) ? NK : KA
	if (n < 2) return "na"
	m = 0
	for (i = 1; i <= n; i++) for (j = i + 1; j <= n; j++) {
		if (sel == 0) { x1 = GKX[i]; y1 = GKY[i]; x2 = GKX[j]; y2 = GKY[j] }
		else          { x1 = AKX[i]; y1 = AKY[i]; x2 = AKX[j]; y2 = AKY[j] }
		if (x2 > x1) { m++; PS[m] = (y2 - y1) * 3600.0 / (x2 - x1) }
	}
	for (i = 2; i <= m; i++) { v = PS[i]; j = i - 1; while (j >= 1 && PS[j] > v) { PS[j + 1] = PS[j]; j-- } PS[j + 1] = v }
	return sprintf("%+.3f", (m % 2) ? PS[(m + 1) / 2] : (PS[m / 2] + PS[m / 2 + 1]) / 2)
}
BEGIN {
	nwin = 0
	if (windows != "") {
		while ((getline wl < windows) > 0) {
			if (wl ~ /^#/ || wl ~ /^[[:space:]]*$/) continue
			# split by hand: $1..$NF here would be the fields of the main file, which
			# has not been read yet at BEGIN. Own names too: `n` counts rows below, and
			# a window file that shifted it by its field count produced a report about
			# a series that does not exist.
			nf = split(wl, wf, /[ \t]+/)
			if (nf < 3) continue
			nwin++
			WFROM[nwin] = wf[1]; WTO[nwin] = wf[2]
			lab = wf[3]
			for (wi = 4; wi <= nf; wi++) lab = lab " " wf[wi]
			WLAB[nwin] = lab
		}
		close(windows)
	}
}
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
	# which declared window, if any, this probe sits in
	wi = 0
	if ($c_iso != "" && $c_iso != "0")
		for (i = 1; i <= nwin; i++)
			if ($c_iso >= WFROM[i] && $c_iso <= WTO[i]) { wi = i; break }
	if (wi > 0) {
		WCNT[wi]++
		if (WCNT[wi] == 1 || $c_iso < WFIRST[wi]) WFIRST[wi] = $c_iso
		if ($c_iso > WLAST[wi]) WLAST[wi] = $c_iso
		if (WCNT[wi] == 1 || r < WMIN[wi]) WMIN[wi] = r
		if (r > WMAX[wi]) WMAX[wi] = r
		if (WCNT[wi] == 1 || m < WMPMIN[wi]) WMPMIN[wi] = m
		if (m > WMPMAX[wi]) WMPMAX[wi] = m
	} else if (nwin > 0) {
		# the trend set: outside every window, after warm-up. A warm-up probe sits in
		# the post-restart replay and the allocator ramp, and a line through it measures
		# the restart, not the node.
		if ($c_gate != "warmup") { NK++; GKX[NK] = $c_epoch; GKY[NK] = r }
	}
	if (nwin > 0 && $c_gate != "warmup") { KA++; AKX[KA] = $c_epoch; AKY[KA] = r }
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
	if (nwin > 0) {
		printf "  окна нагрузки: %d (из %s)\n", nwin, windows
		for (i = 1; i <= nwin; i++) {
			if (WCNT[i] > 0)
				printf "    %s: проб %d, %s .. %s | rss %.1f..%.1f | mp_alloc %.1f..%.1f\n", \
					WLAB[i], WCNT[i], WFIRST[i], WLAST[i], WMIN[i], WMAX[i], WMPMIN[i], WMPMAX[i]
			else
				printf "    %s: проб 0 — окно не накрыло ни одной пробы\n", WLAB[i]
		}
		printf "  тренд rss_mib ВНЕ окон, после warmup (Theil-Sen, МиБ/ч, отчётно): %s по %d пробам\n", theil(0), NK
		printf "  тренд rss_mib по всем после warmup (для сравнения):                  %s по %d пробам\n", theil(1), KA
	}
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
