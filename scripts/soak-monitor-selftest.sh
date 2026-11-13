#!/bin/sh
# soak-monitor-selftest.sh — dry run of the soak recorder on a FIXTURE, no stand touched.
#
# WHAT IT PROVES. The recorder's claims are only worth what its last empty log says, so this
# script builds a fake server (a shell that ignores the probe signal), a fake memprobe CSV
# that grows, and a fake console-client log that both succeeds and fails, then runs
# soak-run.sh against them in --no-kill mode for a few checkpoints. It then checks the
# produced CSV itself and the verdict's reading of it:
#
#   1. the header carries barechat_total — the client's traffic has a column of its own;
#   2. barechat_total equals the number of SUCCESS lines in the fixture log: a failed send
#      line in the same file must not be counted;
#   3. mp_alloc_mib and the three structure gauges are numbers, not "-": the gauges the card
#      asks for are really read from the probe line;
#   4. rate_mp_alloc_mib_h is computed and non-zero: MiB/h is derived, not printed as 0;
#   5. soak-verdict prints the barechat line with the same count;
#   6. the default sample step is 600 s and the default warm-up is 3 checkpoints;
#   7. host_free_mib is a number > 0. That column exists to tell apart the two readings of a
#      flat rss: "rss flat because nothing accumulates (host free steady)" and "rss flat while
#      the host is being evicted (host free falling)". The check only proves the number is
#      read; the eviction reading is the verdict's range, never a proof.
#
# Each check prints PASS or FAIL with the value it read, and the script exits non-zero if any
# check failed. It starts and stops only processes it started itself, by exact pid.
#
# READ-ONLY towards the stand: no endpoint, no token, no network. The fixture pid is a local
# `sh` loop and is the only thing signalled.
#
# Usage: scripts/soak-monitor-selftest.sh [--keep-dir DIR]
#
# The fixture and the CSV stay under the output directory (printed at the end) so the run can
# be inspected after the fact; nothing is deleted.

set -eu

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
OUT_BASE="${SCRIPT_DIR}/../soak-pending"
while [ $# -gt 0 ]; do
	case "$1" in
	--keep-dir) OUT_BASE=${2:?--keep-dir needs a path}; shift 2 ;;
	-h|--help) sed -n '2,30p' "$0" | cut -c3-; exit 0 ;;
	*) echo "soak-monitor-selftest: unknown argument: $1" >&2; exit 2 ;;
	esac
done

RUN_DIR="${OUT_BASE}/monitor-selftest-$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$RUN_DIR" || { echo "cannot create $RUN_DIR" >&2; exit 1; }

MP="$RUN_DIR/memprobe.csv"
BC="$RUN_DIR/gen-barechat.log"
CSV="$RUN_DIR/soak.csv"

: > "$MP"
: > "$BC"

fails=0
check() { # check <label> <got> <want>
	if [ "$2" = "$3" ]; then
		printf 'PASS  %-46s %s\n' "$1" "$2"
	else
		printf 'FAIL  %-46s got=%s want=%s\n' "$1" "$2" "$3"
		fails=$(( fails + 1 ))
	fi
}

# ── fixture server: alive, ignores the probe signal, costs no CPU of note ────────────────
sh -c 'trap "" USR2; while :; do sleep 1; done' &
FIXPID=$!
fixture_stop() {
	if ps -p "$FIXPID" >/dev/null 2>&1; then
		echo "stopping fixture pid $FIXPID: $(ps -o pid= -o command= -p "$FIXPID")"
		kill -TERM "$FIXPID" 2>/dev/null || true
	fi
}
trap fixture_stop EXIT INT TERM

# ── writers: one probe line and one client line per second ───────────────────────────────
# The probe line's layout is the one soak-run.sh reads: $2 seq, $4 alloc bytes, $5 resident
# bytes, $18/$19/$20 the three structure gauges. Fields 1..20 are written explicitly so a
# shifted column shows up as a FAIL, not as a plausible number.
(
	n=0
	while :; do
		n=$(( n + 1 ))
		# alloc grows 1 MiB/s → +3600 MiB/h, a rate the recorder must derive, not invent.
		# Fields 1..20: 1 ts, 2 seq, 3 pad, 4 alloc, 5 resident, 6..17 pad (twelve),
		# 18/19/20 the three structure gauges.
		awk -v n="$n" 'BEGIN{printf "%d,%d,x,%d,%d,0,0,0,0,0,0,0,0,0,0,0,0,%d,%d,%d\n", \
			n, n, n*1048576, n*524288, n+10, n+20, n+30}' >> "$MP"
		sleep 1
	done
) &
MPPID=$!

(
	n=0
	while :; do
		n=$(( n + 1 ))
		printf '[%s] barechat msg %d sent + synced\n' "$(date -u +%H:%M:%SZ)" "$n" >> "$BC"
		# the negative control: same prefix, a send that did NOT land
		printf '[%s] barechat msg %d failed: timeout\n' "$(date -u +%H:%M:%SZ)" "$n" >> "$BC"
		sleep 1
	done
) &
BCPID=$!

writers_stop() {
	for p in "$MPPID" "$BCPID"; do
		if ps -p "$p" >/dev/null 2>&1; then
			echo "stopping fixture writer pid $p: $(ps -o pid= -o command= -p "$p" | cut -c1-60)"
			kill -TERM "$p" 2>/dev/null || true
		fi
	done
}
trap 'fixture_stop; writers_stop' EXIT INT TERM

sleep 2

# ── the recorder, on the fixture ─────────────────────────────────────────────────────────
# 0.004 h = 14.4 s, 2 s step: three or four checkpoints, gates armed but --no-kill.
sh "$SCRIPT_DIR/soak-run.sh" \
	--pid "$FIXPID" --memprobe "$MP" --barechat-log "$BC" \
	--hours 0.004 --interval 2 --no-kill --log "$CSV" \
	> "$RUN_DIR/soak-run.stdout" 2>&1 || {
		echo "FAIL  recorder exited non-zero; see $RUN_DIR/soak-run.stdout" >&2
		exit 1
	}

writers_stop

rows=$(( $(wc -l < "$CSV") - 1 ))
[ "$rows" -ge 2 ] && check "rows recorded (>=2)" "ok" "ok" || check "rows recorded (>=2)" "$rows rows" "ok"

check "header has barechat_total" \
	"$(head -1 "$CSV" | tr ',' '\n' | grep -c '^barechat_total$')" "1"

# The last row, read by NAME: the assertion must not depend on the column order it is testing.
col() { head -1 "$CSV" | tr ',' '\n' | grep -n "^$1\$" | cut -d: -f1; }
last() { awk -F, -v c="$1" 'NR>1 {v=$c} END{print v}' "$CSV"; }

C_BC=$(col barechat_total)
C_MP=$(col mp_alloc_mib)
C_TD=$(col td_seen)
C_TY=$(col typing_rooms)
C_UI=$(col uia_sessions)
C_RATE=$(col rate_mp_alloc_mib_h)
C_HF=$(col host_free_mib)
C_ISO=$(col iso)

# The writers keep appending until the recorder exits, so the honest expectation is the
# number of SUCCESS lines that already existed when the last row was taken: same seconds
# field, HH:MM:SS, compared as a string. A failed line carries the same prefix and must not
# be in the count — that is the whole point of the check.
last_iso=$(last "$C_ISO")
success_lines=$(awk -v cutoff="$last_iso" \
	'/barechat msg/ && /sent \+ synced/ && substr($0,2,8) <= substr(cutoff,12,8) {n++} END{print n+0}' "$BC")
any_failed=$(awk '/barechat msg/ && /failed/ {n++} END{print n+0}' "$BC")
check "fixture contains failed lines (negative control)" \
	"$(awk -v n="$any_failed" 'BEGIN{print (n>0)?"ok":"bad"}')" "ok"
check "barechat_total counts SUCCESS lines only" "$(last "$C_BC")" "$success_lines"

check "mp_alloc_mib is numeric (>0)" \
	"$(last "$C_MP" | awk '{print ($1 ~ /^[0-9.]+$/ && $1 > 0) ? "ok" : "bad:" $1}')" "ok"
check "td_seen is a number" \
	"$(last "$C_TD" | awk '{print ($1 ~ /^[0-9]+$/ && $1 > 0) ? "ok" : "bad:" $1}')" "ok"
check "typing_rooms is a number" \
	"$(last "$C_TY" | awk '{print ($1 ~ /^[0-9]+$/ && $1 > 0) ? "ok" : "bad:" $1}')" "ok"
check "uia_sessions is a number" \
	"$(last "$C_UI" | awk '{print ($1 ~ /^[0-9]+$/ && $1 > 0) ? "ok" : "bad:" $1}')" "ok"
check "rate_mp_alloc_mib_h derived (non-zero)" \
	"$(last "$C_RATE" | awk '{print ($1 == "0.00" || $1 == "0") ? "bad:" $1 : "ok"}')" "ok"
check "host_free_mib numeric (>0)" \
	"$(last "$C_HF" | awk '{print ($1 ~ /^[0-9.]+$/ && $1 > 0) ? "ok" : "bad:" $1}')" "ok"

# ── the verdict reads the column, on its own line ────────────────────────────────────────
sh "$SCRIPT_DIR/soak-verdict.sh" --csv "$CSV" --gates "${CSV%.csv}.gates.log" \
	> "$RUN_DIR/verdict.txt" 2>&1 || true
check "verdict prints a barechat line" \
	"$(grep -c 'barechat (отдельно' "$RUN_DIR/verdict.txt")" "1"
check "verdict barechat count matches the CSV" \
	"$(grep 'barechat (отдельно' "$RUN_DIR/verdict.txt" | sed -n 's/.*к концу=\([0-9]*\).*/\1/p')" \
	"$(last "$C_BC")"

# ── defaults: the card's 10-minute sample and the 30-minute warm-up ──────────────────────
sh "$SCRIPT_DIR/soak-run.sh" --pid "$$" --memprobe "$MP" --hours 0 --log "$RUN_DIR/defaults.csv" \
	> "$RUN_DIR/defaults.stdout" 2>&1 || true
GATE_LOG="${RUN_DIR}/defaults.gates.log"
check "default sample step is 600 s" \
	"$(grep -c 'interval=600s' "$GATE_LOG" 2>/dev/null || echo 0)" "1"
check "default warm-up is 3 checkpoints (30 min)" \
	"$(grep -c 'warmup: first 3 checkpoints' "$GATE_LOG" 2>/dev/null || echo 0)" "1"

echo
echo "artifacts: $RUN_DIR"
if [ "$fails" -ne 0 ]; then
	echo "RESULT: FAIL ($fails checks)"
	exit 1
fi
echo "RESULT: PASS (all checks)"
