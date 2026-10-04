#!/bin/sh
# soak-run.sh — background checkpoint recorder for a capped soak run.
#
# WHAT IT IS. One loop, one line per checkpoint, for hours. It pokes the allocator probe
# (SIGUSR2), reads the caps actually in force, reads RSS, reads the served depth of the
# rooms through the API, and appends a CSV row. It also judges the three gates from the
# card and KILLS the server when one trips, because a gate nobody evaluates during the
# night is a gate that fires after the damage.
#
# WHAT IT DELIBERATELY DOES NOT CLAIM. The card asks for roomlog / timeline / seen /
# typing-outer / uia at every checkpoint. Three of those five CANNOT be read from outside
# the process — they are in-process counters with no endpoint. Rather than print a 0 and
# let it be quoted later as "measured", the row carries an explicit
# `not_observable=to_device_seen,typing_outer,uia_sessions` field. Roomlog and timeline are
# approximated by SERVED DEPTH (what /messages returns), which is a lower bound on the
# stored depth, not the stored depth itself: the caps bound what is served, so a flat
# served depth is evidence the cap holds, not a measurement of the Vec's length.
#
# THE CAPS ROW IS A GATE ON THE RUN, NOT A COMMENT. If the caps cannot be read, the run is
# testing a configuration nobody wrote down, so the recorder says so in the row and keeps
# going — but the operator must treat an unreadable caps row as a STOP, not as a detail.
#
# INTERACTIVE TRAFFIC. The service log has no per-request lines, so "was a client talking"
# is measured by whether the served depth moved during the interval: moved = interactive,
# unchanged = quiet. That is a proxy and is labelled as one.
#
# GATES (from the card; the run is killed and the reason written on the last line):
#   1. RSS above 2 GiB at any checkpoint.
#   2. RSS growth of +100 MiB/h or more at two consecutive checkpoints.
#   3. the baseline climbing linearly for 2 hours or more — least-squares slope over the
#      trailing 2-hour window at or above +2 MiB/h. The threshold is mine: "climbing
#      linearly" needs a number to be a gate, and +2 MiB/h is well above noise and well
#      below the ~15 MiB/h this run exists to disprove.
#
# READ-ONLY except for the probe signals and the kill a gate authorises.
#
# Usage (must run as the process owner — the caps row needs the process environment):
#   scripts/soak-run.sh --pid N [--hours 6] [--interval 300] [--memprobe CSV]
#                       [--token-file F] [--endpoint URL] [--log F]

set -eu

PID=""
HOURS=6
INTERVAL=300
MEMPROBE=""
TOKEN_FILE=""
ENDPOINT=""   # no default: a hardcoded address would be a deployment map in a public file
LOG=""
NO_KILL=0

while [ $# -gt 0 ]; do
	case "$1" in
	--pid) PID=${2:?--pid needs a pid}; shift 2 ;;
	--hours) HOURS=${2:?--hours needs a number}; shift 2 ;;
	--interval) INTERVAL=${2:?--interval needs seconds}; shift 2 ;;
	--memprobe) MEMPROBE=${2:?--memprobe needs a path}; shift 2 ;;
	--token-file) TOKEN_FILE=${2:?--token-file needs a path}; shift 2 ;;
	--endpoint) ENDPOINT=${2:?--endpoint needs a url}; shift 2 ;;
	--log) LOG=${2:?--log needs a path}; shift 2 ;;
	--no-kill) NO_KILL=1; shift ;;
	-h|--help) sed -n '2,34p' "$0" | cut -c3-; exit 0 ;;
	*) echo "soak-run: unknown argument: $1" >&2; exit 2 ;;
	esac
done

[ -n "$PID" ] || { echo "soak-run: --pid is required" >&2; exit 2; }
[ -n "$LOG" ] || LOG="$(dirname "$MEMPROBE" 2>/dev/null || echo .)/soak-run-$(date -u +%Y%m%dT%H%M%SZ).csv"
mkdir -p "$(dirname "$LOG")"

CSV="$LOG"
GATE_LOG="${LOG%.csv}.gates.log"
CSV_HDR='epoch,iso,caps_timeline,caps_roomlog,rss_mib,vsz_mib,rss_after_poke_mib,poke_delta_mib,d_rss_mib,rate_mib_h,mp_seq,mp_resident_mib,mp_alloc_mib,rooms,served_depth,d_depth,traffic,gate,not_observable'

[ -s "$CSV" ] || echo "$CSV_HDR" > "$CSV"

say() { echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) $*" | tee -a "$GATE_LOG" >/dev/null; }

say "soak-run start: pid=$PID hours=$HOURS interval=${INTERVAL}s csv=$CSV gates=$GATE_LOG"
say "gates: rss>2048MiB | rate>=+100MiB/h twice in a row | trailing-2h slope>=+2MiB/h"
say "warmup: first $WARMUP_TICKS checkpoints are labelled warmup and cannot trip a gate"
[ "$NO_KILL" = "1" ] && say "VALIDATION MODE (--no-kill): gates are evaluated and logged, the server is NOT killed"
say "not observable from outside the process: to_device_seen, typing_outer_map, uia_sessions"

caps_of() {
	# Capture, then judge. `ps eww` of another user's process yields nothing on FreeBSD,
	# and a pipeline ending in sed always exits 0 — so the emptiness has to be inspected,
	# not inferred from an exit code.
	ps eww -p "$PID" 2>/dev/null | tr ' ' '\n' \
		| grep -E '^MATRIX_HS_(TIMELINE|ROOMLOG)_MAX_EVENTS=' \
		| sed 's/.*=//' | tr '\n' ' ' || true
}

# One call, two columns, and $2 is RSS because the FIRST -o is pid. The previous
# version asked for `-o pid= -o pid= -o vsz=` and read $2 as vsz — which was the pid
# a second time, so every row carried a pid in the vsz column (29.1 "MiB" = pid).
proc_mib() { ps -o pid= -o rss= -o vsz= -p "$PID" 2>/dev/null | awk '{print $2, $3}'; }

served_depth() {
	[ -n "$TOKEN_FILE" ] && [ -r "$TOKEN_FILE" ] || { echo "-"; return 0; }
	TOK=$(cat "$TOKEN_FILE")
	total=0
	rooms=$(curl -s --max-time 15 -H "Authorization: Bearer $TOK" \
		"$ENDPOINT/_matrix/client/v3/joined_rooms" 2>/dev/null \
		| python3 -c 'import sys,json
try: print(len(json.load(sys.stdin).get("joined_rooms",[])))
except Exception: print(0)' 2>/dev/null || echo 0)
	for room in $(curl -s --max-time 15 -H "Authorization: Bearer $TOK" \
		"$ENDPOINT/_matrix/client/v3/joined_rooms" 2>/dev/null \
		| python3 -c 'import sys,json
try:
    for r in json.load(sys.stdin).get("joined_rooms",[]): print(r)
except Exception: pass' 2>/dev/null); do
		n=$(curl -s --max-time 15 -H "Authorization: Bearer $TOK" \
			"$ENDPOINT/_matrix/client/v3/rooms/$room/messages?limit=5000" 2>/dev/null \
			| grep -o '"event_id"' | wc -l | tr -d ' ')
		total=$(( total + n ))
	done
	echo "$total|${rooms:-0}"
}

last_field() { # last_field <column-index> — 1-based, header on line 1
	awk -F, -v c="$1" 'NR>1 && NF>=2 {v=$c} END{print v+0}' "$CSV" 2>/dev/null || echo 0
}

prev_rss=$(last_field 5)
prev_epoch=$(last_field 1)
prev_depth=$(awk -F, 'NR>1 && NF>=16 {v=$16} END{print v+0}' "$CSV" 2>/dev/null || echo 0)
rate_prev=0
fast_streak=0
deadline=$(( $(date +%s) + HOURS * 3600 ))

# Warm-up: gates are suppressed for the first two checkpoints. The restart replays the
# journal and the allocator returns memory over the first minutes, and each SIGUSR2 probe
# itself walks every arena — so the earliest intervals measure the settling, not the soak.
# A gate that fires on the settling would kill the run it was meant to judge.
WARMUP_TICKS=${WARMUP_TICKS:-4}
tick=0
first_row=1

while [ "$(date +%s)" -lt "$deadline" ]; do
	if ! ps -p "$PID" >/dev/null 2>&1; then
		say "STOP: pid $PID is gone. A flat line from a dead server is not a result."
		exit 4
	fi

	# Sampled BEFORE the probe, on purpose. The probe is not free: it walks every arena and
	# builds a row of strings, and on this allocator profile each poke leaves ~80 MiB more
	# resident (measured: 101.2 -> 182.1 -> 262.9 MiB over three consecutive pokes). Poke
	# first and that retention is charged to the interval as if it were growth — which is
	# exactly how my own dry run tripped GATE_RATE_100MIB_H_TWICE at +41700 MiB/h and killed
	# a healthy server two minutes after its restart. So: read RSS, poke, read RSS again,
	# and report the probe's own cost in its own column instead of folding it into growth.
	set -- $(proc_mib)
	rss_kb=${1:-0}
	vsz_kb=${2:-0}
	kill -USR2 "$PID" 2>/dev/null || true
	sleep 2
	set -- $(proc_mib)
	rss_after_kb=${1:-0}

	epoch=$(date +%s)
	iso=$(date -u +%Y-%m-%dT%H:%M:%SZ)
	caps=$(caps_of)
	caps_t=$(printf '%s' "$caps" | awk '{print $1}')
	caps_r=$(printf '%s' "$caps" | awk '{print $2}')
	[ -n "$caps_t" ] || caps_t="UNREADABLE"
	[ -n "$caps_r" ] || caps_r="UNREADABLE"

	# One ps call, two columns: field 1 is rss, field 2 is vsz. (The two helpers this
	# replaced were deleted in the same edit that introduced proc_mib, while their call
	# sites stayed — under `set -e` the script died on "command not found" before writing
	# a single checkpoint, which is exactly what a dry run is for.)
	rss=$(awk -v k="${rss_kb:-0}" 'BEGIN{printf "%.1f", k/1024}')
	vsz=$(awk -v k="${vsz_kb:-0}" 'BEGIN{printf "%.1f", k/1024}')
	# The first row has no previous row, so every derived number below it would be
	# measured against zero: the dry run printed a rate of 41914 MiB/h and called its own
	# first interval "interactive" because depth went 0 → 1000. On the FIRST checkpoint
	# they are zeroed and the interval is labelled baseline.
	if [ "$first_row" = "1" ]; then
		d_rss=0.0; rate=0.00; d_depth=0; traffic="baseline"
	else
		d_rss=$(awk -v a="$rss" -v b="$prev_rss" 'BEGIN{printf "%.1f", a-b}')
	fi
	rss_after=$(awk -v k="${rss_after_kb:-0}" 'BEGIN{printf "%.1f", k/1024}')
	poke_delta=$(awk -v a="$rss_after" -v b="$rss" 'BEGIN{printf "%.1f", a-b}')
	if [ "$first_row" != "1" ] && [ "$epoch" -gt "$prev_epoch" ] && [ "$epoch" -gt 0 ]; then
		rate=$(awk -v d="$d_rss" -v dt="$(( epoch - prev_epoch ))" 'BEGIN{printf "%.2f", d/(dt/3600.0)}')
	else
		rate=0.00
	fi

	mp_seq="-"; mp_res="-"; mp_alloc="-"
	if [ -n "$MEMPROBE" ] && [ -r "$MEMPROBE" ]; then
		read -r mp_seq mp_res mp_alloc <<EOF
$(awk -F, 'NF>=7 {s=$2; r=$5; a=$4} END{print s, r, a}' "$MEMPROBE")
EOF
		mp_res=$(awk -v b="${mp_res:-0}" 'BEGIN{printf "%.1f", b/1048576}')
		mp_alloc=$(awk -v b="${mp_alloc:-0}" 'BEGIN{printf "%.1f", b/1048576}')
	fi

	depth_pair=$(served_depth)
	depth=${depth_pair%%|*}
	rooms=${depth_pair##*|}
	[ -n "$depth" ] || depth="-"
	if [ "$first_row" != "1" ]; then
		d_depth=$(awk -v a="$depth" -v b="$prev_depth" 'BEGIN{printf "%d", (a ~ /^[0-9]+$/ && b ~ /^[0-9]+$/) ? a-b : 0}')
		if [ "$d_depth" -gt 0 ] 2>/dev/null; then traffic="interactive"; else traffic="quiet"; fi
	fi

	# ── gates ────────────────────────────────────────────────────────────────────────
	gate="ok"
	tick=$(( tick + 1 ))
	if [ "$tick" -le "$WARMUP_TICKS" ]; then
		gate="warmup"
	else
	awk -v r="$rss" 'BEGIN{exit !(r>2048)}' && { gate="GATE_RSS_OVER_2GIB"; }
	awk -v r="$rate" 'BEGIN{exit !(r>=100)}' && fast_streak=$(( fast_streak + 1 )) || fast_streak=0
	[ "$fast_streak" -ge 2 ] && gate="GATE_RATE_100MIB_H_TWICE"
	# The window must actually SPAN two hours, not merely contain four points. The first
	# version only required n>=4, so GATE_BASE_LINEAR_2H fired on a 28-second stretch of
	# dry-run data — a "2-hour" gate judging half a minute. A gate that can fire before its
	# own window exists is worse than no gate: it gets disarmed by the operator and then
	# misses the thing it was written for.
	slope=$(awk -F, -v now="$epoch" -v win=7200 '
		NR>1 && NF>=5 && $1 > now-win { n++; if (mn=="" || $1<mn) mn=$1; if ($1>mx) mx=$1
		                                 sx+=$1; sy+=$5; sxx+=$1*$1; sxy+=$1*$5 }
		END { if (n<4 || mx-mn < win) {print "na"; exit} d=n*sxx-sx*sx; if (d==0) {print "na"; exit}
		       printf "%.2f", (n*sxy-sx*sy)/d*3600.0 }' "$CSV" 2>/dev/null || echo na)
	if [ "$slope" != "na" ] && awk -v s="$slope" 'BEGIN{exit !(s>=2)}'; then
		gate="GATE_BASE_LINEAR_2H"
	fi
	fi

	echo "$epoch,$iso,$caps_t,$caps_r,$rss,$vsz,$rss_after,$poke_delta,$d_rss,$rate,$mp_seq,$mp_res,$mp_alloc,$rooms,$depth,$d_depth,$traffic,$gate,to_device_seen+typing_outer+uia_sessions" >> "$CSV"

	first_row=0
	prev_rss=$rss; prev_epoch=$epoch; prev_depth=$depth
	[ "$d_depth" -ge 0 ] 2>/dev/null && rate_prev=$rate

	case "$gate" in
	GATE_*)
		say "$gate at $iso — last rows:"
		tail -5 "$CSV" | sed 's/^/    /' | tee -a "$GATE_LOG"
		if [ "$NO_KILL" = "1" ]; then
			say "WOULD KILL on $gate, but --no-kill is set: pid $PID left running."
			continue
		fi
		kill -TERM "$PID" 2>/dev/null || true
		say "killed pid $PID on $gate. report immediately with the rows above."
		exit 3
		;;
	esac

	sleep "$INTERVAL"
done

say "soak-run finished: $HOURS h elapsed, pid $PID still alive, gates never tripped."
tail -3 "$CSV" | sed 's/^/    /' | tee -a "$GATE_LOG"
