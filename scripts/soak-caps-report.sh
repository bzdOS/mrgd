#!/bin/sh
# soak-caps-report.sh — read-only checkpoint reporter for a capped soak run.
#
# WHAT THIS IS FOR. A six-hour run answers one question: did memory stop growing? RSS on
# its own cannot answer it — a flat line is also what a dead process looks like — and the
# retention caps only help if they were actually IN EFFECT when the process started. So
# this prints three things, in the order they can invalidate each other:
#
#   1. the caps PROPOSED by deploy/soak-caps.env.example (a fragment; nothing applies it),
#   2. the caps IN EFFECT in a live process, read from its environment (read-only),
#   3. a checkpoint table from the allocator memprobe CSV with the growth rate over the
#      window, plus a per-trigger breakdown.
#
# If (1) and (2) disagree, the run is not testing what the fragment says, and every number
# below it is about a different configuration than the one on disk.
#
# WHAT IT DELIBERATELY DOES NOT PRINT. The leak survey's other candidates — the to-device
# dedup set, the typing map's room keys, the abandoned-UIA set — are in-process counters
# with no external read path. This script cannot see them, and their absence here is NOT a
# measurement of zero. The one structure depth that IS visible from outside is the served
# depth per room through the messages endpoint; feed it with --token to get it, otherwise
# skip it. Guessing a number for an invisible counter is worse than printing nothing.
#
# TRAFFIC CLASSES. Checkpoints carry the trigger that produced them, so automated and
# interactive stretches are separable: pass --class-regex to tag one subset (e.g. rows
# poked while the console client was driving). Rows are counted per trigger, so an
# interactive window cannot hide inside an automated average.
#
# READ-ONLY. It starts nothing, signals nothing, writes nothing. Not the process, not its
# env, not its data.
#
# Usage:
#   scripts/soak-caps-report.sh [--caps-file F] [--pid N] [--memprobe CSV]
#                               [--token TOK] [--class-regex RE] [--class-label NAME]
#                               [--endpoint URL]

set -eu

CAPS_FILE=deploy/soak-caps.env.example.example
PID=""
MEMPROBE=""
TOKEN=""
ENDPOINT=""
CLASS_REGEX=""
CLASS_LABEL="interactive"

usage() {
	sed -n '2,30p' "$0" | cut -c3-
	exit 2
}

while [ $# -gt 0 ]; do
	case "$1" in
	--caps-file) CAPS_FILE=${2:?--caps-file needs a path}; shift 2 ;;
	--pid) PID=${2:?--pid needs a pid}; shift 2 ;;
	--memprobe) MEMPROBE=${2:?--memprobe needs a path}; shift 2 ;;
	--token) TOKEN=${2:?--token needs a token}; shift 2 ;;
	--endpoint) ENDPOINT=${2:?--endpoint needs a url}; shift 2 ;;
	--class-regex) CLASS_REGEX=${2:?--class-regex needs a pattern}; shift 2 ;;
	--class-label) CLASS_LABEL=${2:?--class-label needs a name}; shift 2 ;;
	-h|--help) usage ;;
	*) echo "soak-caps-report: unknown argument: $1" >&2; usage ;;
	esac
done

echo "== 1. caps PROPOSED by the fragment (nothing applies it) =="
if [ -r "$CAPS_FILE" ]; then
	grep -E '^[[:space:]]*MATRIX_HS_(ROOMLOG|TIMELINE)_MAX_EVENTS=' "$CAPS_FILE" \
		| sed 's/^/   /' || echo "   (no cap lines found in $CAPS_FILE)"
else
	echo "   fragment not readable: $CAPS_FILE"
fi

echo
echo "== 2. caps IN EFFECT in a live process (read-only) =="
if [ -z "$PID" ]; then
	echo "   no --pid given; skipped. Without this row, 'caps were set' is an assumption."
elif ! kill -0 "$PID" 2>/dev/null; then
	echo "   pid $PID is not running — a flat RSS line from a dead process looks like success."
	echo "   STOP: there is nothing to read."
	exit 1
else
	echo "   pid $PID alive"
	ps -o pid=,rss=,vsz=,etime= -p "$PID" 2>/dev/null \
		| awk '{printf "   rss=%.1f MiB  vsz=%.1f MiB  elapsed=%s\n", $2/1024, $3/1024, $4}'
	# The environment of the running process, caps only. Read-only; no secrets printed.
	ps eww -p "$PID" 2>/dev/null \
		| tr ' ' '\n' \
		| grep -E '^MATRIX_HS_(ROOMLOG|TIMELINE)_MAX_EVENTS=' \
		| sed 's/^/   /' || echo "   no cap variables in the process environment (0/absent = unlimited)"
fi

echo
echo "== 3. checkpoint table from the memprobe CSV =="
if [ -z "$MEMPROBE" ]; then
	echo "   no --memprobe given; skipped."
elif [ ! -r "$MEMPROBE" ]; then
	echo "   memprobe not readable: $MEMPROBE"
	echo "   NOTE: a missing CSV is not a flat line. Check the log before reading anything"
	echo "         into the growth numbers below."
else
	echo "   file: $MEMPROBE"
	echo "   seq  trigger        resident  allocated  retained  (MiB)"
	awk -F, 'NR>1 && NF>=7 {
		printf "   %-4s %-14s %8s %10s %9s\n", $2, $3, $5/1048576, $4/1048576, $7/1048576
	}' "$MEMPROBE" | head -40
	echo "   --- rows: $(awk -F, 'NR>1 && NF>=7' "$MEMPROBE" | wc -l | tr -d ' ')"

	first_epoch=$(awk -F, 'NR>1 && NF>=7 {print $1; exit}' "$MEMPROBE")
	last_epoch=$(awk -F, 'NR>1 && NF>=7 {e=$1} END{print e}' "$MEMPROBE")
	first_res=$(awk -F, 'NR>1 && NF>=7 {print $5; exit}' "$MEMPROBE")
	last_res=$(awk -F, 'NR>1 && NF>=7 {r=$5} END{print r}' "$MEMPROBE")
	if [ -n "${first_epoch:-}" ] && [ -n "${last_epoch:-}" ] && [ "$last_epoch" -gt "$first_epoch" ]; then
		awk -v a="$first_epoch" -v b="$last_epoch" -v ra="$first_res" -v rb="$last_res" \
			'BEGIN{hrs=(b-a)/3600.0; if (hrs>0) printf "   window=%.2f h  resident %+.1f MiB  rate %+.1f MiB/h\n", hrs, (rb-ra)/1048576, (rb-ra)/1048576/hrs}'
	else
		echo "   window not computable (fewer than two checkpoints, or zero elapsed)"
	fi

	echo "   --- checkpoints per trigger (an interactive window must not hide in an average)"
	awk -F, 'NR>1 && NF>=7 {c[$3]++} END{for (t in c) printf "   %-16s %s\n", t, c[t]}' "$MEMPROBE"

	if [ -n "$CLASS_REGEX" ]; then
		echo "   --- rows matching --class-regex ('$CLASS_REGEX') tagged as: $CLASS_LABEL"
		awk -F, -v re="$CLASS_REGEX" -v lab="$CLASS_LABEL" '
			NR>1 && NF>=7 && $3 ~ re {
				printf "   %-6s %-14s %-14s %8.1f %10.1f\n", $2, $3, lab, $5/1048576, $4/1048576
			}' "$MEMPROBE" | head -20
	fi
fi

echo
echo "== 4. served depth per room (optional, needs a token; the ONLY structure depth"
echo "      visible from outside the process) =="
if [ -z "$TOKEN" ] || [ -z "$ENDPOINT" ]; then
	echo "   skipped (--token and --endpoint required)."
else
	for room in $(curl -sk -H "Authorization: Bearer $TOKEN" \
		"$ENDPOINT/_matrix/client/v3/joined_rooms" 2>/dev/null \
		| sed 's/.*"joined_rooms":\[\([^]]*\)\].*/\1/' | tr ',' '\n' | tr -d '" '); do
		depth=$(curl -sk -H "Authorization: Bearer $TOKEN" \
			"$ENDPOINT/_matrix/client/v3/rooms/$room/messages?limit=1000" 2>/dev/null \
			| grep -o '"event_id"' | wc -l | tr -d ' ')
		echo "   $room  served_depth=$depth"
	done
fi

echo
echo "== not measured here, on purpose =="
echo "   to-device dedup set, typing map room keys, abandoned-UIA set: in-process"
echo "   counters with no external read path. Their absence above is not a zero."
echo "   The caps fragment deletes history beyond its window — read deploy/soak-caps.env.example"
echo "   before applying it anywhere whose past matters."
