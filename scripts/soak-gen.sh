#!/bin/sh
# START_AI_HEADER
# MODULE: scripts/soak-gen.sh
# PURPOSE: Steady chat-load generator for a soak run: one persistent set of users and rooms kept in
#          a 0600 state file, never exits on an error, logs failures and backs off.
# DEPENDENCIES: sh, curl, awk
# PUBLIC_API: script (run by soak-run.sh)
# END_AI_HEADER

# soak-gen.sh — steady chat-load generator for a soak run (no flapping).
#
# WHY THIS EXISTS. The previous generator ran under `set -eu`, so any curl timeout
# killed it; the restarter then made a NEW user and a NEW room every time (46 restarts
# in 30 minutes), and the run measured account/room churn instead of steady traffic.
# This one:
#   * keeps ONE persistent set of users and rooms. Their tokens and room ids live in
#     a 0600 state file in the run directory and are reused after a restart; a restart
#     registers nobody and creates no room;
#   * never exits on an error: every failed call is counted, logged on its own line
#     with a code, and the loop pauses with an exponential back-off (capped);
#   * if a token stops working (HTTP 401), it logs in again with the stored password
#     of the SAME user — still no new account.
#
# LOOP. Per tick: one send (PUT m.room.message) by the next user into the next room,
# then an incremental /sync (since=<next_batch>, timeout=0) by that user. The tick is a
# success only if both answered 200.
#
# LOG FORMAT (read by scripts/soak-run.sh `barechat_total`, which counts lines that
# contain "barechat msg" AND "sent + synced"):
#   [HH:MM:SSZ] barechat msg N sent + synced          one per successful tick
#   [HH:MM:SSZ] ERR op=<send|sync|register|...> code=<http status | curl:<rc>> [errcode=M_...] consecutive=K pause=Ss
#   [HH:MM:SSZ] SETUP registered user=I | created room=J | joined user=I room=J | relogin user=I
#   [HH:MM:SSZ] SUMMARY sent=.. errors=.. registrations=.. rooms=..     (on exit)
# Error and setup lines never contain the words "sent + synced". Tokens, passwords and
# response bodies are never printed; the Authorization header goes through a 0600 file,
# not the command line.
#
# ENDPOINT. Only from --endpoint. The host must be 127.0.0.1, or be listed in
# --allow-host (comma separated); anything else is refused before any request.
#
# Usage:
#   scripts/soak-gen.sh --endpoint http://127.0.0.1:PORT --run-dir DIR
#       [--users 5] [--rooms 2] [--rate 6] [--allow-host H1,H2]
#       [--backoff-cap 60] [--duration SECONDS|0] [--log FILE]
#   --rate is messages per minute (default 6). --duration 0 (default) runs until killed.
# Files in DIR: gen-barechat.log (log), gen-state (state, 0600), token (user 0's
# token, 0600, for the recorder's --token-file).

umask 077
ENDPOINT=""
RUN_DIR=""
USERS=5
ROOMS=2
RATE=6
ALLOW=""
CAP=60
DURATION=0
LOG=""

while [ $# -gt 0 ]; do
	case "$1" in
	--endpoint) ENDPOINT=${2:?--endpoint needs a url}; shift 2 ;;
	--run-dir) RUN_DIR=${2:?--run-dir needs a path}; shift 2 ;;
	--users) USERS=${2:?--users needs a number}; shift 2 ;;
	--rooms) ROOMS=${2:?--rooms needs a number}; shift 2 ;;
	--rate) RATE=${2:?--rate needs msgs per minute}; shift 2 ;;
	--allow-host) ALLOW=${2:?--allow-host needs a list}; shift 2 ;;
	--backoff-cap) CAP=${2:?--backoff-cap needs seconds}; shift 2 ;;
	--duration) DURATION=${2:?--duration needs seconds}; shift 2 ;;
	--log) LOG=${2:?--log needs a path}; shift 2 ;;
	-h|--help) sed -n '2,41p' "$0" | cut -c3-; exit 0 ;;
	*) echo "soak-gen: unknown argument: $1" >&2; exit 2 ;;
	esac
done

[ -n "$ENDPOINT" ] || { echo "soak-gen: --endpoint is required" >&2; exit 2; }
[ -n "$RUN_DIR" ] || { echo "soak-gen: --run-dir is required" >&2; exit 2; }
for _v in USERS ROOMS RATE CAP DURATION; do
	eval "_x=\$$_v"
	case "$_x" in ''|*[!0-9]*) echo "soak-gen: --$_v must be a non-negative integer" >&2; exit 2 ;; esac
done
[ "$USERS" -ge 1 ] && [ "$ROOMS" -ge 1 ] && [ "$RATE" -ge 1 ] || {
	echo "soak-gen: users, rooms and rate must be >= 1" >&2; exit 2; }

# Host gate: scheme://host[:port][/path]. Only http(s); host 127.0.0.1 or explicitly allowed.
case "$ENDPOINT" in
http://*|https://*) : ;;
*) echo "soak-gen: endpoint must be http:// or https://" >&2; exit 2 ;;
esac
_hp=${ENDPOINT#*://}
_hp=${_hp%%/*}
case "$_hp" in
	'['*) HOST=${_hp%%]*}]; ;;
	*) HOST=${_hp%%:*} ;;
esac
ENDPOINT=${ENDPOINT%/}
_ok=0
[ "$HOST" = "127.0.0.1" ] && _ok=1
if [ "$_ok" = 0 ] && [ -n "$ALLOW" ]; then
	_old=$IFS; IFS=,
	for _h in $ALLOW; do [ "$_h" = "$HOST" ] && _ok=1; done
	IFS=$_old
fi
[ "$_ok" = 1 ] || {
	echo "soak-gen: host '$HOST' is not 127.0.0.1 and not in --allow-host — refusing." >&2
	exit 2
}

mkdir -p "$RUN_DIR" || { echo "soak-gen: cannot create $RUN_DIR" >&2; exit 2; }
[ -n "$LOG" ] || LOG="$RUN_DIR/gen-barechat.log"
STATE="$RUN_DIR/gen-state"
HDR="$RUN_DIR/.gen-hdr"
TOKEN_FILE="$RUN_DIR/token"
[ -e "$STATE" ] || : > "$STATE"
chmod 600 "$STATE"
: > "$HDR"; chmod 600 "$HDR"

# CONTRACT: Append a timestamped line to the run log; never fails.
log() { printf '[%s] %s\n' "$(date -u +%H:%M:%SZ)" "$*" >> "$LOG"; }

NSENT=0; NERR=0; NREG=0; NROOM=0
CONSEC=0
trap 'log "SUMMARY sent=$NSENT errors=$NERR registrations=$NREG rooms=$NROOM"; rm -f "$HDR"; exit 0' INT TERM

# ── HTTP: sets CODE (status, or curl:<rc>) and BODY. Never exits. ──────────────
# req:start
#   purpose: One HTTP call with curl; never exits the script on failure.
#   input:  method, path, optional token, optional JSON body
#   output: sets CODE (status or curl:<rc>) and BODY
#   sideEffects: network call to the node under test
# req:end
req() { # method path [token] [json]
	_m=$1; _p=$2; _t=${3:-}; _d=${4:-}
	if [ -n "$_t" ]; then
		printf 'Authorization: Bearer %s\n' "$_t" > "$HDR"
		set -- -H "@$HDR"
	else
		set --
	fi
	if [ -n "$_d" ]; then
		_raw=$(curl --noproxy '*' -s --max-time 10 -X "$_m" "$@" \
			-H 'Content-Type: application/json' -d "$_d" \
			-w '\n%{http_code}' "$ENDPOINT$_p" 2>/dev/null)
	else
		_raw=$(curl --noproxy '*' -s --max-time 10 -X "$_m" "$@" \
			-w '\n%{http_code}' "$ENDPOINT$_p" 2>/dev/null)
	fi
	_rc=$?
	if [ "$_rc" -ne 0 ]; then CODE="curl:$_rc"; BODY=""; return 1; fi
	CODE=${_raw##*
}
	BODY=${_raw%
*}
	[ "$CODE" = 200 ]
}

# CONTRACT: Value of the first "key":"value" pair of BODY.
jget() { # key -> value of first "key":"value" in BODY
	printf '%s' "$BODY" | sed -n "s/.*\"$1\"[[:space:]]*:[[:space:]]*\"\\([^\"]*\\)\".*/\\1/p" | head -n 1
}

# CONTRACT: Record an error for an op and sleep exponentially, capped.
backoff() { # op  (records an error and sleeps exponentially, capped)
	NERR=$((NERR + 1)); CONSEC=$((CONSEC + 1))
	_ec=$(jget errcode)
	_pause=1; _i=1
	while [ "$_i" -lt "$CONSEC" ] && [ "$_pause" -lt "$CAP" ]; do _pause=$((_pause * 2)); _i=$((_i + 1)); done
	[ "$_pause" -gt "$CAP" ] && _pause=$CAP
	[ "$_pause" -lt 1 ] && _pause=1
	log "ERR op=$1 code=$CODE${_ec:+ errcode=$_ec} consecutive=$CONSEC pause=${_pause}s"
	sleep "$_pause"
}

# ── State: lines "u IDX NAME PASS TOKEN" and "r IDX ROOMID" and "t TAG" ──────
# CONTRACT: Field f of the state-file user line with index i.
st_get_u() { awk -v i="$1" -v f="$2" '$1=="u" && $2==i {print $f}' "$STATE" | tail -n 1; }
# CONTRACT: Room id of the state-file room line with index i.
st_get_r() { awk -v i="$1" '$1=="r" && $2==i {print $3}' "$STATE" | tail -n 1; }
# CONTRACT: Rewrite the token of user idx in the state file atomically, keeping mode 0600.
st_set_tok() { # idx token: rewrite atomically, keep 0600
	awk -v i="$1" -v t="$2" '$1=="u" && $2==i {$5=t} {print}' "$STATE" > "$STATE.new" \
		&& chmod 600 "$STATE.new" && mv "$STATE.new" "$STATE"
}

TAG=$(awk '$1=="t" {print $2}' "$STATE" | tail -n 1)
if [ -z "$TAG" ]; then
	TAG=$(od -An -N4 -tx1 /dev/urandom | tr -d ' \n')
	echo "t $TAG" >> "$STATE"
fi

# CONTRACT: 12 random bytes as hex, used as a throwaway password.
mkpass() { od -An -N12 -tx1 /dev/urandom | tr -d ' \n'; }

# CONTRACT: Log the user idx in again with its stored password and store the new token; no new account.
login() { # idx -> sets token in state; uses stored password of the same user
	_n=$(st_get_u "$1" 3); _pw=$(st_get_u "$1" 4)
	req POST /_matrix/client/v3/login "" \
		"{\"type\":\"m.login.password\",\"identifier\":{\"type\":\"m.id.user\",\"user\":\"$_n\"},\"password\":\"$_pw\"}" \
		|| return 1
	_tk=$(jget access_token); [ -n "$_tk" ] || return 1
	st_set_tok "$1" "$_tk"
	log "SETUP relogin user=$1"
}

# CONTRACT: Register user idx; appended to the state file only once a token is in hand.
register_user() { # idx; appends the user to state only once a token is in hand
	_n="soakgen_${TAG}_$1"; _pw=$(mkpass)
	req POST /_matrix/client/v3/register "" "{\"username\":\"$_n\",\"password\":\"$_pw\"}"
	_sess=$(jget session)
	if [ -n "$_sess" ]; then
		req POST /_matrix/client/v3/register "" \
			"{\"username\":\"$_n\",\"password\":\"$_pw\",\"auth\":{\"type\":\"m.login.dummy\",\"session\":\"$_sess\"}}" || :
	fi
	_tk=$(jget access_token)
	if [ -z "$_tk" ]; then return 1; fi
	echo "u $1 $_n $_pw $_tk" >> "$STATE"
	NREG=$((NREG + 1))
	log "SETUP registered user=$1"
}

# ── Setup: idempotent; whatever is already in the state file is reused ────────
setup_done=0
while [ "$setup_done" = 0 ]; do
	_all=1
	i=0
	while [ "$i" -lt "$USERS" ]; do
		if [ -z "$(st_get_u "$i" 5)" ]; then
			_all=0
			if register_user "$i"; then CONSEC=0; else backoff register; fi
		fi
		i=$((i + 1))
	done
	[ "$_all" = 1 ] || continue
	j=0
	while [ "$j" -lt "$ROOMS" ]; do
		if [ -z "$(st_get_r "$j")" ]; then
			_all=0
			_o=$(( j % USERS ))
			if req POST /_matrix/client/v3/createRoom "$(st_get_u "$_o" 5)" \
				"{\"name\":\"soak-gen-$j\",\"preset\":\"public_chat\"}" && [ -n "$(jget room_id)" ]; then
				echo "r $j $(jget room_id)" >> "$STATE"
				NROOM=$((NROOM + 1)); CONSEC=0
				log "SETUP created room=$j"
			else
				[ "$CODE" = 401 ] && login "$_o"
				backoff createRoom
			fi
		fi
		j=$((j + 1))
	done
	[ "$_all" = 1 ] || continue
	# Every user joins every room (idempotent on the server); marker lines in state.
	i=0
	while [ "$i" -lt "$USERS" ]; do
		j=0
		while [ "$j" -lt "$ROOMS" ]; do
			if ! grep -q "^j $i $j\$" "$STATE"; then
				_rid=$(st_get_r "$j")
				if req POST "/_matrix/client/v3/join/$_rid" "$(st_get_u "$i" 5)" '{}'; then
					echo "j $i $j" >> "$STATE"; CONSEC=0
					log "SETUP joined user=$i room=$j"
				else
					[ "$CODE" = 401 ] && login "$i"
					backoff join; _all=0
				fi
			fi
			j=$((j + 1))
		done
		i=$((i + 1))
	done
	[ "$_all" = 1 ] && setup_done=1
done
st_get_u 0 5 > "$TOKEN_FILE"; chmod 600 "$TOKEN_FILE"

# ── Main loop ─────────────────────────────────────────────────────────────────
IV=$((60 / RATE)); [ "$IV" -ge 1 ] || IV=1
N=$(awk '/barechat msg/ && /sent \+ synced/ {n++} END{print n+0}' "$LOG" 2>/dev/null)
START=$(date +%s)
log "generator up: users=$USERS rooms=$ROOMS rate=${RATE}/min interval=${IV}s resume_at=$N"

# Per-user sync cursor, in memory (shell variables since_0, since_1, ...).
TICK=0
while :; do
	t0=$(date +%s)
	[ "$DURATION" -gt 0 ] && [ $((t0 - START)) -ge "$DURATION" ] && break
	u=$(( TICK % USERS )); r=$(( (TICK / USERS + TICK) % ROOMS ))
	TICK=$((TICK + 1))
	tok=$(st_get_u "$u" 5); rid=$(st_get_r "$r")
	ok=1
	if ! req PUT "/_matrix/client/v3/rooms/$rid/send/m.room.message/gen-$TAG-$t0-$TICK" "$tok" \
		"{\"msgtype\":\"m.text\",\"body\":\"bc msg $((N + 1))\"}"; then
		ok=0
		if [ "$CODE" = 401 ]; then login "$u" || :; fi
		backoff send
	else
		eval "_since=\${since_$u:-}"
		if [ -n "$_since" ]; then _q="?timeout=0&since=$_since"; else _q="?timeout=0"; fi
		if req GET "/_matrix/client/v3/sync$_q" "$tok"; then
			_nb=$(jget next_batch)
			[ -n "$_nb" ] && eval "since_$u=\$_nb"
		else
			ok=0
			[ "$CODE" = 401 ] && { login "$u" || :; }
			backoff sync
		fi
	fi
	if [ "$ok" = 1 ]; then
		CONSEC=0; N=$((N + 1)); NSENT=$((NSENT + 1))
		log "barechat msg $N sent + synced"
		el=$(( $(date +%s) - t0 ))
		[ "$el" -lt "$IV" ] && sleep $(( IV - el ))
	fi
done
log "SUMMARY sent=$NSENT errors=$NERR registrations=$NREG rooms=$NROOM"
rm -f "$HDR"
exit 0
