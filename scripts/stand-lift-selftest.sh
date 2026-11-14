#!/bin/sh
# stand-lift-selftest.sh — прогон гейтов stand-lift.sh на КОПИИ, во временном
# стенде. Живой стенд не трогается: свой STAND_HOME под mktemp, своя копия
# бинаря, свой порт на 127.0.0.1, свой pid-файл; в конце процесс снимается.
#
# Случаи:
#   a  env без ZENOH-переменных        -> поднят, порт слушает, rc=0
#   b  повторный запуск                -> no-op (гейт 1), rc=0
#   c  ZENOH_LISTEN=tcp/0.0.0.0:...    -> отказ гейта 5, rc=1
#   d  ZENOH_LISTEN=obfs/... и бинарь без кластерного слоя -> отказ гейта 4
#   e  HUBD_QUEUES_DIR в env           -> отказ моста, rc=1
#   f  ZENOH_CONNECT без LISTEN, peer  -> отказ гейта 5 (неявный wildcard), rc=1
#   g  ZENOH_LISTEN=obfs/0.0.0.0:...   -> отказ гейта 5 (wildcard, любой протокол)
#   h  ZENOH_LISTEN=udp/[::]:...       -> то же
# f/g/h — отказ ДО запуска бинаря: ни pid-файла, ни слушателя на порту.
# Случай d пропускается (SKIP), если бинарь кластерный.
#
# Использование: sh scripts/stand-lift-selftest.sh <matrix-hs> [порт]
# Без порта берётся первый свободный в 18460..18499. Выход 0 — всё как надо.
set -eu

HERE=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
SRC=${1:?usage: $0 <matrix-hs binary> [port]}
[ -r "$SRC" ] || { echo "selftest: бинарь не читается: $SRC" >&2; exit 2; }

PORT=${2:-}
if [ -z "$PORT" ]; then
	_p=18460
	while [ "$_p" -le 18499 ]; do
		sockstat -4 -l -p "$_p" 2>/dev/null | grep -qE ":$_p[[:space:]]" || { PORT=$_p; break; }
		_p=$((_p + 1))
	done
fi
[ -n "$PORT" ] || { echo "selftest: нет свободного порта" >&2; exit 2; }

T=$(mktemp -d "${TMPDIR:-/tmp}/stand-lift-selftest.XXXXXX")
H="$T/home"
mkdir -p "$H/bin" "$H/data"
cp "$SRC" "$H/bin/matrix-hs"
chmod 755 "$H/bin/matrix-hs"
PIDFILE="$H/mrgd.pid"

cleanup() {
	if [ -f "$PIDFILE" ]; then
		_pid=$(cat "$PIDFILE" 2>/dev/null || echo "")
		[ -n "$_pid" ] && kill "$_pid" 2>/dev/null || :
		sleep 1
	fi
	rm -rf "$T"
}
trap cleanup EXIT INT TERM

SECRET=$(head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n')
write_env() {
	cat > "$H/mrgd.env" <<-ENV
		MATRIX_HS_LISTEN=127.0.0.1:$PORT
		MATRIX_HS_SERVER_NAME=selftest.invalid
		MATRIX_HS_TOKEN_SECRET=$SECRET
		MATRIX_HS_DATA_DIR=$H/data
		MALLOC_CONF=
		$1
	ENV
}

fail=0
run() { # name, expected rc, expected substring
	set +e
	STAND_HOME="$H" STAND_WAIT=${STAND_WAIT:-60} sh "$HERE/stand-lift.sh" > "$T/out" 2>&1
	_rc=$?
	set -e
	_last=$(tail -n 1 "$T/out")
	echo "($1) rc=$_rc last: $_last"
	[ "$_rc" -eq "$2" ] || { echo "  FAIL: ожидался rc=$2" >&2; fail=1; }
	grep -q -- "$3" "$T/out" || { echo "  FAIL: нет '$3' в выводе" >&2; fail=1; }
}

# Отказ гейта обязан случиться до запуска бинаря: нет pid-файла, порт свободен.
refused_before_start() {
	[ ! -f "$PIDFILE" ] || { echo "  FAIL: pid-файл есть — бинарь стартовал" >&2; fail=1; }
	if sockstat -46 -l -p "$PORT" 2>/dev/null | grep -qE ":$PORT[[:space:]]"; then
		echo "  FAIL: порт $PORT слушается — бинарь стартовал" >&2; fail=1
	fi
	! grep -q 'ПОДНЯТ' "$T/out" || { echo "  FAIL: в выводе ПОДНЯТ" >&2; fail=1; }
}

write_env ""
run a 0 'ПОДНЯТ'
sockstat -4 -l -p "$PORT" | grep -q "127.0.0.1:$PORT" || { echo "  FAIL: порт не слушает" >&2; fail=1; }
run b 0 'УЖЕ ПОДНЯТ'

# c/d/e идут на уже поднятом? Нет: гейт 1 сработал бы раньше, поэтому снимаем.
_pid=$(cat "$PIDFILE"); kill "$_pid" 2>/dev/null || :; sleep 1; rm -f "$PIDFILE"

write_env "MATRIX_HS_ZENOH_LISTEN=tcp/0.0.0.0:7999"
run c 1 'plaintext tcp/ Zenoh'

if strings -a "$H/bin/matrix-hs" | grep -q 'cluster mode: connect='; then
	echo "(d) SKIP: бинарь кластерный, гейт 4 не применим"
else
	write_env "MATRIX_HS_ZENOH_LISTEN=obfs/127.0.0.1:7999"
	run d 1 'нет кластерного слоя'
fi

write_env "MATRIX_HS_HUBD_QUEUES_DIR=$H/q"
run e 1 'мост hubd'

write_env "MATRIX_HS_ZENOH_CONNECT=tcp/127.0.0.1:7998"
run f 1 'without explicit MATRIX_HS_ZENOH_LISTEN'
refused_before_start
write_env "MATRIX_HS_ZENOH_LISTEN=obfs/0.0.0.0:7999#obfs_psk_base64=AAECAw=="
run g 1 'listens on all interfaces'
refused_before_start
write_env "MATRIX_HS_ZENOH_LISTEN=udp/[::]:7999"
run h 1 'listens on all interfaces'
refused_before_start

[ -f "$PIDFILE" ] && { echo "  FAIL: pid-файл остался после отказов" >&2; fail=1; }
if [ "$fail" -eq 0 ]; then echo "OK: все случаи как надо (порт $PORT)"; else echo "FAIL" >&2; fi
exit "$fail"
