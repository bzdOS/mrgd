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
#   s1 живой + READONLY=1 -> разрешён; s2/s3 живой без флага (или не '1') -> отказ;
#   s4/s5 READONLY=1, но курсоры в хабе / нет каталога -> отказ
#   e  HUBD_QUEUES_DIR в env, список живых путей пуст -> отказ моста (fail-safe), rc=1
#   j  HUBD-пути = копия хаба (tmp)    -> разрешено, поднят, rc=0
#   k  HUB_DIR = живой                 -> отказ, rc=1
#   l  HUB_DIR = симлинк на живой      -> отказ (realpath), rc=1
#   m  QUEUES_DIR внутри живого        -> отказ (m2: через симлинк), rc=1
#   n  список живых пуст, HUB_DIR задан -> отказ (fail-safe), rc=1
#   o  живой лежит внутри HUB_DIR      -> отказ, rc=1
#   p  QUEUES_DIR вне HUB_DIR          -> отказ, rc=1
#   q/r HUB_DIR / живой путь не существует -> отказ, rc=1
# «Живой» в этих случаях — tmp-каталог из STAND_LIVE_HUB_PATHS.
#   f  ZENOH_CONNECT без LISTEN, peer  -> отказ гейта 5 (неявный wildcard), rc=1
#   g  ZENOH_LISTEN=obfs/0.0.0.0:...   -> отказ гейта 5 (wildcard, любой протокол)
#   h  ZENOH_LISTEN=udp/[::]:...       -> то же
#   i  ZENOH_SCOUTING=on, кластерный бинарь -> отказ гейта 4b (на некластерном SKIP)
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
		MATRIX_HS_ZENOH_SCOUTING=off
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

# Гейт моста: «живой» хаб здесь — tmp-каталог, переданный через
# STAND_LIVE_HUB_PATHS; копия — другой tmp-каталог. Живых путей хоста нет.
LIVE="$T/live/hub"; COPY="$T/copy/hub"
mkdir -p "$LIVE/queues" "$COPY/queues" "$T/x/other"
ln -s "$LIVE" "$T/lnk"
write_env "STAND_LIVE_HUB_PATHS=$LIVE
MATRIX_HS_HUBD_HUB_DIR=$T/lnk"
run l 1 'равен живому'
refused_before_start
write_env "STAND_LIVE_HUB_PATHS=$LIVE
MATRIX_HS_HUBD_HUB_DIR=$LIVE"
run k 1 'равен живому'
refused_before_start
write_env "STAND_LIVE_HUB_PATHS=$LIVE
MATRIX_HS_HUBD_HUB_DIR=$COPY
MATRIX_HS_HUBD_QUEUES_DIR=$LIVE/queues"
run m 1 'равен живому'
refused_before_start
write_env "STAND_LIVE_HUB_PATHS=$LIVE
MATRIX_HS_HUBD_QUEUES_DIR=$T/lnk/queues"
run m2 1 'равен живому'
refused_before_start
write_env "STAND_LIVE_HUB_PATHS=$LIVE
MATRIX_HS_HUBD_HUB_DIR=$T/live"
run o 1 'лежит внутри'
refused_before_start
write_env "STAND_LIVE_HUB_PATHS=
MATRIX_HS_HUBD_HUB_DIR=$COPY"
run n 1 'fail-safe'
refused_before_start
write_env "STAND_LIVE_HUB_PATHS=$LIVE
MATRIX_HS_HUBD_HUB_DIR=$COPY
MATRIX_HS_HUBD_QUEUES_DIR=$T/x/other"
run p 1 'не лежит внутри HUB_DIR'
refused_before_start
write_env "STAND_LIVE_HUB_PATHS=$LIVE
MATRIX_HS_HUBD_HUB_DIR=$T/no-such"
run q 1 'не существует'
refused_before_start
write_env "STAND_LIVE_HUB_PATHS=$T/no-such-live
MATRIX_HS_HUBD_HUB_DIR=$COPY"
run r 1 'не существует'
refused_before_start
# копия хаба разрешена: гейт пропускает, узел поднимается (loopback, tmp)
write_env "STAND_LIVE_HUB_PATHS=$LIVE
MATRIX_HS_HUBD_HUB_DIR=$COPY
MATRIX_HS_HUBD_QUEUES_DIR=$COPY/queues"
run j 0 'ПОДНЯТ'
_pid=$(cat "$PIDFILE"); kill "$_pid" 2>/dev/null || :; sleep 1; rm -f "$PIDFILE"

# READONLY=1: живой путь разрешён (мост только читает, курсоры в data-каталоге).
write_env "STAND_LIVE_HUB_PATHS=$LIVE
MATRIX_HS_HUBD_HUB_DIR=$LIVE
MATRIX_HS_HUBD_QUEUES_DIR=$LIVE/queues
MATRIX_HS_HUBD_READONLY=1"
run s1 0 'ПОДНЯТ'
_pid=$(cat "$PIDFILE"); kill "$_pid" 2>/dev/null || :; sleep 1; rm -f "$PIDFILE"
# те же пути БЕЗ флага (и с '0'/'true' — флаг только ровно '1') — прежний отказ
write_env "STAND_LIVE_HUB_PATHS=$LIVE
MATRIX_HS_HUBD_HUB_DIR=$LIVE
MATRIX_HS_HUBD_QUEUES_DIR=$LIVE/queues"
run s2 1 'равен живому'
refused_before_start
write_env "STAND_LIVE_HUB_PATHS=$LIVE
MATRIX_HS_HUBD_HUB_DIR=$LIVE
MATRIX_HS_HUBD_QUEUES_DIR=$LIVE/queues
MATRIX_HS_HUBD_READONLY=true"
run s3 1 'равен живому'
refused_before_start
# READONLY=1, но курсоры пришлись бы внутрь хаба / в его родителя — отказ
write_env "STAND_LIVE_HUB_PATHS=$LIVE
MATRIX_HS_HUBD_HUB_DIR=$LIVE
MATRIX_HS_HUBD_QUEUES_DIR=$LIVE/queues
MATRIX_HS_HUBD_READONLY=1
MATRIX_HS_HUBD_STATE_DIR=$T/live"
run s4 1 'каталог курсоров'
refused_before_start
# READONLY=1 без каталога курсоров — отказ
write_env "STAND_LIVE_HUB_PATHS=$LIVE
MATRIX_HS_HUBD_HUB_DIR=$LIVE
MATRIX_HS_HUBD_QUEUES_DIR=$LIVE/queues
MATRIX_HS_HUBD_READONLY=1
MATRIX_HS_DATA_DIR=$T/no-such-data"
run s5 1 'требует существующий'
refused_before_start

write_env "MATRIX_HS_ZENOH_CONNECT=tcp/127.0.0.1:7998"
run f 1 'without explicit MATRIX_HS_ZENOH_LISTEN'
refused_before_start
write_env "MATRIX_HS_ZENOH_LISTEN=obfs/0.0.0.0:7999#obfs_psk_base64=AAECAw=="
run g 1 'listens on all interfaces'
refused_before_start
write_env "MATRIX_HS_ZENOH_LISTEN=udp/[::]:7999"
run h 1 'listens on all interfaces'
refused_before_start

# Базовый env несёт SCOUTING=off, чтобы самотест сам ничего не биндил вне
# loopback; последняя строка env побеждает при source, поэтому on идёт после.
if strings -a "$H/bin/matrix-hs" | grep -q 'cluster mode: connect='; then
	write_env "MATRIX_HS_ZENOH_SCOUTING=on"
	run i 1 'скаутинг слушает на всех интерфейсах'
	refused_before_start
else
	echo "(i) SKIP: бинарь не кластерный, гейт 4b не применим"
fi

[ -f "$PIDFILE" ] && { echo "  FAIL: pid-файл остался после отказов" >&2; fail=1; }
if [ "$fail" -eq 0 ]; then echo "OK: все случаи как надо (порт $PORT)"; else echo "FAIL" >&2; fi
exit "$fail"
