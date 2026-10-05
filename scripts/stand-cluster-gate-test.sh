#!/bin/sh
# stand-cluster-gate-test.sh — регресс-тест гейта 4 в stand-lift.sh, без сети.
#
# ЧТО ТЕСТИРУЕТ. Фича cluster в Cargo.toml ОПЦИОНАЛЬНА (`default = []`), и без
# неё блок #[cfg(feature = "cluster")] вырезается: бинарь поднимается, слушает
# свой адрес и не имеет ни зеновского слушателя, ни catch-up, ни строки
# "cluster mode:" в логе. Стенд на таком бинаре неотличим от живого по
# pid-файлу и по порту — и ровно это произошло 05.10 в 19:15Z: подъём прошёл,
# кластер не поднялся, гейт молчал.
#
# КАК. Два настоящих бинаря: собранный БЕЗ фичи (промах) и собранный С ней
# (эталон). Каждый кладётся в свой STAND_HOME, и stand-lift.sh запускается на
# нём. env в обоих домах просит zenoh (MATRIX_HS_ZENOH_LISTEN), как это делает
# стенд, — тогда гейт 4 обязан сработать на промахе и НЕ сработать на эталоне.
# Сетевых действий нет: ни один случай не доходит до exec.
#
#   промах  -> отказ с текстом «нет кластерного слоя»
#   эталон  -> гейт 4 молчит; дальше срабатывает гейт 3 (адрес 127.0.0.1:22
#              занят sshd) — отказ по другой, правильной причине.
#
# ГДЕ БЕРУТСЯ БИНАРИ. Пути — аргументы; по умолчанию — тот, что лежит в TARGET_DIR
# (свежая сборка) и STAND_HOME/bin/matrix-hs (эталон, тот, что стоял на стенде).
# Оба читаются, ничего не запускается и не пишется: дома — во временном каталоге.
#
# ПОСЛЕ ФИКСА И БЕЗ НЕГО. На скрипте без гейта 4 промах НЕ будет отклонён — он
# дойдёт до гейта 3 и скажет другое, поэтому тест на нём падает. Проверено:
# на stand-lift.sh из базы промах даёт отказ по адресу, а не по кластеру.
#
# Использование:
#   sh scripts/stand-cluster-gate-test.sh [--bad BIN] [--good BIN]
# Выход 0 — оба случая как надо, 1 — нет.

set -eu

HERE=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
BAD=${BAD:-}
GOOD=${GOOD:-}
while [ $# -gt 0 ]; do
	case "$1" in
	--bad) BAD=${2:?--bad needs a path}; shift 2 ;;
	--good) GOOD=${2:?--good needs a path}; shift 2 ;;
	-h|--help) sed -n '2,30p' "$0" | cut -c3-; exit 0 ;;
	*) echo "stand-cluster-gate-test: unknown argument: $1" >&2; exit 2 ;;
	esac
done

# Оба бинаря обязательны: значение по умолчанию здесь означало бы «эталон», а на
# стенде стоит ровно тот бинарь, который и оказался промахом. Угадывать нечего.
if [ -z "$BAD" ] || [ -z "$GOOD" ]; then
	echo "usage: $0 --bad <бинарь без фичи cluster> --good <бинарь с ней>" >&2
	exit 2
fi
[ -f "$BAD" ] || { echo "test: нет бинаря без фичи: $BAD" >&2; exit 2; }
[ -f "$GOOD" ] || { echo "test: нет эталона: $GOOD" >&2; exit 2; }

TMP=$(mktemp -d "${TMPDIR:-/tmp}/stand-cluster-gate.XXXXXX")
trap 'rm -rf "$TMP"' EXIT INT TERM
rc=0

home_for() {
	_h="$TMP/$1"
	mkdir -p "$_h/bin"
	cp "$2" "$_h/bin/matrix-hs"
	# env как у стенда, только с адресом, который заведомо занят: так гейт 4
	# проверяется без сети, а гейт 3 не даёт дойти до exec.
	cat > "$_h/mrgd.env" <<-ENV
		MATRIX_HS_LISTEN=127.0.0.1:22
		MATRIX_HS_SERVER_NAME=localhost
		MATRIX_HS_DATA_DIR=$_h/data
		MATRIX_HS_ZENOH_LISTEN=obfs/192.0.2.1:7447
		MATRIX_HS_ZENOH_SCOUTING=off
		MALLOC_CONF=
	ENV
	echo "$_h"
}

marker() { strings -a "$1" 2>/dev/null | grep -c 'cluster mode: connect=' || true; }

say_marker() {
	_m=$(marker "$1")
	if [ "$_m" -gt 0 ]; then
		echo "  $2: метка кластерной сборки есть ($_m) — ожидаю, что гейт 4 молчит"
	else
		echo "  $2: метки нет (0) — ожидаю отказ «нет кластерного слоя»"
	fi
}

BAD_HOME=$(home_for bad "$BAD")
GOOD_HOME=$(home_for good "$GOOD")

echo "== промах (бинарь без фичи) =="
say_marker "$BAD" "бинарь"
set +e
STAND_HOME="$BAD_HOME" sh "$HERE/stand-lift.sh" > "$TMP/bad.out" 2>&1
bad_rc=$?
set -e
grep -q 'нет кластерного слоя' "$TMP/bad.out" || {
	echo "  FAIL: гейт 4 не отказал (rc=$bad_rc); вывод:" >&2
	sed 's/^/    /' "$TMP/bad.out" >&2
	rc=1
}
grep -q 'пересобери с --features cluster' "$TMP/bad.out" || {
	echo "  FAIL: в отказе нет подсказки про --features cluster" >&2
	rc=1
}
echo "  отказ: $(head -1 "$TMP/bad.out")"

echo "== эталон (бинарь с фичей) =="
say_marker "$GOOD" "бинарь"
set +e
STAND_HOME="$GOOD_HOME" sh "$HERE/stand-lift.sh" > "$TMP/good.out" 2>&1
good_rc=$?
set -e
if grep -q 'нет кластерного слоя' "$TMP/good.out"; then
	echo "  FAIL: гейт 4 отказал на эталоне; вывод:" >&2
	sed 's/^/    /' "$TMP/good.out" >&2
	rc=1
else
	echo "  гейт 4 молчит, отказ по другой причине: $(head -1 "$TMP/good.out") (rc=$good_rc)"
fi

if [ "$rc" -eq 0 ]; then
	echo "OK: промах отклонён, эталон пропущен — обе сборки различаются гейтом."
else
	echo "FAIL: есть расхождение." >&2
fi
exit "$rc"