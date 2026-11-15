#!/bin/sh
# ПОДЪЁМ СТЕНДА matrix-hs — воспроизводимая запись и идемпотентный запуск.
#
# СКРИПТ ЗАМЕНЯЕТ ИНЛАЙН-КОМАНДУ, которой стенд поднимали вручную: файла
# запуска не осталось (состав старта жил только в памяти оператора), а
# перезапуски гостя повторяются, поэтому подъём должен быть одной командой и
# не должен ломать уже поднятый стенд.
#
# Ничего host-local здесь не зашито: путь к дому стенда приходит в STAND_HOME,
# иначе check_no_private справедливо отказывает (путь под /home в публикуемом
# файле — это карта развёртывания). Порт тоже НЕ зашит — он выводится из
# MATRIX_HS_LISTEN, иначе проверка «слушает ли то, что мы подняли» врала бы на
# любом стенде с другим портом.
#
# ЗАПУСКАТЬ ПОД ПОЛЬЗОВАТЕЛЕМ стенда, не под root:
#   STAND_HOME=<дом стенда> sh stand-lift.sh
#
# Что скрипт делает, по порядку:
#   1. уже поднят (пид-файл живой)  -> no-op, выход 0, НИЧЕГО не меняя;
#   2. пид-файл протух               -> снести его и продолжить;
#   3. порт занят чужим              -> ОТКАЗ, второй раз не стартуем;
#   4. не тот адрес / мост включён   -> ОТКАЗ до exec;
#   5. иначе                         -> daemon(8), затем проверка, что порт
#                                      реально слушает.
#
# ОКНО ИЗМЕРЕНИЙ — НЕ ЭТОТ СКРИПТ. Старт окна = свой лаунчер рядом со
# soak-каталогом, со своим бинарием и своим MALLOC_CONF, и только по GO
# головы. Здесь — только стенд, чтобы его можно было проверить и снять.
set -eu

: "${STAND_HOME:?STAND_HOME не задан — укажи дом стенда (каталог с mrgd.env)}"
case "$STAND_HOME" in
	/*) : ;;
	*)  echo "stand-lift: STAND_HOME='$STAND_HOME' — нужен абсолютный путь." >&2; exit 2 ;;
esac

BIN=${STAND_BIN:-$STAND_HOME/bin/matrix-hs}
ENV_FILE=${STAND_ENV:-$STAND_HOME/mrgd.env}
LOG=${STAND_LOG:-$STAND_HOME/soak/stand-service.log}
PIDFILE=${STAND_PIDFILE:-$STAND_HOME/mrgd.pid}
MEMPROBE=${STAND_MEMPROBE:-$STAND_HOME/soak/memprobe.csv}

# ── Гейт 1: стенд уже поднят — выходим, ничего не трогая ────────────────────
# Идемпотентность: повторный прогон на живом стенде обязан быть no-op.
# Именно этот гейт закрывает требование «прогон на уже поднятом стенде не
# ломает работающий», и он же не даёт поднять второй matrix-hs на том же
# data_dir — два процесса на одном журнале спорят за него.
if [ -f "$PIDFILE" ]; then
	_pid=$(cat "$PIDFILE" 2>/dev/null || echo "")
	if [ -n "$_pid" ] && kill -0 "$_pid" 2>/dev/null; then
		echo "stand-lift: УЖЕ ПОДНЯТ (pid=$_pid) — no-op, ничего не меняю."
		exit 0
	fi
	echo "stand-lift: pid-файл протух (pid=${_pid:-пусто}), процесс мёртв — чищу."
	rm -f "$PIDFILE"
fi

# ── Пре-долёт: падать ДО записи в pid-файл и лог ────────────────────────────
[ -x "$BIN" ] || {
	echo "stand-lift: бинарь не найден или не исполняем: $BIN" >&2
	exit 1
}
[ -r "$ENV_FILE" ] || {
	echo "stand-lift: env не читается: $ENV_FILE" >&2
	exit 1
}
mkdir -p "$(dirname "$LOG")" "$(dirname "$MEMPROBE")" 2>/dev/null || :

# Карандаш: наружу ничего не слушаем. Проверяем ДО exec, потому что после exec
# уже поздно — процесс сам себя не остановит.
set -a
. "$ENV_FILE"
set +a

[ -n "${MATRIX_HS_LISTEN:-}" ] || {
	echo "stand-lift: в env нет MATRIX_HS_LISTEN — не знаю, что поднимать." >&2
	exit 1
}
LISTEN_HOST=${MATRIX_HS_LISTEN%:*}
LISTEN_PORT=${MATRIX_HS_LISTEN##*:}

case "$LISTEN_HOST" in
	0.0.0.0|'[::]'|'[::]') echo "stand-lift: MATRIX_HS_LISTEN='$MATRIX_HS_LISTEN' — наружу слушать нельзя." >&2; exit 1 ;;
esac

# ── Гейт 5: Zenoh must not be plaintext tcp/ on a public interface ─────────
# Fleet rule (.env.example): "never plaintext Zenoh on a public interface."
# A tcp/ endpoint on 0.0.0.0, a public IP, or a hostname is a bug — refuse
# the lift so the process never starts that way. Only tcp/ on loopback
# (127.0.0.1, ::1, localhost) or an obfs/tls carrier is accepted.
# ZENOH_* are OPTIONAL: under `set -u` a bare "$VAR" aborts the lift with
# "parameter not set" when the env file omits them, hence ${VAR:-} throughout.
for _zen in "${MATRIX_HS_ZENOH_LISTEN:-}" "${MATRIX_HS_ZENOH_CONNECT:-}"; do
	[ -z "$_zen" ] && continue
	for _ep in $(echo "$_zen" | tr ',' ' '); do
		case "$_ep" in
			tcp/127.0.0.1:*|tcp/localhost:*|tcp/\[::1]:*|tcp/::1:*) ;;
			tcp/*)
				_h=${_ep#tcp/}
				_h=${_h%%:*}
				echo "stand-lift: plaintext tcp/ Zenoh on address '$_h' — forbidden; use obfs/ or TLS." >&2
				exit 1
				;;
		esac
	done
done
unset _zen _ep _h

# Wildcard listen, ANY protocol (tcp/udp/tls/obfs...): 0.0.0.0 and [::] open the
# node on every interface, obfs or not — a public host must name its interface.
for _ep in $(echo "${MATRIX_HS_ZENOH_LISTEN:-}" | tr ',' ' '); do
	_a=${_ep#*/}
	_a=${_a%%#*}
	case "$_a" in
		0.0.0.0|0.0.0.0:*|'[::]'|'[::]:'*|'[0:0:0:0:0:0:0:0]'|'[0:0:0:0:0:0:0:0]:'*|::|::0)
			echo "stand-lift: MATRIX_HS_ZENOH_LISTEN endpoint '$_ep' listens on all interfaces — forbidden; name a loopback/private address." >&2
			exit 1
			;;
	esac
done
unset _ep _a

# Peer mode with CONNECT but no explicit LISTEN: zenoh peers open a
# listener on all interfaces by default (tcp/[::]:0, zenoh-config defaults.rs). Refuse before
# exec; set a loopback/private LISTEN, or MATRIX_HS_ZENOH_MODE=client.
_zmode=$(printf '%s' "${MATRIX_HS_ZENOH_MODE:-}" | tr -d '[:space:]' | tr '[:upper:]' '[:lower:]')
if [ -n "${MATRIX_HS_ZENOH_CONNECT:-}" ] && [ -z "${MATRIX_HS_ZENOH_LISTEN:-}" ] \
	&& [ "$_zmode" != client ]; then
	echo "stand-lift: peer mode without explicit MATRIX_HS_ZENOH_LISTEN would listen on all interfaces; set a loopback/private listen or MATRIX_HS_ZENOH_MODE=client." >&2
	exit 1
fi
unset _zmode

# Живой хаб не трогаем, но мост можно включить на КОПИИ хаба. Запрещены только
# ЖИВЫЕ пути: список в STAND_LIVE_HUB_PATHS (через ':'; из env-файла узла или из
# окружения; конкретные пути в публикуемый скрипт не зашиты). Всё сравнивается
# по realpath, так что симлинк на живой хаб тоже отказ. Список пуст — fail-safe:
# любой заданный HUBD-путь отказ. Стоит ДО exec.
#   - HUB_DIR / QUEUES_DIR обязаны существовать (иначе realpath нечего считать);
#   - путь равен живому, лежит внутри живого или живой лежит внутри него — отказ;
#   - QUEUES_DIR при заданном HUB_DIR обязан лежать внутри HUB_DIR;
#   - у моста state-каталог = <родитель QUEUES_DIR>/.mxstate (hubd_bridge.rs),
#     поэтому родитель QUEUES_DIR проверяется как запись тем же правилом.
hubd_rp() { _r=$(realpath -- "$1" 2>/dev/null) && [ -n "$_r" ]; }
hubd_within() { # $1 равен $2 или лежит внутри $2
	[ "$2" = / ] && return 0
	[ "$1" = "$2" ] && return 0
	case "$1" in "$2"/*) return 0 ;; esac
	return 1
}
hubd_vs_live() { # метка, realpath проверяемого пути
	while IFS= read -r _l; do
		[ -n "$_l" ] || continue
		if hubd_within "$2" "$_l"; then
			echo "stand-lift: мост hubd: $1 ($2) равен живому хабу или лежит внутри него ($_l) — отказ." >&2
			return 1
		fi
		if hubd_within "$_l" "$2"; then
			echo "stand-lift: мост hubd: живой хаб ($_l) лежит внутри $1 ($2) — отказ." >&2
			return 1
		fi
	done <<-EOF
	$_lr
	EOF
}
hubd_gate() {
	_q=${MATRIX_HS_HUBD_QUEUES_DIR:-}
	_h=${MATRIX_HS_HUBD_HUB_DIR:-}
	[ -n "$_q$_h" ] || return 0
	_live=${STAND_LIVE_HUB_PATHS:-}
	if [ -z "$(printf '%s' "$_live" | tr -d ':[:space:]')" ]; then
		echo "stand-lift: мост hubd: STAND_LIVE_HUB_PATHS пуст — копию от живого хаба не отличить, отказ (fail-safe)." >&2
		return 1
	fi
	_lr=
	while IFS= read -r _p; do
		[ -n "$_p" ] || continue
		if ! hubd_rp "$_p"; then
			echo "stand-lift: мост hubd: живой путь '$_p' из STAND_LIVE_HUB_PATHS не существует — не проверить, отказ." >&2
			return 1
		fi
		_lr="$_lr$_r
"
	done <<-EOF
	$(printf '%s' "$_live" | tr ':' '\n')
	EOF
	_hr=
	_qr=
	if [ -n "$_h" ]; then
		hubd_rp "$_h" || { echo "stand-lift: мост hubd: MATRIX_HS_HUBD_HUB_DIR '$_h' не существует — отказ." >&2; return 1; }
		_hr=$_r
		hubd_vs_live HUB_DIR "$_hr" || return 1
	fi
	if [ -n "$_q" ]; then
		hubd_rp "$_q" || { echo "stand-lift: мост hubd: MATRIX_HS_HUBD_QUEUES_DIR '$_q' не существует — отказ." >&2; return 1; }
		_qr=$_r
		hubd_vs_live QUEUES_DIR "$_qr" || return 1
		hubd_vs_live "родитель QUEUES_DIR (там .mxstate)" "$(dirname "$_qr")" || return 1
		if [ -n "$_hr" ] && ! hubd_within "$_qr" "$_hr"; then
			echo "stand-lift: мост hubd: QUEUES_DIR ($_qr) не лежит внутри HUB_DIR ($_hr) — отказ." >&2
			return 1
		fi
	fi
	return 0
}
hubd_gate || exit 1

# Занят ли ИМЕННО наш адрес. Адрес-слепой grep по ':<порт>' считал конфликтом
# любого соседа на том же порту, а это разные сокеты: процесс на loopback не
# мешает привязке конкретного адреса, и наоборот. Такой сосед на стенде — это
# rc-сервибс самого хоста, и гейт отказывал на живом, свободном адресе.
# Конфликтом остаётся: наш адрес занят кем угодно; тот же порт слушают wildcard'ом
# ('*' или 0.0.0.0) — такой сокет пересекается с нашим и блокирует bind.
# grep по классу пробелов, а НЕ `grep -q .`: sockstat печатает строку заголовка
# таблицы, и `grep -q .` считает занятым свободный порт. На этом споткнулся
# прогон на юник-сокетах.
ADDR_TAKEN="[[:space:]](${LISTEN_HOST}|\\*|0\\.0\\.0\\.0):${LISTEN_PORT}[[:space:]]"
addr_taken() {
	sockstat -4 -l -p "$LISTEN_PORT" 2>/dev/null | grep -qE "$ADDR_TAKEN"
}

# ── Гейт 4: env просит zenoh, а бинарь собран без кластерного слоя ───────────
# cluster — ОПЦИОНАЛЬНАЯ фича (в Cargo.toml `default = []`), и без неё весь блок
# #[cfg(feature = "cluster")] вырезается компилятором. Тогда процесс поднимается,
# слушает свой адрес и молча живёт без зеновского слушателя, без catch-up и без
# строки "cluster mode:" в логе — выглядит как живой стенд, а им не является.
# Признак ровно один и однозначный: строка, которую печатает только кластерная
# сборка, в бинаре есть — печатать её нечем, если блока нет.
if [ -n "${MATRIX_HS_ZENOH_LISTEN:-}${MATRIX_HS_ZENOH_CONNECT:-}" ] \
	|| [ "${MATRIX_HS_ZENOH_MODE:-}" = client ]; then
	if ! strings -a "$BIN" 2>/dev/null | grep -q 'cluster mode: connect='; then
		echo "stand-lift: в бинаре нет кластерного слоя, а env просит zenoh." >&2
		echo "stand-lift: zenoh — ОПЦИОНАЛЬНАЯ фича (Cargo.toml: default = []), собрать без неё можно," >&2
		echo "stand-lift: и тогда подъём молча теряет слушатель, catch-up и строку 'cluster mode:' в логе." >&2
		echo "stand-lift: пересобери с --features cluster." >&2
		exit 1
	fi
fi

# ── Гейт 4b: кластерный бинарь и скаутинг явно включён — отказ ────────────────
# Причина: на хосте с публичным интерфейсом мультикаст-скаутинг Zenoh слушает на
# ВСЕХ интерфейсах (UDP, 224.0.0.224:7446 и адреса каждого интерфейса), а правило —
# слушать только loopback или частный адрес. По умолчанию скаутинг выключен;
# MATRIX_HS_ZENOH_SCOUTING=on на стенде не поднимаем.
_zsc=$(printf '%s' "${MATRIX_HS_ZENOH_SCOUTING:-}" | tr -d '[:space:]' | tr '[:upper:]' '[:lower:]')
case "$_zsc" in
	on|1|true|yes)
		if strings -a "$BIN" 2>/dev/null | grep -q 'cluster mode: connect='; then
			echo "stand-lift: MATRIX_HS_ZENOH_SCOUTING включён, а бинарь кластерный — отказ: скаутинг слушает на всех интерфейсах." >&2
			echo "stand-lift: убери переменную или поставь MATRIX_HS_ZENOH_SCOUTING=off." >&2
			exit 1
		fi
		;;
esac
unset _zsc

# ── Гейт 3: наш адрес занят, а пид-файла нет — это не наш процесс ───────────
if addr_taken; then
	echo "stand-lift: адрес $LISTEN_HOST:$LISTEN_PORT УЖЕ слушает, а pid-файла нет." >&2
	echo "stand-lift: это не мой процесс — двойной старт НЕ делаю. Разбирайся руками." >&2
	sockstat -4 -l -p "$LISTEN_PORT" >&2
	exit 1
fi

  MATRIX_HS_MEMPROBE_LOG="$MEMPROBE"
  export MATRIX_HS_MEMPROBE_LOG
  
  # Арены jemalloc: виртуальный след не возвращается системе. На FreeBSD аллокатор
  # живёт в libc, и в этой сборке `opt.retain` по умолчанию true — арена держит
  # свой virtual high-water, а `ps rss` считает и подкачанные страницы, поэтому
  # память узла на стоянке выглядит как растущий остаток, хотя живой набор плоский.
  # Замерено на пробном процессе с тем же профилем аллокаций (24 потока, транзиентные
  # мегабайтные буферы), а не на стенде:
  #   дефолт                      high-water 233.9 МБ, удержание 233.9 МБ, RES 29.0
  #   retain:false                 high-water  74.6 МБ, удержание  64.9 МБ, RES  1.7
  #   retain:false + background_thread + dirty_decay_ms:1000
  #                                high-water  97.5 МБ, удержание  35.8 МБ, RES  6.0
  #   mallctl("opt.retain") в рантайме — НЕ РАБОТАЕТ (возвращает ошибку), поэтому
  #   вариант «подрезать из кода» отпадает, остаётся только переменная окружения.
  # Значение читается ПОСле sourcing env-файла, поэтому его можно переопределить.
  # ОТКАТ ОДНИМ ШАГОМ: дописать в env-файл строку `MALLOC_CONF=` (пустое значение) —
  # `${MALLOC_CONF-…}` (без двоеточия) уважает именно пустое значение, а не заменяет его.
  MALLOC_CONF="${MALLOC_CONF-retain:false,background_thread:true,dirty_decay_ms:1000}"
  export MALLOC_CONF
  
# Метка старта ДО запуска — состав старта должен читаться с диска, а не из
# памяти оператора (на этом окно #6 и потеряло воспроизводимость).
printf '[launcher] stand-lift start epoch=%s launcher_pid=%s bin_sha=%s listen=%s\n' \
	"$(date -u +%s)" "$$" \
	"$(sha256 -q "$BIN" 2>/dev/null | cut -c1-8)" \
	"$MATRIX_HS_LISTEN" >> "$LOG"

# Отцепление от терминала и от группы процессов оператора.
# daemon(8) — тем же способом, что rc-служба в deploy/bsdos/rc.d.
# Проверено на живом стенде: в -p daemon(8) кладёт ПИД СЫНА (сервера), а не
# свой собственный — pid-файл содержал 32664 при обёртке 32662. Это ровно то,
# что нужно: poke-usr2.sh целится USR2 в этот файл, и бить надо в сервер, а не
# в обёртку. Поэтому гейт 1 проверяет живость именно этого пида.
# ПРЕДУПРЕЖДЕНИЕ (проверено 01.10): exec "$BIN" без отцепления НЕ работает —
# сессия оператора убивает группу процессов вместе с сервером, стенд ложится
# через минуту после подъёма, и это выглядит как «поднялся и упал».
# Фоновый exec & тоже не спасает: группа процессов остаётся общей.
# STAND_FOREGROUND=1 — только для отладки руками, не из автоматики.
if [ "${STAND_FOREGROUND:-0}" = "1" ]; then
	echo "stand-lift: STAND_FOREGROUND=1 — в текущей сессии, не отцепляю."
	exec >> "$LOG" 2>&1
	exec "$BIN"
fi

/usr/sbin/daemon -f -o "$LOG" -p "$PIDFILE" "$BIN"

# ── Пост-проверка: подъём засчитан, только если НАШ адрес реально слушает ────
# Иначе «поднялся» означало бы «процесс запустился», а сервер может не
# поднять сокет (занятый порт, битый конфиг) — и следующий ход увидит стенд
# лежащим и не поймёт почему. Тот же адресный тест, что и в гейте 3: строка
# соседа на ДРУГОМ адресе того же порта подъёмом считаться не может.
# Ожидание настраиваемое: replay данных на живом стенде идёт ~35 с, 30 с давало
# ложное «НЕ поднялся» при нормально поднимающемся процессе.
_wait=${STAND_WAIT:-90}
case "$_wait" in
	''|*[!0-9]*) echo "stand-lift: STAND_WAIT='$_wait' — нужно целое число секунд." >&2; exit 2 ;;
esac
_t0=$(date +%s)
# Итоговое время ожидания держим ОТДЕЛЬНО: счётчик ниже обнуляется циклом, и
# печатать его в сообщении об отказе нельзя — оператор читал «не поднялся за
# 0с» после тридцати секунд ожидания и искал не тот адрес (проверено 02.10 на
# скретч-стенде: замер 31 с, в тексте 0 с).
_wait_total=$_wait
while [ "$_wait" -gt 0 ]; do
	if addr_taken; then
		echo "stand-lift: ПОДНЯТ за $(( $(date +%s) - _t0 ))с. $(sockstat -4 -l -p "$LISTEN_PORT" | grep -E "$ADDR_TAKEN")"
		exit 0
	fi
	sleep 1
	_wait=$(( _wait - 1 ))
done

echo "stand-lift: НЕ поднялся за $(( $(date +%s) - _t0 ))с (бюджет ${_wait_total}с, STAND_WAIT) — адрес $LISTEN_HOST:$LISTEN_PORT молчит." >&2
tail -5 "$LOG" >&2 2>/dev/null || :
exit 1
