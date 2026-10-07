#!/bin/sh
# soak-rooms-origin.sh — КТО ПРИНЁС КОМНАТУ НА СТЕНД: remote-apply от соседа или локальная запись.
#
# ЗАЧЕМ. Одна приёмка soak требовала «счётчик комнат в покое на хвосте, Δ=0».
# На soak-h вышло Δ=+2: две комнаты родились внутри окна. Выяснилось, что этот критерий смешивает
# две разные вещи — покой СТЕНДА и приход новых комнат ОТ СОСЕДА. Этот скрипт разделяет их и
# печатает обе цифры: полный Δ окна (справочно) и хвостовой Δ (тревожный).
#
# ЧТО ЭТО НЕ УМЕЕТ, И ПОЧЕМУ. Origin НЕ ЗАПИСЫВАЕТСЯ НИГДЕ В ДАННЫХ, и это не пропуск этого
# скрипта, а свойство хранилища: журнал комнаты хранит sender, а sender у обеих нод
# заканчивается на :localhost, потому что MATRIX_HS_SERVER_NAME=localhost стоит на обоих узлах;
# pdumeta-сайдкар хранит signer_node, и он тоже "localhost" у входящих и у локальных комнат.
# Лог стенда per-room не пишет для пути apply_remote_state_event (строки «state catch-up for …»
# относятся к другому, media/history-проходу) — проверено: в логе стенда нет ни одного упоминания
# двух входящих комнат. Поэтому класс «remote-apply» здесь ВЫВОД по косвенному доказательству,
# и доказательство печатается рядом с классом, а не за него:
#   комната родилась внутри окна прогона, тогда как ВСЕ ряды рекордера имеют traffic=quiet
#   и mp_alloc без движения — локальный клиент не мог создать комнату в такое окно.
# Класс никогда не заявляется как факт. Всё, что не выводится, помечено как «не атрибутируется».
#
# УСТРОЙСТВО. Окно прогона берётся из колонки iso рекордера; окном «хвост» считаются последние
# TAIL_MIN минут окна (по умолчанию 120), потому что Δ хвоста иначе вырождается в «0 по
# определению»: последнее рождение и есть конец нулевого отрезка.
#
# ВАКУУМНЫЕ ГЕЙТЫ — ПОЧЕМУ БЕЗ --memprobe СКРИПТ ОТКАЗЫВАЕТСЯ. Рост читается из mp_alloc, а mp_alloc
# живёт в memprobe-файле. Без него раннер пишет mp_alloc=0.0 (soak-run-g.sh:239, гвард
# [ -n "$MEMPROBE" ]), и ОБА гейта роста читают константный ноль, то есть молчат вакуумно.
# Анализ, который не замечает этого, печатает «спокойно» по прогону, в котором никто не мерил.
# Поэтому отсутствие данных здесь — отказ с кодом 2, а не пустой отчёт.
#
# Usage: soak-rooms-origin.sh --soak-csv F --memprobe F --rooms DIR [--tail-min N]
#        soak-rooms-origin.sh --selftest
#   --selftest  ничего не пишет: печатает блок на переданных данных
#   input:  --soak-csv  csv рекордера (soak-run --log), из него окно и traffic
#           --memprobe  memprobe.csv стенда; ОБЯЗАТЕЛЕН, см. выше
#           --rooms     каталог журналов комнат, читается ТОЛЬКО на чтение
#   output: origin-таблица, хвостовой Δ, полный Δ окна, строки тревоги
#   sideEffects: none — ни одного файла не создаётся и не меняется

SOAK_CSV=""
MEMPROBE=""
ROOMS=""
TAIL_MIN=120
SELFTEST=0

while [ $# -gt 0 ]; do
	case "$1" in
	--soak-csv) SOAK_CSV=${2:?--soak-csv needs a path}; shift 2 ;;
	--memprobe) MEMPROBE=${2:?--memprobe needs a path}; shift 2 ;;
	--rooms) ROOMS=${2:?--rooms needs a path}; shift 2 ;;
	--tail-min) TAIL_MIN=${2:?--tail-min needs a number}; shift 2 ;;
	--selftest) SELFTEST=1; shift ;;
	-h|--help) sed -n '2,30p' "$0" | cut -c3-; exit 0 ;;
	*) echo "soak-rooms-origin: unknown argument: $1" >&2; exit 2 ;;
	esac
done

# ── Сторож данных: без mp_alloc любой вывод о покое пуст ──────────────────────
if [ -z "$MEMPROBE" ]; then
	echo "soak-rooms-origin: не передан --memprobe. БЕЗ НЕГО ГЕЙТЫ МОЛЧАТ ВАКУУМНО." >&2
	echo "  mp_alloc читается из memprobe-файла; без него раннер пишет mp_alloc=0.0" >&2
	echo "  (soak-run-g.sh:239, гвард [ -n \"\$MEMPROBE\" ]), и оба гейта роста читают константный" >&2
	echo "  ноль. Отчёт по такому прогону не значит «спокойно» — он значит «никто не мерил»." >&2
	exit 2
fi
[ -r "$MEMPROBE" ] || {
	echo "soak-rooms-origin: --memprobe не читается: $MEMPROBE — гейты молчат вакуумно, отказ." >&2
	exit 2
}
[ -n "$SOAK_CSV" ] || { echo "soak-rooms-origin: --soak-csv обязателен" >&2; exit 2; }
[ -r "$SOAK_CSV" ] || { echo "soak-rooms-origin: --soak-csv не читается: $SOAK_CSV" >&2; exit 2; }
[ -d "$ROOMS" ] || { echo "soak-rooms-origin: --rooms не каталог: $ROOMS" >&2; exit 2; }

block() {
	# Окно прогона и признаки локальной активности.
	WIN_FIRST=$(awk -F, 'NR>1 && $2!="" {print $2; exit}' "$SOAK_CSV")
	WIN_LAST=$(awk -F, 'NR>1 && $2!="" {l=$2} END{print l}' "$SOAK_CSV")
	ROWS=$(( $(wc -l < "$SOAK_CSV" | tr -d ' ') - 1 ))
	# traffic: сколько рядов НЕ quiet (это и есть следы локальной активности)
	# traffic: считаем ТОЛЬКО клиентскую активность. "baseline" — это первый ряд рекордера,
	# а не трафик, и раньше я по ошибке считал его за активность, из-за чего входящие комнаты
	# попадали в «не атрибутируется».
	TRAFFIC_ROWS=$(awk -F, 'NR>1 && $17!="quiet" && $17!="baseline" && $17!="" {n++} END{print n+0}' "$SOAK_CSV")
	# mp_alloc: есть ли вообще числа в ряду (0.0 = мерить нечем)
	MP_OK=$(awk -F, 'NR>1 && $13!="" && $13+0>0 {n++} END{print n+0}' "$SOAK_CSV")
	MP_LAST=$(awk -F, 'NR>1 && $13!="" {l=$13} END{print l}' "$SOAK_CSV")

	echo "== ДАННЫЕ =="
	echo "  csv рекордера: $SOAK_CSV"
	echo "  memprobe:      $MEMPROBE (последний mp_alloc в ряду: ${MP_LAST:-—})"
	echo "  окно прогона:  $WIN_FIRST .. $WIN_LAST, рядов $ROWS"
	echo "  рядов с mp_alloc>0: $MP_OK из $ROWS | рядов с traffic НЕ quiet: $TRAFFIC_ROWS"

	if [ "$MP_OK" -eq 0 ] 2>/dev/null; then
		echo "  ВНИМАНИЕ: mp_alloc ни в одном ряду не положителен — гейты роста читали ноль." >&2
	fi

	# ── Хвост окна ───────────────────────────────────────────────────────────
	# Порядок флагов у FreeBSD date: -v ДОЛЖЕН идти перед -f, иначе -v трактуется как
	# позиционный аргумент и выходной формат молча игнорируется (я получил TAIL_FROM
	# в формате по умолчанию и сравнивал с ним ISO-строки — сравнение было бессмысленным,
	# а «хвостовой Δ=0» не означало ничего).
	TAIL_FROM=$(date -u -j -v"-${TAIL_MIN}M" -f '%Y-%m-%dT%H:%M:%SZ' "$WIN_LAST" '+%Y-%m-%dT%H:%M:%SZ' 2>/dev/null)

	# ── Origin-таблица ───────────────────────────────────────────────────────
	# Классификация по доказательству, а не по имени комнаты: комната, родившаяся
	# ВНУТРИ окна при traffic=quiet на всех рядах, не могла быть создана локальным
	# клиентом. Всё остальное честно помечается как не атрибутируемое.
	echo
	echo "== ORIGIN-ТАБЛИЦА (класс = ВЫВОД по доказательству, не запись в данных) =="
	printf '  %-46s %-22s %-16s %s\n' "ROOM" "BIRTH" "КЛАСС" "РЯДЫ/СОБЫТИЯ"
	REMOTE=0
	INWIN_OTHER=0
	PREWIN=0
	LAST_BIRTH=""
	TAIL_BIRTHS=0
	TAIL_BIRTH_LIST=""
	for f in "$ROOMS"/*.jsonl; do
		case "$f" in *pdumeta.jsonl) continue ;; esac
		[ -r "$f" ] || continue
		BIRTH=$(stat -f '%SB' -t '%Y-%m-%dT%H:%M:%SZ' "$f" 2>/dev/null)
		[ -n "$BIRTH" ] || continue
		EVS=$(grep -c . "$f" 2>/dev/null || echo 0)
		# имя комнаты: %XX обратно в символ
		ROOM=$(printf '%s' "${f##*/}" | sed 's/\.jsonl$//' \
			| sed 's/%21/!/g; s/%23/#/g; s/%3a/:/g; s/%2f/\//g')
		# Три класса, и ни один из них не зовётся «локальная», если это не доказано:
		# журнал не хранит origin, поэтому класс «раньше окна» означает ровно одно —
		# комната существовала до окна, откуда она пришла, по этому файлу не читается.
		if [ -n "$WIN_FIRST" ] && [ "$BIRTH" \> "$WIN_FIRST" ] && [ "$BIRTH" \< "$WIN_LAST" ]; then
			if [ "$TRAFFIC_ROWS" -eq 0 ] 2>/dev/null; then
				CLS="remote-apply*"
				REMOTE=$((REMOTE + 1))
			else
				CLS="внутри, клиент был"
				INWIN_OTHER=$((INWIN_OTHER + 1))
			fi
		else
			CLS="раньше окна"
			PREWIN=$((PREWIN + 1))
		fi
		printf '  %-46s %-22s %-16s %s\n' "$ROOM" "$BIRTH" "$CLS" "событий $EVS"
		if [ -z "$LAST_BIRTH" ] || [ "$BIRTH" \> "$LAST_BIRTH" ]; then LAST_BIRTH=$BIRTH; fi
		# хвост: родилась ЛИ эта комната в последние TAIL_MIN минут окна. Считается, а не
		# подставляется: иначе «хвостовой Δ=0» было бы утверждением без измерения.
		# В test нет оператора >=; сравнение строк строгое, поэтому «больше или равно»
		# разворачиваю в два теста (раньше здесь был \>=, и sh писал «unexpected operator»
		# на каждой комнате — то есть ветка хвоста не отрабатывала вовсе).
		if [ -n "$TAIL_FROM" ] \
			&& { [ "$BIRTH" \> "$TAIL_FROM" ] || [ "$BIRTH" = "$TAIL_FROM" ]; } \
			&& [ "$BIRTH" \< "$WIN_LAST" ]; then
			TAIL_BIRTHS=$((TAIL_BIRTHS + 1))
			TAIL_BIRTH_LIST="$TAIL_BIRTH_LIST $ROOM($BIRTH)"
		fi
	done

	echo
	echo "  * доказательство класса remote-apply: рождение внутри окна при traffic=quiet"
	echo "    на всех рядах (рядов с иным traffic: $TRAFFIC_ROWS) и mp_alloc без роста."
	echo "    В ДАННЫХ ORIGIN НЕ ЗАПИСЫВАЕТСЯ: sender и signer_node у обеих нод = localhost,"
	echo "    лог стенда эти комнаты не упоминает. Класс — вывод, не запись."

	echo
	echo "== СЧЁТЧИК: ДВЕ ЦИФРЫ, НЕ ОДНА =="
	echo "  ПОЛНЫЙ Δ окна — входящие показаны, не спрятаны:"
	echo "    всего журналов: $(ls "$ROOMS" | grep -v pdumeta | grep -c '\.jsonl$')"
	echo "    из них: remote-apply* $REMOTE (внутри окна), внутри окна при клиенте $INWIN_OTHER, раньше окна $PREWIN"
	echo "    хвостовой Δ (последние ${TAIL_MIN} мин окна, с $TAIL_FROM): $TAIL_BIRTHS"
	if [ "$TAIL_BIRTHS" -gt 0 ] 2>/dev/null; then
		echo "      родившиеся в хвосте:$TAIL_BIRTH_LIST"
	fi
	echo "    последнее рождение во всём окне: ${LAST_BIRTH:-нет}"

	echo
	echo "== ТРЕВОГА =="
	if [ "$INWIN_OTHER" -gt 0 ] 2>/dev/null; then
		echo "  ВНИМАНИЕ: комнат, родившихся ВНУТРИ окна при клиентском трафике: $INWIN_OTHER —"
		echo "    это единственный класс, который говорит о СТЕНДЕ. Рост такого класса = утечка."
	fi
	echo "  Комнат, родившихся внутри окна без клиента: $REMOTE (входящие). Раньше окна: $PREWIN —"
	echo "    они в счёт покоя не идут: их происхождение по журналу не читается вовсе."
	if [ "$REMOTE" -gt 0 ] 2>/dev/null; then
		echo "  remote-apply: $REMOTE — входящие от соседа, само по себе утечкой НЕ является."
		echo "    Тревога на remote-apply — только МОНОТОННЫЙ рост: если при следующем запуске"
		echo "    это число выше, а не так же, значит сосед льёт новое без остановки."
	fi
	echo "  Критерий «покой» измерять по локальным записям, а не по общему счётчику:"
	echo "  общий счётчик растёт от соседа и говорит о соседе, а не о стенде."
}

if [ "$SELFTEST" = "1" ]; then
	block
	exit 0
fi
block
