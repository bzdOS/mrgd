# Runbook рестарта стенда matrix-hs (cap redactions, a5ad8ad)

## Переменные (владелец подставляет перед запуском)
```sh
ADDR="<ADDR>"                      # адрес/порт стенда; найти слушатель: sockstat -4 -l | grep 8448
PORT="8448"
PID="<PID>"                        # текущий pid стенда: sockstat -4 -l | grep 8448 (или ps -ax -o pid,user,command | grep '[m]atrix-hs')
USER="<USER>"                      # пользователь стенда
BIN="<BIN>"                        # путь рабочего бинарника стенда
SOAK_CANDIDATE="<SOAK-КАНДИДАТ>"   # справочный кандидат из soak-каталога (если нужен)
ENVF="<ENVF>"                      # env-файл стенда
LOG="<LOG>"                        # лог стенда
ART="<ПУТЬ_СОБРАННОГО_RELEASE_A5AD8AD_9453920>"  # собранный release a5ad8ad, 9453920 байт (из дерева сборки)
```

Назначение: один заход владельца (кнопка установки+запуска стенда; стенд не запущен). Контекст агента не нужен.
Стенд: на момент подготовки не запущен (процесса user `<USER>` / слушателя `8448` нет по замеру 02:02Z). Текущий слушатель/pid при необходимости проверить: `sockstat -4 -l | grep 8448`; user `<USER>`, бинарник в работе `<BIN>`, слушает `<ADDR>:8448`.
`rc.d`-скрипты других узлов к этому стенду не применять.

## 1) Бинарник (release, a5ad8ad)

- Ожидаемый артефакт по наряду: release-бинарник commit `a5ad8ad` ("redactions: add bounded cap to in-memory redactions map").
- Путь размещения на стенде (рабочий): `<BIN>`
- Путь свежесобранного release-бинарника (дерево сборки, замер 2026-11-13, main 51edb5e / код a5ad8ad): `/opt/cargo-target/release/matrix-hs`
- Размер (байт): `9453920`
- SHA256: `ca99a4206f446331027b37bb254fcc112e3da72175e97079b42374fcfd0e3a4e`

Команды владельца для фиксации/проверки фактического бинарника перед выкладкой:
```sh
# Свежесобранный release-бинарник (замер 2026-11-13)
ART="/opt/cargo-target/release/matrix-hs"
ls -l "$ART"
wc -c "$ART"                  # ожидание: 9453920
sha256 "$ART" || sha256sum "$ART"   # ожидание: ca99a4206f446331027b37bb254fcc112e3da72175e97079b42374fcfd0e3a4e
file "$ART"
```

## 2) Переменная окружения (добавить в env-файл `<ENVF>`)

Точная строка для добавления:
```sh
MATRIX_HS_REDACTIONS_MAX_ENTRIES=10000
```

Проверка после правки:
```sh
grep -n '^MATRIX_HS_REDACTIONS_MAX_ENTRIES=' "$ENVF"
```

## 3) Команды рестарта (от имени владельца, одним заходом)

```sh
set -eu

BIN="${BIN:-<BIN>}"
ENVF="${ENVF:-<ENVF>}"
LOG="${LOG:-<LOG>}"
PID="${PID:-<PID>}"   # текущий pid: sockstat -4 -l | grep 8448

# 3.1 Сохранить старый бинарник для отката
cp -p "$BIN" "${BIN}.pre-cap"
ls -l "$BIN" "${BIN}.pre-cap"

# 3.2 (опционально) выложить новый бинарник a5ad8ad, если ART подготовлен
# cp -p "$ART" "$BIN" && chmod 755 "$BIN"

# 3.3 Останов текущего стенда, если он запущен (guard: стенд может отсутствовать)
# Проверка слушателя на :8448; если процесса нет — останов пропустить, идти к 3.4
LISTEN_PID=$(sockstat -4 -l 2>/dev/null | awk -v p=":$PORT" '$6 ~ p"$" {print $3; exit}')
if [ -z "${LISTEN_PID:-}" ]; then
  echo "INFO: слушатель :$PORT не найден (sockstat) — останов пропускаю, стенд не запущен"
else
  echo "INFO: найден слушатель :$PORT, pid $LISTEN_PID (ожидаемый $PID)"
  ps -p "$LISTEN_PID" -o pid,user,command || true
  kill "$LISTEN_PID"
  # дождаться завершения (до ~30с)
  for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15; do
    kill -0 "$LISTEN_PID" 2>/dev/null || break
    sleep 2
  done
  if kill -0 "$LISTEN_PID" 2>/dev/null; then
    echo "WARN: pid $LISTEN_PID ещё жив; при необходимости: kill -TERM $LISTEN_PID && sleep 5"
  fi
fi

# 3.4 Запуск как <USER> с env-файлом (выполнять от пользователя <USER>)
# Если вы root — переключитесь: su -m <USER> -c '...'
# Загрузка env и запуск в фоне с логом:
su -m <USER> -c "set -a; . '$ENVF'; set +a; nohup '$BIN' >> '$LOG' 2>&1 & echo started:\$!"

# 3.5 Проверка слушания <ADDR>:8448
sleep 2
sockstat -4 -l | grep 8448 || netstat -an | grep 8448

# 3.6 Проверка строки версии/старта в логе
# (точная строка зависит от сборки; ищем старт/версию/commit рядом с запуском)
tail -n 200 "$LOG" | egrep -i 'start|version|a5ad8ad|matrix-hs|listen|8448' || tail -n 50 "$LOG"
ps -ax -o pid,user,command | grep '[m]atrix-hs'
```

Примечание: фактический путь лога стенда (`<LOG>` выше — плейсхолдер) уточните по текущему запуску/окружению перед проверкой шага 3.6.

## 4) Откат

Сохранённый старый бинарник: `<BIN>.pre-cap`

```sh
set -eu
BIN="${BIN:-<BIN>}"
ENVF="${ENVF:-<ENVF>}"
LOG="${LOG:-<LOG>}"

# определить новый pid стенда и остановить его
NEWPID=$(sockstat -4 -l | awk '/8448/ {print $3; exit}')
[ -z "${NEWPID:-}" ] && NEWPID=$(ps -ax -o pid,user,command | awk '/[m]atrix-hs/ && $2=="<USER>" {print $1; exit}')
echo "stopping ${NEWPID:-<PID>}"
[ -n "${NEWPID:-}" ] && kill "$NEWPID"
sleep 3

# восстановить старый бинарник
cp -p "${BIN}.pre-cap" "$BIN"
ls -l "$BIN"

# (опционально) убрать/закомментировать переменную cap в env, если нужен полный откат поведения
# sed -i.bak '/^MATRIX_HS_REDACTIONS_MAX_ENTRIES=/d' "$ENVF"

# запуск как <USER> с env-файлом
su -m <USER> -c "set -a; . '$ENVF'; set +a; nohup '$BIN' >> '$LOG' 2>&1 & echo started:\$!"
sleep 2
sockstat -4 -l | grep 8448 || netstat -an | grep 8448
tail -n 50 "$LOG"
```

## 5) cargo test (main a5ad8ad)

Строки "test result:" дословно:
test result: ok. 302 passed; 0 failed; 7 ignored; 0 measured; 0 filtered out; finished in 6.21s
test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
test result: ok. 11 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
