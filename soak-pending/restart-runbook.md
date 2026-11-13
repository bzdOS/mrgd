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

Назначение: один заход владельца (кнопка рестарта стенда). Контекст агента не нужен.
Стенд: `pid <PID>` (текущий найти: `sockstat -4 -l | grep 8448`), user `<USER>`, бинарник в работе `<BIN>`, слушает `<ADDR>:8448`.
`rc.d`-скрипты других узлов к этому стенду не применять.

## 1) Бинарник (release, a5ad8ad)

- Ожидаемый артефакт по наряду: release-бинарник commit `a5ad8ad` ("redactions: add bounded cap to in-memory redactions map"), размер `9453920` байт.
- Путь размещения на стенде (рабочий): `<BIN>`
- Путь собранного артефакта в дереве сборки (для выкладки владельцем): `<ПУТЬ_СОБРАННОГО_RELEASE_A5AD8AD_9453920>` (дерево сборки, generic).
  (замер воркера на 2026-11-13: файл размером 9453920 байт в доступном дереве сборки и доступных путях не найден; каталог рабочего бинарника недоступен на чтение воркеру. Владелец подставляет фактический путь собранного файла перед выкладкой.)
- SHA256 (ожидаемого собранного бинарника, release a5ad8ad, 9453920 байт): `НЕ ИЗМЕРЕН ВОРКЕРОМ (артефакт недоступен/не найден в доступных путях)`

Команды владельца для фиксации/проверки фактического бинарника перед рестартом:
```sh
# Подставьте фактический путь собранного release-бинарника (a5ad8ad, 9453920 байт)
ART="<ПУТЬ_СОБРАННОГО_RELEASE_A5AD8AD_9453920>"
ls -l "$ART"
wc -c "$ART"                  # ожидание: 9453920
sha256 "$ART"                 # зафиксировать SHA256 фактического артефакта
file "$ART"
```
Справочно (доступный воркеру кандидат, НЕ заявленный артефакт a5ad8ad/9453920): `<SOAK-КАНДИДАТ>`, 25192920 байт, SHA256 `188b49f619f5917ef36a8c58bcb34ad74e95d4ee29fcc9e91b9ec9373b90bd0b` (замер 2026-11-13).

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

# 3.3 Останов текущего стенда (именно pid <PID>, user <USER>)
ps -p "$PID" -o pid,user,command
kill "$PID"
# дождаться завершения (до ~30с)
for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15; do
  kill -0 "$PID" 2>/dev/null || break
  sleep 2
done
if kill -0 "$PID" 2>/dev/null; then
  echo "WARN: pid $PID ещё жив; при необходимости: kill -TERM $PID && sleep 5"
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
