# Retry для транзиентных 404 от Google Sheets API (operation-level, идемпотентные режимы)

## Overview

Google Sheets API иногда возвращает 404 на реально существующий spreadsheet после
тяжёлых операций записи. Текущий retry обрабатывает только `(429, 500, 503)` — 404
не ретраится и задача Airflow падает с `HttpError 404`. Реальный инцидент произошёл
в **merge**-режиме (`write_ovz_control_to_sheets`, 2026-06-27).

### Scope (после серии ревью)

Этот план закрывает 404 **только для идемпотентных режимов** — `merge`,
`overwrite` и `_ensure_sheet_exists`. Механизм: повтор всей операции на транзиентный
404 (операции идемпотентны при полном re-run, поэтому повтор безопасен).

**`append` намеренно вне scope.** append не идемпотентен, и безопасный retry для
него требует не «обёртки на 404», а смены контракта (идемпотентный dedup-ключ или
позиционная запись с persist-состоянием) — это отдельный дизайн с продуктовыми
решениями. Любой best-effort guard в памяти не переживает Airflow task-retry и
добавляет риск **тихой потери данных** (ложное «батч записан» → пропуск батча),
что хуже текущего fail-fast. Поэтому append оставляем как есть (404 падает сразу,
как сегодня), а идемпотентный append вынесен в отдельный план — см.
`docs/plans/20260701-idempotent-append-design.md`.

### Почему operation-level, а не hook-level 404-retry

Слепой per-call retry на hook-декораторе небезопасен для многошаговых
read-modify-write операций (merge: read keys → delete → append → sort): повтор
одного шага ломает инвариант. И 404-retry на общих read-методах ломает read/manage
операторы (опечатка в `spreadsheet_id` висела бы ~35с вместо мгновенного падения).
Поэтому 404 обрабатывается **только в операторе, на уровне всей операции**;
`utils/retry.py` и хуки не меняются, read-методы остаются fail-fast.

## Idempotency режимов (при полном повторе операции)

| Режим     | Шаги                                                        | Идемпотентен при re-run? | В scope? |
|-----------|-------------------------------------------------------------|--------------------------|----------|
| overwrite | `clear_values` + `ensure_rows` + `update_values` + `trim_sheet` + `sortRange`* | ✅ да (фикс. диапазоны; sortRange идемпотентен) | ✅ |
| merge     | read keys + `update_values`(headers) + `deleteDimension` + `append_values` + `repeatCell` + `sortRange`* | ✅ да (удаляет все строки входящих ключей, потом дописывает) | ✅ |
| append    | header/height setup + `append_values` в цикле + `sortRange`* | ❌ нет (повтор допишет всё второй раз) | ❌ отдельный план |

*`sortRange` (`_execute_sort` → `batch_update`) добавлен веткой `sort_keys` (в
main, план 20260629-sort-keys) во все три режима. Пересортировка уже
отсортированного диапазона идемпотентна.

**Sort-хвост и внутренние чтения покрыты автоматически:** overwrite и merge
обёрнуты в `_run_with_transient_404_retry` целиком, поэтому их sort-хвост
(`_get_sheet_id`→`get_spreadsheet_metadata`, `_execute_sort`) и все внутренние
чтения (`ensure_rows`/`trim_sheet` metadata, merge key read) 404-устойчивы через
повтор операции. Отдельной локальной защиты не требуется.

**Обоснование идемпотентности merge при частичном сбое** (проверить тестами):
- сбой ДО записи → повтор = первый прогон;
- deletes применились, appends нет → повтор читает ключи (старых строк уже нет),
  delete_ops пусты, дописывает → корректно;
- deletes + часть appends применились → повтор видит частично дописанные строки
  (у них входящие ключи), удаляет их все, дописывает заново → корректно;
- всё применилось, упал `repeatCell`/`sortRange` → повтор пере-делает всё →
  корректно (лишняя работа, результат тот же).

**Load-bearing допущение merge:** идемпотентность держится на round-trip
стабильности merge-ключа — дописанные строки при повторном чтении должны
нормализоваться (`normalize_merge_key`, SERIAL_NUMBER/FORMATTED_STRING) обратно в
тот же ключ, иначе повтор их не «увидит» для удаления → дубли. Это то же допущение,
на котором базовый merge работает и сейчас — приемлемо, но назвать явно.

## Files

- Modify: `airflow_provider_google_sheets/operators/write.py` — operation-level 404 retry для merge/overwrite/ensure_sheet
- Modify: `tests/test_operators/test_write.py` — idempotency (со stateful fake sheet) / operation-retry / non-404 fail-fast
- Modify: `tests/test_hooks/test_google_sheets.py` — hook-level fail-fast на 404 (реальный декоратор `get_values`/`get_spreadsheet_metadata`, не мок)
- Modify: `readme.md`, `readme_ru.md` — задокументировать новые параметры оператора
- Modify: `CHANGELOG.md` — секция unreleased/fixed
- Move: план → `docs/plans/completed/` после завершения
- **Не меняются:** `utils/retry.py`, `hooks/google_sheets.py` (existing `(429,500,503)` retry остаётся), `_execute_append` (append 404 остаётся fail-fast — см. отдельный план)

## Design

### Параметры оператора

```python
transient_404_max_retries: int = 3
transient_404_base_delay: float = 5.0
```

Валидация в `__init__` (fail на DAG-load, как прочие параметры):
- `transient_404_max_retries` — **non-bool `int >= 0`** (`bool` — подкласс `int`, так
  что `True`/`False` явно отклонять, иначе даст странное число попыток);
- `transient_404_base_delay` — **non-bool real (`int`/`float`) `>= 0`**;
- non-numeric → `TypeError`, отрицательные → `ValueError` (иначе отрицательный delay
  уйдёт в `time.sleep`).

### Хелпер (единственный механизм 404-обработки)

```python
def _run_with_transient_404_retry(self, fn):
    attempt = 0
    while True:
        try:
            return fn()
        except HttpError as e:
            if e.resp.status != 404 or attempt >= self.transient_404_max_retries:
                raise
            attempt += 1
            delay = self.transient_404_base_delay * (2 ** (attempt - 1))
            logger.warning("Transient 404 in %s, re-running (attempt %d/%d) in %.1fs",
                           self.write_mode, attempt, self.transient_404_max_retries, delay)
            time.sleep(delay)
```

### Точки оборачивания в `execute()`

```python
if self.create_sheet_if_missing and self.sheet_name:
    self._run_with_transient_404_retry(
        lambda: self._ensure_sheet_exists(hook, self.spreadsheet_id, self.sheet_name))
...
if self.write_mode == "overwrite":
    return self._run_with_transient_404_retry(
        lambda: self._execute_overwrite(hook, headers, rows))
elif self.write_mode == "append":
    return self._execute_append(hook, headers, rows)   # НЕ обёрнут: append 404 = fail-fast (как сегодня)
elif self.write_mode in ("merge", "smart_merge"):
    return self._run_with_transient_404_retry(
        lambda: self._execute_merge(hook, headers, rows, original_headers=original_headers))
```

`_ensure_sheet_exists` идемпотентен (проверяет существование + ловит 400 "already
exists"); его `create_sheet`→`batch_update` и metadata-чтение покрываются повтором.

### Read-методы: без глобального 404-retry

`get_values` / `batch_get_values` / `get_spreadsheet_metadata` в хуках не меняются →
read/manage-операторы при неверном `spreadsheet_id` падают мгновенно. Внутренние
чтения merge/overwrite покрыты обёрткой всей операции; append-чтения остаются
fail-fast (append вне scope).

### Worst-case wall-time (merge / overwrite)

Один слой, НЕ мультипликативно: до `1 + transient_404_max_retries=3` = **4** полных
прогона `_execute_<mode>`, между ними backoff 5+10+20 = **35 c**; каждый вызов
ограничен `request_timeout=300 c`. Перезапуск большой записи (40k) занимает минуты,
но безопасен. Указать в docstring и рекомендовать держать Airflow `execution_timeout`
с запасом. `transient_404_*` тюнингуемые.

## Implementation Steps

Тест-gate в конце каждого таска: «прогнать релевантные тесты — должны пройти». Во
всех 404-тестах backoff отключать: `transient_404_base_delay=0` или patch
`write.time.sleep`.

### Task 1: параметры + хелпер + wiring (реализация)

**Files:** Modify `airflow_provider_google_sheets/operators/write.py`

- [x] добавить параметры `transient_404_max_retries=3`, `transient_404_base_delay=5.0` (в `__init__` + docstring; описать worst-case + `execution_timeout`)
- [x] валидация на DAG-load: `transient_404_max_retries` — non-bool `int >= 0`; `transient_404_base_delay` — non-bool real `>= 0`; non-numeric → `TypeError`, отрицательные → `ValueError`
- [x] добавить хелпер `_run_with_transient_404_retry(fn)` (backoff, только 404; не-404 `HttpError` пробрасывается сразу)
- [x] в `execute()`: обернуть `_ensure_sheet_exists`, `_execute_overwrite`, `_execute_merge`; `_execute_append` — вызывать напрямую (НЕ трогать)
- [x] `retry.py`, хуки, `_execute_append` — НЕ трогать

Тесты (дешёвые, без fake sheet — держим рядом с реализацией):
- [x] валидация: `transient_404_max_retries=-1` / `True` / non-int / `transient_404_base_delay=-1` / non-numeric → ошибка на DAG-load
- [x] прогнать существующие + новые тесты — зелёные (wiring ничего не сломал)

### Task 2: stateful fake sheet + idempotency-доказательство (merge/overwrite)

**Files:** Modify `tests/test_operators/test_write.py`

- [ ] завести **stateful in-memory fake sheet/hook** (хранит строки листа; `deleteDimension`/`append_values`/`update_values`/`clear`/`sortRange`/metadata меняют состояние) — иначе идемпотентность недоказуема на `MagicMock` (проверять ФИНАЛЬНЫЕ строки после retry, не call-count)
- [ ] merge: 404 на `deleteDimension` (deletes применились) → re-run всей merge → финальные строки без дублей/потери
- [ ] merge: 404 на `append_values` mid-loop → re-run → финальные строки корректны
- [ ] merge: round-trip — частично дописанные строки с входящим ключом на re-run находятся и удаляются (date-ключ через SERIAL_NUMBER)
- [ ] merge + `sort_keys`: 404 на sort-хвосте → re-run → отсортировано без дублей
- [ ] overwrite: 404 в середине → re-run → финальные строки без дублей
- [ ] overwrite + `sort_keys`: 404 на sort-хвосте → re-run → корректно
- [ ] `_ensure_sheet_exists`: 404 на `create_sheet`/metadata → повтор через wrapper, не падает
- [ ] прогнать тесты — должны пройти

### Task 3: fail-fast тесты (не-404, append, hook-level)

**Files:** Modify `tests/test_operators/test_write.py`,
`tests/test_hooks/test_google_sheets.py`

- [ ] wrapper: **не-404 `HttpError`** (`403`/`400`) в merge/overwrite → пробрасывается сразу, `time.sleep` НЕ вызывается
- [ ] merge/overwrite: 404 не исчезает за `transient_404_max_retries` → падает (не бесконечно)
- [ ] `transient_404_max_retries=0` → 404 пробрасывается без повторов
- [ ] **append не тронут:** append + 404 → падает мгновенно, без ретраев (фиксируем неизменённое поведение)
- [ ] **hook-level fail-fast** (в `tests/test_hooks/test_google_sheets.py`, реальный декоратор, не мок): `get_values` / `get_spreadsheet_metadata` + 404 → НЕ ретраит, `time.sleep` не вызывается (регресс-гард на случай будущего 404-retry в хуках)
- [ ] прогнать тесты — должны пройти

### Task 4: Finalization (checklist, не implementation-task)

- [ ] обновить `readme.md` и `readme_ru.md`: описать `transient_404_max_retries`, `transient_404_base_delay` в параметрах `GoogleSheetsWriteOperator`
- [ ] `python -m pytest tests/ -v` (repo-local путь; НЕ абсолютный `/Users/...`)
- [ ] все тесты зелёные, включая ранее существующие
- [ ] обновить `CHANGELOG.md` (unreleased/fixed: operation-level 404-retry для merge/overwrite/ensure_sheet; append и read-методы остаются fail-fast)
- [ ] переместить план в `docs/plans/completed/`

## Post-Completion

**Деплой в Airflow:**
- Опубликовать новую версию провайдера и обновить на `realcombi.mgcom.ru`
- Перезапустить упавший run `osnova_dash_ch_to_gs / write_ovz_control_to_sheets`
  от 2026-06-27 вручную. Режим merge идемпотентен — повтор безопасен (данные не
  повреждены: задача упала до завершения последовательности delete/append).

## Related

- `docs/plans/20260701-idempotent-append-design.md` — отдельный дизайн идемпотентного
  append (404-устойчивость + переживание Airflow task-retry без дублей/потери).
  Требует продуктовых решений (dedup-колонка vs позиционная запись с persist-state).
