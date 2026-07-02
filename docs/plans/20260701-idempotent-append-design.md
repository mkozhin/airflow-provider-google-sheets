# Идемпотентный append: устойчивость к транзиентным 404 через позиционную запись

## Overview

Сделать write-режим `append` устойчивым к транзиентным HTTP 404 (и к
ambiguous-success внутри hook-retry на `429/500/503`) **без риска дублей** —
переведя запись с `values.append` (`INSERT_ROWS`) на **позиционную**
`values.update` в фиксированный диапазон.

Это прямое продолжение уже завершённого плана
`docs/plans/completed/20260627-transient-404-retry.md`, который закрыл 404 для
идемпотентных режимов (`overwrite`/`merge`/`ensure_sheet`) через
`_run_with_transient_404_retry`, но **намеренно исключил** `append` — потому что
повтор `values.append` даёт дубли.

**Ключевая идея.** Если писать `append` не через `INSERT_ROWS`, а позиционно
(`update_values` в `[E0+1 .. E0+N]`, где `E0` — высота таблицы, зафиксированная
*до* первой попытки), то повтор записи перезаписывает те же ячейки → операция
идемпотентна на in-process повторе. `E0` держится в **локальной переменной**
процесса (никакого durable-хранилища), потому что мы чиним ровно тот случай,
когда таск **жив** и оператор ретраит 404 внутри себя.

### Что чиним (scope)

- **Источник дублей №2** — транзиентный 404 в середине записи, таск жив,
  оператор ретраит внутри процесса. ← основная цель.
- **Источник дублей №3** — ambiguous-success внутри hook-retry (`append_values`
  применился на сервере, но ответ потерян / пришёл 503, hook повторяет батч).
  Позиционный `update_values` делает повтор безвредным.

### Чего сознательно НЕ делаем

- **Источник дублей №1** — Airflow task-retry после **полного падения таска**.
  Durable-хранилища нет; при полном рестарте свежий прогон перечитает `E0` =
  текущую (уже выросшую) высоту и может задублить. Это **ровно сегодняшнее
  поведение append**, оно не ухудшается — просто не улучшается. Фиксируется как
  осознанное ограничение в docstring / CHANGELOG / README.

### API-решение (Вариант A — idempotent-by-default + opt-out)

- **Не** вводим новый `write_mode`. Делаем существующий `append` позиционным
  **по умолчанию**. Для обычной раскладки (таблица — последний блок, дописываем
  вниз, **один writer на лист**) видимый результат **идентичен** текущему
  `INSERT_ROWS`, поэтому существующие DAG-и получают защиту от 404 **без
  переписывания**.
- **Допущение single-writer (важно).** Позиционная запись фиксирует `E0` между
  чтением высоты и `update_values`; если между ними в лист допишет **другой**
  writer, дефолтный путь может **перезаписать чужие строки** (в отличие от
  атомарного `INSERT_ROWS`). В нашем контракте это исключено — по требованию в
  один лист пишет ровно один task/DAG. Для сценариев с конкурентной атомарной
  вставкой явно рекомендуем `append_insert_rows=True`. Ограничение фиксируется в
  docstring / README / CHANGELOG, и покрывается **interleaving-тестом** как
  демонстрация небезопасности при конкуренции.
- Добавляем **opt-out** булев параметр `append_insert_rows: bool = False`.
  `True` → возвращается сегодняшнее легаси-поведение (`hook.append_values` /
  `INSERT_ROWS`, вне 404-retry, `1:1` как сейчас) — для редких раскладок, где под
  таблицей в тех же столбцах есть контент (футер), который `INSERT_ROWS` сдвигал
  бы вниз, а позиционная запись перезапишет.

## Context (from discovery)

- **`airflow_provider_google_sheets/operators/write.py`**
  - `_execute_append` (~612–707): текущий `INSERT_ROWS`-цикл + sort-хвост.
  - `_run_with_transient_404_retry(fn, label)` (~382–417): re-run `fn` на 404,
    иначе re-raise; логирует WARNING на каждую попытку.
  - `execute` (~423–489): `append` вызывается **напрямую без обёртки** (~480).
  - `_execute_overwrite` (~527–606): образец позиционной записи —
    `hook.ensure_rows(...)` + `hook.update_values(batch_range, batch)` батчами.
  - `_execute_merge` (~713–929): образец наблюдаемости/return-dict.
- **`airflow_provider_google_sheets/hooks/google_sheets.py`**
  - `update_values(spreadsheet_id, range_, values, value_input_option="USER_ENTERED")`
  - `append_values(...)` — декорирован `@retry_with_backoff()` (источник №3).
  - `ensure_rows(spreadsheet_id, sheet_name, required)`, `get_values(...)`.
- **`airflow_provider_google_sheets/utils/retry.py`**: `retry_with_backoff`,
  `DEFAULT_RETRYABLE_STATUS_CODES=(429,500,503)`, WARNING на каждую попытку.
- **`tests/test_operators/test_write.py`**: готовые `FakeSheet` (mutable grid,
  моделирует `update_values`/`append_values`/`ensure_rows`/`_last_occupied_row`/
  `get_values` с триммингом хвоста) и `FakeSheetsHook` с одноразовой инъекцией
  404 через `fail_once(op, status=404, when="before|after", occurrence=N)`.
  Хелпер `_run_with_fake(fake_hook, op, context)` патчит hook и `time.sleep`.
  Существующие append-тесты: `TestAppend`, `TestAppendSortKeys`.
- Airflow support range: `apache-airflow>=2.7,<3.0`.

## Development Approach

- **Testing approach**: Regular (реализация → тесты в той же задаче).
- Маленькие сфокусированные изменения; тесты в каждой задаче; весь набор тестов
  зелёный перед следующей задачей.
- Обратная совместимость: `append_insert_rows=True` = легаси `1:1`; дефолтный
  позиционный путь визуально идентичен для обычной раскладки.
- Команда тестов (каноничная): `python -m pytest tests/ -q`. Если в окружении
  нужен venv — активировать доступный интерпретатор проекта; команда прогона от
  этого не меняется.
- ⚠️ НЕ использовать `cat`/heredoc для правки файлов (в окружении `cat`
  заалиасен на сломанный пейджер) — только Read/Edit/Write.

## Testing Strategy

- **Unit-тесты** — обязательны в каждой задаче, с использованием stateful
  `FakeSheet`/`FakeSheetsHook`. Идемпотентность проверять **по финальной сетке**
  (`fake.sheet.grid` / `data_rows()`), а не по числу вызовов.
- **E2E**: в проекте нет UI/e2e — не применимо.

## Progress Tracking

- `[x]` сразу по завершении пункта; `➕` — новые задачи; `⚠️` — блокеры.

## Solution Overview

**Механика дефолтного (позиционного) append:**

1. Определить оконный диапазон столбцов: если задан `cell_range` — из него
   (start_col..end_col + верхняя строка); иначе start_col/start_row из
   `table_start`, ширина = `len(headers)` (или max ширина строк payload при
   отсутствии headers).
2. Прочитать `E0` — абсолютный номер последней непустой строки в оконном
   диапазоне от верхней строки таблицы вниз (та же логика, что в sort-ветке:
   `any(str(c).strip() for c in r)`); если контента нет — `E0 = start_row - 1`.
   Чтение обернуть в `_run_with_transient_404_retry(..., label="append")`
   (чтение идемпотентно). `E0` зафиксировать в локальной переменной **до**
   retry-обёртки записи.
3. Решение о заголовках принять из **до-мутационного** `E0`:
   `write_header_flag = (E0 < start_row) and self.write_headers and headers`
   (лист пуст). `data_start_abs = start_row + 1 if write_header_flag else E0 + 1`.
4. Позиционная запись — единый retried-блок
   `_run_with_transient_404_retry(_write, label="append")`, где `_write`:
   - `hook.ensure_rows(spreadsheet_id, sheet_name, data_start_abs + N - 1)`;
   - если `write_header_flag` — `update_values(header_range=start_row, [headers])`;
   - цикл батчами: `update_values(row=data_start_abs + i, batch)` (батч по
     `batch_size`, `pause_between_batches` между батчами).
   Повтор `_write` с тем же `E0` идемпотентен (перезапись тех же ячеек) →
   закрывает и №2, и №3. **Важно про №3:** hook-level retry на `429/500/503`
   (`@retry_with_backoff`) повторяет **тот же** `update_values` в **тот же**
   фиксированный range — как и operation-level 404-ретрай. То есть №3
   закрывается ровно тем же свойством «позиционная запись идемпотентна на
   повторе». В юнит-тестах через `FakeSheetsHook` (он заменяет декорированный
   hook целиком, поэтому `@retry_with_backoff` там не срабатывает) №3 проверяется
   как **двойное применение** `_write` к тому же `E0` → grid без дублей; реальный
   `429/500/503` retry — при желании отдельным hook-level тестом.
5. **Пустой / zero-width payload (no-op).** Если `rows == []` — данные не пишем
   (цикл `update_values` пропускается, `ensure_rows` не двигает лист),
   `rows_written = 0`; заголовки в пустой лист при `write_header_flag` всё равно
   пишем (паритет с текущим append). Ширину окна вычислять с защитой от пустоты:
   `max((len(r) for r in rows), default=0)` и `len(headers) if headers else 0` —
   на `headers is None and rows == []` не должно быть `ValueError` из `max()`.
6. `sort_keys` (если заданы) — **последний шаг, ВНЕ retried-блока записи** (как и
   сейчас: append не оборачивал sort). Так post-write 404 никогда не приводит к
   повторной записи поверх отсортированных строк. 404 на самом sort остаётся
   fail-fast (как сегодня). Точный перевод `E0` в термины существующей sort-ветки
   (write.py:674–704), которая оперирует **относительным** `existing_row_count`:
   - `existing_row_count = max(0, E0 - (start_row - 1))`;
   - `skip_header = write_header_flag or (E0 >= start_row and self.has_headers)`;
   - `end_row = (start_row - 1) + existing_row_count + (1 if write_header_flag
     else 0) + N` — что при `write_header_flag=False` эквивалентно `E0 + N`.
   Эти формулы зафиксировать в коде и покрыть тестом на диапазон sort (start/end
   row + ширина) под позиционным путём.

**Легаси-путь (`append_insert_rows=True`)** — целиком текущее тело
`_execute_append` (через `hook.append_values`), вызывается напрямую, **без**
404-retry. Поведение `1:1` с сегодняшним.

**Наблюдаемость (единообразно для всех операций):**

- `execute()` в начале ставит `self._transient_404_retries = 0`.
- `_run_with_transient_404_retry` инкрементирует `self._transient_404_retries` на
  каждый повтор (WARNING-строки на попытку сохраняются).
- Каждый return-dict (`overwrite`/`append`/`merge`) получает поле
  `"transient_404_retries": self._transient_404_retries` (0 без ретраев).
- Если счётчик > 0 — в конце `execute()` INFO-строка
  `"<mode> completed after K transient-404 retries"`.

## Technical Details

- **Контракт/ограничения дефолтного append** (документировать):
  - `cell_range` **разрешён** — задаёт оконный диапазон столбцов; соседние
    столбцы не трогаются. Прежний запрет `sort_keys` + `append` + `cell_range`
    (в `__init__`) **остаётся**.
  - Допущение: таблица — **последний блок** контента в своих столбцах; ниже
    последней непустой строки пусто. Контент под таблицей в тех же столбцах будет
    перезаписан → для таких раскладок `append_insert_rows=True`.
  - **Расхождение с легаси по заголовкам** (документировать как осознанное):
    дефолтный путь проверяет пустоту по **оконному** диапазону, а легаси
    non-sort append — по **одной** верхней ячейке. Для раскладки «верхняя левая
    ячейка пуста, но правее в той же строке есть данные»
    (`test_append_empty_start_cell_with_data_to_the_right`) они расходятся:
    легаси пишет заголовки, позиционный — нет (`E0 >= start_row`). Тест этой
    раскладки мигрировать на `append_insert_rows=True`.
  - **Ширина при single-cell `cell_range`** (напр. `"C3"`): ширину окна брать как
    `max(end_col - start_col + 1, max(len(r) for r in payload))`, чтобы более
    широкие строки payload не обрезались.
- **Валидация `append_insert_rows`** в `__init__`: должен быть `bool`
  (`isinstance(x, bool)`; иначе `TypeError`). Дефолт `False`.
- `value_input_option` в позиционной записи — оставить `"USER_ENTERED"` (как
  `append_values`/`overwrite`), чтобы не менять коэрцию значений.
- Return-dict дефолтного append сохранить форму `{"mode": "append",
  "rows_written": N, "transient_404_retries": K}`.

## What Goes Where

- **Implementation Steps** (`[ ]`): код + тесты + docs — всё в этом репозитории.
- **Post-Completion** (без чекбоксов): публикация версии провайдера, ручной
  прогон боевого DAG — вне репозитория.

## Implementation Steps

### Task 1: Счётчик 404-ретраев в `_run_with_transient_404_retry` + инициализация

**Files:**
- Modify: `airflow_provider_google_sheets/operators/write.py`
- Modify: `tests/test_operators/test_write.py`

- [ ] Инициализировать `self._transient_404_retries = 0` в `__init__` (для
      робастности при прямом юнит-вызове приватных методов) **и** сбрасывать в 0
      в начале `execute()` (до `create_sheet_if_missing`-ветки).
- [ ] В `_run_with_transient_404_retry` при каждом повторе инкрементировать
      `self._transient_404_retries` (рядом с существующим WARNING-логом), не меняя
      сигнатуру/семантику re-raise.
- [ ] Написать тест: одиночный 404 → `self._transient_404_retries == 1` после
      успешной операции (через `fail_once` на существующей idempotent-операции,
      напр. overwrite/merge).
- [ ] Написать тест: без 404 счётчик остаётся 0.
- [ ] Прогнать тесты — должны пройти перед Task 2.

### Task 2: Поле `transient_404_retries` в return-dict всех операций + INFO-summary

**Files:**
- Modify: `airflow_provider_google_sheets/operators/write.py`
- Modify: `tests/test_operators/test_write.py`

- [ ] В `_execute_overwrite`, `_execute_append` (легаси-ветка тоже), `_execute_merge`
      добавить в возвращаемый dict `"transient_404_retries": self._transient_404_retries`.
- [ ] В `execute()` после получения результата операции: если
      `self._transient_404_retries > 0` — INFO-лог
      `"<write_mode> completed after %d transient-404 retries"`.
- [ ] Тест: return-dict overwrite/append/merge содержит `transient_404_retries: 0`
      без ретраев.
- [ ] Тест: при инъекции 404 return-dict содержит `transient_404_retries: K` (K>0)
      и пишется INFO-строка (проверить через `caplog`).
- [ ] Прогнать тесты — должны пройти перед Task 3.

### Task 3: opt-out `append_insert_rows` + выделение легаси-пути (рефактор без смены поведения)

> Чистый рефактор + новый параметр. Дефолт (`False`) на этом шаге **ещё
> использует легаси** `INSERT_ROWS`, поэтому существующие append-тесты остаются
> зелёными без изменений — позиционная ветка включается только в Task 4.

**Files:**
- Modify: `airflow_provider_google_sheets/operators/write.py`
- Modify: `tests/test_operators/test_write.py`

- [ ] Добавить параметр `append_insert_rows: bool = False` в `__init__`
      (после `sort_keys`), сохранить в `self.append_insert_rows`. Валидация:
      `if not isinstance(append_insert_rows, bool): raise TypeError(...)`.
- [ ] Выделить текущее тело `_execute_append` в `_execute_append_insert_rows(...)`
      (легаси `INSERT_ROWS`-путь без изменений, включая его sort-ветку).
- [ ] `_execute_append` временно делегирует **всегда** в
      `_execute_append_insert_rows(...)` (TODO: позиционная ветка в Task 4) — так
      дефолтное поведение на этом шаге идентично сегодняшнему.
- [ ] Тест: `append_insert_rows` с не-bool → `TypeError` на DAG-load.
- [ ] Тест: `append_insert_rows=True` идёт легаси-путём (вызывается
      `append_values`).
- [ ] Прогнать `python -m pytest tests/ -q` — всё зелёное (существующие
      append-тесты без правок); перед Task 4.

### Task 4: Позиционная запись по умолчанию (без sort) + миграция non-sort тестов

**Files:**
- Modify: `airflow_provider_google_sheets/operators/write.py`
- Modify: `tests/test_operators/test_write.py`

- [ ] Реализовать `_execute_append_positional(...)` **без sort**: оконный диапазон
      столбцов (`cell_range` → start/end col + верхняя строка, ширина
      `max(окно, max ширина payload)` с защитой от пустоты; иначе `table_start` +
      ширина данных). ⚠️ В операторе есть только `_parse_range_start` (start col/row,
      write.py:496) — для `cell_range`-ветки нужен новый хелпер парсинга **end-столбца**
      (напр. `"C3:E"` → end col `E`; образец логики — `_parse_range` в тестах);
      чтение `E0` (последняя непустая строка окна, иначе
      `start_row-1`) в локальную переменную, обёрнутое в
      `_run_with_transient_404_retry(..., label="append")`;
      `write_header_flag`/`data_start_abs` из до-мутационного `E0`; `_write`
      (ensure_rows → опц. заголовки → батчи `update_values` в
      `[data_start_abs + i ...]`) в `_run_with_transient_404_retry(_write,
      label="append")`; **обработка пустого payload** (`rows == []` → no-op,
      `rows_written=0`, заголовки при пустом листе всё равно пишутся); return-dict
      с `rows_written` и `transient_404_retries`.
- [ ] Переключить `_execute_append` на позиционную ветку при
      `append_insert_rows=False` (диспетчер: `True → legacy`, `False → positional`).
      Sort в позиционной ветке пока НЕ реализован — если `self.sort_keys` заданы,
      временно допустимо делегировать в легаси или явно оставить TODO; полноценный
      sort добавляется в Task 5 (не оставлять несортированным то, что тест ждёт
      отсортированным — sort-тесты мигрируются в Task 5).
- [ ] **Аудит всех append-тестов**: прогнать
      `rg 'write_mode="append"' tests/test_operators/test_write.py` и для КАЖДОГО
      кейса явно решить — перевести на `append_insert_rows=True` (проверка
      легаси-механики: `append_values`-asserts, single-cell read,
      `update_values.assert_not_called()`), на grid-assert через `FakeSheet`, или
      оставить (sort-кейсы — в Task 5). Затронуты минимум: `TestAppend` (145–266),
      `TestTableStart` append (~1199–1283), append-кейсы `TestDataSources`,
      `TestPartitionBy`, `TestColumnMapping`. Мотив: на bare-`MagicMock`
      `get_values` позиционное чтение `E0` кидает `TypeError`.
- [ ] **`TestTransient404FailFast.test_append_404_fails_fast_no_retry` (4205)**:
      перевести на `append_insert_rows=True` (fail-fast там сохраняется) + НОВЫЙ
      тест: дефолтный путь **ретраит** 404 на позиционной записи.
- [ ] Тест (идемпотентность №2): 404 `when="after"` на первом `update_values`
      **дата-батче** → `fake.sheet.grid` содержит РОВНО ожидаемые строки, без
      дублей. ⚠️ Внутри `_write` заголовок в пустой лист тоже пишется через
      `update_values` ДО дата-батчей — либо использовать непустой лист (без
      header-write), либо учесть это в `occurrence` для `fail_once`.
- [ ] Тест (№3, позиционная идемпотентность): **двойное применение** `_write` к
      тому же `E0` → grid без дублей (моделирует и operation-level 404-ретрай, и
      hook-level `429/500/503` retry — оба повторяют тот же `update_values` в тот
      же range).
- [ ] Тест (E0 фиксирован): непустой лист + 404 на записи → ретрай пишет в тот же
      `[E0+1..]`, высота не «уползает».
- [ ] Тест (single-writer, документирующий): если между чтением `E0` и записью в
      лист «чужим» writer'ом добавлена строка (FakeSheetsHook, мутирующий grid
      между read и write) — позиционная запись её перезаписывает; тест фиксирует
      требование single-writer и рекомендацию `append_insert_rows=True` для
      конкурентной вставки.
- [ ] Тест: пустой лист + `write_headers=True/False`; пустой payload (`data=[]`) →
      no-op без `ValueError`; `cell_range` окно (соседние столбцы слева и справа не
      тронуты); дефолтный append использует `update_values`, а не `append_values`.
- [ ] Прогнать `python -m pytest tests/ -q` — всё зелёное перед Task 5.

### Task 5: Интеграция sort в позиционный append + миграция sort-тестов

**Files:**
- Modify: `airflow_provider_google_sheets/operators/write.py`
- Modify: `tests/test_operators/test_write.py`

- [ ] Реализовать sort-хвост в `_execute_append_positional` как последний шаг
      **вне** retried-блока записи, по формулам из Solution Overview §6
      (`existing_row_count = max(0, E0 - (start_row-1))`,
      `skip_header = write_header_flag or (E0 >= start_row and has_headers)`,
      `end_row = (start_row-1) + existing_row_count + (1 if write_header_flag
      else 0) + N`); убрать временное делегирование в легаси при `sort_keys`.
- [ ] Мигрировать `TestAppendSortKeys` (~2911–3050) на позиционный путь
      (grid-assert / проверка диапазона sort).
- [ ] Тест: диапазон sort (start/end row + ширина) под позиционным путём
      соответствует эталонным кейсам; 404 на записи + затем sort не приводит к
      повторной записи поверх отсортированных строк.
- [ ] Прогнать `python -m pytest tests/ -q` — всё зелёное перед Task 6.

### Task 6: execute()-комментарий + актуализация docstrings + integration-тест

**Files:**
- Modify: `airflow_provider_google_sheets/operators/write.py`
- Modify: `tests/test_operators/test_write.py`

- [ ] Убедиться, что в `execute()` `append` вызывается напрямую
      (`self._execute_append(...)`), БЕЗ внешнего `_run_with_transient_404_retry`
      (гранулярность retry — внутри `_execute_append`); обновить комментарий у
      append-ветки (убрать «append is NOT idempotent — fail-fast», описать новый
      контракт и opt-out).
- [ ] Актуализировать docstrings, ставшие неверными:
      `_run_with_transient_404_retry` (write.py:390–391, «append is never
      wrapped»); `transient_404_max_retries` (write.py:107–114, «append is
      intentionally excluded … fail-fast»); class-docstring bullet про `append`
      (write.py:39). Плюс docstring нового `append_insert_rows` (назначение,
      допущения single-writer / «под таблицей пусто» / расхождение по заголовкам).
- [ ] Тест интеграции через `execute()`: дефолтный append с одним 404 в середине
      записи завершается успешно, финальная сетка корректна, `transient_404_retries`
      в XCom-результате отражает число ретраев.
- [ ] Прогнать `python -m pytest tests/ -q` — всё зелёное перед Task 7.

### Task 7: Обновить документацию

**Files:**
- Modify: `readme.md`
- Modify: `readme_ru.md`
- Modify: `CHANGELOG.md`

- [ ] `readme.md` / `readme_ru.md`: описать новое дефолтное поведение `append`
      (позиционная запись, устойчивость к транзиентным 404), параметр
      `append_insert_rows`, допущения «под таблицей в тех же столбцах пусто» и
      **single-writer** (для конкурентной вставки — `append_insert_rows=True`),
      поле `transient_404_retries` в XCom; отметить, что Airflow task-retry не
      покрывается.
- [ ] `CHANGELOG.md` (Unreleased): **Changed** — `append` теперь пишет позиционно
      и устойчив к транзиентным 404/ambiguous-success (opt-out
      `append_insert_rows=True` возвращает легаси `INSERT_ROWS`); **Added** —
      `transient_404_retries` в XCom всех режимов (`overwrite`/`append`/`merge` —
      **API addition**) + INFO-summary; примечание про непокрытый task-retry и
      требование single-writer.
- [ ] Прогнать `python -m pytest tests/ -q` — всё зелёное перед Task 8.

### Task 8: Verify acceptance criteria + перенос плана

- [ ] Проверить, что закрыты источники №2 и №3, а №1 (task-retry) явно вне scope.
- [ ] Проверить контракт: `cell_range` разрешён, `sort_keys`+`cell_range` запрет
      сохранён, `append_insert_rows=True` = легаси `1:1` (включая fail-fast на 404),
      допущение single-writer задокументировано.
- [ ] Проверить наблюдаемость: `transient_404_retries` в return-dict всех режимов;
      INFO-summary при K>0; WARNING на каждую попытку.
- [ ] Прогнать весь набор: `python -m pytest tests/ -q` — всё зелёное.
- [ ] Переместить этот план в `docs/plans/completed/`.

## Post-Completion

*Только информационно — вне репозитория.*

**External system updates:**
- Опубликовать новую версию провайдера и обновить на `realcombi.mgcom.ru`.
- (Опционально) добавить пример keyless-журнала на дефолтном append в
  `examples/`, если появится боевой DAG.

**Manual verification:**
- Прогнать боевой append-DAG (при появлении) и убедиться, что WARNING-строки
  транзиентных 404 и `transient_404_retries` в XCom видны в логах Airflow.
