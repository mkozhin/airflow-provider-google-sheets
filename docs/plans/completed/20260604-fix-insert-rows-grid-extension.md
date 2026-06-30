# Fix: INSERT_ROWS для надёжного расширения сетки в merge и append режимах

## Overview

Фикс производственного бага: `GoogleSheetsWriteOperator` в режиме `merge` падает с HTTP 400
"Range exceeds grid limits" при выгрузке исторических данных, полностью перекрывающихся
с существующими строками в листе.

**Что сломано:** задача `write_ovz_control_to_sheets` (DAG `osnova_dash_ch_to_gs`) упала с:
```
HttpError 400: Range (RAW_af!A8374:BF25587) exceeds grid limits. Max rows: 8373
```

**Потенциальный скрытый баг в append:** режим `append` может молча потерять данные (не
написать ничего, вернув `rows_written: N`) если сетка листа заполнена полностью. Исключения
не бросается. Производственного кейса не зафиксировано, но защита добавляется превентивно.

## Причина ошибки (детальный разбор)

### Цепочка событий при историческом запуске

1. В листе `RAW_af` существуют данные с 2026-04-20 по 2026-05-20.
2. Запускается историческая выгрузка за 2026-04-01 — 2026-05-01.
3. Оператор находит совпадающие ключи (даты 2026-04-20 — 2026-05-01) и удаляет
   их через `deleteDimension`.
4. **Ключевой момент:** `deleteDimension` физически удаляет строки → Google Sheets
   сжимает сетку ровно до оставшегося числа строк данных. Slack (пустые строки в конце)
   исчезает. Сетка заполнена на 100%.
5. `values.append` с `insertDataOption=OVERWRITE` (дефолт) пытается писать за конец
   заполненной сетки. Google Sheets не расширяет сетку — данные не записываются, но
   API возвращает 200 OK.
6. `repeatCell` (очистка форматирования у новых строк) отправляет диапазон
   `startRowIndex=8373, endRowIndex=25587`, но сетка по-прежнему имеет 8373 строки
   → HTTP 400 "exceeds grid limits".

### Почему обычные (не исторические) запуски работают

Обычный запуск (например 2025-05-10 — 2026-05-21) содержит даты **за пределами**
существующего диапазона (после 2026-05-20). Предыдущие запуски уже расширяли сетку
за эти даты, в конце листа всегда есть slack (пустые строки). `OVERWRITE` успешно
пишет в этот slack без необходимости расширять сетку.

### Почему _execute_overwrite уже защищён

В `_execute_overwrite` разработчики заблаговременно добавили `hook.ensure_rows()`:
```python
# строка 333
hook.ensure_rows(self.spreadsheet_id, self.sheet_name, required_rows)
```
Этот паттерн не был применён для `_execute_merge` и `_execute_append`.

## Решение

Добавить `insertDataOption="INSERT_ROWS"` в вызов Google Sheets API внутри `hook.append_values` — одна строка. Сигнатура метода и код операторов не меняются.

`INSERT_ROWS` явно вставляет строки, гарантированно расширяя сетку вне зависимости
от её текущего заполнения.

**Поведение slack-строк после изменения:** с `INSERT_ROWS` пустые строки в конце листа
(если были) смещаются вниз, их количество не меняется. Данные корректны.

## Context (from discovery)

- **Файлы реализации:**
  - `airflow_provider_google_sheets/hooks/google_sheets.py` — метод `append_values` (строка ~191)
  - `airflow_provider_google_sheets/operators/write.py` — `_execute_merge` (строка ~513) и `_execute_append` (строка ~384)
- **Файлы тестов:**
  - `tests/test_hooks/test_google_sheets.py` — `test_append_values_calls_api`
  - `tests/test_operators/test_write.py` — `TestSmartMerge` (строка 341), `TestAppend` (строка 141)
- **Паттерн ensure_rows** уже существует в `_execute_overwrite` (строка 333)
- **Тест `test_uses_append_values_not_insert_or_append_dimension`** проверяет отсутствие
  `appendDimension` в `batch_update` — изменение `insertDataOption` в `values.append`
  этот тест не затрагивает (разные API-вызовы)

## Development Approach

- **testing approach**: Regular (сначала код, затем тесты)
- Выполнять каждую задачу полностью перед переходом к следующей
- Тесты обязательны для каждой задачи
- Все тесты должны проходить перед переходом к следующей задаче

## Progress Tracking

- отмечать выполненные пункты `[x]` сразу по завершении
- добавлять новые задачи с префиксом ➕
- отмечать блокеры с префиксом ⚠️

## Implementation Steps

### Task 1: Добавить `insertDataOption="INSERT_ROWS"` в `hook.append_values`

**Files:**
- Modify: `airflow_provider_google_sheets/hooks/google_sheets.py`
- Modify: `tests/test_hooks/test_google_sheets.py`

- [x] добавить `insertDataOption="INSERT_ROWS"` в вызов `.append(...)` Google API
- [x] обновить `test_append_values_calls_api` — добавить проверки аргументов вызова `.append()`;
  паттерн call-style (консистентен с остальными тестами хука):
  `append_mock = mock_service.spreadsheets().values().append`
  `assert append_mock.call_args.kwargs["insertDataOption"] == "INSERT_ROWS"`
  `assert append_mock.call_args.kwargs["valueInputOption"] == "USER_ENTERED"`
- [x] запустить тесты хука: `python -m pytest tests/test_hooks/test_google_sheets.py -v`

### Task 2: Финальная проверка и CHANGELOG

Код операторов не меняется — вся правка в хуке (Task 1).

- [x] запустить полный набор тестов: `python -m pytest tests/ -v`
- [x] убедиться что `test_uses_append_values_not_insert_or_append_dimension` проходит
  (тест проверяет `batch_update` — конфликта с `insertDataOption` нет)
- [x] дописать в существующий раздел `## v0.9.1` в `CHANGELOG.md` (не создавать новый):
  `**Fixed:** merge mode failed with HTTP 400 "exceeds grid limits" on historical runs where
  all incoming keys overlapped existing data — values.append now uses insertDataOption=INSERT_ROWS
  which guarantees grid extension after deleteDimension shrinks it to zero slack`
- [x] **Примечание:** юнит-тесты проверяют форвардинг параметра. Корректность расширения
  сетки Google Sheets проверяется ручным запуском DAG (Post-Completion)
- [x] переместить план: `mv docs/plans/20260604-fix-insert-rows-grid-extension.md docs/plans/completed/`

## Post-Completion

**Ручная проверка:**
- Перезапустить упавший DAG `osnova_dash_ch_to_gs`, задача `write_ovz_control_to_sheets`,
  run id `manual__2026-06-03T16:26:27.742330+00:00` (или новый ручной запуск с теми же параметрами)
- Убедиться что задача завершается успешно и данные записаны в лист `RAW_af`

**Деплой:**
- После merge обновить версию пакета и выкатить на airflow-инстанс `realcombi.mgcom.ru`
