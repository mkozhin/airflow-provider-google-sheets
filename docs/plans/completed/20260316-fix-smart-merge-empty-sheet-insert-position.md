# Fix: два бага с записью заголовков

## Overview

**Баг 1 (smart_merge):** при первом запуске `smart_merge` на пустом листе с `write_headers=True` данные оказываются **перед** заголовком.

**Баг 2 (append):** проверка пустоты листа читала всю строку вместо одной ячейки, что мешало записи заголовка если правее `table_start` были посторонние данные.

## Implementation Steps

### Task 1: Исправить insert_start в `_execute_smart_merge`

- [x] добавить флаг `headers_just_written = False`
- [x] установить `headers_just_written = True` после записи заголовка
- [x] `total_existing = len(existing_keys_raw) + (1 if headers_just_written else 0)`
- [x] написать тест: insertDimension.startIndex = 1 на пустом листе (A1)
- [x] написать тест: table_start="A3" → insertDimension.startIndex = 3
- [x] обновить `test_empty_sheet_writes_headers` — добавить проверку позиции вставки
- [x] запустить тесты — прошли

### Task 2: Исправить проверку пустоты в `_execute_append`

- [x] изменить диапазон с `{start_col}{start_row}:{start_row}` на `{start_col}{start_row}`
- [x] написать тест: C3 пуст, D3 имеет данные → заголовок всё равно записывается
- [x] существующий `test_append_non_empty_sheet_no_headers` проходит
- [x] запустить тесты — прошли

### Task 3: Финализация

- [x] запустить полный тест-сьют: 350 passed
- [x] обновить CHANGELOG v0.6.1
- [x] переместить план в `docs/plans/completed/`
