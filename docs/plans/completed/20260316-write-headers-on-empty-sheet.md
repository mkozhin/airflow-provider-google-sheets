# Write Headers on Empty Sheet for append/smart_merge

## Overview

Исправить баг: при первой записи в пустую таблицу в режимах `append` и `smart_merge` заголовки не записываются — данные начинаются с первой строки без заголовка.

**Поведение после фикса:** если таблица пуста и `write_headers=True`, перед данными записывается строка с заголовками.

## Context

- **Файл**: `airflow_provider_google_sheets/operators/write.py`
  - `_execute_append` — не пишет заголовки вообще
  - `_execute_smart_merge` — не пишет заголовки, но уже читает ключевую колонку (можно определить пустоту без доп. запроса)
- **Тесты**: `tests/test_operators/test_write.py`
- `write_headers: bool = True` — параметр уже существует, используется только в `overwrite`

## Technical Details

**`smart_merge`:** уже читает ключевую колонку `existing_keys_raw`. Если `existing_keys_raw == []` — таблица пустая → записать заголовок через `update_values` перед append.

**`append`:** нет чтения таблицы. Добавляем один лёгкий запрос: читаем первую строку (диапазон `{prefix}1:1`). Если пустая и `write_headers=True` → записать заголовок, затем append данных.

Оба варианта используют `hook.update_values` для записи заголовка в строку 1 (не `append_values`, чтобы точно попасть в строку 1).

## Implementation Steps

### Task 1: Фикс `_execute_smart_merge`

**Files:**
- Modify: `airflow_provider_google_sheets/operators/write.py`

- [x] после получения `existing_keys_raw`, добавить проверку: если `existing_keys_raw == []` и `self.write_headers` и `headers` → записать заголовок в строку 1 через `hook.update_values`
- [x] написать тест: пустая таблица + smart_merge → первая строка заголовок, данные со второй
- [x] написать тест: непустая таблица + smart_merge → заголовок не добавляется повторно
- [x] запустить тесты — должны пройти

### Task 2: Фикс `_execute_append`

**Files:**
- Modify: `airflow_provider_google_sheets/operators/write.py`

- [x] в начале `_execute_append` читать первую строку: `hook.get_values(self.spreadsheet_id, f"{prefix}1:1")`
- [x] если пустая и `self.write_headers` и `headers` → записать заголовок через `hook.update_values` в строку 1
- [x] написать тест: пустая таблица + append → первая строка заголовок, данные со второй
- [x] написать тест: непустая таблица + append → заголовок не добавляется, данные после последней строки
- [x] написать тест: `write_headers=False` + пустая таблица → заголовок не пишется
- [x] запустить тесты — должны пройти

### Task 3: Финализация

- [x] запустить полный тест-сьют: `.venv/bin/python -m pytest tests/ -q`
- [x] добавить секцию `v0.6.1` в `CHANGELOG.md`


## Post-Completion

Пользователю: при первом запуске DAG с `write_mode="smart_merge"` или `"append"` заголовки теперь запишутся автоматически. Параметр `write_headers=True` (по умолчанию) управляет этим поведением.
