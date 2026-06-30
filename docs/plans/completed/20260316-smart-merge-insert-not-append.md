# Smart Merge: Replace append_values with insertDimension + write

## Overview

**Проблема:** при `smart_merge`, если удаляются все строки данных и в листе остаётся только заголовок, последующий `append_values` добавляет новые строки с форматированием заголовка (жирный шрифт, цвет фона и т.д.) — потому что `append` наследует стиль от последней строки.

**Решение:** заменить `append_values` на `insertDimension(inheritFromBefore=False)` + `batch_update_values`. При вставке в конец листа `inheritFromBefore=False` означает "наследовать от строки ниже" — которой нет → чистое форматирование.

## Context

- **Файл**: `airflow_provider_google_sheets/operators/write.py` — метод `_execute_smart_merge`
- **Тесты**: `tests/test_operators/test_write.py` — класс `TestSmartMerge`
- `_batched_batch_update` и `batch_update_values` в хуке уже есть — новых методов не нужно
- `_get_sheet_id` уже вызывается в `_execute_smart_merge` — `sheet_id` доступен

## Implementation Steps

### Task 1: Заменить append_values на insertDimension + write в `_execute_smart_merge`

**Files:**
- Modify: `airflow_provider_google_sheets/operators/write.py`

- [x] вычислить `total_existing = len(existing_keys_raw)` перед операциями
- [x] вычислить `rows_after_deletion` после формирования `delete_ops`
- [x] если `append_rows` не пуст: отправить `insertDimension` через `_batched_batch_update` с `inheritFromBefore=False`
- [x] отправить `batch_update_values` с данными для вставленных строк (батчами по `batch_size`)
- [x] удалить вызов `hook.append_values` из `_execute_smart_merge`
- [x] обновить счётчик `stats["appended"]`

### Task 2: Обновить тесты

**Files:**
- Modify: `tests/test_operators/test_write.py`

- [x] обновить тесты в `TestSmartMerge` которые проверяли `mock_hook.append_values` — теперь проверяют `mock_hook.batch_update` (insertDimension) и `mock_hook.batch_update_values`
- [x] добавить тест: после удаления всех строк вставка использует `inheritFromBefore=False`
- [x] добавить тест: вставка происходит в правильную позицию (после оставшихся строк)
- [x] добавить тест: батчинг append_rows по `batch_size` через `batch_update_values`
- [x] запустить все тесты — должны пройти

### Task 3: Финализация

- [x] запустить полный тест-сьют: `.venv/bin/python -m pytest tests/ -q`
- [x] добавить секцию `v0.6.3` в `CHANGELOG.md`
- [x] переместить план в `docs/plans/completed/`
