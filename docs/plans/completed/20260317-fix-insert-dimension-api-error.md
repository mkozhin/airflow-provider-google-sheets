# Fix: replace insertDimension with appendDimension in smart_merge

## Overview

**Ошибка API:**
```
HttpError 400: insertDimension: range.startIndex must be less than the grid size (1)
if inheritFromBefore is false.
```

**Причина:** `insertDimension(inheritFromBefore=False)` требует, чтобы `startIndex < gridSize` — т.е. должна существовать строка ниже точки вставки. Но мы всегда вставляем в конец листа (`startIndex == gridSize`), поэтому запрос всегда невалиден в реальном API (тесты с моком этого не замечали).

**Решение:** заменить `insertDimension` на `appendDimension`. Этот запрос создан именно для добавления строк в конец листа и даёт строки с чистым (дефолтным) форматированием — что нам и нужно.

**Файл:** `airflow_provider_google_sheets/operators/write.py`, метод `_execute_smart_merge`

## Technical Details

**Было:**
```python
self._batched_batch_update(hook, [{
    "insertDimension": {
        "range": {
            "sheetId": sheet_id,
            "dimension": "ROWS",
            "startIndex": insert_start,
            "endIndex": insert_end,
        },
        "inheritFromBefore": False,
    }
}])
```

**Стало:**
```python
self._batched_batch_update(hook, [{
    "appendDimension": {
        "sheetId": sheet_id,
        "dimension": "ROWS",
        "length": len(append_rows),
    }
}])
```

Переменная `insert_start` остаётся — она нужна для вычисления номеров строк в `batch_update_values`.
`insert_end` больше не нужна — удалить.

## Implementation Steps

### Task 1: Заменить insertDimension на appendDimension

**Files:**
- Modify: `airflow_provider_google_sheets/operators/write.py`
- Modify: `tests/test_operators/test_write.py`

- [ ] заменить `insertDimension` запрос на `appendDimension` в `_execute_smart_merge`
- [ ] удалить `insert_end` (больше не нужна)
- [ ] обновить комментарий к Step 6
- [ ] обновить тест `test_inherit_from_before_false` → теперь проверять `appendDimension` в batch_update
- [ ] обновить тест `test_insert_position_after_remaining_rows` → проверять `appendDimension.length`
- [ ] обновить тесты `test_empty_sheet_writes_headers` и `test_empty_sheet_insert_position_with_table_start` → искать `appendDimension` вместо `insertDimension`
- [ ] обновить `test_smart_merge_uses_batch_update_for_deletes_and_inserts` → проверять `appendDimension`
- [ ] запустить тесты — должны пройти

### Task 2: Финализация

- [ ] запустить полный тест-сьют: `.venv/bin/python -m pytest tests/ -q`
- [ ] добавить секцию `v0.6.2` в `CHANGELOG.md`
- [ ] переместить план в `docs/plans/completed/`
