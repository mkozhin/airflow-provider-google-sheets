# Add `table_start` Parameter to WriteOperator

## Overview

Добавить параметр `table_start: str = "A1"` в `GoogleSheetsWriteOperator`.

Задаёт верхний левый угол таблицы — ячейку, в которой находится заголовок. Используется в режимах `append` и `smart_merge` для:
- определения, пуста ли таблица (проверяем нужную строку, не хардкод row 1)
- записи заголовка в правильную ячейку при пустой таблице
- чтения ключевой колонки с правильной строки в `smart_merge`

Режим `overwrite` не затрагивается (там `cell_range` уже управляет всем).

Дефолт `"A1"` сохраняет текущее поведение — существующие DAG-и работают без изменений.

## Context (from discovery)

- **Файл**: `airflow_provider_google_sheets/operators/write.py`
  - `_execute_append` — header check hardcoded `{prefix}1:1`, header write hardcoded `{prefix}A1`
  - `_execute_smart_merge` — key column read from row 1, key col letter relative (not absolute), header write hardcoded `{prefix}A1`
  - `_parse_range_start(range_str)` — уже есть, возвращает `(col_letter, row_num)`
  - `_column_letter_to_index`, `_index_to_column_letter` — уже есть
- **Тесты**: `tests/test_operators/test_write.py`
  - `TestAppend`, `TestSmartMerge`, `TestSmartMergeHasHeaders`

## Development Approach

- **testing approach**: Regular (code first, then tests)
- complete each task fully before moving to the next
- backward compatibility critical: default `"A1"` must not break existing tests

## Implementation Steps

### Task 1: Добавить параметр `table_start` в оператор

**Files:**
- Modify: `airflow_provider_google_sheets/operators/write.py`

- [x] добавить `table_start: str = "A1"` в `__init__` и сохранить как `self.table_start`
- [x] добавить `"table_start"` в `template_fields`
- [x] написать тест: `table_start` присваивается корректно при создании оператора
- [x] запустить тесты — должны пройти

### Task 2: Применить `table_start` в `_execute_append`

**Files:**
- Modify: `airflow_provider_google_sheets/operators/write.py`

- [x] распарсить `table_start` → `(start_col, start_row)` в начале `_execute_append`
- [x] заменить хардкод `{prefix}1:1` на `{prefix}{start_col}{start_row}:{start_row}`
- [x] заменить хардкод `{prefix}A1` (header write) на `{prefix}{start_col}{start_row}`
- [x] заменить дефолт `{prefix}A1` в target на `{prefix}{start_col}{start_row}`
- [x] написать тест: `table_start="C3"` — заголовок пишется в C3, data appended к C3
- [x] написать тест: `table_start="A1"` (дефолт) — поведение не изменилось
- [x] запустить тесты — должны пройти

### Task 3: Применить `table_start` в `_execute_smart_merge`

**Files:**
- Modify: `airflow_provider_google_sheets/operators/write.py`

- [x] распарсить `table_start` в начале `_execute_smart_merge`
- [x] вычислить абсолютную букву ключевой колонки через `start_col_idx + key_col_idx`
- [x] изменить `key_range` чтобы читать с `start_row`
- [x] исправить `data_start_row_num`: `start_row + 1` (с заголовком) или `start_row` (без)
- [x] исправить header write range на `{prefix}{start_col}{start_row}`
- [x] исправить append target на `{prefix}{start_col}{start_row}`
- [x] написать тест: `table_start="C3"`, ключ — второй столбец → читается D3:D, заголовок в C3
- [x] написать тест: `table_start="A1"` (дефолт) — поведение идентично текущему
- [x] написать тест: непустая таблица с `table_start="B2"` — строки нумеруются правильно
- [x] запустить тесты — должны пройти

### Task 4: Финализация

**Files:**
- Modify: `CHANGELOG.md`

- [x] запустить полный тест-сьют: `.venv/bin/python -m pytest tests/ -q`
- [x] добавить секцию `v0.6.2` в `CHANGELOG.md`
- [x] переместить план в `docs/plans/completed/`

## Technical Details

**Парсинг `table_start`:**
```python
start_col, start_row = self._parse_range_start(self.table_start)
# "A1" → ("A", 1)
# "C3" → ("C", 3)
# "B2" → ("B", 2)
```

**Абсолютная колонка ключа (smart_merge):**
```python
start_col_idx = self._column_letter_to_index(start_col)  # "C" → 2
# key_col_idx — позиция ключа в заголовках (0-based)
abs_key_col = self._index_to_column_letter(start_col_idx + key_col_idx)
# table_start="C3", key — 2-й столбец (idx=1) → C(2) + 1 = D(3) → "D"
```

**Нумерация строк в индексе smart_merge:**
- `has_headers=True`: данные начинаются с `start_row + 1` (строка `start_row` — заголовок)
- `has_headers=False`: данные начинаются с `start_row`

## Post-Completion

При использовании `table_start` в DAG убедиться что:
- `table_start` указывает ячейку заголовка, а не первой строки данных
- `cell_range` (если задан) указывает тот же диапазон или его подмножество
