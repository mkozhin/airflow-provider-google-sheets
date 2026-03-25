# Write Operator: column_mapping Parameter

## Overview

Add `column_mapping: dict[str, str] | None` parameter to `GoogleSheetsWriteOperator` that renames column headers before writing to Google Sheets.

- **Problem:** Input data uses internal key names (e.g. `"date_period"`, `"revenue_rub"`) but the sheet must have human-readable headers (e.g. `"Период"`, `"Выручка, руб."`).
- **Key benefits:** No need to pre-process data upstream; operator handles renaming transparently for all write modes and data sources.
- **How it integrates:** Applied in `execute()` **last** — after `_apply_partition()` and `_format_rows()`, just before dispatching to the mode method. Headers not in the mapping are kept as-is. This means all other parameters (`merge_key`, `partition_by`, `schema`) always reference **original** column names from the input data.

## Context (from discovery)

- Files involved:
  - `airflow_provider_google_sheets/operators/write.py` — main operator
  - `tests/test_operators/test_write.py` — tests
  - `readme.md` / `readme_ru.md` — documentation
  - `CHANGELOG.md`
- Related patterns: `GoogleSheetsReadOperator.column_mapping` (`read.py` lines ~90, 112, 233-237) — same `[mapping.get(h, h) for h in headers]` pattern
- Dependencies: none (pure in-operator transformation)

## Development Approach

- **Testing approach:** Regular (code first, then tests)
- Complete each task fully before moving to the next
- Every task must include new/updated tests
- All tests must pass before starting next task

## Testing Strategy

- Unit tests in `test_write.py`: new `TestColumnMapping` class
- Cover: basic rename, partial mapping (unmapped columns kept), all three write modes, dict data, file data, no-op when mapping is None

## Progress Tracking

- Mark completed items with `[x]` immediately when done
- Add newly discovered tasks with ➕ prefix
- Document issues/blockers with ⚠️ prefix

## What Goes Where

- **Implementation Steps:** code changes and tests in this repo
- **Post-Completion:** no external actions needed

## Implementation Steps

### Task 1: Add column_mapping to GoogleSheetsWriteOperator

**Files:**
- Modify: `airflow_provider_google_sheets/operators/write.py`

- [ ] Add `column_mapping: dict[str, str] | None = None` to `__init__` signature and store as `self.column_mapping`
- [ ] Add `"column_mapping"` to `template_fields` (consistent with read operator)
- [ ] Add docstring entry for `column_mapping` — указать, что `merge_key`, `partition_by`, `schema` используют **оригинальные** имена колонок
- [ ] Add `_apply_column_mapping(headers)` method: `return [self.column_mapping.get(h, h) for h in headers] if self.column_mapping and headers else headers`
- [ ] Apply mapping in `execute()` **after** `_format_rows()` and `_apply_partition()`, but **before** dispatching to mode methods. Передавать mapped headers в методы режимов. `_execute_merge` должен валидировать `merge_key` против **original** headers ДО маппинга — для этого сохранить `original_headers` перед вызовом `_apply_column_mapping()` и использовать их внутри `_execute_merge` для поиска `key_col_idx`
- [ ] В `_execute_merge`: принять дополнительный параметр `original_headers` (или разрешать `merge_key` до передачи) — чтобы валидация и поиск индекса шли по оригинальным именам, а запись заголовка в таблицу — по mapped
- [ ] Write basic smoke test (overwrite mode): `test_full_mapping` — verify headers written to sheet are mapped names
- [ ] Run tests — must pass before Task 2

### Task 2: Tests for column_mapping

**Files:**
- Modify: `tests/test_operators/test_write.py`

- [ ] Add class `TestColumnMapping`
- [ ] `test_full_mapping` — all columns renamed, sheet receives mapped headers (overwrite, dict data)
- [ ] `test_partial_mapping` — only some columns renamed, unmapped kept as-is
- [ ] `test_mapping_none` — no mapping, headers unchanged (no regression)
- [ ] `test_mapping_with_append_mode` — mapping works in append mode
- [ ] `test_mapping_with_merge_mode` — `merge_key="original_col"`, `column_mapping={"original_col": "Renamed"}`, headers written as "Renamed", merge still works (key_col_idx resolved from original names)
- [ ] `test_mapping_with_partition_by` — `partition_by` uses original column name when mapping is active
- [ ] `test_mapping_with_file_input` — mapping works when data is a .jsonl file path
- [ ] Run full test suite — must pass before Task 3

### Task 3: Verify acceptance criteria

- [ ] column_mapping works for overwrite, append, merge modes
- [ ] unmapped columns are kept as-is
- [ ] merge_key references **original** column name from input data (not raises ValueError after mapping)
- [ ] partition_by references **original** column name from input data
- [ ] only the header row written to the sheet uses mapped names
- [ ] Run full test suite: `pytest tests/`

### Task 4: [Final] Update documentation

**Files:**
- Modify: `readme.md`
- Modify: `readme_ru.md`
- Modify: `CHANGELOG.md`

- [ ] Add `column_mapping` to write operator parameter table in `readme.md`
- [ ] Add `column_mapping` to write operator parameter table in `readme_ru.md`
- [ ] Add v0.8.1 (or next) entry to `CHANGELOG.md`
- [ ] Move this plan to `docs/plans/completed/`

## Technical Details

**Mapping application:**
```python
def _apply_column_mapping(self, headers: list[str] | None) -> list[str] | None:
    if not self.column_mapping or not headers:
        return headers
    return [self.column_mapping.get(h, h) for h in headers]
```

**Call site in `execute()`:**
```python
headers, rows = self._resolve_data(context)
rows = self._format_rows(headers, rows)          # schema keys = original names
rows = self._apply_partition(headers, rows)      # partition_by = original name
headers = self._apply_column_mapping(headers)    # ← new, applied LAST
# dispatch to _execute_overwrite / _execute_append / _execute_merge
# all use already-mapped headers for writing to sheet
```

**Merge mode specifics (critical):**
- `_execute_merge` validates `self.merge_key not in headers` and resolves `key_col_idx = headers.index(self.merge_key)` — эта логика должна работать с **original** headers
- Решение: в `execute()` сохранить `original_headers` до маппинга, передать их в `_execute_merge` как отдельный аргумент (или вычислить `key_col_idx` в `execute()` до маппинга и передать)
- Headers written to sheet (if empty) use **mapped** names
- Incoming rows matched against existing sheet rows by column **position** — position unchanged by mapping

**Interaction with other parameters:**
- `schema` keys → **original** column names from input data
- `partition_by` → **original** column name from input data (consistent with `GoogleSheetsExtractPartitionsOperator`)
- `merge_key` → **original** column name from input data

## Post-Completion

No external actions required.
