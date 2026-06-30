# Fix style inheritance in merge + rename smart_merge → merge

## Overview

Two changes in one release:

1. **Style fix**: `smart_merge` inherits header row formatting on new rows because `appendDimension`
   copies the format of the adjacent row. Fix: replace `appendDimension` + `batch_update_values`
   with `values.append` (writes data + creates rows in one call) followed by `repeatCell` with
   empty `userEnteredFormat` to explicitly clear inherited formatting on the new rows.

2. **Rename**: `write_mode="smart_merge"` → `"merge"`. The old name stays as a silent alias
   (no deprecation warning, no breaking change). Internal method renamed
   `_execute_smart_merge` → `_execute_merge`.

## Context (from discovery)

- Files involved:
  - `airflow_provider_google_sheets/operators/write.py` — Step 6 of `_execute_smart_merge`, dispatch in `execute()`
  - `tests/test_operators/test_write.py` — ~28 occurrences of `write_mode="smart_merge"`, several tests checking `appendDimension`/`batch_update_values`
  - `CHANGELOG.md`
  - `readme.md`, `readme_ru.md`
- Related patterns: `hook.append_values(spreadsheet_id, range_, values)`, `hook.batch_update(spreadsheet_id, requests)`
- `insert_start` (0-based row index) is still needed for `repeatCell` range

## Development Approach

- **testing approach**: Regular (code first, then tests)
- Complete each task fully before moving to the next
- **CRITICAL: every task MUST include updated tests before proceeding**
- **CRITICAL: all tests must pass before starting next task**

## Testing Strategy

- Unit tests only (no e2e / UI tests in this project)
- Run: `.venv/bin/python -m pytest tests/ -q`

## Progress Tracking

- mark completed items with `[x]` immediately when done
- add newly discovered tasks with ➕ prefix
- document issues/blockers with ⚠️ prefix

## Implementation Steps

### Task 1: Fix style inheritance — values.append + repeatCell

**Files:**
- Modify: `airflow_provider_google_sheets/operators/write.py`
- Modify: `tests/test_operators/test_write.py`

- [ ] in `_execute_smart_merge` Step 6: remove `appendDimension` batch_update call
- [ ] remove the `batch_update_values` loop (row-by-row writes)
- [ ] add `values.append` batched loop — range hint: `f"{prefix}{table_start_col}{table_start_row}:{end_col}"`
- [ ] after all batches: add `repeatCell` via `_batched_batch_update` with:
  - `startRowIndex`: `insert_start`, `endRowIndex`: `insert_start + len(append_rows)`
  - `startColumnIndex`: `start_col_idx`, `endColumnIndex`: `start_col_idx + len(headers)`
  - `cell`: `{"userEnteredFormat": {}}`, `fields`: `"userEnteredFormat"`
- [ ] update `test_uses_append_dimension_and_batch_update_values_not_append_values` → `append_values` IS called, `batch_update_values` NOT called
- [ ] update `test_uses_append_dimension_not_insert_dimension` → `append_values` IS called, no `appendDimension`
- [ ] update `test_insert_position_after_remaining_rows` → check `append_values` called with 1 row
- [ ] update `test_empty_sheet_writes_headers` → check `append_values` called + `repeatCell` in batch_update
- [ ] update `test_empty_sheet_insert_position_with_table_start` → check `append_values` + `repeatCell`
- [ ] update `test_smart_merge_uses_batch_update_for_deletes_and_inserts` → check `append_values` + `repeatCell`
- [ ] add new test `test_clears_formatting_after_append` — checks `batch_update` called with `repeatCell` request targeting only our columns
- [ ] run tests — must pass before task 2

### Task 2: Rename smart_merge → merge with alias

**Files:**
- Modify: `airflow_provider_google_sheets/operators/write.py`
- Modify: `tests/test_operators/test_write.py`

- [ ] in `execute()`: change `if self.write_mode == "smart_merge":` to accept both `"merge"` and `"smart_merge"`
- [ ] rename `_execute_smart_merge` → `_execute_merge`
- [ ] update docstring on class: `"smart_merge"` → `"merge"` (keep `smart_merge` mentioned as alias)
- [ ] update `write_mode` param docstring: same
- [ ] update `merge_key` param docstring reference
- [ ] update `return {"mode": "smart_merge", ...}` → `"merge"`
- [ ] update all `ValueError` messages inside `_execute_merge` that mention `smart_merge`
- [ ] in `tests/test_operators/test_write.py`: change `_make_op` helper and all fixtures from `write_mode="smart_merge"` → `write_mode="merge"`
- [ ] add test `test_smart_merge_alias_still_works` — `write_mode="smart_merge"` executes without error
- [ ] run tests — must pass before task 3

### Task 3: Verify acceptance criteria

- [ ] verify style inheritance is fixed (repeatCell targets only written columns)
- [ ] verify `write_mode="merge"` works, `write_mode="smart_merge"` still works as alias
- [ ] run full test suite: `.venv/bin/python -m pytest tests/ -q`
- [ ] confirm 0 failures

### Task 4: Update documentation

**Files:**
- Modify: `CHANGELOG.md`
- Modify: `readme.md`
- Modify: `readme_ru.md`

- [ ] add `v0.7.0` section to `CHANGELOG.md` with both changes (style fix + rename/alias)
- [ ] update `readme.md`: replace `smart_merge` with `merge` in mode list and examples (keep note about `smart_merge` alias)
- [ ] update `readme_ru.md`: same
- [ ] move this plan to `docs/plans/completed/`

## Technical Details

**repeatCell request structure:**
```python
{
    "repeatCell": {
        "range": {
            "sheetId": sheet_id,
            "startRowIndex": insert_start,
            "endRowIndex": insert_start + len(append_rows),
            "startColumnIndex": start_col_idx,
            "endColumnIndex": start_col_idx + len(headers),
        },
        "cell": {"userEnteredFormat": {}},
        "fields": "userEnteredFormat",
    }
}
```

**values.append range hint:**
```python
end_col = self._index_to_column_letter(start_col_idx + len(headers) - 1)
append_range = f"{prefix}{table_start_col}{table_start_row}:{end_col}"
```

**Dispatch alias:**
```python
if self.write_mode in ("merge", "smart_merge"):
    return self._execute_merge(hook, headers, rows)
```

**insert_start** stays — needed for `repeatCell` range address (0-based absolute row index).

## Post-Completion

**Manual verification:**
- Run a real smart_merge / merge on a sheet with a styled header row — confirm new rows have no inherited formatting
