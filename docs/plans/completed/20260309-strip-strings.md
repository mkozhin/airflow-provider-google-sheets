# Add strip_strings Parameter for Text Trimming

## Overview

Add a `strip_strings` parameter to `GoogleSheetsReadOperator` that, when `True`, strips leading and trailing whitespace from all string cell values during schema application. Default is `False` (no change to existing behaviour — load as-is).

**Problem solved:** Leading/trailing spaces in Google Sheets cells cause downstream filtering and deduplication issues. In particular, values like `" bonus"` instead of `"bonus"` make rows "disappear" when filtering JSON output by exact string match.

**Integration:** The trimming is applied inside `apply_schema_to_value` for columns with `type=str` (both with and without schema). The parameter flows from operator → `_read_chunks` → `apply_schema_to_row` → `apply_schema_to_value`.

## Context (from discovery)

- **Files involved:**
  - `airflow_provider_google_sheets/utils/schema.py` — `apply_schema_to_value`, `apply_schema_to_row`
  - `airflow_provider_google_sheets/operators/read.py` — `GoogleSheetsReadOperator`, `_read_chunks`
  - `tests/test_utils/test_schema.py` — unit tests for schema
  - `tests/test_operators/test_read.py` — integration tests for operator
- **Related patterns:** Same pattern as existing bool params (`transliterate_headers`, `sanitize_headers`, `lowercase_headers`, `normalize_headers`)
- **Dependencies:** No new dependencies

## Development Approach

- **Testing approach:** Regular (code first, then tests)
- Complete each task fully before moving to the next
- Make small, focused changes
- **CRITICAL: every task MUST include new/updated tests**
- **CRITICAL: all tests must pass before starting next task**
- Run tests: `python3 -m pytest tests/`

## Testing Strategy

- **Unit tests:** `test_utils/test_schema.py` — `apply_schema_to_value` and `apply_schema_to_row` with `strip_strings=True/False`
- **Integration tests:** `test_operators/test_read.py` — operator parameter propagation and end-to-end trimming behaviour

## Progress Tracking

- mark completed items with `[x]` immediately when done
- add newly discovered tasks with ➕ prefix
- document issues/blockers with ⚠️ prefix

## What Goes Where

- **Implementation Steps** (`[ ]` checkboxes): code changes in this codebase
- **Post-Completion** (no checkboxes): external/manual actions

## Implementation Steps

### Task 1: Add strip_strings to apply_schema_to_value and apply_schema_to_row

**Files:**
- Modify: `airflow_provider_google_sheets/utils/schema.py`

- [ ] Add `strip_strings: bool = False` parameter to `apply_schema_to_value`
- [ ] In the `str` type branch, apply `.strip()` to the result when `strip_strings=True`
- [ ] Add `strip_strings: bool = False` parameter to `apply_schema_to_row`
- [ ] Pass `strip_strings` through to each `apply_schema_to_value` call inside `apply_schema_to_row`
- [ ] Run tests — must pass before task 2: `python3 -m pytest tests/test_utils/test_schema.py`

### Task 2: Wire strip_strings into GoogleSheetsReadOperator

**Files:**
- Modify: `airflow_provider_google_sheets/operators/read.py`

- [ ] Add `strip_strings: bool = False` to `__init__` signature (after `schema` param, before `chunk_size`)
- [ ] Store as `self.strip_strings`
- [ ] Pass `strip_strings=self.strip_strings` to `apply_schema_to_row` call inside `_read_chunks`
- [ ] Add `strip_strings` to docstring parameter list
- [ ] Run tests — must pass before task 3: `python3 -m pytest tests/`

### Task 3: Write tests

**Files:**
- Modify: `tests/test_utils/test_schema.py`
- Modify: `tests/test_operators/test_read.py`

**`test_schema.py` — new test class `TestStripStrings`:**
- [ ] Test `apply_schema_to_value` with `type=str`, `strip_strings=True` — leading space trimmed
- [ ] Test `apply_schema_to_value` with `type=str`, `strip_strings=True` — trailing space trimmed
- [ ] Test `apply_schema_to_value` with `type=str`, `strip_strings=False` — spaces preserved (default)
- [ ] Test `apply_schema_to_value` with `type=str`, `strip_strings=True` — value is already clean (no change)
- [ ] Test `apply_schema_to_value` with `type=int`, `strip_strings=True` — numeric types unaffected
- [ ] Test `apply_schema_to_row` with `strip_strings=True` — str columns trimmed, numeric columns untouched

**`test_read.py` — new tests in `TestSchemaApplication`:**
- [ ] Test operator with `strip_strings=True` — leading/trailing spaces stripped from str cells in JSONL output
- [ ] Test operator with `strip_strings=False` (default) — spaces preserved in output
- [ ] Run full test suite: `python3 -m pytest tests/`

### Task 4: Verify acceptance criteria

- [ ] `strip_strings=False` (default) — existing behaviour unchanged, all pre-existing tests pass
- [ ] `strip_strings=True` — `" bonus"` becomes `"bonus"`, `"Дмитров Дом "` becomes `"Дмитров Дом"`
- [ ] `strip_strings=True` — non-str schema types (int, float, date) are not affected
- [ ] Run full test suite: `python3 -m pytest tests/`

### Task 5: [Final] Update documentation

**Files:**
- Modify: `README.md`

- [ ] Add `strip_strings` to the operator parameters table/list in README
- [ ] Move this plan to `docs/plans/completed/`

## Technical Details

**`apply_schema_to_value` change (str path):**
```python
if col_type == "str":
    result = str(value)
    return result.strip() if strip_strings else result
```

**`apply_schema_to_row` signature:**
```python
def apply_schema_to_row(
    row: list[Any],
    headers: list[str],
    schema: dict[str, dict],
    strip_strings: bool = False,
) -> list[Any]:
```

**`_read_chunks` call:**
```python
rows = [apply_schema_to_row(row, headers, self.schema, strip_strings=self.strip_strings) for row in rows]
```

## Post-Completion

**DAG usage example:**
```python
read_to_csv = GoogleSheetsReadOperator(
    ...
    schema=SHEETS_SCHEMA,
    strip_strings=True,   # trim leading/trailing spaces in all str cells
)
```
