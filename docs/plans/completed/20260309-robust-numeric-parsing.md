# Robust Numeric Parsing for Schema-Based Type Conversion

## Overview

When reading Google Sheets with a schema, numeric columns (`int`, `float`) currently raise
`GoogleSheetsDataError` for values like `"-"`, `"n/a"`, `""`, `"1,2"`, `"1000.4 р."`, `"10.2%"`.

This plan adds robust numeric parsing controlled by an optional `default` field in the column
schema. When `default` is present for a numeric column, the operator applies lenient parsing:
- Comma as decimal separator: `"1,2"` → `1.2`
- Prefix/suffix stripping: `"1000.4 р."` → `1000.4`, `"10.2%"` → `10.2`
- Non-numeric garbage (`"-"`, `"n/a"`, `""`, `" "`) → `default` value (`None` or `0`)

Without `default`, existing strict behavior is preserved (raises error on bad values).

**BigQuery compatibility:** Python `None` serializes as `null` in JSONL/JSON output, which
BigQuery correctly reads as `NULL` for nullable numeric columns.

## Context (from discovery)

- Files involved: `airflow_provider_google_sheets/utils/schema.py`, `tests/test_utils/test_schema.py`
- Related patterns: column_schema dict with `type`, `format`, `required` fields
- Dependencies: no new external libraries needed (uses `re` stdlib)

## Development Approach

- **Testing approach**: Regular (code first, then tests)
- Complete each task fully before moving to the next
- All tests must pass before starting next task

## Progress Tracking

- Mark completed items with `[x]` immediately when done
- Add newly discovered tasks with ➕ prefix
- Document issues/blockers with ⚠️ prefix

## Implementation Steps

### Task 1: Add `_clean_numeric_string` helper in schema.py

**Files:**
- Modify: `airflow_provider_google_sheets/utils/schema.py`

- [x] Add `import re` at the top of schema.py
- [x] Implement `_clean_numeric_string(value: str) -> str | None`:
  - Replace `,` with `.` (comma decimal separator)
  - Use regex `r"-?\d+\.?\d*"` to extract first numeric substring
  - Return matched string or `None` if no numeric part found
- [x] Add `_NUMERIC_TYPES = {"int", "float"}` constant

> **Note on limitations:** `"1.000,4"` (European thousands+decimal) is not handled in this
> iteration — the regex extracts `"1.000"` which parses as `1.0`. Can be addressed later if needed.

### Task 2: Update `apply_schema_to_value` for lenient numeric mode

**Files:**
- Modify: `airflow_provider_google_sheets/utils/schema.py`

- [x] Before the existing `if value is None or ...` check, detect `has_default = "default" in column_schema`
- [x] When `col_type in _NUMERIC_TYPES` and `has_default`:
  - If value is `None` or empty/whitespace string → return `column_schema["default"]`
  - If value is already `int` or `float` → convert normally (int→int, float→float)
  - If value is a string → call `_clean_numeric_string(value.strip())`
    - If result is `None` → return `column_schema["default"]`
    - Otherwise replace `value` with cleaned string and proceed to normal `int(float(...))` / `float(...)` conversion
    - On `ValueError`/`TypeError` → return `column_schema["default"]`
- [x] When `col_type in _NUMERIC_TYPES` and `not has_default` → existing behavior unchanged
- [x] Verify existing non-numeric types (str, date, datetime, bool) are unaffected

### Task 3: Write tests for new behavior

**Files:**
- Modify: `tests/test_utils/test_schema.py`

**Lenient mode — null default (default=None):**
- [x] `"n/a"` → `None`
- [x] `"-"` → `None`
- [x] `""` (empty string) → `None`
- [x] `"  "` (whitespace) → `None`
- [x] `None` → `None`

**Lenient mode — zero default (default=0 for int, 0.0 for float):**
- [x] `"n/a"` with `default=0` → `0`
- [x] `"-"` with `default=0.0` → `0.0`

**Comma decimal separator:**
- [x] `"1,2"` with `type=float, default=None` → `pytest.approx(1.2)`
- [x] `"42,0"` with `type=int, default=None` → `42`

**Prefix/suffix stripping:**
- [x] `"1000.4 р."` with `type=float, default=None` → `pytest.approx(1000.4)`
- [x] `"10.2%"` with `type=float, default=None` → `pytest.approx(10.2)`
- [x] `"$ 55"` with `type=float, default=None` → `pytest.approx(55.0)`

**Normal values still work in lenient mode:**
- [x] `"3.14"` with `type=float, default=None` → `pytest.approx(3.14)`
- [x] `"42"` with `type=int, default=None` → `42`

**Strict mode unchanged (no default):**
- [x] `"n/a"` with `type=float` (no default) → raises `GoogleSheetsDataError`
- [x] `"1,2"` with `type=float` (no default) → raises `GoogleSheetsDataError`

- [x] Run tests: `python -m pytest tests/test_utils/test_schema.py -v` — must pass

### Task 4: Update docstrings

**Files:**
- Modify: `airflow_provider_google_sheets/utils/schema.py`

- [x] Update `apply_schema_to_value` docstring: document `default` field behavior
- [x] Document that `default` enables: comma decimal, prefix/suffix stripping, fallback on non-numeric
- [x] Update `validate_schema` docstring if needed (no structural change required)

### Task 5: Update README

**Files:**
- Modify: `README.md`
- Modify: `readme_ru.md`

- [x] Add section or example under schema usage showing `default` for numeric columns
- [x] Include examples: `"default": None` for BigQuery nullable, `"default": 0` for zero-fill
- [x] Show example with `"1000.4 р."` being parsed correctly

### Task 6: Verify acceptance criteria

- [x] Run full test suite: `python -m pytest tests/ -v` — 280 passed
- [x] Verify: comma decimal separator works for both `int` and `float`
- [x] Verify: prefix/suffix stripped correctly
- [x] Verify: non-numeric → `None` and non-numeric → `0` both work
- [x] Verify: strict mode (no `default`) behavior unchanged
- [ ] Verify: `None` from Python serializes as `null` in JSONL output (manual check or test)

### Task 7: [Final] Housekeeping

- [ ] Move this plan to `docs/plans/completed/`

## Technical Details

### Schema format (before/after)

```python
# Before (strict — raises error on "n/a", "1,2", "10.2%")
schema = {
    "price": {"type": "float"},
    "count": {"type": "int"},
}

# After (lenient — returns None or 0 on bad values)
schema = {
    "price": {"type": "float", "default": None},   # "n/a" → None (BigQuery NULL)
    "count": {"type": "int",   "default": 0},       # "n/a" → 0
    "name":  {"type": "str"},                        # unchanged
}
```

### `_clean_numeric_string` logic

```
input: "1000.4 р."
  step 1: replace "," → "."  →  "1000.4 р."   (no change)
  step 2: regex search r"-?\d+\.?\d*"  →  match "1000.4"
  return: "1000.4"

input: "1,2"
  step 1: replace "," → "."  →  "1.2"
  step 2: regex search  →  match "1.2"
  return: "1.2"

input: "n/a"
  step 1: "n/a"  (no change)
  step 2: regex search  →  no match
  return: None
```

### Parsing flow in `apply_schema_to_value` (numeric + has_default)

```
value → None or empty?  →  return default
value → already int/float?  →  convert directly
value → string?
  cleaned = _clean_numeric_string(value.strip())
  cleaned is None?  →  return default
  try int(float(cleaned)) or float(cleaned)
  except ValueError/TypeError  →  return default
```

## Post-Completion

**Manual verification:**
- Test with a real Google Sheets file containing mixed numeric/text values
- Verify JSONL output with `null` values loads into BigQuery without errors

**Version bump:**
- This is a backward-compatible addition (new opt-in field) → patch or minor version bump (v0.2.1)
