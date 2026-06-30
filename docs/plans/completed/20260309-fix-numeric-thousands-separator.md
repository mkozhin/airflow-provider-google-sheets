# Fix: Numeric Parsing Drops Digits After Space Thousands Separator

## Overview

`_clean_numeric_string` in `schema.py` uses the regex `r"-?\d+\.?\d*"` which stops at the
first whitespace character. As a result, numbers formatted with a space as a thousands separator
(common in Russian locale) are silently truncated:

- `"р.250 000"` → `250.0` instead of `250000.0`
- `"р.2 722"` → `2.0` instead of `2722.0`
- `"1 946"` → `1.0` instead of `1946.0`

**Fix:** strip all space-like characters (visual thousands separators) from the string
**before** running the regex. Common variants from Excel / Google Sheets / Russian locale:

| Character | Code | Name |
|---|---|---|
| ` ` | U+0020 | Regular space |
| `\xa0` | U+00A0 | Non-breaking space |
| `\u202F` | U+202F | Narrow no-break space (русская локаль) |
| `\u2009` | U+2009 | Thin space |

Cleanest approach: `re.sub(r'[\s\u202F\u2009]', '', value)` — strips all Unicode whitespace
including the exotic variants.

This is a pure bug fix — no API changes, fully backward-compatible.

## Context (from discovery)

- **Root cause:** `airflow_provider_google_sheets/utils/schema.py` — `_clean_numeric_string`
- **Only lenient mode is affected** — strict numeric mode doesn't go through `_clean_numeric_string`
- **Tests:** `tests/test_utils/test_schema.py` → `TestLenientNumericParsing`
- **No operator changes needed** — fix is entirely in one utility function

## Development Approach

- **Testing approach:** Regular (code first, then tests)
- Make the minimal change — one line in `_clean_numeric_string`
- Verify no existing tests break

## Implementation Steps

### Task 1: Fix _clean_numeric_string to strip whitespace

**Files:**
- Modify: `airflow_provider_google_sheets/utils/schema.py`

- [ ] In `_clean_numeric_string`, after `value.replace(",", ".")`, add removal of all space-like chars:
  `value = re.sub(r'[\s\u202F\u2009]', '', value)`
- [ ] Verify "р.250 000" → 250000.0, "р.2 722" → 2722.0, "1 946" → 1946.0
- [ ] Run existing tests — must all pass: `.venv/bin/pytest tests/test_utils/test_schema.py`

### Task 2: Write tests

**Files:**
- Modify: `tests/test_utils/test_schema.py`

- [ ] Add test: `"р.250 000"` with `{"type": "float", "default": 0}` → `250000.0`
- [ ] Add test: `"р.2 722"` with `{"type": "float", "default": 0}` → `2722.0`
- [ ] Add test: `"1 946"` with `{"type": "int", "default": 0}` → `1946`
- [ ] Add test: `"1 234 567"` (multiple separators) → `1234567`
- [ ] Add test: `"1\xa0000"` (non-breaking space U+00A0) → `1000.0`
- [ ] Add test: `"1\u202F000"` (narrow no-break space U+202F) → `1000.0`
- [ ] Add test: `"1\u2009000"` (thin space U+2009) → `1000.0`
- [ ] Run full test suite: `.venv/bin/pytest tests/`

### Task 3: Verify acceptance criteria

- [ ] All numbers with space thousands separators parse correctly
- [ ] Existing behaviour for other formats unchanged (`"10.2%"`, `"-"`, `""`, `"1,2"`)
- [ ] Run full test suite: `.venv/bin/pytest tests/`

### Task 4: [Final] Update CHANGELOG and bump version

**Files:**
- Modify: `CHANGELOG.md`

- [ ] Add entry under new version (v0.4.1) describing the fix
- [ ] Move this plan to `docs/plans/completed/`

## Technical Details

**Current code:**
```python
def _clean_numeric_string(value: str) -> str | None:
    value = value.replace(",", ".")
    match = _NUMERIC_RE.search(value)
    return match.group(0) if match else None
```

**Fixed code:**
```python
def _clean_numeric_string(value: str) -> str | None:
    value = value.replace(",", ".")
    value = re.sub(r'[\s\u202F\u2009]', '', value)  # strip thousands-separator spaces (all variants)
    match = _NUMERIC_RE.search(value)
    return match.group(0) if match else None
```

**Why this is safe:**
- Space-like chars inside number strings are always thousands separators (no numeric meaning)
- `\s` covers U+0020 and U+00A0; explicit `\u202F\u2009` covers narrow no-break and thin space
- Stripping before regex doesn't affect prefix/suffix stripping (`"р.250000"` → regex finds `"250000"`)
- Does not affect comma→period replacement (comma replaced first, then spaces removed)
- `re` module already imported in `schema.py`

## Post-Completion

No external changes needed. The fix is backward-compatible — previously incorrect values
will now parse correctly, which is the intended behaviour.
