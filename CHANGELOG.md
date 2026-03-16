# Changelog

## v0.5.1

- **Fixed:** `row_stop` / `row_skip` conditions incorrectly matched every row when the specified column did not exist in the sheet headers. Missing columns are now silently ignored — the condition is skipped, and all rows are returned as expected

## v0.5.0

- Added `GoogleSheetsListSheetsOperator` — returns a `list[str]` of sheet (tab) names from a spreadsheet, with optional filtering by `name_pattern` (regex include), `exclude_pattern` (regex exclude), and `index_range` (positional slice). Compatible with Airflow dynamic task mapping (`expand(sheet_name=op.output)`)
- Added `row_skip` parameter to `GoogleSheetsReadOperator` — skip (filter out) rows matching condition(s) during reading. Accepts a single `dict` or `list[dict]` with `column`, `value`, and optional `op` (supports: `equals`, `not_equals`, `contains`, `not_contains`, `starts_with`, `ends_with`, `empty`, `not_empty`). Multiple conditions use OR logic
- Added `row_stop` parameter to `GoogleSheetsReadOperator` — stop reading when a matching row is found (the matching row and all subsequent rows are discarded). No further API calls are made after the stop condition triggers, saving quota and time on large sheets
- `row_skip` and `row_stop` can be used together: stop is applied first, then skip filters the remaining rows

## v0.4.1

- **Fixed:** numeric strings with space-as-thousands-separator were silently truncated — `"р.250 000"` parsed as `250.0` instead of `250000.0`, `"р.2 722"` as `2.0` instead of `2722.0`
- All common space variants used by Excel / Google Sheets / Russian locale are now stripped before number extraction: regular space, non-breaking space (U+00A0), narrow no-break space (U+202F), thin space (U+2009)
- Only affects lenient numeric mode (columns with `"default"` in schema); strict mode behaviour unchanged

## v0.4.0

- Added `strip_strings` parameter to `GoogleSheetsReadOperator` (default `False`) — when `True`, strips leading and trailing whitespace from all string cell values during schema application. Useful when Google Sheets cells contain accidental spaces that cause mismatches in downstream filtering or deduplication.

## v0.3.0

- Added `"default"` field to numeric schema columns (`int`, `float`) — enables lenient parsing mode:
  - Non-numeric values (`"-"`, `"n/a"`, `""`, `" "`) are replaced with the default value (`None` or `0`) instead of raising an error
  - Comma accepted as decimal separator: `"1,2"` → `1.2`
  - Non-numeric prefixes and suffixes are stripped automatically: `"1000.4 р."` → `1000.4`, `"10.2%"` → `10.2`
  - Without `"default"`, existing strict behaviour is preserved (error on invalid values)
  - `None` default maps to `null` in JSONL/JSON output — compatible with BigQuery nullable columns

## v0.2.0

- **Breaking:** `output_type="json"` in `GoogleSheetsReadOperator` now writes a valid JSON array (`[{...}, {...}]`). Previously it wrote JSONL — use `output_type="jsonl"` for the old behavior
- Added `output_type="jsonl"` to `GoogleSheetsReadOperator` — streams one JSON object per line (memory-efficient, BigQuery-compatible)
- Added `output_type="json"` to `GoogleSheetsReadOperator` — streams a valid JSON array file, readable via `json.load()`
- `GoogleSheetsWriteOperator` now reads file input as JSONL by default (`.csv` files still auto-detected as CSV)
- Added `source_type="json"` support in `normalize_input_data()` for reading JSON array files explicitly
- Added `read_jsonl_file()` utility — reads JSONL files and returns `(headers, rows)`
- Renamed `write_json_file()` → `write_jsonl_file()` (writes JSONL format)
- New `write_json_file()` — writes valid JSON array format
- Added `clear_mode` parameter to `GoogleSheetsWriteOperator` overwrite mode:
  - `"sheet"` (default) — clears entire sheet and trims extra rows after writing (fixes stale data remaining below new export)
  - `"range"` — clears only the data columns, leaves neighbouring columns and extra rows untouched
- Added `trim_sheet()` method to `GoogleSheetsHook` — deletes rows beyond a specified count
- **Fixed:** overwrite mode no longer leaves old data below new export (was clearing only `A1` cell instead of entire sheet)
- Simplified Sheets → BigQuery example DAGs (removed intermediate conversion tasks)
- Updated all example DAGs and both READMEs (EN/RU) for new JSON/JSONL support

## v0.1.9

- **Breaking:** `transliterate_headers` default changed from `False` to `True` — Cyrillic headers are now transliterated to Latin by default
- Added `sanitize_headers` parameter (default `True`) — removes spaces and special characters from header names, keeping only letters, digits, and underscores
- Added `lowercase_headers` parameter (default `True`) — converts header names to lowercase
- When `normalize_headers=True`, `sanitize_headers` and `lowercase_headers` are ignored (normalize already includes both)
- `column_mapping` now takes priority over all header processing — when provided, transliteration, sanitization, and lowercasing are skipped; the mapping is applied directly to raw header names from the spreadsheet

## v0.1.8

- Added `column_mapping` parameter to `GoogleSheetsReadOperator` — rename headers on read via a dict (e.g. Cyrillic → Latin), applied after transliteration/normalization

## v0.1.7

- Changed JSON output format from JSON array to JSONL (newline-delimited JSON) — each line is a separate JSON object, compatible with BigQuery and other tools that expect NDJSON

## v0.1.6

- Fixed `HttpError 400: Range exceeds grid limits` when writing more rows than the sheet contains
- Added automatic sheet grid expansion — the operator now checks `rowCount` before writing and adds rows via `appendDimension` if needed
- Added `ensure_rows()` and `get_sheet_properties()` methods to `GoogleSheetsHook`

## v0.1.5

- Added support for `key_path` / `keyfile_path` authentication — the hook can now read service account credentials from a JSON file on disk
- Added support for custom scopes from the `scope` field in connection extra
- Full compatibility with standard `Google Cloud` connection type (Keyfile Path, Keyfile JSON, Scopes)
- Refactored example DAGs to use `@dag` and `@task` decorators (TaskFlow API)
- Updated README with detailed connection setup options

## v0.1.4

- Added `Documentation` and `Issues` URLs to package metadata for PyPI sidebar
- Added `project-url` to `get_provider_info()` for Airflow providers page link
- Fixed typo in Documentation URL in `pyproject.toml`

## v0.1.3

- Renamed package from `airflow_google_sheets` to `airflow_provider_google_sheets`
- Fixed `post_insert_updates` index recalculation after structural operations in smart merge
- Added `_adjust_post_insert_indices()` method that correctly skips parent operations
- Fixed internal fields (`row_num`, `_source_op`) leaking into `batch_update_values` API payload
- Added payload cleanliness tests
- Expanded `get_provider_info()` with integrations, operators, hooks metadata
- Switched to `setuptools-scm` for automatic versioning from git tags
- Relaxed dependencies: Python >= 3.10, Apache Airflow >= 2.7 < 3.0
- Changed license from Apache-2.0 to MIT
- Added `readme.md` (EN), `readme_ru.md` (RU), `CHANGELOG.md`, `LICENSE`
- Added example DAGs including BigQuery integration
- Added GitHub Actions workflow for testing and publishing to PyPI

## v0.1.2

- Fixed `_build_range()` generating invalid A1-notation when `cell_range=None`
- Fixed `dicts_to_rows()` to collect union of keys from all dicts (not just first)
- Fixed `has_headers=False` handling in smart merge (row 1 was incorrectly skipped)
- Replaced `set()` with `dict.fromkeys()` for deterministic key ordering in smart merge
- Added `batch_update_values()` to hook — bulk value writes via `values.batchUpdate` API
- Smart merge now uses `batch_update_values` instead of individual `update_values` calls

## v0.1.1

- Fixed overwrite mode writing to wrong column when `cell_range` specifies non-A start column
- Added `_parse_range_start()` for extracting start column/row from A1 ranges
- Added `_adjust_row_indices()` for recalculating value-update positions after structural operations
- Added `_group_contiguous()` for correct deletion of non-adjacent rows
- Rewrote read operator with streaming output — CSV/JSON written chunk-by-chunk without memory accumulation
- Added `_read_chunks()` generator, `_stream_to_csv()`, `_stream_to_json()`, `_read_to_xcom()`
- Added `max_xcom_rows` parameter (default 50,000) with error on exceeded limit

## v0.1.0

- Initial release
- `GoogleSheetsHook` — authentication via service account, all CRUD operations with retry
- `GoogleSheetsReadOperator` — chunked reading, schema validation, CSV/JSON/XCom output
- `GoogleSheetsWriteOperator` — overwrite, append, smart merge modes
- `GoogleSheetsCreateSpreadsheetOperator` / `GoogleSheetsCreateSheetOperator`
- Schema-based type conversion (str, int, float, date, datetime, bool)
- Header processing (deduplication, transliteration, normalization)
- Retry with exponential backoff and jitter
- Custom exceptions hierarchy
