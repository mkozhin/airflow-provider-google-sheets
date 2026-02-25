# Changelog

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
