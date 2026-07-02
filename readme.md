# airflow-provider-google-sheets

Apache Airflow provider for Google Sheets API v4. Read, write, and manage Google Sheets spreadsheets from Airflow DAGs.

---

> **AI Disclosure:** This provider was developed with the assistance of **Claude Code** (Anthropic, model **Claude Opus 4.6**). The code, tests, and documentation were co-authored by a human developer and an LLM. Please evaluate the code quality on its own merits and make informed decisions about whether to use it in your projects.

---

## Features

- **Read** data from Google Sheets with chunked streaming, schema-based type conversion, and CSV/JSON/JSONL/XCom output
- **Write** data in three modes: overwrite, append, and merge (upsert by key)
- **Merge** — update, insert, and delete rows based on a key column with correct index recalculation
- **Manage** spreadsheets — create new spreadsheets, sheets, list sheets with filtering, and auto-create sheets on write
- **Partitioned write** — fan-out data to multiple sheets by a column value using Airflow dynamic task mapping
- **Large datasets** — streaming read/write without loading everything into memory
- **Schema support** — automatic type conversion (date, int, float, bool) on read and write
- **Header processing** — deduplication, Cyrillic transliteration (on by default), special character removal, lowercase conversion, snake_case normalization

## Installation

```bash
pip install airflow-provider-google-sheets
```

With Cyrillic header transliteration support:

```bash
pip install airflow-provider-google-sheets[transliterate]
```

## Requirements

- Python >= 3.10
- Apache Airflow 2.x (>= 2.7, tested on 2.9.1; Airflow 3.x not tested)
- Google service account with Sheets API access

## Connection Setup

1. Create a Google Cloud service account with **Google Sheets API** enabled.
2. Download the JSON key file.
3. In Airflow UI, create a connection with one of the supported configurations:

### Option A: Standard Google Cloud connection (recommended)

Use this if you already have a `google_cloud_platform` connection configured in Airflow.

- **Conn Id**: `google_cloud_default`
- **Conn Type**: `Google Cloud`
- **Keyfile Path**: `/path/to/service-account.json`
- **Scopes**: `https://www.googleapis.com/auth/spreadsheets` (add more if needed)

### Option B: Inline JSON key

- **Conn Id**: `google_cloud_default`
- **Conn Type**: `Google Cloud` or `google_sheets`
- **Keyfile JSON**: paste the full service account JSON

### Option C: JSON in Extra field

- **Conn Id**: `google_cloud_default`
- **Conn Type**: `google_sheets`
- **Extra**: paste the full JSON key, or use `{"keyfile_dict": <JSON key>}`

The hook checks credentials in this order: `key_path` / `keyfile_path` (file on disk) → `keyfile_dict` (inline JSON) → raw Extra JSON.

## Operators

### GoogleSheetsReadOperator

Read data from a spreadsheet.

```python
from airflow_provider_google_sheets.operators.read import GoogleSheetsReadOperator

# Basic read — returns list[dict] via XCom
read = GoogleSheetsReadOperator(
    task_id="read_sheets",
    spreadsheet_id="your-spreadsheet-id",
    sheet_name="Sheet1",
)

# Stream large sheet to CSV file (no memory accumulation)
read_csv = GoogleSheetsReadOperator(
    task_id="read_to_csv",
    spreadsheet_id="your-spreadsheet-id",
    output_type="csv",
    output_path="/tmp/export.csv",
    chunk_size=10000,
)

# Stream to JSONL file (one JSON object per line, memory-efficient)
read_jsonl = GoogleSheetsReadOperator(
    task_id="read_to_jsonl",
    spreadsheet_id="your-spreadsheet-id",
    output_type="jsonl",
    output_path="/tmp/export.json",
    chunk_size=10000,
)

# Stream to JSON array file
read_json = GoogleSheetsReadOperator(
    task_id="read_to_json",
    spreadsheet_id="your-spreadsheet-id",
    output_type="json",
    output_path="/tmp/export.json",
)

# Read with type conversion
read_typed = GoogleSheetsReadOperator(
    task_id="read_typed",
    spreadsheet_id="your-spreadsheet-id",
    schema={
        "date": {"type": "date", "format": "%Y-%m-%d"},
        "revenue": {"type": "float", "required": True},
        "quantity": {"type": "int"},
    },
)

# Default behavior: headers are transliterated, sanitized, and lowercased.
# "Дата отчёта" → "data_otchyota", "Клиент (ФИО)" → "klient_fio"
read_default = GoogleSheetsReadOperator(
    task_id="read_default",
    spreadsheet_id="your-spreadsheet-id",
)

# column_mapping takes priority — all other header processing is skipped,
# mapping keys use the original raw header names from the spreadsheet.
read_mapped = GoogleSheetsReadOperator(
    task_id="read_mapped",
    spreadsheet_id="your-spreadsheet-id",
    output_type="jsonl",
    output_path="/tmp/export.json",
    column_mapping={
        "Дата": "report_date",
        "Клиент": "client",
        "Сумма": "amount",
    },
)

# Disable all header processing to keep original names
read_raw = GoogleSheetsReadOperator(
    task_id="read_raw",
    spreadsheet_id="your-spreadsheet-id",
    transliterate_headers=False,
    sanitize_headers=False,
    lowercase_headers=False,
)

# Skip rows where status is "deleted" and stop reading at "ИТОГО"
read_filtered = GoogleSheetsReadOperator(
    task_id="read_filtered",
    spreadsheet_id="your-spreadsheet-id",
    row_skip={"column": "status", "value": "deleted"},
    row_stop={"column": "name", "value": "ИТОГО"},
)

# Skip multiple conditions — row is skipped if ANY condition matches (OR logic)
read_multi_skip = GoogleSheetsReadOperator(
    task_id="read_multi_skip",
    spreadsheet_id="your-spreadsheet-id",
    row_skip=[
        {"column": "status", "value": "deleted"},
        {"column": "status", "value": "archived"},
        {"column": "amount", "op": "empty"},
    ],
)

# row_stop also accepts a list — stops at the first row matching any condition
read_stop_multi = GoogleSheetsReadOperator(
    task_id="read_stop_multi",
    spreadsheet_id="your-spreadsheet-id",
    row_stop=[
        {"column": "name", "value": "ИТОГО"},
        {"column": "type", "op": "starts_with", "value": "total_"},
    ],
)

# Filter rows by a single column value (include-filter)
# filter_column must be the PROCESSED header name (after transliterate/sanitize/lowercase/column_mapping)
read_city = GoogleSheetsReadOperator(
    task_id="read_moscow",
    spreadsheet_id="your-spreadsheet-id",
    filter_column="city",
    filter_value="Moscow",
)

# Filter by multiple values (OR logic)
read_cities = GoogleSheetsReadOperator(
    task_id="read_two_cities",
    spreadsheet_id="your-spreadsheet-id",
    filter_column="city",
    filter_value=["Moscow", "Berlin"],
)

# Dynamic fan-out: read data for each city in separate mapped tasks
# "Город" in the spreadsheet + column_mapping → filter_column="city"
from airflow_provider_google_sheets.operators.manage import GoogleSheetsUniqueValuesOperator

cities = GoogleSheetsUniqueValuesOperator(
    task_id="get_cities",
    spreadsheet_id="your-spreadsheet-id",
    column="city",
    column_mapping={"Город": "city"},
    exclude_values=[""],   # skip empty cells
)
read_by_city = GoogleSheetsReadOperator.partial(
    task_id="read_by_city",
    spreadsheet_id="your-spreadsheet-id",
    column_mapping={"Город": "city"},
    filter_column="city",
).expand(filter_value=cities.output)
```

**Parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `gcp_conn_id` | str | `"google_cloud_default"` | Airflow Connection ID |
| `spreadsheet_id` | str | — | Spreadsheet ID |
| `sheet_name` | str | `None` | Sheet name (None = first sheet) |
| `cell_range` | str | `None` | A1-notation range (None = entire sheet) |
| `has_headers` | bool | `True` | First row contains headers |
| `transliterate_headers` | bool | `True` | Transliterate Cyrillic to Latin |
| `sanitize_headers` | bool | `True` | Remove spaces and special characters (keep letters, digits, `_`) |
| `lowercase_headers` | bool | `True` | Convert headers to lowercase |
| `normalize_headers` | bool | `False` | Normalize to snake_case (overrides `sanitize` + `lowercase`) |
| `column_mapping` | dict | `None` | Rename headers using raw names: `{"Original": "new_name"}`. Skips all other processing |
| `schema` | dict | `None` | Column type schema |
| `strip_strings` | bool | `False` | Strip leading/trailing whitespace from string cell values |
| `row_skip` | dict \| list[dict] | `None` | Skip rows matching a condition. A row is skipped if **any** condition matches (OR). Single dict or list of dicts: `{"column": "status", "value": "deleted", "op": "equals"}` |
| `row_stop` | dict \| list[dict] | `None` | Stop reading at the first matching row (the matching row is also discarded). Accepts a single dict or list — stops when **any** condition matches. No further API calls after stopping |
| `chunk_size` | int | `5000` | Rows per API request |
| `output_type` | str | `"xcom"` | `"xcom"`, `"csv"`, `"json"` (JSON array), or `"jsonl"` (one object per line) |
| `output_path` | str | `None` | File path for csv/json/jsonl output |
| `max_xcom_rows` | int | `50000` | Max rows for XCom output |
| `max_xcom_bytes` | int | `None` | Max XCom payload size in bytes. Raises if exceeded. `None` = no limit (a WARNING is still emitted when the estimated size exceeds 5 MB) |
| `filter_column` | str | `None` | Include-filter column name (processed header name). Must be set together with `filter_value` |
| `filter_value` | str \| list[str] | `None` | Value(s) to keep. OR logic when a list is given. Supports Jinja templates and dynamic mapping via `expand(filter_value=...)` |

**Supported `op` values for `row_skip` / `row_stop`:**

| `op` | Description | `value` required |
|---|---|---|
| `equals` (default) | Exact string match | yes |
| `not_equals` | Not equal | yes |
| `contains` | Cell contains substring | yes |
| `not_contains` | Cell does not contain substring | yes |
| `starts_with` | Cell starts with value | yes |
| `ends_with` | Cell ends with value | yes |
| `empty` | Cell is empty or None | no |
| `not_empty` | Cell is not empty | no |

All comparisons are performed on string representations of cell values. If the referenced `column` is not present in the row, the condition is silently ignored (the row is not skipped/stopped on that condition).

### GoogleSheetsWriteOperator

Write data to a spreadsheet.

```python
from airflow_provider_google_sheets.operators.write import GoogleSheetsWriteOperator

# Overwrite with list[dict]
write = GoogleSheetsWriteOperator(
    task_id="write_sheets",
    spreadsheet_id="your-spreadsheet-id",
    sheet_name="Output",
    write_mode="overwrite",
    data=[{"date": "2024-01-01", "value": 100}],
)

# Append rows
# By default append writes positionally (values.update into a fixed range),
# which makes it resilient to transient 404s without creating duplicates.
append = GoogleSheetsWriteOperator(
    task_id="append_sheets",
    spreadsheet_id="your-spreadsheet-id",
    write_mode="append",
    data=[{"event": "login", "user": "alice"}],
)

# Merge by key
merge = GoogleSheetsWriteOperator(
    task_id="merge",
    spreadsheet_id="your-spreadsheet-id",
    write_mode="merge",  # "smart_merge" is accepted as an alias
    merge_key="date",
    data=[
        {"date": "2024-01-01", "value": 110},  # update existing
        {"date": "2024-01-03", "value": 200},  # append new
    ],
)

# Table starting at a non-default cell (e.g. C3)
# Headers are written to C3 on first run; key column is resolved relative to C
merge_offset = GoogleSheetsWriteOperator(
    task_id="merge_offset",
    spreadsheet_id="your-spreadsheet-id",
    sheet_name="Report",
    write_mode="merge",
    merge_key="date",
    table_start="C3",   # table header lives at C3
    data=[{"date": "2024-01-01", "revenue": 110}],
)

# Sort the table after writing (newest dates on top)
sorted_merge = GoogleSheetsWriteOperator(
    task_id="sorted_merge",
    spreadsheet_id="your-spreadsheet-id",
    write_mode="merge",
    merge_key="date",
    data=[{"date": "2024-01-03", "value": 200}],
    sort_keys=["date:desc"],   # server-side sortRange after the write
)

# Sort by two columns: newest dates first, then region A→Z within each date
multi_sorted = GoogleSheetsWriteOperator(
    task_id="multi_sorted",
    spreadsheet_id="your-spreadsheet-id",
    write_mode="merge",
    merge_key="date",
    data=[
        {"date": "2024-01-03", "region": "EU", "value": 200},
        {"date": "2024-01-03", "region": "US", "value": 150},
    ],
    sort_keys=["date:desc", "region:asc"],   # primary key first, then secondary
)
```

**Parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `gcp_conn_id` | str | `"google_cloud_default"` | Airflow Connection ID |
| `spreadsheet_id` | str | — | Spreadsheet ID |
| `sheet_name` | str | `None` | Sheet name |
| `cell_range` | str | `None` | Target A1 range (overwrite mode) |
| `write_mode` | str | `"overwrite"` | `"overwrite"`, `"append"`, `"merge"` (alias: `"smart_merge"`) |
| `clear_mode` | str | `"sheet"` | Overwrite clearing strategy: `"sheet"` clears entire sheet and trims extra rows; `"range"` clears only data columns |
| `data` | Any | `None` | Data: list[list], list[dict], or file path |
| `data_xcom_task_id` | str | `None` | Pull data from this task's XCom |
| `data_xcom_key` | str | `"return_value"` | XCom key |
| `has_headers` | bool | `True` | Data contains headers |
| `write_headers` | bool | `True` | Write header row. In `append`/`merge` modes, headers are written automatically when the sheet is empty |
| `schema` | dict | `None` | Schema for formatting values |
| `batch_size` | int | `1000` | Rows per API request |
| `pause_between_batches` | float | `1.0` | Seconds between batches |
| `merge_key` | str | `None` | Key column for merge mode |
| `normalize_merge_key_format` | bool | `True` | When `True`, merge-key normalization tries extended fallbacks: `input_format`, then Google Sheets serial date number (date/datetime columns only). Also enables schema-free date inference for `merge_key` (see below). Set to `False` to restore legacy single-format behaviour |
| `table_start` | str | `"A1"` | Top-left cell of the table (e.g. `"C3"`). Used by `append` and `merge` to locate the header and resolve column positions. Ignored in `overwrite` mode — which uses `cell_range` instead |
| `create_sheet_if_missing` | bool | `False` | When `True`, create the target sheet if it does not exist. Safe to use with parallel tasks — concurrent creation attempts are handled gracefully |
| `partition_by` | str | `None` | Column name to filter data by before writing. Only rows where the column value matches `partition_value` are written |
| `partition_value` | str | `None` | Value to match against `partition_by` column. Required when `partition_by` is set |
| `column_mapping` | dict | `None` | Rename headers before writing: `{"source_col": "Sheet Header"}`. Applied after all filtering — `merge_key`, `partition_by`, and `schema` always reference the **original** column names from the input data |
| `sort_keys` | list[str] | `None` | Sort the table server-side after writing (any write mode). Items are `"column:asc"` / `"column:desc"` (direction case-insensitive), e.g. `["date:desc", "region:asc"]`. Column names refer to headers **after** `column_mapping`. Requires named headers; only the table's own columns are sorted. Format validated at DAG-load time. Not compatible with `overwrite` + `clear_mode="range"` or `append` + `cell_range` |
| `append_insert_rows` | bool | `False` | `append` mode only. `False` (default) writes appended rows **positionally** (`values.update` into a fixed range), which makes the write resilient to transient `404` and to ambiguous-success without creating duplicates. `True` restores the legacy `values.append` (`INSERT_ROWS`) behaviour — atomic row insertion, but **not** covered by the transient-404 retry (fail-fast on 404). Use `True` for layouts that keep content directly below the table in the same columns (a footer that `INSERT_ROWS` should push down rather than overwrite), or when several writers append to the same sheet concurrently. See "Append behavior" below. Must be a `bool` |
| `transient_404_max_retries` | int | `3` | Number of times to re-run the **whole** operation when Google Sheets returns a transient `404` on a spreadsheet that really exists (observed after heavy writes). Applies to the idempotent modes — `overwrite` and `merge` — to the default (positional) `append`, and to `create_sheet_if_missing` setup. Legacy `append` (`append_insert_rows=True`) and read paths stay fail-fast. A non-404 `HttpError` is re-raised immediately. Must be a non-bool `int >= 0`; `0` disables the retry |
| `transient_404_base_delay` | float | `5.0` | Base delay in seconds for the exponential backoff between transient-404 re-runs (delay = `base_delay * 2 ** (attempt - 1)`). Must be a non-bool real `>= 0`. Worst case with defaults: 1 initial run plus up to 3 re-runs (4 runs total) with `5 + 10 + 20 = 35`s of backoff between them — keep the Airflow `execution_timeout` comfortably above this |

**Data input formats:**
- `list[dict]` — headers auto-detected from keys
- `list[list]` — raw rows (set `has_headers=True` if first row is header)
- `str` — file path (`.csv` files read as CSV; all other extensions read as JSONL by default)
- XCom — set `data_xcom_task_id`

File format is auto-detected by extension: `.csv` → CSV, everything else → JSONL.
To read a JSON array file, pass `source_type="json"` to `normalize_input_data()` or write data as JSONL instead.

### Append behavior

By default (`append_insert_rows=False`), `append` writes **positionally**: it reads the current height of the table once, then writes the new rows with `values.update` into the fixed range immediately below it. For the ordinary layout (the table is the last block of content in its columns and you append to the bottom) the visible result is identical to the legacy `values.append` (`INSERT_ROWS`) path, so existing DAGs get the new behaviour without any changes.

The positional write is **idempotent on an in-process retry** — a repeated write targets the same cells rather than adding new ones. This is what lets the default `append` participate in the transient-404 retry (see `transient_404_max_retries`) and survive an ambiguous-success inside the hook-level retry without producing duplicate rows.

**Assumptions of the default path:**

- **Nothing below the table in its columns.** The rows immediately under the last non-empty row (in the table's own columns) are assumed to be empty and will be overwritten. If you keep a footer or other content directly below the table in the same columns, set `append_insert_rows=True` so `INSERT_ROWS` pushes it down instead.
- **Single writer per sheet.** The height is captured once and reused for the write; if another writer appends to the same sheet between the read and the write, the positional path can overwrite those rows (unlike the atomic `INSERT_ROWS`). The provider's contract is exactly one task/DAG writing a given sheet. For concurrent atomic insertion, use `append_insert_rows=True`.

**What is not covered:** the positional append is only resilient while the task is **alive** (an in-process 404 retry). A full Airflow **task-retry** (the task fails and Airflow restarts it) is not covered — a fresh run re-reads the now-grown height and may append again. This is the same behaviour as the legacy append; it is not made worse, only the in-process case is improved.

### Observability

Every write mode returns a result dict via XCom that includes `transient_404_retries` — the number of times the operation re-ran after a transient `404` (`0` when there were none). A `WARNING` is logged on each retry attempt, and when the count is greater than zero an INFO summary (`"<mode> completed after N transient-404 retries"`) is emitted at the end of the task.

```python
# e.g. {"mode": "append", "rows_written": 3, "transient_404_retries": 0}
```

### Merge Algorithm

Merge reads the key column from the sheet, compares with incoming data, and generates minimal operations:

1. **Read** the key column to build an index `{key_value: [row_numbers]}`
2. **Delete** all existing rows for each key present in incoming data (bottom-up to avoid index shifts)
3. **Append** all incoming rows via `values.append`
4. **Clear** inherited formatting on the new rows via `repeatCell`

**Key normalization:** Values read from the sheet in step 1 are normalized to match the canonical write format defined in `schema`. When `normalize_merge_key_format=True` (default), three strategies are tried in order: (1) parse with `format`, (2) parse with `input_format`, (3) interpret as a Google Sheets serial date number (date/datetime columns only). Set `normalize_merge_key_format=False` to disable extended normalization.

**Schema-free date merge keys:** If `merge_key` has no entry in `schema` at all, merging by an ISO date key (`YYYY-MM-DD`, e.g. `"2026-01-01"`) is safe out of the box when `normalize_merge_key_format=True` (default) — the column is auto-detected as a date key from the shape of the incoming values, and the existing sheet key is read back as a serial number so the match survives the column's display format being changed (Date ↔ Number ↔ a different date format). This auto-detection only recognizes the `YYYY-MM-DD` shape. Other date formats (e.g. `"01.03.2026"`) and `datetime` keys (e.g. `"2026-01-01 12:30:00"`) are **not** inferred without `schema` — for those, pass an explicit `schema` entry for `merge_key` (with `format`/`input_format` as needed, see below). Set `normalize_merge_key_format=False` to disable this inference and restore strictly legacy behaviour (raw values compared as-is).

### GoogleSheetsCreateSpreadsheetOperator

```python
from airflow_provider_google_sheets.operators.manage import GoogleSheetsCreateSpreadsheetOperator

create = GoogleSheetsCreateSpreadsheetOperator(
    task_id="create_spreadsheet",
    title="Monthly Report",
    sheet_titles=["Summary", "Details"],
)
# Returns spreadsheet_id via XCom
```

### GoogleSheetsCreateSheetOperator

```python
from airflow_provider_google_sheets.operators.manage import GoogleSheetsCreateSheetOperator

add_sheet = GoogleSheetsCreateSheetOperator(
    task_id="add_sheet",
    spreadsheet_id="your-spreadsheet-id",
    sheet_title="NewSheet",
)
```

### GoogleSheetsListSheetsOperator

List sheet (tab) names of a spreadsheet with optional filtering. Returns `list[str]`, compatible with Airflow dynamic task mapping.

```python
from airflow_provider_google_sheets.operators.manage import GoogleSheetsListSheetsOperator

# List all sheets
list_sheets = GoogleSheetsListSheetsOperator(
    task_id="list_sheets",
    spreadsheet_id="your-spreadsheet-id",
)

# Filter by regex and use with dynamic task mapping
list_data_sheets = GoogleSheetsListSheetsOperator(
    task_id="list_data_sheets",
    spreadsheet_id="your-spreadsheet-id",
    name_pattern=r"^Data",          # include only sheets starting with "Data"
    exclude_pattern=r"_archive$",   # exclude sheets ending with "_archive"
    index_range=(0, 10),            # only first 10 sheets
)

# Dynamic task mapping — read each sheet in parallel
read_each = GoogleSheetsReadOperator.partial(
    task_id="read_each",
    spreadsheet_id="your-spreadsheet-id",
).expand(sheet_name=list_data_sheets.output)
```

**Parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `gcp_conn_id` | str | `"google_cloud_default"` | Airflow Connection ID |
| `spreadsheet_id` | str | — | Spreadsheet ID |
| `name_pattern` | str | `None` | Regex to include sheets by name (`re.search`) |
| `exclude_pattern` | str | `None` | Regex to exclude sheets by name (`re.search`) |
| `index_range` | tuple[int, int] | `None` | Positional slice `(start, end)`, 0-based, start inclusive, end exclusive |

### GoogleSheetsExtractPartitionsOperator

Extract unique partition values from data and return a list of `{"sheet_name", "partition_value"}` dicts for Airflow `expand_kwargs`. Does **not** call the Google Sheets API — operates purely on in-memory data.

Primary use case: fan-out writing where each unique value in a column maps to its own sheet.

```python
from airflow_provider_google_sheets.operators.manage import (
    GoogleSheetsExtractPartitionsOperator,
)
from airflow_provider_google_sheets.operators.write import GoogleSheetsWriteOperator

# Returns [{"sheet_name": "Report 2026-01", "partition_value": "2026-01"}, ...]
partitions = GoogleSheetsExtractPartitionsOperator(
    task_id="get_partitions",
    data_xcom_task_id="fetch_data",   # or data="/path/to/file.jsonl"
    partition_column="period",
    sheet_name_template="Report {value}",   # optional, default = "{value}"
)

# Write each partition to its own sheet — one Airflow task per partition
write = GoogleSheetsWriteOperator.partial(
    task_id="write_to_sheet",
    spreadsheet_id="your-spreadsheet-id",
    data_xcom_task_id="fetch_data",
    partition_by="period",          # filter data inside each task
    create_sheet_if_missing=True,   # create sheet if it doesn't exist
    write_mode="overwrite",
).expand_kwargs(partitions.output)
```

**Parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `partition_column` | str | — | Column name whose unique values define partitions |
| `sheet_name_template` | str | `"{value}"` | Format string for sheet names. Use `{value}` as placeholder |
| `data` | Any | `None` | Data: `list[dict]`, `list[list]`, or file path (`.jsonl`, `.csv`) |
| `data_xcom_task_id` | str | `None` | Pull data from this task's XCom |
| `data_xcom_key` | str | `"return_value"` | XCom key |
| `has_headers` | bool | `True` | Must be `True` — partition column lookup requires headers |

**Returns:** `list[dict]` — one entry per unique partition value, in order of first appearance:
```python
[
    {"sheet_name": "Report 2026-01", "partition_value": "2026-01"},
    {"sheet_name": "Report 2026-02", "partition_value": "2026-02"},
]
```

### GoogleSheetsUniqueValuesOperator

Read unique values from a single column of a Google Sheets spreadsheet and return a `list[str]` in order of first occurrence. Designed for Airflow dynamic task mapping — unlike `ExtractPartitionsOperator`, it reads directly from the API instead of working on in-memory data.

The `column` parameter must be the **processed** header name (after transliterate / sanitize / lowercase / column_mapping — same processing as `GoogleSheetsReadOperator`).

```python
from airflow_provider_google_sheets.operators.manage import GoogleSheetsUniqueValuesOperator
from airflow_provider_google_sheets.operators.read import GoogleSheetsReadOperator

# Get unique city values from the sheet
cities = GoogleSheetsUniqueValuesOperator(
    task_id="get_cities",
    spreadsheet_id="your-spreadsheet-id",
    column="city",           # processed header name
    exclude_values=[""],     # skip empty cells
)

# Fan-out: read data filtered by each city in parallel mapped tasks
read_by_city = GoogleSheetsReadOperator.partial(
    task_id="read_by_city",
    spreadsheet_id="your-spreadsheet-id",
    filter_column="city",
).expand(filter_value=cities.output)

# With column_mapping — "Город" in the sheet → "city" after mapping
cities_mapped = GoogleSheetsUniqueValuesOperator(
    task_id="get_cities",
    spreadsheet_id="your-spreadsheet-id",
    column="city",                          # name AFTER mapping
    column_mapping={"Город": "city"},
    exclude_values=[""],
)
```

**Parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `gcp_conn_id` | str | `"google_cloud_default"` | Airflow Connection ID |
| `spreadsheet_id` | str | — | Spreadsheet ID |
| `sheet_name` | str | `None` | Sheet name (None = first sheet) |
| `cell_range` | str | `None` | A1-notation range (None = entire sheet) |
| `column` | str | — | Processed header name of the column to extract unique values from |
| `exclude_values` | list[str] | `None` | Values to exclude. Pass `[""]` to exclude empty cells |
| `chunk_size` | int | `5000` | Rows per API request |
| `has_headers` | bool | `True` | First row contains headers |
| `transliterate_headers` | bool | `True` | Transliterate Cyrillic to Latin |
| `sanitize_headers` | bool | `True` | Remove spaces and special characters |
| `lowercase_headers` | bool | `True` | Convert headers to lowercase |
| `column_mapping` | dict | `None` | Rename headers using raw names. Skips all other processing |

**Returns:** `list[str]` — unique values in order of first occurrence.

## Schema

Define column types for automatic conversion on read/write:

```python
schema = {
    "date": {"type": "date", "format": "%Y-%m-%d", "required": True},
    "revenue": {"type": "float", "required": True},
    "quantity": {"type": "int"},
    "comment": {"type": "str"},
    "is_active": {"type": "bool"},
}
```

**Supported types:** `str`, `int`, `float`, `date`, `datetime`, `bool`

### Robust numeric parsing

For numeric columns (`int`, `float`) add `"default"` to enable lenient parsing.
Non-numeric values are replaced with the default instead of raising an error:

```python
schema = {
    "revenue": {"type": "float", "default": None},   # "n/a", "-", "" → None
    "quantity": {"type": "int",   "default": 0},       # "n/a", "-", "" → 0
}
```

Lenient mode also handles:
- Comma as decimal separator: `"1,2"` → `1.2`
- Prefix/suffix stripping: `"1000.4 р."` → `1000.4`, `"10.2%"` → `10.2`

Without `"default"`, the strict behaviour is preserved (error on invalid values).

### Separate parse and write formats for dates (`input_format`)

By default, the `format` field is used both for **parsing** incoming string values and for **writing** to the sheet.
If your input data uses a different date format than the sheet (e.g. ISO `"2026-03-01"` from JSONL vs `"01.03.2026"` in the sheet), use `input_format` to specify the parse format separately:

```python
schema = {
    "date": {
        "type": "date",
        "input_format": "%Y-%m-%d",   # how to parse incoming strings (e.g. from JSONL)
        "format": "%d.%m.%Y",          # how to write to the sheet
    }
}
```

This is especially important in `merge` mode: without `input_format` the incoming key `"2026-03-01"` and the existing sheet key `"01.03.2026"` would not match, causing duplicate rows on every run.

`input_format` only affects `date` and `datetime` columns. For other types (`str`, `int`, etc.) it has no effect.

When Google Sheets stores a date cell without a date format applied (or converts it back to a raw number), the API returns the serial integer (e.g. `"46023"`). With an explicit `schema` entry and `type: "date"`, this case is handled robustly by `normalize_merge_key_format=True`: the key column is read with the `SERIAL_NUMBER` render option, so matching is unaffected by the cell's display format. For `date` keys specifically, this also works **without** any `schema` entry at all, as long as the incoming values are ISO dates (`YYYY-MM-DD`) — see "Schema-free date merge keys" above. For `type: "datetime"`, the key column is always read as `FORMATTED_STRING` (never `SERIAL_NUMBER`, to avoid truncating time-of-day in the common case); if the cell's format happens to display a bare serial number, the existing key can still fall back to being decoded from it, but only down to date granularity — **time-of-day is truncated to midnight**. `datetime` keys always require an explicit `schema`; there is no schema-free inference for them.

## Examples

See the `examples/` directory for complete DAG examples:

- `example_read.py` — reading with various configurations
- `example_write.py` — overwrite and append modes
- `example_smart_merge.py` — merge scenarios
- `example_manage.py` — creating spreadsheets and sheets
- `example_sheets_to_bigquery.py` — Google Sheets → BigQuery (overwrite, append, date-range update)
- `example_bigquery_to_sheets.py` — BigQuery → Google Sheets (overwrite, merge by date)

## License

MIT License
