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

# Skip multiple conditions (OR logic)
read_multi_skip = GoogleSheetsReadOperator(
    task_id="read_multi_skip",
    spreadsheet_id="your-spreadsheet-id",
    row_skip=[
        {"column": "status", "value": "deleted"},
        {"column": "status", "value": "archived"},
        {"column": "amount", "op": "empty"},
    ],
)
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
| `row_skip` | dict \| list[dict] | `None` | Condition(s) to skip rows: `{"column": "status", "value": "deleted", "op": "equals"}`. Multiple dicts = OR logic |
| `row_stop` | dict \| list[dict] | `None` | Condition(s) to stop reading: rows from the first match onward are discarded, no further API calls |
| `chunk_size` | int | `5000` | Rows per API request |
| `output_type` | str | `"xcom"` | `"xcom"`, `"csv"`, `"json"` (JSON array), or `"jsonl"` (one object per line) |
| `output_path` | str | `None` | File path for csv/json/jsonl output |
| `max_xcom_rows` | int | `50000` | Max rows for XCom output |

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
| `table_start` | str | `"A1"` | Top-left cell of the table (e.g. `"C3"`). Used by `append` and `merge` to locate the header and resolve column positions. Defaults to `"A1"` |
| `create_sheet_if_missing` | bool | `False` | When `True`, create the target sheet if it does not exist. Safe to use with parallel tasks — concurrent creation attempts are handled gracefully |
| `partition_by` | str | `None` | Column name to filter data by before writing. Only rows where the column value matches `partition_value` are written |
| `partition_value` | str | `None` | Value to match against `partition_by` column. Required when `partition_by` is set |
| `column_mapping` | dict | `None` | Rename headers before writing: `{"source_col": "Sheet Header"}`. Applied after all filtering — `merge_key`, `partition_by`, and `schema` always reference the **original** column names from the input data |

**Data input formats:**
- `list[dict]` — headers auto-detected from keys
- `list[list]` — raw rows (set `has_headers=True` if first row is header)
- `str` — file path (`.csv` files read as CSV; all other extensions read as JSONL by default)
- XCom — set `data_xcom_task_id`

File format is auto-detected by extension: `.csv` → CSV, everything else → JSONL.
To read a JSON array file, pass `source_type="json"` to `normalize_input_data()` or write data as JSONL instead.

### Merge Algorithm

Merge reads the key column from the sheet, compares with incoming data, and generates minimal operations:

1. **Read** the key column to build an index `{key_value: [row_numbers]}`
2. **Delete** all existing rows for each key present in incoming data (bottom-up to avoid index shifts)
3. **Append** all incoming rows via `values.append`
4. **Clear** inherited formatting on the new rows via `repeatCell`

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
