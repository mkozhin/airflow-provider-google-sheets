# airflow-provider-google-sheets

Apache Airflow provider for Google Sheets API v4. Read, write, and manage Google Sheets spreadsheets from Airflow DAGs.

---

> **AI Disclosure:** This provider was developed with the assistance of **Claude Code** (Anthropic, model **Claude Opus 4.6**). The code, tests, and documentation were co-authored by a human developer and an LLM. Please evaluate the code quality on its own merits and make informed decisions about whether to use it in your projects.

---

## Features

- **Read** data from Google Sheets with chunked streaming, schema-based type conversion, and CSV/JSON/JSONL/XCom output
- **Write** data in three modes: overwrite, append, and smart merge (upsert by key)
- **Smart merge** — update, insert, and delete rows based on a key column with correct index recalculation
- **Manage** spreadsheets — create new spreadsheets and sheets
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

# Smart merge by key
merge = GoogleSheetsWriteOperator(
    task_id="smart_merge",
    spreadsheet_id="your-spreadsheet-id",
    write_mode="smart_merge",
    merge_key="date",
    data=[
        {"date": "2024-01-01", "value": 110},  # update existing
        {"date": "2024-01-03", "value": 200},  # append new
    ],
)
```

**Parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `gcp_conn_id` | str | `"google_cloud_default"` | Airflow Connection ID |
| `spreadsheet_id` | str | — | Spreadsheet ID |
| `sheet_name` | str | `None` | Sheet name |
| `cell_range` | str | `None` | Target A1 range (overwrite mode) |
| `write_mode` | str | `"overwrite"` | `"overwrite"`, `"append"`, `"smart_merge"` |
| `clear_mode` | str | `"sheet"` | Overwrite clearing strategy: `"sheet"` clears entire sheet and trims extra rows; `"range"` clears only data columns |
| `data` | Any | `None` | Data: list[list], list[dict], or file path |
| `data_xcom_task_id` | str | `None` | Pull data from this task's XCom |
| `data_xcom_key` | str | `"return_value"` | XCom key |
| `has_headers` | bool | `True` | Data contains headers |
| `write_headers` | bool | `True` | Write header row (overwrite mode) |
| `schema` | dict | `None` | Schema for formatting values |
| `batch_size` | int | `1000` | Rows per API request |
| `pause_between_batches` | float | `1.0` | Seconds between batches |
| `merge_key` | str | `None` | Key column for smart_merge |

**Data input formats:**
- `list[dict]` — headers auto-detected from keys
- `list[list]` — raw rows (set `has_headers=True` if first row is header)
- `str` — file path (`.csv` files read as CSV; all other extensions read as JSONL by default)
- XCom — set `data_xcom_task_id`

File format is auto-detected by extension: `.csv` → CSV, everything else → JSONL.
To read a JSON array file, pass `source_type="json"` to `normalize_input_data()` or write data as JSONL instead.

### Smart Merge Algorithm

Smart merge reads the key column from the sheet, compares with incoming data, and generates minimal operations:

1. **Read** the key column to build an index `{key_value: [row_numbers]}`
2. **Compare** each key: same count → update, more incoming → insert, fewer → delete, new key → append
3. **Sort** structural operations bottom-up (descending row number) to prevent index corruption
4. **Execute** inserts/deletes via `batchUpdate`, then recalculate row indices for value updates
5. **Write** values via `batch_update_values` for efficiency

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

## Examples

See the `examples/` directory for complete DAG examples:

- `example_read.py` — reading with various configurations
- `example_write.py` — overwrite and append modes
- `example_smart_merge.py` — smart merge scenarios
- `example_manage.py` — creating spreadsheets and sheets
- `example_sheets_to_bigquery.py` — Google Sheets → BigQuery (overwrite, append, date-range update)
- `example_bigquery_to_sheets.py` — BigQuery → Google Sheets (overwrite, smart merge by date)

## License

MIT License
