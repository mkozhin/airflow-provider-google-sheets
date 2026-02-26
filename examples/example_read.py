"""Example DAGs for reading data from Google Sheets.

Demonstrates various GoogleSheetsReadOperator configurations:
- Basic XCom read (with/without headers)
- Schema-based type conversion
- Streaming to CSV/JSON/JSONL files
- Header transliteration and normalization
"""

from datetime import datetime

from airflow.decorators import dag, task

from airflow_provider_google_sheets.operators.read import GoogleSheetsReadOperator

SPREADSHEET_ID = "your-spreadsheet-id-here"
GCP_CONN_ID = "google_cloud_default"


# ---------------------------------------------------------------------------
# DAG 1: Basic read to XCom
# ---------------------------------------------------------------------------
@dag(
    dag_id="example_sheets_read_basic",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["google-sheets", "example"],
)
def sheets_read_basic():
    # Read with headers — returns list[dict]
    read_with_headers = GoogleSheetsReadOperator(
        task_id="read_with_headers",
        gcp_conn_id=GCP_CONN_ID,
        spreadsheet_id=SPREADSHEET_ID,
        sheet_name="Sheet1",
    )

    # Read without headers — returns list[list]
    read_without_headers = GoogleSheetsReadOperator(
        task_id="read_without_headers",
        gcp_conn_id=GCP_CONN_ID,
        spreadsheet_id=SPREADSHEET_ID,
        sheet_name="Sheet1",
        has_headers=False,
    )

    # Read specific range
    read_range = GoogleSheetsReadOperator(
        task_id="read_specific_range",
        gcp_conn_id=GCP_CONN_ID,
        spreadsheet_id=SPREADSHEET_ID,
        cell_range="B2:D100",
    )

    @task
    def show_data(**context):
        data = context["ti"].xcom_pull(task_ids="read_with_headers")
        print(f"Read {len(data)} rows")
        for row in data[:5]:
            print(row)

    read_with_headers >> show_data()


sheets_read_basic()


# ---------------------------------------------------------------------------
# DAG 2: Read with schema type conversion
# ---------------------------------------------------------------------------
@dag(
    dag_id="example_sheets_read_schema",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["google-sheets", "example"],
)
def sheets_read_schema():
    GoogleSheetsReadOperator(
        task_id="read_with_schema",
        gcp_conn_id=GCP_CONN_ID,
        spreadsheet_id=SPREADSHEET_ID,
        sheet_name="Sales",
        schema={
            "date": {"type": "date", "format": "%d/%m/%Y"},
            "revenue": {"type": "float", "required": True},
            "quantity": {"type": "int"},
            "is_refund": {"type": "bool"},
        },
    )


sheets_read_schema()


# ---------------------------------------------------------------------------
# DAG 3: Streaming to CSV / JSON / JSONL files
# ---------------------------------------------------------------------------
@dag(
    dag_id="example_sheets_read_streaming",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["google-sheets", "example"],
)
def sheets_read_streaming():
    # Stream to CSV — no memory accumulation for large sheets
    GoogleSheetsReadOperator(
        task_id="stream_to_csv",
        gcp_conn_id=GCP_CONN_ID,
        spreadsheet_id=SPREADSHEET_ID,
        sheet_name="LargeData",
        output_type="csv",
        output_path="/tmp/sheets_export.csv",
        chunk_size=10000,
    )

    # Stream to JSONL (one JSON object per line) — best for large datasets
    # and for piping into GoogleSheetsWriteOperator (JSONL is the default input)
    GoogleSheetsReadOperator(
        task_id="stream_to_jsonl",
        gcp_conn_id=GCP_CONN_ID,
        spreadsheet_id=SPREADSHEET_ID,
        sheet_name="LargeData",
        output_type="jsonl",
        output_path="/tmp/sheets_export.json",
        chunk_size=10000,
    )

    # Stream to JSON array — standard JSON format, readable with json.load()
    GoogleSheetsReadOperator(
        task_id="stream_to_json",
        gcp_conn_id=GCP_CONN_ID,
        spreadsheet_id=SPREADSHEET_ID,
        sheet_name="LargeData",
        output_type="json",
        output_path="/tmp/sheets_export_array.json",
        chunk_size=10000,
    )

    # XCom with row limit protection
    GoogleSheetsReadOperator(
        task_id="read_xcom_with_limit",
        gcp_conn_id=GCP_CONN_ID,
        spreadsheet_id=SPREADSHEET_ID,
        sheet_name="LargeData",
        max_xcom_rows=10_000,
    )


sheets_read_streaming()


# ---------------------------------------------------------------------------
# DAG 4: Header processing (transliteration, normalization)
# ---------------------------------------------------------------------------
@dag(
    dag_id="example_sheets_read_headers",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["google-sheets", "example"],
)
def sheets_read_headers():
    # Transliterate Cyrillic headers to Latin
    # "Дата" → "data", "Выручка" → "vyruchka"
    GoogleSheetsReadOperator(
        task_id="read_transliterated",
        gcp_conn_id=GCP_CONN_ID,
        spreadsheet_id=SPREADSHEET_ID,
        transliterate_headers=True,
    )

    # Normalize headers to snake_case
    # "Revenue (USD)" → "revenue_usd"
    GoogleSheetsReadOperator(
        task_id="read_normalized",
        gcp_conn_id=GCP_CONN_ID,
        spreadsheet_id=SPREADSHEET_ID,
        normalize_headers=True,
    )

    # Both: transliterate + normalize
    GoogleSheetsReadOperator(
        task_id="read_translit_and_normalized",
        gcp_conn_id=GCP_CONN_ID,
        spreadsheet_id=SPREADSHEET_ID,
        transliterate_headers=True,
        normalize_headers=True,
    )


sheets_read_headers()
