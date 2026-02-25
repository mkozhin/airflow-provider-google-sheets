"""Example DAGs for reading data from Google Sheets.

Demonstrates various GoogleSheetsReadOperator configurations:
- Basic XCom read (with/without headers)
- Schema-based type conversion
- Streaming to CSV/JSON files
- Header transliteration and normalization
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow_provider_google_sheets.operators.read import GoogleSheetsReadOperator

SPREADSHEET_ID = "your-spreadsheet-id-here"
GCP_CONN_ID = "google_cloud_default"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}


# ---------------------------------------------------------------------------
# DAG 1: Basic read to XCom
# ---------------------------------------------------------------------------
with DAG(
    dag_id="example_sheets_read_basic",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["google-sheets", "example"],
) as dag_basic:

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

    def print_data(**context):
        data = context["ti"].xcom_pull(task_ids="read_with_headers")
        print(f"Read {len(data)} rows")
        for row in data[:5]:
            print(row)

    show = PythonOperator(task_id="show_data", python_callable=print_data)

    read_with_headers >> show


# ---------------------------------------------------------------------------
# DAG 2: Read with schema type conversion
# ---------------------------------------------------------------------------
with DAG(
    dag_id="example_sheets_read_schema",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["google-sheets", "example"],
) as dag_schema:

    read_typed = GoogleSheetsReadOperator(
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


# ---------------------------------------------------------------------------
# DAG 3: Streaming to CSV / JSON files
# ---------------------------------------------------------------------------
with DAG(
    dag_id="example_sheets_read_streaming",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["google-sheets", "example"],
) as dag_streaming:

    # Stream to CSV — no memory accumulation for large sheets
    stream_csv = GoogleSheetsReadOperator(
        task_id="stream_to_csv",
        gcp_conn_id=GCP_CONN_ID,
        spreadsheet_id=SPREADSHEET_ID,
        sheet_name="LargeData",
        output_type="csv",
        output_path="/tmp/sheets_export.csv",
        chunk_size=10000,
    )

    # Stream to JSON
    stream_json = GoogleSheetsReadOperator(
        task_id="stream_to_json",
        gcp_conn_id=GCP_CONN_ID,
        spreadsheet_id=SPREADSHEET_ID,
        sheet_name="LargeData",
        output_type="json",
        output_path="/tmp/sheets_export.json",
        chunk_size=10000,
    )

    # XCom with row limit protection
    read_safe = GoogleSheetsReadOperator(
        task_id="read_xcom_with_limit",
        gcp_conn_id=GCP_CONN_ID,
        spreadsheet_id=SPREADSHEET_ID,
        sheet_name="LargeData",
        max_xcom_rows=10_000,
    )


# ---------------------------------------------------------------------------
# DAG 4: Header processing (transliteration, normalization)
# ---------------------------------------------------------------------------
with DAG(
    dag_id="example_sheets_read_headers",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["google-sheets", "example"],
) as dag_headers:

    # Transliterate Cyrillic headers to Latin
    # "Дата" → "data", "Выручка" → "vyruchka"
    read_translit = GoogleSheetsReadOperator(
        task_id="read_transliterated",
        gcp_conn_id=GCP_CONN_ID,
        spreadsheet_id=SPREADSHEET_ID,
        transliterate_headers=True,
    )

    # Normalize headers to snake_case
    # "Revenue (USD)" → "revenue_usd"
    read_normalized = GoogleSheetsReadOperator(
        task_id="read_normalized",
        gcp_conn_id=GCP_CONN_ID,
        spreadsheet_id=SPREADSHEET_ID,
        normalize_headers=True,
    )

    # Both: transliterate + normalize
    read_both = GoogleSheetsReadOperator(
        task_id="read_translit_and_normalized",
        gcp_conn_id=GCP_CONN_ID,
        spreadsheet_id=SPREADSHEET_ID,
        transliterate_headers=True,
        normalize_headers=True,
    )