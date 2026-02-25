"""Example DAGs for writing data to Google Sheets.

Demonstrates GoogleSheetsWriteOperator in overwrite and append modes:
- Overwrite with cell_range
- Append
- Data from XCom, CSV file, JSON file, list[dict]
- Schema formatting on write
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow_provider_google_sheets.operators.write import GoogleSheetsWriteOperator

SPREADSHEET_ID = "your-spreadsheet-id-here"
GCP_CONN_ID = "google_cloud_default"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}


# ---------------------------------------------------------------------------
# DAG 1: Overwrite mode
# ---------------------------------------------------------------------------
with DAG(
    dag_id="example_sheets_write_overwrite",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["google-sheets", "example"],
) as dag_overwrite:

    # Write list[dict] data — headers auto-detected from keys
    write_dicts = GoogleSheetsWriteOperator(
        task_id="write_from_dicts",
        gcp_conn_id=GCP_CONN_ID,
        spreadsheet_id=SPREADSHEET_ID,
        sheet_name="Output",
        write_mode="overwrite",
        data=[
            {"date": "2024-01-01", "revenue": 1000, "region": "US"},
            {"date": "2024-01-02", "revenue": 1500, "region": "EU"},
        ],
    )

    # Write to a specific range (e.g. starting from B2)
    write_range = GoogleSheetsWriteOperator(
        task_id="write_to_range",
        gcp_conn_id=GCP_CONN_ID,
        spreadsheet_id=SPREADSHEET_ID,
        sheet_name="Output",
        cell_range="B2:D",
        write_mode="overwrite",
        data=[
            {"col1": "a", "col2": "b", "col3": "c"},
            {"col1": "d", "col2": "e", "col3": "f"},
        ],
    )

    # Write list[list] data without headers
    write_raw = GoogleSheetsWriteOperator(
        task_id="write_raw_lists",
        gcp_conn_id=GCP_CONN_ID,
        spreadsheet_id=SPREADSHEET_ID,
        sheet_name="RawOutput",
        write_mode="overwrite",
        has_headers=False,
        data=[[1, 2, 3], [4, 5, 6]],
    )

    # Write from a CSV file
    write_csv = GoogleSheetsWriteOperator(
        task_id="write_from_csv",
        gcp_conn_id=GCP_CONN_ID,
        spreadsheet_id=SPREADSHEET_ID,
        sheet_name="FromCSV",
        write_mode="overwrite",
        data="/tmp/data.csv",
    )

    # Batched write for large datasets
    write_batched = GoogleSheetsWriteOperator(
        task_id="write_large_batched",
        gcp_conn_id=GCP_CONN_ID,
        spreadsheet_id=SPREADSHEET_ID,
        sheet_name="LargeOutput",
        write_mode="overwrite",
        data="/tmp/large_data.csv",
        batch_size=500,
        pause_between_batches=2.0,
    )


# ---------------------------------------------------------------------------
# DAG 2: Append mode
# ---------------------------------------------------------------------------
with DAG(
    dag_id="example_sheets_write_append",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["google-sheets", "example"],
) as dag_append:

    append_rows = GoogleSheetsWriteOperator(
        task_id="append_new_rows",
        gcp_conn_id=GCP_CONN_ID,
        spreadsheet_id=SPREADSHEET_ID,
        sheet_name="Log",
        write_mode="append",
        data=[
            {"timestamp": "2024-01-15 10:00:00", "event": "login", "user": "alice"},
            {"timestamp": "2024-01-15 10:05:00", "event": "purchase", "user": "bob"},
        ],
    )


# ---------------------------------------------------------------------------
# DAG 3: Write from XCom (read → transform → write pipeline)
# ---------------------------------------------------------------------------
with DAG(
    dag_id="example_sheets_read_transform_write",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["google-sheets", "example"],
) as dag_pipeline:

    def generate_report(**context):
        """Generate report data and push to XCom."""
        report = [
            {"metric": "total_users", "value": 42000},
            {"metric": "active_users", "value": 15000},
            {"metric": "revenue", "value": 125000.50},
        ]
        return report

    generate = PythonOperator(
        task_id="generate_report",
        python_callable=generate_report,
    )

    write_report = GoogleSheetsWriteOperator(
        task_id="write_report_to_sheets",
        gcp_conn_id=GCP_CONN_ID,
        spreadsheet_id=SPREADSHEET_ID,
        sheet_name="DailyReport",
        write_mode="overwrite",
        data_xcom_task_id="generate_report",
    )

    generate >> write_report


# ---------------------------------------------------------------------------
# DAG 4: Write with schema formatting
# ---------------------------------------------------------------------------
with DAG(
    dag_id="example_sheets_write_schema",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["google-sheets", "example"],
) as dag_schema:

    write_formatted = GoogleSheetsWriteOperator(
        task_id="write_with_schema",
        gcp_conn_id=GCP_CONN_ID,
        spreadsheet_id=SPREADSHEET_ID,
        sheet_name="Formatted",
        write_mode="overwrite",
        schema={
            "date": {"type": "date", "format": "%d.%m.%Y"},
            "amount": {"type": "float"},
            "is_paid": {"type": "bool"},
        },
        data=[
            {"date": "2024-01-15", "amount": 99.99, "is_paid": True},
            {"date": "2024-01-16", "amount": 150.00, "is_paid": False},
        ],
    )