"""Example DAG for managing Google Sheets spreadsheets and sheets.

Demonstrates:
- Creating a new spreadsheet with multiple sheets
- Adding a sheet to an existing spreadsheet
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow_provider_google_sheets.operators.manage import (
    GoogleSheetsCreateSheetOperator,
    GoogleSheetsCreateSpreadsheetOperator,
)

GCP_CONN_ID = "google_cloud_default"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="example_sheets_manage",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["google-sheets", "example"],
) as dag:

    # Create a new spreadsheet with predefined sheets
    create_spreadsheet = GoogleSheetsCreateSpreadsheetOperator(
        task_id="create_spreadsheet",
        gcp_conn_id=GCP_CONN_ID,
        title="Monthly Report - {{ ds }}",
        sheet_titles=["Summary", "Details", "Charts"],
    )

    # Add another sheet to the newly created spreadsheet
    # spreadsheet_id is pulled from XCom of the previous task
    add_sheet = GoogleSheetsCreateSheetOperator(
        task_id="add_archive_sheet",
        gcp_conn_id=GCP_CONN_ID,
        spreadsheet_id="{{ ti.xcom_pull(task_ids='create_spreadsheet') }}",
        sheet_title="Archive",
    )

    def log_result(**context):
        sid = context["ti"].xcom_pull(task_ids="create_spreadsheet")
        print(f"Created spreadsheet: https://docs.google.com/spreadsheets/d/{sid}")

    log = PythonOperator(task_id="log_url", python_callable=log_result)

    create_spreadsheet >> add_sheet >> log
