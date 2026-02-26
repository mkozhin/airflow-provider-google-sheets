"""Example DAGs for Google Sheets → BigQuery pipelines.

Demonstrates three patterns:
1. Overwrite — full replace of a BigQuery table (stream via JSONL)
2. Append — add rows to BigQuery (stream via JSONL)
3. Update by date range — delete+insert for a date period read from Sheets
"""

from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)

from airflow_provider_google_sheets.operators.read import GoogleSheetsReadOperator

SPREADSHEET_ID = "your-spreadsheet-id-here"
GCP_CONN_ID = "google_cloud_default"
BQ_PROJECT = "your-gcp-project"
BQ_DATASET = "your_dataset"
BQ_TABLE = "your_table"


# ---------------------------------------------------------------------------
# DAG 1: Sheets → BigQuery (overwrite)
# ---------------------------------------------------------------------------
@dag(
    dag_id="example_sheets_to_bq_overwrite",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["google-sheets", "bigquery", "example"],
)
def sheets_to_bq_overwrite():
    # Stream directly to JSONL file — no XCom, no memory accumulation
    read_sheets = GoogleSheetsReadOperator(
        task_id="read_from_sheets",
        gcp_conn_id=GCP_CONN_ID,
        spreadsheet_id=SPREADSHEET_ID,
        sheet_name="SalesData",
        output_type="jsonl",
        output_path="/tmp/sheets_to_bq.jsonl",
        schema={
            "date": {"type": "date", "format": "%Y-%m-%d"},
            "revenue": {"type": "float"},
            "quantity": {"type": "int"},
        },
    )

    load_bq = BigQueryInsertJobOperator(
        task_id="load_to_bigquery",
        gcp_conn_id=GCP_CONN_ID,
        configuration={
            "load": {
                "sourceUris": ["{{ ti.xcom_pull(task_ids='read_from_sheets') }}"],
                "destinationTable": {
                    "projectId": BQ_PROJECT,
                    "datasetId": BQ_DATASET,
                    "tableId": BQ_TABLE,
                },
                "sourceFormat": "NEWLINE_DELIMITED_JSON",
                "writeDisposition": "WRITE_TRUNCATE",
                "autodetect": True,
            }
        },
    )

    read_sheets >> load_bq


sheets_to_bq_overwrite()


# ---------------------------------------------------------------------------
# DAG 2: Sheets → BigQuery (append)
# ---------------------------------------------------------------------------
@dag(
    dag_id="example_sheets_to_bq_append",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["google-sheets", "bigquery", "example"],
)
def sheets_to_bq_append():
    # Stream directly to JSONL file
    read_new_rows = GoogleSheetsReadOperator(
        task_id="read_new_rows",
        gcp_conn_id=GCP_CONN_ID,
        spreadsheet_id=SPREADSHEET_ID,
        sheet_name="NewEntries",
        output_type="jsonl",
        output_path="/tmp/sheets_append.jsonl",
    )

    append_bq = BigQueryInsertJobOperator(
        task_id="append_to_bigquery",
        gcp_conn_id=GCP_CONN_ID,
        configuration={
            "load": {
                "sourceUris": ["{{ ti.xcom_pull(task_ids='read_new_rows') }}"],
                "destinationTable": {
                    "projectId": BQ_PROJECT,
                    "datasetId": BQ_DATASET,
                    "tableId": BQ_TABLE,
                },
                "sourceFormat": "NEWLINE_DELIMITED_JSON",
                "writeDisposition": "WRITE_APPEND",
                "autodetect": True,
            }
        },
    )

    read_new_rows >> append_bq


sheets_to_bq_append()


# ---------------------------------------------------------------------------
# DAG 3: Sheets → BigQuery (update by date range)
# ---------------------------------------------------------------------------
@dag(
    dag_id="example_sheets_to_bq_date_update",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["google-sheets", "bigquery", "example"],
)
def sheets_to_bq_date_update():
    # Read to XCom to compute date range, then stream to JSONL for BQ load
    read_data = GoogleSheetsReadOperator(
        task_id="read_sheets_data",
        gcp_conn_id=GCP_CONN_ID,
        spreadsheet_id=SPREADSHEET_ID,
        sheet_name="DailyMetrics",
        schema={
            "date": {"type": "date", "format": "%Y-%m-%d"},
            "metric": {"type": "str"},
            "value": {"type": "float"},
        },
    )

    @task
    def compute_date_range_and_export(**context):
        """Determine min/max date and write JSONL file for BQ load."""
        import json

        rows = context["ti"].xcom_pull(task_ids="read_sheets_data")
        dates = [row["date"] for row in rows if row.get("date")]
        date_strings = [str(d) for d in dates]
        min_date = min(date_strings)
        max_date = max(date_strings)
        context["ti"].xcom_push(key="min_date", value=min_date)
        context["ti"].xcom_push(key="max_date", value=max_date)

        path = "/tmp/sheets_date_update.jsonl"
        with open(path, "w") as f:
            for row in rows:
                row_copy = {k: str(v) if hasattr(v, "isoformat") else v for k, v in row.items()}
                json.dump(row_copy, f, ensure_ascii=False)
                f.write("\n")
        return path

    # Delete existing rows in BigQuery for the date range
    delete_period = BigQueryInsertJobOperator(
        task_id="delete_existing_period",
        gcp_conn_id=GCP_CONN_ID,
        configuration={
            "query": {
                "query": f"""
                    DELETE FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}`
                    WHERE date BETWEEN '{{{{ ti.xcom_pull(task_ids="compute_date_range_and_export", key="min_date") }}}}'
                      AND '{{{{ ti.xcom_pull(task_ids="compute_date_range_and_export", key="max_date") }}}}'
                """,
                "useLegacySql": False,
            }
        },
    )

    insert_new = BigQueryInsertJobOperator(
        task_id="insert_new_data",
        gcp_conn_id=GCP_CONN_ID,
        configuration={
            "load": {
                "sourceUris": ["{{ ti.xcom_pull(task_ids='compute_date_range_and_export') }}"],
                "destinationTable": {
                    "projectId": BQ_PROJECT,
                    "datasetId": BQ_DATASET,
                    "tableId": BQ_TABLE,
                },
                "sourceFormat": "NEWLINE_DELIMITED_JSON",
                "writeDisposition": "WRITE_APPEND",
                "autodetect": True,
            }
        },
    )

    read_data >> compute_date_range_and_export() >> delete_period >> insert_new


sheets_to_bq_date_update()
