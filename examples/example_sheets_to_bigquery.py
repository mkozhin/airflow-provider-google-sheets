"""Example DAGs for Google Sheets → BigQuery pipelines.

Demonstrates three patterns:
1. Overwrite — full replace of a BigQuery table
2. Append — add rows to BigQuery
3. Update by date range — delete+insert for a date period read from Sheets
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)

from airflow_provider_google_sheets.operators.read import GoogleSheetsReadOperator

SPREADSHEET_ID = "your-spreadsheet-id-here"
GCP_CONN_ID = "google_cloud_default"
BQ_PROJECT = "your-gcp-project"
BQ_DATASET = "your_dataset"
BQ_TABLE = "your_table"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}


# ---------------------------------------------------------------------------
# DAG 1: Sheets → BigQuery (overwrite)
# ---------------------------------------------------------------------------
with DAG(
    dag_id="example_sheets_to_bq_overwrite",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["google-sheets", "bigquery", "example"],
) as dag_overwrite:

    read_sheets = GoogleSheetsReadOperator(
        task_id="read_from_sheets",
        gcp_conn_id=GCP_CONN_ID,
        spreadsheet_id=SPREADSHEET_ID,
        sheet_name="SalesData",
        schema={
            "date": {"type": "date", "format": "%Y-%m-%d"},
            "revenue": {"type": "float"},
            "quantity": {"type": "int"},
        },
    )

    def build_bq_rows(**context):
        """Convert list[dict] from XCom to newline-delimited JSON for BQ load."""
        import json
        rows = context["ti"].xcom_pull(task_ids="read_from_sheets")
        ndjson_path = "/tmp/sheets_to_bq.jsonl"
        with open(ndjson_path, "w") as f:
            for row in rows:
                # Convert date objects to strings for JSON serialization
                row_copy = {k: str(v) if hasattr(v, "isoformat") else v for k, v in row.items()}
                f.write(json.dumps(row_copy) + "\n")
        return ndjson_path

    prepare = PythonOperator(
        task_id="prepare_bq_load",
        python_callable=build_bq_rows,
    )

    load_bq = BigQueryInsertJobOperator(
        task_id="load_to_bigquery",
        gcp_conn_id=GCP_CONN_ID,
        configuration={
            "load": {
                "sourceUris": ["{{ ti.xcom_pull(task_ids='prepare_bq_load') }}"],
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

    read_sheets >> prepare >> load_bq


# ---------------------------------------------------------------------------
# DAG 2: Sheets → BigQuery (append)
# ---------------------------------------------------------------------------
with DAG(
    dag_id="example_sheets_to_bq_append",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["google-sheets", "bigquery", "example"],
) as dag_append:

    read_new_rows = GoogleSheetsReadOperator(
        task_id="read_new_rows",
        gcp_conn_id=GCP_CONN_ID,
        spreadsheet_id=SPREADSHEET_ID,
        sheet_name="NewEntries",
    )

    def prepare_append_rows(**context):
        import json
        rows = context["ti"].xcom_pull(task_ids="read_new_rows")
        path = "/tmp/sheets_append.jsonl"
        with open(path, "w") as f:
            for row in rows:
                row_copy = {k: str(v) if hasattr(v, "isoformat") else v for k, v in row.items()}
                f.write(json.dumps(row_copy) + "\n")
        return path

    prepare = PythonOperator(
        task_id="prepare_append",
        python_callable=prepare_append_rows,
    )

    append_bq = BigQueryInsertJobOperator(
        task_id="append_to_bigquery",
        gcp_conn_id=GCP_CONN_ID,
        configuration={
            "load": {
                "sourceUris": ["{{ ti.xcom_pull(task_ids='prepare_append') }}"],
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

    read_new_rows >> prepare >> append_bq


# ---------------------------------------------------------------------------
# DAG 3: Sheets → BigQuery (update by date range)
# ---------------------------------------------------------------------------
with DAG(
    dag_id="example_sheets_to_bq_date_update",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["google-sheets", "bigquery", "example"],
) as dag_date_update:

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

    def compute_date_range(**context):
        """Determine min/max date from the data read from Sheets."""
        rows = context["ti"].xcom_pull(task_ids="read_sheets_data")
        dates = [row["date"] for row in rows if row.get("date")]
        date_strings = [str(d) for d in dates]
        min_date = min(date_strings)
        max_date = max(date_strings)
        context["ti"].xcom_push(key="min_date", value=min_date)
        context["ti"].xcom_push(key="max_date", value=max_date)
        return {"min_date": min_date, "max_date": max_date}

    calc_range = PythonOperator(
        task_id="compute_date_range",
        python_callable=compute_date_range,
    )

    # Delete existing rows in BigQuery for the date range
    delete_period = BigQueryInsertJobOperator(
        task_id="delete_existing_period",
        gcp_conn_id=GCP_CONN_ID,
        configuration={
            "query": {
                "query": f"""
                    DELETE FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}`
                    WHERE date BETWEEN '{{{{ ti.xcom_pull(task_ids="compute_date_range", key="min_date") }}}}'
                      AND '{{{{ ti.xcom_pull(task_ids="compute_date_range", key="max_date") }}}}'
                """,
                "useLegacySql": False,
            }
        },
    )

    def prepare_insert_rows(**context):
        import json
        rows = context["ti"].xcom_pull(task_ids="read_sheets_data")
        path = "/tmp/sheets_date_update.jsonl"
        with open(path, "w") as f:
            for row in rows:
                row_copy = {k: str(v) if hasattr(v, "isoformat") else v for k, v in row.items()}
                f.write(json.dumps(row_copy) + "\n")
        return path

    prepare = PythonOperator(
        task_id="prepare_insert",
        python_callable=prepare_insert_rows,
    )

    insert_new = BigQueryInsertJobOperator(
        task_id="insert_new_data",
        gcp_conn_id=GCP_CONN_ID,
        configuration={
            "load": {
                "sourceUris": ["{{ ti.xcom_pull(task_ids='prepare_insert') }}"],
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

    read_data >> calc_range >> delete_period >> prepare >> insert_new
