"""Example DAGs for Google Sheets → BigQuery pipelines.

Demonstrates three patterns:
1. Overwrite — full replace of a BigQuery table (stream via JSONL)
2. Append — add rows to BigQuery (stream via JSONL)
3. Update by date range — delete+insert for a date period read from Sheets

Each pattern:
  - streams Sheets data to a local JSONL file (no XCom memory pressure)
  - uploads the file to GCS with LocalFilesystemToGCSOperator
  - runs a BigQuery Load Job against the GCS URI

The local file path is defined once per DAG as a constant; downstream tasks
derive the GCS object name from the XCom-returned path via Jinja, so nothing
is duplicated or hardcoded inside task bodies.
"""

from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

from airflow_provider_google_sheets.operators.read import GoogleSheetsReadOperator

SPREADSHEET_ID = "your-spreadsheet-id-here"
GCP_CONN_ID = "google_cloud_default"
BQ_PROJECT = "your-gcp-project"
BQ_DATASET = "your_dataset"
BQ_TABLE = "your_table"
GCS_BUCKET = "your-gcs-bucket"
GCS_TMP_PREFIX = "airflow/sheets-to-bq"


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
    local_file = "/tmp/sheets_to_bq.jsonl"

    read_sheets = GoogleSheetsReadOperator(
        task_id="read_from_sheets",
        gcp_conn_id=GCP_CONN_ID,
        spreadsheet_id=SPREADSHEET_ID,
        sheet_name="SalesData",
        output_type="jsonl",
        output_path=local_file,
        schema={
            "date": {"type": "date", "format": "%Y-%m-%d"},
            "revenue": {"type": "float"},
            "quantity": {"type": "int"},
        },
    )

    # Upload local JSONL to GCS; filename derived from the path returned by read task.
    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_to_gcs",
        gcp_conn_id=GCP_CONN_ID,
        src="{{ ti.xcom_pull(task_ids='read_from_sheets') }}",
        dst=GCS_TMP_PREFIX + "/{{ ti.xcom_pull(task_ids='read_from_sheets').split('/')[-1] }}",
        bucket=GCS_BUCKET,
    )

    load_bq = BigQueryInsertJobOperator(
        task_id="load_to_bigquery",
        gcp_conn_id=GCP_CONN_ID,
        configuration={
            "load": {
                "sourceUris": [
                    "gs://" + GCS_BUCKET + "/" + GCS_TMP_PREFIX
                    + "/{{ ti.xcom_pull(task_ids='read_from_sheets').split('/')[-1] }}"
                ],
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

    read_sheets >> upload_to_gcs >> load_bq


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
    local_file = "/tmp/sheets_append.jsonl"

    read_new_rows = GoogleSheetsReadOperator(
        task_id="read_new_rows",
        gcp_conn_id=GCP_CONN_ID,
        spreadsheet_id=SPREADSHEET_ID,
        sheet_name="NewEntries",
        output_type="jsonl",
        output_path=local_file,
    )

    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_to_gcs",
        gcp_conn_id=GCP_CONN_ID,
        src="{{ ti.xcom_pull(task_ids='read_new_rows') }}",
        dst=GCS_TMP_PREFIX + "/{{ ti.xcom_pull(task_ids='read_new_rows').split('/')[-1] }}",
        bucket=GCS_BUCKET,
    )

    append_bq = BigQueryInsertJobOperator(
        task_id="append_to_bigquery",
        gcp_conn_id=GCP_CONN_ID,
        configuration={
            "load": {
                "sourceUris": [
                    "gs://" + GCS_BUCKET + "/" + GCS_TMP_PREFIX
                    + "/{{ ti.xcom_pull(task_ids='read_new_rows').split('/')[-1] }}"
                ],
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

    read_new_rows >> upload_to_gcs >> append_bq


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
    # Read to XCom — needed to compute the date range for the DELETE query.
    # Dates are kept as strings (type "str") intentionally: Google Sheets
    # already stores them in ISO format, and datetime.date objects are not
    # safely serializable by all Airflow XCom backends.
    read_data = GoogleSheetsReadOperator(
        task_id="read_sheets_data",
        gcp_conn_id=GCP_CONN_ID,
        spreadsheet_id=SPREADSHEET_ID,
        sheet_name="DailyMetrics",
        schema={
            "date": {"type": "str"},
            "metric": {"type": "str"},
            "value": {"type": "float"},
        },
    )

    @task
    def compute_date_range_and_export(output_path: str, **context):
        """Determine min/max date from XCom rows and write JSONL for BQ load.

        Returns the local file path so downstream tasks can derive the GCS URI.
        """
        import json

        rows = context["ti"].xcom_pull(task_ids="read_sheets_data")
        dates = [row["date"] for row in rows if row.get("date")]
        date_strings = [str(d) for d in dates]
        context["ti"].xcom_push(key="min_date", value=min(date_strings))
        context["ti"].xcom_push(key="max_date", value=max(date_strings))

        with open(output_path, "w") as f:
            for row in rows:
                row_copy = {k: str(v) if hasattr(v, "isoformat") else v for k, v in row.items()}
                json.dump(row_copy, f, ensure_ascii=False)
                f.write("\n")
        return output_path

    local_file = "/tmp/sheets_date_update.jsonl"
    compute_task = compute_date_range_and_export(output_path=local_file)

    # Upload only after successful export — avoids deleting BQ data before
    # the replacement file is confirmed in GCS.
    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_to_gcs",
        gcp_conn_id=GCP_CONN_ID,
        src="{{ ti.xcom_pull(task_ids='compute_date_range_and_export') }}",
        dst=GCS_TMP_PREFIX + "/{{ ti.xcom_pull(task_ids='compute_date_range_and_export').split('/')[-1] }}",
        bucket=GCS_BUCKET,
    )

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
                "sourceUris": [
                    "gs://" + GCS_BUCKET + "/" + GCS_TMP_PREFIX
                    + "/{{ ti.xcom_pull(task_ids='compute_date_range_and_export').split('/')[-1] }}"
                ],
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

    read_data >> compute_task >> upload_to_gcs >> delete_period >> insert_new


sheets_to_bq_date_update()
