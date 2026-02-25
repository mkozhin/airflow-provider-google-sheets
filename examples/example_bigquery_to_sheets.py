"""Example DAGs for BigQuery → Google Sheets pipelines.

Demonstrates two patterns:
1. Overwrite — read from BigQuery, write to Sheets (full replace)
2. Smart merge by date — read from BigQuery, update Sheets by date key

For large datasets (>50k rows), uses CSV streaming via GCS.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import (
    BigQueryToGCSOperator,
)

from airflow_provider_google_sheets.operators.write import GoogleSheetsWriteOperator

SPREADSHEET_ID = "your-spreadsheet-id-here"
GCP_CONN_ID = "google_cloud_default"
BQ_PROJECT = "your-gcp-project"
BQ_DATASET = "your_dataset"
BQ_TABLE = "your_table"
GCS_BUCKET = "your-temp-bucket"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}


# ---------------------------------------------------------------------------
# DAG 1: BigQuery → Sheets (overwrite, small dataset via XCom)
# ---------------------------------------------------------------------------
with DAG(
    dag_id="example_bq_to_sheets_overwrite_small",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["google-sheets", "bigquery", "example"],
) as dag_small:

    query_bq = BigQueryInsertJobOperator(
        task_id="query_bigquery",
        gcp_conn_id=GCP_CONN_ID,
        configuration={
            "query": {
                "query": f"""
                    SELECT date, region, revenue, quantity
                    FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}`
                    WHERE date >= '2024-01-01'
                    ORDER BY date
                """,
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": BQ_PROJECT,
                    "datasetId": BQ_DATASET,
                    "tableId": "tmp_sheets_export",
                },
                "writeDisposition": "WRITE_TRUNCATE",
            }
        },
    )

    def fetch_query_results(**context):
        """Read results from the temp table via BigQuery API."""
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
        hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID)
        records = hook.get_records(
            f"SELECT * FROM `{BQ_PROJECT}.{BQ_DATASET}.tmp_sheets_export`"
        )
        # BigQuery returns list[tuple], convert to list[dict]
        columns = ["date", "region", "revenue", "quantity"]
        return [dict(zip(columns, row)) for row in records]

    fetch = PythonOperator(
        task_id="fetch_results",
        python_callable=fetch_query_results,
    )

    write_sheets = GoogleSheetsWriteOperator(
        task_id="write_to_sheets",
        gcp_conn_id=GCP_CONN_ID,
        spreadsheet_id=SPREADSHEET_ID,
        sheet_name="BQ Report",
        write_mode="overwrite",
        data_xcom_task_id="fetch_results",
        batch_size=1000,
        pause_between_batches=1.0,
    )

    query_bq >> fetch >> write_sheets


# ---------------------------------------------------------------------------
# DAG 2: BigQuery → Sheets (overwrite, large dataset via GCS+CSV)
# ---------------------------------------------------------------------------
with DAG(
    dag_id="example_bq_to_sheets_overwrite_large",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["google-sheets", "bigquery", "example"],
) as dag_large:

    # Export BigQuery table to CSV in GCS
    export_to_gcs = BigQueryToGCSOperator(
        task_id="export_to_gcs",
        gcp_conn_id=GCP_CONN_ID,
        source_project_dataset_table=f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}",
        destination_cloud_storage_uris=[f"gs://{GCS_BUCKET}/tmp/bq_export.csv"],
        export_format="CSV",
        print_header=True,
    )

    def download_from_gcs(**context):
        """Download CSV from GCS to local filesystem."""
        from airflow.providers.google.cloud.hooks.gcs import GCSHook
        hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
        local_path = "/tmp/bq_export.csv"
        hook.download(
            bucket_name=GCS_BUCKET,
            object_name="tmp/bq_export.csv",
            filename=local_path,
        )
        return local_path

    download = PythonOperator(
        task_id="download_csv",
        python_callable=download_from_gcs,
    )

    # Write CSV file to Google Sheets
    # GoogleSheetsWriteOperator reads CSV file directly when data is a path
    write_large = GoogleSheetsWriteOperator(
        task_id="write_large_to_sheets",
        gcp_conn_id=GCP_CONN_ID,
        spreadsheet_id=SPREADSHEET_ID,
        sheet_name="LargeExport",
        write_mode="overwrite",
        data="{{ ti.xcom_pull(task_ids='download_csv') }}",
        batch_size=500,
        pause_between_batches=2.0,
    )

    export_to_gcs >> download >> write_large


# ---------------------------------------------------------------------------
# DAG 3: BigQuery → Sheets (smart merge by date)
# ---------------------------------------------------------------------------
with DAG(
    dag_id="example_bq_to_sheets_smart_merge",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["google-sheets", "bigquery", "example", "smart-merge"],
) as dag_merge:

    def query_recent_data(**context):
        """Query BigQuery for recent data to merge into Sheets."""
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
        hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID)
        # Read last 30 days of data
        records = hook.get_records(f"""
            SELECT date, region, revenue, quantity
            FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}`
            WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
            ORDER BY date
        """)
        columns = ["date", "region", "revenue", "quantity"]
        return [dict(zip(columns, row)) for row in records]

    fetch_recent = PythonOperator(
        task_id="fetch_recent_data",
        python_callable=query_recent_data,
    )

    # Smart merge: existing rows with matching date are updated,
    # new dates are appended, dates removed from BQ are deleted from Sheets
    merge_to_sheets = GoogleSheetsWriteOperator(
        task_id="smart_merge_to_sheets",
        gcp_conn_id=GCP_CONN_ID,
        spreadsheet_id=SPREADSHEET_ID,
        sheet_name="Daily Metrics",
        write_mode="smart_merge",
        merge_key="date",
        data_xcom_task_id="fetch_recent_data",
        batch_size=500,
        pause_between_batches=1.0,
    )

    fetch_recent >> merge_to_sheets
