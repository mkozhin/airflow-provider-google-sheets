"""Example DAG: arbitrary BigQuery SQL query → local JSON → Google Sheets.

Pattern:
1. Execute SQL query via cursor and stream results to a local JSONL file
   (constant ~10 MB RAM regardless of result size)
2. Write the JSON file to Google Sheets

Memory usage stays constant because the cursor fetches data in pages
(~10 MB / ~10k rows each) and each chunk is written to disk immediately.
"""

from datetime import datetime

from airflow.decorators import dag, task

from airflow_provider_google_sheets.operators.write import GoogleSheetsWriteOperator

SPREADSHEET_ID = "your-spreadsheet-id-here"
GCP_CONN_ID = "google_cloud_default"
BQ_PROJECT = "your-gcp-project"

# Your SQL query — change this to whatever you need
SQL_QUERY = f"""
    SELECT
        o.order_id,
        o.order_date,
        c.customer_name,
        o.total_amount,
        o.status
    FROM `{BQ_PROJECT}.sales.orders` o
    JOIN `{BQ_PROJECT}.sales.customers` c
        ON o.customer_id = c.customer_id
    WHERE o.order_date >= '2024-01-01'
    ORDER BY o.order_date DESC
"""

FETCH_CHUNK_SIZE = 5000


@dag(
    dag_id="example_bq_query_to_sheets",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["google-sheets", "bigquery", "example"],
)
def bq_query_to_sheets():

    # Step 1 — run query and stream results to local JSONL file
    @task
    def run_query_to_json() -> str:
        """Execute SQL query and stream results to a local JSONL file.

        Uses cursor.fetchmany() to read data in chunks.  Only one chunk
        (~5k rows) is held in memory at a time — RAM stays at ~10 MB
        regardless of the total number of rows.

        The file is opened with mode="w", so on retry it is overwritten
        from scratch — no duplicates.
        """
        import json
        import logging

        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

        log = logging.getLogger(__name__)

        hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(SQL_QUERY)

        columns = [desc[0] for desc in cursor.description]
        local_path = "/tmp/bq_query_result.json"
        row_count = 0

        with open(local_path, "w", encoding="utf-8") as f:
            while True:
                chunk = cursor.fetchmany(FETCH_CHUNK_SIZE)
                if not chunk:
                    break
                for row in chunk:
                    json.dump(
                        dict(zip(columns, row)),
                        f,
                        ensure_ascii=False,
                        default=str,
                    )
                    f.write("\n")
                row_count += len(chunk)

        log.info("Wrote %d rows (%d columns) to %s", row_count, len(columns), local_path)
        return local_path

    # Step 2 — write JSON file to Google Sheets
    write_to_sheets = GoogleSheetsWriteOperator(
        task_id="write_to_sheets",
        gcp_conn_id=GCP_CONN_ID,
        spreadsheet_id=SPREADSHEET_ID,
        sheet_name="BQ Query Results",
        write_mode="overwrite",
        data="{{ ti.xcom_pull(task_ids='run_query_to_json') }}",
        batch_size=1000,
        pause_between_batches=1.0,
    )

    run_query_to_json() >> write_to_sheets


bq_query_to_sheets()
