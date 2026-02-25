"""Example DAGs for Google Sheets smart merge mode.

Demonstrates GoogleSheetsWriteOperator with write_mode='smart_merge':
- Update existing rows by key
- Insert new rows for an existing key
- Delete surplus rows
- Append rows for new keys
- Combined scenario
- Usage with has_headers=False
"""

from datetime import datetime

from airflow.decorators import dag

from airflow_provider_google_sheets.operators.write import GoogleSheetsWriteOperator

SPREADSHEET_ID = "your-spreadsheet-id-here"
GCP_CONN_ID = "google_cloud_default"


# ---------------------------------------------------------------------------
# DAG 1: Basic smart merge — update by date key
# ---------------------------------------------------------------------------
@dag(
    dag_id="example_sheets_smart_merge_basic",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["google-sheets", "example", "smart-merge"],
)
def sheets_smart_merge_basic():
    # Sheet has:
    #   date       | revenue | region
    #   2024-01-01 | 1000    | US
    #   2024-01-02 | 1500    | EU
    #
    # Incoming data updates 2024-01-01 and adds 2024-01-03:
    GoogleSheetsWriteOperator(
        task_id="merge_by_date",
        gcp_conn_id=GCP_CONN_ID,
        spreadsheet_id=SPREADSHEET_ID,
        sheet_name="Sales",
        write_mode="smart_merge",
        merge_key="date",
        data=[
            {"date": "2024-01-01", "revenue": 1100, "region": "US"},  # update
            {"date": "2024-01-03", "revenue": 2000, "region": "APAC"},  # new → append
        ],
    )


sheets_smart_merge_basic()


# ---------------------------------------------------------------------------
# DAG 2: Smart merge with multiple rows per key (insert/delete)
# ---------------------------------------------------------------------------
@dag(
    dag_id="example_sheets_smart_merge_multi_row",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["google-sheets", "example", "smart-merge"],
)
def sheets_smart_merge_multi_row():
    # Sheet has 2 rows for key "US", incoming has 4 → insert 2 extra rows
    # Sheet has 3 rows for key "EU", incoming has 1 → delete 2 surplus rows
    GoogleSheetsWriteOperator(
        task_id="merge_multi_row_keys",
        gcp_conn_id=GCP_CONN_ID,
        spreadsheet_id=SPREADSHEET_ID,
        sheet_name="RegionData",
        write_mode="smart_merge",
        merge_key="region",
        data=[
            {"region": "US", "city": "New York", "sales": 500},
            {"region": "US", "city": "Los Angeles", "sales": 300},
            {"region": "US", "city": "Chicago", "sales": 200},
            {"region": "US", "city": "Houston", "sales": 150},
            {"region": "EU", "city": "London", "sales": 800},
        ],
        batch_size=500,
        pause_between_batches=1.0,
    )


sheets_smart_merge_multi_row()


# ---------------------------------------------------------------------------
# DAG 3: Combined scenario — insert + delete + update + append
# ---------------------------------------------------------------------------
@dag(
    dag_id="example_sheets_smart_merge_combined",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["google-sheets", "example", "smart-merge"],
)
def sheets_smart_merge_combined():
    # Existing sheet:
    #   id | name   | status
    #   A  | Alice  | active     ← will be updated
    #   B  | Bob    | active     ← key B has 1 row, incoming has 3 → insert 2
    #   B  | -      | -          ← (doesn't exist yet, will be inserted)
    #   C  | Charlie| inactive   ← key C has 2 rows, incoming has 1 → delete 1
    #   C  | Chris  | active     ← (will be deleted)
    #   D  | Dave   | active     ← not in incoming → left as-is
    #                              key E is new → append

    GoogleSheetsWriteOperator(
        task_id="merge_combined",
        gcp_conn_id=GCP_CONN_ID,
        spreadsheet_id=SPREADSHEET_ID,
        sheet_name="Users",
        write_mode="smart_merge",
        merge_key="id",
        data=[
            {"id": "A", "name": "Alice Updated", "status": "active"},
            {"id": "B", "name": "Bob", "status": "active"},
            {"id": "B", "name": "Barbara", "status": "pending"},
            {"id": "B", "name": "Ben", "status": "inactive"},
            {"id": "C", "name": "Charlie", "status": "active"},
            {"id": "E", "name": "Eve", "status": "new"},
        ],
    )


sheets_smart_merge_combined()


# ---------------------------------------------------------------------------
# DAG 4: Smart merge without headers (has_headers=False)
# ---------------------------------------------------------------------------
@dag(
    dag_id="example_sheets_smart_merge_no_headers",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["google-sheets", "example", "smart-merge"],
)
def sheets_smart_merge_no_headers():
    # When has_headers=False, row 1 is treated as data (not header).
    # merge_key still references the column name from the incoming data headers.
    GoogleSheetsWriteOperator(
        task_id="merge_no_headers",
        gcp_conn_id=GCP_CONN_ID,
        spreadsheet_id=SPREADSHEET_ID,
        sheet_name="RawData",
        write_mode="smart_merge",
        merge_key="key",
        has_headers=False,
        data=[
            {"key": "X", "value": 100},
            {"key": "Y", "value": 200},
        ],
    )


sheets_smart_merge_no_headers()
