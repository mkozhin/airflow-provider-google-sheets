"""Operator for writing data to Google Sheets."""

from __future__ import annotations

import logging
import time
from collections import defaultdict
from typing import Any, Sequence

from airflow.models import BaseOperator

from airflow_google_sheets.hooks.google_sheets import GoogleSheetsHook
from airflow_google_sheets.utils.data_formats import normalize_input_data
from airflow_google_sheets.utils.schema import format_row_for_write

logger = logging.getLogger(__name__)


class GoogleSheetsWriteOperator(BaseOperator):
    """Write data to a Google Sheets spreadsheet.

    Supports three write modes:

    * ``overwrite`` — clear the target range and write new data.
    * ``append`` — append rows after the last occupied row.
    * ``smart_merge`` — update / insert / delete rows based on a key column.

    Args:
        gcp_conn_id: Airflow Connection ID.
        spreadsheet_id: Target spreadsheet ID.
        sheet_name: Target sheet (tab) name.
        cell_range: Target A1 range (used by *overwrite*).
        write_mode: ``"overwrite"``, ``"append"`` or ``"smart_merge"``.
        data: Inline data — ``list[list]``, ``list[dict]``, or a file path.
        data_xcom_task_id: Pull data from this task's XCom instead.
        data_xcom_key: XCom key when using *data_xcom_task_id*.
        has_headers: Whether *data* contains a header row.
        write_headers: Write the header row (for *overwrite* mode).
        schema: Optional column schema for formatting values before write.
        batch_size: Rows per API request.
        pause_between_batches: Seconds to wait between batches.
        merge_key: Column name used as the key for *smart_merge*.
    """

    template_fields: Sequence[str] = (
        "spreadsheet_id",
        "sheet_name",
        "cell_range",
        "data",
        "data_xcom_task_id",
    )

    def __init__(
        self,
        *,
        gcp_conn_id: str = "google_cloud_default",
        spreadsheet_id: str,
        sheet_name: str | None = None,
        cell_range: str | None = None,
        write_mode: str = "overwrite",
        data: Any = None,
        data_xcom_task_id: str | None = None,
        data_xcom_key: str = "return_value",
        has_headers: bool = True,
        write_headers: bool = True,
        schema: dict[str, dict] | None = None,
        batch_size: int = 1000,
        pause_between_batches: float = 1.0,
        merge_key: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.spreadsheet_id = spreadsheet_id
        self.sheet_name = sheet_name
        self.cell_range = cell_range
        self.write_mode = write_mode
        self.data = data
        self.data_xcom_task_id = data_xcom_task_id
        self.data_xcom_key = data_xcom_key
        self.has_headers = has_headers
        self.write_headers = write_headers
        self.schema = schema
        self.batch_size = batch_size
        self.pause_between_batches = pause_between_batches
        self.merge_key = merge_key

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    def _resolve_data(self, context: Any) -> tuple[list[str] | None, list[list[Any]]]:
        """Obtain and normalise input data from inline value, XCom or file."""
        raw = self.data
        if raw is None and self.data_xcom_task_id:
            ti = context["ti"]
            raw = ti.xcom_pull(task_ids=self.data_xcom_task_id, key=self.data_xcom_key)
        if raw is None:
            raise ValueError("No data provided: set 'data' or 'data_xcom_task_id'.")
        return normalize_input_data(raw, has_headers=self.has_headers)

    def _sheet_prefix(self) -> str:
        return f"{self.sheet_name}!" if self.sheet_name else ""

    def _format_rows(self, headers: list[str] | None, rows: list[list[Any]]) -> list[list[Any]]:
        """Apply schema formatting to rows if a schema is provided."""
        if not self.schema or not headers:
            return rows
        return [format_row_for_write(r, headers, self.schema) for r in rows]

    # ------------------------------------------------------------------
    # execute
    # ------------------------------------------------------------------

    def execute(self, context: Any) -> dict[str, Any]:
        hook = GoogleSheetsHook(gcp_conn_id=self.gcp_conn_id)
        headers, rows = self._resolve_data(context)
        rows = self._format_rows(headers, rows)

        if self.write_mode == "overwrite":
            return self._execute_overwrite(hook, headers, rows)
        if self.write_mode == "append":
            return self._execute_append(hook, headers, rows)
        if self.write_mode == "smart_merge":
            return self._execute_smart_merge(hook, headers, rows)

        raise ValueError(f"Unknown write_mode: '{self.write_mode}'")

    # ------------------------------------------------------------------
    # overwrite
    # ------------------------------------------------------------------

    def _execute_overwrite(
        self,
        hook: GoogleSheetsHook,
        headers: list[str] | None,
        rows: list[list[Any]],
    ) -> dict[str, Any]:
        prefix = self._sheet_prefix()
        target = self.cell_range or f"{prefix}A1"
        if not target.startswith(prefix) and prefix:
            target = f"{prefix}{target}"

        # Clear existing content
        logger.info("Clearing range %s", target)
        hook.clear_values(self.spreadsheet_id, target)

        # Prepare payload
        all_rows: list[list[Any]] = []
        if self.write_headers and headers:
            all_rows.append(headers)
        all_rows.extend(rows)

        total_written = 0
        for i in range(0, len(all_rows), self.batch_size):
            batch = all_rows[i : i + self.batch_size]
            start_row = i + 1
            batch_range = f"{prefix}A{start_row}"
            logger.info("Writing batch of %d rows to %s", len(batch), batch_range)
            hook.update_values(self.spreadsheet_id, batch_range, batch)
            total_written += len(batch)
            if i + self.batch_size < len(all_rows):
                time.sleep(self.pause_between_batches)

        logger.info("Overwrite complete: %d rows written", total_written)
        return {"mode": "overwrite", "rows_written": total_written}

    # ------------------------------------------------------------------
    # append
    # ------------------------------------------------------------------

    def _execute_append(
        self,
        hook: GoogleSheetsHook,
        headers: list[str] | None,
        rows: list[list[Any]],
    ) -> dict[str, Any]:
        prefix = self._sheet_prefix()
        target = self.cell_range or f"{prefix}A1"
        if not target.startswith(prefix) and prefix:
            target = f"{prefix}{target}"

        total_written = 0
        for i in range(0, len(rows), self.batch_size):
            batch = rows[i : i + self.batch_size]
            logger.info("Appending batch of %d rows", len(batch))
            hook.append_values(self.spreadsheet_id, target, batch)
            total_written += len(batch)
            if i + self.batch_size < len(rows):
                time.sleep(self.pause_between_batches)

        logger.info("Append complete: %d rows written", total_written)
        return {"mode": "append", "rows_written": total_written}

    # ------------------------------------------------------------------
    # smart merge
    # ------------------------------------------------------------------

    def _execute_smart_merge(
        self,
        hook: GoogleSheetsHook,
        headers: list[str] | None,
        rows: list[list[Any]],
    ) -> dict[str, Any]:
        if not self.merge_key:
            raise ValueError("merge_key is required for smart_merge mode")
        if not headers:
            raise ValueError("Headers are required for smart_merge mode")
        if self.merge_key not in headers:
            raise ValueError(
                f"merge_key '{self.merge_key}' not found in headers: {headers}"
            )

        key_col_idx = headers.index(self.merge_key)
        prefix = self._sheet_prefix()

        # Step 1 — Determine the key column letter
        key_col_letter = self._index_to_column_letter(key_col_idx)

        # Step 2 — Read the entire key column from the sheet
        key_range = f"{prefix}{key_col_letter}:{key_col_letter}"
        logger.info("Reading key column from %s", key_range)
        existing_keys_raw = hook.get_values(self.spreadsheet_id, key_range)

        # Build index: {key_value: [row_numbers]} (1-based, row 1 = header)
        existing_index: dict[str, list[int]] = defaultdict(list)
        # Skip header row (row 1)
        for row_num, row in enumerate(existing_keys_raw[1:], start=2):
            if row:
                key_val = str(row[0])
                existing_index[key_val].append(row_num)

        # Step 3 — Group incoming data by key
        incoming_groups: dict[str, list[list[Any]]] = defaultdict(list)
        for row in rows:
            key_val = str(row[key_col_idx]) if key_col_idx < len(row) else ""
            incoming_groups[key_val].append(row)

        # Step 4 — Build operations
        # We need the sheet's numeric ID for insert/delete
        sheet_id = self._get_sheet_id(hook)
        num_cols = len(headers)

        updates: list[dict] = []          # value update requests
        structural: list[dict] = []       # insert/delete dimension requests
        append_rows: list[list[Any]] = [] # rows to append at the end

        # Collect all keys and process
        all_keys = set(list(existing_index.keys()) + list(incoming_groups.keys()))

        for key_val in all_keys:
            existing_row_nums = existing_index.get(key_val, [])
            incoming_row_data = incoming_groups.get(key_val, [])

            if not incoming_row_data:
                # Key exists in sheet but not in incoming data — leave as-is
                # (smart merge only touches keys present in the incoming data)
                continue

            if not existing_row_nums:
                # New key — append at the end
                append_rows.extend(incoming_row_data)
                continue

            existing_count = len(existing_row_nums)
            incoming_count = len(incoming_row_data)

            # Update rows that exist in both
            overlap = min(existing_count, incoming_count)
            for j in range(overlap):
                row_num = existing_row_nums[j]
                range_str = f"{prefix}A{row_num}:{self._index_to_column_letter(num_cols - 1)}{row_num}"
                updates.append({
                    "row_num": row_num,
                    "range": range_str,
                    "values": [incoming_row_data[j]],
                })

            if incoming_count > existing_count:
                # Need to insert extra rows after the last existing row
                insert_after = existing_row_nums[-1]  # 1-based
                extra = incoming_row_data[existing_count:]
                structural.append({
                    "row_num": insert_after,
                    "type": "insert",
                    "start_index": insert_after,      # 0-based for API
                    "end_index": insert_after + len(extra),
                    "values": extra,
                    "num_cols": num_cols,
                })
            elif incoming_count < existing_count:
                # Need to delete surplus rows
                rows_to_delete = existing_row_nums[incoming_count:]
                # Processed bottom-up later
                structural.append({
                    "row_num": rows_to_delete[0],
                    "type": "delete",
                    "start_index": rows_to_delete[0] - 1,  # 0-based
                    "end_index": rows_to_delete[-1],        # exclusive in API
                })

        # Step 5 — Sort structural operations bottom-up (descending row number)
        structural.sort(key=lambda op: op["row_num"], reverse=True)

        # Step 6 — Execute structural changes (insert/delete) via batchUpdate
        stats = {"updated": 0, "inserted": 0, "deleted": 0, "appended": 0}

        if structural:
            batch_requests: list[dict] = []
            post_insert_updates: list[dict] = []

            for op in structural:
                if op["type"] == "delete":
                    batch_requests.append({
                        "deleteDimension": {
                            "range": {
                                "sheetId": sheet_id,
                                "dimension": "ROWS",
                                "startIndex": op["start_index"],
                                "endIndex": op["end_index"],
                            }
                        }
                    })
                    stats["deleted"] += op["end_index"] - op["start_index"]

                elif op["type"] == "insert":
                    batch_requests.append({
                        "insertDimension": {
                            "range": {
                                "sheetId": sheet_id,
                                "dimension": "ROWS",
                                "startIndex": op["start_index"],
                                "endIndex": op["end_index"],
                            },
                            "inheritFromBefore": True,
                        }
                    })
                    # Queue value writes for inserted rows
                    for k, row_data in enumerate(op["values"]):
                        row_num = op["start_index"] + 1 + k  # 1-based
                        end_col = self._index_to_column_letter(op["num_cols"] - 1)
                        post_insert_updates.append({
                            "range": f"{prefix}A{row_num}:{end_col}{row_num}",
                            "values": [row_data],
                        })
                    stats["inserted"] += len(op["values"])

            # Execute structural batch
            if batch_requests:
                logger.info("Executing %d structural operations", len(batch_requests))
                self._batched_batch_update(hook, batch_requests)

            # Write values into newly inserted rows
            for upd in post_insert_updates:
                hook.update_values(self.spreadsheet_id, upd["range"], upd["values"])

        # Step 7 — Execute value updates in batches
        if updates:
            logger.info("Updating %d existing rows", len(updates))
            for i in range(0, len(updates), self.batch_size):
                batch = updates[i : i + self.batch_size]
                for upd in batch:
                    hook.update_values(self.spreadsheet_id, upd["range"], upd["values"])
                stats["updated"] += len(batch)
                if i + self.batch_size < len(updates):
                    time.sleep(self.pause_between_batches)

        # Step 8 — Append new-key rows
        if append_rows:
            target = f"{prefix}A1"
            logger.info("Appending %d new rows", len(append_rows))
            for i in range(0, len(append_rows), self.batch_size):
                batch = append_rows[i : i + self.batch_size]
                hook.append_values(self.spreadsheet_id, target, batch)
                stats["appended"] += len(batch)
                if i + self.batch_size < len(append_rows):
                    time.sleep(self.pause_between_batches)

        logger.info("Smart merge complete: %s", stats)
        return {"mode": "smart_merge", **stats}

    # ------------------------------------------------------------------
    # internal utilities
    # ------------------------------------------------------------------

    def _get_sheet_id(self, hook: GoogleSheetsHook) -> int:
        """Resolve the numeric sheet ID."""
        if self.sheet_name:
            return hook.get_sheet_id(self.spreadsheet_id, self.sheet_name)
        # Default: first sheet
        meta = hook.get_spreadsheet_metadata(self.spreadsheet_id)
        return meta["sheets"][0]["properties"]["sheetId"]

    def _batched_batch_update(self, hook: GoogleSheetsHook, requests: list[dict]) -> None:
        """Send requests through batchUpdate, splitting into batches if needed."""
        for i in range(0, len(requests), self.batch_size):
            batch = requests[i : i + self.batch_size]
            hook.batch_update(self.spreadsheet_id, batch)
            if i + self.batch_size < len(requests):
                time.sleep(self.pause_between_batches)

    @staticmethod
    def _index_to_column_letter(index: int) -> str:
        """Convert a 0-based column index to an A1-notation letter.

        0 → A, 1 → B, … 25 → Z, 26 → AA, etc.
        """
        result = ""
        i = index
        while True:
            result = chr(ord("A") + i % 26) + result
            i = i // 26 - 1
            if i < 0:
                break
        return result
