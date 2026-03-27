"""Operator for writing data to Google Sheets."""

from __future__ import annotations

import logging
import time
from collections import defaultdict
from typing import Any, Sequence

from airflow.models import BaseOperator

from googleapiclient.errors import HttpError

from airflow_provider_google_sheets.exceptions import GoogleSheetsDataError
from airflow_provider_google_sheets.hooks.google_sheets import GoogleSheetsHook
from airflow_provider_google_sheets.utils.data_formats import normalize_input_data
from airflow_provider_google_sheets.utils.schema import apply_schema_to_value, format_row_for_write, format_value_for_write

logger = logging.getLogger(__name__)

_VALID_WRITE_MODES = frozenset({"overwrite", "append", "merge", "smart_merge"})


class GoogleSheetsWriteOperator(BaseOperator):
    """Write data to a Google Sheets spreadsheet.

    Supports three write modes:

    * ``overwrite`` — clear the target range and write new data.
    * ``append`` — append rows after the last occupied row.
    * ``merge`` — update / insert / delete rows based on a key column.
      ``smart_merge`` is accepted as a silent alias.

    Args:
        gcp_conn_id: Airflow Connection ID.
        spreadsheet_id: Target spreadsheet ID.
        sheet_name: Target sheet (tab) name.
        cell_range: Target A1 range (used by *overwrite*).
        write_mode: ``"overwrite"``, ``"append"`` or ``"merge"`` (alias: ``"smart_merge"``).
        data: Inline data — ``list[list]``, ``list[dict]``, or a file path.
        data_xcom_task_id: Pull data from this task's XCom instead.
        data_xcom_key: XCom key when using *data_xcom_task_id*.
        has_headers: Whether *data* contains a header row.
        write_headers: Write the header row (for *overwrite* mode).
        schema: Optional column schema for formatting values before write.
        clear_mode: How to clear existing data in *overwrite* mode.
            ``"sheet"`` (default) clears the entire sheet and trims extra rows
            after writing.  ``"range"`` clears only the data columns, leaving
            neighbouring columns and extra rows untouched.
        batch_size: Rows per API request.
        pause_between_batches: Seconds to wait between batches.
        merge_key: Column name used as the key for *merge*.
        table_start: Top-left cell of the table (e.g. ``"A1"``, ``"C3"``).
            Used by *append* and *merge* to locate the header row and
            compute absolute column positions.  Defaults to ``"A1"``.
        create_sheet_if_missing: When ``True``, create the target sheet (tab)
            if it does not already exist.  Defaults to ``False``.
        partition_by: Column name to filter data by.  When set, only rows
            where ``str(row[partition_by]) == partition_value`` are written.
        partition_value: The value to match against when *partition_by* is set.
        column_mapping: Optional ``{source_name: sheet_name}`` dict that renames
            column headers before writing.  Headers not present in the mapping
            are written as-is.  Applied *after* ``partition_by``, ``schema``, and
            ``merge_key`` resolution so all other parameters reference the
            **original** column names from the input data.
    """

    template_fields: Sequence[str] = (
        "spreadsheet_id",
        "sheet_name",
        "cell_range",
        "table_start",
        "data",
        "data_xcom_task_id",
        "clear_mode",
        "partition_by",
        "partition_value",
        "column_mapping",
    )

    def __init__(
        self,
        *,
        gcp_conn_id: str = "google_cloud_default",
        spreadsheet_id: str,
        sheet_name: str | None = None,
        cell_range: str | None = None,
        write_mode: str = "overwrite",
        clear_mode: str = "sheet",
        data: Any = None,
        data_xcom_task_id: str | None = None,
        data_xcom_key: str = "return_value",
        has_headers: bool = True,
        write_headers: bool = True,
        schema: dict[str, dict] | None = None,
        batch_size: int = 1000,
        pause_between_batches: float = 1.0,
        merge_key: str | None = None,
        table_start: str = "A1",
        create_sheet_if_missing: bool = False,
        partition_by: str | None = None,
        partition_value: str | None = None,
        column_mapping: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        if write_mode not in _VALID_WRITE_MODES:
            raise ValueError(
                f"Invalid write_mode '{write_mode}'. "
                f"Supported: {sorted(_VALID_WRITE_MODES)}"
            )
        self.gcp_conn_id = gcp_conn_id
        self.spreadsheet_id = spreadsheet_id
        self.sheet_name = sheet_name
        self.cell_range = cell_range
        self.write_mode = write_mode
        self.clear_mode = clear_mode
        self.data = data
        self.data_xcom_task_id = data_xcom_task_id
        self.data_xcom_key = data_xcom_key
        self.has_headers = has_headers
        self.write_headers = write_headers
        self.schema = schema
        self.batch_size = batch_size
        self.pause_between_batches = pause_between_batches
        self.merge_key = merge_key
        self.table_start = table_start
        self.create_sheet_if_missing = create_sheet_if_missing
        self.partition_by = partition_by
        self.partition_value = partition_value
        self.column_mapping = column_mapping

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
        result = []
        for row in rows:
            preprocessed = list(row)
            for i, value in enumerate(row):
                if i < len(headers):
                    col_schema = self.schema.get(headers[i], {})
                    if col_schema.get("input_format"):
                        preprocessed[i] = apply_schema_to_value(value, col_schema)
            result.append(format_row_for_write(preprocessed, headers, self.schema))
        return result

    def _normalize_sheet_key(self, raw: str) -> str:
        """Normalize a key value read from the sheet to canonical write format."""
        key_schema = (self.schema or {}).get(self.merge_key)
        if not key_schema:
            return raw
        # Sheet keys were written using "format" — parse with "format", not "input_format"
        parse_schema = {k: v for k, v in key_schema.items() if k != "input_format"}
        try:
            parsed = apply_schema_to_value(raw, parse_schema)
            return format_value_for_write(parsed, key_schema)
        except (ValueError, TypeError, GoogleSheetsDataError):
            logger.warning("Could not normalize existing key %r via schema, using raw value", raw)
            return raw

    def _apply_partition(
        self, headers: list[str] | None, rows: list[list[Any]]
    ) -> list[list[Any]]:
        """Filter rows by partition_by column matching partition_value."""
        if self.partition_by is None:
            return rows
        if self.partition_value is None:
            raise ValueError(
                "partition_value is required when partition_by is set."
            )
        if not headers:
            raise ValueError(
                "Headers are required for partition filtering."
            )
        if self.partition_by not in headers:
            raise ValueError(
                f"partition_by column '{self.partition_by}' not found in "
                f"headers: {headers}"
            )
        idx = headers.index(self.partition_by)
        filtered = [
            r for r in rows
            if idx < len(r) and str(r[idx]) == self.partition_value
        ]
        logger.info(
            "Partition filter: %d/%d rows match %s=%s",
            len(filtered), len(rows), self.partition_by, self.partition_value,
        )
        return filtered

    def _apply_column_mapping(self, headers: list[str] | None) -> list[str] | None:
        """Rename headers according to column_mapping (original → sheet name)."""
        if not self.column_mapping or not headers:
            return headers
        return [self.column_mapping.get(h, h) for h in headers]

    def _ensure_sheet_exists(
        self, hook: GoogleSheetsHook, spreadsheet_id: str, sheet_name: str
    ) -> None:
        """Create the sheet if it does not exist (when create_sheet_if_missing is True)."""
        meta = hook.get_spreadsheet_metadata(spreadsheet_id)
        existing = {s["properties"]["title"] for s in meta.get("sheets", [])}
        if sheet_name not in existing:
            try:
                hook.create_sheet(spreadsheet_id, sheet_name)
                logger.info("Created missing sheet '%s'", sheet_name)
            except HttpError as e:
                if e.resp.status == 400 and "already exists" in str(e).lower():
                    # Race condition: another task already created the sheet
                    logger.info(
                        "Sheet '%s' was created by another task (race condition)",
                        sheet_name,
                    )
                else:
                    raise

    # ------------------------------------------------------------------
    # execute
    # ------------------------------------------------------------------

    def execute(self, context: Any) -> dict[str, Any]:
        hook = GoogleSheetsHook(gcp_conn_id=self.gcp_conn_id)

        if self.create_sheet_if_missing and self.sheet_name:
            self._ensure_sheet_exists(hook, self.spreadsheet_id, self.sheet_name)

        headers, rows = self._resolve_data(context)
        rows = self._format_rows(headers, rows)
        rows = self._apply_partition(headers, rows)
        # Apply column_mapping last so that partition_by, schema, and merge_key
        # all reference the original column names from the input data.
        original_headers = headers
        headers = self._apply_column_mapping(headers)

        if self.write_mode == "overwrite":
            return self._execute_overwrite(hook, headers, rows)
        elif self.write_mode == "append":
            return self._execute_append(hook, headers, rows)
        elif self.write_mode in ("merge", "smart_merge"):
            return self._execute_merge(hook, headers, rows, original_headers=original_headers)
        else:
            raise ValueError(f"Unknown write_mode: '{self.write_mode}'")

    # ------------------------------------------------------------------
    # overwrite
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_range_start(range_str: str) -> tuple[str, int]:
        """Extract the start column and row from an A1-notation range.

        Examples:
            ``"B2:D10"`` → ``("B", 2)``
            ``"Sheet1!C5:F"`` → ``("C", 5)``
            ``"A1"`` → ``("A", 1)``

        When no row number is present, defaults to ``1``.
        """
        r = range_str
        if "!" in r:
            r = r.split("!", 1)[1]
        # Take only the left part (before ':')
        left = r.split(":")[0]
        col = "".join(c for c in left if c.isalpha()) or "A"
        row_str = "".join(c for c in left if c.isdigit())
        row = int(row_str) if row_str else 1
        return col, row

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

        # Determine the start column and row from the target range
        start_col, start_row_num = self._parse_range_start(target)

        # Prepare payload
        all_rows: list[list[Any]] = []
        if self.write_headers and headers:
            all_rows.append(headers)
        all_rows.extend(rows)

        # Determine data width (number of columns)
        num_data_cols = len(headers) if headers else (max(len(r) for r in all_rows) if all_rows else 0)

        # Clear existing content based on clear_mode
        if self.clear_mode == "sheet":
            # Clear entire sheet
            clear_range = prefix.rstrip("!") if prefix else "Sheet1"
            logger.info("Clearing entire sheet %s", clear_range)
            hook.clear_values(self.spreadsheet_id, clear_range)
        else:
            # clear_mode == "range": clear only the data columns
            start_col_idx = self._column_letter_to_index(start_col)
            end_col = self._index_to_column_letter(start_col_idx + num_data_cols - 1) if num_data_cols else start_col
            clear_range = f"{prefix}{start_col}{start_row_num}:{end_col}"
            logger.info("Clearing range %s", clear_range)
            hook.clear_values(self.spreadsheet_id, clear_range)

        # Ensure the sheet has enough rows for all data
        required_rows = start_row_num + len(all_rows) - 1
        hook.ensure_rows(self.spreadsheet_id, self.sheet_name, required_rows)

        total_written = 0
        for i in range(0, len(all_rows), self.batch_size):
            batch = all_rows[i : i + self.batch_size]
            row_offset = start_row_num + i
            batch_range = f"{prefix}{start_col}{row_offset}"
            logger.info("Writing batch of %d rows to %s", len(batch), batch_range)
            hook.update_values(self.spreadsheet_id, batch_range, batch)
            total_written += len(batch)
            if i + self.batch_size < len(all_rows):
                time.sleep(self.pause_between_batches)

        # Trim extra rows in sheet mode
        if self.clear_mode == "sheet":
            keep_rows = start_row_num + len(all_rows) - 1
            hook.trim_sheet(self.spreadsheet_id, self.sheet_name, keep_rows)

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
        start_col, start_row = self._parse_range_start(self.table_start)
        target = self.cell_range or f"{prefix}{start_col}{start_row}"
        if not target.startswith(prefix) and prefix:
            target = f"{prefix}{target}"

        # Write headers if the sheet is empty and write_headers is requested
        if self.write_headers and headers:
            first_row = hook.get_values(
                self.spreadsheet_id, f"{prefix}{start_col}{start_row}"
            )
            if not first_row:
                header_range = f"{prefix}{start_col}{start_row}"
                logger.info("Sheet is empty — writing headers to %s", header_range)
                hook.update_values(self.spreadsheet_id, header_range, [headers])

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

    def _execute_merge(
        self,
        hook: GoogleSheetsHook,
        headers: list[str] | None,
        rows: list[list[Any]],
        original_headers: list[str] | None = None,
    ) -> dict[str, Any]:
        # Use original_headers (pre-mapping) for merge_key lookup so that
        # merge_key always references the input data column names.
        key_lookup = original_headers if original_headers is not None else headers
        if not self.merge_key:
            raise ValueError("merge_key is required for merge mode")
        if not key_lookup:
            raise ValueError("Headers are required for merge mode")
        if self.merge_key not in key_lookup:
            raise ValueError(
                f"merge_key '{self.merge_key}' not found in headers: {key_lookup}"
            )

        key_col_idx = key_lookup.index(self.merge_key)
        prefix = self._sheet_prefix()

        # Step 1 — Resolve table start position
        table_start_col, table_start_row = self._parse_range_start(self.table_start)
        start_col_idx = self._column_letter_to_index(table_start_col)

        # Step 2 — Determine the absolute key column letter and read from start row
        abs_key_col = self._index_to_column_letter(start_col_idx + key_col_idx)
        key_range = f"{prefix}{abs_key_col}{table_start_row}:{abs_key_col}"
        logger.info("Reading key column from %s", key_range)
        existing_keys_raw = hook.get_values(self.spreadsheet_id, key_range)

        # Write headers if the sheet is completely empty
        headers_just_written = False
        if not existing_keys_raw and self.write_headers and headers:
            header_range = f"{prefix}{table_start_col}{table_start_row}"
            logger.info("Sheet is empty — writing headers to %s", header_range)
            hook.update_values(self.spreadsheet_id, header_range, [headers])
            headers_just_written = True

        # Build index: {key_value: [row_numbers]} (1-based absolute)
        existing_index: dict[str, list[int]] = defaultdict(list)
        if self.has_headers:
            data_rows = existing_keys_raw[1:]
            start_row_num = table_start_row + 1
        else:
            data_rows = existing_keys_raw
            start_row_num = table_start_row
        for row_num, row in enumerate(data_rows, start=start_row_num):
            if row:
                key_val = self._normalize_sheet_key(str(row[0]))
                existing_index[key_val].append(row_num)

        # Step 3 — Group incoming data by key
        incoming_groups: dict[str, list[list[Any]]] = defaultdict(list)
        for row in rows:
            key_val = str(row[key_col_idx]) if key_col_idx < len(row) else ""
            incoming_groups[key_val].append(row)

        # Step 4 — Build delete and append operations
        # Strategy: for each key present in incoming data —
        #   delete ALL existing rows with that key, then append the new rows.
        # Keys present in the sheet but absent from incoming data are left untouched.
        sheet_id = self._get_sheet_id(hook)

        delete_ops: list[dict] = []   # deleteDimension requests
        append_rows: list[list[Any]] = []

        for key_val, incoming_row_data in incoming_groups.items():
            existing_row_nums = existing_index.get(key_val, [])
            if existing_row_nums:
                for seg_start, seg_end in self._group_contiguous(existing_row_nums):
                    delete_ops.append({
                        "row_num": seg_start,
                        "start_index": seg_start - 1,  # 0-based
                        "end_index": seg_end,           # exclusive
                    })
            append_rows.extend(incoming_row_data)

        # Step 5 — Execute deletes bottom-up (descending row number avoids index shifts)
        stats = {"deleted": 0, "appended": 0}
        total_deleted = 0

        if delete_ops:
            delete_ops.sort(key=lambda op: op["row_num"], reverse=True)
            batch_requests = [
                {
                    "deleteDimension": {
                        "range": {
                            "sheetId": sheet_id,
                            "dimension": "ROWS",
                            "startIndex": op["start_index"],
                            "endIndex": op["end_index"],
                        }
                    }
                }
                for op in delete_ops
            ]
            logger.info("Deleting rows for %d key groups", len(delete_ops))
            self._batched_batch_update(hook, batch_requests)
            total_deleted = sum(op["end_index"] - op["start_index"] for op in delete_ops)
            stats["deleted"] = total_deleted

        # Step 6 — Append incoming rows with clean (default) formatting
        if append_rows:
            total_existing = len(existing_keys_raw) + (1 if headers_just_written else 0)
            rows_after_deletion = total_existing - total_deleted
            # 0-based absolute row position where new rows will land (for repeatCell)
            insert_start = (table_start_row - 1) + rows_after_deletion

            # Compute range hint for values.append
            end_col = self._index_to_column_letter(start_col_idx + len(headers) - 1)
            append_range = f"{prefix}{table_start_col}{table_start_row}:{end_col}"

            logger.info("Appending %d rows via values.append", len(append_rows))
            for i in range(0, len(append_rows), self.batch_size):
                batch = append_rows[i : i + self.batch_size]
                hook.append_values(self.spreadsheet_id, append_range, batch)
                stats["appended"] += len(batch)
                if i + self.batch_size < len(append_rows):
                    time.sleep(self.pause_between_batches)

            # Clear inherited visual formatting from the new rows,
            # but preserve numberFormat (date/number display formats).
            style_fields = (
                "userEnteredFormat.textFormat,"
                "userEnteredFormat.backgroundColor,"
                "userEnteredFormat.backgroundColorStyle,"
                "userEnteredFormat.borders,"
                "userEnteredFormat.padding,"
                "userEnteredFormat.horizontalAlignment,"
                "userEnteredFormat.verticalAlignment,"
                "userEnteredFormat.wrapStrategy,"
                "userEnteredFormat.textDirection,"
                "userEnteredFormat.textRotation,"
                "userEnteredFormat.hyperlinkDisplayType"
            )
            self._batched_batch_update(hook, [
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": insert_start,
                            "endRowIndex": insert_start + len(append_rows),
                            "startColumnIndex": start_col_idx,
                            "endColumnIndex": start_col_idx + len(headers),
                        },
                        "cell": {"userEnteredFormat": {}},
                        "fields": style_fields,
                    }
                }
            ])

        logger.info("Merge complete: %s", stats)
        return {"mode": "merge", **stats}

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
    def _group_contiguous(rows: list[int]) -> list[tuple[int, int]]:
        """Group sorted row numbers into contiguous segments.

        Each segment is ``(start, end)`` where both are 1-based inclusive.

        Example::

            [3, 7, 8, 12] → [(3, 3), (7, 8), (12, 12)]
        """
        if not rows:
            return []
        groups: list[tuple[int, int]] = []
        start = rows[0]
        prev = rows[0]
        for r in rows[1:]:
            if r == prev + 1:
                prev = r
            else:
                groups.append((start, prev))
                start = r
                prev = r
        groups.append((start, prev))
        return groups

    @staticmethod
    def _column_letter_to_index(letter: str) -> int:
        """Convert an A1-notation column letter to a 0-based index.

        A → 0, B → 1, … Z → 25, AA → 26, etc.
        """
        result = 0
        for ch in letter.upper():
            result = result * 26 + (ord(ch) - ord("A") + 1)
        return result - 1

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
