"""Operator for writing data to Google Sheets."""

from __future__ import annotations

import logging
import math
import time
from collections import defaultdict
from typing import Any, Callable, Sequence

from airflow.models import BaseOperator

from googleapiclient.errors import HttpError

from airflow_provider_google_sheets.hooks.google_sheets import GoogleSheetsHook
from airflow_provider_google_sheets.utils.a1 import (
    A1Range,
    column_letter_to_index,
    index_to_column_letter,
    parse_range_start,
)
from airflow_provider_google_sheets.utils.data_formats import normalize_input_data
from airflow_provider_google_sheets.utils.merge_key import resolve_merge_key_schema
from airflow_provider_google_sheets.utils.merge_planner import (
    MergePlan,
    build_existing_key_index,
    plan_merge_operations,
)
from airflow_provider_google_sheets.utils.schema import (
    apply_schema_to_value,
    format_row_for_write,
)

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
        request_timeout: Per-request HTTP timeout in seconds.  Overrides the
            Airflow socket timeout (``AIRFLOW__CORE__SOCKET_TIMEOUT``).  Set to
            a value larger than the longest expected API call — e.g. ``900`` for
            a 15-minute merge on large datasets.  ``None`` inherits the Airflow
            socket timeout.
        normalize_merge_key_format: When ``True`` (default), apply extended
            fallback strategies when normalising merge-key values read from the
            sheet.  Tries: (1) primary ``format``, (2) ``input_format``,
            (3) Google Sheets serial date number (for ``date``/``datetime``
            columns only).  Set to ``False`` to restore legacy behaviour (only
            the primary ``format`` is tried).  This flag also gates schema-free
            inference (Inferred Schema): when no explicit ``schema`` entry is
            given for ``merge_key`` and the incoming values look like ISO dates
            (``YYYY-MM-DD``), the column is automatically treated as a ``date``
            key and read back with a serial-number render option so it is
            robust to the sheet's display format changing — without requiring
            any ``schema`` to be passed at all. Set to ``False`` to disable this
            inference too (key read falls back to the legacy
            ``FORMATTED_STRING`` behaviour). Boolean flags are not
            template-rendered.
        sort_keys: Optional list of sort specifications applied server-side
            (via a single ``sortRange`` ``batchUpdate``) after the write
            completes, in any write mode.  Each item has the form
            ``"column:asc"`` or ``"column:desc"`` (direction is
            case-insensitive), e.g. ``["date:desc", "region:asc"]``.  Column
            names refer to the headers **after** ``column_mapping`` is applied.
            Requires named headers (``has_headers=True`` or ``list[dict]``
            input) and only sorts the table's own columns.  Not compatible with
            ``overwrite`` + ``clear_mode="range"`` or with ``append`` +
            ``cell_range`` (write target and sort range must share the same
            ``table_start``).  Format is validated at DAG-load time; it is not a
            template field.
        transient_404_max_retries: Number of times to re-run the *whole*
            operation when Google Sheets returns a transient ``404`` on a
            spreadsheet that really exists (observed after heavy writes).  Only
            applies to the idempotent modes — ``overwrite`` and ``merge`` — and
            to ``create_sheet_if_missing`` setup; ``append`` is intentionally
            excluded (not idempotent, so it stays fail-fast).  A non-404
            ``HttpError`` is re-raised immediately.  Must be a non-bool
            ``int >= 0``; ``0`` disables the retry.  Defaults to ``3``.
        transient_404_base_delay: Base delay in seconds for the exponential
            backoff between transient-404 re-runs (delay =
            ``base_delay * 2 ** (attempt - 1)``).  Must be a non-bool
            real (``int``/``float``) ``>= 0``.  Defaults to ``5.0``.

            Worst-case wall time: with the defaults, 1 initial run plus up to
            3 re-runs (4 runs of the operation total) with ``5 + 10 + 20 = 35``
            seconds of backoff between them, each call bounded by
            ``request_timeout``.  When ``create_sheet_if_missing`` is set, the
            ensure-sheet setup is wrapped in its own separate retry budget, so
            backoff can occur once there and again during the write — roughly
            doubling the worst case (add another ~35s of backoff).
            Re-running a large write can take minutes but is safe (the modes are
            idempotent on full re-run).  Keep the Airflow ``execution_timeout``
            comfortably above this.
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
        request_timeout: int | None = 300,
        normalize_merge_key_format: bool = True,
        sort_keys: list[str] | None = None,
        transient_404_max_retries: int = 3,
        transient_404_base_delay: float = 5.0,
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
        self.request_timeout = request_timeout
        self.normalize_merge_key_format = normalize_merge_key_format
        self.sort_keys = sort_keys
        self.transient_404_max_retries = transient_404_max_retries
        self.transient_404_base_delay = transient_404_base_delay

        # Observability counter for transient-404 re-runs. Initialised here (not
        # only reset in execute()) so direct unit calls to private methods that
        # go through _run_with_transient_404_retry see a defined attribute.
        self._transient_404_retries = 0

        # Validate transient-404 retry knobs on DAG-load (like other params) so
        # mistakes surface at parse time rather than mid-run. bool is a subclass
        # of int, so True/False must be rejected explicitly — otherwise they'd
        # silently mean 1/0 retries or a 0/1s delay.
        if isinstance(transient_404_max_retries, bool) or not isinstance(
            transient_404_max_retries, int
        ):
            raise TypeError(
                "transient_404_max_retries must be a non-bool int, got "
                f"{type(transient_404_max_retries).__name__}"
            )
        if transient_404_max_retries < 0:
            raise ValueError(
                "transient_404_max_retries must be >= 0, got "
                f"{transient_404_max_retries}"
            )
        if isinstance(transient_404_base_delay, bool) or not isinstance(
            transient_404_base_delay, (int, float)
        ):
            raise TypeError(
                "transient_404_base_delay must be a non-bool int or float, got "
                f"{type(transient_404_base_delay).__name__}"
            )
        if transient_404_base_delay < 0:
            raise ValueError(
                "transient_404_base_delay must be >= 0, got "
                f"{transient_404_base_delay}"
            )
        if not math.isfinite(transient_404_base_delay):
            raise ValueError(
                "transient_404_base_delay must be finite, got "
                f"{transient_404_base_delay}"
            )

        # Full format validation happens here (in __init__) so that mistakes
        # surface at DAG-load time. sort_keys is intentionally NOT a template
        # field; if templating is ever needed, move this validation to execute().
        if sort_keys is not None:
            if not isinstance(sort_keys, list):
                raise TypeError(
                    f"sort_keys must be a list, got {type(sort_keys).__name__}"
                )
            if not sort_keys:
                raise ValueError("sort_keys must be a non-empty list or None")
            # Parse-time guard against the plain literal "range". clear_mode IS a
            # template field, so a templated value like "{{ ... }}" is a non-"sheet"
            # literal at DAG-parse time — matching on "!= 'sheet'" here would falsely
            # raise on such templates. The authoritative enforcement is the runtime
            # guard in execute() (clear_mode != "sheet"), which runs AFTER Jinja
            # rendering and catches "range", "RANGE", "range ", typos, and templated
            # values that render to any non-"sheet" range-clear.
            if write_mode == "overwrite" and clear_mode == "range":
                raise ValueError(
                    "sort_keys is not compatible with overwrite + clear_mode='range': "
                    "sorting across partial column ranges desyncs neighbouring columns"
                )
            if self.cell_range and write_mode == "append":
                raise ValueError(
                    "sort_keys is not compatible with append + cell_range: "
                    "write target and sort range must share the same table_start"
                )
            seen_cols: set[str] = set()
            for spec in sort_keys:
                if not isinstance(spec, str):
                    raise TypeError(
                        f"sort_keys item must be a string, "
                        f"got {type(spec).__name__}: {spec!r}"
                    )
                if ":" not in spec:
                    raise ValueError(
                        f"sort_keys item '{spec}' must be 'column:asc' or 'column:desc'"
                    )
                col, direction = spec.split(":", 1)
                col = col.strip()
                if not col:
                    raise ValueError(f"sort_keys item '{spec}': column name is empty")
                if direction.strip().lower() not in ("asc", "desc"):
                    raise ValueError(
                        f"sort_keys item '{spec}': direction must be 'asc' or 'desc', "
                        f"got '{direction.strip()}'"
                    )
                if col in seen_cols:
                    raise ValueError(
                        f"sort_keys column '{col}' is specified more than once"
                    )
                seen_cols.add(col)

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

    def _run_with_transient_404_retry(
        self, fn: Callable[[], Any], label: str
    ) -> Any:
        """Run *fn*, re-running the whole call on a transient Google 404.

        Only ``404`` is retried (the observed transient failure on an existing
        spreadsheet after heavy writes). Any other ``HttpError`` — and a 404
        once ``transient_404_max_retries`` re-runs are exhausted — is re-raised
        immediately. Used only to wrap the idempotent operations
        (overwrite / merge / ensure-sheet); ``append`` is never wrapped.

        *label* names the wrapped operation (e.g. ``"overwrite"`` or
        ``"ensure_sheet"``) for the retry log — it is logged instead of
        ``self.write_mode`` because the ensure-sheet wrap runs for *all* modes,
        including ``append``, so ``self.write_mode`` would be misleading there.
        """
        attempt = 0
        while True:
            try:
                return fn()
            except HttpError as e:
                if (
                    e.resp.status != 404
                    or attempt >= self.transient_404_max_retries
                ):
                    raise
                attempt += 1
                self._transient_404_retries += 1
                delay = self.transient_404_base_delay * (2 ** (attempt - 1))
                logger.warning(
                    "Transient 404 in %s, re-running (attempt %d/%d) in %.1fs",
                    label,
                    attempt,
                    self.transient_404_max_retries,
                    delay,
                )
                time.sleep(delay)

    # ------------------------------------------------------------------
    # execute
    # ------------------------------------------------------------------

    def execute(self, context: Any) -> dict[str, Any]:
        # Reset the transient-404 counter at the start of every run so the
        # value reflects only this execution (operators can be reused).
        self._transient_404_retries = 0

        hook = GoogleSheetsHook(gcp_conn_id=self.gcp_conn_id, request_timeout=self.request_timeout)

        if self.create_sheet_if_missing and self.sheet_name:
            self._run_with_transient_404_retry(
                lambda: self._ensure_sheet_exists(
                    hook, self.spreadsheet_id, self.sheet_name
                ),
                label="ensure_sheet",
            )

        headers, rows = self._resolve_data(context)
        rows = self._format_rows(headers, rows)
        rows = self._apply_partition(headers, rows)
        # Apply column_mapping last so that partition_by, schema, and merge_key
        # all reference the original column names from the input data.
        original_headers = headers
        headers = self._apply_column_mapping(headers)

        # Runtime-only sort_keys checks (format was validated in __init__).
        # Must run after column_mapping so sort_keys reference post-mapping names.
        if self.sort_keys:
            # Runtime re-check of the overwrite + partial-range-clear guard.
            # clear_mode IS a template field, so a Jinja expression can render
            # to any value at runtime and bypass the __init__ guard (which runs
            # at DAG-parse time, before templating). _execute_overwrite treats
            # ANY clear_mode != "sheet" as a partial range clear, so the guard
            # must match that (not just the literal "range") to prevent a
            # partial range clear followed by a sortRange that desyncs
            # neighbouring columns. write_mode is not templated, so only
            # clear_mode can change between __init__ and execute().
            if self.write_mode == "overwrite" and self.clear_mode != "sheet":
                raise ValueError(
                    "sort_keys is not compatible with overwrite + a partial "
                    "range clear (clear_mode != 'sheet', e.g. clear_mode='range'): "
                    "sorting across partial column ranges desyncs neighbouring columns"
                )
            if not headers:
                raise ValueError(
                    "sort_keys requires named headers (has_headers=True or "
                    "list[dict] input)"
                )
            for spec in self.sort_keys:
                col = self._parse_sort_spec(spec)[0]
                if col not in headers:
                    raise ValueError(
                        f"sort_keys column '{col}' not found in headers: {headers}"
                    )

        if self.write_mode == "overwrite":
            return self._run_with_transient_404_retry(
                lambda: self._execute_overwrite(hook, headers, rows),
                label="overwrite",
            )
        elif self.write_mode == "append":
            # append is NOT idempotent — a re-run would double-write, so 404
            # stays fail-fast here (see plan / idempotent-append design).
            return self._execute_append(hook, headers, rows)
        elif self.write_mode in ("merge", "smart_merge"):
            return self._run_with_transient_404_retry(
                lambda: self._execute_merge(
                    hook, headers, rows, original_headers=original_headers
                ),
                label="merge",
            )
        else:
            raise ValueError(f"Unknown write_mode: '{self.write_mode}'")

    # ------------------------------------------------------------------
    # overwrite
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_range_start(range_str: str) -> tuple[str, int]:
        """Extract the start column and row from an A1-notation range.

        Thin delegate to :func:`utils.a1.parse_range_start` (lenient: missing
        column defaults to ``"A"``, missing row to ``1``).
        """
        return parse_range_start(range_str)

    @staticmethod
    def _parse_sort_spec(spec: str) -> tuple[str, str]:
        """Split a validated ``"column:direction"`` sort spec into normalised parts.

        Returns ``(column, direction)`` with the column stripped and the
        direction lower-cased. Assumes the spec format was already validated in
        ``__init__`` (i.e. it contains a ``:`` and a valid direction).
        """
        col, direction = spec.split(":", 1)
        return col.strip(), direction.strip().lower()

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
            window = A1Range.parse(f"{start_col}{start_row_num}")
            end_col = window.col_at(num_data_cols - 1) if num_data_cols else start_col
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

        # Server-side sort (clear_mode="range" is already blocked in __init__).
        if self.sort_keys:
            skip_header = bool(self.write_headers and headers)
            # all_rows already includes the header row when write_headers=True.
            end_row = (start_row_num - 1) + len(all_rows)
            # Cover the widest written row: data rows can be wider than headers.
            num_columns = max(
                len(headers) if headers else 0,
                max((len(r) for r in all_rows), default=0),
            )
            self._execute_sort(
                hook,
                headers,
                self._get_sheet_id(hook),
                start_col,
                start_row_num,
                skip_header,
                end_row,
                num_columns,
            )

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

        # Determine header state and, when sorting, the existing table height.
        header_written_this_run = False
        existing_row_count = 0
        if self.sort_keys:
            # Sorting needs the existing table height. The first column may
            # have internal gaps and Sheets trims trailing empties, so read
            # the full table width and take the last row that has any
            # non-empty cell.
            window = A1Range.parse(f"{start_col}{start_row}")
            end_col = window.col_at(len(headers) - 1)
            existing_block = hook.get_values(
                self.spreadsheet_id, f"{prefix}{start_col}{start_row}:{end_col}"
            )
            for i, r in enumerate(existing_block):
                if any(str(c).strip() for c in r):
                    existing_row_count = i + 1
            header_written_this_run = (
                existing_row_count == 0 and self.write_headers and bool(headers)
            )
            if header_written_this_run:
                header_range = f"{prefix}{start_col}{start_row}"
                logger.info("Sheet is empty — writing headers to %s", header_range)
                hook.update_values(self.spreadsheet_id, header_range, [headers])
        elif self.write_headers and headers:
            # Legacy single-cell emptiness check (backward compatible).
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

        # Server-side sort (cell_range is already blocked in __init__ when set).
        if self.sort_keys:
            # A physical header row exists at the table top if we just wrote one
            # this run (header_written_this_run), OR the sheet already had data
            # and the table declares headers (has_headers). This is consistent
            # with build_existing_key_index (which skips the existing first row
            # whenever has_headers=True, independent of write_headers) and with
            # the operator's has_headers model of existing-sheet header presence.
            skip_header = header_written_this_run or (
                existing_row_count > 0 and self.has_headers
            )
            end_row = (
                (start_row - 1)
                + existing_row_count
                + (1 if header_written_this_run else 0)
                + len(rows)
            )
            # Width contract: the sort range spans the table's declared/written
            # width — max(len(headers), widest row written this run). Cells to
            # the RIGHT of that width are intentionally NOT reordered. We cannot
            # widen to swallow pre-existing on-sheet rows that are wider than the
            # headers, because an open-width read cannot distinguish same-table
            # ragged cells from FOREIGN adjacent-column data — reordering foreign
            # data would be a worse corruption. Headers define the table; rows
            # that belong to the same table must not exceed the written width.
            num_columns = max(
                len(headers) if headers else 0,
                max((len(r) for r in rows), default=0),
            )
            self._execute_sort(
                hook,
                headers,
                self._get_sheet_id(hook),
                start_col,
                start_row,
                skip_header,
                end_row,
                num_columns,
            )

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

        # Resolve key_schema BEFORE reading key_range — it determines which
        # date_time_render_option to use. Explicit schema wins; otherwise,
        # when normalize_merge_key_format is enabled, infer an ISO-date schema
        # from the shape of the incoming key values.
        explicit_key_schema = (self.schema or {}).get(self.merge_key)
        incoming_key_strs = [
            str(r[key_col_idx]) for r in rows
            if key_col_idx < len(r) and r[key_col_idx] not in (None, "")
        ]
        key_schema = resolve_merge_key_schema(
            explicit_key_schema, incoming_key_strs, infer=self.normalize_merge_key_format
        )
        if explicit_key_schema is None and key_schema is not None:
            logger.info(
                "merge_key '%s' looks like a date column (no schema provided) — "
                "applying automatic date normalization",
                self.merge_key,
            )

        # SERIAL_NUMBER only when there's a real need to decode a serial number
        # as a date (type="date", normalization enabled). "datetime" is
        # intentionally excluded — decoding via int(float()) truncates time of
        # day, which would break the already-working explicit-schema-datetime
        # case. normalize_merge_key_format=False always reads FORMATTED_STRING
        # (strict legacy — the raw value itself does not change).
        use_serial_number = (
            self.normalize_merge_key_format
            and key_schema is not None
            and key_schema.get("type") == "date"
        )
        logger.info("Reading key column from %s", key_range)
        existing_keys_raw = hook.get_values(
            self.spreadsheet_id,
            key_range,
            date_time_render_option="SERIAL_NUMBER" if use_serial_number else "FORMATTED_STRING",
        )

        # Write headers if the sheet is completely empty
        headers_just_written = False
        if not existing_keys_raw and self.write_headers and headers:
            header_range = f"{prefix}{table_start_col}{table_start_row}"
            logger.info("Sheet is empty — writing headers to %s", header_range)
            hook.update_values(self.spreadsheet_id, header_range, [headers])
            headers_just_written = True

        # Build index: {key_value: [row_numbers]} (1-based absolute)
        existing_index = build_existing_key_index(
            existing_keys_raw,
            has_headers=self.has_headers,
            table_start_row=table_start_row,
            key_schema=key_schema,
            extended=self.normalize_merge_key_format,
        )

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

        plan: MergePlan = plan_merge_operations(existing_index, incoming_groups)
        delete_ops = plan.delete_ops
        append_rows = plan.append_rows

        # Step 5 — Execute deletes bottom-up (descending row number avoids index shifts)
        stats = {"deleted": 0, "appended": 0}
        total_deleted = 0

        if delete_ops:
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

        # total_existing is needed for both the append branch (insert position)
        # and the sort branch (end_row), which runs even when append_rows is
        # empty — so it lives above the `if append_rows:` guard.
        total_existing = len(existing_keys_raw) + (1 if headers_just_written else 0)

        # Step 6 — Append incoming rows with clean (default) formatting
        if append_rows:
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

        # Server-side sort after all mutations (deletes + appends).
        if self.sort_keys:
            rows_after_merge = total_existing - total_deleted + len(append_rows)
            end_row = (table_start_row - 1) + rows_after_merge
            # A physical header row exists only if we just wrote one, or the
            # sheet already had data AND the table declares headers (has_headers).
            # This matches build_existing_key_index, where has_headers governs
            # whether the existing first row is treated as a header (and thus
            # skipped) — independent of write_headers.
            # `has_headers=False` (reachable via list[dict] input, which sets
            # `headers` so sort_keys passes the execute() guard while the sheet
            # itself has no header row) must NOT skip the first data row.
            skip_header = headers_just_written or (
                bool(existing_keys_raw) and self.has_headers
            )
            # Width contract: the sort range spans the table's declared/written
            # width — max(len(headers), widest row written this run). Cells to
            # the RIGHT of that width are intentionally NOT reordered. We cannot
            # widen to swallow pre-existing on-sheet rows that are wider than the
            # headers, because an open-width read cannot distinguish same-table
            # ragged cells from FOREIGN adjacent-column data — reordering foreign
            # data would be a worse corruption. Headers define the table; rows
            # that belong to the same table must not exceed the written width.
            num_columns = max(
                len(headers) if headers else 0,
                max((len(r) for r in append_rows), default=0),
            )
            self._execute_sort(
                hook,
                headers,
                sheet_id,
                table_start_col,
                table_start_row,
                skip_header,
                end_row,
                num_columns,
            )

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

    def _execute_sort(
        self,
        hook: GoogleSheetsHook,
        headers: list[str],
        sheet_id: int,
        table_start_col: str,
        table_start_row: int,
        skip_header: bool,
        end_row: int,
        num_columns: int | None = None,
    ) -> None:
        """Server-side sort the written table via a single ``sortRange`` request.

        Args:
            hook: Google Sheets hook used to issue the ``batchUpdate`` request.
            headers: Table headers (post column_mapping), used to resolve sort
                columns to absolute dimension indices.
            sheet_id: Numeric sheet ID.
            table_start_col: Left-most column letter of the table (e.g. ``"A"``).
            table_start_row: 1-based row of the table's top edge.
            skip_header: When ``True``, the first table row is a header and is
                excluded from the sort range.
            end_row: 0-based exclusive end row of the sort range (i.e. the total
                row count from the top of the sheet). The sort never touches rows
                outside the table.
            num_columns: Width (in columns) of the sort range. Data rows can be
                wider than the header row (e.g. list[list] input), so the sort
                range must cover the widest written row, not just
                ``len(headers)`` — otherwise the extra right-hand cells are not
                reordered and desync from the sorted columns. Defaults to
                ``len(headers)`` when not provided. Sort keys always live within
                ``headers`` so ``dimensionIndex < endColumnIndex`` still holds.

                Width contract: the sort range spans the table's declared /
                written width — ``max(len(headers), widest row written this
                run)``. In append/merge modes, pre-existing on-sheet rows that
                are wider than that width are intentionally NOT covered: their
                right-hand cells are left unsorted. This is deliberate. An
                open-width read of the sheet cannot distinguish same-table
                ragged cells from FOREIGN data in adjacent columns, so widening
                the sort range to reach them would risk reordering unrelated
                neighbouring data — a worse corruption than leaving a few
                over-wide same-table cells unsorted. Headers define the table;
                same-table rows must not exceed the written width.
        """
        start_col_idx = self._column_letter_to_index(table_start_col)
        end_col_idx = start_col_idx + (
            num_columns if num_columns is not None else len(headers)
        )
        sort_specs: list[dict] = []
        for spec in self.sort_keys:
            col, direction = self._parse_sort_spec(spec)
            col_idx = start_col_idx + headers.index(col)
            sort_specs.append({
                "dimensionIndex": col_idx,
                "sortOrder": "ASCENDING" if direction == "asc" else "DESCENDING",
            })
        data_start = (table_start_row - 1) + (1 if skip_header else 0)
        # No-op on an empty range: the Google API rejects a sortRange where
        # startRowIndex >= endRowIndex. Covers empty data / header-only tables.
        if data_start >= end_row:
            return
        hook.batch_update(self.spreadsheet_id, [{
            "sortRange": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": data_start,
                    "endRowIndex": end_row,
                    "startColumnIndex": start_col_idx,
                    "endColumnIndex": end_col_idx,
                },
                "sortSpecs": sort_specs,
            }
        }])

    @staticmethod
    def _column_letter_to_index(letter: str) -> int:
        """Convert an A1-notation column letter to a 0-based index.

        Thin delegate to :func:`utils.a1.column_letter_to_index`.
        """
        return column_letter_to_index(letter)

    @staticmethod
    def _index_to_column_letter(index: int) -> str:
        """Convert a 0-based column index to an A1-notation letter.

        Thin delegate to :func:`utils.a1.index_to_column_letter`.
        """
        return index_to_column_letter(index)
