"""Operator for reading data from Google Sheets."""

from __future__ import annotations

import csv
import json
import logging
from typing import Any, Iterator, Sequence

from airflow.models import BaseOperator

from airflow_provider_google_sheets.exceptions import GoogleSheetsDataError
from airflow_provider_google_sheets.hooks.google_sheets import GoogleSheetsHook
from airflow_provider_google_sheets.utils.data_formats import rows_to_dicts
from airflow_provider_google_sheets.utils.headers import process_headers
from airflow_provider_google_sheets.utils.row_filter import matches_any, normalize_conditions, validate_conditions
from airflow_provider_google_sheets.utils.schema import apply_schema_to_row, validate_schema

logger = logging.getLogger(__name__)


class GoogleSheetsReadOperator(BaseOperator):
    """Read data from a Google Sheets spreadsheet.

    Data is read in chunks to handle large sheets.  For ``csv`` / ``json`` /
    ``jsonl`` output the chunks are streamed directly to the file without
    accumulating the entire dataset in memory.  For ``xcom`` output, rows are
    collected in memory (subject to *max_xcom_rows* limit).

    Supported ``output_type`` values:
    - ``"xcom"`` (default) — return data as a Python object via XCom
    - ``"csv"`` — stream to a CSV file
    - ``"json"`` — stream to a JSON array file
    - ``"jsonl"`` — stream to a JSONL file (one JSON object per line)

    Args:
        gcp_conn_id: Airflow Connection ID for the Google service account.
        spreadsheet_id: The Google Sheets spreadsheet ID.
        sheet_name: Sheet (tab) name.  ``None`` reads the first sheet.
        cell_range: A1-notation range, e.g. ``"A1:Z"``.  ``None`` reads the
            entire sheet.
        has_headers: Whether the first row contains column headers.
        transliterate_headers: Transliterate Cyrillic headers to Latin.
            Defaults to ``True``.
        normalize_headers: Sanitize headers to ``snake_case``.
        sanitize_headers: Remove spaces and special characters from headers,
            keeping only letters, digits, and underscores.  Defaults to ``True``.
        lowercase_headers: Convert headers to lowercase.  Defaults to ``True``.
        column_mapping: Optional dict mapping original header names to new names.
            When provided, all other header processing (transliterate, sanitize,
            lowercase, normalize) is skipped — the mapping is applied directly
            to the raw header names from the spreadsheet.  Headers not present
            in the mapping are kept as-is.
        schema: Optional column type schema for validation and conversion.
        strip_strings: If ``True``, strip leading and trailing whitespace from
            all string cell values.  Defaults to ``False`` (load as-is).
        row_skip: Condition(s) for rows to skip (filter out).  A ``dict`` or
            ``list[dict]`` with keys ``column``, ``value``, ``op``.
        row_stop: Condition(s) to stop reading.  When a matching row is found,
            all subsequent rows (including the matching one) are discarded and
            no further API calls are made.
        chunk_size: Number of rows to read per API request.
        output_type: ``"xcom"``, ``"csv"`` or ``"json"``.
        output_path: File path for ``csv`` / ``json`` output.
        xcom_key: XCom key used when pushing the result.
        max_xcom_rows: Maximum rows allowed for XCom output.
    """

    template_fields: Sequence[str] = (
        "spreadsheet_id",
        "sheet_name",
        "cell_range",
        "output_path",
        "row_skip",
        "row_stop",
    )

    def __init__(
        self,
        *,
        gcp_conn_id: str = "google_cloud_default",
        spreadsheet_id: str,
        sheet_name: str | None = None,
        cell_range: str | None = None,
        has_headers: bool = True,
        transliterate_headers: bool = True,
        normalize_headers: bool = False,
        sanitize_headers: bool = True,
        lowercase_headers: bool = True,
        column_mapping: dict[str, str] | None = None,
        schema: dict[str, dict] | None = None,
        strip_strings: bool = False,
        row_skip: dict | list[dict] | None = None,
        row_stop: dict | list[dict] | None = None,
        chunk_size: int = 5000,
        output_type: str = "xcom",
        output_path: str | None = None,
        xcom_key: str = "return_value",
        max_xcom_rows: int = 50_000,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.spreadsheet_id = spreadsheet_id
        self.sheet_name = sheet_name
        self.cell_range = cell_range
        self.has_headers = has_headers
        self.transliterate_headers = transliterate_headers
        self.normalize_headers = normalize_headers
        self.sanitize_headers = sanitize_headers
        self.lowercase_headers = lowercase_headers
        self.column_mapping = column_mapping
        self.schema = schema
        self.strip_strings = strip_strings
        self.row_skip = row_skip
        self.row_stop = row_stop
        self.chunk_size = chunk_size
        self.output_type = output_type
        self.output_path = output_path
        self.xcom_key = xcom_key
        self.max_xcom_rows = max_xcom_rows

    # ------------------------------------------------------------------
    # Range helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _column_letter_from_range(cell_range: str | None) -> tuple[str, str] | None:
        """Extract start and end column letters from an A1 range.

        Returns ``None`` when no range is given (meaning "whole sheet").
        """
        if not cell_range:
            return None
        # Strip sheet prefix if present (e.g. "Sheet1!A1:Z" → "A1:Z")
        if "!" in cell_range:
            cell_range = cell_range.split("!", 1)[1]
        parts = cell_range.split(":")
        start_col = "".join(c for c in parts[0] if c.isalpha())
        end_col = "".join(c for c in parts[1] if c.isalpha()) if len(parts) > 1 else start_col
        return start_col or "A", end_col or "A"

    def _build_range(self, start_row: int, end_row: int) -> str:
        """Build a fully-qualified A1-notation range for a chunk."""
        prefix = f"{self.sheet_name}!" if self.sheet_name else ""
        cols = self._column_letter_from_range(self.cell_range)
        if cols:
            start_col, end_col = cols
            return f"{prefix}{start_col}{start_row}:{end_col}{end_row}"
        # No cell_range → row-only range (all columns)
        return f"{prefix}{start_row}:{end_row}"

    def _build_header_range(self) -> str:
        """Build range for the header row only."""
        prefix = f"{self.sheet_name}!" if self.sheet_name else ""
        cols = self._column_letter_from_range(self.cell_range)
        if cols:
            start_col, end_col = cols
            return f"{prefix}{start_col}1:{end_col}1"
        return f"{prefix}1:1"

    # ------------------------------------------------------------------
    # Chunk generator
    # ------------------------------------------------------------------

    def _read_chunks(
        self,
        hook: GoogleSheetsHook,
        data_start_row: int,
        headers: list[str] | None,
        row_skip: list[dict] | None = None,
        row_stop: list[dict] | None = None,
    ) -> Iterator[list[list[Any]]]:
        """Yield rows chunk-by-chunk from the sheet."""
        current_row = data_start_row
        while True:
            end_row = current_row + self.chunk_size - 1
            chunk_range = self._build_range(current_row, end_row)
            logger.info("Reading chunk %s", chunk_range)

            rows = hook.get_values(self.spreadsheet_id, chunk_range)
            if not rows:
                break

            raw_count = len(rows)

            if self.schema and headers:
                rows = [apply_schema_to_row(row, headers, self.schema, strip_strings=self.strip_strings) for row in rows]

            # Apply row_stop: find the first matching row and truncate
            stopped = False
            if row_stop and headers:
                for idx, row in enumerate(rows):
                    row_dict = {h: (row[i] if i < len(row) else None) for i, h in enumerate(headers)}
                    if matches_any(row_dict, row_stop):
                        rows = rows[:idx]
                        stopped = True
                        break

            # Apply row_skip: filter out matching rows
            if row_skip and headers:
                rows = [
                    row for row in rows
                    if not matches_any(
                        {h: (row[i] if i < len(row) else None) for i, h in enumerate(headers)},
                        row_skip,
                    )
                ]

            if rows:
                yield rows

            if stopped or raw_count < self.chunk_size:
                break
            current_row += self.chunk_size

    # ------------------------------------------------------------------
    # execute
    # ------------------------------------------------------------------

    def execute(self, context: Any) -> Any:
        hook = GoogleSheetsHook(gcp_conn_id=self.gcp_conn_id)

        headers: list[str] | None = None
        data_start_row = 1

        # Step 1 — read headers
        if self.has_headers:
            header_range = self._build_header_range()
            logger.info("Reading headers from %s", header_range)
            header_rows = hook.get_values(self.spreadsheet_id, header_range)
            if header_rows:
                if self.column_mapping:
                    # column_mapping takes priority — skip all header
                    # processing and apply the mapping to raw names.
                    headers = process_headers(header_rows[0])
                    headers = [self.column_mapping.get(h, h) for h in headers]
                else:
                    headers = process_headers(
                        header_rows[0],
                        transliterate=self.transliterate_headers,
                        normalize=self.normalize_headers,
                        sanitize=self.sanitize_headers,
                        lowercase=self.lowercase_headers,
                    )
                logger.info("Headers: %s", headers)
            data_start_row = 2

        # Step 2 — validate schema
        if self.schema and headers:
            validate_schema(headers, self.schema)

        # Step 2b — normalize and validate row filter conditions
        row_skip = normalize_conditions(self.row_skip)
        row_stop = normalize_conditions(self.row_stop)
        if row_skip:
            validate_conditions(row_skip)
        if row_stop:
            validate_conditions(row_stop)

        # Step 3 — dispatch by output type
        if self.output_type == "csv":
            return self._stream_to_csv(hook, headers, data_start_row, row_skip, row_stop)
        if self.output_type == "json":
            return self._stream_to_json(hook, headers, data_start_row, row_skip, row_stop)
        if self.output_type == "jsonl":
            return self._stream_to_jsonl(hook, headers, data_start_row, row_skip, row_stop)
        return self._read_to_xcom(hook, headers, data_start_row, row_skip, row_stop)

    # ------------------------------------------------------------------
    # Output strategies
    # ------------------------------------------------------------------

    def _stream_to_csv(
        self,
        hook: GoogleSheetsHook,
        headers: list[str] | None,
        data_start_row: int,
        row_skip: list[dict] | None = None,
        row_stop: list[dict] | None = None,
    ) -> str:
        if not self.output_path:
            raise ValueError("output_path is required when output_type='csv'")

        total = 0
        with open(self.output_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            if headers:
                writer.writerow(headers)
            for chunk in self._read_chunks(hook, data_start_row, headers, row_skip, row_stop):
                writer.writerows(chunk)
                total += len(chunk)

        logger.info("Streamed %d rows to CSV %s", total, self.output_path)
        return self.output_path

    def _stream_to_json(
        self,
        hook: GoogleSheetsHook,
        headers: list[str] | None,
        data_start_row: int,
        row_skip: list[dict] | None = None,
        row_stop: list[dict] | None = None,
    ) -> str:
        if not self.output_path:
            raise ValueError("output_path is required when output_type='json'")

        total = 0
        first = True
        with open(self.output_path, "w", encoding="utf-8") as f:
            f.write("[")
            for chunk in self._read_chunks(hook, data_start_row, headers, row_skip, row_stop):
                for row in chunk:
                    if not first:
                        f.write(",")
                    first = False
                    f.write("\n")
                    if headers:
                        obj = {h: (row[i] if i < len(row) else None) for i, h in enumerate(headers)}
                        json.dump(obj, f, ensure_ascii=False, default=str)
                    else:
                        json.dump(row, f, ensure_ascii=False, default=str)
                    total += 1
            f.write("\n]\n")

        logger.info("Streamed %d rows to JSON %s", total, self.output_path)
        return self.output_path

    def _stream_to_jsonl(
        self,
        hook: GoogleSheetsHook,
        headers: list[str] | None,
        data_start_row: int,
        row_skip: list[dict] | None = None,
        row_stop: list[dict] | None = None,
    ) -> str:
        if not self.output_path:
            raise ValueError("output_path is required when output_type='jsonl'")

        total = 0
        with open(self.output_path, "w", encoding="utf-8") as f:
            for chunk in self._read_chunks(hook, data_start_row, headers, row_skip, row_stop):
                for row in chunk:
                    if headers:
                        obj = {h: (row[i] if i < len(row) else None) for i, h in enumerate(headers)}
                        json.dump(obj, f, ensure_ascii=False, default=str)
                    else:
                        json.dump(row, f, ensure_ascii=False, default=str)
                    f.write("\n")
                    total += 1

        logger.info("Streamed %d rows to JSONL %s", total, self.output_path)
        return self.output_path

    def _read_to_xcom(
        self,
        hook: GoogleSheetsHook,
        headers: list[str] | None,
        data_start_row: int,
        row_skip: list[dict] | None = None,
        row_stop: list[dict] | None = None,
    ) -> Any:
        all_rows: list[list[Any]] = []
        for chunk in self._read_chunks(hook, data_start_row, headers, row_skip, row_stop):
            all_rows.extend(chunk)
            if len(all_rows) > self.max_xcom_rows:
                raise GoogleSheetsDataError(
                    f"Row count ({len(all_rows)}) exceeds max_xcom_rows "
                    f"({self.max_xcom_rows}). Use output_type='csv' or "
                    f"'json' for large datasets."
                )

        logger.info("Finished reading. Total rows: %d", len(all_rows))

        if headers:
            return rows_to_dicts(all_rows, headers)
        return all_rows
