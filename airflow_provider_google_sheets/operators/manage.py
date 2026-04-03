"""Operators for managing Google Sheets spreadsheets and sheets."""

from __future__ import annotations

import logging
import re
from typing import Any, Sequence

from airflow.models import BaseOperator

from airflow_provider_google_sheets.hooks.google_sheets import GoogleSheetsHook
from airflow_provider_google_sheets.utils.data_formats import normalize_input_data
from airflow_provider_google_sheets.utils.headers import process_headers

logger = logging.getLogger(__name__)


class GoogleSheetsCreateSpreadsheetOperator(BaseOperator):
    """Create a new Google Sheets spreadsheet.

    The newly created ``spreadsheet_id`` is returned (pushed to XCom).

    Args:
        gcp_conn_id: Airflow Connection ID.
        title: Spreadsheet title.
        sheet_titles: Optional list of sheet (tab) names.  When *None* the
            spreadsheet is created with a single default sheet.
    """

    template_fields: Sequence[str] = ("title", "sheet_titles")

    def __init__(
        self,
        *,
        gcp_conn_id: str = "google_cloud_default",
        title: str,
        sheet_titles: list[str] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.title = title
        self.sheet_titles = sheet_titles

    def execute(self, context: Any) -> str:
        hook = GoogleSheetsHook(gcp_conn_id=self.gcp_conn_id)
        spreadsheet_id = hook.create_spreadsheet(
            title=self.title,
            sheet_titles=self.sheet_titles,
        )
        logger.info(
            "Created spreadsheet '%s' (id=%s) with sheets %s",
            self.title,
            spreadsheet_id,
            self.sheet_titles or ["(default)"],
        )
        return spreadsheet_id


class GoogleSheetsCreateSheetOperator(BaseOperator):
    """Add a new sheet (tab) to an existing Google Sheets spreadsheet.

    Args:
        gcp_conn_id: Airflow Connection ID.
        spreadsheet_id: Target spreadsheet ID.
        sheet_title: Name of the new sheet.
    """

    template_fields: Sequence[str] = ("spreadsheet_id", "sheet_title")

    def __init__(
        self,
        *,
        gcp_conn_id: str = "google_cloud_default",
        spreadsheet_id: str,
        sheet_title: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.spreadsheet_id = spreadsheet_id
        self.sheet_title = sheet_title

    def execute(self, context: Any) -> dict:
        hook = GoogleSheetsHook(gcp_conn_id=self.gcp_conn_id)
        result = hook.create_sheet(
            spreadsheet_id=self.spreadsheet_id,
            title=self.sheet_title,
        )
        logger.info(
            "Created sheet '%s' in spreadsheet '%s'",
            self.sheet_title,
            self.spreadsheet_id,
        )
        return result


class GoogleSheetsListSheetsOperator(BaseOperator):
    """List sheet (tab) names of a Google Sheets spreadsheet.

    Returns a ``list[str]`` of sheet names, compatible with Airflow dynamic
    task mapping (``expand(sheet_name=op.output)``).

    Args:
        gcp_conn_id: Airflow Connection ID.
        spreadsheet_id: Target spreadsheet ID.
        name_pattern: Regex to include sheets by name (``re.search``).
        exclude_pattern: Regex to exclude sheets by name (``re.search``).
        index_range: ``(start, end)`` slice by sheet position (0-based,
            start inclusive, end exclusive).
    """

    template_fields: Sequence[str] = ("spreadsheet_id", "name_pattern", "exclude_pattern")

    def __init__(
        self,
        *,
        gcp_conn_id: str = "google_cloud_default",
        spreadsheet_id: str,
        name_pattern: str | None = None,
        exclude_pattern: str | None = None,
        index_range: tuple[int, int] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.spreadsheet_id = spreadsheet_id
        self.name_pattern = name_pattern
        self.exclude_pattern = exclude_pattern
        self.index_range = index_range

    def execute(self, context: Any) -> list[str]:
        hook = GoogleSheetsHook(gcp_conn_id=self.gcp_conn_id)
        meta = hook.get_spreadsheet_metadata(self.spreadsheet_id)
        sheets = [s["properties"]["title"] for s in meta.get("sheets", [])]

        if self.index_range is not None:
            start, end = self.index_range
            sheets = sheets[start:end]

        if self.name_pattern is not None:
            sheets = [s for s in sheets if re.search(self.name_pattern, s)]

        if self.exclude_pattern is not None:
            sheets = [s for s in sheets if not re.search(self.exclude_pattern, s)]

        logger.info(
            "Listed %d sheets in spreadsheet '%s'",
            len(sheets),
            self.spreadsheet_id,
        )
        return sheets


class GoogleSheetsExtractPartitionsOperator(BaseOperator):
    """Extract unique partition values from data for dynamic task mapping.

    Reads data (inline, from XCom, or from a file), finds unique values in
    the specified ``partition_column``, and returns a ``list[dict]`` suitable
    for Airflow ``expand_kwargs``::

        [
            {"sheet_name": "Report 2026-01", "partition_value": "2026-01"},
            {"sheet_name": "Report 2026-02", "partition_value": "2026-02"},
        ]

    This operator does **not** call the Google Sheets API — it works purely
    with in-memory data.

    Args:
        partition_column: Column name whose unique values define partitions.
        sheet_name_template: Python format string applied to each unique value.
            Use ``{value}`` as the placeholder.  Defaults to ``"{value}"``.
        data: Inline data (``list[list]``, ``list[dict]``, or a file path).
        data_xcom_task_id: Pull data from this task's XCom instead.
        data_xcom_key: XCom key when using *data_xcom_task_id*.
        has_headers: Whether *data* contains a header row.  Must be ``True``
            because partition column lookup requires headers.
    """

    template_fields: Sequence[str] = (
        "partition_column",
        "sheet_name_template",
        "data_xcom_task_id",
        "data_xcom_key",
    )

    def __init__(
        self,
        *,
        partition_column: str,
        sheet_name_template: str = "{value}",
        data: Any = None,
        data_xcom_task_id: str | None = None,
        data_xcom_key: str = "return_value",
        has_headers: bool = True,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.partition_column = partition_column
        self.sheet_name_template = sheet_name_template
        self.data = data
        self.data_xcom_task_id = data_xcom_task_id
        self.data_xcom_key = data_xcom_key
        self.has_headers = has_headers

    def execute(self, context: Any) -> list[dict[str, str]]:
        if not self.has_headers:
            raise ValueError(
                "has_headers must be True for GoogleSheetsExtractPartitionsOperator "
                "because partition column lookup requires header names."
            )

        raw = self.data
        if raw is None and self.data_xcom_task_id:
            ti = context["ti"]
            raw = ti.xcom_pull(
                task_ids=self.data_xcom_task_id, key=self.data_xcom_key
            )
        if raw is None:
            raise ValueError("No data provided: set 'data' or 'data_xcom_task_id'.")

        headers, rows = normalize_input_data(raw, has_headers=self.has_headers)

        if not headers:
            if not rows:
                logger.info("Empty data — returning empty partition list.")
                return []
            raise ValueError("Data has no headers — cannot locate partition column.")

        if self.partition_column not in headers:
            raise ValueError(
                f"Partition column '{self.partition_column}' not found in headers: "
                f"{headers}"
            )

        col_idx = headers.index(self.partition_column)

        seen: dict[str, bool] = {}
        unique_values: list[str] = []
        for row in rows:
            val = str(row[col_idx]) if col_idx < len(row) else ""
            if val not in seen:
                seen[val] = True
                unique_values.append(val)

        result = [
            {
                "sheet_name": self.sheet_name_template.format(value=v),
                "partition_value": v,
            }
            for v in unique_values
        ]

        logger.info(
            "Extracted %d partitions from column '%s': %s",
            len(result),
            self.partition_column,
            [r["partition_value"] for r in result],
        )
        return result


class GoogleSheetsUniqueValuesOperator(BaseOperator):
    """Read unique values from a single column of a Google Sheets spreadsheet.

    Reads the sheet directly via the API (chunk by chunk) and returns a
    ``list[str]`` of unique values in order of first occurrence.  Designed
    for Airflow dynamic task mapping::

        cities = GoogleSheetsUniqueValuesOperator(
            task_id="get_cities", column="city", ...
        )
        read = GoogleSheetsReadOperator.partial(
            task_id="read_by_city", filter_column="city", ...
        ).expand(filter_value=cities.output)

    The ``column`` parameter must be the **processed** header name — i.e. the
    name after transliteration / sanitize / lowercase / column_mapping have
    been applied (the same processing used by ``GoogleSheetsReadOperator``).

    Args:
        gcp_conn_id: Airflow Connection ID for the Google service account.
        spreadsheet_id: The Google Sheets spreadsheet ID.
        sheet_name: Sheet (tab) name.  ``None`` reads the first sheet.
        cell_range: A1-notation range, e.g. ``"A1:Z"``.  ``None`` reads all columns.
        column: Processed header name of the column to extract unique values from.
        exclude_values: Values to exclude from the result.  Pass ``[""]`` to
            exclude empty cells.
        chunk_size: Number of rows to read per API request.
        has_headers: Whether the first row contains column headers.
        transliterate_headers: Transliterate Cyrillic headers to Latin.
        sanitize_headers: Remove spaces and special characters from headers.
        lowercase_headers: Convert headers to lowercase.
        column_mapping: Optional dict mapping raw header names to new names.
            When provided, mapping is applied directly to raw headers (other
            processing options are skipped).
    """

    template_fields: Sequence[str] = ("spreadsheet_id", "sheet_name", "cell_range", "column")

    def __init__(
        self,
        *,
        gcp_conn_id: str = "google_cloud_default",
        spreadsheet_id: str,
        sheet_name: str | None = None,
        cell_range: str | None = None,
        column: str,
        exclude_values: list[str] | None = None,
        chunk_size: int = 5000,
        has_headers: bool = True,
        transliterate_headers: bool = True,
        sanitize_headers: bool = True,
        lowercase_headers: bool = True,
        column_mapping: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.spreadsheet_id = spreadsheet_id
        self.sheet_name = sheet_name
        self.cell_range = cell_range
        self.column = column
        self.exclude_values = exclude_values
        self.chunk_size = chunk_size
        self.has_headers = has_headers
        self.transliterate_headers = transliterate_headers
        self.sanitize_headers = sanitize_headers
        self.lowercase_headers = lowercase_headers
        self.column_mapping = column_mapping

    def _build_range(self, start_row: int, end_row: int) -> str:
        prefix = f"{self.sheet_name}!" if self.sheet_name else ""
        if self.cell_range:
            # strip sheet prefix if present
            cr = self.cell_range.split("!", 1)[1] if "!" in self.cell_range else self.cell_range
            parts = cr.split(":")
            start_col = "".join(c for c in parts[0] if c.isalpha()) or "A"
            end_col = "".join(c for c in parts[1] if c.isalpha()) if len(parts) > 1 else start_col
            return f"{prefix}{start_col}{start_row}:{end_col}{end_row}"
        return f"{prefix}{start_row}:{end_row}"

    def execute(self, context: Any) -> list[str]:
        hook = GoogleSheetsHook(gcp_conn_id=self.gcp_conn_id)

        # Read and process headers
        prefix = f"{self.sheet_name}!" if self.sheet_name else ""
        if self.cell_range:
            cr = self.cell_range.split("!", 1)[1] if "!" in self.cell_range else self.cell_range
            parts = cr.split(":")
            start_col = "".join(c for c in parts[0] if c.isalpha()) or "A"
            end_col = "".join(c for c in parts[1] if c.isalpha()) if len(parts) > 1 else start_col
            header_range = f"{prefix}{start_col}1:{end_col}1"
        else:
            header_range = f"{prefix}1:1"

        logger.info("Reading headers from %s", header_range)
        header_rows = hook.get_values(self.spreadsheet_id, header_range)
        if not header_rows:
            raise ValueError("Sheet has no header row — cannot locate column.")

        if self.column_mapping:
            headers = process_headers(header_rows[0])
            headers = [self.column_mapping.get(h, h) for h in headers]
        else:
            headers = process_headers(
                header_rows[0],
                transliterate=self.transliterate_headers,
                sanitize=self.sanitize_headers,
                lowercase=self.lowercase_headers,
            )

        if self.column not in headers:
            raise ValueError(
                f"Column '{self.column}' not found in processed headers: {headers}. "
                "Note: 'column' must match the processed header name "
                "(after transliterate/sanitize/lowercase/column_mapping)."
            )

        col_idx = headers.index(self.column)
        exclude_set = set(self.exclude_values or [])

        seen: dict[str, None] = {}
        current_row = 2  # data starts after header
        while True:
            end_row = current_row + self.chunk_size - 1
            chunk_range = self._build_range(current_row, end_row)
            logger.info("Reading chunk %s", chunk_range)
            rows = hook.get_values(self.spreadsheet_id, chunk_range)
            if not rows:
                break
            raw_count = len(rows)
            for row in rows:
                val = str(row[col_idx]) if col_idx < len(row) else ""
                if val not in seen and val not in exclude_set:
                    seen[val] = None
            if raw_count < self.chunk_size:
                break
            current_row += self.chunk_size

        result = list(seen.keys())
        logger.info(
            "Found %d unique values in column '%s': %s",
            len(result),
            self.column,
            result,
        )
        return result
