"""Operators for managing Google Sheets spreadsheets and sheets."""

from __future__ import annotations

import logging
import re
from typing import Any, Sequence

from airflow.models import BaseOperator

from airflow_provider_google_sheets.hooks.google_sheets import GoogleSheetsHook

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
