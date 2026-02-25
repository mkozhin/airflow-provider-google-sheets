"""Operators for managing Google Sheets spreadsheets and sheets."""

from __future__ import annotations

import logging
from typing import Any, Sequence

from airflow.models import BaseOperator

from airflow_google_sheets.hooks.google_sheets import GoogleSheetsHook

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
