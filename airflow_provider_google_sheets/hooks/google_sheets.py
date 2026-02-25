"""Hook for interacting with Google Sheets API v4."""

from __future__ import annotations

import json
import logging
from typing import Any, Sequence

from airflow.hooks.base import BaseHook
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build, Resource
from googleapiclient.errors import HttpError

from airflow_provider_google_sheets.exceptions import GoogleSheetsAuthError, GoogleSheetsAPIError
from airflow_provider_google_sheets.utils.retry import retry_with_backoff

logger = logging.getLogger(__name__)

SHEETS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


class GoogleSheetsHook(BaseHook):
    """Hook for Google Sheets API v4.

    Authenticates via a Google service account whose JSON key is stored
    in an Airflow Connection (``conn_type='google_sheets'``).

    The service-account JSON can be provided in:
    - The ``keyfile_dict`` field inside Connection *extra*, **or**
    - The entire Connection *extra* field as the raw JSON key.

    Args:
        gcp_conn_id: Airflow Connection ID.
    """

    conn_type = "google_sheets"
    conn_name_attr = "gcp_conn_id"
    default_conn_name = "google_cloud_default"
    hook_name = "Google Sheets"

    def __init__(self, gcp_conn_id: str = default_conn_name, **kwargs: Any) -> None:
        super().__init__()
        self.gcp_conn_id = gcp_conn_id
        self._service: Resource | None = None

    def get_conn(self) -> Resource:
        """Return a cached Google Sheets API service resource."""
        if self._service is not None:
            return self._service
        self._service = self._build_service()
        return self._service

    def _build_service(self) -> Resource:
        connection = self.get_connection(self.gcp_conn_id)
        try:
            extra = connection.extra_dejson
            keyfile_dict = extra.get("keyfile_dict", extra)
            if isinstance(keyfile_dict, str):
                keyfile_dict = json.loads(keyfile_dict)

            credentials = Credentials.from_service_account_info(
                keyfile_dict,
                scopes=SHEETS_SCOPES,
            )
        except Exception as e:
            raise GoogleSheetsAuthError(
                f"Failed to authenticate with Google Sheets API using connection "
                f"'{self.gcp_conn_id}': {e}"
            ) from e

        return build("sheets", "v4", credentials=credentials, cache_discovery=False)

    # ------------------------------------------------------------------
    # Read helpers
    # ------------------------------------------------------------------

    @retry_with_backoff()
    def get_values(
        self,
        spreadsheet_id: str,
        range_: str,
        value_render_option: str = "UNFORMATTED_VALUE",
        date_time_render_option: str = "FORMATTED_STRING",
    ) -> list[list[Any]]:
        """Read values from a single range.

        Returns an empty list when the range contains no data.
        """
        result = (
            self.get_conn()
            .spreadsheets()
            .values()
            .get(
                spreadsheetId=spreadsheet_id,
                range=range_,
                valueRenderOption=value_render_option,
                dateTimeRenderOption=date_time_render_option,
            )
            .execute()
        )
        return result.get("values", [])

    @retry_with_backoff()
    def batch_get_values(
        self,
        spreadsheet_id: str,
        ranges: list[str],
        value_render_option: str = "UNFORMATTED_VALUE",
        date_time_render_option: str = "FORMATTED_STRING",
    ) -> list[list[list[Any]]]:
        """Read values from multiple ranges in a single request."""
        result = (
            self.get_conn()
            .spreadsheets()
            .values()
            .batchGet(
                spreadsheetId=spreadsheet_id,
                ranges=ranges,
                valueRenderOption=value_render_option,
                dateTimeRenderOption=date_time_render_option,
            )
            .execute()
        )
        return [vr.get("values", []) for vr in result.get("valueRanges", [])]

    # ------------------------------------------------------------------
    # Write helpers
    # ------------------------------------------------------------------

    @retry_with_backoff()
    def update_values(
        self,
        spreadsheet_id: str,
        range_: str,
        values: list[list[Any]],
        value_input_option: str = "USER_ENTERED",
    ) -> dict:
        """Write (overwrite) values to a range."""
        body = {"values": values}
        return (
            self.get_conn()
            .spreadsheets()
            .values()
            .update(
                spreadsheetId=spreadsheet_id,
                range=range_,
                valueInputOption=value_input_option,
                body=body,
            )
            .execute()
        )

    @retry_with_backoff()
    def append_values(
        self,
        spreadsheet_id: str,
        range_: str,
        values: list[list[Any]],
        value_input_option: str = "USER_ENTERED",
    ) -> dict:
        """Append values after the last row of a range."""
        body = {"values": values}
        return (
            self.get_conn()
            .spreadsheets()
            .values()
            .append(
                spreadsheetId=spreadsheet_id,
                range=range_,
                valueInputOption=value_input_option,
                body=body,
            )
            .execute()
        )

    @retry_with_backoff()
    def clear_values(self, spreadsheet_id: str, range_: str) -> dict:
        """Clear all values in a range."""
        return (
            self.get_conn()
            .spreadsheets()
            .values()
            .clear(spreadsheetId=spreadsheet_id, range=range_)
            .execute()
        )

    @retry_with_backoff()
    def batch_update_values(
        self,
        spreadsheet_id: str,
        data: list[dict],
        value_input_option: str = "USER_ENTERED",
    ) -> dict:
        """Update multiple ranges in a single request via ``values.batchUpdate``.

        Args:
            spreadsheet_id: Target spreadsheet.
            data: List of ``{"range": "...", "values": [[...]]}`` dicts.
            value_input_option: How to interpret input data.
        """
        body = {
            "valueInputOption": value_input_option,
            "data": data,
        }
        return (
            self.get_conn()
            .spreadsheets()
            .values()
            .batchUpdate(spreadsheetId=spreadsheet_id, body=body)
            .execute()
        )

    @retry_with_backoff()
    def batch_update(self, spreadsheet_id: str, requests: list[dict]) -> dict:
        """Execute a batch of spreadsheet update requests (insert/delete rows, etc.)."""
        body = {"requests": requests}
        return (
            self.get_conn()
            .spreadsheets()
            .batchUpdate(spreadsheetId=spreadsheet_id, body=body)
            .execute()
        )

    # ------------------------------------------------------------------
    # Spreadsheet / sheet management
    # ------------------------------------------------------------------

    @retry_with_backoff()
    def create_spreadsheet(self, title: str, sheet_titles: list[str] | None = None) -> str:
        """Create a new spreadsheet and return its ID.

        Args:
            title: Spreadsheet title.
            sheet_titles: Optional list of sheet names.  When *None*, the
                spreadsheet is created with a single default sheet.

        Returns:
            The newly created ``spreadsheetId``.
        """
        body: dict[str, Any] = {"properties": {"title": title}}
        if sheet_titles:
            body["sheets"] = [
                {"properties": {"title": st}} for st in sheet_titles
            ]
        result = self.get_conn().spreadsheets().create(body=body).execute()
        return result["spreadsheetId"]

    @retry_with_backoff()
    def create_sheet(self, spreadsheet_id: str, title: str) -> dict:
        """Add a new sheet (tab) to an existing spreadsheet."""
        request = {"addSheet": {"properties": {"title": title}}}
        return self.batch_update(spreadsheet_id, [request])

    @retry_with_backoff()
    def get_spreadsheet_metadata(self, spreadsheet_id: str) -> dict:
        """Return spreadsheet metadata (sheets, properties, etc.)."""
        return (
            self.get_conn()
            .spreadsheets()
            .get(spreadsheetId=spreadsheet_id, includeGridData=False)
            .execute()
        )

    def get_sheet_id(self, spreadsheet_id: str, sheet_name: str) -> int:
        """Resolve a sheet *name* to its numeric ``sheetId``."""
        meta = self.get_spreadsheet_metadata(spreadsheet_id)
        for sheet in meta.get("sheets", []):
            if sheet["properties"]["title"] == sheet_name:
                return sheet["properties"]["sheetId"]
        raise GoogleSheetsAPIError(
            f"Sheet '{sheet_name}' not found in spreadsheet '{spreadsheet_id}'"
        )
