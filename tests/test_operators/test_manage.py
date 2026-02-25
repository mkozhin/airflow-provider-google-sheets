"""Tests for spreadsheet/sheet management operators."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from googleapiclient.errors import HttpError
from httplib2 import Response

from airflow_google_sheets.operators.manage import (
    GoogleSheetsCreateSheetOperator,
    GoogleSheetsCreateSpreadsheetOperator,
)


SPREADSHEET_ID = "test-spreadsheet-id"


@pytest.fixture
def mock_hook():
    with patch(
        "airflow_google_sheets.operators.manage.GoogleSheetsHook"
    ) as hook_cls:
        hook = MagicMock()
        hook_cls.return_value = hook
        yield hook


@pytest.fixture
def context():
    return MagicMock()


# ------------------------------------------------------------------
# CreateSpreadsheet
# ------------------------------------------------------------------


class TestCreateSpreadsheet:
    def test_creates_with_default_sheet(self, mock_hook, context):
        mock_hook.create_spreadsheet.return_value = "new-id-123"

        op = GoogleSheetsCreateSpreadsheetOperator(
            task_id="test",
            title="My Report",
        )
        result = op.execute(context)

        assert result == "new-id-123"
        mock_hook.create_spreadsheet.assert_called_once_with(
            title="My Report",
            sheet_titles=None,
        )

    def test_creates_with_named_sheets(self, mock_hook, context):
        mock_hook.create_spreadsheet.return_value = "new-id-456"

        op = GoogleSheetsCreateSpreadsheetOperator(
            task_id="test",
            title="Multi-Sheet",
            sheet_titles=["Data", "Summary", "Logs"],
        )
        result = op.execute(context)

        assert result == "new-id-456"
        mock_hook.create_spreadsheet.assert_called_once_with(
            title="Multi-Sheet",
            sheet_titles=["Data", "Summary", "Logs"],
        )

    def test_returns_spreadsheet_id_for_xcom(self, mock_hook, context):
        mock_hook.create_spreadsheet.return_value = "xcom-id"

        op = GoogleSheetsCreateSpreadsheetOperator(
            task_id="test",
            title="XCom Test",
        )
        result = op.execute(context)

        # Return value is pushed to XCom automatically by Airflow
        assert isinstance(result, str)
        assert result == "xcom-id"

    def test_propagates_api_error(self, mock_hook, context):
        mock_hook.create_spreadsheet.side_effect = HttpError(
            Response({"status": 403}), b"quota exceeded"
        )

        op = GoogleSheetsCreateSpreadsheetOperator(
            task_id="test",
            title="Will Fail",
        )
        with pytest.raises(HttpError):
            op.execute(context)


# ------------------------------------------------------------------
# CreateSheet
# ------------------------------------------------------------------


class TestCreateSheet:
    def test_creates_sheet(self, mock_hook, context):
        mock_hook.create_sheet.return_value = {"replies": [{}]}

        op = GoogleSheetsCreateSheetOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            sheet_title="NewTab",
        )
        result = op.execute(context)

        mock_hook.create_sheet.assert_called_once_with(
            spreadsheet_id=SPREADSHEET_ID,
            title="NewTab",
        )
        assert result == {"replies": [{}]}

    def test_propagates_duplicate_sheet_error(self, mock_hook, context):
        mock_hook.create_sheet.side_effect = HttpError(
            Response({"status": 400}),
            b"A sheet with the name 'Existing' already exists",
        )

        op = GoogleSheetsCreateSheetOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            sheet_title="Existing",
        )
        with pytest.raises(HttpError):
            op.execute(context)

    def test_propagates_invalid_spreadsheet_error(self, mock_hook, context):
        mock_hook.create_sheet.side_effect = HttpError(
            Response({"status": 404}), b"Spreadsheet not found"
        )

        op = GoogleSheetsCreateSheetOperator(
            task_id="test",
            spreadsheet_id="bad-id",
            sheet_title="Tab",
        )
        with pytest.raises(HttpError):
            op.execute(context)

    def test_custom_connection_id(self, mock_hook, context):
        mock_hook.create_sheet.return_value = {"replies": [{}]}

        with patch(
            "airflow_google_sheets.operators.manage.GoogleSheetsHook"
        ) as hook_cls:
            hook_cls.return_value = mock_hook
            op = GoogleSheetsCreateSheetOperator(
                task_id="test",
                gcp_conn_id="my_custom_conn",
                spreadsheet_id=SPREADSHEET_ID,
                sheet_title="Tab",
            )
            op.execute(context)
            hook_cls.assert_called_once_with(gcp_conn_id="my_custom_conn")
