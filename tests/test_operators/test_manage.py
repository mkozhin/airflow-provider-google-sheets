"""Tests for spreadsheet/sheet management operators."""

from __future__ import annotations

import json
import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest
from googleapiclient.errors import HttpError
from httplib2 import Response

from airflow_provider_google_sheets.operators.manage import (
    GoogleSheetsCreateSheetOperator,
    GoogleSheetsCreateSpreadsheetOperator,
    GoogleSheetsExtractPartitionsOperator,
    GoogleSheetsListSheetsOperator,
    GoogleSheetsUniqueValuesOperator,
)


SPREADSHEET_ID = "test-spreadsheet-id"


@pytest.fixture
def mock_hook():
    with patch(
        "airflow_provider_google_sheets.operators.manage.GoogleSheetsHook"
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
            "airflow_provider_google_sheets.operators.manage.GoogleSheetsHook"
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


# ------------------------------------------------------------------
# ListSheets
# ------------------------------------------------------------------

METADATA_RESPONSE = {
    "sheets": [
        {"properties": {"title": "Data", "index": 0}},
        {"properties": {"title": "Summary", "index": 1}},
        {"properties": {"title": "Logs", "index": 2}},
        {"properties": {"title": "_Hidden", "index": 3}},
        {"properties": {"title": "Archive_2024", "index": 4}},
    ]
}


class TestListSheets:
    def test_returns_all_sheets(self, mock_hook, context):
        mock_hook.get_spreadsheet_metadata.return_value = METADATA_RESPONSE

        op = GoogleSheetsListSheetsOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
        )
        result = op.execute(context)

        assert result == ["Data", "Summary", "Logs", "_Hidden", "Archive_2024"]

    def test_name_pattern_filter(self, mock_hook, context):
        mock_hook.get_spreadsheet_metadata.return_value = METADATA_RESPONSE

        op = GoogleSheetsListSheetsOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            name_pattern=r"^(Data|Summary)$",
        )
        result = op.execute(context)

        assert result == ["Data", "Summary"]

    def test_name_pattern_no_match(self, mock_hook, context):
        mock_hook.get_spreadsheet_metadata.return_value = METADATA_RESPONSE

        op = GoogleSheetsListSheetsOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            name_pattern=r"^NonExistent$",
        )
        result = op.execute(context)

        assert result == []

    def test_exclude_pattern(self, mock_hook, context):
        mock_hook.get_spreadsheet_metadata.return_value = METADATA_RESPONSE

        op = GoogleSheetsListSheetsOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            exclude_pattern=r"^_",
        )
        result = op.execute(context)

        assert result == ["Data", "Summary", "Logs", "Archive_2024"]

    def test_index_range(self, mock_hook, context):
        mock_hook.get_spreadsheet_metadata.return_value = METADATA_RESPONSE

        op = GoogleSheetsListSheetsOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            index_range=(1, 3),
        )
        result = op.execute(context)

        assert result == ["Summary", "Logs"]

    def test_combined_filters(self, mock_hook, context):
        mock_hook.get_spreadsheet_metadata.return_value = METADATA_RESPONSE

        op = GoogleSheetsListSheetsOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            index_range=(0, 4),
            name_pattern=r"[A-Z]",
            exclude_pattern=r"^_",
        )
        result = op.execute(context)

        assert result == ["Data", "Summary", "Logs"]

    def test_returns_list_of_strings(self, mock_hook, context):
        mock_hook.get_spreadsheet_metadata.return_value = METADATA_RESPONSE

        op = GoogleSheetsListSheetsOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
        )
        result = op.execute(context)

        assert isinstance(result, list)
        assert all(isinstance(s, str) for s in result)


# ------------------------------------------------------------------
# ExtractPartitions
# ------------------------------------------------------------------


class TestExtractPartitions:
    def test_inline_list_of_dicts(self, context):
        data = [
            {"period": "2026-01", "value": 10},
            {"period": "2026-02", "value": 20},
            {"period": "2026-01", "value": 30},
            {"period": "2026-03", "value": 40},
        ]

        op = GoogleSheetsExtractPartitionsOperator(
            task_id="test",
            partition_column="period",
            data=data,
        )
        result = op.execute(context)

        assert result == [
            {"sheet_name": "2026-01", "partition_value": "2026-01"},
            {"sheet_name": "2026-02", "partition_value": "2026-02"},
            {"sheet_name": "2026-03", "partition_value": "2026-03"},
        ]

    def test_custom_template(self, context):
        data = [
            {"segment": "A", "val": 1},
            {"segment": "B", "val": 2},
        ]

        op = GoogleSheetsExtractPartitionsOperator(
            task_id="test",
            partition_column="segment",
            sheet_name_template="Отчёт {value}",
            data=data,
        )
        result = op.execute(context)

        assert result == [
            {"sheet_name": "Отчёт A", "partition_value": "A"},
            {"sheet_name": "Отчёт B", "partition_value": "B"},
        ]

    def test_numeric_partition_values(self, context):
        data = [
            {"year": 2024, "amount": 100},
            {"year": 2025, "amount": 200},
            {"year": 2024, "amount": 300},
        ]

        op = GoogleSheetsExtractPartitionsOperator(
            task_id="test",
            partition_column="year",
            data=data,
        )
        result = op.execute(context)

        assert result == [
            {"sheet_name": "2024", "partition_value": "2024"},
            {"sheet_name": "2025", "partition_value": "2025"},
        ]
        # partition_value is always a string
        assert all(isinstance(r["partition_value"], str) for r in result)

    def test_xcom_data(self, context):
        xcom_data = [
            {"region": "US", "sales": 100},
            {"region": "EU", "sales": 200},
        ]
        context["ti"].xcom_pull.return_value = xcom_data

        op = GoogleSheetsExtractPartitionsOperator(
            task_id="test",
            partition_column="region",
            data_xcom_task_id="fetch_data",
        )
        result = op.execute(context)

        context["ti"].xcom_pull.assert_called_once_with(
            task_ids="fetch_data", key="return_value"
        )
        assert result == [
            {"sheet_name": "US", "partition_value": "US"},
            {"sheet_name": "EU", "partition_value": "EU"},
        ]

    def test_empty_data(self, context):
        op = GoogleSheetsExtractPartitionsOperator(
            task_id="test",
            partition_column="period",
            data=[],
        )
        result = op.execute(context)

        assert result == []

    def test_column_not_found_raises(self, context):
        data = [{"period": "2026-01", "value": 10}]

        op = GoogleSheetsExtractPartitionsOperator(
            task_id="test",
            partition_column="nonexistent",
            data=data,
        )
        with pytest.raises(ValueError, match="not found in headers"):
            op.execute(context)

    def test_has_headers_false_raises(self, context):
        data = [["2026-01", 10], ["2026-02", 20]]

        op = GoogleSheetsExtractPartitionsOperator(
            task_id="test",
            partition_column="period",
            has_headers=False,
            data=data,
        )
        with pytest.raises(ValueError, match="has_headers must be True"):
            op.execute(context)

    def test_deduplication_preserves_order(self, context):
        data = [
            {"cat": "C", "v": 1},
            {"cat": "A", "v": 2},
            {"cat": "B", "v": 3},
            {"cat": "A", "v": 4},
            {"cat": "C", "v": 5},
        ]

        op = GoogleSheetsExtractPartitionsOperator(
            task_id="test",
            partition_column="cat",
            data=data,
        )
        result = op.execute(context)

        assert [r["partition_value"] for r in result] == ["C", "A", "B"]

    def test_jsonl_file(self, context):
        """Primary use case: data from a JSONL file."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".jsonl", delete=False
        ) as f:
            for row in [
                {"period": "2026-01", "amount": 100},
                {"period": "2026-02", "amount": 200},
                {"period": "2026-01", "amount": 150},
                {"period": "2026-03", "amount": 300},
            ]:
                json.dump(row, f)
                f.write("\n")
            path = f.name

        try:
            op = GoogleSheetsExtractPartitionsOperator(
                task_id="test",
                partition_column="period",
                sheet_name_template="Report {value}",
                data=path,
            )
            result = op.execute(context)

            assert result == [
                {"sheet_name": "Report 2026-01", "partition_value": "2026-01"},
                {"sheet_name": "Report 2026-02", "partition_value": "2026-02"},
                {"sheet_name": "Report 2026-03", "partition_value": "2026-03"},
            ]
        finally:
            os.unlink(path)

    def test_csv_file(self, context):
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        ) as f:
            f.write("region,sales\n")
            f.write("US,100\n")
            f.write("EU,200\n")
            f.write("US,300\n")
            path = f.name

        try:
            op = GoogleSheetsExtractPartitionsOperator(
                task_id="test",
                partition_column="region",
                data=path,
            )
            result = op.execute(context)

            assert result == [
                {"sheet_name": "US", "partition_value": "US"},
                {"sheet_name": "EU", "partition_value": "EU"},
            ]
        finally:
            os.unlink(path)

    def test_json_file_via_xcom(self, context):
        """File path passed through XCom."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".jsonl", delete=False
        ) as f:
            for row in [
                {"cat": "A", "v": 1},
                {"cat": "B", "v": 2},
            ]:
                json.dump(row, f)
                f.write("\n")
            path = f.name

        try:
            context["ti"].xcom_pull.return_value = path

            op = GoogleSheetsExtractPartitionsOperator(
                task_id="test",
                partition_column="cat",
                data_xcom_task_id="upstream",
            )
            result = op.execute(context)

            assert result == [
                {"sheet_name": "A", "partition_value": "A"},
                {"sheet_name": "B", "partition_value": "B"},
            ]
        finally:
            os.unlink(path)

    def test_no_data_raises(self, context):
        op = GoogleSheetsExtractPartitionsOperator(
            task_id="test",
            partition_column="period",
        )
        with pytest.raises(ValueError, match="No data provided"):
            op.execute(context)


# ------------------------------------------------------------------
# UniqueValuesOperator
# ------------------------------------------------------------------


class TestUniqueValuesOperator:
    def test_basic_unique_values_in_order(self, mock_hook, context):
        mock_hook.get_values.side_effect = [
            [["city", "value"]],                                           # headers
            [["Moscow", "10"], ["Berlin", "20"], ["Moscow", "30"], ["Paris", "40"]],  # data
            [],
        ]
        op = GoogleSheetsUniqueValuesOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            column="city",
            transliterate_headers=False,
            sanitize_headers=False,
            lowercase_headers=False,
        )
        result = op.execute(context)
        assert result == ["Moscow", "Berlin", "Paris"]

    def test_exclude_values(self, mock_hook, context):
        mock_hook.get_values.side_effect = [
            [["city"]],
            [["Moscow"], ["Berlin"], ["Paris"], ["Moscow"]],
            [],
        ]
        op = GoogleSheetsUniqueValuesOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            column="city",
            exclude_values=["Berlin"],
            transliterate_headers=False,
            sanitize_headers=False,
            lowercase_headers=False,
        )
        result = op.execute(context)
        assert result == ["Moscow", "Paris"]

    def test_exclude_empty_string(self, mock_hook, context):
        mock_hook.get_values.side_effect = [
            [["city"]],
            [["Moscow"], [""], ["Berlin"], [""]],
            [],
        ]
        op = GoogleSheetsUniqueValuesOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            column="city",
            exclude_values=[""],
            transliterate_headers=False,
            sanitize_headers=False,
            lowercase_headers=False,
        )
        result = op.execute(context)
        assert result == ["Moscow", "Berlin"]

    def test_column_not_found_raises(self, mock_hook, context):
        mock_hook.get_values.side_effect = [
            [["city", "value"]],
        ]
        op = GoogleSheetsUniqueValuesOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            column="nonexistent",
            transliterate_headers=False,
            sanitize_headers=False,
            lowercase_headers=False,
        )
        with pytest.raises(ValueError, match="Column 'nonexistent' not found"):
            op.execute(context)

    def test_column_with_column_mapping(self, mock_hook, context):
        mock_hook.get_values.side_effect = [
            [["Город", "Значение"]],
            [["Москва", "10"], ["Берлин", "20"], ["Москва", "30"]],
            [],
        ]
        op = GoogleSheetsUniqueValuesOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            column="city",
            column_mapping={"Город": "city", "Значение": "value"},
        )
        result = op.execute(context)
        assert result == ["Москва", "Берлин"]

    def test_duplicates_across_chunks_excluded(self, mock_hook, context):
        mock_hook.get_values.side_effect = [
            [["city"]],
            [["Moscow"], ["Berlin"]],   # chunk 1 (chunk_size=2)
            [["Moscow"], ["Paris"]],    # chunk 2
            [],
        ]
        op = GoogleSheetsUniqueValuesOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            column="city",
            chunk_size=2,
            transliterate_headers=False,
            sanitize_headers=False,
            lowercase_headers=False,
        )
        result = op.execute(context)
        assert result == ["Moscow", "Berlin", "Paris"]
