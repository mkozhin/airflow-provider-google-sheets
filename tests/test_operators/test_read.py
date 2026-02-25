"""Tests for GoogleSheetsReadOperator."""

from __future__ import annotations

import json
import os
import tempfile
from datetime import date
from unittest.mock import MagicMock, patch, call

import pytest

from airflow_google_sheets.operators.read import GoogleSheetsReadOperator


SPREADSHEET_ID = "test-spreadsheet-id"


@pytest.fixture
def mock_hook():
    with patch(
        "airflow_google_sheets.operators.read.GoogleSheetsHook"
    ) as hook_cls:
        hook = MagicMock()
        hook_cls.return_value = hook
        yield hook


@pytest.fixture
def context():
    return MagicMock()


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield d


# ------------------------------------------------------------------
# Basic reading
# ------------------------------------------------------------------


class TestBasicRead:
    def test_read_with_headers(self, mock_hook, context):
        mock_hook.get_values.side_effect = [
            [["Name", "Age"]],           # header row
            [["Alice", "30"], ["Bob", "25"]],  # data chunk
        ]

        op = GoogleSheetsReadOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            has_headers=True,
            chunk_size=5000,
        )
        result = op.execute(context)

        assert result == [
            {"Name": "Alice", "Age": "30"},
            {"Name": "Bob", "Age": "25"},
        ]

    def test_read_without_headers(self, mock_hook, context):
        mock_hook.get_values.side_effect = [
            [["Alice", "30"], ["Bob", "25"]],  # data chunk (no header fetch)
        ]

        op = GoogleSheetsReadOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            has_headers=False,
            chunk_size=5000,
        )
        result = op.execute(context)

        assert result == [["Alice", "30"], ["Bob", "25"]]

    def test_empty_sheet(self, mock_hook, context):
        mock_hook.get_values.side_effect = [
            [["Name", "Age"]],  # headers
            [],                  # no data
        ]

        op = GoogleSheetsReadOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            has_headers=True,
        )
        result = op.execute(context)
        assert result == []

    def test_empty_sheet_no_headers(self, mock_hook, context):
        mock_hook.get_values.return_value = []

        op = GoogleSheetsReadOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            has_headers=False,
        )
        result = op.execute(context)
        assert result == []


# ------------------------------------------------------------------
# Chunked reading
# ------------------------------------------------------------------


class TestChunkedRead:
    def test_multiple_chunks(self, mock_hook, context):
        chunk1 = [["r1"], ["r2"], ["r3"]]
        chunk2 = [["r4"], ["r5"]]  # less than chunk_size → last

        mock_hook.get_values.side_effect = [
            [["Col"]],   # headers
            chunk1,       # chunk 1 (size=3, equals chunk_size)
            chunk2,       # chunk 2 (size=2 < chunk_size → done)
        ]

        op = GoogleSheetsReadOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            has_headers=True,
            chunk_size=3,
        )
        result = op.execute(context)

        assert len(result) == 5
        assert result[0] == {"Col": "r1"}
        assert result[4] == {"Col": "r5"}

    def test_exact_chunk_size_triggers_next_read(self, mock_hook, context):
        """When a chunk returns exactly chunk_size rows, the operator
        should try to read the next chunk (which will be empty)."""
        mock_hook.get_values.side_effect = [
            [["Col"]],                  # headers
            [["a"], ["b"]],             # chunk 1 — exactly chunk_size=2
            [],                          # chunk 2 — empty → done
        ]

        op = GoogleSheetsReadOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            has_headers=True,
            chunk_size=2,
        )
        result = op.execute(context)
        assert len(result) == 2
        # 3 get_values calls: headers + chunk1 + chunk2(empty)
        assert mock_hook.get_values.call_count == 3


# ------------------------------------------------------------------
# Sheet name and range
# ------------------------------------------------------------------


class TestRangeBuilding:
    def test_with_sheet_name(self, mock_hook, context):
        mock_hook.get_values.side_effect = [
            [["H"]],      # headers
            [["data"]],   # one row
        ]

        op = GoogleSheetsReadOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            sheet_name="MySheet",
            has_headers=True,
        )
        op.execute(context)

        # Both calls should include sheet name prefix
        for c in mock_hook.get_values.call_args_list:
            range_arg = c[0][1]  # second positional arg
            assert range_arg.startswith("MySheet!")

    def test_with_cell_range(self, mock_hook, context):
        mock_hook.get_values.side_effect = [
            [["H1", "H2"]],
            [["a", "b"]],
        ]

        op = GoogleSheetsReadOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            cell_range="B1:D",
            has_headers=True,
        )
        op.execute(context)

        header_range = mock_hook.get_values.call_args_list[0][0][1]
        assert "B" in header_range
        assert "D" in header_range


# ------------------------------------------------------------------
# Headers processing
# ------------------------------------------------------------------


class TestHeaderProcessing:
    def test_transliterate_headers(self, mock_hook, context):
        mock_hook.get_values.side_effect = [
            [["Дата", "Выручка"]],
            [["01.01", "100"]],
        ]

        op = GoogleSheetsReadOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            has_headers=True,
            transliterate_headers=True,
        )
        result = op.execute(context)

        # Keys should be ASCII
        for key in result[0]:
            assert key.isascii(), f"Non-ASCII key: {key}"

    def test_normalize_headers(self, mock_hook, context):
        mock_hook.get_values.side_effect = [
            [["My Column", "Another One"]],
            [["a", "b"]],
        ]

        op = GoogleSheetsReadOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            has_headers=True,
            normalize_headers=True,
        )
        result = op.execute(context)
        assert list(result[0].keys()) == ["my_column", "another_one"]

    def test_duplicate_headers(self, mock_hook, context):
        mock_hook.get_values.side_effect = [
            [["Col", "Col", "Col"]],
            [["a", "b", "c"]],
        ]

        op = GoogleSheetsReadOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            has_headers=True,
        )
        result = op.execute(context)
        keys = list(result[0].keys())
        assert keys == ["Col", "Col_1", "Col_2"]


# ------------------------------------------------------------------
# Schema
# ------------------------------------------------------------------


class TestSchemaApplication:
    def test_schema_converts_types(self, mock_hook, context):
        mock_hook.get_values.side_effect = [
            [["date", "amount"]],
            [["2024-04-01", "123.45"]],
        ]

        op = GoogleSheetsReadOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            has_headers=True,
            schema={
                "date": {"type": "date"},
                "amount": {"type": "float"},
            },
        )
        result = op.execute(context)
        assert result[0]["date"] == date(2024, 4, 1)
        assert result[0]["amount"] == pytest.approx(123.45)

    def test_schema_validation_fails(self, mock_hook, context):
        mock_hook.get_values.side_effect = [
            [["other_col"]],  # missing required "date" column
        ]

        op = GoogleSheetsReadOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            has_headers=True,
            schema={"date": {"type": "date", "required": True}},
        )

        from airflow_google_sheets.exceptions import GoogleSheetsSchemaError
        with pytest.raises(GoogleSheetsSchemaError):
            op.execute(context)


# ------------------------------------------------------------------
# Output types
# ------------------------------------------------------------------


class TestOutputTypes:
    def test_output_csv(self, mock_hook, context, tmp_dir):
        mock_hook.get_values.side_effect = [
            [["a", "b"]],
            [["1", "2"]],
        ]

        path = os.path.join(tmp_dir, "out.csv")
        op = GoogleSheetsReadOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            has_headers=True,
            output_type="csv",
            output_path=path,
        )
        result = op.execute(context)

        assert result == path
        assert os.path.exists(path)
        with open(path) as f:
            content = f.read()
        assert "a,b" in content
        assert "1,2" in content

    def test_output_json(self, mock_hook, context, tmp_dir):
        mock_hook.get_values.side_effect = [
            [["a", "b"]],
            [["1", "2"]],
        ]

        path = os.path.join(tmp_dir, "out.json")
        op = GoogleSheetsReadOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            has_headers=True,
            output_type="json",
            output_path=path,
        )
        result = op.execute(context)

        assert result == path
        with open(path) as f:
            data = json.load(f)
        assert data == [{"a": "1", "b": "2"}]

    def test_output_csv_requires_path(self, mock_hook, context):
        mock_hook.get_values.side_effect = [
            [["a"]],
            [["1"]],
        ]

        op = GoogleSheetsReadOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            has_headers=True,
            output_type="csv",
        )

        with pytest.raises(ValueError, match="output_path is required"):
            op.execute(context)

    def test_output_xcom_without_headers(self, mock_hook, context):
        mock_hook.get_values.side_effect = [
            [["a", "b"], ["c", "d"]],
        ]

        op = GoogleSheetsReadOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            has_headers=False,
            output_type="xcom",
        )
        result = op.execute(context)
        assert result == [["a", "b"], ["c", "d"]]
