"""Tests for GoogleSheetsReadOperator."""

from __future__ import annotations

import json
import os
import tempfile
from datetime import date
from unittest.mock import MagicMock, patch, call

import pytest

from airflow_provider_google_sheets.operators.read import GoogleSheetsReadOperator


SPREADSHEET_ID = "test-spreadsheet-id"


@pytest.fixture
def mock_hook():
    with patch(
        "airflow_provider_google_sheets.operators.read.GoogleSheetsHook"
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

    def test_column_mapping(self, mock_hook, context):
        mock_hook.get_values.side_effect = [
            [["Дата", "Клиент", "Сумма"]],
            [["2026-01-01", "ФСК", "100"]],
        ]

        op = GoogleSheetsReadOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            has_headers=True,
            column_mapping={"Дата": "date", "Клиент": "client", "Сумма": "amount"},
        )
        result = op.execute(context)
        assert list(result[0].keys()) == ["date", "client", "amount"]
        assert result[0] == {"date": "2026-01-01", "client": "ФСК", "amount": "100"}

    def test_column_mapping_partial(self, mock_hook, context):
        """Headers not in mapping are kept as-is."""
        mock_hook.get_values.side_effect = [
            [["Дата", "value"]],
            [["2026-01-01", "42"]],
        ]

        op = GoogleSheetsReadOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            has_headers=True,
            column_mapping={"Дата": "date"},
        )
        result = op.execute(context)
        assert list(result[0].keys()) == ["date", "value"]

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

        from airflow_provider_google_sheets.exceptions import GoogleSheetsSchemaError
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
            data = [json.loads(line) for line in f if line.strip()]
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


# ------------------------------------------------------------------
# Task 7.4 — Streaming output
# ------------------------------------------------------------------


class TestStreamingOutput:
    def test_csv_streaming_multiple_chunks(self, mock_hook, context, tmp_dir):
        """CSV output streams chunks directly to file without accumulating."""
        mock_hook.get_values.side_effect = [
            [["a", "b"]],                     # headers
            [["1", "2"], ["3", "4"]],          # chunk 1 (chunk_size=2)
            [["5", "6"]],                      # chunk 2 (< chunk_size → done)
        ]

        path = os.path.join(tmp_dir, "stream.csv")
        op = GoogleSheetsReadOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            has_headers=True,
            chunk_size=2,
            output_type="csv",
            output_path=path,
        )
        result = op.execute(context)

        assert result == path
        with open(path) as f:
            lines = f.readlines()
        assert len(lines) == 4  # header + 3 data rows
        assert lines[0].strip() == "a,b"
        assert lines[3].strip() == "5,6"

    def test_json_streaming_multiple_chunks(self, mock_hook, context, tmp_dir):
        """JSON output streams chunks directly to file."""
        mock_hook.get_values.side_effect = [
            [["x"]],                # headers
            [["1"], ["2"]],         # chunk 1
            [["3"]],               # chunk 2
        ]

        path = os.path.join(tmp_dir, "stream.json")
        op = GoogleSheetsReadOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            has_headers=True,
            chunk_size=2,
            output_type="json",
            output_path=path,
        )
        result = op.execute(context)

        assert result == path
        with open(path) as f:
            data = [json.loads(line) for line in f if line.strip()]
        assert data == [{"x": "1"}, {"x": "2"}, {"x": "3"}]

    def test_json_streaming_without_headers(self, mock_hook, context, tmp_dir):
        """JSON without headers outputs list of lists."""
        mock_hook.get_values.side_effect = [
            [["a", "b"], ["c", "d"]],
        ]

        path = os.path.join(tmp_dir, "stream_noheader.json")
        op = GoogleSheetsReadOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            has_headers=False,
            output_type="json",
            output_path=path,
        )
        result = op.execute(context)

        with open(path) as f:
            data = [json.loads(line) for line in f if line.strip()]
        assert data == [["a", "b"], ["c", "d"]]

    def test_xcom_max_rows_exceeded(self, mock_hook, context):
        """XCom output raises when exceeding max_xcom_rows."""
        mock_hook.get_values.side_effect = [
            [["col"]],                         # headers
            [["r"] for _ in range(10)],        # 10 rows
        ]

        from airflow_provider_google_sheets.exceptions import GoogleSheetsDataError

        op = GoogleSheetsReadOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            has_headers=True,
            max_xcom_rows=5,
        )
        with pytest.raises(GoogleSheetsDataError, match="max_xcom_rows"):
            op.execute(context)

    def test_xcom_within_limit_works(self, mock_hook, context):
        """XCom output works normally within the limit."""
        mock_hook.get_values.side_effect = [
            [["col"]],
            [["a"], ["b"], ["c"]],
        ]

        op = GoogleSheetsReadOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            has_headers=True,
            max_xcom_rows=100,
        )
        result = op.execute(context)
        assert len(result) == 3


# ------------------------------------------------------------------
# Range building fix (cell_range=None)
# ------------------------------------------------------------------


class TestRangeBuildingFix:
    def test_build_range_no_cell_range(self):
        """When cell_range=None, _build_range should produce row-only range."""
        op = GoogleSheetsReadOperator(
            task_id="test",
            spreadsheet_id="sid",
            cell_range=None,
        )
        result = op._build_range(2, 5001)
        # Should be "2:5001" (no column letters)
        assert result == "2:5001"
        # Should NOT contain something like "A2:5001" (invalid)
        assert "A2:" not in result or "A2:5001" not in result

    def test_build_range_no_cell_range_with_sheet(self):
        op = GoogleSheetsReadOperator(
            task_id="test",
            spreadsheet_id="sid",
            cell_range=None,
            sheet_name="Sheet1",
        )
        result = op._build_range(2, 5001)
        assert result == "Sheet1!2:5001"

    def test_build_range_with_cell_range(self):
        op = GoogleSheetsReadOperator(
            task_id="test",
            spreadsheet_id="sid",
            cell_range="B1:D",
        )
        result = op._build_range(2, 5001)
        assert result == "B2:D5001"

    def test_build_header_range_no_cell_range(self):
        op = GoogleSheetsReadOperator(
            task_id="test",
            spreadsheet_id="sid",
            cell_range=None,
        )
        result = op._build_header_range()
        assert result == "1:1"
