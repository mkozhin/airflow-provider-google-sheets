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

        # Default settings: transliterate=True, sanitize=True, lowercase=True
        assert result == [
            {"name": "Alice", "age": "30"},
            {"name": "Bob", "age": "25"},
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
        assert result[0] == {"col": "r1"}
        assert result[4] == {"col": "r5"}

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

    def test_transliterate_headers_disabled(self, mock_hook, context):
        mock_hook.get_values.side_effect = [
            [["Дата", "Выручка"]],
            [["01.01", "100"]],
        ]

        op = GoogleSheetsReadOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            has_headers=True,
            transliterate_headers=False,
            sanitize_headers=False,
            lowercase_headers=False,
        )
        result = op.execute(context)
        assert list(result[0].keys()) == ["Дата", "Выручка"]

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

    def test_sanitize_headers(self, mock_hook, context):
        mock_hook.get_values.side_effect = [
            [["My Column!", "Price ($)"]],
            [["a", "b"]],
        ]

        op = GoogleSheetsReadOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            has_headers=True,
            transliterate_headers=False,
            sanitize_headers=True,
            lowercase_headers=False,
        )
        result = op.execute(context)
        assert list(result[0].keys()) == ["My_Column", "Price"]

    def test_sanitize_headers_disabled(self, mock_hook, context):
        mock_hook.get_values.side_effect = [
            [["My Column!", "Price ($)"]],
            [["a", "b"]],
        ]

        op = GoogleSheetsReadOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            has_headers=True,
            transliterate_headers=False,
            sanitize_headers=False,
            lowercase_headers=False,
        )
        result = op.execute(context)
        assert list(result[0].keys()) == ["My Column!", "Price ($)"]

    def test_lowercase_headers(self, mock_hook, context):
        mock_hook.get_values.side_effect = [
            [["Name", "AGE"]],
            [["a", "b"]],
        ]

        op = GoogleSheetsReadOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            has_headers=True,
            transliterate_headers=False,
            sanitize_headers=False,
            lowercase_headers=True,
        )
        result = op.execute(context)
        assert list(result[0].keys()) == ["name", "age"]

    def test_lowercase_headers_disabled(self, mock_hook, context):
        mock_hook.get_values.side_effect = [
            [["Name", "AGE"]],
            [["a", "b"]],
        ]

        op = GoogleSheetsReadOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            has_headers=True,
            transliterate_headers=False,
            sanitize_headers=False,
            lowercase_headers=False,
        )
        result = op.execute(context)
        assert list(result[0].keys()) == ["Name", "AGE"]

    def test_column_mapping(self, mock_hook, context):
        """column_mapping skips all header processing and maps raw names."""
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
        """Headers not in mapping are kept as raw originals."""
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

    def test_column_mapping_ignores_processing_flags(self, mock_hook, context):
        """Even with transliterate/sanitize/lowercase on, column_mapping
        receives raw header names because it takes priority."""
        mock_hook.get_values.side_effect = [
            [["Дата Отчёта", "Клиент (ФИО)"]],
            [["2026-01-01", "Иванов"]],
        ]

        op = GoogleSheetsReadOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            has_headers=True,
            transliterate_headers=True,
            sanitize_headers=True,
            lowercase_headers=True,
            column_mapping={
                "Дата Отчёта": "report_date",
                "Клиент (ФИО)": "client_name",
            },
        )
        result = op.execute(context)
        assert list(result[0].keys()) == ["report_date", "client_name"]

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
        # With default lowercase=True
        assert keys == ["col", "col_1", "col_2"]


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

    def test_strip_strings_true_trims_whitespace(self, mock_hook, context):
        mock_hook.get_values.side_effect = [
            [["project", "bonus"]],
            [[" Дмитров Дом ", " bonus"]],
        ]

        op = GoogleSheetsReadOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            has_headers=True,
            schema={"project": {"type": "str"}, "bonus": {"type": "str"}},
            strip_strings=True,
        )
        result = op.execute(context)
        assert result[0]["project"] == "Дмитров Дом"
        assert result[0]["bonus"] == "bonus"

    def test_strip_strings_false_preserves_whitespace(self, mock_hook, context):
        mock_hook.get_values.side_effect = [
            [["bonus"]],
            [[" bonus "]],
        ]

        op = GoogleSheetsReadOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            has_headers=True,
            schema={"bonus": {"type": "str"}},
            strip_strings=False,
        )
        result = op.execute(context)
        assert result[0]["bonus"] == " bonus "


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
        """output_type='json' writes a valid JSON array."""
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

    def test_output_jsonl(self, mock_hook, context, tmp_dir):
        """output_type='jsonl' writes one JSON object per line."""
        mock_hook.get_values.side_effect = [
            [["a", "b"]],
            [["1", "2"]],
        ]

        path = os.path.join(tmp_dir, "out.jsonl")
        op = GoogleSheetsReadOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            has_headers=True,
            output_type="jsonl",
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

    def test_jsonl_streaming_multiple_chunks(self, mock_hook, context, tmp_dir):
        """JSONL output streams chunks directly to file."""
        mock_hook.get_values.side_effect = [
            [["x"]],                # headers
            [["1"], ["2"]],         # chunk 1
            [["3"]],               # chunk 2
        ]

        path = os.path.join(tmp_dir, "stream.jsonl")
        op = GoogleSheetsReadOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            has_headers=True,
            chunk_size=2,
            output_type="jsonl",
            output_path=path,
        )
        result = op.execute(context)

        assert result == path
        with open(path) as f:
            data = [json.loads(line) for line in f if line.strip()]
        assert data == [{"x": "1"}, {"x": "2"}, {"x": "3"}]

    def test_jsonl_streaming_without_headers(self, mock_hook, context, tmp_dir):
        """JSONL without headers outputs list of lists."""
        mock_hook.get_values.side_effect = [
            [["a", "b"], ["c", "d"]],
        ]

        path = os.path.join(tmp_dir, "stream_noheader.jsonl")
        op = GoogleSheetsReadOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            has_headers=False,
            output_type="jsonl",
            output_path=path,
        )
        result = op.execute(context)

        with open(path) as f:
            data = [json.loads(line) for line in f if line.strip()]
        assert data == [["a", "b"], ["c", "d"]]

    def test_json_streaming_multiple_chunks(self, mock_hook, context, tmp_dir):
        """JSON array output streams chunks directly to file."""
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
            data = json.load(f)
        assert data == [{"x": "1"}, {"x": "2"}, {"x": "3"}]

    def test_json_streaming_without_headers(self, mock_hook, context, tmp_dir):
        """JSON array without headers outputs list of lists."""
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
            data = json.load(f)
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

    def test_xcom_max_bytes_exceeded(self, mock_hook, context):
        """XCom output raises when estimated size exceeds max_xcom_bytes."""
        mock_hook.get_values.side_effect = [
            [["col"]],
            [["x" * 100], ["x" * 100]],
        ]

        from airflow_provider_google_sheets.exceptions import GoogleSheetsDataError

        op = GoogleSheetsReadOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            has_headers=True,
            max_xcom_bytes=10,
        )
        with pytest.raises(GoogleSheetsDataError, match="max_xcom_bytes"):
            op.execute(context)

    def test_xcom_max_bytes_ok_under_limit(self, mock_hook, context):
        """XCom output works normally when size is under max_xcom_bytes."""
        mock_hook.get_values.side_effect = [
            [["col"]],
            [["a"], ["b"]],
        ]

        op = GoogleSheetsReadOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            has_headers=True,
            max_xcom_bytes=10_000_000,
        )
        result = op.execute(context)
        assert len(result) == 2

    def test_xcom_max_bytes_none_no_check(self, mock_hook, context):
        """max_xcom_bytes=None (default) does not raise even for larger payloads."""
        mock_hook.get_values.side_effect = [
            [["col"]],
            [["x" * 100], ["x" * 100]],
        ]

        op = GoogleSheetsReadOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            has_headers=True,
            max_xcom_bytes=None,
        )
        result = op.execute(context)
        assert len(result) == 2

    def test_xcom_large_payload_warning(self, mock_hook, context, caplog):
        """Payload > 5 MB triggers a warning even without max_xcom_bytes."""
        import logging
        from unittest.mock import patch

        large_rows = [["x" * 200] for _ in range(30_000)]

        op = GoogleSheetsReadOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            has_headers=True,
            max_xcom_bytes=None,
        )
        with patch.object(op, "_read_chunks", return_value=iter([large_rows])), \
             caplog.at_level(logging.WARNING):
            op._read_to_xcom(mock_hook, ["col"], 2)

        assert "XCom payload is large" in caplog.text


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


# ------------------------------------------------------------------
# Row filtering (row_skip / row_stop)
# ------------------------------------------------------------------


class TestRowSkip:
    def test_skip_rows_by_value(self, mock_hook, context):
        mock_hook.get_values.side_effect = [
            [["status", "value"]],
            [["active", "10"], ["deleted", "20"], ["active", "30"]],
        ]

        op = GoogleSheetsReadOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            row_skip={"column": "status", "value": "deleted"},
        )
        result = op.execute(context)

        assert result == [
            {"status": "active", "value": "10"},
            {"status": "active", "value": "30"},
        ]

    def test_skip_multiple_conditions_or(self, mock_hook, context):
        mock_hook.get_values.side_effect = [
            [["status", "value"]],
            [["active", "10"], ["deleted", "20"], ["archived", "30"], ["active", "40"]],
        ]

        op = GoogleSheetsReadOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            row_skip=[
                {"column": "status", "value": "deleted"},
                {"column": "status", "value": "archived"},
            ],
        )
        result = op.execute(context)

        assert result == [
            {"status": "active", "value": "10"},
            {"status": "active", "value": "40"},
        ]


class TestRowStop:
    def test_stop_at_matching_row(self, mock_hook, context):
        mock_hook.get_values.side_effect = [
            [["name", "amount"]],
            [["Item1", "100"], ["Item2", "200"], ["ИТОГО", "300"], ["Extra", "400"]],
        ]

        op = GoogleSheetsReadOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            row_stop={"column": "name", "value": "ИТОГО"},
        )
        result = op.execute(context)

        assert result == [
            {"name": "Item1", "amount": "100"},
            {"name": "Item2", "amount": "200"},
        ]

    def test_stop_no_extra_api_calls(self, mock_hook, context):
        """When row_stop triggers in the first chunk, no further API calls."""
        mock_hook.get_values.side_effect = [
            [["name", "amount"]],
            [["Item1", "100"], ["ИТОГО", "300"]],
        ]

        op = GoogleSheetsReadOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            chunk_size=5000,
            row_stop={"column": "name", "value": "ИТОГО"},
        )
        op.execute(context)

        # 1 call for headers + 1 call for data chunk = 2 total
        assert mock_hook.get_values.call_count == 2

    def test_stop_mid_chunk_no_further_chunks(self, mock_hook, context):
        """row_stop in chunk prevents fetching the next chunk."""
        chunk1 = [["Row1", "a"], ["ИТОГО", "b"]]
        chunk2_should_not_be_called = [["Row3", "c"]]

        mock_hook.get_values.side_effect = [
            [["name", "value"]],
            chunk1,
            chunk2_should_not_be_called,
        ]

        op = GoogleSheetsReadOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            chunk_size=2,
            row_stop={"column": "name", "value": "ИТОГО"},
        )
        result = op.execute(context)

        assert result == [{"name": "Row1", "value": "a"}]
        # Only header + first data chunk fetched
        assert mock_hook.get_values.call_count == 2


class TestRowStopAndSkipCombined:
    def test_stop_and_skip_together(self, mock_hook, context):
        mock_hook.get_values.side_effect = [
            [["name", "status"]],
            [
                ["Item1", "active"],
                ["Item2", "deleted"],
                ["Item3", "active"],
                ["ИТОГО", ""],
                ["Extra", "active"],
            ],
        ]

        op = GoogleSheetsReadOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            row_skip={"column": "status", "value": "deleted"},
            row_stop={"column": "name", "value": "ИТОГО"},
        )
        result = op.execute(context)

        assert result == [
            {"name": "Item1", "status": "active"},
            {"name": "Item3", "status": "active"},
        ]


class TestRowFilterValidation:
    def test_invalid_condition_raises_before_reading(self, mock_hook, context):
        mock_hook.get_values.side_effect = [
            [["name", "value"]],
        ]

        op = GoogleSheetsReadOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            row_skip={"column": "name", "op": "like", "value": "x"},
        )
        with pytest.raises(ValueError, match="unknown op 'like'"):
            op.execute(context)


class TestRowSkipMissingColumn:
    def test_skip_condition_on_absent_column_is_ignored(self, mock_hook, context):
        """When the column referenced in row_skip does not exist, the condition
        is silently ignored and the row is NOT skipped."""
        mock_hook.get_values.side_effect = [
            [["name", "value"]],
            [["Alice", "10"], ["Bob", "20"]],
        ]

        op = GoogleSheetsReadOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            row_skip={"column": "nonexistent", "value": "Alice"},
        )
        result = op.execute(context)

        # Both rows pass through — the missing column condition is ignored
        assert len(result) == 2


class TestRowStopMultipleConditions:
    def test_stop_on_any_condition_or_logic(self, mock_hook, context):
        """row_stop with a list stops at the first row matching ANY condition."""
        mock_hook.get_values.side_effect = [
            [["name", "type"]],
            [
                ["Item1", "data"],
                ["Item2", "data"],
                ["ИТОГО", "data"],      # matches first condition
                ["Item4", "data"],
            ],
        ]

        op = GoogleSheetsReadOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            row_stop=[
                {"column": "name", "value": "ИТОГО"},
                {"column": "type", "op": "starts_with", "value": "total_"},
            ],
        )
        result = op.execute(context)

        assert result == [
            {"name": "Item1", "type": "data"},
            {"name": "Item2", "type": "data"},
        ]

    def test_stop_on_second_condition_when_first_does_not_match(self, mock_hook, context):
        """Stop triggered by the second condition in the list."""
        mock_hook.get_values.side_effect = [
            [["name", "type"]],
            [
                ["Item1", "data"],
                ["Item2", "total_summary"],  # matches second condition
                ["Item3", "data"],
            ],
        ]

        op = GoogleSheetsReadOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            row_stop=[
                {"column": "name", "value": "ИТОГО"},
                {"column": "type", "op": "starts_with", "value": "total_"},
            ],
        )
        result = op.execute(context)

        assert result == [{"name": "Item1", "type": "data"}]
