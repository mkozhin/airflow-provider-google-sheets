"""Tests for GoogleSheetsWriteOperator."""

from __future__ import annotations

import json
import os
import tempfile
from datetime import date
from unittest.mock import MagicMock, patch, call

import pytest

from airflow_provider_google_sheets.operators.write import GoogleSheetsWriteOperator


SPREADSHEET_ID = "test-spreadsheet-id"


@pytest.fixture
def mock_hook():
    with patch(
        "airflow_provider_google_sheets.operators.write.GoogleSheetsHook"
    ) as hook_cls:
        hook = MagicMock()
        hook_cls.return_value = hook
        # Default metadata for smart merge
        hook.get_spreadsheet_metadata.return_value = {
            "sheets": [{"properties": {"sheetId": 0, "title": "Sheet1"}}]
        }
        hook.get_sheet_id.return_value = 0
        hook.get_sheet_properties.return_value = {
            "sheetId": 0,
            "title": "Sheet1",
            "gridProperties": {"rowCount": 1000},
        }
        yield hook


@pytest.fixture
def context():
    return MagicMock()


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield d


# ==================================================================
# Task 4.1 — Overwrite and Append
# ==================================================================


class TestOverwrite:
    def test_overwrite_with_headers(self, mock_hook, context):
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="overwrite",
            data=[{"name": "Alice", "age": 30}],
        )
        result = op.execute(context)

        mock_hook.clear_values.assert_called_once()
        # Default clear_mode="sheet" clears entire sheet
        clear_range = mock_hook.clear_values.call_args[0][1]
        assert clear_range == "Sheet1"
        mock_hook.update_values.assert_called_once()
        # First row should be headers, second row data
        written = mock_hook.update_values.call_args[0][2]
        assert written[0] == ["name", "age"]
        assert written[1] == ["Alice", 30]
        assert result["mode"] == "overwrite"
        assert result["rows_written"] == 2  # header + 1 data row
        # trim_sheet called in sheet mode
        mock_hook.trim_sheet.assert_called_once_with(SPREADSHEET_ID, None, 2)

    def test_overwrite_without_headers(self, mock_hook, context):
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="overwrite",
            write_headers=False,
            data=[{"name": "Alice", "age": 30}],
        )
        result = op.execute(context)

        written = mock_hook.update_values.call_args[0][2]
        assert written[0] == ["Alice", 30]
        assert result["rows_written"] == 1
        mock_hook.trim_sheet.assert_called_once_with(SPREADSHEET_ID, None, 1)

    def test_overwrite_batching(self, mock_hook, context):
        data = [[i, i * 10] for i in range(5)]
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="overwrite",
            data=data,
            has_headers=False,
            batch_size=2,
            pause_between_batches=0,
        )
        result = op.execute(context)

        # 5 rows / batch_size=2 → 3 batches
        assert mock_hook.update_values.call_count == 3
        assert result["rows_written"] == 5

    def test_overwrite_with_sheet_name(self, mock_hook, context):
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            sheet_name="Data",
            write_mode="overwrite",
            data=[["x"]],
            has_headers=False,
        )
        op.execute(context)

        clear_range = mock_hook.clear_values.call_args[0][1]
        assert clear_range == "Data"
        mock_hook.trim_sheet.assert_called_once_with(SPREADSHEET_ID, "Data", 1)

    def test_overwrite_with_schema(self, mock_hook, context):
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="overwrite",
            data=[{"dt": date(2024, 4, 1), "val": 42}],
            schema={"dt": {"type": "date", "format": "%d/%m/%Y"}},
        )
        op.execute(context)

        written = mock_hook.update_values.call_args[0][2]
        # Row 0 = headers, Row 1 = data
        assert written[1][0] == "01/04/2024"


class TestAppend:
    def test_append_basic(self, mock_hook, context):
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="append",
            data=[{"a": 1}, {"a": 2}],
        )
        result = op.execute(context)

        mock_hook.append_values.assert_called_once()
        appended = mock_hook.append_values.call_args[0][2]
        assert appended == [[1], [2]]
        assert result["mode"] == "append"
        assert result["rows_written"] == 2

    def test_append_batching(self, mock_hook, context):
        data = [[i] for i in range(5)]
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="append",
            data=data,
            has_headers=False,
            batch_size=2,
            pause_between_batches=0,
        )
        result = op.execute(context)

        assert mock_hook.append_values.call_count == 3
        assert result["rows_written"] == 5

    def test_append_with_sheet_name(self, mock_hook, context):
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            sheet_name="Log",
            write_mode="append",
            data=[["x"]],
            has_headers=False,
        )
        op.execute(context)

        target = mock_hook.append_values.call_args[0][1]
        assert target.startswith("Log!")

    def test_append_empty_sheet_writes_headers(self, mock_hook, context):
        """Empty sheet + write_headers=True → header row written before data."""
        mock_hook.get_values.return_value = []
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="append",
            data=[{"a": 1, "b": 2}],
            write_headers=True,
        )
        op.execute(context)

        mock_hook.update_values.assert_called_once()
        call_args = mock_hook.update_values.call_args[0]
        assert call_args[2] == [["a", "b"]]
        assert "A1" in call_args[1]
        mock_hook.append_values.assert_called_once()

    def test_append_non_empty_sheet_no_headers(self, mock_hook, context):
        """Non-empty sheet → headers not written, data appended normally."""
        mock_hook.get_values.return_value = [["a", "b"]]
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="append",
            data=[{"a": 1, "b": 2}],
            write_headers=True,
        )
        op.execute(context)

        mock_hook.update_values.assert_not_called()
        mock_hook.append_values.assert_called_once()

    def test_append_write_headers_false_empty_sheet(self, mock_hook, context):
        """write_headers=False → no header even on empty sheet."""
        mock_hook.get_values.return_value = []
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="append",
            data=[{"a": 1}],
            write_headers=False,
        )
        op.execute(context)

        mock_hook.update_values.assert_not_called()
        mock_hook.append_values.assert_called_once()

    def test_append_empty_start_cell_with_data_to_the_right(self, mock_hook, context):
        """table_start='C3' is empty but D3 has data → header still written to C3."""
        # get_values returns non-empty only if we read the whole row;
        # with the fix (single cell check) it returns [] → header is written
        mock_hook.get_values.return_value = []  # C3 cell is empty
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="append",
            data=[{"a": 1, "b": 2}],
            table_start="C3",
            write_headers=True,
            pause_between_batches=0,
        )
        op.execute(context)

        # Header must be written to C3
        mock_hook.update_values.assert_called_once()
        args = mock_hook.update_values.call_args[0]
        assert "C3" in args[1]

        # The emptiness check must use a single-cell range (not C3:3)
        check_range = mock_hook.get_values.call_args[0][1]
        assert check_range.endswith("C3"), (
            f"Expected single-cell check 'C3', got: {check_range!r}"
        )


class TestDataSources:
    def test_data_from_xcom(self, mock_hook, context):
        context["ti"].xcom_pull.return_value = [{"a": 1}]

        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="append",
            data_xcom_task_id="upstream_task",
            data_xcom_key="result",
        )
        op.execute(context)

        context["ti"].xcom_pull.assert_called_once_with(
            task_ids="upstream_task", key="result"
        )
        mock_hook.append_values.assert_called_once()

    def test_data_from_csv_file(self, mock_hook, context, tmp_dir):
        path = os.path.join(tmp_dir, "data.csv")
        with open(path, "w") as f:
            f.write("a,b\n1,2\n3,4\n")

        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="append",
            data=path,
        )
        op.execute(context)

        appended = mock_hook.append_values.call_args[0][2]
        assert appended == [["1", "2"], ["3", "4"]]

    def test_data_from_json_file(self, mock_hook, context, tmp_dir):
        """File path auto-detected as JSONL."""
        path = os.path.join(tmp_dir, "data.json")
        with open(path, "w") as f:
            f.write('{"x": 10}\n{"x": 20}\n')

        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="append",
            data=path,
        )
        op.execute(context)

        appended = mock_hook.append_values.call_args[0][2]
        assert appended == [[10], [20]]

    def test_data_as_list_of_dicts(self, mock_hook, context):
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="append",
            data=[{"col1": "a", "col2": "b"}],
        )
        op.execute(context)

        appended = mock_hook.append_values.call_args[0][2]
        assert appended == [["a", "b"]]

    def test_no_data_raises(self, mock_hook, context):
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="append",
        )
        with pytest.raises(ValueError, match="No data provided"):
            op.execute(context)


# ==================================================================
# Task 4.2 — Smart Merge
# ==================================================================


class TestSmartMerge:
    """Tests for the merge write mode.

    Strategy: for each key value present in incoming data —
    delete ALL existing rows with that key, then append all incoming rows.
    Keys absent from incoming data are left untouched.
    """

    def _make_op(self, data, merge_key="date", **kwargs):
        defaults = dict(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="merge",
            merge_key=merge_key,
            batch_size=1000,
            pause_between_batches=0,
        )
        defaults.update(kwargs)
        return GoogleSheetsWriteOperator(data=data, **defaults)

    def test_existing_key_deletes_and_appends(self, mock_hook, context):
        """Key exists in sheet → delete all existing rows, append incoming."""
        mock_hook.get_values.return_value = [
            ["date"],
            ["2024-04-01"],  # row 2
            ["2024-04-01"],  # row 3
        ]
        incoming = [
            {"date": "2024-04-01", "val": "new1"},
            {"date": "2024-04-01", "val": "new2"},
        ]
        op = self._make_op(incoming)
        result = op.execute(context)

        assert result["deleted"] == 2
        assert result["appended"] == 2
        mock_hook.batch_update.assert_called()
        mock_hook.append_values.assert_called()

    def test_existing_key_different_row_count(self, mock_hook, context):
        """3 existing rows, 5 incoming → delete 3, append 5."""
        mock_hook.get_values.return_value = [
            ["date"],
            ["2024-04-01"],
            ["2024-04-01"],
            ["2024-04-01"],
        ]
        incoming = [{"date": "2024-04-01", "val": str(i)} for i in range(5)]
        op = self._make_op(incoming)
        result = op.execute(context)

        assert result["deleted"] == 3
        assert result["appended"] == 5

    def test_new_key_only_appends(self, mock_hook, context):
        """Key not in sheet → no deletes, just append."""
        mock_hook.get_values.return_value = [
            ["date"],
            ["2024-04-01"],
        ]
        incoming = [{"date": "2024-04-02", "val": "new"}]
        op = self._make_op(incoming)
        result = op.execute(context)

        assert result["deleted"] == 0
        assert result["appended"] == 1
        # No deleteDimension — repeatCell uses batch_update
        all_batch_calls = mock_hook.batch_update.call_args_list
        all_requests = [r for call in all_batch_calls for r in call[0][1]]
        assert not any("deleteDimension" in r for r in all_requests)
        mock_hook.append_values.assert_called()

    def test_key_in_sheet_not_in_incoming_is_untouched(self, mock_hook, context):
        """Key only in sheet (not incoming) → left as-is."""
        mock_hook.get_values.return_value = [
            ["date"],
            ["2024-04-01"],  # not in incoming → should NOT be deleted
        ]
        incoming = [{"date": "2024-04-02", "val": "new"}]
        op = self._make_op(incoming)
        result = op.execute(context)

        assert result["deleted"] == 0
        assert result["appended"] == 1

    def test_multiple_keys_mixed(self, mock_hook, context):
        """Some keys exist (delete+append), some are new (append only)."""
        mock_hook.get_values.return_value = [
            ["date"],
            ["2024-04-01"],  # row 2 — will be replaced
            ["2024-04-01"],  # row 3
        ]
        incoming = [
            {"date": "2024-04-01", "val": "a"},
            {"date": "2024-04-01", "val": "b"},
            {"date": "2024-04-02", "val": "c"},  # new key
        ]
        op = self._make_op(incoming)
        result = op.execute(context)

        assert result["deleted"] == 2
        assert result["appended"] == 3

    def test_bottom_up_ordering_for_deletes(self, mock_hook, context):
        """Delete operations for multiple keys must be sorted bottom-up."""
        mock_hook.get_values.return_value = [
            ["date"],
            ["2024-04-01"],  # row 2
            ["2024-04-01"],  # row 3
            ["2024-04-05"],  # row 4
            ["2024-04-05"],  # row 5
        ]
        incoming = [
            {"date": "2024-04-01", "val": "a"},
            {"date": "2024-04-05", "val": "b"},
        ]
        op = self._make_op(incoming)
        result = op.execute(context)

        assert result["deleted"] == 4

        batch_args = mock_hook.batch_update.call_args[0][1]
        delete_starts = [
            r["deleteDimension"]["range"]["startIndex"]
            for r in batch_args
            if "deleteDimension" in r
        ]
        assert delete_starts == sorted(delete_starts, reverse=True)

    def test_missing_merge_key_raises(self, mock_hook, context):
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="merge",
            data=[{"a": 1}],
        )
        with pytest.raises(ValueError, match="merge_key is required"):
            op.execute(context)

    def test_merge_key_not_in_headers_raises(self, mock_hook, context):
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="merge",
            merge_key="nonexistent",
            data=[{"a": 1}],
        )
        with pytest.raises(ValueError, match="not found in headers"):
            op.execute(context)

    def test_no_headers_raises(self, mock_hook, context):
        """merge requires headers."""
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="merge",
            merge_key="date",
            data=[],
            has_headers=False,
            pause_between_batches=0,
        )
        with pytest.raises(ValueError, match="Headers are required"):
            op.execute(context)

    def test_smart_merge_alias_still_works(self, mock_hook, context):
        """write_mode='smart_merge' is a silent alias for 'merge'."""
        mock_hook.get_values.return_value = [["date"], ["2024-04-01"]]
        incoming = [{"date": "2024-04-01", "val": "x"}]
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="smart_merge",
            merge_key="date",
            data=incoming,
            pause_between_batches=0,
        )
        result = op.execute(context)
        assert result["deleted"] == 1
        assert result["appended"] == 1

    def test_with_sheet_name(self, mock_hook, context):
        """Key column range must include sheet name prefix."""
        mock_hook.get_values.return_value = [["date"], ["2024-04-01"]]
        incoming = [{"date": "2024-04-01", "val": "x"}]
        op = self._make_op(incoming, sheet_name="MySheet")
        op.execute(context)

        key_range = mock_hook.get_values.call_args[0][1]
        assert key_range.startswith("MySheet!")

    def test_uses_append_values_not_batch_update_values(self, mock_hook, context):
        """New rows use append_values, never batch_update_values."""
        mock_hook.get_values.return_value = [["date"], ["2024-04-01"]]
        incoming = [{"date": "2024-04-02", "val": "new"}]
        op = self._make_op(incoming)
        op.execute(context)

        mock_hook.append_values.assert_called()
        mock_hook.batch_update_values.assert_not_called()

    def test_uses_append_values_not_insert_or_append_dimension(self, mock_hook, context):
        """New rows use append_values (no appendDimension or insertDimension)."""
        mock_hook.get_values.return_value = [["date"]]  # only header row
        incoming = [{"date": "2024-04-01", "val": "x"}]
        op = self._make_op(incoming)
        op.execute(context)

        mock_hook.append_values.assert_called()
        all_requests = [r for call in mock_hook.batch_update.call_args_list for r in call[0][1]]
        assert not any("appendDimension" in r for r in all_requests), "appendDimension must not be used"
        assert not any("insertDimension" in r for r in all_requests), "insertDimension must not be used"

    def test_insert_position_after_remaining_rows(self, mock_hook, context):
        """After deleting key rows, append_values is called with 1 row."""
        # Sheet: header (row 1) + 2024-04-01 x2 (rows 2,3) + 2024-04-02 x1 (row 4)
        mock_hook.get_values.return_value = [
            ["date"],
            ["2024-04-01"],  # row 2 — will be deleted
            ["2024-04-01"],  # row 3 — will be deleted
            ["2024-04-02"],  # row 4 — stays
        ]
        incoming = [{"date": "2024-04-01", "val": "new"}]
        op = self._make_op(incoming)
        op.execute(context)

        mock_hook.append_values.assert_called_once()
        appended = mock_hook.append_values.call_args[0][2]
        assert len(appended) == 1

    def test_empty_sheet_writes_headers(self, mock_hook, context):
        """Empty sheet + write_headers=True → header written before data."""
        mock_hook.get_values.return_value = []
        incoming = [{"date": "2024-04-01", "val": "x"}]
        op = self._make_op(incoming, write_headers=True)
        op.execute(context)

        # update_values must be called with headers in row 1
        assert mock_hook.update_values.call_count >= 1
        first_call_args = mock_hook.update_values.call_args_list[0][0]
        assert first_call_args[2] == [["date", "val"]]
        assert "A1" in first_call_args[1]

        # append_values called with 1 data row
        mock_hook.append_values.assert_called_once()
        appended = mock_hook.append_values.call_args[0][2]
        assert len(appended) == 1

        # repeatCell sent via batch_update to clear formatting
        all_requests = [r for call in mock_hook.batch_update.call_args_list for r in call[0][1]]
        repeat_reqs = [r for r in all_requests if "repeatCell" in r]
        assert repeat_reqs, "Expected repeatCell request to clear formatting"

    def test_empty_sheet_insert_position_with_table_start(self, mock_hook, context):
        """Empty sheet + table_start='A3' → append_values called with 1 data row + repeatCell."""
        mock_hook.get_values.return_value = []
        incoming = [{"date": "2024-04-01", "val": "x"}]
        op = self._make_op(incoming, write_headers=True, table_start="A3")
        op.execute(context)

        mock_hook.append_values.assert_called_once()
        appended = mock_hook.append_values.call_args[0][2]
        assert len(appended) == 1

        all_requests = [r for call in mock_hook.batch_update.call_args_list for r in call[0][1]]
        repeat_reqs = [r for r in all_requests if "repeatCell" in r]
        assert repeat_reqs, "Expected repeatCell request to clear formatting"

    def test_empty_sheet_write_headers_false(self, mock_hook, context):
        """write_headers=False → no header written even on empty sheet."""
        mock_hook.get_values.return_value = []
        incoming = [{"date": "2024-04-01", "val": "x"}]
        op = self._make_op(incoming, write_headers=False)
        op.execute(context)

        mock_hook.update_values.assert_not_called()

    def test_non_empty_sheet_does_not_write_headers(self, mock_hook, context):
        """Non-empty sheet → headers not written again."""
        mock_hook.get_values.return_value = [["date"], ["2024-04-01"]]
        incoming = [{"date": "2024-04-01", "val": "x"}]
        op = self._make_op(incoming, write_headers=True)
        op.execute(context)

        # update_values should NOT be called (no header write; only batch_update for delete + append_values)
        mock_hook.update_values.assert_not_called()

    def test_clears_formatting_after_append(self, mock_hook, context):
        """repeatCell targeting only written columns is sent after appending rows."""
        mock_hook.get_values.return_value = [["date", "val"]]  # header only
        incoming = [
            {"date": "2024-04-01", "val": "x"},
            {"date": "2024-04-02", "val": "y"},
        ]
        op = self._make_op(incoming)
        op.execute(context)

        all_requests = [r for call in mock_hook.batch_update.call_args_list for r in call[0][1]]
        repeat_reqs = [r["repeatCell"] for r in all_requests if "repeatCell" in r]
        assert repeat_reqs, "Expected repeatCell request to clear formatting"
        rng = repeat_reqs[0]["range"]
        # Table starts at A1 → startColumnIndex=0, endColumnIndex=2 (2 headers)
        assert rng["startColumnIndex"] == 0
        assert rng["endColumnIndex"] == 2
        # 1 existing row (header) → insert_start = 0 + 1 = 1; 2 rows appended
        assert rng["startRowIndex"] == 1
        assert rng["endRowIndex"] == 3


# ==================================================================
# Task 7.1 — Overwrite range mismatch fix
# ==================================================================


class TestOverwriteRangeAlignment:
    def test_overwrite_with_cell_range_sheet_mode(self, mock_hook, context):
        """Sheet mode with cell_range: clears entire sheet, writes at B2."""
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="overwrite",
            cell_range="B2:D",
            data=[["x", "y", "z"]],
            has_headers=False,
            pause_between_batches=0,
        )
        op.execute(context)

        # Sheet mode clears entire sheet even with cell_range
        clear_range = mock_hook.clear_values.call_args[0][1]
        assert clear_range == "Sheet1"

        # Write should start at B2
        write_range = mock_hook.update_values.call_args[0][1]
        assert "B2" in write_range
        assert "A1" not in write_range

    def test_overwrite_without_cell_range_starts_at_a1(self, mock_hook, context):
        """Default (cell_range=None) writes from A1."""
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="overwrite",
            data=[["x"]],
            has_headers=False,
            pause_between_batches=0,
        )
        op.execute(context)

        write_range = mock_hook.update_values.call_args[0][1]
        assert "A1" in write_range
        mock_hook.trim_sheet.assert_called_once()

    def test_overwrite_batching_with_cell_range(self, mock_hook, context):
        """Multiple batches with cell_range='C5:F' continue from correct column."""
        data = [[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12]]
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="overwrite",
            cell_range="C5:F",
            data=data,
            has_headers=False,
            batch_size=2,
            pause_between_batches=0,
        )
        op.execute(context)

        # 3 rows / batch_size=2 → 2 batches
        assert mock_hook.update_values.call_count == 2
        ranges = [c[0][1] for c in mock_hook.update_values.call_args_list]
        assert "C5" in ranges[0]
        assert "C7" in ranges[1]

    def test_overwrite_with_sheet_and_cell_range(self, mock_hook, context):
        """Sheet prefix + cell_range combined correctly."""
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            sheet_name="Data",
            cell_range="B3:E",
            write_mode="overwrite",
            data=[["x"]],
            has_headers=False,
            pause_between_batches=0,
        )
        op.execute(context)

        # Sheet mode: clears entire sheet
        clear_range = mock_hook.clear_values.call_args[0][1]
        assert clear_range == "Data"

        write_range = mock_hook.update_values.call_args[0][1]
        assert write_range.startswith("Data!")
        assert "B3" in write_range


class TestOverwriteRangeMode:
    """Tests for clear_mode='range' — clears only data columns, no trim."""

    def test_range_mode_clears_only_data_columns(self, mock_hook, context):
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            sheet_name="Sheet1",
            write_mode="overwrite",
            clear_mode="range",
            data=[{"a": 1, "b": 2, "c": 3}],
        )
        op.execute(context)

        clear_range = mock_hook.clear_values.call_args[0][1]
        # default cell_range starts at A1, so clear begins from row 1
        assert clear_range == "Sheet1!A1:C"

    def test_range_mode_with_cell_range(self, mock_hook, context):
        """cell_range='B2:D' → clear columns B:D starting from row 2 (preserves row 1)."""
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            sheet_name="Sheet1",
            write_mode="overwrite",
            clear_mode="range",
            cell_range="B2:D",
            data=[["x", "y", "z"]],
            has_headers=False,
        )
        op.execute(context)

        clear_range = mock_hook.clear_values.call_args[0][1]
        assert clear_range == "Sheet1!B2:D"

        write_range = mock_hook.update_values.call_args[0][1]
        assert "B2" in write_range

    def test_range_mode_no_trim(self, mock_hook, context):
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="overwrite",
            clear_mode="range",
            data=[{"a": 1}],
        )
        op.execute(context)

        mock_hook.trim_sheet.assert_not_called()

    def test_range_mode_without_sheet_name(self, mock_hook, context):
        """Without sheet_name, uses Sheet1 as default prefix."""
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="overwrite",
            clear_mode="range",
            data=[["x", "y"]],
            has_headers=False,
        )
        op.execute(context)

        clear_range = mock_hook.clear_values.call_args[0][1]
        # no sheet_name, default cell_range starts at A1
        assert clear_range == "A1:B"

    def test_range_mode_preserves_rows_above_start(self, mock_hook, context):
        """cell_range='A3' — rows 1-2 (headers) must not be cleared."""
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            sheet_name="raw",
            write_mode="overwrite",
            clear_mode="range",
            cell_range="A3",
            data=[["val1", "val2"]],
            has_headers=False,
        )
        op.execute(context)

        clear_range = mock_hook.clear_values.call_args[0][1]
        # Must start at row 3, not row 1
        assert clear_range == "raw!A3:B"
        assert "A1" not in clear_range
        assert "A2" not in clear_range


class TestOverwriteSheetModeTrim:
    """Tests for clear_mode='sheet' trimming behavior."""

    def test_sheet_mode_trims_extra_rows(self, mock_hook, context):
        """Write 5 rows to sheet with 20 rows → trim to 5."""
        data = [[i] for i in range(5)]
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            sheet_name="Data",
            write_mode="overwrite",
            data=data,
            has_headers=False,
        )
        op.execute(context)

        mock_hook.trim_sheet.assert_called_once_with(SPREADSHEET_ID, "Data", 5)

    def test_sheet_mode_trim_with_headers_and_offset(self, mock_hook, context):
        """With headers + cell_range starting at row 3, keep_rows accounts for offset."""
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            sheet_name="Data",
            write_mode="overwrite",
            cell_range="A3",
            data=[{"x": 1, "y": 2}],
        )
        op.execute(context)

        # start_row=3, header + 1 data row = 2 rows → keep_rows = 3 + 2 - 1 = 4
        mock_hook.trim_sheet.assert_called_once_with(SPREADSHEET_ID, "Data", 4)


class TestColumnLetterToIndex:
    def test_single_letter(self):
        assert GoogleSheetsWriteOperator._column_letter_to_index("A") == 0
        assert GoogleSheetsWriteOperator._column_letter_to_index("B") == 1
        assert GoogleSheetsWriteOperator._column_letter_to_index("Z") == 25

    def test_double_letter(self):
        assert GoogleSheetsWriteOperator._column_letter_to_index("AA") == 26
        assert GoogleSheetsWriteOperator._column_letter_to_index("AB") == 27
        assert GoogleSheetsWriteOperator._column_letter_to_index("AZ") == 51

    def test_lowercase(self):
        assert GoogleSheetsWriteOperator._column_letter_to_index("a") == 0
        assert GoogleSheetsWriteOperator._column_letter_to_index("aa") == 26


class TestParseRangeStart:
    def test_simple_range(self):
        assert GoogleSheetsWriteOperator._parse_range_start("B2:D10") == ("B", 2)

    def test_with_sheet_prefix(self):
        assert GoogleSheetsWriteOperator._parse_range_start("Sheet1!C5:F") == ("C", 5)

    def test_single_cell(self):
        assert GoogleSheetsWriteOperator._parse_range_start("A1") == ("A", 1)

    def test_no_row_number(self):
        assert GoogleSheetsWriteOperator._parse_range_start("B:D") == ("B", 1)

    def test_default_column(self):
        col, row = GoogleSheetsWriteOperator._parse_range_start("1:100")
        assert col == "A"
        assert row == 1


# ==================================================================
# Task 7.2 — Non-contiguous row deletion
# ==================================================================


class TestNonContiguousDeletion:
    def test_non_contiguous_rows_deleted_separately(self, mock_hook, context):
        """Non-contiguous rows for a key must produce separate delete ops
        so that intermediate rows belonging to other keys are not destroyed."""
        mock_hook.get_values.return_value = [
            ["id"],
            ["A"],      # row 2
            ["other"],  # row 3 — different key, must NOT be deleted
            ["A"],      # row 4
            ["other2"], # row 5 — different key
            ["A"],      # row 6
        ]

        # A has 3 non-contiguous rows (2, 4, 6) → all 3 deleted, 1 appended
        incoming = [{"id": "A", "val": "a1"}]
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="merge",
            merge_key="id",
            data=incoming,
            pause_between_batches=0,
        )
        result = op.execute(context)

        assert result["deleted"] == 3
        assert result["appended"] == 1

        # Verify that delete operations are for individual rows, not a merged range
        # (a merged range would delete "other" and "other2" rows too)
        batch_args = mock_hook.batch_update.call_args[0][1]
        delete_ops = [r["deleteDimension"]["range"] for r in batch_args if "deleteDimension" in r]
        for dop in delete_ops:
            span = dop["endIndex"] - dop["startIndex"]
            assert span == 1, f"Expected single-row delete, got span={span}: {dop}"


# ==================================================================
# Task 8.2 — Deterministic key ordering
# ==================================================================


class TestDeterministicKeyOrder:
    def test_keys_processed_in_stable_order(self, mock_hook, context):
        """Keys should be processed without errors; C (not in incoming) stays untouched."""
        mock_hook.get_values.return_value = [
            ["id"],
            ["C"],   # row 2 — not in incoming, must stay
            ["A"],   # row 3
            ["B"],   # row 4
        ]

        incoming = [
            {"id": "B", "val": "b1"},
            {"id": "A", "val": "a1"},
            {"id": "D", "val": "d1"},  # new key
        ]
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="merge",
            merge_key="id",
            data=incoming,
            pause_between_batches=0,
        )
        result = op.execute(context)

        # A and B deleted + appended; D appended; C left untouched
        assert result["deleted"] == 2  # A(row3) + B(row4)
        assert result["appended"] == 3  # a1 + b1 + d1


# ==================================================================
# Task 8.3 — Batch update values
# ==================================================================


class TestBatchUpdateValuesUsage:
    def test_smart_merge_uses_batch_update_for_deletes_and_append_values_for_inserts(self, mock_hook, context):
        """Deletes use batch_update (deleteDimension); inserts use append_values + repeatCell."""
        mock_hook.get_values.return_value = [
            ["id"],
            ["A"],   # row 2
            ["B"],   # row 3
        ]

        incoming = [
            {"id": "A", "val": "a1"},
            {"id": "B", "val": "b1"},
        ]
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="merge",
            merge_key="id",
            data=incoming,
            pause_between_batches=0,
        )
        result = op.execute(context)

        assert result["deleted"] == 2
        assert result["appended"] == 2
        # batch_update called for deleteDimension and repeatCell
        mock_hook.batch_update.assert_called()
        # append_values used for writing data; batch_update_values not used
        mock_hook.append_values.assert_called()
        mock_hook.batch_update_values.assert_not_called()

    def test_insert_respects_batch_size(self, mock_hook, context):
        """Inserted rows should respect batch_size via append_values."""
        mock_hook.get_values.return_value = [["id"]]  # empty sheet

        incoming = [{"id": str(i), "val": str(i)} for i in range(5)]
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="merge",
            merge_key="id",
            data=incoming,
            batch_size=2,
            pause_between_batches=0,
        )
        result = op.execute(context)

        assert result["appended"] == 5
        # 5 rows / batch_size=2 → 3 append_values calls
        assert mock_hook.append_values.call_count == 3
        mock_hook.batch_update_values.assert_not_called()


# ==================================================================
# Task 8.4 — has_headers in smart merge
# ==================================================================


class TestSmartMergeHasHeaders:
    def test_has_headers_false_processes_row1_as_data(self, mock_hook, context):
        """When has_headers=False, row 1 is data — both rows should be deleted."""
        mock_hook.get_values.return_value = [
            ["A"],   # row 1 — data, NOT header
            ["B"],   # row 2
        ]

        incoming = [
            {"id": "A", "val": "a_new"},
            {"id": "B", "val": "b_new"},
        ]
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="merge",
            merge_key="id",
            data=incoming,
            has_headers=False,
            pause_between_batches=0,
        )
        result = op.execute(context)

        # Both rows indexed (row 1 not skipped), both deleted and re-appended
        assert result["deleted"] == 2
        assert result["appended"] == 2

    def test_has_headers_true_skips_row1(self, mock_hook, context):
        """When has_headers=True (default), row 1 is the header and is not indexed."""
        mock_hook.get_values.return_value = [
            ["id"],   # row 1 — header, must NOT be deleted
            ["A"],    # row 2
        ]

        incoming = [{"id": "A", "val": "new"}]
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="merge",
            merge_key="id",
            data=incoming,
            pause_between_batches=0,
        )
        result = op.execute(context)

        # Only row 2 deleted (row 1 header is skipped)
        assert result["deleted"] == 1
        assert result["appended"] == 1
        batch_args = mock_hook.batch_update.call_args[0][1]
        delete_ranges = [r["deleteDimension"]["range"] for r in batch_args if "deleteDimension" in r]
        # Delete must target row 2 (startIndex=1, endIndex=2), not row 1
        assert all(d["startIndex"] >= 1 for d in delete_ranges)


class TestColumnLetterConversion:
    def test_basic_letters(self):
        f = GoogleSheetsWriteOperator._index_to_column_letter
        assert f(0) == "A"
        assert f(1) == "B"
        assert f(25) == "Z"

    def test_double_letters(self):
        f = GoogleSheetsWriteOperator._index_to_column_letter
        assert f(26) == "AA"
        assert f(27) == "AB"
        assert f(51) == "AZ"
        assert f(52) == "BA"

    def test_triple_letters(self):
        f = GoogleSheetsWriteOperator._index_to_column_letter
        assert f(702) == "AAA"


class TestUnknownWriteMode:
    def test_raises_on_unknown_mode(self):
        with pytest.raises(ValueError, match="Invalid write_mode"):
            GoogleSheetsWriteOperator(
                task_id="test",
                spreadsheet_id=SPREADSHEET_ID,
                write_mode="invalid",
                data=[["x"]],
                has_headers=False,
            )


# ==================================================================
# Payload validation — no internal fields leak to API
# ==================================================================

ALLOWED_VALUE_RANGE_KEYS = {"range", "values", "majorDimension"}


class TestBatchUpdatePayloadCleanliness:
    """Ensure batch_update_values receives only API-compatible fields."""

    def _make_op(self, data, merge_key="id", **kwargs):
        defaults = dict(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="merge",
            merge_key=merge_key,
            batch_size=1000,
            pause_between_batches=0,
        )
        defaults.update(kwargs)
        return GoogleSheetsWriteOperator(data=data, **defaults)

    def _assert_clean_payloads(self, mock_hook):
        """Check every batch_update_values call has only allowed keys."""
        for call in mock_hook.batch_update_values.call_args_list:
            data = call[0][1]
            for item in data:
                extra = set(item.keys()) - ALLOWED_VALUE_RANGE_KEYS
                assert not extra, (
                    f"Internal fields leaked to API payload: {extra}. "
                    f"Item: {item}"
                )

    def test_post_insert_updates_have_no_internal_fields(self, mock_hook, context):
        """post_insert_updates should not contain row_num or _source_op."""
        mock_hook.get_values.return_value = [
            ["id"],
            ["A"],
            ["B"],
        ]
        incoming = [
            {"id": "A", "val": "a1"},
            {"id": "A", "val": "a2"},
            {"id": "A", "val": "a3"},
            {"id": "B", "val": "b1"},
            {"id": "B", "val": "b2"},
        ]
        op = self._make_op(incoming)
        op.execute(context)
        self._assert_clean_payloads(mock_hook)

    def test_regular_updates_have_no_internal_fields(self, mock_hook, context):
        """Regular value updates should also be clean."""
        mock_hook.get_values.return_value = [
            ["id"],
            ["A"],
            ["B"],
        ]
        incoming = [
            {"id": "A", "val": "updated_a"},
            {"id": "B", "val": "updated_b"},
        ]
        op = self._make_op(incoming)
        op.execute(context)
        self._assert_clean_payloads(mock_hook)


# ==================================================================
# table_start parameter
# ==================================================================


class TestTableStart:
    """Tests for the table_start parameter in append and smart_merge modes."""

    # ---- append ----

    def test_append_table_start_default_stored(self, mock_hook, context):
        """table_start defaults to 'A1'."""
        mock_hook.get_values.return_value = []
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="append",
            data=[{"a": 1}],
        )
        assert op.table_start == "A1"

    def test_append_table_start_empty_sheet_header_at_start_cell(self, mock_hook, context):
        """Empty sheet + table_start='C3' → header written to C3."""
        mock_hook.get_values.return_value = []
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="append",
            data=[{"x": 1, "y": 2}],
            table_start="C3",
            write_headers=True,
            pause_between_batches=0,
        )
        op.execute(context)

        # Header written to C3
        mock_hook.update_values.assert_called_once()
        args = mock_hook.update_values.call_args[0]
        assert "C3" in args[1]
        assert args[2] == [["x", "y"]]

        # Append target starts from C3
        append_target = mock_hook.append_values.call_args[0][1]
        assert "C3" in append_target

    def test_append_table_start_empty_row_check_uses_start_row(self, mock_hook, context):
        """Empty check reads from table_start row, not row 1."""
        mock_hook.get_values.return_value = []
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="append",
            data=[{"a": 1}],
            table_start="B5",
            write_headers=True,
            pause_between_batches=0,
        )
        op.execute(context)

        # get_values called to check emptiness at row 5 (not row 1)
        check_range = mock_hook.get_values.call_args[0][1]
        assert "5" in check_range
        assert "B" in check_range

    def test_append_table_start_non_empty_no_header(self, mock_hook, context):
        """Non-empty sheet at table_start row → header not written."""
        mock_hook.get_values.return_value = [["a", "b"]]
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="append",
            data=[{"a": 1, "b": 2}],
            table_start="C3",
            write_headers=True,
            pause_between_batches=0,
        )
        op.execute(context)

        mock_hook.update_values.assert_not_called()

    def test_append_table_start_default_behavior_unchanged(self, mock_hook, context):
        """table_start='A1' (default) behaves identical to before."""
        mock_hook.get_values.return_value = []
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="append",
            data=[{"a": 1}],
            write_headers=True,
            pause_between_batches=0,
        )
        op.execute(context)

        args = mock_hook.update_values.call_args[0]
        assert "A1" in args[1]

    # ---- smart_merge ----

    def test_smart_merge_table_start_key_column_offset(self, mock_hook, context):
        """table_start='C1': key is first col → absolute key col is C."""
        mock_hook.get_values.return_value = [["date"], ["2024-01-01"]]
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="merge",
            merge_key="date",
            data=[{"date": "2024-01-01", "val": "x"}],
            table_start="C1",
            pause_between_batches=0,
        )
        op.execute(context)

        key_range = mock_hook.get_values.call_args[0][1]
        # C is the start col; key_col_idx=0 → absolute = C
        assert key_range.startswith("C")

    def test_smart_merge_table_start_key_column_second(self, mock_hook, context):
        """table_start='C3': key is second column (idx=1) → absolute key col is D."""
        mock_hook.get_values.return_value = [["name", "date"], ["Alice", "2024-01-01"]]
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="merge",
            merge_key="date",
            data=[{"name": "Alice", "date": "2024-01-01"}],
            table_start="C3",
            pause_between_batches=0,
        )
        op.execute(context)

        key_range = mock_hook.get_values.call_args[0][1]
        # C(idx=2) + key_col_idx(1) = D(idx=3)
        assert "D" in key_range
        assert "3" in key_range  # starts from row 3

    def test_smart_merge_table_start_row_numbering(self, mock_hook, context):
        """table_start='A3': row 3 is header, data indexed from row 4."""
        mock_hook.get_values.return_value = [
            ["date"],        # row 3 — header
            ["2024-01-01"],  # row 4 — data
        ]
        incoming = [{"date": "2024-01-01", "val": "x"}]
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="merge",
            merge_key="date",
            data=incoming,
            table_start="A3",
            pause_between_batches=0,
        )
        result = op.execute(context)

        assert result["deleted"] == 1
        # Deletion should target row 4 (0-based index 3)
        all_requests = [r for call in mock_hook.batch_update.call_args_list for r in call[0][1]]
        delete_ranges = [r["deleteDimension"]["range"] for r in all_requests if "deleteDimension" in r]
        assert delete_ranges[0]["startIndex"] == 3   # 0-based row 4

    def test_smart_merge_table_start_empty_sheet_header_at_start_cell(self, mock_hook, context):
        """Empty sheet + table_start='C3' → header written to C3."""
        mock_hook.get_values.return_value = []
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="merge",
            merge_key="date",
            data=[{"date": "2024-01-01", "val": "x"}],
            table_start="C3",
            write_headers=True,
            pause_between_batches=0,
        )
        op.execute(context)

        mock_hook.update_values.assert_called_once()
        args = mock_hook.update_values.call_args[0]
        assert "C3" in args[1]
        assert args[2] == [["date", "val"]]

    def test_smart_merge_table_start_insert_position(self, mock_hook, context):
        """Inserted rows use table_start column in append_values range, not hardcoded A."""
        mock_hook.get_values.return_value = [["date"]]
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="merge",
            merge_key="date",
            data=[{"date": "2024-01-02", "val": "new"}],
            table_start="B2",
            pause_between_batches=0,
        )
        op.execute(context)

        # append_values range should start with B (table_start col)
        av_range = mock_hook.append_values.call_args[0][1]
        assert "B" in av_range

    def test_smart_merge_table_start_default_behavior_unchanged(self, mock_hook, context):
        """table_start='A1' (default) — behavior same as before."""
        mock_hook.get_values.return_value = [["date"], ["2024-01-01"]]
        incoming = [{"date": "2024-01-01", "val": "x"}]
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="merge",
            merge_key="date",
            data=incoming,
            pause_between_batches=0,
        )
        result = op.execute(context)

        assert result["deleted"] == 1
        assert result["appended"] == 1


# ==================================================================
# create_sheet_if_missing
# ==================================================================


class TestCreateSheetIfMissing:
    def test_sheet_already_exists_no_create(self, mock_hook, context):
        """When the sheet exists, create_sheet should not be called."""
        mock_hook.get_spreadsheet_metadata.return_value = {
            "sheets": [{"properties": {"sheetId": 0, "title": "Jan"}}]
        }
        mock_hook.get_sheet_properties.return_value = {
            "sheetId": 0,
            "title": "Jan",
            "gridProperties": {"rowCount": 1000},
        }

        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            sheet_name="Jan",
            create_sheet_if_missing=True,
            data=[["col"], [1]],
            pause_between_batches=0,
        )
        op.execute(context)

        mock_hook.create_sheet.assert_not_called()

    def test_sheet_missing_creates_it(self, mock_hook, context):
        """When the sheet does not exist, create_sheet is called."""
        # First call: metadata shows no "Feb" sheet
        mock_hook.get_spreadsheet_metadata.return_value = {
            "sheets": [{"properties": {"sheetId": 0, "title": "Jan"}}]
        }
        mock_hook.create_sheet.return_value = {"replies": [{}]}
        mock_hook.get_sheet_properties.return_value = {
            "sheetId": 1,
            "title": "Feb",
            "gridProperties": {"rowCount": 1000},
        }

        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            sheet_name="Feb",
            create_sheet_if_missing=True,
            data=[["col"], [1]],
            pause_between_batches=0,
        )
        op.execute(context)

        mock_hook.create_sheet.assert_called_once_with(SPREADSHEET_ID, "Feb")

    def test_race_condition_http_400_swallowed(self, mock_hook, context):
        """When create_sheet raises HTTP 400 (race condition), it is ignored."""
        from googleapiclient.errors import HttpError
        from httplib2 import Response

        mock_hook.get_spreadsheet_metadata.return_value = {
            "sheets": [{"properties": {"sheetId": 0, "title": "Jan"}}]
        }
        mock_hook.create_sheet.side_effect = HttpError(
            Response({"status": 400}),
            b"A sheet with the name 'Feb' already exists",
        )
        mock_hook.get_sheet_properties.return_value = {
            "sheetId": 1,
            "title": "Feb",
            "gridProperties": {"rowCount": 1000},
        }

        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            sheet_name="Feb",
            create_sheet_if_missing=True,
            data=[["col"], [1]],
            pause_between_batches=0,
        )
        # Should not raise
        op.execute(context)

    def test_non_400_http_error_propagates(self, mock_hook, context):
        """Non-400 HttpErrors from create_sheet should propagate."""
        from googleapiclient.errors import HttpError
        from httplib2 import Response

        mock_hook.get_spreadsheet_metadata.return_value = {
            "sheets": [{"properties": {"sheetId": 0, "title": "Jan"}}]
        }
        mock_hook.create_sheet.side_effect = HttpError(
            Response({"status": 403}), b"Forbidden"
        )

        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            sheet_name="Feb",
            create_sheet_if_missing=True,
            data=[["col"], [1]],
            pause_between_batches=0,
        )
        with pytest.raises(HttpError):
            op.execute(context)

    def test_400_other_error_propagates(self, mock_hook, context):
        """HTTP 400 without 'already exists' in body should propagate, not be swallowed."""
        from googleapiclient.errors import HttpError
        from httplib2 import Response

        mock_hook.get_spreadsheet_metadata.return_value = {
            "sheets": [{"properties": {"sheetId": 0, "title": "Jan"}}]
        }
        mock_hook.create_sheet.side_effect = HttpError(
            Response({"status": 400}),
            b"Too many sheets in the spreadsheet",
        )

        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            sheet_name="Feb",
            create_sheet_if_missing=True,
            data=[["col"], [1]],
            pause_between_batches=0,
        )
        with pytest.raises(HttpError):
            op.execute(context)

    def test_default_false_does_not_call_ensure(self, mock_hook, context):
        """With create_sheet_if_missing=False (default), no metadata check."""
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            sheet_name="Sheet1",
            data=[["col"], [1]],
            pause_between_batches=0,
        )
        op.execute(context)

        # get_spreadsheet_metadata may be called by other methods,
        # but create_sheet should never be called
        mock_hook.create_sheet.assert_not_called()

    def test_sheet_name_none_does_not_call_ensure(self, mock_hook, context):
        """When sheet_name is None, _ensure_sheet_exists is not called."""
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            create_sheet_if_missing=True,
            data=[["col"], [1]],
            pause_between_batches=0,
        )
        op.execute(context)

        mock_hook.create_sheet.assert_not_called()


# ==================================================================
# partition_by / partition_value
# ==================================================================


class TestPartitionBy:
    def test_filters_correct_rows(self, mock_hook, context):
        """Only rows matching partition_value are written."""
        data = [
            {"period": "2026-01", "val": 10},
            {"period": "2026-02", "val": 20},
            {"period": "2026-01", "val": 30},
            {"period": "2026-03", "val": 40},
        ]

        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            sheet_name="Sheet1",
            write_mode="overwrite",
            partition_by="period",
            partition_value="2026-01",
            data=data,
            pause_between_batches=0,
        )
        result = op.execute(context)

        assert result["rows_written"] == 3  # 1 header + 2 data rows
        # Check that update_values was called with filtered data
        written_rows = []
        for c in mock_hook.update_values.call_args_list:
            written_rows.extend(c[0][2])  # third positional arg = values
        # Header row + two "2026-01" rows
        assert len(written_rows) == 3
        assert written_rows[0] == ["period", "val"]  # header
        assert written_rows[1] == ["2026-01", 10]
        assert written_rows[2] == ["2026-01", 30]

    def test_numeric_partition_value_match(self, mock_hook, context):
        """Integer values in data match string partition_value."""
        data = [
            {"year": 2024, "amount": 100},
            {"year": 2025, "amount": 200},
            {"year": 2024, "amount": 300},
        ]

        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            sheet_name="Sheet1",
            write_mode="overwrite",
            partition_by="year",
            partition_value="2024",
            data=data,
            pause_between_batches=0,
        )
        result = op.execute(context)

        assert result["rows_written"] == 3  # header + 2 rows

    def test_no_matches_writes_empty(self, mock_hook, context):
        """No matching rows → only header written (overwrite mode)."""
        data = [
            {"period": "2026-01", "val": 10},
        ]

        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            sheet_name="Sheet1",
            write_mode="overwrite",
            partition_by="period",
            partition_value="2026-99",
            data=data,
            pause_between_batches=0,
        )
        result = op.execute(context)

        assert result["rows_written"] == 1  # header only

    def test_column_not_found_raises(self, mock_hook, context):
        data = [{"period": "2026-01", "val": 10}]

        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            sheet_name="Sheet1",
            partition_by="nonexistent",
            partition_value="x",
            data=data,
            pause_between_batches=0,
        )
        with pytest.raises(ValueError, match="not found in headers"):
            op.execute(context)

    def test_partition_by_none_no_filtering(self, mock_hook, context):
        """Without partition_by, all rows are written."""
        data = [
            {"a": 1, "b": 2},
            {"a": 3, "b": 4},
        ]

        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            sheet_name="Sheet1",
            write_mode="overwrite",
            data=data,
            pause_between_batches=0,
        )
        result = op.execute(context)

        assert result["rows_written"] == 3  # header + 2 rows

    def test_partition_by_without_value_raises(self, mock_hook, context):
        data = [{"period": "2026-01", "val": 10}]

        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            sheet_name="Sheet1",
            partition_by="period",
            # partition_value not set
            data=data,
            pause_between_batches=0,
        )
        with pytest.raises(ValueError, match="partition_value is required"):
            op.execute(context)

    def test_partition_by_with_merge_mode(self, mock_hook, context):
        """partition_by filters data before merge runs — only matching rows are merged."""
        mock_hook.get_values.return_value = []  # empty sheet
        data = [
            {"key": "a", "period": "2026-01", "val": 10},
            {"key": "b", "period": "2026-02", "val": 20},
            {"key": "c", "period": "2026-01", "val": 30},
        ]

        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            sheet_name="Sheet1",
            write_mode="merge",
            merge_key="key",
            partition_by="period",
            partition_value="2026-01",
            data=data,
            pause_between_batches=0,
        )
        result = op.execute(context)

        # Only 2026-01 rows (key=a, key=c) were passed to merge
        assert result["appended"] == 2

    def test_partition_with_append_mode(self, mock_hook, context):
        """partition_by works with append mode too."""
        data = [
            {"region": "US", "sales": 100},
            {"region": "EU", "sales": 200},
            {"region": "US", "sales": 300},
        ]
        mock_hook.get_values.return_value = [["region", "sales"]]

        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            sheet_name="Sheet1",
            write_mode="append",
            partition_by="region",
            partition_value="US",
            data=data,
            pause_between_batches=0,
        )
        result = op.execute(context)

        assert result["rows_written"] == 2  # only US rows appended


# ==================================================================
# column_mapping
# ==================================================================


class TestColumnMapping:
    """Tests for column_mapping parameter in GoogleSheetsWriteOperator."""

    def test_full_mapping_headers_written_to_sheet(self, mock_hook, context):
        """All columns renamed — sheet receives mapped header names."""
        data = [{"date_period": "2026-01", "revenue": 100}]
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            sheet_name="Sheet1",
            write_mode="overwrite",
            data=data,
            column_mapping={"date_period": "Период", "revenue": "Выручка"},
            pause_between_batches=0,
        )
        op.execute(context)

        written = mock_hook.update_values.call_args[0][2]
        assert written[0] == ["Период", "Выручка"]
        assert written[1] == ["2026-01", 100]

    def test_partial_mapping_unmapped_kept_as_is(self, mock_hook, context):
        """Only specified columns renamed; unmapped columns written with original names."""
        data = [{"date_period": "2026-01", "revenue": 100, "region": "US"}]
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            sheet_name="Sheet1",
            write_mode="overwrite",
            data=data,
            column_mapping={"date_period": "Период"},
            pause_between_batches=0,
        )
        op.execute(context)

        written = mock_hook.update_values.call_args[0][2]
        assert written[0] == ["Период", "revenue", "region"]

    def test_mapping_none_headers_unchanged(self, mock_hook, context):
        """No mapping — original headers written to sheet (no regression)."""
        data = [{"date_period": "2026-01", "revenue": 100}]
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            sheet_name="Sheet1",
            write_mode="overwrite",
            data=data,
            column_mapping=None,
            pause_between_batches=0,
        )
        op.execute(context)

        written = mock_hook.update_values.call_args[0][2]
        assert written[0] == ["date_period", "revenue"]

    def test_mapping_with_append_mode(self, mock_hook, context):
        """Mapping works in append mode — mapped headers written when sheet is empty."""
        mock_hook.get_values.return_value = []  # empty sheet
        data = [{"col_a": "x", "col_b": "y"}]
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            sheet_name="Sheet1",
            write_mode="append",
            data=data,
            column_mapping={"col_a": "Колонка А", "col_b": "Колонка Б"},
            pause_between_batches=0,
        )
        op.execute(context)

        # First update_values call writes headers
        header_call = mock_hook.update_values.call_args_list[0]
        assert header_call[0][2] == [["Колонка А", "Колонка Б"]]

    def test_mapping_with_merge_mode_merge_key_uses_original_name(self, mock_hook, context):
        """merge_key references original column name even when mapping renames it.

        column_mapping = {"period": "Период"}, merge_key = "period"
        Sheet is empty → headers written as ["Период", "rev"], merge works.
        """
        mock_hook.get_values.return_value = []  # empty sheet
        data = [{"period": "2026-01", "rev": 100}, {"period": "2026-02", "rev": 200}]
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            sheet_name="Sheet1",
            write_mode="merge",
            merge_key="period",  # original column name
            data=data,
            column_mapping={"period": "Период"},
            pause_between_batches=0,
        )
        result = op.execute(context)

        # Merge completed without ValueError
        assert result["appended"] == 2
        # Headers written with mapped name
        header_call = mock_hook.update_values.call_args
        assert header_call[0][2] == [["Период", "rev"]]

    def test_mapping_with_partition_by_uses_original_name(self, mock_hook, context):
        """partition_by references original column name when mapping is active."""
        data = [
            {"period": "2026-01", "rev": 100},
            {"period": "2026-02", "rev": 200},
            {"period": "2026-01", "rev": 300},
        ]
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            sheet_name="Sheet1",
            write_mode="overwrite",
            data=data,
            partition_by="period",       # original column name
            partition_value="2026-01",
            column_mapping={"period": "Период", "rev": "Выручка"},
            pause_between_batches=0,
        )
        op.execute(context)

        written = mock_hook.update_values.call_args[0][2]
        assert written[0] == ["Период", "Выручка"]   # mapped headers
        assert len(written) == 3                      # header + 2 matching rows

    def test_mapping_with_file_input(self, mock_hook, context, tmp_dir):
        """Mapping works when data is a .jsonl file path."""
        path = os.path.join(tmp_dir, "data.jsonl")
        with open(path, "w") as f:
            f.write('{"src_col": "val1", "amount": 10}\n')
            f.write('{"src_col": "val2", "amount": 20}\n')

        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            sheet_name="Sheet1",
            write_mode="overwrite",
            data=path,
            column_mapping={"src_col": "Источник", "amount": "Сумма"},
            pause_between_batches=0,
        )
        op.execute(context)

        written = mock_hook.update_values.call_args[0][2]
        assert written[0] == ["Источник", "Сумма"]
        assert written[1] == ["val1", 10]
        assert written[2] == ["val2", 20]


# ==================================================================
# input_format support in _format_rows
# ==================================================================


class TestFormatRowsInputFormat:
    def test_format_rows_with_input_format(self):
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="overwrite",
            data=[],
            schema={"date": {"type": "date", "input_format": "%Y-%m-%d", "format": "%d.%m.%Y"}},
        )
        result = op._format_rows(["date", "value"], [["2026-03-01", "val"]])
        assert result == [["01.03.2026", "val"]]

    def test_format_rows_without_input_format(self):
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="overwrite",
            data=[],
            schema={"date": {"type": "date", "format": "%d.%m.%Y"}},
        )
        result = op._format_rows(["date", "value"], [["01.03.2026", "val"]])
        assert result == [["01.03.2026", "val"]]


class TestMergeInputFormat:
    """Merge with input_format: no duplicates when incoming keys are ISO, sheet keys are formatted."""

    def _make_op(self, data, schema, **kwargs):
        defaults = dict(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="merge",
            merge_key="date",
            schema=schema,
            batch_size=1000,
            pause_between_batches=0,
        )
        defaults.update(kwargs)
        return GoogleSheetsWriteOperator(data=data, **defaults)

    def test_merge_no_duplicates_with_input_format(self, mock_hook, context):
        """Existing key '01.03.2026', incoming key '2026-03-01' → recognized as same → delete + re-append."""
        mock_hook.get_values.return_value = [
            ["date", "value"],
            ["01.03.2026", "old"],
        ]
        schema = {"date": {"type": "date", "input_format": "%Y-%m-%d", "format": "%d.%m.%Y"}}
        incoming = [{"date": "2026-03-01", "value": "new"}]
        op = self._make_op(incoming, schema=schema)
        result = op.execute(context)

        # Old row should be deleted (keys matched)
        assert result["deleted"] == 1
        assert result["appended"] == 1

        # Appended row must use "format" ("%d.%m.%Y"), not raw ISO string
        appended = mock_hook.append_values.call_args[0][2]
        assert appended[0][0] == "01.03.2026"


# ==================================================================
# normalize_merge_key_format flag
# ==================================================================


class TestNormalizeMergeKeyFormat:
    """Smoke tests: normalize_merge_key_format flag controls key normalization during merge."""

    def _make_op(self, data, schema, normalize_merge_key_format=True, **kwargs):
        defaults = dict(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="merge",
            merge_key="date",
            schema=schema,
            normalize_merge_key_format=normalize_merge_key_format,
            batch_size=1000,
            pause_between_batches=0,
        )
        defaults.update(kwargs)
        return GoogleSheetsWriteOperator(data=data, **defaults)

    def test_disabled_iso_key_in_sheet_not_normalized(self, mock_hook, context):
        """normalize_merge_key_format=False: sheet stores key in input_format (ISO '2024-01-01')
        while output format is '%d.%m.%Y'. Primary format fails to parse ISO → key not normalized
        → no match → existing row NOT deleted."""
        mock_hook.get_values.return_value = [
            ["date", "value"],
            ["2024-01-01", "old"],   # stored in ISO (input_format), not output format
        ]
        schema = {"date": {"type": "date", "input_format": "%Y-%m-%d", "format": "%d.%m.%Y"}}
        incoming = [{"date": "2024-01-01", "value": "new"}]
        op = self._make_op(incoming, schema=schema, normalize_merge_key_format=False)
        result = op.execute(context)

        # extended=False: tries to parse "2024-01-01" via format "%d.%m.%Y" → fails → returns raw "2024-01-01"
        # incoming "2024-01-01" formatted via format "%d.%m.%Y" → "01.01.2024"
        # "2024-01-01" != "01.01.2024" → no match → deleted=0
        assert result["deleted"] == 0
        assert result["appended"] == 1

    def test_enabled_iso_key_in_sheet_normalized_via_input_format(self, mock_hook, context):
        """normalize_merge_key_format=True (default): sheet stores key in input_format (ISO)
        Extended fallback: parse via input_format succeeds → key normalized to output format → match."""
        mock_hook.get_values.return_value = [
            ["date", "value"],
            ["2024-01-01", "old"],   # stored in ISO (input_format), not output format
        ]
        schema = {"date": {"type": "date", "input_format": "%Y-%m-%d", "format": "%d.%m.%Y"}}
        incoming = [{"date": "2024-01-01", "value": "new"}]
        op = self._make_op(incoming, schema=schema, normalize_merge_key_format=True)
        result = op.execute(context)

        # extended=True: step1 fails, step2 parses "2024-01-01" via input_format "%Y-%m-%d" → date(2024,1,1)
        # formatted via format "%d.%m.%Y" → "01.01.2024"
        # incoming "2024-01-01" formatted via format "%d.%m.%Y" → "01.01.2024"
        # "01.01.2024" == "01.01.2024" → match → deleted=1
        assert result["deleted"] == 1
        assert result["appended"] == 1

        # Appended row must use output format
        appended = mock_hook.append_values.call_args[0][2]
        assert appended[0][0] == "01.01.2024"

    def test_enabled_serial_date_key_in_sheet_normalized(self, mock_hook, context):
        """normalize_merge_key_format=True: sheet stores key as serial date number.
        Extended fallback (step 3) parses serial → date → match with incoming ISO key → deleted=1."""
        mock_hook.get_values.return_value = [
            ["date", "value"],
            ["46023", "old"],   # stored as Google Sheets serial number for 2026-01-01
        ]
        schema = {"date": {"type": "date", "format": "%Y-%m-%d"}}
        incoming = [{"date": "2026-01-01", "value": "new"}]
        op = self._make_op(incoming, schema=schema, normalize_merge_key_format=True)
        result = op.execute(context)

        # extended=True: step1 fails (can't parse "46023" via "%Y-%m-%d"), step2 skipped (no input_format),
        # step3 converts serial "46023" → date(2026,1,1) → "2026-01-01"
        # incoming "2026-01-01" formatted via "%Y-%m-%d" → "2026-01-01"
        # "2026-01-01" == "2026-01-01" → match → deleted=1
        assert result["deleted"] == 1
        assert result["appended"] == 1

        # Explicit schema with type="date" must also trigger SERIAL_NUMBER reads
        # (use_serial_number is keyed on key_schema.get("type") == "date" regardless
        # of whether key_schema came from an explicit schema or from inference).
        kwargs = mock_hook.get_values.call_args.kwargs
        assert kwargs.get("date_time_render_option") == "SERIAL_NUMBER"


# ==================================================================
# Schema-free date merge-key inference (Inferred Schema)
# ==================================================================


class TestMergeSchemaFreeDateInference:
    """merge_key date normalization without an explicit schema.

    Regression coverage for the production incident where a date-typed
    merge_key column had no explicit schema, and a manual format change on
    the sheet (Date → Number) caused merge to silently duplicate rows.
    """

    def _make_op(self, data, merge_key="date", **kwargs):
        defaults = dict(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="merge",
            merge_key=merge_key,
            batch_size=1000,
            pause_between_batches=0,
        )
        defaults.update(kwargs)
        return GoogleSheetsWriteOperator(data=data, **defaults)

    def test_iso_date_key_no_schema_serial_number_in_sheet_matches(self, mock_hook, context):
        """No schema, incoming ISO dates; sheet stores key as a serial number
        (e.g. after the cell display format was reset) → still matches and
        updates instead of duplicating."""
        mock_hook.get_values.return_value = [
            ["date", "value"],
            ["46023", "old"],  # serial number for 2026-01-01
        ]
        incoming = [
            {"date": "2026-01-01", "value": "new"},
            {"date": "2026-01-02", "value": "new2"},
        ]
        op = self._make_op(incoming)
        result = op.execute(context)

        assert result["deleted"] == 1
        assert result["appended"] == 2

        kwargs = mock_hook.get_values.call_args.kwargs
        assert kwargs.get("date_time_render_option") == "SERIAL_NUMBER"

    def test_numeric_non_date_key_no_schema_unaffected(self, mock_hook, context):
        """No schema, numeric (non-date) merge_key → no date inference,
        behaviour unchanged, FORMATTED_STRING still used."""
        mock_hook.get_values.return_value = [
            ["id", "value"],
            ["1001", "old"],
        ]
        incoming = [
            {"id": "1001", "value": "new"},
            {"id": "1002", "value": "new2"},
        ]
        op = self._make_op(incoming, merge_key="id")
        result = op.execute(context)

        assert result["deleted"] == 1
        assert result["appended"] == 2

        kwargs = mock_hook.get_values.call_args.kwargs
        assert kwargs.get("date_time_render_option") == "FORMATTED_STRING"

    def test_explicit_non_date_schema_blocks_inference(self, mock_hook, context):
        """Explicit schema entry for merge_key (even non-date) wins over
        inference — inference is never even attempted, read stays
        FORMATTED_STRING."""
        mock_hook.get_values.return_value = [
            ["date", "value"],
            ["2026-01-01", "old"],
        ]
        schema = {"date": {"type": "str"}}
        incoming = [{"date": "2026-01-01", "value": "new"}]
        op = self._make_op(incoming, schema=schema)
        result = op.execute(context)

        kwargs = mock_hook.get_values.call_args.kwargs
        assert kwargs.get("date_time_render_option") == "FORMATTED_STRING"
        # type="str" → normalize_merge_key returns raw unchanged → exact string match
        assert result["deleted"] == 1
        assert result["appended"] == 1

    def test_normalize_merge_key_format_false_forces_formatted_string(self, mock_hook, context):
        """Regression guard (review finding #1): normalize_merge_key_format=False
        disables inference entirely AND forces FORMATTED_STRING, even though
        incoming keys look like ISO dates — the flag is strictly legacy, not
        just legacy normalization on top of a changed read."""
        mock_hook.get_values.return_value = [
            ["date", "value"],
            ["2026-01-01", "old"],
        ]
        incoming = [{"date": "2026-01-01", "value": "new"}]
        op = self._make_op(incoming, normalize_merge_key_format=False)
        op.execute(context)

        kwargs = mock_hook.get_values.call_args.kwargs
        assert kwargs.get("date_time_render_option") == "FORMATTED_STRING"

    def test_explicit_datetime_schema_matching_format_keeps_formatted_string(
        self, mock_hook, context
    ):
        """Regression guard (review finding #2): explicit schema with
        type="datetime", format matching the sheet's current display, and a
        non-midnight time of day → still matches WITHOUT losing time of day
        (FORMATTED_STRING, not SERIAL_NUMBER — Step 1 parses the formatted
        string directly)."""
        mock_hook.get_values.return_value = [
            ["date", "value"],
            ["2026-01-01 12:30:00", "old"],
        ]
        schema = {"date": {"type": "datetime", "format": "%Y-%m-%d %H:%M:%S"}}
        incoming = [{"date": "2026-01-01 12:30:00", "value": "new"}]
        op = self._make_op(incoming, schema=schema)
        result = op.execute(context)

        kwargs = mock_hook.get_values.call_args.kwargs
        assert kwargs.get("date_time_render_option") == "FORMATTED_STRING"
        # Time of day preserved → keys match → old row replaced, not duplicated
        assert result["deleted"] == 1
        assert result["appended"] == 1

    def test_datetime_like_incoming_keys_no_schema_inference_skipped(self, mock_hook, context):
        """No schema, incoming keys look like datetimes
        ('2026-01-01 12:30:00') rather than plain ISO dates → inference
        returns None (datetime is not supported schema-free), read stays
        FORMATTED_STRING, behaviour unchanged from before this fix."""
        mock_hook.get_values.return_value = [
            ["date", "value"],
            ["2026-01-01 12:30:00", "old"],
        ]
        incoming = [{"date": "2026-01-01 12:30:00", "value": "new"}]
        op = self._make_op(incoming)
        result = op.execute(context)

        kwargs = mock_hook.get_values.call_args.kwargs
        assert kwargs.get("date_time_render_option") == "FORMATTED_STRING"
        assert result["deleted"] == 1
        assert result["appended"] == 1


# ==================================================================
# merge_key with duplicate incoming values
# ==================================================================


class TestMergeDuplicateIncomingKeys:
    """Behaviour when incoming data contains duplicate merge_key values."""

    def test_duplicate_keys_existing_rows_deleted_once_all_incoming_appended(
        self, mock_hook, context
    ):
        """One existing row for key 'A'; two incoming rows for key 'A'.
        The existing row should be deleted once, and both incoming rows appended."""
        mock_hook.get_values.return_value = [
            ["id", "val"],
            ["A", "old"],
        ]
        incoming = [
            {"id": "A", "val": "v1"},
            {"id": "A", "val": "v2"},
        ]
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="merge",
            merge_key="id",
            data=incoming,
            pause_between_batches=0,
        )
        result = op.execute(context)

        # Existing row deleted once, both incoming rows appended
        assert result["deleted"] == 1
        assert result["appended"] == 2

    def test_duplicate_keys_no_existing_rows_all_incoming_appended(
        self, mock_hook, context
    ):
        """Empty sheet; incoming data has duplicate keys — all rows appended."""
        mock_hook.get_values.return_value = []
        incoming = [
            {"id": "A", "val": "v1"},
            {"id": "A", "val": "v2"},
            {"id": "B", "val": "v3"},
        ]
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="merge",
            merge_key="id",
            data=incoming,
            write_headers=True,
            pause_between_batches=0,
        )
        result = op.execute(context)

        assert result["deleted"] == 0
        assert result["appended"] == 3


class TestRequestTimeout:
    def test_custom_timeout_passed_to_hook(self, context):
        with patch(
            "airflow_provider_google_sheets.operators.write.GoogleSheetsHook"
        ) as hook_cls:
            hook_cls.return_value = MagicMock()
            hook_cls.return_value.get_spreadsheet_metadata.return_value = {
                "sheets": [{"properties": {"sheetId": 0, "title": "Sheet1"}}]
            }
            hook_cls.return_value.get_values.return_value = []
            op = GoogleSheetsWriteOperator(
                task_id="test",
                spreadsheet_id=SPREADSHEET_ID,
                write_mode="overwrite",
                data=[["a", "b"]],
                request_timeout=600,
            )
            op.execute(context)
            hook_cls.assert_called_once_with(gcp_conn_id="google_cloud_default", request_timeout=600)

    def test_default_timeout_is_300(self, context):
        with patch(
            "airflow_provider_google_sheets.operators.write.GoogleSheetsHook"
        ) as hook_cls:
            hook_cls.return_value = MagicMock()
            hook_cls.return_value.get_values.return_value = []
            op = GoogleSheetsWriteOperator(
                task_id="test",
                spreadsheet_id=SPREADSHEET_ID,
                write_mode="overwrite",
                data=[["a", "b"]],
            )
            op.execute(context)
            hook_cls.assert_called_once_with(gcp_conn_id="google_cloud_default", request_timeout=300)

    def test_none_timeout_passed_to_hook(self, context):
        with patch(
            "airflow_provider_google_sheets.operators.write.GoogleSheetsHook"
        ) as hook_cls:
            hook_cls.return_value = MagicMock()
            hook_cls.return_value.get_values.return_value = []
            op = GoogleSheetsWriteOperator(
                task_id="test",
                spreadsheet_id=SPREADSHEET_ID,
                write_mode="overwrite",
                data=[["a", "b"]],
                request_timeout=None,
            )
            op.execute(context)
            hook_cls.assert_called_once_with(gcp_conn_id="google_cloud_default", request_timeout=None)


# ==================================================================
# sort_keys — Task 1: validation in __init__ and execute
# ==================================================================


class TestSortKeysValidation:
    def test_valid_sort_keys_init(self):
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="merge",
            merge_key="date",
            sort_keys=["date:desc"],
        )
        assert op.sort_keys == ["date:desc"]

    def test_sort_keys_default_none(self):
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
        )
        assert op.sort_keys is None

    def test_sort_keys_whitespace_padded_spec(self, mock_hook, context):
        """A padded spec 'date : desc' is accepted at init and resolves to the
        correct column and direction at execute()-time (both sides stripped)."""
        # init must not raise on the padded spec
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="overwrite",
            data=[{"date": "2024-01-01", "region": "a"}],
            sort_keys=["date : desc"],
        )
        op.execute(context)

        sort_range = _find_sort_range(mock_hook)
        assert sort_range is not None
        # column resolved despite surrounding spaces → header index 0
        assert sort_range["sortSpecs"][0]["dimensionIndex"] == 0
        # direction resolved despite spaces
        assert sort_range["sortSpecs"][0]["sortOrder"] == "DESCENDING"

    def test_sort_keys_not_list_raises_typeerror(self):
        with pytest.raises(TypeError, match="sort_keys must be a list"):
            GoogleSheetsWriteOperator(
                task_id="test",
                spreadsheet_id=SPREADSHEET_ID,
                sort_keys="date:desc",
            )

    def test_sort_keys_non_string_item_raises_typeerror(self):
        with pytest.raises(TypeError, match="sort_keys item must be a string"):
            GoogleSheetsWriteOperator(
                task_id="test",
                spreadsheet_id=SPREADSHEET_ID,
                sort_keys=[123],
            )

    def test_sort_keys_empty_list_raises_valueerror(self):
        with pytest.raises(ValueError, match="non-empty list or None"):
            GoogleSheetsWriteOperator(
                task_id="test",
                spreadsheet_id=SPREADSHEET_ID,
                sort_keys=[],
            )

    def test_sort_keys_duplicate_column_raises_valueerror(self):
        with pytest.raises(ValueError, match="specified more than once"):
            GoogleSheetsWriteOperator(
                task_id="test",
                spreadsheet_id=SPREADSHEET_ID,
                sort_keys=["date:asc", "date:desc"],
            )

    def test_sort_keys_missing_colon_raises_valueerror(self):
        with pytest.raises(ValueError, match="must be 'column:asc' or 'column:desc'"):
            GoogleSheetsWriteOperator(
                task_id="test",
                spreadsheet_id=SPREADSHEET_ID,
                sort_keys=["date"],
            )

    def test_sort_keys_bad_direction_raises_valueerror(self):
        with pytest.raises(ValueError, match="direction must be 'asc' or 'desc'"):
            GoogleSheetsWriteOperator(
                task_id="test",
                spreadsheet_id=SPREADSHEET_ID,
                sort_keys=["date:ascending"],
            )

    def test_sort_keys_empty_column_raises_valueerror(self):
        with pytest.raises(ValueError, match="column name is empty"):
            GoogleSheetsWriteOperator(
                task_id="test",
                spreadsheet_id=SPREADSHEET_ID,
                sort_keys=[":desc"],
            )

    def test_sort_keys_overwrite_clear_range_raises_valueerror(self):
        with pytest.raises(ValueError, match="clear_mode='range'"):
            GoogleSheetsWriteOperator(
                task_id="test",
                spreadsheet_id=SPREADSHEET_ID,
                write_mode="overwrite",
                clear_mode="range",
                sort_keys=["date:desc"],
            )

    def test_sort_keys_overwrite_clear_non_sheet_raises_valueerror(self, mock_hook, context):
        # The __init__ guard only raises on the exact literal "range" (to avoid
        # false positives on templated clear_mode values that are non-"sheet"
        # literals at DAG-parse time). Enforcement for an arbitrary non-"sheet"
        # value happens at the execute() runtime guard, which runs after Jinja
        # rendering and treats ANY clear_mode != "sheet" as a partial range clear.
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="overwrite",
            clear_mode="partial",  # non-"sheet" literal: allowed at construction
            data=[{"date": "2024-01-01", "region": "a"}],
            sort_keys=["date:desc"],
        )
        with pytest.raises(ValueError, match="clear_mode != 'sheet'"):
            op.execute(context)

    def test_sort_keys_append_cell_range_raises_valueerror(self):
        with pytest.raises(ValueError, match=r"append \+ cell_range"):
            GoogleSheetsWriteOperator(
                task_id="test",
                spreadsheet_id=SPREADSHEET_ID,
                write_mode="append",
                cell_range="B2",
                sort_keys=["date:desc"],
            )

    def test_sort_keys_append_clear_range_ok(self):
        # clear_mode is ignored in append; must not raise
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="append",
            clear_mode="range",
            sort_keys=["date:desc"],
        )
        assert op.sort_keys == ["date:desc"]

    def test_sort_keys_direction_case_insensitive_init(self):
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            sort_keys=["date:DESC", "region:Asc"],
        )
        assert op.sort_keys == ["date:DESC", "region:Asc"]

    def test_sort_keys_unknown_column_raises_in_execute(self, mock_hook, context):
        mock_hook.get_values.return_value = []
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="overwrite",
            data=[{"name": "a", "value": 1}],
            sort_keys=["missing:desc"],
        )
        with pytest.raises(ValueError, match="not found in headers"):
            op.execute(context)

    def test_sort_keys_column_mapping_name_ok_in_execute(self, mock_hook, context):
        mock_hook.get_values.return_value = []
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="overwrite",
            data=[{"src": "2024-01-01"}],
            column_mapping={"src": "date"},
            sort_keys=["date:desc"],
        )
        # Post-mapping name "date" exists → no error
        op.execute(context)

    def test_sort_keys_requires_headers_raises_in_execute(self, mock_hook, context):
        mock_hook.get_values.return_value = []
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="overwrite",
            data=[["a", "b"]],
            has_headers=False,
            sort_keys=["date:desc"],
        )
        with pytest.raises(ValueError, match="requires named headers"):
            op.execute(context)


# ==================================================================
# transient 404 retry — DAG-load validation of the knobs
# ==================================================================


class TestTransient404RetryValidation:
    def test_defaults(self):
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
        )
        assert op.transient_404_max_retries == 3
        assert op.transient_404_base_delay == 5.0

    def test_max_retries_negative_raises_valueerror(self):
        with pytest.raises(ValueError, match="transient_404_max_retries must be >= 0"):
            GoogleSheetsWriteOperator(
                task_id="test",
                spreadsheet_id=SPREADSHEET_ID,
                transient_404_max_retries=-1,
            )

    def test_max_retries_bool_raises_typeerror(self):
        with pytest.raises(TypeError, match="transient_404_max_retries must be a non-bool int"):
            GoogleSheetsWriteOperator(
                task_id="test",
                spreadsheet_id=SPREADSHEET_ID,
                transient_404_max_retries=True,
            )

    def test_max_retries_non_int_raises_typeerror(self):
        with pytest.raises(TypeError, match="transient_404_max_retries must be a non-bool int"):
            GoogleSheetsWriteOperator(
                task_id="test",
                spreadsheet_id=SPREADSHEET_ID,
                transient_404_max_retries=1.5,
            )

    def test_base_delay_negative_raises_valueerror(self):
        with pytest.raises(ValueError, match="transient_404_base_delay must be >= 0"):
            GoogleSheetsWriteOperator(
                task_id="test",
                spreadsheet_id=SPREADSHEET_ID,
                transient_404_base_delay=-1,
            )

    def test_base_delay_bool_raises_typeerror(self):
        with pytest.raises(TypeError, match="transient_404_base_delay must be a non-bool"):
            GoogleSheetsWriteOperator(
                task_id="test",
                spreadsheet_id=SPREADSHEET_ID,
                transient_404_base_delay=True,
            )

    def test_base_delay_non_numeric_raises_typeerror(self):
        with pytest.raises(TypeError, match="transient_404_base_delay must be a non-bool"):
            GoogleSheetsWriteOperator(
                task_id="test",
                spreadsheet_id=SPREADSHEET_ID,
                transient_404_base_delay="5",
            )

    def test_max_retries_zero_and_int_delay_ok(self):
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            transient_404_max_retries=0,
            transient_404_base_delay=0,
        )
        assert op.transient_404_max_retries == 0
        assert op.transient_404_base_delay == 0


# ==================================================================
# sort_keys — Task 2: _execute_sort method
# ==================================================================


class TestExecuteSort:
    def _make_op(self, sort_keys):
        return GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            sort_keys=sort_keys,
        )

    def test_single_key_descending(self):
        hook = MagicMock()
        op = self._make_op(["date:desc"])
        op._execute_sort(
            hook,
            headers=["date", "region", "value"],
            sheet_id=0,
            table_start_col="A",
            table_start_row=1,
            skip_header=False,
            end_row=10,
        )
        hook.batch_update.assert_called_once()
        requests = hook.batch_update.call_args[0][1]
        sort_range = requests[0]["sortRange"]
        specs = sort_range["sortSpecs"]
        assert len(specs) == 1
        assert specs[0]["sortOrder"] == "DESCENDING"
        assert specs[0]["dimensionIndex"] == 0
        assert sort_range["range"]["endRowIndex"] == 10

    def test_two_keys_order_preserved(self):
        hook = MagicMock()
        op = self._make_op(["date:desc", "region:asc"])
        op._execute_sort(
            hook,
            headers=["date", "region", "value"],
            sheet_id=0,
            table_start_col="A",
            table_start_row=1,
            skip_header=False,
            end_row=5,
        )
        specs = hook.batch_update.call_args[0][1][0]["sortRange"]["sortSpecs"]
        assert len(specs) == 2
        assert specs[0]["dimensionIndex"] == 0
        assert specs[0]["sortOrder"] == "DESCENDING"
        assert specs[1]["dimensionIndex"] == 1
        assert specs[1]["sortOrder"] == "ASCENDING"

    def test_skip_header_true_offsets_start_row(self):
        hook = MagicMock()
        op = self._make_op(["date:desc"])
        op._execute_sort(
            hook,
            headers=["date"],
            sheet_id=0,
            table_start_col="A",
            table_start_row=3,
            skip_header=True,
            end_row=10,
        )
        rng = hook.batch_update.call_args[0][1][0]["sortRange"]["range"]
        # table_start_row - 1 + 1 = 3
        assert rng["startRowIndex"] == 3

    def test_skip_header_false_no_offset(self):
        hook = MagicMock()
        op = self._make_op(["date:desc"])
        op._execute_sort(
            hook,
            headers=["date"],
            sheet_id=0,
            table_start_col="A",
            table_start_row=3,
            skip_header=False,
            end_row=10,
        )
        rng = hook.batch_update.call_args[0][1][0]["sortRange"]["range"]
        # table_start_row - 1 = 2
        assert rng["startRowIndex"] == 2

    def test_end_row_passed_through(self):
        hook = MagicMock()
        op = self._make_op(["date:desc"])
        op._execute_sort(
            hook,
            headers=["date"],
            sheet_id=0,
            table_start_col="A",
            table_start_row=1,
            skip_header=False,
            end_row=42,
        )
        rng = hook.batch_update.call_args[0][1][0]["sortRange"]["range"]
        assert rng["endRowIndex"] == 42

    def test_table_start_col_c_offsets_indices(self):
        hook = MagicMock()
        op = self._make_op(["value:asc"])
        op._execute_sort(
            hook,
            headers=["date", "region", "value"],
            sheet_id=0,
            table_start_col="C",
            table_start_row=1,
            skip_header=False,
            end_row=10,
        )
        sort_range = hook.batch_update.call_args[0][1][0]["sortRange"]
        rng = sort_range["range"]
        # C → index 2
        assert rng["startColumnIndex"] == 2
        assert rng["endColumnIndex"] == 2 + 3
        # "value" is index 2 in headers → 2 + 2 = 4
        assert sort_range["sortSpecs"][0]["dimensionIndex"] == 4

    def test_direction_case_insensitive(self):
        hook = MagicMock()
        op = self._make_op(["date:ASC", "region:Desc"])
        op._execute_sort(
            hook,
            headers=["date", "region"],
            sheet_id=0,
            table_start_col="A",
            table_start_row=1,
            skip_header=False,
            end_row=10,
        )
        specs = hook.batch_update.call_args[0][1][0]["sortRange"]["sortSpecs"]
        assert specs[0]["sortOrder"] == "ASCENDING"
        assert specs[1]["sortOrder"] == "DESCENDING"

    def test_noop_on_empty_range(self):
        hook = MagicMock()
        op = self._make_op(["date:desc"])
        # header-only: skip_header=True, end_row == table_start_row
        op._execute_sort(
            hook,
            headers=["date"],
            sheet_id=0,
            table_start_col="A",
            table_start_row=1,
            skip_header=True,
            end_row=1,
        )
        hook.batch_update.assert_not_called()


# ==================================================================
# sort_keys — Task 3: integration into _execute_overwrite
# ==================================================================


def _find_sort_range(mock_hook):
    """Return the sortRange payload from the first batch_update call that has it."""
    for call_obj in mock_hook.batch_update.call_args_list:
        for req in call_obj[0][1]:
            if "sortRange" in req:
                return req["sortRange"]
    return None


class TestOverwriteSortKeys:
    def test_overwrite_sort_with_headers(self, mock_hook, context):
        data = [{"name": f"n{i}", "age": i} for i in range(5)]
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="overwrite",
            write_headers=True,
            data=data,
            sort_keys=["name:asc"],
        )
        op.execute(context)

        sort_range = _find_sort_range(mock_hook)
        assert sort_range is not None
        rng = sort_range["range"]
        # 1 header + 5 data rows, start_row_num=1 → end_row = 0 + 6
        assert rng["endRowIndex"] == 6
        # skip header row
        assert rng["startRowIndex"] == 1
        assert sort_range["sortSpecs"][0]["dimensionIndex"] == 0
        assert sort_range["sortSpecs"][0]["sortOrder"] == "ASCENDING"

    def test_overwrite_sort_without_headers(self, mock_hook, context):
        data = [{"name": f"n{i}", "age": i} for i in range(5)]
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="overwrite",
            write_headers=False,
            data=data,
            sort_keys=["name:asc"],
        )
        op.execute(context)

        sort_range = _find_sort_range(mock_hook)
        assert sort_range is not None
        rng = sort_range["range"]
        # no header row written → end_row = 0 + 5
        assert rng["endRowIndex"] == 5
        assert rng["startRowIndex"] == 0

    def test_overwrite_no_sort_keys_no_sort_range(self, mock_hook, context):
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="overwrite",
            data=[{"name": "Alice", "age": 30}],
        )
        op.execute(context)

        assert _find_sort_range(mock_hook) is None

    def test_overwrite_sort_unknown_column_raises(self, mock_hook, context):
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="overwrite",
            data=[{"name": "Alice", "age": 30}],
            sort_keys=["missing:desc"],
        )
        with pytest.raises(ValueError, match="not found in headers"):
            op.execute(context)

    def test_overwrite_sort_column_mapping(self, mock_hook, context):
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="overwrite",
            data=[{"src": "2024-01-01", "val": 1}],
            column_mapping={"src": "date"},
            sort_keys=["date:desc"],
        )
        op.execute(context)

        sort_range = _find_sort_range(mock_hook)
        assert sort_range is not None
        # mapped header order: ["date", "val"] → "date" at index 0
        assert sort_range["sortSpecs"][0]["dimensionIndex"] == 0

    def test_overwrite_sort_range_covers_wide_rows(self, mock_hook, context):
        """list[list] data rows wider than the header row: the sortRange must
        cover the widest written row, not just len(headers) — otherwise the
        extra right-hand columns are not reordered and desync from sorted ones."""
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="overwrite",
            # header width 2, data rows width 3
            data=[
                ["date", "region"],
                ["2024-01-02", "a", "extra1"],
                ["2024-01-01", "b", "extra2"],
            ],
            has_headers=True,
            write_headers=True,
            sort_keys=["date:desc"],
        )
        op.execute(context)

        sort_range = _find_sort_range(mock_hook)
        assert sort_range is not None
        rng = sort_range["range"]
        # widest row is 3 columns → endColumnIndex = 0 + 3, not len(headers)=2
        assert rng["startColumnIndex"] == 0
        assert rng["endColumnIndex"] == 3
        # sort column still resolved via headers
        assert sort_range["sortSpecs"][0]["dimensionIndex"] == 0

    def test_overwrite_runtime_clear_mode_range_raises(self, mock_hook, context):
        """clear_mode is a template field; a value rendered to 'range' at runtime
        must be re-checked in execute() (the __init__ guard runs before Jinja
        rendering) and raise before any write happens."""
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="overwrite",
            clear_mode="sheet",  # valid at construction time
            data=[{"date": "2024-01-01", "region": "a"}],
            sort_keys=["date:desc"],
        )
        # Simulate a templated clear_mode rendering to "range" at runtime.
        op.clear_mode = "range"
        with pytest.raises(ValueError, match="clear_mode='range'"):
            op.execute(context)
        # guard fired before any mutation
        mock_hook.clear_values.assert_not_called()
        mock_hook.update_values.assert_not_called()
        assert _find_sort_range(mock_hook) is None

    def test_overwrite_runtime_clear_mode_non_sheet_raises(self, mock_hook, context):
        """_execute_overwrite treats ANY clear_mode != 'sheet' as a partial
        range clear, so a templated value like 'RANGE' or 'range ' (not the exact
        literal 'range') must still be caught in execute() before any write."""
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="overwrite",
            clear_mode="sheet",  # valid at construction time
            data=[{"date": "2024-01-01", "region": "a"}],
            sort_keys=["date:desc"],
        )
        # Simulate a templated clear_mode rendering to a non-"sheet" value that
        # still triggers a partial range clear.
        op.clear_mode = "RANGE"
        with pytest.raises(ValueError, match="clear_mode != 'sheet'"):
            op.execute(context)
        # guard fired before any mutation
        mock_hook.clear_values.assert_not_called()
        mock_hook.update_values.assert_not_called()
        assert _find_sort_range(mock_hook) is None


# ==================================================================
# sort_keys — Task 4: integration into _execute_append
# ==================================================================


class TestAppendSortKeys:
    def test_append_non_empty_sheet_sort(self, mock_hook, context):
        """Non-empty sheet + has_headers=True → skip_header, end covers all rows."""
        # full-width read: header + 2 data rows
        mock_hook.get_values.return_value = [
            ["date", "region"],
            ["2024-01-01", "a"],
            ["2024-01-02", "b"],
        ]
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="append",
            data=[{"date": "2024-01-03", "region": "c"}],
            has_headers=True,
            sort_keys=["date:desc"],
        )
        op.execute(context)

        sort_range = _find_sort_range(mock_hook)
        assert sort_range is not None
        rng = sort_range["range"]
        # existing 3 rows + 1 new = 4
        assert rng["endRowIndex"] == 4
        # header present → skip it
        assert rng["startRowIndex"] == 1
        assert sort_range["sortSpecs"][0]["sortOrder"] == "DESCENDING"
        # no header re-written on a non-empty sheet
        mock_hook.update_values.assert_not_called()

    def test_append_sort_width_ignores_wider_existing_rows(self, mock_hook, context):
        """Width contract (Finding B, Option 2): the sort range spans the table's
        declared/written width — max(len(headers), widest new row) — and does NOT
        widen to cover pre-existing on-sheet rows that are wider than the headers.
        Their right-hand cells are intentionally left unsorted, because an
        open-width read cannot distinguish same-table ragged cells from foreign
        adjacent-column data and reordering foreign data would be worse."""
        # Pre-existing rows on the sheet are 3 columns wide, but headers are 2.
        mock_hook.get_values.return_value = [
            ["date", "region"],
            ["2024-01-01", "a", "foreign1"],
            ["2024-01-02", "b", "foreign2"],
        ]
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="append",
            data=[{"date": "2024-01-03", "region": "c"}],
            has_headers=True,
            sort_keys=["date:desc"],
        )
        op.execute(context)

        sort_range = _find_sort_range(mock_hook)
        assert sort_range is not None
        rng = sort_range["range"]
        # Width stays at the declared table width (2), NOT the wider existing row.
        assert rng["startColumnIndex"] == 0
        assert rng["endColumnIndex"] == 2

    def test_append_empty_sheet_write_headers_sort(self, mock_hook, context):
        """Empty sheet + write_headers=True → header written, skip_header."""
        mock_hook.get_values.return_value = []
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="append",
            data=[{"date": "2024-01-01", "region": "a"}],
            write_headers=True,
            sort_keys=["date:desc"],
        )
        op.execute(context)

        mock_hook.update_values.assert_called_once()
        sort_range = _find_sort_range(mock_hook)
        assert sort_range is not None
        rng = sort_range["range"]
        # 1 header + 1 data row
        assert rng["endRowIndex"] == 2
        assert rng["startRowIndex"] == 1

    def test_append_empty_sheet_no_headers_sort(self, mock_hook, context):
        """Empty sheet + write_headers=False → no header, no skip."""
        mock_hook.get_values.return_value = []
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="append",
            data=[{"date": "2024-01-01", "region": "a"}],
            write_headers=False,
            has_headers=False,
            sort_keys=["date:desc"],
        )
        op.execute(context)

        mock_hook.update_values.assert_not_called()
        sort_range = _find_sort_range(mock_hook)
        assert sort_range is not None
        rng = sort_range["range"]
        assert rng["endRowIndex"] == 1
        assert rng["startRowIndex"] == 0

    def test_append_sort_counts_full_width_not_first_column(self, mock_hook, context):
        """Existing rows counted across full width, not by the first column."""
        # First column is empty on every row; second column has data.
        mock_hook.get_values.return_value = [["", "x"], ["", "y"]]
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="append",
            data=[{"date": "2024-01-03", "region": "z"}],
            has_headers=False,
            sort_keys=["date:desc"],
        )
        op.execute(context)

        sort_range = _find_sort_range(mock_hook)
        assert sort_range is not None
        # existing_row_count = 2 (counted by full width) + 1 new = 3
        assert sort_range["range"]["endRowIndex"] == 3
        # full-width read range, not a single cell
        read_range = mock_hook.get_values.call_args[0][1]
        assert read_range.endswith("A1:B")

    def test_append_no_sort_keys_no_sort_range(self, mock_hook, context):
        """sort_keys=None → no sortRange, single-cell emptiness check kept."""
        mock_hook.get_values.return_value = []
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="append",
            data=[{"date": "2024-01-01", "region": "a"}],
            write_headers=True,
        )
        op.execute(context)

        assert _find_sort_range(mock_hook) is None
        # emptiness check uses a single cell, not a full-width range
        check_range = mock_hook.get_values.call_args[0][1]
        assert check_range.endswith("A1")
        assert ":" not in check_range

    def test_append_table_start_c3_column_and_row_offset(self, mock_hook, context):
        """table_start='C3': the (start_row-1) row offset, full-width read range,
        and column-offset indices must all be derived from C3, not A1."""
        # header + 2 data rows at the full width starting from C3
        mock_hook.get_values.return_value = [
            ["date", "region"],
            ["2024-01-01", "a"],
            ["2024-01-02", "b"],
        ]
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="append",
            table_start="C3",
            data=[{"date": "2024-01-03", "region": "c"}],
            has_headers=True,
            sort_keys=["region:asc"],
        )
        op.execute(context)

        # full-width read range anchored at C3, spanning C..D
        read_range = mock_hook.get_values.call_args[0][1]
        assert read_range.endswith("C3:D")

        sort_range = _find_sort_range(mock_hook)
        assert sort_range is not None
        rng = sort_range["range"]
        # start_row=3, existing 3 rows + 1 new = 4 → end_row = (3-1) + 3 + 1 = 6
        assert rng["endRowIndex"] == 6
        # header present + row offset: data_start = (3-1) + 1 = 3
        assert rng["startRowIndex"] == 3
        # column offset: table starts at C (index 2)
        assert rng["startColumnIndex"] == 2
        assert rng["endColumnIndex"] == 4  # 2 + len(["date","region"])
        # "region" is header index 1 → absolute dimensionIndex 2 + 1 = 3
        assert sort_range["sortSpecs"][0]["dimensionIndex"] == 3
        assert sort_range["sortSpecs"][0]["sortOrder"] == "ASCENDING"

    def test_append_multi_key_sort_execute(self, mock_hook, context):
        """execute()-level multi-key sort keeps spec order and directions."""
        mock_hook.get_values.return_value = [
            ["date", "region"],
            ["2024-01-01", "a"],
        ]
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="append",
            data=[{"date": "2024-01-02", "region": "b"}],
            has_headers=True,
            sort_keys=["date:desc", "region:asc"],
        )
        op.execute(context)

        sort_range = _find_sort_range(mock_hook)
        assert sort_range is not None
        specs = sort_range["sortSpecs"]
        assert len(specs) == 2
        assert specs[0]["dimensionIndex"] == 0  # date
        assert specs[0]["sortOrder"] == "DESCENDING"
        assert specs[1]["dimensionIndex"] == 1  # region
        assert specs[1]["sortOrder"] == "ASCENDING"

    def test_append_write_headers_false_nonempty_skips_header(self, mock_hook, context):
        """write_headers=False + non-empty sheet + has_headers=True → the existing
        first row is treated as a header and SKIPPED. has_headers (not
        write_headers) governs existing-sheet header presence, consistent with
        build_existing_key_index. startRowIndex == table_start_row - 1 + 1 == 1."""
        mock_hook.get_values.return_value = [
            ["date", "region"],
            ["2024-01-02", "b"],
        ]
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="append",
            data=[{"date": "2024-01-03", "region": "c"}],
            has_headers=True,
            write_headers=False,
            sort_keys=["date:desc"],
        )
        op.execute(context)

        # no header row is ever written when write_headers=False
        mock_hook.update_values.assert_not_called()
        sort_range = _find_sort_range(mock_hook)
        assert sort_range is not None
        rng = sort_range["range"]
        # existing 2 rows + 1 new = 3, header row skipped → sort starts at row 1
        assert rng["endRowIndex"] == 3
        assert rng["startRowIndex"] == 1

    def test_append_sort_range_covers_wide_rows(self, mock_hook, context):
        """list[list] data rows wider than the header row: the sortRange must
        cover the widest written row, not just len(headers)."""
        mock_hook.get_values.return_value = []  # empty sheet
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="append",
            # header width 2, data row width 3
            data=[["date", "region"], ["2024-01-02", "a", "extra"]],
            has_headers=True,
            write_headers=False,
            sort_keys=["date:desc"],
        )
        op.execute(context)

        sort_range = _find_sort_range(mock_hook)
        assert sort_range is not None
        rng = sort_range["range"]
        # widest row is 3 columns → endColumnIndex = 0 + 3, not len(headers)=2
        assert rng["startColumnIndex"] == 0
        assert rng["endColumnIndex"] == 3
        # sort column still resolved via headers
        assert sort_range["sortSpecs"][0]["dimensionIndex"] == 0


# ==================================================================
# sort_keys — Task 5: integration into _execute_merge
# ==================================================================


class TestMergeSortKeys:
    def test_merge_sort_called_last(self, mock_hook, context):
        """sortRange runs after delete/append and has the correct range."""
        mock_hook.get_values.return_value = [["id"], ["A"], ["B"]]
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="merge",
            merge_key="id",
            data=[{"id": "B", "val": "x"}],
            has_headers=True,
            pause_between_batches=0,
            sort_keys=["id:desc"],
        )
        result = op.execute(context)

        assert result["deleted"] == 1
        assert result["appended"] == 1

        sort_range = _find_sort_range(mock_hook)
        assert sort_range is not None
        rng = sort_range["range"]
        # total_existing 3 - deleted 1 + appended 1 = 3 rows
        assert rng["endRowIndex"] == 3
        # header present in the sheet → skip it
        assert rng["startRowIndex"] == 1
        assert sort_range["sortSpecs"][0]["dimensionIndex"] == 0
        assert sort_range["sortSpecs"][0]["sortOrder"] == "DESCENDING"

        # sortRange must be the last batch_update issued
        last_requests = mock_hook.batch_update.call_args_list[-1][0][1]
        assert any("sortRange" in r for r in last_requests)

    def test_merge_no_sort_keys_no_sort_range(self, mock_hook, context):
        mock_hook.get_values.return_value = [["id"], ["A"], ["B"]]
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="merge",
            merge_key="id",
            data=[{"id": "B", "val": "x"}],
            pause_between_batches=0,
        )
        op.execute(context)

        assert _find_sort_range(mock_hook) is None

    def test_merge_sort_empty_incoming_still_sorts(self, mock_hook, context):
        """rows=[] (no append, no delete): sort still runs — total_existing must
        be computed outside the `if append_rows:` branch (else NameError)."""
        mock_hook.get_values.return_value = [["id"], ["A"], ["B"]]
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="merge",
            merge_key="id",
            # header-only list[list] → headers set, rows empty
            data=[["id", "val"]],
            has_headers=True,
            pause_between_batches=0,
            sort_keys=["id:desc"],
        )
        result = op.execute(context)

        assert result["deleted"] == 0
        assert result["appended"] == 0

        sort_range = _find_sort_range(mock_hook)
        assert sort_range is not None
        rng = sort_range["range"]
        # existing 3 rows, untouched
        assert rng["endRowIndex"] == 3
        assert rng["startRowIndex"] == 1

    def test_merge_sort_skip_header_write_headers_false_nonempty(self, mock_hook, context):
        """write_headers=False + non-empty sheet + has_headers=True → header SKIPPED.

        has_headers (not write_headers) governs whether the existing first row is
        a header, matching build_existing_key_index (which skips the existing
        first row whenever has_headers=True, independent of write_headers). So the
        existing header row is excluded from the sort → startRowIndex ==
        table_start_row - 1 + 1 == 1.
        """
        mock_hook.get_values.return_value = [["id"], ["A"], ["B"]]
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="merge",
            merge_key="id",
            data=[{"id": "C", "val": "x"}],
            has_headers=True,
            write_headers=False,
            pause_between_batches=0,
            sort_keys=["id:desc"],
        )
        op.execute(context)

        sort_range = _find_sort_range(mock_hook)
        assert sort_range is not None
        rng = sort_range["range"]
        # existing 3 rows + 1 appended (new key C) = 4
        assert rng["endRowIndex"] == 4
        # has_headers=True → existing first row is a header → skipped
        assert rng["startRowIndex"] == 1

    def test_merge_sort_skip_header_empty_sheet_no_headers(self, mock_hook, context):
        """write_headers=False + empty sheet → no header row → do not skip."""
        mock_hook.get_values.return_value = []
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="merge",
            merge_key="id",
            data=[["id", "val"], ["A", "x"]],
            has_headers=True,
            write_headers=False,
            pause_between_batches=0,
            sort_keys=["id:desc"],
        )
        op.execute(context)

        sort_range = _find_sort_range(mock_hook)
        assert sort_range is not None
        rng = sort_range["range"]
        # nothing existed, 1 appended, no header written
        assert rng["endRowIndex"] == 1
        assert rng["startRowIndex"] == 0

    def test_merge_has_headers_false_nonempty_includes_first_row(self, mock_hook, context):
        """has_headers=False + non-empty sheet: list[dict] input sets headers so
        sort_keys passes the execute() guard, but the sheet has NO header row.
        The first data row must be included in the sort range (startRowIndex=0)."""
        # Two existing DATA rows, no header row (has_headers=False).
        mock_hook.get_values.return_value = [["A"], ["B"]]
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="merge",
            merge_key="id",
            # list[dict] populates headers regardless of has_headers
            data=[{"id": "C", "val": "x"}],
            has_headers=False,
            write_headers=False,
            pause_between_batches=0,
            sort_keys=["id:desc"],
        )
        op.execute(context)

        sort_range = _find_sort_range(mock_hook)
        assert sort_range is not None
        rng = sort_range["range"]
        # existing 2 data rows + 1 appended (new key C) = 3
        assert rng["endRowIndex"] == 3
        # no physical header row → first data row IS included in the sort
        assert rng["startRowIndex"] == 0

    def test_merge_empty_sheet_write_headers_sort(self, mock_hook, context):
        """Empty sheet + write_headers=True: headers_just_written drives
        total_existing, end_row and skip_header. Header written, first data row
        skipped, end_row == 1 header + len(rows)."""
        mock_hook.get_values.return_value = []  # empty sheet
        rows_data = [{"id": "A", "val": "x"}, {"id": "B", "val": "y"}]
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="merge",
            merge_key="id",
            data=rows_data,
            has_headers=True,
            write_headers=True,
            pause_between_batches=0,
            sort_keys=["id:desc"],
        )
        op.execute(context)

        # header row was physically written to the empty sheet
        mock_hook.update_values.assert_called_once()
        written = mock_hook.update_values.call_args[0][2]
        assert written == [["id", "val"]]

        sort_range = _find_sort_range(mock_hook)
        assert sort_range is not None
        rng = sort_range["range"]
        # start_row=1 (table_start A1) → data_start = (1-1) + 1(skip header) = 1
        assert rng["startRowIndex"] == 1  # == table_start_row
        assert rng["endRowIndex"] == 1 + len(rows_data)


# ==================================================================
# sort_keys — Task 6: edge cases and finalization
# ==================================================================


class TestSortKeysEdgeCases:
    def test_overwrite_empty_after_partition_sort_noop(self, mock_hook, context):
        """partition filters out all rows → header-only table → sort is a no-op
        (data_start >= end_row) and must not raise."""
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="overwrite",
            data=[{"date": "2024-01-01", "region": "a"}],
            partition_by="region",
            partition_value="zzz",  # matches nothing
            write_headers=True,
            sort_keys=["date:desc"],
        )
        op.execute(context)

        # header-only range: startRowIndex(1) >= endRowIndex(1) → no sortRange
        assert _find_sort_range(mock_hook) is None

    def test_overwrite_empty_rows_write_headers_sort_noop(self, mock_hook, context):
        """overwrite with rows=[] (header-only list[list]) + write_headers=True →
        header-only range → sort no-op, no crash."""
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="overwrite",
            data=[["date", "region"]],  # header only → rows empty
            has_headers=True,
            write_headers=True,
            sort_keys=["date:desc"],
        )
        op.execute(context)

        assert _find_sort_range(mock_hook) is None

    def test_overwrite_empty_rows_no_headers_sort_noop(self, mock_hook, context):
        """overwrite with rows=[] + write_headers=False → fully empty range →
        sort no-op, no crash."""
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="overwrite",
            data=[["date", "region"]],  # header only → rows empty
            has_headers=True,
            write_headers=False,
            sort_keys=["date:desc"],
        )
        op.execute(context)

        assert _find_sort_range(mock_hook) is None

    def test_merge_no_append_no_delete_sorts_existing(self, mock_hook, context):
        """merge with no append and no delete still issues a sortRange over the
        existing rows only."""
        # 3 existing rows; incoming key "C" is absent → no delete, but appends.
        # To have neither delete nor append, send header-only data (rows empty).
        mock_hook.get_values.return_value = [["id"], ["A"], ["B"], ["C"]]
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="merge",
            merge_key="id",
            data=[["id", "val"]],  # header only → rows empty
            has_headers=True,
            pause_between_batches=0,
            sort_keys=["id:desc"],
        )
        result = op.execute(context)

        assert result["deleted"] == 0
        assert result["appended"] == 0
        sort_range = _find_sort_range(mock_hook)
        assert sort_range is not None
        rng = sort_range["range"]
        # 4 existing rows (1 header + 3 data), untouched
        assert rng["endRowIndex"] == 4
        assert rng["startRowIndex"] == 1

    def test_sort_keys_has_headers_false_raises_in_execute(self, mock_hook, context):
        """has_headers=False → no named headers → ValueError before sorting."""
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="overwrite",
            data=[["2024-01-01", "a"]],
            has_headers=False,
            sort_keys=["date:desc"],
        )
        with pytest.raises(ValueError, match="requires named headers"):
            op.execute(context)
        # execution stopped before any sortRange was issued
        assert _find_sort_range(mock_hook) is None

    def test_sort_keys_create_sheet_missing_empty_after_partition_noop(
        self, mock_hook, context
    ):
        """create_sheet_if_missing on a brand-new empty sheet + rows=[] after
        partition → sort no-op, no crash."""
        mock_hook.get_values.return_value = []  # freshly created sheet is empty
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            sheet_name="New",  # not in default metadata → gets created
            create_sheet_if_missing=True,
            write_mode="append",
            data=[{"date": "2024-01-01", "region": "a"}],
            partition_by="region",
            partition_value="zzz",  # matches nothing → rows empty
            write_headers=True,
            sort_keys=["date:desc"],
        )
        op.execute(context)

        mock_hook.create_sheet.assert_called_once()
        # header written (empty sheet), no data → header-only range → sort no-op
        assert _find_sort_range(mock_hook) is None


# ==================================================================
# Task 2 — Stateful in-memory fake sheet + transient-404 idempotency
# ==================================================================
#
# These tests prove idempotency by asserting the FINAL sheet rows after a
# re-run triggered by a transient 404 — NOT by call-count. The fake below
# actually mutates an in-memory grid on deleteDimension / append_values /
# update_values / clear / sortRange, and models the SERIAL_NUMBER date
# round-trip that merge idempotency depends on.

import re as _re
from collections import defaultdict as _defaultdict
from datetime import date as _date

from googleapiclient.errors import HttpError as _HttpError
from httplib2 import Response as _Response

_SHEETS_EPOCH = _date(1899, 12, 30)
_ISO_RE = _re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _to_serial(value):
    """Model Sheets' SERIAL_NUMBER render option for ISO-date cells."""
    if isinstance(value, str) and _ISO_RE.match(value):
        try:
            return (_date.fromisoformat(value) - _SHEETS_EPOCH).days
        except ValueError:
            return value
    return value


def _parse_cell(token):
    """'A1' -> (0, 1); 'B' -> (1, None)."""
    letters = "".join(c for c in token if c.isalpha())
    digits = "".join(c for c in token if c.isdigit())
    col = 0
    for ch in letters.upper():
        col = col * 26 + (ord(ch) - ord("A") + 1)
    col -= 1
    row = int(digits) if digits else None
    return col, row


def _parse_range(rng):
    """Parse an A1 range (optionally 'Title!...') into col/row bounds."""
    if "!" in rng:
        rng = rng.split("!", 1)[1]
    if ":" in rng:
        left, right = rng.split(":", 1)
        sc, sr = _parse_cell(left)
        ec, er = _parse_cell(right)
    else:
        sc, sr = _parse_cell(rng)
        ec, er = sc, sr
    return sc, sr, ec, er


class FakeSheet:
    """Mutable in-memory grid modelling a single sheet tab.

    Rows are ``list[list]``; column A is index 0. Trailing empties are trimmed
    on read, mirroring the real ``values.get`` behaviour.
    """

    def __init__(self, grid=None, titles=None):
        # deep-ish copy so callers can keep their literal
        self.grid = [list(r) for r in (grid or [])]
        self.titles = list(titles or ["Sheet1"])

    # -- internal helpers -------------------------------------------------
    def _set(self, row_1based, col_0based, value):
        while len(self.grid) < row_1based:
            self.grid.append([])
        row = self.grid[row_1based - 1]
        while len(row) <= col_0based:
            row.append("")
        row[col_0based] = value

    def _last_occupied_row(self):
        last = 0
        for i, row in enumerate(self.grid, start=1):
            if any(str(c).strip() != "" for c in row):
                last = i
        return last

    # -- mutating / reading operations -----------------------------------
    def get_values(self, rng, date_time_render_option="FORMATTED_STRING"):
        sc, sr, ec, er = _parse_range(rng)
        sr = sr or 1
        end_row = er if er is not None else self._last_occupied_row()
        end_col = ec if ec is not None else sc
        out = []
        for r in range(sr, end_row + 1):
            row = self.grid[r - 1] if r - 1 < len(self.grid) else []
            cells = []
            for c in range(sc, end_col + 1):
                v = row[c] if c < len(row) else ""
                if date_time_render_option == "SERIAL_NUMBER":
                    v = _to_serial(v)
                cells.append(v)
            # trim trailing empty cells within the row (-> [] for empty rows)
            while cells and (cells[-1] == "" or cells[-1] is None):
                cells.pop()
            out.append(cells)
        # trim trailing empty rows
        while out and not out[-1]:
            out.pop()
        return out

    def update_values(self, rng, values):
        sc, sr, _ec, _er = _parse_range(rng)
        sr = sr or 1
        for i, row in enumerate(values):
            for j, v in enumerate(row):
                self._set(sr + i, sc + j, v)

    def append_values(self, rng, values):
        sc, _sr, _ec, _er = _parse_range(rng)
        base = self._last_occupied_row()
        for i, row in enumerate(values):
            for j, v in enumerate(row):
                self._set(base + 1 + i, sc + j, v)

    def clear_values(self, rng):
        if ":" not in rng:
            # bare title -> whole-sheet clear
            self.grid = []
            return
        sc, sr, ec, er = _parse_range(rng)
        sr = sr or 1
        end_row = er if er is not None else len(self.grid)
        end_col = ec if ec is not None else sc
        for r in range(sr, end_row + 1):
            if r - 1 < len(self.grid):
                row = self.grid[r - 1]
                for c in range(sc, end_col + 1):
                    if c < len(row):
                        row[c] = ""

    def ensure_rows(self, required):
        while len(self.grid) < required:
            self.grid.append([])

    def trim_sheet(self, keep_rows):
        self.grid = self.grid[:keep_rows]

    def delete_dimension(self, start_index, end_index):
        # 0-based [start, end) row deletion, shifts subsequent rows up
        del self.grid[start_index:end_index]

    def sort_range(self, start_row, end_row, sort_specs):
        block = self.grid[start_row:end_row]
        for spec in reversed(sort_specs):
            dim = spec["dimensionIndex"]
            reverse = spec["sortOrder"] == "DESCENDING"
            block.sort(
                key=lambda r, d=dim: (r[d] if d < len(r) else ""),
                reverse=reverse,
            )
        self.grid[start_row:end_row] = block

    def data_rows(self, skip_header=True):
        """Return grid rows (optionally skipping the header) for assertions."""
        rows = self.grid[1:] if skip_header else self.grid
        return [list(r) for r in rows]


class FakeSheetsHook:
    """Stateful fake hook wrapping a :class:`FakeSheet`.

    Supports one-shot fault injection via :meth:`fail_once` so a transient
    404 can be raised at a chosen operation/occurrence (``before`` or
    ``after`` the state mutation is applied).
    """

    def __init__(self, sheet):
        self.sheet = sheet
        self._counts = _defaultdict(int)
        self._faults = _defaultdict(list)

    # -- fault injection --------------------------------------------------
    def fail_once(self, op, status=404, when="after", occurrence=1):
        self._faults[op].append(
            {"occurrence": occurrence, "status": status, "when": when, "fired": False}
        )

    def _enter(self, op):
        self._counts[op] += 1
        return self._counts[op]

    def _maybe_fail(self, op, count, phase):
        for f in self._faults[op]:
            if not f["fired"] and f["occurrence"] == count and f["when"] == phase:
                f["fired"] = True
                raise _HttpError(_Response({"status": f["status"]}), b"boom")

    # -- metadata ---------------------------------------------------------
    def get_spreadsheet_metadata(self, spreadsheet_id):
        n = self._enter("get_spreadsheet_metadata")
        self._maybe_fail("get_spreadsheet_metadata", n, "before")
        result = {
            "sheets": [
                {"properties": {"sheetId": i, "title": t}}
                for i, t in enumerate(self.sheet.titles)
            ]
        }
        self._maybe_fail("get_spreadsheet_metadata", n, "after")
        return result

    def get_sheet_id(self, spreadsheet_id, sheet_name):
        return (
            self.sheet.titles.index(sheet_name)
            if sheet_name in self.sheet.titles
            else 0
        )

    def create_sheet(self, spreadsheet_id, sheet_name):
        n = self._enter("create_sheet")
        self._maybe_fail("create_sheet", n, "before")
        if sheet_name not in self.sheet.titles:
            self.sheet.titles.append(sheet_name)
        self._maybe_fail("create_sheet", n, "after")

    # -- values -----------------------------------------------------------
    def get_values(self, spreadsheet_id, rng, date_time_render_option="FORMATTED_STRING"):
        n = self._enter("get_values")
        self._maybe_fail("get_values", n, "before")
        result = self.sheet.get_values(rng, date_time_render_option)
        self._maybe_fail("get_values", n, "after")
        return result

    def update_values(self, spreadsheet_id, rng, values):
        n = self._enter("update_values")
        self._maybe_fail("update_values", n, "before")
        self.sheet.update_values(rng, values)
        self._maybe_fail("update_values", n, "after")

    def append_values(self, spreadsheet_id, rng, values):
        n = self._enter("append_values")
        self._maybe_fail("append_values", n, "before")
        self.sheet.append_values(rng, values)
        self._maybe_fail("append_values", n, "after")

    def clear_values(self, spreadsheet_id, rng):
        n = self._enter("clear_values")
        self._maybe_fail("clear_values", n, "before")
        self.sheet.clear_values(rng)
        self._maybe_fail("clear_values", n, "after")

    def ensure_rows(self, spreadsheet_id, sheet_name, required):
        self.sheet.ensure_rows(required)

    def trim_sheet(self, spreadsheet_id, sheet_name, keep_rows):
        self.sheet.trim_sheet(keep_rows)

    # -- batch update (deleteDimension / repeatCell / sortRange) ----------
    def batch_update(self, spreadsheet_id, requests):
        op = next(iter(requests[0].keys()))
        n = self._enter(op)
        self._maybe_fail(op, n, "before")
        for req in requests:
            self._apply_request(req)
        self._maybe_fail(op, n, "after")

    def _apply_request(self, req):
        if "deleteDimension" in req:
            rng = req["deleteDimension"]["range"]
            self.sheet.delete_dimension(rng["startIndex"], rng["endIndex"])
        elif "sortRange" in req:
            sr = req["sortRange"]
            rng = sr["range"]
            self.sheet.sort_range(
                rng["startRowIndex"], rng["endRowIndex"], sr["sortSpecs"]
            )
        # repeatCell: formatting only -> no-op on the value grid


def _run_with_fake(fake_hook, op, context):
    """Execute *op* with the fake hook patched in and sleep disabled."""
    with patch(
        "airflow_provider_google_sheets.operators.write.GoogleSheetsHook",
        return_value=fake_hook,
    ), patch("airflow_provider_google_sheets.operators.write.time.sleep"):
        return op.execute(context)


class TestTransient404IdempotencyMerge:
    def test_404_on_delete_dimension_reruns_no_dupes(self, context):
        """404 right after deleteDimension (deletes applied, appends not) →
        full merge re-run yields the correct final rows without dupes/loss."""
        sheet = FakeSheet(
            grid=[
                ["date", "value"],
                ["2026-01-14", "old-A"],
                ["2026-01-15", "old-B"],
            ]
        )
        hook = FakeSheetsHook(sheet)
        hook.fail_once("deleteDimension", status=404, when="after", occurrence=1)

        op = GoogleSheetsWriteOperator(
            task_id="t",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="merge",
            merge_key="date",
            data=[{"date": "2026-01-15", "value": "new-B"}],
            pause_between_batches=0,
            transient_404_base_delay=0,
        )
        _run_with_fake(hook, op, context)

        assert sheet.data_rows() == [
            ["2026-01-14", "old-A"],
            ["2026-01-15", "new-B"],
        ]

    def test_404_on_append_mid_loop_reruns_correct(self, context):
        """404 mid append loop (first batch applied) → re-run → correct rows."""
        sheet = FakeSheet(
            grid=[
                ["date", "value"],
                ["2026-01-14", "old-A"],
            ]
        )
        hook = FakeSheetsHook(sheet)
        # batch_size=1 -> two append calls; fail before the 2nd (first applied)
        hook.fail_once("append_values", status=404, when="before", occurrence=2)

        op = GoogleSheetsWriteOperator(
            task_id="t",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="merge",
            merge_key="date",
            data=[
                {"date": "2026-01-16", "value": "C"},
                {"date": "2026-01-17", "value": "D"},
            ],
            batch_size=1,
            pause_between_batches=0,
            transient_404_base_delay=0,
        )
        _run_with_fake(hook, op, context)

        assert sorted(sheet.data_rows()) == [
            ["2026-01-14", "old-A"],
            ["2026-01-16", "C"],
            ["2026-01-17", "D"],
        ]

    def test_round_trip_partial_append_found_and_deleted_on_rerun(self, context):
        """The load-bearing merge assumption: a row appended in run 1 (its date
        key stored) is read back via SERIAL_NUMBER, normalised to the same key,
        found and deleted on the re-run → no duplicate."""
        sheet = FakeSheet(
            grid=[
                ["date", "value"],
                ["2026-01-14", "old-A"],
                ["2026-01-15", "old-B"],
            ]
        )
        hook = FakeSheetsHook(sheet)
        # deletes + append applied, then 404 on repeatCell (post-append)
        hook.fail_once("repeatCell", status=404, when="after", occurrence=1)

        op = GoogleSheetsWriteOperator(
            task_id="t",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="merge",
            merge_key="date",
            data=[{"date": "2026-01-15", "value": "new-B"}],
            pause_between_batches=0,
            transient_404_base_delay=0,
        )
        _run_with_fake(hook, op, context)

        # Exactly one 2026-01-15 row, carrying the new value (no duplicate).
        assert sheet.data_rows() == [
            ["2026-01-14", "old-A"],
            ["2026-01-15", "new-B"],
        ]

    def test_merge_sort_tail_404_reruns_sorted_no_dupes(self, context):
        """404 on the sort tail → re-run → sorted, no dupes."""
        sheet = FakeSheet(
            grid=[
                ["date", "value"],
                ["2026-01-14", "old-A"],
                ["2026-01-15", "old-B"],
            ]
        )
        hook = FakeSheetsHook(sheet)
        hook.fail_once("sortRange", status=404, when="before", occurrence=1)

        op = GoogleSheetsWriteOperator(
            task_id="t",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="merge",
            merge_key="date",
            data=[{"date": "2026-01-16", "value": "C"}],
            sort_keys=["date:desc"],
            pause_between_batches=0,
            transient_404_base_delay=0,
        )
        _run_with_fake(hook, op, context)

        assert sheet.data_rows() == [
            ["2026-01-16", "C"],
            ["2026-01-15", "old-B"],
            ["2026-01-14", "old-A"],
        ]


class TestTransient404IdempotencyOverwrite:
    def test_overwrite_404_mid_write_reruns_no_dupes(self, context):
        """404 mid write (partial batch written) → re-clear + rewrite on re-run
        → exactly the new rows, no dupes or stale rows."""
        sheet = FakeSheet(
            grid=[
                ["name", "n"],
                ["stale-1", 91],
                ["stale-2", 92],
                ["stale-3", 93],
                ["stale-4", 94],
            ]
        )
        hook = FakeSheetsHook(sheet)
        # header + 3 rows @ batch_size=1 -> 4 update_values calls; fail after #3
        hook.fail_once("update_values", status=404, when="after", occurrence=3)

        op = GoogleSheetsWriteOperator(
            task_id="t",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="overwrite",
            data=[
                {"name": "Al", "n": 1},
                {"name": "Bo", "n": 2},
                {"name": "Cy", "n": 3},
            ],
            batch_size=1,
            pause_between_batches=0,
            transient_404_base_delay=0,
        )
        _run_with_fake(hook, op, context)

        assert sheet.grid == [
            ["name", "n"],
            ["Al", 1],
            ["Bo", 2],
            ["Cy", 3],
        ]

    def test_overwrite_sort_tail_404_reruns_sorted(self, context):
        """404 on the sort tail → full overwrite re-run → sorted correctly."""
        sheet = FakeSheet(grid=[["old"], ["x"]])
        hook = FakeSheetsHook(sheet)
        hook.fail_once("sortRange", status=404, when="before", occurrence=1)

        op = GoogleSheetsWriteOperator(
            task_id="t",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="overwrite",
            data=[
                {"date": "2026-01-14", "v": "a"},
                {"date": "2026-01-16", "v": "c"},
                {"date": "2026-01-15", "v": "b"},
            ],
            sort_keys=["date:desc"],
            pause_between_batches=0,
            transient_404_base_delay=0,
        )
        _run_with_fake(hook, op, context)

        assert sheet.grid == [
            ["date", "v"],
            ["2026-01-16", "c"],
            ["2026-01-15", "b"],
            ["2026-01-14", "a"],
        ]


class TestTransient404EnsureSheet:
    def test_ensure_sheet_404_on_create_reruns_and_creates(self, context):
        """404 on create_sheet → wrapper re-runs the ensure step → sheet is
        created on the retry and the write proceeds without raising."""
        sheet = FakeSheet(grid=[], titles=["Jan"])
        hook = FakeSheetsHook(sheet)
        hook.fail_once("create_sheet", status=404, when="before", occurrence=1)

        op = GoogleSheetsWriteOperator(
            task_id="t",
            spreadsheet_id=SPREADSHEET_ID,
            sheet_name="Feb",
            create_sheet_if_missing=True,
            write_mode="overwrite",
            data=[{"col": 1}],
            pause_between_batches=0,
            transient_404_base_delay=0,
        )
        _run_with_fake(hook, op, context)

        assert "Feb" in sheet.titles
        assert hook._counts["create_sheet"] == 2  # first 404, second succeeds

    def test_ensure_sheet_404_on_metadata_reruns(self, context):
        """404 on the metadata read inside ensure → wrapper re-runs → succeeds."""
        sheet = FakeSheet(grid=[], titles=["Jan"])
        hook = FakeSheetsHook(sheet)
        hook.fail_once("get_spreadsheet_metadata", status=404, when="before", occurrence=1)

        op = GoogleSheetsWriteOperator(
            task_id="t",
            spreadsheet_id=SPREADSHEET_ID,
            sheet_name="Feb",
            create_sheet_if_missing=True,
            write_mode="overwrite",
            data=[{"col": 1}],
            pause_between_batches=0,
            transient_404_base_delay=0,
        )
        _run_with_fake(hook, op, context)

        assert "Feb" in sheet.titles
