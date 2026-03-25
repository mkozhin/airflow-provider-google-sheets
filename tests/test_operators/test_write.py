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
    def test_group_contiguous_basic(self):
        f = GoogleSheetsWriteOperator._group_contiguous
        assert f([3, 7, 8, 12]) == [(3, 3), (7, 8), (12, 12)]

    def test_group_contiguous_all_sequential(self):
        f = GoogleSheetsWriteOperator._group_contiguous
        assert f([5, 6, 7]) == [(5, 7)]

    def test_group_contiguous_single(self):
        f = GoogleSheetsWriteOperator._group_contiguous
        assert f([10]) == [(10, 10)]

    def test_group_contiguous_empty(self):
        f = GoogleSheetsWriteOperator._group_contiguous
        assert f([]) == []

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
    def test_raises_on_unknown_mode(self, mock_hook, context):
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="invalid",
            data=[["x"]],
            has_headers=False,
        )
        with pytest.raises(ValueError, match="Unknown write_mode"):
            op.execute(context)


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
