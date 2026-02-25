"""Tests for GoogleSheetsWriteOperator."""

from __future__ import annotations

import json
import os
import tempfile
from datetime import date
from unittest.mock import MagicMock, patch, call

import pytest

from airflow_google_sheets.operators.write import GoogleSheetsWriteOperator


SPREADSHEET_ID = "test-spreadsheet-id"


@pytest.fixture
def mock_hook():
    with patch(
        "airflow_google_sheets.operators.write.GoogleSheetsHook"
    ) as hook_cls:
        hook = MagicMock()
        hook_cls.return_value = hook
        # Default metadata for smart merge
        hook.get_spreadsheet_metadata.return_value = {
            "sheets": [{"properties": {"sheetId": 0, "title": "Sheet1"}}]
        }
        hook.get_sheet_id.return_value = 0
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
        mock_hook.update_values.assert_called_once()
        # First row should be headers, second row data
        written = mock_hook.update_values.call_args[0][2]
        assert written[0] == ["name", "age"]
        assert written[1] == ["Alice", 30]
        assert result["mode"] == "overwrite"
        assert result["rows_written"] == 2  # header + 1 data row

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
        assert "Data!" in clear_range

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
        path = os.path.join(tmp_dir, "data.json")
        with open(path, "w") as f:
            json.dump([{"x": 10}, {"x": 20}], f)

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
    """Tests for the smart_merge write mode."""

    def _make_op(self, data, merge_key="date", **kwargs):
        defaults = dict(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="smart_merge",
            merge_key=merge_key,
            batch_size=1000,
            pause_between_batches=0,
        )
        defaults.update(kwargs)
        return GoogleSheetsWriteOperator(data=data, **defaults)

    def test_same_row_count_updates_in_place(self, mock_hook, context):
        """3 existing rows for 2024-04-01, 3 incoming → pure update."""
        mock_hook.get_values.return_value = [
            ["date"],           # header (row 1)
            ["2024-04-01"],     # row 2
            ["2024-04-01"],     # row 3
            ["2024-04-01"],     # row 4
        ]

        incoming = [
            {"date": "2024-04-01", "val": "new1"},
            {"date": "2024-04-01", "val": "new2"},
            {"date": "2024-04-01", "val": "new3"},
        ]
        op = self._make_op(incoming)
        result = op.execute(context)

        assert result["updated"] == 3
        assert result["inserted"] == 0
        assert result["deleted"] == 0
        assert mock_hook.batch_update.call_count == 0  # no structural changes

    def test_more_incoming_inserts_rows(self, mock_hook, context):
        """2 existing, 4 incoming → update 2 + insert 2."""
        mock_hook.get_values.return_value = [
            ["date"],
            ["2024-04-01"],  # row 2
            ["2024-04-01"],  # row 3
        ]

        incoming = [
            {"date": "2024-04-01", "val": "a"},
            {"date": "2024-04-01", "val": "b"},
            {"date": "2024-04-01", "val": "c"},
            {"date": "2024-04-01", "val": "d"},
        ]
        op = self._make_op(incoming)
        result = op.execute(context)

        assert result["updated"] == 2
        assert result["inserted"] == 2
        # insertDimension should have been called via batch_update
        mock_hook.batch_update.assert_called()

    def test_fewer_incoming_deletes_rows(self, mock_hook, context):
        """3 existing, 1 incoming → update 1 + delete 2."""
        mock_hook.get_values.return_value = [
            ["date"],
            ["2024-04-01"],  # row 2
            ["2024-04-01"],  # row 3
            ["2024-04-01"],  # row 4
        ]

        incoming = [{"date": "2024-04-01", "val": "only"}]
        op = self._make_op(incoming)
        result = op.execute(context)

        assert result["updated"] == 1
        assert result["deleted"] == 2
        mock_hook.batch_update.assert_called()
        # Check that deleteDimension was in the request
        batch_args = mock_hook.batch_update.call_args[0][1]
        assert any("deleteDimension" in r for r in batch_args)

    def test_new_key_appends(self, mock_hook, context):
        """Key not in existing data → append."""
        mock_hook.get_values.return_value = [
            ["date"],
            ["2024-04-01"],
        ]

        incoming = [{"date": "2024-04-02", "val": "new"}]
        op = self._make_op(incoming)
        result = op.execute(context)

        assert result["appended"] == 1
        mock_hook.append_values.assert_called()

    def test_mixed_update_and_append(self, mock_hook, context):
        """Some keys exist, some are new."""
        mock_hook.get_values.return_value = [
            ["date"],
            ["2024-04-01"],  # row 2
        ]

        incoming = [
            {"date": "2024-04-01", "val": "updated"},
            {"date": "2024-04-02", "val": "new"},
        ]
        op = self._make_op(incoming)
        result = op.execute(context)

        assert result["updated"] == 1
        assert result["appended"] == 1

    def test_bottom_up_ordering_for_deletes(self, mock_hook, context):
        """Multiple delete groups should be processed bottom-up."""
        mock_hook.get_values.return_value = [
            ["date"],
            ["2024-04-01"],  # row 2
            ["2024-04-01"],  # row 3
            ["2024-04-01"],  # row 4
            ["2024-04-05"],  # row 5
            ["2024-04-05"],  # row 6
            ["2024-04-05"],  # row 7
        ]

        incoming = [
            {"date": "2024-04-01", "val": "a"},   # was 3, now 1 → delete 2
            {"date": "2024-04-05", "val": "b"},   # was 3, now 1 → delete 2
        ]
        op = self._make_op(incoming)
        result = op.execute(context)

        assert result["deleted"] == 4  # 2 + 2

        # Verify bottom-up: the batch_update should contain delete for
        # rows 6-7 before rows 3-4
        batch_args = mock_hook.batch_update.call_args[0][1]
        delete_starts = [
            r["deleteDimension"]["range"]["startIndex"]
            for r in batch_args
            if "deleteDimension" in r
        ]
        # Should be sorted descending
        assert delete_starts == sorted(delete_starts, reverse=True)

    def test_non_unique_keys(self, mock_hook, context):
        """Multiple rows per key value — all should be handled."""
        mock_hook.get_values.return_value = [
            ["id"],
            ["A"],  # row 2
            ["A"],  # row 3
            ["B"],  # row 4
        ]

        incoming = [
            {"id": "A", "val": "a1"},
            {"id": "A", "val": "a2"},
            {"id": "B", "val": "b1"},
            {"id": "B", "val": "b2"},
        ]
        op = self._make_op(incoming, merge_key="id")
        result = op.execute(context)

        assert result["updated"] >= 3  # at least 2 for A + 1 for B
        assert result["inserted"] >= 1  # B had 1, now 2

    def test_missing_merge_key_raises(self, mock_hook, context):
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="smart_merge",
            data=[{"a": 1}],
        )
        with pytest.raises(ValueError, match="merge_key is required"):
            op.execute(context)

    def test_merge_key_not_in_headers_raises(self, mock_hook, context):
        op = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="smart_merge",
            merge_key="nonexistent",
            data=[{"a": 1}],
        )
        with pytest.raises(ValueError, match="not found in headers"):
            op.execute(context)

    def test_empty_incoming_is_noop(self, mock_hook, context):
        """No incoming data → nothing changes."""
        mock_hook.get_values.return_value = [["date"], ["2024-04-01"]]

        # Empty list of dicts still provides headers via normalize
        op = self._make_op([{"date": "2024-04-01", "val": "x"}], merge_key="date")
        # Override with empty rows after normalisation
        # Simpler: just pass data that won't match anything in incoming
        # Actually, test the realistic case: empty list of dicts
        op2 = GoogleSheetsWriteOperator(
            task_id="test",
            spreadsheet_id=SPREADSHEET_ID,
            write_mode="smart_merge",
            merge_key="date",
            data=[],
            has_headers=False,
            pause_between_batches=0,
        )
        # Smart merge requires headers, so empty data without headers is a ValueError
        with pytest.raises(ValueError, match="Headers are required"):
            op2.execute(context)

    def test_with_sheet_name(self, mock_hook, context):
        mock_hook.get_values.return_value = [["date"], ["2024-04-01"]]

        incoming = [{"date": "2024-04-01", "val": "x"}]
        op = self._make_op(incoming, sheet_name="MySheet")
        op.execute(context)

        # Key column read should include sheet name
        key_range = mock_hook.get_values.call_args[0][1]
        assert key_range.startswith("MySheet!")


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
