"""Direct unit tests for the WrittenExtent value type."""

from __future__ import annotations

from airflow_provider_google_sheets.utils.write_extent import WrittenExtent


class TestWrittenExtent:
    def test_empty_sheet_header_written_this_run(self):
        # Empty sheet at A1: header written this run + 5 data rows.
        # total_rows counts the header row too (1 + 5).
        extent = WrittenExtent(
            start_row=1, header_present=True, total_rows=6, width=3
        )
        assert extent.sort_start == 1  # skip the header row
        assert extent.sort_end == 6
        assert extent.num_columns == 3

    def test_non_empty_without_header(self):
        # has_headers=False: no physical header row, sort covers every row.
        extent = WrittenExtent(
            start_row=1, header_present=False, total_rows=4, width=2
        )
        assert extent.sort_start == 0
        assert extent.sort_end == 4
        assert extent.num_columns == 2

    def test_pre_existing_header_not_rewritten_this_run(self):
        # The case that distinguishes the total_rows semantics: the sheet
        # already had a header + 3 data rows, and 2 new rows are appended this
        # run without rewriting the header. total_rows includes the pre-existing
        # header (1 + 3 + 2 = 6); without counting it, sort_end would be off by
        # one (5 instead of 6) and the last appended row would be left unsorted.
        extent = WrittenExtent(
            start_row=1, header_present=True, total_rows=6, width=4
        )
        assert extent.sort_start == 1
        assert extent.sort_end == 6

    def test_e0_based_construction_positional_append(self):
        # Positional append formula: total_rows =
        #   max(0, E0 - (start_row - 1)) + (1 if write_header_flag else 0) + N.
        # Non-empty window: start_row=1, E0=4 (4 existing rows incl. header),
        # header not written this run, N=3 new rows.
        start_row = 1
        e0 = 4
        write_header_flag = False
        n = 3
        existing = max(0, e0 - (start_row - 1))
        total_rows = existing + (1 if write_header_flag else 0) + n
        extent = WrittenExtent(
            start_row=start_row,
            header_present=(write_header_flag or e0 >= start_row),
            total_rows=total_rows,
            width=3,
        )
        # end_row should equal E0 + N when no header is written this run.
        assert extent.sort_end == e0 + n
        assert extent.sort_start == start_row  # skip the pre-existing header

    def test_e0_based_construction_empty_window_with_header(self):
        # Empty window (E0 = start_row - 1), header written this run, N rows.
        start_row = 3
        e0 = start_row - 1
        write_header_flag = True
        n = 2
        existing = max(0, e0 - (start_row - 1))  # 0
        total_rows = existing + (1 if write_header_flag else 0) + n
        extent = WrittenExtent(
            start_row=start_row,
            header_present=write_header_flag,
            total_rows=total_rows,
            width=2,
        )
        # start_row-1=2 as 0-based top; header at index 2, data 3..4 exclusive 5
        assert extent.sort_start == 3
        assert extent.sort_end == 5

    def test_empty_data_nothing_to_sort(self):
        # Header-only table: sort_start >= sort_end → _execute_sort no-ops.
        extent = WrittenExtent(
            start_row=1, header_present=True, total_rows=1, width=1
        )
        assert extent.sort_start >= extent.sort_end

    def test_empty_data_no_header_nothing_to_sort(self):
        # Totally empty extent: no rows at all.
        extent = WrittenExtent(
            start_row=1, header_present=False, total_rows=0, width=0
        )
        assert extent.sort_start >= extent.sort_end
