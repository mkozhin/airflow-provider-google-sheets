"""Tests for Merge Plan construction (existing-key index and delete/append planning)."""

from __future__ import annotations

from airflow_provider_google_sheets.utils.merge_planner import (
    MergePlan,
    _group_contiguous,
    build_existing_key_index,
    plan_merge_operations,
)


# ==================================================================
# _group_contiguous
# ==================================================================


class TestGroupContiguous:
    def test_group_contiguous_basic(self):
        assert _group_contiguous([3, 7, 8, 12]) == [(3, 3), (7, 8), (12, 12)]

    def test_group_contiguous_all_sequential(self):
        assert _group_contiguous([5, 6, 7]) == [(5, 7)]

    def test_group_contiguous_single(self):
        assert _group_contiguous([10]) == [(10, 10)]

    def test_group_contiguous_empty(self):
        assert _group_contiguous([]) == []


# ==================================================================
# build_existing_key_index
# ==================================================================


class TestBuildExistingKeyIndex:
    def test_has_headers_true_skips_first_row(self):
        existing_keys_raw = [["id"], ["A"], ["B"], ["A"]]
        index = build_existing_key_index(
            existing_keys_raw,
            has_headers=True,
            table_start_row=1,
            key_schema=None,
            extended=True,
        )
        # data rows start at row 2 (header at row 1)
        assert index == {"A": [2, 4], "B": [3]}

    def test_has_headers_false_includes_first_row(self):
        existing_keys_raw = [["A"], ["B"], ["A"]]
        index = build_existing_key_index(
            existing_keys_raw,
            has_headers=False,
            table_start_row=1,
            key_schema=None,
            extended=True,
        )
        # no header row to skip; data starts at table_start_row itself
        assert index == {"A": [1, 3], "B": [2]}

    def test_key_schema_none_uses_raw_values_as_is(self):
        existing_keys_raw = [["id"], ["  A  "], ["B"]]
        index = build_existing_key_index(
            existing_keys_raw,
            has_headers=True,
            table_start_row=1,
            key_schema=None,
            extended=True,
        )
        # raw values used as-is (no trimming/normalization without a schema)
        assert index == {"  A  ": [2], "B": [3]}

    def test_key_schema_date_normalizes_serial_numbers(self):
        # 46023 is the Google Sheets serial number for 2026-01-01
        # (epoch 1899-12-30).
        existing_keys_raw = [["id"], ["46023"], ["2026-01-02"]]
        key_schema = {"type": "date", "format": "%Y-%m-%d"}
        index = build_existing_key_index(
            existing_keys_raw,
            has_headers=True,
            table_start_row=1,
            key_schema=key_schema,
            extended=True,
        )
        assert index == {"2026-01-01": [2], "2026-01-02": [3]}

    def test_empty_rows_are_skipped(self):
        existing_keys_raw = [["id"], ["A"], [], ["B"]]
        index = build_existing_key_index(
            existing_keys_raw,
            has_headers=True,
            table_start_row=1,
            key_schema=None,
            extended=True,
        )
        assert index == {"A": [2], "B": [4]}

    def test_table_start_row_offset(self):
        # Table starts at row 5 (e.g. table_start="A5"); header at row 5,
        # data rows start at row 6.
        existing_keys_raw = [["id"], ["A"], ["B"]]
        index = build_existing_key_index(
            existing_keys_raw,
            has_headers=True,
            table_start_row=5,
            key_schema=None,
            extended=True,
        )
        assert index == {"A": [6], "B": [7]}


# ==================================================================
# plan_merge_operations
# ==================================================================


class TestPlanMergeOperations:
    def test_key_in_both_existing_and_incoming_deletes_and_appends(self):
        existing_index = {"A": [2]}
        incoming_groups = {"A": [["A", "new-val"]]}
        plan = plan_merge_operations(existing_index, incoming_groups)
        assert isinstance(plan, MergePlan)
        assert plan.delete_ops == [{"row_num": 2, "start_index": 1, "end_index": 2}]
        assert plan.append_rows == [["A", "new-val"]]

    def test_key_only_in_incoming_appends_without_delete(self):
        existing_index: dict[str, list[int]] = {}
        incoming_groups = {"NEW": [["NEW", "val"]]}
        plan = plan_merge_operations(existing_index, incoming_groups)
        assert plan.delete_ops == []
        assert plan.append_rows == [["NEW", "val"]]

    def test_key_only_in_existing_left_untouched(self):
        existing_index = {"OLD": [2]}
        incoming_groups: dict[str, list[list]] = {}
        plan = plan_merge_operations(existing_index, incoming_groups)
        assert plan.delete_ops == []
        assert plan.append_rows == []

    def test_non_contiguous_existing_rows_produce_separate_segments(self):
        existing_index = {"A": [2, 4, 6]}
        incoming_groups = {"A": [["A", "new-val"]]}
        plan = plan_merge_operations(existing_index, incoming_groups)
        # three non-contiguous rows -> three separate delete segments
        assert plan.delete_ops == [
            {"row_num": 6, "start_index": 5, "end_index": 6},
            {"row_num": 4, "start_index": 3, "end_index": 4},
            {"row_num": 2, "start_index": 1, "end_index": 2},
        ]
        assert plan.append_rows == [["A", "new-val"]]

    def test_contiguous_existing_rows_merge_into_one_segment(self):
        existing_index = {"A": [4, 5, 6]}
        incoming_groups = {"A": [["A", "new-val"]]}
        plan = plan_merge_operations(existing_index, incoming_groups)
        assert plan.delete_ops == [{"row_num": 4, "start_index": 3, "end_index": 6}]

    def test_delete_ops_sorted_by_descending_row_num(self):
        existing_index = {"A": [2], "B": [10], "C": [5]}
        incoming_groups = {
            "A": [["A", "1"]],
            "B": [["B", "2"]],
            "C": [["C", "3"]],
        }
        plan = plan_merge_operations(existing_index, incoming_groups)
        row_nums = [op["row_num"] for op in plan.delete_ops]
        assert row_nums == sorted(row_nums, reverse=True)
        assert row_nums == [10, 5, 2]
