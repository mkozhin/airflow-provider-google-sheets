"""Tests for row filtering utilities."""

import pytest

from airflow_provider_google_sheets.utils.row_filter import (
    matches_any,
    normalize_conditions,
    validate_conditions,
)


class TestNormalizeConditions:
    def test_dict_to_list(self):
        result = normalize_conditions({"column": "status", "value": "done"})
        assert result == [{"column": "status", "value": "done"}]

    def test_list_passthrough(self):
        raw = [{"column": "a", "value": "1"}, {"column": "b", "value": "2"}]
        assert normalize_conditions(raw) == raw

    def test_none_returns_empty(self):
        assert normalize_conditions(None) == []


class TestValidateConditions:
    def test_valid_conditions(self):
        validate_conditions([{"column": "status", "value": "done", "op": "equals"}])

    def test_default_op_valid(self):
        validate_conditions([{"column": "status", "value": "done"}])

    def test_missing_column(self):
        with pytest.raises(ValueError, match="missing required key 'column'"):
            validate_conditions([{"value": "done"}])

    def test_unknown_op(self):
        with pytest.raises(ValueError, match="unknown op 'like'"):
            validate_conditions([{"column": "x", "op": "like", "value": "y"}])

    def test_value_required_for_equals(self):
        with pytest.raises(ValueError, match="requires a 'value' key"):
            validate_conditions([{"column": "x", "op": "equals"}])

    def test_value_not_required_for_empty(self):
        validate_conditions([{"column": "x", "op": "empty"}])

    def test_value_not_required_for_not_empty(self):
        validate_conditions([{"column": "x", "op": "not_empty"}])


class TestMatchesAny:
    def test_equals(self):
        row = {"status": "done"}
        assert matches_any(row, [{"column": "status", "value": "done"}]) is True

    def test_equals_no_match(self):
        row = {"status": "pending"}
        assert matches_any(row, [{"column": "status", "value": "done"}]) is False

    def test_not_equals(self):
        row = {"status": "pending"}
        assert matches_any(row, [{"column": "status", "value": "done", "op": "not_equals"}]) is True

    def test_contains(self):
        row = {"name": "hello world"}
        assert matches_any(row, [{"column": "name", "value": "world", "op": "contains"}]) is True

    def test_not_contains(self):
        row = {"name": "hello"}
        assert matches_any(row, [{"column": "name", "value": "world", "op": "not_contains"}]) is True

    def test_starts_with(self):
        row = {"name": "hello world"}
        assert matches_any(row, [{"column": "name", "value": "hello", "op": "starts_with"}]) is True

    def test_ends_with(self):
        row = {"name": "hello world"}
        assert matches_any(row, [{"column": "name", "value": "world", "op": "ends_with"}]) is True

    def test_empty(self):
        row = {"status": ""}
        assert matches_any(row, [{"column": "status", "op": "empty"}]) is True

    def test_empty_with_none(self):
        row = {"status": None}
        assert matches_any(row, [{"column": "status", "op": "empty"}]) is True

    def test_empty_missing_column(self):
        # Missing column is ignored — condition does not match
        row = {"other": "x"}
        assert matches_any(row, [{"column": "status", "op": "empty"}]) is False

    def test_not_empty(self):
        row = {"status": "active"}
        assert matches_any(row, [{"column": "status", "op": "not_empty"}]) is True

    def test_multiple_conditions_or_logic(self):
        row = {"status": "draft", "priority": "high"}
        conditions = [
            {"column": "status", "value": "done"},
            {"column": "priority", "value": "high"},
        ]
        assert matches_any(row, conditions) is True

    def test_multiple_conditions_none_match(self):
        row = {"status": "draft", "priority": "low"}
        conditions = [
            {"column": "status", "value": "done"},
            {"column": "priority", "value": "high"},
        ]
        assert matches_any(row, conditions) is False

    def test_numeric_value_converted_to_string(self):
        row = {"count": 42}
        assert matches_any(row, [{"column": "count", "value": "42"}]) is True
