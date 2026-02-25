"""Tests for schema validation and type conversion."""

from datetime import date, datetime

import pytest

from airflow_provider_google_sheets.exceptions import GoogleSheetsDataError, GoogleSheetsSchemaError
from airflow_provider_google_sheets.utils.schema import (
    apply_schema_to_row,
    apply_schema_to_value,
    format_row_for_write,
    format_value_for_write,
    validate_schema,
)


class TestValidateSchema:
    def test_valid_schema(self):
        headers = ["date", "revenue", "comment"]
        schema = {
            "date": {"type": "date", "required": True},
            "revenue": {"type": "float", "required": True},
        }
        validate_schema(headers, schema)  # should not raise

    def test_missing_required_column(self):
        headers = ["revenue"]
        schema = {"date": {"type": "date", "required": True}}
        with pytest.raises(GoogleSheetsSchemaError, match="Required column 'date'"):
            validate_schema(headers, schema)

    def test_optional_column_missing_is_ok(self):
        headers = ["date"]
        schema = {"comment": {"type": "str", "required": False}}
        validate_schema(headers, schema)  # should not raise

    def test_unknown_type(self):
        headers = ["x"]
        schema = {"x": {"type": "uuid"}}
        with pytest.raises(GoogleSheetsSchemaError, match="Unknown type 'uuid'"):
            validate_schema(headers, schema)


class TestApplySchemaToValue:
    def test_str(self):
        assert apply_schema_to_value(42, {"type": "str"}) == "42"

    def test_int(self):
        assert apply_schema_to_value("42", {"type": "int"}) == 42

    def test_int_from_float_string(self):
        assert apply_schema_to_value("42.9", {"type": "int"}) == 42

    def test_float(self):
        assert apply_schema_to_value("3.14", {"type": "float"}) == pytest.approx(3.14)

    def test_date_iso(self):
        result = apply_schema_to_value("2024-04-01", {"type": "date"})
        assert result == date(2024, 4, 1)

    def test_date_with_format(self):
        result = apply_schema_to_value("01/04/2024", {"type": "date", "format": "%d/%m/%Y"})
        assert result == date(2024, 4, 1)

    def test_date_us_format(self):
        result = apply_schema_to_value("04/01/2024", {"type": "date", "format": "%m/%d/%Y"})
        assert result == date(2024, 4, 1)

    def test_datetime_iso(self):
        result = apply_schema_to_value("2024-04-01 12:30:00", {"type": "datetime"})
        assert result == datetime(2024, 4, 1, 12, 30)

    def test_datetime_with_format(self):
        result = apply_schema_to_value(
            "01.04.2024 12:30",
            {"type": "datetime", "format": "%d.%m.%Y %H:%M"},
        )
        assert result == datetime(2024, 4, 1, 12, 30)

    def test_bool_true(self):
        assert apply_schema_to_value("true", {"type": "bool"}) is True
        assert apply_schema_to_value("1", {"type": "bool"}) is True
        assert apply_schema_to_value("yes", {"type": "bool"}) is True
        assert apply_schema_to_value("да", {"type": "bool"}) is True
        assert apply_schema_to_value(True, {"type": "bool"}) is True
        assert apply_schema_to_value(1, {"type": "bool"}) is True

    def test_bool_false(self):
        assert apply_schema_to_value("false", {"type": "bool"}) is False
        assert apply_schema_to_value("0", {"type": "bool"}) is False
        assert apply_schema_to_value("no", {"type": "bool"}) is False
        assert apply_schema_to_value("нет", {"type": "bool"}) is False

    def test_bool_invalid(self):
        with pytest.raises(GoogleSheetsDataError):
            apply_schema_to_value("maybe", {"type": "bool"})

    def test_none_passthrough(self):
        assert apply_schema_to_value(None, {"type": "int"}) is None

    def test_empty_string_passthrough(self):
        assert apply_schema_to_value("", {"type": "float"}) == ""
        assert apply_schema_to_value("  ", {"type": "date"}) == "  "

    def test_invalid_int(self):
        with pytest.raises(GoogleSheetsDataError, match="Cannot convert"):
            apply_schema_to_value("abc", {"type": "int"})

    def test_invalid_date(self):
        with pytest.raises(GoogleSheetsDataError):
            apply_schema_to_value("not-a-date", {"type": "date"})

    def test_date_object_passthrough(self):
        d = date(2024, 1, 1)
        assert apply_schema_to_value(d, {"type": "date"}) == d

    def test_datetime_to_date(self):
        dt = datetime(2024, 1, 1, 12, 0)
        result = apply_schema_to_value(dt, {"type": "date"})
        assert result == date(2024, 1, 1)


class TestApplySchemaToRow:
    def test_full_row(self):
        headers = ["date", "amount", "note"]
        schema = {
            "date": {"type": "date", "format": "%d/%m/%Y"},
            "amount": {"type": "float"},
        }
        row = ["01/04/2024", "123.45", "some text"]
        result = apply_schema_to_row(row, headers, schema)
        assert result == [date(2024, 4, 1), 123.45, "some text"]

    def test_column_not_in_schema_unchanged(self):
        headers = ["a", "b"]
        schema = {"a": {"type": "int"}}
        row = ["42", "hello"]
        result = apply_schema_to_row(row, headers, schema)
        assert result == [42, "hello"]

    def test_error_includes_column_name(self):
        headers = ["val"]
        schema = {"val": {"type": "int"}}
        with pytest.raises(GoogleSheetsDataError, match="Column 'val'"):
            apply_schema_to_row(["abc"], headers, schema)

    def test_short_row(self):
        headers = ["a", "b", "c"]
        schema = {"a": {"type": "int"}}
        row = ["1"]
        result = apply_schema_to_row(row, headers, schema)
        assert result == [1]


class TestFormatValueForWrite:
    def test_date_with_format(self):
        result = format_value_for_write(date(2024, 4, 1), {"type": "date", "format": "%d/%m/%Y"})
        assert result == "01/04/2024"

    def test_datetime_with_format(self):
        result = format_value_for_write(
            datetime(2024, 4, 1, 12, 30),
            {"type": "datetime", "format": "%d.%m.%Y %H:%M"},
        )
        assert result == "01.04.2024 12:30"

    def test_bool_true(self):
        assert format_value_for_write(True, {"type": "bool"}) == "TRUE"

    def test_bool_false(self):
        assert format_value_for_write(False, {"type": "bool"}) == "FALSE"

    def test_none(self):
        assert format_value_for_write(None, {"type": "str"}) == ""

    def test_int(self):
        assert format_value_for_write(42, {"type": "int"}) == "42"

    def test_date_no_format(self):
        result = format_value_for_write(date(2024, 4, 1), {"type": "date"})
        assert result == "2024-04-01"


class TestFormatRowForWrite:
    def test_full_row(self):
        headers = ["date", "amount", "note"]
        schema = {
            "date": {"type": "date", "format": "%d/%m/%Y"},
            "amount": {"type": "float"},
        }
        row = [date(2024, 4, 1), 123.45, "text"]
        result = format_row_for_write(row, headers, schema)
        assert result == ["01/04/2024", "123.45", "text"]

    def test_none_values(self):
        headers = ["a"]
        schema = {"a": {"type": "str"}}
        result = format_row_for_write([None], headers, schema)
        assert result == [""]
