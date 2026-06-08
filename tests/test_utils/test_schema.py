"""Tests for schema validation and type conversion."""

import logging
from datetime import date, datetime

import pytest

from airflow_provider_google_sheets.exceptions import GoogleSheetsDataError, GoogleSheetsSchemaError
from airflow_provider_google_sheets.utils.schema import (
    apply_schema_to_row,
    apply_schema_to_value,
    format_row_for_write,
    format_value_for_write,
    normalize_merge_key,
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

    def test_date_input_format_differs_from_format(self):
        result = apply_schema_to_value(
            "2026-03-01",
            {"type": "date", "input_format": "%Y-%m-%d", "format": "%d.%m.%Y"},
        )
        assert result == date(2026, 3, 1)

    def test_datetime_input_format(self):
        result = apply_schema_to_value(
            "2026-03-01 12:00",
            {"type": "datetime", "input_format": "%Y-%m-%d %H:%M", "format": "%d.%m.%Y %H:%M"},
        )
        assert result == datetime(2026, 3, 1, 12, 0)

    def test_input_format_not_affects_format_value_for_write(self):
        result = format_value_for_write(
            date(2026, 3, 1),
            {"type": "date", "input_format": "%Y-%m-%d", "format": "%d.%m.%Y"},
        )
        assert result == "01.03.2026"


class TestLenientNumericParsing:
    """Tests for numeric columns with ``default`` field (lenient mode)."""

    # --- default=None: non-numeric → None ---

    def test_float_na_returns_none(self):
        assert apply_schema_to_value("n/a", {"type": "float", "default": None}) is None

    def test_float_dash_returns_none(self):
        assert apply_schema_to_value("-", {"type": "float", "default": None}) is None

    def test_float_empty_returns_none(self):
        assert apply_schema_to_value("", {"type": "float", "default": None}) is None

    def test_float_whitespace_returns_none(self):
        assert apply_schema_to_value("  ", {"type": "float", "default": None}) is None

    def test_float_none_returns_none(self):
        assert apply_schema_to_value(None, {"type": "float", "default": None}) is None

    def test_int_na_returns_none(self):
        assert apply_schema_to_value("n/a", {"type": "int", "default": None}) is None

    # --- default=0: non-numeric → 0 ---

    def test_float_na_returns_zero(self):
        assert apply_schema_to_value("n/a", {"type": "float", "default": 0.0}) == 0.0

    def test_int_dash_returns_zero(self):
        assert apply_schema_to_value("-", {"type": "int", "default": 0}) == 0

    # --- comma as decimal separator ---

    def test_float_comma_decimal(self):
        assert apply_schema_to_value("1,2", {"type": "float", "default": None}) == pytest.approx(1.2)

    def test_int_comma_decimal(self):
        assert apply_schema_to_value("42,0", {"type": "int", "default": None}) == 42

    # --- prefix/suffix stripping ---

    def test_float_suffix_rub(self):
        assert apply_schema_to_value("1000.4 р.", {"type": "float", "default": None}) == pytest.approx(1000.4)

    def test_float_suffix_percent(self):
        assert apply_schema_to_value("10.2%", {"type": "float", "default": None}) == pytest.approx(10.2)

    def test_float_prefix_dollar(self):
        assert apply_schema_to_value("$ 55", {"type": "float", "default": None}) == pytest.approx(55.0)

    def test_int_suffix_stripped(self):
        assert apply_schema_to_value("100 шт.", {"type": "int", "default": None}) == 100

    # --- normal values still work in lenient mode ---

    def test_float_normal(self):
        assert apply_schema_to_value("3.14", {"type": "float", "default": None}) == pytest.approx(3.14)

    def test_int_normal(self):
        assert apply_schema_to_value("42", {"type": "int", "default": None}) == 42

    def test_float_from_python_float(self):
        assert apply_schema_to_value(3.14, {"type": "float", "default": None}) == pytest.approx(3.14)

    def test_int_from_python_int(self):
        assert apply_schema_to_value(42, {"type": "int", "default": None}) == 42

    # --- strict mode unchanged (no default) ---

    def test_strict_float_na_raises(self):
        with pytest.raises(GoogleSheetsDataError):
            apply_schema_to_value("n/a", {"type": "float"})

    def test_strict_float_comma_raises(self):
        with pytest.raises(GoogleSheetsDataError):
            apply_schema_to_value("1,2", {"type": "float"})

    # --- space as thousands separator ---

    def test_float_space_thousands_separator(self):
        assert apply_schema_to_value("р.250 000", {"type": "float", "default": 0}) == pytest.approx(250000.0)

    def test_float_space_thousands_separator_small(self):
        assert apply_schema_to_value("р.2 722", {"type": "float", "default": 0}) == pytest.approx(2722.0)

    def test_int_space_thousands_separator(self):
        assert apply_schema_to_value("1 946", {"type": "int", "default": 0}) == 1946

    def test_int_multiple_space_separators(self):
        assert apply_schema_to_value("1 234 567", {"type": "int", "default": 0}) == 1234567

    def test_float_nbsp_thousands_separator(self):
        # U+00A0 non-breaking space
        assert apply_schema_to_value("1\xa0000", {"type": "float", "default": 0}) == pytest.approx(1000.0)

    def test_float_narrow_nbsp_thousands_separator(self):
        # U+202F narrow no-break space (Russian locale, Excel)
        assert apply_schema_to_value("1\u202F000", {"type": "float", "default": 0}) == pytest.approx(1000.0)

    def test_float_thin_space_thousands_separator(self):
        # U+2009 thin space
        assert apply_schema_to_value("1\u2009000", {"type": "float", "default": 0}) == pytest.approx(1000.0)


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


class TestNormalizeMergeKey:
    """Tests for normalize_merge_key (serial date conversion and fallback strategies)."""

    # ------------------------------------------------------------------
    # Serial date epoch verification (via normalize_merge_key)
    # ------------------------------------------------------------------

    def test_serial_to_date_epoch_verification(self):
        """Serial 46023 must map to 2026-01-01 (epoch = 1899-12-30)."""
        schema = {"type": "date", "format": "%Y-%m-%d"}
        assert normalize_merge_key("46023", schema) == "2026-01-01"

    def test_serial_to_date_known_value(self):
        """Serial 1 maps to 1899-12-31 (one day after epoch 1899-12-30)."""
        schema = {"type": "date", "format": "%Y-%m-%d"}
        assert normalize_merge_key("1", schema) == "1899-12-31"

    # ------------------------------------------------------------------
    # normalize_merge_key — no schema
    # ------------------------------------------------------------------

    def test_no_schema_returns_raw(self):
        assert normalize_merge_key("anything", None) == "anything"

    def test_no_schema_returns_raw_extended_false(self):
        assert normalize_merge_key("anything", None, extended=False) == "anything"

    # ------------------------------------------------------------------
    # Step 1: key already in format
    # ------------------------------------------------------------------

    def test_key_already_in_format_no_change(self):
        schema = {"type": "date", "format": "%Y-%m-%d"}
        assert normalize_merge_key("2026-01-01", schema) == "2026-01-01"

    def test_key_in_format_extended_false(self):
        schema = {"type": "date", "format": "%Y-%m-%d"}
        assert normalize_merge_key("2026-01-01", schema, extended=False) == "2026-01-01"

    # ------------------------------------------------------------------
    # Step 2: key in input_format → converted to format
    # ------------------------------------------------------------------

    def test_extended_true_key_in_input_format_converted(self):
        """Key stored in regional format (dd.mm.yyyy) → normalised to ISO."""
        schema = {"type": "date", "format": "%Y-%m-%d", "input_format": "%d.%m.%Y"}
        assert normalize_merge_key("01.01.2026", schema) == "2026-01-01"

    def test_extended_false_key_in_input_format_not_converted(self):
        """With extended=False input_format fallback is skipped — raw returned."""
        schema = {"type": "date", "format": "%Y-%m-%d", "input_format": "%d.%m.%Y"}
        result = normalize_merge_key("01.01.2026", schema, extended=False)
        assert result == "01.01.2026"

    def test_extended_false_serial_date_not_converted(self):
        """With extended=False serial date fallback (step 3) is skipped — raw returned."""
        schema = {"type": "date", "format": "%Y-%m-%d"}
        result = normalize_merge_key("46023", schema, extended=False)
        assert result == "46023"

    # ------------------------------------------------------------------
    # Step 3: serial date number
    # ------------------------------------------------------------------

    def test_extended_true_serial_date_converted(self):
        """Serial date string "46023" for a date column → "2026-01-01"."""
        schema = {"type": "date", "format": "%Y-%m-%d"}
        assert normalize_merge_key("46023", schema) == "2026-01-01"

    def test_extended_true_serial_date_non_iso_format(self):
        """Serial → regional format."""
        schema = {"type": "date", "format": "%d.%m.%Y"}
        assert normalize_merge_key("46023", schema) == "01.01.2026"

    def test_extended_true_serial_datetime_converted(self):
        """Serial "46023" for datetime column → midnight datetime."""
        schema = {"type": "datetime", "format": "%Y-%m-%d %H:%M:%S"}
        assert normalize_merge_key("46023", schema) == "2026-01-01 00:00:00"

    def test_serial_date_not_triggered_for_str_type(self):
        """For type=str, numeric value must NOT be treated as serial date."""
        schema = {"type": "str"}
        # "46023" is a valid str, so step 1 returns it unchanged
        assert normalize_merge_key("46023", schema) == "46023"

    def test_serial_date_not_triggered_for_int_type(self):
        """For type=int, numeric ID must NOT be treated as serial date."""
        schema = {"type": "int"}
        assert normalize_merge_key("46023", schema) == "46023"

    # ------------------------------------------------------------------
    # Step 4: nothing works → raw + warning
    # ------------------------------------------------------------------

    def test_unparseable_key_returns_raw_with_warning(self, caplog):
        """Key that matches no parse strategy returns raw and emits a warning."""
        schema = {"type": "date", "format": "%Y-%m-%d"}
        with caplog.at_level(logging.WARNING, logger="airflow_provider_google_sheets.utils.schema"):
            result = normalize_merge_key("not-a-date-at-all!", schema)
        assert result == "not-a-date-at-all!"
        assert caplog.records, "Expected a warning to be logged"

    def test_extended_false_unparseable_returns_raw_with_warning(self, caplog):
        """With extended=False, unparseable key also returns raw + warning."""
        schema = {"type": "date", "format": "%Y-%m-%d"}
        with caplog.at_level(logging.WARNING, logger="airflow_provider_google_sheets.utils.schema"):
            result = normalize_merge_key("not-a-date!", schema, extended=False)
        assert result == "not-a-date!"
        assert caplog.records, "Expected a warning to be logged"


class TestStripStrings:
    def test_leading_space_stripped(self):
        result = apply_schema_to_value(" bonus", {"type": "str"}, strip_strings=True)
        assert result == "bonus"

    def test_trailing_space_stripped(self):
        result = apply_schema_to_value("bonus ", {"type": "str"}, strip_strings=True)
        assert result == "bonus"

    def test_both_sides_stripped(self):
        result = apply_schema_to_value("  Дмитров Дом  ", {"type": "str"}, strip_strings=True)
        assert result == "Дмитров Дом"

    def test_spaces_preserved_when_false(self):
        result = apply_schema_to_value(" bonus ", {"type": "str"}, strip_strings=False)
        assert result == " bonus "

    def test_default_preserves_spaces(self):
        result = apply_schema_to_value(" bonus ", {"type": "str"})
        assert result == " bonus "

    def test_clean_value_unchanged(self):
        result = apply_schema_to_value("bonus", {"type": "str"}, strip_strings=True)
        assert result == "bonus"

    def test_numeric_type_unaffected(self):
        result = apply_schema_to_value(42, {"type": "int", "default": 0}, strip_strings=True)
        assert result == 42

    def test_apply_schema_to_row_str_trimmed_numeric_untouched(self):
        headers = ["bonus", "amount"]
        schema = {"bonus": {"type": "str"}, "amount": {"type": "int", "default": 0}}
        row = [" bonus ", 100]
        result = apply_schema_to_row(row, headers, schema, strip_strings=True)
        assert result == ["bonus", 100]
