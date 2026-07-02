"""Pure unit tests for the A1-notation value type and column math."""

import pytest

from airflow_provider_google_sheets.utils.a1 import (
    A1Range,
    column_letter_to_index,
    index_to_column_letter,
    parse_range_start,
)


class TestColumnLetterToIndex:
    def test_single_letter(self):
        assert column_letter_to_index("A") == 0
        assert column_letter_to_index("B") == 1
        assert column_letter_to_index("Z") == 25

    def test_double_letter(self):
        assert column_letter_to_index("AA") == 26
        assert column_letter_to_index("AB") == 27
        assert column_letter_to_index("AZ") == 51

    def test_lowercase(self):
        assert column_letter_to_index("a") == 0
        assert column_letter_to_index("aa") == 26


class TestIndexToColumnLetter:
    def test_basic_letters(self):
        assert index_to_column_letter(0) == "A"
        assert index_to_column_letter(25) == "Z"

    def test_boundaries_z_aa_aaa(self):
        # Z → AA → AAA boundary crossings.
        assert index_to_column_letter(25) == "Z"
        assert index_to_column_letter(26) == "AA"
        assert index_to_column_letter(51) == "AZ"
        assert index_to_column_letter(52) == "BA"
        assert index_to_column_letter(701) == "ZZ"
        assert index_to_column_letter(702) == "AAA"

    def test_round_trip(self):
        for idx in [0, 1, 25, 26, 27, 51, 52, 701, 702, 1000]:
            assert column_letter_to_index(index_to_column_letter(idx)) == idx


class TestParseRangeStartLenient:
    def test_simple_range(self):
        assert parse_range_start("B2:D10") == ("B", 2)

    def test_with_sheet_prefix(self):
        assert parse_range_start("Sheet1!C5:F") == ("C", 5)

    def test_single_cell(self):
        assert parse_range_start("A1") == ("A", 1)

    def test_no_row_number_defaults_to_1(self):
        assert parse_range_start("B:D") == ("B", 1)

    def test_no_column_defaults_to_A(self):
        assert parse_range_start("1:100") == ("A", 1)


class TestA1RangeParse:
    def test_simple_range(self):
        r = A1Range.parse("B2:D10")
        assert r == A1Range(None, "B", 2, "D", 10)

    def test_with_sheet_prefix(self):
        r = A1Range.parse("Sheet1!C5:F")
        assert r.sheet == "Sheet1"
        assert r.start_col == "C"
        assert r.start_row == 5
        assert r.end_col == "F"
        assert r.end_row is None

    def test_open_range_has_no_end_row(self):
        r = A1Range.parse("C3:F")
        assert r == A1Range(None, "C", 3, "F", None)

    def test_bare_cell_has_no_end_fields(self):
        r = A1Range.parse("C3")
        assert r == A1Range(None, "C", 3, None, None)

    def test_sheet_argument(self):
        r = A1Range.parse("C3:F", sheet="Data")
        assert r.sheet == "Data"

    def test_text_sheet_overrides_argument(self):
        r = A1Range.parse("Sheet1!C3", sheet="Other")
        assert r.sheet == "Sheet1"

    def test_lowercase_columns_uppercased(self):
        r = A1Range.parse("b2:d10")
        assert r.start_col == "B"
        assert r.end_col == "D"

    @pytest.mark.parametrize("bad", ["", "   ", "!!!", "###", "A:B:C", "3"])
    def test_garbage_raises(self, bad):
        with pytest.raises(ValueError):
            A1Range.parse(bad)


class TestA1RangeMethods:
    def test_width(self):
        assert A1Range.parse("C3:F10").width() == 4  # C, D, E, F
        assert A1Range.parse("C3:F").width() == 4  # open row, end col present
        assert A1Range.parse("A1:A1").width() == 1

    def test_width_bare_cell_is_none(self):
        assert A1Range.parse("C3").width() is None

    def test_col_at(self):
        r = A1Range.parse("C3")
        assert r.col_at(0) == "C"
        assert r.col_at(1) == "D"
        assert r.col_at(3) == "F"

    def test_col_at_boundary(self):
        # Z (index 25) + 1 → AA
        assert A1Range.parse("Z1").col_at(1) == "AA"

    def test_cell(self):
        assert A1Range.parse("Sheet1!C3:F").cell(7) == "Sheet1!C7"
        assert A1Range.parse("C3").cell(9) == "C9"


class TestA1RangeRender:
    @pytest.mark.parametrize(
        "text",
        ["B2:D10", "Sheet1!C5:F", "A1", "C3", "Sheet1!C3", "C3:F"],
    )
    def test_round_trip(self, text):
        assert A1Range.parse(text).render() == text

    def test_render_normalises_case(self):
        assert A1Range.parse("b2:d10").render() == "B2:D10"
