"""Tests for header processing utilities."""

import pytest

from airflow_google_sheets.utils.headers import (
    process_headers,
    sanitize_header,
    _deduplicate,
    _builtin_transliterate,
)


class TestSanitizeHeader:
    def test_basic(self):
        assert sanitize_header("My Column") == "my_column"

    def test_hyphens(self):
        assert sanitize_header("some-column-name") == "some_column_name"

    def test_special_chars(self):
        assert sanitize_header("col@#$name!") == "colname"

    def test_multiple_spaces(self):
        assert sanitize_header("  a   b  ") == "a_b"

    def test_leading_trailing_underscores(self):
        assert sanitize_header("__test__") == "test"

    def test_cyrillic_preserved(self):
        result = sanitize_header("Моя Колонка")
        assert result == "моя_колонка"

    def test_empty_string(self):
        assert sanitize_header("") == ""


class TestDeduplicate:
    def test_no_duplicates(self):
        assert _deduplicate(["a", "b", "c"]) == ["a", "b", "c"]

    def test_two_duplicates(self):
        assert _deduplicate(["a", "b", "a"]) == ["a", "b", "a_1"]

    def test_three_duplicates(self):
        assert _deduplicate(["x", "x", "x"]) == ["x", "x_1", "x_2"]

    def test_mixed_duplicates(self):
        assert _deduplicate(["a", "b", "a", "b", "a"]) == ["a", "b", "a_1", "b_1", "a_2"]

    def test_empty_list(self):
        assert _deduplicate([]) == []


class TestBuiltinTransliterate:
    def test_basic_cyrillic(self):
        result = _builtin_transliterate("привет")
        assert result == "privet"

    def test_mixed_text(self):
        result = _builtin_transliterate("Дата123")
        assert result.startswith("D") or result.startswith("d")
        assert "123" in result

    def test_latin_unchanged(self):
        assert _builtin_transliterate("hello") == "hello"


class TestProcessHeaders:
    def test_basic_no_options(self):
        result = process_headers(["Name", "Age", "City"])
        assert result == ["Name", "Age", "City"]

    def test_deduplicate(self):
        result = process_headers(["Col", "Col", "Col"])
        assert result == ["Col", "Col_1", "Col_2"]

    def test_transliterate(self):
        result = process_headers(["Дата", "Выручка"], transliterate=True)
        # Should contain Latin characters
        for h in result:
            assert all(ord(c) < 128 or c == "_" for c in h), f"Non-ASCII in: {h}"

    def test_transliterate_with_duplicates(self):
        result = process_headers(["Выручка", "Выручка"], transliterate=True)
        assert len(result) == 2
        assert result[0] != result[1]

    def test_normalize(self):
        result = process_headers(["My Column", "Another One"], normalize=True)
        assert result == ["my_column", "another_one"]

    def test_normalize_and_transliterate(self):
        result = process_headers(["Моя Колонка"], transliterate=True, normalize=True)
        assert result[0].isascii()
        assert "_" in result[0] or result[0].isalpha()

    def test_empty_headers_become_unnamed(self):
        result = process_headers(["", None, "  "])
        assert result == ["unnamed", "unnamed_1", "unnamed_2"]

    def test_none_header(self):
        result = process_headers([None, "ok"])
        assert result[0] == "unnamed"
        assert result[1] == "ok"

    def test_numeric_headers(self):
        result = process_headers([1, 2, 3])
        assert result == ["1", "2", "3"]
