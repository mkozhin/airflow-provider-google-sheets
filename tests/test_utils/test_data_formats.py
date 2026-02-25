"""Tests for data format conversion utilities."""

import json
import os
import tempfile

import pytest

from airflow_provider_google_sheets.exceptions import GoogleSheetsDataError
from airflow_provider_google_sheets.utils.data_formats import (
    dicts_to_rows,
    normalize_input_data,
    read_csv_file,
    read_json_file,
    rows_to_dicts,
    write_csv_file,
    write_json_file,
)


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield d


# ---------------------------------------------------------------------------
# CSV
# ---------------------------------------------------------------------------

class TestReadCsvFile:
    def test_with_headers(self, tmp_dir):
        path = os.path.join(tmp_dir, "data.csv")
        with open(path, "w") as f:
            f.write("name,age\nAlice,30\nBob,25\n")

        headers, rows = read_csv_file(path)
        assert headers == ["name", "age"]
        assert rows == [["Alice", "30"], ["Bob", "25"]]

    def test_without_headers(self, tmp_dir):
        path = os.path.join(tmp_dir, "data.csv")
        with open(path, "w") as f:
            f.write("Alice,30\nBob,25\n")

        headers, rows = read_csv_file(path, has_headers=False)
        assert headers is None
        assert rows == [["Alice", "30"], ["Bob", "25"]]

    def test_empty_file(self, tmp_dir):
        path = os.path.join(tmp_dir, "empty.csv")
        with open(path, "w") as f:
            pass
        headers, rows = read_csv_file(path)
        assert headers is None
        assert rows == []

    def test_cyrillic(self, tmp_dir):
        path = os.path.join(tmp_dir, "data.csv")
        with open(path, "w", encoding="utf-8") as f:
            f.write("Имя,Возраст\nАлиса,30\n")

        headers, rows = read_csv_file(path)
        assert headers == ["Имя", "Возраст"]
        assert rows == [["Алиса", "30"]]

    def test_custom_delimiter(self, tmp_dir):
        path = os.path.join(tmp_dir, "data.tsv")
        with open(path, "w") as f:
            f.write("a\tb\n1\t2\n")

        headers, rows = read_csv_file(path, delimiter="\t")
        assert headers == ["a", "b"]
        assert rows == [["1", "2"]]

    def test_missing_file_raises(self):
        with pytest.raises(GoogleSheetsDataError, match="Failed to read CSV"):
            read_csv_file("/nonexistent/path.csv")


class TestWriteCsvFile:
    def test_write_and_read_back(self, tmp_dir):
        path = os.path.join(tmp_dir, "out.csv")
        write_csv_file(path, ["x", "y"], [["1", "2"], ["3", "4"]])

        headers, rows = read_csv_file(path)
        assert headers == ["x", "y"]
        assert rows == [["1", "2"], ["3", "4"]]

    def test_write_without_headers(self, tmp_dir):
        path = os.path.join(tmp_dir, "out.csv")
        write_csv_file(path, None, [["1", "2"]])

        headers, rows = read_csv_file(path, has_headers=False)
        assert headers is None
        assert rows == [["1", "2"]]


# ---------------------------------------------------------------------------
# JSON
# ---------------------------------------------------------------------------

class TestReadJsonFile:
    def test_list_of_dicts(self, tmp_dir):
        path = os.path.join(tmp_dir, "data.json")
        with open(path, "w") as f:
            json.dump([{"a": 1, "b": 2}, {"a": 3, "b": 4}], f)

        headers, rows = read_json_file(path)
        assert headers == ["a", "b"]
        assert rows == [[1, 2], [3, 4]]

    def test_list_of_lists(self, tmp_dir):
        path = os.path.join(tmp_dir, "data.json")
        with open(path, "w") as f:
            json.dump([[1, 2], [3, 4]], f)

        headers, rows = read_json_file(path)
        assert headers is None
        assert rows == [[1, 2], [3, 4]]

    def test_empty_array(self, tmp_dir):
        path = os.path.join(tmp_dir, "data.json")
        with open(path, "w") as f:
            json.dump([], f)

        headers, rows = read_json_file(path)
        assert headers is None
        assert rows == []

    def test_non_array_raises(self, tmp_dir):
        path = os.path.join(tmp_dir, "data.json")
        with open(path, "w") as f:
            json.dump({"key": "value"}, f)

        with pytest.raises(GoogleSheetsDataError, match="Expected a JSON array"):
            read_json_file(path)


class TestWriteJsonFile:
    def test_write_with_headers(self, tmp_dir):
        path = os.path.join(tmp_dir, "out.json")
        write_json_file(path, ["a", "b"], [[1, 2], [3, 4]])

        with open(path) as f:
            data = json.load(f)
        assert data == [{"a": 1, "b": 2}, {"a": 3, "b": 4}]

    def test_write_without_headers(self, tmp_dir):
        path = os.path.join(tmp_dir, "out.json")
        write_json_file(path, None, [[1, 2], [3, 4]])

        with open(path) as f:
            data = json.load(f)
        assert data == [[1, 2], [3, 4]]


# ---------------------------------------------------------------------------
# Conversion helpers
# ---------------------------------------------------------------------------

class TestRowsToDicts:
    def test_basic(self):
        result = rows_to_dicts([["Alice", 30], ["Bob", 25]], ["name", "age"])
        assert result == [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]

    def test_short_row(self):
        result = rows_to_dicts([["Alice"]], ["name", "age"])
        assert result == [{"name": "Alice", "age": None}]

    def test_empty(self):
        assert rows_to_dicts([], ["a"]) == []


class TestDictsToRows:
    def test_basic(self):
        headers, rows = dicts_to_rows([{"a": 1, "b": 2}])
        assert headers == ["a", "b"]
        assert rows == [[1, 2]]

    def test_with_explicit_headers(self):
        headers, rows = dicts_to_rows([{"a": 1, "b": 2}], headers=["b", "a"])
        assert headers == ["b", "a"]
        assert rows == [[2, 1]]

    def test_missing_key(self):
        headers, rows = dicts_to_rows([{"a": 1}], headers=["a", "b"])
        assert rows == [[1, None]]

    def test_empty(self):
        headers, rows = dicts_to_rows([])
        assert headers == []
        assert rows == []

    def test_union_keys_from_all_dicts(self):
        """Headers should be union of all dicts' keys, not just first."""
        data = [{"a": 1}, {"a": 2, "b": 3}]
        headers, rows = dicts_to_rows(data)
        assert headers == ["a", "b"]
        assert rows == [[1, None], [2, 3]]

    def test_union_keys_preserves_order(self):
        """Keys appear in order of first occurrence across all dicts."""
        data = [{"b": 1}, {"a": 2}]
        headers, rows = dicts_to_rows(data)
        assert headers == ["b", "a"]
        assert rows == [[1, None], [None, 2]]

    def test_union_keys_all_same(self):
        """When all dicts have same keys, behavior unchanged."""
        data = [{"x": 1, "y": 2}, {"x": 3, "y": 4}]
        headers, rows = dicts_to_rows(data)
        assert headers == ["x", "y"]
        assert rows == [[1, 2], [3, 4]]

    def test_union_keys_with_empty_dicts(self):
        """Empty dicts in the list should not break anything."""
        data = [{"a": 1}, {}, {"b": 2}]
        headers, rows = dicts_to_rows(data)
        assert headers == ["a", "b"]
        assert rows == [[1, None], [None, None], [None, 2]]


# ---------------------------------------------------------------------------
# normalize_input_data
# ---------------------------------------------------------------------------

class TestNormalizeInputData:
    def test_list_of_dicts(self):
        data = [{"x": 1, "y": 2}]
        headers, rows = normalize_input_data(data)
        assert headers == ["x", "y"]
        assert rows == [[1, 2]]

    def test_list_of_lists_with_headers(self):
        data = [["a", "b"], [1, 2]]
        headers, rows = normalize_input_data(data, has_headers=True)
        assert headers == ["a", "b"]
        assert rows == [[1, 2]]

    def test_list_of_lists_without_headers(self):
        data = [[1, 2], [3, 4]]
        headers, rows = normalize_input_data(data, has_headers=False)
        assert headers is None
        assert rows == [[1, 2], [3, 4]]

    def test_csv_file_auto(self, tmp_dir):
        path = os.path.join(tmp_dir, "data.csv")
        with open(path, "w") as f:
            f.write("a,b\n1,2\n")

        headers, rows = normalize_input_data(path)
        assert headers == ["a", "b"]
        assert rows == [["1", "2"]]

    def test_json_file_auto(self, tmp_dir):
        path = os.path.join(tmp_dir, "data.json")
        with open(path, "w") as f:
            json.dump([{"a": 1}], f)

        headers, rows = normalize_input_data(path)
        assert headers == ["a"]
        assert rows == [[1]]

    def test_explicit_csv_source(self, tmp_dir):
        path = os.path.join(tmp_dir, "data.csv")
        with open(path, "w") as f:
            f.write("a\n1\n")

        headers, rows = normalize_input_data(None, source_type="csv", file_path=path)
        assert headers == ["a"]

    def test_explicit_json_source(self, tmp_dir):
        path = os.path.join(tmp_dir, "data.json")
        with open(path, "w") as f:
            json.dump([{"a": 1}], f)

        headers, rows = normalize_input_data(None, source_type="json", file_path=path)
        assert headers == ["a"]

    def test_explicit_dicts_source(self):
        headers, rows = normalize_input_data([{"a": 1}], source_type="dicts")
        assert headers == ["a"]

    def test_explicit_rows_source(self):
        headers, rows = normalize_input_data([["h"], [1]], source_type="rows", has_headers=True)
        assert headers == ["h"]
        assert rows == [[1]]

    def test_csv_without_file_path_raises(self):
        with pytest.raises(GoogleSheetsDataError, match="file_path is required"):
            normalize_input_data(None, source_type="csv")

    def test_unsupported_data_type(self):
        with pytest.raises(GoogleSheetsDataError, match="Cannot normalize"):
            normalize_input_data(12345)
