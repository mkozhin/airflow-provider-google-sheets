"""Utilities for reading/writing data in various formats (CSV, JSON, list of dicts)."""

from __future__ import annotations

import csv
import json
import logging
import os
from typing import Any

from airflow_google_sheets.exceptions import GoogleSheetsDataError

logger = logging.getLogger(__name__)


def read_csv_file(
    file_path: str,
    has_headers: bool = True,
    delimiter: str = ",",
) -> tuple[list[str] | None, list[list[str]]]:
    """Read a CSV file and return ``(headers, rows)``.

    Args:
        file_path: Path to the CSV file.
        has_headers: Whether the first row contains column names.
        delimiter: Column delimiter character.

    Returns:
        A tuple of ``(headers_or_None, rows)``.
    """
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f, delimiter=delimiter)
            rows = list(reader)
    except Exception as e:
        raise GoogleSheetsDataError(f"Failed to read CSV file '{file_path}': {e}") from e

    if not rows:
        return None, []

    if has_headers:
        return rows[0], rows[1:]

    return None, rows


def read_json_file(file_path: str) -> tuple[list[str] | None, list[list[Any]]]:
    """Read a JSON file (expected: list of dicts) and return ``(headers, rows)``."""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        raise GoogleSheetsDataError(f"Failed to read JSON file '{file_path}': {e}") from e

    if not isinstance(data, list):
        raise GoogleSheetsDataError(
            f"Expected a JSON array in '{file_path}', got {type(data).__name__}"
        )

    if not data:
        return None, []

    if isinstance(data[0], dict):
        return dicts_to_rows(data)

    # Assume list of lists
    return None, data


def write_csv_file(
    file_path: str,
    headers: list[str] | None,
    rows: list[list[Any]],
) -> None:
    """Write data to a CSV file."""
    try:
        with open(file_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            if headers:
                writer.writerow(headers)
            writer.writerows(rows)
    except Exception as e:
        raise GoogleSheetsDataError(f"Failed to write CSV file '{file_path}': {e}") from e


def write_json_file(
    file_path: str,
    headers: list[str] | None,
    rows: list[list[Any]],
) -> None:
    """Write data to a JSON file as a list of dicts (when headers are available)
    or a list of lists otherwise."""
    try:
        if headers:
            data = rows_to_dicts(rows, headers)
        else:
            data = rows

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=str)
    except Exception as e:
        raise GoogleSheetsDataError(f"Failed to write JSON file '{file_path}': {e}") from e


def rows_to_dicts(rows: list[list[Any]], headers: list[str]) -> list[dict[str, Any]]:
    """Convert a list of rows to a list of dicts using *headers* as keys."""
    result: list[dict[str, Any]] = []
    for row in rows:
        d: dict[str, Any] = {}
        for i, h in enumerate(headers):
            d[h] = row[i] if i < len(row) else None
        result.append(d)
    return result


def dicts_to_rows(
    data: list[dict[str, Any]],
    headers: list[str] | None = None,
) -> tuple[list[str], list[list[Any]]]:
    """Convert a list of dicts to ``(headers, rows)``.

    If *headers* is ``None``, keys are collected as a union of all dicts'
    keys, preserving the order of first appearance.
    """
    if not data:
        return headers or [], []

    if headers is None:
        seen: dict[str, bool] = {}
        for d in data:
            for key in d:
                if key not in seen:
                    seen[key] = True
        headers = list(seen.keys())

    rows: list[list[Any]] = []
    for d in data:
        rows.append([d.get(h) for h in headers])

    return headers, rows


def normalize_input_data(
    data: Any,
    source_type: str = "auto",
    file_path: str | None = None,
    has_headers: bool = True,
) -> tuple[list[str] | None, list[list[Any]]]:
    """Universal normalizer: accept various input formats and return ``(headers, rows)``.

    Supported *source_type* values:
    - ``"auto"`` — detect from *data* type or *file_path* extension
    - ``"csv"`` — read from CSV file at *file_path*
    - ``"json"`` — read from JSON file at *file_path*
    - ``"dicts"`` — *data* is ``list[dict]``
    - ``"rows"`` — *data* is ``list[list]``

    When *source_type* is ``"auto"``:
    - If *data* is a ``str``, it is treated as a file path.
    - If *data* is a ``list[dict]``, it is treated as dicts.
    - If *data* is a ``list[list]``, it is treated as rows.
    """
    # Explicit file-based sources
    if source_type == "csv":
        if file_path is None:
            raise GoogleSheetsDataError("file_path is required for source_type='csv'")
        return read_csv_file(file_path, has_headers=has_headers)

    if source_type == "json":
        if file_path is None:
            raise GoogleSheetsDataError("file_path is required for source_type='json'")
        return read_json_file(file_path)

    if source_type == "dicts":
        if not isinstance(data, list):
            raise GoogleSheetsDataError(f"Expected list[dict], got {type(data).__name__}")
        return dicts_to_rows(data)

    if source_type == "rows":
        if not isinstance(data, list):
            raise GoogleSheetsDataError(f"Expected list[list], got {type(data).__name__}")
        if has_headers and data:
            return data[0], data[1:]
        return None, data

    # Auto-detection
    if source_type == "auto":
        # String → file path
        if isinstance(data, str):
            path = file_path or data
            ext = os.path.splitext(path)[1].lower()
            if ext == ".json":
                return read_json_file(path)
            return read_csv_file(path, has_headers=has_headers)

        # list[dict]
        if isinstance(data, list) and data and isinstance(data[0], dict):
            return dicts_to_rows(data)

        # list[list]
        if isinstance(data, list):
            if has_headers and data:
                return data[0], data[1:]
            return None, data

    raise GoogleSheetsDataError(
        f"Cannot normalize data: unsupported source_type='{source_type}' "
        f"or data type '{type(data).__name__}'"
    )
