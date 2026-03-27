"""Schema validation and type conversion for spreadsheet data."""

from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any

from airflow_provider_google_sheets.exceptions import GoogleSheetsDataError, GoogleSheetsSchemaError

# Supported type names
_SUPPORTED_TYPES = {"str", "int", "float", "date", "datetime", "bool"}
_NUMERIC_TYPES = {"int", "float"}
_NUMERIC_RE = re.compile(r"-?\d+\.?\d*")


def _clean_numeric_string(value: str) -> str | None:
    """Extract a numeric substring from *value*.

    Handles comma as decimal separator (``"1,2"`` → ``"1.2"``) and strips
    non-numeric prefixes/suffixes (``"1000.4 р."`` → ``"1000.4"``).

    Space-like characters used as thousands separators are removed before
    parsing, including regular space, non-breaking space (U+00A0), narrow
    no-break space (U+202F), and thin space (U+2009) — common in Russian
    locale and Excel/Google Sheets exports.  For example ``"р.250 000"``
    and ``"1\u202F234"`` both yield ``"250000"`` / ``"1234"``.

    Returns ``None`` when no numeric part is found.
    """
    value = value.replace(",", ".")
    value = re.sub(r'[\s\u202F\u2009]', '', value)
    match = _NUMERIC_RE.search(value)
    return match.group(0) if match else None


def validate_schema(headers: list[str], schema: dict[str, dict]) -> None:
    """Check that all *required* schema columns are present in *headers*.

    Raises:
        GoogleSheetsSchemaError: When a required column is missing or an
            unknown type is specified.
    """
    for col_name, col_def in schema.items():
        col_type = col_def.get("type", "str")
        if col_type not in _SUPPORTED_TYPES:
            raise GoogleSheetsSchemaError(
                f"Unknown type '{col_type}' for column '{col_name}'. "
                f"Supported types: {sorted(_SUPPORTED_TYPES)}"
            )
        if col_def.get("required", False) and col_name not in headers:
            raise GoogleSheetsSchemaError(
                f"Required column '{col_name}' is missing. "
                f"Available columns: {headers}"
            )


def apply_schema_to_value(value: Any, column_schema: dict, strip_strings: bool = False) -> Any:
    """Convert a single cell value according to *column_schema*.

    Empty / ``None`` values are returned as-is (no conversion attempted).

    For numeric types (``int``, ``float``), if the column schema contains a
    ``"default"`` key the function switches to *lenient* mode:

    * Comma is accepted as decimal separator (``"1,2"`` → ``1.2``).
    * Non-numeric prefixes/suffixes are stripped (``"10.2%"`` → ``10.2``).
    * Values that cannot be parsed as a number return the ``default`` value
      (typically ``None`` for BigQuery ``NULL`` or ``0``).

    Without ``"default"`` the existing strict behaviour is preserved — a
    ``GoogleSheetsDataError`` is raised on conversion failure.

    Raises:
        GoogleSheetsDataError: When conversion fails (strict mode only).
    """
    col_type = column_schema.get("type", "str")
    has_default = "default" in column_schema

    # --- Lenient numeric path -------------------------------------------
    if col_type in _NUMERIC_TYPES and has_default:
        default = column_schema["default"]

        if value is None or (isinstance(value, str) and value.strip() == ""):
            return default

        if isinstance(value, (int, float)) and not isinstance(value, bool):
            if col_type == "int":
                return int(value)
            return float(value)

        if isinstance(value, str):
            cleaned = _clean_numeric_string(value.strip())
            if cleaned is None:
                return default
            try:
                if col_type == "int":
                    return int(float(cleaned))
                return float(cleaned)
            except (ValueError, TypeError):
                return default

        return default

    # --- Original path (strict) -----------------------------------------
    if value is None or (isinstance(value, str) and value.strip() == ""):
        return value

    fmt = column_schema.get("input_format") or column_schema.get("format")

    try:
        if col_type == "str":
            result = str(value)
            return result.strip() if strip_strings else result

        if col_type == "int":
            return int(float(value))

        if col_type == "float":
            return float(value)

        if col_type == "date":
            if isinstance(value, datetime):
                return value.date()
            if isinstance(value, date):
                return value
            if fmt:
                return datetime.strptime(str(value), fmt).date()
            return date.fromisoformat(str(value))

        if col_type == "datetime":
            if isinstance(value, datetime):
                return value
            if fmt:
                return datetime.strptime(str(value), fmt)
            return datetime.fromisoformat(str(value))

        if col_type == "bool":
            if isinstance(value, bool):
                return value
            if isinstance(value, (int, float)):
                return bool(value)
            str_val = str(value).strip().lower()
            if str_val in ("true", "1", "yes", "да"):
                return True
            if str_val in ("false", "0", "no", "нет"):
                return False
            raise ValueError(f"Cannot convert '{value}' to bool")

    except (ValueError, TypeError) as e:
        raise GoogleSheetsDataError(
            f"Cannot convert value '{value}' to type '{col_type}': {e}"
        ) from e

    return value


def apply_schema_to_row(
    row: list[Any],
    headers: list[str],
    schema: dict[str, dict],
    strip_strings: bool = False,
) -> list[Any]:
    """Apply schema conversions to an entire row.

    Columns not present in *schema* are left unchanged.
    """
    result: list[Any] = []
    for i, value in enumerate(row):
        if i < len(headers) and headers[i] in schema:
            try:
                result.append(apply_schema_to_value(value, schema[headers[i]], strip_strings=strip_strings))
            except GoogleSheetsDataError as e:
                raise GoogleSheetsDataError(
                    f"Column '{headers[i]}' (index {i}): {e}"
                ) from e
        else:
            result.append(value)
    return result


def format_value_for_write(value: Any, column_schema: dict) -> str:
    """Format a Python value back to a string suitable for writing to Sheets.

    This is the inverse of :func:`apply_schema_to_value`.
    """
    if value is None:
        return ""

    col_type = column_schema.get("type", "str")
    fmt = column_schema.get("format")

    if col_type in ("date", "datetime") and fmt and isinstance(value, (date, datetime)):
        return value.strftime(fmt)

    if col_type == "bool":
        return str(value).upper()

    return str(value)


def format_row_for_write(
    row: list[Any],
    headers: list[str],
    schema: dict[str, dict],
) -> list[str]:
    """Format an entire row for writing, applying schema formatting."""
    result: list[str] = []
    for i, value in enumerate(row):
        if i < len(headers) and headers[i] in schema:
            result.append(format_value_for_write(value, schema[headers[i]]))
        else:
            result.append("" if value is None else str(value))
    return result
