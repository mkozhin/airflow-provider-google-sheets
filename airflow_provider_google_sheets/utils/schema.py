"""Schema validation and type conversion for spreadsheet data."""

from __future__ import annotations

import logging
import re
from datetime import date, datetime, timedelta
from typing import Any

from airflow_provider_google_sheets.exceptions import GoogleSheetsDataError, GoogleSheetsSchemaError

logger = logging.getLogger(__name__)

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


# Google Sheets epoch quirk inherited from Lotus 1-2-3: day 1 = 1900-01-01,
# but Lotus incorrectly treated 1900 as a leap year, so the actual epoch is
# 1899-12-30 (serial 1 → 1899-12-31, serial 2 → 1900-01-01, …).
_SHEETS_EPOCH = date(1899, 12, 30)


def normalize_merge_key(
    raw: str,
    key_schema: dict | None,
    extended: bool = True,
) -> str:
    """Normalise a raw key value read from Google Sheets to canonical write format.

    The function is used during *merge* operations to convert the key values
    stored in the sheet (which may differ from the input format due to regional
    locale settings, manual cell formatting, or storage as serial date numbers)
    back to the canonical string representation used when writing.

    Args:
        raw: Raw string value read from the sheet.
        key_schema: Schema definition for the merge-key column (e.g.
            ``{"type": "date", "format": "%Y-%m-%d", "input_format": "%d.%m.%Y"}``).
            When ``None``, *raw* is returned unchanged.
        extended: When ``True`` (default), tries additional fallback strategies
            beyond the primary ``format``-based parse:

            1. Parse with ``format`` from schema (primary, same as ``extended=False``).
            2. Parse with ``input_format`` from schema (if present).
            3. Interpret as a Google Sheets serial date number (only when the
               column type is ``"date"`` or ``"datetime"``).
            4. If none of the above succeed, return *raw* and emit a warning.

            When ``False``, only step 1 is attempted (legacy behaviour).

    Returns:
        Normalised string key, or *raw* if normalisation is not possible.

    Note:
        For serial datetime values the fractional day part (time of day) is
        ignored — ``int(float(raw))`` truncates the fraction.  This is a
        known limitation.
    """
    if key_schema is None:
        return raw

    col_type = key_schema.get("type", "str")
    schema_without_input_fmt = {k: v for k, v in key_schema.items() if k != "input_format"}

    # ------------------------------------------------------------------ #
    # Step 1: parse with "format" (primary, used even when extended=False) #
    # ------------------------------------------------------------------ #
    parse_schema_primary = schema_without_input_fmt
    try:
        parsed = apply_schema_to_value(raw, parse_schema_primary)
        return format_value_for_write(parsed, key_schema)
    except (ValueError, TypeError, GoogleSheetsDataError):
        if not extended:
            logger.warning(
                "Could not normalize existing key %r via schema, using raw value", raw
            )
            return raw

    # ------------------------------------------------------------------ #
    # Step 2: parse with "input_format"                                    #
    # ------------------------------------------------------------------ #
    input_fmt = key_schema.get("input_format")
    if input_fmt:
        input_schema = {**schema_without_input_fmt, "format": input_fmt}
        # Temporarily treat input_format as the parsing format
        try:
            parsed = apply_schema_to_value(raw, input_schema)
            return format_value_for_write(parsed, key_schema)
        except (ValueError, TypeError, GoogleSheetsDataError):
            pass

    # ------------------------------------------------------------------ #
    # Step 3: Google Sheets serial date number                             #
    # (only for date/datetime columns to avoid false positives on IDs)     #
    # ------------------------------------------------------------------ #
    if col_type in ("date", "datetime"):
        try:
            serial = int(float(raw))
            parsed_date = _SHEETS_EPOCH + timedelta(days=serial)
            if col_type == "datetime":
                parsed = datetime.combine(parsed_date, datetime.min.time())
            else:
                parsed = parsed_date
            return format_value_for_write(parsed, key_schema)
        except (ValueError, TypeError, OverflowError):
            pass

    # ------------------------------------------------------------------ #
    # Step 4: give up — return raw with a warning                          #
    # ------------------------------------------------------------------ #
    logger.warning(
        "Could not normalize existing key %r via schema (tried format, input_format, "
        "serial date), using raw value",
        raw,
    )
    return raw
