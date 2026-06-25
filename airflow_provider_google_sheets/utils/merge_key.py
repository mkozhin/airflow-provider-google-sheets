"""Merge Key normalization and Inferred Schema resolution for merge operations."""

from __future__ import annotations

import logging
import re
from datetime import date, datetime, timedelta

from airflow_provider_google_sheets.exceptions import GoogleSheetsDataError
from airflow_provider_google_sheets.utils.schema import apply_schema_to_value, format_value_for_write

logger = logging.getLogger(__name__)

# Google Sheets epoch quirk inherited from Lotus 1-2-3: day 1 = 1900-01-01,
# but Lotus incorrectly treated 1900 as a leap year, so the actual epoch is
# 1899-12-30 (serial 1 → 1899-12-31, serial 2 → 1900-01-01, …).
_SHEETS_EPOCH = date(1899, 12, 30)

_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def infer_date_key_schema(values: list[str]) -> dict | None:
    """Infer an ISO-date schema for a merge key column from incoming values.

    Used to make date-keyed merges safe even when the caller does not pass an
    explicit ``schema`` for the merge-key column. Looks only at the shape of
    the *incoming* (current run) key values — if every non-empty value looks
    like an ISO date (``YYYY-MM-DD``) and parses as a real calendar date, the
    column is treated as a date column.

    The check is strict on purpose: **all** non-empty values must match,
    otherwise ``None`` is returned. This avoids false positives on columns
    that merely happen to contain a few date-like strings (e.g. free-text or
    ID columns).

    Args:
        values: Raw incoming key values (as strings) for the current run.

    Returns:
        ``{"type": "date", "format": "%Y-%m-%d"}`` if all non-empty values are
        valid ISO dates, otherwise ``None``. Also returns ``None`` for an
        empty input or one containing only empty strings.
    """
    samples = [v for v in values if v]
    if not samples:
        return None
    for v in samples:
        if not _ISO_DATE_RE.match(v):
            return None
        try:
            date.fromisoformat(v)
        except ValueError:
            return None
    return {"type": "date", "format": "%Y-%m-%d"}


def resolve_merge_key_schema(
    explicit_key_schema: dict | None,
    incoming_values: list[str],
    *,
    infer: bool,
) -> dict | None:
    """Resolve the effective schema to use for Merge Key normalization.

    Priority: an *explicit* schema (even one that is not formally about
    dates, e.g. ``{"type": "str"}``) always wins and disables inference.
    When no explicit schema is given and *infer* is ``True``, falls back to
    :func:`infer_date_key_schema` (Inferred Schema). Otherwise returns
    ``None``.

    This function is pure — it does not log. Callers (``write.py``) are
    responsible for logging when an Inferred Schema is applied.

    Args:
        explicit_key_schema: Schema entry for the merge-key column taken
            from the operator's ``schema`` parameter, if any.
        incoming_values: Raw incoming key values (as strings) for the
            current run, used for inference.
        infer: Whether schema-free inference is enabled
            (``normalize_merge_key_format``).

    Returns:
        The schema to use for Merge Key normalization, or ``None``.
    """
    if explicit_key_schema is not None:
        return explicit_key_schema
    if not infer:
        return None
    return infer_date_key_schema(incoming_values)


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
