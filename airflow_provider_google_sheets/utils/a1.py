"""A1-notation value type and column-letter math.

This module is the single home for A1-notation parsing and column-letter ⇄
index conversion. It exposes:

* :class:`A1Range` — a frozen value object describing an A1 range (with an
  optional sheet prefix and possibly-open end bound), plus helpers to do the
  column math (``width`` / ``col_at`` / ``cell`` / ``render``).
* :func:`column_letter_to_index` / :func:`index_to_column_letter` — the
  letter ⇄ 0-based-index conversions (``A → 0``, ``Z → 25``, ``AA → 26`` …).
* :func:`parse_range_start` — a *lenient* start-cell extractor that fills in
  defaults (missing column → ``"A"``, missing row → ``1``) and never raises on
  garbage.  Kept separate from the strict :meth:`A1Range.parse` on purpose.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

# A single A1 endpoint: leading column letters, optional trailing row digits.
_CELL_RE = re.compile(r"^([A-Za-z]+)([0-9]+)?$")


def column_letter_to_index(letter: str) -> int:
    """Convert an A1-notation column letter to a 0-based index.

    ``A → 0``, ``B → 1``, … ``Z → 25``, ``AA → 26``, etc.
    """
    result = 0
    for ch in letter.upper():
        result = result * 26 + (ord(ch) - ord("A") + 1)
    return result - 1


def index_to_column_letter(index: int) -> str:
    """Convert a 0-based column index to an A1-notation letter.

    ``0 → A``, ``1 → B``, … ``25 → Z``, ``26 → AA``, etc.
    """
    result = ""
    i = index
    while True:
        result = chr(ord("A") + i % 26) + result
        i = i // 26 - 1
        if i < 0:
            break
    return result


def col_offset(start_col: str, offset: int) -> str:
    """Column letter *offset* columns to the right of *start_col*.

    ``col_offset("C", 0) == "C"``, ``col_offset("C", 3) == "F"``. Convenience
    for callers that already hold a start column and only need the end column
    of a fixed-width window (avoids fabricating an :class:`A1Range` just to call
    :meth:`A1Range.col_at`).
    """
    return index_to_column_letter(column_letter_to_index(start_col) + offset)


def parse_range_start(range_str: str) -> tuple[str, int]:
    """Extract the start column and row from an A1-notation range, leniently.

    Examples:
        ``"B2:D10"`` → ``("B", 2)``
        ``"Sheet1!C5:F"`` → ``("C", 5)``
        ``"A1"`` → ``("A", 1)``
        ``"B:D"`` → ``("B", 1)`` (missing row defaults to ``1``)
        ``"1:100"`` → ``("A", 1)`` (missing column defaults to ``"A"``)

    Unlike :meth:`A1Range.parse`, this never raises: missing pieces fall back
    to defaults.  Used for operator parameters that must tolerate partial A1
    ranges (e.g. ``table_start``).
    """
    r = range_str
    if "!" in r:
        r = r.split("!", 1)[1]
    # Take only the left part (before ':')
    left = r.split(":")[0]
    col = "".join(c for c in left if c.isalpha()) or "A"
    row_str = "".join(c for c in left if c.isdigit())
    row = int(row_str) if row_str else 1
    return col, row


@dataclass(frozen=True)
class A1Range:
    """An immutable A1-notation range.

    Fields:
        sheet: Optional sheet (tab) name prefix, or ``None``.
        start_col: Start column letters (upper-cased), e.g. ``"C"``.
        start_row: 1-based start row.
        end_col: End column letters, or ``None`` for a bare single cell.
        end_row: 1-based end row, or ``None`` for an open range (``"C3:F"``).

    Use :meth:`parse` to build one from an A1 string and the helper methods
    (:meth:`width`, :meth:`col_at`, :meth:`cell`, :meth:`render`) for the
    column math.
    """

    sheet: str | None
    start_col: str
    start_row: int
    end_col: str | None
    end_row: int | None

    @classmethod
    def parse(cls, text: str, sheet: str | None = None) -> "A1Range":
        """Parse a (strict) A1 range string into an :class:`A1Range`.

        Handles a sheet prefix (``"Sheet1!C3:F"``), open-ended ranges
        (``"C3:F"`` → ``end_row=None``) and bare single cells (``"C3"`` →
        both end fields ``None``).  A sheet name found in *text* takes
        precedence over the *sheet* argument.

        Raises:
            ValueError: on an empty string or anything that is not a valid A1
                range (garbage, a missing start row, more than one ``:``).
        """
        if text is None:
            raise ValueError("A1 range cannot be None")
        raw = text.strip()
        if not raw:
            raise ValueError("A1 range cannot be empty")

        sheet_final = sheet
        if "!" in raw:
            sheet_part, raw = raw.split("!", 1)
            sheet_part = sheet_part.strip()
            if sheet_part:
                sheet_final = sheet_part
            raw = raw.strip()

        parts = raw.split(":")
        if len(parts) > 2:
            raise ValueError(f"Invalid A1 range: {text!r}")

        start_col, start_row = cls._parse_cell(parts[0])
        if start_row is None:
            raise ValueError(
                f"Invalid A1 range: {text!r} (start cell must include a row)"
            )

        if len(parts) == 2:
            end_col, end_row = cls._parse_cell(parts[1])
        else:
            end_col, end_row = None, None

        return cls(sheet_final, start_col, start_row, end_col, end_row)

    @staticmethod
    def _parse_cell(token: str) -> tuple[str, int | None]:
        """Parse a single A1 endpoint into ``(column, row_or_None)``."""
        m = _CELL_RE.match(token.strip())
        if not m:
            raise ValueError(f"Invalid A1 cell reference: {token!r}")
        col = m.group(1).upper()
        row = int(m.group(2)) if m.group(2) is not None else None
        return col, row

    def width(self) -> int | None:
        """Number of columns spanned, or ``None`` if the end column is open."""
        if self.end_col is None:
            return None
        return column_letter_to_index(self.end_col) - column_letter_to_index(self.start_col) + 1

    def col_at(self, offset: int) -> str:
        """Column letter *offset* columns to the right of :attr:`start_col`."""
        return index_to_column_letter(column_letter_to_index(self.start_col) + offset)

    def _prefix(self) -> str:
        return f"{self.sheet}!" if self.sheet else ""

    def cell(self, row: int) -> str:
        """Render a single cell reference at :attr:`start_col` and *row*.

        Example: ``A1Range.parse("Sheet1!C3:F").cell(7)`` → ``"Sheet1!C7"``.
        """
        return f"{self._prefix()}{self.start_col}{row}"

    def render(self) -> str:
        """Render back to an A1 string (round-trips :meth:`parse`)."""
        start = f"{self.start_col}{self.start_row}"
        if self.end_col is None:
            return f"{self._prefix()}{start}"
        end = f"{self.end_col}{self.end_row if self.end_row is not None else ''}"
        return f"{self._prefix()}{start}:{end}"
