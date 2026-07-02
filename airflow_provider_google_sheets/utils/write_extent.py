"""Value type describing the table extent written by a write operation.

:class:`WrittenExtent` is the single carrier for the geometry a server-side
``sortRange`` needs: where the table starts, whether a physical header row is
present, how many physical rows it spans, and how wide the sort range is. All
three write modes (``overwrite`` / ``append`` / ``merge``) construct one from
the quantities they already compute; :meth:`GoogleSheetsWriteOperator._execute_sort`
consumes it. Centralising the arithmetic here keeps the emitted ``sortRange``
byte-identical across modes while removing three duplicated copies of the
same off-by-one-prone formulas and the width-contract explanation.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class WrittenExtent:
    """Geometry of the table a write left on the sheet, for server-side sort.

    Fields:
        start_row: 1-based row of the table's top edge.
        header_present: Whether a physical header row sits at the table top on
            the sheet *after* the operation — either written this run, or a
            pre-existing header row when ``has_headers``. When ``True`` the
            first table row is excluded from the sort range.
        total_rows: **All** physical table rows on the sheet after the
            operation, **including the header row when one is present**. This
            matches how each mode already counts today: overwrite
            ``len(all_rows)``; append
            ``existing_row_count + (1 if header_written_this_run) + len(rows)``;
            merge ``rows_after_merge``. The header is included **deliberately**
            — a "data rows only" field would cause an off-by-one in
            :attr:`sort_end` for the "existing data + a pre-existing header not
            rewritten this run" case (append/merge already count that header
            inside their totals).
        width: Column width of the sort range —
            ``max(len(headers), widest written row)``.

    Width contract: the sort range spans the table's declared / written width
    only. Data rows can be wider than the header row (e.g. ``list[list]``
    input), so the width covers the widest written row, not just
    ``len(headers)``. Cells to the RIGHT of that width are intentionally NOT
    reordered. In append / merge modes, pre-existing on-sheet rows that are
    wider than this width are deliberately left uncovered: an open-width read
    of the sheet cannot distinguish same-table ragged cells from FOREIGN data
    in adjacent columns, so widening the sort range to reach them would risk
    reordering unrelated neighbouring data — a worse corruption than leaving a
    few over-wide same-table cells unsorted. Headers define the table;
    same-table rows must not exceed the written width. Sort keys always live
    within ``headers`` so ``dimensionIndex < endColumnIndex`` still holds.
    """

    start_row: int
    header_present: bool
    total_rows: int
    width: int

    @property
    def sort_start(self) -> int:
        """0-based inclusive start row of the sort range (skips the header)."""
        return (self.start_row - 1) + (1 if self.header_present else 0)

    @property
    def sort_end(self) -> int:
        """0-based exclusive end row of the sort range (the old ``end_row``)."""
        return (self.start_row - 1) + self.total_rows

    @property
    def num_columns(self) -> int:
        """Column width of the sort range."""
        return self.width
