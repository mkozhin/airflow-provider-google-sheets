"""Merge Plan construction for merge operations.

Builds an index of existing rows keyed by the normalized Merge Key, and plans
the delete/append operations (the Merge Plan) needed to reconcile incoming
data against that index.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any, NamedTuple

from airflow_provider_google_sheets.utils.merge_key import normalize_merge_key


class MergePlan(NamedTuple):
    """Delete and append operations computed for one merge run."""

    delete_ops: list[dict]
    append_rows: list[list[Any]]


def build_existing_key_index(
    existing_keys_raw: list[list[Any]],
    *,
    has_headers: bool,
    table_start_row: int,
    key_schema: dict | None,
    extended: bool,
) -> dict[str, list[int]]:
    """Build an index of ``{normalized_key: [row_numbers]}`` (1-based absolute).

    Args:
        existing_keys_raw: Raw key-column values read from the sheet (one row
            per sheet row, as returned by ``values.get``).
        has_headers: Whether the first row of *existing_keys_raw* is a header
            row and should be skipped when indexing.
        table_start_row: 1-based row number where the table starts (i.e. the
            header row when *has_headers* is ``True``, otherwise the first
            data row).
        key_schema: Schema (explicit or Inferred Schema) used to normalize
            each raw key value. ``None`` means raw values are used as-is.
        extended: Passed through to :func:`normalize_merge_key` as
            ``extended`` — whether to try fallback normalization strategies.

    Returns:
        A ``dict`` mapping each normalized key value to the list of 1-based
        absolute row numbers in the sheet that carry that key.
    """
    existing_index: dict[str, list[int]] = defaultdict(list)
    if has_headers:
        data_rows = existing_keys_raw[1:]
        start_row_num = table_start_row + 1
    else:
        data_rows = existing_keys_raw
        start_row_num = table_start_row
    for row_num, row in enumerate(data_rows, start=start_row_num):
        if row:
            key_val = normalize_merge_key(str(row[0]), key_schema, extended=extended)
            existing_index[key_val].append(row_num)
    return existing_index


def plan_merge_operations(
    existing_index: dict[str, list[int]],
    incoming_groups: dict[str, list[list[Any]]],
) -> MergePlan:
    """Compute the Merge Plan: which existing rows to delete and which rows to append.

    For each key present in the incoming data, all existing rows carrying
    that key are deleted (as contiguous row-range segments) and the
    corresponding incoming rows are appended. Keys present only in
    *existing_index* (i.e. absent from the incoming data) are left untouched
    — no delete, no append.

    ``delete_ops`` is sorted by descending ``row_num`` so that callers can
    execute deletes bottom-up, avoiding row-index shifts as earlier deletes
    invalidate the row numbers of later ones. This ordering is part of the
    function's contract.

    Args:
        existing_index: ``{normalized_key: [row_numbers]}`` as produced by
            :func:`build_existing_key_index`.
        incoming_groups: ``{key_value: [rows]}`` grouping the incoming data
            by (raw, non-normalized) key value.

    Returns:
        A :class:`MergePlan` with ``delete_ops`` (each a ``dict`` with
        ``row_num``, ``start_index`` (0-based), and ``end_index``
        (exclusive)) sorted by descending ``row_num``, and ``append_rows``
        containing all incoming rows.
    """
    delete_ops: list[dict] = []
    append_rows: list[list[Any]] = []

    for key_val, incoming_row_data in incoming_groups.items():
        existing_row_nums = existing_index.get(key_val, [])
        if existing_row_nums:
            for seg_start, seg_end in _group_contiguous(existing_row_nums):
                delete_ops.append({
                    "row_num": seg_start,
                    "start_index": seg_start - 1,  # 0-based
                    "end_index": seg_end,           # exclusive
                })
        append_rows.extend(incoming_row_data)

    delete_ops.sort(key=lambda op: op["row_num"], reverse=True)

    return MergePlan(delete_ops=delete_ops, append_rows=append_rows)


def _group_contiguous(rows: list[int]) -> list[tuple[int, int]]:
    """Group sorted row numbers into contiguous segments.

    Each segment is ``(start, end)`` where both are 1-based inclusive.

    Example::

        [3, 7, 8, 12] → [(3, 3), (7, 8), (12, 12)]
    """
    if not rows:
        return []
    groups: list[tuple[int, int]] = []
    start = rows[0]
    prev = rows[0]
    for r in rows[1:]:
        if r == prev + 1:
            prev = r
        else:
            groups.append((start, prev))
            start = r
            prev = r
    groups.append((start, prev))
    return groups
