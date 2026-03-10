"""Row filtering utilities for skip/stop conditions."""

from __future__ import annotations

_VALID_OPS = {"equals", "not_equals", "contains", "not_contains", "starts_with", "ends_with", "empty", "not_empty"}
_VALUE_NOT_REQUIRED_OPS = {"empty", "not_empty"}


def normalize_conditions(raw: dict | list[dict] | None) -> list[dict]:
    """Normalize a single dict or list of dicts into a list."""
    if raw is None:
        return []
    if isinstance(raw, dict):
        return [raw]
    return list(raw)


def validate_conditions(conditions: list[dict]) -> None:
    """Validate condition structure. Raises ValueError on invalid input."""
    for i, cond in enumerate(conditions):
        if "column" not in cond:
            raise ValueError(f"Condition {i}: missing required key 'column'")
        op = cond.get("op", "equals")
        if op not in _VALID_OPS:
            raise ValueError(f"Condition {i}: unknown op '{op}'. Supported: {sorted(_VALID_OPS)}")
        if op not in _VALUE_NOT_REQUIRED_OPS and "value" not in cond:
            raise ValueError(f"Condition {i}: op '{op}' requires a 'value' key")


def matches_any(row_dict: dict, conditions: list[dict]) -> bool:
    """Return True if row matches at least one condition (OR logic)."""
    for cond in conditions:
        col = cond["column"]
        op = cond.get("op", "equals")
        cell = str(row_dict.get(col, "")) if row_dict.get(col) is not None else ""
        expected = str(cond.get("value", ""))

        if op == "equals" and cell == expected:
            return True
        if op == "not_equals" and cell != expected:
            return True
        if op == "contains" and expected in cell:
            return True
        if op == "not_contains" and expected not in cell:
            return True
        if op == "starts_with" and cell.startswith(expected):
            return True
        if op == "ends_with" and cell.endswith(expected):
            return True
        if op == "empty" and cell == "":
            return True
        if op == "not_empty" and cell != "":
            return True
    return False
