"""Utilities for processing Google Sheets header rows."""

from __future__ import annotations

import re
import logging

logger = logging.getLogger(__name__)


def sanitize_header(header: str) -> str:
    """Normalize a single header to a clean snake_case identifier.

    - Strips leading/trailing whitespace
    - Replaces spaces and hyphens with underscores
    - Removes characters that are not alphanumeric, underscores, or Cyrillic
    - Collapses consecutive underscores
    - Strips leading/trailing underscores
    - Lowercases the result
    """
    header = header.strip()
    header = re.sub(r"[\s\-]+", "_", header)
    header = re.sub(r"[^\w\u0400-\u04FF]", "", header)  # keep word chars + Cyrillic
    header = re.sub(r"_+", "_", header)
    header = header.strip("_")
    return header.lower()


def _strip_special_chars(header: str) -> str:
    """Remove all characters except letters, digits, and underscores.

    Spaces are replaced with underscores before removal so that
    multi-word headers remain readable (e.g. "My Column" → "My_Column").
    """
    header = header.strip()
    header = re.sub(r"[\s]+", "_", header)
    header = re.sub(r"[^\w\u0400-\u04FF]", "", header)  # keep word chars + Cyrillic
    header = re.sub(r"_+", "_", header)
    return header.strip("_")


def _transliterate_text(text: str) -> str:
    """Transliterate Cyrillic text to Latin.

    Uses the ``transliterate`` library if available, otherwise falls back
    to a basic built-in mapping.
    """
    try:
        from transliterate import translit
        return translit(text, "ru", reversed=True)
    except ImportError:
        return _builtin_transliterate(text)


_CYRILLIC_MAP = {
    "а": "a", "б": "b", "в": "v", "г": "g", "д": "d", "е": "e", "ё": "yo",
    "ж": "zh", "з": "z", "и": "i", "й": "j", "к": "k", "л": "l", "м": "m",
    "н": "n", "о": "o", "п": "p", "р": "r", "с": "s", "т": "t", "у": "u",
    "ф": "f", "х": "kh", "ц": "ts", "ч": "ch", "ш": "sh", "щ": "shch",
    "ъ": "", "ы": "y", "ь": "", "э": "e", "ю": "yu", "я": "ya",
}


def _builtin_transliterate(text: str) -> str:
    """Simple Cyrillic → Latin transliteration without external dependencies."""
    result: list[str] = []
    for char in text:
        lower = char.lower()
        if lower in _CYRILLIC_MAP:
            mapped = _CYRILLIC_MAP[lower]
            result.append(mapped.upper() if char.isupper() and mapped else mapped)
        else:
            result.append(char)
    return "".join(result)


def _deduplicate(headers: list[str]) -> list[str]:
    """Add numeric suffixes to duplicate headers.

    >>> _deduplicate(["a", "b", "a", "a"])
    ['a', 'b', 'a_1', 'a_2']
    """
    seen: dict[str, int] = {}
    result: list[str] = []
    for h in headers:
        if h in seen:
            seen[h] += 1
            result.append(f"{h}_{seen[h]}")
        else:
            seen[h] = 0
            result.append(h)
    return result


def process_headers(
    headers: list[str],
    transliterate: bool = False,
    normalize: bool = False,
    sanitize: bool = False,
    lowercase: bool = False,
) -> list[str]:
    """Process a list of raw header strings.

    Args:
        headers: Raw header values from the spreadsheet.
        transliterate: If *True*, Cyrillic characters are transliterated to Latin.
        normalize: If *True*, headers are sanitized to snake_case identifiers
            (includes special char removal and lowercasing).
        sanitize: If *True*, spaces and special characters are removed from
            headers, keeping only letters, digits, and underscores.
        lowercase: If *True*, headers are converted to lowercase.

    Returns:
        Processed headers with duplicates resolved.
    """
    processed: list[str] = []
    for h in headers:
        value = str(h).strip() if h is not None else ""
        if not value:
            value = "unnamed"
        if transliterate:
            value = _transliterate_text(value)
        if normalize:
            value = sanitize_header(value)
        else:
            if sanitize:
                value = _strip_special_chars(value)
            if lowercase:
                value = value.lower()
        processed.append(value)

    return _deduplicate(processed)
