"""Narrowing helpers for untrusted YAML / JSON values (pure).

Mapping YAML, reconciliation reports and LLM feedback arrive as nested plain
dicts. These helpers turn each ``object`` read into the precise type the caller
needs, so the rest of the package never touches ``Any``.
"""

from __future__ import annotations

from collections.abc import Mapping

RawMap = Mapping[str, object]

# A reconciliation dimension member: PBI/UCMV values are text, but numeric keys
# (e.g. a year or company code column) must keep their type in DAX literals.
DimensionValue = str | int | float


def as_map(value: object) -> RawMap:
    """The value as a mapping, or an empty one (absent or malformed section)."""
    if isinstance(value, Mapping):
        return value
    return {}


def as_list(value: object) -> list[object]:
    """The value as a list, or an empty one (absent or malformed section)."""
    if isinstance(value, list):
        return list(value)
    return []


def opt_str(value: object) -> str | None:
    """``None`` stays ``None``; anything else is rendered as text."""
    if value is None:
        return None
    return str(value)


def str_or(value: object, default: str) -> str:
    """The value as text, falling back to ``default`` when absent (None)."""
    if value is None:
        return default
    return str(value)


def opt_str_list(value: object) -> list[str] | None:
    """A list of text values, or ``None`` when the value is not a list."""
    if not isinstance(value, list):
        return None
    return [str(item) for item in value]


def opt_scalar_list(value: object) -> list[DimensionValue] | None:
    """A list of dimension members (text or numbers kept as-is), else ``None``."""
    if not isinstance(value, list):
        return None
    return [_scalar(item) for item in value]


def _scalar(value: object) -> DimensionValue:
    if isinstance(value, (str, int, float)):
        return value
    return str(value)


def opt_bool(value: object) -> bool | None:
    """``None`` stays ``None``; anything else is coerced to bool."""
    if value is None:
        return None
    return bool(value)


def as_int(value: object) -> int:
    """An integer count from a report value; anything non-numeric counts as 0."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return 0
    return int(value)


def opt_float(value: object) -> float | None:
    """A numeric value as float, else ``None`` (bools and text are rejected)."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)
