"""Classify single values by what the confidential-values mode may show.

The rule is the one the no-KPI-values guard of the PBI-to-UCMV migration
tooling uses: text, dates, booleans and integers are categorical (names,
keys, flags, counts) and may be shown; fractional numbers are what measures
are made of and are never shown. A value is classified by its Python type
when the tool hands over structured data, and by its text when the tool
hands over a rendered table or a JSON string.
"""

import re
from typing import Literal

CellKind = Literal["null", "bool", "int", "date", "text", "fractional"]

#: What a withheld fractional value is replaced with.
PLACEHOLDER = "<withheld>"

#: A fractional literal anywhere in a string: ``12.5``, ``1,234.50``, ``3.0%``,
#: ``1.5e3``, ``2e-4``. Deliberately broad: a string that quotes a fraction is
#: withheld whole, which is the conservative direction.
_FRACTION = re.compile(
    r"[-+]?\d[\d,]*\.\d+(?:[eE][-+]?\d+)?|[-+]?\d+(?:\.\d+)?[eE][-+]?\d+"
)
_DATE = re.compile(
    r"\d{4}-\d{2}-\d{2}(?:[T ]\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?(?:Z|[+-]\d{2}:?\d{2})?)?"
)
_INT = re.compile(r"[-+]?\d+")
_BOOL = re.compile(r"true|false", re.IGNORECASE)
_NULL_TEXT = frozenset({"", "null", "none", "nan", "nat"})


def classify_text(text: str) -> CellKind:
    """Kind of a value that arrived as text (a table cell, a JSON string)."""
    stripped = text.strip()
    if stripped.lower() in _NULL_TEXT:
        return "null"
    if _DATE.fullmatch(stripped):
        return "date"
    if _INT.fullmatch(stripped):
        return "int"
    if _BOOL.fullmatch(stripped):
        return "bool"
    if _FRACTION.search(stripped):
        return "fractional"
    return "text"


def classify_value(value: object) -> CellKind:
    """Kind of a structured value (JSON scalar, DAX row cell, Python object)."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        # Every float is fractional, including 3.0: the type is what decides.
        return "fractional"
    if isinstance(value, str):
        return classify_text(value)
    if hasattr(value, "isoformat"):  # date, datetime, time
        return "date"
    return classify_text(str(value))


def is_fractional_text(text: str) -> bool:
    """True when a string carries a fractional number anywhere in it."""
    return classify_text(text) == "fractional"


def may_show_kind(kind: CellKind) -> bool:
    """True for kinds the mode may print: categorical text, dates, booleans, integers."""
    return kind in ("null", "bool", "int", "date", "text")
