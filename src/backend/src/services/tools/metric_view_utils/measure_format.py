"""Display ``format`` for UC metric-view measures (pure).

Genie writes a measure the way its ``format`` says. A measure without one comes
out as a raw float (``0.6362550405764922`` instead of ``63.63%``). This module
decides the format of one measure from what is known about it. No I/O, no LLM.

Resolution order (the first rule that decides wins):

1. ``_pp`` name suffix: a difference in percentage points -> ``number``, 2 places.
2. The Power Bi ``formatString`` harvested from the TMDL measure, when it is a
   percent, currency-with-a-known-symbol or plain number format. Percent
   formats map to ``percentage`` (the fraction standard, 2 places).
3. Name heuristics for percentages (``_pct``, ``rate``, ``share`` ...). A
   measure whose expression already multiplies by 100 is NOT given a percentage
   format (that would scale it twice); it is a number on the 0-100 scale.
4. An aggregate expression (``SUM``/``COUNT``/``AVG`` ...) without a percentage
   name is an amount or a count: ``number``, ``max`` 2 places.
5. Otherwise ``None``: no format is derived and the metadata lint reports it
   (MV009). Division alone is not enough to call a measure a percentage.

Per-unit values (``*_per_<unit>``) use ``max`` 4 places, so small per-unit
numbers do not round to zero.

``currency`` is only emitted for a symbol that names one currency (EUR, GBP).
A bare ``$`` is ambiguous (several dollar currencies), so it becomes ``number``.
Any other currency has to come from an explicit per-measure override.
"""

from __future__ import annotations

import re

# Name vocabulary. Conservative: a name must carry the word as a whole token.
_PP_NAME_RE = re.compile(r"_pp(?:_|$)", re.IGNORECASE)
_PERCENT_NAME_RE = re.compile(
    r"(?:_pct|percent|ratio|rate|share|accuracy|yield|utili[sz]ation|attainment)(?:_|$)",
    re.IGNORECASE,
)
_PER_UNIT_NAME_RE = re.compile(r"(?:^|_)per_", re.IGNORECASE)

# Aggregates that make an expression an amount or a count (not a bare column).
_AGGREGATE_RE = re.compile(
    r"\b(?:SUM|SUM0|COUNT|COUNT_IF|COUNTIF|AVG|MIN|MAX|COUNTROWS|DISTINCTCOUNT)\s*\(",
    re.IGNORECASE,
)

# A literal factor of 100 next to an operator, e.g. ``100*x`` or ``x * 100``.
_SCALE_100_RE = re.compile(r"(?<![\w.])100(?:\.0+)?\s*\*|\*\s*100(?:\.0+)?(?![\w.])")

_CURRENCY_SYMBOLS = {"€": "EUR", "£": "GBP"}

_PERCENT_PLACES = 2
_AMOUNT_PLACES = 2
_PER_UNIT_PLACES = 4


def _strip_string_literals(expr: str) -> str:
    """Drop quoted literals so a ``*`` or a ``100`` inside a string is ignored."""
    without_single = re.sub(r"'[^']*'", " ", expr)
    return re.sub(r'"[^"]*"', " ", without_single)


def expr_scales_by_100(sql_expr: str | None) -> bool:
    """True if the expression multiplies (or is multiplied) by a literal 100."""
    return bool(_SCALE_100_RE.search(_strip_string_literals(sql_expr or "")))


def percentage_format() -> dict:
    """The fraction standard: ``percentage`` with exactly 2 decimal places."""
    return {
        "type": "percentage",
        "decimal_places": {"type": "exact", "places": _PERCENT_PLACES},
    }


def number_format(places: int = _AMOUNT_PLACES) -> dict:
    """``number`` with up to ``places`` decimals (trailing zeros dropped)."""
    return {"type": "number", "decimal_places": {"type": "max", "places": places}}


def _currency_format(code: str) -> dict:
    return {
        "type": "currency",
        "currency_code": code,
        "decimal_places": {"type": "max", "places": _AMOUNT_PLACES},
    }


def format_from_pbi_format_string(format_string: str | None) -> dict | None:
    """Map a Power BI ``formatString`` to a UC metric-view format.

    Returns ``None`` when the string says nothing numeric (``General``, empty,
    a date or a text format). Only the first section of a multi-section
    format string (the positive-number section) is read.
    """
    section = (format_string or "").split(";")[0].strip().strip('"').strip()
    if not section or section.lower() == "general":
        return None
    if not any(ch in section for ch in "0#"):
        return None  # date / text / literal: not a number
    if "%" in section:
        return percentage_format()
    for symbol, code in _CURRENCY_SYMBOLS.items():
        if symbol in section:
            return _currency_format(code)
    return number_format(_AMOUNT_PLACES)


def derive_measure_format(
    measure_name: str | None,
    sql_expr: str | None = None,
    pbi_format_string: str | None = None,
) -> dict | None:
    """Derive the UC metric-view ``format`` of one measure, or ``None``.

    See the module docstring for the resolution order. The returned dict is
    fresh on every call, so callers may mutate it.
    """
    name = measure_name or ""
    expr = sql_expr or ""
    per_unit = bool(_PER_UNIT_NAME_RE.search(name))

    if _PP_NAME_RE.search(name):
        return number_format(_AMOUNT_PLACES)

    pbi = format_from_pbi_format_string(pbi_format_string)
    if pbi is not None:
        if pbi["type"] == "number" and per_unit:
            return number_format(_PER_UNIT_PLACES)
        return pbi

    if _PERCENT_NAME_RE.search(name):
        if expr_scales_by_100(expr):
            return number_format(_AMOUNT_PLACES)
        return percentage_format()

    if _AGGREGATE_RE.search(expr):
        return number_format(_PER_UNIT_PLACES if per_unit else _AMOUNT_PLACES)

    return None
