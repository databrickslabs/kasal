"""``SUMX(T, T[a] + T[b] + ...)``: a row expression summed over the table.

DAX adds the non-blank terms of a row: a row where one term is blank still counts the others.
``SUM(source.a + source.b)`` in SQL makes the whole row NULL when one term is NULL, so the row
drops out of the sum. Each term is therefore taken as ``COALESCE(term, 0)``.

All-blank rows: a row whose terms are all blank is blank in DAX and contributes nothing;
``COALESCE(term, 0)`` gives it 0, which is the same sum. The total of a set where every row is
all-blank is blank in DAX, but 0 in SQL, so the emitted measure is NULL when no row has a
non-null term (``COUNT(COALESCE(a, b, ...)) = 0``), as Power BI shows blank.
"""

from __future__ import annotations

import re

_TERM = r"'?[A-Za-z_]\w*'?\[\w+\]"
_TERMS = re.compile(rf"\s*[+-]?\s*{_TERM}(?:\s*[+-]\s*{_TERM})*\s*", re.IGNORECASE)
_SUMX_TERMS = re.compile(
    r"\s*(CALCULATE\s*\(\s*)?SUMX\s*\(\s*(\w+)\s*,\s*(.+?)\s*\)(\s*\))?\s*",
    re.IGNORECASE | re.DOTALL,
)
_ONE_TERM = re.compile(r"\s*([+-]?)\s*'?[A-Za-z_]\w*'?\[(\w+)\]\s*")


def match_sumx_terms(dax: str) -> dict | None:
    """``{"terms": [(sign, column), ...]}`` for ``SUMX(T, T[a] +/- T[b] ...)`` (optionally
    inside one CALCULATE), when the row expression is nothing but signed column terms.
    """
    m = _SUMX_TERMS.fullmatch(dax)
    if not m:
        return None
    wrapped, closed = m.group(1) is not None, m.group(4) is not None
    if wrapped != closed:
        return None
    body = m.group(3)
    if not _TERMS.fullmatch(body):
        return None
    terms = [
        (t.group(1) or "+", t.group(2))
        for t in (_ONE_TERM.fullmatch(p) for p in _split_signed(body))
        if t is not None
    ]
    if len(terms) < 2:
        return None  # a single term is the simple SUMX, handled elsewhere
    return {"terms": terms}


def _split_signed(body: str) -> list[str]:
    """``a + b - c`` as ``["a", "+ b", "- c"]`` (each piece keeps its sign)."""
    return [p for p in re.split(r"(?=[+-])", body.strip()) if p.strip()]


def translate_sumx_terms(terms: list[tuple[str, str]]) -> tuple[str, str]:
    """SQL for a match from :func:`match_sumx_terms`: ``(sql, "")``."""
    pieces = []
    for i, (sign, col) in enumerate(terms):
        term = f"COALESCE(source.{col}, 0)"
        if i == 0:
            pieces.append(f"-{term}" if sign == "-" else term)
        else:
            pieces.append(f"{sign} {term}")
    total = " ".join(pieces)
    any_term = ", ".join(f"source.{col}" for _, col in terms)
    return (
        f"CASE WHEN COUNT(COALESCE({any_term})) = 0 THEN NULL "
        f"ELSE SUM({total}) END",
        "",
    )
