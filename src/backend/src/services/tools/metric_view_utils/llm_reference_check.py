"""Deterministic post-check of the measure references an LLM translation uses.

The LLM sometimes resolves ``[Measure X]`` to a sibling measure with a similar expression. The
translation is checked against the DAX it came from:

* a ``MEASURE(<name>)`` in the SQL whose measure the DAX does not reference, directly or through
  one of the measures it references (one level down), is a violation. It rejects the
  translation; the caller counts it as not translated, with the reason. A base column the DAX
  names as ``Table[col]`` (a column measure of the view) is always allowed.
* a bare ``[Ref]`` naming a measure of this view that the SQL does not call as ``MEASURE()`` is
  only reported (``missing_references``): a translation may inline the referenced measure's SQL,
  which is a correct form (measured on the saved live run: such inlined translations match Power
  BI), so it is not a rejection.
"""

from __future__ import annotations

import re
from collections.abc import Mapping

from .utils import to_snake_case

_BARE_REF = re.compile(r"(?<![\w'\]])\[([^\]]+)\]")
_TABLE_COLUMN = re.compile(r"'?[A-Za-z_]\w*'?\[(\w+)\]")
_MEASURE_CALL = re.compile(r"MEASURE\(\s*(\w+)\s*\)", re.IGNORECASE)
_COMMENT_LINE = re.compile(r"^\s*//.*$", re.MULTILINE)


def _bare_refs(dax: str) -> set[str]:
    return set(_BARE_REF.findall(_COMMENT_LINE.sub("", dax or "")))


def _measure_calls(sql: str) -> set[str]:
    return {m.lower() for m in _MEASURE_CALL.findall(sql or "")}


def _snake(ref: str, original_to_snake: Mapping[str, str]) -> str:
    """The MEASURE() name of a referenced measure: the view's own name when it is in the view,
    else the snake form of the reference (the view may not list every measure)."""
    return original_to_snake.get(ref, to_snake_case(ref)).lower()


def _columns(dax: str) -> set[str]:
    return {to_snake_case(c).lower() for c in _TABLE_COLUMN.findall(dax or "")}


def reference_violation(
    dax: str,
    sql: str,
    original_to_snake: Mapping[str, str],
    measure_dax: Mapping[str, str] | None = None,
) -> str | None:
    """``None`` when every ``MEASURE()`` in the SQL is a measure the DAX reads, else the reason.

    ``original_to_snake``: the view's measures, original (Power BI) name to snake name.
    ``measure_dax``: the DAX of those measures, keyed by original name, for the one-level rule.
    """
    allowed = _columns(dax)
    for ref in _bare_refs(dax):
        allowed.add(_snake(ref, original_to_snake))
        for inner in _bare_refs((measure_dax or {}).get(ref, "")):
            allowed.add(_snake(inner, original_to_snake))
    extra = sorted(name for name in _measure_calls(sql) if name not in allowed)
    if extra:
        return (
            "SQL uses MEASURE() of a measure the DAX does not reference: "
            + ", ".join(extra)
        )
    return None


_IDENTIFIER = re.compile(r"\b[A-Za-z_]\w*\b")


def column_violation(dax: str, sql: str, view_columns: frozenset[str]) -> str | None:
    """A ``Table[col]`` of the DAX whose exact name is a column of the view must be read in the
    SQL as ``source.<col>``. The LLM sometimes reads a similar column instead (the ``_final``
    column for its prefix). ``view_columns``: the view's source column names, lower case.
    """
    # Any identifier, with or without the ``source.`` prefix: a filter reads its column bare.
    read = {c.lower() for c in _IDENTIFIER.findall(sql or "")}
    wanted = set()
    for col in _TABLE_COLUMN.findall(dax or ""):
        exact, snake = col.lower(), to_snake_case(col).lower()
        if exact in view_columns:
            wanted.add(exact)
        elif snake in view_columns:
            wanted.add(snake)
    missing = sorted(c for c in wanted if c not in read)
    if missing:
        return "DAX column(s) of the view not read in the SQL: " + ", ".join(missing)
    return None


def missing_references(
    dax: str, sql: str, original_to_snake: Mapping[str, str]
) -> list[str]:
    """Snake names of the view's measures the DAX references by ``[Ref]`` that the SQL does not
    call as ``MEASURE()``. Reported for review; not a rejection (the SQL may inline them).
    """
    called = _measure_calls(sql)
    return sorted(
        original_to_snake[ref]
        for ref in _bare_refs(dax)
        if ref in original_to_snake and original_to_snake[ref].lower() not in called
    )
