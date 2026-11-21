"""Which tables filter a fact in Power BI, and DAX that filters on a table that does not.

A filter travels from the one side to the many side of an active relationship, across several
of them. In the relationship list a row's ``from_table`` is the many side and its ``to_table``
the one side. So the tables that filter a fact are found by walking ``from_table -> to_table``
from the fact.

A predicate on a table that does not filter the fact does nothing in Power BI:
``CALCULATE(SUMX(Fact, Fact[x]), FILTER(Dim, Dim[c] <> 'a'))`` returns the same as the sum without
the filter when ``Dim`` has no active path to ``Fact``. The translation must not carry that
predicate into a join the view does not have. ``drop_non_filtering_filters`` takes such
predicates out of ``CALCULATE`` arguments, and only when nothing else in the measure depends on
a table that is not reachable from the fact.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping

from .dax_condition_sql import split_top_level_args

# Functions that create or change a relationship inside the measure (a virtual
# relationship, an inactive one switched on, a row lookup). A predicate next to one of them
# may filter the fact, so the measure is left as written.
RELATIONSHIP_FUNCTIONS = re.compile(
    r"\b(TREATAS|USERELATIONSHIP|CROSSFILTER|RELATED|RELATEDTABLE|LOOKUPVALUE|"
    r"NATURALINNERJOIN|NATURALLEFTOUTERJOIN|SUMMARIZE|SUMMARIZECOLUMNS|ADDCOLUMNS|"
    r"GENERATE|GENERATEALL)\s*\(",
    re.IGNORECASE,
)
_CALCULATE = re.compile(r"\bCALCULATE\s*\(", re.IGNORECASE)
_TABLE_COLUMN = re.compile(r"'?([A-Za-z_]\w*)'?\s*\[")
_TABLE_ARG = re.compile(
    r"\b(?:FILTER|ALL|ALLEXCEPT|VALUES|CALCULATETABLE)\s*\(\s*'?([A-Za-z_]\w*)'?\s*[,)]",
    re.IGNORECASE,
)
# A bare [Measure] reference (not Table[column]): the measure may read any table, so the
# measure's data is not the fact's alone. Slicer date pickers (F_Start_date, ...) are scaffolding
# the translator ignores (dax_translator._SCAFFOLD_REFS), so they do not count.
_MEASURE_REF = re.compile(r"(?<![\w'\]])\[([^\]]+)\]")
_SCAFFOLD_REF = re.compile(r"^(?:F|PY)_(?:START|END)_DATE$", re.IGNORECASE)


def _reads_a_measure(dax: str) -> bool:
    return any(not _SCAFFOLD_REF.match(m.group(1)) for m in _MEASURE_REF.finditer(dax))


_FILTER_ARG = re.compile(r"\s*FILTER\s*\(\s*'?([A-Za-z_]\w*)'?\s*,", re.IGNORECASE)
_BOOLEAN_ARG = re.compile(
    r"\s*'?([A-Za-z_]\w*)'?\[\w+\]\s*(?:=|<>|<=|>=|<|>|IN\b)", re.IGNORECASE
)


def filter_reach(fact: str, relationships: Iterable[Mapping]) -> frozenset[str]:
    """The tables that filter ``fact`` over active relationships, the fact included."""
    rels = list(relationships)
    reachable = {fact}
    frontier = [fact]
    while frontier:
        current = frontier.pop()
        for rel in rels:
            target = str(rel.get("to_table") or "")
            if (
                rel.get("is_active") is not False
                and str(rel.get("from_table") or "") == current
                and target not in reachable
            ):
                reachable.add(target)
                frontier.append(target)
    return frozenset(reachable)


def model_mentions(table: str, relationships: Iterable[Mapping]) -> bool:
    """Whether the relationship list describes ``table`` at all (a list without it says
    nothing about the table's filters, so nothing may be concluded)."""
    return any(
        str(rel.get("from_table") or "") == table
        or str(rel.get("to_table") or "") == table
        for rel in relationships
    )


def dax_table_names(dax: str) -> set[str]:
    """The tables a DAX expression names: ``Table[column]`` and table arguments of FILTER,
    ALL, VALUES and similar."""
    names = {m.group(1) for m in _TABLE_COLUMN.finditer(dax)}
    names |= {m.group(1) for m in _TABLE_ARG.finditer(dax)}
    return names


def _matching_paren(text: str, open_at: int) -> int | None:
    depth = 0
    in_str = False
    for i in range(open_at, len(text)):
        ch = text[i]
        if ch == '"':
            in_str = not in_str
        elif in_str:
            continue
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return i
    return None


def _non_filtering_argument(arg: str, fact: str, reach: frozenset[str]) -> str | None:
    """The table a CALCULATE filter argument filters, when that table does not reach the fact."""
    for pattern in (_FILTER_ARG, _BOOLEAN_ARG):
        match = pattern.match(arg)
        if match:
            table = match.group(1)
            if table != fact and table not in reach:
                return table
    return None


def drop_non_filtering_filters(
    dax: str, fact: str, reach: frozenset[str]
) -> tuple[str, list[str]]:
    """``(DAX without the CALCULATE filters on tables that do not reach the fact, the tables
    dropped)``. The DAX is returned unchanged when nothing qualifies, when the measure has a
    relationship-changing function, when the measure reads another measure (its data may come
    from any table), or when a table that does not reach the fact is still named elsewhere in
    the measure (then the measure's own data is not the fact's alone).
    """
    if RELATIONSHIP_FUNCTIONS.search(dax) or _reads_a_measure(dax):
        return dax, []
    result = dax
    dropped: list[str] = []
    for match in reversed(list(_CALCULATE.finditer(dax))):
        open_at = match.end() - 1
        close_at = _matching_paren(result, open_at)
        if close_at is None:
            continue
        args = split_top_level_args(result[open_at + 1 : close_at])
        kept = [args[0]]
        here: list[str] = []
        for arg in args[1:]:
            table = _non_filtering_argument(arg, fact, reach)
            if table is None:
                kept.append(arg)
            else:
                here.append(table)
        if not here:
            continue
        result = result[: open_at + 1] + ",".join(kept) + result[close_at:]
        dropped.extend(here)
    if not dropped:
        return dax, []
    if any(name not in reach for name in dax_table_names(result)):
        return dax, []
    return result, sorted(set(dropped))
