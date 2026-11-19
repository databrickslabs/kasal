"""The columns a metric-view measure may reference as ``source.<col>``.

The emitter drops a measure that references any other column: it would fail on deploy with
UNRESOLVED_COLUMN (a dimension-table column, or a column Power Query computes).
"""

from __future__ import annotations

import re

from .data_classes import MetricViewSpec


def known_source_columns(
    spec: MetricViewSpec,
    base_measures: list,
    implicit_measures: list,
    switch_measures: list,
) -> set[str]:
    """Columns a measure may reference as ``source.<col>``: those base measures, implicit
    measures, dimensions and switch filters read, and the native query's output columns.
    """
    _known_source_cols: set[str] = set()
    for m in base_measures:
        if m.sql_expr:
            _known_source_cols.update(re.findall(r"\bsource\.(\w+)", m.sql_expr))
    # Implicit visual-column measures are, like base measures, a literal
    # physical column (not derived DAX) — confirmed to exist on this table by
    # the DMV query that produced them (column_metadata.py), so they count as
    # a KNOWN column exactly like a base measure's, not something to validate
    # away for lack of independent corroboration.
    for m in implicit_measures:
        if m.sql_expr:
            _known_source_cols.update(re.findall(r"\bsource\.(\w+)", m.sql_expr))
    for d in spec.dimensions:
        _known_source_cols.update(re.findall(r"\bsource\.(\w+)", d["expr"]))
    _known_source_cols.update(spec.fact_columns)  # the native query's output columns
    # Also pull columns from FILTER clauses in switch measures
    for m in switch_measures:
        if m.sql_expr:
            for fc_m in re.finditer(r"FILTER\s*\(WHERE\s+([^)]+)\)", m.sql_expr):
                _known_source_cols.update(
                    re.findall(r"\b(\w+)\s*(?:=|<>|!=|IN\b)", fc_m.group(1))
                )
    return _known_source_cols
