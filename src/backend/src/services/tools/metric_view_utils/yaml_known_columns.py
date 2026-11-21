"""The columns a metric-view measure may reference as ``source.<col>``.

The emitter drops a measure that references any other column: it would fail on deploy with
UNRESOLVED_COLUMN (a dimension-table column, or a column Power Query computes).
"""

from __future__ import annotations

import re

from .data_classes import MetricViewSpec
from .measure_order import order_by_dependencies


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


def drop_phantom_dimensions(
    spec: MetricViewSpec,
    base_measures: list,
    implicit_measures: list,
    dax_measures: list,
    switch_measures: list,
) -> None:
    """Drop the ``source.<column>`` dimensions whose column the fact is not known to have."""
    # Early dimension validation: drop phantom source.column dimensions.
    # Build column set from base measures and FILTER clauses only (not from dimensions
    # themselves — that would be circular self-validation).
    _base_only_cols_early: set[str] = set()
    for m in base_measures + implicit_measures:
        if m.sql_expr:
            _base_only_cols_early.update(re.findall(r"\bsource\.(\w+)", m.sql_expr))
    for m in dax_measures + switch_measures:
        if m.sql_expr:
            for fc_m in re.finditer(r"FILTER\s*\(WHERE\s+([^)]+)\)", m.sql_expr):
                _base_only_cols_early.update(
                    re.findall(r"\b(\w+)\s*(?:=|<>|!=|IN\b)", fc_m.group(1))
                )
    if spec.source_filter:
        _base_only_cols_early.update(
            re.findall(r"\b(\w+)\s*(?:=|<>|!=|IN\b|NOT\s+IN\b)", spec.source_filter)
        )
    # When source_sql is present (inline SQL), ALL columns in the SELECT list are valid
    if spec.source_sql:
        # Extract AS aliases
        for col_m in re.finditer(r"\bAS\s+(\w+)", spec.source_sql, re.IGNORECASE):
            _base_only_cols_early.add(col_m.group(1))
            _base_only_cols_early.add(col_m.group(1).lower())
        # Extract bare column refs from first SELECT (before FROM)
        first_arm = re.split(
            r"\bUNION\b", spec.source_sql, maxsplit=1, flags=re.IGNORECASE
        )[0]
        sel_m = re.match(
            r"\s*SELECT\s+(.*?)\s+FROM\s+", first_arm, re.IGNORECASE | re.DOTALL
        )
        if sel_m:
            for col_part in sel_m.group(1).split(","):
                bare = re.match(r"^\s*(\w+)\s*$", col_part.strip())
                if bare:
                    _base_only_cols_early.add(bare.group(1))
                    _base_only_cols_early.add(bare.group(1).lower())
    # The native query's output columns are the fact's columns: in read mode (the view reads
    # the source layer, so there is no inline source_sql here) they were unknown, and every
    # dimension on a column no measure touches was dropped as a phantom.
    measure_side = set(_base_only_cols_early)
    _base_only_cols_early.update(spec.fact_columns)
    # A joined table's dimension keeps its name: where the fact and a join both offer
    # ``comp_code``, the join's was the one emitted before, and still is.
    joined_names = {
        d["name"]
        for d in spec.dimensions
        if not re.search(r"\bsource\.", str(d.get("expr") or ""))
    }
    if _base_only_cols_early and spec.dimensions:
        valid_dims = []
        for d in spec.dimensions:
            src_refs = set(re.findall(r"\bsource\.(\w+)", d["expr"]))
            if src_refs:
                unknown_dim_cols = src_refs - _base_only_cols_early
                if unknown_dim_cols:
                    continue  # phantom dimension — skip
                if (
                    measure_side
                    and not src_refs <= measure_side
                    and d["name"] in joined_names
                ):
                    continue
            valid_dims.append(d)
        spec.dimensions = valid_dims
    _drop_orphan_windows(
        spec, [base_measures, implicit_measures, dax_measures, switch_measures]
    )
    # Each list is emitted in its order: a measure must come after the ones it reads.
    order_by_dependencies(dax_measures)
    order_by_dependencies(switch_measures)


_MEASURE_REF = re.compile(r"\bMEASURE\(\s*`?(\w+)`?\s*\)", re.IGNORECASE)


def _drop_orphan_windows(spec: MetricViewSpec, measure_lists: list[list]) -> None:
    """Take out the window measures whose order dimension is not among the view's
    dimensions, and the measures that read them: a view with such a window does not
    deploy at all. ``prior_year.report_unemitted`` reports them with the reason."""
    dimensions = {str(d.get("name")) for d in spec.dimensions}
    gone: dict[str, str] = {}
    changed = True
    while changed:
        changed = False
        for measures in measure_lists:
            for measure in list(measures):
                window = getattr(measure, "window_spec", None) or {}
                reads = {
                    n.lower() for n in _MEASURE_REF.findall(str(measure.sql_expr or ""))
                }
                if window.get("order") and str(window["order"]) not in dimensions:
                    reason = (
                        f"the view has no dimension {window.get('order')} "
                        "for the measure's window to order by"
                    )
                elif reads & set(gone):
                    reason = "uses a window measure that is not in this view"
                else:
                    continue
                measures.remove(measure)
                gone[measure.measure_name.lower()] = reason
                measure.is_translatable = False
                measure.sql_expr = None
                measure.window_spec = None
                measure.skip_reason = reason
                spec.untranslatable.append(measure)
                changed = True
