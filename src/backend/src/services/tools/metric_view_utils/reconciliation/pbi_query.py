"""PBI-side DAX (``SUMMARIZECOLUMNS`` / ``CALCULATE`` + ``TREATAS``) builders (pure).

One query per shape, batched aggressively — a single ``SUMMARIZECOLUMNS`` call
combines multiple dimension values (via ``TREATAS``) and, for switch measures,
multiple selector values (by grouping BY the selector column). The mapping's
``pbi_kind`` decides the DAX expression per measure; everything here is a pure
string builder with no I/O (the executor-driven orchestration lives in
``runner.py``).
"""

from __future__ import annotations

import math
from typing import Optional

from .mapping import UCMVMapping
from .periods import normalize_snapshot_date

# The separator every SUMMARIZECOLUMNS body is joined with.
_JOIN_SEP = ",\n    "

# Every PBI SummarizeBy value -> the DAX aggregation function that reproduces it.
_PBI_AGGREGATION_TO_DAX_FUNC = {
    "Sum": "SUM",
    "Average": "AVERAGE",
    "Count": "COUNT",
    "Min": "MIN",
    "Max": "MAX",
    "DistinctCount": "DISTINCTCOUNT",
}

_COMPOSITE_OPERATORS = {"subtract": "-", "add": "+"}

# dimension_conditional resolves to one named PBI measure branching internally
# on ISFILTERED()/HASONEVALUE() of context_dimension — query-shape-identical to
# direct/raw_column/composite (one expression, no selector).
_DIRECT_SHAPED_KINDS = ("direct", "raw_column", "dimension_conditional", "composite")


def strip_table_prefix(col: str) -> str:
    """ "Table[Column]" -> "Column" (passed through if there's no bracket)."""
    return col.split("[")[-1].rstrip("]") if "[" in col else col


def _dax_literal(value) -> str:
    if isinstance(value, bool):
        return "TRUE()" if value else "FALSE()"
    if isinstance(value, str):
        return '"' + value.replace('"', '""') + '"'
    if value is None:
        return "BLANK()"
    if isinstance(value, (int, float)) and math.isfinite(value):
        return str(value)
    raise ValueError(f"Unsupported DAX filter value: {value!r}")


def _dax_set_literal(values: list) -> str:
    return ", ".join(_dax_literal(v) for v in values)


def filter_expression(f: dict) -> str:
    """Render one ``{table, column, values, operator?}`` filter as a DAX filter
    argument. ``operator`` ∈ in (default, TREATAS) / not_in / prefix; an ``in``
    filter may set ``legacy_blank_through`` + ``date_column`` to retain verified
    historical blank-flagged rows without accepting newer blanks."""
    table = f["table"].replace("'", "''")
    column = f["column"].replace("]", "]]")
    ref = f"'{table}'[{column}]"
    values = f["values"]
    operator = f.get("operator", "in")
    if not values:
        raise ValueError("A mapping filter needs at least one value.")
    if f.get("legacy_blank_through"):
        if operator != "in" or not f.get("date_column"):
            raise ValueError(
                "A dated blank exception requires operator=in and date_column."
            )
        cutoff = normalize_snapshot_date(f["legacy_blank_through"])
        parts = ", ".join(str(int(part)) for part in cutoff.split("-"))
        date_column = f["date_column"].replace("]", "]]")
        date_ref = f"'{table}'[{date_column}]"
        return (
            f"KEEPFILTERS(FILTER(ALL({ref}, {date_ref}), {ref} IN {{{_dax_set_literal(values)}}} "
            f"|| (ISBLANK({ref}) && {date_ref} <= DATE({parts}))))"
        )
    if operator == "in":
        return f"TREATAS({{{_dax_set_literal(values)}}}, {ref})"
    if operator == "not_in":
        return f"FILTER(ALL({ref}), NOT({ref} IN {{{_dax_set_literal(values)}}}))"
    if (
        operator == "prefix"
        and len(values) == 1
        and isinstance(values[0], str)
        and values[0]
    ):
        return f"FILTER(ALL({ref}), LEFT({ref}, {len(values[0])}) = {_dax_literal(values[0])})"
    raise ValueError(f"Unsupported mapping filter operator or values: {operator!r}")


def _extra_filter_key(m) -> tuple:
    return tuple(filter_expression(f) for f in m.pbi_extra_filter or [])


def _extra_filter_lines(measures: list) -> list:
    return [filter_expression(f) for f in measures[0].pbi_extra_filter or []]


def _raw_column_expr(pbi_column: str, pbi_aggregation: str = "Sum") -> str:
    dax_func = _PBI_AGGREGATION_TO_DAX_FUNC[pbi_aggregation]
    table, _, column = pbi_column.partition("[")  # column carries trailing ']'
    return f"{dax_func}('{table}'[{column})"


def _composite_operand_expr(operand: dict) -> str:
    base = _raw_column_expr(operand["pbi_column"])
    filters = operand.get("extra_filter") or []
    if not filters:
        return base
    filter_args = ", ".join(filter_expression(f) for f in filters)
    return f"CALCULATE({base}, {filter_args})"


def measure_dax_expr(m) -> str:
    """The DAX expression aggregated for one measure — a named-measure ref for
    direct/dimension_conditional, ``<AGG>('Table'[Column])`` for raw_column, or
    an arithmetic combination of two independently-filtered operands for
    composite."""
    if m.pbi_kind == "raw_column":
        return _raw_column_expr(m.pbi_column, m.pbi_aggregation)
    if m.pbi_kind == "composite":
        expr_a = _composite_operand_expr(m.composite_a)
        expr_b = _composite_operand_expr(m.composite_b)
        op = _COMPOSITE_OPERATORS[m.composite_operator]
        return f"({expr_a} {op} {expr_b})"
    return f"[{m.pbi_measure}]"


def build_dimension_scope_query(mapping: UCMVMapping, dimension_name: str) -> str:
    dim = mapping.binding.dimensions[dimension_name]
    table = dim.pbi_dim_table.replace("'", "''")
    column = dim.pbi_dim_column.replace("]", "]]")
    fact = mapping.binding.pbi_fact_table.replace("'", "''")
    return f"EVALUATE CALCULATETABLE(DISTINCT('{table}'[{column}]), '{fact}')"


def build_direct_query(
    mapping: UCMVMapping, dimension_name: str, dimension_values: list, measures: list
) -> Optional[str]:
    """One combined SUMMARIZECOLUMNS call for plain direct-shaped measures (no
    ``pbi_extra_group_by``). Returns None when there are none."""
    if not measures:
        return None
    binding = mapping.binding
    dim = binding.dimensions[dimension_name]
    dim_values_sql = _dax_set_literal(dimension_values) if dimension_values else None
    time_table = binding.time_dimension.pbi_table or binding.pbi_fact_table

    lines = [
        f"'{dim.pbi_dim_table}'[{dim.pbi_dim_column}]",
        f"'{time_table}'[{binding.time_dimension.pbi_column}]",
    ]
    if dim_values_sql:
        lines.append(
            f"TREATAS({{{dim_values_sql}}}, '{dim.pbi_dim_table}'[{dim.pbi_dim_column}])"
        )
    lines.extend(_extra_filter_lines(measures))
    for m in measures:
        lines.append(f'"{m.ucmv_measure}", {measure_dax_expr(m)}')

    body = _JOIN_SEP.join(lines)
    return f"EVALUATE\nSUMMARIZECOLUMNS(\n    {body}\n)"


def build_direct_queries(
    mapping: UCMVMapping, dimension_name: str, dimension_values: list, measures: list
) -> list:
    """One SUMMARIZECOLUMNS per distinct ``extra_filter`` group. Measures with
    DIFFERENT extra_filters cannot share a query — a SUMMARIZECOLUMNS filter
    applies to every measure column in it, so combining e.g. an '0000'- and a
    'B000'-filtered raw column in one call would silently give both the same
    (wrong) value. Mirrors the live runner's grouping so a dry-run preview matches
    what actually executes. Returns the list of non-empty query strings."""
    groups: dict = {}
    for m in measures:
        groups.setdefault(_extra_filter_key(m), []).append(m)
    out: list = []
    for group in groups.values():
        q = build_direct_query(mapping, dimension_name, dimension_values, group)
        if q:
            out.append(q)
    return out


def _month_of_period(period: str) -> int:
    """Month-grain only: a "YYYYNNN" fiscper's last 3 chars are the month."""
    return int(str(period)[-3:])


def _context_value(ref: str, period: str) -> str:
    if ref == "C_Dim_Calendar[Month]":
        return str(_month_of_period(period))
    raise NotImplementedError(
        f"No value-derivation rule for extra_group_by column {ref!r} — only "
        "'C_Dim_Calendar[Month]' is implemented; add a case to _context_value()."
    )


def build_direct_context_queries(
    mapping: UCMVMapping,
    dimension_name: str,
    dimension_values: list,
    periods: list,
    measures: list,
) -> list:
    """One SUMMARIZECOLUMNS call PER PERIOD for direct measures whose DAX needs a
    context column in single-value filter context (``pbi_extra_group_by``).
    Filtering that column via TREATAS to a single derived value, one period at a
    time, avoids the cross-join a naive grouping would produce. Returns a list of
    (dax_query, period) tuples."""
    if not measures or not periods:
        return []
    binding = mapping.binding
    dim = binding.dimensions[dimension_name]
    dim_values_sql = _dax_set_literal(dimension_values) if dimension_values else None
    time_table = binding.time_dimension.pbi_table or binding.pbi_fact_table
    extra_refs = sorted({ref for m in measures for ref in (m.pbi_extra_group_by or ())})

    queries = []
    for period in periods:
        lines = [f"'{dim.pbi_dim_table}'[{dim.pbi_dim_column}]"]
        if dim_values_sql:
            lines.append(
                f"TREATAS({{{dim_values_sql}}}, '{dim.pbi_dim_table}'[{dim.pbi_dim_column}])"
            )
        lines.append(
            f"TREATAS({{\"{period}\"}}, '{time_table}'[{binding.time_dimension.pbi_column}])"
        )
        for ref in extra_refs:
            table, _, column = ref.partition("[")
            column = column.rstrip("]")
            lines.append(
                f"TREATAS({{{_context_value(ref, period)}}}, '{table}'[{column}])"
            )
        lines.extend(_extra_filter_lines(measures))
        for m in measures:
            lines.append(f'"{m.ucmv_measure}", {measure_dax_expr(m)}')

        body = _JOIN_SEP.join(lines)
        queries.append((f"EVALUATE\nSUMMARIZECOLUMNS(\n    {body}\n)", period))

    return queries


def build_switch_queries(
    mapping: UCMVMapping, dimension_name: str, dimension_values: list, measures: list
) -> list:
    """One SUMMARIZECOLUMNS call per distinct (selector table/column[, selector2],
    pbi_measure, extra_filter) combination among switch measures — grouped BY the
    selector column(s) with TREATAS restricted to the needed values. Returns a
    list of (dax_query, group_measures, selector_column, selector2_column_or_None)."""
    binding = mapping.binding
    dim = binding.dimensions[dimension_name]
    dim_values_sql = _dax_set_literal(dimension_values) if dimension_values else None
    time_table = binding.time_dimension.pbi_table or binding.pbi_fact_table

    groups: dict = {}
    for m in measures:
        key = (
            m.switch_selector_table,
            m.switch_selector_column,
            m.switch_selector2_table,
            m.switch_selector2_column,
            m.pbi_measure,
            _extra_filter_key(m),
        )
        groups.setdefault(key, []).append(m)

    queries = []
    for (
        sel_table,
        sel_col,
        sel2_table,
        sel2_col,
        pbi_measure,
        _,
    ), group_measures in groups.items():
        selector_values = sorted({m.switch_selector_value for m in group_measures})
        lines = [
            f"'{dim.pbi_dim_table}'[{dim.pbi_dim_column}]",
            f"'{time_table}'[{binding.time_dimension.pbi_column}]",
            f"'{sel_table}'[{sel_col}]",
        ]
        if sel2_table:
            lines.append(f"'{sel2_table}'[{sel2_col}]")
        if dim_values_sql:
            lines.append(
                f"TREATAS({{{dim_values_sql}}}, '{dim.pbi_dim_table}'[{dim.pbi_dim_column}])"
            )
        lines.append(
            f"TREATAS({{{_dax_set_literal(selector_values)}}}, '{sel_table}'[{sel_col}])"
        )
        if sel2_table:
            selector2_values = sorted(
                {m.switch_selector2_value for m in group_measures}
            )
            lines.append(
                f"TREATAS({{{_dax_set_literal(selector2_values)}}}, '{sel2_table}'[{sel2_col}])"
            )
        lines.extend(_extra_filter_lines(group_measures))
        lines.append(f'"value", [{pbi_measure}]')

        body = _JOIN_SEP.join(lines)
        queries.append(
            (
                f"EVALUATE\nSUMMARIZECOLUMNS(\n    {body}\n)",
                group_measures,
                sel_col,
                sel2_col,
            )
        )

    return queries


def validate_dimension_conditional_usage(measures: list, dimension_name: str) -> None:
    """Guard the one mistake that silently yields a wrong-branch result for a
    dimension_conditional measure: querying a context_included=True measure
    without its context_dimension in the grouping, or a context_included=False
    measure WITH it."""
    for m in measures:
        if m.pbi_kind != "dimension_conditional":
            continue
        if m.context_included and m.context_dimension != dimension_name:
            raise ValueError(
                f"{m.ucmv_measure}: dimension_conditional context_included=true requires "
                f"dimension_name={m.context_dimension!r} (its context_dimension) — got {dimension_name!r}."
            )
        if not m.context_included and m.context_dimension == dimension_name:
            raise ValueError(
                f"{m.ucmv_measure}: dimension_conditional context_included=false requires "
                f"context_dimension ({m.context_dimension!r}) ABSENT from the grouping — "
                f"but dimension_name={dimension_name!r} is exactly that dimension."
            )


def partition_measures_by_shape(measures: list) -> tuple:
    """Split into (direct, direct_context, switch) — the three query shapes."""
    direct = [
        m
        for m in measures
        if m.pbi_kind in _DIRECT_SHAPED_KINDS and not m.pbi_extra_group_by
    ]
    direct_context = [
        m
        for m in measures
        if m.pbi_kind in _DIRECT_SHAPED_KINDS and m.pbi_extra_group_by
    ]
    switch = [m for m in measures if m.pbi_kind == "switch"]
    return direct, direct_context, switch
