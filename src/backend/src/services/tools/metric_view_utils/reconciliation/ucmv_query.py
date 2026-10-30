"""UCMV-side (Spark SQL / Unity Catalog Metric View) query builder (pure).

Builds one combined ``SELECT <dim>, <period>, MEASURE(`m`) ... FROM <ucmv>
WHERE ... GROUP BY <dim>, <period>`` for every requested measure — grouped by
(dimension_value, period), not looped per value. No special-casing is needed
for the ``dimension_conditional`` PBI pattern here: on the UCMV side those are
just two independently-named measures.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Optional

from ..raw_fields import DimensionValue
from .mapping import Measure, UCMVMapping


def _quote_ident(name: str) -> str:
    """Backtick-quote a UCMV-side identifier. Harmless when unneeded; required
    for measure/dimension names containing spaces (e.g. "Company Code")."""
    return "`{}`".format(str(name).replace("`", "``"))


def _quote_sql_string(value: object) -> str:
    return "'{}'".format(str(value).replace("'", "''"))


def build_ucmv_query(
    mapping: UCMVMapping,
    dimension_name: str,
    dimension_values: list[DimensionValue],
    reference_years: Sequence[object],
    measures: Optional[list[Measure]] = None,
) -> str:
    binding = mapping.binding
    td = binding.time_dimension
    dim = binding.dimensions[dimension_name]
    measures = measures if measures is not None else mapping.resolved_measures()
    if td.ucmv_mode == "column" and td.ucmv_column is None:
        raise ValueError("time_dimension.ucmv.mode='column' requires ucmv.name")
    if td.ucmv_mode != "column" and not td.ucmv_reconstruct_from:
        raise ValueError("time_dimension.ucmv.mode='reconstruct' requires ucmv.from")

    if dim.include_values is not None or dim.exclude_values:
        dimension_values = dim.select_values(
            dimension_values or dim.include_values or []
        )
        if not dimension_values:
            raise ValueError(
                "No dimension values remain inside this measure's applicability."
            )

    dim_alias = _quote_ident(dimension_name)
    select_cols = [f"{_quote_ident(dim.ucmv_column)} AS {dim_alias}"]
    group_cols = [_quote_ident(dim.ucmv_column)]

    if td.ucmv_mode == "column":
        time_col = _quote_ident(str(td.ucmv_column))
        select_cols.append(f"{time_col} AS period")
        group_cols.append(time_col)
    else:  # "reconstruct"
        cols = td.ucmv_reconstruct_from or []
        quoted_cols = [_quote_ident(c) for c in cols]
        concat_parts = [f"CAST({quoted_cols[0]} AS STRING)"] + quoted_cols[1:]
        select_cols.append(f"CONCAT({', '.join(concat_parts)}) AS period")
        group_cols.extend(quoted_cols)

    for m in measures:
        select_cols.append(
            f"MEASURE({_quote_ident(m.ucmv_measure)}) AS {_quote_ident(m.ucmv_measure)}"
        )

    # The year filter is OPTIONAL. With no reference_years the comparison spans
    # all years (no year predicate) — reconciliation can run without the operator
    # pinning specific fiscal years (closed-period filtering in compare() still
    # applies).
    where_clauses: list[str] = []
    if reference_years:
        years_sql = ", ".join(_quote_sql_string(y) for y in reference_years)
        if td.grain == "snapshot":
            where_clauses.append(
                f"YEAR({_quote_ident(str(td.ucmv_column))}) IN ({years_sql})"
            )
        else:
            where_clauses.append(
                f"{_quote_ident(td.year_filter_column)} IN ({years_sql})"
            )
    if dimension_values:
        values_sql = ", ".join(_quote_sql_string(v) for v in dimension_values)
        where_clauses.append(f"{_quote_ident(dim.ucmv_column)} IN ({values_sql})")

    select_sql = ",\n        ".join(select_cols)
    where_sql = "\n      AND ".join(where_clauses)
    group_sql = ", ".join(group_cols)

    # "catalog.schema.table" -> `catalog`.`schema`.`table`: every part quoted
    # (and escaped), since the table comes from a mapping, not from Kasal.
    from_target = ".".join(_quote_ident(p) for p in binding.ucmv_table.split("."))

    where_line = f"\n    WHERE {where_sql}" if where_clauses else ""
    return f"""
    SELECT
        {select_sql}
    FROM {from_target}{where_line}
    GROUP BY {group_sql}
    ORDER BY {dim_alias}, period
""".strip()
