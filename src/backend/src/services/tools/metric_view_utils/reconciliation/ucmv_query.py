"""UCMV-side (Spark SQL / Unity Catalog Metric View) query builder (pure).

Builds one combined ``SELECT <dim>, <period>, MEASURE(`m`) ... FROM <ucmv>
WHERE ... GROUP BY <dim>, <period>`` for every requested measure — grouped by
(dimension_value, period), not looped per value. No special-casing is needed
for the ``dimension_conditional`` PBI pattern here: on the UCMV side those are
just two independently-named measures.
"""

from __future__ import annotations

from typing import Optional

from .mapping import UCMVMapping


def _quote_ident(name: str) -> str:
    """Backtick-quote a UCMV-side identifier. Harmless when unneeded; required
    for measure/dimension names containing spaces (e.g. "Company Code")."""
    return f"`{name}`"


def _quote_sql_string(value) -> str:
    return "'{}'".format(str(value).replace("'", "''"))


def build_ucmv_query(
    mapping: UCMVMapping,
    dimension_name: str,
    dimension_values: list,
    reference_years: list,
    measures: Optional[list] = None,
) -> str:
    binding = mapping.binding
    td = binding.time_dimension
    dim = binding.dimensions[dimension_name]
    measures = measures if measures is not None else mapping.resolved_measures()

    if dim.include_values is not None or dim.exclude_values:
        dimension_values = dim.select_values(
            dimension_values or dim.include_values or []
        )
        if not dimension_values:
            raise ValueError(
                "No dimension values remain inside this measure's applicability."
            )

    select_cols = [f"{_quote_ident(dim.ucmv_column)} AS {dimension_name}"]
    group_cols = [_quote_ident(dim.ucmv_column)]

    if td.ucmv_mode == "column":
        select_cols.append(f"{_quote_ident(td.ucmv_column)} AS period")
        group_cols.append(_quote_ident(td.ucmv_column))
    else:  # "reconstruct"
        cols = td.ucmv_reconstruct_from
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
    where_clauses = []
    if reference_years:
        years_sql = ", ".join(_quote_sql_string(y) for y in reference_years)
        if td.grain == "snapshot":
            where_clauses.append(
                f"YEAR({_quote_ident(td.ucmv_column)}) IN ({years_sql})"
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

    # Split "catalog.schema.table" -> `catalog`.schema.table (only the catalog
    # is backtick-wrapped, matching the reference's addressing).
    parts = binding.ucmv_table.split(".")
    from_target = f"`{parts[0]}`" + (
        "." + ".".join(parts[1:]) if len(parts) > 1 else ""
    )

    where_line = f"\n    WHERE {where_sql}" if where_clauses else ""
    return f"""
    SELECT
        {select_sql}
    FROM {from_target}{where_line}
    GROUP BY {group_sql}
    ORDER BY {dimension_name}, period
""".strip()
