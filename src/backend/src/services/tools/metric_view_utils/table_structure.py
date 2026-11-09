"""Structure of one fact table: base measures, dimensions and joins.

Steps 1-4 of the per-table pipeline (``table_processor.process_table``):
the SUM-over-column base measures, the GROUP BY / calculated-column dimensions,
the DAX- and enrichment-driven joins (incl. USERELATIONSHIP alternates), and the
dimension columns contributed by those joins. Translation of the DAX measures
happens afterwards in ``table_processor``, against the structure built here.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING

from .data_classes import TableInfo, TranslationResult
from .utils import col_to_readable

if TYPE_CHECKING:
    from .dax_translator import DaxTranslator
    from .join_detector import JoinDetector


@dataclass
class TableStructure:
    """Base measures, dimensions and joins derived for a single fact table."""

    base_measures: list[TranslationResult]
    base_names: set[str]
    dimensions: list[dict]
    joins: list[dict]
    fact_joins: list[dict]


def build_table_structure(
    table_key: str,
    table_info: TableInfo,
    dax_measures: list[dict],
    *,
    translator: DaxTranslator,
    join_detector: JoinDetector,
    enrichment_joins: dict,
    inactive_rels: list[dict],
    mquery_tables: dict[str, TableInfo],
    inner_dim_joins: bool,
    dimension_exclusions: dict,
) -> TableStructure:
    """Build base measures, dimensions and joins for one fact table."""
    # ── Step 1: Auto-generate base measures from MQuery SUM columns ───
    # Build a DAX-column → source-output-column map so BOTH base and derived
    # measures reference the column the emitted `source:` SELECT exposes: a
    # pre-aggregating source (SUM(kbi_value) AS value) outputs its ALIAS (`name`),
    # not the physical `source_col` (which would fail: column not found).
    _src_alias_map: dict[str, str] = {}
    for _c in table_info.aggregate_columns:
        _sc = _c.get("source_col")
        _nm = _c.get("name")
        if _sc and _nm and _sc != _nm:
            _src_alias_map[_sc] = _nm  # kbi_value -> value (source output col)
    translator.set_source_col_map(_src_alias_map)

    base_measures: list[TranslationResult] = []
    base_names: set[str] = set()
    for agg_col in table_info.aggregate_columns:
        name = agg_col["name"]
        if "expr" in agg_col:
            expr = agg_col["expr"]
        else:
            # Reference the source's OUTPUT column (`name`), which is what the
            # emitted source SELECT exposes — not the inner `source_col`.
            expr = f"SUM(source.{name})"
        base_measures.append(
            TranslationResult(
                measure_name=name,
                original_name=name,
                sql_expr=expr,
                is_translatable=True,
                skip_reason=col_to_readable(name),
                dax_expression="",
                confidence="high",
                category="base",
            )
        )
        base_names.add(name)

    # ── Step 2: Auto-generate dimensions from GROUP BY + calculated columns
    dimensions: list[dict] = []
    for col in table_info.group_by_columns:
        dimensions.append(
            {
                "name": col,
                "expr": f"source.{col}",
                "comment": col_to_readable(col),
            }
        )
    for calc in table_info.calculated_columns:
        expr = calc["expr"]
        for gb_col in table_info.group_by_columns:
            expr = re.sub(
                rf"(?<![.\w])\b{re.escape(gb_col)}\b(?!\s*\()",
                f"source.{gb_col}",
                expr,
            )
        # Normalize DATE() -> MAKE_DATE() for Databricks UC Metric Views
        expr = re.sub(r"\bDATE\s*\(", "MAKE_DATE(", expr)
        dimensions.append(
            {
                "name": calc["name"],
                "expr": expr,
                "comment": f"Calculated: {col_to_readable(calc['name'])}",
            }
        )

    # ── Step 3: Auto-detect joins from DAX dimension refs ─────────────
    joins = join_detector.detect(
        table_key, dax_measures, table_info, inner_dim_joins=inner_dim_joins
    )

    # 3b. Auto-detect fact-to-fact joins for cross-table DIVIDEs
    fact_joins = join_detector.detect_fact_joins(table_key, dax_measures, table_info)
    joins.extend(fact_joins)

    # 3c. Add enrichment joins (domain-specific lookups not in DAX).
    enrichment_cfg = enrichment_joins.get(table_key, [])
    detected_aliases = {j["name"] for j in joins}
    for ej in enrichment_cfg:
        if ej["name"] in detected_aliases:
            continue  # DAX detector already added this join
        joins.append(
            {
                "name": ej["name"],
                "source": ej["source"],
                "join_on": ej["join_on"],
            }
        )
        detected_aliases.add(ej["name"])
        for col in ej.get("dim_columns", []):
            if isinstance(col, dict):
                dim_name = col["name"]
                dim_expr = f"{ej['name']}.{col['expr']}"
            else:
                dim_name = col
                dim_expr = f"{ej['name']}.{col}"
            dimensions.append(
                {
                    "name": dim_name,
                    "expr": dim_expr,
                    "comment": f"{col_to_readable(dim_name)} from {ej['name']}",
                }
            )

    # 3d. Detect USERELATIONSHIP measures and generate alternate join aliases
    userel_pattern = re.compile(
        r"USERELATIONSHIP\s*\(\s*(\w+)\[(\w+)\]\s*,\s*(\w+)\[(\w+)\]\s*\)",
        re.IGNORECASE,
    )
    for dm in dax_measures:
        dax = dm.get("dax_expression", "")
        for match in userel_pattern.finditer(dax):
            fact_col = match.group(2)  # e.g. ShipDate
            dim_table = match.group(3)  # e.g. Calendar
            dim_col = match.group(4)  # e.g. Date
            # Find matching inactive relationship
            for irel in inactive_rels:
                if (
                    irel["from_column"].lower() == fact_col.lower()
                    and irel["to_table"].lower() == dim_table.lower()
                ):
                    # Generate alternate join alias
                    base_alias = dim_table.lower()
                    alt_alias = f"{base_alias}_{fact_col.lower()}"
                    # Check if this join already exists
                    if not any(j["name"] == alt_alias for j in joins):
                        dim_info = mquery_tables.get(irel["to_table"])
                        if dim_info and dim_info.source_table:
                            joins.append(
                                {
                                    "name": alt_alias,
                                    "source": dim_info.source_table,
                                    "join_on": f"source.{fact_col.lower()} = {alt_alias}.{dim_col.lower()}",
                                    "_userelationship": True,
                                }
                            )

    # ── Step 4: Add dimension columns from joined tables ──────────────
    dim_dims = join_detector.get_dim_dimensions(joins, table_info)
    dimensions.extend(dim_dims)

    # 4b. Normalize dimension names to lowercase
    for d in dimensions:
        if d["name"] != d["name"].lower():
            old_name = d["name"]
            d["name"] = old_name.lower()
            d["expr"] = d["expr"].replace(old_name, old_name.lower())

    # 4c. Remove excluded dimensions (join/filter-only columns, not user-facing)
    excluded = dimension_exclusions.get(table_key, set())
    if excluded:
        dimensions = [d for d in dimensions if d["name"] not in excluded]

    return TableStructure(
        base_measures=base_measures,
        base_names=base_names,
        dimensions=dimensions,
        joins=joins,
        fact_joins=fact_joins,
    )
