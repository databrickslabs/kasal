"""Shape config-gen measures into the UC Metric View ``measures_json`` handoff.

A measure's ``table_name`` is the PBI table it is defined on, often a
measure-holder with no data. Left as-is the KPI is dropped by the UCMV pipeline,
so each measure is re-homed onto the fact table(s) its DAX actually references.
"""

import logging
import re
from collections.abc import Mapping, Sequence

from src.services.tools.pipeline_config_generator_utils.config_access import (
    first_text,
)

logger = logging.getLogger("src.services.tools.pipeline_config_generator_tool")

# DAX ``Table[Column]`` reference (the table half is what we allocate on).
# Matches bare ``Table[Col]`` and quoted ``'Table Name'[Col]``.
DAX_TABLE_REF = re.compile(r"(?:'([^']+)'|(\w+))\s*\[")


def resolve_measure_allocations(
    dax: str,
    home_table: str,
    fact_tables: set[str],
    table_lookup: Mapping[str, str],
) -> list[dict[str, str]]:
    """Resolve which FACT tables a measure's DAX draws its columns from.

    Returns ``[{'table': <fact>, 'role': 'primary'|'secondary'}, ...]``: the first
    referenced fact is primary, the rest secondary. ``[]`` when nothing resolves
    (the caller then falls back to the home table).
    """
    if not dax:
        return []
    seen: list[str] = []
    for match in DAX_TABLE_REF.finditer(dax):
        raw = (match.group(1) or match.group(2) or "").strip()
        if not raw:
            continue
        resolved = table_lookup.get(raw) or table_lookup.get(raw.lower())
        if resolved and resolved in fact_tables and resolved not in seen:
            seen.append(resolved)
    # A measure whose home table is itself a fact stays primary on its home.
    if home_table in fact_tables and home_table not in seen:
        seen.insert(0, home_table)
    if not seen:
        return []
    return [
        {"table": table, "role": "primary" if index == 0 else "secondary"}
        for index, table in enumerate(seen)
    ]


def _fact_join_keys(config: Mapping[str, object] | None) -> set[str]:
    fact_join_map = (config or {}).get("fact_join_map")
    return (
        {str(key) for key in fact_join_map}
        if isinstance(fact_join_map, dict)
        else set()
    )


def _allocations_carried(measure: Mapping[str, object]) -> list[dict[str, str]]:
    carried = measure.get("all_allocations")
    return carried if isinstance(carried, list) and carried else []


def build_ucmv_measures(
    measures: Sequence[Mapping[str, object]],
    admin_tables: Mapping[str, object] | None = None,
    config: Mapping[str, object] | None = None,
) -> list[dict[str, object]]:
    """Convert config-gen measures into the UCMV ``measures_json`` shape.

    Emits ``{measure_name, original_name, dax_expression, proposed_allocation,
    table_refs}`` (plus ``all_allocations`` when the measure spans facts), the
    shape UCMV's own PBI-API path produces, so the handoff is a drop-in.
    ``proposed_allocation`` is the primary allocation, kept as the single-table
    fallback for older consumers.
    """
    fact_tables = _fact_join_keys(config)
    # Build a name -> canonical-key lookup so DAX refs (raw PBI table names)
    # resolve to the mquery/admin keys. fact_join_map keys are authoritative
    # facts even when absent from admin_tables.
    table_lookup: dict[str, str] = {}
    for key in admin_tables or {}:
        table_lookup[key] = key
        table_lookup[key.lower()] = key
    for key in fact_tables:
        table_lookup.setdefault(key, key)
        table_lookup.setdefault(key.lower(), key)

    out: list[dict[str, object]] = []
    rehomed = 0
    orphaned = 0
    for measure in measures or []:
        if not isinstance(measure, dict):
            continue
        name = first_text(measure, "measure_name", "name").strip()
        if not name:
            continue
        dax = first_text(measure, "dax_expression", "expression")
        home_table = first_text(measure, "proposed_allocation", "table_name", "table")

        # Prefer an allocation the measure already carries; otherwise resolve
        # from the DAX references against the known fact tables.
        all_allocations = _allocations_carried(measure)
        if not all_allocations and fact_tables:
            all_allocations = resolve_measure_allocations(
                dax, home_table, fact_tables, table_lookup
            )
            if all_allocations:
                if all_allocations[0]["table"] != home_table:
                    rehomed += 1
            elif home_table and home_table not in fact_tables:
                # DAX referenced no known fact: left on its home table, counted
                # so the orphaned measure is visible rather than silent.
                orphaned += 1

        allocation = (
            (all_allocations[0]["table"] if all_allocations else None)
            or home_table
            or "__unassigned__"
        )
        entry: dict[str, object] = {
            "measure_name": name,
            "original_name": first_text(measure, "original_name") or name,
            "dax_expression": dax,
            "proposed_allocation": allocation,
            "table_refs": measure.get("table_refs") or [],
        }
        if all_allocations:
            entry["all_allocations"] = all_allocations
        out.append(entry)

    if rehomed or orphaned:
        logger.info(
            "[PipelineConfigGen] Measure allocation: %s re-homed to "
            "referenced fact(s) via DAX, %s left on home table "
            "(no known fact referenced)",
            rehomed,
            orphaned,
        )
    return out
