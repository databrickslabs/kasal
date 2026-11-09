"""Derive filter_sets, switch_decompositions and measure_resolutions from DAX.

The UCMV tool depends on these three config keys. The PBI APIs do not supply
them directly, so they are scanned out of the measure expressions here.
"""

import re
from collections import defaultdict
from collections.abc import Mapping, Sequence

from src.services.tools.pipeline_config_generator_utils.config_access import (
    first_text,
    read_section,
    text_value,
)

_FILTER_EQUALS = re.compile(r'(\w+)\[(\w+)\]\s*=\s*"([^"]*)"')
_FILTER_IN = re.compile(r"(\w+)\[(\w+)\]\s+in\s+\{([^}]+)\}", re.IGNORECASE)
_VAR_LIST = re.compile(r"var\s+(\w+)\s*=\s*\{([^}]+)\}", re.IGNORECASE)
_QUOTED = re.compile(r'"([^"]*)"')
_SWITCH_TRUE = re.compile(
    r"SWITCH\s*\(\s*TRUE\s*\(\s*\)\s*,\s*(.*?)\s*\)\s*$",
    re.IGNORECASE | re.DOTALL,
)
_BRANCH_CONDITION = re.compile(
    r'SELECTEDVALUE\s*\(\s*\w+\[(\w+)\]\s*\)\s*=\s*"([^"]*)"', re.IGNORECASE
)
_SELECTED_VALUE_DIM = re.compile(
    r"SELECTEDVALUE\s*\(\s*(\w+)\[(\w+)\]\s*\)", re.IGNORECASE
)


def _split_top_level(body: str) -> list[str]:
    """Split on commas that are not nested inside parentheses."""
    parts: list[str] = []
    depth = 0
    buf: list[str] = []
    for ch in body:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == "," and depth == 0:
            parts.append("".join(buf).strip())
            buf = []
            continue
        buf.append(ch)
    if buf:
        parts.append("".join(buf).strip())
    return parts


def _switch_branches(body: str) -> dict[str, dict[str, str]]:
    """``SWITCH(TRUE(), cond, expr, ...)`` body -> ``{branch: {condition, sql_expr}}``."""
    parts = _split_top_level(body)
    branches: dict[str, dict[str, str]] = {}
    # Pair up (condition, expression); only the condition names a branch.
    for idx in range(0, len(parts) - 1, 2):
        cond_text = parts[idx].strip()
        cond_match = _BRANCH_CONDITION.search(cond_text)
        if cond_match:
            branches[cond_match.group(2)] = {
                "condition": cond_text,
                "sql_expr": "",  # needs manual fill or further translation
            }
    return branches


def _measure_fields(measure: Mapping[str, object]) -> tuple[str, str, str]:
    """``(dax, table_name, measure_name)`` as text."""
    return (
        text_value(measure.get("expression")),
        first_text(measure, "table_name"),
        first_text(measure, "measure_name"),
    )


def _collect_filter_values(
    measures: Sequence[Mapping[str, object]],
    filter_sets: dict[str, object],
) -> tuple[dict[str, set[str]], bool]:
    """Literal values per column, plus named ``var X = {...}`` sets added in place."""
    changed = False
    col_values: dict[str, set[str]] = defaultdict(set)
    for measure in measures:
        dax, _, _ = _measure_fields(measure)
        # Table[col] = "val" inside CALCULATE filter arguments
        for hit in _FILTER_EQUALS.finditer(dax):
            col_values[hit.group(2)].add(hit.group(3))
        # Table[col] IN {"a","b","c"}
        for hit in _FILTER_IN.finditer(dax):
            col_values[hit.group(2)].update(_QUOTED.findall(hit.group(3)))
        # var CWC_List = {"APET","CAN",...}
        for hit in _VAR_LIST.finditer(dax):
            vals = _QUOTED.findall(hit.group(2))
            if vals and len(vals) >= 2:
                fs_key = hit.group(1).upper()
                if fs_key not in filter_sets:
                    filter_sets[fs_key] = sorted(set(vals))
                    changed = True
    return col_values, changed


def enrich_config_from_dax(
    config: dict[str, object], measures: Sequence[Mapping[str, object]]
) -> bool:
    """Populate filter_sets, switch_decompositions and measure_resolutions in place.

    Returns True when anything was added.
    """
    if not measures:
        return False

    filter_sets = read_section(config, "filter_sets")
    switch_decompositions = read_section(config, "switch_decompositions")
    measure_resolutions = read_section(config, "measure_resolutions")

    # 1. Literal value sets in filter conditions, then named var sets.
    col_values, changed = _collect_filter_values(measures, filter_sets)
    # Promote column value sets into named filter sets when they have 3+ values.
    for col, col_vals in col_values.items():
        if len(col_vals) >= 3:
            fs_key = f"{col.upper()}_FILTER"
            if fs_key not in filter_sets:
                filter_sets[fs_key] = sorted(col_vals)
                changed = True

    # 2. SWITCH(TRUE(), ...) decompositions.
    for measure in measures:
        dax, table_name, measure_name = _measure_fields(measure)
        switch_match = _SWITCH_TRUE.search(dax)
        if not switch_match:
            continue
        branches = _switch_branches(switch_match.group(1))
        if branches:
            # build_config may already have written a LIST skeleton for this
            # table; only a missing or dict entry is enriched here, and the list
            # form is left for UCMV to flag for human review.
            existing = switch_decompositions.get(table_name)
            if existing is None:
                switch_decompositions[table_name] = {measure_name: branches}
            elif isinstance(existing, dict):
                existing[measure_name] = branches
            changed = True

    # 3. SELECTEDVALUE dimensions -> measure_resolutions.
    for measure in measures:
        dax, table_name, measure_name = _measure_fields(measure)
        for hit in _SELECTED_VALUE_DIM.finditer(dax):
            dim_table, dim_col = hit.group(1), hit.group(2)
            key = f"{dim_table}.{dim_col}"
            if key not in measure_resolutions:
                measure_resolutions[key] = {
                    "table": dim_table,
                    "column": dim_col,
                    "used_by": [],
                }
                changed = True
            entry = measure_resolutions[key]
            users = entry.get("used_by", []) if isinstance(entry, dict) else []
            ref = f"{table_name}.{measure_name}"
            if isinstance(users, list) and ref not in users:
                users.append(ref)

    if changed:
        config["filter_sets"] = filter_sets
        config["switch_decompositions"] = switch_decompositions
        config["measure_resolutions"] = measure_resolutions
    return changed
