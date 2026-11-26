"""Warehouse and LLM enrichment for config-gen (opt-in, gated on a warehouse id).

P2 resolves flag-column ``filter_sets`` from warehouse rows. P3 drafts the
``fact_join_map`` join strategy for cross-fact merges. Both are strictly
additive and fail open: a failure is recorded in the audit log and never aborts
config generation.
"""

import json
import logging
import re

from src.services.tools.pipeline_config_generator_utils.config_access import (
    ensure_section,
    read_section,
    str_list,
)

logger = logging.getLogger("src.services.tools.pipeline_config_generator_tool")

_JOIN_KEY_PROMPT = (
    "You are drafting a cross-fact join strategy for a Databricks UC metric "
    "view migration. Given these fact tables that share conformed dimensions, "
    "propose how to combine them. For EACH fact return a JSON object keyed by "
    "the fact name with fields: join_key (the shared key expression or column), "
    "and optionally union_mode (true if same grain → UNION, false if JOIN), "
    "target_fact (the primary fact to merge into). Return ONLY a JSON object, "
    "no prose.\n\nFacts:\n"
)


def detect_cross_fact_merge(relationships: list[dict]) -> list[str]:
    """P3 gate (deterministic, no warehouse or LLM): facts in a cross-fact merge.

    A cross-fact merge is when two or more fact tables share a conformed
    dimension. Returns ``[]`` when there is none, which skips the LLM call.
    """
    from src.services.powerbi import pipeline_config as _gc

    facts = (
        _gc._identify_fact_tables(relationships, admin_tables={})
        if relationships
        else set()
    )
    if len(facts) < 2:
        return []
    # fact -> set(dims), mirroring _build_fact_join_map_skeleton's fact_dims.
    fact_dims: dict[str, set[str]] = {}
    for rel in relationships:
        fc = str(rel.get("from_cardinality", "")).lower()
        tc = str(rel.get("to_cardinality", "")).lower()
        if "many" in tc and "one" in fc:
            fact_dims.setdefault(rel["to_table"], set()).add(rel["from_table"])
        elif "many" in fc and "one" in tc:
            fact_dims.setdefault(rel["from_table"], set()).add(rel["to_table"])
    # Any dimension shared by two or more facts puts those facts in the merge.
    involved: set[str] = set()
    fact_list = [fact for fact in facts if fact in fact_dims]
    for i, a in enumerate(fact_list):
        for b in fact_list[i + 1 :]:
            if fact_dims[a] & fact_dims[b]:
                involved.add(a)
                involved.add(b)
    return sorted(involved)


async def enrich_filter_sets_from_warehouse(
    config: dict[str, object],
    admin_tables: dict[str, dict],
    measures: list[dict],
    warehouse_id: str,
    host_override: str | None = None,
) -> list[dict[str, object]]:
    """P2: resolve a flag column's ``filter_sets`` values from the warehouse.

    A DAX filter like ``Table[cwc_filter] = 1`` names a boolean flag; the category
    values it selects live in the dimension rows. Finds the dimension's
    ``source_table`` (filled by P1) and value column, then runs
    ``SELECT DISTINCT value WHERE flag = 1``. Only writes a ``filter_sets`` key
    that is absent. Returns an audit log and never raises.
    """
    from src.services.powerbi import pipeline_config as _gc
    from src.services.tools.metric_view_utils import uc_query

    log: list[dict[str, object]] = []
    flag_col = _gc._detect_cwc_filter_column(measures, admin_tables)
    if not flag_col or flag_col.startswith("TODO"):
        log.append(
            {
                "tier": "P2",
                "key": "filter_sets",
                "status": "skipped",
                "detail": "no boolean flag column detected in DAX",
            }
        )
        return log

    # The flag lives on a dimension; its value column is that dimension's first
    # business column that is not the flag itself.
    target: tuple[str, str] | None = None
    for entry in read_section(config, "join_key_map").values():
        if not isinstance(entry, dict):
            continue
        src = entry.get("source_table")
        cols = str_list(entry.get("dim_columns"))
        if src and not str(src).startswith("TODO") and flag_col in cols:
            value_col = next((c for c in cols if c != flag_col), None)
            if value_col:
                target = (str(src), value_col)
                break
    if target is None:
        log.append(
            {
                "tier": "P2",
                "key": "filter_sets",
                "status": "skipped",
                "detail": f"flag column '{flag_col}' has no resolved dimension "
                "source_table + value column (run P1 / set source_table)",
            }
        )
        return log

    source_table, value_col = target
    # Resolve the warehouse once, then reuse it for the query.
    try:
        resolved = await uc_query.resolve_workspace_and_warehouse(
            warehouse_id, host_override
        )
    except Exception as e:  # noqa: BLE001 — fail-open, recorded in audit log
        log.append(
            {
                "tier": "P2",
                "key": "filter_sets",
                "status": "error",
                "detail": f"warehouse resolve failed: {e}",
            }
        )
        return log

    # Structured params (not a raw WHERE string) so uc_query identifier-validates
    # flag_col, which derives from PBI-scan dim_columns (SEC #2).
    res = await uc_query.select_distinct(
        source_table, value_col, flag_col=flag_col, flag_value=1, _resolved=resolved
    )
    if not res.get("success"):
        log.append(
            {
                "tier": "P2",
                "key": "filter_sets",
                "status": "error",
                "detail": f"query failed: {res.get('error')}",
            }
        )
        return log

    values = sorted(set(res.get("values", [])))
    if not values:
        log.append(
            {
                "tier": "P2",
                "key": "filter_sets",
                "status": "skipped",
                "detail": f"{flag_col}=1 returned no values",
            }
        )
        return log

    set_key = _gc.to_snake_case(flag_col).upper()
    filter_sets = ensure_section(config, "filter_sets")
    if set_key in filter_sets:
        log.append(
            {
                "tier": "P2",
                "key": "filter_sets",
                "target": set_key,
                "status": "skipped",
                "detail": "filter set already present (not overwritten)",
            }
        )
    else:
        filter_sets[set_key] = values
        log.append(
            {
                "tier": "P2",
                "key": "filter_sets",
                "target": set_key,
                "status": "filled",
                "value": values,
                "detail": f"SELECT DISTINCT {value_col} FROM {source_table} "  # report text; the query ran via select_distinct — nosemgrep: tmp.databricks.sql-injection.fstring-select
                f"WHERE {flag_col}=1 ({len(values)} values)",  # report text — nosemgrep: tmp.databricks.sql-injection.fstring-where
            }
        )
    return log


async def enrich_fact_join_map_llm(
    config: dict[str, object],
    involved_facts: list[str],
    warehouse_id: str,
    host_override: str | None = None,
) -> list[dict[str, object]]:
    """P3: draft ``fact_join_map`` join keys for cross-fact merges.

    Runs deterministic grain probes (row counts) against the warehouse, then ONE
    LLM call. Only replaces ``TODO:`` join keys and marks every drafted value
    ``TODO: verify``, because a wrong join produces silently wrong numbers.
    Returns an audit log and never raises.
    """
    from src.services.powerbi import pipeline_config as _gc
    from src.services.tools.metric_view_utils import uc_query

    log: list[dict[str, object]] = []
    fact_join_map = read_section(config, "fact_join_map")

    # Deterministic grain probes (no LLM): per involved fact, resolve row grain.
    try:
        resolved = await uc_query.resolve_workspace_and_warehouse(
            warehouse_id, host_override
        )
    except Exception as e:  # noqa: BLE001 — fail-open, recorded in audit log
        log.append(
            {
                "tier": "P3",
                "key": "fact_join_map",
                "status": "error",
                "detail": f"warehouse resolve failed: {e}",
            }
        )
        return log

    join_key_map = read_section(config, "join_key_map")
    facts_summary: list[dict[str, object]] = []
    for fact in involved_facts:
        entry = read_section(fact_join_map, fact)
        src = read_section(join_key_map, fact).get("source_table")
        summary: dict[str, object] = {
            "fact": fact,
            "alias": entry.get("alias", _gc.to_snake_case(fact)),
            "source_table": src,
        }
        if src and not str(src).startswith("TODO"):
            res = await uc_query.run_query(
                f"SELECT COUNT(*) AS n FROM {uc_query._quote_ident(str(src))}",  # identifier validated and quoted — nosemgrep: tmp.databricks.sql-injection.fstring-select
                _resolved=resolved,
            )
            if res.get("success") and res.get("rows"):
                summary["row_count"] = res["rows"][0][0]
        facts_summary.append(summary)

    # ONE LLM call to assemble the merge strategy.
    try:
        draft = await call_llm_fact_join_map(facts_summary)
    except Exception as e:  # noqa: BLE001 — fail-open, recorded in audit log
        log.append(
            {
                "tier": "P3",
                "key": "fact_join_map",
                "status": "error",
                "detail": f"LLM assembly failed: {e}",
            }
        )
        return log
    if not isinstance(draft, dict):
        log.append(
            {
                "tier": "P3",
                "key": "fact_join_map",
                "status": "error",
                "detail": "LLM returned no usable draft",
            }
        )
        return log

    # Merge additively: only replace TODO join_key values, and mark them as drafts.
    for fact, drafted in draft.items():
        join_entry = fact_join_map.get(fact)
        if not isinstance(join_entry, dict):
            continue
        jk = join_entry.get("join_key", "")
        if not str(jk).startswith("TODO"):
            log.append(
                {
                    "tier": "P3",
                    "key": "fact_join_map",
                    "target": fact,
                    "status": "skipped",
                    "detail": "non-TODO value preserved",
                }
            )
            continue
        if not isinstance(drafted, dict) or not drafted.get("join_key"):
            continue
        for k, v in drafted.items():
            join_entry[k] = v
        # A drafted join is unverified, so it forces human confirmation.
        join_entry["join_key"] = f"{drafted['join_key']}  -- TODO: verify (LLM-drafted)"
        log.append(
            {
                "tier": "P3",
                "key": "fact_join_map",
                "target": fact,
                "status": "filled",
                "value": drafted,
                "detail": "LLM-drafted from warehouse grain probes — verify before deploy",
            }
        )
    config["fact_join_map"] = fact_join_map
    return log


async def call_llm_fact_join_map(facts_summary: list[dict[str, object]]) -> object:
    """ONE LLM call drafting the cross-fact merge strategy.

    Returns the parsed JSON (``{fact: {join_key, ...}}``), or ``{}`` when the
    model returns nothing usable. group_id and OBO come from the call context.
    """
    from src.services.llm.manager import LLMManager
    from src.utils.telemetry import KasalProduct, get_user_agent_header

    messages = [
        {
            "role": "user",
            "content": _JOIN_KEY_PROMPT + json.dumps(facts_summary, indent=2),
        }
    ]
    headers = get_user_agent_header(KasalProduct.POWERBI)
    content = await LLMManager.completion(
        messages=messages,
        model="databricks-claude-sonnet-4-5",
        temperature=0.1,
        max_tokens=2000,
        extra_headers=headers,
    )
    if not content:
        return {}
    # The model may wrap the object in ```json fences; take the outermost braces.
    match = re.search(r"\{.*\}", content.strip(), re.DOTALL)
    if not match:
        return {}
    try:
        parsed: object = json.loads(match.group(0))
    except ValueError:
        return {}
    return parsed
