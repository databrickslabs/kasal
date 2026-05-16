#!/usr/bin/env python3
"""Generate pipeline_config.json from Power BI APIs.

Standalone script — no CrewAI, no LLM, no database.  Calls 4 PBI APIs
directly and produces a pipeline_config.json with auto-filled keys and
TODO markers where human input is needed.

Dependencies: requests (pip install requests)

Usage:
  python generate_config.py \
    --workspace-id ac0fa11c-... \
    --dataset-id ecdd57ae-... \
    --tenant-id 9f37a392-... \
    --client-id 7b597aac-... \
    --client-secret "U5b8Q~..." \
    --admin-client-id 8d8aa6ee-... \
    --admin-client-secret "RXm8Q~..." \
    --catalog david_test_metrics \
    --schema cchbc \
    --output proposed_pipeline_config.json
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from collections import defaultdict
from typing import Any

__all__ = [
    "get_token", "extract_relationships", "extract_measures",
    "trigger_admin_scan", "parse_admin_tables", "extract_report_definition",
    "build_config", "to_snake_case",
    "derive_join_key_map", "derive_enrichment_joins", "derive_dim_alias_map",
    "derive_switch_decompositions", "derive_filter_sets",
    "derive_measure_resolutions", "derive_column_overrides",
    "derive_mapping_only_tables", "derive_column_metadata",
    "derive_column_alias_map", "derive_parameter_defaults",
    "derive_name_prefixes", "derive_dimension_exclusions", "derive_period_dims",
    "derive_measure_metadata", "derive_dimension_metadata",
]

try:
    import requests
except ImportError:
    print("ERROR: 'requests' package required.  pip install requests")
    sys.exit(1)


# ═══════════════════════════════════════════════════════════════════════
# Auth
# ═══════════════════════════════════════════════════════════════════════

def get_token(tenant_id: str, client_id: str, client_secret: str) -> str:
    """Acquire OAuth2 token via client_credentials grant."""
    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    resp = requests.post(url, data={
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "https://analysis.windows.net/powerbi/api/.default",
    }, timeout=30)
    _check_response(resp, f"Auth (client_id={client_id[:8]}...)")
    return resp.json()["access_token"]


def _headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def _check_response(resp, context: str = "") -> None:
    """Check HTTP response, raise with PBI error body on failure."""
    if resp.status_code >= 400:
        try:
            body = resp.json()
        except Exception:
            body = resp.text
        raise RuntimeError(
            f"{context} HTTP {resp.status_code}: {body}"
        )


# ═══════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════

def to_snake_case(name: str) -> str:
    """Convert PascalCase/camelCase/mixed to snake_case."""
    s = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1_\2', name)
    s = re.sub(r'([a-z\d])([A-Z])', r'\1_\2', s)
    s = re.sub(r'[\s\-]+', '_', s)
    return s.lower().strip('_')


def _humanize(name: str) -> str:
    """Turn a snake_case column name into a display name."""
    return name.replace('_', ' ').title()


# ═══════════════════════════════════════════════════════════════════════
# API 1: Execute Queries — INFO.VIEW.RELATIONSHIPS()
# ═══════════════════════════════════════════════════════════════════════

def extract_relationships(token: str, workspace_id: str, dataset_id: str) -> list[dict]:
    """Call INFO.VIEW.RELATIONSHIPS() → list of relationship dicts."""
    url = (
        f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}"
        f"/datasets/{dataset_id}/executeQueries"
    )
    body = {
        "queries": [{"query": "EVALUATE INFO.VIEW.RELATIONSHIPS()"}],
        "serializerSettings": {"includeNulls": True},
    }
    resp = requests.post(url, headers=_headers(token), json=body, timeout=60)
    _check_response(resp, "API 1 (Relationships)")
    data = resp.json()

    rows = data.get("results", [{}])[0].get("tables", [{}])[0].get("rows", [])
    relationships = []
    for row in rows:
        relationships.append({
            "from_table": row.get("[FromTable]", ""),
            "from_column": row.get("[FromColumn]", ""),
            "from_cardinality": row.get("[FromCardinality]", ""),
            "to_table": row.get("[ToTable]", ""),
            "to_column": row.get("[ToColumn]", ""),
            "to_cardinality": row.get("[ToCardinality]", ""),
            "is_active": row.get("[IsActive]", True),
            "id": row.get("[ID]", ""),
        })
    return relationships


def derive_join_key_map(
    relationships: list[dict],
    admin_tables: dict[str, dict],
) -> dict[str, dict]:
    """Relationships → join_key_map config.

    Each dim table gets: alias, join_key, dim_columns (from admin scan).
    One-to-many: "many" side is fact, "one" side is dim.
    """
    join_key_map: dict[str, dict] = {}

    for rel in relationships:
        if not rel.get("is_active"):
            continue

        # Determine which side is dim (One) and which is fact (Many)
        from_card = str(rel.get("from_cardinality", "")).lower()
        to_card = str(rel.get("to_cardinality", "")).lower()

        if "one" in from_card and "many" in to_card:
            dim_table = rel["from_table"]
            dim_col = rel["from_column"]
            fact_col = rel["to_column"]
        elif "many" in from_card and "one" in to_card:
            dim_table = rel["to_table"]
            dim_col = rel["to_column"]
            fact_col = rel["from_column"]
        else:
            # many-to-many or one-to-one — skip for join_key_map
            continue

        if dim_table in join_key_map:
            continue  # first relationship wins

        alias = to_snake_case(dim_table)
        # Strip common prefixes for alias
        for prefix in ("c_dim_", "dim_", "c_"):
            if alias.startswith(prefix):
                alias = "dim_" + alias[len(prefix):]
                break
        else:
            if not alias.startswith("dim_"):
                alias = "dim_" + alias

        entry: dict[str, Any] = {
            "alias": alias,
            "join_key": to_snake_case(fact_col),
        }

        snake_dim_col = to_snake_case(dim_col)
        if snake_dim_col != entry["join_key"]:
            entry["dim_key"] = snake_dim_col

        # Enrich with dim_columns from admin scan
        dim_cols = _get_dim_columns(dim_table, admin_tables)
        entry["dim_columns"] = dim_cols

        join_key_map[dim_table] = entry

    return join_key_map


def _get_dim_columns(table_name: str, admin_tables: dict[str, dict]) -> list[str]:
    """Get non-hidden column names from admin scan for a dim table."""
    tbl = admin_tables.get(table_name, {})
    columns = tbl.get("columns", [])
    result = []
    for col in columns:
        if col.get("isHidden", False):
            continue
        name = to_snake_case(col.get("name", ""))
        if name:
            result.append(name)
    return result


def derive_enrichment_joins(
    relationships: list[dict],
    admin_tables: dict[str, dict],
) -> dict[str, list[dict]]:
    """Relationships → enrichment_joins config.

    Groups one-to-many joins by fact table.
    """
    joins: dict[str, list[dict]] = defaultdict(list)

    for rel in relationships:
        if not rel.get("is_active"):
            continue

        from_card = str(rel.get("from_cardinality", "")).lower()
        to_card = str(rel.get("to_cardinality", "")).lower()

        if "one" in from_card and "many" in to_card:
            dim_table = rel["from_table"]
            fact_table = rel["to_table"]
            dim_col = rel["from_column"]
            fact_col = rel["to_column"]
        elif "many" in from_card and "one" in to_card:
            dim_table = rel["to_table"]
            fact_table = rel["from_table"]
            dim_col = rel["to_column"]
            fact_col = rel["from_column"]
        else:
            continue

        alias = to_snake_case(dim_table)
        for prefix in ("c_dim_", "dim_", "c_"):
            if alias.startswith(prefix):
                alias = "dim_" + alias[len(prefix):]
                break
        else:
            if not alias.startswith("dim_"):
                alias = "dim_" + alias

        dim_cols = _get_dim_columns(dim_table, admin_tables)

        join_entry = {
            "name": alias,
            "source": f"{{catalog}}.{{schema}}.{to_snake_case(dim_table)}",
            "join_on": (
                f"source.{to_snake_case(fact_col)} = "
                f"{alias}.{to_snake_case(dim_col)}"
            ),
            "dim_columns": dim_cols,
        }
        joins[fact_table].append(join_entry)

    return dict(joins)


def derive_dim_alias_map(relationships: list[dict]) -> dict[str, str]:
    """Dim table name → lowercase alias convention."""
    alias_map: dict[str, str] = {}
    for rel in relationships:
        if not rel.get("is_active"):
            continue

        from_card = str(rel.get("from_cardinality", "")).lower()
        to_card = str(rel.get("to_cardinality", "")).lower()

        if "one" in from_card:
            dim_table = rel["from_table"]
        elif "one" in to_card:
            dim_table = rel["to_table"]
        else:
            continue

        if dim_table not in alias_map:
            alias = to_snake_case(dim_table)
            for prefix in ("c_dim_", "dim_", "c_"):
                if alias.startswith(prefix):
                    alias = "dim_" + alias[len(prefix):]
                    break
            else:
                if not alias.startswith("dim_"):
                    alias = "dim_" + alias
            alias_map[dim_table] = alias

    return alias_map


# ═══════════════════════════════════════════════════════════════════════
# API 2: Execute Queries — $SYSTEM.MDSCHEMA_MEASURES
# ═══════════════════════════════════════════════════════════════════════

def extract_measures(token: str, workspace_id: str, dataset_id: str) -> list[dict]:
    """Extract measures via Execute Queries API.

    Tries DMV query ($SYSTEM.MDSCHEMA_MEASURES) first, falls back to
    EVALUATE INFO.VIEW.MEASURES() if the DMV returns 400.
    """
    url = (
        f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}"
        f"/datasets/{dataset_id}/executeQueries"
    )

    # Strategy 1: DMV query (richer — includes EXPRESSION)
    dmv_query = (
        "SELECT "
        "[MEASURE_NAME] as [Measure Name], "
        "[EXPRESSION] as [Expression], "
        "[DESCRIPTION] as [Description], "
        "[MEASUREGROUP_NAME] as [Table] "
        "FROM $SYSTEM.MDSCHEMA_MEASURES"
    )
    body = {
        "queries": [{"query": dmv_query}],
        "serializerSettings": {"includeNulls": True},
    }
    resp = requests.post(url, headers=_headers(token), json=body, timeout=60)

    if resp.status_code == 200:
        data = resp.json()
        rows = data.get("results", [{}])[0].get("tables", [{}])[0].get("rows", [])
        measures = []
        for row in rows:
            name = row.get("[Measure Name]", "")
            if name.startswith("__"):
                continue
            measures.append({
                "measure_name": name,
                "table_name": row.get("[Table]", ""),
                "expression": row.get("[Expression]", ""),
                "description": row.get("[Description]", ""),
            })
        return measures

    # Strategy 2: DAX INFO function (works with SP workspace member)
    print(f"  DMV query returned {resp.status_code}, falling back to EVALUATE INFO.VIEW.MEASURES()...")
    fallback_query = (
        "EVALUATE SELECTCOLUMNS("
        "INFO.VIEW.MEASURES(), "
        "\"Measure Name\", [Name], "
        "\"Table\", [Table], "
        "\"Expression\", [Expression], "
        "\"Description\", [Description]"
        ")"
    )
    body2 = {
        "queries": [{"query": fallback_query}],
        "serializerSettings": {"includeNulls": True},
    }
    resp2 = requests.post(url, headers=_headers(token), json=body2, timeout=60)
    _check_response(resp2, "API 2 (Measures fallback via INFO.VIEW.MEASURES)")
    data2 = resp2.json()

    rows2 = data2.get("results", [{}])[0].get("tables", [{}])[0].get("rows", [])
    measures = []
    for row in rows2:
        name = row.get("[Measure Name]", "")
        if name.startswith("__"):
            continue
        measures.append({
            "measure_name": name,
            "table_name": row.get("[Table]", ""),
            "expression": row.get("[Expression]", ""),
            "description": row.get("[Description]", ""),
        })
    return measures


def derive_switch_decompositions(measures: list[dict]) -> dict[str, list[dict]]:
    """Detect SELECTEDVALUE+SWITCH measures → skeleton decompositions."""
    decompositions: dict[str, list[dict]] = defaultdict(list)

    for m in measures:
        dax = m.get("expression", "") or ""
        name = m.get("measure_name", "")
        table = m.get("table_name", "")

        if not dax:
            continue
        dax_upper = dax.upper()
        if "SELECTEDVALUE" not in dax_upper or "SWITCH" not in dax_upper:
            continue

        branches = _extract_switch_branches(dax)
        skeleton: dict[str, Any] = {
            "name": to_snake_case(name),
            "raw_expr": (
                f"TODO: SQL expression for SWITCH measure '{name}' "
                f"(DAX: {dax[:120]}...)"
            ),
            "comment": f"SWITCH measure from {name}",
        }
        if branches:
            skeleton["_detected_branches"] = branches
            skeleton["comment"] = (
                f"SWITCH({len(branches)} branches): "
                + ", ".join(b.get("case_value", "?") for b in branches[:5])
            )

        decompositions[table].append(skeleton)

    return dict(decompositions)


def _extract_switch_branches(dax: str) -> list[dict]:
    """Parse SWITCH(TRUE(), ...) branches from DAX."""
    branches: list[dict] = []
    switch_match = re.search(
        r'SWITCH\s*\(\s*TRUE\s*\(\s*\)\s*,\s*(.+)',
        dax, re.IGNORECASE | re.DOTALL,
    )
    if not switch_match:
        return branches

    body = switch_match.group(1)
    branch_re = re.compile(
        r'(\w+)\s*=\s*"([^"]+)"\s*,\s*([^,]+?)(?=,\s*\w+\s*=\s*"|$)',
        re.DOTALL,
    )
    for bm in branch_re.finditer(body):
        branches.append({
            "variable": bm.group(1),
            "case_value": bm.group(2),
            "dax_snippet": bm.group(3).strip().rstrip(",").strip()[:200],
        })
    return branches


def derive_filter_sets(
    measures: list[dict],
    switch_decomps: dict[str, list[dict]],
) -> dict[str, list[str]]:
    """Extract IN({...}) value lists from DAX and SWITCH branches."""
    filter_sets: dict[str, list[str]] = {}

    # From SWITCH branches
    for _table_key, decomps in switch_decomps.items():  # noqa: unused _table_key
        for decomp in decomps:
            branches = decomp.get("_detected_branches", [])
            if not branches:
                continue
            var_name = branches[0].get("variable", "")
            if not var_name:
                continue
            set_key = to_snake_case(var_name).upper()
            values = [b["case_value"] for b in branches if b.get("case_value")]
            if values and set_key not in filter_sets:
                filter_sets[set_key] = sorted(set(values))

    # From DAX IN({...}) patterns
    in_pattern = re.compile(
        r"(\w+)\s+IN\s*\(\s*\{([^}]+)\}\s*\)", re.IGNORECASE
    )
    for m in measures:
        dax = m.get("expression", "") or ""
        for im in in_pattern.finditer(dax):
            col_name = im.group(1)
            values_str = im.group(2)
            values = [
                v.strip().strip('"').strip("'")
                for v in values_str.split(",")
            ]
            set_key = to_snake_case(col_name).upper()
            if values and set_key not in filter_sets:
                filter_sets[set_key] = sorted(set(values))

    return filter_sets


def derive_measure_resolutions(measures: list[dict]) -> dict[str, dict]:
    """Detect [MeasureRef] patterns in expressions → resolution map."""
    resolutions: dict[str, dict] = {}
    # Build lookup: name → measure
    measure_by_name = {m["measure_name"]: m for m in measures}

    ref_re = re.compile(r'\[([^\]]+)\]')

    for m in measures:
        dax = m.get("expression", "") or ""
        if not dax:
            continue

        for ref_match in ref_re.finditer(dax):
            ref = ref_match.group(1)
            if ref == m["measure_name"]:
                continue  # self-reference
            if ref in resolutions:
                continue

            matched = measure_by_name.get(ref)
            if matched:
                # It's a measure reference — add to resolutions
                matched_dax = matched.get("expression", "") or ""
                snippet = matched_dax[:150] + ("..." if len(matched_dax) > 150 else "")
                resolutions[ref] = {
                    "base_expr": "TODO: fill SQL expression",
                    "base_filters": [],
                    "_hint": (
                        f"Matches measure '{ref}' on table "
                        f"'{matched.get('table_name', '?')}' — "
                        f"DAX: {snippet}"
                    ),
                }

    return resolutions


def derive_column_overrides(
    measures: list[dict],
    admin_tables: dict[str, dict],
) -> dict[str, str]:
    """Detect mismatches between DAX Table[Column] references and admin schema."""
    overrides: dict[str, str] = {}

    # Build column index: {table_name: {snake_col, ...}}
    table_columns: dict[str, set[str]] = {}
    for tbl_name, tbl_info in admin_tables.items():
        cols = set()
        for col in tbl_info.get("columns", []):
            cols.add(to_snake_case(col["name"]))
            cols.add(col["name"])
        table_columns[tbl_name] = cols

    dax_ref_re = re.compile(r"'?(\w[\w\s]*?)'?\[(\w+)\]")

    for m in measures:
        dax = m.get("expression", "") or ""
        if not dax:
            continue
        for ref_match in dax_ref_re.finditer(dax):
            tbl_ref = ref_match.group(1).strip()
            dax_col = ref_match.group(2)
            dax_col_snake = to_snake_case(dax_col)

            # Find matching table
            target_table = ""
            for tbl_name in admin_tables:
                if (tbl_name == tbl_ref or
                        tbl_name.replace(" ", "_") == tbl_ref.replace(" ", "_")):
                    target_table = tbl_name
                    break
            if not target_table:
                continue

            sql_cols = table_columns.get(target_table, set())
            if not sql_cols:
                continue

            if dax_col_snake not in sql_cols and dax_col not in sql_cols:
                for sql_col in sql_cols:
                    if sql_col.lower() == dax_col_snake.lower():
                        overrides[f"{target_table}.{dax_col}"] = sql_col
                        break

    return overrides


def derive_mapping_only_tables(
    measures: list[dict],
    admin_tables: dict[str, dict],
) -> dict[str, dict]:
    """Measures allocated to tables not in admin scan → mapping_only_tables."""
    mapping_only: dict[str, dict] = {}

    allocated_tables: set[str] = set()
    for m in measures:
        table = m.get("table_name", "")
        if table:
            allocated_tables.add(table)

    admin_table_names = set(admin_tables.keys())

    for tbl in sorted(allocated_tables - admin_table_names):
        # Collect measure names for this table
        tbl_measures = [
            m["measure_name"] for m in measures if m.get("table_name") == tbl
        ]
        mapping_only[tbl] = {
            "source_table": "{catalog}.{schema}." + to_snake_case(tbl),
            "dimensions": "TODO: specify dimensions for this table",
            "aggregate_columns": "TODO: specify aggregate columns",
            "_hint": f"{len(tbl_measures)} measures: {', '.join(tbl_measures[:5])}",
        }

    return mapping_only


# ═══════════════════════════════════════════════════════════════════════
# API 3: Admin Scanner — Workspace Scan
# ═══════════════════════════════════════════════════════════════════════

def trigger_admin_scan(
    admin_token: str,
    workspace_id: str,
    poll_interval: int = 5,
    max_wait: int = 300,
) -> dict[str, Any]:
    """Trigger workspace scan, poll until complete, return result.

    Returns dict with 'datasets' list, each containing 'tables' with
    columns, M-Query expressions, etc.
    """
    base = "https://api.powerbi.com/v1.0/myorg/admin/workspaces"

    # Step 1: Trigger scan
    trigger_url = f"{base}/getInfo?datasetSchema=true&datasetExpressions=true"
    resp = requests.post(
        trigger_url,
        headers=_headers(admin_token),
        json={"workspaces": [workspace_id]},
        timeout=30,
    )
    _check_response(resp, "API 3 (Admin Scan trigger)")
    scan_id = resp.json().get("id")
    if not scan_id:
        raise RuntimeError(f"No scan ID returned: {resp.json()}")

    print(f"  Admin scan triggered: {scan_id}")

    # Step 2: Poll until complete
    status_url = f"{base}/scanStatus/{scan_id}"
    elapsed = 0
    while elapsed < max_wait:
        time.sleep(poll_interval)
        elapsed += poll_interval
        status_resp = requests.get(
            status_url, headers=_headers(admin_token), timeout=30
        )
        _check_response(status_resp, "API 3 (Admin Scan poll)")
        status = status_resp.json().get("status", "")
        if status == "Succeeded":
            break
        if status in ("Failed", "Error"):
            raise RuntimeError(f"Admin scan failed: {status_resp.json()}")
        print(f"  Scan status: {status} ({elapsed}s elapsed)")
    else:
        raise RuntimeError(f"Admin scan timed out after {max_wait}s")

    # Step 3: Get results
    result_url = f"{base}/scanResult/{scan_id}"
    result_resp = requests.get(
        result_url, headers=_headers(admin_token), timeout=60
    )
    _check_response(result_resp, "API 3 (Admin Scan result)")
    return result_resp.json()


def parse_admin_tables(
    scan_result: dict,
    dataset_id: str | None = None,
) -> dict[str, dict]:
    """Parse admin scan result into table-level dict.

    Args:
        scan_result: Raw scan result from admin API.
        dataset_id: Optional — filter to only this dataset's tables.
            If None, includes tables from ALL datasets in the workspace.

    Returns {table_name: {columns: [...], mquery: "...", measures: [...]}}.
    """
    tables: dict[str, dict] = {}

    workspaces = scan_result.get("workspaces", [])
    for ws in workspaces:
        for dataset in ws.get("datasets", []):
            # Filter by dataset_id if provided
            if dataset_id:
                ds_id = dataset.get("id", "")
                if ds_id.lower() != dataset_id.lower():
                    continue

            for tbl in dataset.get("tables", []):
                name = tbl.get("name", "")
                columns = []
                for col in tbl.get("columns", []):
                    columns.append({
                        "name": col.get("columnName", col.get("name", "")),
                        "dataType": col.get("dataType", ""),
                        "isHidden": col.get("isHidden", False),
                    })
                source_expr = ""
                for src in tbl.get("source", []):
                    source_expr = src.get("expression", "")

                tables[name] = {
                    "columns": columns,
                    "mquery_expression": source_expr,
                    "measures": tbl.get("measures", []),
                }
    return tables


def derive_column_metadata(admin_tables: dict[str, dict]) -> dict[str, dict]:
    """Column names → display_name + synonyms heuristics."""
    metadata: dict[str, dict] = {}

    # Collect all column names across tables
    all_columns: set[str] = set()
    for tbl in admin_tables.values():
        for col in tbl.get("columns", []):
            all_columns.add(col["name"])

    for col_name in sorted(all_columns):
        snake = to_snake_case(col_name)
        display = _humanize(snake)

        # Generate synonyms from common patterns
        synonyms: list[str] = []
        if "comp_code" in snake:
            synonyms.extend(["company", "entity code"])
        elif "fiscper" in snake or "fiscal_period" in snake:
            synonyms.extend(["period", "fiscal year period"])
        elif "plant" in snake:
            synonyms.extend(["production site", "factory"])
        elif "region" in snake:
            synonyms.extend(["geographic region", "area", "territory"])
        elif "country" in snake:
            synonyms.extend(["nation", "geography"])
        elif "version" in snake:
            synonyms.extend(["plan version", "data version", "scenario"])

        if snake not in metadata:
            metadata[snake] = {
                "display_name": display,
                "synonyms": synonyms,
            }

    return metadata


def derive_column_alias_map(admin_tables: dict[str, dict]) -> dict[str, str]:
    """Parse M-Query Table.RenameColumns steps for column renames."""
    alias_map: dict[str, str] = {}

    rename_re = re.compile(
        r'Table\.RenameColumns\s*\([^,]+,\s*\{([^}]+)\}',
        re.IGNORECASE | re.DOTALL,
    )
    pair_re = re.compile(r'\{"([^"]+)"\s*,\s*"([^"]+)"\}')

    for tbl in admin_tables.values():
        mquery = tbl.get("mquery_expression", "") or ""
        for rm in rename_re.finditer(mquery):
            pairs_str = rm.group(1)
            for pm in pair_re.finditer(pairs_str):
                old_name = pm.group(1)
                new_name = pm.group(2)
                if old_name != new_name:
                    alias_map[to_snake_case(new_name)] = to_snake_case(old_name)

    return alias_map


def derive_parameter_defaults(admin_tables: dict[str, dict]) -> dict[str, str]:
    """Extract ${Param} and #"Param" from M-Query expressions."""
    params: dict[str, str] = {}

    param_patterns = [
        re.compile(r'\$\{(\w+)\}'),
        re.compile(r'#"(\w+)"'),
        re.compile(r':(\w+(?:Filter|Version|Range))\b'),
    ]

    for tbl in admin_tables.values():
        mquery = tbl.get("mquery_expression", "") or ""
        for pat in param_patterns:
            for pm in pat.finditer(mquery):
                param_name = pm.group(1)
                if param_name not in params:
                    params[param_name] = "TODO: set default value"

    return params


def derive_name_prefixes(admin_tables: dict[str, dict]) -> list[str]:
    """Detect common column prefixes across tables → prefixes_to_strip."""
    prefix_counts: dict[str, int] = defaultdict(int)

    for tbl in admin_tables.values():
        for col in tbl.get("columns", []):
            name = to_snake_case(col["name"])
            # Check common prefixes
            for prefix in ("bic_", "khr", "kco", "kpe", "kfx", "kbi",
                           "zz_", "xx_", "bw_"):
                if name.startswith(prefix):
                    prefix_counts[prefix] += 1
                    break

    # Only keep prefixes that appear in >=3 columns
    return sorted(p for p, c in prefix_counts.items() if c >= 3)


def derive_dimension_exclusions(admin_tables: dict[str, dict]) -> dict[str, list[str]]:
    """Hidden columns per table → dimension_exclusions."""
    exclusions: dict[str, list[str]] = {}

    for tbl_name, tbl in admin_tables.items():
        hidden = []
        for col in tbl.get("columns", []):
            if col.get("isHidden", False):
                hidden.append(to_snake_case(col["name"]))
        if hidden:
            exclusions[tbl_name] = hidden

    return exclusions


def derive_period_dims(
    admin_tables: dict[str, dict],
) -> tuple[list[str], list[str]]:
    """Date/period columns → (period_dim_priority, int_period_dims)."""
    period_keywords = {"date", "period", "fiscper", "fiscal", "month", "year",
                       "quarter", "day", "time", "week"}
    date_types = {"datetime", "date", "dateTime", "DateTime"}
    int_types = {"int64", "int32", "integer", "Int64", "int", "long", "whole"}

    period_cols: list[str] = []
    int_period_cols: list[str] = []

    seen: set[str] = set()
    for tbl in admin_tables.values():
        for col in tbl.get("columns", []):
            name = col["name"]
            snake = to_snake_case(name)
            dtype = col.get("dataType", "")

            if snake in seen:
                continue

            is_period = False
            # Check name
            if any(kw in snake.lower() for kw in period_keywords):
                is_period = True
            # Check type
            if dtype in date_types:
                is_period = True

            if is_period:
                period_cols.append(snake)
                seen.add(snake)
                if dtype in int_types:
                    int_period_cols.append(snake)

    # Sort by priority heuristic: fiscper first, then date, then others
    def _priority(col: str) -> int:
        if "fiscper" in col:
            return 0
        if "fiscal" in col and "period" in col:
            return 1
        if "date" in col:
            return 2
        return 3

    period_cols.sort(key=_priority)
    int_period_cols.sort(key=_priority)

    return period_cols, int_period_cols


# ═══════════════════════════════════════════════════════════════════════
# API 4: Report Definition (optional)
# ═══════════════════════════════════════════════════════════════════════

def get_fabric_token(tenant_id: str, client_id: str, client_secret: str) -> str:
    """Acquire Fabric API token (different scope from PBI API)."""
    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    resp = requests.post(url, data={
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "https://api.fabric.microsoft.com/.default",
    }, timeout=30)
    _check_response(resp, "Auth (Fabric API)")
    return resp.json()["access_token"]


def extract_report_definition(
    token: str, workspace_id: str, report_id: str,
    tenant_id: str | None = None,
    client_id: str | None = None,
    client_secret: str | None = None,
) -> dict | None:
    """Get PBIR report definition → visual bindings.

    The Fabric getDefinition API requires a Fabric-scoped token. If
    tenant_id/client_id/client_secret are provided, acquires a separate
    Fabric token. Otherwise tries with the PBI token (may fail).

    Returns parsed report parts or None if unavailable.
    """
    import base64

    # Get Fabric token if credentials provided
    fabric_token = token
    if tenant_id and client_id and client_secret:
        try:
            fabric_token = get_fabric_token(tenant_id, client_id, client_secret)
        except Exception as e:
            print(f"  Fabric token failed ({e}), trying PBI token...")

    url = (
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}"
        f"/reports/{report_id}/getDefinition"
    )
    try:
        resp = requests.post(url, headers=_headers(fabric_token), timeout=120)

        # Handle 202 Accepted — long-running operation with polling
        if resp.status_code == 202:
            location = resp.headers.get("Location")
            if not location:
                print("  Report definition: 202 but no Location header")
                return None

            print(f"  Report definition: polling (202 Accepted)...")
            for attempt in range(60):
                time.sleep(2)
                poll_resp = requests.get(location, headers=_headers(fabric_token), timeout=30)
                poll_data = poll_resp.json()
                status = poll_data.get("status", "")
                if status == "Succeeded":
                    print(f"  Report definition: succeeded after {attempt + 1} poll(s)")
                    result_url = location + "/result"
                    result_resp = requests.get(
                        result_url, headers=_headers(fabric_token), timeout=60
                    )
                    _check_response(result_resp, "API 4 (Report Definition result)")
                    data = result_resp.json()
                    break
                elif status == "Failed":
                    error = poll_data.get("error", {})
                    print(f"  Report definition failed: {error}")
                    return None
            else:
                print("  Report definition: timed out after 120s")
                return None
        elif resp.status_code == 200:
            data = resp.json()
        else:
            _check_response(resp, "API 4 (Report Definition)")
            return None

        # Parse response: {definition: {parts: [{path, payload, payloadType}]}}
        definition = data.get("definition", data)
        parts = definition.get("parts", [])

        if not parts:
            print(f"  Report definition: no parts found (keys: {list(data.keys())})")
            return data

        # Decode base64 payloads into parsed JSON
        decoded_parts = []
        for part in parts:
            path = part.get("path", "")
            payload = part.get("payload", "")
            payload_type = part.get("payloadType", "")

            decoded = payload
            if payload_type == "InlineBase64" and payload:
                try:
                    decoded = base64.b64decode(payload).decode("utf-8")
                except Exception:
                    decoded = payload

            # Try to parse JSON payloads
            parsed = decoded
            if isinstance(decoded, str) and decoded.strip().startswith(("{", "[")):
                try:
                    parsed = json.loads(decoded)
                except json.JSONDecodeError:
                    parsed = decoded

            decoded_parts.append({
                "path": path,
                "payload": parsed,
            })

        print(f"  Report definition: {len(decoded_parts)} parts")
        paths = [p["path"] for p in decoded_parts[:10]]
        print(f"  Parts: {paths}")

        return {"definition": {"parts": decoded_parts}}

    except Exception as e:
        print(f"  Report definition unavailable: {e}")
        return None


def _extract_visual_synonym_map(report_def: dict | None) -> dict[str, dict]:
    """Parse PBIR report definition → {queryName: displayName} synonym map.

    Extracts displayName↔queryName pairs from visual configurations using the
    same extraction patterns as Tool 78 (PowerBIReportReferencesTool):
    - dataTransforms.selects[].displayName / queryName
    - singleVisual.projections[role][].queryRef
    - singleVisual.prototypeQuery.Select[].displayName / Name
    - objects.title[].properties.text.expr.Literal.Value

    Returns: {
        "Table.Field": {
            "display_name": "User-Facing Label",
            "table": "Table",
            "field": "Field",
            "role": "Values|Category|...",
            "visual_type": "barChart|...",
        }
    }
    """
    if not report_def:
        return {}

    synonym_map: dict[str, dict] = {}

    # PBIR getDefinition returns {definition: {parts: [...]}} or flat parts
    definition = report_def.get("definition", report_def)
    parts = definition.get("parts", [])

    if not parts:
        # Try flat PBIR structure: pages[] → visuals[]
        pages = definition.get("pages", [])
        for page in pages:
            for visual in page.get("visuals", []):
                _extract_synonyms_from_visual(visual, synonym_map)
        return synonym_map

    # Parse PBIR parts structure (from getDefinition API)
    for part in parts:
        path = part.get("path", "")
        payload = part.get("payload", "")

        if not payload:
            continue

        # visual.json files contain the visual config
        if "visual" in path.lower() and path.endswith(".json"):
            try:
                visual_data = json.loads(payload) if isinstance(payload, str) else payload
                _extract_synonyms_from_visual(visual_data, synonym_map)
            except (json.JSONDecodeError, TypeError):
                continue

        # report.json may contain embedded visuals
        if path.endswith("report.json") or path.endswith("definition.json"):
            try:
                report_data = json.loads(payload) if isinstance(payload, str) else payload
                # Embedded visuals in pages
                for section in report_data.get("sections", report_data.get("pages", [])):
                    for vc in section.get("visualContainers", section.get("visuals", [])):
                        config = vc.get("config", {})
                        if isinstance(config, str):
                            try:
                                config = json.loads(config)
                            except json.JSONDecodeError:
                                continue
                        _extract_synonyms_from_visual(config, synonym_map)
            except (json.JSONDecodeError, TypeError):
                continue

    return synonym_map


def _extract_synonyms_from_visual(
    visual: dict, synonym_map: dict[str, dict],
) -> None:
    """Extract displayName→queryName pairs from a single visual config."""
    config = visual.get("config", visual)
    if isinstance(config, str):
        try:
            config = json.loads(config)
        except (json.JSONDecodeError, TypeError):
            return

    sv = config.get("singleVisual", config)
    visual_type = sv.get("visualType", config.get("visualType", ""))

    # ── Method 1: dataTransforms.selects[] (richest source) ──
    data_transforms = sv.get("dataTransforms", {})
    selects = data_transforms.get("selects", [])
    for select in selects:
        if not isinstance(select, dict):
            continue
        display_name = select.get("displayName", "")
        query_name = select.get("queryName", "")
        if query_name and "." in query_name:
            table, field = query_name.split(".", 1)
            key = query_name
            if key not in synonym_map and display_name and display_name != field:
                synonym_map[key] = {
                    "display_name": display_name,
                    "table": table,
                    "field": field,
                    "visual_type": visual_type,
                }

    # ── Method 2: projections[role][].queryRef ──
    projections = sv.get("projections", {})
    for role, bindings in projections.items():
        if not isinstance(bindings, list):
            continue
        for binding in bindings:
            if not isinstance(binding, dict):
                continue
            query_ref = binding.get("queryRef", "")
            if query_ref and "." in query_ref:
                table, field = query_ref.split(".", 1)
                key = query_ref
                if key not in synonym_map:
                    synonym_map[key] = {
                        "display_name": _humanize(field),
                        "table": table,
                        "field": field,
                        "role": role,
                        "visual_type": visual_type,
                    }

    # ── Method 3: prototypeQuery.Select[].Name / displayName ──
    pq = sv.get("prototypeQuery", {})
    from_clause = pq.get("From", [])
    source_map: dict[str, str] = {}
    for from_item in from_clause:
        if isinstance(from_item, dict):
            entity = from_item.get("Entity", "")
            alias = from_item.get("Name", "")
            if entity and alias:
                source_map[alias] = entity

    select_clause = pq.get("Select", [])
    for item in select_clause:
        if not isinstance(item, dict):
            continue
        sel_name = item.get("Name", "")
        sel_display = item.get("displayName", "")

        # Extract table.field from Measure or Column refs
        for ref_type in ("Measure", "Column"):
            ref = item.get(ref_type, {})
            if not ref:
                continue
            prop = ref.get("Property", "")
            expr = ref.get("Expression", {})
            source_ref = expr.get("SourceRef", {})
            entity = source_ref.get("Entity", "")
            source = source_ref.get("Source", "")
            table = entity or source_map.get(source, "")

            if table and prop:
                key = f"{table}.{prop}"
                display = sel_display or sel_name or _humanize(prop)
                if key not in synonym_map and display != prop:
                    synonym_map[key] = {
                        "display_name": display,
                        "table": table,
                        "field": prop,
                        "visual_type": visual_type,
                    }


def derive_measure_metadata(report_def: dict | None) -> dict[str, dict]:
    """Visual display names → measure_metadata per table.

    Uses PBIR visual synonym extraction (same patterns as Tool 78)
    to build display_name + synonyms from how measures appear in reports.
    """
    synonym_map = _extract_visual_synonym_map(report_def)
    if not synonym_map:
        return {}

    # Group by table, collect measure synonyms
    metadata: dict[str, dict] = {}
    for query_name, info in synonym_map.items():
        table = info["table"]
        field = info["field"]
        display = info.get("display_name", "")
        snake = to_snake_case(field)

        if table not in metadata:
            metadata[table] = {}

        if snake not in metadata[table]:
            metadata[table][snake] = {
                "display_name": display or _humanize(snake),
                "synonyms": [],
            }

        # Add display_name as synonym if it differs
        entry = metadata[table][snake]
        if display and display != entry["display_name"] and display not in entry["synonyms"]:
            entry["synonyms"].append(display)
        # Also add the original PBI field name as synonym
        if field != snake and field not in entry["synonyms"]:
            entry["synonyms"].append(field)

    return metadata


def derive_dimension_metadata(report_def: dict | None) -> dict[str, dict]:
    """Visual column labels → dimension_metadata per table.

    Extracts dimension display names from report visual Axis/Category/Row
    bindings using the PBIR synonym map.
    """
    synonym_map = _extract_visual_synonym_map(report_def)
    if not synonym_map:
        return {}

    # Dimension roles in visuals (not Values/Y which are measures)
    dim_roles = {"Category", "Series", "Rows", "Columns", "Row", "Column",
                 "X", "Legend", "Tooltips", "Details"}

    metadata: dict[str, dict] = {}
    for query_name, info in synonym_map.items():
        role = info.get("role", "")
        # If role is known, use it to classify. If not, include all.
        if role and role not in dim_roles and role in ("Values", "Y", "Y2"):
            continue  # This is a measure, not a dimension

        table = info["table"]
        field = info["field"]
        display = info.get("display_name", "")
        snake = to_snake_case(field)

        if table not in metadata:
            metadata[table] = {}

        if snake not in metadata[table]:
            metadata[table][snake] = {
                "display_name": display or _humanize(snake),
                "comment": "",
                "synonyms": [],
            }

        entry = metadata[table][snake]
        if display and display != entry["display_name"] and display not in entry["synonyms"]:
            entry["synonyms"].append(display)
        if field != snake and field not in entry["synonyms"]:
            entry["synonyms"].append(field)

    return metadata


# ═══════════════════════════════════════════════════════════════════════
# Assembly — Build the full 26-key config
# ═══════════════════════════════════════════════════════════════════════

def build_config(
    relationships: list[dict],
    measures: list[dict],
    admin_tables: dict[str, dict],
    report_def: dict | None = None,
    catalog: str = "main",
    schema: str = "default",
) -> dict[str, Any]:
    """Assemble all config keys into pipeline_config.json format."""
    config: dict[str, Any] = {}

    # ── Auto-derived keys ──────────────────────────────────────────

    # 1. join_key_map
    config["join_key_map"] = derive_join_key_map(relationships, admin_tables)

    # 2. fact_join_map (human-input heavy — provide skeleton with TODO)
    fact_tables = _identify_fact_tables(relationships, admin_tables)
    config["fact_join_map"] = _build_fact_join_map_skeleton(
        fact_tables, relationships, measures
    )

    # 3. enrichment_joins
    config["enrichment_joins"] = derive_enrichment_joins(
        relationships, admin_tables
    )

    # 4. filter_sets
    switch_decomps = derive_switch_decompositions(measures)
    config["filter_sets"] = derive_filter_sets(measures, switch_decomps)

    # 5. switch_decompositions
    # Clean internal keys before adding to config
    clean_decomps: dict[str, list[dict]] = {}
    for table_key, decomps in switch_decomps.items():
        clean_list = []
        for d in decomps:
            clean = {k: v for k, v in d.items() if not k.startswith("_")}
            clean_list.append(clean)
        clean_decomps[table_key] = clean_list
    config["switch_decompositions"] = clean_decomps

    # 6. column_overrides
    config["column_overrides"] = derive_column_overrides(measures, admin_tables)

    # 7. measure_resolutions
    raw_resolutions = derive_measure_resolutions(measures)
    # Clean internal hints
    clean_resolutions: dict[str, dict] = {}
    for ref, entry in raw_resolutions.items():
        clean_resolutions[ref] = {
            k: v for k, v in entry.items() if not k.startswith("_")
        }
    config["measure_resolutions"] = clean_resolutions

    # 8. mapping_only_tables
    raw_mapping = derive_mapping_only_tables(measures, admin_tables)
    clean_mapping: dict[str, dict] = {}
    for tbl, entry in raw_mapping.items():
        clean_mapping[tbl] = {
            k: v for k, v in entry.items() if not k.startswith("_")
        }
    config["mapping_only_tables"] = clean_mapping

    # 9. dimension_exclusions
    config["dimension_exclusions"] = derive_dimension_exclusions(admin_tables)

    # 10. measure_metadata
    config["measure_metadata"] = derive_measure_metadata(report_def)

    # 11. comment_overrides
    config["comment_overrides"] = {}

    # 12. dimension_metadata
    config["dimension_metadata"] = derive_dimension_metadata(report_def)

    # 13. dimension_order
    config["dimension_order"] = {}

    # 14. column_metadata
    config["column_metadata"] = derive_column_metadata(admin_tables)

    # 15. column_alias_map
    config["column_alias_map"] = derive_column_alias_map(admin_tables)

    # 16. name_prefixes_to_strip
    config["name_prefixes_to_strip"] = derive_name_prefixes(admin_tables)

    # 17. parameter_defaults
    config["parameter_defaults"] = derive_parameter_defaults(admin_tables)

    # 18. dim_alias_map
    config["dim_alias_map"] = derive_dim_alias_map(relationships)

    # 19. period_dim_priority + 20. int_period_dims
    period_dims, int_dims = derive_period_dims(admin_tables)
    config["period_dim_priority"] = period_dims
    config["int_period_dims"] = int_dims

    # ── Human-input keys (TODO markers) ──────────────────────────

    # 21. percentage_multiplier_patterns
    config["percentage_multiplier_patterns"] = [
        "TODO: regex for measures needing *100 (e.g., turnover(?!.*_bp$))"
    ]

    # 22. budget_suffix
    config["budget_suffix"] = _detect_budget_suffix(measures)

    # 23. cwc_filter_column
    config["cwc_filter_column"] = _detect_cwc_filter_column(measures, admin_tables)

    # 24. switch_join_alias
    config["switch_join_alias"] = (
        "TODO: SWITCH decomposition join alias"
        if switch_decomps
        else None
    )

    # 25. switch_join_col
    config["switch_join_col"] = (
        "TODO: SWITCH decomposition join column"
        if switch_decomps
        else None
    )

    # 26. manual_overrides
    complex_measures = _find_complex_measures(measures)
    config["manual_overrides"] = _build_manual_overrides_skeleton(complex_measures)

    return config


def _identify_fact_tables(
    relationships: list[dict],
    admin_tables: dict[str, dict],
) -> set[str]:
    """Identify fact tables (appear on 'many' side of relationships)."""
    facts: set[str] = set()
    for rel in relationships:
        from_card = str(rel.get("from_cardinality", "")).lower()
        to_card = str(rel.get("to_cardinality", "")).lower()

        if "many" in from_card:
            facts.add(rel["from_table"])
        if "many" in to_card:
            facts.add(rel["to_table"])

    return facts


def _build_fact_join_map_skeleton(
    fact_tables: set[str],
    relationships: list[dict],
    measures: list[dict],
) -> dict[str, dict]:
    """Build fact_join_map with TODO markers for grain decisions."""
    fact_join_map: dict[str, dict] = {}

    # Find which dim tables each fact connects to
    fact_dims: dict[str, list[str]] = defaultdict(list)
    for rel in relationships:
        from_card = str(rel.get("from_cardinality", "")).lower()
        to_card = str(rel.get("to_cardinality", "")).lower()

        if "many" in to_card and "one" in from_card:
            fact_dims[rel["to_table"]].append(rel["from_table"])
        elif "many" in from_card and "one" in to_card:
            fact_dims[rel["from_table"]].append(rel["to_table"])

    # Find measures per fact table
    fact_measures: dict[str, list[str]] = defaultdict(list)
    for m in measures:
        table = m.get("table_name", "")
        if table in fact_tables:
            fact_measures[table].append(m["measure_name"])

    for fact in sorted(fact_tables):
        dims = fact_dims.get(fact, [])
        ms = fact_measures.get(fact, [])
        fact_join_map[fact] = {
            "alias": to_snake_case(fact),
            "join_key": (
                f"TODO: grain decision — {fact} connects to dims "
                f"[{', '.join(dims[:5])}], has measures "
                f"[{', '.join(ms[:5])}]"
            ),
        }

    return fact_join_map


def _detect_budget_suffix(measures: list[dict]) -> str:
    """Heuristic: look for BP/RE/FC suffixes in measure names."""
    bp_count = 0
    re_count = 0
    fc_count = 0

    for m in measures:
        name = m.get("measure_name", "").upper()
        dax = (m.get("expression", "") or "").upper()
        if "_BP" in name or "B000" in dax or "BUDGET" in name:
            bp_count += 1
        if "_RE" in name or "'RE'" in dax:
            re_count += 1
        if "_FC" in name or "FORECAST" in name:
            fc_count += 1

    if bp_count > 0:
        return "_bp"
    if re_count > 0:
        return "_re"
    if fc_count > 0:
        return "_fc"
    return "TODO: budget variant suffix (e.g., _bp, _re, _fc)"


def _detect_cwc_filter_column(
    measures: list[dict],
    admin_tables: dict[str, dict],
) -> str:
    """Heuristic: look for CWC or work-center-type filter columns."""
    # Check measure DAX for common filter patterns
    for m in measures:
        dax = m.get("expression", "") or ""
        # Look for cwc_type, wc_type, bic_cwc_type patterns
        cwc_match = re.search(
            r"(\w*cwc[_\s]*type\w*)", dax, re.IGNORECASE
        )
        if cwc_match:
            return to_snake_case(cwc_match.group(1))

    # Check admin tables for columns matching CWC pattern
    for tbl in admin_tables.values():
        for col in tbl.get("columns", []):
            name = col["name"].lower()
            if "cwc" in name and "type" in name:
                return to_snake_case(col["name"])

    return "TODO: CWC filter column if applicable"


def _find_complex_measures(measures: list[dict]) -> list[dict]:
    """Identify measures too complex for auto-translation (need manual override)."""
    complex_measures: list[dict] = []

    complex_patterns = [
        r'CALCULATE\s*\(',
        r'FILTER\s*\(\s*ALL',
        r'SUMX\s*\(',
        r'RANKX\s*\(',
        r'TOPN\s*\(',
        r'EARLIER\s*\(',
        r'USERELATIONSHIP\s*\(',
        r'CROSSJOIN\s*\(',
    ]
    complex_re = re.compile('|'.join(complex_patterns), re.IGNORECASE)

    for m in measures:
        dax = m.get("expression", "") or ""
        if not dax:
            continue
        if complex_re.search(dax):
            complex_measures.append(m)

    return complex_measures


def _build_manual_overrides_skeleton(
    complex_measures: list[dict],
) -> dict[str, list[dict]]:
    """Build manual_overrides with TODO markers for complex measures."""
    overrides: dict[str, list[dict]] = defaultdict(list)

    for m in complex_measures:
        table = m.get("table_name", "unknown")
        dax = m.get("expression", "") or ""
        snippet = dax[:200] + ("..." if len(dax) > 200 else "")
        overrides[table].append({
            "name": to_snake_case(m["measure_name"]),
            "expr": f"TODO: complex measure '{m['measure_name']}' — DAX: {snippet}",
            "comment": m.get("description", "") or m["measure_name"],
        })

    return dict(overrides)


# ═══════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description=(
            "Generate pipeline_config.json from Power BI APIs. "
            "No CrewAI, no LLM — pure API extraction."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Example:\n"
            "  python generate_config.py \\\n"
            "    --workspace-id ac0fa11c-... \\\n"
            "    --dataset-id ecdd57ae-... \\\n"
            "    --tenant-id 9f37a392-... \\\n"
            "    --client-id 7b597aac-... \\\n"
            "    --client-secret 'U5b8Q~...' \\\n"
            "    --admin-client-id 8d8aa6ee-... \\\n"
            "    --admin-client-secret 'RXm8Q~...' \\\n"
            "    --catalog david_test_metrics \\\n"
            "    --schema cchbc \\\n"
            "    --output proposed_pipeline_config.json"
        ),
    )

    parser.add_argument("--workspace-id", required=True, help="PBI workspace GUID")
    parser.add_argument("--dataset-id", required=True, help="PBI dataset GUID")
    parser.add_argument("--tenant-id", required=True, help="Azure AD tenant GUID")
    parser.add_argument(
        "--client-id", required=True,
        help="Non-admin SP client ID (workspace member, Execute Queries)",
    )
    parser.add_argument("--client-secret", required=True, help="Non-admin SP secret")
    parser.add_argument(
        "--admin-client-id", required=True,
        help="Admin SP client ID (Admin Scanner API)",
    )
    parser.add_argument("--admin-client-secret", required=True, help="Admin SP secret")
    parser.add_argument(
        "--catalog", default="main",
        help="Target UC catalog name (default: main)",
    )
    parser.add_argument(
        "--schema", default="default",
        help="Target UC schema name (default: default)",
    )
    parser.add_argument(
        "--output", default="proposed_pipeline_config.json",
        help="Output file path (default: proposed_pipeline_config.json)",
    )
    parser.add_argument(
        "--report-id", default=None,
        help="Optional PBI report GUID for report-layer metadata",
    )

    args = parser.parse_args()

    print("=" * 60)
    print("Pipeline Config Generator — PBI API Extraction")
    print("=" * 60)

    # ── Step 1: Auth ──────────────────────────────────────────────
    print("\n[1/4] Authenticating...")
    token = get_token(args.tenant_id, args.client_id, args.client_secret)
    admin_token = get_token(
        args.tenant_id, args.admin_client_id, args.admin_client_secret
    )
    print("  OK — both tokens acquired")

    # ── Step 2: Extract from APIs ────────────────────────────────
    print("\n[2/4] Extracting from Power BI APIs...")

    print("  API 1: INFO.VIEW.RELATIONSHIPS()...")
    relationships = extract_relationships(token, args.workspace_id, args.dataset_id)
    print(f"    → {len(relationships)} relationships")

    print("  API 2: $SYSTEM.MDSCHEMA_MEASURES...")
    measures = extract_measures(token, args.workspace_id, args.dataset_id)
    print(f"    → {len(measures)} measures")

    print("  API 3: Admin Scanner (workspace scan)...")
    scan_result = trigger_admin_scan(admin_token, args.workspace_id)
    admin_tables = parse_admin_tables(scan_result, dataset_id=args.dataset_id)
    print(f"    → {len(admin_tables)} tables in admin scan")

    report_def = None
    if args.report_id:
        print("  API 4: Report Definition...")
        report_def = extract_report_definition(
            token, args.workspace_id, args.report_id,
            tenant_id=args.tenant_id, client_id=args.client_id,
            client_secret=args.client_secret,
        )
        if report_def:
            print("    → Report definition retrieved")
    else:
        print("  API 4: Report Definition — skipped (no --report-id)")

    # ── Step 3: Build config ─────────────────────────────────────
    print("\n[3/4] Deriving config keys...")
    config = build_config(
        relationships, measures, admin_tables, report_def,
        catalog=args.catalog, schema=args.schema,
    )

    # Print per-key summary
    for key, val in config.items():
        if val is None:
            status = "null"
        elif isinstance(val, dict):
            status = f"{len(val)} entries"
        elif isinstance(val, list):
            status = f"{len(val)} items"
        elif isinstance(val, str) and "TODO" in val:
            status = "TODO"
        elif isinstance(val, str):
            status = f'"{val}"'
        else:
            status = str(val)
        print(f"  {key}: {status}")

    # ── Step 4: Output ───────────────────────────────────────────
    print(f"\n[4/4] Writing {args.output}...")
    with open(args.output, "w") as f:
        json.dump(config, f, indent=2, default=str)

    # ── Summary ──────────────────────────────────────────────────
    config_json = json.dumps(config, default=str)
    auto_count = 0
    todo_count = 0
    for key, val in config.items():
        val_str = json.dumps(val, default=str)
        if "TODO" in val_str:
            todo_count += 1
        elif val:
            auto_count += 1

    print(f"\n{'=' * 60}")
    print(f"CONFIG SUMMARY")
    print(f"{'=' * 60}")
    print(f"  Total keys: {len(config)}")
    print(f"  Auto-filled: {auto_count}")
    print(f"  Need human review (TODO): {todo_count}")
    print(f"  Empty/null: {len(config) - auto_count - todo_count}")
    print(f"  Output: {args.output}")
    print(f"\nTotal TODO markers in output: {config_json.count('TODO')}")
    print()


if __name__ == "__main__":
    main()
