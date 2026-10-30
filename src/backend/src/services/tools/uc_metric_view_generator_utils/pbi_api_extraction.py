"""Power BI API extraction for the UC Metric View generator (API mode).

Pulls measures, M-Query sources, relationships and scan data straight from the
Power BI API when the caller supplies a workspace/dataset and credentials
instead of pre-extracted JSON. Each source falls back to the TMDL/admin paths in
:mod:`pbi_extraction_fallbacks` when the primary path comes back empty.
"""

from __future__ import annotations

import logging
import re
import urllib.parse
from collections.abc import Callable
from types import ModuleType

from .pbi_extraction_fallbacks import extract_measures_fallback, extract_mquery_fallback
from .shared_types import RunAsync

logger = logging.getLogger(__name__)

UUID_PATTERN = re.compile(r"^[a-fA-F0-9\-]{8,50}$")
ALLOWED_PBI_DOMAINS = frozenset(
    {
        "api.powerbi.com",
        "api.powerbigov.us",
        "api.powerbi.cn",
        "api.powerbi.de",
        "api.microsoftcloud.de",
    }
)


def validate_pbi_inputs(
    workspace_id: str, dataset_id: str, pbi_api_base_url: str
) -> tuple[bool, str]:
    """Validate PBI API inputs to prevent SSRF."""
    if not UUID_PATTERN.match(workspace_id):
        return False, f"Invalid workspace_id format: {workspace_id}"
    if not UUID_PATTERN.match(dataset_id):
        return False, f"Invalid dataset_id format: {dataset_id}"
    parsed = urllib.parse.urlparse(pbi_api_base_url or "https://api.powerbi.com")
    if parsed.hostname not in ALLOWED_PBI_DOMAINS:
        return False, f"Untrusted PBI API domain: {parsed.hostname}"
    return True, ""


def _measures_from_connector(
    workspace_id: str, dataset_id: str, token: str
) -> list[dict[str, object]]:
    """Measures via the Execute Queries / XMLA connector."""
    from src.services.converters.formats.powerbi.connector import PowerBIConnector

    connector = PowerBIConnector(
        semantic_model_id=dataset_id,
        group_id=workspace_id,
        access_token=token,
    )
    connector.connect()
    kpis = connector.extract_measures(include_hidden=True)

    measures: list[dict[str, object]] = []
    for kpi in kpis:
        # kpi.technical_name is derived from the actual PBI measure name (snake_cased).
        # kpi.description may contain a textual description, not the measure name.
        measure_name = kpi.technical_name or kpi.description
        measures.append(
            {
                "measure_name": measure_name,
                "original_name": measure_name,
                "dax_expression": kpi.formula or "",
                "proposed_allocation": kpi.source_table or "__unassigned__",
                "table_refs": [],
            }
        )
    return measures


def _mquery_from_admin_scan(
    result: dict[str, object],
    workspace_id: str,
    dataset_id: str,
    token: str,
    run_async: RunAsync,
) -> None:
    """MQuery entries and raw scan data via the Admin API scanner (mutates result)."""
    from src.services.converters.formats.mquery.scanner import PowerBIAdminScanner

    scanner = PowerBIAdminScanner(access_token=token)

    scan_result, raw_scan = run_async(
        scanner.scan_workspace(workspace_id, dataset_id=dataset_id)
    )

    if raw_scan:
        result["scan_data"] = raw_scan

    mquery_entries: list[dict[str, object]] = []
    for model in scan_result:
        for table in model.tables:
            for expr in table.source_expressions:
                mquery_entries.append(
                    {
                        "table_name": table.name,
                        "transpiled_sql": expr.embedded_sql
                        or expr.raw_expression
                        or "",
                        "validation_passed": ("Yes" if expr.embedded_sql else "No"),
                    }
                )
    result["mquery"] = mquery_entries
    logger.info(
        "[UCMV] Extracted %s MQuery tables from PBI Admin API", len(mquery_entries)
    )


def _relationships_from_api(
    result: dict[str, object],
    workspace_id: str,
    dataset_id: str,
    token: str,
    pbi_api_base_url: str,
) -> None:
    """Relationships via an INFO.VIEW.RELATIONSHIPS Execute Queries call (mutates result)."""
    import requests as req_lib

    pbi_api_base = pbi_api_base_url or "https://api.powerbi.com/v1.0/myorg"
    url = f"{pbi_api_base}/groups/{workspace_id}/datasets/{dataset_id}/executeQueries"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    payload = {
        "queries": [{"query": "EVALUATE INFO.VIEW.RELATIONSHIPS()"}],
        "serializerSettings": {"includeNulls": True},
    }

    resp = req_lib.post(url, headers=headers, json=payload, timeout=60)  # ids UUID-checked, host allow-listed by validate_pbi_inputs — nosemgrep: tmp.databricks.ssrf.requests-user-url  # fmt: skip
    if resp.status_code == 200:
        raw = resp.json()
        rows = raw.get("results", [{}])[0].get("tables", [{}])[0].get("rows", [])
        relationships: list[dict[str, object]] = []
        for row in rows:
            relationships.append(
                {
                    "from_table": row.get("[FromTable]", ""),
                    "from_column": row.get("[FromColumn]", ""),
                    "from_cardinality": row.get("[FromCardinality]", "Many"),
                    "to_table": row.get("[ToTable]", ""),
                    "to_column": row.get("[ToColumn]", ""),
                    "to_cardinality": row.get("[ToCardinality]", "One"),
                    "is_active": row.get("[IsActive]", True),
                }
            )
        result["relationships"] = relationships
        logger.info(
            "[UCMV] Extracted %s relationships from PBI API", len(relationships)
        )
    else:
        logger.warning("[UCMV] Relationships API returned %s", resp.status_code)


def extract_from_pbi_api(
    *,
    workspace_id: str,
    dataset_id: str,
    tenant_id: str,
    client_id: str,
    client_secret: str,
    username: str,
    password: str,
    auth_method: str | None,
    access_token: str,
    load_generate_config: Callable[[], ModuleType],
    run_async: RunAsync,
    pbi_api_base_url: str = "",
    admin_client_id: str = "",
    admin_client_secret: str = "",
) -> dict[str, object]:
    """Extract measures, MQuery, relationships, and scan data from PBI API.

    Returns dict with keys: measures, mquery, relationships, scan_data
    """
    auth_config: dict[str, str | None] = {
        "tenant_id": tenant_id,
        "client_id": client_id,
        "client_secret": client_secret,
        "username": username,
        "password": password,
        "auth_method": auth_method,
        "access_token": access_token,
    }

    result: dict[str, object] = {}

    # Obtain an OAuth token (unless a pre-obtained one was supplied)
    token = access_token
    if not token:
        from src.services.tools.powerbi_auth_utils import (
            get_powerbi_access_token_from_config,
        )

        token = run_async(get_powerbi_access_token_from_config(auth_config))

    # 1. Extract measures via Execute Queries API
    try:
        measures = _measures_from_connector(workspace_id, dataset_id, token)
        result["measures"] = measures
        logger.info("[UCMV] Extracted %s measures from PBI API", len(measures))
    except Exception as e:
        logger.warning("[UCMV] Measure extraction failed: %s", e, exc_info=True)

    # 1b. Measure DAX fallback — the Execute Queries / XMLA path above is
    # frequently rejected for Service-Account (ROPC) tokens, yielding zero
    # measures. Recover via (a) Fabric TMDL (which an SA CAN read) and, if a
    # client_secret is available, (b) a Service-Principal retry.
    if not result.get("measures"):
        recovered = extract_measures_fallback(
            load_generate_config=load_generate_config,
            workspace_id=workspace_id,
            dataset_id=dataset_id,
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret,
            username=username,
            password=password,
        )
        if recovered:
            result["measures"] = recovered
            logger.info(
                "[UCMV] Recovered %s measures via TMDL/SP fallback", len(recovered)
            )

    # 2. Extract MQuery via Admin API scan
    try:
        _mquery_from_admin_scan(result, workspace_id, dataset_id, token, run_async)
    except Exception as e:
        logger.warning("[UCMV] MQuery extraction failed: %s", e, exc_info=True)

    # 2b. MQuery fallback — the Admin Scanner (used above) rejects
    # Service-Account tokens (401/403), leaving mquery empty. Without MQuery
    # no fact table is detected and the generator emits 0 views even when
    # measures were extracted. Recover the table source expressions via
    # Fabric TMDL (SA-readable) or a Service-Principal retry, mirroring the
    # measure fallback.
    if not result.get("mquery"):
        recovered_mq = extract_mquery_fallback(
            load_generate_config=load_generate_config,
            run_async=run_async,
            workspace_id=workspace_id,
            dataset_id=dataset_id,
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret,
            username=username,
            password=password,
            admin_client_id=admin_client_id,
            admin_client_secret=admin_client_secret,
        )
        if recovered_mq:
            result["mquery"] = recovered_mq
            logger.info(
                "[UCMV] Recovered %s MQuery tables via TMDL/SP fallback",
                len(recovered_mq),
            )

    # 3. Extract relationships via Execute Queries API
    try:
        _relationships_from_api(
            result, workspace_id, dataset_id, token, pbi_api_base_url
        )
    except Exception as e:
        logger.warning("[UCMV] Relationship extraction failed: %s", e, exc_info=True)

    return result
