"""Power BI model recovery when the primary API path yields nothing.

Measures and M-Query sources come back empty for Service-Account tokens, which
the Execute Queries and Admin Scanner paths reject. These fallbacks recover them
through Fabric TMDL (readable with a Service Account, or a Service Principal),
and finally through the tenant-admin scanner retried with a distinct admin
Service Principal.

The config-generator module is passed in as a loader so the owning tool can
supply it (and tests can substitute it).
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from types import ModuleType

from .shared_types import RunAsync, as_dict, as_list, as_str

logger = logging.getLogger(__name__)


def import_generate_config() -> ModuleType:
    """Load the shared Power BI library through its canonical package."""
    from src.services.powerbi import pipeline_config

    return pipeline_config


def tmdl_tables_to_measures(admin_tables: object) -> list[dict[str, object]]:
    """Convert generate_config TMDL/admin table dict → UCMV measure shape."""
    measures: list[dict[str, object]] = []
    for tbl_name, tbl_info in as_dict(admin_tables).items():
        for m_value in as_list(as_dict(tbl_info).get("measures")):
            m = as_dict(m_value)
            name = m.get("name", "")
            if not name:
                continue
            measures.append(
                {
                    "measure_name": name,
                    "original_name": name,
                    "dax_expression": m.get("expression", "") or "",
                    "proposed_allocation": tbl_name or "__unassigned__",
                    "table_refs": [],
                }
            )
    return measures


def tmdl_tables_to_mquery(admin_tables: object) -> list[dict[str, object]]:
    """Convert generate_config TMDL table dict → UCMV mquery entry shape.

    UCMV expects [{table_name, transpiled_sql, validation_passed}].

    IMPORTANT: validation_passed MUST start with 'Yes'. MQueryParser.parse_json
    silently drops any entry whose validation_passed is not 'Yes...' unless the
    SQL contains both SUM( and GROUP BY. The earlier 'No' value meant every
    TMDL-recovered table was discarded → the fallback recovered rows but the
    parser produced zero tables → 0 fact tables → 0 views. The source here is
    the authoritative partition expression (embedded native SQL where the
    datasource is a SQL DB, otherwise raw M), so mark it accepted and let the
    parser extract what it can.
    """
    entries: list[dict[str, object]] = []
    for tbl_name, tbl_info in as_dict(admin_tables).items():
        src = as_str(as_dict(tbl_info).get("mquery_expression")).strip()
        if not src:
            continue
        entries.append(
            {
                "table_name": tbl_name,
                "transpiled_sql": src,
                "validation_passed": "Yes",
            }
        )
    return entries


def extract_mquery_fallback(
    *,
    load_generate_config: Callable[[], ModuleType],
    run_async: RunAsync,
    workspace_id: str,
    dataset_id: str,
    tenant_id: str,
    client_id: str,
    client_secret: str,
    username: str,
    password: str,
    admin_client_id: str = "",
    admin_client_secret: str = "",
) -> list[dict[str, object]]:
    """Recover MQuery/table-source when the Admin Scanner fails for a Service Account.

    Tier 1: Fabric TMDL with the SA (works if the workspace is Fabric-enabled).
    Tier 2: Fabric TMDL with a Service Principal (if client_secret provided).
    Reuses generate_config so the logic is shared with the config generator.
    """
    try:
        gen = load_generate_config()
    except Exception as e:
        logger.warning(
            "[UCMV] Could not load generate_config for MQuery fallback: %s",
            e,
            exc_info=True,
        )
        return []

    for label, sa_user, sa_pw, sp_secret in (
        ("SA", username, password, None),
        ("SP", None, None, client_secret),
    ):
        if label == "SA" and not (sa_user and sa_pw):
            continue
        if label == "SP" and not sp_secret:
            continue
        try:
            fabric_token = gen.get_fabric_token(
                tenant_id,
                client_id,
                sp_secret,
                username=sa_user,
                password=sa_pw,
            )
            tmdl_parts = gen.fetch_tmdl_parts(fabric_token, workspace_id, dataset_id)
            if tmdl_parts:
                tables = gen.parse_tmdl_to_admin_tables(
                    tmdl_parts, dataset_id=dataset_id
                )
                entries = tmdl_tables_to_mquery(tables)
                if entries:
                    logger.info(
                        "[UCMV] MQuery TMDL fallback (%s) recovered %s tables",
                        label,
                        len(entries),
                    )
                    return entries
        except Exception as e:
            logger.warning(
                "[UCMV] MQuery TMDL fallback (%s) failed: %s",
                label,
                e,
                exc_info=True,
            )

    # Tier 3: retry the Admin Scanner with a distinct admin Service Principal
    # (admin_client_id/admin_client_secret) when tiers 1+2 came up empty. Some
    # workspaces gate tenant-admin API access to a narrower allow-list than
    # ordinary dataset read — the primary token 401s on getInfo specifically —
    # AND aren't Fabric-enabled (TMDL unavailable), so only an admin SP recovers
    # MQuery. Distinct from client_id/client_secret on purpose.
    if admin_client_id and admin_client_secret:
        try:
            from src.services.tools.powerbi_auth_utils import (
                get_powerbi_access_token_from_config,
            )

            sp_admin_token = run_async(
                get_powerbi_access_token_from_config(
                    {
                        "tenant_id": tenant_id,
                        "client_id": admin_client_id,
                        "client_secret": admin_client_secret,
                        "username": None,
                        "password": None,
                        "auth_method": "service_principal",
                        "access_token": None,
                    }
                )
            )
            scan_result = gen.trigger_admin_scan(sp_admin_token, workspace_id)
            tables = gen.parse_admin_tables(scan_result, dataset_id=dataset_id)
            entries = tmdl_tables_to_mquery(tables)
            if entries:
                logger.info(
                    "[UCMV] MQuery Admin Scanner (SP admin retry) recovered %s tables",
                    len(entries),
                )
                return entries
        except Exception as e:
            logger.warning(
                "[UCMV] MQuery Admin Scanner (SP admin retry) failed: %s",
                e,
                exc_info=True,
            )
    return []


def extract_measures_fallback(
    *,
    load_generate_config: Callable[[], ModuleType],
    workspace_id: str,
    dataset_id: str,
    tenant_id: str,
    client_id: str,
    client_secret: str,
    username: str,
    password: str,
) -> list[dict[str, object]]:
    """Recover measure DAX when Execute Queries/XMLA fails for a Service Account.

    Tier 1: Fabric TMDL with the SA (works if the workspace is Fabric-enabled).
    Tier 2: Fabric TMDL with a Service Principal (if client_secret provided).
    Both reuse the standalone helpers in generate_config so the logic is
    shared with the Pipeline Config Generator.
    """
    try:
        gen = load_generate_config()
    except Exception as e:
        logger.warning(
            "[UCMV] Could not load generate_config for fallback: %s",
            e,
            exc_info=True,
        )
        return []

    # Tier 1: TMDL with whatever creds are present (SA preferred, else SP).
    for label, sa_user, sa_pw, sp_secret in (
        ("SA", username, password, None),
        ("SP", None, None, client_secret),
    ):
        if label == "SA" and not (sa_user and sa_pw):
            continue
        if label == "SP" and not sp_secret:
            continue
        try:
            fabric_token = gen.get_fabric_token(
                tenant_id,
                client_id,
                sp_secret,
                username=sa_user,
                password=sa_pw,
            )
            tmdl_parts = gen.fetch_tmdl_parts(fabric_token, workspace_id, dataset_id)
            if tmdl_parts:
                tables = gen.parse_tmdl_to_admin_tables(
                    tmdl_parts, dataset_id=dataset_id
                )
                measures = tmdl_tables_to_measures(tables)
                if measures:
                    logger.info(
                        "[UCMV] TMDL fallback (%s) recovered %s measures",
                        label,
                        len(measures),
                    )
                    return measures
        except Exception as e:
            logger.warning(
                "[UCMV] TMDL fallback (%s) failed: %s",
                label,
                e,
                exc_info=True,
            )
    return []
