"""Fetching the semantic model: tokens, the three model sources (Fabric TMDL,
the admin scanner, the Power BI DAX endpoint) and the metadata that enriches it.

Mixed into ``PowerBIAnalysisTool`` rather than composed, so this is pure
movement: every method still reads ``self`` exactly as it did in the single
3,506-line file, and every ``tool._method(...)`` call site is unchanged.
"""

import asyncio
import base64
import contextvars
import logging
import json
import re
from typing import Any, Optional, Type, Dict, List
from concurrent.futures import ThreadPoolExecutor
from datetime import date

from src.services.tools.base import BaseTool
from pydantic import BaseModel, Field, PrivateAttr
import httpx

from src.services.tools.tool_session_provider import ToolSessionProvider


logger = logging.getLogger(__name__)


class PowerBIModelFetchMixin:
    async def _get_access_token(self, config: Dict[str, Any]) -> str:
        """
        Get OAuth access token using centralized AadService.

        Supports three authentication methods:
        1. Pre-obtained access_token (User OAuth)
        2. Service Principal (client credentials)
        3. Service Account (username/password ROPC flow)

        Uses the shared powerbi_auth_utils module for consistent authentication
        across all Power BI tools.
        """
        from src.services.tools.powerbi_auth_utils import get_powerbi_access_token_from_config
        return await get_powerbi_access_token_from_config(config)

    async def _get_fabric_token(self, config: Dict[str, Any]) -> str:
        """
        Get Fabric API token for TMDL access.

        Uses the shared powerbi_auth_utils module for consistent authentication.
        Supports Service Principal and Service Account authentication.
        """
        from src.services.tools.powerbi_auth_utils import get_fabric_access_token_from_config
        return await get_fabric_access_token_from_config(config)

    async def _fetch_tmdl_via_fabric(
        self,
        workspace_id: str,
        dataset_id: str,
        access_token: str
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Fetch TMDL definition from the Fabric REST API.

        Returns:
            List of TMDL parts when Fabric API responds successfully (may be empty for
            models with no tables yet).  Returns None when the workspace is not
            Fabric-enabled or the API is otherwise unavailable, signalling the caller
            to fall back to the Power BI DAX INFO function approach.
        """
        url = (
            f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}"
            f"/semanticModels/{dataset_id}/getDefinition?format=TMDL"
        )
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }

        async with httpx.AsyncClient(timeout=180.0) as client:
            try:
                response = await client.post(url, headers=headers)

                if response.status_code == 202:
                    # Long-running operation — poll for completion
                    location = response.headers.get("Location")
                    if not location:
                        logger.warning("[TMDL] Fabric returned 202 but no Location header")
                        return None

                    for _ in range(60):
                        await asyncio.sleep(2)
                        poll_response = await client.get(location, headers=headers)
                        poll_data = poll_response.json()

                        if poll_data.get("status") == "Succeeded":
                            result_url = location + "/result"
                            result_response = await client.get(result_url, headers=headers)
                            result_response.raise_for_status()
                            return result_response.json().get("definition", {}).get("parts", [])
                        elif poll_data.get("status") == "Failed":
                            logger.error(f"[TMDL] Fabric long-running operation failed: {poll_data}")
                            return None

                    logger.warning("[TMDL] Fabric polling timed out after 120 s")
                    return None

                elif response.status_code == 200:
                    return response.json().get("definition", {}).get("parts", [])

                else:
                    # 400/403/404 typically means the workspace is not Fabric-enabled
                    logger.warning(
                        f"[TMDL] Fabric API returned HTTP {response.status_code} — "
                        "workspace is likely not Fabric-enabled, will try Power BI fallback"
                    )
                    return None

            except Exception as e:
                logger.error(f"[TMDL] Fabric API exception: {e}")
                return None

    async def _fetch_model_via_admin_scanner(
        self,
        workspace_id: str,
        dataset_id: str,
        access_token: str,
        config: Dict[str, Any]
    ) -> tuple:
        """
        Fetch full model schema via the Power BI Admin Metadata Scanner API.

        Returns the same (measures, tables) tuple as the Fabric TMDL path, including
        measure DAX expressions and column names.  Works for any workspace type
        (non-Fabric Premium, PPU, Fabric) as long as the Service Principal has the
        Power BI Service admin role or the Tenant.Read.All / Tenant.ReadWrite.All
        permission granted in Azure AD.

        Flow:
            POST  /admin/workspaces/getInfo   → scan_id  (may return 202 + Location)
            GET   /admin/workspaces/scanStatus/{scan_id}   (poll until Succeeded)
            GET   /admin/workspaces/scanResult/{scan_id}   → full schema JSON

        Returns ([], []) when the SP lacks admin permissions (401/403) so the caller
        can transparently fall through to the DAX-only fallback.

        Required SP permissions (one of):
          • Power BI Service Administrator tenant role, OR
          • Tenant.Read.All / Tenant.ReadWrite.All app permission in Azure AD
            (granted by a Global Administrator via admin consent)
        """
        base = "https://api.powerbi.com/v1.0/myorg/admin/workspaces"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        skip_system = config.get("skip_system_tables", True)

        async with httpx.AsyncClient(timeout=120.0) as client:

            # ── Step 1: kick off the scan ──────────────────────────────────────
            try:
                response = await client.post(
                    f"{base}/getInfo"
                    "?lineage=false"
                    "&datasourceDetails=false"
                    "&datasetSchema=true"
                    "&datasetExpressions=true",
                    headers=headers,
                    json={"workspaces": [workspace_id]}
                )

                if response.status_code in (401, 403):
                    logger.info(
                        f"[Admin Scanner] SP lacks admin permissions (HTTP {response.status_code}) — "
                        "skipping. To enable: assign the 'Power BI Service Administrator' tenant role "
                        "or grant Tenant.Read.All app permission to the SP."
                    )
                    return [], []

                response.raise_for_status()
                scan_id = response.json().get("id")
                if not scan_id:
                    logger.warning("[Admin Scanner] No scan ID in response")
                    return [], []

            except httpx.HTTPStatusError as e:
                logger.warning(f"[Admin Scanner] getInfo failed HTTP {e.response.status_code}")
                return [], []
            except Exception as e:
                logger.warning(f"[Admin Scanner] getInfo exception: {e}")
                return [], []

            # ── Step 2: poll until the scan is ready ───────────────────────────
            try:
                for _ in range(30):
                    await asyncio.sleep(2)
                    poll = await client.get(
                        f"{base}/scanStatus/{scan_id}", headers=headers
                    )
                    poll.raise_for_status()
                    status = poll.json().get("status", "")
                    if status == "Succeeded":
                        break
                    if status == "Failed":
                        logger.error(f"[Admin Scanner] Scan failed: {poll.json()}")
                        return [], []
                else:
                    logger.warning("[Admin Scanner] Scan polling timed out after 60 s")
                    return [], []
            except Exception as e:
                logger.warning(f"[Admin Scanner] Polling exception: {e}")
                return [], []

            # ── Step 3: fetch the result ───────────────────────────────────────
            try:
                result_resp = await client.get(
                    f"{base}/scanResult/{scan_id}", headers=headers
                )
                result_resp.raise_for_status()
                workspaces = result_resp.json().get("workspaces", [])
            except Exception as e:
                logger.warning(f"[Admin Scanner] scanResult exception: {e}")
                return [], []

        # ── Step 4: parse the result for the requested dataset ─────────────────
        measures: List[Dict[str, Any]] = []
        tables: List[Dict[str, Any]] = []

        target_ws = next(
            (ws for ws in workspaces if ws.get("id", "").lower() == workspace_id.lower()),
            None
        )
        if not target_ws:
            logger.warning("[Admin Scanner] Workspace not found in scan result")
            return [], []

        target_ds = next(
            (
                ds for ds in target_ws.get("datasets", [])
                if ds.get("id", "").lower() == dataset_id.lower()
            ),
            None
        )
        if not target_ds:
            logger.warning("[Admin Scanner] Dataset not found in scan result")
            return [], []

        for table in target_ds.get("tables", []):
            table_name = table.get("name", "")

            if skip_system:
                if "LocalDateTable" in table_name or "DateTableTemplate" in table_name:
                    continue

            columns = [
                col.get("name", "")
                for col in table.get("columns", [])
                if col.get("name") and not col.get("isHidden", False)
            ]
            tables.append({"name": table_name, "columns": columns})

            for measure in table.get("measures", []):
                measure_name = measure.get("name", "")
                expression = measure.get("expression", "")
                if not measure_name or not expression.strip():
                    continue
                measures.append({
                    "name": measure_name,
                    "table": table_name,
                    "expression": expression.strip()
                })

        logger.info(
            f"[Admin Scanner] {len(measures)} measure(s), {len(tables)} table(s) "
            f"retrieved for dataset {dataset_id}"
        )
        return measures, tables

    async def _fetch_model_via_powerbi_dax(
        self,
        workspace_id: str,
        dataset_id: str,
        access_token: str,
        config: Dict[str, Any]
    ) -> tuple:
        """
        Fallback model extraction for non-Fabric Power BI workspaces.

        Power BI's executeQueries REST API blocks raw Analysis Services DMV functions
        (INFO.TABLES, INFO.MEASURES, INFO.COLUMNS) — they return HTTP 400 with AS error
        code 3239575574 (DMV_NOT_AUTHORIZED) regardless of token or SP permissions.

        Only INFO.VIEW.* safe views are allowed via REST.  We use
        INFO.VIEW.RELATIONSHIPS() — which is already called by _fetch_relationships —
        to derive the set of table names present in the model.

        Measure expressions are not retrievable via the REST API without XMLA access
        (Premium/PPU with XMLA endpoint enabled).  We return an empty measures list
        so the LLM works with table/relationship context only.

        Customers who need full measure extraction on a non-Fabric workspace must
        either:
          a) Migrate to a Fabric workspace (recommended), or
          b) Enable the XMLA endpoint and provide XMLA credentials.
        """
        query_url = (
            f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}"
            f"/datasets/{dataset_id}/executeQueries"
        )
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }

        tables: List[Dict[str, Any]] = []

        # ── Extract unique table names from relationship data ───────────────────
        # INFO.VIEW.RELATIONSHIPS() is the only INFO.VIEW.* function permitted via
        # the REST executeQueries API on non-Fabric workspaces.
        try:
            response = await httpx.AsyncClient(timeout=60.0).post(
                query_url,
                headers=headers,
                json={
                    "queries": [{"query": "EVALUATE INFO.VIEW.RELATIONSHIPS()"}],
                    "serializerSettings": {"includeNulls": True}
                }
            )
            response.raise_for_status()
            rows = (
                response.json()
                .get("results", [{}])[0]
                .get("tables", [{}])[0]
                .get("rows", [])
            )

            seen: set = set()
            for row in rows:
                for key in ("[FromTable]", "[ToTable]"):
                    name = row.get(key, "")
                    if not name or name in seen:
                        continue
                    if config.get("skip_system_tables", True):
                        if "LocalDateTable" in name or "DateTableTemplate" in name:
                            continue
                    seen.add(name)
                    tables.append({"name": name})

            logger.info(
                f"[PowerBI Fallback] {len(tables)} table(s) derived from "
                "INFO.VIEW.RELATIONSHIPS() — measures not available via REST API"
            )

        except Exception as e:
            logger.warning(f"[PowerBI Fallback] INFO.VIEW.RELATIONSHIPS() failed: {e}")

        # Measures cannot be retrieved without XMLA on non-Fabric workspaces.
        measures: List[Dict[str, Any]] = []
        if not measures:
            logger.warning(
                "[PowerBI Fallback] Measure expressions unavailable on non-Fabric workspace. "
                "LLM will work with table/relationship context only. "
                "To enable full measure extraction, use a Fabric workspace."
            )

        return measures, tables

    async def _fetch_relationships(
        self,
        workspace_id: str,
        dataset_id: str,
        access_token: str,
        config: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Extract relationships using INFO.VIEW.RELATIONSHIPS() DAX function."""
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/executeQueries"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        payload = {
            "queries": [{"query": "EVALUATE INFO.VIEW.RELATIONSHIPS()"}],
            "serializerSettings": {"includeNulls": True}
        }

        async with httpx.AsyncClient(timeout=120.0) as client:
            try:
                response = await client.post(url, headers=headers, json=payload)
                response.raise_for_status()
                data = response.json()

                rows = data.get("results", [{}])[0].get("tables", [{}])[0].get("rows", [])
                relationships = []
                seen_ids = set()

                for row in rows:
                    rel_id = row.get("[ID]")
                    if rel_id in seen_ids:
                        continue
                    seen_ids.add(rel_id)

                    from_table = row.get("[FromTable]", "")
                    to_table = row.get("[ToTable]", "")

                    # Skip system tables
                    if config.get("skip_system_tables", True):
                        if "LocalDateTable" in from_table or "LocalDateTable" in to_table:
                            continue

                    relationships.append({
                        "from_table": from_table,
                        "from_column": row.get("[FromColumn]", ""),
                        "to_table": to_table,
                        "to_column": row.get("[ToColumn]", ""),
                        "is_active": row.get("[IsActive]", True)
                    })

                return relationships

            except Exception as e:
                logger.error(f"Relationships extraction error: {e}")
                return []

    async def _fetch_column_metadata(
        self,
        workspace_id: str,
        dataset_id: str,
        access_token: str,
        config: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Fetch column metadata using INFO.COLUMNS("TableName") DAX function.

        Note: INFO.COLUMNS() requires a table name parameter and must be called
        per table. This method is called BEFORE model_context has tables populated,
        so it returns an empty list. Use _enrich_model_context_with_metadata() instead
        which is called after tables are extracted.
        """
        # This method is kept for backwards compatibility but is not used
        # Column metadata is now fetched in _fetch_column_metadata_for_table()
        return []

    async def _fetch_column_metadata_for_table(
        self,
        workspace_id: str,
        dataset_id: str,
        access_token: str,
        table_name: str,
        config: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Fetch column metadata for a specific table using INFO.COLUMNS("TableName").

        Args:
            workspace_id: Power BI workspace ID
            dataset_id: Power BI dataset ID
            access_token: Authentication token
            table_name: Name of the table to get metadata for
            config: Configuration dict

        Returns:
            List of column metadata dictionaries
        """
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/executeQueries"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }

        # Correct syntax: INFO.COLUMNS("TableName")
        dax_query = f'EVALUATE INFO.COLUMNS("{table_name}")'

        payload = {
            "queries": [{"query": dax_query}],
            "serializerSettings": {"includeNulls": True}
        }

        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                response = await client.post(url, headers=headers, json=payload)

                # Check for error response
                if response.status_code != 200:
                    error_detail = ""
                    try:
                        error_json = response.json()
                        error_detail = error_json.get("error", {}).get("message", response.text)
                    except:
                        error_detail = response.text[:500]

                    logger.debug(
                        f"[Context Enrichment] INFO.COLUMNS('{table_name}') failed (HTTP {response.status_code}). "
                        f"Error: {error_detail[:100]}"
                    )
                    return []

                response.raise_for_status()
                data = response.json()

                rows = data.get("results", [{}])[0].get("tables", [{}])[0].get("rows", [])
                columns = []

                for row in rows:
                    column_name = row.get("[ExplicitName]", "") or row.get("[Name]", "")
                    data_type = row.get("[DataType]", "")

                    columns.append({
                        "table_name": table_name,
                        "column_name": column_name,
                        "data_type": data_type,
                        "is_hidden": row.get("[IsHidden]", False),
                        "description": row.get("[Description]", "")
                    })

                return columns

            except httpx.HTTPStatusError as e:
                logger.debug(
                    f"[Context Enrichment] INFO.COLUMNS('{table_name}') query failed: {e}"
                )
                return []

            except Exception as e:
                logger.debug(
                    f"[Context Enrichment] Column metadata extraction failed for '{table_name}': {e}"
                )
                return []

    async def _fetch_sample_column_values(
        self,
        workspace_id: str,
        dataset_id: str,
        access_token: str,
        model_context: Dict[str, Any],
        config: Dict[str, Any]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Fetch sample values for key columns to help LLM understand data patterns.

        For categorical columns: fetches distinct values (up to 10)
        For numeric columns: fetches min/max values
        """
        sample_values = {}

        # Limit to first 5 tables to avoid too many queries
        tables = model_context.get("tables", [])[:5]

        for table in tables:
            table_name = table["name"]
            columns = table.get("columns", [])[:10]  # Limit columns per table

            for column in columns:
                try:
                    # Skip if column looks like a key/ID
                    if any(suffix in column.lower() for suffix in ["id", "key", "_pk", "_fk"]):
                        continue

                    # Try to get distinct values (for categorical)
                    dax_query = f"""
                    EVALUATE
                    TOPN(
                        10,
                        SUMMARIZECOLUMNS(
                            {table_name}[{column}]
                        )
                    )
                    """

                    result = await self._execute_dax_query(
                        workspace_id, dataset_id, access_token, dax_query
                    )

                    if result.get("success") and result.get("data"):
                        values = [list(row.values())[0] for row in result["data"][:10]]
                        sample_values[f"{table_name}[{column}]"] = {
                            "type": "categorical",
                            "sample_values": values
                        }

                except Exception as e:
                    logger.debug(f"Could not fetch sample values for {table_name}[{column}]: {e}")
                    continue

        return sample_values

    async def _extract_model_context(
        self,
        workspace_id: str,
        dataset_id: str,
        access_token: str,
        config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Extract measures, relationships, and tables from the semantic model."""
        model_context = {
            "measures": [],
            "relationships": [],
            "tables": []
        }

        # Get Fabric token for TMDL (may need different scope)
        fabric_token = access_token
        try:
            if config.get("tenant_id") and config.get("client_id") and config.get("client_secret"):
                fabric_token = await self._get_fabric_token(config)
        except Exception as e:
            logger.warning(f"Could not get Fabric token, using Power BI token: {e}")

        # Fetch measures and tables — three-tier fallback:
        #   1. Fabric TMDL          (full context, Fabric workspaces)
        #   2. Admin Scanner API    (full context, any workspace where SP has admin read)
        #   3. DAX fallback         (partial — tables from relationships only, no measures)
        tmdl_parts = await self._fetch_tmdl_via_fabric(workspace_id, dataset_id, fabric_token)
        if tmdl_parts is not None:
            measures, tables = self._parse_tmdl_for_measures_and_tables(tmdl_parts, config)
            logger.info(
                f"[Model Context] Fabric TMDL: {len(measures)} measure(s), {len(tables)} table(s)"
            )
        else:
            logger.info("[Model Context] Fabric API unavailable — trying Admin Scanner API")
            measures, tables = await self._fetch_model_via_admin_scanner(
                workspace_id, dataset_id, access_token, config
            )
            if not measures and not tables:
                logger.info(
                    "[Model Context] Admin Scanner unavailable — "
                    "falling back to relationship-derived table names (no measures)"
                )
                measures, tables = await self._fetch_model_via_powerbi_dax(
                    workspace_id, dataset_id, access_token, config
                )
        model_context["measures"] = measures
        model_context["tables"] = tables

        # Fetch relationships via DAX
        relationships = await self._fetch_relationships(workspace_id, dataset_id, access_token, config)
        model_context["relationships"] = relationships

        return model_context

    async def _enrich_model_context_with_metadata(
        self,
        model_context: Dict[str, Any],
        workspace_id: str,
        dataset_id: str,
        access_token: str,
        config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Enrich model context with additional metadata (Microsoft Copilot-style).

        Adds:
        - Column descriptions and data types
        - Sample values (min/max for numeric, examples for categorical)
        - Table descriptions
        - Enhanced metadata for better LLM understanding
        """
        enriched_context = {**model_context}

        # Extract column-level metadata using INFO.COLUMNS("TableName") per table
        # This is OPTIONAL and disabled by default since most environments don't support DMV queries
        if config.get("enable_info_columns", False):
            try:
                tables = enriched_context.get("tables", [])
                total_columns_enriched = 0
                tables_enriched = 0

                logger.info("[Context Enrichment] INFO.COLUMNS() enrichment enabled - fetching column metadata...")

                # Limit to first 10 tables to avoid too many API calls
                for table in tables[:10]:
                    table_name = table["name"]

                    # Skip system tables
                    if config.get("skip_system_tables", True):
                        if "LocalDateTable" in table_name or "DateTableTemplate" in table_name:
                            continue

                    try:
                        # Fetch column metadata for this specific table
                        columns_metadata = await self._fetch_column_metadata_for_table(
                            workspace_id, dataset_id, access_token, table_name, config
                        )

                        if columns_metadata:
                            table["column_metadata"] = columns_metadata

                            # Extract data types
                            table["column_types"] = {
                                col["column_name"]: col["data_type"]
                                for col in columns_metadata
                            }

                            # Extract descriptions if available
                            table["column_descriptions"] = {
                                col["column_name"]: col.get("description", "")
                                for col in columns_metadata
                                if col.get("description")
                            }

                            total_columns_enriched += len(columns_metadata)
                            tables_enriched += 1

                    except Exception as e:
                        logger.debug(f"[Context Enrichment] Could not fetch metadata for table '{table_name}': {e}")
                        continue

                if tables_enriched > 0:
                    logger.info(
                        f"[Context Enrichment] Added column metadata for {tables_enriched} tables "
                        f"({total_columns_enriched} columns)"
                    )
                else:
                    logger.info(
                        "[Context Enrichment] INFO.COLUMNS() not supported in this environment - "
                        "using TMDL column information instead"
                    )

            except Exception as e:
                logger.warning(f"[Context Enrichment] Column metadata enrichment error: {e}")
        else:
            logger.debug(
                "[Context Enrichment] INFO.COLUMNS() disabled (enable_info_columns=False). "
                "Using TMDL column information - this is the recommended default."
            )

        # Add sample values for key columns (helps LLM understand data patterns)
        try:
            sample_values = await self._fetch_sample_column_values(
                workspace_id, dataset_id, access_token, enriched_context, config
            )
            enriched_context["sample_values"] = sample_values
            logger.info(f"[Context Enrichment] Added sample values for {len(sample_values)} columns")
        except Exception as e:
            logger.warning(f"[Context Enrichment] Could not fetch sample values: {e}")

        return enriched_context
