"""Pipeline Config Generator Tool — calls PBI APIs directly, no LLM intermediation.

Wraps generate_config.py logic as a CrewAI tool so it can be used in the Kasal UI
with its own config form (including both SP credential sets).
"""
import json
import logging
import os
import re
import sys
from collections import defaultdict
from typing import Any, Optional, Type

from crewai.tools import BaseTool
from pydantic import BaseModel, Field, PrivateAttr

from src.engines.crewai.tools.custom.metric_view_utils.utils import run_async as _run_async

logger = logging.getLogger(__name__)

# DAX ``Table[Column]`` reference (the table half is what we allocate on).
# Matches bare ``Table[Col]`` and quoted ``'Table Name'[Col]``. Module-level
# because BaseTool is a Pydantic model — a class attribute would be treated as
# a model field.
_DAX_TABLE_REF = re.compile(r"(?:'([^']+)'|(\w+))\s*\[")


class PipelineConfigGeneratorSchema(BaseModel):
    """Input schema — drives the Kasal UI form."""

    # ── PBI Configuration ──
    workspace_id: Optional[str] = Field(
        None, description="[PBI] Workspace ID (GUID)")
    dataset_id: Optional[str] = Field(
        None, description="[PBI] Dataset / Semantic Model ID (GUID)")
    report_id: Optional[str] = Field(
        None, description="[PBI] Optional Report ID (GUID) for visual metadata")

    # ── Non-Admin credentials (Execute Queries API — data / mapping extraction) ──
    # Provide EITHER a Service Principal (client_id + client_secret) OR a
    # Service Account (client_id + username + password). SA is used when
    # username + password are supplied.
    tenant_id: Optional[str] = Field(
        None, description="[Auth] Azure AD Tenant ID (shared by all credentials)")
    client_id: Optional[str] = Field(
        None, description="[Auth - Non-Admin] Client ID (SP or SA app; workspace member, SemanticModel.ReadWrite.All)")
    client_secret: Optional[str] = Field(
        None, description="[Auth - Non-Admin SP] Client Secret (Service Principal). Optional for a Service Account.")
    username: Optional[str] = Field(
        None, description="[Auth - Non-Admin SA] Service Account username/UPN (enables password grant)")
    password: Optional[str] = Field(
        None, description="[Auth - Non-Admin SA] Service Account password")
    access_token: Optional[str] = Field(
        None, description="[Auth - Non-Admin] Pre-obtained OAuth access token (bypasses SP/SA for the data path)")
    auth_method: Optional[str] = Field(
        None, description="[Auth] Optional explicit method: 'service_principal' or 'service_account'. Auto-detected from the fields provided when unset (SP-first, matching the shared AadService).")

    # ── Admin credentials (Admin Scanner API) ──
    # Also accepts SP (admin_client_id + admin_client_secret) or SA
    # (admin_client_id + admin_username + admin_password).
    admin_client_id: Optional[str] = Field(
        None, description="[Auth - Admin] Client ID (SP or SA app; Tenant.Read.All / Power BI Admin)")
    admin_client_secret: Optional[str] = Field(
        None, description="[Auth - Admin SP] Client Secret (Service Principal). Optional for a Service Account.")
    admin_username: Optional[str] = Field(
        None, description="[Auth - Admin SA] Service Account username/UPN (enables password grant)")
    admin_password: Optional[str] = Field(
        None, description="[Auth - Admin SA] Service Account password")

    # ── Target UC Namespace ──
    catalog: Optional[str] = Field(
        None, description="[Target] Unity Catalog name (default: main)")
    schema_name: Optional[str] = Field(
        None, description="[Target] UC Schema name (default: default)")


class PipelineConfigGeneratorTool(BaseTool):
    """Generate pipeline_config.json by calling 4 PBI APIs directly."""

    name: str = "Pipeline Config Generator"
    description: str = (
        "Generate pipeline_config.json by calling 4 Power BI APIs directly — "
        "no LLM intermediation, no data truncation. Produces all 26 config keys "
        "with auto-filled values and TODO markers where human input is needed. "
        "Requires two credential sets — non-admin (Execute Queries API) and "
        "admin (Admin Scanner API) — each of which may be a Service Principal "
        "(client_id + client_secret) or a Service Account (client_id + username "
        "+ password). If the Admin Scanner rejects a Service Account token, it "
        "falls back to Fabric TMDL to read the model. Output is the full config "
        "JSON ready for the UC Metric View Generator (Tool 86)."
    )
    args_schema: Type[BaseModel] = PipelineConfigGeneratorSchema
    _default_config: dict = PrivateAttr(default_factory=dict)

    model_config = {"arbitrary_types_allowed": True, "extra": "allow"}

    def __init__(self, **kwargs: Any) -> None:
        config_keys = (
            "workspace_id", "dataset_id", "report_id",
            "tenant_id", "client_id", "client_secret",
            "username", "password", "access_token", "auth_method",
            "admin_client_id", "admin_client_secret",
            "admin_username", "admin_password",
            "catalog", "schema_name",
        )
        default_config = {}
        for key in config_keys:
            val = kwargs.pop(key, None)
            if val is not None:
                default_config[key] = val
        super().__init__(**kwargs)
        self._default_config = default_config

    def _run(self, **kwargs: Any) -> str:
        """Execute the pipeline config generation."""
        def _get(key: str) -> Any:
            # A capable agent, told to "call the tool with all inputs", often
            # passes its OWN EMPTY placeholder for fields it doesn't have (e.g.
            # workspace_id="") . The prior `if val is not None` returned that empty
            # string, shadowing the real value the flow injected into
            # _default_config → "workspace_id and dataset_id are required." Treat
            # empty string/empty collection as absent and fall back to the
            # injected default (same guard as the UCMV tool's `_get`).
            val = kwargs.get(key)
            if val is None or val == "" or val == [] or val == {}:
                return self._default_config.get(key)
            return val

        workspace_id = _get("workspace_id")
        dataset_id = _get("dataset_id")
        report_id = _get("report_id")
        tenant_id = _get("tenant_id")
        client_id = _get("client_id")
        client_secret = _get("client_secret")
        username = _get("username")
        password = _get("password")
        access_token = _get("access_token")
        auth_method = _get("auth_method")
        admin_client_id = _get("admin_client_id")
        admin_client_secret = _get("admin_client_secret")
        admin_username = _get("admin_username")
        admin_password = _get("admin_password")
        catalog = _get("catalog") or "main"
        schema = _get("schema_name") or "default"

        # A credential set is valid as a Service Principal (client_id +
        # client_secret), a Service Account (client_id + username + password),
        # or a pre-obtained OAuth access_token (non-admin only).
        def _creds_ok(cid, secret, user, pw) -> bool:
            has_sp = bool(cid and secret)
            has_sa = bool(cid and user and pw)
            return has_sp or has_sa

        # Validate required fields
        if not workspace_id or not dataset_id:
            return json.dumps({"error": "workspace_id and dataset_id are required."})
        if not access_token and not tenant_id:
            return json.dumps({"error": "tenant_id is required (unless a pre-obtained access_token is supplied)."})
        if not access_token and not _creds_ok(client_id, client_secret, username, password):
            return json.dumps({"error": (
                "Non-admin credentials required: a Service Principal "
                "(client_id + client_secret), a Service Account "
                "(client_id + username + password), or a pre-obtained access_token."
            )})
        if not _creds_ok(admin_client_id, admin_client_secret, admin_username, admin_password):
            return json.dumps({"error": (
                "Admin credentials required: either a Service Principal "
                "(admin_client_id + admin_client_secret) or a Service Account "
                "(admin_client_id + admin_username + admin_password)."
            )})

        try:
            # Import generate_config module
            gen = self._import_generate_config()

            # Resolve both tokens through the shared AadService — the SAME auth
            # path the Semantic Model Fetcher and UC Metric View Generator use.
            # AadService supports Service Principal, Service Account, and User
            # OAuth, with explicit auth_method honoured and SP-first auto-detect.
            logger.info("[PipelineConfigGen] Acquiring tokens via shared AadService...")
            token = self._resolve_token(
                tenant_id=tenant_id, client_id=client_id, client_secret=client_secret,
                username=username, password=password, access_token=access_token,
                auth_method=auth_method, label="non-admin",
            )
            admin_token = self._resolve_token(
                tenant_id=tenant_id, client_id=admin_client_id, client_secret=admin_client_secret,
                username=admin_username, password=admin_password, access_token=None,
                auth_method=auth_method, label="admin",
            )
            warnings: list[str] = []

            # The Execute Queries / XMLA endpoints (relationships + measure DAX)
            # are frequently rejected for Service-Account (ROPC) tokens. When
            # that happens we lazily mint a Service-Principal token from the
            # non-admin client_secret (if provided) and retry — this is what
            # recovers the measure DAX needed for switch_decompositions etc.
            _sp_data_token_cache: dict = {}

            def _sp_data_token() -> Optional[str]:
                if not client_secret:
                    return None
                if "tok" not in _sp_data_token_cache:
                    try:
                        _sp_data_token_cache["tok"] = self._resolve_token(
                            tenant_id=tenant_id, client_id=client_id,
                            client_secret=client_secret,
                            username=None, password=None, access_token=None,
                            auth_method="service_principal", label="data-SP-fallback",
                        )
                    except Exception as _e:
                        logger.warning(f"[PipelineConfigGen] SP data-token fallback failed: {_e}")
                        _sp_data_token_cache["tok"] = None
                return _sp_data_token_cache["tok"]

            # API 1: Relationships (XMLA — may fail for a Service Account)
            relationships: list[dict] = []
            try:
                logger.info("[PipelineConfigGen] API 1: INFO.VIEW.RELATIONSHIPS()...")
                relationships = gen.extract_relationships(token, workspace_id, dataset_id)
                logger.info(f"[PipelineConfigGen]   → {len(relationships)} relationships")
            except Exception as e:
                logger.warning(f"[PipelineConfigGen] API 1 (Relationships) failed: {e}")
                sp_tok = _sp_data_token()
                if sp_tok:
                    try:
                        logger.info("[PipelineConfigGen] API 1: retrying with Service Principal...")
                        relationships = gen.extract_relationships(sp_tok, workspace_id, dataset_id)
                        logger.info(f"[PipelineConfigGen]   → {len(relationships)} relationships (SP fallback)")
                    except Exception as e2:
                        msg = f"API 1 (Relationships) failed (SA + SP): {e2}"
                        logger.warning(f"[PipelineConfigGen] {msg}")
                        warnings.append(msg)
                else:
                    warnings.append(f"API 1 (Relationships) failed: {e}")

            # API 2: Measures + DAX (XMLA — may fail for a Service Account).
            # Measure DAX is the source for switch_decompositions / filter_sets /
            # measure_resolutions, so the SP fallback matters most here.
            measures: list[dict] = []
            try:
                logger.info("[PipelineConfigGen] API 2: $SYSTEM.MDSCHEMA_MEASURES...")
                measures = gen.extract_measures(token, workspace_id, dataset_id)
                logger.info(f"[PipelineConfigGen]   → {len(measures)} measures")
            except Exception as e:
                logger.warning(f"[PipelineConfigGen] API 2 (Measures) failed: {e}")
                sp_tok = _sp_data_token()
                if sp_tok:
                    try:
                        logger.info("[PipelineConfigGen] API 2: retrying with Service Principal...")
                        measures = gen.extract_measures(sp_tok, workspace_id, dataset_id)
                        logger.info(f"[PipelineConfigGen]   → {len(measures)} measures (SP fallback)")
                    except Exception as e2:
                        msg = f"API 2 (Measures) failed (SA + SP): {e2}"
                        logger.warning(f"[PipelineConfigGen] {msg}")
                        warnings.append(msg)
                else:
                    warnings.append(f"API 2 (Measures) failed: {e}")

            # API 3: Admin Scanner (uses admin token). Graceful — the Power BI
            # Admin Scanner rejects Service-Account tokens (401/403), so this is
            # NOT fatal: on failure we fall back to Fabric TMDL (which a Service
            # Account CAN read), mirroring the Semantic Model Fetcher.
            admin_tables = {}
            logger.info("[PipelineConfigGen] API 3: Admin Scanner (workspace scan)...")
            try:
                scan_result = gen.trigger_admin_scan(admin_token, workspace_id)
                admin_tables = gen.parse_admin_tables(scan_result, dataset_id=dataset_id)
                logger.info(f"[PipelineConfigGen]   → {len(admin_tables)} tables (admin scanner)")
            except Exception as e:
                msg = f"API 3 (Admin Scanner) failed: {e}"
                logger.warning(f"[PipelineConfigGen] {msg}")
                warnings.append(msg)

            # API 3 fallback: Fabric TMDL (works for Service Accounts that the
            # Admin Scanner blocks). Uses whichever non-admin creds are present
            # (SA username/password OR SP client_secret) to mint a Fabric token.
            if not admin_tables:
                logger.info("[PipelineConfigGen] Admin scan empty — trying Fabric TMDL fallback...")
                try:
                    fabric_token = gen.get_fabric_token(
                        tenant_id, client_id, client_secret,
                        username=username, password=password,
                    )
                    tmdl_parts = gen.fetch_tmdl_parts(fabric_token, workspace_id, dataset_id)
                    if tmdl_parts:
                        admin_tables = gen.parse_tmdl_to_admin_tables(tmdl_parts, dataset_id=dataset_id)
                        logger.info(f"[PipelineConfigGen]   → {len(admin_tables)} tables (Fabric TMDL)")
                    else:
                        logger.warning("[PipelineConfigGen] Fabric TMDL returned no parts (workspace may not be Fabric-enabled)")
                        warnings.append("Fabric TMDL fallback returned no data (workspace may not be Fabric-enabled).")
                except Exception as e:
                    msg = f"Fabric TMDL fallback failed: {e}"
                    logger.warning(f"[PipelineConfigGen] {msg}")
                    warnings.append(msg)

            # API 3 last-resort fallback: retry the Admin Scanner with a Service
            # PRINCIPAL token, if an admin client_secret was supplied. Covers the
            # case where the workspace is NOT Fabric-enabled (TMDL unavailable)
            # and the SA cannot use the Admin Scanner — an SP with admin rights
            # still can. Only runs when a distinct SP secret is available.
            if not admin_tables and admin_client_secret:
                logger.info("[PipelineConfigGen] TMDL empty — retrying Admin Scanner with Service Principal...")
                try:
                    sp_admin_token = self._resolve_token(
                        tenant_id=tenant_id, client_id=admin_client_id,
                        client_secret=admin_client_secret,
                        username=None, password=None, access_token=None,
                        auth_method="service_principal", label="admin-SP-fallback",
                    )
                    scan_result = gen.trigger_admin_scan(sp_admin_token, workspace_id)
                    admin_tables = gen.parse_admin_tables(scan_result, dataset_id=dataset_id)
                    logger.info(f"[PipelineConfigGen]   → {len(admin_tables)} tables (SP fallback)")
                except Exception as e:
                    msg = f"Service Principal Admin Scanner fallback failed: {e}"
                    logger.warning(f"[PipelineConfigGen] {msg}")
                    warnings.append(msg)

            # If API 1+2 failed but admin scan has measures, extract from scan
            if not measures and admin_tables:
                logger.info("[PipelineConfigGen] Extracting measures from admin scan (XMLA fallback)...")
                for tbl_name, tbl_info in admin_tables.items():
                    for m in tbl_info.get("measures", []):
                        measures.append({
                            "measure_name": m.get("name", ""),
                            "table_name": tbl_name,
                            "expression": m.get("expression", ""),
                            "description": m.get("description", ""),
                        })
                logger.info(f"[PipelineConfigGen]   → {len(measures)} measures from admin scan")

            # API 4: Report Definition (optional)
            report_def = None
            if report_id:
                logger.info("[PipelineConfigGen] API 4: Report Definition...")
                report_def = gen.extract_report_definition(
                    token, workspace_id, report_id,
                    tenant_id=tenant_id, client_id=client_id, client_secret=client_secret,
                )

            # Build config
            logger.info("[PipelineConfigGen] Building config...")
            config = gen.build_config(
                relationships, measures, admin_tables, report_def,
                catalog=catalog, schema=schema,
            )

            # ── Auto-enrich: scan DAX to populate filter_sets, switch_decompositions,
            # and measure_resolutions that the UCMV tool depends on ──
            enriched = self._enrich_config_from_dax(config, measures)
            if enriched:
                logger.info(
                    f"[PipelineConfigGen] DAX enrichment: "
                    f"{len(config.get('filter_sets', {}))} filter sets, "
                    f"{len(config.get('switch_decompositions', {}))} switch tables, "
                    f"{len(config.get('measure_resolutions', {}))} measure resolutions"
                )

            # Summary stats
            config_json = json.dumps(config, default=str)
            auto_count = 0
            todo_count = 0
            for val in config.values():
                val_str = json.dumps(val, default=str)
                if "TODO" in val_str:
                    todo_count += 1
                elif val:
                    auto_count += 1

            # ── UCMV handoff payload ────────────────────────────────────────
            # The downstream UC Metric View Generator runs in JSON mode and
            # builds views from `measures_json` + `mquery_json`, NOT from the
            # pipeline config. Emit both arrays here (in the exact shape UCMV's
            # API-extraction path produces) so the flow can inject them into
            # UCMV.measures_json / UCMV.mquery_json. Without this the UCMV crew
            # receives only `proposed_config` → empty mapping → 0 views, even
            # though config extraction (measures + DAX) fully succeeded.
            ucmv_measures = self._build_ucmv_measures(
                measures, admin_tables=admin_tables, config=config)
            ucmv_mquery = self._build_ucmv_mquery(admin_tables)

            # ── Measure usage ranking (reviewer prioritization) ──────────────
            # How many OTHER measures reference each measure (in-degree). Lets
            # reviewers prioritize gaps: a high-usage measure that ends up a TODO
            # blocks that many downstream translations. Measure→measure refs only
            # (not dashboard/visual usage). Sorted highest-first; zero-usage
            # measures omitted from the ranking to keep it focused.
            _usage = config.get("measure_usage", {}) or {}
            usage_ranking = [
                {"measure_name": name, "referenced_by": count}
                for name, count in sorted(
                    _usage.items(), key=lambda kv: kv[1], reverse=True)
                if count > 0
            ]
            # Carry the count into the UCMV handoff so the overview can show it.
            for _um in ucmv_measures:
                _um["referenced_by"] = _usage.get(
                    _um.get("original_name") or _um.get("measure_name"), 0)

            output = {
                "proposed_config": config,
                # Consumed by the flow handoff → UCMV JSON mode.
                "measures_json": ucmv_measures,
                "mquery_json": ucmv_mquery,
                "relationships_json": relationships,
                # Measures ranked by how many other measures reference them —
                # reviewers use this to prioritize which gaps/TODOs to fix first.
                "measure_usage_ranking": usage_ranking,
                "summary": {
                    "total_keys": len(config),
                    "auto_filled": auto_count,
                    "needs_human_review": todo_count,
                    "empty_null": len(config) - auto_count - todo_count,
                    "total_todo_markers": config_json.count("TODO"),
                    "relationships_extracted": len(relationships),
                    "measures_extracted": len(measures),
                    "measures_referenced_by_others": len(usage_ranking),
                    "mquery_tables_for_ucmv": len(ucmv_mquery),
                    "admin_tables_scanned": len(admin_tables),
                },
            }

            logger.info(
                f"[PipelineConfigGen] Done: {auto_count} auto-filled, "
                f"{todo_count} need review, {config_json.count('TODO')} TODO markers"
            )

            # ── Durable persistence (conversion_history / Lakebase) ─────────────
            # Store the extracted measures WITH their DAX so it is queryable after
            # the fact — this is the observability that answers "did we actually
            # get the DAX, and were there SWITCH measures?" for every run.
            try:
                _run_async(self._save_to_conversion_history(
                    measures=measures,
                    config=config,
                    relationships=relationships,
                    admin_tables=admin_tables,
                    workspace_id=workspace_id,
                    dataset_id=dataset_id,
                ))
            except Exception as _hist_err:
                logger.warning(f"[PipelineConfigGen] conversion_history persistence skipped: {_hist_err}")

            return json.dumps(output, indent=2, default=str)

        except Exception as e:
            logger.exception("[PipelineConfigGen] Failed")
            return json.dumps({"error": str(e)})

    @staticmethod
    def _resolve_measure_allocations(
        dax: str,
        home_table: str,
        fact_tables: set,
        table_lookup: dict,
    ) -> list[dict]:
        """Resolve which FACT tables a measure's DAX draws its columns from.

        PBI measures are defined on a *home* table (``table_name``) which — in the
        common measure-holder pattern (``C_Measure_Table_*``) — is NOT a fact and
        carries no data. The measure's real inputs are the ``Table[Column]``
        references in its DAX. This walks those references, keeps the ones that
        resolve to a known fact table, and returns them as ``all_allocations``:
            [{'table': <fact>, 'role': 'primary'|'secondary'}, ...]
        The first referenced fact is primary; the rest secondary. Returns ``[]``
        when nothing resolves (caller then falls back to the home table).
        """
        if not dax:
            return []
        seen: list = []
        for m in _DAX_TABLE_REF.finditer(dax):
            raw = (m.group(1) or m.group(2) or "").strip()
            if not raw:
                continue
            # Resolve the referenced name to a canonical fact-table key.
            resolved = table_lookup.get(raw) or table_lookup.get(raw.lower())
            if resolved and resolved in fact_tables and resolved not in seen:
                seen.append(resolved)
        # A measure whose home table is itself a fact stays primary on its home.
        if home_table in fact_tables and home_table not in seen:
            seen.insert(0, home_table)
        if not seen:
            return []
        return [
            {"table": t, "role": "primary" if i == 0 else "secondary"}
            for i, t in enumerate(seen)
        ]

    @staticmethod
    def _build_ucmv_measures(
        measures: list[dict],
        admin_tables: dict = None,
        config: dict = None,
    ) -> list[dict]:
        """Convert config-gen measures into the UCMV `measures_json` shape.

        Config-gen stores measures as
            {measure_name, table_name, expression, description}
        (or, via API 1+2, they may already carry `dax_expression`). UCMV's
        pipeline iterates `mapping` expecting
            {measure_name, original_name, dax_expression, proposed_allocation}
        — identical to what UCMV's own PBI-API path emits. Normalise here so the
        handoff is a drop-in for JSON mode.

        **Measure re-homing (the fix for holder-pattern models):** a measure's
        ``table_name`` is the PBI table it is *defined* on — often a measure-holder
        table with no data. Left as-is, ``proposed_allocation`` points at the
        holder, the holder is skipped as a non-fact, and the measure's KPI is
        dropped (this is what happened to 11/12 CCHBC tables). Here we resolve the
        fact table(s) the measure's DAX actually references and emit
        ``all_allocations`` so the UCMV pipeline places the measure on every fact
        it draws from. ``proposed_allocation`` is kept (set to the primary
        allocation) as the single-table fallback for older consumers.
        """
        # Build the fact-table set + a name→canonical-key lookup so DAX refs
        # (which use the raw PBI table name) resolve to mquery/admin keys.
        fact_tables: set = set((config or {}).get("fact_join_map", {}).keys())
        table_lookup: dict = {}
        for key in (admin_tables or {}):
            table_lookup[key] = key
            table_lookup[key.lower()] = key
        # fact_join_map keys are authoritative facts even if absent from admin_tables.
        for key in fact_tables:
            table_lookup.setdefault(key, key)
            table_lookup.setdefault(key.lower(), key)

        out: list[dict] = []
        rehomed = 0
        orphaned = 0
        for m in measures or []:
            if not isinstance(m, dict):
                continue
            name = (m.get("measure_name") or m.get("name") or "").strip()
            if not name:
                continue
            dax = m.get("dax_expression") or m.get("expression") or ""
            home_table = (
                m.get("proposed_allocation")
                or m.get("table_name")
                or m.get("table")
                or ""
            )

            # Prefer an allocation the measure already carries; otherwise resolve
            # from the DAX references against the known fact tables.
            all_allocations = m.get("all_allocations") or []
            if not all_allocations and fact_tables:
                all_allocations = PipelineConfigGeneratorTool._resolve_measure_allocations(
                    dax, home_table, fact_tables, table_lookup)
                if all_allocations:
                    primary = all_allocations[0]["table"]
                    if primary != home_table:
                        rehomed += 1
                elif home_table and home_table not in fact_tables:
                    # DAX referenced no known fact → cannot re-home. Left on its
                    # home table (base-only). Counted so it is visible, not silent.
                    orphaned += 1

            allocation = (
                (all_allocations[0]["table"] if all_allocations else None)
                or home_table
                or "__unassigned__"
            )
            entry = {
                "measure_name": name,
                "original_name": m.get("original_name") or name,
                "dax_expression": dax,
                "proposed_allocation": allocation,
                "table_refs": m.get("table_refs") or [],
            }
            if all_allocations:
                entry["all_allocations"] = all_allocations
            out.append(entry)

        if rehomed or orphaned:
            logger.info(
                f"[PipelineConfigGen] Measure allocation: {rehomed} re-homed to "
                f"referenced fact(s) via DAX, {orphaned} left on home table "
                f"(no known fact referenced)"
            )
        return out

    @staticmethod
    def _build_ucmv_mquery(admin_tables: dict) -> list[dict]:
        """Convert admin_tables into the UCMV `mquery_json` shape.

        Both `parse_admin_tables` and `parse_tmdl_to_admin_tables` populate
        ``admin_tables[table]["mquery_expression"]`` with the partition source
        (embedded SQL or raw M). UCMV's MQueryParser.parse_json expects
            {table_name, transpiled_sql, validation_passed}
        and skips any entry without both a table_name and sql — so tables with
        no source expression are omitted (they carry no fact/source signal).
        """
        out: list[dict] = []
        for tbl_name, tbl_info in (admin_tables or {}).items():
            if not isinstance(tbl_info, dict):
                continue
            sql = (tbl_info.get("mquery_expression") or tbl_info.get("mquery") or "").strip()
            if not tbl_name or not sql:
                continue
            out.append({
                "table_name": tbl_name,
                "transpiled_sql": sql,
                # MUST be "Yes": UCMV's MQueryParser.parse_json drops any entry
                # whose validation_passed does not start with "Yes" (unless the
                # SQL happens to contain both SUM( and GROUP BY). The source here
                # is the authoritative partition expression from the Admin
                # Scanner / Fabric TMDL, so mark it accepted — otherwise every
                # table is silently skipped and UCMV emits 0 views.
                "validation_passed": "Yes",
            })
        return out

    async def _save_to_conversion_history(
        self, measures, config, relationships, admin_tables, workspace_id, dataset_id,
    ) -> None:
        """Persist the config-gen result to conversion_history (fail-open).

        Records the extracted measures with DAX (input_data.measures), the
        proposed config (output_data.proposed_config), and a DAX diagnostic so
        it is obvious per-run whether measure DAX was captured and how many
        SELECTEDVALUE+SWITCH measures were seen. Retrievable via
        GET /conversion-history (filter source_format=powerbi_config).
        """
        try:
            from src.engines.crewai.tools.tool_session_provider import ToolSessionProvider
            from src.schemas.conversion import ConversionHistoryCreate
            from src.utils.user_context import UserContext

            measures = measures or []
            with_dax = sum(
                1 for m in measures
                if (m.get("expression") or m.get("dax_expression") or "").strip()
            )
            switch_cnt = sum(
                1 for m in measures
                if "SWITCH" in (m.get("expression") or m.get("dax_expression") or "").upper()
                and "SELECTEDVALUE" in (m.get("expression") or m.get("dax_expression") or "").upper()
            )

            history_data = ConversionHistoryCreate(
                execution_id=(getattr(self, "trace_context", None) or {}).get("job_id"),
                source_format="powerbi_config",
                target_format="pipeline_config",
                input_data={
                    "workspace_id": workspace_id or "",
                    "dataset_id": dataset_id or "",
                    "measures": measures,
                    "relationships_count": len(relationships or []),
                    "admin_tables_count": len(admin_tables or {}),
                },
                input_summary=(
                    f"{len(measures)} measures ({with_dax} with DAX, "
                    f"{switch_cnt} SELECTEDVALUE+SWITCH)"
                )[:500],
                output_data={"proposed_config": config},
                output_summary=(
                    f"config: {len(config.get('switch_decompositions', {}))} switch tables, "
                    f"{len(config.get('measure_resolutions', {}))} measure resolutions"
                )[:500],
                configuration={"workspace_id": workspace_id, "dataset_id": dataset_id},
                status="success",
                measure_count=len(measures),
            )

            group_id = None
            try:
                gc = UserContext.get_group_context()
                if gc:
                    group_id = getattr(gc, "primary_group_id", None)
            except Exception:
                pass

            async with ToolSessionProvider.conversion_repo() as repo:
                record = await repo.create(history_data.model_dump())
                if group_id:
                    record.group_id = group_id
                await repo.session.commit()
                logger.info(
                    f"[PipelineConfigGen] Saved conversion_history id={record.id} "
                    f"(measures={len(measures)}, with_dax={with_dax}, switch={switch_cnt})"
                )
        except Exception as e:
            logger.warning(f"[PipelineConfigGen] Failed to save conversion_history (non-fatal): {e}")

    @staticmethod
    def _enrich_config_from_dax(config: dict, measures: list[dict]) -> bool:
        """Scan DAX expressions to auto-populate filter_sets, switch_decompositions,
        and measure_resolutions.

        Mutates config in place. Returns True if anything was added.
        """
        if not measures:
            return False

        changed = False
        filter_sets: dict[str, list[str]] = config.get('filter_sets', {})
        switch_decompositions: dict[str, dict] = config.get('switch_decompositions', {})
        measure_resolutions: dict[str, dict] = config.get('measure_resolutions', {})

        # ── 1. Extract filter_sets from CALCULATE(..., Table[col] = "val") patterns ──
        # Collects all literal value sets used in filter conditions per column.
        col_values: dict[str, set[str]] = defaultdict(set)
        for m in measures:
            dax = m.get('expression', '') or ''
            # Table[col] = "val" in CALCULATE filter arguments
            for hit in re.finditer(r'(\w+)\[(\w+)\]\s*=\s*"([^"]*)"', dax):
                col_name = hit.group(2)
                col_values[col_name].add(hit.group(3))
            # Table[col] IN {"a","b","c"}
            for hit in re.finditer(r'(\w+)\[(\w+)\]\s+in\s+\{([^}]+)\}', dax, re.IGNORECASE):
                col_name = hit.group(2)
                vals = re.findall(r'"([^"]*)"', hit.group(3))
                for v in vals:
                    col_values[col_name].add(v)
            # var CWC_List = {"APET","CAN",...}
            for hit in re.finditer(r'var\s+(\w+)\s*=\s*\{([^}]+)\}', dax, re.IGNORECASE):
                var_name = hit.group(1)
                vals = re.findall(r'"([^"]*)"', hit.group(2))
                if vals and len(vals) >= 2:
                    fs_key = var_name.upper()
                    if fs_key not in filter_sets:
                        filter_sets[fs_key] = sorted(set(vals))
                        changed = True

        # Promote column value sets into named filter sets when they have 3+ values
        for col, vals in col_values.items():
            if len(vals) >= 3:
                fs_key = f'{col.upper()}_FILTER'
                if fs_key not in filter_sets:
                    filter_sets[fs_key] = sorted(vals)
                    changed = True

        # ── 2. Detect SWITCH(TRUE(), ...) decompositions ──
        switch_re = re.compile(
            r'SWITCH\s*\(\s*TRUE\s*\(\s*\)\s*,\s*(.*?)\s*\)\s*$',
            re.IGNORECASE | re.DOTALL,
        )
        for m in measures:
            dax = m.get('expression', '') or ''
            table_name = m.get('table_name', '')
            measure_name = m.get('measure_name', '')
            sm = switch_re.search(dax)
            if not sm:
                continue

            body = sm.group(1)
            # Parse SWITCH branches: condition, expression pairs
            branches: dict[str, dict] = {}
            # Split on top-level commas (respecting parens)
            parts: list[str] = []
            depth = 0
            buf: list[str] = []
            for ch in body:
                if ch == '(':
                    depth += 1
                elif ch == ')':
                    depth -= 1
                elif ch == ',' and depth == 0:
                    parts.append(''.join(buf).strip())
                    buf = []
                    continue
                buf.append(ch)
            if buf:
                parts.append(''.join(buf).strip())

            # Pair up: (condition, expression), (condition, expression), ...
            for idx in range(0, len(parts) - 1, 2):
                cond_text = parts[idx].strip()
                expr_text = parts[idx + 1].strip()
                # Extract a readable branch name from the condition
                cond_match = re.search(r'SELECTEDVALUE\s*\(\s*\w+\[(\w+)\]\s*\)\s*=\s*"([^"]*)"', cond_text, re.IGNORECASE)
                if cond_match:
                    branch_name = cond_match.group(2)
                    branches[branch_name] = {
                        'condition': cond_text,
                        'sql_expr': '',  # needs manual fill or further translation
                    }

            if branches:
                existing = switch_decompositions.get(table_name)
                # `derive_switch_decompositions` (in build_config) may already have
                # produced a LIST of skeleton entries for this table. The two
                # writers use incompatible shapes (list vs dict), so only apply
                # the dict enrichment when the table is absent or already a dict —
                # never index into a list with a measure name (that crashes).
                if existing is None:
                    switch_decompositions[table_name] = {measure_name: branches}
                    changed = True
                elif isinstance(existing, dict):
                    existing[measure_name] = branches
                    changed = True
                # else: existing is a list skeleton from build_config — leave it;
                # UCMV already handles the list form and it flags human review.
                changed = True

        # ── 3. Detect SELECTEDVALUE dimensions for measure_resolutions ──
        for m in measures:
            dax = m.get('expression', '') or ''
            table_name = m.get('table_name', '')
            measure_name = m.get('measure_name', '')
            for hit in re.finditer(r'SELECTEDVALUE\s*\(\s*(\w+)\[(\w+)\]\s*\)', dax, re.IGNORECASE):
                dim_table = hit.group(1)
                dim_col = hit.group(2)
                key = f'{dim_table}.{dim_col}'
                if key not in measure_resolutions:
                    measure_resolutions[key] = {
                        'table': dim_table,
                        'column': dim_col,
                        'used_by': [],
                    }
                    changed = True
                users = measure_resolutions[key].get('used_by', [])
                ref = f'{table_name}.{measure_name}'
                if ref not in users:
                    users.append(ref)

        if changed:
            config['filter_sets'] = filter_sets
            config['switch_decompositions'] = switch_decompositions
            config['measure_resolutions'] = measure_resolutions
        return changed

    @staticmethod
    @staticmethod
    def _resolve_token(
        tenant_id: Optional[str],
        client_id: Optional[str],
        client_secret: Optional[str],
        username: Optional[str],
        password: Optional[str],
        access_token: Optional[str],
        auth_method: Optional[str],
        label: str,
    ) -> str:
        """Resolve a Power BI token via the shared AadService.

        Same auth path as the Semantic Model Fetcher and UC Metric View
        Generator: honours an explicit ``auth_method``; otherwise auto-detects
        Service Principal first, then Service Account. ``AadService`` is
        synchronous, so it is safe to call directly from this sync tool.
        """
        from src.converters.services.powerbi.authentication import AadService

        service = AadService(
            client_id=client_id,
            client_secret=client_secret,
            tenant_id=tenant_id,
            access_token=access_token,
            username=username,
            password=password,
            auth_method=auth_method,
        )
        detected = service._determine_auth_method() if not access_token else "user_oauth"
        logger.info(f"[PipelineConfigGen] {label} token via: {detected or 'UNKNOWN'}")
        return service.get_access_token()

    @staticmethod
    def _import_generate_config():
        """Import the generate_config module.

        Search order:
        1. Same directory as this tool file (bundled for deployed apps).
        2. examples/uc_metric_view_migration/ relative to the repo root
           (local dev convenience).
        """
        this_dir = os.path.dirname(os.path.abspath(__file__))

        # Priority 1: bundled alongside the tool (deployed app path)
        candidates = [this_dir]

        # Priority 2: examples directory (local dev)
        project_root = os.path.abspath(
            os.path.join(this_dir, "..", "..", "..", "..", "..", "..", "..")
        )
        candidates.append(os.path.join(project_root, "examples", "uc_metric_view_migration"))

        gen_config_dir = None
        for candidate in candidates:
            if os.path.isfile(os.path.join(candidate, "generate_config.py")):
                gen_config_dir = candidate
                break

        if gen_config_dir is None:
            checked = ", ".join(candidates)
            raise ImportError(
                f"generate_config.py not found in any of: [{checked}]. "
                f"Resolved from {this_dir} (project_root={project_root})"
            )

        if gen_config_dir not in sys.path:
            sys.path.insert(0, gen_config_dir)

        import generate_config  # noqa: E402
        return generate_config
