"""Pipeline Config Generator Tool — calls PBI APIs directly, no LLM intermediation.

Wraps the Power BI pipeline configuration library as a CrewAI tool with its own Kasal UI config form (both SP credential sets).
"""

import json
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from types import ModuleType
from typing import Any, Optional, Type

from pydantic import BaseModel, Field, PrivateAttr

from src.services.tools.base import BaseTool
from src.services.tools.metric_view_utils.mquery_handoff import (
    build_ucmv_mquery,
    enrich_join_source_tables,
)
from src.services.tools.metric_view_utils.utils import run_async as _run_async
from src.services.tools.pipeline_config_generator_utils.dax_enrichment import (
    enrich_config_from_dax,
)
from src.services.tools.pipeline_config_generator_utils.inputs import (
    ConfigGenInputs,
    input_error,
    resolve_inputs,
)
from src.services.tools.pipeline_config_generator_utils.measure_quality import (
    dax_quality,
    degraded_dax_message,
    is_degraded,
)
from src.services.tools.pipeline_config_generator_utils.persistence import (
    save_conversion_history,
    save_powerbi_extraction,
)
from src.services.tools.pipeline_config_generator_utils.ucmv_handoff import (
    DAX_TABLE_REF,
    build_ucmv_measures,
    resolve_measure_allocations,
)
from src.services.tools.pipeline_config_generator_utils.warehouse_enrichment import (
    call_llm_fact_join_map,
    detect_cross_fact_merge,
    enrich_fact_join_map_llm,
    enrich_filter_sets_from_warehouse,
)

logger = logging.getLogger(__name__)

# DAX ``Table[Column]`` reference, kept importable under its historical name
# (switch_decomposition.py cites it). Module-level because BaseTool is a Pydantic
# model; a class attribute would be treated as a model field.
_DAX_TABLE_REF = DAX_TABLE_REF


@dataclass
class _ConfigGenRun:
    """State one ``_run`` call gathers, threaded through its extraction steps."""

    inputs: ConfigGenInputs
    gen: ModuleType
    workspace_id: str
    dataset_id: str
    token: str
    admin_token: str
    warnings: list[str] = field(default_factory=list)
    relationships: list[dict] = field(default_factory=list)
    measures: list[dict] = field(default_factory=list)
    admin_tables: dict = field(default_factory=dict)
    expressions: dict = field(default_factory=dict)
    admin_tables_source: Optional[str] = None
    expressions_source: Optional[str] = None
    tmdl_parts: list[dict] = field(default_factory=list)
    scan_result: Optional[dict] = None
    sp_data_token_cache: dict[str, Optional[str]] = field(default_factory=dict)
    with_real_dax: int = 0
    dax_degraded: bool = False
    report_id: Optional[str] = None
    report_def: Optional[dict] = None
    visual_usage_index: dict = field(default_factory=dict)
    calculated_columns: dict = field(default_factory=dict)
    fiscal_calendars: dict = field(default_factory=dict)
    measure_metadata: dict = field(default_factory=dict)


def _count_auto_and_todo(config: dict) -> tuple[int, int]:
    """Top-level config keys that are auto-filled vs still carry a TODO marker."""
    auto_count = 0
    todo_count = 0
    for val in config.values():
        val_str = json.dumps(val, default=str)
        if "TODO" in val_str:
            todo_count += 1
        elif val:
            auto_count += 1
    return auto_count, todo_count


def _usage_ranking(config: dict, ucmv_measures: list[dict]) -> list[dict]:
    """Measures ranked by how many other measures reference them.

    Reviewers use this to prioritise gaps: a high-usage measure left as a TODO
    blocks many downstream translations. Also stamps ``referenced_by`` onto each
    UCMV measure. Zero-usage measures are left out of the ranking.
    """
    usage = config.get("measure_usage", {}) or {}
    ranking = [
        {"measure_name": name, "referenced_by": count}
        for name, count in sorted(usage.items(), key=lambda kv: kv[1], reverse=True)
        if count > 0
    ]
    for um in ucmv_measures:
        um["referenced_by"] = usage.get(
            um.get("original_name") or um.get("measure_name"), 0
        )
    return ranking


class PipelineConfigGeneratorSchema(BaseModel):
    """Input schema — drives the Kasal UI form."""

    # ── PBI Configuration ──
    workspace_id: Optional[str] = Field(None, description="[PBI] Workspace ID (GUID)")
    dataset_id: Optional[str] = Field(
        None, description="[PBI] Dataset / Semantic Model ID (GUID)"
    )
    report_id: Optional[str] = Field(
        None, description="[PBI] Optional Report ID (GUID) for visual metadata"
    )

    # ── Non-Admin credentials (Execute Queries API — data / mapping extraction) ──
    # Provide EITHER a Service Principal (client_id + client_secret) OR a
    # Service Account (client_id + username + password). SA is used when
    # username + password are supplied.
    tenant_id: Optional[str] = Field(
        None, description="[Auth] Azure AD Tenant ID (shared by all credentials)"
    )
    client_id: Optional[str] = Field(
        None,
        description="[Auth - Non-Admin] Client ID (SP or SA app; workspace member, SemanticModel.ReadWrite.All)",
    )
    client_secret: Optional[str] = Field(
        None,
        description="[Auth - Non-Admin SP] Client Secret (Service Principal). Optional for a Service Account.",
    )
    username: Optional[str] = Field(
        None,
        description="[Auth - Non-Admin SA] Service Account username/UPN (enables password grant)",
    )
    password: Optional[str] = Field(
        None, description="[Auth - Non-Admin SA] Service Account password"
    )
    access_token: Optional[str] = Field(
        None,
        description="[Auth - Non-Admin] Pre-obtained OAuth access token (bypasses SP/SA for the data path)",
    )
    auth_method: Optional[str] = Field(
        None,
        description="[Auth] Optional explicit method: 'service_principal' or 'service_account'. Auto-detected from the fields provided when unset (SP-first, matching the shared AadService).",
    )

    # ── Admin credentials (Admin Scanner API) ──
    # Also accepts SP (admin_client_id + admin_client_secret) or SA
    # (admin_client_id + admin_username + admin_password).
    admin_client_id: Optional[str] = Field(
        None,
        description="[Auth - Admin] Client ID (SP or SA app; Tenant.Read.All / Power BI Admin)",
    )
    admin_client_secret: Optional[str] = Field(
        None,
        description="[Auth - Admin SP] Client Secret (Service Principal). Optional for a Service Account.",
    )
    admin_username: Optional[str] = Field(
        None,
        description="[Auth - Admin SA] Service Account username/UPN (enables password grant)",
    )
    admin_password: Optional[str] = Field(
        None, description="[Auth - Admin SA] Service Account password"
    )

    # ── Target UC Namespace ──
    catalog: Optional[str] = Field(
        None, description="[Target] Unity Catalog name (default: main)"
    )
    schema_name: Optional[str] = Field(
        None, description="[Target] UC Schema name (default: default)"
    )

    # ── Optional warehouse + LLM enrichment ──
    warehouse_id: Optional[str] = Field(
        None,
        description="[Enrichment, optional] Databricks SQL warehouse id (or SQL "
        "endpoint URL). When set, config-gen runs SELECT DISTINCT to resolve "
        "flag-column filter_sets and (for cross-fact merges) one LLM call to draft "
        "fact_join_map. Slower and consumes tokens. Omit to keep the fast, "
        "deterministic, LLM-free default.",
    )
    databricks_host: Optional[str] = Field(
        None,
        description="[Enrichment, optional] Workspace host override for the "
        "warehouse queries (defaults to the authenticated workspace).",
    )


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
            "workspace_id",
            "dataset_id",
            "report_id",
            "tenant_id",
            "client_id",
            "client_secret",
            "username",
            "password",
            "access_token",
            "auth_method",
            "admin_client_id",
            "admin_client_secret",
            "admin_username",
            "admin_password",
            "catalog",
            "schema_name",
            # Optional warehouse-enrichment (P2/P3): when a warehouse_id is
            # supplied, config-gen runs SELECT DISTINCT / cross-fact probes to fill
            # keys the PBI APIs can't (filter-set values, fact-join strategy).
            "warehouse_id",
            "databricks_host",
        )
        default_config = {}
        for key in config_keys:
            val = kwargs.pop(key, None)
            if val is not None:
                default_config[key] = val
        super().__init__(**kwargs)
        self._default_config = default_config

    def _resolve_execution_id(self) -> Optional[str]:
        """Job id used as the provenance key on persisted extraction rows.

        Prefers this instance's attached ``trace_context`` job_id, then falls
        back to the process-scoped current execution id. The fallback is what
        makes the handoff reliable: ToolFactory can rebuild this tool at
        task-execution time — after ``attach_tools_trace_context`` tagged an
        earlier instance — leaving ``self.trace_context`` empty. Without it the
        powerbi_extraction row is saved with a NULL execution_id and the UC
        Metric View Generator's DB-fallback cannot find it (0 views).
        """
        try:
            from src.services.execution.kernel.trace_context import (
                resolve_tool_execution_id,
            )

            return resolve_tool_execution_id(self)
        except Exception:
            logger.debug(
                "[PipelineConfigGen] trace context unavailable; using attached job id",
                exc_info=True,
            )
            return (getattr(self, "trace_context", None) or {}).get("job_id")

    def _run(self, **kwargs: Any) -> str:
        """Execute the pipeline config generation."""
        inputs = resolve_inputs(kwargs, self._default_config)
        error = input_error(inputs)
        if error:
            return json.dumps({"error": error})
        try:
            gen = self._import_generate_config()
            run = self._acquire_tokens(gen, inputs)
            self._extract_pbi_artifacts(run)
            config: dict = self._build_proposed_config(run)
            enrichment_log = self._enrich_config(run, config)
            output = self._assemble_output(run, config, enrichment_log)
            self._persist_run(run, config)
            return json.dumps(output, indent=2, default=str)
        except Exception as e:
            logger.exception("[PipelineConfigGen] Failed")
            return json.dumps({"error": str(e)})

    # ── Token acquisition ─────────────────────────────────────────────────

    def _acquire_tokens(
        self, gen: ModuleType, inputs: ConfigGenInputs
    ) -> _ConfigGenRun:
        """Resolve both credential sets through the shared AadService.

        Same auth path as the UC Metric View Generator: an explicit auth_method is
        honoured, otherwise Service Principal is tried first, then Service Account.
        """
        logger.info("[PipelineConfigGen] Acquiring tokens via shared AadService...")
        token = self._resolve_token(
            tenant_id=inputs.tenant_id,
            client_id=inputs.client_id,
            client_secret=inputs.client_secret,
            username=inputs.username,
            password=inputs.password,
            access_token=inputs.access_token,
            auth_method=inputs.auth_method,
            label="non-admin",
        )
        admin_token = self._resolve_token(
            tenant_id=inputs.tenant_id,
            client_id=inputs.admin_client_id,
            client_secret=inputs.admin_client_secret,
            username=inputs.admin_username,
            password=inputs.admin_password,
            access_token=None,
            auth_method=inputs.auth_method,
            label="admin",
        )
        return _ConfigGenRun(
            inputs=inputs,
            gen=gen,
            workspace_id=inputs.workspace_id or "",
            dataset_id=inputs.dataset_id or "",
            token=token,
            admin_token=admin_token,
        )

    def _sp_data_token(self, run: _ConfigGenRun) -> Optional[str]:
        """Lazily mint, once per run, a Service Principal token for the data path."""
        inputs = run.inputs
        if not inputs.client_secret:
            return None
        cache = run.sp_data_token_cache
        if "tok" not in cache:
            try:
                cache["tok"] = self._resolve_token(
                    tenant_id=inputs.tenant_id,
                    client_id=inputs.client_id,
                    client_secret=inputs.client_secret,
                    username=None,
                    password=None,
                    access_token=None,
                    auth_method="service_principal",
                    label="data-SP-fallback",
                )
            except Exception as e:
                logger.warning(
                    "[PipelineConfigGen] SP data-token fallback failed: %s",
                    e,
                    exc_info=True,
                )
                cache["tok"] = None
        return cache["tok"]

    # ── PBI extraction (API 1-4 with fallback chains) ─────────────────────

    def _extract_pbi_artifacts(self, run: _ConfigGenRun) -> None:
        """Run API 1-4 and their fallbacks, leaving the raw artifacts on ``run``."""
        self._extract_relationships_and_measures(run)
        self._scan_admin_model(run)
        self._measures_from_admin_scan(run)
        self._guard_measure_dax(run)
        self._resolve_report(run)

    def _extract_relationships_and_measures(self, run: _ConfigGenRun) -> None:
        """API 1 (relationships) and API 2 (measures and DAX), both XMLA."""
        run.relationships = self._fetch_with_sp_fallback(
            run,
            tag="API 1",
            label="API 1 (Relationships)",
            start="API 1: INFO.VIEW.RELATIONSHIPS()",
            noun="relationships",
            fetch=lambda token: run.gen.extract_relationships(
                token, run.workspace_id, run.dataset_id
            ),
        )
        # Measure DAX feeds switch_decompositions / filter_sets / measure_resolutions,
        # so the SP fallback matters most here.
        run.measures = self._fetch_with_sp_fallback(
            run,
            tag="API 2",
            label="API 2 (Measures)",
            start="API 2: $SYSTEM.MDSCHEMA_MEASURES",
            noun="measures",
            fetch=lambda token: run.gen.extract_measures(
                token, run.workspace_id, run.dataset_id
            ),
        )

    def _fetch_with_sp_fallback(
        self,
        run: _ConfigGenRun,
        *,
        tag: str,
        label: str,
        start: str,
        noun: str,
        fetch: Callable[[str], list[dict]],
    ) -> list[dict]:
        """Run one XMLA fetch; on failure retry once with the Service Principal token.

        The Execute Queries / XMLA endpoints are often rejected for Service Account
        (ROPC) tokens. Returns ``[]`` when every attempt fails and records why in
        ``run.warnings``.
        """
        try:
            logger.info("[PipelineConfigGen] %s...", start)
            rows = fetch(run.token)
            logger.info("[PipelineConfigGen]   → %s %s", len(rows), noun)
            return rows
        except Exception as e:
            logger.warning("[PipelineConfigGen] %s failed: %s", label, e, exc_info=True)
            sp_tok = self._sp_data_token(run)
            if not sp_tok:
                run.warnings.append(f"{label} failed: {e}")
                return []
            try:
                logger.info(
                    "[PipelineConfigGen] %s: retrying with Service Principal...", tag
                )
                rows = fetch(sp_tok)
                logger.info(
                    "[PipelineConfigGen]   → %s %s (SP fallback)", len(rows), noun
                )
                return rows
            except Exception as e2:
                msg = f"{label} failed (SA + SP): {e2}"
                logger.warning("[PipelineConfigGen] %s", msg, exc_info=True)
                run.warnings.append(msg)
                return []

    def _scan_admin_model(self, run: _ConfigGenRun) -> None:
        """API 3: Admin Scanner, then Fabric TMDL, then an SP Admin Scanner retry."""
        self._scan_admin_tables(run)
        self._fallback_fabric_tmdl(run)
        self._fallback_admin_scanner_sp(run)

    def _scan_admin_tables(self, run: _ConfigGenRun) -> None:
        """Admin Scanner (workspace scan). Not fatal: it rejects Service Account tokens."""
        logger.info("[PipelineConfigGen] API 3: Admin Scanner (workspace scan)...")
        try:
            run.scan_result = run.gen.trigger_admin_scan(
                run.admin_token, run.workspace_id
            )
            run.admin_tables = run.gen.parse_admin_tables(
                run.scan_result, dataset_id=run.dataset_id
            )
            if run.admin_tables:
                run.admin_tables_source = "admin scanner"
            run.expressions = run.gen.parse_admin_expressions(
                run.scan_result, dataset_id=run.dataset_id
            )
            if run.expressions:
                run.expressions_source = "admin scanner"
            logger.info(
                "[PipelineConfigGen]   → %s tables, %s shared expressions (admin scanner)",
                len(run.admin_tables),
                len(run.expressions),
            )
        except Exception as e:
            msg = f"API 3 (Admin Scanner) failed: {e}"
            logger.warning("[PipelineConfigGen] %s", msg, exc_info=True)
            run.warnings.append(msg)

    def _fallback_fabric_tmdl(self, run: _ConfigGenRun) -> None:
        """Fabric TMDL fills whichever of tables or expressions is still missing.

        Each tier only fills what is still empty; it never overwrites a good result
        from an earlier tier.
        """
        if run.admin_tables and run.expressions:
            return
        inputs = run.inputs
        logger.info(
            "[PipelineConfigGen] Admin scan tables/expressions incomplete — trying Fabric TMDL fallback..."
        )
        try:
            fabric_token = run.gen.get_fabric_token(
                inputs.tenant_id,
                inputs.client_id,
                inputs.client_secret,
                username=inputs.username,
                password=inputs.password,
            )
            tmdl_parts = run.gen.fetch_tmdl_parts(
                fabric_token, run.workspace_id, run.dataset_id
            )
            if not tmdl_parts:
                logger.warning(
                    "[PipelineConfigGen] Fabric TMDL returned no parts (workspace may not be Fabric-enabled)"
                )
                run.warnings.append(
                    "Fabric TMDL fallback returned no data (workspace may not be Fabric-enabled)."
                )
                return
            run.tmdl_parts = tmdl_parts
            if not run.admin_tables:
                run.admin_tables = run.gen.parse_tmdl_to_admin_tables(
                    tmdl_parts, dataset_id=run.dataset_id
                )
                if run.admin_tables:
                    run.admin_tables_source = "Fabric TMDL"
            if not run.expressions:
                run.expressions = run.gen.parse_tmdl_expressions(tmdl_parts)
                if run.expressions:
                    run.expressions_source = "Fabric TMDL"
            logger.info(
                "[PipelineConfigGen]   → %s tables, %s shared expressions (Fabric TMDL)",
                len(run.admin_tables),
                len(run.expressions),
            )
        except Exception as e:
            msg = f"Fabric TMDL fallback failed: {e}"
            logger.warning("[PipelineConfigGen] %s", msg, exc_info=True)
            run.warnings.append(msg)

    def _fallback_admin_scanner_sp(self, run: _ConfigGenRun) -> None:
        """Retry the Admin Scanner with an SP token, when an admin secret exists.

        Covers a workspace that is not Fabric-enabled, where TMDL is unavailable
        and a Service Account cannot use the Admin Scanner.
        """
        inputs = run.inputs
        if (run.admin_tables and run.expressions) or not inputs.admin_client_secret:
            return
        logger.info(
            "[PipelineConfigGen] Tables/expressions still incomplete — retrying Admin Scanner with Service Principal..."
        )
        try:
            sp_admin_token = self._resolve_token(
                tenant_id=inputs.tenant_id,
                client_id=inputs.admin_client_id,
                client_secret=inputs.admin_client_secret,
                username=None,
                password=None,
                access_token=None,
                auth_method="service_principal",
                label="admin-SP-fallback",
            )
            run.scan_result = run.gen.trigger_admin_scan(
                sp_admin_token, run.workspace_id
            )
            if not run.admin_tables:
                run.admin_tables = run.gen.parse_admin_tables(
                    run.scan_result, dataset_id=run.dataset_id
                )
                if run.admin_tables:
                    run.admin_tables_source = "SP fallback"
            if not run.expressions:
                run.expressions = run.gen.parse_admin_expressions(
                    run.scan_result, dataset_id=run.dataset_id
                )
                if run.expressions:
                    run.expressions_source = "SP fallback"
            logger.info(
                "[PipelineConfigGen]   → %s tables, %s shared expressions (SP fallback)",
                len(run.admin_tables),
                len(run.expressions),
            )
        except Exception as e:
            msg = f"Service Principal Admin Scanner fallback failed: {e}"
            logger.warning("[PipelineConfigGen] %s", msg, exc_info=True)
            run.warnings.append(msg)

    def _measures_from_admin_scan(self, run: _ConfigGenRun) -> None:
        """If API 1+2 returned no measures, take them from the admin scan."""
        if run.measures or not run.admin_tables:
            return
        logger.info(
            "[PipelineConfigGen] Extracting measures from admin scan (XMLA fallback)..."
        )
        for tbl_name, tbl_info in run.admin_tables.items():
            for m in tbl_info.get("measures", []):
                run.measures.append(
                    {
                        "measure_name": m.get("name", ""),
                        "table_name": tbl_name,
                        "expression": m.get("expression", ""),
                        "description": m.get("description", ""),
                    }
                )
        logger.info(
            "[PipelineConfigGen]   → %s measures from admin scan", len(run.measures)
        )

    def _guard_measure_dax(self, run: _ConfigGenRun) -> None:
        """DAX-quality guard: retry a degraded extraction, then warn loudly.

        Measure extraction can "succeed" with bare column references instead of
        DAX. The downstream pipeline then emits plausible but wrong measures, so the
        run is never quietly shipped as good.
        """
        with_real, total = dax_quality(run.measures)
        if is_degraded(with_real, total):
            logger.warning(
                "[PipelineConfigGen] DAX-quality guard: only %s/%s measures have real DAX — attempting re-extraction via SP + DAX INFO path",
                with_real,
                total,
            )
            # The DMV / Execute Queries path can return bare DAX for some token
            # types or under throttling, so retry with the SP data token.
            retry_tok = self._sp_data_token(run) or run.token
            try:
                retried = run.gen.extract_measures(
                    retry_tok, run.workspace_id, run.dataset_id
                )
                r_real, r_total = dax_quality(retried)
                if r_real > with_real:
                    logger.info(
                        "[PipelineConfigGen]   → re-extraction improved DAX: %s/%s (was %s/%s)",
                        r_real,
                        r_total,
                        with_real,
                        total,
                    )
                    run.measures = retried
                    with_real, total = r_real, r_total
            except Exception as e:
                logger.warning(
                    "[PipelineConfigGen] DAX re-extraction failed: %s", e, exc_info=True
                )
        run.with_real_dax = with_real
        run.dax_degraded = is_degraded(with_real, total)
        if run.dax_degraded:
            msg = degraded_dax_message(with_real, total)
            logger.error("[PipelineConfigGen] %s", msg)
            run.warnings.append(msg)
        else:
            logger.info(
                "[PipelineConfigGen] DAX-quality OK: %s/%s measures carry real DAX",
                with_real,
                total,
            )

    def _resolve_report(self, run: _ConfigGenRun) -> None:
        """API 4: report definition (PBIR visual bindings), auto-discovering report_id.

        The visual bindings carry the full measure DAX, so running without one
        silently degrades measure quality.
        """
        run.report_id = run.inputs.report_id
        if not run.report_id:
            discovered = run.gen.discover_report_id(
                run.token,
                run.workspace_id,
                run.dataset_id,
                scan_result=run.scan_result,
            )
            if discovered:
                run.report_id = discovered
                logger.info(
                    "[PipelineConfigGen] Auto-discovered report_id=%s bound to dataset %s",
                    discovered,
                    run.dataset_id,
                )
            else:
                msg = (
                    "No report_id supplied and none auto-discovered for this "
                    "dataset — measure DAX may be degraded (report visual "
                    "bindings carry full measure expressions). Supply a "
                    "report_id for best quality."
                )
                logger.warning("[PipelineConfigGen] %s", msg)
                run.warnings.append(msg)
        if not run.report_id:
            return
        logger.info("[PipelineConfigGen] API 4: Report Definition...")
        run.report_def = run.gen.extract_report_definition(
            run.token,
            run.workspace_id,
            run.report_id,
            tenant_id=run.inputs.tenant_id,
            client_id=run.inputs.client_id,
            client_secret=run.inputs.client_secret,
        )
        # Business-usage signal: which page/visual each measure is drawn on. Same
        # PBIR parts API 4 already fetched, no extra call. Never fatal.
        try:
            from src.services.powerbi.visual_usage import derive_visual_usage_index

            run.visual_usage_index = derive_visual_usage_index(run.report_def)
            logger.info(
                "[PipelineConfigGen]   → visual usage for %s field(s)",
                len(run.visual_usage_index),
            )
        except Exception as e:
            logger.warning(
                "[PipelineConfigGen] Visual usage extraction failed: %s",
                e,
                exc_info=True,
            )

    # ── Config build and enrichment ───────────────────────────────────────

    def _build_proposed_config(self, run: _ConfigGenRun) -> dict:
        """Build the config and derive the DAX-driven keys the UCMV tool needs."""
        logger.info("[PipelineConfigGen] Building config...")
        inputs = run.inputs
        config: dict = run.gen.build_config(
            run.relationships,
            run.measures,
            run.admin_tables,
            run.report_def,
            catalog=inputs.catalog,
            schema=inputs.schema,
        )
        if enrich_config_from_dax(config, run.measures):
            logger.info(
                "[PipelineConfigGen] DAX enrichment: %s filter sets, %s switch tables, %s measure resolutions",
                len(config.get("filter_sets", {})),
                len(config.get("switch_decompositions", {})),
                len(config.get("measure_resolutions", {})),
            )
        return config

    def _enrich_config(self, run: _ConfigGenRun, config: dict) -> list[dict]:
        """Additive enrichment post-pass. Returns the audit trail.

        Only writes a slot that is absent, empty or TODO, never a human or derived
        value. P1 is deterministic; P2/P3 need a warehouse; TMDL adds signals only
        the TMDL carries.
        """
        enrichment_log: list[dict] = []
        # P1 (always): join_key_map[dim].source_table from the dimension's M query.
        enrichment_log += self._enrich_source_tables_from_mquery(
            config, run.admin_tables, run.expressions
        )
        if run.inputs.warehouse_id:
            self._enrich_from_warehouse(run, config, enrichment_log)
        self._enrich_from_tmdl(run, config)
        return enrichment_log

    def _enrich_from_warehouse(
        self, run: _ConfigGenRun, config: dict, enrichment_log: list[dict]
    ) -> None:
        """P2 (filter_sets) and P3 (fact_join_map). Enrichment never aborts config-gen."""
        from src.services.tools.async_bridge import run_async_with_context

        inputs = run.inputs
        warehouse_id = inputs.warehouse_id or ""
        try:
            enrichment_log += run_async_with_context(
                enrich_filter_sets_from_warehouse(
                    config,
                    run.admin_tables,
                    run.measures,
                    warehouse_id,
                    inputs.databricks_host,
                ),
                timeout=300,
            )
        except Exception as e:
            logger.warning(
                "[PipelineConfigGen] Warehouse filter_sets enrichment failed: %s",
                e,
                exc_info=True,
            )
            enrichment_log.append(
                {
                    "tier": "P2",
                    "key": "filter_sets",
                    "status": "error",
                    "detail": f"warehouse enrichment failed: {e}",
                }
            )

        # P3 is doubly gated: a warehouse AND a detected cross-fact merge. Without
        # the merge there is no LLM call (the cost gate).
        involved = detect_cross_fact_merge(run.relationships)
        if not involved:
            enrichment_log.append(
                {
                    "tier": "P3",
                    "key": "fact_join_map",
                    "status": "skipped",
                    "detail": "no cross-fact merge detected (no LLM call)",
                }
            )
            return
        try:
            enrichment_log += run_async_with_context(
                enrich_fact_join_map_llm(
                    config, involved, warehouse_id, inputs.databricks_host
                ),
                timeout=300,
            )
        except Exception as e:
            logger.warning(
                "[PipelineConfigGen] fact_join_map enrichment failed: %s",
                e,
                exc_info=True,
            )
            enrichment_log.append(
                {
                    "tier": "P3",
                    "key": "fact_join_map",
                    "status": "error",
                    "detail": f"cross-fact enrichment failed: {e}",
                }
            )

    def _enrich_from_tmdl(self, run: _ConfigGenRun, config: dict) -> None:
        """TMDL enrichment (calculated columns, fiscal calendars, measure metadata).

        Fail-open. When only the Admin Scanner ran, a Fabric TMDL fetch is attempted
        just for the signals it uniquely carries.
        """
        inputs = run.inputs
        if not run.tmdl_parts and run.admin_tables:
            try:
                enrich_token = run.gen.get_fabric_token(
                    inputs.tenant_id,
                    inputs.client_id,
                    inputs.client_secret,
                    username=inputs.username,
                    password=inputs.password,
                )
                enrich_parts = run.gen.fetch_tmdl_parts(
                    enrich_token, run.workspace_id, run.dataset_id
                )
                if enrich_parts:
                    run.tmdl_parts = enrich_parts
            except Exception as e:
                logger.debug(
                    "[PipelineConfigGen] TMDL enrichment-only fetch skipped: %s",
                    e,
                    exc_info=True,
                )
        if run.tmdl_parts:
            try:
                from src.services.tools.metric_view_utils.tmdl_enrichment import (
                    detect_fiscal_calendar_tables,
                    extract_calculated_columns_from_tmdl,
                    harvest_measure_metadata_from_tmdl,
                )

                run.calculated_columns = extract_calculated_columns_from_tmdl(
                    run.tmdl_parts
                )
                run.fiscal_calendars = detect_fiscal_calendar_tables(
                    run.tmdl_parts, run.calculated_columns
                )
                run.measure_metadata = harvest_measure_metadata_from_tmdl(
                    run.tmdl_parts
                )
            except Exception as e:
                logger.warning(
                    "[PipelineConfigGen] TMDL enrichment failed (non-fatal): %s",
                    e,
                    exc_info=True,
                )

        if run.calculated_columns:  # S6
            config["calculated_columns"] = run.calculated_columns
        if run.fiscal_calendars:  # S9
            time_dimension = config.setdefault("time_dimension", {})
            if (
                isinstance(time_dimension, dict)
                and "pbi_period_format" not in time_dimension
            ):
                time_dimension["pbi_period_format"] = "date_to_fiscper"
                time_dimension["fiscal_calendar_tables"] = sorted(
                    run.fiscal_calendars.keys()
                )
            config["fiscal_calendar_tables"] = {
                tbl: info.get("indicators", [])
                for tbl, info in run.fiscal_calendars.items()
            }

    # ── Output assembly ───────────────────────────────────────────────────

    def _assemble_output(
        self, run: _ConfigGenRun, config: dict, enrichment_log: list[dict]
    ) -> dict:
        """The tool's JSON payload: config, UCMV handoff arrays, and a summary."""
        # Measured before the handoff below adds keys to config, as before.
        config_json = json.dumps(config, default=str)
        auto_count, todo_count = _count_auto_and_todo(config)

        # UCMV handoff payload. The downstream UC Metric View Generator runs in
        # JSON mode and builds from measures_json + mquery_json, not the pipeline
        # config, so both arrays are emitted here in UCMV's own shape.
        ucmv_measures: list[dict] = self._build_ucmv_measures(
            run.measures, admin_tables=run.admin_tables, config=config
        )
        ucmv_mquery = self._build_ucmv_mquery(
            run.admin_tables, run.expressions, run.warnings
        )
        if run.measure_metadata:
            self._apply_measure_metadata(ucmv_measures, run.measure_metadata)
        implicit_column_measures = self._implicit_column_measures(
            run, config, ucmv_measures
        )
        direct_resolved = self._count_direct_mquery(run.admin_tables)
        context_resolved = sum(
            1
            for entry in ucmv_mquery
            if entry.get("transpiled_sql", "").strip().upper().startswith("SELECT")
        )
        usage_ranking = _usage_ranking(config, ucmv_measures)

        # Raw M per table, so UCMV can resolve physical names and materialise
        # generated (List.Dates) tables without a separate scan_data handoff.
        config["table_mquery_expressions"] = {
            name: (info.get("mquery_expression") or info.get("mquery") or "")
            for name, info in (run.admin_tables or {}).items()
            if isinstance(info, dict)
            and (info.get("mquery_expression") or info.get("mquery"))
        }
        rec_scaffolds = self._reconciliation_scaffolds(run, config, ucmv_measures)

        output = {
            "proposed_config": config,
            # Consumed by the flow handoff → UCMV JSON mode.
            "measures_json": ucmv_measures,
            "mquery_json": ucmv_mquery,
            "relationships_json": run.relationships,
            # Measures ranked by how many other measures reference them.
            "measure_usage_ranking": usage_ranking,
            # dqa/kpi_reconciliation scaffolds, one per fact, direct binding.
            "reconciliation_mapping_scaffolds": rec_scaffolds,
            # {original_measure_name: [{page, visual_type, role}, ...]}
            "visual_usage_index": run.visual_usage_index,
            # {table: [raw columns drawn in a visual with no named measure behind them]}
            "implicit_column_measures": implicit_column_measures,
            "summary": {
                "total_keys": len(config),
                "auto_filled": auto_count,
                "needs_human_review": todo_count,
                "empty_null": len(config) - auto_count - todo_count,
                "total_todo_markers": config_json.count("TODO"),
                "relationships_extracted": len(run.relationships),
                "measures_extracted": len(run.measures),
                "measures_with_real_dax": run.with_real_dax,
                "dax_degraded": run.dax_degraded,
                "measures_referenced_by_others": len(usage_ranking),
                "mquery_tables_for_ucmv": len(ucmv_mquery),
                "admin_tables_scanned": len(run.admin_tables),
                "calculated_column_tables": len(run.calculated_columns),  # S6
                "fiscal_calendar_tables_detected": len(run.fiscal_calendars),  # S9
                "measures_with_tmdl_description": len(run.measure_metadata),  # rec#9
                "reconciliation_scaffolds_emitted": len(rec_scaffolds),  # rec#10
                "admin_tables_source": run.admin_tables_source,
                "expressions_captured": len(run.expressions),
                "expressions_source": run.expressions_source,
                "mquery_resolved_direct_only": direct_resolved,
                "mquery_resolved_with_context": context_resolved,
                "implicit_column_measures_found": sum(
                    len(v) for v in implicit_column_measures.values()
                ),
            },
            "warnings": run.warnings,
            # Additive-enrichment audit trail: what was auto-filled and what still
            # needs review. Surfaced in the Config Editor.
            "enrichment_log": enrichment_log,
            # Same diagnostic as UC Metric View Generator's "trace_context_seen", so
            # the two tools' job ids can be compared on one run.
            "trace_context_seen": (
                dict(getattr(self, "trace_context", None) or {})
                if isinstance(getattr(self, "trace_context", None), dict)
                else getattr(self, "trace_context", None)
            ),
        }

        logger.info(
            "[PipelineConfigGen] Done: %s auto-filled, %s need review, %s TODO markers",
            auto_count,
            todo_count,
            config_json.count("TODO"),
        )
        return output

    @staticmethod
    def _apply_measure_metadata(
        ucmv_measures: list[dict], measure_metadata: dict
    ) -> None:
        """rec#9: inject description/display_name/comment from TMDL measure tags."""
        try:
            from src.services.tools.metric_view_utils.tmdl_enrichment import (
                enrich_ucmv_measures_with_metadata,
            )

            enrich_ucmv_measures_with_metadata(ucmv_measures, measure_metadata)
        except Exception as e:
            logger.warning(
                "[PipelineConfigGen] measure metadata enrichment failed: %s",
                e,
                exc_info=True,
            )

    def _implicit_column_measures(
        self, run: _ConfigGenRun, config: dict, ucmv_measures: list[dict]
    ) -> dict:
        """Implicit-aggregation columns drawn in a visual with no named measure behind them."""
        if not run.visual_usage_index:
            return {}
        try:
            from src.services.powerbi.column_metadata import (
                extract_column_summarize_by,
            )
            from src.services.powerbi.implicit_column_measures import (
                derive_implicit_column_measures,
            )

            column_summarize_by = extract_column_summarize_by(
                run.token, run.workspace_id, run.dataset_id
            )
            implicit = derive_implicit_column_measures(
                run.visual_usage_index,
                ucmv_measures,
                column_summarize_by,
                fact_tables=set(config.get("fact_join_map", {}).keys()),
            )
            if implicit:
                logger.info(
                    "[PipelineConfigGen]   → %s implicit visual-column measure(s) across %s table(s)",
                    sum(len(v) for v in implicit.values()),
                    len(implicit),
                )
            return implicit
        except Exception as e:
            logger.warning(
                "[PipelineConfigGen] Implicit column measure detection failed: %s",
                e,
                exc_info=True,
            )
            return {}

    @staticmethod
    def _count_direct_mquery(admin_tables: dict) -> Optional[int]:
        """Tables that resolve through the direct M path alone (no ``expressions``).

        Diagnostic: when this equals the context-aware count although expressions
        were captured, the context half is not contributing at runtime (a stale
        deploy rather than a data problem). ``None`` when the probe is unavailable.
        """
        try:
            from src.services.tools.metric_view_utils.mquery_parser import (
                resolve_mquery_to_sql as _direct_resolve,
            )

            return sum(
                1
                for _tname, tinfo in (admin_tables or {}).items()
                if isinstance(tinfo, dict)
                and _direct_resolve(
                    (
                        tinfo.get("mquery_expression") or tinfo.get("mquery") or ""
                    ).strip()
                )
            )
        except Exception:
            logger.debug(
                "[PipelineConfigGen] direct M-resolve diagnostic unavailable",
                exc_info=True,
            )
            return None

    def _reconciliation_scaffolds(
        self, run: _ConfigGenRun, config: dict, ucmv_measures: list[dict]
    ) -> list[dict]:
        """rec#10: dqa/kpi_reconciliation scaffolds, one per fact (fail-open)."""
        try:
            from src.services.tools.metric_view_utils.tmdl_enrichment import (
                build_reconciliation_mapping_scaffolds,
            )

            return build_reconciliation_mapping_scaffolds(
                ucmv_measures,
                config,
                workspace_id=run.workspace_id,
                dataset_id=run.dataset_id,
                catalog=run.inputs.catalog,
                schema=run.inputs.schema,
                fiscal_calendars=run.fiscal_calendars,
            )
        except Exception as e:
            logger.warning(
                "[PipelineConfigGen] reconciliation scaffold build failed (non-fatal): %s",
                e,
                exc_info=True,
            )
            return []

    # ── Durable persistence ───────────────────────────────────────────────

    def _persist_run(self, run: _ConfigGenRun, config: dict) -> None:
        """Write conversion_history, then powerbi_extraction. Both fail open."""
        try:
            _run_async(
                self._save_to_conversion_history(
                    measures=run.measures,
                    config=config,
                    relationships=run.relationships,
                    admin_tables=run.admin_tables,
                    workspace_id=run.workspace_id,
                    dataset_id=run.dataset_id,
                )
            )
        except Exception as e:
            logger.warning(
                "[PipelineConfigGen] conversion_history persistence skipped: %s",
                e,
                exc_info=True,
            )
        try:
            _run_async(
                self._save_powerbi_extraction(
                    relationships=run.relationships,
                    measures=run.measures,
                    admin_tables=run.admin_tables,
                    report_def=run.report_def,
                    config=config,
                    warnings=run.warnings,
                    workspace_id=run.workspace_id,
                    dataset_id=run.dataset_id,
                    report_id=run.report_id,
                    expressions=run.expressions,
                )
            )
        except Exception as e:
            logger.warning(
                "[PipelineConfigGen] powerbi_extraction persistence skipped: %s",
                e,
                exc_info=True,
            )

    async def _save_to_conversion_history(
        self,
        measures: list,
        config: dict,
        relationships: list,
        admin_tables: dict,
        workspace_id: str,
        dataset_id: str,
    ) -> None:
        """Persist the config-gen result to conversion_history (fail-open)."""
        await save_conversion_history(
            resolve_execution_id=self._resolve_execution_id,
            measures=measures,
            config=config,
            relationships=relationships,
            admin_tables=admin_tables,
            workspace_id=workspace_id,
            dataset_id=dataset_id,
        )

    async def _save_powerbi_extraction(
        self,
        relationships: list,
        measures: list,
        admin_tables: dict,
        report_def: Optional[dict],
        config: dict,
        warnings: list,
        workspace_id: str,
        dataset_id: str,
        report_id: Optional[str],
        expressions: Optional[dict] = None,
    ) -> None:
        """Persist the full raw extraction to powerbi_extraction (fail-open)."""
        await save_powerbi_extraction(
            resolve_execution_id=self._resolve_execution_id,
            relationships=relationships,
            measures=measures,
            admin_tables=admin_tables,
            report_def=report_def,
            config=config,
            warnings=warnings,
            workspace_id=workspace_id,
            dataset_id=dataset_id,
            report_id=report_id,
            expressions=expressions,
        )

    # ── Thin delegates to the helper modules (stable call surface) ────────

    @staticmethod
    def _resolve_measure_allocations(
        dax: str,
        home_table: str,
        fact_tables: set,
        table_lookup: dict,
    ) -> list[dict]:
        """See ``ucmv_handoff.resolve_measure_allocations``."""
        return resolve_measure_allocations(dax, home_table, fact_tables, table_lookup)

    @staticmethod
    def _build_ucmv_measures(
        measures: list[dict],
        admin_tables: Optional[dict] = None,
        config: Optional[dict] = None,
    ) -> list[dict]:
        """See ``ucmv_handoff.build_ucmv_measures``."""
        return build_ucmv_measures(measures, admin_tables=admin_tables, config=config)

    @staticmethod
    def _build_ucmv_mquery(
        admin_tables: dict,
        expressions: dict | None = None,
        warnings: list[str] | None = None,
    ) -> list[dict]:
        """admin_tables -> the UCMV ``mquery_json`` entries (see
        ``metric_view_utils.mquery_handoff.build_ucmv_mquery``)."""
        return build_ucmv_mquery(admin_tables, expressions, warnings)

    @staticmethod
    def _enrich_source_tables_from_mquery(
        config: dict, admin_tables: dict, expressions: dict | None = None
    ) -> list[dict]:
        """P1: fill ``join_key_map[dim].source_table`` from the dimension's M,
        only when that M keeps every row of the table (see
        ``metric_view_utils.mquery_handoff.enrich_join_source_tables``)."""
        return enrich_join_source_tables(config, admin_tables, expressions)

    @staticmethod
    def _enrich_config_from_dax(config: dict, measures: list[dict]) -> bool:
        """See ``dax_enrichment.enrich_config_from_dax``."""
        return enrich_config_from_dax(config, measures)

    @staticmethod
    def _detect_cross_fact_merge(relationships: list) -> list[str]:
        """See ``warehouse_enrichment.detect_cross_fact_merge``."""
        return detect_cross_fact_merge(relationships)

    @staticmethod
    async def _enrich_filter_sets_from_warehouse(
        config: dict,
        admin_tables: dict,
        measures: list[dict],
        warehouse_id: str,
        host_override: Optional[str] = None,
    ) -> list[dict]:
        """See ``warehouse_enrichment.enrich_filter_sets_from_warehouse``."""
        return await enrich_filter_sets_from_warehouse(
            config, admin_tables, measures, warehouse_id, host_override
        )

    @staticmethod
    async def _enrich_fact_join_map_llm(
        config: dict,
        involved_facts: list,
        warehouse_id: str,
        host_override: Optional[str] = None,
    ) -> list[dict]:
        """See ``warehouse_enrichment.enrich_fact_join_map_llm``."""
        return await enrich_fact_join_map_llm(
            config, involved_facts, warehouse_id, host_override
        )

    @staticmethod
    async def _call_llm_fact_join_map(facts_summary: list[dict]) -> object:
        """See ``warehouse_enrichment.call_llm_fact_join_map``."""
        return await call_llm_fact_join_map(facts_summary)

    @staticmethod
    def _import_generate_config() -> ModuleType:
        """Load the shared Power BI library through its canonical package."""
        from src.services.powerbi import pipeline_config

        return pipeline_config

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

        Same auth path as the UC Metric View Generator: honours an explicit
        ``auth_method``; otherwise auto-detects Service Principal first, then
        Service Account. ``AadService`` is synchronous, so it is safe to call
        directly from this sync tool.
        """
        from src.services.converters.formats.powerbi.authentication import AadService

        service = AadService(
            client_id=client_id,
            client_secret=client_secret,
            tenant_id=tenant_id,
            access_token=access_token,
            username=username,
            password=password,
            auth_method=auth_method,
        )
        detected = (
            service._determine_auth_method() if not access_token else "user_oauth"
        )
        logger.info(
            "[PipelineConfigGen] %s token via: %s", label, detected or "UNKNOWN"
        )
        return service.get_access_token()
