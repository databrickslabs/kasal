"""UC Metric View Generator Tool for CrewAI — full pipeline.

The tool class orchestrates the generation steps, which live in the
``uc_metric_view_generator_utils`` package. Its public surface (this module,
``UCMetricViewGeneratorSchema`` and ``UCMetricViewGeneratorTool``) is unchanged.
"""

import json
import logging
from types import ModuleType
from typing import Any, ClassVar, Optional, Tuple, Type

from pydantic import BaseModel, Field, PrivateAttr

from src.services.tools.base import BaseTool
from src.services.tools.uc_metric_view_generator_utils.conversion_history_writer import (
    save_dax_to_conversion_history,
)
from src.services.tools.uc_metric_view_generator_utils.db_fallback import (
    apply_db_fallback,
)
from src.services.tools.uc_metric_view_generator_utils.llm_config import (
    build_llm_config,
)
from src.services.tools.uc_metric_view_generator_utils.model_inputs import (
    parse_model_inputs,
)
from src.services.tools.uc_metric_view_generator_utils.output_steps import (
    audit_source_mquery,
    build_none_allocated,
    build_resolved_measures_by_table,
    llm_translation_warning_for,
    run_expression_validation,
)
from src.services.tools.uc_metric_view_generator_utils.pbi_api_extraction import (
    extract_from_pbi_api,
    validate_pbi_inputs,
)
from src.services.tools.uc_metric_view_generator_utils.pbi_extraction_fallbacks import (
    extract_measures_fallback,
    extract_mquery_fallback,
    import_generate_config,
    tmdl_tables_to_measures,
    tmdl_tables_to_mquery,
)
from src.services.tools.uc_metric_view_generator_utils.pipeline_config_assembly import (
    apply_config_overlays,
    capture_raw_sources,
    recover_raw_m_sources,
)
from src.services.tools.uc_metric_view_generator_utils.pipeline_run import (
    run_pipeline,
)
from src.services.tools.uc_metric_view_generator_utils.reconciliation_gate import (
    run_reconciliation_gate,
)
from src.services.tools.uc_metric_view_generator_utils.result_views import (
    build_fallback_extract,
    build_raw_dax_extract,
    build_untranslatable_items,
)
from src.services.tools.uc_metric_view_generator_utils.shared_types import (
    RawInputs,
    UCMVDiagnostics,
    parse_json_input,
)
from src.services.tools.uc_metric_view_generator_utils.source_layer_steps import (
    build_source_layer_outputs,
)
from src.services.tools.uc_metric_view_generator_utils.visual_usage_triage import (
    apply_visual_usage,
    derive_mapping_and_connections,
)
from src.services.tools.uc_metric_view_generator_utils.zero_view_diagnosis import (
    build_best_effort_views,
    diagnose_zero_views,
)

logger = logging.getLogger(__name__)


from src.services.tools.metric_view_utils.utils import (  # noqa: E402 - import follows module initialization
    run_async as _run_async,
)


class UCMetricViewGeneratorSchema(BaseModel):
    """Input schema for UCMetricViewGeneratorTool."""

    measures_json: Optional[str] = Field(
        None,
        description="JSON string of measure_table_mapping (from Measure Conversion Pipeline)",
    )
    mquery_json: Optional[str] = Field(
        None,
        description="JSON string of mquery_transpilation (from MQuery Conversion Pipeline)",
    )
    relationships_json: Optional[str] = Field(
        None, description="JSON string of PBI relationships (from Relationships Tool)"
    )
    scan_data_json: Optional[str] = Field(
        None, description="JSON string of PBI scan data (optional, for enrichment)"
    )
    visual_usage_index: Optional[str] = Field(
        None,
        description=(
            "JSON string of {original_measure_name: [{page, visual_type, role}, ...]} "
            "(from Pipeline Config Generator's report-definition parse) — tags each "
            "translated measure with where it's actually used in the report."
        ),
    )
    config_json: Optional[str] = Field(
        None,
        description="JSON pipeline config overrides (join_key_map, fact_join_map, etc.)",
    )
    refinement_feedback: Optional[str] = Field(
        None,
        description=(
            "Iterative-reconciliation feedback from a prior cycle (JSON): the "
            "build_refinement_feedback list [{measure, pct_aligned, "
            "sample_mismatches}] or a {measure: hint} dict. When present with LLM "
            "fallback on, the DAX translator is told which measures mismatched "
            "Power BI last cycle so it fixes them. Set by the reconciliation loop; "
            "not usually supplied by hand."
        ),
    )
    naming_config: Optional[str] = Field(
        None,
        description=(
            "JSON of per-model naming/period vocabulary for the metadata lint and "
            "calendar period-folding — the reference model values are the defaults, so this "
            'is only needed for other models. Shape: {"lint": {"scenario_suffixes": '
            '[...], "unit_suffixes": [...], "dimension_alias_groups": [[...]], '
            '"min_description_len": int}, "period": {"column_overrides": '
            '{"latest_month_label": "IsCurrentMonth", ...}}}.'
        ),
    )
    domain_context: Optional[str] = Field(
        None,
        description=(
            "OPTIONAL free text (not JSON): the model's domain knowledge — "
            "business/metric vocabulary, naming conventions, fiscal-calendar "
            "quirks (e.g. 4-4-5), cost-accounting terms — fed verbatim into the "
            "DAX→SQL translation prompt so the LLM translates with the model's "
            "business semantics. Normally filled from a Skill attached to the "
            "agent (Configuration → Skills, tagged kasal-tool-param: "
            "domain_context); a value here is kept after the skill text. Only "
            "used when LLM fallback is on."
        ),
    )
    implicit_column_measures: Optional[str] = Field(
        None,
        description=(
            "JSON string of {table: [{name, original_name, raw_expr, comment, "
            "used_in_visuals, ...}, ...]} (from Pipeline Config Generator) — raw "
            "columns with PBI's own implicit aggregation that are drawn/filtered "
            "directly in a report visual with no named DAX measure behind them."
        ),
    )
    enable_reconciliation: Optional[bool] = Field(
        False,
        description=(
            "When true (and warehouse_id + reference_years + Power BI credentials "
            "are provided), run the iterative loop: after each generation cycle, "
            "deploy the metric view and reconcile it cell-by-cell against the live "
            "Power BI model, feeding mismatches back to refine failing measures. "
            "Deploys once per cycle. Without the prerequisites, generation is "
            "single-pass as usual."
        ),
    )
    max_reconciliation_cycles: Optional[int] = Field(
        5, description="Max iterative reconciliation cycles (when enabled)."
    )
    reconciliation_target_pct: Optional[float] = Field(
        100.0,
        description="Stop early once overall cell alignment reaches this percentage.",
    )
    reference_years: Optional[str] = Field(
        None,
        description="Fiscal years to reconcile, as a JSON array or comma list "
        '(e.g. "[2025, 2026]" or "2025,2026"). OPTIONAL — if omitted, '
        "reconciliation compares across all years.",
    )
    warehouse_id: Optional[str] = Field(
        None,
        description="Databricks SQL warehouse ID — required for reconciliation "
        "(deploys + queries the metric view).",
    )
    prune_non_visual_measures: Optional[bool] = Field(
        False,
        description=(
            "Triage: when true AND a visual-usage index is available, skip "
            "measures that are not shown or filtered in any report visual, "
            "directly or through a dependency chain (legacy/unused measures). "
            "Base/implicit column measures are always kept. No-op without visual "
            "usage data (nothing would be considered used)."
        ),
    )
    catalog: Optional[str] = Field(None, description="Target UC catalog name")
    schema_name: Optional[str] = Field(None, description="Target UC schema name")
    inner_dim_joins: bool = Field(False, description="Use INNER JOIN for dimensions")
    unflatten_tables: bool = Field(
        False, description="Unflatten __-separated table names"
    )
    use_llm_fallback: bool = Field(
        False, description="Enable LLM fallback for unmatched DAX patterns (opt-in)"
    )
    validate_against_pbi: bool = Field(
        False,
        description=(
            "Opt-in: run the rec#4 validation loop against Power BI. Requires "
            "access_token + workspace_id + dataset_id; compares each measure/view "
            "against PBI EVALUATE. Off by default so a normal generation run makes "
            "no live network calls."
        ),
    )
    translation_mode: Optional[str] = Field(
        None,
        description="'llm_first' (default when LLM enabled — skill-corpus-driven) or 'regex_first' (regex patterns primary)",
    )
    llm_model: Optional[str] = Field(
        None,
        description="LLM model for fallback (default: databricks-claude-sonnet-4-5)",
    )
    llm_workspace_url: Optional[str] = Field(
        None, description="Databricks workspace URL for LLM endpoint"
    )
    llm_token: Optional[str] = Field(
        None, description="Databricks token for LLM endpoint"
    )

    # ===== PBI API EXTRACTION (optional — alternative to providing pre-extracted JSON) =====
    workspace_id: Optional[str] = Field(
        None,
        description="Power BI workspace/group ID. When provided with credentials, extracts data from PBI API instead of requiring pre-extracted JSON.",
    )
    dataset_id: Optional[str] = Field(
        None, description="Power BI dataset/semantic model ID"
    )
    tenant_id: Optional[str] = Field(
        None, description="Azure AD tenant ID for Service Principal auth"
    )
    client_id: Optional[str] = Field(None, description="Azure AD application/client ID")
    client_secret: Optional[str] = Field(
        None, description="Client secret for Service Principal auth", repr=False
    )
    username: Optional[str] = Field(
        None, description="Service account username (alternative to SP)"
    )
    password: Optional[str] = Field(
        None, description="Service account password", repr=False
    )
    auth_method: Optional[str] = Field(
        None,
        description="Auth method: 'service_principal', 'service_account', or auto-detect",
    )
    access_token: Optional[str] = Field(
        None,
        description="Pre-obtained OAuth access token (alternative to SP/SA)",
        repr=False,
    )
    pbi_api_base_url: Optional[str] = Field(
        None,
        description="Power BI API base URL. Defaults to commercial cloud. Use 'https://api.powerbigov.us/v1.0/myorg' for GCC, 'https://api.powerbi.cn/v1.0/myorg' for China cloud.",
    )
    admin_client_id: Optional[str] = Field(
        None,
        description="[Auth - Admin SP] Service Principal client ID with tenant-admin API access, for the MQuery Admin Scanner retry when client_id lacks admin rights. Distinct from client_id.",
    )
    admin_client_secret: Optional[str] = Field(
        None,
        description="[Auth - Admin SP] Service Principal client secret with tenant-admin API access",
        repr=False,
    )


class UCMetricViewGeneratorTool(BaseTool):
    """Generate UC Metric View YAML + deploy SQL from PBI measures and MQuery data."""

    name: str = "UC Metric View Generator"
    description: str = (
        "Full pipeline: generates UC Metric View YAML + deploy SQL per fact table. "
        "TWO MODES: (1) API mode — provide workspace_id + dataset_id + PBI credentials, "
        "and the tool extracts measures, MQuery, and relationships from the PBI API automatically. "
        "(2) JSON mode — provide pre-extracted measures_json + mquery_json from upstream tools. "
        "Combines MQuery parsing, DAX translation (14+ patterns + LLM fallback), "
        "Kahn's dependency graph, join detection, and YAML/SQL emission."
    )
    args_schema: Type[BaseModel] = UCMetricViewGeneratorSchema
    _default_config: dict = PrivateAttr(default_factory=dict)
    #: Parameters a skill attached to the agent may fill (Configuration → Skills,
    #: tagged ``kasal-tool-param``) — see execution/kernel/skill_tool_context.py.
    skill_context_params: ClassVar[Tuple[str, ...]] = ("domain_context",)

    model_config = {"arbitrary_types_allowed": True, "extra": "allow"}

    def __init__(self, **kwargs: Any) -> None:
        config_keys = (
            "measures_json",
            "mquery_json",
            "relationships_json",
            "scan_data_json",
            "visual_usage_index",
            "implicit_column_measures",
            "config_json",
            "naming_config",
            "domain_context",
            "refinement_feedback",
            "enable_reconciliation",
            "max_reconciliation_cycles",
            "reconciliation_target_pct",
            "reference_years",
            "warehouse_id",
            "prune_non_visual_measures",
            "catalog",
            "schema_name",
            "inner_dim_joins",
            "unflatten_tables",
            "use_llm_fallback",
            "validate_against_pbi",
            "translation_mode",
            "llm_model",
            "llm_workspace_url",
            "llm_token",
            "workspace_id",
            "dataset_id",
            "tenant_id",
            "client_id",
            "client_secret",
            "username",
            "password",
            "auth_method",
            "access_token",
            "pbi_api_base_url",
            "admin_client_id",
            "admin_client_secret",
            "allow_best_effort",
            "fact_source_map",
        )
        default_config = {}
        for key in config_keys:
            val = kwargs.pop(key, None)
            if val is not None:
                default_config[key] = val
        super().__init__(**kwargs)
        self._default_config = default_config

    # ------------------------------------------------------------------
    # Delegating helpers. The logic lives in uc_metric_view_generator_utils;
    # these names stay on the class because callers and tests use them.
    # ------------------------------------------------------------------

    @staticmethod
    def _build_raw_dax_extract(measures: object) -> list:
        """Collect the full, untruncated original DAX expression per measure."""
        return build_raw_dax_extract(measures)

    @staticmethod
    def _build_fallback_extract(measures: object, mquery_entries: object) -> list:
        """Per-table tabular extract of M-Query source + associated measures/DAX."""
        return build_fallback_extract(measures, mquery_entries)

    @staticmethod
    def _import_generate_config() -> ModuleType:
        """Load the shared Power BI library through its canonical package."""
        return import_generate_config()

    @staticmethod
    def _tmdl_tables_to_measures(admin_tables: object) -> list:
        """Convert generate_config TMDL/admin table dict → UCMV measure shape."""
        return tmdl_tables_to_measures(admin_tables)

    @staticmethod
    def _tmdl_tables_to_mquery(admin_tables: object) -> list:
        """Convert generate_config TMDL table dict → UCMV mquery entry shape."""
        return tmdl_tables_to_mquery(admin_tables)

    def _extract_mquery_fallback(
        self,
        workspace_id: str,
        dataset_id: str,
        tenant_id: str,
        client_id: str,
        client_secret: str,
        username: str,
        password: str,
        admin_client_id: str = "",
        admin_client_secret: str = "",
    ) -> list:
        """Recover MQuery/table-source when the Admin Scanner fails for a Service Account."""
        return extract_mquery_fallback(
            load_generate_config=self._import_generate_config,
            run_async=_run_async,
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

    def _extract_measures_fallback(
        self,
        workspace_id: str,
        dataset_id: str,
        tenant_id: str,
        client_id: str,
        client_secret: str,
        username: str,
        password: str,
    ) -> list:
        """Recover measure DAX when Execute Queries/XMLA fails for a Service Account."""
        return extract_measures_fallback(
            load_generate_config=self._import_generate_config,
            workspace_id=workspace_id,
            dataset_id=dataset_id,
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret,
            username=username,
            password=password,
        )

    def _extract_from_pbi_api(
        self,
        workspace_id: str,
        dataset_id: str,
        tenant_id: str,
        client_id: str,
        client_secret: str,
        username: str,
        password: str,
        auth_method: Optional[str],
        access_token: str,
        pbi_api_base_url: str = "",
        admin_client_id: str = "",
        admin_client_secret: str = "",
    ) -> dict:
        """Extract measures, MQuery, relationships, and scan data from PBI API.

        Returns dict with keys: measures, mquery, relationships, scan_data
        """
        return extract_from_pbi_api(
            workspace_id=workspace_id,
            dataset_id=dataset_id,
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret,
            username=username,
            password=password,
            auth_method=auth_method,
            access_token=access_token,
            load_generate_config=self._import_generate_config,
            run_async=_run_async,
            pbi_api_base_url=pbi_api_base_url,
            admin_client_id=admin_client_id,
            admin_client_secret=admin_client_secret,
        )

    def _validate_pbi_inputs(
        self, workspace_id: str, dataset_id: str, pbi_api_base_url: str
    ) -> tuple[bool, str]:
        """Validate PBI API inputs to prevent SSRF."""
        return validate_pbi_inputs(workspace_id, dataset_id, pbi_api_base_url)

    async def _save_dax_to_conversion_history(
        self,
        raw_dax: list,
        yaml_output: Any,
        sql_output: Any,
        workspace_id: Optional[str],
        dataset_id: Optional[str],
        catalog: Optional[str],
        schema: Optional[str],
        untranslatable_items: Optional[list] = None,
        pbi_ucmv_mapping: Optional[dict] = None,
        live_connections: Optional[dict] = None,
        source_layer_ddl: Optional[dict] = None,
        pbi_only_ingestion_tasks: Optional[list] = None,
        pbi_validation: Optional[dict] = None,
        reconciliation_skipped: Optional[str] = None,
    ) -> None:
        """Persist the full raw DAX extract to conversion_history (fail-open)."""
        await save_dax_to_conversion_history(
            self,
            raw_dax=raw_dax,
            yaml_output=yaml_output,
            sql_output=sql_output,
            workspace_id=workspace_id,
            dataset_id=dataset_id,
            catalog=catalog,
            schema=schema,
            untranslatable_items=untranslatable_items,
            pbi_ucmv_mapping=pbi_ucmv_mapping,
            live_connections=live_connections,
            source_layer_ddl=source_layer_ddl,
            pbi_only_ingestion_tasks=pbi_only_ingestion_tasks,
            pbi_validation=pbi_validation,
            reconciliation_skipped=reconciliation_skipped,
        )

    # ------------------------------------------------------------------
    # Generation
    # ------------------------------------------------------------------

    def _run(self, **kwargs: Any) -> str:
        def _get(key: str) -> Any:
            return kwargs.get(key) or self._default_config.get(key)

        # Iterative reconciliation: when requested (and not already inside the
        # loop), delegate to the generate→deploy→reconcile→refine orchestrator.
        # Falls through to single-pass, naming the reason, when it cannot run.
        loop_report, recon_skipped = run_reconciliation_gate(self, kwargs, _get)
        if loop_report is not None:
            return loop_report

        def _parse_json_input(raw_value: Any, default: Any) -> Any:
            """Parse a JSON input; treat empty/blank as the default (never error)."""
            return parse_json_input(raw_value, default)

        # JSON inputs (measures/mquery/config/relationships/scan) are injected into
        # _default_config by the flow handoff. A capable agent, told to "call the
        # tool with ALL inputs", often passes its OWN placeholder for these (e.g.
        # a description string or "<measures_json>") which is truthy and overrides
        # the good injected value via _get's `or` — then json.loads() fails at
        # char 0. Guard: for JSON-shaped fields, if the kwarg isn't valid JSON but
        # the injected default IS present, use the injected default.
        def _looks_like_json(v: Any) -> bool:
            if not isinstance(v, (str, list, dict)):
                return False
            if isinstance(v, (list, dict)):
                return True
            s = v.strip()
            return s.startswith("{") or s.startswith("[")

        def _get_json(key: str) -> Any:
            kw_val = kwargs.get(key)
            default_val = self._default_config.get(key)
            # Prefer a valid kwarg; else the injected default; else the kwarg as-is.
            if kw_val is not None and _looks_like_json(kw_val):
                return kw_val
            if default_val is not None and _looks_like_json(default_val):
                if kw_val is not None and not _looks_like_json(kw_val):
                    logger.warning(
                        "[UCMV] agent passed non-JSON %s=%r; using flow-injected value instead",
                        key,
                        str(kw_val)[:40],
                    )
                return default_val
            return kw_val or default_val

        measures_raw = _get_json("measures_json") or "[]"
        mquery_raw = _get_json("mquery_json") or "[]"
        relationships_raw = _get_json("relationships_json")
        scan_raw = _get_json("scan_data_json")
        visual_usage_raw = _get_json("visual_usage_index")
        implicit_column_measures_raw = _get_json("implicit_column_measures")
        config_raw = _get_json("config_json") or "{}"
        raw = RawInputs(
            measures=measures_raw,
            mquery=mquery_raw,
            config=config_raw,
            relationships=relationships_raw,
            scan=scan_raw,
            visual_usage=visual_usage_raw,
            implicit_column_measures=implicit_column_measures_raw,
        )
        # Diagnostic: what arrived via flow injection/kwargs BEFORE any API-mode
        # extraction or DB fallback runs. Carried into `output['_diagnostics']`
        # (not just logged) so it is visible via the execution_trace pull.
        diag: UCMVDiagnostics = {
            "preinject_measures_json_chars": (
                len(measures_raw) if isinstance(measures_raw, str) else None
            ),
            "preinject_mquery_json_chars": (
                len(mquery_raw) if isinstance(mquery_raw, str) else None
            ),
            "preinject_config_json_chars": (
                len(config_raw) if isinstance(config_raw, str) else None
            ),
            "preinject_visual_usage_index_chars": (
                len(visual_usage_raw) if isinstance(visual_usage_raw, str) else None
            ),
            "preinject_implicit_column_measures_chars": (
                len(implicit_column_measures_raw)
                if isinstance(implicit_column_measures_raw, str)
                else None
            ),
            "db_fallback_fired_for": [],
            "db_fallback_extraction_id": None,
            # Shows exactly what this tool instance saw on self.trace_context at the
            # moment it ran, so it can be compared with the upstream crew's own log.
            "trace_context_seen": (
                dict(getattr(self, "trace_context", None) or {})
                if isinstance(getattr(self, "trace_context", None), dict)
                else getattr(self, "trace_context", None)
            ),
        }
        catalog = _get("catalog") or "main"
        schema = _get("schema_name") or "default"
        inner_joins = _get("inner_dim_joins") or False
        unflatten = _get("unflatten_tables") or False

        # DB fallback: rebuild empty inputs from Crew 1's persisted extraction. It
        # runs BEFORE API-mode extraction, which is the true last resort.
        apply_db_fallback(tool=self, raw=raw, diag=diag, run_async=_run_async)

        # API extraction mode (PBI credentials provided). Skipped entirely when the
        # DB fallback already filled both measures and mquery: API-mode's live
        # Admin Scanner trigger+poll (up to 5 minutes) would only redo that work.
        workspace_id = _get("workspace_id")
        dataset_id = _get("dataset_id")

        if workspace_id and dataset_id and (raw.measures == "[]" or raw.mquery == "[]"):
            pbi_api_base_url = _get("pbi_api_base_url") or ""
            valid, err_msg = self._validate_pbi_inputs(
                workspace_id, dataset_id, pbi_api_base_url
            )
            if not valid:
                return json.dumps({"error": f"PBI input validation failed: {err_msg}"})
            logger.info(
                "[UCMV] API extraction mode: workspace=%s, dataset=%s, token_provided=%s",
                workspace_id,
                dataset_id,
                bool(_get("access_token")),
            )
            try:
                extracted = self._extract_from_pbi_api(
                    workspace_id=workspace_id,
                    dataset_id=dataset_id,
                    tenant_id=_get("tenant_id") or "",
                    client_id=_get("client_id") or "",
                    client_secret=_get("client_secret") or "",
                    username=_get("username") or "",
                    password=_get("password") or "",
                    auth_method=_get("auth_method"),
                    access_token=_get("access_token") or "",
                    pbi_api_base_url=_get("pbi_api_base_url") or "",
                    admin_client_id=_get("admin_client_id") or "",
                    admin_client_secret=_get("admin_client_secret") or "",
                )
                # Use extracted data (override only when manually provided JSON is empty/default)
                if extracted.get("measures") and raw.measures == "[]":
                    raw.measures = json.dumps(extracted["measures"])
                if extracted.get("mquery") and raw.mquery == "[]":
                    raw.mquery = json.dumps(extracted["mquery"])
                if extracted.get("relationships") and not raw.relationships:
                    raw.relationships = json.dumps(extracted["relationships"])
                if extracted.get("scan_data") and not raw.scan:
                    raw.scan = json.dumps(extracted["scan_data"])
            except Exception as e:
                logger.exception("[UCMV] API extraction failed: %s", e)
                return json.dumps({"error": f"PBI API extraction failed: {e}"})

        # LLM fallback config (opt-in). Domain context and the in-loop time-
        # intelligence flag ride along inside it.
        use_llm = _get("use_llm_fallback") or False
        llm_config = build_llm_config(
            use_llm=use_llm,
            llm_workspace_url=_get("llm_workspace_url"),
            llm_token=_get("llm_token"),
            llm_model=_get("llm_model"),
            translation_mode=_get("translation_mode"),
            domain_context=_get("domain_context"),
            in_recon_loop=bool(kwargs.get("_in_recon_loop")),
        )

        # Iterative-reconciliation feedback → DAX LLM. Normalised to {measure -> hint}
        # and only used when LLM fallback is on.
        rf_raw = _get_json("refinement_feedback")
        if rf_raw and llm_config is not None:
            try:
                from src.services.tools.metric_view_utils.reconciliation import (
                    feedback_to_prompt_map,
                )

                fb_map = feedback_to_prompt_map(_parse_json_input(rf_raw, None))
                if fb_map:
                    llm_config["refinement_feedback_by_measure"] = fb_map
            except Exception as e:  # feedback is advisory
                logger.warning(
                    "[UCMV] Could not parse refinement_feedback: %s", e, exc_info=True
                )

        try:
            measures = _parse_json_input(raw.measures, [])
            mquery_entries = _parse_json_input(raw.mquery, [])
            config = _parse_json_input(raw.config, {})
        except json.JSONDecodeError as e:
            return json.dumps({"error": f"Invalid JSON input: {e}"})

        apply_config_overlays(
            config,
            in_recon_loop=bool(kwargs.get("_in_recon_loop")),
            naming_raw=_get_json("naming_config"),
            implicit_column_measures_raw=raw.implicit_column_measures,
            manual_overrides_raw=_get("manual_overrides"),
        )

        # Capture the ORIGINAL source per table BEFORE the raw-M → SQL recovery
        # rewrites it, so the full M can be shown and audited.
        raw_source_by_name = capture_raw_sources(mquery_entries)
        m_recovery = recover_raw_m_sources(
            mquery_entries,
            use_llm=use_llm,
            llm_model=_get("llm_model"),
            run_async=_run_async,
        )
        mquery_entries = m_recovery.entries

        # Parse MQuery, relationships and scan data, then build and run the pipeline.
        parsed = parse_model_inputs(
            mquery_entries=mquery_entries,
            measures=measures,
            relationships_raw=raw.relationships,
            scan_raw=raw.scan,
        )
        pipeline = run_pipeline(
            measures=measures,
            parsed=parsed,
            config=config,
            inner_dim_joins=inner_joins,
            unflatten_tables=unflatten,
            llm_config=llm_config,
        )

        # Visual usage: tag measures (direct, indirect), optional triage, then the
        # mapping and live-connection flags. All of it runs before emission.
        visual = apply_visual_usage(
            pipeline,
            raw.visual_usage,
            prune_requested=_get("prune_non_visual_measures"),
        )
        pbi_ucmv_mapping, live_connections = derive_mapping_and_connections(pipeline)

        # Emit YAML + SQL
        yaml_output = pipeline.emit_all_yaml(catalog=catalog, schema=schema)
        sql_output = pipeline.emit_all_sql(catalog=catalog, schema=schema)
        results = pipeline.get_results()

        source_mquery, source_transform_todos = audit_source_mquery(
            pipeline, raw_source_by_name
        )
        llm_translation_warning = llm_translation_warning_for(
            use_llm=use_llm,
            view_count=len(yaml_output or {}),
            raw_m_count=m_recovery.raw_m_count,
            raw_m_recovered=m_recovery.raw_m_recovered,
        )

        # Catch-all so no reference measure is silently dropped: measures that never
        # landed on a fact view go into a synthetic `none_allocated_measures` view.
        # Merged into yaml_output after validation below, so it is not validated.
        none_allocated_yaml = build_none_allocated(
            measures=measures,
            pipeline=pipeline,
            yaml_output=yaml_output,
            visual_usage_obj=visual.visual_usage_obj,
        )

        validation_results = run_expression_validation(yaml_output, raw.measures)

        # Store measures with DAX for downstream validation
        measures_for_validation = measures if isinstance(measures, list) else []
        resolved_measures_by_table = build_resolved_measures_by_table(results)

        # Worst-case fallback: per-table tabular extract, populated regardless of
        # whether the transpilation pipeline produced any view.
        fallback_extract = build_fallback_extract(measures, mquery_entries)

        # When nothing generated, always explain WHY. When the user also opted in
        # (allow_best_effort) AND supplied fact_source_map, draft thin views from the
        # measures that already resolved to real SQL. Every drafted measure is TODO.
        zero_view_diagnosis = None
        if not yaml_output:
            zero_view_diagnosis = diagnose_zero_views(mquery_entries, measures, config)
            logger.warning(
                "[UCMVGenerator] 0 views — %s: %s",
                zero_view_diagnosis["case"],
                zero_view_diagnosis["recommended_action"],
            )

        best_effort_report = None
        if (not yaml_output) and _get("allow_best_effort") and _get("fact_source_map"):
            fsm = _get("fact_source_map")
            if isinstance(fsm, str):
                try:
                    fsm = json.loads(fsm)
                except Exception:
                    logger.debug(
                        "[UCMVGenerator] fact_source_map is not valid JSON; ignoring it",
                        exc_info=True,
                    )
                    fsm = None
            be_views, best_effort_report = build_best_effort_views(
                fact_source_map=fsm, config=config, measures=measures
            )
            if be_views:
                yaml_output = be_views
                logger.warning(
                    "[UCMVGenerator] BEST-EFFORT mode: drafted %s view(s) / %s measure(s) "
                    "from a thin report; %s measure(s) unresolved and skipped. "
                    "Tables/measures may be MISSING — every measure marked TODO: verify.",
                    best_effort_report["tables_emitted"],
                    best_effort_report["measures_emitted"],
                    best_effort_report["measures_skipped_unresolved"],
                )

        # Count only the real generated views; the catch-all is added next and must
        # not inflate views_generated.
        generated_view_count = len(yaml_output) if isinstance(yaml_output, dict) else 0
        # Surface the catch-all as its own downloadable file (none_allocated_measures.yaml).
        if none_allocated_yaml:
            yaml_output = {
                **yaml_output,
                "none_allocated_measures": none_allocated_yaml,
            }

        source_layers = build_source_layer_outputs(
            pipeline=pipeline,
            mquery_tables=parsed.mquery_tables,
            config=config,
            catalog=catalog,
            schema=schema,
            yaml_output=yaml_output,
            validate_against_pbi=_get("validate_against_pbi"),
            access_token=_get("access_token"),
            workspace_id=_get("workspace_id"),
            dataset_id=_get("dataset_id"),
            pbi_api_base_url=_get("pbi_api_base_url"),
        )

        untranslatable_items = build_untranslatable_items(results.get("specs", {}))

        output = {
            "yaml": yaml_output,
            "sql": sql_output,
            # Full raw Power Query M per view (keyed like `sql`) + an audit of the
            # transformation steps it carries that may not be reflected in that SQL.
            "source_mquery": source_mquery,
            "source_transform_todos": source_transform_todos,
            # Loud, specific reason when LLM translation was on but produced nothing.
            # None when translation produced views.
            "llm_translation_warning": llm_translation_warning,
            "stats": results["stats"],
            "migration_report": results.get("migration_report", ""),
            "limitations": results.get("limitations", {}),
            "validation": validation_results,
            "measures_with_dax": measures_for_validation,
            "resolved_measures_by_table": resolved_measures_by_table,
            "mquery_raw": mquery_entries if isinstance(mquery_entries, list) else [],
            # Always present; the UI shows it as a tabular reference and falls back
            # to it as the primary artifact when `yaml` is empty (0 views).
            "fallback_extract": fallback_extract,
            # Present only when best-effort mode fired: surfaces the coverage gap.
            "best_effort_report": best_effort_report,
            # Present when 0 views generated: actionable "why + what to do".
            "zero_view_diagnosis": zero_view_diagnosis,
            "views_generated": generated_view_count,
            "specs_summary": {
                k: {
                    "view_name": v.get("view_name"),
                    "measures": v.get("measures_count"),
                    "untranslatable": v.get("untranslatable_count"),
                }
                for k, v in results.get("specs", {}).items()
            },
            # UI-ready flat list of non-emitted measures for the "Not transpiled" panel.
            "untranslatable_items": untranslatable_items,
            # How many measures got a non-empty used_in_visuals (a sanity count).
            "visual_usage_annotated_count": visual.annotated,
            # How many measures gained INDIRECT usage (backtraced sub-KPIs).
            "indirect_visual_usage_count": visual.indirect_annotated,
            # How many measures were pruned by visual-usage triage (opt-in).
            "measures_pruned_non_visual": visual.pruned,
            # {view_name: mapping_candidates_yaml_text} — the reconciliation
            # framework's input, deterministically derived. A DRAFT.
            "pbi_ucmv_mapping": pbi_ucmv_mapping,
            # Non-null ONLY when reconciliation was requested but did not run.
            "reconciliation_skipped": recon_skipped,
            # {view_name: {server, database, table}} — views backed by a live connection.
            "live_connections": live_connections,
            # rec#3: one CREATE VIEW per PBI table the metric views sit on.
            "source_layer_ddl": source_layers.source_layer_ddl,
            # S8: tables sourced from Excel/SharePoint/Web/typed literals.
            "pbi_only_ingestion_tasks": source_layers.pbi_only_ingestion_tasks,
            # rec#4: validation-loop status ("skipped" until a PBI callback is wired).
            "pbi_validation": source_layers.pbi_validation,
            "_diagnostics": diag,
        }
        output_json = json.dumps(output, indent=2)

        # Durable raw DAX persistence (Lakebase / conversion_history), fail-open.
        try:
            _run_async(
                self._save_dax_to_conversion_history(
                    raw_dax=build_raw_dax_extract(measures_for_validation),
                    yaml_output=yaml_output,
                    sql_output=sql_output,
                    workspace_id=_get("workspace_id"),
                    dataset_id=_get("dataset_id"),
                    catalog=catalog,
                    schema=schema,
                    untranslatable_items=untranslatable_items or [],
                    pbi_ucmv_mapping=pbi_ucmv_mapping or {},
                    live_connections=live_connections or {},
                    source_layer_ddl=source_layers.source_layer_ddl or {},
                    pbi_only_ingestion_tasks=source_layers.pbi_only_ingestion_tasks
                    or [],
                    pbi_validation=source_layers.pbi_validation or {},
                    reconciliation_skipped=recon_skipped,
                )
            )
        except Exception as hist_err:
            logger.warning(
                "[UCMVGenerator] conversion_history persistence skipped: %s",
                hist_err,
                exc_info=True,
            )

        return output_json
