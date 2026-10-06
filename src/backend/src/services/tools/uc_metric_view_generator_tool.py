"""UC Metric View Generator Tool for CrewAI — full pipeline."""

import json
import logging
import os
import re
import urllib.parse
from typing import Any, ClassVar, Optional, Tuple, Type

from pydantic import BaseModel, Field, PrivateAttr

from src.services.tools.base import BaseTool

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
            "calendar period-folding — CCH/Total-SC values are the defaults, so this "
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

    @staticmethod
    def _mask_secret(value: str | None) -> str:
        """Mask a secret value for logging."""
        if not value:
            return "none"
        if len(value) <= 8:
            return "***"
        return f"{value[:4]}...{value[-4:]}"

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

    def _run(self, **kwargs: Any) -> str:
        from src.services.tools.metric_view_utils.mquery_parser import MQueryParser
        from src.services.tools.metric_view_utils.pipeline import MetricViewPipeline
        from src.services.tools.metric_view_utils.relationships_loader import (
            RelationshipsLoader,
        )
        from src.services.tools.metric_view_utils.scan_data_parser import ScanDataParser

        def _get(key):
            return kwargs.get(key) or self._default_config.get(key)

        # Iterative reconciliation: when requested (and not already inside the
        # loop), delegate to the generate→deploy→reconcile→refine orchestrator. It
        # calls this same tool with reconciliation off (guarded by _in_recon_loop),
        # so there is no recursion. Falls through to single-pass if prerequisites
        # (warehouse_id / Power BI creds) are missing.
        #
        # Enablement is DERIVED, not just the explicit flag: not every UI surface
        # renders the enable_reconciliation checkbox (the generic tool-config form
        # shows only text fields), so a warehouse_id — meaningful for nothing but
        # reconciliation in this tool — is itself the request to reconcile. Without
        # this, an operator fills the field and reconciliation silently never runs.
        # reference_years is OPTIONAL (empty = compare all years), so it is NOT a
        # prerequisite; only the warehouse is. run_from_generator still validates
        # and degrades to single-pass if the warehouse/creds cannot be resolved.
        # Surfaced in the output (and persisted to history) whenever reconciliation
        # was WANTED but did not run, so a single-pass is never silent — the UI and
        # the saved result name the exact reason. None when recon ran or was never
        # requested.
        _recon_skipped = None
        _recon_requested = _get("enable_reconciliation") or _get("warehouse_id")
        if _recon_requested and not kwargs.get("_in_recon_loop"):
            # Reconciliation needs BOTH sides: a Databricks SQL warehouse (to
            # deploy + query the view) AND a Power BI connection + credentials (to
            # pull the ground-truth numbers). A warehouse alone is NOT enough — with
            # no PBI creds the loop deploys the view then fails every one on
            # "pbi_workspace_id is required", which surfaces as a generic "deploy
            # failed". Name exactly what is missing so the UI/history says why.
            from src.services.tools.metric_view_utils.reconciliation_prereqs import (
                missing_reconciliation_inputs,
            )

            _missing = missing_reconciliation_inputs(_get)
            if _missing:
                _recon_skipped = (
                    "Reconciliation was requested but did NOT run — missing: "
                    f"{'; '.join(_missing)}. Fill these in the UC Metric View "
                    "Generator's Reconciliation tab to enable the live cell-by-cell "
                    "Power BI check; generation ran single-pass."
                )
                logger.warning(f"[UCMV] {_recon_skipped}")
            else:
                try:
                    from src.services.tools.ucmv_iterative_recon import (
                        run_from_generator,
                    )

                    _loop_out = run_from_generator(self, kwargs)
                    if _loop_out is not None:
                        return json.dumps(_loop_out)
                    _recon_skipped = (
                        "Reconciliation was requested but did NOT run — the warehouse "
                        "or Power BI credentials could not be resolved; generation ran "
                        "single-pass."
                    )
                    logger.warning(f"[UCMV] {_recon_skipped}")
                except (
                    Exception
                ) as e:  # noqa: BLE001 — never let the loop break generation
                    _recon_skipped = f"Reconciliation was requested but FAILED: {e}"
                    logger.warning(
                        f"[UCMV] {_recon_skipped} — falling back to single-pass."
                    )

        # JSON inputs (measures/mquery/config/relationships/scan) are injected into
        # _default_config by the flow handoff. A capable agent, told to "call the
        # tool with ALL inputs", often passes its OWN placeholder for these (e.g.
        # a description string or "<measures_json>") which is truthy and overrides
        # the good injected value via _get's `or` — then json.loads() fails at
        # char 0. Guard: for JSON-shaped fields, if the kwarg isn't valid JSON but
        # the injected default IS present, use the injected default.
        _JSON_KEYS = (
            "measures_json",
            "mquery_json",
            "config_json",
            "naming_config",
            "refinement_feedback",
            "relationships_json",
            "scan_data_json",
            "visual_usage_index",
            "implicit_column_measures",
        )

        def _get_json(key):
            kw_val = kwargs.get(key)
            default_val = self._default_config.get(key)

            def _looks_like_json(v):
                if not isinstance(v, (str, list, dict)):
                    return False
                if isinstance(v, (list, dict)):
                    return True
                s = v.strip()
                return s.startswith("{") or s.startswith("[")

            # Prefer a valid kwarg; else the injected default; else the kwarg as-is.
            if kw_val is not None and _looks_like_json(kw_val):
                return kw_val
            if default_val is not None and _looks_like_json(default_val):
                if kw_val is not None and not _looks_like_json(kw_val):
                    logger.warning(
                        f"[UCMV] agent passed non-JSON {key}={str(kw_val)[:40]!r}; "
                        f"using flow-injected value instead"
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
        # Diagnostic: what arrived via flow injection/kwargs BEFORE any API-mode
        # extraction or DB fallback runs below, and whether the DB fallback ends
        # up firing. Carried into `output['_diagnostics']` (not just logged)
        # because raw log lines aren't reliably queryable in every deployment —
        # this makes it visible via the same execution_trace pull already used
        # for Crew 1.
        _diag: dict = {
            "preinject_measures_json_chars": (
                len(measures_raw) if isinstance(measures_raw, str) else None
            ),
            "preinject_mquery_json_chars": (
                len(mquery_raw) if isinstance(mquery_raw, str) else None
            ),
            "preinject_config_json_chars": (
                len(config_raw) if isinstance(config_raw, str) else None
            ),
            # Same shape as the three above, added after visual usage tags and
            # the PBI<->UCMV mapping showed up empty in a run where
            # config/measures/mquery all arrived intact — this is what tells
            # the difference between "the flow handoff dropped this one field"
            # and "Pipeline Config Generator's own report-definition extraction
            # legitimately came back empty this run" (both look like 0
            # annotations downstream, only this shows which one happened).
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
            # Cuts through speculation about WHY the DB fallback did/didn't
            # fire, without needing live debugger access: shows exactly what
            # this tool instance saw on self.trace_context (set by
            # attach_tools_trace_context via attach_execution_trace_context)
            # at the moment it ran. Compare job_id here against Pipeline
            # Config Generator's own "trace_context_seen" on the SAME run —
            # a mismatch (or a None on either side) is the actual root cause,
            # not something inferable from the narrative "Final Answer" text.
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

        # DB fallback: rebuild measures_json/mquery_json/config_json from Crew
        # 1's powerbi_extraction row for THIS SAME flow execution, whenever flow
        # injection left them empty. Deliberately runs BEFORE API-mode extraction
        # below, not after — API-mode extraction is Crew 2's OWN independent
        # (and materially weaker — no reference-following / parameter-
        # substitution resolution) re-derivation, and it always fills something
        # non-empty when workspace_id/dataset_id are configured. With the DB
        # fallback gated on "still empty" and running AFTER API-mode extraction,
        # it was silently dead code — API-mode had always already filled the
        # fields with its own degraded data by the time the check ran. Running
        # the DB fallback FIRST means Crew 1's higher-quality result is
        # preferred, and API-mode extraction becomes the true last resort (only
        # fills whatever's still missing after this).
        #
        # NOT a /tmp file — Crew 1 and Crew 2 run in separate subprocesses (a
        # file written by one is invisible to the other). This goes through
        # Kasal's DB (the same mechanism KasalFlowPersistence's checkpoints use,
        # which survives process/container boundaries), looked up by
        # execution_id == this tool's own job_id — not "most recent row for this
        # dataset" — so a same-day rerun of the same report can never pull
        # another run's data. Rebuilds via the exact same
        # PipelineConfigGeneratorTool static methods Crew 1 itself used, so
        # quality matches (not a degraded re-derivation): needs admin_tables +
        # expressions (for the reference-following / parameter-substitution
        # resolution), not just the raw table list a naive re-extraction gets.
        #
        # Architecture: reach the powerbi_extraction row through the OWNING
        # service (PowerBIExtractionService) via ToolSessionProvider — the tool
        # imports no repository and query construction stays in the repository.
        _rel_raw_missing = (not relationships_raw) or (
            isinstance(relationships_raw, str)
            and relationships_raw.strip() in ("", "[]", "null")
        )
        # Visual usage can be absent even when measures/mquery arrived — the flow
        # handoff drops it independently, and it is NOT persisted as its own
        # column. Trigger the extraction lookup for it too, so it can be
        # re-derived from the persisted report_definition below.
        _vu_raw_missing = (not visual_usage_raw) or (
            isinstance(visual_usage_raw, str)
            and visual_usage_raw.strip() in ("", "{}", "[]", "null")
        )
        if (
            measures_raw == "[]"
            or mquery_raw == "[]"
            or config_raw == "{}"
            or _rel_raw_missing
            or _vu_raw_missing
        ):
            try:
                from src.services.execution.kernel.trace_context import (
                    resolve_tool_execution_id,
                )

                _job_id = resolve_tool_execution_id(self)
            except Exception:
                _job_id = (getattr(self, "trace_context", None) or {}).get("job_id")
            if not _job_id:
                logger.info(
                    "[UCMV] DB fallback: no job_id on trace_context — cannot look up powerbi_extraction"
                )
            else:
                try:

                    async def _load_extraction():
                        from src.services.tools.tool_session_provider import (
                            ToolSessionProvider,
                        )

                        async with (
                            ToolSessionProvider.powerbi_extraction_service() as svc
                        ):
                            rows = await svc.list_for_execution(_job_id)
                            return rows[0] if rows else None

                    _extraction = _run_async(_load_extraction())
                    if _extraction is None:
                        logger.info(
                            f"[UCMV] DB fallback: no powerbi_extraction row found for job_id={_job_id}"
                        )
                    else:
                        _diag["db_fallback_extraction_id"] = _extraction.id
                        _db_admin_tables = _extraction.admin_tables or {}
                        _db_expressions = _extraction.expressions or {}
                        _db_measures = _extraction.measures or []
                        _db_config = _extraction.proposed_config or {}
                        logger.info(
                            f"[UCMV] DB fallback: found powerbi_extraction id={_extraction.id} for "
                            f"job_id={_job_id} ({len(_db_admin_tables)} tables, {len(_db_expressions)} expressions)"
                        )
                        from src.services.tools.pipeline_config_generator_tool import (
                            PipelineConfigGeneratorTool,
                        )

                        if mquery_raw == "[]" and _db_admin_tables:
                            _rebuilt_mquery = (
                                PipelineConfigGeneratorTool._build_ucmv_mquery(
                                    _db_admin_tables, _db_expressions
                                )
                            )
                            if _rebuilt_mquery:
                                mquery_raw = json.dumps(_rebuilt_mquery)
                                _diag["db_fallback_fired_for"].append("mquery_json")
                                logger.info(
                                    f"[UCMV] DB fallback: rebuilt mquery_json ({len(_rebuilt_mquery)} tables)"
                                )
                        if measures_raw == "[]" and _db_measures:
                            _rebuilt_measures = (
                                PipelineConfigGeneratorTool._build_ucmv_measures(
                                    _db_measures,
                                    admin_tables=_db_admin_tables,
                                    config=_db_config,
                                )
                            )
                            if _rebuilt_measures:
                                measures_raw = json.dumps(_rebuilt_measures)
                                _diag["db_fallback_fired_for"].append("measures_json")
                                logger.info(
                                    f"[UCMV] DB fallback: rebuilt measures_json ({len(_rebuilt_measures)} measures)"
                                )
                        if config_raw == "{}" and _db_config:
                            config_raw = json.dumps(_db_config)
                            _diag["db_fallback_fired_for"].append("config_json")
                        # Relationships drive RelationshipsLoader's fact→dim join
                        # enrichment. Without rebuilding them here, every SELECT-*
                        # fact whose joins come from PBI relationships (not in-SQL
                        # GROUP BY keys) silently loses ALL its joins when the flow
                        # injects an empty relationships_json — which is exactly the
                        # measures/mquery empty-injection case this fallback exists
                        # for. (Recovered fact_pe005's Dim_wkctr/Dim_Plant joins.)
                        _db_relationships = getattr(_extraction, "relationships", None)
                        if _rel_raw_missing and _db_relationships:
                            relationships_raw = json.dumps(_db_relationships)
                            _diag["db_fallback_fired_for"].append("relationships_json")
                            logger.info(
                                "[UCMV] DB fallback: rebuilt relationships_json "
                                f"({len(_db_relationships)} relationships)"
                            )
                        # Visual usage has no column of its own, but the raw
                        # report_definition IS persisted — re-derive the index from
                        # it when the handoff dropped it, so used_in_visuals / the
                        # "Used on" column / the visual-usage triage don't silently
                        # vanish on an empty handoff.
                        _db_report_def = getattr(_extraction, "report_definition", None)
                        if _vu_raw_missing and _db_report_def:
                            try:
                                from src.services.powerbi.visual_usage import (
                                    derive_visual_usage_index,
                                )

                                _rebuilt_vu = derive_visual_usage_index(_db_report_def)
                                if _rebuilt_vu:
                                    visual_usage_raw = json.dumps(_rebuilt_vu)
                                    _diag["db_fallback_fired_for"].append(
                                        "visual_usage_index"
                                    )
                                    logger.info(
                                        "[UCMV] DB fallback: re-derived "
                                        f"visual_usage_index ({len(_rebuilt_vu)} "
                                        "field(s)) from persisted report_definition"
                                    )
                            except Exception as _vu_err:  # noqa: BLE001
                                logger.warning(
                                    "[UCMV] DB fallback: visual_usage re-derive "
                                    f"failed: {_vu_err}"
                                )
                except Exception as _db_err:
                    _diag["db_fallback_error"] = str(_db_err)
                    logger.warning(f"[UCMV] DB fallback failed: {_db_err}")

        # Check if API extraction mode (PBI credentials provided). Skipped
        # entirely when the DB fallback above already filled both
        # measures_json and mquery_json — API-mode's own extraction includes a
        # live Admin Scanner trigger+poll (up to 5 minutes) that would otherwise
        # redo, slowly, work Crew 1 already did and this tool just reused for free.
        workspace_id = _get("workspace_id")
        dataset_id = _get("dataset_id")

        if workspace_id and dataset_id and (measures_raw == "[]" or mquery_raw == "[]"):
            pbi_api_base_url = _get("pbi_api_base_url") or ""
            valid, err_msg = self._validate_pbi_inputs(
                workspace_id, dataset_id, pbi_api_base_url
            )
            if not valid:
                return json.dumps({"error": f"PBI input validation failed: {err_msg}"})
            logger.info(
                f"[UCMV] API extraction mode: workspace={workspace_id}, dataset={dataset_id}, token={self._mask_secret(_get('access_token'))}"
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
                if extracted.get("measures") and measures_raw == "[]":
                    measures_raw = json.dumps(extracted["measures"])
                if extracted.get("mquery") and mquery_raw == "[]":
                    mquery_raw = json.dumps(extracted["mquery"])
                if extracted.get("relationships") and not relationships_raw:
                    relationships_raw = json.dumps(extracted["relationships"])
                if extracted.get("scan_data") and not scan_raw:
                    scan_raw = json.dumps(extracted["scan_data"])
            except Exception as e:
                logger.error(f"[UCMV] API extraction failed: {e}")
                return json.dumps({"error": f"PBI API extraction failed: {e}"})

        # Build LLM config if fallback enabled
        llm_config = None
        use_llm = _get("use_llm_fallback") or False
        if use_llm:
            llm_config = {
                "use_llm_fallback": True,
                "llm_model": _get("llm_model") or "databricks-claude-sonnet-4-5",
                "llm_workspace_url": _get("llm_workspace_url")
                or os.environ.get("DATABRICKS_HOST", ""),
                "llm_token": _get("llm_token")
                or os.environ.get("DATABRICKS_TOKEN", ""),
                # LLM-first translation (skill-corpus driven) is the default; the
                # regex patterns become a trivial fast-path. 'regex_first' restores
                # the prior regex-primary behaviour.
                "translation_mode": _get("translation_mode") or "llm_first",
            }
            # Customer-supplied domain context (free text) → the DAX→SQL
            # translation prompt, so the LLM translates with the model's business
            # semantics. Advisory and fail-open; only meaningful when LLM fallback
            # is on, which is why it rides llm_config.
            _dc = _get("domain_context")
            if isinstance(_dc, str) and _dc.strip():
                llm_config["domain_context"] = _dc.strip()
            # Time-intelligence is translated ONLY inside the reconciliation loop
            # (which validates the numbers); otherwise the DAX-LLM declines it so no
            # unvalidated prior-year/YTD math ships. This generation is the in-loop
            # one when _in_recon_loop is set.
            if kwargs.get("_in_recon_loop"):
                llm_config["reconciliation_active"] = True

        def _parse_json_input(raw, default):
            """Parse a JSON input; treat empty/blank as the default (never error)."""
            if not isinstance(raw, str):
                return raw if raw is not None else default
            s = raw.strip()
            if not s:
                return default
            return json.loads(s)

        # Iterative-reconciliation feedback → DAX LLM. Normalised to
        # {measure -> hint} and only used when LLM fallback is on.
        _rf_raw = _get_json("refinement_feedback")
        if _rf_raw and llm_config is not None:
            try:
                from src.services.tools.metric_view_utils.reconciliation import (
                    feedback_to_prompt_map,
                )

                _fb_map = feedback_to_prompt_map(_parse_json_input(_rf_raw, None))
                if _fb_map:
                    llm_config["refinement_feedback_by_measure"] = _fb_map
            except Exception as e:  # noqa: BLE001 — feedback is advisory
                logger.warning(f"[UCMV] Could not parse refinement_feedback: {e}")

        try:
            measures = _parse_json_input(measures_raw, [])
            mquery_entries = _parse_json_input(mquery_raw, [])
            config = _parse_json_input(config_raw, {})
        except json.JSONDecodeError as e:
            return json.dumps({"error": f"Invalid JSON input: {e}"})

        # Cross-fact UNION sources are emitted ONLY inside the reconciliation loop,
        # where PBI validates the numbers cell-by-cell. This generation IS the
        # in-loop one when `_in_recon_loop` is set (the iterative runner re-invokes
        # the tool with it). Off → no combined sources, no silently-wrong cross-fact
        # numbers (see cross_table_source_planner).
        if kwargs.get("_in_recon_loop"):
            config["reconciliation_active"] = True

        # Per-model naming/period vocabulary for the metadata lint + calendar
        # period-folding (defaults are CCH/Total-SC; only other models need it).
        # Lands under config["naming_config"] where the pipeline (lint) and
        # JoinDetector (period) read it.
        _naming_raw = _get_json("naming_config")
        if _naming_raw:
            try:
                _nc = _parse_json_input(_naming_raw, {})
                if isinstance(_nc, dict) and _nc:
                    config["naming_config"] = _nc
            except (json.JSONDecodeError, TypeError) as e:
                logger.warning(f"[UCMV] Failed to parse naming_config: {e}")

        # Implicit visual-column measures (from Pipeline Config Generator) ride
        # inside `config`, same bucket shape as switch_decompositions — merge
        # rather than overwrite in case config_json already carried one (e.g.
        # a manually-supplied config in a standalone/JSON-mode run).
        if implicit_column_measures_raw:
            try:
                _icm = _parse_json_input(implicit_column_measures_raw, {})
                if isinstance(_icm, dict) and _icm:
                    config.setdefault("implicit_column_measures", {})
                    for _tbl, _entries in _icm.items():
                        config["implicit_column_measures"].setdefault(_tbl, []).extend(
                            _entries
                        )
            except (json.JSONDecodeError, TypeError) as e:
                logger.warning(f"[UCMV] Failed to parse implicit_column_measures: {e}")

        # Carry-forward overrides from the reconciliation loop: measures that
        # already reconciled in a PRIOR cycle arrive as manual_overrides so
        # table_processor reuses their SQL verbatim and drops them from the LLM
        # batch — only the measures that MISSED are re-translated on a refine
        # cycle. Same bucket shape / merge semantics as the blocks above.
        _mo_raw = _get("manual_overrides")
        if _mo_raw:
            try:
                _mo = (
                    _parse_json_input(_mo_raw, {})
                    if isinstance(_mo_raw, str)
                    else _mo_raw
                )
                if isinstance(_mo, dict) and _mo:
                    config.setdefault("manual_overrides", {})
                    for _tbl, _entries in _mo.items():
                        if isinstance(_entries, list):
                            config["manual_overrides"].setdefault(_tbl, []).extend(
                                _entries
                            )
            except (json.JSONDecodeError, TypeError) as e:
                logger.warning(f"[UCMV] Failed to parse manual_overrides: {e}")

        # Capture the ORIGINAL source expression per table BEFORE the M→SQL LLM
        # recovery below rewrites it, so the UI can show the FULL raw Power Query M
        # (not just the derived Source SQL) and audit which transformation steps it
        # carries. Keyed by PBI table_name; mapped to the emit table_key later.
        _raw_source_by_name: dict = {}
        if isinstance(mquery_entries, list):
            for _e in mquery_entries:
                if isinstance(_e, dict) and _e.get("table_name"):
                    _raw_source_by_name[str(_e["table_name"])] = str(
                        _e.get("transpiled_sql") or ""
                    )

        # ── Raw Power Query M → SQL source recovery (opt-in) ────────────────
        # When a table's source is raw M (`let ... in ...`) with no embedded
        # native SQL, MQueryParser cannot extract a FROM clause → the table is
        # neither a fact nor has a source → 0 views. If LLM fallback is enabled,
        # rewrite those entries' transpiled_sql to a Spark SQL SELECT the parser
        # CAN read. Fail-open: entries the LLM can't translate are left as-is.
        # Hoisted so the post-emit safety net can tell "no raw-M tables" from "raw-M
        # tables whose LLM source-recovery produced nothing" (the shape of an LLM
        # failure — provider routing / FMAPI throttle / timeout — that otherwise
        # reads as a silent 0 views).
        _raw_m_count = 0
        _raw_m_recovered = 0
        if use_llm and isinstance(mquery_entries, list) and mquery_entries:
            try:
                from src.services.tools.metric_view_utils.mquery_parser import (
                    looks_like_raw_mquery,
                )

                _raw_m_count = sum(
                    1
                    for e in mquery_entries
                    if isinstance(e, dict)
                    and looks_like_raw_mquery(e.get("transpiled_sql") or "")
                )
                if _raw_m_count:
                    from src.services.tools.metric_view_utils.mquery_llm_fallback import (
                        recover_sources_with_llm,
                    )

                    logger.info(
                        f"[UCMV] {_raw_m_count} raw M-Query table(s) detected; attempting M→SQL LLM recovery"
                    )
                    mquery_entries, _raw_m_recovered = _run_async(
                        recover_sources_with_llm(
                            mquery_entries,
                            model=(_get("llm_model") or "databricks-claude-sonnet-4-5"),
                        )
                    )
                    logger.info(
                        f"[UCMV] M→SQL recovery: {_raw_m_recovered}/{_raw_m_count} table(s) recovered"
                    )
            except Exception as _m_err:
                logger.warning(
                    f"[UCMV] M→SQL LLM recovery skipped (non-fatal): {_m_err}"
                )

        # Parse MQuery
        parser = MQueryParser()
        mquery_tables = parser.parse_json(mquery_entries)

        # Parse relationships
        relationships_enrichment = {}
        m2n_relationships: list[dict] = []
        inactive_relationships: list[dict] = []
        if relationships_raw:
            try:
                rel_data = (
                    json.loads(relationships_raw)
                    if isinstance(relationships_raw, str)
                    else relationships_raw
                )
                loader = RelationshipsLoader()
                fact_keys = {k for k, v in mquery_tables.items() if v.is_fact}
                # Also enrich tables that have DAX measures allocated to them but are
                # NOT aggregate-SQL facts (raw-grain / data-vault sources like fact_pe005,
                # whose M is a plain SELECT). Phase 1b promotes these to facts *during*
                # the run — but RelationshipsLoader only builds joins for tables in
                # fact_keys, so without adding them here they get ZERO joins and their
                # join-dependent measures (e.g. the *_Yeild_Actual ratios filtering on
                # Dim_wkctr/Dim_Plant) decline. Gate on a real source_table (matches
                # Phase 1b) so UI/selection/measure-holder tables aren't pulled in.
                for _m in measures or []:
                    _allocs = [
                        a.get("table") for a in (_m.get("all_allocations") or [])
                    ] or [_m.get("proposed_allocation")]
                    for _t in _allocs:
                        _ti = mquery_tables.get(_t)
                        if (
                            _t
                            and _ti is not None
                            and getattr(_ti, "source_table", None)
                        ):
                            fact_keys.add(_t)
                relationships_enrichment = loader.load(
                    rel_data, mquery_tables, fact_keys
                )
                m2n_relationships = loader.get_skipped_m2n()
                inactive_relationships = loader.get_inactive_relationships()
            except Exception as e:
                logger.warning(f"Failed to parse relationships: {e}")

        # Parse scan data
        scan_data = {}
        scan_parser = ScanDataParser()
        if scan_raw:
            try:
                scan_obj = (
                    json.loads(scan_raw) if isinstance(scan_raw, str) else scan_raw
                )
                scan_data = scan_parser.parse(scan_obj)
            except Exception as e:
                logger.warning(f"Failed to parse scan data: {e}")

        # Run pipeline
        pipeline = MetricViewPipeline(
            mapping=measures,
            mquery_tables=mquery_tables,
            config=config,
            inner_dim_joins=inner_joins,
            scan_data=scan_data,
            unflatten_tables=unflatten or bool(scan_data),
            relationships_enrichment=relationships_enrichment,
            llm_config=llm_config,
            inactive_relationships=inactive_relationships or None,
            m2n_relationships=m2n_relationships or None,
            refresh_policy_tables=scan_parser.get_refresh_policy_tables() or None,
            no_summarize_columns=scan_parser.get_no_summarize_columns() or None,
            rls_tables=scan_parser.get_rls_tables() or None,
        )
        pipeline.run()

        # Tag each measure with WHERE it's actually seen in the report (PROP-8)
        # — mutates pipeline.all_specs in place, so it must run before emission
        # to reach the YAML comment, migration report, and JSON output alike.
        visual_usage_annotated = 0
        # Hoisted so the none_allocated catch-all can reuse it to annotate its own
        # (freshly-translated) measures with the same visual reference.
        visual_usage_obj: dict | None = None
        if visual_usage_raw:
            try:
                _parsed_vu = (
                    json.loads(visual_usage_raw)
                    if isinstance(visual_usage_raw, str)
                    else visual_usage_raw
                )
                visual_usage_obj = _parsed_vu if isinstance(_parsed_vu, dict) else None
                if isinstance(visual_usage_obj, dict):
                    from src.services.tools.metric_view_utils.visual_usage_annotator import (
                        annotate_visual_usage,
                    )

                    visual_usage_annotated = annotate_visual_usage(
                        pipeline.all_specs, visual_usage_obj
                    )
            except Exception as e:
                logger.warning(f"Failed to parse/apply visual_usage_index: {e}")

        # Backtrace: propagate that direct usage DOWN the measure-dependency
        # graph so sub-KPIs feeding a visual-placed KPI are surfaced too
        # (indirect_visual_usage, kept separate from the direct tag). Runs after
        # the direct pass and before emission; fail-open.
        indirect_visual_annotated = 0
        try:
            from src.services.tools.metric_view_utils.visual_usage_annotator import (
                annotate_indirect_visual_usage,
            )

            indirect_visual_annotated = annotate_indirect_visual_usage(
                pipeline.all_specs
            )
        except Exception as e:
            logger.warning(f"Failed to derive indirect visual usage: {e}")

        # TRIAGE (opt-in): drop measures with no direct AND no indirect visual
        # usage, so a report's legacy/unused measures are not translated-noise in
        # the output. Gated on BOTH the flag AND real visual data (direct
        # annotation count > 0) — without a visual-usage index every measure looks
        # unused and we must NOT prune (we'd drop the whole model). Base/implicit
        # column measures are always kept (see prune_non_visual_measures).
        measures_pruned_non_visual = 0
        if _get("prune_non_visual_measures") and visual_usage_annotated > 0:
            try:
                from src.services.tools.metric_view_utils.visual_usage_annotator import (
                    prune_non_visual_measures,
                )

                measures_pruned_non_visual = prune_non_visual_measures(
                    pipeline.all_specs
                )
                logger.info(
                    f"[UCMVGenerator] Visual-usage triage pruned "
                    f"{measures_pruned_non_visual} measure(s) not reachable from any "
                    "visual (directly or via dependency chain)."
                )
            except Exception as e:
                logger.warning(f"Visual-usage pruning skipped: {e}")

        # Reconciliation framework input (priority 3): a deterministic
        # PBI-measure <-> UCMV-measure mapping draft per fact table. Reads
        # each measure's used_in_visuals, so it runs after the annotation
        # above — but unlike that step, ordering relative to emission doesn't
        # matter here: this is its own output key, not embedded in the
        # YAML/report.
        pbi_ucmv_mapping: dict = {}
        try:
            from src.services.tools.metric_view_utils.pbi_ucmv_mapping import (
                derive_pbi_ucmv_mapping,
            )

            pbi_ucmv_mapping = derive_pbi_ucmv_mapping(pipeline.all_specs)
        except Exception as e:
            logger.warning(f"Failed to derive pbi_ucmv_mapping: {e}")

        # Live-connection flag: which views are backed by a live connection to a
        # semantic model (M-Query AnalysisServices.Database) rather than a
        # warehouse table — the transpiler can't resolve those, so surface WHICH
        # semantic model/table to parse. {view_name: {server, database, table}}.
        live_connections: dict = {}
        try:
            from src.services.powerbi.live_connection import derive_live_connections

            live_connections = derive_live_connections(
                pipeline.all_specs, getattr(pipeline, "_mquery_expressions", {})
            )
            if live_connections:
                logger.info(
                    f"[UCMVGenerator] {len(live_connections)} view(s) on a live "
                    "semantic-model connection (flagged for manual parse)"
                )
        except Exception as e:
            logger.warning(f"Failed to derive live_connections: {e}")

        # Emit YAML + SQL
        yaml_output = pipeline.emit_all_yaml(catalog=catalog, schema=schema)
        sql_output = pipeline.emit_all_sql(catalog=catalog, schema=schema)
        results = pipeline.get_results()

        # Full raw Power Query M per view (same table_key space as sql_output) so
        # the UI can show the FULL M statement alongside the derived Source SQL,
        # plus an audit of transformation steps the M carries that may NOT be
        # reflected in that SQL (filters, group-by, joins, …) — so the loss is
        # visible, not silent. Prefer the pre-recovery original M; fall back to
        # the parsed source expression.
        source_mquery: dict = {}
        source_transform_todos: dict = {}
        try:
            from src.services.tools.metric_view_utils.mquery_transform_audit import (
                audit_mquery_transformations,
            )

            for _tk, _ti in (pipeline.mquery_tables or {}).items():
                _raw = (
                    _raw_source_by_name.get(getattr(_ti, "table_name", ""))
                    or getattr(_ti, "raw_transpiled_sql", "")
                    or getattr(_ti, "full_sql", "")
                )
                if _raw:
                    source_mquery[_tk] = _raw
                    _todos = audit_mquery_transformations(_raw)
                    if _todos:
                        source_transform_todos[_tk] = _todos
        except Exception as _sm_err:  # noqa: BLE001 — display aid, never fatal
            logger.warning(f"[UCMV] source_mquery/audit build failed: {_sm_err}")

        # Safety net: when LLM translation is ON but nothing came through, say WHY
        # loudly instead of returning a silent empty result. A total 0-view yield —
        # or raw-M tables that recovered NONE of their sources — is the exact shape
        # of the subprocess LLM failure (provider routing / FMAPI rate-limit /
        # timeout) that otherwise reads as "the tool just produced nothing".
        llm_translation_warning = None
        if use_llm:
            _view_count = len(yaml_output or {})
            if _raw_m_count and not _raw_m_recovered:
                llm_translation_warning = (
                    f"LLM source recovery produced nothing for all {_raw_m_count} raw "
                    "Power Query table(s), so they yielded no views. This is almost "
                    "always the DAX/M LLM calls failing — provider routing, an FMAPI "
                    "rate-limit/throttle, or the subprocess timing out — NOT an empty "
                    "model. Check the run log for 'LLM Provider NOT provided' or "
                    "throttling and re-run; if it timed out, lower the DAX LLM batch "
                    "size/concurrency or raise the subprocess limit."
                )
            elif _view_count == 0:
                llm_translation_warning = (
                    "0 metric views were generated with LLM translation enabled. If "
                    "measures were present, the DAX→SQL LLM calls likely failed "
                    "(provider routing / FMAPI throttle / timeout) rather than the "
                    "model being empty — check the run log and re-run."
                )
            if llm_translation_warning:
                logger.warning(f"[UCMV] {llm_translation_warning}")

        # Catch-all so no reference measure is silently dropped: gather every
        # measure that never landed on a fact view into a synthetic
        # `none_allocated_measures` view — best-effort translated where possible,
        # the rest documented as comments with DAX + reason (visual/formatting/
        # slicer artifacts are labelled as such, since Genie/analytics don't use
        # them). Merged into yaml_output AFTER validation below so it is not
        # validated as a deployable view (it spans multiple facts by design).
        none_allocated_yaml = None
        try:
            from src.services.tools.metric_view_utils.none_allocated_emitter import (
                build_none_allocated_yaml,
            )

            # Reconcile against the FULL extracted measure set (all 138), not a
            # downstream-derived subset, so nothing can slip through the gap
            # between "extracted" and "allocated".
            none_allocated_yaml = build_none_allocated_yaml(
                measures if isinstance(measures, list) else pipeline.mapping,
                pipeline.all_specs,
                yaml_output,  # rendered real views: covered == actually in the export
                pipeline.translator,
                pipeline._PBI_ARTIFACT_PATTERNS,
                visual_usage_index=visual_usage_obj,
            )
        except Exception as _na_err:
            logger.warning(
                f"[UCMVGenerator] none_allocated_measures emit skipped: {_na_err}"
            )

        # Run validation (optional — compares DAX structure vs generated SQL)
        validation_results = {}
        try:
            from src.services.tools.metric_view_validation_utils.pipeline import (
                MetricExpressionValidatorPipeline,
            )

            if measures_raw and measures_raw != "[]":
                mapping_for_val = (
                    json.loads(measures_raw)
                    if isinstance(measures_raw, str)
                    else measures_raw
                )
                import tempfile  # NOTE: os is already imported at module level; importing it

                # here too would make `os` a function-local for all of _run() and break the
                # earlier os.environ.get(...) calls with UnboundLocalError.
                for table_key, yml in yaml_output.items():
                    with tempfile.NamedTemporaryFile(
                        mode="w", suffix=".yml", delete=False
                    ) as yf:
                        yf.write(yml)
                        yf_path = yf.name
                    with tempfile.NamedTemporaryFile(
                        mode="w", suffix=".json", delete=False
                    ) as mf:
                        json.dump(mapping_for_val, mf)
                        mf_path = mf.name
                    try:
                        validator = MetricExpressionValidatorPipeline(
                            table_mappings={table_key: "source"}
                        )
                        vr = validator.run(
                            metrics_view_yaml_path=yf_path,
                            table_mapping_json_path=mf_path,
                        )
                        evaluated = vr.get("evaluated", [])
                        if evaluated:
                            valid = sum(
                                1
                                for m in evaluated
                                if m.get("measure_eval_result", {}).get("status")
                                == "VALID"
                            )
                            validation_results[table_key] = {
                                "evaluated": len(evaluated),
                                "valid": valid,
                            }
                    finally:
                        os.unlink(yf_path)
                        os.unlink(mf_path)
        except ImportError:
            pass  # Validation package not available
        except Exception as e:
            logger.warning(f"Validation failed: {e}")

        # Store measures with DAX for downstream validation
        _measures_for_validation = []
        if isinstance(measures, list):
            _measures_for_validation = measures

        # Resolved measure→DAX map, keyed by FACT TABLE (the YAML table key).
        # This is what the Quality Validator needs: each translated measure paired
        # with its ORIGINAL DAX, allocated to the correct fact table. Unlike the
        # raw `measures` list (keyed by PBI holder-table), this uses the pipeline's
        # own allocation so the validator can pair YAML measures ↔ DAX and run the
        # filter/aggregation comparison.
        resolved_measures_by_table = {}
        for table_key, spec in (results.get("specs", {}) or {}).items():
            rows = []
            for m in spec.get("measures", []):
                rows.append(
                    {
                        "measure_name": m.get("name", ""),
                        "original_name": m.get("original_name", ""),
                        "sql_expr": m.get("sql_expr", ""),
                        "dax_expression": m.get("dax_expression", ""),
                        "proposed_allocation": table_key,  # fact-table key, matches YAML
                        "table_name": table_key,
                    }
                )
            if rows:
                resolved_measures_by_table[table_key] = rows

        # ── Worst-case fallback: per-table tabular extract ─────────────────
        # Even when NO views generate (e.g. an all-raw-Power-Query-M model where
        # neither fact detection nor the M→SQL fallback could produce a UCMV),
        # the customer should still get the raw material they can act on: for
        # each PBI table, its M-Query source expression plus the measures/DAX
        # associated with it. Built from the tool's inputs, so it is populated
        # regardless of whether the transpilation pipeline succeeded.
        fallback_extract = self._build_fallback_extract(measures, mquery_entries)

        # ── Opt-in best-effort views for a THIN-REPORT model ────────────────
        # When nothing generated, always compute an actionable diagnosis of WHY
        # (thin report? no source? unresolvable DAX?) so the run is never a
        # silent empty result. Computed before best-effort so it reflects the
        # real state. When the user also opted in (allow_best_effort) AND
        # supplied physical sources (fact_source_map), draft thin UCMVs from
        # measures that already resolved to real SQL — the fallback-of-the-
        # fallback; every drafted measure is flagged TODO: verify.
        zero_view_diagnosis = None
        if not yaml_output:
            zero_view_diagnosis = self._diagnose_zero_views(
                mquery_entries, measures, config
            )
            logger.warning(
                f"[UCMVGenerator] 0 views — {zero_view_diagnosis['case']}: "
                f"{zero_view_diagnosis['recommended_action']}"
            )

        best_effort_report = None
        if (not yaml_output) and _get("allow_best_effort") and _get("fact_source_map"):
            fsm = _get("fact_source_map")
            if isinstance(fsm, str):
                try:
                    fsm = json.loads(fsm)
                except Exception:
                    fsm = None
            be_views, best_effort_report = self._build_best_effort_views(
                fact_source_map=fsm, config=config, measures=measures
            )
            if be_views:
                yaml_output = be_views
                logger.warning(
                    f"[UCMVGenerator] BEST-EFFORT mode: drafted {best_effort_report['tables_emitted']} "
                    f"view(s) / {best_effort_report['measures_emitted']} measure(s) from a thin report; "
                    f"{best_effort_report['measures_skipped_unresolved']} measure(s) unresolved and skipped. "
                    f"Tables/measures may be MISSING — every measure marked TODO: verify."
                )

        # Count only the real generated views; the catch-all worklist is added
        # next and must not inflate views_generated.
        _generated_view_count = len(yaml_output) if isinstance(yaml_output, dict) else 0
        # Surface the catch-all as its own downloadable file
        # (none_allocated_measures.yaml), alongside the real views.
        if none_allocated_yaml:
            yaml_output = {
                **yaml_output,
                "none_allocated_measures": none_allocated_yaml,
            }

        # Source-layer views (rec#3), PBI-only ingestion tasks (S8) and the
        # PBI-validation hook (rec#4). Each fail-open, each its own output key —
        # the bridge lives in source_artifacts so this tool doesn't carry the glue.
        source_layer_ddl: dict = {}
        pbi_only_ingestion_tasks: list = []
        pbi_validation_report: dict = {}
        try:
            from src.services.tools.metric_view_utils.source_artifacts import (
                build_source_layer,
                detect_pbi_only_tables,
                run_pbi_validation,
            )

            _mq_exprs = getattr(pipeline, "_mquery_expressions", {}) or {}
            source_layer_ddl = build_source_layer(
                pipeline.all_specs,
                mquery_tables,
                config,
                catalog,
                schema,
                mquery_expressions=_mq_exprs,
            )
            pbi_only_ingestion_tasks = detect_pbi_only_tables(
                _mq_exprs, catalog, schema
            )
            # rec#4: only build a live PBI callback when explicitly opted in (and
            # creds are present) — otherwise run_pbi_validation reports "skipped"
            # and makes no network calls.
            _evaluate_fn = None
            if _get("validate_against_pbi"):
                from src.services.tools.metric_view_utils.source_artifacts import (
                    build_pbi_evaluate_fn,
                )

                _evaluate_fn = build_pbi_evaluate_fn(
                    _get("access_token"),
                    _get("workspace_id"),
                    _get("dataset_id"),
                    base_url=_get("pbi_api_base_url"),
                )
            pbi_validation_report = run_pbi_validation(
                pipeline.all_specs, yaml_output, evaluate_fn=_evaluate_fn
            )
            if source_layer_ddl or pbi_only_ingestion_tasks:
                logger.info(
                    f"[UCMVGenerator] source layer: {len(source_layer_ddl)} view(s); "
                    f"PBI-only ingestion tasks: {len(pbi_only_ingestion_tasks)}"
                )
        except Exception as e:
            logger.warning(
                f"[UCMVGenerator] source-layer/ingestion/validation step skipped: {e}"
            )

        output = {
            "yaml": yaml_output,
            "sql": sql_output,
            # Full raw Power Query M per view (keyed like `sql`) + an audit of the
            # transformation steps it carries that may not be reflected in that SQL.
            "source_mquery": source_mquery,
            "source_transform_todos": source_transform_todos,
            # Loud, specific reason when LLM translation was on but produced nothing
            # — so an LLM failure (provider/throttle/timeout) never reads as a silent
            # 0-view result. None when translation produced views.
            "llm_translation_warning": llm_translation_warning,
            "stats": results["stats"],
            "migration_report": results.get("migration_report", ""),
            "limitations": results.get("limitations", {}),
            "validation": validation_results,
            "measures_with_dax": _measures_for_validation,
            "resolved_measures_by_table": resolved_measures_by_table,
            "mquery_raw": mquery_entries if isinstance(mquery_entries, list) else [],
            # Always present; the UI shows it as a tabular reference and falls back
            # to it as the primary artifact when `yaml` is empty (0 views).
            "fallback_extract": fallback_extract,
            # Present only when best-effort mode fired: surfaces the coverage gap
            # (skipped tables/measures) so the UI/consumer can warn "may be missing".
            "best_effort_report": best_effort_report,
            # Present when 0 views generated: actionable "why + what to do" so a
            # thin-report run is never a silent empty result.
            "zero_view_diagnosis": zero_view_diagnosis,
            "views_generated": _generated_view_count,
            "specs_summary": {
                k: {
                    "view_name": v.get("view_name"),
                    "measures": v.get("measures_count"),
                    "untranslatable": v.get("untranslatable_count"),
                }
                for k, v in results.get("specs", {}).items()
            },
            # UI-ready flat list of non-emitted measures (original DAX, why, proposal,
            # DRAFT source-view SQL) for the validation "Not transpiled" review panel.
            "untranslatable_items": self._build_untranslatable_items(
                results.get("specs", {})
            ),
            # How many measures got a non-empty used_in_visuals — a sanity
            # count, not the data itself (that's on each measure in `specs`/
            # `yaml`/the migration report). 0 with a non-empty
            # visual_usage_index means nothing in scope matched any page's
            # projections/filters, which is a real (if unusual) result, not
            # necessarily a bug.
            "visual_usage_annotated_count": visual_usage_annotated,
            # How many measures gained INDIRECT usage (a visual-placed measure
            # references them transitively) — backtraced sub-KPIs.
            "indirect_visual_usage_count": indirect_visual_annotated,
            # How many measures were pruned by visual-usage triage (opt-in via
            # prune_non_visual_measures) — not shown/filtered in any visual,
            # directly or via a dependency chain. 0 when triage is off or there
            # was no visual-usage data to triage on.
            "measures_pruned_non_visual": measures_pruned_non_visual,
            # {view_name: mapping_candidates_yaml_text} — the reconciliation
            # framework's (dqa/kpi_reconciliation) input, deterministically
            # derived instead of hand-authored. A DRAFT: binding: fields are
            # left TODO (see pbi_ucmv_mapping.py's own docstring for why),
            # and every measure's pbi_kind should be spot-checked before
            # trusting it for reconciliation.
            "pbi_ucmv_mapping": pbi_ucmv_mapping,
            # Non-null ONLY when reconciliation was requested but did not run
            # (missing reference_years/warehouse, unresolved creds, or an error).
            # Names the reason so a single-pass is never silent — the UI renders
            # it as a warning.
            "reconciliation_skipped": _recon_skipped,
            # {view_name: {server, database, table}} — views backed by a live
            # connection to a semantic model; the UI flags these as a note
            # telling the team which model/table to parse to complete them.
            "live_connections": live_connections,
            # rec#3: one CREATE VIEW per PBI table reproducing the physical read
            # (native-query SQL / passthrough + calc columns) the metric views sit
            # on. {table: {ddl, todo_steps, error}}.
            "source_layer_ddl": source_layer_ddl,
            # S8: tables sourced from Excel/SharePoint/Web/typed literals — need an
            # ingestion snapshot, not a SQL translation. [{table, uc_target, kind,
            # recommended_approach, snapshot_loader_stub}].
            "pbi_only_ingestion_tasks": pbi_only_ingestion_tasks,
            # rec#4: validation-loop hook. "skipped" until a live PBI EVALUATE
            # callback (service-account executeQueries) is wired; the mechanism is
            # ready in pbi_validation.
            "pbi_validation": pbi_validation_report,
            "_diagnostics": _diag,
        }
        output_json = json.dumps(output, indent=2)

        # Write to /tmp so the Validator tool can find it as a fallback
        # (in case flow injection into _default_config fails for any reason).
        try:
            import glob as _glob

            # Clean up old UCMV tmp files to avoid stale data
            for _old in _glob.glob("/tmp/ucmv_latest_*.json"):
                try:
                    os.unlink(_old)
                except OSError:
                    pass
            _tmp_path = f"/tmp/ucmv_latest_{os.getpid()}.json"
            with open(_tmp_path, "w") as _f:
                _f.write(output_json)
            logger.info(
                f"[UCMVGenerator] Wrote output to {_tmp_path} for validator fallback"
            )
        except Exception as _tmp_err:
            logger.debug(f"[UCMVGenerator] Could not write /tmp fallback: {_tmp_err}")

        # ── Durable raw DAX persistence (Lakebase / conversion_history) ──────
        try:
            _run_async(
                self._save_dax_to_conversion_history(
                    raw_dax=self._build_raw_dax_extract(_measures_for_validation),
                    yaml_output=yaml_output,
                    sql_output=sql_output,
                    workspace_id=_get("workspace_id"),
                    dataset_id=_get("dataset_id"),
                    catalog=catalog,
                    schema=schema,
                    untranslatable_items=output.get("untranslatable_items") or [],
                    pbi_ucmv_mapping=output.get("pbi_ucmv_mapping") or {},
                    live_connections=output.get("live_connections") or {},
                    source_layer_ddl=output.get("source_layer_ddl") or {},
                    pbi_only_ingestion_tasks=output.get("pbi_only_ingestion_tasks")
                    or [],
                    pbi_validation=output.get("pbi_validation") or {},
                    reconciliation_skipped=output.get("reconciliation_skipped"),
                )
            )
        except Exception as _hist_err:
            logger.warning(
                f"[UCMVGenerator] conversion_history persistence skipped: {_hist_err}"
            )
        # ────────────────────────────────────────────────────────────────────

        return output_json

    @staticmethod
    def _build_untranslatable_items(specs: dict) -> list:
        """Flatten every spec's untranslatable measures into one UI-ready list.

        Feeds the validation-UI "Not transpiled" review panel: reviewers see the
        non-emitted measures as first-class rows (original DAX + why skipped +
        category + dependency in-degree + proposal + DRAFT source-view SQL) instead
        of digging through the YAML `-- comment` block. Additive. Returns [] when
        nothing was skipped; high-impact (most-referenced) gaps sorted first.
        """
        items: list = []
        for table_key, spec in (specs or {}).items():
            view_name = spec.get("view_name")
            for m in spec.get("untranslatable", []) or []:
                items.append(
                    {
                        "table_key": table_key,
                        "view_name": view_name,
                        "original_name": m.get("original_name") or m.get("name"),
                        "dax_expression": m.get("dax_expression", ""),
                        "skip_reason": m.get("skip_reason", ""),
                        "category": m.get("category", ""),
                        "dax_class": m.get("dax_class"),
                        "referenced_by": m.get("referenced_by", 0),
                        # Actionable next-step for the reviewer (HOW to handle it),
                        # sourced from the LLM's recipe or a class-based default.
                        "proposal": m.get("proposal", ""),
                        "explanation": m.get("explanation"),
                        # Labeled DRAFT CREATE VIEW scaffold for cross-fact / multi-stage
                        # (proposal artifact, never an emitted measure).
                        "source_view_sql_draft": m.get("source_view_sql_draft"),
                        "used_in_visuals": m.get("used_in_visuals") or [],
                        # Backtraced usage: a visual-placed KPI references this
                        # non-emitted measure — surfaced in the review panel too.
                        "indirect_visual_usage": m.get("indirect_visual_usage") or [],
                    }
                )
        items.sort(key=lambda x: x.get("referenced_by", 0), reverse=True)
        return items

    # ------------------------------------------------------------------
    # Durable raw DAX persistence (conversion_history / Lakebase)
    # ------------------------------------------------------------------

    @staticmethod
    def _build_raw_dax_extract(measures: Any) -> list:
        """Collect the full, untruncated original DAX expression per measure.

        Mirrors the M-Query extract produced by the MQuery Conversion Pipeline:
        the complete raw source DAX is retained verbatim so it can be persisted
        and retrieved later, rather than only living in the transient result.
        """
        extract = []
        if not isinstance(measures, list):
            return extract
        for m in measures:
            if not isinstance(m, dict):
                continue
            extract.append(
                {
                    "measure_name": m.get("measure_name")
                    or m.get("original_name")
                    or "",
                    "original_name": m.get("original_name")
                    or m.get("measure_name")
                    or "",
                    "dax_expression": m.get("dax_expression") or "",
                    "proposed_allocation": m.get("proposed_allocation") or "",
                }
            )
        return extract

    @staticmethod
    def _build_fallback_extract(measures: Any, mquery_entries: Any) -> list:
        """Per-table tabular extract of M-Query source + associated measures/DAX.

        The worst-case safety net: when no UC Metric View can be generated (e.g.
        an all-raw-Power-Query-M model), the customer still gets the raw material
        organised per table — the M-Query source expression and every measure
        (with its DAX) allocated to that table. Rendered as a table in the UI.

        Groups by the table each measure is allocated to (proposed_allocation /
        table_name) and each M-Query entry's table_name. Tables that appear in
        either source are included, so a table with M-Query but no measures (and
        vice-versa) is still listed.
        """
        by_table: dict[str, dict] = {}

        def _slot(name: str) -> dict:
            key = name or "__unassigned__"
            if key not in by_table:
                by_table[key] = {"table_name": key, "mquery": "", "measures": []}
            return by_table[key]

        # M-Query source per table
        if isinstance(mquery_entries, list):
            for e in mquery_entries:
                if not isinstance(e, dict):
                    continue
                tname = e.get("table_name") or ""
                if not tname:
                    continue
                slot = _slot(tname)
                # Prefer a non-empty source; keep the first seen otherwise.
                src = e.get("transpiled_sql") or e.get("mquery_expression") or ""
                if src and not slot["mquery"]:
                    slot["mquery"] = src

        # Measures + DAX per table
        if isinstance(measures, list):
            for m in measures:
                if not isinstance(m, dict):
                    continue
                alloc = (
                    m.get("proposed_allocation")
                    or m.get("table_name")
                    or m.get("table")
                    or "__unassigned__"
                )
                _slot(alloc)["measures"].append(
                    {
                        "measure_name": m.get("measure_name")
                        or m.get("original_name")
                        or "",
                        "dax_expression": m.get("dax_expression")
                        or m.get("expression")
                        or "",
                    }
                )

        # Stable, readable ordering: tables with measures first, then by name.
        rows = list(by_table.values())
        rows.sort(key=lambda r: (-len(r["measures"]), r["table_name"]))
        for r in rows:
            r["measure_count"] = len(r["measures"])
            r["has_mquery"] = bool(r["mquery"])
        return rows

    @staticmethod
    def _diagnose_zero_views(
        mquery_entries: Any,
        measures: Any,
        config: Any,
    ) -> dict:
        """Explain WHY 0 UC Metric Views were generated, and what to do about it.

        A thin-report model otherwise silently yields a fallback JSON and no
        views, with no signal to the user about the cause or fix. This turns
        that into an actionable diagnosis. It classifies the run by two
        deterministic signals:
          * source tables present?  → any mquery entry with real transpiled SQL /
            an M source expression (not raw/empty).
          * measures resolvable?    → any config.measure_resolutions with real
            aggregatable SQL (not TODO).

        Returns {case, reason, recommended_action, signals} — see the customer
        guide thin-report-and-source-resolution.md for the three cases.
        """
        # Signal 1: do we have any real source table?
        has_source = False
        if isinstance(mquery_entries, list):
            for e in mquery_entries:
                if not isinstance(e, dict):
                    continue
                sql = (
                    e.get("transpiled_sql") or e.get("mquery_expression") or ""
                ).strip()
                if sql and sql not in ("{}", "null"):
                    has_source = True
                    break

        # Signal 2: any measure that resolved to real aggregatable SQL?
        resolvable = 0
        _AGG = (
            "SUM",
            "COUNT",
            "AVG",
            "MIN",
            "MAX",
            "DIVIDE",
            "CALCULATE",
            "SUMX",
            "COUNTX",
        )
        resolutions = (
            (config or {}).get("measure_resolutions", {})
            if isinstance(config, dict)
            else {}
        )
        for res in (resolutions or {}).values():
            base = (res or {}).get("base_expr", "") if isinstance(res, dict) else ""
            if (
                base
                and not base.strip().upper().startswith("TODO")
                and base.strip().upper().startswith(_AGG)
            ):
                resolvable += 1

        n_measures = len(measures) if isinstance(measures, list) else 0
        signals = {
            "has_source_tables": has_source,
            "resolvable_measures": resolvable,
            "total_measures": n_measures,
        }

        if has_source:
            # Sources exist but still 0 views — a translation/allocation issue,
            # not a thin-report problem. Leave it to the normal migration report.
            return {
                "case": "sources_present_no_views",
                "reason": (
                    "Source tables were found but no metric view was emitted — "
                    "likely all measures were untranslatable DAX. See the "
                    "migration report / not-emitted notes."
                ),
                "recommended_action": "Review the not-emitted measures; no source mapping needed.",
                "signals": signals,
            }
        # No source tables → thin report (case B) or hand-entered (case C).
        if resolvable > 0:
            action = (
                f"This looks like a THIN REPORT on an upstream semantic model — its "
                f"source tables are not in what was extracted. PREFERRED: re-run "
                f"against the upstream model's dataset_id (see Power BI lineage). "
                f"OR: enable allow_best_effort and supply fact_source_map to draft "
                f"views for the {resolvable} resolvable measure(s) — tables/measures "
                f"may be missing. See thin-report-and-source-resolution.md."
            )
        else:
            action = (
                "This looks like a THIN REPORT (or a report-logic-only model): no "
                "source tables AND no measures reduce to a table aggregate (mostly "
                "selector / measure-on-measure DAX). Re-run against the upstream "
                "semantic model's dataset_id if one exists; otherwise these measures "
                "cannot be converted to metric views. See "
                "thin-report-and-source-resolution.md."
            )
        return {
            "case": "thin_report_no_source_tables",
            "reason": (
                "No source tables (M-Queries) were found in the extracted model, so "
                "there is no physical table to build a UC Metric View on."
            ),
            "recommended_action": action,
            "signals": signals,
        }

    @staticmethod
    def _build_best_effort_views(
        fact_source_map: Any,
        config: Any,
        measures: Any,
    ) -> tuple[dict, dict]:
        """Best-effort UCMVs for a THIN-REPORT model (no M-Query source tables).

        Opt-in fallback used ONLY when the normal path produced 0 views AND the
        human supplied `fact_source_map` (PBI table -> physical
        catalog.schema.table). See docs/powerbi/thin-report-and-source-
        resolution.md — the PREFERRED fix is to point Kasal at the upstream
        semantic model; this is for when that model is unreachable.

        Emits one thin metric view per supplied source, containing only measures
        that ALREADY resolved to real aggregatable SQL in
        `config['measure_resolutions']` (reusing the pipeline's own base_expr +
        base_filters). It NEVER fabricates SQL:
        - measures with a `TODO`/empty resolution are skipped (documented, not emitted);
        - tables not in `fact_source_map` are skipped;
        so the "never emit silently-wrong SQL" contract holds.

        Every emitted measure is flagged `TODO: verify` because it is a DRAFT
        produced without a validated transpiled source. Returns (views,
        coverage_report) where coverage_report makes the GAP explicit —
        tables/measures that were skipped.
        """
        views: dict = {}
        report = {
            "mode": "best_effort",
            "warning": (
                "DRAFT views built from a thin report without validated source "
                "tables. Tables/measures may be MISSING; every emitted measure "
                'is marked "TODO: verify". Prefer converting the upstream '
                "semantic model — see thin-report-and-source-resolution.md."
            ),
            "sources_supplied": 0,
            "tables_emitted": 0,
            "measures_emitted": 0,
            "measures_skipped_unresolved": 0,
            "tables_without_source": [],
            "skipped_measures": [],
        }
        if not isinstance(fact_source_map, dict) or not fact_source_map:
            return views, report
        resolutions = (
            (config or {}).get("measure_resolutions", {})
            if isinstance(config, dict)
            else {}
        )
        report["sources_supplied"] = len(fact_source_map)

        _AGG = (
            "SUM",
            "COUNT",
            "AVG",
            "MIN",
            "MAX",
            "DIVIDE",
            "CALCULATE",
            "SUMX",
            "COUNTX",
        )

        def _to_snake(name: str) -> str:
            import re as _re

            s = _re.sub(r"[^0-9a-zA-Z]+", "_", str(name)).strip("_").lower()
            return s or "measure"

        def _alloc(m: dict) -> str:
            return (
                m.get("proposed_allocation")
                or m.get("table_name")
                or m.get("table")
                or "__unassigned__"
            )

        # measure name -> its allocated PBI table (from the measures list)
        measure_table = {}
        if isinstance(measures, list):
            for m in measures:
                if isinstance(m, dict):
                    nm = m.get("measure_name") or m.get("original_name") or ""
                    if nm:
                        measure_table[nm] = _alloc(m)

        # group RESOLVED measures by their allocated PBI table
        by_table: dict[str, list] = {}
        for mname, res in (resolutions or {}).items():
            base = (res or {}).get("base_expr", "") if isinstance(res, dict) else ""
            if (
                (not base)
                or base.strip().upper().startswith("TODO")
                or not base.strip().upper().startswith(_AGG)
            ):
                report["measures_skipped_unresolved"] += 1
                report["skipped_measures"].append(mname)
                continue
            by_table.setdefault(measure_table.get(mname, "__unassigned__"), []).append(
                (mname, res)
            )

        for pbi_table, source in fact_source_map.items():
            rows = by_table.get(pbi_table, [])
            if not rows:
                report["tables_without_source"].append(pbi_table)
                continue
            measures_out = []
            for mname, res in rows:
                expr = res["base_expr"]
                filters = res.get("base_filters") or []
                if filters:
                    expr = f"{expr} FILTER (WHERE {' AND '.join(filters)})"
                measures_out.append(
                    {
                        "name": _to_snake(mname),
                        "expr": expr,
                        "comment": (
                            f"BEST-EFFORT DRAFT — no validated source. TODO: verify. "
                            f"From DAX '{mname}'."
                        ),
                    }
                )
            views[pbi_table] = {
                "version": "1.1",
                "source": source,
                "comment": (
                    f"BEST-EFFORT UC Metric View (thin-report model). "
                    f"{len(measures_out)} measures drafted from resolved SQL; "
                    f"NOT validated against a transpiled source. Review before "
                    f"deploy. Some measures/tables from the original model may "
                    f"be MISSING."
                ),
                "measures": measures_out,
            }
            report["tables_emitted"] += 1
            report["measures_emitted"] += len(measures_out)
        return views, report

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
        """Persist the full raw DAX extract to conversion_history (fail-open).

        Durable, queryable counterpart to the transient tool result: the
        untruncated source DAX lands in ``input_data.dax_raw`` and is retrievable
        afterwards via ``GET /conversion-history`` (filter by
        ``source_format=powerbi_dax`` / ``execution_id``) or
        ``GET /conversion-history/{id}``. Any failure here is non-fatal — it must
        never break the generation itself.

        ``untranslatable_items`` and the transpiler ``capability_fingerprint`` are
        also persisted so re-evaluation can later answer "which measures failed,
        and at what capability level?" and decide whether a retry can gain
        anything — without re-hitting the PowerBI API.

        ``pbi_ucmv_mapping`` (per-view PBI<->UCMV reconciliation mapping draft) is
        persisted in ``output_data`` alongside ``yaml``/``sql`` so the
        "Download Mapping" artifact is retrievable by ``execution_id`` after the
        run, not only from the transient tool result.
        """

        def _capability_fp() -> str:
            """Current transpiler capability fingerprint (fail-open)."""
            try:
                from src.services.tools.metric_view_utils.capability_version import (
                    capability_fingerprint,
                )

                return capability_fingerprint()
            except Exception:
                return "unknown"

        try:
            from src.schemas.conversion import ConversionHistoryCreate
            from src.services.tools.tool_session_provider import ToolSessionProvider
            from src.utils.user_context import UserContext

            raw_dax_count = len(raw_dax)
            # In JSON/flow mode the tool does not re-extract, so raw_dax is empty
            # even though views ARE generated (measures arrive via config_json /
            # task context). Count the actual views produced so the diagnostic
            # reflects success instead of a misleading "0 measures".
            view_count = len(yaml_output) if isinstance(yaml_output, dict) else 0
            # measure_count reflects extracted DAX when present, else views built.
            measure_count = raw_dax_count or view_count
            try:
                from src.services.execution.kernel.trace_context import (
                    resolve_tool_execution_id,
                )

                _hist_exec_id = resolve_tool_execution_id(self)
            except Exception:
                _hist_exec_id = (getattr(self, "trace_context", None) or {}).get(
                    "job_id"
                )
            history_data = ConversionHistoryCreate(
                execution_id=_hist_exec_id,
                source_format="powerbi_dax",
                target_format="uc_metrics",
                input_data={
                    "workspace_id": workspace_id or "",
                    "dataset_id": dataset_id or "",
                    "dax_raw": raw_dax,
                },
                input_summary=(
                    f"DAX extract: {raw_dax_count} measure(s)"
                    + (f" from workspace {workspace_id}" if workspace_id else "")
                )[:500],
                output_data={
                    "yaml": yaml_output,
                    "sql": sql_output,
                    "catalog": catalog,
                    "schema": schema,
                    # Re-evaluation inputs: WHICH measures failed, and at WHAT
                    # capability level. A later sweep re-tries only these.
                    "untranslatable_items": untranslatable_items or [],
                    # Per-view PBI<->UCMV reconciliation mapping draft
                    # ({view_name: mapping_candidates_yaml_text}, from
                    # pbi_ucmv_mapping.py). Persisted alongside yaml/sql so the
                    # "Download Mapping" artifact is retrievable by execution_id
                    # (GET /conversion-history), not only from the live tool
                    # result the UI happens to hold in-session.
                    "pbi_ucmv_mapping": pbi_ucmv_mapping or {},
                    # Live-connection flag (which semantic model/table to parse),
                    # persisted so the UI note is retrievable by execution_id.
                    "live_connections": live_connections or {},
                    # Source-layer DDL (rec#3): {table: {ddl, todo_steps, error}}
                    # — persisted so the UI can show the base views a run proposes.
                    "source_layer_ddl": source_layer_ddl or {},
                    # PBI-only ingestion tasks (S8): tables needing a snapshot
                    # loader rather than a SQL source.
                    "pbi_only_ingestion_tasks": pbi_only_ingestion_tasks or [],
                    # Validation-loop status (rec#4): skipped/ran/error + detail.
                    "pbi_validation": pbi_validation or {},
                    # Reason reconciliation did not run when it was requested
                    # (persisted so it is retrievable by execution_id, not just
                    # in the live result).
                    "reconciliation_skipped": reconciliation_skipped,
                },
                output_summary=(
                    f"Generated {view_count} UC metric view(s)"
                    + (
                        f" from {raw_dax_count} extracted measure(s)"
                        if raw_dax_count
                        else " (JSON/flow mode)"
                    )
                )[:500],
                configuration={
                    "workspace_id": workspace_id,
                    "dataset_id": dataset_id,
                    "catalog": catalog,
                    "schema": schema,
                    # Capability level that produced this result. Re-evaluation
                    # compares it against the current fingerprint to decide
                    # whether a retry can possibly gain anything.
                    "capability_fingerprint": _capability_fp(),
                },
                status="success",
                measure_count=measure_count,
            )

            try:
                group_context = UserContext.get_group_context()
                if group_context:
                    getattr(group_context, "primary_group_id", None)
            except Exception as _gc_err:
                logger.debug(
                    f"[UCMVGenerator] Could not resolve group_id for history: {_gc_err}"
                )

            # Through ConverterService, which OWNS conversion history and stamps
            # group_id/created_by_email itself — the repository path required every
            # tool to remember that by hand.
            async with ToolSessionProvider.converter_service(
                group_context=group_context
            ) as converter:
                record = await converter.create_history(history_data)
                await converter.session.commit()
                logger.info(
                    f"[UCMVGenerator] Saved conversion_history record id={record.id} "
                    f"(source_format=powerbi_dax, views={view_count}, extracted_measures={raw_dax_count})"
                )
        except Exception as e:
            logger.warning(
                f"[UCMVGenerator] Failed to save conversion_history (non-fatal): {e}"
            )

    # ------------------------------------------------------------------
    # PBI API input validation (SSRF prevention)
    # ------------------------------------------------------------------

    _UUID_PATTERN = re.compile(r"^[a-fA-F0-9\-]{8,50}$")
    _ALLOWED_PBI_DOMAINS = frozenset(
        {
            "api.powerbi.com",
            "api.powerbigov.us",
            "api.powerbi.cn",
            "api.powerbi.de",
            "api.microsoftcloud.de",
        }
    )

    def _validate_pbi_inputs(
        self, workspace_id: str, dataset_id: str, pbi_api_base_url: str
    ) -> tuple[bool, str]:
        """Validate PBI API inputs to prevent SSRF."""
        if not self._UUID_PATTERN.match(workspace_id):
            return False, f"Invalid workspace_id format: {workspace_id}"
        if not self._UUID_PATTERN.match(dataset_id):
            return False, f"Invalid dataset_id format: {dataset_id}"
        parsed = urllib.parse.urlparse(pbi_api_base_url or "https://api.powerbi.com")
        if parsed.hostname not in self._ALLOWED_PBI_DOMAINS:
            return False, f"Untrusted PBI API domain: {parsed.hostname}"
        return True, ""

    # ------------------------------------------------------------------
    # PBI API extraction helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _import_generate_config():
        """Load the shared Power BI library through its canonical package."""
        from src.services.powerbi import pipeline_config

        return pipeline_config

    @staticmethod
    def _tmdl_tables_to_measures(admin_tables: dict) -> list:
        """Convert generate_config TMDL/admin table dict → UCMV measure shape."""
        measures = []
        for tbl_name, tbl_info in (admin_tables or {}).items():
            for m in tbl_info.get("measures", []):
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

    @staticmethod
    def _tmdl_tables_to_mquery(admin_tables: dict) -> list:
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
        entries = []
        for tbl_name, tbl_info in (admin_tables or {}).items():
            src = (tbl_info.get("mquery_expression") or "").strip()
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

    def _extract_mquery_fallback(
        self,
        workspace_id,
        dataset_id,
        tenant_id,
        client_id,
        client_secret,
        username,
        password,
        admin_client_id: str = "",
        admin_client_secret: str = "",
    ) -> list:
        """Recover MQuery/table-source when the Admin Scanner fails for a Service Account.

        Tier 1: Fabric TMDL with the SA (works if the workspace is Fabric-enabled).
        Tier 2: Fabric TMDL with a Service Principal (if client_secret provided).
        Reuses generate_config so the logic is shared with the config generator.
        """
        try:
            gen = self._import_generate_config()
        except Exception as e:
            logger.warning(
                f"[UCMV] Could not load generate_config for MQuery fallback: {e}"
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
                tmdl_parts = gen.fetch_tmdl_parts(
                    fabric_token, workspace_id, dataset_id
                )
                if tmdl_parts:
                    tables = gen.parse_tmdl_to_admin_tables(
                        tmdl_parts, dataset_id=dataset_id
                    )
                    entries = self._tmdl_tables_to_mquery(tables)
                    if entries:
                        logger.info(
                            f"[UCMV] MQuery TMDL fallback ({label}) recovered {len(entries)} tables"
                        )
                        return entries
            except Exception as e:
                logger.warning(f"[UCMV] MQuery TMDL fallback ({label}) failed: {e}")

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

                sp_admin_token = _run_async(
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
                entries = self._tmdl_tables_to_mquery(tables)
                if entries:
                    logger.info(
                        f"[UCMV] MQuery Admin Scanner (SP admin retry) recovered {len(entries)} tables"
                    )
                    return entries
            except Exception as e:
                logger.warning(
                    f"[UCMV] MQuery Admin Scanner (SP admin retry) failed: {e}"
                )
        return []

    def _extract_measures_fallback(
        self,
        workspace_id,
        dataset_id,
        tenant_id,
        client_id,
        client_secret,
        username,
        password,
    ) -> list:
        """Recover measure DAX when Execute Queries/XMLA fails for a Service Account.

        Tier 1: Fabric TMDL with the SA (works if the workspace is Fabric-enabled).
        Tier 2: Fabric TMDL with a Service Principal (if client_secret provided).
        Both reuse the standalone helpers in generate_config so the logic is
        shared with the Pipeline Config Generator.
        """
        try:
            gen = self._import_generate_config()
        except Exception as e:
            logger.warning(f"[UCMV] Could not load generate_config for fallback: {e}")
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
                tmdl_parts = gen.fetch_tmdl_parts(
                    fabric_token, workspace_id, dataset_id
                )
                if tmdl_parts:
                    tables = gen.parse_tmdl_to_admin_tables(
                        tmdl_parts, dataset_id=dataset_id
                    )
                    measures = self._tmdl_tables_to_measures(tables)
                    if measures:
                        logger.info(
                            f"[UCMV] TMDL fallback ({label}) recovered {len(measures)} measures"
                        )
                        return measures
            except Exception as e:
                logger.warning(f"[UCMV] TMDL fallback ({label}) failed: {e}")
        return []

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
        auth_config = {
            "tenant_id": tenant_id,
            "client_id": client_id,
            "client_secret": client_secret,
            "username": username,
            "password": password,
            "auth_method": auth_method,
            "access_token": access_token,
        }

        result: dict = {}

        # Obtain an OAuth token (unless a pre-obtained one was supplied)
        token = access_token
        if not token:
            from src.services.tools.powerbi_auth_utils import (
                get_powerbi_access_token_from_config,
            )

            token = _run_async(get_powerbi_access_token_from_config(auth_config))

        # 1. Extract measures via Execute Queries API
        try:
            from src.services.converters.formats.powerbi.connector import (
                PowerBIConnector,
            )

            connector = PowerBIConnector(
                semantic_model_id=dataset_id,
                group_id=workspace_id,
                access_token=token,
            )
            connector.connect()
            kpis = connector.extract_measures(include_hidden=True)

            measures = []
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
            result["measures"] = measures
            logger.info(f"[UCMV] Extracted {len(measures)} measures from PBI API")
        except Exception as e:
            logger.warning(f"[UCMV] Measure extraction failed: {e}")

        # 1b. Measure DAX fallback — the Execute Queries / XMLA path above is
        # frequently rejected for Service-Account (ROPC) tokens, yielding zero
        # measures. Recover via (a) Fabric TMDL (which an SA CAN read) and, if a
        # client_secret is available, (b) a Service-Principal retry.
        if not result.get("measures"):
            recovered = self._extract_measures_fallback(
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
                    f"[UCMV] Recovered {len(recovered)} measures via TMDL/SP fallback"
                )

        # 2. Extract MQuery via Admin API scan
        try:
            from src.services.converters.formats.mquery.scanner import (
                PowerBIAdminScanner,
            )

            scanner = PowerBIAdminScanner(access_token=token)

            scan_result, raw_scan = _run_async(
                scanner.scan_workspace(workspace_id, dataset_id=dataset_id)
            )

            if raw_scan:
                result["scan_data"] = raw_scan

            mquery_entries = []
            for model in scan_result:
                for table in model.tables:
                    for expr in table.source_expressions:
                        mquery_entries.append(
                            {
                                "table_name": table.name,
                                "transpiled_sql": expr.embedded_sql
                                or expr.raw_expression
                                or "",
                                "validation_passed": (
                                    "Yes" if expr.embedded_sql else "No"
                                ),
                            }
                        )
            result["mquery"] = mquery_entries
            logger.info(
                f"[UCMV] Extracted {len(mquery_entries)} MQuery tables from PBI Admin API"
            )
        except Exception as e:
            logger.warning(f"[UCMV] MQuery extraction failed: {e}")

        # 2b. MQuery fallback — the Admin Scanner (used above) rejects
        # Service-Account tokens (401/403), leaving mquery empty. Without MQuery
        # no fact table is detected and the generator emits 0 views even when
        # measures were extracted. Recover the table source expressions via
        # Fabric TMDL (SA-readable) or a Service-Principal retry, mirroring the
        # measure fallback.
        if not result.get("mquery"):
            recovered_mq = self._extract_mquery_fallback(
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
                    f"[UCMV] Recovered {len(recovered_mq)} MQuery tables via TMDL/SP fallback"
                )

        # 3. Extract relationships via Execute Queries API
        try:
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

            resp = req_lib.post(url, headers=headers, json=payload, timeout=60)
            if resp.status_code == 200:
                raw = resp.json()
                rows = (
                    raw.get("results", [{}])[0].get("tables", [{}])[0].get("rows", [])
                )
                relationships = []
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
                    f"[UCMV] Extracted {len(relationships)} relationships from PBI API"
                )
            else:
                logger.warning(f"[UCMV] Relationships API returned {resp.status_code}")
        except Exception as e:
            logger.warning(f"[UCMV] Relationship extraction failed: {e}")

        return result
