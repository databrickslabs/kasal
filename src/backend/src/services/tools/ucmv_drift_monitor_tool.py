"""UCMV Drift Monitor Tool — keep deployed UC metric views in sync with Power BI.

WHY: a metric view generated from a Power BI model and verified by a human starts
drifting the moment the PBI model changes — a measure's DAX is edited, a new KPI is
added, an old one removed. Re-running the full generator would re-translate (and
possibly change) every verified measure. This tool instead:

  1. extracts today's model with the Pipeline Config Generator (same PBI APIs and
     credentials as the conversion pipeline — visuals are not compared),
  2. reads each monitored metric view's DEPLOYED YAML from Unity Catalog (the
     verified baseline),
  3. classifies every measure (unchanged / changed / new / removed / unassigned),
  4. translates ONLY new and changed measures (UC Metric View Generator on that
     subset), and
  5. patches them into the baseline YAML — appending, never rewriting — with a
     structural check that nothing else in the verified view moved.

PROPOSE ONLY. Nothing is deployed; the output's ``yaml`` key feeds the Metric View
Deployer (tool 88) after review.

Logic lives in ``metric_view_utils/drift/``; this class only wires real I/O into it.
"""

import json
import logging
from typing import Any, ClassVar, Optional, Tuple, Type

from crewai.tools import BaseTool
from pydantic import BaseModel, Field, PrivateAttr

logger = logging.getLogger(__name__)

# Forwarded verbatim to the Pipeline Config Generator.
_PBI_KEYS = (
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
)
_OWN_KEYS = (
    "ucmv_names",
    "warehouse_id",
    "databricks_host",
    "catalog",
    "schema_name",
    "llm_compare_legacy",
    "apply_suspected_changes",
    "llm_model",
    "domain_context",
    "naming_config",
    "max_semantic_measures",
)
_DEFAULT_LLM = "databricks-claude-sonnet-4-5"


class UCMVDriftMonitorSchema(BaseModel):
    """Input schema for UCMVDriftMonitorTool. Normally all values come from the task form."""

    ucmv_names: Optional[str] = Field(
        None,
        description=(
            "Deployed metric views to monitor: catalog.schema.view names (or Catalog "
            "Explorer URLs), as a JSON list or one per line / comma-separated."
        ),
    )
    warehouse_id: Optional[str] = Field(
        None, description="SQL warehouse used to read the deployed views."
    )
    databricks_host: Optional[str] = Field(
        None, description="Workspace URL override (optional)."
    )
    workspace_id: Optional[str] = Field(None, description="Power BI workspace GUID.")
    dataset_id: Optional[str] = Field(
        None, description="Power BI dataset / semantic model GUID."
    )
    report_id: Optional[str] = Field(
        None, description="Power BI report GUID (optional)."
    )
    llm_compare_legacy: Optional[bool] = Field(
        True,
        description=(
            "For views deployed before DAX fingerprinting, ask an LLM whether today's DAX "
            "still matches the deployed SQL (costs tokens; batched)."
        ),
    )
    apply_suspected_changes: Optional[bool] = Field(
        False,
        description="Also patch measures the LLM only SUSPECTS changed (default: show, don't apply).",
    )


class UCMVDriftMonitorTool(BaseTool):
    """Detect drift between deployed UC metric views and their Power BI model."""

    name: str = "UCMV Drift Monitor"
    description: str = (
        "Compares deployed Unity Catalog metric views against the CURRENT Power BI "
        "semantic model they were generated from. Reads each view's deployed YAML as "
        "the verified baseline, extracts today's measures from Power BI, and reports "
        "per measure: unchanged, changed in PBI, new in PBI, removed from PBI, or "
        "unassigned. New and changed measures are translated (DAX→SQL, LLM fallback) "
        "and appended to the baseline YAML without touching anything else; a "
        "structural check rejects any patch that alters the verified view. Read-only: "
        "produces a proposal for the Metric View Deployer, never deploys."
    )
    args_schema: Type[BaseModel] = UCMVDriftMonitorSchema
    _default_config: dict = PrivateAttr(default_factory=dict)
    #: Parameters a skill attached to the agent may fill (Configuration → Skills,
    #: tagged ``kasal-tool-param``) — see execution/kernel/skill_tool_context.py.
    skill_context_params: ClassVar[Tuple[str, ...]] = ("domain_context",)

    model_config = {"arbitrary_types_allowed": True, "extra": "allow"}

    def __init__(self, **kwargs: Any) -> None:
        default_config: dict = {}
        for key in _PBI_KEYS + _OWN_KEYS:
            val = kwargs.pop(key, None)
            if val is not None:
                default_config[key] = val
        super().__init__(**kwargs)
        self._default_config = default_config

    # ------------------------------------------------------------------
    # I/O wiring
    # ------------------------------------------------------------------
    def _extract(self, get, catalog: str, schema: str) -> dict:
        from src.services.tools.pipeline_config_generator_tool import (
            PipelineConfigGeneratorTool,
        )

        cfg = {k: get(k) for k in _PBI_KEYS if get(k) not in (None, "")}
        # No warehouse_id: extraction stays the deterministic, LLM-free P1 path.
        tool = PipelineConfigGeneratorTool(catalog=catalog, schema_name=schema, **cfg)
        return json.loads(tool._run())

    def _sql_fn(self, get, warehouse_id: str):
        from src.services.tools.metric_view_deployer_tool import MetricViewDeployerTool
        from src.utils.telemetry import KasalProduct, get_user_agent_header
        from src.utils.url_security import is_trusted_databricks_host

        deployer = MetricViewDeployerTool()
        auth = deployer._authenticate(host_override=get("databricks_host") or None)
        if not auth:
            raise RuntimeError("Databricks authentication failed")
        url = (auth.workspace_url or "").rstrip("/")
        if not url or not is_trusted_databricks_host(url):
            raise RuntimeError(
                "workspace URL is not configured or not a trusted Databricks host"
            )
        headers = auth.get_headers()
        headers.update(get_user_agent_header(KasalProduct.POWERBI))

        def _run(statement: str) -> dict:
            return MetricViewDeployerTool._execute_sql_sync(
                statement, url, warehouse_id, headers
            )

        return _run

    def _generate_fn(self, get, extraction: dict, catalog: str, schema: str):
        from src.services.tools.uc_metric_view_generator_tool import (
            UCMetricViewGeneratorTool,
        )

        def _gen(measures: list[dict]) -> dict:
            cfg = {
                "measures_json": json.dumps(measures),
                "mquery_json": json.dumps(extraction.get("mquery_json") or []),
                "relationships_json": json.dumps(
                    extraction.get("relationships_json") or []
                ),
                "config_json": json.dumps(extraction.get("proposed_config") or {}),
                "visual_usage_index": json.dumps(
                    extraction.get("visual_usage_index") or {}
                ),
                "use_llm_fallback": True,
                "llm_model": get("llm_model") or _DEFAULT_LLM,
                "catalog": catalog,
                "schema_name": schema,
            }
            for k in ("domain_context", "naming_config"):
                if get(k):
                    cfg[k] = get(k)
            # No warehouse_id → the generator stays single-pass (no reconciliation).
            out = UCMetricViewGeneratorTool(**cfg)._run()
            return json.loads(out) if isinstance(out, str) else (out or {})

        return _gen

    def _complete_fn(self, model: str):
        from src.services.tools.async_bridge import run_async_with_context
        from src.services.tools.metric_view_utils.dax_llm_fallback import _call_llm

        def _complete(system_prompt: str, user_prompt: str) -> Optional[str]:
            res = run_async_with_context(
                _call_llm(user_prompt, system_prompt, model, max_tokens=4000),
                timeout=300,
            )
            return (res or {}).get("content")

        return _complete

    # ------------------------------------------------------------------
    # Run
    # ------------------------------------------------------------------
    def _run(self, **kwargs: Any) -> str:
        from src.services.tools.metric_view_utils.drift import run_drift_check
        from src.services.tools.metric_view_utils.drift.baseline import (
            parse_view_names,
            split_full_name,
        )

        def get(key: str) -> Any:
            # Agents often pass their own empty placeholders; never let "" shadow
            # the value configured in the task form.
            val = kwargs.get(key)
            if val is None or val == "" or val == [] or val == {}:
                return self._default_config.get(key)
            return val

        def _fail(msg: str) -> str:
            logger.error(f"[UCMVDrift] {msg}")
            return json.dumps(
                {
                    "drift_monitor": True,
                    "error": msg,
                    "views": [],
                    "summary": {"views_checked": 0},
                }
            )

        views = parse_view_names(get("ucmv_names"))
        if not views:
            return _fail(
                "ucmv_names is required: one or more catalog.schema.view metric views to monitor."
            )
        bad = [v for v in views if not split_full_name(v)]
        if bad:
            return _fail(f"not catalog.schema.view names: {bad}")
        warehouse_id = get("warehouse_id")
        if not warehouse_id:
            return _fail("warehouse_id is required to read the deployed metric views.")

        first = split_full_name(views[0]) or ("main", "default", "")
        catalog = get("catalog") or first[0]
        schema = get("schema_name") or first[1]

        try:
            extraction = self._extract(get, catalog, schema)
        except Exception as exc:  # noqa: BLE001
            return _fail(f"Power BI extraction failed: {exc}")
        if extraction.get("error"):
            return _fail(f"Power BI extraction failed: {extraction['error']}")

        try:
            sql_fn = self._sql_fn(get, warehouse_id)
        except Exception as exc:  # noqa: BLE001
            return _fail(f"cannot connect to Databricks: {exc}")

        model = get("llm_model") or _DEFAULT_LLM
        compare_legacy = get("llm_compare_legacy")
        complete_fn = (
            self._complete_fn(model)
            if compare_legacy in (None, True, "true", "True")
            else None
        )
        apply_suspected = get("apply_suspected_changes") in (True, "true", "True")

        report = run_drift_check(
            views,
            extraction,
            sql_fn=sql_fn,
            generate_fn=self._generate_fn(get, extraction, catalog, schema),
            complete_fn=complete_fn,
            apply_suspected_changes=apply_suspected,
            max_semantic_measures=int(get("max_semantic_measures") or 300),
        )
        summary = extraction.get("summary") or {}
        report["pbi"] = {
            "workspace_id": get("workspace_id"),
            "dataset_id": get("dataset_id"),
            "report_id": get("report_id"),
            "measures_extracted": summary.get("measures_extracted"),
            "dax_degraded": summary.get("dax_degraded"),
        }
        if summary.get("dax_degraded"):
            report["warnings"].insert(
                0,
                "Power BI returned bare DAX for most measures — fingerprints of today's "
                "DAX are unreliable, so 'changed' results may be false positives.",
            )
        report["warnings"].extend(extraction.get("warnings") or [])
        return json.dumps(report, indent=2, default=str)
