"""UCMV Workbench (tool 101): finish generated metric views to 100 % reconciled, step by step.

The UC Metric View Generator gets most measures right in one pass; the rest needs an agent
that looks at each open measure, writes its SQL, deploys to a sandbox, reconciles against
Power BI and repeats, until every measure is at 100 % or its gap is classified. This tool is
that agent's workbench. It holds the generator output as a working copy for the run and offers
small, safe actions; deploy and reconciliation reuse the Metric View Deployer and the UCMV
Reconciliation tool. Credentials, the sandbox catalog/schema and the warehouse come from the
task form; the agent only passes view / measure / expression arguments.

Actions: ``overview``, ``gaps``, ``show``, ``set_measure``, ``remove_measure``, ``deploy``,
``check``, ``reconcile``, ``classify``, ``report``, ``export``. Outputs carry shares, counts,
names and definitions (statistics-only reconciliation by default), never data values.
"""

from __future__ import annotations

import json
import logging
from typing import Optional, Type

import yaml
from pydantic import BaseModel, Field, PrivateAttr

from src.services.tools.base import BaseTool
from src.services.tools.ucmv_workbench_utils import edits
from src.services.tools.ucmv_workbench_utils.state import CLASSIFICATIONS, Workbench

logger = logging.getLogger(__name__)

_RECON_KEYS = ("pct_aligned", "classification", "cells_total", "periods_failing", "dims_failing")


def _map(value: object) -> dict[str, object]:
    return value if isinstance(value, dict) else {}

_ACTIONS = (
    "overview", "gaps", "show", "set_measure", "remove_measure", "deploy", "check",
    "reconcile", "classify", "report", "export",
)
_CONFIG_KEYS = (
    "ucmv_output", "catalog", "schema_name", "warehouse_id", "databricks_host",
    "workspace_id", "dataset_id", "tenant_id", "client_id", "client_secret",
    "reference_years", "statistics_only", "confidential_values",
)
_PBI_KEYS = {
    "workspace_id": "pbi_workspace_id",
    "dataset_id": "pbi_dataset_id",
    "tenant_id": "pbi_tenant_id",
    "client_id": "pbi_client_id",
    "client_secret": "pbi_client_secret",
}


class UCMVWorkbenchSchema(BaseModel):
    """What the agent passes. Everything else comes from the task form."""

    action: str = Field(..., description=" | ".join(_ACTIONS))
    view: Optional[str] = Field(None, description="View key (from overview), for all actions but overview/report/export")
    measure: Optional[str] = Field(None, description="Metric view measure name (identifier)")
    expr: Optional[str] = Field(None, description="set_measure: the measure's SQL expression over `source` / joins")
    pbi_measure: Optional[str] = Field(None, description="set_measure: the Power BI measure it reproduces (for reconciliation)")
    display_name: Optional[str] = Field(None, description="set_measure: display name")
    format_json: Optional[str] = Field(None, description='set_measure: format as JSON, e.g. {"type": "number"}')
    comment: Optional[str] = Field(None, description="set_measure: comment")
    classification: Optional[str] = Field(None, description="classify: " + " | ".join(CLASSIFICATIONS))
    note: Optional[str] = Field(None, description="classify: what was checked and why it is not a translation error")


class UCMVWorkbenchTool(BaseTool):
    name: str = "UCMV Workbench"
    description: str = (
        "Finish generated UC metric views to 100 % reconciled with Power BI. Holds the UC Metric View "
        "Generator output as a working copy. Start with action=overview, then per view: gaps (untranslated "
        "measures with DAX, reason and proposal; measures below 100 %), show, set_measure (one measure: "
        "name + SQL expr + pbi_measure), deploy (to the sandbox), check (every measure runs), reconcile "
        "(per-measure alignment with Power BI), classify (data_drift | pbi_defect | structural | "
        "out_of_scope, with a note) for a gap that is not a translation error. report shows what is still "
        "open; export returns the final YAML for the Metric View Deployer."
    )
    args_schema: Type[BaseModel] = UCMVWorkbenchSchema
    _default_config: dict = PrivateAttr(default_factory=dict)
    _bench: Optional[Workbench] = PrivateAttr(default=None)
    _bench_job: str = PrivateAttr(default="")

    model_config = {"arbitrary_types_allowed": True, "extra": "allow"}

    def __init__(self, **kwargs: object) -> None:
        default_config: dict = {"ucmv_output": None}
        for key in _CONFIG_KEYS:
            val = kwargs.pop(key, None)
            if val is not None:
                default_config[key] = val
        super().__init__(**kwargs)
        self._default_config = default_config

    # ── state ──────────────────────────────────────────────────────────────

    def _cfg(self, key: str) -> object:
        return self._default_config.get(key)

    def _job_id(self) -> str:
        context = getattr(self, "trace_context", None)
        return str(context.get("job_id") or "") if isinstance(context, dict) else ""

    def _generator_output(self) -> dict[str, object]:
        raw = self._cfg("ucmv_output")
        try:
            data = json.loads(raw) if isinstance(raw, str) else raw
        except ValueError:
            data = None
        if isinstance(data, dict) and data.get("yaml"):
            return data
        from src.services.tools.metric_view_validator_tool import MetricViewValidatorTool

        job_id = self._job_id()
        latest = MetricViewValidatorTool._fetch_latest_ucmv_from_db(job_id, attempts=1) if job_id else {}
        if not (isinstance(latest, dict) and latest.get("yaml")):
            latest = MetricViewValidatorTool._fetch_latest_ucmv_from_db()
        return latest if isinstance(latest, dict) else {}

    def _workbench(self) -> Workbench:
        job = self._job_id()
        if self._bench is None or self._bench_job != job:
            self._bench = Workbench.from_generator_output(self._generator_output())
            self._bench_job = job
        return self._bench

    # ── deploy / reconcile through the existing tools ──────────────────────

    def _target(self) -> tuple[str, str]:
        return str(self._cfg("catalog") or "main"), str(self._cfg("schema_name") or "default")

    def _deploy(self, bench: Workbench, view: str, mode: Optional[str] = None) -> dict[str, object]:
        from src.services.tools.metric_view_deployer_tool import MetricViewDeployerTool

        v = bench.view(view)
        catalog, schema = self._target()
        raw = MetricViewDeployerTool()._run(
            yaml_specs_json=json.dumps({v.key: v.yaml}),
            catalog=catalog,
            schema_name=schema,
            warehouse_id=self._cfg("warehouse_id"),
            databricks_host=self._cfg("databricks_host"),
            dry_run=False,
            mode=mode,
        )
        out = json.loads(raw) if isinstance(raw, str) else {}
        return out if isinstance(out, dict) else {}

    def _fqn(self, view_key: str) -> str:
        from src.services.tools.metric_view_deployer_tool import _view_parts

        return ".".join(_view_parts(view_key, *self._target()))

    def _reconcile(self, bench: Workbench, view: str) -> dict[str, object]:
        from src.services.tools.ucmv_reconciliation_tool import UCMVReconciliationTool

        v = bench.view(view)
        if not v.mapping.get("measures"):
            raise edits.EditError("this view has no reconciliation mapping; set pbi_measure on set_measure first")
        fqn = self._fqn(v.key)
        mapping = edits.rebind_mapping(v.mapping, fqn)
        ctor = {dst: self._cfg(src) for src, dst in _PBI_KEYS.items() if self._cfg(src)}
        statistics_only = self._cfg("statistics_only")
        ctor["statistics_only"] = True if statistics_only is None else bool(statistics_only)
        reconciler = UCMVReconciliationTool(**ctor)
        policy = getattr(self, "_confidential_policy", None)
        if policy:
            setattr(reconciler, "_confidential_policy", policy)
        years = self._cfg("reference_years")
        if isinstance(years, str):
            years = [y.strip() for y in years.strip("[]").split(",") if y.strip()]
        raw = reconciler._run(
            mappings_json=json.dumps({fqn: mapping}),
            warehouse_id=self._cfg("warehouse_id"),
            databricks_host=self._cfg("databricks_host"),
            reference_years=years or None,
        )
        out = json.loads(raw) if isinstance(raw, str) else {}
        return out if isinstance(out, dict) else {}

    # ── actions ────────────────────────────────────────────────────────────

    def _act(self, bench: Workbench, action: str, a: dict[str, object]) -> dict[str, object]:
        view, measure = str(a.get("view") or ""), str(a.get("measure") or "")
        if action == "overview":
            return {"views": bench.overview(), "target": ".".join(self._target())}
        if action == "gaps":
            return bench.gaps(view)
        if action == "show":
            return bench.show(view, measure or None)
        if action == "set_measure":
            how = bench.set_measure(
                view, measure, str(a.get("expr") or ""), str(a.get("pbi_measure") or ""),
                display_name=str(a.get("display_name") or ""),
                fmt=edits.parse_format(str(a.get("format_json") or "") or None),
                comment=str(a.get("comment") or ""),
            )
            return {"ok": True, "measure": measure, "change": how, "next": "deploy, then reconcile this view"}
        if action == "remove_measure":
            bench.remove_measure(view, measure)
            return {"ok": True, "removed": measure}
        if action == "classify":
            bench.classify(view, measure, str(a.get("classification") or ""), str(a.get("note") or ""))
            return {"ok": True, "classified": measure}
        if action in ("deploy", "check"):
            return self._deploy_action(bench, view, action)
        if action == "reconcile":
            return self._reconcile_action(bench, view)
        if action == "report":
            return bench.report()
        if action == "export":
            return {
                "yaml": {k: v.yaml for k, v in bench.views.items()},
                "pbi_ucmv_mapping": {
                    k: yaml.safe_dump(edits.rebind_mapping(v.mapping, self._fqn(k)), sort_keys=False)
                    for k, v in bench.views.items() if v.mapping
                },
                "finishing_report": bench.report(),
            }
        raise edits.EditError(f"unknown action {action!r}; one of {', '.join(_ACTIONS)}")

    def _deploy_action(self, bench: Workbench, view: str, action: str) -> dict[str, object]:
        out = self._deploy(bench, view, mode="check" if action == "check" else None)
        v = bench.view(view)
        if action == "check":
            return {"view": v.key, "check": out.get("summary"), "results": out.get("check_results") or out.get("deployment_results")}
        result = _map(_map(out.get("deployment_results")).get(v.key))
        status = str(result.get("status") or out.get("error") or "unknown")
        message = str(result.get("message") or "")
        v.deployed = "ok" if status.startswith("deployed") else (message or status)[:400]
        return {"view": v.key, "status": status, "deployed_as": self._fqn(v.key), "message": message}

    def _reconcile_action(self, bench: Workbench, view: str) -> dict[str, object]:
        v = bench.view(view)
        if v.deployed != "ok":
            raise edits.EditError("deploy this view first (action=deploy); reconciliation reads the deployed view")
        out = self._reconcile(bench, view)
        if out.get("error"):
            return {"view": v.key, "error": out["error"]}
        views = _map(out.get("views"))
        report = _map(next(iter(views.values()), None))
        compact: dict[str, dict[str, object]] = {}
        for m, e in _map(report.get("measures")).items():
            entry = _map(e)
            compact[str(m)] = {k: entry[k] for k in _RECON_KEYS if k in entry}
        bench.record_reconciliation(v.key, compact)
        return {"view": v.key, "summary": report.get("summary"), "measures": compact}

    def _run(self, **kwargs: object) -> str:
        action = str(kwargs.get("action") or "overview")
        try:
            bench = self._workbench()
            if not bench.views:
                return json.dumps({"error": "no generated metric views found: run the UC Metric View Generator first"})
            return json.dumps(self._act(bench, action, kwargs), indent=2, default=str)
        except edits.EditError as e:
            return json.dumps({"error": str(e), "action": action})
        except Exception as e:  # noqa: BLE001 — a tool returns its error to the agent instead of raising
            logger.warning("[UCMVWorkbench] %s failed", action, exc_info=True)
            return json.dumps({"error": f"{action} failed: {e}"})
