"""UCMV Reconciliation Tool for CrewAI.

Given a reconciliation MAPPING (which PBI measure/column each UCMV measure maps
to), a deployed UC Metric View, and the live Power BI model, VERIFY every UCMV
measure returns the numbers PBI shows: run the UCMV side on a Databricks SQL
warehouse, run the PBI side via the Execute Queries (DAX) API, join cell-by-cell
within tolerance, and classify mismatches.

The tool does I/O + orchestration only. Every query is built and every
comparison is made by the deterministic core in
``metric_view_utils/reconciliation/`` — the LLM is never in the comparison path.

Reuse (no reinvention):

- PBI auth: ``powerbi_auth_utils.get_powerbi_access_token`` (same as
  ``powerbi_dax_executor_tool.py``).
- PBI Execute Queries: ``PowerBIDaxExecutorTool._execute_dax_query`` (reused
  verbatim through a wrapper executor).
- Databricks SQL warehouse: ``MetricViewDeployerTool._execute_sql_sync`` (a
  staticmethod, reused directly — no change to the deployer).
- Databricks auth / User-Agent telemetry / SSRF: ``databricks_auth`` +
  ``telemetry.get_user_agent_header(KasalProduct.POWERBI)`` + ``url_security``,
  the same pattern the deployer uses.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import contextvars
import json
import logging
from typing import Any, Optional, Type

import pandas as pd
from pydantic import BaseModel, Field, PrivateAttr

from src.services.tools.base import BaseTool
from src.services.tools.metric_view_utils import reconciliation as recon

logger = logging.getLogger(__name__)

# Cap the number of mismatching cells reported per measure — enough to see the
# pattern, not the whole failing set.
_MAX_SAMPLE_MISMATCHES = 10

# Constructor-injected config keys (kept out of the LLM-facing schema). Defined
# at module scope, not as a class attribute: a leading-underscore class
# attribute is treated by pydantic as a private attr and cannot be read in
# __init__ before super().__init__() has run.
_CONFIG_KEYS = (
    "mapping",
    "mappings_json",
    "warehouse_id",
    "databricks_host",
    "reference_years",
    "dimension_values",
    "periods",
    "dry_run",
    # PBI connection + auth
    "pbi_workspace_id",
    "pbi_dataset_id",
    "pbi_access_token",
    "pbi_tenant_id",
    "pbi_client_id",
    "pbi_client_secret",
    "pbi_username",
    "pbi_password",
    "pbi_auth_method",
    # Optional refresh boundaries for classification
    "pbi_refresh_period",
    "mv_refresh_period",
)


class UCMVReconciliationSchema(BaseModel):
    """Input schema for UCMVReconciliationTool.

    Connection/auth/ids are injected at construction time (see ``__init__``) —
    exposing credentials as LLM-fillable parameters bloats every call and invites
    the model to echo them, so only the "what to reconcile" knobs live here.
    """

    mapping: Optional[str] = Field(
        None,
        description="A single reconciliation mapping as YAML or JSON text.",
    )
    mappings_json: Optional[str] = Field(
        None,
        description="JSON object of {view_name: mapping} for reconciling several views at once. "
        "Each mapping value may be a mapping object or a YAML string.",
    )
    warehouse_id: Optional[str] = Field(
        None, description="Databricks SQL warehouse ID."
    )
    databricks_host: Optional[str] = Field(
        None, description="Override workspace URL (optional)."
    )
    reference_years: Optional[list] = Field(
        None, description="Fiscal years to reconcile, e.g. [2025, 2026]."
    )
    dimension_values: Optional[list] = Field(
        None,
        description="Explicit dimension values to reconcile (e.g. countries). If omitted, they are "
        "discovered from the PBI model.",
    )
    periods: Optional[list] = Field(
        None,
        description="Snapshot load dates (snapshot grain) or explicit context periods. Optional; "
        "monthly context periods are derived from the UCMV result when needed.",
    )
    dry_run: bool = Field(
        False,
        description="If True, build and return the UCMV SQL and PBI DAX without executing anything.",
    )


# ── Executors: thin adapters over the reused I/O helpers ─────────────────────


def _run_coro_sync(coro):
    """Run a coroutine to completion from a sync context, propagating contextvars
    (execution_id, group_id, OBO token) into any worker thread."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()
            asyncio.set_event_loop(None)
    ctx = contextvars.copy_context()
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
        return ex.submit(ctx.run, asyncio.run, coro).result()


class _PBIExecutor:
    """``execute(semantic_model_id, dax) -> (DataFrame, err)`` over the reused
    ``PowerBIDaxExecutorTool._execute_dax_query``."""

    def __init__(self, workspace_id: str, access_token: str):
        from src.services.tools.powerbi_dax_executor_tool import PowerBIDaxExecutorTool

        self.workspace_id = workspace_id
        self.access_token = access_token
        self._tool = PowerBIDaxExecutorTool()

    def execute(self, semantic_model_id: str, dax_query: str):
        result = _run_coro_sync(
            self._tool._execute_dax_query(
                self.workspace_id, semantic_model_id, self.access_token, dax_query
            )
        )
        if not result.get("success"):
            return pd.DataFrame(), result.get("error")
        return pd.DataFrame(result.get("data") or []), None


def _sql_result_to_df(payload: dict) -> pd.DataFrame:
    """Turn a Databricks SQL Statement API SUCCEEDED payload into a DataFrame,
    using the manifest column names for the (inline) data_array rows."""
    manifest = (payload or {}).get("manifest", {}) or {}
    columns = [
        c.get("name") for c in manifest.get("schema", {}).get("columns", []) or []
    ]
    rows = (payload or {}).get("result", {}).get("data_array") or []
    if not columns:
        return pd.DataFrame(rows)
    return pd.DataFrame(rows, columns=columns)


class _SQLExecutor:
    """``execute(sql) -> (DataFrame, err)`` over the reused
    ``MetricViewDeployerTool._execute_sql_sync``."""

    def __init__(self, workspace_url: str, warehouse_id: str, headers: dict):
        from src.services.tools.metric_view_deployer_tool import MetricViewDeployerTool

        self.workspace_url = workspace_url
        self.warehouse_id = warehouse_id
        self.headers = headers
        self._exec = MetricViewDeployerTool._execute_sql_sync

    def execute(self, sql: str):
        result = self._exec(sql, self.workspace_url, self.warehouse_id, self.headers)
        if not result.get("success"):
            return pd.DataFrame(), result.get("error")
        return _sql_result_to_df(result.get("data", {})), None


class UCMVReconciliationTool(BaseTool):
    """Reconcile deployed UC Metric View measures against the live Power BI model."""

    name: str = "UCMV Reconciliation"
    description: str = (
        "Verify that a deployed UC Metric View returns the same numbers as its source Power BI "
        "report. Runs the UCMV side on a Databricks SQL warehouse and the PBI side via the Execute "
        "Queries (DAX) API, then compares every measure cell-by-cell within tolerance and classifies "
        "mismatches. Input: a reconciliation mapping (or a dict of them). Output: per-measure "
        "alignment percentages, cell counts, classification, and sample mismatches, plus an overall "
        "summary. Deterministic — no LLM in the comparison path."
    )
    args_schema: Type[BaseModel] = UCMVReconciliationSchema
    _default_config: dict = PrivateAttr(default_factory=dict)

    model_config = {"arbitrary_types_allowed": True, "extra": "allow"}

    def __init__(self, **kwargs: Any) -> None:
        default_config = {k: kwargs.pop(k, None) for k in _CONFIG_KEYS}
        super().__init__(**kwargs)
        self._default_config = {
            k: v for k, v in default_config.items() if v is not None
        }

    # ── config + mapping parsing ────────────────────────────────────────────

    def _cfg(self, kwargs: dict, key: str):
        val = kwargs.get(key)
        return val if val is not None else self._default_config.get(key)

    @staticmethod
    def _parse_mappings(
        mapping_text: Optional[str], mappings_json: Optional[str]
    ) -> dict:
        """Return {view_name: UCMVMapping}. Accepts a single mapping (keyed by its
        UCMV table) or a JSON dict of many."""
        out: dict = {}
        if mappings_json:
            parsed = (
                json.loads(mappings_json)
                if isinstance(mappings_json, str)
                else mappings_json
            )
            if not isinstance(parsed, dict):
                raise ValueError(
                    "mappings_json must be a JSON object of {view_name: mapping}."
                )
            for name, m in parsed.items():
                out[name] = (
                    recon.load_mapping(m)
                    if isinstance(m, str)
                    else recon.load_mapping_dict(m)
                )
        if mapping_text:
            mp = recon.load_mapping(mapping_text)
            out[mp.binding.ucmv_table] = mp
        return out

    # ── auth ────────────────────────────────────────────────────────────────

    @staticmethod
    def _authenticate_databricks(host_override: Optional[str] = None):
        """Resolve a Databricks AuthContext (OBO -> PAT -> SPN), off the event loop."""
        from src.utils.databricks_auth import get_auth_context

        def _run_in_thread():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                return loop.run_until_complete(get_auth_context())
            finally:
                loop.close()
                asyncio.set_event_loop(None)

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            auth = executor.submit(_run_in_thread).result(timeout=30)
        if auth is not None and host_override:
            url = host_override.strip().rstrip("/")
            if not url.startswith("https://"):
                url = f"https://{url}"
            auth.workspace_url = url
        return auth

    def _build_sql_executor(
        self, warehouse_id: str, host_override: Optional[str]
    ) -> _SQLExecutor:
        auth = self._authenticate_databricks(host_override=host_override)
        if not auth:
            raise RuntimeError("Databricks authentication failed")
        headers = auth.get_headers()
        from src.utils.telemetry import KasalProduct, get_user_agent_header

        headers.update(get_user_agent_header(KasalProduct.POWERBI))
        workspace_url = (auth.workspace_url or "").rstrip("/")
        if not workspace_url:
            raise RuntimeError("workspace_url not configured")

        # SSRF check — fail closed.
        try:
            from src.utils.url_security import is_trusted_databricks_host

            trusted = is_trusted_databricks_host(workspace_url)
        except Exception:  # noqa: BLE001
            trusted = False
        if not trusted:
            raise RuntimeError(f"Untrusted Databricks host: {workspace_url}")
        return _SQLExecutor(workspace_url, warehouse_id, headers)

    def _build_pbi_executor(
        self, kwargs: dict, mapping: recon.UCMVMapping
    ) -> _PBIExecutor:
        from src.services.tools.powerbi_auth_utils import get_powerbi_access_token

        workspace_id = (
            self._cfg(kwargs, "pbi_workspace_id") or mapping.binding.pbi_workspace_id
        )
        if not workspace_id:
            raise RuntimeError(
                "pbi_workspace_id is required (config or mapping binding)"
            )
        token = _run_coro_sync(
            get_powerbi_access_token(
                tenant_id=self._cfg(kwargs, "pbi_tenant_id"),
                client_id=self._cfg(kwargs, "pbi_client_id"),
                client_secret=self._cfg(kwargs, "pbi_client_secret"),
                access_token=self._cfg(kwargs, "pbi_access_token"),
                username=self._cfg(kwargs, "pbi_username"),
                password=self._cfg(kwargs, "pbi_password"),
                auth_method=self._cfg(kwargs, "pbi_auth_method"),
            )
        )
        return _PBIExecutor(workspace_id, token)

    # ── dimension discovery ──────────────────────────────────────────────────

    @staticmethod
    def _discover_dimension_values(
        pbi_executor: _PBIExecutor,
        model_id: str,
        mapping: recon.UCMVMapping,
        dimension_name: str,
    ) -> list:
        dax = recon.build_dimension_scope_query(mapping, dimension_name)
        df, err = pbi_executor.execute(model_id, dax)
        if df is None or df.empty:
            raise RuntimeError(f"Could not discover dimension values from PBI: {err}")
        return [v for v in df.iloc[:, 0].tolist() if v is not None]

    # ── per-view reconciliation ──────────────────────────────────────────────

    def _reconcile_view(
        self, view_name: str, mapping: recon.UCMVMapping, kwargs: dict, dry_run: bool
    ) -> dict:
        dimension_name = mapping.binding.default_dimension
        # reference_years is OPTIONAL: empty means "all years" (no year filter).
        # The query builder and comparator both treat an empty list that way.
        reference_years = self._cfg(kwargs, "reference_years") or []
        dimension_values = self._cfg(kwargs, "dimension_values")
        periods = self._cfg(kwargs, "periods")
        model_id = (
            self._cfg(kwargs, "pbi_dataset_id") or mapping.binding.pbi_semantic_model_id
        )

        if dry_run:
            ucmv_sql = recon.build_ucmv_query(
                mapping, dimension_name, dimension_values or [], reference_years
            )
            measures = mapping.resolved_measures()
            direct, direct_ctx, switch = recon.partition_measures_by_shape(measures)
            pbi_dax: dict = {}
            # One query PER extra_filter group — mirrors the live runner, so the
            # preview does not misleadingly collapse differently-filtered measures
            # (e.g. actual '0000' vs budget 'B000') into one filterless query.
            direct_qs = recon.build_direct_queries(
                mapping, dimension_name, dimension_values or [], direct
            )
            if direct_qs:
                pbi_dax["direct"] = direct_qs
            if switch:
                pbi_dax["switch"] = [
                    q
                    for q, *_ in recon.build_switch_queries(
                        mapping, dimension_name, dimension_values or [], switch
                    )
                ]
            return {
                "dry_run": True,
                "dimension": dimension_name,
                "measures": [m.ucmv_measure for m in measures],
                "ucmv_sql": ucmv_sql,
                "pbi_dax": pbi_dax,
            }

        warehouse_id = self._cfg(kwargs, "warehouse_id")
        if not warehouse_id:
            return {"error": "warehouse_id is required"}
        host_override = self._cfg(kwargs, "databricks_host")

        pbi_executor = self._build_pbi_executor(kwargs, mapping)
        sql_executor = self._build_sql_executor(warehouse_id, host_override)

        if not dimension_values:
            dimension_values = self._discover_dimension_values(
                pbi_executor, model_id, mapping, dimension_name
            )

        # UCMV side
        ucmv_sql = recon.build_ucmv_query(
            mapping, dimension_name, dimension_values, reference_years
        )
        ucmv_df = recon.run_ucmv_query(sql_executor, ucmv_sql, dimension_name)
        for m in mapping.resolved_measures():
            if m.ucmv_measure in ucmv_df.columns:
                ucmv_df[m.ucmv_measure] = pd.to_numeric(
                    ucmv_df[m.ucmv_measure], errors="coerce"
                )

        # PBI side — context periods cover snapshot dates + monthly extra_group_by
        context_periods = periods or sorted(
            ucmv_df["period"].astype(str).unique().tolist()
        )
        pbi_df = recon.run_pbi_queries(
            pbi_executor,
            model_id,
            mapping,
            dimension_name,
            dimension_values,
            context_periods=context_periods,
        )

        return self._compare_and_summarize(
            view_name, mapping, dimension_name, reference_years, ucmv_df, pbi_df, kwargs
        )

    def _compare_and_summarize(
        self,
        view_name,
        mapping,
        dimension_name,
        reference_years,
        ucmv_df,
        pbi_df,
        kwargs,
    ) -> dict:
        wide = recon.compare(ucmv_df, pbi_df, mapping, dimension_name, reference_years)
        long_df = recon.to_long_format(wide, mapping, dimension_name)
        summary = recon.summarize(long_df)

        pbi_refresh = self._cfg(kwargs, "pbi_refresh_period")
        mv_refresh = self._cfg(kwargs, "mv_refresh_period")

        measures_out: dict = {}
        for _, row in summary.iterrows():
            name = row["ucmv_kbi_name"]
            measure = mapping.measure(name)
            cell_mask = long_df["ucmv_kbi_name"] == name
            cells = long_df[cell_mask]
            failing = cells[~cells["within_tolerance"].astype(bool)]
            failing_periods = sorted(failing["period"].astype(str).unique().tolist())
            stats = recon.MismatchStats(
                pct_aligned=float(row["pct_aligned"]),
                n_periods=int(row["n_periods"]),
                periods_failing=int(row["periods_failing"]),
                n_dims=int(row["n_dims"]),
                dims_failing=int(row["dims_failing"]),
                failing_periods=failing_periods,
                value_format=measure.value_format,
            )
            classification = recon.classify(
                stats, pbi_refresh_period=pbi_refresh, mv_refresh_period=mv_refresh
            )
            samples = [
                {
                    "dimension": s["dimension"],
                    "period": s["period"],
                    "ucmv_value": _num(s["ucmv_value"]),
                    "pbi_value": _num(s["pbi_value"]),
                    "delta": _num(s["delta"]),
                }
                for _, s in failing.head(_MAX_SAMPLE_MISMATCHES).iterrows()
            ]
            measures_out[name] = {
                "pct_aligned": float(row["pct_aligned"]),
                "cells_total": int(row["total"]),
                "cells_aligned": int(row["aligned"]),
                "classification": classification,
                "sample_mismatches": samples,
            }

        total_cells = int(long_df.shape[0])
        aligned_cells = int(long_df["within_tolerance"].astype(bool).sum())
        measures_at_100 = sum(
            1 for v in measures_out.values() if v["pct_aligned"] >= 100
        )
        return {
            "dimension": dimension_name,
            "measures": measures_out,
            "summary": {
                "measures_at_100": measures_at_100,
                "total_measures": len(measures_out),
                "cells_total": total_cells,
                "cells_aligned": aligned_cells,
                "overall_cell_pct": (
                    round(100 * aligned_cells / total_cells, 2) if total_cells else 0.0
                ),
            },
        }

    # ── entry point ───────────────────────────────────────────────────────────

    def _run(self, **kwargs: Any) -> str:
        dry_run = self._cfg(kwargs, "dry_run") or False
        try:
            mappings = self._parse_mappings(
                self._cfg(kwargs, "mapping"), self._cfg(kwargs, "mappings_json")
            )
        except Exception as e:  # noqa: BLE001
            return json.dumps({"error": f"Could not parse mapping(s): {e}"})
        if not mappings:
            return json.dumps(
                {"error": "No mapping provided (mapping or mappings_json required)."}
            )

        views: dict = {}
        for view_name, mapping in mappings.items():
            try:
                views[view_name] = self._reconcile_view(
                    view_name, mapping, kwargs, dry_run
                )
            except Exception as e:  # noqa: BLE001
                logger.warning("[UCMVReconciliation] %s failed: %s", view_name, e)
                views[view_name] = {"error": str(e)}

        return json.dumps(
            {"views": views, "overall": _overall(views)}, indent=2, default=str
        )


def _num(value):
    """Coerce a pandas/NumPy cell into a JSON-friendly number or None."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    try:
        return float(value)
    except (TypeError, ValueError):
        return value


def _overall(views: dict) -> dict:
    scored = [v for v in views.values() if "summary" in v]
    measures_at_100 = sum(v["summary"]["measures_at_100"] for v in scored)
    total_measures = sum(v["summary"]["total_measures"] for v in scored)
    cells_total = sum(v["summary"]["cells_total"] for v in scored)
    cells_aligned = sum(v["summary"]["cells_aligned"] for v in scored)
    return {
        "views": len(views),
        "views_scored": len(scored),
        "measures_at_100": measures_at_100,
        "total_measures": total_measures,
        "cells_total": cells_total,
        "cells_aligned": cells_aligned,
        "overall_cell_pct": (
            round(100 * cells_aligned / cells_total, 2) if cells_total else 0.0
        ),
    }
