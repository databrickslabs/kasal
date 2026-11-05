"""UCMV Source Layer Manager: the materialized-view layer under the metric views.

WHY: the metric views only aggregate; the source layer holds what Power BI did
before them: one materialized view per PBI table (the M query plus calculated
columns), snapshot tables for data that exists only inside the model, a refresh
that reloads the snapshots and then the views, and a count check that proves
the warehouse matches the published model.

Actions (``action``):

- ``emit``           source-layer DDL with row filters and RLS review hints (no I/O)
- ``deploy``         emit, then CREATE the views in dependency-safe order
- ``load_snapshots`` PBI-only tables -> typed ``pbi_<table>`` tables, filter re-applied
- ``refresh``        snapshots, then REFRESH MATERIALIZED VIEW (optionally skipped
                     when Power BI has not refreshed since the views were refreshed)
- ``count_check``    PBI vs UC counts, per period, sums, members; verdicts only

Logic lives in ``ucmv_source_layer_utils/``; this class wires real clients in.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable, Mapping
from typing import ClassVar, Optional, Tuple, Type

import httpx
from pydantic import BaseModel, Field, PrivateAttr

from src.services.tools.async_bridge import run_async_with_context
from src.services.tools.base import BaseTool
from src.services.tools.metric_view_utils.raw_fields import as_map, opt_str
from src.services.tools.powerbi_auth_utils import (
    get_fabric_access_token,
    get_powerbi_access_token,
)
from src.services.tools.ucmv_source_layer_utils.count_check import (
    CountRequest,
    run_count_check,
)
from src.services.tools.ucmv_source_layer_utils.deploy_order import deploy_order
from src.services.tools.ucmv_source_layer_utils.inputs import (
    parse_column_map,
    parse_names,
    parse_row_filters,
)
from src.services.tools.ucmv_source_layer_utils.names import (
    is_uc_fqn,
    parse_utc,
    quote_fqn,
    utc_stamp,
)
from src.services.tools.ucmv_source_layer_utils.pbi_client import PbiClient, PbiError
from src.services.tools.ucmv_source_layer_utils.refresh_plan import (
    RefreshRequest,
    list_views_sql,
    parse_views,
    run_refresh,
    runner,
)
from src.services.tools.ucmv_source_layer_utils.snapshot_loader import (
    SnapshotContext,
    expressions_tmdl_text,
    load_snapshots,
    parse_snapshot_specs,
    utc_now_stamp,
)
from src.services.tools.ucmv_source_layer_utils.sql_types import (
    SqlError,
    SqlFn,
    SqlResult,
)
from src.services.tools.ucmv_source_layer_utils.view_specs import emit_source_layer

logger = logging.getLogger(__name__)

Getter = Callable[[str], object]
ACTIONS = ("emit", "deploy", "load_snapshots", "refresh", "count_check")
FABRIC_API = "https://api.fabric.microsoft.com/v1"
_TIMEOUT = 120.0
_POLL_SECONDS = 2.0
_POLL_LIMIT = 60
_DEFAULT_CONFIG: dict[str, object] = {
    "action": "emit",
    "materialized": True,
    "skip_if_unchanged": False,
    "sums": False,
}

# Forwarded verbatim to the Power BI token helper (same keys as the drift monitor).
_PBI_KEYS = (
    "workspace_id",
    "dataset_id",
    "tenant_id",
    "client_id",
    "client_secret",
    "username",
    "password",
    "access_token",
    "auth_method",
)
_OWN_KEYS = (
    "action",
    "catalog",
    "schema_name",
    "warehouse_id",
    "databricks_host",
    "view_specs_json",
    "row_filters",
    "report_scope",
    "materialized",
    "snapshot_tables_json",
    "views",
    "view_prefix",
    "skip_if_unchanged",
    "pbi_table",
    "uc_object",
    "by",
    "uc_by",
    "members",
    "sums",
    "column_map_json",
    "cutoff_column",
)


class UCMVSourceLayerSchema(BaseModel):
    """Input schema. Normally every value comes from the task form."""

    action: Optional[str] = Field(
        None,
        description="emit | deploy | load_snapshots | refresh | count_check",
    )
    catalog: Optional[str] = Field(
        None, description="Unity Catalog catalog of the source layer."
    )
    schema_name: Optional[str] = Field(
        None, description="Unity Catalog schema of the source layer."
    )
    warehouse_id: Optional[str] = Field(
        None, description="SQL warehouse for DDL, loads and checks."
    )
    databricks_host: Optional[str] = Field(
        None, description="Workspace URL override (optional)."
    )
    workspace_id: Optional[str] = Field(None, description="Power BI workspace GUID.")
    dataset_id: Optional[str] = Field(None, description="Power BI semantic model GUID.")
    view_specs_json: Optional[str] = Field(
        None,
        description=(
            'JSON list for emit/deploy: [{"table", "source_table" or "native_sql", '
            '"m_expression", "group_by_columns", "calc_columns", "target"}].'
        ),
    )
    row_filters: Optional[str] = Field(
        None,
        description=(
            'JSON object keyed by PBI table (or "*" for all): {"function": "cat.sch.fn", '
            '"columns": [...], "constants": [...]}. Set at CREATE; snapshots get it re-applied.'
        ),
    )
    report_scope: Optional[str] = Field(
        None,
        description="Scope table (CSV or JSON rows); rls_filter rows become review hints.",
    )
    materialized: Optional[bool] = Field(
        True, description="Emit CREATE MATERIALIZED VIEW (required for row filters)."
    )
    snapshot_tables_json: Optional[str] = Field(
        None,
        description=(
            'JSON list: [{"pbi_table", "uc_table", "page_by", "types", "column_map", '
            '"tmdl_expression"}]. uc_table defaults to pbi_<table>.'
        ),
    )
    views: Optional[str] = Field(
        None, description="Materialized views to refresh (comma/newline separated)."
    )
    view_prefix: Optional[str] = Field(
        None, description="Refresh every view starting with this prefix."
    )
    skip_if_unchanged: Optional[bool] = Field(
        False,
        description="Skip the refresh when every view was refreshed after Power BI's last refresh.",
    )
    pbi_table: Optional[str] = Field(
        None, description="count_check: the Power BI table."
    )
    uc_object: Optional[str] = Field(
        None,
        description="count_check: the UC object (schema.object or catalog.schema.object).",
    )
    by: Optional[str] = Field(
        None, description="count_check: period / grouping column (PBI name)."
    )
    uc_by: Optional[str] = Field(
        None, description="count_check: UC name of the grouping column."
    )
    members: Optional[str] = Field(
        None,
        description="count_check: categorical columns to compare ('Col' or 'Col=uc_col').",
    )
    sums: Optional[bool] = Field(
        False,
        description="count_check: compare numeric sums per period (names only are reported).",
    )
    column_map_json: Optional[str] = Field(
        None, description='count_check: JSON {"PBI name": "uc_name"}.'
    )
    cutoff_column: Optional[str] = Field(
        None,
        description="count_check: UC load-stamp column; rows after the PBI refresh are excluded.",
    )
    tenant_id: Optional[str] = Field(None, description="Power BI auth: tenant id.")
    client_id: Optional[str] = Field(None, description="Power BI auth: client id.")
    client_secret: Optional[str] = Field(
        None, description="Power BI auth: client secret."
    )
    username: Optional[str] = Field(
        None, description="Power BI auth: service account user."
    )
    password: Optional[str] = Field(
        None, description="Power BI auth: service account password."
    )
    access_token: Optional[str] = Field(
        None, description="Power BI access token (if already held)."
    )
    auth_method: Optional[str] = Field(None, description="Power BI auth method.")


def _flag(value: object) -> bool:
    return value in (True, "true", "True")


class UCMVSourceLayerTool(BaseTool):
    """Emit, deploy, load and check the materialized-view source layer."""

    name: str = "UCMV Source Layer Manager"
    description: str = (
        "Manages the materialized-view layer beneath Unity Catalog metric views. "
        "action=emit: source-layer DDL per Power BI table, with row filters "
        "(set at CREATE) and RLS review hints from the scope table. action=deploy: "
        "emit and CREATE the views in dependency-safe order. action=load_snapshots: "
        "PBI-only tables (typed-in, Excel, other models) read from the published "
        "model into typed pbi_<table> tables, with the row filter re-applied. "
        "action=refresh: reload snapshots, then REFRESH MATERIALIZED VIEW, optionally "
        "skipped when Power BI has not refreshed. action=count_check: PBI vs UC row "
        "counts, per period, members and sums; verdicts and counts only."
    )
    args_schema: Type[BaseModel] = UCMVSourceLayerSchema
    _default_config: dict = PrivateAttr(default_factory=dict)
    #: Parameters a skill attached to the agent may fill (Configuration -> Skills).
    skill_context_params: ClassVar[Tuple[str, ...]] = ("row_filters",)

    model_config = {"arbitrary_types_allowed": True, "extra": "allow"}

    def __init__(self, **kwargs: object) -> None:
        default_config: dict[str, object] = dict(_DEFAULT_CONFIG)
        for key in _PBI_KEYS + _OWN_KEYS:
            val = kwargs.pop(key, None)
            if val is not None:
                default_config[key] = val
        super().__init__(**kwargs)
        self._default_config = default_config

    # ------------------------------------------------------------------
    # Clients (the only I/O in this class)
    # ------------------------------------------------------------------
    def _sql(self, get: Getter) -> SqlFn:
        from src.services.tools.metric_view_deployer_tool import MetricViewDeployerTool
        from src.utils.telemetry import KasalProduct, get_user_agent_header
        from src.utils.url_security import is_trusted_databricks_host

        warehouse_id = str(get("warehouse_id") or "")
        if not warehouse_id:
            raise ValueError("warehouse_id is required")
        auth = MetricViewDeployerTool()._authenticate(
            host_override=opt_str(get("databricks_host")) or None
        )
        if not auth:
            raise ValueError("Databricks authentication failed")
        url = (auth.workspace_url or "").rstrip("/")
        if not url or not is_trusted_databricks_host(url):
            raise ValueError(
                "workspace URL is not configured or not a trusted Databricks host"
            )
        headers = auth.get_headers()
        headers.update(get_user_agent_header(KasalProduct.POWERBI))

        def _run(statement: str) -> SqlResult:
            payload = MetricViewDeployerTool._execute_sql_sync(
                statement, url, warehouse_id, headers
            )
            if not payload.get("success"):
                raise SqlError(
                    str(
                        payload.get("error")
                        or payload.get("state")
                        or "statement failed"
                    )
                )
            return _sql_result(as_map(payload.get("data")))

        return _run

    def _pbi(self, get: Getter) -> PbiClient:
        token = run_async_with_context(
            get_powerbi_access_token(
                tenant_id=opt_str(get("tenant_id")),
                client_id=opt_str(get("client_id")),
                client_secret=opt_str(get("client_secret")),
                access_token=opt_str(get("access_token")),
                username=opt_str(get("username")),
                password=opt_str(get("password")),
                auth_method=opt_str(get("auth_method")),
            ),
            timeout=_TIMEOUT,
        )
        return PbiClient(
            token, str(get("workspace_id") or ""), str(get("dataset_id") or "")
        )

    def _tmdl_text(self, get: Getter) -> str:
        """expressions.tmdl of the model (Fabric getDefinition, long-running)."""
        token = run_async_with_context(
            get_fabric_access_token(
                tenant_id=opt_str(get("tenant_id")),
                client_id=opt_str(get("client_id")),
                client_secret=opt_str(get("client_secret")),
                username=opt_str(get("username")),
                password=opt_str(get("password")),
                auth_method=opt_str(get("auth_method")),
            ),
            timeout=_TIMEOUT,
        )
        workspace = str(get("workspace_id") or "")
        model = str(get("dataset_id") or "")
        url = f"{FABRIC_API}/workspaces/{workspace}/semanticModels/{model}/getDefinition?format=TMDL"
        headers = {"Authorization": f"Bearer {token}"}
        with httpx.Client(timeout=_TIMEOUT) as client:
            response = client.post(url, headers=headers, json={})
            if response.status_code == 200:
                return expressions_tmdl_text(response.json())
            if response.status_code != 202:
                raise PbiError(f"getDefinition failed: HTTP {response.status_code}")
            operation = _operation_url(response.headers.get("Location"))
            for _ in range(_POLL_LIMIT):
                time.sleep(_POLL_SECONDS)  # sync tool, runs in a worker thread
                state = client.get(operation, headers=headers)
                status = as_map(state.json()).get("status")
                if status == "Succeeded":
                    result = client.get(f"{operation}/result", headers=headers)
                    return expressions_tmdl_text(result.json())
                if status == "Failed":
                    raise PbiError("getDefinition operation failed")
        raise PbiError("getDefinition did not finish in time")

    # ------------------------------------------------------------------
    # Actions
    # ------------------------------------------------------------------
    def _catalog_schema(self, get: Getter) -> tuple[str, str]:
        catalog = str(get("catalog") or "").strip()
        schema = str(get("schema_name") or "").strip()
        if not catalog or not schema or not is_uc_fqn(f"{catalog}.{schema}.x"):
            raise ValueError("catalog and schema_name are required (plain names)")
        return catalog, schema

    def _emit_result(self, get: Getter) -> dict[str, object]:
        catalog, schema = self._catalog_schema(get)
        return emit_source_layer(
            entries_json=get("view_specs_json"),
            catalog=catalog,
            schema=schema,
            row_filters_json=get("row_filters"),
            materialized=get("materialized") not in (False, "false", "False"),
            scope_json=get("report_scope"),
        )

    def _emit(self, get: Getter) -> dict[str, object]:
        return self._emit_result(get)

    def _deploy(self, get: Getter) -> dict[str, object]:
        emitted = self._emit_result(get)
        views = as_map(emitted.get("views"))
        pending: dict[str, tuple[str, str]] = {}
        for table, view in views.items():
            view_map = as_map(view)
            if not view_map.get("error"):
                pending[table] = (str(view_map["target"]), str(view_map["ddl"]))
        sql = self._sql(get)
        steps: list[dict[str, object]] = []
        stopped_by: str | None = None
        for table in deploy_order(pending):
            target = pending[table][0]
            if stopped_by:
                steps.append(
                    {"view": target, "status": "not attempted", "reason": stopped_by}
                )
                continue
            try:
                sql(pending[table][1])
                steps.append({"view": target, "status": "deployed"})
            except SqlError as exc:
                steps.append({"view": target, "status": "failed", "error": str(exc)})
                stopped_by = f"{target} failed"
        for table, view in views.items():
            if as_map(view).get("error"):
                steps.append(
                    {
                        "view": table,
                        "status": "not deployed",
                        "error": as_map(view)["error"],
                    }
                )
        return {
            "deploy": steps,
            "all_deployed": all(s["status"] == "deployed" for s in steps),
            "views_with_todo_steps": [
                t for t, v in views.items() if as_map(v).get("todo_steps")
            ],
            "notes": emitted.get("notes"),
        }

    def _snapshot_context(
        self,
        get: Getter,
        refreshed_at: str | None,
        sql: SqlFn,
        pbi: PbiClient,
    ) -> SnapshotContext:
        catalog, schema = self._catalog_schema(get)
        cache: dict[str, str] = {}

        def _tmdl() -> str:
            if "text" not in cache:
                cache["text"] = self._tmdl_text(get)
            return cache["text"]

        return SnapshotContext(
            catalog=catalog,
            schema=schema,
            dax=pbi.dax,
            sql=sql,
            filters=parse_row_filters(get("row_filters")),
            loaded_at=utc_now_stamp(),
            refreshed_at=refreshed_at,
            tmdl=_tmdl,
        )

    def _load_snapshots(self, get: Getter) -> dict[str, object]:
        specs = parse_snapshot_specs(get("snapshot_tables_json"))
        if not specs:
            raise ValueError("snapshot_tables_json lists no tables")
        sql = self._sql(get)
        pbi = self._pbi(get)
        refreshed = pbi.last_completed_refresh()
        refreshed_at = utc_stamp(parse_utc(refreshed)) if refreshed else None
        return load_snapshots(
            specs, self._snapshot_context(get, refreshed_at, sql, pbi)
        )

    def _refresh(self, get: Getter) -> dict[str, object]:
        catalog, schema = self._catalog_schema(get)
        sql = self._sql(get)
        pbi = self._pbi(get)
        specs_raw = get("snapshot_tables_json")
        has_snapshots = bool(parse_snapshot_specs(specs_raw))

        def _load(refreshed: str | None) -> dict[str, object]:
            refreshed_at = utc_stamp(parse_utc(refreshed)) if refreshed else None
            return load_snapshots(
                parse_snapshot_specs(specs_raw),
                self._snapshot_context(get, refreshed_at, sql, pbi),
            )

        request = RefreshRequest(
            catalog=catalog,
            schema=schema,
            views=tuple(parse_names(get("views"))),
            prefix=str(get("view_prefix") or ""),
            skip_if_unchanged=_flag(get("skip_if_unchanged")),
        )
        return run_refresh(
            request,
            last_refresh=pbi.last_completed_refresh,
            list_views=lambda: parse_views(sql(list_views_sql(catalog, schema))),
            load_snapshots=_load if has_snapshots else None,
            refresh_view=runner(sql),
        )

    def _count_check(self, get: Getter) -> dict[str, object]:
        catalog, schema = self._catalog_schema(get)
        pbi_table = str(get("pbi_table") or "").strip()
        if not pbi_table:
            raise ValueError("pbi_table is required for count_check")
        uc_object = str(get("uc_object") or "").strip()
        parts = uc_object.split(".")
        if len(parts) == 2:
            uc_object = f"{catalog}.{uc_object}"
        elif len(parts) == 1:
            uc_object = f"{catalog}.{schema}.{uc_object}"
        if not is_uc_fqn(uc_object):
            raise ValueError("uc_object must be schema.object or catalog.schema.object")
        request = CountRequest(
            pbi_table=pbi_table,
            target=quote_fqn(uc_object),
            by=str(get("by") or ""),
            uc_by=str(get("uc_by") or ""),
            members=tuple(parse_names(get("members"))),
            sums=_flag(get("sums")),
            column_map=parse_column_map(get("column_map_json")),
            cutoff_column=str(get("cutoff_column") or ""),
        )
        pbi = self._pbi(get)
        return run_count_check(
            request,
            dax=pbi.dax,
            sql=self._sql(get),
            last_refresh=pbi.last_completed_refresh,
        )

    # ------------------------------------------------------------------
    # Run
    # ------------------------------------------------------------------
    def _run(self, **kwargs: object) -> str:
        def get(key: str) -> object:
            # Agents often pass empty placeholders; never let "" shadow the task form.
            val = kwargs.get(key)
            if val is None or val == "" or val == [] or val == {}:
                return self._default_config.get(key)
            return val

        action = str(get("action") or "emit").strip().lower()
        handlers: dict[str, Callable[[Getter], dict[str, object]]] = {
            "emit": self._emit,
            "deploy": self._deploy,
            "load_snapshots": self._load_snapshots,
            "refresh": self._refresh,
            "count_check": self._count_check,
        }
        if action not in handlers:
            return _fail(action, f"action must be one of {', '.join(ACTIONS)}")
        try:
            result = handlers[action](get)
        except (ValueError, PbiError, SqlError, httpx.HTTPError) as exc:
            return _fail(action, str(exc))
        return json.dumps({"action": action, **result}, indent=2, default=str)


def _fail(action: str, message: str) -> str:
    logger.warning("[UCMVSourceLayer] %s: %s", action, message)
    return json.dumps({"action": action, "error": message}, indent=2)


def _operation_url(location: str | None) -> str:
    """A Fabric long-running operation URL; only Fabric's own host is followed."""
    if not location or not location.startswith(FABRIC_API + "/"):
        raise PbiError("getDefinition returned no usable operation location")
    return location


def _sql_result(data: Mapping[str, object]) -> SqlResult:
    """Statement API payload -> columns and text rows (no row is logged)."""
    manifest = as_map(data.get("manifest"))
    schema = as_map(manifest.get("schema"))
    column_items = schema.get("columns")
    columns = [
        str(as_map(c).get("name", ""))
        for c in (column_items if isinstance(column_items, list) else [])
    ]
    result = as_map(data.get("result"))
    data_rows = result.get("data_array")
    rows: list[list[str | None]] = []
    for row in data_rows if isinstance(data_rows, list) else []:
        if isinstance(row, list):
            rows.append([None if v is None else str(v) for v in row])
    return SqlResult(columns=columns, rows=rows)
