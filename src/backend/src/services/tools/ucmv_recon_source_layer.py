"""Deploy the generator's source layer before the metric views of a reconciliation cycle.

In read mode (``read_from_source_layer``) every metric view reads views of the source layer: one
view per Power BI table, built from its Power Query (``Value.NativeQuery`` SQL, applied steps).
The generator emits their DDL as ``source_layer_ddl`` but deploys nothing, so a metric view that
joins ``<target>.c_dim_calendar`` fails with "Invalid source" until those views exist.

This module runs that DDL through the deployer's own session (auth, host check, telemetry), in
dependency order (``ucmv_source_layer_utils.deploy_order``), and reports every view that could not
be created. A view that fails does not stop the others; the metric views that read it fail on
deploy and say which source is missing.
"""

from __future__ import annotations

import hashlib
import logging
import re
from collections.abc import Mapping

from src.services.tools.metric_view_deployer_utils.sql_helpers import (
    SqlRunner,
    error_text,
    sanitize_namespace,
)
from src.services.tools.metric_view_utils.raw_fields import RawMap, as_map
from src.services.tools.ucmv_source_layer_utils.deploy_order import deploy_order

logger = logging.getLogger(__name__)

_TARGET = re.compile(
    r"\bVIEW\s+(?:IF\s+NOT\s+EXISTS\s+)?((?:`[^`]+`|[\w-]+)(?:\.(?:`[^`]+`|[\w-]+)){2})",
    re.I,
)
_MAX_REASON = 300


def planned_views(
    ddl_map: Mapping[str, object],
) -> tuple[dict[str, tuple[str, str]], dict[str, str]]:
    """``{table: (target, ddl)}`` for every emitted view, and ``{table: reason}`` for the rest."""
    views: dict[str, tuple[str, str]] = {}
    failures: dict[str, str] = {}
    for table, raw in ddl_map.items():
        entry = as_map(raw)
        ddl = str(entry.get("ddl") or "")
        if entry.get("error") or not ddl.strip():
            failures[str(table)] = f"not emitted: {entry.get('error') or 'no DDL'}"[
                :_MAX_REASON
            ]
            continue
        target = _TARGET.search(ddl)
        if not target:
            failures[str(table)] = "not emitted: no view name in the DDL"
            continue
        views[str(table)] = (target.group(1), ddl)
    return views, failures


def fingerprint(ddl_map: Mapping[str, object]) -> str:
    """Changes when any DDL changes, so an unchanged source layer is deployed once per run."""
    digest = hashlib.sha256()
    for table in sorted(ddl_map):
        digest.update(table.encode())
        digest.update(str(as_map(ddl_map[table]).get("ddl") or "").encode())
    return digest.hexdigest()


def layer_session(
    deployer: object, deploy_base: Mapping[str, object]
) -> tuple[str, str, SqlRunner | None, str]:
    """``(catalog, schema, SQL runner, error)`` for the deployer's session on the layer's
    namespace; the runner is None and the error set when no session opens."""
    from src.services.tools.metric_view_deployer_tool import _RunOptions

    catalog = sanitize_namespace(str(deploy_base.get("catalog") or "main"))
    schema = sanitize_namespace(str(deploy_base.get("schema_name") or "default"))
    opts = _RunOptions(
        catalog=catalog,
        schema=schema,
        host_override=str(deploy_base.get("databricks_host") or "") or None,
        warehouse_id=str(deploy_base.get("warehouse_id") or ""),
        preserve_grants=False,
        group_by=None,
        timing=False,
        slow_s=5.0,
    )
    open_session = getattr(deployer, "_open_session")
    runner, session_error = open_session(opts)
    return catalog, schema, runner, str(session_error or "")


def deploy_source_layer(
    deployer: object, ddl_map: Mapping[str, object], deploy_base: Mapping[str, object]
) -> RawMap:
    """Create the source-layer views. Returns ``{deployed: [...], failures: {table: reason}}``."""
    from src.services.tools.metric_view_deployer_tool import MetricViewDeployerTool

    views, failures = planned_views(ddl_map)
    if not views:
        return {"deployed": [], "failures": failures}
    try:
        order = deploy_order(views)
    except ValueError as error:
        return {"deployed": [], "failures": {**failures, "_order": str(error)}}
    if not isinstance(deployer, MetricViewDeployerTool):
        return {
            "deployed": [],
            "failures": {**failures, "_session": "no deployer session"},
        }

    catalog, schema, runner, session_error = layer_session(deployer, deploy_base)
    if runner is None:
        return {
            "deployed": [],
            "failures": {**failures, "_session": session_error},
        }
    MetricViewDeployerTool._ensure_schema(runner, catalog, schema)

    deployed: list[str] = []
    for number, table in enumerate(order, 1):
        logger.info("[UCMV] source layer %d/%d: %s", number, len(order), table)
        outcome = runner(views[table][1])
        if outcome.get("success"):
            deployed.append(table)
        else:
            failures[table] = error_text(outcome)[:_MAX_REASON]
    logger.info(
        "[UCMV] source layer: %d view(s) created, %d not", len(deployed), len(failures)
    )
    return {"deployed": deployed, "failures": failures}
