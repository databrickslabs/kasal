"""Bridge: turn a run's pipeline state into the source-layer, PBI-only-ingestion
and validation artifacts, so the generator tool can emit them without carrying
the glue itself (the tool is already over the size ceiling).

Each function is fail-open and pure-ish: it reads the pipeline's already-built
`all_specs` / `mquery_tables` / `config` and returns plain dicts/lists ready to
drop into the tool's output. The heavy lifting lives in the three standalone
modules — `source_layer_emitter`, `pbi_only_ingestion`, `pbi_validation`; this
module only adapts pipeline shapes to their inputs.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Callable
from typing import TYPE_CHECKING, Optional

from .source_layer_binding import (
    normalized_name,
    read_from_source_layer,
    source_layer_ddl_name,
    source_layer_leaf_name,
    source_layer_naming,
)

if TYPE_CHECKING:
    from .data_classes import MetricViewSpec, TableInfo
    from .pbi_validation import EvaluateFn, Rows

logger = logging.getLogger(__name__)


def _bq(fqn: str) -> str:
    """Back-quote each dotted part of a UC name (``a.b-c.d`` → `` `a`.`b-c`.`d` ``).

    A resolved PBI source catalog frequently contains characters that are not
    valid unquoted SQL (e.g. ``acme_adb-landing-zone-002``), so every emitted
    target/source name is quoted part-by-part.
    """
    parts = [p for p in str(fqn).split(".") if p]
    return ".".join(f"`{p.strip('`')}`" for p in parts)


def _planned_tables(
    all_specs: dict, mquery_tables: dict, include_unspecced: bool
) -> list[tuple[str, MetricViewSpec | None, TableInfo | None]]:
    """``(key, spec | None, TableInfo | None)`` for every table to plan a view for.

    Default: the metric-view specs only. In read mode the PBI tables that are not
    specs (dimensions) are planned too, since a join reads their view.
    """
    planned: list[tuple[str, MetricViewSpec | None, TableInfo | None]] = [
        (key, spec, (mquery_tables or {}).get(key))
        for key, spec in (all_specs or {}).items()
    ]
    if include_unspecced:
        planned += [
            (key, None, info)
            for key, info in (mquery_tables or {}).items()
            if key not in (all_specs or {})
        ]
    return planned


def _fold_dimension_read(
    key: str,
    src_tbl: str,
    info: TableInfo | None,
    m_expression: str,
    fold_todos: dict[str, list],
) -> str:
    """The M transform steps folded onto a base ``SELECT * FROM`` read ("" if none).

    Fail-open to a plain passthrough. Any untranslatable step is recorded in
    ``fold_todos`` so it is surfaced, never silently dropped (S5).
    """
    from .m_transform_folder import MTransformFolder

    folder = MTransformFolder()
    base = f"SELECT * FROM {_bq(src_tbl)}"
    cols = getattr(info, "group_by_columns", None) if info is not None else None
    try:
        folded = folder.fold_from_mquery(base, m_expression, cols)
    except Exception as exc:
        logger.debug(
            "[source_layer] transform fold failed for %s: %s",
            key,
            exc,
            exc_info=True,
        )
        return ""
    if getattr(folder, "transform_todos", None):
        fold_todos[key] = list(folder.transform_todos)
    if folded and folded.strip() != base.strip():
        return folded
    return ""


def _calc_columns(key: str, calc_cfg: dict, calc_column_cls: type) -> list:
    """DAX calculated columns for a table, as ``todo`` CalcColumns (never SQL)."""
    return [
        calc_column_cls(
            name=cc.get("name", ""),
            sql_expr=cc.get("expression", ""),
            translation_status="todo",  # DAX expr, not yet translated to SQL
            comment="DAX calculated column — verify/translate before use",
        )
        for cc in (calc_cfg.get(key) or [])
        if isinstance(cc, dict) and cc.get("name")
    ]


def _would_overwrite_source(target_fqn: str, src_tbl: str, native_sql: str) -> bool:
    """True when the target view name is the table the view itself reads."""
    target = normalized_name(target_fqn)
    if src_tbl and normalized_name(src_tbl) == target:
        return True
    if not native_sql:
        return False
    text = native_sql.replace("`", "").lower()
    return re.search(rf"(?<![\w.]){re.escape(target)}(?!\w)", text) is not None


def build_source_layer(
    all_specs: dict,
    mquery_tables: dict,
    config: dict | None,
    catalog: str,
    schema: str,
    mquery_expressions: dict | None = None,
) -> dict:
    """One source-layer UC view per PBI table (rec #3).

    Reproduces the physical read each metric view sits on:
    - a ``Value.NativeQuery`` table keeps its verbatim SQL
      (``TableInfo.native_query_sql``, derived columns / filters preserved);
    - a table without native SQL reads ``SELECT * FROM <source_table>`` with the
      M transformation steps that follow the source (filters, code remaps, dedup)
      FOLDED in via ``MTransformFolder.fold_from_mquery``. Any untranslatable step
      is surfaced as a TODO, never silently dropped.

    DAX calculated columns from config-gen enrichment
    (``config['calculated_columns']``) are attached as ``todo`` calc columns —
    surfaced as ``-- TODO`` rather than emitted as (untranslated) SQL.

    Config (read by ``source_layer_binding``):
    - ``source_layer_naming``: ``"src_prefix"`` (``src_<snake>``, default) or
      ``"table_name"`` (``<snake>``);
    - ``read_from_source_layer``: when true, every PBI table gets a view (not only
      the metric-view specs, since joins read dimension views), each view is
      materialized, and the views are what the metric views read.

    Returns ``{table: {ddl, todo_steps, error}}`` keyed by PBI table.
    """
    try:
        from .source_layer_emitter import CalcColumn, TableSource, ViewSpec, emit_views
    except Exception as e:  # module import must never break generation
        logger.warning("[source_layer] emitter import failed: %s", e, exc_info=True)
        return {}
    from src.services.tools.ucmv_source_layer_utils.inputs import (
        parse_row_filters,
        row_filter_for,
    )

    read_mode = read_from_source_layer(config)
    planned = _planned_tables(all_specs, mquery_tables, read_mode)
    try:
        naming = source_layer_naming(config)
        row_filters = parse_row_filters((config or {}).get("row_filters"))
    except ValueError as e:
        # A bad config is reported per view, never silently dropped.
        return {
            key: {"ddl": "", "todo_steps": [], "error": str(e)}
            for key, _s, _i in planned
        }

    calc_cfg = (config or {}).get("calculated_columns") or {}
    mq_exprs = mquery_expressions or {}
    fold_todos: dict[str, list] = {}
    specs: list = []
    errors: dict[str, str] = {}
    for key, spec, info in planned:
        native = (info.native_query_sql or "") if info is not None else ""
        src_tbl = (getattr(spec, "source_table", "") or "") or (
            info.source_table if info is not None else ""
        )
        if not native and not src_tbl:
            continue  # nothing resolvable to read from — skip (not a stub)
        if not native and src_tbl and mq_exprs.get(key):
            folded = _fold_dimension_read(key, src_tbl, info, mq_exprs[key], fold_todos)
            if folded:
                native = folded
        try:
            source = (
                TableSource(native_sql=native)
                if native
                else TableSource(full_table_name=_bq(src_tbl))
            )
        except Exception:
            logger.debug("[source_layer] unusable source for %s", key, exc_info=True)
            continue
        target_fqn = f"{catalog}.{schema}.{source_layer_leaf_name(key, naming)}"
        if _would_overwrite_source(target_fqn, src_tbl, native):
            errors[key] = (
                "target view would overwrite the table it reads; use "
                "source_layer_naming 'src_prefix' or another schema"
            )
            continue
        # A row filter exists only on materialized views (set at CREATE). In read
        # mode every view is materialized: the metric views read a frozen copy.
        row_filter = row_filter_for(key, row_filters)
        specs.append(
            ViewSpec(
                table_name=key,
                target_view=source_layer_ddl_name(
                    catalog, schema, source_layer_leaf_name(key, naming)
                ),
                source=source,
                calc_columns=_calc_columns(key, calc_cfg, CalcColumn),
                comment=f"Source layer for PBI table {key} — read the metric view off this.",
                materialized=read_mode or row_filter is not None,
                row_filter=row_filter,
            )
        )
    out: dict = {
        key: {"ddl": "", "todo_steps": [], "error": msg} for key, msg in errors.items()
    }
    if not specs:
        return out
    try:
        emitted = emit_views(specs)
    except Exception as e:
        logger.warning("[source_layer] emit_views failed: %s", e, exc_info=True)
        return out
    out.update(
        {
            e.table_name: {
                "ddl": e.ddl,
                # emitter TODOs (calc columns) + any untranslatable M transform steps
                # folded in above — so a dropped/uncertain step is always visible.
                "todo_steps": list(e.todo_steps) + fold_todos.get(e.table_name, []),
                "error": e.error,
            }
            for e in emitted
        }
    )
    return out


def detect_pbi_only_tables(
    mquery_expressions: dict, catalog: str, schema: str
) -> list[dict]:
    """PBI-only source detection + ingestion tasks (S8).

    For every table whose M reads from Excel / SharePoint / Web / a typed or
    Json.Document literal (no warehouse source to transpile), emit an ingestion
    task so the team knows it needs a snapshot loader, not a SQL translation.
    Returns a list of task descriptors (empty when none / on failure).
    """
    try:
        from .pbi_only_ingestion import build_ingestion_task, detect_pbi_only_source
    except Exception as e:
        logger.warning("[pbi_only] import failed: %s", e, exc_info=True)
        return []
    tasks: list[dict] = []
    for table, mquery in (mquery_expressions or {}).items():
        if not mquery:
            continue
        try:
            src = detect_pbi_only_source(mquery)
            if src is None:
                continue
            task = build_ingestion_task(table, src.kind, catalog, schema, source=src)
            tasks.append(
                {
                    "table": task.table_name,
                    "uc_target": task.uc_target,
                    "kind": getattr(task.kind, "value", str(task.kind)),
                    "source_description": task.source_description,
                    "recommended_approach": task.recommended_approach,
                    "snapshot_loader_stub": task.snapshot_loader_stub,
                }
            )
        except Exception as e:
            logger.debug(
                "[pbi_only] detection failed for %s: %s", table, e, exc_info=True
            )
    return tasks


def build_pbi_evaluate_fn(
    access_token: Optional[str],
    workspace_id: Optional[str],
    dataset_id: Optional[str],
    *,
    base_url: Optional[str] = None,
    uc_sql_fn: Optional[Callable[[str], Rows | None]] = None,
) -> Optional[EvaluateFn]:
    """Build the ``evaluate_fn`` the validation loop (rec#4) needs, or ``None``
    when the PBI credentials required to run it aren't present.

    The returned callable routes by query shape, which is exactly the contract
    ``pbi_validation.validate_view`` / ``validate_measure`` expect:
    - a DAX query (``EVALUATE`` / ``DEFINE``) → Power BI ``executeQueries`` REST
      call against the dataset (the reference side);
    - anything else (the generated UC SQL) → ``uc_sql_fn`` when supplied (a
      Databricks SQL executor for the deployed view), else ``[]`` — so the view
      is reported *unverified* rather than crashing when it isn't deployed yet.

    Synchronous (httpx.Client) so it drops straight into the tool's sync
    post-processing. Fail-soft: a failed PBI call returns ``[]``.
    """
    if not (access_token and workspace_id and dataset_id):
        return None
    api_base = (base_url or "https://api.powerbi.com/v1.0/myorg").rstrip("/")
    url = f"{api_base}/groups/{workspace_id}/datasets/{dataset_id}/executeQueries"

    def _evaluate(query: str) -> Rows:
        q = (query or "").strip()
        if not q.upper().startswith(("EVALUATE", "DEFINE")):
            # Generated UC SQL — needs a live warehouse + deployed view.
            if uc_sql_fn is None:
                return []
            try:
                return list(uc_sql_fn(query) or [])
            except Exception as e:
                logger.debug("[pbi_validation] uc_sql_fn failed: %s", e, exc_info=True)
                return []
        try:
            import httpx

            headers = {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            }
            payload = {
                "queries": [{"query": q}],
                "serializerSettings": {"includeNulls": True},
            }
            with httpx.Client(timeout=120.0) as client:
                resp = client.post(url, headers=headers, json=payload)
                resp.raise_for_status()
                data = resp.json()
            if isinstance(data, dict) and "error" in data:
                logger.debug("[pbi_validation] PBI error: %s", data["error"])
                return []
            tables = (data.get("results", [{}]) or [{}])[0].get("tables", [])
            rows: Rows = tables[0].get("rows", []) if tables else []
            return rows
        except Exception as e:
            logger.debug("[pbi_validation] executeQueries failed: %s", e, exc_info=True)
            return []

    return _evaluate


def run_pbi_validation(
    all_specs: dict,
    yaml_output: dict,
    evaluate_fn: Optional[EvaluateFn] = None,
) -> dict:
    """Validation loop hook (rec #4).

    When a live PBI ``evaluate_fn`` (a service-account ``executeQueries`` callback)
    is supplied, this is where each view/measure would be compared against
    ``EVALUATE`` per period/country and marked verified/unverified. No callback is
    wired in the generation subprocess yet, so this reports ``skipped`` with the
    reason — the honest state — rather than silently claiming "verified". The
    mechanism (``pbi_validation.validate_view`` / ``validate_measure``) is ready
    for the caller that can provide the callback.
    """
    if evaluate_fn is None:
        return {
            "status": "skipped",
            "reason": (
                "no live PBI EVALUATE callback configured — provide a "
                "service-account executeQueries callback to verify views/measures "
                "against Power BI per period/country"
            ),
            "candidate_views": sorted((yaml_output or {}).keys()),
        }
    # A callback IS available — validate each emitted view. (Reserved for the
    # caller that wires live creds; kept minimal and fail-open.)
    try:
        from .pbi_validation import validate_view

        results: dict[str, str] = {}
        for view_name, sql in (yaml_output or {}).items():
            try:
                res = validate_view(view_name, sql, evaluate_fn)
                results[view_name] = res.status.value
            except Exception as e:
                logger.warning(
                    "[pbi_validation] view %s failed: %s", view_name, e, exc_info=True
                )
                results[view_name] = f"error: {e}"
        return {"status": "ran", "views": results}
    except Exception as e:
        logger.warning("[pbi_validation] failed: %s", e, exc_info=True)
        return {"status": "error", "reason": str(e)}
