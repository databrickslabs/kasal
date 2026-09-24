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
from typing import Any, Callable, Optional

from .utils import to_snake_case

logger = logging.getLogger(__name__)


def _bq(fqn: str) -> str:
    """Back-quote each dotted part of a UC name (``a.b-c.d`` → `` `a`.`b-c`.`d` ``).

    A resolved PBI source catalog frequently contains characters that are not
    valid unquoted SQL (e.g. ``dc_adb-landing-zone-002``), so every emitted
    target/source name is quoted part-by-part.
    """
    parts = [p for p in str(fqn).split(".") if p]
    return ".".join(f"`{p.strip('`')}`" for p in parts)


def build_source_layer(
    all_specs: dict,
    mquery_tables: dict,
    config: dict | None,
    catalog: str,
    schema: str,
    mquery_expressions: dict | None = None,
) -> dict:
    """One source-layer UC view per fact/dim table (rec #3).

    Reproduces the physical read each metric view sits on:
    - a ``Value.NativeQuery`` fact keeps its verbatim SQL
      (``TableInfo.native_query_sql``, derived columns / filters preserved);
    - a dimension (no native SQL) reads ``SELECT * FROM <source_table>`` with the
      M transformation steps that follow the source (filters, code remaps, dedup)
      FOLDED in via ``MTransformFolder.fold_from_mquery`` — so a dim view carries
      its transforms too, not just a bare passthrough. Any untranslatable step is
      surfaced as a TODO, never silently dropped.

    DAX calculated columns from config-gen enrichment
    (``config['calculated_columns']``) are attached as ``todo`` calc columns —
    surfaced as ``-- TODO`` rather than emitted as (untranslated) SQL.

    Returns ``{table: {ddl, todo_steps, error}}`` — empty on any failure.
    """
    try:
        from .source_layer_emitter import (
            CalcColumn,
            TableSource,
            ViewSpec,
            emit_views,
        )
    except Exception as e:  # module import must never break generation
        logger.warning(f"[source_layer] emitter import failed: {e}")
        return {}

    calc_cfg = (config or {}).get("calculated_columns") or {}
    mq_exprs = mquery_expressions or {}
    fold_todos: dict[str, list] = {}  # table -> untranslatable M steps (surfaced)
    specs: list = []
    for key, spec in (all_specs or {}).items():
        ti = (mquery_tables or {}).get(key)
        native = (getattr(ti, "native_query_sql", "") or "") if ti else ""
        src_tbl = getattr(spec, "source_table", "") or (
            getattr(ti, "source_table", "") if ti else ""
        )
        if not native and not src_tbl:
            continue  # nothing resolvable to read from — skip (not a stub)
        # Dimension (no native SQL): fold the M transform steps onto the base read
        # so filters/remaps/dedup are reproduced, not dropped (S5). Fail-open to a
        # plain passthrough.
        if not native and src_tbl and mq_exprs.get(key):
            try:
                from .m_transform_folder import MTransformFolder

                _folder = MTransformFolder()
                _base = f"SELECT * FROM {_bq(src_tbl)}"
                _cols = getattr(ti, "group_by_columns", None) if ti else None
                _folded = _folder.fold_from_mquery(_base, mq_exprs[key], _cols)
                if _folded and _folded.strip() != _base.strip():
                    native = _folded
                if getattr(_folder, "transform_todos", None):
                    fold_todos[key] = list(_folder.transform_todos)
            except Exception as _fe:
                logger.debug(f"[source_layer] transform fold failed for {key}: {_fe}")
        try:
            source = (
                TableSource(native_sql=native)
                if native
                else TableSource(full_table_name=_bq(src_tbl))
            )
        except Exception:
            continue
        calc_columns = [
            CalcColumn(
                name=cc.get("name", ""),
                sql_expr=cc.get("expression", ""),
                translation_status="todo",  # DAX expr, not yet translated to SQL
                comment="DAX calculated column — verify/translate before use",
            )
            for cc in (calc_cfg.get(key) or [])
            if isinstance(cc, dict) and cc.get("name")
        ]
        target = f"{_bq(catalog)}.{_bq(schema)}.`src_{to_snake_case(key)}`"
        specs.append(
            ViewSpec(
                table_name=key,
                target_view=target,
                source=source,
                calc_columns=calc_columns,
                comment=f"Source layer for PBI table {key} — read the metric view off this.",
            )
        )
    if not specs:
        return {}
    try:
        emitted = emit_views(specs)
    except Exception as e:
        logger.warning(f"[source_layer] emit_views failed: {e}")
        return {}
    return {
        e.table_name: {
            "ddl": e.ddl,
            # emitter TODOs (calc columns) + any untranslatable M transform steps
            # folded in above — so a dropped/uncertain step is always visible.
            "todo_steps": list(e.todo_steps) + fold_todos.get(e.table_name, []),
            "error": e.error,
        }
        for e in emitted
    }


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
        logger.warning(f"[pbi_only] import failed: {e}")
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
            logger.debug(f"[pbi_only] detection failed for {table}: {e}")
    return tasks


def build_pbi_evaluate_fn(
    access_token: Optional[str],
    workspace_id: Optional[str],
    dataset_id: Optional[str],
    *,
    base_url: Optional[str] = None,
    uc_sql_fn: Optional[Callable[[str], Any]] = None,
) -> Optional[Callable[[str], list]]:
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

    def _evaluate(query: str) -> list:
        q = (query or "").strip()
        if not q.upper().startswith(("EVALUATE", "DEFINE")):
            # Generated UC SQL — needs a live warehouse + deployed view.
            if uc_sql_fn is None:
                return []
            try:
                return list(uc_sql_fn(query) or [])
            except Exception as e:
                logger.debug(f"[pbi_validation] uc_sql_fn failed: {e}")
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
                logger.debug(f"[pbi_validation] PBI error: {data['error']}")
                return []
            tables = (data.get("results", [{}]) or [{}])[0].get("tables", [])
            return tables[0].get("rows", []) if tables else []
        except Exception as e:
            logger.debug(f"[pbi_validation] executeQueries failed: {e}")
            return []

    return _evaluate


def run_pbi_validation(
    all_specs: dict,
    yaml_output: dict,
    evaluate_fn: Optional[Callable[[str], Any]] = None,
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

        results: dict = {}
        for view_name, sql in (yaml_output or {}).items():
            try:
                res = validate_view(view_name, sql, evaluate_fn)
                results[view_name] = getattr(res, "status", None) and res.status.value
            except Exception as e:
                results[view_name] = f"error: {e}"
        return {"status": "ran", "views": results}
    except Exception as e:
        logger.warning(f"[pbi_validation] failed: {e}")
        return {"status": "error", "reason": str(e)}
