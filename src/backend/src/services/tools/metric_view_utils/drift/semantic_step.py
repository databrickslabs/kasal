"""The semantic half of the drift check: what changed in the Power BI model and report, and what it means.

The measure drift check (``orchestrator``) compares today's DAX with the deployed
metric views. This step looks wider — tables, columns, relationships, Power Query,
RLS, pages, page fields, tracked data — by comparing the semantic snapshot the
Pipeline Config Generator took today (``semantic_snapshot``) with the previous one,
and routes every change through the report's scope table.

The previous snapshot comes from the caller (``semantic_baseline``) or from a
Unity Catalog table (``semantic_snapshot_table``): the last snapshot stored for
the semantic model is read, and today's is appended when anything changed. The
first run against an empty table stores the baseline and reports no changes.

SQL is injected (``sql_fn``), so this module holds no auth or HTTP code.
"""

from __future__ import annotations

import csv
import io
import json
import logging
import re
from collections import Counter
from collections.abc import Callable, Mapping
from typing import Optional, TypedDict

import yaml

from src.services.powerbi.scope.semantic_sync import (
    compare,
    ignore_rules,
    with_ignores,
    with_impact,
)
from src.services.tools.metric_view_utils.dax_condition_sql import _sql_str_literal
from src.services.tools.metric_view_utils.drift.baseline import split_full_name
from src.services.tools.metric_view_utils.drift.semantic_proposals import (
    MAX_PROPOSALS,
    CompleteFn,
    propose,
)

logger = logging.getLogger(__name__)

SqlFn = Callable[[str], dict]
_IDENT = re.compile(r"^[A-Za-z_][\w\-]*$")


class SyncReport(TypedDict, total=False):
    status: str
    changes: list[dict]
    counts: dict[str, object]
    warnings: list[str]
    snapshot: dict[str, object]


def _parse_json(raw: object) -> object:
    """``raw`` as parsed JSON (None when absent or not JSON); dicts and lists pass through."""
    if raw is None or raw == "":
        return None
    if isinstance(raw, (dict, list)):
        return raw
    try:
        parsed: object = json.loads(str(raw))
    except ValueError:
        return None
    return parsed


def scope_rows(raw: object) -> list[dict]:
    """The scope table from JSON (``{rows}`` or a list) or CSV text; ``[]`` when absent."""
    if isinstance(raw, str) and raw.lstrip().startswith("object_type"):
        return list(csv.DictReader(io.StringIO(raw)))
    value = _parse_json(raw)
    if isinstance(value, dict):
        value = value.get("rows")
    return [r for r in value if isinstance(r, dict)] if isinstance(value, list) else []


def _table_name(full_name: str) -> str | None:
    parts = split_full_name(full_name)
    if not parts or not all(_IDENT.match(p) for p in parts):
        return None
    return ".".join(f"`{p}`" for p in parts)


def load_previous(sql_fn: SqlFn, table: str, dataset_id: str) -> Optional[dict]:
    """The newest stored snapshot of ``dataset_id`` (creates the table on first use)."""
    sql_fn(
        f"CREATE TABLE IF NOT EXISTS {table} (dataset_id STRING, report_id STRING, "
        "taken_at_utc STRING, snapshot STRING) COMMENT 'Power BI semantic snapshots (Kasal UCMV drift monitor)'"
    )
    res = sql_fn(
        f"SELECT snapshot FROM {table} WHERE dataset_id = {_sql_str_literal(dataset_id)} "
        "ORDER BY taken_at_utc DESC LIMIT 1"
    )
    if not res.get("success"):
        raise RuntimeError(res.get("error") or "reading the snapshot table failed")
    rows = (res.get("data") or {}).get("result", {}).get("data_array") or []
    if not rows or not rows[0] or not rows[0][0]:
        return None
    snapshot = _parse_json(rows[0][0])
    return snapshot if isinstance(snapshot, dict) else None


def store(
    sql_fn: SqlFn, table: str, dataset_id: str, report_id: str, snapshot: dict
) -> Optional[str]:
    res = sql_fn(
        f"INSERT INTO {table} VALUES ({_sql_str_literal(dataset_id)}, {_sql_str_literal(report_id)}, "
        f"{_sql_str_literal(snapshot.get('taken_at_utc', ''))}, {_sql_str_literal(json.dumps(snapshot))})"
    )
    return None if res.get("success") else str(res.get("error") or "insert failed")


def _deployed_specs(drift_report: Mapping[str, object]) -> dict[str, dict]:
    """view name -> parsed deployed spec, from the measure drift check's baseline YAML."""
    specs = {}
    views = drift_report.get("views")
    for view in views if isinstance(views, list) else []:
        if not isinstance(view, dict):
            continue
        try:
            spec = yaml.safe_load(view.get("baseline_yaml") or "") or {}
        except yaml.YAMLError:
            continue
        if isinstance(spec, dict) and view.get("name"):
            specs[str(view["name"])] = spec
    return specs


def run_semantic_sync(
    current: Mapping[str, object] | None,
    *,
    baseline: object = None,
    scope: object = None,
    draft_scope: object = None,
    ignore: object = None,
    drift_report: Mapping[str, object] | None = None,
    complete_fn: Optional[CompleteFn] = None,
    sql_fn: Optional[SqlFn] = None,
    snapshot_table: str = "",
    dataset_id: str = "",
    report_id: str = "",
    max_proposals: int = MAX_PROPOSALS,
) -> SyncReport:
    """``{status, changes, counts, warnings}``; ``status`` is ``compared``, ``baseline_stored``,
    ``no_baseline`` or ``skipped``."""
    out: SyncReport = {"status": "skipped", "changes": [], "counts": {}, "warnings": []}
    if not current or not current.get("objects"):
        out["warnings"].append(
            "no semantic snapshot in the extraction (TMDL not available)"
        )
        return out
    table = _table_name(snapshot_table) if snapshot_table else None
    if snapshot_table and not table:
        out["warnings"].append(
            f"semantic_snapshot_table is not a catalog.schema.table name: {snapshot_table!r}"
        )
    parsed = _parse_json(baseline)
    previous: dict | None = parsed if isinstance(parsed, dict) else None
    if previous is None and table and sql_fn is not None:
        try:
            previous = load_previous(sql_fn, table, dataset_id)
        except RuntimeError as exc:
            out["warnings"].append(f"could not read {snapshot_table}: {exc}")
    try:
        given = _parse_json(ignore)
        rules = ignore_rules(
            [r for r in given if isinstance(r, dict)] if isinstance(given, list) else []
        )
    except ValueError as exc:
        out["warnings"].append(str(exc))
        rules = {}

    draft = scope_rows(draft_scope)
    signed_off = scope_rows(scope)
    if not signed_off and draft:
        out["warnings"].append(
            "no signed-off report_scope given: changes are routed by today's scope draft"
        )
    changes: list[dict] = (
        with_ignores(
            with_impact(compare(previous, current), signed_off or draft, draft), rules
        )
        if isinstance(previous, dict)
        else []
    )
    if changes and complete_fn is not None:
        propose(
            changes, _deployed_specs(drift_report or {}), complete_fn, max_proposals
        )
    if table and sql_fn is not None and (previous is None or changes):
        error = store(sql_fn, table, dataset_id, report_id, dict(current))
        if error:
            out["warnings"].append(
                f"could not store the snapshot in {snapshot_table}: {error}"
            )

    out["status"] = (
        "compared"
        if isinstance(previous, dict)
        else ("baseline_stored" if table else "no_baseline")
    )
    if (
        not table
    ):  # nowhere to keep it: hand it back, to pass as semantic_baseline next time
        out["snapshot"] = dict(current)
    out["changes"] = changes
    out["counts"] = {
        "changes": len(changes),
        "needing_action": sum(
            1 for c in changes if not str(c.get("action", "")).startswith("none")
        ),
        "by_object_type": dict(Counter(c["object_type"] for c in changes)),
        "proposals": dict(
            Counter(
                str(c["proposal"].get("verdict")) for c in changes if c.get("proposal")
            )
        ),
    }
    return out
