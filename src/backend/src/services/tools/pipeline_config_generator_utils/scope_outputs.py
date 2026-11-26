"""The scope and semantic-sync outputs of one Pipeline Config Generator run.

Both are built from the TMDL and PBIR parts the run already fetched
(``services/powerbi/scope``), fail-open: a failure here never costs the config.

- ``report_scope``: the reachability-based scope table plus filters, page scopes,
  links, related reports and the table inventory. The per-measure page filters
  are also written into the config as ``page_filters_by_measure``, so the UC
  Metric View Generator's reconciliation mapping drafts carry them as
  ``extra_filter``.
- ``semantic_snapshot``: every semantic object, hashed, plus the rows of the
  ``tracked_tables`` the caller named (small tables whose DATA carries meaning,
  e.g. a KPI status list) — read with DAX ``EVALUATE``; fractional numbers are
  never stored.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable, Mapping

import requests

from src.services.powerbi.scope.parts import arr

logger = logging.getLogger(__name__)

# A tracked table is a small status / mapping list, never a fact.
MAX_TRACKED_ROWS = 2000

DaxFn = Callable[[str], requests.Response]  # DAX query -> executeQueries response


def parse_tracked_tables(raw: object) -> list[str]:
    """A JSON list, or comma / newline separated table names."""
    if not raw:
        return []
    if isinstance(raw, list):
        return [str(t).strip() for t in raw if str(t).strip()]
    text = str(raw).strip()
    if text.startswith("["):
        try:
            value = json.loads(text)
        except ValueError:
            value = []
        if isinstance(value, list):
            return [str(t).strip() for t in value if str(t).strip()]
    return [t.strip() for t in text.replace("\n", ",").split(",") if t.strip()]


def _dax_table(name: str) -> str:
    return "'" + name.replace("'", "''") + "'"


def tracked_rows(tables: list[str], dax_fn: DaxFn, warnings: list[str]) -> dict[str, list[Mapping[str, object]]]:
    """table -> rows of ``EVALUATE TOPN(n, '<table>')``; a failing table is a warning, not an error."""
    found: dict[str, list[Mapping[str, object]]] = {}
    for table in tables:
        try:
            response = dax_fn(f"EVALUATE TOPN({MAX_TRACKED_ROWS}, {_dax_table(table)})")
            status = response.status_code
            body: object = response.json() if status == 200 else None
        except (OSError, ValueError, RuntimeError, requests.RequestException) as exc:
            warnings.append(f"tracked table {table!r} not read: {exc}")
            continue
        rows = _result_rows(body)
        if rows is None:
            warnings.append(f"tracked table {table!r} not read (HTTP {status})")
            continue
        found[table] = rows
    return found


def _result_rows(body: object) -> list[Mapping[str, object]] | None:
    results = arr(body, "results")
    tables = arr(results[0], "tables") if results else []
    if not tables or not isinstance(tables[0], dict) or not isinstance(tables[0].get("rows"), list):
        return None
    return [r for r in arr(tables[0], "rows") if isinstance(r, dict)]


def report_scope(
    tmdl_parts: list[dict],
    report_def: Mapping[str, object] | None,
    relationships: list[dict],
    config: dict,
    *,
    scan_result: Mapping[str, object] | None = None,
    dataset_id: str = "",
    report_id: str = "",
) -> dict[str, object]:
    """The draft scope (``{}`` without both parts); page filters per measure go into ``config``."""
    if not tmdl_parts or not report_def:
        return {}
    try:
        from src.services.powerbi.scope import build_report_scope, relationships_from_rows

        rels = relationships_from_rows(relationships) if relationships else None
        scope = build_report_scope(
            tmdl_parts,
            report_def,
            relationships=rels,
            scan_result=scan_result,
            dataset_id=dataset_id,
            report_id=report_id,
        )
    except (AttributeError, KeyError, TypeError, ValueError) as exc:
        logger.warning("[PipelineConfigGen] Report scope failed: %s", exc, exc_info=True)
        return {}
    page_filters = scope.get("measure_page_filters")
    if page_filters:
        config["page_filters_by_measure"] = page_filters
    if scope:
        logger.info("[PipelineConfigGen]   → report scope: %s", scope.get("summary"))
    return scope


def semantic_snapshot(
    tmdl_parts: list[dict],
    report_def: Mapping[str, object] | None,
    relationships: list[dict],
    *,
    tracked: Mapping[str, list[Mapping[str, object]]] | None = None,
) -> dict[str, object]:
    """Every semantic object of the model (+ report, + tracked rows), hashed; ``{}`` without TMDL."""
    if not tmdl_parts:
        return {}
    try:
        from src.services.powerbi.scope import relationships_from_rows
        from src.services.powerbi.scope.semantic_sync import build_snapshot

        rels = relationships_from_rows(relationships) if relationships else None
        return build_snapshot(tmdl_parts, report_def, relationships=rels, tracked=tracked)
    except (AttributeError, KeyError, TypeError, ValueError) as exc:
        logger.warning("[PipelineConfigGen] Semantic snapshot failed: %s", exc, exc_info=True)
        return {}
