"""Per-column default-aggregation metadata (PBI's ``SummarizeBy``), via DAX.

Split out as its own module (not appended to ``pipeline_config.py``, which is
over the file-size ceiling) — see that module's docstring for why sibling
modules in this package have no dependency on it.

Why this exists: a PBI report author can drag a raw numeric COLUMN straight
into a visual's Values well with no named measure at all — PBI implicitly
aggregates it using the column's own ``SummarizeBy`` setting (Sum by default,
but can be Average/Count/Min/Max/DistinctCount). Kasal's own visual-usage
extraction (``visual_usage.py``, ported from the reconciliation framework's
``pbi_visual_usage.py``) already records these as ``Column`` field refs
alongside ``Measure`` ones, so the DATA is there — this module supplies the
missing piece needed to act on it: which columns are even aggregatable, and
by what default. Neither the Admin Scanner's schema (``parse_admin_tables`` —
confirmed empirically to omit ``summarizeBy`` entirely) nor any other tier
already captures this; it only exists via a live DAX query against the
model, exactly as the reconciliation framework's own
``mapping_candidates.py`` (``raw_column_candidates``) does with
``INFO.VIEW.COLUMNS()``.
"""

from __future__ import annotations

import requests


def _headers(token: str) -> dict[str, str]:
    """Kept identical to (and independently of) ``pipeline_config._headers``."""
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def _row_get(row: dict, col: str, default: str = "") -> str:
    """Kept identical to (and independently of) ``pipeline_config._row_get``
    — tolerant of the Execute Queries API's bracketed-or-not column keys."""
    for key in (f"[{col}]", col):
        if key in row and row[key] is not None:
            return row[key]
    return default


def extract_column_summarize_by(
    token: str, workspace_id: str, dataset_id: str
) -> dict[str, dict[str, str]]:
    """``{table_name: {column_name: summarize_by}}`` via ``EVALUATE
    INFO.VIEW.COLUMNS()``.

    Uses the SAME Execute Queries endpoint and token as
    ``pipeline_config.extract_measures`` — the SA token that works for DAX/DMV
    queries there works here too (confirmed live), no new auth tier needed.
    ``summarize_by`` is PBI's literal setting: ``"Sum"``, ``"Average"``,
    ``"Count"``, ``"Min"``, ``"Max"``, ``"DistinctCount"``, or ``"None"``
    (not aggregatable — most text/key columns). Fail-open: an empty dict on
    any error, since this only enables an ADDITIONAL measure-coverage feature
    and must never block the rest of extraction.
    """
    url = (
        f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}"
        f"/datasets/{dataset_id}/executeQueries"
    )
    query = (
        "EVALUATE SELECTCOLUMNS(INFO.VIEW.COLUMNS(), "
        '"Table", [Table], "Column", [Name], "SummarizeBy", [SummarizeBy])'
    )
    body = {
        "queries": [{"query": query}],
        "serializerSettings": {"includeNulls": True},
    }
    try:
        resp = requests.post(url, headers=_headers(token), json=body, timeout=60)
        if resp.status_code != 200:
            return {}
        data = resp.json()
        rows = data.get("results", [{}])[0].get("tables", [{}])[0].get("rows", [])
    except Exception:
        return {}

    out: dict[str, dict[str, str]] = {}
    for row in rows:
        table = _row_get(row, "Table")
        column = _row_get(row, "Column")
        summarize_by = _row_get(row, "SummarizeBy")
        if not table or not column:
            continue
        out.setdefault(table, {})[column] = summarize_by or "None"
    return out
