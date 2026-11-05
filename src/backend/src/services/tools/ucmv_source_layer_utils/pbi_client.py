"""Power BI semantic-model access: DAX executeQueries and refresh history.

Synchronous (httpx.Client), meant to run in a worker thread. The base URL is
fixed here; no caller supplies a host. Result rows are returned with their
DAX keys cleaned (``Table[Col]`` -> ``Col``).
"""

from __future__ import annotations

import re

import httpx

from src.services.tools.ucmv_source_layer_utils.inputs import check_uuid

PBI_API_BASE = "https://api.powerbi.com/v1.0/myorg"
#: executeQueries returns at most this many rows (silently cut beyond it).
DAX_ROW_LIMIT = 100_000
#: ...and at most this many values (rows x columns); also cut silently.
DAX_VALUE_LIMIT = 1_000_000
_TIMEOUT = 120.0
_BARE_KEY = re.compile(r"\[([^\[\]]+)\]$")


class PbiError(RuntimeError):
    """A Power BI request failed; the message never includes result data."""


def bare_key(key: str) -> str:
    """``'Table[Col]'`` and ``'[Measure]'`` -> ``'Col'`` / ``'Measure'``."""
    match = _BARE_KEY.search(key)
    return match.group(1) if match else key


def clean_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    return [{bare_key(str(k)): v for k, v in row.items()} for row in rows]


def dax_table(name: str) -> str:
    """A DAX table reference: ``'Name with spaces'``."""
    return "'" + name.replace("'", "''") + "'"


def dax_column(table: str, column: str) -> str:
    return f"{dax_table(table)}[{column.replace(']', ']]')}]"


def dax_value(value: object) -> str:
    """A DAX literal for a page value (the comparison side of a filter)."""
    if value is None:
        return "BLANK()"
    if isinstance(value, bool):
        return "TRUE()" if value else "FALSE()"
    if isinstance(value, (int, float)):
        return repr(value)
    return '"' + str(value).replace('"', '""') + '"'


class PbiClient:
    """One semantic model, read with the caller's Power BI token."""

    def __init__(self, token: str, workspace_id: str, dataset_id: str) -> None:
        self._workspace = check_uuid(workspace_id, "workspace_id")
        self._dataset = check_uuid(dataset_id, "dataset_id")
        self._headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        self._base = f"{PBI_API_BASE}/groups/{self._workspace}/datasets/{self._dataset}"

    def dax(self, query: str) -> list[dict[str, object]]:
        """Run one DAX query; the first table's rows, keys cleaned."""
        payload = {
            "queries": [{"query": query}],
            "serializerSettings": {"includeNulls": True},
        }
        with httpx.Client(timeout=_TIMEOUT) as client:
            response = client.post(
                f"{self._base}/executeQueries", headers=self._headers, json=payload
            )
        if response.status_code >= 400:
            raise PbiError(f"executeQueries failed: HTTP {response.status_code}")
        data = response.json()
        if isinstance(data, dict) and "error" in data:
            message = (
                data["error"].get("message")
                if isinstance(data["error"], dict)
                else None
            )
            raise PbiError(f"executeQueries failed: {message or 'unknown error'}")
        results = data.get("results") or [{}] if isinstance(data, dict) else [{}]
        tables = (results[0] or {}).get("tables") or []
        rows = tables[0].get("rows") if tables else []
        return clean_rows(rows or [])

    def last_completed_refresh(self) -> str | None:
        """End time (UTC text) of the most recent Completed refresh, or None.

        Reads the newest refresh first (``$top=1``); when that one is still
        running or failed, looks further back for the last Completed one.
        """
        newest = self._refreshes(1)
        if newest and newest[0].get("status") == "Completed":
            return _end_time(newest[0])
        completed = [r for r in self._refreshes(20) if r.get("status") == "Completed"]
        if not completed:
            return None
        return max((_end_time(r) or "") for r in completed) or None

    def _refreshes(self, top: int) -> list[dict[str, object]]:
        with httpx.Client(timeout=_TIMEOUT) as client:
            response = client.get(
                f"{self._base}/refreshes", headers=self._headers, params={"$top": top}
            )
        if response.status_code >= 400:
            raise PbiError(f"refresh history failed: HTTP {response.status_code}")
        value = response.json().get("value") or []
        return [r for r in value if isinstance(r, dict)]


def _end_time(refresh: dict[str, object]) -> str | None:
    value = refresh.get("endTime") or refresh.get("startTime")
    return str(value) if value else None
