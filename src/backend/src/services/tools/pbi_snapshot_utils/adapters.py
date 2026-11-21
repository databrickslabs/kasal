"""Real executors for the snapshot runner, built on the existing helpers.

* Power BI reads go through ``PbiClient`` (``ucmv_source_layer_utils``), which
  carries the caller's token and the executeQueries contract. It is synchronous,
  so each call runs in a worker thread and the event loop is not blocked.
* Databricks SQL goes through ``uc_query.run_query``, which attaches the Kasal
  User-Agent (telemetry rule in ``src/backend/CLAUDE.md``).
"""

from __future__ import annotations

import asyncio

from src.services.tools.metric_view_utils.uc_query import (
    resolve_workspace_and_warehouse,
    run_query,
)
from src.services.tools.pbi_snapshot_utils.snapshot_runner import PbiQueryFn
from src.services.tools.pbi_snapshot_utils.sql_writer import SqlRunFn
from src.services.tools.ucmv_source_layer_utils.pbi_client import PbiClient


class SqlRunError(RuntimeError):
    """A Databricks statement failed; the message is the warehouse's short error."""


def pbi_query_from_client(client: PbiClient) -> PbiQueryFn:
    """``pbi_query`` over an existing ``PbiClient``, run off the event loop."""

    async def pbi_query(dax: str) -> list[dict[str, object]]:
        return await asyncio.to_thread(client.dax, dax)

    return pbi_query


async def sql_run_for_warehouse(warehouse_id: str | None) -> SqlRunFn:
    """``run_sql`` on one resolved warehouse. Resolution happens once, here.

    Raises ``UCQueryError`` when no workspace or warehouse can be resolved.
    """
    resolved = await resolve_workspace_and_warehouse(warehouse_id)

    async def run_sql(sql: str) -> list[list[object]]:
        result = await run_query(sql, _resolved=resolved)
        if not result.get("success"):
            raise SqlRunError(str(result.get("error") or "statement failed")[:300])
        return [list(row) for row in result.get("rows") or []]

    return run_sql
