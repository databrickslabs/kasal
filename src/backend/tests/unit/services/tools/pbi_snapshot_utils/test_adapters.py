"""The real-executor adapters, with the network calls replaced."""

import pytest

from src.services.tools.pbi_snapshot_utils import adapters


class _FakeClient:
    def __init__(self):
        self.calls = []

    def dax(self, query):
        self.calls.append(query)
        return [{"Code": 1}]


async def test_pbi_query_runs_the_client_off_the_loop():
    client = _FakeClient()
    pbi_query = adapters.pbi_query_from_client(client)
    rows = await pbi_query("EVALUATE 'T'")
    assert rows == [{"Code": 1}]
    assert client.calls == ["EVALUATE 'T'"]


async def test_sql_run_uses_the_resolved_warehouse_and_returns_rows(monkeypatch):
    seen = {}

    async def fake_resolve(warehouse_id=None, host_override=None):
        seen["warehouse"] = warehouse_id
        return ("https://example.com", "wh-1", {"User-Agent": "kasal"})

    async def fake_run_query(sql, _resolved=None, **_):
        seen["resolved"] = _resolved
        return {"success": True, "columns": [], "rows": [["7"]]}

    monkeypatch.setattr(adapters, "resolve_workspace_and_warehouse", fake_resolve)
    monkeypatch.setattr(adapters, "run_query", fake_run_query)

    run_sql = await adapters.sql_run_for_warehouse("wh-1")
    assert await run_sql("SELECT 7") == [["7"]]
    assert seen["warehouse"] == "wh-1"
    assert seen["resolved"][1] == "wh-1"


async def test_sql_run_raises_on_a_failed_statement(monkeypatch):
    async def fake_resolve(warehouse_id=None, host_override=None):
        return ("https://example.com", "wh-1", {})

    async def fake_run_query(sql, _resolved=None, **_):
        return {"success": False, "error": "table is locked"}

    monkeypatch.setattr(adapters, "resolve_workspace_and_warehouse", fake_resolve)
    monkeypatch.setattr(adapters, "run_query", fake_run_query)

    run_sql = await adapters.sql_run_for_warehouse(None)
    with pytest.raises(adapters.SqlRunError, match="table is locked"):
        await run_sql("DROP TABLE x")
