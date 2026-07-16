"""Tests for the UC warehouse query helper (uc_query.py).

Covers identifier safety, the SSRF allowlist, SELECT-DISTINCT SQL shaping, and the
row-parsing of the statement API — all without hitting a real warehouse (httpx is
patched).
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.engines.crewai.tools.custom.metric_view_utils import uc_query as U


class TestIdentifierSafety:
    def test_quote_qualified_identifier(self):
        assert U._quote_ident("cat.sch.tbl") == "`cat`.`sch`.`tbl`"

    def test_quote_single_identifier(self):
        assert U._quote_ident("col") == "`col`"

    def test_rejects_injection(self):
        with pytest.raises(U.UCQueryError):
            U._quote_ident("cat; DROP TABLE x")

    def test_rejects_space(self):
        with pytest.raises(U.UCQueryError):
            U._quote_ident("bad name")


class TestHostAllowlist:
    def test_databricks_host_allowed(self):
        assert U._host_allowed("https://foo.cloud.databricks.com") is True
        assert U._host_allowed("https://bar.azuredatabricks.net") is True

    def test_arbitrary_host_rejected(self):
        assert U._host_allowed("https://evil.example.com") is False
        assert U._host_allowed("http://localhost:8080") is False


class TestSelectDistinctSql:
    """select_distinct builds safe SQL and flattens the first column into values."""

    @pytest.mark.asyncio
    async def test_builds_sql_and_flattens_values(self):
        captured = {}

        async def fake_run_query(sql, warehouse_id=None, host_override=None, _resolved=None):
            captured["sql"] = sql
            return {"success": True, "columns": ["bic_cwc_type"],
                    "rows": [["APET"], ["CAN"], ["PET"]]}

        with patch.object(U, "run_query", side_effect=fake_run_query):
            res = await U.select_distinct(
                "cat.sch.dim", "bic_cwc_type", where="cwc_filter = 1",
                _resolved=("https://x.cloud.databricks.com", "wh1", {}))
        assert res["values"] == ["APET", "CAN", "PET"]
        assert "SELECT DISTINCT `bic_cwc_type` FROM `cat`.`sch`.`dim`" in captured["sql"]
        assert "WHERE cwc_filter = 1" in captured["sql"]
        assert "ORDER BY `bic_cwc_type`" in captured["sql"] and "LIMIT" in captured["sql"]

    @pytest.mark.asyncio
    async def test_unsafe_table_returns_error_not_raises(self):
        res = await U.select_distinct("cat; DROP", "col")
        assert res["success"] is False
        assert "unsafe identifier" in res["error"]

    @pytest.mark.asyncio
    async def test_query_failure_propagates_without_values(self):
        with patch.object(U, "run_query",
                          AsyncMock(return_value={"success": False, "error": "boom"})):
            res = await U.select_distinct("a.b.c", "col",
                                          _resolved=("https://x.cloud.databricks.com", "w", {}))
        assert res["success"] is False and "values" not in res


class TestRunQuery:
    """run_query parses the statement API's manifest + data_array; never raises."""

    def _resp(self, payload):
        r = MagicMock()
        r.raise_for_status = MagicMock()
        r.json.return_value = payload
        return r

    @pytest.mark.asyncio
    async def test_success_parses_columns_and_rows(self):
        success = {
            "statement_id": "s1",
            "status": {"state": "SUCCEEDED"},
            "manifest": {"schema": {"columns": [{"name": "c1"}, {"name": "c2"}]}},
            "result": {"data_array": [["a", "1"], ["b", "2"]]},
        }
        client = AsyncMock()
        client.post = AsyncMock(return_value=self._resp(success))
        cm = MagicMock()
        cm.__aenter__ = AsyncMock(return_value=client)
        cm.__aexit__ = AsyncMock(return_value=False)
        with patch("httpx.AsyncClient", return_value=cm):
            res = await U.run_query("SELECT 1",
                                    _resolved=("https://x.cloud.databricks.com", "wh", {}))
        assert res == {"success": True, "columns": ["c1", "c2"],
                       "rows": [["a", "1"], ["b", "2"]]}

    @pytest.mark.asyncio
    async def test_failed_state_returns_error(self):
        failed = {"statement_id": "s1",
                  "status": {"state": "FAILED", "error": {"message": "bad sql"}}}
        client = AsyncMock()
        client.post = AsyncMock(return_value=self._resp(failed))
        cm = MagicMock()
        cm.__aenter__ = AsyncMock(return_value=client)
        cm.__aexit__ = AsyncMock(return_value=False)
        with patch("httpx.AsyncClient", return_value=cm):
            res = await U.run_query("SELECT bad",
                                    _resolved=("https://x.cloud.databricks.com", "wh", {}))
        assert res["success"] is False and res["error"] == "bad sql"

    @pytest.mark.asyncio
    async def test_resolve_failure_returns_error(self):
        # untrusted host during resolve → error dict, not an exception
        with patch.object(U, "resolve_workspace_and_warehouse",
                          AsyncMock(side_effect=U.UCQueryError("untrusted host: evil.com"))):
            res = await U.run_query("SELECT 1", warehouse_id="wh")
        assert res["success"] is False and "untrusted host" in res["error"]


class TestResolveWarehouse:
    @pytest.mark.asyncio
    async def test_parses_id_from_endpoint_url(self):
        with patch.object(U, "_auth_headers",
                          AsyncMock(return_value=("https://x.cloud.databricks.com", {}))):
            ws, wid, _ = await U.resolve_workspace_and_warehouse(
                "https://x.cloud.databricks.com/sql/1.0/warehouses/abc123")
        assert wid == "abc123"

    @pytest.mark.asyncio
    async def test_autopicks_running_warehouse(self):
        client = AsyncMock()
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        resp.json.return_value = {"warehouses": [
            {"id": "stopped1", "state": "STOPPED"},
            {"id": "run1", "state": "RUNNING"}]}
        client.get = AsyncMock(return_value=resp)
        cm = MagicMock()
        cm.__aenter__ = AsyncMock(return_value=client)
        cm.__aexit__ = AsyncMock(return_value=False)
        with patch.object(U, "_auth_headers",
                          AsyncMock(return_value=("https://x.cloud.databricks.com", {}))), \
             patch("httpx.AsyncClient", return_value=cm):
            ws, wid, _ = await U.resolve_workspace_and_warehouse()
        assert wid == "run1"


class TestObOTokenFromContext:
    """_auth_headers fetches the OBO token from UserContext so warehouse queries
    authenticate on-behalf-of the user (get_auth_context skips OBO when token=None)."""

    @pytest.mark.asyncio
    async def test_uses_user_context_token(self):
        captured = {}

        async def fake_gac(user_token=None, **kw):
            captured["user_token"] = user_token
            m = MagicMock()
            m.workspace_url = "https://x.cloud.databricks.com"
            m.get_headers = MagicMock(return_value={"Authorization": "Bearer y"})
            return m

        with patch("src.utils.databricks_auth.get_auth_context", side_effect=fake_gac), \
             patch("src.utils.user_context.UserContext.get_user_token", return_value="obo-xyz"):
            ws, headers = await U._auth_headers()
        assert captured["user_token"] == "obo-xyz"
        assert ws == "https://x.cloud.databricks.com"

    @pytest.mark.asyncio
    async def test_falls_back_to_group_context_token(self):
        captured = {}

        async def fake_gac(user_token=None, **kw):
            captured["user_token"] = user_token
            m = MagicMock()
            m.workspace_url = "https://x.cloud.databricks.com"
            m.get_headers = MagicMock(return_value={})
            return m

        gc = MagicMock()
        gc.access_token = "grp-token"
        with patch("src.utils.databricks_auth.get_auth_context", side_effect=fake_gac), \
             patch("src.utils.user_context.UserContext.get_user_token", return_value=None), \
             patch("src.utils.user_context.UserContext.get_group_context", return_value=gc):
            await U._auth_headers()
        assert captured["user_token"] == "grp-token"
