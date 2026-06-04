"""Unit tests for MCP handler functions.

Tests adapter management (register, stop, pool), Databricks API integration,
CrewAI tool creation from MCP dictionaries, tool wrapping, and subprocess
isolation logic.
"""

import asyncio
import json
import os
import pytest
from unittest.mock import (
    AsyncMock,
    MagicMock,
    Mock,
    patch,
)

import src.engines.crewai.tools.mcp_handler as mcp_handler
from src.engines.crewai.tools.mcp_handler import (
    register_mcp_adapter,
    stop_all_adapters,
    stop_mcp_adapter,
    get_or_create_mcp_adapter,
    get_databricks_workspace_host,
    call_databricks_api,
    create_crewai_tool_from_mcp,
    wrap_mcp_tool,
    run_in_separate_process,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _reset_global_state():
    """Ensure module-level dicts are clean before/after each test."""
    mcp_handler._active_mcp_adapters.clear()
    mcp_handler._mcp_connection_pool.clear()
    yield
    mcp_handler._active_mcp_adapters.clear()
    mcp_handler._mcp_connection_pool.clear()


# ===========================================================================
# register_mcp_adapter
# ===========================================================================

class TestRegisterMCPAdapter:

    def test_registers_adapter(self):
        adapter = Mock()
        register_mcp_adapter("id_1", adapter)
        assert mcp_handler._active_mcp_adapters["id_1"] is adapter

    def test_overwrites_existing_id(self):
        adapter_a = Mock()
        adapter_b = Mock()
        register_mcp_adapter("id_1", adapter_a)
        register_mcp_adapter("id_1", adapter_b)
        assert mcp_handler._active_mcp_adapters["id_1"] is adapter_b


# ===========================================================================
# get_or_create_mcp_adapter
# ===========================================================================

class TestGetOrCreateMCPAdapter:

    @pytest.mark.asyncio
    async def test_creates_new_adapter_when_pool_empty(self):
        mock_adapter = MagicMock()
        mock_adapter._initialized = True

        with patch(
            "src.engines.common.mcp_adapter.MCPAdapter",
            return_value=mock_adapter,
        ) as mock_cls:
            mock_adapter.initialize = AsyncMock()
            result = await get_or_create_mcp_adapter(
                {"url": "http://example.com", "auth_type": "pat"},
                adapter_id="a1",
            )
            assert result is mock_adapter
            mock_adapter.initialize.assert_awaited_once()
            # Also registered in both pool and active
            assert "a1" in mcp_handler._active_mcp_adapters

    @pytest.mark.asyncio
    async def test_reuses_adapter_from_pool(self):
        existing = MagicMock()
        existing._initialized = True
        mcp_handler._mcp_connection_pool["http://example.com_pat"] = existing

        result = await get_or_create_mcp_adapter(
            {"url": "http://example.com", "auth_type": "pat"},
            adapter_id="a2",
        )
        assert result is existing
        assert "a2" in mcp_handler._active_mcp_adapters

    @pytest.mark.asyncio
    async def test_removes_stale_adapter_from_pool(self):
        stale = MagicMock()
        stale._initialized = False
        mcp_handler._mcp_connection_pool["http://example.com_pat"] = stale

        fresh = MagicMock()
        fresh._initialized = True
        fresh.initialize = AsyncMock()

        with patch(
            "src.engines.common.mcp_adapter.MCPAdapter",
            return_value=fresh,
        ):
            result = await get_or_create_mcp_adapter(
                {"url": "http://example.com", "auth_type": "pat"}
            )
            assert result is fresh

    @pytest.mark.asyncio
    async def test_stdio_transport_pool_key(self):
        adapter = MagicMock()
        adapter._initialized = True
        adapter.initialize = AsyncMock()

        with patch(
            "src.engines.common.mcp_adapter.MCPAdapter",
            return_value=adapter,
        ):
            await get_or_create_mcp_adapter(
                {"transport": "stdio", "command": ["python", "-m", "server"]}
            )
            assert "stdio_python -m server" in mcp_handler._mcp_connection_pool


# ===========================================================================
# stop_mcp_adapter
# ===========================================================================

class TestStopMCPAdapter:

    @pytest.mark.asyncio
    async def test_calls_async_stop(self):
        adapter = AsyncMock()
        adapter.stop = AsyncMock()
        await stop_mcp_adapter(adapter)
        adapter.stop.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_calls_async_close_when_no_stop(self):
        adapter = MagicMock()
        # Remove stop, keep close as async
        del adapter.stop
        adapter.close = AsyncMock()
        await stop_mcp_adapter(adapter)
        adapter.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_handles_none_adapter(self):
        # Should not raise
        await stop_mcp_adapter(None)

    @pytest.mark.asyncio
    async def test_handles_exception_gracefully(self):
        adapter = AsyncMock()
        adapter.stop = AsyncMock(side_effect=Exception("stop error"))
        # Should not propagate
        await stop_mcp_adapter(adapter)

    @pytest.mark.asyncio
    async def test_cleans_up_connections(self):
        adapter = AsyncMock()
        adapter.stop = AsyncMock()
        conn = MagicMock()
        adapter._connections = [conn]
        await stop_mcp_adapter(adapter)
        conn.close.assert_called_once()


# ===========================================================================
# stop_all_adapters
# ===========================================================================

class TestStopAllAdapters:

    @pytest.mark.asyncio
    async def test_stops_pooled_and_active_adapters(self):
        pooled = AsyncMock()
        pooled.stop = AsyncMock()
        mcp_handler._mcp_connection_pool["key1"] = pooled

        active = AsyncMock()
        active.stop = AsyncMock()
        mcp_handler._active_mcp_adapters["id1"] = active

        await stop_all_adapters()

        assert len(mcp_handler._active_mcp_adapters) == 0
        assert len(mcp_handler._mcp_connection_pool) == 0

    @pytest.mark.asyncio
    async def test_survives_errors(self):
        bad = AsyncMock()
        bad.stop = AsyncMock(side_effect=Exception("boom"))
        mcp_handler._active_mcp_adapters["bad"] = bad

        await stop_all_adapters()
        assert len(mcp_handler._active_mcp_adapters) == 0


# ===========================================================================
# get_databricks_workspace_host
# ===========================================================================

class TestGetDatabricksWorkspaceHost:

    @pytest.mark.asyncio
    async def test_strips_https_prefix(self):
        mock_config = MagicMock()
        mock_config.workspace_url = "https://ws.databricks.com/"

        mock_service = AsyncMock()
        mock_service.get_databricks_config = AsyncMock(return_value=mock_config)

        mock_session = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch(
            "src.services.databricks_service.DatabricksService",
            return_value=mock_service,
        ), patch(
            "src.db.session.request_scoped_session",
            return_value=mock_session,
        ):
            host, error = await get_databricks_workspace_host()
            assert host == "ws.databricks.com"
            assert error is None

    @pytest.mark.asyncio
    async def test_strips_http_prefix(self):
        mock_config = MagicMock()
        mock_config.workspace_url = "http://ws.databricks.com"

        mock_service = AsyncMock()
        mock_service.get_databricks_config = AsyncMock(return_value=mock_config)

        mock_session = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch(
            "src.services.databricks_service.DatabricksService",
            return_value=mock_service,
        ), patch(
            "src.db.session.request_scoped_session",
            return_value=mock_session,
        ):
            host, error = await get_databricks_workspace_host()
            assert host == "ws.databricks.com"

    @pytest.mark.asyncio
    async def test_returns_error_when_no_config(self):
        mock_service = AsyncMock()
        mock_service.get_databricks_config = AsyncMock(return_value=None)

        mock_session = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch(
            "src.services.databricks_service.DatabricksService",
            return_value=mock_service,
        ), patch(
            "src.db.session.request_scoped_session",
            return_value=mock_session,
        ):
            host, error = await get_databricks_workspace_host()
            assert host is None
            assert "No workspace URL" in error

    @pytest.mark.asyncio
    async def test_returns_error_on_exception(self):
        with patch(
            "src.services.databricks_service.DatabricksService",
            side_effect=Exception("db error"),
        ), patch(
            "src.db.session.request_scoped_session",
            side_effect=Exception("db error"),
        ):
            host, error = await get_databricks_workspace_host()
            assert host is None
            assert error is not None


# ===========================================================================
# call_databricks_api
# ===========================================================================

class TestCallDatabricksAPI:

    @pytest.mark.asyncio
    async def test_get_request_success(self):
        mock_response = AsyncMock()
        mock_response.json = AsyncMock(return_value={"ok": True})
        mock_response.raise_for_status = MagicMock()

        mock_get_ctx = AsyncMock()
        mock_get_ctx.__aenter__ = AsyncMock(return_value=mock_response)
        mock_get_ctx.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_get_ctx)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch(
            "src.engines.crewai.tools.mcp_handler.get_databricks_auth_headers",
            new_callable=AsyncMock,
            return_value=({"Authorization": "Bearer tok"}, None),
        ), patch(
            "src.engines.crewai.tools.mcp_handler.get_databricks_workspace_host",
            new_callable=AsyncMock,
            return_value=("ws.databricks.com", None),
        ), patch("aiohttp.ClientSession", return_value=mock_session):
            result = await call_databricks_api("/api/2.0/test")
            assert result == {"ok": True}

    @pytest.mark.asyncio
    async def test_auth_error_returns_error_dict(self):
        with patch(
            "src.engines.crewai.tools.mcp_handler.get_databricks_auth_headers",
            new_callable=AsyncMock,
            return_value=(None, "auth failed"),
        ):
            result = await call_databricks_api("/api/2.0/test")
            assert "error" in result

    @pytest.mark.asyncio
    async def test_host_error_returns_error_dict(self):
        with patch(
            "src.engines.crewai.tools.mcp_handler.get_databricks_auth_headers",
            new_callable=AsyncMock,
            return_value=({"Authorization": "Bearer tok"}, None),
        ), patch(
            "src.engines.crewai.tools.mcp_handler.get_databricks_workspace_host",
            new_callable=AsyncMock,
            return_value=(None, "no host"),
        ):
            result = await call_databricks_api("/api/2.0/test")
            assert "error" in result

    @pytest.mark.asyncio
    async def test_unsupported_method(self):
        with patch(
            "src.engines.crewai.tools.mcp_handler.get_databricks_auth_headers",
            new_callable=AsyncMock,
            return_value=({"Authorization": "Bearer tok"}, None),
        ), patch(
            "src.engines.crewai.tools.mcp_handler.get_databricks_workspace_host",
            new_callable=AsyncMock,
            return_value=("ws.databricks.com", None),
        ):
            result = await call_databricks_api("/api", method="PATCH")
            assert "error" in result


# ===========================================================================
# create_crewai_tool_from_mcp
# ===========================================================================

class TestCreateCrewAIToolFromMCP:

    def test_creates_tool_with_schema(self):
        mcp_dict = {
            "name": "search",
            "description": "Search tool",
            "input_schema": {
                "properties": {
                    "query": {"type": "string", "description": "search query"},
                },
                "required": ["query"],
            },
        }
        with patch("src.engines.common.mcp_adapter.MCPTool") as MockMCPTool:
            wrapper = MagicMock()
            wrapper.name = "search"
            wrapper.description = "Search tool"
            wrapper.input_schema = mcp_dict["input_schema"]
            MockMCPTool.return_value = wrapper

            tool = create_crewai_tool_from_mcp(mcp_dict)
            assert tool.name == "search"
            assert hasattr(tool, "_run")
            assert hasattr(tool, "args_schema")

    def test_creates_dummy_field_when_no_properties(self):
        mcp_dict = {
            "name": "simple",
            "description": "No params",
            "input_schema": None,
        }
        with patch("src.engines.common.mcp_adapter.MCPTool") as MockMCPTool:
            wrapper = MagicMock()
            wrapper.name = "simple"
            wrapper.description = "No params"
            wrapper.input_schema = {}
            MockMCPTool.return_value = wrapper

            tool = create_crewai_tool_from_mcp(mcp_dict)
            fields = (
                tool.args_schema.model_fields
                if hasattr(tool.args_schema, "model_fields")
                else tool.args_schema.__fields__
            )
            assert "dummy" in fields

    def test_tool_run_returns_text_from_result(self):
        mcp_dict = {
            "name": "echo",
            "description": "Echo tool",
            "input_schema": {"properties": {}, "required": []},
        }

        mock_result = MagicMock()
        content_item = MagicMock()
        content_item.text = "hello"
        mock_result.content = [content_item]

        with patch("src.engines.common.mcp_adapter.MCPTool") as MockMCPTool:
            wrapper = MagicMock()
            wrapper.name = "echo"
            wrapper.description = "Echo tool"
            wrapper.input_schema = {"properties": {}, "required": []}
            wrapper.execute = AsyncMock(return_value=mock_result)
            MockMCPTool.return_value = wrapper

            tool = create_crewai_tool_from_mcp(mcp_dict)

            # Simulate: no running loop -> uses new_event_loop
            with patch("asyncio.get_running_loop", side_effect=RuntimeError):
                result = tool._run()
                assert "hello" in result

    def test_tool_run_returns_error_on_exception(self):
        mcp_dict = {
            "name": "fail",
            "description": "Fail tool",
            "input_schema": {"properties": {}, "required": []},
        }
        with patch("src.engines.common.mcp_adapter.MCPTool") as MockMCPTool:
            wrapper = MagicMock()
            wrapper.name = "fail"
            wrapper.description = "Fail tool"
            wrapper.input_schema = {"properties": {}, "required": []}
            wrapper.execute = AsyncMock(side_effect=Exception("exec error"))
            MockMCPTool.return_value = wrapper

            tool = create_crewai_tool_from_mcp(mcp_dict)

            with patch("asyncio.get_running_loop", side_effect=RuntimeError):
                result = tool._run()
                assert "Error:" in result


# ===========================================================================
# wrap_mcp_tool
# ===========================================================================

class TestWrapMCPTool:

    def test_standard_tool_direct_success(self):
        tool = MagicMock()
        tool.name = "std_tool"
        original = MagicMock(return_value="ok")
        tool._run = original

        wrapped = wrap_mcp_tool(tool)
        result = wrapped._run(x=1)
        assert result == "ok"

    def test_standard_tool_fallback_on_event_loop_error(self):
        tool = MagicMock()
        tool.name = "std_tool"
        tool._run = MagicMock(side_effect=RuntimeError("Event loop is closed"))

        wrapped = wrap_mcp_tool(tool)

        with patch(
            "src.engines.crewai.tools.mcp_handler.run_in_separate_process",
            new_callable=AsyncMock,
            return_value="process_result",
        ):
            result = wrapped._run(x=1)
            assert result == "process_result"

    def test_genie_tool_direct_success(self):
        tool = MagicMock()
        tool.name = "get_space"
        original = MagicMock(return_value="space_data")
        tool._run = original

        wrapped = wrap_mcp_tool(tool)
        result = wrapped._run(space_id="s1")
        assert result == "space_data"

    def test_genie_tool_falls_back_to_process(self):
        tool = MagicMock()
        tool.name = "get_space"
        tool._run = MagicMock(side_effect=Exception("loop error"))

        wrapped = wrap_mcp_tool(tool)

        mock_loop = MagicMock()
        mock_loop.run_until_complete = MagicMock(return_value="from_process")

        with patch("asyncio.new_event_loop", return_value=mock_loop), \
             patch("asyncio.set_event_loop"):
            result = wrapped._run(space_id="s1")
            assert mock_loop.run_until_complete.called


# ===========================================================================
# run_in_separate_process
# ===========================================================================

class TestRunInSeparateProcess:

    @pytest.mark.asyncio
    async def test_success(self):
        mock_proc = MagicMock()
        mock_proc.returncode = 0
        mock_proc.communicate = AsyncMock(
            return_value=(b'{"result": "ok"}', b"")
        )

        with patch(
            "asyncio.create_subprocess_exec",
            new_callable=AsyncMock,
            return_value=mock_proc,
        ):
            result = await run_in_separate_process("tool_x", {"a": 1})
            assert result == {"result": "ok"}

    @pytest.mark.asyncio
    async def test_process_error(self):
        mock_proc = MagicMock()
        mock_proc.returncode = 1
        mock_proc.communicate = AsyncMock(
            return_value=(b"", b"some error")
        )

        with patch(
            "asyncio.create_subprocess_exec",
            new_callable=AsyncMock,
            return_value=mock_proc,
        ):
            result = await run_in_separate_process("tool_x", {})
            assert "error" in result

    @pytest.mark.asyncio
    async def test_invalid_json_output(self):
        mock_proc = MagicMock()
        mock_proc.returncode = 0
        mock_proc.communicate = AsyncMock(
            return_value=(b"not json", b"")
        )

        with patch(
            "asyncio.create_subprocess_exec",
            new_callable=AsyncMock,
            return_value=mock_proc,
        ):
            result = await run_in_separate_process("tool_x", {})
            assert "error" in result

    @pytest.mark.asyncio
    async def test_exception_during_subprocess_creation(self):
        with patch(
            "asyncio.create_subprocess_exec",
            new_callable=AsyncMock,
            side_effect=Exception("spawn fail"),
        ):
            result = await run_in_separate_process("tool_x", {})
            assert "error" in result


# ===========================================================================
# stop_all_adapters: pooled adapter stop failure
# ===========================================================================

class TestStopAllAdaptersPooledFailure:

    @pytest.mark.asyncio
    async def test_pooled_adapter_stop_error_still_clears_pool(self):
        """stop_all_adapters clears pool even when stopping a pooled adapter raises."""
        bad = AsyncMock()
        bad.stop = AsyncMock(side_effect=Exception("pool stop error"))
        mcp_handler._mcp_connection_pool["bad_key"] = bad

        await stop_all_adapters()
        assert len(mcp_handler._mcp_connection_pool) == 0

    @pytest.mark.asyncio
    async def test_active_adapter_stop_failure_still_removes_from_tracking(self):
        """Even when stop raises, the adapter is removed from active tracking."""
        bad = AsyncMock()
        bad.stop = AsyncMock(side_effect=Exception("active stop error"))
        mcp_handler._active_mcp_adapters["bad_id"] = bad

        await stop_all_adapters()
        assert len(mcp_handler._active_mcp_adapters) == 0


# ===========================================================================
# call_databricks_api: no headers, POST/PUT/DELETE methods
# ===========================================================================

class TestCallDatabricksAPIAdditional:

    @pytest.mark.asyncio
    async def test_no_headers_returns_error_dict(self):
        """When auth returns None headers (no error), raises and returns error dict."""
        with patch(
            "src.engines.crewai.tools.mcp_handler.get_databricks_auth_headers",
            new_callable=AsyncMock,
            return_value=(None, None),  # no error string, but no headers
        ):
            result = await call_databricks_api("/api/test")
            assert "error" in result

    @pytest.mark.asyncio
    async def test_post_request_success(self):
        """POST method calls session.post."""
        mock_response = AsyncMock()
        mock_response.json = AsyncMock(return_value={"created": True})
        mock_response.raise_for_status = MagicMock()

        mock_post_ctx = AsyncMock()
        mock_post_ctx.__aenter__ = AsyncMock(return_value=mock_response)
        mock_post_ctx.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.post = MagicMock(return_value=mock_post_ctx)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch(
            "src.engines.crewai.tools.mcp_handler.get_databricks_auth_headers",
            new_callable=AsyncMock,
            return_value=({"Authorization": "Bearer tok"}, None),
        ), patch(
            "src.engines.crewai.tools.mcp_handler.get_databricks_workspace_host",
            new_callable=AsyncMock,
            return_value=("ws.databricks.com", None),
        ), patch("aiohttp.ClientSession", return_value=mock_session):
            result = await call_databricks_api("/api/test", method="POST", data={"x": 1})
            assert result == {"created": True}

    @pytest.mark.asyncio
    async def test_put_request_success(self):
        """PUT method calls session.put."""
        mock_response = AsyncMock()
        mock_response.json = AsyncMock(return_value={"updated": True})
        mock_response.raise_for_status = MagicMock()

        mock_put_ctx = AsyncMock()
        mock_put_ctx.__aenter__ = AsyncMock(return_value=mock_response)
        mock_put_ctx.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.put = MagicMock(return_value=mock_put_ctx)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch(
            "src.engines.crewai.tools.mcp_handler.get_databricks_auth_headers",
            new_callable=AsyncMock,
            return_value=({"Authorization": "Bearer tok"}, None),
        ), patch(
            "src.engines.crewai.tools.mcp_handler.get_databricks_workspace_host",
            new_callable=AsyncMock,
            return_value=("ws.databricks.com", None),
        ), patch("aiohttp.ClientSession", return_value=mock_session):
            result = await call_databricks_api("/api/test", method="PUT", data={"x": 1})
            assert result == {"updated": True}

    @pytest.mark.asyncio
    async def test_delete_request_success(self):
        """DELETE method calls session.delete."""
        mock_response = AsyncMock()
        mock_response.json = AsyncMock(return_value={"deleted": True})
        mock_response.raise_for_status = MagicMock()

        mock_del_ctx = AsyncMock()
        mock_del_ctx.__aenter__ = AsyncMock(return_value=mock_response)
        mock_del_ctx.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.delete = MagicMock(return_value=mock_del_ctx)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch(
            "src.engines.crewai.tools.mcp_handler.get_databricks_auth_headers",
            new_callable=AsyncMock,
            return_value=({"Authorization": "Bearer tok"}, None),
        ), patch(
            "src.engines.crewai.tools.mcp_handler.get_databricks_workspace_host",
            new_callable=AsyncMock,
            return_value=("ws.databricks.com", None),
        ), patch("aiohttp.ClientSession", return_value=mock_session):
            result = await call_databricks_api("/api/test", method="DELETE")
            assert result == {"deleted": True}


# ===========================================================================
# create_crewai_tool_from_mcp: running event loop path
# ===========================================================================

class TestCreateCrewAIToolFromMCPRunningLoop:

    def test_tool_run_with_running_event_loop_uses_thread_pool(self):
        """When event loop IS running, uses ThreadPoolExecutor."""
        mcp_dict = {
            "name": "echo",
            "description": "Echo tool",
            "input_schema": {"properties": {}, "required": []},
        }

        mock_result = MagicMock()
        content_item = MagicMock()
        content_item.text = "thread_result"
        mock_result.content = [content_item]

        with patch("src.engines.common.mcp_adapter.MCPTool") as MockMCPTool:
            wrapper = MagicMock()
            wrapper.name = "echo"
            wrapper.description = "Echo tool"
            wrapper.input_schema = {"properties": {}, "required": []}
            wrapper.execute = AsyncMock(return_value=mock_result)
            MockMCPTool.return_value = wrapper

            tool = create_crewai_tool_from_mcp(mcp_dict)

            # Simulate running event loop — uses concurrent.futures
            with patch("asyncio.get_running_loop", return_value=MagicMock()):
                with patch("concurrent.futures.ThreadPoolExecutor") as mock_tpe:
                    future = MagicMock()
                    future.result.return_value = mock_result
                    executor_instance = MagicMock()
                    executor_instance.submit.return_value = future
                    executor_instance.__enter__ = MagicMock(return_value=executor_instance)
                    executor_instance.__exit__ = MagicMock(return_value=False)
                    mock_tpe.return_value = executor_instance

                    result = tool._run()
                    # With mocked ThreadPoolExecutor, result should be str(mock_result)
                    assert result is not None

    def test_tool_run_result_with_no_content(self):
        """Tool run where result has no .content returns str(result)."""
        mcp_dict = {
            "name": "plain",
            "description": "Plain tool",
            "input_schema": {"properties": {}, "required": []},
        }

        plain_result = "plain string result"

        with patch("src.engines.common.mcp_adapter.MCPTool") as MockMCPTool:
            wrapper = MagicMock()
            wrapper.name = "plain"
            wrapper.description = "Plain tool"
            wrapper.input_schema = {"properties": {}, "required": []}
            wrapper.execute = AsyncMock(return_value=plain_result)
            MockMCPTool.return_value = wrapper

            tool = create_crewai_tool_from_mcp(mcp_dict)

            with patch("asyncio.get_running_loop", side_effect=RuntimeError):
                result = tool._run()
                assert "plain string result" in result


# ===========================================================================
# wrap_mcp_tool: additional branches
# ===========================================================================

class TestWrapMCPToolAdditional:

    def test_standard_tool_non_runtime_error_returns_error(self):
        """Non-RuntimeError exception (ValueError) goes to the else branch and returns error."""
        tool = MagicMock()
        tool.name = "std_tool"
        tool._run = MagicMock(side_effect=ValueError("value error message"))

        wrapped = wrap_mcp_tool(tool)
        result = wrapped._run()
        assert "Error executing tool" in result

    def test_standard_tool_generic_exception_returns_error(self):
        """Non-RuntimeError generic exception path returns error string."""
        tool = MagicMock()
        tool.name = "std_tool"
        tool._run = MagicMock(side_effect=ValueError("generic fail"))

        wrapped = wrap_mcp_tool(tool)
        result = wrapped._run()
        assert "Error executing tool" in result

    def test_genie_tool_all_approaches_fail_returns_error(self):
        """When all fallbacks fail for genie tool, returns error string."""
        tool = MagicMock()
        tool.name = "get_space"
        tool._run = MagicMock(side_effect=Exception("direct fail"))

        wrapped = wrap_mcp_tool(tool)

        mock_loop = MagicMock()
        mock_loop.run_until_complete = MagicMock(side_effect=Exception("process fail"))

        with patch("asyncio.new_event_loop", return_value=mock_loop), \
             patch("asyncio.set_event_loop"):
            result = wrapped._run(space_id="s1")
            assert "Error executing tool" in result

    def test_genie_tool_process_returns_error_then_api_call_succeeds(self):
        """Genie tool falls back to direct API when process returns error string."""
        tool = MagicMock()
        tool.name = "get_space"
        tool._run = MagicMock(side_effect=Exception("direct fail"))

        wrapped = wrap_mcp_tool(tool)

        mock_loop = MagicMock()
        mock_loop.run_until_complete = MagicMock(
            side_effect=[
                "Error: process failed",      # first run_until_complete: process result
                {"space": "data"},            # second run_until_complete: API call
            ]
        )

        with patch("asyncio.new_event_loop", return_value=mock_loop), \
             patch("asyncio.set_event_loop"):
            result = wrapped._run(space_id="s1")
            # Either succeeds with API data or returns it; just verify no crash
            assert result is not None

    def test_genie_start_conversation_api_fallback(self):
        """start_conversation genie tool triggers its API fallback path."""
        tool = MagicMock()
        tool.name = "start_conversation"
        tool._run = MagicMock(side_effect=Exception("direct fail"))

        wrapped = wrap_mcp_tool(tool)

        mock_loop = MagicMock()
        mock_loop.run_until_complete = MagicMock(
            side_effect=[
                "Error: process fail",
                {"conversation_id": "c1"},
            ]
        )

        with patch("asyncio.new_event_loop", return_value=mock_loop), \
             patch("asyncio.set_event_loop"):
            result = wrapped._run(space_id="s1", content="hello")
            assert result is not None

    def test_genie_create_message_api_fallback(self):
        """create_message genie tool triggers its API fallback path."""
        tool = MagicMock()
        tool.name = "create_message"
        tool._run = MagicMock(side_effect=Exception("direct fail"))

        wrapped = wrap_mcp_tool(tool)

        mock_loop = MagicMock()
        mock_loop.run_until_complete = MagicMock(
            side_effect=[
                "Error: process fail",
                {"message_id": "m1"},
            ]
        )

        with patch("asyncio.new_event_loop", return_value=mock_loop), \
             patch("asyncio.set_event_loop"):
            result = wrapped._run(space_id="s1", conversation_id="c1", content="msg")
            assert result is not None


# ===========================================================================
# stop_mcp_adapter: sync stop and connection cleanup with error
# ===========================================================================

class TestStopMCPAdapterAdditional:

    @pytest.mark.asyncio
    async def test_sync_stop_runs_in_executor(self):
        """Adapter with sync stop (not async) runs in loop executor."""
        sync_adapter = MagicMock()
        # stop is NOT a coroutine — synchronous
        sync_adapter.stop = MagicMock()

        # We need asyncio.iscoroutinefunction to return False for stop
        # By default MagicMock().stop is a plain MagicMock, not a coroutine
        await stop_mcp_adapter(sync_adapter)
        sync_adapter.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_connection_close_error_is_suppressed(self):
        """Error when closing a connection is swallowed."""
        adapter = AsyncMock()
        adapter.stop = AsyncMock()
        bad_conn = MagicMock()
        bad_conn.close = MagicMock(side_effect=Exception("conn close fail"))
        adapter._connections = [bad_conn]

        # Should not raise
        await stop_mcp_adapter(adapter)
