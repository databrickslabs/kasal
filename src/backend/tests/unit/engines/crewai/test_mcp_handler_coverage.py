"""
Coverage tests for src/engines/crewai/tools/mcp_handler.py
"""
import pytest
import asyncio
import sys
from unittest.mock import AsyncMock, MagicMock, patch, call
import src.engines.crewai.tools.mcp_handler as mcp_handler


# ─── helpers ─────────────────────────────────────────────────────────────────

def reset_globals():
    mcp_handler._active_mcp_adapters.clear()
    mcp_handler._mcp_connection_pool.clear()


def make_mock_mcp_module(adapter=None):
    mock_adapter = adapter or MagicMock()
    # Satisfy _adapter_is_healthy: initialized, NO initialization_error, and a
    # non-empty tool list — an auto-generated Mock attribute would read as a
    # truthy error and the adapter would (correctly) never be pooled.
    mock_adapter._initialized = True
    mock_adapter.initialization_error = None
    mock_adapter.tools = [MagicMock()]
    mock_adapter.initialize = AsyncMock()
    mock_module = MagicMock()
    mock_module.MCPAdapter = MagicMock(return_value=mock_adapter)
    mock_module.MCPTool = MagicMock()
    return mock_module, mock_adapter


# ─── register_mcp_adapter ─────────────────────────────────────────────────────

def test_register_mcp_adapter():
    reset_globals()
    adapter = MagicMock()
    mcp_handler.register_mcp_adapter("test-id", adapter)
    assert "test-id" in mcp_handler._active_mcp_adapters
    assert mcp_handler._active_mcp_adapters["test-id"] is adapter


# ─── get_or_create_mcp_adapter ────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_or_create_mcp_adapter_creates_new():
    reset_globals()
    mock_module, mock_adapter = make_mock_mcp_module()

    params = {"url": "http://example.com", "auth_type": "token"}
    with patch.dict(sys.modules, {"src.engines.common.mcp_adapter": mock_module}):
        result = await mcp_handler.get_or_create_mcp_adapter(params, adapter_id="my-id")

    assert "my-id" in mcp_handler._active_mcp_adapters


@pytest.mark.asyncio
async def test_get_or_create_mcp_adapter_reuses_from_pool():
    reset_globals()
    # A pooled adapter is only reused when HEALTHY (_adapter_is_healthy):
    # initialized, no initialization_error, and discovered tools.
    mock_adapter = MagicMock()
    mock_adapter._initialized = True
    mock_adapter.initialization_error = None
    mock_adapter.tools = [MagicMock()]

    # HTTP pool keys carry an identity fingerprint (per-user OBO pooling);
    # with no credential material the fingerprint is 'noauth'.
    pool_key = "http://example.com_token_noauth"
    mcp_handler._mcp_connection_pool[pool_key] = mock_adapter

    params = {"url": "http://example.com", "auth_type": "token"}
    result = await mcp_handler.get_or_create_mcp_adapter(params)

    assert result is mock_adapter


@pytest.mark.asyncio
async def test_get_or_create_mcp_adapter_pools_per_identity():
    """Two callers with DIFFERENT credentials must not share a pooled
    connection — a pooled connection's server-side identity is fixed when it
    opens (the per-user OBO 'does not own conversation' bug)."""
    reset_globals()
    pooled = MagicMock()
    pooled._initialized = True
    pooled.initialization_error = None
    pooled.tools = [MagicMock()]

    import hashlib
    fp_a = hashlib.sha256(b"Bearer token-user-a").hexdigest()[:12]
    mcp_handler._mcp_connection_pool[f"http://example.com_token_{fp_a}"] = pooled

    # Same URL/auth_type, user A's token → reused.
    params_a = {"url": "http://example.com", "auth_type": "token",
                "headers": {"Authorization": "Bearer token-user-a"}}
    assert await mcp_handler.get_or_create_mcp_adapter(params_a) is pooled

    # User B's token → different key → a NEW adapter (mocked), never A's.
    fresh = MagicMock()
    fresh._initialized = True
    fresh.initialization_error = None
    fresh.tools = [MagicMock()]
    fresh.initialize = AsyncMock()
    mock_module = MagicMock()
    mock_module.MCPAdapter = MagicMock(return_value=fresh)
    params_b = {"url": "http://example.com", "auth_type": "token",
                "headers": {"Authorization": "Bearer token-user-b"}}
    with patch.dict(sys.modules, {"src.engines.common.mcp_adapter": mock_module}):
        assert await mcp_handler.get_or_create_mcp_adapter(params_b) is fresh


@pytest.mark.asyncio
async def test_get_or_create_mcp_adapter_removes_stale_from_pool():
    reset_globals()
    stale_adapter = MagicMock()
    stale_adapter._initialized = False

    pool_key = "http://example.com_token_noauth"
    mcp_handler._mcp_connection_pool[pool_key] = stale_adapter

    mock_new_adapter = MagicMock()
    mock_new_adapter._initialized = True
    mock_new_adapter.initialize = AsyncMock()

    mock_module = MagicMock()
    mock_module.MCPAdapter = MagicMock(return_value=mock_new_adapter)

    params = {"url": "http://example.com", "auth_type": "token"}
    with patch.dict(sys.modules, {"src.engines.common.mcp_adapter": mock_module}):
        result = await mcp_handler.get_or_create_mcp_adapter(params)

    # Stale was removed and new one added
    assert result is mock_new_adapter


@pytest.mark.asyncio
async def test_get_or_create_mcp_adapter_stdio_key():
    reset_globals()
    mock_module, mock_adapter = make_mock_mcp_module()

    params = {"transport": "stdio", "command": ["python", "server.py"]}
    with patch.dict(sys.modules, {"src.engines.common.mcp_adapter": mock_module}):
        result = await mcp_handler.get_or_create_mcp_adapter(params)

    pool_key = "stdio_python server.py"
    assert pool_key in mcp_handler._mcp_connection_pool


@pytest.mark.asyncio
async def test_get_or_create_mcp_adapter_stdio_string_command():
    reset_globals()
    mock_module, mock_adapter = make_mock_mcp_module()

    params = {"transport": "stdio", "command": "python server.py"}
    with patch.dict(sys.modules, {"src.engines.common.mcp_adapter": mock_module}):
        result = await mcp_handler.get_or_create_mcp_adapter(params)

    pool_key = "stdio_python server.py"
    assert pool_key in mcp_handler._mcp_connection_pool


@pytest.mark.asyncio
async def test_get_or_create_mcp_adapter_registers_with_id():
    reset_globals()
    mock_module, mock_adapter = make_mock_mcp_module()

    params = {"url": "http://example.com", "auth_type": "none"}
    with patch.dict(sys.modules, {"src.engines.common.mcp_adapter": mock_module}):
        result = await mcp_handler.get_or_create_mcp_adapter(params, adapter_id="adapter-42")

    assert "adapter-42" in mcp_handler._active_mcp_adapters


# ─── stop_all_adapters ────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_stop_all_adapters_empty():
    reset_globals()
    await mcp_handler.stop_all_adapters()
    assert len(mcp_handler._active_mcp_adapters) == 0


@pytest.mark.asyncio
async def test_stop_all_adapters_stops_pooled_and_tracked():
    reset_globals()
    mock_adapter = AsyncMock()
    mock_adapter.stop = AsyncMock()
    mcp_handler._mcp_connection_pool["key1"] = mock_adapter
    mcp_handler._active_mcp_adapters["id1"] = mock_adapter

    with patch("src.engines.crewai.tools.mcp_handler.stop_mcp_adapter", new=AsyncMock()) as mock_stop:
        await mcp_handler.stop_all_adapters()

    assert len(mcp_handler._active_mcp_adapters) == 0
    assert len(mcp_handler._mcp_connection_pool) == 0


@pytest.mark.asyncio
async def test_stop_all_adapters_handles_errors():
    reset_globals()
    mock_adapter = AsyncMock()
    mcp_handler._active_mcp_adapters["id1"] = mock_adapter

    async def raise_error(adapter):
        raise Exception("stop failed")

    with patch("src.engines.crewai.tools.mcp_handler.stop_mcp_adapter", side_effect=raise_error):
        await mcp_handler.stop_all_adapters()

    # Should complete without raising despite error


@pytest.mark.asyncio
async def test_stop_all_adapters_pooled_adapter_error():
    reset_globals()
    mock_adapter = MagicMock()
    mcp_handler._mcp_connection_pool["key1"] = mock_adapter

    async def raise_error(adapter):
        raise Exception("pool stop failed")

    with patch("src.engines.crewai.tools.mcp_handler.stop_mcp_adapter", side_effect=raise_error):
        await mcp_handler.stop_all_adapters()

    # Pool should be cleared even after error
    assert len(mcp_handler._mcp_connection_pool) == 0


# ─── get_databricks_workspace_host ────────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_databricks_workspace_host_success():
    mock_config = MagicMock()
    mock_config.workspace_url = "https://adb-123.azuredatabricks.net/"

    mock_service = AsyncMock()
    mock_service.get_databricks_config = AsyncMock(return_value=mock_config)

    mock_session = AsyncMock()
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=None)

    # Patch the imports used inside the function
    with patch.dict(sys.modules, {
        "src.services.databricks_service": MagicMock(DatabricksService=MagicMock(return_value=mock_service)),
        "src.db.session": MagicMock(request_scoped_session=MagicMock(return_value=mock_session))
    }):
        host, error = await mcp_handler.get_databricks_workspace_host()

    assert error is None
    assert "adb-123.azuredatabricks.net" in host


@pytest.mark.asyncio
async def test_get_databricks_workspace_host_http_prefix():
    mock_config = MagicMock()
    mock_config.workspace_url = "http://example.databricks.com/"

    mock_service = AsyncMock()
    mock_service.get_databricks_config = AsyncMock(return_value=mock_config)
    mock_session = AsyncMock()
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=None)

    with patch.dict(sys.modules, {
        "src.services.databricks_service": MagicMock(DatabricksService=MagicMock(return_value=mock_service)),
        "src.db.session": MagicMock(request_scoped_session=MagicMock(return_value=mock_session))
    }):
        host, error = await mcp_handler.get_databricks_workspace_host()

    assert error is None
    assert host == "example.databricks.com"


@pytest.mark.asyncio
async def test_get_databricks_workspace_host_no_url():
    mock_config = MagicMock()
    mock_config.workspace_url = None

    mock_service = AsyncMock()
    mock_service.get_databricks_config = AsyncMock(return_value=mock_config)
    mock_session = AsyncMock()
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=None)

    with patch.dict(sys.modules, {
        "src.services.databricks_service": MagicMock(DatabricksService=MagicMock(return_value=mock_service)),
        "src.db.session": MagicMock(request_scoped_session=MagicMock(return_value=mock_session))
    }):
        host, error = await mcp_handler.get_databricks_workspace_host()

    assert host is None
    assert error == "No workspace URL found in configuration"


@pytest.mark.asyncio
async def test_get_databricks_workspace_host_exception():
    with patch.dict(sys.modules, {
        "src.db.session": MagicMock(request_scoped_session=MagicMock(side_effect=Exception("session error")))
    }):
        host, error = await mcp_handler.get_databricks_workspace_host()

    assert host is None
    assert "session error" in error


# ─── call_databricks_api ──────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_call_databricks_api_auth_error():
    with patch("src.engines.crewai.tools.mcp_handler.get_databricks_auth_headers",
               new=AsyncMock(return_value=(None, "auth failed"))):
        result = await mcp_handler.call_databricks_api("/api/test")

    assert "error" in result


@pytest.mark.asyncio
async def test_call_databricks_api_no_headers():
    with patch("src.engines.crewai.tools.mcp_handler.get_databricks_auth_headers",
               new=AsyncMock(return_value=(None, None))):
        result = await mcp_handler.call_databricks_api("/api/test")

    assert "error" in result


@pytest.mark.asyncio
async def test_call_databricks_api_host_error():
    with patch("src.engines.crewai.tools.mcp_handler.get_databricks_auth_headers",
               new=AsyncMock(return_value=({"Authorization": "Bearer tok"}, None))):
        with patch("src.engines.crewai.tools.mcp_handler.get_databricks_workspace_host",
                   new=AsyncMock(return_value=(None, "host error"))):
            result = await mcp_handler.call_databricks_api("/api/test")

    assert "error" in result


# ─── stop_mcp_adapter ─────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_stop_mcp_adapter_none():
    await mcp_handler.stop_mcp_adapter(None)  # Should not raise


@pytest.mark.asyncio
async def test_stop_mcp_adapter_async_stop():
    adapter = MagicMock()
    adapter.stop = AsyncMock()
    await mcp_handler.stop_mcp_adapter(adapter)
    adapter.stop.assert_called_once()


@pytest.mark.asyncio
async def test_stop_mcp_adapter_async_close():
    adapter = MagicMock(spec=["close"])
    adapter.close = AsyncMock()
    await mcp_handler.stop_mcp_adapter(adapter)
    adapter.close.assert_called_once()


@pytest.mark.asyncio
async def test_stop_mcp_adapter_with_connections():
    adapter = MagicMock()
    adapter.stop = AsyncMock()
    conn1 = MagicMock()
    adapter._connections = [conn1]
    await mcp_handler.stop_mcp_adapter(adapter)
    conn1.close.assert_called_once()


@pytest.mark.asyncio
async def test_stop_mcp_adapter_connection_close_error():
    adapter = MagicMock()
    adapter.stop = AsyncMock()
    conn1 = MagicMock()
    conn1.close.side_effect = Exception("close error")
    adapter._connections = [conn1]
    await mcp_handler.stop_mcp_adapter(adapter)  # Should not raise


@pytest.mark.asyncio
async def test_stop_mcp_adapter_stop_exception():
    adapter = MagicMock()
    adapter.stop = AsyncMock(side_effect=Exception("stop error"))
    await mcp_handler.stop_mcp_adapter(adapter)  # Should not raise


# ─── wrap_mcp_tool ────────────────────────────────────────────────────────────

def test_wrap_mcp_tool_standard():
    tool = MagicMock()
    tool.name = "standard_tool"
    original_run = MagicMock(return_value="result")
    tool._run = original_run

    result_tool = mcp_handler.wrap_mcp_tool(tool)
    assert result_tool is tool
    # _run should have been replaced
    assert tool._run is not original_run


def test_wrap_mcp_tool_standard_direct_success():
    tool = MagicMock()
    tool.name = "standard_tool"
    tool._run = MagicMock(return_value="direct result")

    wrapped = mcp_handler.wrap_mcp_tool(tool)
    result = wrapped._run(arg1="val1")
    assert result == "direct result"


def test_wrap_mcp_tool_standard_event_loop_fallback():
    tool = MagicMock()
    tool.name = "standard_tool"
    original_run = MagicMock(side_effect=RuntimeError("Event loop is closed"))
    tool._run = original_run

    wrapped = mcp_handler.wrap_mcp_tool(tool)

    with patch("src.engines.crewai.tools.mcp_handler.run_in_separate_process", new=AsyncMock(return_value="process_result")):
        result = wrapped._run()
    assert result == "process_result"


def test_wrap_mcp_tool_standard_other_error():
    tool = MagicMock()
    tool.name = "standard_tool"
    original_run = MagicMock(side_effect=ValueError("some other error"))
    tool._run = original_run

    wrapped = mcp_handler.wrap_mcp_tool(tool)
    result = wrapped._run()
    assert "Error" in result


def test_wrap_mcp_tool_genie_tool():
    tool = MagicMock()
    tool.name = "get_space"
    original_run = MagicMock(return_value="space result")
    tool._run = original_run

    wrapped = mcp_handler.wrap_mcp_tool(tool)
    assert wrapped is tool

    result = wrapped._run(space_id="s1")
    assert result == "space result"


def test_wrap_mcp_tool_genie_tool_fallback_to_process():
    tool = MagicMock()
    tool.name = "start_conversation"
    original_run = MagicMock(side_effect=Exception("mcp failed"))
    tool._run = original_run

    wrapped = mcp_handler.wrap_mcp_tool(tool)

    with patch("src.engines.crewai.tools.mcp_handler.run_in_separate_process", new=AsyncMock(return_value="process result")):
        result = wrapped._run(space_id="s1", content="hello")
    assert result == "process result"


def test_wrap_mcp_tool_genie_fallback_process_starts_error():
    tool = MagicMock()
    tool.name = "create_message"
    original_run = MagicMock(side_effect=Exception("mcp failed"))
    tool._run = original_run

    wrapped = mcp_handler.wrap_mcp_tool(tool)

    with patch("src.engines.crewai.tools.mcp_handler.run_in_separate_process", new=AsyncMock(return_value="Error: something")):
        with patch("src.engines.crewai.tools.mcp_handler.call_databricks_api", new=AsyncMock(return_value={"data": "ok"})):
            result = wrapped._run(space_id="s1", conversation_id="c1", content="hello")
    # Should return api result
    assert result is not None


def test_wrap_mcp_tool_genie_all_approaches_fail():
    tool = MagicMock()
    tool.name = "get_space"
    original_run = MagicMock(side_effect=Exception("initial fail"))
    tool._run = original_run

    wrapped = mcp_handler.wrap_mcp_tool(tool)

    with patch("src.engines.crewai.tools.mcp_handler.run_in_separate_process", new=AsyncMock(side_effect=Exception("process fail"))):
        result = wrapped._run(space_id="s1")
    assert "Error" in result


# ─── create_crewai_tool_from_mcp ──────────────────────────────────────────────

def test_create_crewai_tool_from_mcp_with_properties():
    mcp_tool_dict = {
        "name": "test_tool",
        "description": "A test tool",
        "inputSchema": {
            "properties": {
                "param1": {"description": "First param", "type": "string"},
            },
            "required": ["param1"]
        }
    }

    mock_tool = MagicMock()
    mock_tool.name = "test_tool"
    mock_tool.description = "A test tool"
    mock_tool.input_schema = {
        "properties": {
            "param1": {"description": "First param"},
        },
        "required": ["param1"]
    }

    mock_module = MagicMock()
    mock_module.MCPTool = MagicMock(return_value=mock_tool)

    with patch.dict(sys.modules, {"src.engines.common.mcp_adapter": mock_module}):
        result = mcp_handler.create_crewai_tool_from_mcp(mcp_tool_dict)

    assert result is not None


def test_create_crewai_tool_from_mcp_empty_schema():
    mcp_tool_dict = {
        "name": "simple_tool",
        "description": "Simple tool",
    }

    mock_tool = MagicMock()
    mock_tool.name = "simple_tool"
    mock_tool.description = "Simple tool"
    mock_tool.input_schema = {}

    mock_module = MagicMock()
    mock_module.MCPTool = MagicMock(return_value=mock_tool)

    with patch.dict(sys.modules, {"src.engines.common.mcp_adapter": mock_module}):
        result = mcp_handler.create_crewai_tool_from_mcp(mcp_tool_dict)

    assert result is not None


def test_create_crewai_tool_from_mcp_optional_fields():
    """Test with optional fields (not in required)."""
    mcp_tool_dict = {"name": "opt_tool", "description": "Optional"}

    mock_tool = MagicMock()
    mock_tool.name = "opt_tool"
    mock_tool.description = "Optional"
    mock_tool.input_schema = {
        "properties": {
            "opt_param": {"description": "Optional param"},
        },
        "required": []
    }

    mock_module = MagicMock()
    mock_module.MCPTool = MagicMock(return_value=mock_tool)

    with patch.dict(sys.modules, {"src.engines.common.mcp_adapter": mock_module}):
        result = mcp_handler.create_crewai_tool_from_mcp(mcp_tool_dict)

    assert result is not None
