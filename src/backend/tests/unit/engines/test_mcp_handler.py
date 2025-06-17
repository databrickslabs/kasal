"""
Unit tests for MCP handler module.
"""
import pytest
import asyncio
import json
import os
import tempfile
from unittest.mock import Mock, AsyncMock, patch, MagicMock, call
from typing import Dict, Any

from src.engines.crewai.tools.mcp_handler import (
    register_mcp_adapter,
    stop_all_adapters,
    get_databricks_workspace_host,
    call_databricks_api,
    wrap_mcp_tool,
    run_in_separate_process,
    create_mcp_adapter,
    stop_mcp_adapter,
    _active_mcp_adapters
)


class TestMCPHandler:
    """Test cases for MCP handler functions."""

    def setup_method(self):
        """Setup for each test method."""
        # Clear active adapters before each test
        global _active_mcp_adapters
        _active_mcp_adapters.clear()

    def test_register_mcp_adapter(self):
        """Test registering MCP adapter."""
        adapter = Mock()
        adapter_id = "test_adapter"
        
        register_mcp_adapter(adapter_id, adapter)
        
        assert adapter_id in _active_mcp_adapters
        assert _active_mcp_adapters[adapter_id] == adapter

    @pytest.mark.asyncio
    async def test_stop_all_adapters_empty(self):
        """Test stopping all adapters when none are registered."""
        await stop_all_adapters()
        
        assert len(_active_mcp_adapters) == 0

    @pytest.mark.asyncio
    async def test_stop_all_adapters_success(self):
        """Test successfully stopping all adapters."""
        adapter1 = Mock()
        adapter2 = Mock()
        
        register_mcp_adapter("adapter1", adapter1)
        register_mcp_adapter("adapter2", adapter2)
        
        with patch('src.engines.crewai.tools.mcp_handler.stop_mcp_adapter', new_callable=AsyncMock) as mock_stop:
            await stop_all_adapters()
        
        assert len(_active_mcp_adapters) == 0
        assert mock_stop.call_count == 2

    @pytest.mark.asyncio
    async def test_stop_all_adapters_with_error(self):
        """Test stopping all adapters when one fails."""
        adapter1 = Mock()
        adapter2 = Mock()
        
        register_mcp_adapter("adapter1", adapter1)
        register_mcp_adapter("adapter2", adapter2)
        
        with patch('src.engines.crewai.tools.mcp_handler.stop_mcp_adapter', new_callable=AsyncMock) as mock_stop:
            mock_stop.side_effect = [Exception("Stop error"), None]
            await stop_all_adapters()
        
        assert len(_active_mcp_adapters) == 0

    @pytest.mark.asyncio
    async def test_stop_all_adapters_remove_error_continues(self):
        """Test stop_all_adapters continues when removal fails."""
        adapter1 = Mock()
        adapter2 = Mock()
        
        register_mcp_adapter("adapter1", adapter1)
        register_mcp_adapter("adapter2", adapter2)
        
        # Manually corrupt the dictionary to test both except blocks (lines 56-57)
        global _active_mcp_adapters
        original_dict = _active_mcp_adapters.copy()
        
        with patch('src.engines.crewai.tools.mcp_handler.stop_mcp_adapter', new_callable=AsyncMock) as mock_stop:
            mock_stop.side_effect = [Exception("Stop error"), None]
            
            # Create a custom dict that raises on both del operations to test lines 56-57
            class FailingDict(dict):
                def __delitem__(self, key):
                    # This will trigger both the main exception and the nested except block
                    raise Exception("Remove error")
            
            _active_mcp_adapters = FailingDict(original_dict)
            
            await stop_all_adapters()
        
        # Reset to clean state
        _active_mcp_adapters = {}

    @pytest.mark.asyncio
    async def test_get_databricks_workspace_host_success(self):
        """Test getting databricks workspace host successfully."""
        with patch('src.services.databricks_service.DatabricksService') as mock_service_class, \
             patch('src.core.unit_of_work.UnitOfWork') as mock_uow_class:
            
            mock_config = Mock()
            mock_config.workspace_url = "https://test.databricks.com/"
            
            mock_service = Mock()
            mock_service.get_databricks_config = AsyncMock(return_value=mock_config)
            mock_service_class.from_unit_of_work = AsyncMock(return_value=mock_service)
            
            mock_uow = Mock()
            mock_uow.__aenter__ = AsyncMock(return_value=mock_uow)
            mock_uow.__aexit__ = AsyncMock(return_value=None)
            mock_uow_class.return_value = mock_uow
            
            result, error = await get_databricks_workspace_host()
            
            assert result == "test.databricks.com"
            assert error is None

    @pytest.mark.asyncio
    async def test_get_databricks_workspace_host_no_config(self):
        """Test getting databricks workspace host when config is None."""
        with patch('src.services.databricks_service.DatabricksService') as mock_service_class, \
             patch('src.core.unit_of_work.UnitOfWork') as mock_uow_class:
            
            mock_service = Mock()
            mock_service.get_databricks_config = AsyncMock(return_value=None)
            mock_service_class.from_unit_of_work = AsyncMock(return_value=mock_service)
            
            mock_uow = Mock()
            mock_uow.__aenter__ = AsyncMock(return_value=mock_uow)
            mock_uow.__aexit__ = AsyncMock(return_value=None)
            mock_uow_class.return_value = mock_uow
            
            result, error = await get_databricks_workspace_host()
            
            assert result is None
            assert "No workspace URL found" in error

    @pytest.mark.asyncio
    async def test_get_databricks_workspace_host_no_url(self):
        """Test getting databricks workspace host when config has no URL."""
        with patch('src.services.databricks_service.DatabricksService') as mock_service_class, \
             patch('src.core.unit_of_work.UnitOfWork') as mock_uow_class:
            
            mock_config = Mock()
            mock_config.workspace_url = None
            
            mock_service = Mock()
            mock_service.get_databricks_config = AsyncMock(return_value=mock_config)
            mock_service_class.from_unit_of_work = AsyncMock(return_value=mock_service)
            
            mock_uow = Mock()
            mock_uow.__aenter__ = AsyncMock(return_value=mock_uow)
            mock_uow.__aexit__ = AsyncMock(return_value=None)
            mock_uow_class.return_value = mock_uow
            
            result, error = await get_databricks_workspace_host()
            
            assert result is None
            assert "No workspace URL found" in error

    @pytest.mark.asyncio
    async def test_get_databricks_workspace_host_exception(self):
        """Test getting databricks workspace host with exception."""
        with patch('src.core.unit_of_work.UnitOfWork') as mock_uow_class:
            mock_uow_class.side_effect = Exception("Service error")
            
            result, error = await get_databricks_workspace_host()
            
            assert result is None
            assert "Service error" in error

    @pytest.mark.asyncio
    async def test_get_databricks_workspace_host_http_prefix(self):
        """Test getting databricks workspace host with http prefix."""
        with patch('src.services.databricks_service.DatabricksService') as mock_service_class, \
             patch('src.core.unit_of_work.UnitOfWork') as mock_uow_class:
            
            mock_config = Mock()
            mock_config.workspace_url = "http://test.databricks.com/"
            
            mock_service = Mock()
            mock_service.get_databricks_config = AsyncMock(return_value=mock_config)
            mock_service_class.from_unit_of_work = AsyncMock(return_value=mock_service)
            
            mock_uow = Mock()
            mock_uow.__aenter__ = AsyncMock(return_value=mock_uow)
            mock_uow.__aexit__ = AsyncMock(return_value=None)
            mock_uow_class.return_value = mock_uow
            
            result, error = await get_databricks_workspace_host()
            
            assert result == "test.databricks.com"
            assert error is None

    @pytest.mark.asyncio
    async def test_get_databricks_workspace_host_url_variations(self):
        """Test various URL formats in workspace host."""
        test_cases = [
            ("https://test.databricks.com", "test.databricks.com"),
            ("http://test.databricks.com", "test.databricks.com"),
            ("test.databricks.com", "test.databricks.com"),
            ("https://test.databricks.com/", "test.databricks.com"),
            ("http://test.databricks.com/", "test.databricks.com"),
        ]
        
        for input_url, expected_output in test_cases:
            with patch('src.services.databricks_service.DatabricksService') as mock_service_class, \
                 patch('src.core.unit_of_work.UnitOfWork') as mock_uow_class:
                
                mock_config = Mock()
                mock_config.workspace_url = input_url
                
                mock_service = Mock()
                mock_service.get_databricks_config = AsyncMock(return_value=mock_config)
                mock_service_class.from_unit_of_work = AsyncMock(return_value=mock_service)
                
                mock_uow = Mock()
                mock_uow.__aenter__ = AsyncMock(return_value=mock_uow)
                mock_uow.__aexit__ = AsyncMock(return_value=None)
                mock_uow_class.return_value = mock_uow
                
                result, error = await get_databricks_workspace_host()
                
                assert result == expected_output
                assert error is None

    @pytest.mark.asyncio
    async def test_call_databricks_api_auth_error(self):
        """Test API call with authentication error."""
        # Import the module reference directly to patch the imported function
        import src.engines.crewai.tools.mcp_handler as mcp_handler_module
        
        with patch.object(mcp_handler_module, 'get_databricks_auth_headers', new_callable=AsyncMock) as mock_auth:
            mock_auth.return_value = (None, "Auth error")
            
            result = await call_databricks_api("/api/test")
            
            assert "error" in result
            assert "Authentication error" in result["error"]

    @pytest.mark.asyncio
    async def test_call_databricks_api_no_headers(self):
        """Test API call when no headers are returned."""
        import src.engines.crewai.tools.mcp_handler as mcp_handler_module
        
        with patch.object(mcp_handler_module, 'get_databricks_auth_headers', new_callable=AsyncMock) as mock_auth:
            mock_auth.return_value = (None, None)
            
            result = await call_databricks_api("/api/test")
            
            assert "error" in result
            assert "Failed to get authentication headers" in result["error"]

    @pytest.mark.asyncio
    async def test_call_databricks_api_host_error(self):
        """Test API call with host resolution error."""
        import src.engines.crewai.tools.mcp_handler as mcp_handler_module
        
        with patch.object(mcp_handler_module, 'get_databricks_auth_headers', new_callable=AsyncMock) as mock_auth, \
             patch.object(mcp_handler_module, 'get_databricks_workspace_host', new_callable=AsyncMock) as mock_host:
            
            mock_auth.return_value = ({"Authorization": "Bearer token"}, None)
            mock_host.return_value = (None, "Host error")
            
            result = await call_databricks_api("/api/test")
            
            assert "error" in result
            assert "Configuration error: Host error" in result["error"]

    @pytest.mark.asyncio
    async def test_call_databricks_api_unsupported_method(self):
        """Test API call with unsupported HTTP method."""
        import src.engines.crewai.tools.mcp_handler as mcp_handler_module
        
        with patch.object(mcp_handler_module, 'get_databricks_auth_headers', new_callable=AsyncMock) as mock_auth, \
             patch.object(mcp_handler_module, 'get_databricks_workspace_host', new_callable=AsyncMock) as mock_host:
            
            mock_auth.return_value = ({"Authorization": "Bearer token"}, None)
            mock_host.return_value = ("test.databricks.com", None)
            
            result = await call_databricks_api("/api/test", method="PATCH")
            
            assert "error" in result
            assert "API error: Unsupported HTTP method: PATCH" in result["error"]

    @pytest.mark.asyncio 
    async def test_call_databricks_api_get_success(self):
        """Test successful GET API call."""
        import src.engines.crewai.tools.mcp_handler as mcp_handler_module
        mock_response_data = {"result": "success"}
        
        with patch.object(mcp_handler_module, 'get_databricks_auth_headers', new_callable=AsyncMock) as mock_auth, \
             patch.object(mcp_handler_module, 'get_databricks_workspace_host', new_callable=AsyncMock) as mock_host, \
             patch('aiohttp.ClientSession') as mock_session_class:
            
            mock_auth.return_value = ({"Authorization": "Bearer token"}, None)
            mock_host.return_value = ("test.databricks.com", None)
            
            mock_response = Mock()
            mock_response.json = AsyncMock(return_value=mock_response_data)
            mock_response.raise_for_status = Mock()
            
            mock_session = Mock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_session.get.return_value.__aenter__ = AsyncMock(return_value=mock_response)
            mock_session.get.return_value.__aexit__ = AsyncMock(return_value=None)
            mock_session_class.return_value = mock_session
            
            result = await call_databricks_api("/api/test")
            
            assert result == mock_response_data
            mock_session.get.assert_called_once()

    @pytest.mark.asyncio
    async def test_call_databricks_api_post_success(self):
        """Test successful POST API call."""
        import src.engines.crewai.tools.mcp_handler as mcp_handler_module
        mock_response_data = {"result": "created"}
        
        with patch.object(mcp_handler_module, 'get_databricks_auth_headers', new_callable=AsyncMock) as mock_auth, \
             patch.object(mcp_handler_module, 'get_databricks_workspace_host', new_callable=AsyncMock) as mock_host, \
             patch('aiohttp.ClientSession') as mock_session_class:
            
            mock_auth.return_value = ({"Authorization": "Bearer token"}, None)
            mock_host.return_value = ("test.databricks.com", None)
            
            mock_response = Mock()
            mock_response.json = AsyncMock(return_value=mock_response_data)
            mock_response.raise_for_status = Mock()
            
            mock_session = Mock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_session.post.return_value.__aenter__ = AsyncMock(return_value=mock_response)
            mock_session.post.return_value.__aexit__ = AsyncMock(return_value=None)
            mock_session_class.return_value = mock_session
            
            result = await call_databricks_api("/api/test", method="POST", data={"key": "value"})
            
            assert result == mock_response_data
            mock_session.post.assert_called_once()

    @pytest.mark.asyncio
    async def test_call_databricks_api_put_success(self):
        """Test successful PUT API call."""
        import src.engines.crewai.tools.mcp_handler as mcp_handler_module
        mock_response_data = {"result": "updated"}
        
        with patch.object(mcp_handler_module, 'get_databricks_auth_headers', new_callable=AsyncMock) as mock_auth, \
             patch.object(mcp_handler_module, 'get_databricks_workspace_host', new_callable=AsyncMock) as mock_host, \
             patch('aiohttp.ClientSession') as mock_session_class:
            
            mock_auth.return_value = ({"Authorization": "Bearer token"}, None)
            mock_host.return_value = ("test.databricks.com", None)
            
            mock_response = Mock()
            mock_response.json = AsyncMock(return_value=mock_response_data)
            mock_response.raise_for_status = Mock()
            
            mock_session = Mock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_session.put.return_value.__aenter__ = AsyncMock(return_value=mock_response)
            mock_session.put.return_value.__aexit__ = AsyncMock(return_value=None)
            mock_session_class.return_value = mock_session
            
            result = await call_databricks_api("/api/test", method="PUT", data={"key": "value"})
            
            assert result == mock_response_data
            mock_session.put.assert_called_once()

    @pytest.mark.asyncio
    async def test_call_databricks_api_delete_success(self):
        """Test successful DELETE API call."""
        import src.engines.crewai.tools.mcp_handler as mcp_handler_module
        mock_response_data = {"result": "deleted"}
        
        with patch.object(mcp_handler_module, 'get_databricks_auth_headers', new_callable=AsyncMock) as mock_auth, \
             patch.object(mcp_handler_module, 'get_databricks_workspace_host', new_callable=AsyncMock) as mock_host, \
             patch('aiohttp.ClientSession') as mock_session_class:
            
            mock_auth.return_value = ({"Authorization": "Bearer token"}, None)
            mock_host.return_value = ("test.databricks.com", None)
            
            mock_response = Mock()
            mock_response.json = AsyncMock(return_value=mock_response_data)
            mock_response.raise_for_status = Mock()
            
            mock_session = Mock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_session.delete.return_value.__aenter__ = AsyncMock(return_value=mock_response)
            mock_session.delete.return_value.__aexit__ = AsyncMock(return_value=None)
            mock_session_class.return_value = mock_session
            
            result = await call_databricks_api("/api/test", method="DELETE")
            
            assert result == mock_response_data
            mock_session.delete.assert_called_once()

    @pytest.mark.asyncio
    async def test_call_databricks_api_with_params(self):
        """Test API call with query parameters."""
        import src.engines.crewai.tools.mcp_handler as mcp_handler_module
        mock_response_data = {"result": "success"}
        
        with patch.object(mcp_handler_module, 'get_databricks_auth_headers', new_callable=AsyncMock) as mock_auth, \
             patch.object(mcp_handler_module, 'get_databricks_workspace_host', new_callable=AsyncMock) as mock_host, \
             patch('aiohttp.ClientSession') as mock_session_class:
            
            mock_auth.return_value = ({"Authorization": "Bearer token"}, None)
            mock_host.return_value = ("test.databricks.com", None)
            
            mock_response = Mock()
            mock_response.json = AsyncMock(return_value=mock_response_data)
            mock_response.raise_for_status = Mock()
            
            mock_session = Mock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_session.get.return_value.__aenter__ = AsyncMock(return_value=mock_response)
            mock_session.get.return_value.__aexit__ = AsyncMock(return_value=None)
            mock_session_class.return_value = mock_session
            
            result = await call_databricks_api("/api/test", params={"key": "value"})
            
            assert result == mock_response_data
            mock_session.get.assert_called_once()

    @pytest.mark.asyncio
    async def test_call_databricks_api_exception_during_request(self):
        """Test API call with exception during HTTP request."""
        import src.engines.crewai.tools.mcp_handler as mcp_handler_module
        
        with patch.object(mcp_handler_module, 'get_databricks_auth_headers', new_callable=AsyncMock) as mock_auth, \
             patch.object(mcp_handler_module, 'get_databricks_workspace_host', new_callable=AsyncMock) as mock_host, \
             patch('aiohttp.ClientSession') as mock_session_class:
            
            mock_auth.return_value = ({"Authorization": "Bearer token"}, None)
            mock_host.return_value = ("test.databricks.com", None)
            
            mock_session = Mock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_session.get.side_effect = Exception("HTTP request failed")
            mock_session_class.return_value = mock_session
            
            result = await call_databricks_api("/api/test")
            
            assert "error" in result
            assert "API error: HTTP request failed" in result["error"]

    def test_wrap_mcp_tool_success(self):
        """Test wrapping MCP tool successfully."""
        mock_tool = Mock()
        mock_tool.name = "test_tool"
        mock_tool._run = Mock(return_value="success")
        
        wrapped_tool = wrap_mcp_tool(mock_tool)
        
        assert wrapped_tool == mock_tool
        assert wrapped_tool.name == "test_tool"
        
        # Test the wrapped _run method
        result = wrapped_tool._run("arg1", kwarg1="value1")
        assert result == "success"

    def test_wrap_mcp_tool_genie_tool_success(self):
        """Test wrapping Databricks Genie tool successfully."""
        mock_tool = Mock()
        mock_tool.name = "get_space"
        mock_tool._run = Mock(return_value="space_data")
        
        wrapped_tool = wrap_mcp_tool(mock_tool)
        
        assert wrapped_tool == mock_tool
        
        # Test the wrapped _run method
        result = wrapped_tool._run(space_id="123")
        assert result == "space_data"

    def test_wrap_mcp_tool_genie_tool_with_fallback(self):
        """Test wrapping Databricks Genie tool with fallback to process isolation."""
        mock_tool = Mock()
        mock_tool.name = "start_conversation"
        mock_tool._run = Mock(side_effect=Exception("Direct execution failed"))
        
        with patch('asyncio.new_event_loop') as mock_new_loop, \
             patch('asyncio.set_event_loop') as mock_set_loop, \
             patch('src.engines.crewai.tools.mcp_handler.run_in_separate_process', new_callable=AsyncMock) as mock_run_process:
            
            mock_loop = Mock()
            mock_loop.run_until_complete.return_value = "conversation_data"
            mock_loop.close = Mock()
            mock_new_loop.return_value = mock_loop
            mock_run_process.return_value = "conversation_data"
            
            wrapped_tool = wrap_mcp_tool(mock_tool)
            
            result = wrapped_tool._run(space_id="123", content="test")
            assert result == "conversation_data"

    def test_wrap_mcp_tool_genie_tool_api_fallback(self):
        """Test wrapping Databricks Genie tool with API fallback."""
        mock_tool = Mock()
        mock_tool.name = "get_space"
        mock_tool._run = Mock(side_effect=Exception("Direct execution failed"))
        
        with patch('asyncio.new_event_loop') as mock_new_loop, \
             patch('asyncio.set_event_loop') as mock_set_loop, \
             patch('src.engines.crewai.tools.mcp_handler.run_in_separate_process', new_callable=AsyncMock) as mock_run_process, \
             patch('src.engines.crewai.tools.mcp_handler.call_databricks_api', new_callable=AsyncMock) as mock_api_call:
            
            mock_loop = Mock()
            mock_loop.run_until_complete.side_effect = ["Error: Process failed", "api_result"]
            mock_loop.close = Mock()
            mock_new_loop.return_value = mock_loop
            mock_run_process.return_value = "Error: Process failed"
            mock_api_call.return_value = "api_result"
            
            wrapped_tool = wrap_mcp_tool(mock_tool)
            
            result = wrapped_tool._run(space_id="123")
            assert result == "api_result"

    def test_wrap_mcp_tool_start_conversation_api_fallback(self):
        """Test start_conversation tool with API fallback."""
        mock_tool = Mock()
        mock_tool.name = "start_conversation"
        mock_tool._run = Mock(side_effect=Exception("Direct execution failed"))
        
        with patch('asyncio.new_event_loop') as mock_new_loop, \
             patch('asyncio.set_event_loop') as mock_set_loop, \
             patch('src.engines.crewai.tools.mcp_handler.run_in_separate_process', new_callable=AsyncMock) as mock_run_process, \
             patch('src.engines.crewai.tools.mcp_handler.call_databricks_api', new_callable=AsyncMock) as mock_api_call:
            
            mock_loop = Mock()
            mock_loop.run_until_complete.side_effect = ["Error: Process failed", "api_result"]
            mock_loop.close = Mock()
            mock_new_loop.return_value = mock_loop
            mock_run_process.return_value = "Error: Process failed"
            mock_api_call.return_value = "api_result"
            
            wrapped_tool = wrap_mcp_tool(mock_tool)
            
            result = wrapped_tool._run(space_id="123", content="test")
            assert result == "api_result"
            mock_api_call.assert_called_once_with(
                "/api/2.0/genie/spaces/123/conversations",
                method="POST",
                data={"content": "test"}
            )

    def test_wrap_mcp_tool_create_message_api_fallback(self):
        """Test create_message tool with API fallback."""
        mock_tool = Mock()
        mock_tool.name = "create_message"
        mock_tool._run = Mock(side_effect=Exception("Direct execution failed"))
        
        with patch('asyncio.new_event_loop') as mock_new_loop, \
             patch('asyncio.set_event_loop') as mock_set_loop, \
             patch('src.engines.crewai.tools.mcp_handler.run_in_separate_process', new_callable=AsyncMock) as mock_run_process, \
             patch('src.engines.crewai.tools.mcp_handler.call_databricks_api', new_callable=AsyncMock) as mock_api_call:
            
            mock_loop = Mock()
            mock_loop.run_until_complete.side_effect = ["Error: Process failed", "api_result"]
            mock_loop.close = Mock()
            mock_new_loop.return_value = mock_loop
            mock_run_process.return_value = "Error: Process failed"
            mock_api_call.return_value = "api_result"
            
            wrapped_tool = wrap_mcp_tool(mock_tool)
            
            result = wrapped_tool._run(space_id="123", conversation_id="456", content="test")
            assert result == "api_result"
            mock_api_call.assert_called_once_with(
                "/api/2.0/genie/spaces/123/conversations/456/messages",
                method="POST",
                data={"content": "test"}
            )

    def test_wrap_mcp_tool_genie_missing_required_params(self):
        """Test Genie tool API fallback with missing required parameters."""
        mock_tool = Mock()
        mock_tool.name = "get_space"
        mock_tool._run = Mock(side_effect=Exception("Direct execution failed"))
        
        with patch('asyncio.new_event_loop') as mock_new_loop, \
             patch('asyncio.set_event_loop') as mock_set_loop, \
             patch('src.engines.crewai.tools.mcp_handler.run_in_separate_process', new_callable=AsyncMock) as mock_run_process:
            
            mock_loop = Mock()
            mock_loop.run_until_complete.return_value = "Error: Process failed"
            mock_loop.close = Mock()
            mock_new_loop.return_value = mock_loop
            mock_run_process.return_value = "Error: Process failed"
            
            wrapped_tool = wrap_mcp_tool(mock_tool)
            
            # Missing space_id parameter
            result = wrapped_tool._run(other_param="value")
            assert "Error: Process failed" in result

    def test_wrap_mcp_tool_event_loop_error(self):
        """Test wrapping MCP tool with event loop error."""
        mock_tool = Mock()
        mock_tool.name = "test_tool"
        mock_tool._run = Mock(side_effect=RuntimeError("Event loop is closed"))
        
        with patch('asyncio.new_event_loop') as mock_new_loop, \
             patch('asyncio.set_event_loop') as mock_set_loop, \
             patch('src.engines.crewai.tools.mcp_handler.run_in_separate_process', new_callable=AsyncMock) as mock_run_process:
            
            mock_loop = Mock()
            mock_loop.run_until_complete.return_value = "process_result"
            mock_loop.close = Mock()
            mock_new_loop.return_value = mock_loop
            mock_run_process.return_value = "process_result"
            
            wrapped_tool = wrap_mcp_tool(mock_tool)
            
            result = wrapped_tool._run("arg1", kwarg1="value1")
            assert result == "process_result"

    def test_wrap_mcp_tool_event_loop_string_error(self):
        """Test wrap_mcp_tool with 'Event loop is closed' string error."""
        mock_tool = Mock()
        mock_tool.name = "test_tool"
        mock_tool._run = Mock(side_effect=Exception("Event loop is closed"))
        
        with patch('asyncio.new_event_loop') as mock_new_loop, \
             patch('asyncio.set_event_loop') as mock_set_loop, \
             patch('src.engines.crewai.tools.mcp_handler.run_in_separate_process', new_callable=AsyncMock) as mock_run_process:
            
            mock_loop = Mock()
            mock_loop.run_until_complete.return_value = "process_result"
            mock_loop.close = Mock()
            mock_new_loop.return_value = mock_loop
            mock_run_process.return_value = "process_result"
            
            wrapped_tool = wrap_mcp_tool(mock_tool)
            
            result = wrapped_tool._run("arg1", kwarg1="value1")
            assert result == "process_result"

    def test_wrap_mcp_tool_other_error(self):
        """Test wrapping MCP tool with other error."""
        mock_tool = Mock()
        mock_tool.name = "test_tool"
        mock_tool._run = Mock(side_effect=ValueError("Some other error"))
        
        wrapped_tool = wrap_mcp_tool(mock_tool)
        
        result = wrapped_tool._run("arg1", kwarg1="value1")
        assert "Error executing tool" in result

    def test_wrap_mcp_tool_non_runtime_error(self):
        """Test wrap_mcp_tool with non-RuntimeError exception."""
        mock_tool = Mock()
        mock_tool.name = "test_tool"
        mock_tool._run = Mock(side_effect=ValueError("Not a runtime error"))
        
        wrapped_tool = wrap_mcp_tool(mock_tool)
        
        result = wrapped_tool._run("arg1", kwarg1="value1")
        assert "Error executing tool: Not a runtime error" in result

    def test_wrap_mcp_tool_exception_handling(self):
        """Test wrap_mcp_tool exception handling."""
        mock_tool = Mock()
        mock_tool.name = "test_tool"
        mock_tool._run = Mock(side_effect=ValueError("Some error"))
        
        wrapped_tool = wrap_mcp_tool(mock_tool)
        
        result = wrapped_tool._run("arg1", kwarg1="value1")
        assert "Error executing tool: Some error" in result

    def test_wrap_mcp_tool_process_isolation_error(self):
        """Test wrapping MCP tool when process isolation fails."""
        mock_tool = Mock()
        mock_tool.name = "test_tool"
        mock_tool._run = Mock(side_effect=RuntimeError("Event loop is closed"))
        
        with patch('asyncio.new_event_loop') as mock_new_loop, \
             patch('asyncio.set_event_loop') as mock_set_loop:
            
            mock_loop = Mock()
            mock_loop.run_until_complete.side_effect = Exception("Process error")
            mock_loop.close = Mock()
            mock_new_loop.return_value = mock_loop
            
            wrapped_tool = wrap_mcp_tool(mock_tool)
            
            result = wrapped_tool._run("arg1", kwarg1="value1")
            assert "Error executing tool" in result

    def test_wrap_mcp_tool_genie_api_call_errors(self):
        """Test wrapping Genie tool with API call errors."""
        mock_tool = Mock()
        mock_tool.name = "create_message"
        mock_tool._run = Mock(side_effect=Exception("Direct execution failed"))
        
        with patch('asyncio.new_event_loop') as mock_new_loop, \
             patch('asyncio.set_event_loop') as mock_set_loop, \
             patch('src.engines.crewai.tools.mcp_handler.run_in_separate_process', new_callable=AsyncMock) as mock_run_process, \
             patch('src.engines.crewai.tools.mcp_handler.call_databricks_api', new_callable=AsyncMock) as mock_api_call:
            
            mock_loop = Mock()
            mock_loop.run_until_complete.side_effect = ["Error: Process failed", Exception("API error")]
            mock_loop.close = Mock()
            mock_new_loop.return_value = mock_loop
            mock_run_process.return_value = "Error: Process failed"
            mock_api_call.side_effect = Exception("API error")
            
            wrapped_tool = wrap_mcp_tool(mock_tool)
            
            result = wrapped_tool._run(space_id="123", conversation_id="456", content="test")
            assert "API call failed" in result

    def test_wrap_mcp_tool_all_approaches_fail(self):
        """Test wrapping tool when all approaches fail."""
        mock_tool = Mock()
        mock_tool.name = "get_space"  # Use a Genie tool name to trigger the special handling
        mock_tool._run = Mock(side_effect=Exception("Direct execution failed"))
        
        with patch('asyncio.new_event_loop') as mock_new_loop, \
             patch('asyncio.set_event_loop') as mock_set_loop, \
             patch('src.engines.crewai.tools.mcp_handler.run_in_separate_process', new_callable=AsyncMock) as mock_run_process, \
             patch('src.engines.crewai.tools.mcp_handler.call_databricks_api', new_callable=AsyncMock) as mock_api_call:
            
            mock_loop = Mock()
            mock_loop.run_until_complete.side_effect = [Exception("Process error"), Exception("API error")]
            mock_loop.close = Mock()
            mock_new_loop.return_value = mock_loop
            mock_run_process.side_effect = Exception("Process error")
            mock_api_call.side_effect = Exception("API error")
            
            wrapped_tool = wrap_mcp_tool(mock_tool)
            
            result = wrapped_tool._run(space_id="123")  # Provide required param to trigger API fallback
            assert "Error executing tool" in result

    @pytest.mark.asyncio
    async def test_run_in_separate_process_success(self):
        """Test running tool in separate process successfully."""
        with patch('asyncio.create_subprocess_exec') as mock_create_subprocess:
            mock_process = Mock()
            mock_process.communicate = AsyncMock(return_value=(b'{"result": "success"}', b''))
            mock_process.returncode = 0
            mock_create_subprocess.return_value = mock_process
            
            result = await run_in_separate_process("test_tool", {"arg": "value"})
            
            assert result == {"result": "success"}

    @pytest.mark.asyncio
    async def test_run_in_separate_process_failure(self):
        """Test running tool in separate process with failure."""
        with patch('asyncio.create_subprocess_exec') as mock_create_subprocess:
            mock_process = Mock()
            mock_process.communicate = AsyncMock(return_value=(b'', b'Process error'))
            mock_process.returncode = 1
            mock_create_subprocess.return_value = mock_process
            
            result = await run_in_separate_process("test_tool", {"arg": "value"})
            
            assert "error" in result
            assert "Process error" in result["error"]

    @pytest.mark.asyncio
    async def test_run_in_separate_process_invalid_json(self):
        """Test running tool in separate process with invalid JSON output."""
        with patch('asyncio.create_subprocess_exec') as mock_create_subprocess:
            mock_process = Mock()
            mock_process.communicate = AsyncMock(return_value=(b'invalid json', b''))
            mock_process.returncode = 0
            mock_create_subprocess.return_value = mock_process
            
            result = await run_in_separate_process("test_tool", {"arg": "value"})
            
            assert "error" in result
            assert "Failed to parse result" in result["error"]

    @pytest.mark.asyncio
    async def test_run_in_separate_process_exception(self):
        """Test running tool in separate process with exception."""
        with patch('asyncio.create_subprocess_exec', side_effect=Exception("Subprocess error")):
            result = await run_in_separate_process("test_tool", {"arg": "value"})
            
            assert "error" in result
            assert "Error running tool" in result["error"]

    @pytest.mark.asyncio
    async def test_run_in_separate_process_script_cleanup(self):
        """Test that temporary script is cleaned up."""
        with patch('asyncio.create_subprocess_exec') as mock_create_subprocess, \
             patch('os.remove') as mock_remove:
            
            mock_process = Mock()
            mock_process.communicate = AsyncMock(return_value=(b'{"result": "success"}', b''))
            mock_process.returncode = 0
            mock_create_subprocess.return_value = mock_process
            
            await run_in_separate_process("test_tool", {"arg": "value"})
            
            # Check that os.remove was called (script cleanup)
            mock_remove.assert_called_once()

    @pytest.mark.asyncio
    async def test_run_in_separate_process_cleanup_exception(self):
        """Test that cleanup exception is handled gracefully."""
        with patch('asyncio.create_subprocess_exec') as mock_create_subprocess, \
             patch('os.remove', side_effect=Exception("Cleanup failed")) as mock_remove:
            
            mock_process = Mock()
            mock_process.communicate = AsyncMock(return_value=(b'{"result": "success"}', b''))
            mock_process.returncode = 0
            mock_create_subprocess.return_value = mock_process
            
            result = await run_in_separate_process("test_tool", {"arg": "value"})
            
            assert result == {"result": "success"}
            mock_remove.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_mcp_adapter_success(self):
        """Test creating MCP adapter successfully."""
        with patch('src.utils.databricks_auth.get_mcp_auth_headers', new_callable=AsyncMock) as mock_auth, \
             patch('src.engines.crewai.tools.mcp_adapter.AsyncMCPAdapter') as mock_adapter_class:
            
            mock_auth.return_value = ({"Authorization": "Bearer token"}, None)
            
            mock_adapter = Mock()
            mock_adapter.initialize = AsyncMock()
            mock_adapter.tools = ["tool1", "tool2"]
            mock_adapter_class.return_value = mock_adapter
            
            result = await create_mcp_adapter()
            
            assert result == mock_adapter
            mock_adapter.initialize.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_mcp_adapter_no_tools(self):
        """Test creating MCP adapter with no tools."""
        with patch('src.utils.databricks_auth.get_mcp_auth_headers', new_callable=AsyncMock) as mock_auth, \
             patch('src.engines.crewai.tools.mcp_adapter.AsyncMCPAdapter') as mock_adapter_class:
            
            mock_auth.return_value = ({"Authorization": "Bearer token"}, None)
            
            mock_adapter = Mock()
            mock_adapter.initialize = AsyncMock()
            mock_adapter.tools = None
            mock_adapter_class.return_value = mock_adapter
            
            result = await create_mcp_adapter()
            
            assert result == mock_adapter
            mock_adapter.initialize.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_mcp_adapter_auth_error(self):
        """Test creating MCP adapter with auth error."""
        import src.engines.crewai.tools.mcp_handler as mcp_handler_module
        
        with patch.object(mcp_handler_module, 'get_mcp_auth_headers', new_callable=AsyncMock) as mock_auth:
            mock_auth.return_value = (None, "Auth error")
            
            with pytest.raises(ValueError, match="Failed to get MCP auth headers: Auth error"):
                await create_mcp_adapter()

    @pytest.mark.asyncio
    async def test_create_mcp_adapter_initialization_error(self):
        """Test creating MCP adapter with initialization error."""
        with patch('src.utils.databricks_auth.get_mcp_auth_headers', new_callable=AsyncMock) as mock_auth, \
             patch('src.engines.crewai.tools.mcp_adapter.AsyncMCPAdapter') as mock_adapter_class:
            
            mock_auth.return_value = ({"Authorization": "Bearer token"}, None)
            
            mock_adapter = Mock()
            mock_adapter.initialize = AsyncMock(side_effect=Exception("Init error"))
            mock_adapter_class.return_value = mock_adapter
            
            with pytest.raises(Exception, match="Init error"):
                await create_mcp_adapter()

    @pytest.mark.asyncio
    async def test_create_mcp_adapter_registers_adapter(self):
        """Test that create_mcp_adapter registers the adapter."""
        import src.engines.crewai.tools.mcp_handler as mcp_handler_module
        
        with patch.object(mcp_handler_module, 'get_mcp_auth_headers', new_callable=AsyncMock) as mock_auth, \
             patch('src.engines.crewai.tools.mcp_adapter.AsyncMCPAdapter') as mock_adapter_class, \
             patch.object(mcp_handler_module, 'register_mcp_adapter') as mock_register:
            
            mock_auth.return_value = ({"Authorization": "Bearer token"}, None)
            
            mock_adapter = Mock()
            mock_adapter.initialize = AsyncMock()
            mock_adapter.tools = ["tool1", "tool2"]
            mock_adapter_class.return_value = mock_adapter
            
            result = await create_mcp_adapter()
            
            # Check that register_mcp_adapter was called
            mock_register.assert_called_once()
            # Check that it was called with the right parameters
            call_args = mock_register.call_args
            assert call_args[0][0] == id(result)  # adapter_id
            assert call_args[0][1] == result      # adapter

    @pytest.mark.asyncio
    async def test_stop_mcp_adapter_async_adapter(self):
        """Test stopping async MCP adapter."""
        mock_adapter = Mock()
        mock_adapter.stop = AsyncMock()
        
        await stop_mcp_adapter(mock_adapter)
        
        mock_adapter.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_stop_mcp_adapter_sync_adapter(self):
        """Test stopping sync MCP adapter."""
        mock_adapter = Mock()
        mock_adapter.stop = Mock()  # Sync method
        
        with patch('asyncio.get_event_loop') as mock_get_loop:
            mock_loop = Mock()
            mock_loop.run_in_executor = AsyncMock()
            mock_get_loop.return_value = mock_loop
            
            await stop_mcp_adapter(mock_adapter)
            
            mock_loop.run_in_executor.assert_called_once()

    @pytest.mark.asyncio
    async def test_stop_mcp_adapter_none(self):
        """Test stopping None adapter."""
        await stop_mcp_adapter(None)
        # Should not raise exception

    @pytest.mark.asyncio
    async def test_stop_mcp_adapter_no_stop_method(self):
        """Test stopping adapter without stop method."""
        mock_adapter = Mock()
        # Remove stop method
        if hasattr(mock_adapter, 'stop'):
            delattr(mock_adapter, 'stop')
        
        await stop_mcp_adapter(mock_adapter)
        # Should not raise exception

    @pytest.mark.asyncio
    async def test_stop_mcp_adapter_with_connections(self):
        """Test stopping adapter with connections cleanup."""
        mock_connection = Mock()
        mock_connection.close = Mock()
        
        mock_adapter = Mock()
        mock_adapter.stop = AsyncMock()
        mock_adapter._connections = [mock_connection]
        
        await stop_mcp_adapter(mock_adapter)
        
        mock_adapter.stop.assert_called_once()
        mock_connection.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_stop_mcp_adapter_connection_error(self):
        """Test stopping adapter with connection cleanup error."""
        mock_connection = Mock()
        mock_connection.close.side_effect = Exception("Close error")
        
        mock_adapter = Mock()
        mock_adapter.stop = AsyncMock()
        mock_adapter._connections = [mock_connection]
        
        await stop_mcp_adapter(mock_adapter)
        
        mock_adapter.stop.assert_called_once()
        # Should handle connection error gracefully

    @pytest.mark.asyncio
    async def test_stop_mcp_adapter_stop_error(self):
        """Test stopping adapter when stop method fails."""
        mock_adapter = Mock()
        mock_adapter.stop = AsyncMock(side_effect=Exception("Stop error"))
        
        await stop_mcp_adapter(mock_adapter)
        
        # Should handle stop error gracefully

    def test_wrap_mcp_tool_outer_exception_handler(self):
        """Test wrap_mcp_tool outer exception handler (lines 264-267)."""
        mock_tool = Mock()
        mock_tool.name = "test_tool"
        
        # Create a mock that raises during attribute access to trigger outer except
        def failing_run(*args, **kwargs):
            # Raise an exception that will be caught by the outer except block
            raise Exception("Outer exception")
        
        mock_tool._run = failing_run
        
        wrapped_tool = wrap_mcp_tool(mock_tool)
        
        # This should trigger the outer exception handler (lines 264-267)
        result = wrapped_tool._run("arg1", kwarg1="value1")
        assert "Error executing tool: Outer exception" in result

    @pytest.mark.asyncio
    async def test_run_in_separate_process_script_write_error(self):
        """Test run_in_separate_process with script write error."""
        # Create a mock for open that raises an exception
        with patch('builtins.open', side_effect=IOError("Cannot write file")):
            result = await run_in_separate_process("test_tool", {"arg": "value"})
            
            assert "error" in result
            assert "Error running tool" in result["error"]
            assert "Cannot write file" in str(result["error"])

    @pytest.mark.asyncio
    async def test_stop_mcp_adapter_connection_without_close_method(self):
        """Test stopping adapter with connection that doesn't have close method."""
        mock_connection = Mock()
        # Remove close method if it exists
        if hasattr(mock_connection, 'close'):
            delattr(mock_connection, 'close')
        
        mock_adapter = Mock()
        mock_adapter.stop = AsyncMock()
        mock_adapter._connections = [mock_connection]
        
        await stop_mcp_adapter(mock_adapter)
        
        mock_adapter.stop.assert_called_once()
        # Should handle missing close method gracefully