"""Unit tests for MCPAdapter."""

import asyncio
import time
import pytest
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from typing import Dict, Any, List

from src.core.exceptions import MCPConnectionError
from src.engines.common.mcp_adapter import MCPAdapter, MCPTool, _extract_error_summary, _is_http_auth_error, _log_exception_group


class TestMCPAdapter:
    """Test suite for MCPAdapter."""
    
    @pytest.fixture
    def server_params(self) -> Dict[str, Any]:
        """Create test server parameters."""
        return {
            'url': 'https://test.mcp.server/api/mcp/',
            'timeout_seconds': 30,
            'max_retries': 3,
            'rate_limit': 60,
            'headers': {'Authorization': 'Bearer test-token'}
        }
    
    @pytest.fixture
    def mock_mcp_tool(self) -> Dict[str, Any]:
        """Create a mock MCP tool dictionary."""
        return {
            'name': 'test_tool',
            'description': 'A test tool',
            'mcp_tool': Mock(name='test_tool', description='A test tool'),
            'input_schema': {
                'properties': {
                    'input': {'type': 'string', 'description': 'Test input'}
                },
                'required': ['input']
            },
            'adapter': None
        }
    
    def test_adapter_initialization(self, server_params):
        """Test MCPAdapter initialization."""
        adapter = MCPAdapter(server_params)

        assert adapter.server_url == server_params['url']
        assert adapter.timeout_seconds == server_params['timeout_seconds']
        assert adapter.max_retries == server_params['max_retries']
        assert adapter.rate_limit == server_params['rate_limit']
        assert adapter._tools == []
        assert adapter._initialized is False
        assert adapter.initialization_error is None
    
    @pytest.mark.asyncio
    async def test_initialize_success(self, server_params):
        """Test successful adapter initialization."""
        adapter = MCPAdapter(server_params)
        
        # Mock the authentication and discovery methods
        with patch.object(adapter, '_get_authentication_headers', new_callable=AsyncMock) as mock_auth:
            with patch.object(adapter, '_discover_tools_with_mcp_client', new_callable=AsyncMock) as mock_discover:
                mock_auth.return_value = {'Authorization': 'Bearer token'}
                mock_discover.return_value = [
                    {'name': 'tool1', 'description': 'Tool 1'},
                    {'name': 'tool2', 'description': 'Tool 2'}
                ]
                
                await adapter.initialize()
                
                assert adapter._initialized is True
                assert len(adapter._tools) == 2
                mock_auth.assert_called_once()
                mock_discover.assert_called_once_with({'Authorization': 'Bearer token'})
    
    @pytest.mark.asyncio
    async def test_initialize_no_auth_headers(self, server_params):
        """Test initialization when authentication fails sets MCPConnectionError."""
        adapter = MCPAdapter(server_params)

        with patch.object(adapter, '_get_authentication_headers', new_callable=AsyncMock) as mock_auth:
            mock_auth.return_value = None

            await adapter.initialize()

            assert adapter._initialized is True
            assert adapter._tools == []
            assert adapter.initialization_error is not None
            assert isinstance(adapter.initialization_error, MCPConnectionError)
            assert "authentication headers" in adapter.initialization_error.detail
    
    @pytest.mark.asyncio
    async def test_initialize_exception(self, server_params):
        """Test initialization handles exceptions gracefully and sets MCPConnectionError."""
        adapter = MCPAdapter(server_params)

        with patch.object(adapter, '_get_authentication_headers', new_callable=AsyncMock) as mock_auth:
            mock_auth.side_effect = Exception("Auth failed")

            await adapter.initialize()

            assert adapter._initialized is True
            assert adapter._tools == []
            assert adapter.initialization_error is not None
            assert isinstance(adapter.initialization_error, MCPConnectionError)
            assert "Auth failed" in adapter.initialization_error.detail
    
    @pytest.mark.asyncio
    async def test_discover_tools_with_mcp_client(self, server_params):
        """Test tool discovery using MCP client."""
        adapter = MCPAdapter(server_params)
        headers = {'Authorization': 'Bearer token'}
        
        # Mock MCP client imports and behavior
        mock_tool = Mock()
        mock_tool.name = 'test_tool'
        mock_tool.description = 'Test tool description'
        mock_tool.inputSchema = {'type': 'object'}
        
        mock_tools_result = Mock()
        mock_tools_result.tools = [mock_tool]
        
        mock_session = AsyncMock()
        mock_session.initialize = AsyncMock()
        mock_session.list_tools = AsyncMock(return_value=mock_tools_result)
        
        mock_read_stream = Mock()
        mock_write_stream = Mock()
        
        with patch('mcp.client.streamable_http.streamablehttp_client') as mock_connect:
            with patch('mcp.ClientSession') as mock_client_session:
                # Setup async context managers
                mock_connect.return_value.__aenter__.return_value = (mock_read_stream, mock_write_stream, None)
                mock_client_session.return_value.__aenter__.return_value = mock_session
                
                tools = await adapter._discover_tools_with_mcp_client(headers)
                
                assert len(tools) == 1
                assert tools[0]['name'] == 'test_tool'
                assert tools[0]['description'] == 'Test tool description'
                assert tools[0]['adapter'] == adapter
                
                mock_connect.assert_called_once_with(adapter.server_url, headers={'Authorization': 'Bearer token'}, timeout=30)
                mock_session.initialize.assert_called_once()
                mock_session.list_tools.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_discover_tools_no_tools_found(self, server_params):
        """Test tool discovery when no tools are found on either transport."""
        adapter = MCPAdapter(server_params)
        headers = {'Authorization': 'Bearer token'}

        mock_tools_result = Mock()
        mock_tools_result.tools = None

        mock_session = AsyncMock()
        mock_session.initialize = AsyncMock()
        mock_session.list_tools = AsyncMock(return_value=mock_tools_result)

        with patch('mcp.client.streamable_http.streamablehttp_client') as mock_connect:
            with patch('mcp.client.sse.sse_client') as mock_sse:
                with patch('mcp.ClientSession') as mock_client_session:
                    mock_connect.return_value.__aenter__.return_value = (Mock(), Mock(), None)
                    mock_sse.return_value.__aenter__.return_value = (Mock(), Mock())
                    mock_client_session.return_value.__aenter__.return_value = mock_session

                    tools = await adapter._discover_tools_with_mcp_client(headers)

                    assert tools == []

    @pytest.mark.asyncio
    async def test_discover_tools_exception(self, server_params):
        """Test tool discovery handles exceptions on both transports and sets MCPConnectionError."""
        adapter = MCPAdapter(server_params)
        headers = {'Authorization': 'Bearer token'}

        with patch('mcp.client.streamable_http.streamablehttp_client') as mock_connect:
            with patch('mcp.client.sse.sse_client') as mock_sse:
                mock_connect.side_effect = Exception("Connection failed")
                mock_sse.side_effect = Exception("SSE also failed")

                tools = await adapter._discover_tools_with_mcp_client(headers)

                assert tools == []
                assert adapter.initialization_error is not None
                assert isinstance(adapter.initialization_error, MCPConnectionError)
                assert "SSE also failed" in adapter.initialization_error.detail

    @pytest.mark.asyncio
    async def test_discover_tools_403_error(self, server_params):
        """Test that 403 Forbidden errors are captured as MCPConnectionError with HTTP status."""
        adapter = MCPAdapter(server_params)
        headers = {'Authorization': 'Bearer token'}

        # Simulate httpx.HTTPStatusError with 403
        mock_response = Mock()
        mock_response.status_code = 403
        http_error = Exception("Client error '403 Forbidden'")
        http_error.response = mock_response

        with patch('mcp.client.streamable_http.streamablehttp_client') as mock_connect:
            with patch('mcp.client.sse.sse_client') as mock_sse:
                mock_connect.side_effect = http_error
                mock_sse.side_effect = http_error

                tools = await adapter._discover_tools_with_mcp_client(headers)

                assert tools == []
                assert adapter.initialization_error is not None
                assert isinstance(adapter.initialization_error, MCPConnectionError)
                assert "403" in adapter.initialization_error.detail

    @pytest.mark.asyncio
    async def test_discover_tools_sse_fallback(self, server_params):
        """Test SSE fallback when streamable HTTP fails."""
        adapter = MCPAdapter(server_params)
        headers = {'Authorization': 'Bearer token'}

        mock_tool = Mock()
        mock_tool.name = 'sse_tool'
        mock_tool.description = 'SSE discovered tool'
        mock_tool.inputSchema = {'type': 'object'}

        mock_tools_result = Mock()
        mock_tools_result.tools = [mock_tool]

        mock_session = AsyncMock()
        mock_session.initialize = AsyncMock()
        mock_session.list_tools = AsyncMock(return_value=mock_tools_result)

        with patch('mcp.client.streamable_http.streamablehttp_client') as mock_connect:
            with patch('mcp.client.sse.sse_client') as mock_sse:
                with patch('mcp.ClientSession') as mock_client_session:
                    # Streamable HTTP fails
                    mock_connect.side_effect = Exception("Streamable HTTP failed")
                    # SSE succeeds
                    mock_sse.return_value.__aenter__.return_value = (Mock(), Mock())
                    mock_client_session.return_value.__aenter__.return_value = mock_session

                    tools = await adapter._discover_tools_with_mcp_client(headers)

                    assert len(tools) == 1
                    assert tools[0]['name'] == 'sse_tool'
                    assert adapter._transport == "sse"
    
    @pytest.mark.asyncio
    async def test_execute_tool(self, server_params):
        """Test tool execution via streamable HTTP (default transport)."""
        adapter = MCPAdapter(server_params)

        tool_name = 'test_tool'
        parameters = {'input': 'test'}

        # Mock the result
        mock_result = Mock()
        mock_result.content = [Mock(text='Tool executed successfully')]

        mock_session = AsyncMock()
        mock_session.initialize = AsyncMock()
        mock_session.call_tool = AsyncMock(return_value=mock_result)

        with patch('mcp.client.streamable_http.streamablehttp_client') as mock_connect:
            with patch('mcp.ClientSession') as mock_client_session:
                mock_connect.return_value.__aenter__.return_value = (Mock(), Mock(), None)
                mock_client_session.return_value.__aenter__.return_value = mock_session

                result = await adapter.execute_tool(tool_name, parameters)

                assert result == mock_result
                mock_session.call_tool.assert_called_once_with(tool_name, parameters)

    @pytest.mark.asyncio
    async def test_execute_tool_sse_transport(self, server_params):
        """Test tool execution via SSE transport."""
        adapter = MCPAdapter(server_params)
        adapter._transport = "sse"

        tool_name = 'test_tool'
        parameters = {'input': 'test'}

        mock_result = Mock()
        mock_session = AsyncMock()
        mock_session.initialize = AsyncMock()
        mock_session.call_tool = AsyncMock(return_value=mock_result)

        with patch('mcp.client.sse.sse_client') as mock_sse:
            with patch('mcp.ClientSession') as mock_client_session:
                mock_sse.return_value.__aenter__.return_value = (Mock(), Mock())
                mock_client_session.return_value.__aenter__.return_value = mock_session

                result = await adapter.execute_tool(tool_name, parameters)

                assert result == mock_result
                mock_session.call_tool.assert_called_once_with(tool_name, parameters)
    
    @pytest.mark.asyncio
    async def test_execute_tool_no_auth(self):
        """Test tool execution without authentication."""
        server_params = {'url': 'https://test.server/'}
        adapter = MCPAdapter(server_params)
        
        with patch.object(adapter, '_get_authentication_headers', new_callable=AsyncMock) as mock_auth:
            mock_auth.return_value = None
            
            with pytest.raises(ValueError, match="No authentication headers available"):
                await adapter.execute_tool('test_tool', {})
    
    @pytest.mark.asyncio
    async def test_get_authentication_headers_provided(self, server_params):
        """Test getting authentication headers when provided."""
        adapter = MCPAdapter(server_params)
        
        headers = await adapter._get_authentication_headers()
        
        assert headers == {'Authorization': 'Bearer test-token'}
    
    @pytest.mark.asyncio
    async def test_get_authentication_headers_fallback(self):
        """Test getting authentication headers using fallback."""
        server_params = {'url': 'https://test.server/'}
        adapter = MCPAdapter(server_params)
        
        with patch('src.utils.databricks_auth.get_mcp_auth_headers', new_callable=AsyncMock) as mock_get_auth:
            mock_get_auth.return_value = ({'Authorization': 'Bearer fallback-token'}, None)
            
            headers = await adapter._get_authentication_headers()
            
            assert headers == {'Authorization': 'Bearer fallback-token'}
            mock_get_auth.assert_called_once_with(
                adapter.server_url,
                user_token=None,
                api_key=None,
                include_sse_headers=False
            )
    
    @pytest.mark.asyncio
    async def test_get_authentication_headers_fallback_error(self):
        """Test getting authentication headers when fallback fails."""
        server_params = {'url': 'https://test.server/'}
        adapter = MCPAdapter(server_params)
        
        with patch('src.utils.databricks_auth.get_mcp_auth_headers', new_callable=AsyncMock) as mock_get_auth:
            mock_get_auth.return_value = (None, "Auth failed")
            
            headers = await adapter._get_authentication_headers()
            
            assert headers is None
    
    def test_tools_property(self, server_params):
        """Test tools property."""
        adapter = MCPAdapter(server_params)
        
        # Initially empty
        assert adapter.tools == []
        
        # Set some tools
        adapter._tools = [{'name': 'tool1'}, {'name': 'tool2'}]
        assert len(adapter.tools) == 2
        
        # Test None case
        adapter._tools = None
        assert adapter.tools == []
    
    @pytest.mark.asyncio
    async def test_stop(self, server_params):
        """Test adapter stop method."""
        adapter = MCPAdapter(server_params)
        
        # Should not raise any exceptions
        await adapter.stop()
    
    @pytest.mark.asyncio
    async def test_close(self, server_params):
        """Test adapter close method."""
        adapter = MCPAdapter(server_params)
        
        with patch.object(adapter, 'stop', new_callable=AsyncMock) as mock_stop:
            await adapter.close()
            mock_stop.assert_called_once()


class TestMCPTool:
    """Test suite for MCPTool."""
    
    @pytest.fixture
    def tool_wrapper(self) -> Dict[str, Any]:
        """Create a test tool wrapper."""
        return {
            'name': 'test_tool',
            'description': 'A test tool',
            'input_schema': {'type': 'object'},
            'mcp_tool': Mock(),
            'adapter': Mock()
        }
    
    def test_tool_initialization(self, tool_wrapper):
        """Test MCPTool initialization."""
        tool = MCPTool(tool_wrapper)
        
        assert tool.name == 'test_tool'
        assert tool.description == 'A test tool'
        assert tool.input_schema == {'type': 'object'}
        assert tool.mcp_tool is not None
        assert tool.adapter is not None
    
    def test_tool_initialization_defaults(self):
        """Test MCPTool initialization with defaults."""
        tool = MCPTool({})
        
        assert tool.name == 'unknown'
        assert tool.description == ''
        assert tool.input_schema == {}
        assert tool.mcp_tool is None
        assert tool.adapter is None
    
    @pytest.mark.asyncio
    async def test_execute_success(self, tool_wrapper):
        """Test successful tool execution."""
        mock_adapter = AsyncMock()
        mock_adapter.execute_tool = AsyncMock(return_value="Success")
        tool_wrapper['adapter'] = mock_adapter
        
        tool = MCPTool(tool_wrapper)
        result = await tool.execute({'input': 'test'})
        
        assert result == "Success"
        mock_adapter.execute_tool.assert_called_once_with('test_tool', {'input': 'test'})
    
    @pytest.mark.asyncio
    async def test_execute_no_adapter(self, tool_wrapper):
        """Test tool execution without adapter."""
        tool_wrapper['adapter'] = None
        tool = MCPTool(tool_wrapper)
        
        with pytest.raises(ValueError, match="No MCP adapter available"):
            await tool.execute({'input': 'test'})
    
    @pytest.mark.asyncio
    async def test_execute_exception(self, tool_wrapper):
        """Test tool execution with exception."""
        mock_adapter = AsyncMock()
        mock_adapter.execute_tool = AsyncMock(side_effect=Exception("Execution failed"))
        tool_wrapper['adapter'] = mock_adapter
        
        tool = MCPTool(tool_wrapper)
        
        with pytest.raises(Exception, match="Execution failed"):
            await tool.execute({'input': 'test'})
    
    def test_str_representation(self, tool_wrapper):
        """Test string representation of MCPTool."""
        tool = MCPTool(tool_wrapper)

        assert str(tool) == "MCPTool(name=test_tool, description=A test tool)"


class TestMCPAdapterRetry:
    """Test retry logic in MCPAdapter.execute_tool."""

    @pytest.fixture
    def adapter(self) -> MCPAdapter:
        return MCPAdapter({
            'url': 'https://test.mcp.server/api/mcp/',
            'timeout_seconds': 5,
            'max_retries': 3,
            'rate_limit': 0,  # disable rate limiting for retry tests
            'headers': {'Authorization': 'Bearer test-token'}
        })

    @pytest.mark.asyncio
    async def test_retry_succeeds_on_second_attempt(self, adapter):
        """Transient error on first attempt, success on second."""
        mock_result = Mock()

        mock_session = AsyncMock()
        mock_session.initialize = AsyncMock()
        mock_session.call_tool = AsyncMock(return_value=mock_result)

        call_count = 0

        class FakeConnect:
            async def __aenter__(self_inner):
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    raise ConnectionError("transient failure")
                return (Mock(), Mock(), None)

            async def __aexit__(self_inner, *args):
                pass

        with patch('mcp.client.streamable_http.streamablehttp_client', return_value=FakeConnect()):
            with patch('mcp.ClientSession') as mock_cs:
                mock_cs.return_value.__aenter__.return_value = mock_session
                with patch('asyncio.sleep', new_callable=AsyncMock) as mock_sleep:
                    result = await adapter.execute_tool('test_tool', {})

        assert result == mock_result
        assert call_count == 2
        mock_sleep.assert_called_once_with(1)  # 2^0 = 1

    @pytest.mark.asyncio
    async def test_retry_exhaustion_raises(self, adapter):
        """All attempts fail with transient error -> raises last error."""
        with patch('mcp.client.streamable_http.streamablehttp_client') as mock_connect:
            mock_connect.side_effect = ConnectionError("persistent failure")
            with patch('asyncio.sleep', new_callable=AsyncMock):
                with pytest.raises(ConnectionError, match="persistent failure"):
                    await adapter.execute_tool('test_tool', {})

    @pytest.mark.asyncio
    async def test_non_transient_error_no_retry(self, adapter):
        """Non-transient errors (e.g. ValueError) are raised immediately without retry."""
        mock_session = AsyncMock()
        mock_session.initialize = AsyncMock()
        mock_session.call_tool = AsyncMock(side_effect=ValueError("bad input"))

        with patch('mcp.client.streamable_http.streamablehttp_client') as mock_connect:
            with patch('mcp.ClientSession') as mock_cs:
                mock_connect.return_value.__aenter__.return_value = (Mock(), Mock(), None)
                mock_cs.return_value.__aenter__.return_value = mock_session
                with pytest.raises(ValueError, match="bad input"):
                    await adapter.execute_tool('test_tool', {})

    @pytest.mark.asyncio
    async def test_timeout_passed_to_client(self, adapter):
        """Verify timeout_seconds is passed to streamablehttp_client."""
        mock_session = AsyncMock()
        mock_session.initialize = AsyncMock()
        mock_session.call_tool = AsyncMock(return_value=Mock())

        with patch('mcp.client.streamable_http.streamablehttp_client') as mock_connect:
            with patch('mcp.ClientSession') as mock_cs:
                mock_connect.return_value.__aenter__.return_value = (Mock(), Mock(), None)
                mock_cs.return_value.__aenter__.return_value = mock_session

                await adapter.execute_tool('test_tool', {})

                mock_connect.assert_called_once_with(
                    adapter.server_url,
                    headers={'Authorization': 'Bearer test-token'},
                    timeout=5
                )


class TestMCPAdapterRateLimit:
    """Test rate limiting in MCPAdapter."""

    @pytest.mark.asyncio
    async def test_rate_limit_waits_when_exceeded(self):
        """When rate limit is hit, _wait_for_rate_limit should sleep."""
        adapter = MCPAdapter({
            'url': 'https://test.mcp.server/api/mcp/',
            'rate_limit': 2,
            'headers': {'Authorization': 'Bearer tok'}
        })

        now = time.monotonic()
        # Simulate 2 calls within the last 60s
        adapter._call_timestamps.append(now - 10)
        adapter._call_timestamps.append(now - 5)

        with patch('asyncio.sleep', new_callable=AsyncMock) as mock_sleep:
            await adapter._wait_for_rate_limit()
            # Should have been called with wait_time > 0
            mock_sleep.assert_called_once()
            wait_arg = mock_sleep.call_args[0][0]
            assert wait_arg > 0

    @pytest.mark.asyncio
    async def test_rate_limit_no_wait_when_under_limit(self):
        """No waiting when under the rate limit."""
        adapter = MCPAdapter({
            'url': 'https://test.mcp.server/api/mcp/',
            'rate_limit': 10,
            'headers': {'Authorization': 'Bearer tok'}
        })

        now = time.monotonic()
        adapter._call_timestamps.append(now - 5)

        with patch('asyncio.sleep', new_callable=AsyncMock) as mock_sleep:
            await adapter._wait_for_rate_limit()
            mock_sleep.assert_not_called()

    @pytest.mark.asyncio
    async def test_rate_limit_disabled_when_zero(self):
        """Rate limiting is skipped when rate_limit <= 0."""
        adapter = MCPAdapter({
            'url': 'https://test.mcp.server/api/mcp/',
            'rate_limit': 0,
            'headers': {'Authorization': 'Bearer tok'}
        })

        # Fill timestamps - should still not wait
        now = time.monotonic()
        for i in range(100):
            adapter._call_timestamps.append(now - 1)

        with patch('asyncio.sleep', new_callable=AsyncMock) as mock_sleep:
            await adapter._wait_for_rate_limit()
            mock_sleep.assert_not_called()

    @pytest.mark.asyncio
    async def test_old_timestamps_pruned(self):
        """Timestamps older than 60s should be removed."""
        adapter = MCPAdapter({
            'url': 'https://test.mcp.server/api/mcp/',
            'rate_limit': 5,
            'headers': {'Authorization': 'Bearer tok'}
        })

        now = time.monotonic()
        # Add old timestamps (>60s ago) and fresh ones
        adapter._call_timestamps.append(now - 120)
        adapter._call_timestamps.append(now - 90)
        adapter._call_timestamps.append(now - 5)

        await adapter._wait_for_rate_limit()

        # Old ones should be pruned, only the recent one remains
        assert len(adapter._call_timestamps) == 1


class TestConvertParameters:
    """Test _convert_parameters type conversion logic."""

    @pytest.fixture
    def adapter(self) -> MCPAdapter:
        adapter = MCPAdapter({
            'url': 'https://test.mcp.server/api/mcp/',
            'headers': {'Authorization': 'Bearer tok'}
        })
        return adapter

    def test_no_schema_returns_original(self, adapter):
        """When no schema found for tool, return params as-is."""
        params = {'a': '1'}
        result = adapter._convert_parameters('unknown_tool', params)
        assert result == params

    def test_no_properties_returns_original(self, adapter):
        """When schema has no properties, return params as-is."""
        adapter._tool_schemas['tool'] = {'type': 'object'}
        result = adapter._convert_parameters('tool', {'a': '1'})
        assert result == {'a': '1'}

    def test_convert_number(self, adapter):
        """String to float conversion."""
        adapter._tool_schemas['tool'] = {
            'properties': {'val': {'type': 'number'}}
        }
        result = adapter._convert_parameters('tool', {'val': '3.14'})
        assert result == {'val': 3.14}

    def test_convert_number_non_string(self, adapter):
        """Non-string number stays as float."""
        adapter._tool_schemas['tool'] = {
            'properties': {'val': {'type': 'number'}}
        }
        result = adapter._convert_parameters('tool', {'val': 5})
        assert result == {'val': 5.0}

    def test_convert_integer(self, adapter):
        """String to int conversion."""
        adapter._tool_schemas['tool'] = {
            'properties': {'val': {'type': 'integer'}}
        }
        result = adapter._convert_parameters('tool', {'val': '42'})
        assert result == {'val': 42}

    def test_convert_integer_non_string(self, adapter):
        """Non-string int stays as int."""
        adapter._tool_schemas['tool'] = {
            'properties': {'val': {'type': 'integer'}}
        }
        result = adapter._convert_parameters('tool', {'val': 7})
        assert result == {'val': 7}

    def test_convert_boolean_true(self, adapter):
        """String 'true'/'1'/'yes' to True."""
        adapter._tool_schemas['tool'] = {
            'properties': {'flag': {'type': 'boolean'}}
        }
        for val in ('true', 'True', '1', 'yes'):
            result = adapter._convert_parameters('tool', {'flag': val})
            assert result == {'flag': True}, f"Failed for {val}"

    def test_convert_boolean_false(self, adapter):
        """Other strings convert to False."""
        adapter._tool_schemas['tool'] = {
            'properties': {'flag': {'type': 'boolean'}}
        }
        result = adapter._convert_parameters('tool', {'flag': 'false'})
        assert result == {'flag': False}

    def test_convert_boolean_non_string(self, adapter):
        """Non-string bool stays as bool."""
        adapter._tool_schemas['tool'] = {
            'properties': {'flag': {'type': 'boolean'}}
        }
        result = adapter._convert_parameters('tool', {'flag': 1})
        assert result == {'flag': True}

    def test_convert_array_from_json_string(self, adapter):
        """JSON string parsed to array."""
        adapter._tool_schemas['tool'] = {
            'properties': {'items': {'type': 'array'}}
        }
        result = adapter._convert_parameters('tool', {'items': '[1, 2, 3]'})
        assert result == {'items': [1, 2, 3]}

    def test_convert_array_invalid_json(self, adapter):
        """Invalid JSON string for array is skipped."""
        adapter._tool_schemas['tool'] = {
            'properties': {'items': {'type': 'array'}}
        }
        result = adapter._convert_parameters('tool', {'items': 'not-json'})
        assert 'items' not in result

    def test_convert_array_non_string(self, adapter):
        """Non-string array passed through."""
        adapter._tool_schemas['tool'] = {
            'properties': {'items': {'type': 'array'}}
        }
        result = adapter._convert_parameters('tool', {'items': [1, 2]})
        assert result == {'items': [1, 2]}

    def test_convert_object_from_json_string(self, adapter):
        """JSON string parsed to object."""
        adapter._tool_schemas['tool'] = {
            'properties': {'config': {'type': 'object'}}
        }
        result = adapter._convert_parameters('tool', {'config': '{"a": 1}'})
        assert result == {'config': {'a': 1}}

    def test_convert_object_invalid_json(self, adapter):
        """Invalid JSON string for object is skipped."""
        adapter._tool_schemas['tool'] = {
            'properties': {'config': {'type': 'object'}}
        }
        result = adapter._convert_parameters('tool', {'config': '{bad}'})
        assert 'config' not in result

    def test_convert_object_non_string(self, adapter):
        """Non-string object passed through."""
        adapter._tool_schemas['tool'] = {
            'properties': {'config': {'type': 'object'}}
        }
        result = adapter._convert_parameters('tool', {'config': {'a': 1}})
        assert result == {'config': {'a': 1}}

    def test_string_type_passthrough(self, adapter):
        """String type stays as-is."""
        adapter._tool_schemas['tool'] = {
            'properties': {'name': {'type': 'string'}}
        }
        result = adapter._convert_parameters('tool', {'name': 'hello'})
        assert result == {'name': 'hello'}

    def test_unknown_type_passthrough(self, adapter):
        """Unknown type stays as-is."""
        adapter._tool_schemas['tool'] = {
            'properties': {'x': {'type': 'custom'}}
        }
        result = adapter._convert_parameters('tool', {'x': 'val'})
        assert result == {'x': 'val'}

    def test_no_type_in_schema(self, adapter):
        """Parameter with no type in schema passes through."""
        adapter._tool_schemas['tool'] = {
            'properties': {'x': {}}
        }
        result = adapter._convert_parameters('tool', {'x': 'val'})
        assert result == {'x': 'val'}

    def test_skip_none_values(self, adapter):
        """None values are skipped."""
        adapter._tool_schemas['tool'] = {
            'properties': {'a': {'type': 'string'}}
        }
        result = adapter._convert_parameters('tool', {'a': None})
        assert result == {}

    def test_skip_empty_string(self, adapter):
        """Empty strings are skipped."""
        adapter._tool_schemas['tool'] = {
            'properties': {'a': {'type': 'string'}}
        }
        result = adapter._convert_parameters('tool', {'a': ''})
        assert result == {}

    def test_skip_null_string(self, adapter):
        """'null' string values are skipped."""
        adapter._tool_schemas['tool'] = {
            'properties': {'a': {'type': 'string'}}
        }
        result = adapter._convert_parameters('tool', {'a': 'null'})
        assert result == {}

    def test_enum_validation_valid(self, adapter):
        """Valid enum value passes."""
        adapter._tool_schemas['tool'] = {
            'properties': {'color': {'type': 'string', 'enum': ['red', 'blue']}}
        }
        result = adapter._convert_parameters('tool', {'color': 'red'})
        assert result == {'color': 'red'}

    def test_enum_validation_invalid(self, adapter):
        """Invalid enum value is removed."""
        adapter._tool_schemas['tool'] = {
            'properties': {'color': {'type': 'string', 'enum': ['red', 'blue']}}
        }
        result = adapter._convert_parameters('tool', {'color': 'green'})
        assert 'color' not in result

    def test_conversion_error_skips_param(self, adapter):
        """ValueError during conversion skips the parameter."""
        adapter._tool_schemas['tool'] = {
            'properties': {'val': {'type': 'integer'}}
        }
        result = adapter._convert_parameters('tool', {'val': 'not-a-number'})
        assert 'val' not in result

    def test_multiple_params_mixed(self, adapter):
        """Multiple parameters with different types convert correctly."""
        adapter._tool_schemas['tool'] = {
            'properties': {
                'count': {'type': 'integer'},
                'name': {'type': 'string'},
                'active': {'type': 'boolean'},
                'skip': {'type': 'string'},
            }
        }
        result = adapter._convert_parameters('tool', {
            'count': '5',
            'name': 'test',
            'active': 'true',
            'skip': None,
        })
        assert result == {'count': 5, 'name': 'test', 'active': True}

    def test_skipped_params_logged(self, adapter):
        """Parameters that are skipped get logged (coverage for log branch)."""
        adapter._tool_schemas['tool'] = {
            'properties': {'a': {'type': 'string'}}
        }
        # Pass a param not in properties (will be kept) + one None (will be skipped)
        result = adapter._convert_parameters('tool', {'a': None, 'b': 'val'})
        # 'a' skipped (None), 'b' kept (no type restriction)
        assert 'a' not in result
        assert result['b'] == 'val'

    def test_outer_exception_returns_original(self, adapter):
        """If an unexpected exception occurs, return original params."""
        adapter._tool_schemas['tool'] = {
            'properties': 'not-a-dict'  # Will cause iteration error
        }
        params = {'a': '1'}
        result = adapter._convert_parameters('tool', params)
        assert result == params


class TestAdapterEdgeCases:
    """Cover remaining edge cases for 100% coverage."""

    @pytest.mark.asyncio
    async def test_rate_limit_popleft_after_wait(self):
        """Cover the popleft branch after asyncio.sleep in _wait_for_rate_limit."""
        adapter = MCPAdapter({
            'url': 'https://test.mcp.server/api/mcp/',
            'rate_limit': 1,
            'headers': {'Authorization': 'Bearer tok'}
        })

        now = time.monotonic()
        # One call at now-30s (inside window), at capacity
        adapter._call_timestamps.append(now - 30)

        async def fake_sleep(duration):
            # After sleeping, the timestamp should be >60s old
            # Simulate time passing by manipulating the deque
            adapter._call_timestamps[0] = time.monotonic() - 120

        with patch('asyncio.sleep', side_effect=fake_sleep):
            await adapter._wait_for_rate_limit()

        # The old timestamp should have been pruned after wait
        assert len(adapter._call_timestamps) == 0

    @pytest.mark.asyncio
    async def test_get_auth_headers_exception(self):
        """Cover exception handler in _get_authentication_headers."""
        adapter = MCPAdapter({
            'url': 'https://test.mcp.server/api/mcp/',
        })

        with patch('src.utils.databricks_auth.get_mcp_auth_headers', new_callable=AsyncMock) as mock_auth:
            mock_auth.side_effect = RuntimeError("unexpected")
            result = await adapter._get_authentication_headers()

        assert result is None

    @pytest.mark.asyncio
    async def test_stop_exception_handled(self):
        """Cover exception handler in stop()."""
        adapter = MCPAdapter({
            'url': 'https://test.mcp.server/api/mcp/',
        })

        # Patch logger.info to raise, triggering the except branch
        with patch('src.engines.common.mcp_adapter.logger') as mock_logger:
            mock_logger.info.side_effect = RuntimeError("log error")
            # Should not raise
            await adapter.stop()
            mock_logger.error.assert_called_once()


class TestExtractErrorSummary:
    """Test suite for the _extract_error_summary standalone function."""

    def test_regular_exception_returns_str(self):
        """Regular exception returns str(exc)."""
        from src.engines.common.mcp_adapter import _extract_error_summary

        exc = Exception("something went wrong")
        result = _extract_error_summary(exc)
        assert result == "something went wrong"

    def test_exception_with_response_status_code(self):
        """Exception with .response.status_code returns 'HTTP {code} - {exc}' with body."""
        from src.engines.common.mcp_adapter import _extract_error_summary

        exc = Exception("Forbidden")
        mock_response = Mock()
        mock_response.status_code = 403
        mock_response.text = "Access denied"
        exc.response = mock_response

        result = _extract_error_summary(exc)
        assert "HTTP 403" in result
        assert "Forbidden" in result
        assert "Access denied" in result

    def test_exception_with_status_attribute(self):
        """Exception with .status attribute returns 'HTTP {status} - {exc}'."""
        from src.engines.common.mcp_adapter import _extract_error_summary

        exc = Exception("Unauthorized")
        exc.status = 401

        result = _extract_error_summary(exc)
        assert result == "HTTP 401 - Unauthorized"

    def test_exception_group_recurses_and_joins(self):
        """ExceptionGroup (has .exceptions list) recurses and joins with '; '."""
        from src.engines.common.mcp_adapter import _extract_error_summary

        sub1 = Exception("error one")
        sub2 = Exception("error two")
        group = Exception("group")
        group.exceptions = [sub1, sub2]

        result = _extract_error_summary(group)
        assert result == "error one; error two"

    def test_response_with_empty_body(self):
        """Response with empty body does not append body text."""
        from src.engines.common.mcp_adapter import _extract_error_summary

        exc = Exception("Server Error")
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = ""
        exc.response = mock_response

        result = _extract_error_summary(exc)
        assert "HTTP 500" in result
        assert "Server Error" in result
        assert "body:" not in result


class TestIsHttpAuthError:
    """Test suite for the _is_http_auth_error standalone function."""

    def test_response_status_code_403(self):
        """Returns True for exc with .response.status_code == 403."""
        from src.engines.common.mcp_adapter import _is_http_auth_error

        exc = Exception("Forbidden")
        mock_response = Mock()
        mock_response.status_code = 403
        exc.response = mock_response

        assert _is_http_auth_error(exc) is True

    def test_response_status_code_401(self):
        """Returns True for exc with .response.status_code == 401."""
        from src.engines.common.mcp_adapter import _is_http_auth_error

        exc = Exception("Unauthorized")
        mock_response = Mock()
        mock_response.status_code = 401
        exc.response = mock_response

        assert _is_http_auth_error(exc) is True

    def test_response_status_code_500_returns_false(self):
        """Returns False for .response.status_code == 500."""
        from src.engines.common.mcp_adapter import _is_http_auth_error

        exc = Exception("Server Error")
        mock_response = Mock()
        mock_response.status_code = 500
        exc.response = mock_response

        assert _is_http_auth_error(exc) is False

    def test_status_attribute_403(self):
        """Returns True for exc with .status == 403."""
        from src.engines.common.mcp_adapter import _is_http_auth_error

        exc = Exception("Forbidden")
        # Ensure no .response attribute so .status branch is reached
        exc.status = 403

        # Remove response attribute if Mock adds it
        if hasattr(exc, 'response'):
            delattr(exc, 'response')

        assert _is_http_auth_error(exc) is True

    def test_string_contains_403(self):
        """Returns True when '403' in str(exc)."""
        from src.engines.common.mcp_adapter import _is_http_auth_error

        exc = Exception("Client error 403 Forbidden")
        assert _is_http_auth_error(exc) is True

    def test_string_contains_401(self):
        """Returns True when '401' in str(exc)."""
        from src.engines.common.mcp_adapter import _is_http_auth_error

        exc = Exception("Client error 401 Unauthorized")
        assert _is_http_auth_error(exc) is True

    def test_non_auth_error_returns_false(self):
        """Returns False for non-auth errors."""
        from src.engines.common.mcp_adapter import _is_http_auth_error

        exc = Exception("Connection timed out")
        assert _is_http_auth_error(exc) is False

    def test_exception_group_with_auth_sub_exception(self):
        """Recurses into ExceptionGroup with one auth sub-exception."""
        from src.engines.common.mcp_adapter import _is_http_auth_error

        sub_auth = Exception("Forbidden")
        mock_response = Mock()
        mock_response.status_code = 403
        sub_auth.response = mock_response

        sub_other = Exception("timeout")

        group = Exception("group")
        group.exceptions = [sub_other, sub_auth]

        assert _is_http_auth_error(group) is True

    def test_exception_group_with_no_auth_sub_exceptions(self):
        """Returns False for ExceptionGroup with no auth sub-exceptions."""
        from src.engines.common.mcp_adapter import _is_http_auth_error

        sub1 = Exception("Connection reset")
        sub2 = Exception("DNS resolution failed")

        group = Exception("group")
        group.exceptions = [sub1, sub2]

        assert _is_http_auth_error(group) is False


class TestLogExceptionGroup:
    """Test suite for the _log_exception_group standalone function."""

    def test_logs_error_for_regular_exception(self):
        """Logs error for regular exception."""
        from src.engines.common.mcp_adapter import _log_exception_group

        exc = Exception("something broke")

        with patch('src.engines.common.mcp_adapter.logger') as mock_logger:
            _log_exception_group(exc, "test context")

            # Should log the main error message
            mock_logger.error.assert_any_call("test context: something broke")

    def test_logs_sub_exceptions_for_exception_group(self):
        """Logs sub-exceptions for ExceptionGroup."""
        from src.engines.common.mcp_adapter import _log_exception_group

        sub1 = Exception("sub error 1")
        sub2 = Exception("sub error 2")
        group = Exception("group error")
        group.exceptions = [sub1, sub2]

        with patch('src.engines.common.mcp_adapter.logger') as mock_logger:
            _log_exception_group(group, "ctx")

            # Check that sub-exceptions are logged
            error_calls = [str(call) for call in mock_logger.error.call_args_list]
            # Should contain sub-exception log lines
            assert any("sub-exception [0]" in call for call in error_calls)
            assert any("sub-exception [1]" in call for call in error_calls)

    def test_logs_response_body_for_sub_exception_with_response(self):
        """Logs response body for sub-exception with .response."""
        from src.engines.common.mcp_adapter import _log_exception_group

        sub = Exception("HTTP error")
        mock_response = Mock()
        mock_response.text = "Detailed error body"
        sub.response = mock_response
        sub.__traceback__ = None

        group = Exception("group")
        group.exceptions = [sub]

        with patch('src.engines.common.mcp_adapter.logger') as mock_logger:
            _log_exception_group(group, "ctx")

            error_calls = [str(call) for call in mock_logger.error.call_args_list]
            assert any("response body" in call for call in error_calls)
            assert any("Detailed error body" in call for call in error_calls)


class TestSPNFallbackHeaders:
    """Test suite for MCPAdapter._get_spn_fallback_headers async method."""

    @pytest.fixture
    def adapter_with_group(self) -> MCPAdapter:
        """Create an MCPAdapter with group_id in server_params."""
        return MCPAdapter({
            'url': 'https://test.mcp.server/api/mcp/',
            'headers': {'Authorization': 'Bearer test-token'},
            'group_id': 'test-group-123',
        })

    @pytest.mark.asyncio
    async def test_returns_headers_when_valid_context(self, adapter_with_group):
        """Returns headers when get_auth_context returns valid context."""
        mock_auth_context = Mock()
        mock_auth_context.token = "spn-token-value"
        mock_auth_context.auth_method = "service_principal"

        with patch('src.utils.databricks_auth.get_auth_context', new_callable=AsyncMock) as mock_get_auth:
            mock_get_auth.return_value = mock_auth_context

            result = await adapter_with_group._get_spn_fallback_headers()

            assert result == {"Authorization": "Bearer spn-token-value"}
            mock_get_auth.assert_called_once_with(user_token=None, group_id='test-group-123')

    @pytest.mark.asyncio
    async def test_returns_none_when_context_is_none(self, adapter_with_group):
        """Returns None when get_auth_context returns None."""
        with patch('src.utils.databricks_auth.get_auth_context', new_callable=AsyncMock) as mock_get_auth:
            mock_get_auth.return_value = None

            result = await adapter_with_group._get_spn_fallback_headers()

            assert result is None
            mock_get_auth.assert_called_once_with(user_token=None, group_id='test-group-123')

    @pytest.mark.asyncio
    async def test_returns_none_when_context_raises_exception(self, adapter_with_group):
        """Returns None when get_auth_context raises exception."""
        with patch('src.utils.databricks_auth.get_auth_context', new_callable=AsyncMock) as mock_get_auth:
            mock_get_auth.side_effect = RuntimeError("auth service unavailable")

            result = await adapter_with_group._get_spn_fallback_headers()

            assert result is None


class TestSPNFallbackDiscovery:
    """Test SPN fallback during tool discovery when OBO is rejected."""

    @pytest.fixture
    def databricks_spn_params(self):
        return {
            'url': 'https://example.com/api/2.0/mcp/external/test',
            'timeout_seconds': 30,
            'max_retries': 3,
            'rate_limit': 60,
            'headers': {'Authorization': 'Bearer obo-token'},
            'auth_type': 'databricks_spn',
        }

    def _make_403_error(self):
        """Create a mock 403 HTTP error wrapped in ExceptionGroup."""
        exc = Exception("Client error '403 Forbidden'")
        resp = Mock()
        resp.status_code = 403
        exc.response = resp
        return exc

    @pytest.mark.asyncio
    async def test_spn_fallback_streamable_http_success(self, databricks_spn_params):
        """OBO rejected → SPN fallback succeeds via streamable HTTP → headers saved."""
        adapter = MCPAdapter(databricks_spn_params)
        obo_error = self._make_403_error()
        spn_tools = [{'name': 'tool1', 'description': 'desc', 'mcp_tool': Mock(), 'input_schema': {}}]

        with patch.object(adapter, '_discover_via_streamable_http', new_callable=AsyncMock) as mock_sh, \
             patch.object(adapter, '_discover_via_sse', new_callable=AsyncMock) as mock_sse, \
             patch.object(adapter, '_get_spn_fallback_headers', new_callable=AsyncMock) as mock_spn:
            # First streamable HTTP fails with 403
            mock_sh.side_effect = [obo_error, spn_tools]
            # SSE also fails with 403
            mock_sse.side_effect = obo_error
            mock_spn.return_value = {"Authorization": "Bearer spn-token-abc"}

            result = await adapter._discover_tools_with_mcp_client(
                {"Authorization": "Bearer obo-token"}
            )

        assert len(result) == 1
        assert adapter._spn_fallback_headers == {"Authorization": "Bearer spn-token-abc"}
        assert adapter._transport == "streamable_http"

    @pytest.mark.asyncio
    async def test_spn_fallback_sse_success(self, databricks_spn_params):
        """OBO rejected → SPN streamable fails → SPN SSE succeeds → headers saved."""
        adapter = MCPAdapter(databricks_spn_params)
        obo_error = self._make_403_error()
        spn_tools = [{'name': 'tool1', 'description': 'desc', 'mcp_tool': Mock(), 'input_schema': {}}]

        with patch.object(adapter, '_discover_via_streamable_http', new_callable=AsyncMock) as mock_sh, \
             patch.object(adapter, '_discover_via_sse', new_callable=AsyncMock) as mock_sse, \
             patch.object(adapter, '_get_spn_fallback_headers', new_callable=AsyncMock) as mock_spn:
            mock_sh.side_effect = [obo_error, Exception("SPN streamable also fails")]
            mock_sse.side_effect = [obo_error, spn_tools]
            mock_spn.return_value = {"Authorization": "Bearer spn-token-xyz"}

            result = await adapter._discover_tools_with_mcp_client(
                {"Authorization": "Bearer obo-token"}
            )

        assert len(result) == 1
        assert adapter._spn_fallback_headers == {"Authorization": "Bearer spn-token-xyz"}
        assert adapter._transport == "sse"

    @pytest.mark.asyncio
    async def test_spn_fallback_no_credentials(self, databricks_spn_params):
        """OBO rejected → no SPN credentials available → returns empty, logs warning."""
        adapter = MCPAdapter(databricks_spn_params)
        obo_error = self._make_403_error()

        with patch.object(adapter, '_discover_via_streamable_http', new_callable=AsyncMock) as mock_sh, \
             patch.object(adapter, '_discover_via_sse', new_callable=AsyncMock) as mock_sse, \
             patch.object(adapter, '_get_spn_fallback_headers', new_callable=AsyncMock) as mock_spn:
            mock_sh.side_effect = obo_error
            mock_sse.side_effect = obo_error
            mock_spn.return_value = None

            result = await adapter._discover_tools_with_mcp_client(
                {"Authorization": "Bearer obo-token"}
            )

        assert result == []
        assert adapter._spn_fallback_headers is None
        assert adapter.initialization_error is not None

    @pytest.mark.asyncio
    async def test_spn_fallback_not_triggered_for_non_databricks_auth(self):
        """Non-Databricks auth type should NOT trigger SPN fallback."""
        params = {
            'url': 'https://example.com/mcp',
            'timeout_seconds': 30,
            'max_retries': 3,
            'rate_limit': 60,
            'headers': {'Authorization': 'Bearer api-key'},
            'auth_type': 'api_key',
        }
        adapter = MCPAdapter(params)
        error_403 = self._make_403_error()

        with patch.object(adapter, '_discover_via_streamable_http', new_callable=AsyncMock) as mock_sh, \
             patch.object(adapter, '_discover_via_sse', new_callable=AsyncMock) as mock_sse, \
             patch.object(adapter, '_get_spn_fallback_headers', new_callable=AsyncMock) as mock_spn:
            mock_sh.side_effect = error_403
            mock_sse.side_effect = error_403

            result = await adapter._discover_tools_with_mcp_client(
                {"Authorization": "Bearer api-key"}
            )

        mock_spn.assert_not_called()
        assert result == []

    @pytest.mark.asyncio
    async def test_spn_fallback_both_transports_fail(self, databricks_spn_params):
        """OBO rejected → SPN obtained but both transports fail → returns empty."""
        adapter = MCPAdapter(databricks_spn_params)
        obo_error = self._make_403_error()
        spn_error = Exception("SPN also rejected")

        with patch.object(adapter, '_discover_via_streamable_http', new_callable=AsyncMock) as mock_sh, \
             patch.object(adapter, '_discover_via_sse', new_callable=AsyncMock) as mock_sse, \
             patch.object(adapter, '_get_spn_fallback_headers', new_callable=AsyncMock) as mock_spn:
            mock_sh.side_effect = [obo_error, spn_error]
            mock_sse.side_effect = [obo_error, spn_error]
            mock_spn.return_value = {"Authorization": "Bearer spn-tok"}

            result = await adapter._discover_tools_with_mcp_client(
                {"Authorization": "Bearer obo-token"}
            )

        assert result == []
        assert adapter._spn_fallback_headers is None

    @pytest.mark.asyncio
    async def test_spn_fallback_triggered_for_databricks_obo_auth(self):
        """auth_type=databricks_obo should also trigger SPN fallback."""
        params = {
            'url': 'https://example.com/api/2.0/mcp/external/test',
            'timeout_seconds': 30,
            'max_retries': 3,
            'rate_limit': 60,
            'headers': {'Authorization': 'Bearer obo-tok'},
            'auth_type': 'databricks_obo',
        }
        adapter = MCPAdapter(params)
        obo_error = self._make_403_error()
        spn_tools = [{'name': 't1', 'description': 'd', 'mcp_tool': Mock(), 'input_schema': {}}]

        with patch.object(adapter, '_discover_via_streamable_http', new_callable=AsyncMock) as mock_sh, \
             patch.object(adapter, '_discover_via_sse', new_callable=AsyncMock) as mock_sse, \
             patch.object(adapter, '_get_spn_fallback_headers', new_callable=AsyncMock) as mock_spn:
            mock_sh.side_effect = [obo_error, spn_tools]
            mock_sse.side_effect = obo_error
            mock_spn.return_value = {"Authorization": "Bearer spn-fallback"}

            result = await adapter._discover_tools_with_mcp_client(
                {"Authorization": "Bearer obo-tok"}
            )

        assert len(result) == 1
        mock_spn.assert_called_once()


class TestSPNHeadersReuse:
    """Test that SPN fallback headers are reused for tool execution."""

    @pytest.mark.asyncio
    async def test_get_auth_headers_uses_spn_fallback(self):
        """_get_authentication_headers should return SPN headers when set."""
        params = {
            'url': 'https://example.com/api/2.0/mcp/external/test',
            'headers': {'Authorization': 'Bearer obo-token'},
            'auth_type': 'databricks_spn',
        }
        adapter = MCPAdapter(params)
        adapter._spn_fallback_headers = {"Authorization": "Bearer spn-saved"}

        headers = await adapter._get_authentication_headers()

        assert headers == {"Authorization": "Bearer spn-saved"}

    @pytest.mark.asyncio
    async def test_get_auth_headers_ignores_obo_when_spn_set(self):
        """When SPN fallback is set, original OBO headers should be bypassed."""
        params = {
            'url': 'https://example.com/mcp',
            'headers': {'Authorization': 'Bearer obo-token'},
            'auth_type': 'databricks_spn',
        }
        adapter = MCPAdapter(params)
        adapter._spn_fallback_headers = {"Authorization": "Bearer spn-token"}

        headers = await adapter._get_authentication_headers()

        assert headers["Authorization"] == "Bearer spn-token"
        assert headers["Authorization"] != "Bearer obo-token"

    @pytest.mark.asyncio
    async def test_get_auth_headers_falls_through_when_no_spn(self):
        """When _spn_fallback_headers is None, use provided headers as before."""
        params = {
            'url': 'https://example.com/mcp',
            'headers': {'Authorization': 'Bearer normal-token'},
            'auth_type': 'api_key',
        }
        adapter = MCPAdapter(params)
        assert adapter._spn_fallback_headers is None

        headers = await adapter._get_authentication_headers()

        assert headers["Authorization"] == "Bearer normal-token"

    @pytest.mark.asyncio
    async def test_execute_tool_uses_spn_headers(self):
        """execute_tool should use SPN headers for actual tool calls."""
        params = {
            'url': 'https://example.com/api/2.0/mcp/external/test',
            'headers': {'Authorization': 'Bearer obo-token'},
            'auth_type': 'databricks_spn',
            'timeout_seconds': 30,
            'max_retries': 1,
            'rate_limit': 60,
        }
        adapter = MCPAdapter(params)
        adapter._transport = "streamable_http"
        adapter._spn_fallback_headers = {"Authorization": "Bearer spn-exec"}

        mock_result = Mock()
        with patch.object(adapter, '_execute_with_transport', new_callable=AsyncMock, return_value=mock_result) as mock_exec:
            result = await adapter.execute_tool("test_tool", {"query": "test"})

        # Verify the clean_headers passed to _execute_with_transport use SPN token
        call_args = mock_exec.call_args
        clean_headers = call_args[0][2]  # third positional arg
        assert clean_headers["Authorization"] == "Bearer spn-exec"


class TestExtractErrorSummaryEdgeCases:
    """Additional edge cases for _extract_error_summary."""

    def test_response_text_raises_exception(self):
        """Should handle exception when accessing response.text."""
        exc = Exception("HTTP error")
        resp = Mock()
        resp.status_code = 500
        # Make .text raise an exception
        type(resp).text = property(lambda self: (_ for _ in ()).throw(RuntimeError("decode error")))
        exc.response = resp
        result = _extract_error_summary(exc)
        assert "HTTP 500" in result


class TestLogExceptionGroupEdgeCases:
    """Additional edge cases for _log_exception_group."""

    def test_sub_exception_response_text_raises(self):
        """Should handle exception when accessing sub_exc.response.text."""
        sub_exc = Exception("sub error")
        resp = Mock()
        type(resp).text = property(lambda self: (_ for _ in ()).throw(RuntimeError("decode")))
        sub_exc.response = resp
        group = type('EG', (Exception,), {'exceptions': [sub_exc]})("group")
        # Should not raise
        _log_exception_group(group, "test context")

    def test_sub_exception_with_traceback(self):
        """Should log sub-exception traceback when __traceback__ is set."""
        try:
            raise ValueError("inner error")
        except ValueError as e:
            sub_exc = e  # has __traceback__ set
        group = type('EG', (Exception,), {'exceptions': [sub_exc]})("group")
        _log_exception_group(group, "test context")  # Should not raise


class TestAdapterInitSpnFallbackHeaders:
    """Test _spn_fallback_headers initialization."""

    def test_spn_fallback_headers_initialized_to_none(self):
        params = {'url': 'https://example.com/mcp'}
        adapter = MCPAdapter(params)
        assert adapter._spn_fallback_headers is None

    def test_url_strip(self):
        """URL should be stripped of whitespace."""
        params = {'url': '  https://example.com/mcp  '}
        adapter = MCPAdapter(params)
        assert adapter.server_url == 'https://example.com/mcp'