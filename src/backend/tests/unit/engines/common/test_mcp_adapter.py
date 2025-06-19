"""Unit tests for MCPAdapter."""

import pytest
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from typing import Dict, Any, List

from src.engines.common.mcp_adapter import MCPAdapter, MCPTool


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
        """Test initialization when authentication fails."""
        adapter = MCPAdapter(server_params)
        
        with patch.object(adapter, '_get_authentication_headers', new_callable=AsyncMock) as mock_auth:
            mock_auth.return_value = None
            
            await adapter.initialize()
            
            assert adapter._initialized is True
            assert adapter._tools == []
    
    @pytest.mark.asyncio
    async def test_initialize_exception(self, server_params):
        """Test initialization handles exceptions gracefully."""
        adapter = MCPAdapter(server_params)
        
        with patch.object(adapter, '_get_authentication_headers', new_callable=AsyncMock) as mock_auth:
            mock_auth.side_effect = Exception("Auth failed")
            
            await adapter.initialize()
            
            assert adapter._initialized is True
            assert adapter._tools == []
    
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
                
                mock_connect.assert_called_once_with(adapter.server_url, headers={'Authorization': 'Bearer token'})
                mock_session.initialize.assert_called_once()
                mock_session.list_tools.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_discover_tools_no_tools_found(self, server_params):
        """Test tool discovery when no tools are found."""
        adapter = MCPAdapter(server_params)
        headers = {'Authorization': 'Bearer token'}
        
        mock_tools_result = Mock()
        mock_tools_result.tools = None
        
        mock_session = AsyncMock()
        mock_session.initialize = AsyncMock()
        mock_session.list_tools = AsyncMock(return_value=mock_tools_result)
        
        with patch('mcp.client.streamable_http.streamablehttp_client') as mock_connect:
            with patch('mcp.ClientSession') as mock_client_session:
                mock_connect.return_value.__aenter__.return_value = (Mock(), Mock(), None)
                mock_client_session.return_value.__aenter__.return_value = mock_session
                
                tools = await adapter._discover_tools_with_mcp_client(headers)
                
                assert tools == []
    
    @pytest.mark.asyncio
    async def test_discover_tools_exception(self, server_params):
        """Test tool discovery handles exceptions."""
        adapter = MCPAdapter(server_params)
        headers = {'Authorization': 'Bearer token'}
        
        with patch('mcp.client.streamable_http.streamablehttp_client') as mock_connect:
            mock_connect.side_effect = Exception("Connection failed")
            
            tools = await adapter._discover_tools_with_mcp_client(headers)
            
            assert tools == []
    
    @pytest.mark.asyncio
    async def test_execute_tool(self, server_params):
        """Test tool execution."""
        adapter = MCPAdapter(server_params)
        adapter._auth_headers = {'Authorization': 'Bearer token'}
        adapter._session_id = 'test-session'
        
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