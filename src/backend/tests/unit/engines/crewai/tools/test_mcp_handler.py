"""Unit tests for MCP handler functions."""

import pytest
from unittest.mock import Mock, AsyncMock, patch, MagicMock
import asyncio
from typing import Dict, Any

from src.engines.crewai.tools.mcp_handler import (
    register_mcp_adapter,
    stop_all_adapters,
    get_databricks_workspace_host,
    call_databricks_api,
    create_crewai_tool_from_mcp,
    stop_mcp_adapter,
    wrap_mcp_tool,
    run_in_separate_process,
    _active_mcp_adapters
)
from src.engines.common.mcp_adapter import MCPTool


class TestMCPAdapterManagement:
    """Test suite for MCP adapter management functions."""
    
    def test_register_mcp_adapter(self):
        """Test registering an MCP adapter."""
        # Clear any existing adapters
        _active_mcp_adapters.clear()
        
        adapter_id = "test_adapter_1"
        adapter = Mock()
        
        register_mcp_adapter(adapter_id, adapter)
        
        assert adapter_id in _active_mcp_adapters
        assert _active_mcp_adapters[adapter_id] == adapter
    
    @pytest.mark.asyncio
    async def test_stop_all_adapters(self):
        """Test stopping all adapters."""
        # Clear and add test adapters
        _active_mcp_adapters.clear()
        
        adapter1 = AsyncMock()
        adapter1.stop = AsyncMock()
        adapter2 = AsyncMock()
        adapter2.stop = AsyncMock()
        
        _active_mcp_adapters['adapter1'] = adapter1
        _active_mcp_adapters['adapter2'] = adapter2
        
        await stop_all_adapters()
        
        adapter1.stop.assert_called_once()
        adapter2.stop.assert_called_once()
        assert len(_active_mcp_adapters) == 0
    
    @pytest.mark.asyncio
    async def test_stop_all_adapters_with_error(self):
        """Test stopping all adapters handles errors gracefully."""
        _active_mcp_adapters.clear()
        
        adapter1 = AsyncMock()
        adapter1.stop = AsyncMock(side_effect=Exception("Stop failed"))
        
        _active_mcp_adapters['adapter1'] = adapter1
        
        # Should not raise exception even with error
        try:
            await stop_all_adapters()
        except Exception:
            pytest.fail("stop_all_adapters should not raise exception")
        
        # The adapter should still be removed from tracking
        # Actually the implementation resets the dictionary
        assert len(_active_mcp_adapters) == 0
    
    @pytest.mark.asyncio
    async def test_stop_mcp_adapter_async(self):
        """Test stopping an async MCP adapter."""
        adapter = AsyncMock()
        adapter.stop = AsyncMock()
        
        await stop_mcp_adapter(adapter)
        
        adapter.stop.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_stop_mcp_adapter_sync(self):
        """Test stopping a sync MCP adapter."""
        adapter = Mock()
        adapter.stop = Mock()
        
        await stop_mcp_adapter(adapter)
        
        adapter.stop.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_stop_mcp_adapter_with_close(self):
        """Test stopping adapter that has close method."""
        adapter = AsyncMock()
        adapter.close = AsyncMock()
        adapter.stop = Mock(side_effect=AttributeError)
        
        await stop_mcp_adapter(adapter)
        
        adapter.close.assert_called_once()


class TestDatabricksIntegration:
    """Test suite for Databricks integration functions."""
    
    @pytest.mark.asyncio
    async def test_get_databricks_workspace_host_success(self):
        """Test getting Databricks workspace host successfully."""
        mock_config = Mock()
        mock_config.workspace_url = "https://test.databricks.com/"
        
        with patch('src.core.unit_of_work.UnitOfWork') as mock_uow_class:
            with patch('src.services.databricks_service.DatabricksService') as mock_service_class:
                mock_uow = AsyncMock()
                mock_uow.__aenter__ = AsyncMock(return_value=mock_uow)
                mock_uow.__aexit__ = AsyncMock()
                mock_uow_class.return_value = mock_uow
                
                mock_service = AsyncMock()
                mock_service.get_databricks_config = AsyncMock(return_value=mock_config)
                mock_service_class.from_unit_of_work = AsyncMock(return_value=mock_service)
                
                host, error = await get_databricks_workspace_host()
                
                assert host == "test.databricks.com"
                assert error is None
    
    @pytest.mark.asyncio
    async def test_get_databricks_workspace_host_no_config(self):
        """Test getting workspace host when no config exists."""
        with patch('src.core.unit_of_work.UnitOfWork') as mock_uow_class:
            with patch('src.services.databricks_service.DatabricksService') as mock_service_class:
                mock_uow = AsyncMock()
                mock_uow.__aenter__ = AsyncMock(return_value=mock_uow)
                mock_uow.__aexit__ = AsyncMock()
                mock_uow_class.return_value = mock_uow
                
                mock_service = AsyncMock()
                mock_service.get_databricks_config = AsyncMock(return_value=None)
                mock_service_class.from_unit_of_work = AsyncMock(return_value=mock_service)
                
                host, error = await get_databricks_workspace_host()
                
                assert host is None
                assert error == "No workspace URL found in configuration"
    
    @pytest.mark.asyncio
    async def test_get_databricks_workspace_host_exception(self):
        """Test getting workspace host handles exceptions."""
        with patch('src.core.unit_of_work.UnitOfWork') as mock_uow_class:
            mock_uow_class.side_effect = Exception("DB error")
            
            host, error = await get_databricks_workspace_host()
            
            assert host is None
            assert "DB error" in error
    
    @pytest.mark.asyncio
    async def test_call_databricks_api_get(self):
        """Test calling Databricks API with GET method."""
        endpoint = "/api/2.0/test"
        
        with patch('src.engines.crewai.tools.mcp_handler.get_databricks_auth_headers', new_callable=AsyncMock) as mock_auth:
            with patch('src.engines.crewai.tools.mcp_handler.get_databricks_workspace_host', new_callable=AsyncMock) as mock_host:
                with patch('aiohttp.ClientSession') as mock_session_class:
                    mock_auth.return_value = ({'Authorization': 'Bearer token'}, None)
                    mock_host.return_value = ('test.databricks.com', None)
                    
                    # Mock response
                    mock_response = AsyncMock()
                    mock_response.json = AsyncMock(return_value={'result': 'success'})
                    mock_response.raise_for_status = Mock()
                    
                    # Mock session with context manager
                    mock_session = AsyncMock()
                    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
                    mock_session.__aexit__ = AsyncMock()
                    
                    # Mock the get method to return a context manager
                    mock_get_context = AsyncMock()
                    mock_get_context.__aenter__ = AsyncMock(return_value=mock_response)
                    mock_get_context.__aexit__ = AsyncMock()
                    mock_session.get = Mock(return_value=mock_get_context)
                    
                    mock_session_class.return_value = mock_session
                    
                    result = await call_databricks_api(endpoint)
                    
                    assert result == {'result': 'success'}
                    mock_session.get.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_call_databricks_api_auth_error(self):
        """Test API call with authentication error."""
        with patch('src.engines.crewai.tools.mcp_handler.get_databricks_auth_headers', new_callable=AsyncMock) as mock_auth:
            mock_auth.return_value = (None, "Auth failed")
            
            result = await call_databricks_api("/test")
            
            assert 'error' in result
            assert "Auth failed" in result['error']


class TestCrewAIToolCreation:
    """Test suite for CrewAI tool creation from MCP."""
    
    @pytest.fixture
    def mcp_tool_dict(self) -> Dict[str, Any]:
        """Create a test MCP tool dictionary."""
        return {
            'name': 'test_tool',
            'description': 'A test tool',
            'input_schema': {
                'properties': {
                    'input': {
                        'type': 'string',
                        'description': 'Test input'
                    }
                },
                'required': ['input']
            },
            'adapter': Mock()
        }
    
    def test_create_crewai_tool_from_mcp(self, mcp_tool_dict):
        """Test creating CrewAI tool from MCP dictionary."""
        with patch('src.engines.common.mcp_adapter.MCPTool') as mock_mcp_tool_class:
            mock_mcp_tool = Mock()
            mock_mcp_tool.name = 'test_tool'
            mock_mcp_tool.description = 'A test tool'
            mock_mcp_tool.input_schema = mcp_tool_dict['input_schema']
            mock_mcp_tool_class.return_value = mock_mcp_tool
            
            tool = create_crewai_tool_from_mcp(mcp_tool_dict)
            
            assert tool.name == 'test_tool'
            # CrewAI may format the description, so just check if our description is in there
            assert 'A test tool' in tool.description or tool.description == 'A test tool'
            assert hasattr(tool, '_run')
            assert hasattr(tool, 'args_schema')
    
    def test_create_crewai_tool_no_schema(self):
        """Test creating CrewAI tool with no input schema."""
        mcp_tool_dict = {
            'name': 'simple_tool',
            'description': 'Simple tool',
            'input_schema': None
        }
        
        with patch('src.engines.common.mcp_adapter.MCPTool') as mock_mcp_tool_class:
            mock_mcp_tool = Mock()
            mock_mcp_tool.name = 'simple_tool'
            mock_mcp_tool.description = 'Simple tool'
            mock_mcp_tool.input_schema = {}
            mock_mcp_tool_class.return_value = mock_mcp_tool
            
            tool = create_crewai_tool_from_mcp(mcp_tool_dict)
            
            # Should have dummy field
            fields = tool.args_schema.model_fields if hasattr(tool.args_schema, 'model_fields') else tool.args_schema.__fields__
            assert 'dummy' in fields
    
    def test_crewai_tool_execution(self, mcp_tool_dict):
        """Test executing a created CrewAI tool."""
        with patch('src.engines.common.mcp_adapter.MCPTool') as mock_mcp_tool_class:
            mock_mcp_tool = Mock()
            mock_mcp_tool.name = 'test_tool'
            mock_mcp_tool.description = 'A test tool'
            mock_mcp_tool.input_schema = mcp_tool_dict['input_schema']
            
            # Mock execute method
            async def mock_execute(params):
                return Mock(content=[Mock(text="Success")])
            
            mock_mcp_tool.execute = mock_execute
            mock_mcp_tool_class.return_value = mock_mcp_tool
            
            with patch('asyncio.run') as mock_asyncio_run:
                mock_asyncio_run.return_value = Mock(content=[Mock(text="Success")])
                
                tool = create_crewai_tool_from_mcp(mcp_tool_dict)
                
                # Test execution
                result = tool._run(input="test")
                assert result == "Success"
    
    def test_crewai_tool_execution_error(self, mcp_tool_dict):
        """Test CrewAI tool execution with error."""
        with patch('src.engines.common.mcp_adapter.MCPTool') as mock_mcp_tool_class:
            mock_mcp_tool = Mock()
            mock_mcp_tool.name = 'test_tool'
            mock_mcp_tool.description = 'A test tool'
            mock_mcp_tool.input_schema = mcp_tool_dict['input_schema']
            
            # Mock execute to raise error
            async def mock_execute(params):
                raise Exception("Execution failed")
            
            mock_mcp_tool.execute = mock_execute
            mock_mcp_tool_class.return_value = mock_mcp_tool
            
            with patch('asyncio.run') as mock_asyncio_run:
                mock_asyncio_run.side_effect = Exception("Execution failed")
                
                tool = create_crewai_tool_from_mcp(mcp_tool_dict)
                
                # Test execution
                result = tool._run(input="test")
                assert "Error:" in result
                assert "Execution failed" in result
    
    def test_create_crewai_tool_with_different_field_types(self):
        """Test creating CrewAI tool with different field types."""
        mcp_tool_dict = {
            'name': 'complex_tool',
            'description': 'Complex tool',
            'input_schema': {
                'properties': {
                    'text_field': {'type': 'string', 'description': 'Text input'},
                    'int_field': {'type': 'integer', 'description': 'Integer input'},
                    'float_field': {'type': 'number', 'description': 'Float input'},
                    'bool_field': {'type': 'boolean', 'description': 'Boolean input'}
                }
            }
        }
        
        with patch('src.engines.common.mcp_adapter.MCPTool') as mock_mcp_tool_class:
            mock_mcp_tool = Mock()
            mock_mcp_tool.name = 'complex_tool'
            mock_mcp_tool.description = 'Complex tool'
            mock_mcp_tool.input_schema = mcp_tool_dict['input_schema']
            mock_mcp_tool_class.return_value = mock_mcp_tool
            
            tool = create_crewai_tool_from_mcp(mcp_tool_dict)
            
            # Check that the tool was created with proper field types
            fields = tool.args_schema.model_fields if hasattr(tool.args_schema, 'model_fields') else tool.args_schema.__fields__
            assert 'text_field' in fields
            assert 'int_field' in fields
            assert 'float_field' in fields
            assert 'bool_field' in fields
    
    def test_crewai_tool_execution_with_event_loop(self, mcp_tool_dict):
        """Test executing CrewAI tool when event loop is already running."""
        with patch('src.engines.common.mcp_adapter.MCPTool') as mock_mcp_tool_class:
            mock_mcp_tool = Mock()
            mock_mcp_tool.name = 'test_tool'
            mock_mcp_tool.description = 'A test tool'
            mock_mcp_tool.input_schema = mcp_tool_dict['input_schema']
            
            # Mock execute method
            async def mock_execute(params):
                return Mock(content=[Mock(text="Success from thread")])
            
            mock_mcp_tool.execute = mock_execute
            mock_mcp_tool_class.return_value = mock_mcp_tool
            
            # Mock the event loop check
            with patch('asyncio.get_running_loop') as mock_get_loop:
                mock_get_loop.return_value = Mock()  # Simulate existing event loop
                
                with patch('concurrent.futures.ThreadPoolExecutor') as mock_executor_class:
                    mock_executor = Mock()
                    mock_future = Mock()
                    mock_future.result.return_value = Mock(content=[Mock(text="Success from thread")])
                    mock_executor.submit.return_value = mock_future
                    mock_executor.__enter__ = Mock(return_value=mock_executor)
                    mock_executor.__exit__ = Mock()
                    mock_executor_class.return_value = mock_executor
                    
                    tool = create_crewai_tool_from_mcp(mcp_tool_dict)
                    
                    # Test execution
                    result = tool._run(input="test")
                    assert result == "Success from thread"


class TestWrapMCPTool:
    """Test suite for wrap_mcp_tool function."""
    
    @pytest.fixture
    def mock_tool(self):
        """Create a mock tool with _run method."""
        tool = Mock()
        tool.name = "test_tool"
        tool._run = Mock(return_value="Success")
        return tool
    
    def test_wrap_mcp_tool_direct_execution(self, mock_tool):
        """Test wrapping MCP tool with direct execution."""
        # Store the original run method to check it gets called
        original_run = mock_tool._run
        
        wrapped_tool = wrap_mcp_tool(mock_tool)
        
        # The wrapped tool should be the same object
        assert wrapped_tool == mock_tool
        
        # The _run method should be replaced
        assert wrapped_tool._run != original_run
        
        # Test execution
        result = wrapped_tool._run(test="param")
        assert result == "Success"
        # The original mock should have been called
        original_run.assert_called_once_with(test="param")
    
    def test_wrap_mcp_tool_genie_tool(self):
        """Test wrapping Genie-specific MCP tool."""
        tool = Mock()
        tool.name = "get_space"
        original_run = Mock(side_effect=Exception("Event loop issue"))
        tool._run = original_run
        
        with patch('asyncio.new_event_loop') as mock_new_loop:
            with patch('asyncio.set_event_loop') as mock_set_loop:
                mock_loop = Mock()
                mock_loop.run_until_complete = Mock(return_value="Process result")
                mock_loop.close = Mock()
                mock_new_loop.return_value = mock_loop
                
                wrapped_tool = wrap_mcp_tool(tool)
                
                # Should have modified _run method
                assert wrapped_tool._run != original_run
                
                # Test execution - should handle the error and use process
                result = wrapped_tool._run(space_id="test_space")
                assert mock_loop.run_until_complete.called
    
    @pytest.mark.asyncio
    async def test_run_in_separate_process(self):
        """Test running tool in separate process."""
        tool_name = "test_tool"
        kwargs = {"param": "value"}
        
        with patch('asyncio.create_subprocess_exec') as mock_subprocess:
            # Mock the process
            mock_process = Mock()
            mock_process.returncode = 0
            mock_process.communicate = AsyncMock(return_value=(b'{"result": "success"}', b''))
            mock_subprocess.return_value = mock_process
            
            result = await run_in_separate_process(tool_name, kwargs)
            
            assert result == {"result": "success"}
    
    def test_wrap_mcp_tool_no_run_method(self):
        """Test wrapping tool without _run method."""
        tool = Mock(spec=['name'])
        tool.name = "test_tool"
        
        # wrap_mcp_tool expects a _run method, so this should raise an error
        with pytest.raises(AttributeError):
            wrapped_tool = wrap_mcp_tool(tool)