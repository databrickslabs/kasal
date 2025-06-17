import pytest
import asyncio
from unittest.mock import MagicMock, patch, AsyncMock
from concurrent.futures import ThreadPoolExecutor

from src.engines.crewai.tools.mcp_adapter import AsyncMCPAdapter, MCPAdapter


class TestAsyncMCPAdapter:
    """Test suite for AsyncMCPAdapter class."""
    
    def test_init_creates_executor(self):
        """Test that initialization creates ThreadPoolExecutor."""
        server_params = {"url": "http://test.com"}
        adapter = AsyncMCPAdapter(server_params)
        
        assert adapter.server_params == server_params
        assert adapter._adapter is None
        assert adapter._tools is None
        assert isinstance(adapter._executor, ThreadPoolExecutor)
    
    def test_init_stores_server_params(self):
        """Test that server parameters are stored correctly."""
        server_params = {"url": "http://test.com", "headers": {"auth": "token"}}
        adapter = AsyncMCPAdapter(server_params)
        
        assert adapter.server_params == server_params
    
    def test_init_empty_server_params(self):
        """Test initialization with empty server params."""
        server_params = {}
        adapter = AsyncMCPAdapter(server_params)
        
        assert adapter.server_params == {}
    
    @pytest.mark.asyncio
    async def test_initialize_success(self):
        """Test successful initialization of MCP adapter."""
        server_params = {"url": "http://test.com"}
        adapter = AsyncMCPAdapter(server_params)
        
        mock_mcp_adapter = MagicMock()
        mock_mcp_adapter.tools = ["tool1", "tool2", "tool3"]
        
        with patch('src.engines.crewai.tools.mcp_adapter.logger') as mock_logger, \
             patch('crewai_tools.MCPServerAdapter', return_value=mock_mcp_adapter):
            
            result = await adapter.initialize()
            assert result == adapter
            assert adapter._tools == ["tool1", "tool2", "tool3"]
            mock_logger.info.assert_any_call("Initializing AsyncMCPAdapter")
            mock_logger.info.assert_any_call("AsyncMCPAdapter initialized with 3 tools")
    
    @pytest.mark.asyncio
    async def test_initialize_timeout(self):
        """Test initialization timeout handling."""
        server_params = {"url": "http://slow-server.com"}
        adapter = AsyncMCPAdapter(server_params)
        
        # Mock the asyncio.wait_for to raise TimeoutError in the module
        with patch('src.engines.crewai.tools.mcp_adapter.logger') as mock_logger, \
             patch('crewai_tools.MCPServerAdapter') as mock_adapter_class:
            
            # Create a custom side effect that raises TimeoutError 
            async def mock_wait_for(*args, **kwargs):
                raise asyncio.TimeoutError()
            
            with patch('src.engines.crewai.tools.mcp_adapter.asyncio.wait_for', side_effect=mock_wait_for):
                result = await adapter.initialize()
                # Should set tools to empty list and continue
                assert result == adapter
                assert adapter._tools == []
                # The timeout error gets caught by the outer exception handler
                mock_logger.error.assert_called_with("Error initializing AsyncMCPAdapter: MCP adapter initialization timed out")
                mock_logger.warning.assert_called_with("AsyncMCPAdapter initialization failed, continuing with empty tools")
    
    @pytest.mark.asyncio
    async def test_initialize_timeout_specific_case(self):
        """Test timeout exception when not caught by outer exception handler."""
        server_params = {"url": "http://slow-server.com"}
        adapter = AsyncMCPAdapter(server_params)
        
        # Mock the initialization to bypass the outer exception handler
        with patch('src.engines.crewai.tools.mcp_adapter.logger') as mock_logger:
            # Mock the wait_for to raise TimeoutError directly
            async def mock_wait_for(*args, **kwargs):  
                raise asyncio.TimeoutError()
            
            with patch('src.engines.crewai.tools.mcp_adapter.asyncio.wait_for', side_effect=mock_wait_for), \
                 patch('crewai_tools.MCPServerAdapter'):
                
                # We expect the timeout error to be caught and re-raised as TimeoutError
                # But then caught by the outer exception handler
                result = await adapter.initialize()
                assert result == adapter
                assert adapter._tools == []
    
    @pytest.mark.asyncio
    async def test_initialize_exception_handling(self):
        """Test exception handling during initialization."""
        server_params = {"url": "http://test.com"}
        adapter = AsyncMCPAdapter(server_params)
        
        with patch('src.engines.crewai.tools.mcp_adapter.logger') as mock_logger, \
             patch('crewai_tools.MCPServerAdapter', side_effect=Exception("Connection failed")):
            
            result = await adapter.initialize()
            
            assert result == adapter
            assert adapter._tools == []
            mock_logger.error.assert_called_with("Error initializing AsyncMCPAdapter: Connection failed")
            mock_logger.warning.assert_called_with("AsyncMCPAdapter initialization failed, continuing with empty tools")
    
    @pytest.mark.asyncio
    async def test_initialize_import_error(self):
        """Test initialization when import fails."""
        server_params = {"url": "http://test.com"}
        adapter = AsyncMCPAdapter(server_params)
        
        with patch('src.engines.crewai.tools.mcp_adapter.logger') as mock_logger, \
             patch('crewai_tools.MCPServerAdapter', side_effect=ImportError("Module not found")):
            
            result = await adapter.initialize()
            
            assert result == adapter
            assert adapter._tools == []
            mock_logger.error.assert_called_with("Error initializing AsyncMCPAdapter: Module not found")
    
    def test_tools_property_before_initialization(self):
        """Test tools property before initialization."""
        adapter = AsyncMCPAdapter({"url": "http://test.com"})
        
        assert adapter.tools is None
    
    def test_tools_property_after_initialization(self):
        """Test tools property after successful initialization."""
        adapter = AsyncMCPAdapter({"url": "http://test.com"})
        adapter._tools = ["tool1", "tool2"]
        
        assert adapter.tools == ["tool1", "tool2"]
    
    def test_tools_property_after_failed_initialization(self):
        """Test tools property after failed initialization."""
        adapter = AsyncMCPAdapter({"url": "http://test.com"})
        adapter._tools = []
        
        assert adapter.tools == []
    
    @pytest.mark.asyncio
    async def test_stop_success(self):
        """Test successful stop operation."""
        adapter = AsyncMCPAdapter({"url": "http://test.com"})
        
        # Mock the adapter
        mock_mcp_adapter = MagicMock()
        adapter._adapter = mock_mcp_adapter
        
        with patch('src.engines.crewai.tools.mcp_adapter.logger') as mock_logger:
            await adapter.stop()
            
            mock_logger.info.assert_any_call("Stopping AsyncMCPAdapter")
            mock_logger.info.assert_any_call("AsyncMCPAdapter stopped successfully")
    
    @pytest.mark.asyncio
    async def test_stop_no_adapter(self):
        """Test stop operation when no adapter is initialized."""
        adapter = AsyncMCPAdapter({"url": "http://test.com"})
        # _adapter remains None
        
        # Should not raise any exceptions
        await adapter.stop()
    
    @pytest.mark.asyncio
    async def test_stop_exception_handling(self):
        """Test exception handling during stop operation."""
        adapter = AsyncMCPAdapter({"url": "http://test.com"})
        
        # Mock the adapter with stop method that raises exception
        mock_mcp_adapter = MagicMock()
        mock_mcp_adapter.stop.side_effect = Exception("Stop failed")
        adapter._adapter = mock_mcp_adapter
        
        with patch('src.engines.crewai.tools.mcp_adapter.logger') as mock_logger:
            await adapter.stop()
            
            mock_logger.error.assert_called_with("Error stopping AsyncMCPAdapter: Stop failed")
    
    @pytest.mark.asyncio
    async def test_stop_with_executor_shutdown(self):
        """Test stop operation shuts down executor."""
        adapter = AsyncMCPAdapter({"url": "http://test.com"})
        
        # Mock the adapter
        mock_mcp_adapter = MagicMock()
        adapter._adapter = mock_mcp_adapter
        
        # Mock the executor
        mock_executor = MagicMock()
        adapter._executor = mock_executor
        
        with patch('src.engines.crewai.tools.mcp_adapter.logger'):
            await adapter.stop()
            
            mock_executor.shutdown.assert_called_once_with(wait=False)
    
    @pytest.mark.asyncio
    async def test_close_aliases_stop(self):
        """Test that close() is an alias for stop()."""
        adapter = AsyncMCPAdapter({"url": "http://test.com"})
        
        with patch.object(adapter, 'stop', new_callable=AsyncMock) as mock_stop:
            await adapter.close()
            mock_stop.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_initialize_with_complex_server_params(self):
        """Test initialization with complex server parameters."""
        server_params = {
            "url": "http://test.com",
            "headers": {"Authorization": "Bearer token"},
            "timeout": 30,
            "retries": 3
        }
        adapter = AsyncMCPAdapter(server_params)
        
        mock_mcp_adapter = MagicMock()
        mock_mcp_adapter.tools = []
        
        with patch('crewai_tools.MCPServerAdapter', return_value=mock_mcp_adapter) as mock_constructor:
            await adapter.initialize()
            
            mock_constructor.assert_called_once_with(server_params)
    
    @pytest.mark.asyncio
    async def test_initialize_wait_for_timeout_parameters(self):
        """Test that wait_for is called with correct timeout."""
        adapter = AsyncMCPAdapter({"url": "http://test.com"})
        
        with patch('src.engines.crewai.tools.mcp_adapter.asyncio.wait_for') as mock_wait_for, \
             patch('crewai_tools.MCPServerAdapter'):
            
            mock_wait_for.return_value = MagicMock(tools=[])
            
            await adapter.initialize()
            
            # Verify wait_for was called with 30 second timeout
            assert mock_wait_for.call_args[1]['timeout'] == 30.0
    
    def test_thread_pool_executor_configuration(self):
        """Test ThreadPoolExecutor configuration."""
        adapter = AsyncMCPAdapter({"url": "http://test.com"})
        
        assert isinstance(adapter._executor, ThreadPoolExecutor)
        assert adapter._executor._max_workers == 1
        assert "mcp_adapter" in adapter._executor._thread_name_prefix
    
    @pytest.mark.asyncio
    async def test_multiple_initialize_calls(self):
        """Test multiple calls to initialize."""
        adapter = AsyncMCPAdapter({"url": "http://test.com"})
        
        mock_mcp_adapter = MagicMock()
        mock_mcp_adapter.tools = ["tool1"]
        
        with patch('crewai_tools.MCPServerAdapter', return_value=mock_mcp_adapter):
            # First initialization
            result1 = await adapter.initialize()
            assert result1 == adapter
            assert adapter._tools == ["tool1"]
            
            # Second initialization should work (overwrites previous)
            mock_mcp_adapter.tools = ["tool1", "tool2"]
            result2 = await adapter.initialize()
            assert result2 == adapter
            assert adapter._tools == ["tool1", "tool2"]
    
    def test_getattr_delegation_with_adapter(self):
        """Test __getattr__ delegation when adapter is initialized."""
        adapter = AsyncMCPAdapter({"url": "http://test.com"})
        
        mock_mcp_adapter = MagicMock()
        mock_mcp_adapter.some_method.return_value = "test_result"
        adapter._adapter = mock_mcp_adapter
        
        result = adapter.some_method()
        assert result == "test_result"
        mock_mcp_adapter.some_method.assert_called_once()
    
    def test_getattr_delegation_without_adapter(self):
        """Test __getattr__ delegation when adapter is not initialized."""
        adapter = AsyncMCPAdapter({"url": "http://test.com"})
        
        with pytest.raises(AttributeError, match="AsyncMCPAdapter has no attribute 'nonexistent' \\(adapter not initialized\\)"):
            adapter.nonexistent
    
    def test_adapter_state_management(self):
        """Test adapter state is properly managed."""
        adapter = AsyncMCPAdapter({"url": "http://test.com"})
        
        # Initially no adapter
        assert adapter._adapter is None
        assert adapter._tools is None
        
        # After setting adapter
        mock_adapter = MagicMock()
        adapter._adapter = mock_adapter
        adapter._tools = ["tool1", "tool2"]
        
        assert adapter._adapter == mock_adapter
        assert adapter.tools == ["tool1", "tool2"]
    
    @pytest.mark.asyncio
    async def test_initialize_direct_timeout_error_logging(self):
        """Test that timeout error logging is properly captured."""
        server_params = {"url": "http://test.com"}
        adapter = AsyncMCPAdapter(server_params)
        
        # Test direct timeout error (before being caught by outer exception)
        async def mock_wait_for_with_timeout(*args, **kwargs):
            raise asyncio.TimeoutError()
        
        with patch('src.engines.crewai.tools.mcp_adapter.logger') as mock_logger, \
             patch('crewai_tools.MCPServerAdapter') as mock_adapter_class, \
             patch('src.engines.crewai.tools.mcp_adapter.asyncio.wait_for', side_effect=mock_wait_for_with_timeout):
            
            # This should trigger the timeout error logging
            await adapter.initialize()
            
            # Verify that the timeout error was logged (even if caught by outer handler)
            mock_logger.error.assert_any_call("Error initializing AsyncMCPAdapter: MCP adapter initialization timed out")
    
    @pytest.mark.asyncio
    async def test_initialize_run_in_executor_exception(self):
        """Test exception handling when run_in_executor fails."""
        server_params = {"url": "http://test.com"}
        adapter = AsyncMCPAdapter(server_params)
        
        # Mock the get_event_loop to raise an exception
        with patch('src.engines.crewai.tools.mcp_adapter.logger') as mock_logger, \
             patch('crewai_tools.MCPServerAdapter') as mock_adapter_class, \
             patch('src.engines.crewai.tools.mcp_adapter.asyncio.get_event_loop', side_effect=RuntimeError("Event loop error")):
            
            result = await adapter.initialize()
            
            assert result == adapter
            assert adapter._tools == []
            mock_logger.error.assert_called_with("Error initializing AsyncMCPAdapter: Event loop error")
            mock_logger.warning.assert_called_with("AsyncMCPAdapter initialization failed, continuing with empty tools")
    
    @pytest.mark.asyncio
    async def test_stop_with_no_executor(self):
        """Test stop operation when executor is None."""
        adapter = AsyncMCPAdapter({"url": "http://test.com"})
        
        # Mock the adapter but set executor to None
        mock_mcp_adapter = MagicMock()
        adapter._adapter = mock_mcp_adapter
        adapter._executor = None
        
        # Should raise AttributeError when trying to call shutdown on None
        with pytest.raises(AttributeError):
            await adapter.stop()
    
    @pytest.mark.asyncio
    async def test_stop_executor_shutdown_exception(self):
        """Test stop operation when executor shutdown raises exception."""
        adapter = AsyncMCPAdapter({"url": "http://test.com"})
        
        # Mock the adapter and executor
        mock_mcp_adapter = MagicMock()
        adapter._adapter = mock_mcp_adapter
        
        mock_executor = MagicMock()
        mock_executor.shutdown.side_effect = Exception("Shutdown failed")
        adapter._executor = mock_executor
        
        with patch('src.engines.crewai.tools.mcp_adapter.logger') as mock_logger:
            # The shutdown exception should be raised since it's in finally block
            with pytest.raises(Exception, match="Shutdown failed"):
                await adapter.stop()
    
    @pytest.mark.asyncio
    async def test_stop_get_event_loop_exception(self):
        """Test stop operation when get_event_loop raises exception."""
        adapter = AsyncMCPAdapter({"url": "http://test.com"})
        
        # Mock the adapter
        mock_mcp_adapter = MagicMock()
        adapter._adapter = mock_mcp_adapter
        
        with patch('src.engines.crewai.tools.mcp_adapter.logger') as mock_logger, \
             patch('src.engines.crewai.tools.mcp_adapter.asyncio.get_event_loop', side_effect=RuntimeError("No event loop")):
            
            await adapter.stop()
            
            # Should log the error
            mock_logger.error.assert_called_with("Error stopping AsyncMCPAdapter: No event loop")
    
    @pytest.mark.asyncio
    async def test_stop_run_in_executor_exception(self):
        """Test stop operation when run_in_executor raises exception."""
        adapter = AsyncMCPAdapter({"url": "http://test.com"})
        
        # Mock the adapter
        mock_mcp_adapter = MagicMock()
        adapter._adapter = mock_mcp_adapter
        
        # Mock the event loop to raise exception in run_in_executor
        mock_loop = MagicMock()
        mock_loop.run_in_executor = AsyncMock(side_effect=Exception("Executor error"))
        
        with patch('src.engines.crewai.tools.mcp_adapter.logger') as mock_logger, \
             patch('src.engines.crewai.tools.mcp_adapter.asyncio.get_event_loop', return_value=mock_loop):
            
            await adapter.stop()
            
            # Should log the error
            mock_logger.error.assert_called_with("Error stopping AsyncMCPAdapter: Executor error")
    
    def test_getattr_with_callable_attribute(self):
        """Test __getattr__ delegation with callable attribute."""
        adapter = AsyncMCPAdapter({"url": "http://test.com"})
        
        mock_mcp_adapter = MagicMock()
        mock_callable = MagicMock(return_value="callable_result")
        mock_mcp_adapter.callable_attr = mock_callable
        adapter._adapter = mock_mcp_adapter
        
        # Get the callable attribute
        result_callable = adapter.callable_attr
        assert result_callable == mock_callable
        
        # Call it
        result = result_callable("test_arg")
        assert result == "callable_result"
        mock_callable.assert_called_once_with("test_arg")
    
    @pytest.mark.asyncio
    async def test_initialize_with_tools_none(self):
        """Test initialization when adapter.tools is None."""
        server_params = {"url": "http://test.com"}
        adapter = AsyncMCPAdapter(server_params)
        
        mock_mcp_adapter = MagicMock()
        mock_mcp_adapter.tools = None
        
        with patch('src.engines.crewai.tools.mcp_adapter.logger') as mock_logger, \
             patch('crewai_tools.MCPServerAdapter', return_value=mock_mcp_adapter):
            
            result = await adapter.initialize()
            assert result == adapter
            # When tools is None, len(None) would fail, so it goes to exception handler
            assert adapter._tools == []
            mock_logger.error.assert_any_call("Error initializing AsyncMCPAdapter: object of type 'NoneType' has no len()")
            mock_logger.warning.assert_called_with("AsyncMCPAdapter initialization failed, continuing with empty tools")


class TestMCPAdapter:
    """Test suite for MCPAdapter legacy compatibility class."""
    
    def test_init_stores_parameters(self):
        """Test that initialization stores URL and headers."""
        adapter = MCPAdapter("http://test.com", {"auth": "token"})
        
        assert adapter.mcp_url == "http://test.com"
        assert adapter.headers == {"auth": "token"}
        assert adapter._async_adapter is None
    
    def test_init_with_none_headers(self):
        """Test initialization with None headers."""
        adapter = MCPAdapter("http://test.com", None)
        
        assert adapter.mcp_url == "http://test.com"
        assert adapter.headers is None
    
    @pytest.mark.asyncio
    async def test_initialize_creates_async_adapter(self):
        """Test that initialize creates and initializes AsyncMCPAdapter."""
        adapter = MCPAdapter("http://test.com", {"auth": "token"})
        
        with patch('src.engines.crewai.tools.mcp_adapter.AsyncMCPAdapter') as mock_async_class:
            mock_async_instance = MagicMock()
            mock_async_instance.initialize = AsyncMock()
            mock_async_class.return_value = mock_async_instance
            
            result = await adapter.initialize()
            
            assert result == adapter
            assert adapter._async_adapter == mock_async_instance
            
            # Verify AsyncMCPAdapter was created with correct params
            expected_params = {"url": "http://test.com", "headers": {"auth": "token"}}
            mock_async_class.assert_called_once_with(expected_params)
            mock_async_instance.initialize.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_initialize_without_headers(self):
        """Test initialize when headers is None."""
        adapter = MCPAdapter("http://test.com", None)
        
        with patch('src.engines.crewai.tools.mcp_adapter.AsyncMCPAdapter') as mock_async_class:
            mock_async_instance = MagicMock()
            mock_async_instance.initialize = AsyncMock()
            mock_async_class.return_value = mock_async_instance
            
            await adapter.initialize()
            
            # Verify AsyncMCPAdapter was created without headers
            expected_params = {"url": "http://test.com"}
            mock_async_class.assert_called_once_with(expected_params)
    
    def test_tools_property_with_async_adapter(self):
        """Test tools property when async adapter is initialized."""
        adapter = MCPAdapter("http://test.com", {})
        
        mock_async_adapter = MagicMock()
        mock_async_adapter.tools = ["tool1", "tool2"]
        adapter._async_adapter = mock_async_adapter
        
        assert adapter.tools == ["tool1", "tool2"]
    
    def test_tools_property_without_async_adapter(self):
        """Test tools property when async adapter is not initialized."""
        adapter = MCPAdapter("http://test.com", {})
        
        assert adapter.tools is None
    
    @pytest.mark.asyncio
    async def test_stop_with_async_adapter(self):
        """Test stop operation when async adapter is initialized."""
        adapter = MCPAdapter("http://test.com", {})
        
        mock_async_adapter = MagicMock()
        mock_async_adapter.stop = AsyncMock()
        adapter._async_adapter = mock_async_adapter
        
        await adapter.stop()
        
        mock_async_adapter.stop.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_stop_without_async_adapter(self):
        """Test stop operation when async adapter is not initialized."""
        adapter = MCPAdapter("http://test.com", {})
        
        # Should not raise any exceptions
        await adapter.stop()
    
    @pytest.mark.asyncio
    async def test_close_aliases_stop(self):
        """Test that close() is an alias for stop()."""
        adapter = MCPAdapter("http://test.com", {})
        
        with patch.object(adapter, 'stop', new_callable=AsyncMock) as mock_stop:
            await adapter.close()
            mock_stop.assert_called_once()
    
    def test_getattr_delegation_with_async_adapter(self):
        """Test __getattr__ delegation when async adapter is initialized."""
        adapter = MCPAdapter("http://test.com", {})
        
        mock_async_adapter = MagicMock()
        mock_async_adapter.some_attribute = "test_value"
        adapter._async_adapter = mock_async_adapter
        
        assert adapter.some_attribute == "test_value"
    
    def test_getattr_delegation_without_async_adapter(self):
        """Test __getattr__ delegation when async adapter is not initialized."""
        adapter = MCPAdapter("http://test.com", {})
        
        with pytest.raises(AttributeError, match="MCPAdapter has no attribute 'nonexistent' \\(not initialized\\)"):
            adapter.nonexistent
    
    @pytest.mark.asyncio
    async def test_end_to_end_workflow(self):
        """Test complete workflow from initialization to cleanup."""
        adapter = MCPAdapter("http://test.com", {"auth": "token"})
        
        with patch('src.engines.crewai.tools.mcp_adapter.AsyncMCPAdapter') as mock_async_class:
            mock_async_instance = MagicMock()
            mock_async_instance.initialize = AsyncMock()
            mock_async_instance.stop = AsyncMock()
            mock_async_instance.tools = ["tool1", "tool2"]
            mock_async_class.return_value = mock_async_instance
            
            # Initialize
            await adapter.initialize()
            assert adapter._async_adapter == mock_async_instance
            
            # Use tools
            tools = adapter.tools
            assert tools == ["tool1", "tool2"]
            
            # Stop
            await adapter.stop()
            mock_async_instance.stop.assert_called_once()
    
    def test_url_and_headers_validation(self):
        """Test that URL and headers are properly validated and stored."""
        # Test with various URL formats
        adapter1 = MCPAdapter("http://example.com", {})
        assert adapter1.mcp_url == "http://example.com"
        
        adapter2 = MCPAdapter("https://secure.example.com:8080/path", {"key": "value"})
        assert adapter2.mcp_url == "https://secure.example.com:8080/path"
        assert adapter2.headers == {"key": "value"}
        
        # Test with empty headers
        adapter3 = MCPAdapter("http://test.com", {})
        assert adapter3.headers == {}
    
    @pytest.mark.asyncio
    async def test_initialize_async_adapter_failure(self):
        """Test initialization when async adapter initialization fails."""
        adapter = MCPAdapter("http://test.com", {"auth": "token"})
        
        with patch('src.engines.crewai.tools.mcp_adapter.AsyncMCPAdapter') as mock_async_class:
            mock_async_instance = MagicMock()
            mock_async_instance.initialize = AsyncMock(side_effect=Exception("Async init failed"))
            mock_async_class.return_value = mock_async_instance
            
            # Should re-raise the exception from async adapter
            with pytest.raises(Exception, match="Async init failed"):
                await adapter.initialize()
    
    def test_getattr_method_delegation(self):
        """Test that method calls are properly delegated to async adapter."""
        adapter = MCPAdapter("http://test.com", {})
        
        mock_async_adapter = MagicMock()
        mock_method = MagicMock(return_value="method_result")
        mock_async_adapter.some_method = mock_method
        adapter._async_adapter = mock_async_adapter
        
        # Call the method and check delegation
        result = adapter.some_method("arg1", key="value")
        assert result == "method_result"
        mock_method.assert_called_once_with("arg1", key="value")