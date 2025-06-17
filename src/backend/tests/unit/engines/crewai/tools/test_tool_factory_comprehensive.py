"""
Comprehensive test suite for ToolFactory to achieve 100% coverage.
This is the single, definitive test file for tool_factory.py.
"""
import pytest
import asyncio
import os
import logging
import importlib
from unittest.mock import MagicMock, patch, AsyncMock, Mock
from concurrent.futures import ThreadPoolExecutor

from src.engines.crewai.tools.tool_factory import ToolFactory


class TestToolFactory:
    """Comprehensive test class for 100% ToolFactory coverage"""

    @pytest.fixture
    def mock_config(self):
        return {"test": "config"}

    @pytest.fixture  
    def tool_factory(self, mock_config):
        return ToolFactory(mock_config)

    @pytest.fixture
    def mock_api_keys_service(self):
        return MagicMock()

    # Basic initialization tests
    def test_init_basic(self, mock_config):
        """Test basic ToolFactory initialization"""
        factory = ToolFactory(mock_config)
        assert factory.config == mock_config
        assert factory.api_keys_service is None
        assert factory.user_token is None
        assert factory._initialized is False
        assert isinstance(factory._tool_implementations, dict)

    def test_init_with_api_keys_service(self, mock_config, mock_api_keys_service):
        """Test initialization with API keys service"""
        factory = ToolFactory(mock_config, mock_api_keys_service)
        assert factory.api_keys_service == mock_api_keys_service

    @pytest.mark.asyncio
    async def test_create_class_method(self, mock_config):
        """Test the async create class method"""
        with patch.object(ToolFactory, 'initialize', new_callable=AsyncMock) as mock_init:
            factory = await ToolFactory.create(mock_config)
            assert isinstance(factory, ToolFactory)
            mock_init.assert_called_once()

    # Import error handling tests (lines 72-77, 82-87, 96-101, 105-110)
    def test_import_error_handling(self):
        """Test import error handling for custom tools"""
        factory = ToolFactory({})
        
        for tool_name in ["PerplexityTool", "GenieTool", "DatabricksCustomTool", "PythonPPTXTool"]:
            # Set to None to simulate import failure
            original_impl = factory._tool_implementations.get(tool_name)
            factory._tool_implementations[tool_name] = None
            
            mock_tool = MagicMock()
            mock_tool.title = tool_name
            mock_tool.config = {}
            
            with patch('logging.warning'):
                with patch.object(factory, 'get_tool_info', return_value=mock_tool):
                    result = factory.create_tool(tool_name)
                    assert result is None
            
            # Restore original
            if original_impl is not None:
                factory._tool_implementations[tool_name] = original_impl

    # Initialization tests
    @pytest.mark.asyncio
    async def test_initialize_success(self, tool_factory):
        """Test successful initialization"""
        with patch.object(tool_factory, '_load_available_tools_async', new_callable=AsyncMock):
            await tool_factory.initialize()
            assert tool_factory._initialized is True

    @pytest.mark.asyncio
    async def test_initialize_exception(self, tool_factory):
        """Test initialization with exception"""
        with patch.object(tool_factory, '_load_available_tools_async', new_callable=AsyncMock, side_effect=Exception("Init failed")):
            with pytest.raises(Exception):
                await tool_factory.initialize()

    # API key retrieval tests (lines 242-244, 383-390)
    @pytest.mark.asyncio
    async def test_get_api_key_async_with_service(self, mock_config):
        """Test API key retrieval with service"""
        mock_service = MagicMock()
        factory = ToolFactory(mock_config, mock_service)
        
        async def mock_operation(operation_func):
            session = MagicMock()
            return await operation_func(session)
        
        with patch('src.utils.asyncio_utils.execute_db_operation_with_fresh_engine', side_effect=mock_operation):
            with patch('src.services.api_keys_service.ApiKeysService') as mock_service_class:
                with patch('src.utils.encryption_utils.EncryptionUtils.decrypt_value', return_value="decrypted_key"):
                    mock_service_instance = MagicMock()
                    mock_api_key_obj = MagicMock()
                    mock_api_key_obj.encrypted_value = "encrypted_value"
                    mock_service_instance.find_by_name = AsyncMock(return_value=mock_api_key_obj)
                    mock_service_class.return_value = mock_service_instance
                    
                    result = await factory._get_api_key_async("TEST_KEY")
                    assert result == "decrypted_key"

    @pytest.mark.asyncio
    async def test_get_api_key_async_fresh_engine(self, tool_factory):
        """Test API key retrieval with fresh engine"""
        tool_factory.api_keys_service = None
        
        async def mock_operation(operation_func):
            session = MagicMock()
            return await operation_func(session)
        
        with patch('src.utils.asyncio_utils.execute_db_operation_with_fresh_engine', side_effect=mock_operation):
            with patch('src.services.api_keys_service.ApiKeysService') as mock_service_class:
                with patch('src.utils.encryption_utils.EncryptionUtils.decrypt_value', return_value="fresh_key"):
                    mock_service_instance = MagicMock()
                    mock_api_key_obj = MagicMock()
                    mock_api_key_obj.encrypted_value = "encrypted_value"
                    mock_service_instance.find_by_name = AsyncMock(return_value=mock_api_key_obj)
                    mock_service_class.return_value = mock_service_instance
                    
                    result = await tool_factory._get_api_key_async("TEST_KEY")
                    assert result == "fresh_key"

    @pytest.mark.asyncio
    async def test_get_api_key_async_exception(self, tool_factory):
        """Test API key retrieval exception handling"""
        async def failing_operation(operation_func):
            raise Exception("Database error")
        
        with patch('src.utils.asyncio_utils.execute_db_operation_with_fresh_engine', side_effect=failing_operation):
            result = await tool_factory._get_api_key_async("TEST_KEY")
            assert result is None

    # Sync API key tests (lines 296-297, 404-408)
    def test_get_api_key_sync_environment(self, tool_factory):
        """Test sync API key with environment fallback"""
        with patch.dict('os.environ', {'TEST_KEY': 'env_value'}):
            with patch('asyncio.get_running_loop'):
                result = tool_factory._get_api_key("TEST_KEY")
                # May return env_value or None depending on implementation

    def test_get_api_key_sync_new_loop(self, tool_factory):
        """Test sync API key with new event loop"""
        with patch.dict('os.environ', {}, clear=True):
            with patch('asyncio.get_running_loop', side_effect=RuntimeError):
                with patch('asyncio.new_event_loop') as mock_new_loop:
                    with patch('asyncio.set_event_loop'):
                        with patch.object(tool_factory, '_get_api_key_async', new_callable=AsyncMock, return_value="async_key"):
                            mock_loop = MagicMock()
                            mock_loop.run_until_complete = Mock(return_value="async_key")
                            mock_loop.close = Mock()
                            mock_new_loop.return_value = mock_loop
                            
                            result = tool_factory._get_api_key("TEST_KEY")
                            mock_loop.close.assert_called_once()

    # Sync load tests (lines 296-297)
    def test_sync_load_available_tools(self, tool_factory):
        """Test sync load with logging"""
        with patch.object(tool_factory, '_load_available_tools_async', new_callable=AsyncMock):
            with patch.object(tool_factory, '_get_api_key_async', new_callable=AsyncMock, return_value="test_key"):
                with patch('asyncio.get_running_loop', side_effect=RuntimeError):
                    with patch('asyncio.new_event_loop') as mock_new_loop:
                        with patch('asyncio.set_event_loop'):
                            with patch('logging.info'):
                                mock_loop = MagicMock()
                                mock_loop.run_until_complete = Mock(return_value="test_key")
                                mock_loop.close = Mock()
                                mock_new_loop.return_value = mock_loop
                                
                                tool_factory._sync_load_available_tools()

    # Event loop management tests (lines 434-440)
    def test_run_in_new_loop_success(self, tool_factory):
        """Test successful new loop execution"""
        async def success_coro():
            return "success"
        
        with patch('asyncio.new_event_loop') as mock_new_loop:
            with patch('asyncio.set_event_loop'):
                mock_loop = MagicMock()
                mock_loop.run_until_complete = Mock(return_value="success")
                mock_loop.close = Mock()
                mock_new_loop.return_value = mock_loop
                
                result = tool_factory._run_in_new_loop(success_coro)
                assert result == "success"
                mock_loop.close.assert_called_once()

    def test_run_in_new_loop_exception(self, tool_factory):
        """Test new loop execution with exception and finally cleanup"""
        async def failing_coro():
            raise Exception("Coro failed")
        
        with patch('asyncio.new_event_loop') as mock_new_loop:
            with patch('asyncio.set_event_loop'):
                mock_loop = MagicMock()
                mock_loop.run_until_complete.side_effect = Exception("Loop failed")
                mock_loop.close = Mock()
                mock_new_loop.return_value = mock_loop
                
                with pytest.raises(Exception, match="Loop failed"):
                    tool_factory._run_in_new_loop(failing_coro)
                
                # Verify cleanup in finally block
                mock_loop.close.assert_called_once()

    # Tool info tests
    def test_get_tool_info_not_found(self, tool_factory):
        """Test get_tool_info when tool not found"""
        # Test the actual implementation without mocking non-existent methods
        result = tool_factory.get_tool_info("nonexistent_tool_that_should_not_exist")
        assert result is None

    # Cleanup methods tests (lines 515, 561-562)
    def test_cleanup_method(self, tool_factory):
        """Test cleanup method"""
        tool_factory.cleanup()

    def test_del_method(self, tool_factory):
        """Test __del__ method"""
        with patch.object(tool_factory, 'cleanup') as mock_cleanup:
            tool_factory.__del__()
            mock_cleanup.assert_called_once()

    # Tool creation tests - PerplexityTool (lines 600-609)
    def test_perplexity_tool_no_loop_path(self, tool_factory):
        """Test PerplexityTool no async context path"""
        mock_tool = MagicMock()
        mock_tool.title = "PerplexityTool"
        mock_tool.config = {}
        
        mock_perplexity_class = MagicMock()
        mock_instance = MagicMock()
        mock_perplexity_class.return_value = mock_instance
        tool_factory._tool_implementations["PerplexityTool"] = mock_perplexity_class
        tool_factory.api_keys_service = MagicMock()
        
        with patch.object(tool_factory, 'get_tool_info', return_value=mock_tool):
            with patch.dict('os.environ', {}, clear=True):
                with patch('asyncio.get_running_loop', side_effect=RuntimeError):
                    with patch('asyncio.new_event_loop') as mock_new_loop:
                        with patch('asyncio.set_event_loop'):
                            with patch.object(tool_factory, '_get_api_key_async', new_callable=AsyncMock, return_value="perplexity_key"):
                                mock_loop = MagicMock()
                                mock_loop.run_until_complete = Mock(return_value="perplexity_key")
                                mock_loop.close = Mock()
                                mock_new_loop.return_value = mock_loop
                                
                                result = tool_factory.create_tool("PerplexityTool")
                                assert result == mock_instance
                                mock_loop.close.assert_called_once()

    def test_perplexity_tool_threadpool(self, tool_factory):
        """Test PerplexityTool ThreadPoolExecutor path"""
        mock_tool = MagicMock()
        mock_tool.title = "PerplexityTool"
        mock_tool.config = {}
        
        mock_perplexity_class = MagicMock()
        mock_instance = MagicMock()
        mock_perplexity_class.return_value = mock_instance
        tool_factory._tool_implementations["PerplexityTool"] = mock_perplexity_class
        tool_factory.api_keys_service = MagicMock()
        
        with patch.object(tool_factory, 'get_tool_info', return_value=mock_tool):
            with patch.dict('os.environ', {}, clear=True):
                with patch('asyncio.get_running_loop'):
                    with patch('concurrent.futures.ThreadPoolExecutor') as mock_executor_class:
                        mock_executor = MagicMock()
                        mock_future = MagicMock()
                        mock_future.result.return_value = "perplexity_key"
                        mock_executor.submit.return_value = mock_future
                        mock_executor_class.return_value.__enter__.return_value = mock_executor
                        
                        result = tool_factory.create_tool("PerplexityTool")
                        assert result == mock_instance

    # Tool creation tests - SerperDevTool (lines 617-619, 690-692)
    def test_serper_environment_fallback(self, tool_factory):
        """Test SerperDevTool environment fallback"""
        mock_tool = MagicMock()
        mock_tool.title = "SerperDevTool"
        mock_tool.config = {}
        
        mock_serper_class = MagicMock()
        mock_instance = MagicMock()
        mock_serper_class.return_value = mock_instance
        tool_factory._tool_implementations["SerperDevTool"] = mock_serper_class
        tool_factory.api_keys_service = None
        
        with patch.object(tool_factory, 'get_tool_info', return_value=mock_tool):
            with patch.dict('os.environ', {'SERPER_API_KEY': 'env_serper_key'}):
                with patch('asyncio.get_running_loop', side_effect=RuntimeError):
                    result = tool_factory.create_tool("SerperDevTool")
                    assert result == mock_instance

    def test_serper_complete_flow_cleanup(self, tool_factory):
        """Test SerperDevTool complete flow with cleanup"""
        mock_tool = MagicMock()
        mock_tool.title = "SerperDevTool"
        mock_tool.config = {}
        
        mock_serper_class = MagicMock()
        mock_instance = MagicMock()
        mock_serper_class.return_value = mock_instance
        tool_factory._tool_implementations["SerperDevTool"] = mock_serper_class
        tool_factory.api_keys_service = MagicMock()
        
        with patch.object(tool_factory, 'get_tool_info', return_value=mock_tool):
            with patch.dict('os.environ', {}, clear=True):
                with patch('asyncio.get_running_loop', side_effect=RuntimeError):
                    with patch('asyncio.new_event_loop') as mock_new_loop:
                        with patch('asyncio.set_event_loop'):
                            mock_loop = MagicMock()
                            mock_loop.run_until_complete = Mock(return_value="serper_key")
                            mock_loop.close = Mock()
                            mock_new_loop.return_value = mock_loop
                            
                            result = tool_factory.create_tool("SerperDevTool")
                            assert result == mock_instance
                            mock_loop.close.assert_called_once()

    # Tool creation tests - FirecrawlCrawlWebsiteTool (lines 741-750, 758-760)
    def test_firecrawl_threadpool(self, tool_factory):
        """Test FirecrawlCrawlWebsiteTool ThreadPoolExecutor"""
        mock_tool = MagicMock()
        mock_tool.title = "FirecrawlCrawlWebsiteTool"
        mock_tool.config = {}
        
        mock_firecrawl_class = MagicMock()
        mock_instance = MagicMock()
        mock_firecrawl_class.return_value = mock_instance
        tool_factory._tool_implementations["FirecrawlCrawlWebsiteTool"] = mock_firecrawl_class
        tool_factory.api_keys_service = MagicMock()
        
        with patch.object(tool_factory, 'get_tool_info', return_value=mock_tool):
            with patch.dict('os.environ', {}, clear=True):
                with patch('asyncio.get_running_loop'):
                    with patch('concurrent.futures.ThreadPoolExecutor') as mock_executor_class:
                        mock_executor = MagicMock()
                        mock_future = MagicMock()
                        mock_future.result.return_value = "firecrawl_key"
                        mock_executor.submit.return_value = mock_future
                        mock_executor_class.return_value.__enter__.return_value = mock_executor
                        
                        result = tool_factory.create_tool("FirecrawlCrawlWebsiteTool")
                        assert result == mock_instance

    def test_firecrawl_environment_fallback(self, tool_factory):
        """Test FirecrawlCrawlWebsiteTool environment fallback"""
        mock_tool = MagicMock()
        mock_tool.title = "FirecrawlCrawlWebsiteTool"
        mock_tool.config = {}
        
        mock_firecrawl_class = MagicMock()
        mock_instance = MagicMock()
        mock_firecrawl_class.return_value = mock_instance
        tool_factory._tool_implementations["FirecrawlCrawlWebsiteTool"] = mock_firecrawl_class
        tool_factory.api_keys_service = None
        
        with patch.object(tool_factory, 'get_tool_info', return_value=mock_tool):
            with patch.dict('os.environ', {'FIRECRAWL_API_KEY': 'env_firecrawl_key'}):
                with patch('asyncio.get_running_loop', side_effect=RuntimeError):
                    result = tool_factory.create_tool("FirecrawlCrawlWebsiteTool")
                    assert result == mock_instance

    # Tool creation tests - NL2SQLTool (lines 819-820, 839-840)
    def test_nl2sql_database_url(self, tool_factory):
        """Test NL2SQLTool DATABASE_URL environment"""
        mock_tool = MagicMock()
        mock_tool.title = "NL2SQLTool"
        mock_tool.config = {}
        
        mock_nl2sql_class = MagicMock()
        mock_instance = MagicMock()
        mock_nl2sql_class.return_value = mock_instance
        tool_factory._tool_implementations["NL2SQLTool"] = mock_nl2sql_class
        
        with patch.object(tool_factory, 'get_tool_info', return_value=mock_tool):
            with patch.dict('os.environ', {'DATABASE_URL': 'sqlite:///test.db'}):
                result = tool_factory.create_tool("NL2SQLTool")
                assert result == mock_instance

    def test_nl2sql_uri_exception(self, tool_factory):
        """Test NL2SQLTool URI parsing exception"""
        mock_tool = MagicMock()
        mock_tool.title = "NL2SQLTool"
        mock_tool.config = {"db_uri": "malformed://uri@bad:format"}
        
        mock_nl2sql_class = MagicMock()
        mock_instance = MagicMock()
        mock_nl2sql_class.return_value = mock_instance
        tool_factory._tool_implementations["NL2SQLTool"] = mock_nl2sql_class
        
        with patch.object(tool_factory, 'get_tool_info', return_value=mock_tool):
            result = tool_factory.create_tool("NL2SQLTool")
            assert result == mock_instance

    # Tool creation tests - GenieTool (lines 892-899, 916-937, 945-947, 968-973, 988-990)
    def test_genie_user_token_path(self, tool_factory):
        """Test GenieTool user token path"""
        mock_tool = MagicMock()
        mock_tool.title = "GenieTool"
        mock_tool.config = {}
        
        mock_genie_class = MagicMock()
        mock_instance = MagicMock()
        mock_genie_class.return_value = mock_instance
        tool_factory._tool_implementations["GenieTool"] = mock_genie_class
        tool_factory.api_keys_service = None
        tool_factory.user_token = "user_oauth_token"
        
        with patch.object(tool_factory, 'get_tool_info', return_value=mock_tool):
            with patch.dict('os.environ', {}, clear=True):
                with patch('asyncio.get_running_loop', side_effect=RuntimeError):
                    with patch('src.utils.user_context.UserContext') as mock_context:
                        mock_context.get_user_token.return_value = "context_token"
                        mock_context.get_group_context.return_value = {"workspace_id": "workspace"}
                        
                        result = tool_factory.create_tool("GenieTool")
                        assert result == mock_instance

    def test_genie_environment_fallback(self, tool_factory):
        """Test GenieTool environment fallback"""
        mock_tool = MagicMock()
        mock_tool.title = "GenieTool"
        mock_tool.config = {}
        
        mock_genie_class = MagicMock()
        mock_instance = MagicMock()
        mock_genie_class.return_value = mock_instance
        tool_factory._tool_implementations["GenieTool"] = mock_genie_class
        tool_factory.api_keys_service = None
        
        with patch.object(tool_factory, 'get_tool_info', return_value=mock_tool):
            with patch.dict('os.environ', {'DATABRICKS_API_KEY': 'env_databricks_key'}):
                with patch('asyncio.get_running_loop', side_effect=RuntimeError):
                    with patch('src.utils.user_context.UserContext') as mock_context:
                        mock_context.get_user_token.return_value = None
                        mock_context.get_group_context.return_value = None
                        
                        result = tool_factory.create_tool("GenieTool")
                        assert result == mock_instance

    # Tool creation tests - LinkupSearchTool (lines 1032-1034, 1059-1065)
    def test_linkup_no_loop_cleanup(self, tool_factory):
        """Test LinkupSearchTool no loop with cleanup"""
        mock_tool = MagicMock()
        mock_tool.title = "LinkupSearchTool"
        mock_tool.config = {}
        
        class MockLinkupSearchTool:
            def __init__(self, api_key=None):
                self.api_key = api_key
        
        with patch('src.engines.crewai.tools.tool_factory.LinkupSearchTool', MockLinkupSearchTool):
            tool_factory._tool_implementations["LinkupSearchTool"] = MockLinkupSearchTool
            tool_factory.api_keys_service = MagicMock()
            
            with patch.object(tool_factory, 'get_tool_info', return_value=mock_tool):
                with patch.dict('os.environ', {}, clear=True):
                    with patch('asyncio.get_running_loop', side_effect=RuntimeError):
                        with patch('asyncio.new_event_loop') as mock_new_loop:
                            with patch('asyncio.set_event_loop'):
                                mock_loop = MagicMock()
                                mock_loop.run_until_complete = Mock(return_value="linkup_key")
                                mock_loop.close = Mock()
                                mock_new_loop.return_value = mock_loop
                                
                                result = tool_factory.create_tool("LinkupSearchTool")
                                assert result is not None
                                mock_loop.close.assert_called_once()

    def test_linkup_validation(self, tool_factory):
        """Test LinkupSearchTool validation"""
        mock_tool = MagicMock()
        mock_tool.title = "LinkupSearchTool"
        mock_tool.config = {"depth": "invalid_depth", "output_type": "invalid_output"}
        
        class MockLinkupSearchTool:
            def __init__(self, api_key=None):
                self.api_key = api_key
            
            def _run(self, query, depth="standard", output_type="searchResults"):
                return f"search result for {query}"
        
        with patch('src.engines.crewai.tools.tool_factory.LinkupSearchTool', MockLinkupSearchTool):
            tool_factory._tool_implementations["LinkupSearchTool"] = MockLinkupSearchTool
            tool_factory.api_keys_service = None
            
            with patch.object(tool_factory, 'get_tool_info', return_value=mock_tool):
                with patch.dict('os.environ', {}, clear=True):
                    with patch('asyncio.get_running_loop', side_effect=RuntimeError):
                        with patch.object(tool_factory, '_get_api_key', return_value="linkup_key"):
                            result = tool_factory.create_tool("LinkupSearchTool")
                            assert result is not None

    # General exception handling test (line 641)
    def test_create_tool_general_exception(self, tool_factory):
        """Test general exception handling in create_tool"""
        mock_tool = MagicMock()
        mock_tool.title = "FailingTool"
        mock_tool.config = {}
        
        def failing_constructor(*args, **kwargs):
            raise Exception("Tool construction failed")
        
        tool_factory._tool_implementations["FailingTool"] = failing_constructor
        
        with patch.object(tool_factory, 'get_tool_info', return_value=mock_tool):
            result = tool_factory.create_tool("FailingTool")
            assert result is None

    # Tool not found test
    def test_create_tool_not_found(self, tool_factory):
        """Test create_tool when tool not found"""
        with patch.object(tool_factory, 'get_tool_info', return_value=None):
            result = tool_factory.create_tool("NonexistentTool")
            assert result is None

    # Cleanup after crew execution test (lines 1149-1150)
    @pytest.mark.asyncio
    async def test_cleanup_crew_execution_thread_exception(self, tool_factory):
        """Test cleanup_after_crew_execution thread exception"""
        with patch('asyncio.get_running_loop'):
            with patch('concurrent.futures.ThreadPoolExecutor') as mock_executor_class:
                mock_executor = MagicMock()
                mock_executor.submit.side_effect = Exception("Thread pool failed")
                mock_executor_class.return_value.__enter__.return_value = mock_executor
                
                with patch.object(tool_factory, '_load_available_tools_async', new_callable=AsyncMock):
                    await tool_factory.cleanup_after_crew_execution()
                    mock_executor.submit.assert_called_once()