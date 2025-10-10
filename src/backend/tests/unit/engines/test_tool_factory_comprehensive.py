import pytest
from unittest.mock import Mock, patch, AsyncMock, MagicMock
from typing import Dict, Any, Optional, Union
import asyncio

# Test ToolFactory class - based on actual code inspection

from src.engines.crewai.tools.tool_factory import ToolFactory


class TestToolFactoryInit:
    """Test ToolFactory initialization"""

    def test_tool_factory_init_basic(self):
        """Test ToolFactory __init__ with basic config"""
        config = {"test": "value"}
        
        factory = ToolFactory(config)
        
        assert factory.config == config
        assert factory.api_keys_service is None
        assert factory.user_token is None
        assert isinstance(factory._available_tools, dict)
        assert isinstance(factory._tool_implementations, dict)
        assert factory._initialized is False

    def test_tool_factory_init_with_api_keys_service(self):
        """Test ToolFactory __init__ with api_keys_service"""
        config = {"test": "value"}
        mock_api_keys_service = Mock()
        
        factory = ToolFactory(config, api_keys_service=mock_api_keys_service)
        
        assert factory.config == config
        assert factory.api_keys_service == mock_api_keys_service
        assert factory.user_token is None
        assert factory._initialized is False

    def test_tool_factory_init_with_user_token(self):
        """Test ToolFactory __init__ with user_token"""
        config = {"test": "value"}
        user_token = "test-token"
        
        factory = ToolFactory(config, user_token=user_token)
        
        assert factory.config == config
        assert factory.api_keys_service is None
        assert factory.user_token == user_token
        assert factory._initialized is False

    def test_tool_factory_init_with_all_params(self):
        """Test ToolFactory __init__ with all parameters"""
        config = {"test": "value"}
        mock_api_keys_service = Mock()
        user_token = "test-token"
        
        factory = ToolFactory(config, api_keys_service=mock_api_keys_service, user_token=user_token)
        
        assert factory.config == config
        assert factory.api_keys_service == mock_api_keys_service
        assert factory.user_token == user_token
        assert factory._initialized is False

    def test_tool_factory_init_tool_implementations_populated(self):
        """Test ToolFactory __init__ populates tool implementations"""
        config = {"test": "value"}
        
        factory = ToolFactory(config)
        
        assert isinstance(factory._tool_implementations, dict)
        assert len(factory._tool_implementations) > 0
        # Check for some expected tools
        expected_tools = ["PerplexityTool", "Dall-E Tool", "SerperDevTool", "ScrapeWebsiteTool"]
        for tool in expected_tools:
            assert tool in factory._tool_implementations


class TestToolFactoryAsyncMethods:
    """Test ToolFactory async methods"""

    @pytest.mark.asyncio
    @patch.object(ToolFactory, 'initialize')
    async def test_create_class_method(self, mock_initialize):
        """Test ToolFactory.create class method"""
        mock_initialize.return_value = None
        config = {"test": "value"}
        
        factory = await ToolFactory.create(config)
        
        assert isinstance(factory, ToolFactory)
        assert factory.config == config
        mock_initialize.assert_called_once()

    @pytest.mark.asyncio
    @patch.object(ToolFactory, 'initialize')
    async def test_create_class_method_with_params(self, mock_initialize):
        """Test ToolFactory.create class method with all parameters"""
        mock_initialize.return_value = None
        config = {"test": "value"}
        mock_api_keys_service = Mock()
        user_token = "test-token"
        
        factory = await ToolFactory.create(config, api_keys_service=mock_api_keys_service, user_token=user_token)
        
        assert isinstance(factory, ToolFactory)
        assert factory.config == config
        assert factory.api_keys_service == mock_api_keys_service
        assert factory.user_token == user_token
        mock_initialize.assert_called_once()

    @pytest.mark.asyncio
    @patch.object(ToolFactory, '_load_available_tools_async')
    async def test_initialize_not_initialized(self, mock_load_tools):
        """Test initialize when not already initialized"""
        mock_load_tools.return_value = None
        config = {"test": "value"}
        factory = ToolFactory(config)
        
        await factory.initialize()
        
        assert factory._initialized is True
        mock_load_tools.assert_called_once()

    @pytest.mark.asyncio
    @patch.object(ToolFactory, '_load_available_tools_async')
    async def test_initialize_already_initialized(self, mock_load_tools):
        """Test initialize when already initialized"""
        mock_load_tools.return_value = None
        config = {"test": "value"}
        factory = ToolFactory(config)
        factory._initialized = True
        
        await factory.initialize()
        
        mock_load_tools.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_api_key_async_without_service(self):
        """Test _get_api_key_async without api_keys_service"""
        config = {"test": "value"}
        factory = ToolFactory(config)

        result = await factory._get_api_key_async("TEST_KEY")

        assert result is None

    @pytest.mark.asyncio
    async def test_cleanup_after_crew_execution(self):
        """Test cleanup_after_crew_execution method"""
        config = {"test": "value"}
        factory = ToolFactory(config)
        
        # Should complete without errors
        await factory.cleanup_after_crew_execution()


class TestToolFactorySyncMethods:
    """Test ToolFactory synchronous methods"""

    def test_get_tool_info_by_title(self):
        """Test get_tool_info with tool title"""
        config = {"test": "value"}
        factory = ToolFactory(config)

        # Add a mock tool to available tools
        mock_tool = Mock(id=1, title="Test Tool")
        factory._available_tools["Test Tool"] = mock_tool

        result = factory.get_tool_info("Test Tool")

        assert result == mock_tool

    def test_get_tool_info_not_found(self):
        """Test get_tool_info with non-existent tool"""
        config = {"test": "value"}
        factory = ToolFactory(config)
        
        result = factory.get_tool_info("Non-existent Tool")
        
        assert result is None

    def test_get_api_key_sync_method(self):
        """Test _get_api_key synchronous method"""
        config = {"test": "value"}
        factory = ToolFactory(config)
        
        # Should handle the case where no api_keys_service is available
        result = factory._get_api_key("TEST_KEY")
        
        assert result is None

    def test_register_tool_implementation(self):
        """Test register_tool_implementation method"""
        config = {"test": "value"}
        factory = ToolFactory(config)
        
        mock_tool_class = Mock()
        tool_name = "CustomTool"
        
        factory.register_tool_implementation(tool_name, mock_tool_class)
        
        assert factory._tool_implementations[tool_name] == mock_tool_class

    def test_register_tool_implementations_multiple(self):
        """Test register_tool_implementations with multiple tools"""
        config = {"test": "value"}
        factory = ToolFactory(config)
        
        implementations = {
            "CustomTool1": Mock(),
            "CustomTool2": Mock(),
            "CustomTool3": Mock()
        }
        
        factory.register_tool_implementations(implementations)
        
        for tool_name, tool_class in implementations.items():
            assert factory._tool_implementations[tool_name] == tool_class

    def test_cleanup_method(self):
        """Test cleanup method"""
        config = {"test": "value"}
        factory = ToolFactory(config)
        
        # Should complete without errors
        factory.cleanup()

    def test_del_method(self):
        """Test __del__ method calls cleanup"""
        config = {"test": "value"}
        factory = ToolFactory(config)
        
        with patch.object(factory, 'cleanup') as mock_cleanup:
            factory.__del__()
            mock_cleanup.assert_called_once()

    # Removed complex tests that require database connections or event loop management

    def test_update_tool_config_tool_not_found(self):
        """Test update_tool_config when tool is not found"""
        config = {"test": "value"}
        factory = ToolFactory(config)
        
        config_update = {"new_config": "value"}
        
        result = factory.update_tool_config("Non-existent Tool", config_update)

        assert result is False


class TestToolFactoryToolCreation:
    """Test ToolFactory tool creation methods"""

    def setup_method(self):
        """Set up test fixtures"""
        self.config = {"test": "value"}
        self.factory = ToolFactory(self.config)

    def test_create_tool_tool_not_found(self):
        """Test create_tool when tool is not found"""
        tool_identifier = "non-existent-tool"

        with patch.object(self.factory, 'get_tool_info', return_value=None):
            result = self.factory.create_tool(tool_identifier)

            assert result is None


class TestToolFactoryRegistration:
    """Test ToolFactory tool registration methods"""

    def setup_method(self):
        """Set up test fixtures"""
        self.config = {"test": "value"}
        self.factory = ToolFactory(self.config)

    def test_register_tool_implementation(self):
        """Test register_tool_implementation"""
        tool_name = "TestTool"
        tool_class = Mock()

        self.factory.register_tool_implementation(tool_name, tool_class)

        assert self.factory._tool_implementations[tool_name] == tool_class

    def test_register_tool_implementations(self):
        """Test register_tool_implementations with multiple tools"""
        implementations = {
            "Tool1": Mock(),
            "Tool2": Mock(),
            "Tool3": Mock()
        }

        self.factory.register_tool_implementations(implementations)

        for tool_name, tool_class in implementations.items():
            assert self.factory._tool_implementations[tool_name] == tool_class

    def test_register_tool_implementations_empty_dict(self):
        """Test register_tool_implementations with empty dict"""
        implementations = {}

        self.factory.register_tool_implementations(implementations)

        # Should not raise any errors
        assert True


class TestToolFactoryUtilityMethods:
    """Test ToolFactory utility methods"""

    def setup_method(self):
        """Set up test fixtures"""
        self.config = {"test": "value"}
        self.factory = ToolFactory(self.config)

    def test_get_api_key_without_service(self):
        """Test _get_api_key without api_keys_service"""
        self.factory.api_keys_service = None

        result = self.factory._get_api_key("test_key")

        assert result is None

    def test_run_in_new_loop(self):
        """Test _run_in_new_loop utility method"""
        async def test_async_func(value):
            return value * 2

        result = self.factory._run_in_new_loop(test_async_func, 5)

        assert result == 10

    def test_cleanup(self):
        """Test cleanup method"""
        # Should not raise any errors
        self.factory.cleanup()
        assert True

    def test_del_method(self):
        """Test __del__ method calls cleanup"""
        with patch.object(self.factory, 'cleanup') as mock_cleanup:
            self.factory.__del__()

            mock_cleanup.assert_called_once()
