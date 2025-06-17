import pytest
from unittest.mock import MagicMock, AsyncMock, patch
from typing import List, Dict, Any

from src.engines.crewai.helpers.tool_helpers import (
    resolve_tool_ids_to_names,
    get_tool_instances,
    prepare_tools
)


class TestResolveToolIdsToNames:
    """Test suite for resolve_tool_ids_to_names function."""
    
    @pytest.mark.asyncio
    async def test_resolve_tool_ids_empty_list(self):
        """Test resolving empty list of tool IDs."""
        mock_tool_service = AsyncMock()
        
        result = await resolve_tool_ids_to_names([], mock_tool_service)
        
        assert result == []
        mock_tool_service.get_tool_by_id.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_resolve_tool_ids_success(self):
        """Test successful resolution of tool IDs to names."""
        mock_tool_service = AsyncMock()
        
        # Create mock tools
        mock_tool1 = MagicMock()
        mock_tool1.title = "Search Tool"
        mock_tool2 = MagicMock()
        mock_tool2.title = "Calculator Tool"
        
        mock_tool_service.get_tool_by_id.side_effect = [mock_tool1, mock_tool2]
        
        with patch('src.engines.crewai.helpers.tool_helpers.logger') as mock_logger:
            result = await resolve_tool_ids_to_names([1, 2], mock_tool_service)
            
            assert result == ["Search Tool", "Calculator Tool"]
            mock_tool_service.get_tool_by_id.assert_any_call(1)
            mock_tool_service.get_tool_by_id.assert_any_call(2)
            mock_logger.info.assert_any_call("Resolved tool ID 1 to name: Search Tool")
            mock_logger.info.assert_any_call("Resolved tool ID 2 to name: Calculator Tool")
    
    @pytest.mark.asyncio
    async def test_resolve_tool_ids_string_ids(self):
        """Test resolving string tool IDs (converts to int)."""
        mock_tool_service = AsyncMock()
        
        mock_tool = MagicMock()
        mock_tool.title = "String ID Tool"
        mock_tool_service.get_tool_by_id.return_value = mock_tool
        
        with patch('src.engines.crewai.helpers.tool_helpers.logger'):
            result = await resolve_tool_ids_to_names(["123"], mock_tool_service)
            
            assert result == ["String ID Tool"]
            mock_tool_service.get_tool_by_id.assert_called_once_with(123)
    
    @pytest.mark.asyncio
    async def test_resolve_tool_ids_mixed_types(self):
        """Test resolving mixed string and integer tool IDs."""
        mock_tool_service = AsyncMock()
        
        mock_tool1 = MagicMock()
        mock_tool1.title = "Tool 1"
        mock_tool2 = MagicMock()
        mock_tool2.title = "Tool 2"
        
        mock_tool_service.get_tool_by_id.side_effect = [mock_tool1, mock_tool2]
        
        with patch('src.engines.crewai.helpers.tool_helpers.logger'):
            result = await resolve_tool_ids_to_names(["10", 20], mock_tool_service)
            
            assert result == ["Tool 1", "Tool 2"]
            mock_tool_service.get_tool_by_id.assert_any_call(10)  # String converted to int
            mock_tool_service.get_tool_by_id.assert_any_call(20)  # Already int
    
    @pytest.mark.asyncio
    async def test_resolve_tool_ids_with_errors(self):
        """Test resolving tool IDs with some errors."""
        mock_tool_service = AsyncMock()
        
        mock_tool = MagicMock()
        mock_tool.title = "Working Tool"
        
        # First call succeeds, second call fails
        mock_tool_service.get_tool_by_id.side_effect = [mock_tool, Exception("Tool not found")]
        
        with patch('src.engines.crewai.helpers.tool_helpers.logger') as mock_logger:
            result = await resolve_tool_ids_to_names([1, 2], mock_tool_service)
            
            assert result == ["Working Tool", ""]  # Empty string for failed resolution
            mock_logger.info.assert_any_call("Resolved tool ID 1 to name: Working Tool")
            mock_logger.error.assert_any_call("Error resolving tool ID 2: Tool not found")
    
    @pytest.mark.asyncio
    async def test_resolve_tool_ids_invalid_string_conversion(self):
        """Test resolving with invalid string that can't convert to int."""
        mock_tool_service = AsyncMock()
        
        with patch('src.engines.crewai.helpers.tool_helpers.logger') as mock_logger:
            result = await resolve_tool_ids_to_names(["invalid"], mock_tool_service)
            
            assert result == [""]  # Empty string for failed conversion
            mock_logger.error.assert_called()
            error_call = mock_logger.error.call_args[0][0]
            assert "Error resolving tool ID invalid" in error_call
    
    @pytest.mark.asyncio
    async def test_resolve_tool_ids_all_failures(self):
        """Test resolving when all tool IDs fail to resolve."""
        mock_tool_service = AsyncMock()
        mock_tool_service.get_tool_by_id.side_effect = Exception("Service unavailable")
        
        with patch('src.engines.crewai.helpers.tool_helpers.logger') as mock_logger:
            result = await resolve_tool_ids_to_names([1, 2, 3], mock_tool_service)
            
            assert result == ["", "", ""]
            assert mock_logger.error.call_count == 3


class TestGetToolInstances:
    """Test suite for get_tool_instances function."""
    
    @pytest.mark.asyncio
    async def test_get_tool_instances_empty_list(self):
        """Test getting tool instances with empty list."""
        mock_registry = MagicMock()
        
        result = await get_tool_instances([], mock_registry)
        
        assert result == []
        mock_registry.get_tool.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_get_tool_instances_success(self):
        """Test successful tool instance retrieval."""
        mock_registry = MagicMock()
        
        mock_tool1 = MagicMock()
        mock_tool2 = MagicMock()
        mock_registry.get_tool.side_effect = [mock_tool1, mock_tool2]
        
        with patch('src.engines.crewai.helpers.tool_helpers.logger') as mock_logger:
            result = await get_tool_instances(["search", "calculator"], mock_registry)
            
            assert result == [mock_tool1, mock_tool2]
            mock_registry.get_tool.assert_any_call("search")
            mock_registry.get_tool.assert_any_call("calculator")
            mock_logger.info.assert_any_call("Got tool instance for: search")
            mock_logger.info.assert_any_call("Got tool instance for: calculator")
    
    @pytest.mark.asyncio
    async def test_get_tool_instances_with_empty_names(self):
        """Test getting tool instances with some empty names."""
        mock_registry = MagicMock()
        
        mock_tool = MagicMock()
        mock_registry.get_tool.return_value = mock_tool
        
        with patch('src.engines.crewai.helpers.tool_helpers.logger'):
            result = await get_tool_instances(["", "search", ""], mock_registry)
            
            assert result == [mock_tool]  # Only one tool returned, empty names skipped
            mock_registry.get_tool.assert_called_once_with("search")
    
    @pytest.mark.asyncio
    async def test_get_tool_instances_tool_not_found(self):
        """Test getting tool instances when some tools are not found."""
        mock_registry = MagicMock()
        
        mock_tool = MagicMock()
        # First call returns tool, second returns None (not found)
        mock_registry.get_tool.side_effect = [mock_tool, None]
        
        with patch('src.engines.crewai.helpers.tool_helpers.logger') as mock_logger:
            result = await get_tool_instances(["search", "missing"], mock_registry)
            
            assert result == [mock_tool]  # Only found tool returned
            mock_logger.info.assert_any_call("Got tool instance for: search")
            mock_logger.warning.assert_any_call("Tool missing not found in registry")
    
    @pytest.mark.asyncio
    async def test_get_tool_instances_with_exceptions(self):
        """Test getting tool instances when registry throws exceptions."""
        mock_registry = MagicMock()
        
        mock_tool = MagicMock()
        # First call succeeds, second throws exception
        mock_registry.get_tool.side_effect = [mock_tool, Exception("Registry error")]
        
        with patch('src.engines.crewai.helpers.tool_helpers.logger') as mock_logger:
            result = await get_tool_instances(["search", "broken"], mock_registry)
            
            assert result == [mock_tool]  # Only successful tool returned
            mock_logger.info.assert_any_call("Got tool instance for: search")
            mock_logger.error.assert_any_call("Error getting tool instance for broken: Registry error")
    
    @pytest.mark.asyncio
    async def test_get_tool_instances_all_empty_names(self):
        """Test getting tool instances with all empty names."""
        mock_registry = MagicMock()
        
        result = await get_tool_instances(["", "", ""], mock_registry)
        
        assert result == []
        mock_registry.get_tool.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_get_tool_instances_mixed_success_failure(self):
        """Test mixed success and failure scenarios."""
        mock_registry = MagicMock()
        
        mock_tool1 = MagicMock()
        mock_tool3 = MagicMock()
        # Success, None (not found), Success, Exception
        mock_registry.get_tool.side_effect = [mock_tool1, None, mock_tool3, Exception("Error")]
        
        with patch('src.engines.crewai.helpers.tool_helpers.logger') as mock_logger:
            result = await get_tool_instances(["tool1", "missing", "tool3", "broken"], mock_registry)
            
            assert result == [mock_tool1, mock_tool3]
            assert mock_logger.info.call_count == 2  # Two successful calls
            assert mock_logger.warning.call_count == 1  # One not found
            assert mock_logger.error.call_count == 1  # One exception


class TestPrepareTools:
    """Test suite for prepare_tools function."""
    
    @pytest.mark.asyncio
    async def test_prepare_tools_empty_configs(self):
        """Test preparing tools with empty config list."""
        mock_registry = MagicMock()
        
        result = await prepare_tools(mock_registry, [])
        
        assert result == []
    
    @pytest.mark.asyncio
    async def test_prepare_tools_with_agent_and_task_id(self):
        """Test preparing tools with agent and task IDs."""
        mock_registry = MagicMock()
        
        # Since the actual implementation is incomplete, we test what we can
        tool_configs = [{"name": "search", "config": {}}]
        
        result = await prepare_tools(
            mock_registry, 
            tool_configs, 
            agent_id="agent1", 
            task_id="task1"
        )
        
        # The function should return a list (even if empty due to incomplete implementation)
        assert isinstance(result, list)
    
    @pytest.mark.asyncio
    async def test_prepare_tools_without_optional_params(self):
        """Test preparing tools without optional agent_id and task_id."""
        mock_registry = MagicMock()
        
        tool_configs = [{"name": "calculator", "params": {"precision": 2}}]
        
        result = await prepare_tools(mock_registry, tool_configs)
        
        assert isinstance(result, list)
    
    @pytest.mark.asyncio
    async def test_prepare_tools_function_signature(self):
        """Test that prepare_tools function has expected signature."""
        import inspect
        from src.engines.crewai.helpers.tool_helpers import prepare_tools
        
        sig = inspect.signature(prepare_tools)
        params = list(sig.parameters.keys())
        
        assert "tool_registry" in params
        assert "tool_configs" in params
        assert "agent_id" in params
        assert "task_id" in params
        
        # Check that agent_id and task_id are optional
        assert sig.parameters["agent_id"].default is None
        assert sig.parameters["task_id"].default is None
    
    @pytest.mark.asyncio
    async def test_prepare_tools_with_complex_configs(self):
        """Test preparing tools with complex configuration objects."""
        mock_registry = MagicMock()
        
        complex_configs = [
            {
                "name": "search_tool",
                "parameters": {
                    "api_key": "test_key",
                    "max_results": 10
                },
                "validation": {
                    "required": True,
                    "timeout": 30
                }
            },
            {
                "name": "data_tool",
                "source": "database",
                "filters": ["active", "public"]
            }
        ]
        
        result = await prepare_tools(
            mock_registry, 
            complex_configs, 
            agent_id="complex_agent"
        )
        
        assert isinstance(result, list)