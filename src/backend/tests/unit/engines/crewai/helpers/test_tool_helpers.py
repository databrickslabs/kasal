import pytest
from unittest.mock import MagicMock, AsyncMock, patch
from typing import List, Dict, Any

from src.engines.crewai.kernel.tool_helpers import (
    resolve_tool_ids_to_names,
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
        
        with patch('src.engines.crewai.kernel.tool_helpers.logger') as mock_logger:
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
        
        with patch('src.engines.crewai.kernel.tool_helpers.logger'):
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
        
        with patch('src.engines.crewai.kernel.tool_helpers.logger'):
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
        
        with patch('src.engines.crewai.kernel.tool_helpers.logger') as mock_logger:
            result = await resolve_tool_ids_to_names([1, 2], mock_tool_service)
            
            assert result == ["Working Tool", ""]  # Empty string for failed resolution
            mock_logger.info.assert_any_call("Resolved tool ID 1 to name: Working Tool")
            mock_logger.error.assert_any_call("Error resolving tool ID 2: Tool not found")
    
    @pytest.mark.asyncio
    async def test_resolve_tool_ids_invalid_string_conversion(self):
        """Test resolving with invalid string that can't convert to int."""
        mock_tool_service = AsyncMock()
        
        with patch('src.engines.crewai.kernel.tool_helpers.logger') as mock_logger:
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
        
        with patch('src.engines.crewai.kernel.tool_helpers.logger') as mock_logger:
            result = await resolve_tool_ids_to_names([1, 2, 3], mock_tool_service)
            
            assert result == ["", "", ""]
            assert mock_logger.error.call_count == 3


