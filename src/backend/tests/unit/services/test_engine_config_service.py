"""
Unit tests for EngineConfigService.

Tests the functionality of engine config service including
CrewAI flow configuration management and error handling.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.services.engine_config_service import EngineConfigService
from src.repositories.engine_config_repository import EngineConfigRepository


class TestEngineConfigServiceCrewAIFlowMethods:
    """Test cases for CrewAI flow specific methods."""
    
    @pytest.fixture
    def mock_repository(self):
        """Create a mock repository."""
        return AsyncMock(spec=EngineConfigRepository)
    
    @pytest.fixture
    def engine_config_service(self, mock_repository):
        """Create an engine config service with mock repository."""
        return EngineConfigService(repository=mock_repository)
    
    @pytest.mark.asyncio
    async def test_get_crewai_flow_enabled_success(self, engine_config_service, mock_repository):
        """Test successful get CrewAI flow enabled."""
        mock_repository.get_crewai_flow_enabled.return_value = True
        
        result = await engine_config_service.get_crewai_flow_enabled()
        
        assert result is True
        mock_repository.get_crewai_flow_enabled.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_crewai_flow_enabled_false(self, engine_config_service, mock_repository):
        """Test get CrewAI flow enabled returns false."""
        mock_repository.get_crewai_flow_enabled.return_value = False
        
        result = await engine_config_service.get_crewai_flow_enabled()
        
        assert result is False
        mock_repository.get_crewai_flow_enabled.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_crewai_flow_enabled_error_defaults_false(self, engine_config_service, mock_repository):
        """Test get CrewAI flow enabled defaults to False on error."""
        mock_repository.get_crewai_flow_enabled.side_effect = Exception("Database error")
        
        with patch('src.services.engine_config_service.logger') as mock_logger:
            result = await engine_config_service.get_crewai_flow_enabled()
            
            assert result is False
            mock_repository.get_crewai_flow_enabled.assert_called_once()
            mock_logger.error.assert_called_once()
            
            # Verify the error message contains the expected text
            error_call_args = mock_logger.error.call_args[0][0]
            assert "Error getting CrewAI flow enabled status" in error_call_args
    
    @pytest.mark.asyncio
    async def test_set_crewai_flow_enabled_success(self, engine_config_service, mock_repository):
        """Test successful set CrewAI flow enabled."""
        mock_repository.set_crewai_flow_enabled.return_value = True
        
        result = await engine_config_service.set_crewai_flow_enabled(True)
        
        assert result is True
        mock_repository.set_crewai_flow_enabled.assert_called_once_with(True)
    
    @pytest.mark.asyncio
    async def test_set_crewai_flow_enabled_false(self, engine_config_service, mock_repository):
        """Test set CrewAI flow enabled to false."""
        mock_repository.set_crewai_flow_enabled.return_value = True
        
        result = await engine_config_service.set_crewai_flow_enabled(False)
        
        assert result is True
        mock_repository.set_crewai_flow_enabled.assert_called_once_with(False)
    
    @pytest.mark.asyncio
    async def test_set_crewai_flow_enabled_error(self, engine_config_service, mock_repository):
        """Test set CrewAI flow enabled with error."""
        mock_repository.set_crewai_flow_enabled.side_effect = Exception("Database error")
        
        with patch('src.services.engine_config_service.logger') as mock_logger:
            with pytest.raises(Exception, match="Database error"):
                await engine_config_service.set_crewai_flow_enabled(True)
            
            mock_repository.set_crewai_flow_enabled.assert_called_once_with(True)
            mock_logger.error.assert_called_once()
            
            # Verify the error message contains the expected text
            error_call_args = mock_logger.error.call_args[0][0]
            assert "Error setting CrewAI flow enabled status" in error_call_args


class TestEngineConfigServiceFromUnitOfWork:
    """Test cases for creating service from unit of work."""
    
    @pytest.mark.asyncio
    async def test_from_unit_of_work_success(self):
        """Test successful creation from unit of work."""
        mock_uow = MagicMock()
        mock_repository = AsyncMock(spec=EngineConfigRepository)
        mock_uow.engine_config_repository = mock_repository
        
        service = await EngineConfigService.from_unit_of_work(mock_uow)
        
        assert isinstance(service, EngineConfigService)
        assert service.repository == mock_repository