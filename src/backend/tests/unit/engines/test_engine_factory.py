import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from typing import Dict, Any

from src.engines.engine_factory import EngineFactory


class TestEngineFactory:
    """Test suite for EngineFactory class."""
    
    def setup_method(self):
        """Setup before each test method."""
        # Clear the registry and cache before each test
        EngineFactory._registry = {"crewai": MagicMock}
        EngineFactory._instances = {}
    
    def teardown_method(self):
        """Cleanup after each test method."""
        # Reset to original state
        from src.engines.crewai.crewai_engine_service import CrewAIEngineService
        EngineFactory._registry = {"crewai": CrewAIEngineService}
        EngineFactory._instances = {}
    
    def test_register_engine(self):
        """Test engine registration."""
        mock_engine_class = MagicMock()
        
        EngineFactory.register_engine("test_engine", mock_engine_class)
        
        assert "test_engine" in EngineFactory._registry
        assert EngineFactory._registry["test_engine"] == mock_engine_class
    
    @pytest.mark.asyncio
    async def test_get_engine_success(self):
        """Test successful engine creation."""
        # Create mock engine class
        mock_engine_class = MagicMock()
        mock_engine_instance = MagicMock()
        mock_engine_class.return_value = mock_engine_instance
        mock_engine_instance.initialize = AsyncMock(return_value=True)
        
        # Register the mock engine
        EngineFactory.register_engine("test_engine", mock_engine_class)
        
        # Mock database session
        mock_db = MagicMock()
        
        # Get engine
        result = await EngineFactory.get_engine("test_engine", mock_db, {"param": "value"})
        
        # Verify engine was created and initialized
        assert result == mock_engine_instance
        mock_engine_class.assert_called_once_with(mock_db)
        mock_engine_instance.initialize.assert_called_once_with(param="value")
        
        # Verify engine was cached
        assert "test_engine" in EngineFactory._instances
        assert EngineFactory._instances["test_engine"] == mock_engine_instance
    
    @pytest.mark.asyncio
    async def test_get_engine_from_cache(self):
        """Test getting engine from cache."""
        # Create and cache a mock engine
        mock_engine = MagicMock()
        EngineFactory._instances["cached_engine"] = mock_engine
        EngineFactory._registry["cached_engine"] = MagicMock()
        
        # Get engine from cache
        result = await EngineFactory.get_engine("cached_engine")
        
        # Should return cached instance without creating new one
        assert result == mock_engine
    
    @pytest.mark.asyncio
    async def test_get_engine_unknown_type(self):
        """Test getting unknown engine type."""
        result = await EngineFactory.get_engine("unknown_engine")
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_get_engine_initialization_failure(self):
        """Test engine creation when initialization fails."""
        # Create mock engine class that fails initialization
        mock_engine_class = MagicMock()
        mock_engine_instance = MagicMock()
        mock_engine_class.return_value = mock_engine_instance
        mock_engine_instance.initialize = AsyncMock(return_value=False)
        
        # Register the mock engine
        EngineFactory.register_engine("failing_engine", mock_engine_class)
        
        # Get engine
        result = await EngineFactory.get_engine("failing_engine")
        
        # Should return None when initialization fails
        assert result is None
        
        # Should not cache failed instance
        assert "failing_engine" not in EngineFactory._instances
    
    @pytest.mark.asyncio
    async def test_get_engine_with_default_init_params(self):
        """Test engine creation with default initialization parameters."""
        mock_engine_class = MagicMock()
        mock_engine_instance = MagicMock()
        mock_engine_class.return_value = mock_engine_instance
        mock_engine_instance.initialize = AsyncMock(return_value=True)
        
        EngineFactory.register_engine("test_engine", mock_engine_class)
        
        # Get engine without init_params
        result = await EngineFactory.get_engine("test_engine")
        
        # Should use empty dict as default
        mock_engine_instance.initialize.assert_called_once_with()
    
    def test_get_available_engines(self):
        """Test getting list of available engines."""
        # Register some test engines
        EngineFactory.register_engine("engine1", MagicMock())
        EngineFactory.register_engine("engine2", MagicMock())
        
        available = EngineFactory.get_available_engines()
        
        assert "crewai" in available  # Default engine
        assert "engine1" in available
        assert "engine2" in available
        assert len(available) >= 3
    
    def test_clear_cache_specific_engine(self):
        """Test clearing cache for specific engine."""
        # Add some mock engines to cache
        EngineFactory._instances["engine1"] = MagicMock()
        EngineFactory._instances["engine2"] = MagicMock()
        
        # Clear specific engine
        EngineFactory.clear_cache("engine1")
        
        # Verify only engine1 was removed
        assert "engine1" not in EngineFactory._instances
        assert "engine2" in EngineFactory._instances
    
    def test_clear_cache_nonexistent_engine(self):
        """Test clearing cache for nonexistent engine."""
        # Should not raise exception
        EngineFactory.clear_cache("nonexistent")
    
    def test_clear_cache_all_engines(self):
        """Test clearing all engines from cache."""
        # Add some mock engines to cache
        EngineFactory._instances["engine1"] = MagicMock()
        EngineFactory._instances["engine2"] = MagicMock()
        
        # Clear all
        EngineFactory.clear_cache()
        
        # Verify all engines were removed
        assert len(EngineFactory._instances) == 0
    
    def test_registry_is_class_variable(self):
        """Test that registry is shared across instances."""
        # Test that modifications to registry are shared
        EngineFactory.register_engine("shared_engine", MagicMock())
        
        # Create new instance (though not needed since methods are classmethods)
        assert "shared_engine" in EngineFactory._registry
    
    def test_cache_is_class_variable(self):
        """Test that cache is shared across instances."""
        # Test that cache is shared
        mock_engine = MagicMock()
        EngineFactory._instances["shared_cache"] = mock_engine
        
        assert "shared_cache" in EngineFactory._instances
        assert EngineFactory._instances["shared_cache"] == mock_engine
    
    @pytest.mark.asyncio
    async def test_get_engine_with_database_session(self):
        """Test that database session is passed to engine constructor."""
        mock_engine_class = MagicMock()
        mock_engine_instance = MagicMock()
        mock_engine_class.return_value = mock_engine_instance
        mock_engine_instance.initialize = AsyncMock(return_value=True)
        
        EngineFactory.register_engine("db_engine", mock_engine_class)
        
        mock_db = MagicMock()
        
        await EngineFactory.get_engine("db_engine", mock_db)
        
        # Verify database session was passed to constructor
        mock_engine_class.assert_called_once_with(mock_db)
    
    @pytest.mark.asyncio
    async def test_get_engine_exception_handling(self):
        """Test engine creation when constructor raises exception."""
        # Create mock engine class that raises exception
        mock_engine_class = MagicMock(side_effect=Exception("Constructor error"))
        
        EngineFactory.register_engine("error_engine", mock_engine_class)
        
        # Should handle exception gracefully
        with pytest.raises(Exception, match="Constructor error"):
            await EngineFactory.get_engine("error_engine")
    
    def test_default_registry_contains_crewai(self):
        """Test that default registry contains CrewAI engine."""
        # Reset to verify default state
        from src.engines.crewai.crewai_engine_service import CrewAIEngineService
        EngineFactory._registry = {"crewai": CrewAIEngineService}
        
        assert "crewai" in EngineFactory._registry
        assert EngineFactory._registry["crewai"] == CrewAIEngineService