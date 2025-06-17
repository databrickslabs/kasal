import pytest
import asyncio
from unittest.mock import MagicMock, patch, AsyncMock
from sqlalchemy.ext.asyncio import AsyncSession


class TestEngineFactoryModuleCoverage:
    """Test suite to achieve 100% coverage for EngineFactory module."""
    
    def setup_method(self):
        """Setup before each test method."""
        # Mock all problematic imports at the module level
        self.mock_patches = {}
        
        # Create mock modules
        mock_crewai_service = MagicMock()
        mock_base_service = MagicMock()
        
        # Setup patches for all problematic imports
        self.mock_patches['crewai_service'] = patch('src.engines.crewai.crewai_engine_service.CrewAIEngineService', mock_crewai_service)
        self.mock_patches['base_service'] = patch('src.engines.base.base_engine_service.BaseEngineService', mock_base_service)
        
        # Start all patches
        for patch_obj in self.mock_patches.values():
            patch_obj.start()
            
    def teardown_method(self):
        """Cleanup after each test method."""
        # Stop all patches
        for patch_obj in self.mock_patches.values():
            patch_obj.stop()
    
    @patch('src.engines.engine_factory.logger')
    def test_register_engine_logging(self, mock_logger):
        """Test engine registration with logging."""
        from src.engines.engine_factory import EngineFactory
        
        mock_engine_class = MagicMock()
        EngineFactory.register_engine("test_engine", mock_engine_class)
        
        mock_logger.info.assert_called_once_with("Registered engine type: test_engine")
        assert "test_engine" in EngineFactory._registry
    
    @pytest.mark.asyncio
    @patch('src.engines.engine_factory.logger')
    async def test_get_engine_unknown_type_logging(self, mock_logger):
        """Test unknown engine type error logging."""
        from src.engines.engine_factory import EngineFactory
        
        result = await EngineFactory.get_engine("unknown_engine")
        
        assert result is None
        mock_logger.error.assert_called_once_with("Unknown engine type: unknown_engine")
    
    @pytest.mark.asyncio
    @patch('src.engines.engine_factory.logger')
    async def test_get_engine_initialization_failure_logging(self, mock_logger):
        """Test initialization failure logging."""
        from src.engines.engine_factory import EngineFactory
        
        # Create mock engine that fails initialization
        mock_engine_class = MagicMock()
        mock_engine_instance = MagicMock()
        mock_engine_class.return_value = mock_engine_instance
        mock_engine_instance.initialize = AsyncMock(return_value=False)
        
        EngineFactory.register_engine("failing_engine", mock_engine_class)
        
        result = await EngineFactory.get_engine("failing_engine")
        
        assert result is None
        mock_logger.error.assert_called_once_with("Failed to initialize engine: failing_engine")
    
    @patch('src.engines.engine_factory.logger')
    def test_clear_cache_specific_engine_logging(self, mock_logger):
        """Test clearing specific engine cache with logging."""
        from src.engines.engine_factory import EngineFactory
        
        # Add an item to the cache
        mock_engine = MagicMock()
        EngineFactory._instances["test_engine"] = mock_engine
        
        EngineFactory.clear_cache("test_engine")
        
        assert "test_engine" not in EngineFactory._instances
        mock_logger.info.assert_called_once_with("Cleared cached instance of engine: test_engine")
    
    @patch('src.engines.engine_factory.logger')
    def test_clear_cache_all_engines_logging(self, mock_logger):
        """Test clearing all engines cache with logging."""
        from src.engines.engine_factory import EngineFactory
        
        # Add items to the cache
        EngineFactory._instances["engine1"] = MagicMock()
        EngineFactory._instances["engine2"] = MagicMock()
        
        EngineFactory.clear_cache()
        
        assert len(EngineFactory._instances) == 0
        mock_logger.info.assert_called_once_with("Cleared all cached engine instances")
    
    @patch('src.engines.engine_factory.logger')
    def test_clear_cache_nonexistent_engine_no_logging(self, mock_logger):
        """Test clearing nonexistent engine doesn't log."""
        from src.engines.engine_factory import EngineFactory
        
        EngineFactory.clear_cache("nonexistent_engine")
        
        # Should not log anything for nonexistent engine
        mock_logger.info.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_get_engine_cached_instance_path(self):
        """Test getting cached engine instance path."""
        from src.engines.engine_factory import EngineFactory
        
        # Pre-populate cache
        mock_engine = MagicMock()
        EngineFactory._instances["cached_engine"] = mock_engine
        
        result = await EngineFactory.get_engine("cached_engine")
        
        assert result == mock_engine
    
    @pytest.mark.asyncio
    async def test_get_engine_registry_lookup_path(self):
        """Test engine registry lookup path."""
        from src.engines.engine_factory import EngineFactory
        
        # Clear cache to force registry lookup
        EngineFactory._instances.clear()
        
        # Create mock engine
        mock_engine_class = MagicMock()
        mock_engine_instance = MagicMock()
        mock_engine_class.return_value = mock_engine_instance
        mock_engine_instance.initialize = AsyncMock(return_value=True)
        
        EngineFactory._registry["test_engine"] = mock_engine_class
        
        result = await EngineFactory.get_engine("test_engine")
        
        assert result == mock_engine_instance
        assert "test_engine" in EngineFactory._instances
        assert EngineFactory._instances["test_engine"] == mock_engine_instance
    
    @pytest.mark.asyncio
    async def test_get_engine_init_params_none_handling(self):
        """Test None init_params handling."""
        from src.engines.engine_factory import EngineFactory
        
        mock_engine_class = MagicMock()
        mock_engine_instance = MagicMock()
        mock_engine_class.return_value = mock_engine_instance
        mock_engine_instance.initialize = AsyncMock(return_value=True)
        
        EngineFactory._registry["test_engine"] = mock_engine_class
        EngineFactory._instances.clear()
        
        result = await EngineFactory.get_engine("test_engine", init_params=None)
        
        # Should call initialize with empty dict when init_params is None
        mock_engine_instance.initialize.assert_called_once_with()
        assert result == mock_engine_instance
    
    def test_get_available_engines_returns_list(self):
        """Test get_available_engines returns registry keys."""
        from src.engines.engine_factory import EngineFactory
        
        # Set up known registry state
        EngineFactory._registry = {"engine1": MagicMock(), "engine2": MagicMock()}
        
        available = EngineFactory.get_available_engines()
        
        assert isinstance(available, list)
        assert set(available) == {"engine1", "engine2"}
    
    def test_class_variables_shared_state(self):
        """Test that class variables maintain shared state."""
        from src.engines.engine_factory import EngineFactory
        
        # Test registry sharing
        original_registry = EngineFactory._registry.copy()
        EngineFactory.register_engine("shared_test", MagicMock())
        assert "shared_test" in EngineFactory._registry
        
        # Test cache sharing
        original_cache = EngineFactory._instances.copy()
        test_engine = MagicMock()
        EngineFactory._instances["shared_cache"] = test_engine
        assert "shared_cache" in EngineFactory._instances
        assert EngineFactory._instances["shared_cache"] == test_engine
        
        # Cleanup
        EngineFactory._registry = original_registry
        EngineFactory._instances = original_cache
    
    @pytest.mark.asyncio
    async def test_get_engine_constructor_exception_handling(self):
        """Test engine constructor exception handling."""
        from src.engines.engine_factory import EngineFactory
        
        # Create mock engine class that raises exception in constructor
        mock_engine_class = MagicMock(side_effect=RuntimeError("Constructor failed"))
        EngineFactory._registry["error_engine"] = mock_engine_class
        EngineFactory._instances.clear()
        
        # Should propagate the exception
        with pytest.raises(RuntimeError, match="Constructor failed"):    
            await EngineFactory.get_engine("error_engine")
    
    @pytest.mark.asyncio  
    async def test_get_engine_initialize_exception_handling(self):
        """Test engine initialize method exception handling."""
        from src.engines.engine_factory import EngineFactory
        
        mock_engine_class = MagicMock()
        mock_engine_instance = MagicMock()
        mock_engine_class.return_value = mock_engine_instance
        mock_engine_instance.initialize = AsyncMock(side_effect=RuntimeError("Initialize failed"))
        
        EngineFactory._registry["init_error_engine"] = mock_engine_class
        EngineFactory._instances.clear()
        
        # Should propagate the initialization exception
        with pytest.raises(RuntimeError, match="Initialize failed"):
            await EngineFactory.get_engine("init_error_engine")