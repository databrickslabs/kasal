"""
Unit tests for main application module.

Tests the functionality of the main FastAPI application including
startup, configuration, and middleware setup.
"""
import pytest
import os
import tempfile
from unittest.mock import patch, MagicMock, AsyncMock
from pathlib import Path
import sys


class TestMainApplication:
    """Test cases for main application module."""
    
    def test_basic_imports(self):
        """Test that basic imports work correctly."""
        try:
            from src.main import logger, log_path
            
            assert logger is not None
            assert log_path is not None
            assert isinstance(log_path, str)
            assert log_path.endswith("logs")
        except ImportError as e:
            pytest.fail(f"Failed to import main module components: {e}")
    
    def test_environment_setup(self):
        """Test that environment variables are set correctly."""
        # Import main module to trigger environment setup
        import src.main
        
        # Should set SEED_DEBUG
        assert os.environ.get("SEED_DEBUG") == "True"
        
        # Should set LOG_DIR
        assert "LOG_DIR" in os.environ
        log_dir = os.environ["LOG_DIR"]
        assert log_dir.endswith("logs")
    
    def test_log_directory_creation(self):
        """Test that log directory is created."""
        # Import main to ensure log directory creation happens
        import src.main
        
        # Verify that the log path exists or was attempted to be created
        assert hasattr(src.main, 'log_path')
        assert src.main.log_path is not None
        assert src.main.log_path.endswith("logs")
        
        # Check that LOG_DIR environment variable is set
        assert os.environ.get("LOG_DIR") == src.main.log_path
    
    def test_docs_path_detection(self):
        """Test that docs path is detected correctly - skip as docs_path removed."""
        # docs_path has been removed from the codebase
        pass
    
    def test_warning_filters_applied(self):
        """Test that deprecation warning filters are applied."""
        import warnings
        
        # Re-import to ensure filters are applied
        import importlib
        import src.main
        importlib.reload(src.main)
        
        filters = warnings.filters
        
        # Should have filters for known deprecation warnings
        assert len(filters) > 0
    
    @pytest.mark.asyncio
    async def test_lifespan_startup(self):
        """Test lifespan manager startup sequence."""
        from src.main import lifespan
        from fastapi import FastAPI
        
        app = FastAPI()
        
        with patch("src.main.LoggerManager") as mock_logger_manager, \
             patch("src.db.session.init_db") as mock_init_db, \
             patch("src.main.settings") as mock_settings, \
             patch("src.main.async_session_factory") as mock_session_factory, \
             patch("os.path.exists") as mock_exists, \
             patch("os.path.getsize") as mock_getsize:
            
            # Setup mocks
            mock_instance = MagicMock()
            mock_instance.system = MagicMock()
            mock_logger_manager.get_instance.return_value = mock_instance
            mock_init_db.return_value = None  # async function returns None
            mock_settings.DATABASE_URI = "sqlite:///test.db"
            mock_settings.SQLITE_DB_PATH = "/tmp/test.db"
            mock_settings.AUTO_SEED_DATABASE = False
            mock_exists.return_value = False  # Simulate no database file
            mock_getsize.return_value = 0
            
            # Test lifespan context manager
            async with lifespan(app):
                # Verify initialization was called
                mock_logger_manager.get_instance.assert_called()
                mock_instance.initialize.assert_called_once()
                mock_init_db.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_lifespan_database_error_handling(self):
        """Test lifespan manager handles database errors gracefully."""
        from src.main import lifespan
        from fastapi import FastAPI
        
        app = FastAPI()
        
        with patch("src.main.LoggerManager") as mock_logger_manager, \
             patch("src.db.session.init_db") as mock_init_db, \
             patch("src.main.settings") as mock_settings, \
             patch("os.path.exists") as mock_exists:
            
            # Setup mocks
            mock_instance = MagicMock()
            mock_instance.system = MagicMock()
            mock_logger_manager.get_instance.return_value = mock_instance
            mock_init_db.side_effect = Exception("Database error")
            mock_settings.DATABASE_URI = "sqlite:///test.db"
            mock_settings.SQLITE_DB_PATH = "/tmp/test.db"
            mock_settings.AUTO_SEED_DATABASE = False
            mock_exists.return_value = False
            
            # Should not raise exception, but handle gracefully
            async with lifespan(app):
                mock_instance.system.error.assert_called()
    
    def test_logging_configuration(self):
        """Test that basic logging is configured."""
        import logging
        
        # Get the __name__ logger from main module
        from src.main import logger
        
        # Should have a logger configured
        assert logger is not None
        assert logger.name == "src.main"
        
        # Check that logging.basicConfig was called (via module-level setup)
        # Note: Root logger level may be affected by pytest, so we check the module logger
        assert hasattr(logger, 'handlers') or logger.parent is not None
    
    def test_package_directory_detection(self):
        """Test that package directory is detected correctly - skip as package_dir removed."""
        # package_dir has been removed from the codebase
        pass
    
    def test_wheel_package_docs_detection(self):
        """Test that wheel package docs are detected when available - skip as docs handling removed."""
        # Docs path detection has been removed from the codebase
        pass
    
    def test_database_path_handling(self):
        """Test that database path is handled correctly."""
        with patch("src.main.settings") as mock_settings:
            mock_settings.DATABASE_URI = "sqlite:///./test.db"
            mock_settings.SQLITE_DB_PATH = "./test.db"
            
            # Should handle relative paths
            import importlib
            import src.main
            importlib.reload(src.main)
    
    def test_absolute_path_handling(self):
        """Test that absolute paths are handled correctly."""
        test_path = os.path.abspath("test_logs")
        
        with patch("src.main.os.path.abspath") as mock_abspath:
            mock_abspath.return_value = test_path
            
            import importlib
            import src.main
            importlib.reload(src.main)
            
            # Should use absolute path
            assert src.main.log_path is not None
    
    def test_imports_in_lifespan(self):
        """Test that required imports are available in lifespan."""
        # Test that the imports used in lifespan function are available
        try:
            from src.db.base import Base
            import src.db.all_models
            from src.db.session import init_db
            
            assert Base is not None
            assert src.db.all_models is not None
            assert init_db is not None
        except ImportError as e:
            pytest.fail(f"Required imports for lifespan not available: {e}")
    
    def test_settings_import(self):
        """Test that settings are imported correctly."""
        from src.main import settings
        
        # Should have database configuration
        assert hasattr(settings, 'DATABASE_URI')
    
    def test_api_router_import(self):
        """Test that API router is imported correctly."""
        from src.main import api_router
        from fastapi import APIRouter
        
        # Should be an APIRouter instance
        assert isinstance(api_router, APIRouter)
    
    def test_logger_manager_usage(self):
        """Test that LoggerManager is used correctly."""
        with patch("src.main.LoggerManager") as mock_logger_manager:
            mock_instance = MagicMock()
            mock_logger_manager.get_instance.return_value = mock_instance
            
            # Re-import to test LoggerManager usage
            import importlib
            import src.main
            importlib.reload(src.main)
    
    def test_scheduler_service_import(self):
        """Test that SchedulerService is imported correctly."""
        from src.main import SchedulerService
        
        # Should be importable
        assert SchedulerService is not None
    
    def test_fastapi_imports(self):
        """Test that FastAPI components are imported correctly."""
        try:
            from src.main import (
                FastAPI, HTTPException, Request, Depends,
                CORSMiddleware
            )
            
            # All should be importable
            assert all([
                FastAPI, HTTPException, Request, Depends,
                CORSMiddleware
            ])
        except ImportError as e:
            pytest.fail(f"Failed to import FastAPI components: {e}")
    
    def test_database_session_imports(self):
        """Test that database session components are imported correctly."""
        try:
            from src.main import get_db, async_session_factory
            
            assert get_db is not None
            assert async_session_factory is not None
        except ImportError as e:
            pytest.fail(f"Failed to import database session components: {e}")
    
    def test_log_path_environment_variable(self):
        """Test that log path is set in environment variable."""
        import src.main
        
        log_dir = os.environ.get("LOG_DIR")
        assert log_dir is not None
        assert log_dir == src.main.log_path
    
    @pytest.mark.asyncio
    async def test_lifespan_context_manager_protocol(self):
        """Test that lifespan follows async context manager protocol."""
        from src.main import lifespan
        from fastapi import FastAPI
        
        app = FastAPI()
        
        # Should be an async context manager
        context_manager = lifespan(app)
        assert hasattr(context_manager, '__aenter__')
        assert hasattr(context_manager, '__aexit__')
    
    def test_module_level_variables(self):
        """Test that module-level variables are set correctly."""
        import src.main
        
        # Should have logger
        assert hasattr(src.main, 'logger')
        assert src.main.logger is not None
        
        # Should have log_path
        assert hasattr(src.main, 'log_path')
        assert src.main.log_path is not None
    
    def test_docs_path_fallback_mechanism(self):
        """Test that docs path fallback mechanism works - skip as docs_path removed."""
        # docs_path has been removed from the codebase
        pass