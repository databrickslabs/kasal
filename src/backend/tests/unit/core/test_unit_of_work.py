"""
Unit tests for unit_of_work module.

Tests the functionality of the Unit of Work pattern implementation
including async and sync contexts, repository management, and transactions.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch, Mock
from typing import Optional

from src.core.unit_of_work import UnitOfWork, SyncUnitOfWork


class TestUnitOfWork:
    """Test cases for async UnitOfWork class."""
    
    def test_init(self):
        """Test UnitOfWork initialization."""
        # Act
        uow = UnitOfWork()
        
        # Assert
        assert uow._session is None
        assert uow.tool_repository is None
        assert uow.api_key_repository is None
        assert uow.model_config_repository is None
        assert uow.template_repository is None
        assert uow.task_tracking_repository is None
        assert uow.schema_repository is None
        assert uow.databricks_config_repository is None
        assert uow.mcp_server_repository is None
        assert uow.mcp_settings_repository is None
        assert uow.engine_config_repository is None
    
    @pytest.mark.asyncio
    async def test_aenter_success(self):
        """Test successful async context entry."""
        # Arrange
        mock_session = AsyncMock()
        mock_session_context = AsyncMock()
        mock_session_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session_context.__aexit__ = AsyncMock(return_value=None)
        
        mock_session_factory = MagicMock(return_value=mock_session_context)
        
        with patch('src.db.session.async_session_factory', mock_session_factory):
            with patch('src.repositories.tool_repository.ToolRepository') as mock_tool_repo:
                with patch('src.repositories.api_key_repository.ApiKeyRepository') as mock_api_key_repo:
                    with patch('src.repositories.model_config_repository.ModelConfigRepository') as mock_model_config_repo:
                        with patch('src.repositories.template_repository.TemplateRepository') as mock_template_repo:
                            with patch('src.repositories.task_tracking_repository.TaskTrackingRepository') as mock_task_tracking_repo:
                                with patch('src.repositories.schema_repository.SchemaRepository') as mock_schema_repo:
                                    with patch('src.repositories.databricks_config_repository.DatabricksConfigRepository') as mock_databricks_config_repo:
                                        with patch('src.repositories.mcp_repository.MCPServerRepository') as mock_mcp_server_repo:
                                            with patch('src.repositories.mcp_repository.MCPSettingsRepository') as mock_mcp_settings_repo:
                                                with patch('src.repositories.engine_config_repository.EngineConfigRepository') as mock_engine_config_repo:
                                                    uow = UnitOfWork()
                                                    
                                                    # Act
                                                    result = await uow.__aenter__()
                                                    
                                                    # Assert
                                                    assert result == uow
                                                    assert uow._session == mock_session_context
                                                    # Verify repositories were created (simplified check)
                                                    assert uow.tool_repository is not None
                                                    assert uow.api_key_repository is not None
    
    @pytest.mark.asyncio
    async def test_aexit_success_commit(self):
        """Test successful async context exit with commit."""
        # Arrange
        mock_session = AsyncMock()
        uow = UnitOfWork()
        uow._session = mock_session
        
        # Act
        await uow.__aexit__(None, None, None)
        
        # Assert
        mock_session.commit.assert_called_once()
        mock_session.close.assert_called_once()
        mock_session.rollback.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_aexit_with_exception_rollback(self):
        """Test async context exit with exception causing rollback."""
        # Arrange
        mock_session = AsyncMock()
        uow = UnitOfWork()
        uow._session = mock_session
        
        # Act
        await uow.__aexit__(ValueError, ValueError("Test error"), None)
        
        # Assert
        mock_session.rollback.assert_called_once()
        mock_session.close.assert_called_once()
        mock_session.commit.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_aexit_commit_error_rollback(self):
        """Test async context exit when commit fails."""
        # Arrange
        mock_session = AsyncMock()
        mock_session.commit.side_effect = Exception("Commit failed")
        uow = UnitOfWork()
        uow._session = mock_session
        
        # Act & Assert
        with pytest.raises(Exception, match="Commit failed"):
            await uow.__aexit__(None, None, None)
        
        mock_session.commit.assert_called_once()
        mock_session.rollback.assert_called_once()
        mock_session.close.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_aexit_cleanup_repositories(self):
        """Test that repositories are cleaned up on exit."""
        # Arrange
        mock_session = AsyncMock()
        uow = UnitOfWork()
        uow._session = mock_session
        uow.tool_repository = Mock()
        uow.api_key_repository = Mock()
        uow.model_config_repository = Mock()
        uow.template_repository = Mock()
        uow.task_tracking_repository = Mock()
        uow.schema_repository = Mock()
        uow.databricks_config_repository = Mock()
        uow.mcp_server_repository = Mock()
        uow.mcp_settings_repository = Mock()
        uow.engine_config_repository = Mock()
        
        # Act
        await uow.__aexit__(None, None, None)
        
        # Assert
        assert uow.tool_repository is None
        assert uow.api_key_repository is None
        assert uow.model_config_repository is None
        assert uow.template_repository is None
        assert uow.task_tracking_repository is None
        assert uow.schema_repository is None
        assert uow.databricks_config_repository is None
        assert uow.mcp_server_repository is None
        assert uow.mcp_settings_repository is None
        assert uow.engine_config_repository is None
    
    @pytest.mark.asyncio
    async def test_commit_success(self):
        """Test successful explicit commit."""
        # Arrange
        mock_session = AsyncMock()
        uow = UnitOfWork()
        uow._session = mock_session
        
        # Act
        await uow.commit()
        
        # Assert
        mock_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_commit_error(self):
        """Test commit error handling."""
        # Arrange
        mock_session = AsyncMock()
        mock_session.commit.side_effect = Exception("Commit error")
        uow = UnitOfWork()
        uow._session = mock_session
        
        # Act & Assert
        with pytest.raises(Exception, match="Commit error"):
            await uow.commit()
    
    @pytest.mark.asyncio
    async def test_async_context_manager(self):
        """Test UnitOfWork as async context manager."""
        # Arrange
        mock_session = AsyncMock()
        mock_session_context = AsyncMock()
        mock_session_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session_context.__aexit__ = AsyncMock(return_value=None)
        
        mock_session_factory = MagicMock(return_value=mock_session_context)
        
        with patch('src.db.session.async_session_factory', mock_session_factory):
            with patch('src.repositories.tool_repository.ToolRepository'):
                with patch('src.repositories.api_key_repository.ApiKeyRepository'):
                    with patch('src.repositories.model_config_repository.ModelConfigRepository'):
                        with patch('src.repositories.template_repository.TemplateRepository'):
                            with patch('src.repositories.task_tracking_repository.TaskTrackingRepository'):
                                with patch('src.repositories.schema_repository.SchemaRepository'):
                                    with patch('src.repositories.databricks_config_repository.DatabricksConfigRepository'):
                                        with patch('src.repositories.mcp_repository.MCPServerRepository'):
                                            with patch('src.repositories.mcp_repository.MCPSettingsRepository'):
                                                with patch('src.repositories.engine_config_repository.EngineConfigRepository'):
                                                    # Act
                                                    async with UnitOfWork() as uow:
                                                        assert uow.tool_repository is not None
                                                        assert uow._session == mock_session_context
                                                    
                                                    # Assert - the context manager worked
                                                    # (detailed assertions are covered in other tests)


class TestSyncUnitOfWork:
    """Test cases for synchronous SyncUnitOfWork class."""
    
    def test_singleton_pattern(self):
        """Test that SyncUnitOfWork follows singleton pattern."""
        # Act
        uow1 = SyncUnitOfWork.get_instance()
        uow2 = SyncUnitOfWork.get_instance()
        
        # Assert
        assert uow1 is uow2
        assert isinstance(uow1, SyncUnitOfWork)
    
    def test_init(self):
        """Test SyncUnitOfWork initialization."""
        # Act
        uow = SyncUnitOfWork()
        
        # Assert
        assert uow._session is None
        assert uow.tool_repository is None
        assert uow.api_key_repository is None
        assert uow.model_config_repository is None
        assert uow.template_repository is None
        assert uow.task_tracking_repository is None
        assert uow.schema_repository is None
        assert uow.databricks_config_repository is None
        assert uow.mcp_server_repository is None
        assert uow.mcp_settings_repository is None
        assert uow.engine_config_repository is None
        assert uow._initialized is False
    
    def test_initialize_success(self):
        """Test successful initialization."""
        # Arrange
        mock_session = Mock()
        
        with patch('src.db.session.SessionLocal', return_value=mock_session):
            with patch('src.repositories.tool_repository.ToolRepository') as mock_tool_repo:
                with patch('src.repositories.api_key_repository.ApiKeyRepository') as mock_api_key_repo:
                    with patch('src.repositories.model_config_repository.ModelConfigRepository') as mock_model_config_repo:
                        with patch('src.repositories.template_repository.TemplateRepository') as mock_template_repo:
                            with patch('src.repositories.task_tracking_repository.TaskTrackingRepository') as mock_task_tracking_repo:
                                with patch('src.repositories.schema_repository.SchemaRepository') as mock_schema_repo:
                                    with patch('src.repositories.databricks_config_repository.DatabricksConfigRepository') as mock_databricks_config_repo:
                                        with patch('src.repositories.mcp_repository.MCPServerRepository') as mock_mcp_server_repo:
                                            with patch('src.repositories.mcp_repository.MCPSettingsRepository') as mock_mcp_settings_repo:
                                                with patch('src.repositories.engine_config_repository.EngineConfigRepository') as mock_engine_config_repo:
                                                    uow = SyncUnitOfWork()
                                                    
                                                    # Act
                                                    uow.initialize()
                                                    
                                                    # Assert
                                                    assert uow._initialized is True
                                                    assert uow._session == mock_session
                                                    # Verify repositories were created (simplified check)
                                                    assert uow.tool_repository is not None
                                                    assert uow.api_key_repository is not None
    
    def test_initialize_already_initialized(self):
        """Test that multiple initialization calls don't reinitialize."""
        # Arrange
        mock_session = Mock()
        
        with patch('src.db.session.SessionLocal', return_value=mock_session) as mock_session_local:
            with patch('src.repositories.tool_repository.ToolRepository'):
                with patch('src.repositories.api_key_repository.ApiKeyRepository'):
                    with patch('src.repositories.model_config_repository.ModelConfigRepository'):
                        with patch('src.repositories.template_repository.TemplateRepository'):
                            with patch('src.repositories.task_tracking_repository.TaskTrackingRepository'):
                                with patch('src.repositories.schema_repository.SchemaRepository'):
                                    with patch('src.repositories.databricks_config_repository.DatabricksConfigRepository'):
                                        with patch('src.repositories.mcp_repository.MCPServerRepository'):
                                            with patch('src.repositories.mcp_repository.MCPSettingsRepository'):
                                                with patch('src.repositories.engine_config_repository.EngineConfigRepository'):
                                                    uow = SyncUnitOfWork()
                                                    uow.initialize()
                                                    
                                                    # Act
                                                    uow.initialize()  # Second call
                                                    
                                                    # Assert
                                                    mock_session_local.assert_called_once()  # Only called once
    
    def test_commit_success(self):
        """Test successful commit."""
        # Arrange
        mock_session = Mock()
        uow = SyncUnitOfWork()
        uow._session = mock_session
        uow._initialized = True
        
        # Act
        uow.commit()
        
        # Assert
        mock_session.commit.assert_called_once()
    
    def test_commit_not_initialized(self):
        """Test commit when not initialized."""
        # Arrange
        uow = SyncUnitOfWork()
        
        # Act & Assert
        with pytest.raises(RuntimeError, match="SyncUnitOfWork not initialized"):
            uow.commit()
    
    def test_commit_error_rollback(self):
        """Test commit error handling with rollback."""
        # Arrange
        mock_session = Mock()
        mock_session.commit.side_effect = Exception("Commit failed")
        uow = SyncUnitOfWork()
        uow._session = mock_session
        uow._initialized = True
        
        # Act & Assert
        with pytest.raises(Exception, match="Commit failed"):
            uow.commit()
        
        mock_session.commit.assert_called_once()
        mock_session.rollback.assert_called_once()
    
    def test_rollback_success(self):
        """Test successful rollback."""
        # Arrange
        mock_session = Mock()
        uow = SyncUnitOfWork()
        uow._session = mock_session
        uow._initialized = True
        
        # Act
        uow.rollback()
        
        # Assert
        mock_session.rollback.assert_called_once()
    
    def test_rollback_not_initialized(self):
        """Test rollback when not initialized."""
        # Arrange
        uow = SyncUnitOfWork()
        
        # Act & Assert
        with pytest.raises(RuntimeError, match="SyncUnitOfWork not initialized"):
            uow.rollback()
    
    def test_rollback_error(self):
        """Test rollback error handling."""
        # Arrange
        mock_session = Mock()
        mock_session.rollback.side_effect = Exception("Rollback failed")
        uow = SyncUnitOfWork()
        uow._session = mock_session
        uow._initialized = True
        
        # Act & Assert
        with pytest.raises(Exception, match="Rollback failed"):
            uow.rollback()
    
    def test_cleanup_success(self):
        """Test successful cleanup."""
        # Arrange
        mock_session = Mock()
        uow = SyncUnitOfWork()
        uow._session = mock_session
        uow._initialized = True
        
        # Act
        uow.cleanup()
        
        # Assert
        mock_session.close.assert_called_once()
        assert uow._session is None
        assert uow._initialized is False
    
    def test_cleanup_not_initialized(self):
        """Test cleanup when not initialized."""
        # Arrange
        uow = SyncUnitOfWork()
        
        # Act
        uow.cleanup()
        
        # Assert - should not raise error
        assert uow._session is None
        assert uow._initialized is False
    
    def test_cleanup_no_session(self):
        """Test cleanup when session is None."""
        # Arrange
        uow = SyncUnitOfWork()
        uow._initialized = True
        uow._session = None
        
        # Act
        uow.cleanup()
        
        # Assert - should not raise error, but initialized should remain True since no session to cleanup
        assert uow._session is None
        assert uow._initialized is True  # No cleanup needed, so remains initialized
    
    def test_del_calls_cleanup(self):
        """Test that __del__ calls cleanup."""
        # Arrange
        mock_session = Mock()
        uow = SyncUnitOfWork()
        uow._session = mock_session
        uow._initialized = True
        
        # Act
        uow.__del__()
        
        # Assert
        mock_session.close.assert_called_once()
        assert uow._session is None
        assert uow._initialized is False
    
    def test_singleton_reset_on_cleanup(self):
        """Test that singleton behavior works correctly after cleanup."""
        # Arrange
        uow1 = SyncUnitOfWork.get_instance()
        original_instance = SyncUnitOfWork._instance
        
        # Act - cleanup but don't reset singleton
        uow1.cleanup()
        uow2 = SyncUnitOfWork.get_instance()
        
        # Assert - should be same instance
        assert uow1 is uow2
        assert uow2 is original_instance
    
    def test_multiple_repositories_initialization(self):
        """Test that all repositories are properly initialized."""
        # Arrange
        mock_session = Mock()
        
        with patch('src.db.session.SessionLocal', return_value=mock_session):
            with patch('src.repositories.tool_repository.ToolRepository') as mock_tool_repo:
                with patch('src.repositories.api_key_repository.ApiKeyRepository') as mock_api_key_repo:
                    with patch('src.repositories.model_config_repository.ModelConfigRepository') as mock_model_config_repo:
                        with patch('src.repositories.template_repository.TemplateRepository') as mock_template_repo:
                            with patch('src.repositories.task_tracking_repository.TaskTrackingRepository') as mock_task_tracking_repo:
                                with patch('src.repositories.schema_repository.SchemaRepository') as mock_schema_repo:
                                    with patch('src.repositories.databricks_config_repository.DatabricksConfigRepository') as mock_databricks_config_repo:
                                        with patch('src.repositories.mcp_repository.MCPServerRepository') as mock_mcp_server_repo:
                                            with patch('src.repositories.mcp_repository.MCPSettingsRepository') as mock_mcp_settings_repo:
                                                with patch('src.repositories.engine_config_repository.EngineConfigRepository') as mock_engine_config_repo:
                                                    uow = SyncUnitOfWork()
                                                    
                                                    # Act
                                                    uow.initialize()
                                                    
                                                    # Assert
                                                    assert uow.tool_repository is not None
                                                    assert uow.api_key_repository is not None
                                                    assert uow.model_config_repository is not None
                                                    assert uow.template_repository is not None
                                                    assert uow.task_tracking_repository is not None
                                                    assert uow.schema_repository is not None
                                                    assert uow.databricks_config_repository is not None
                                                    assert uow.mcp_server_repository is not None
                                                    assert uow.mcp_settings_repository is not None
                                                    assert uow.engine_config_repository is not None