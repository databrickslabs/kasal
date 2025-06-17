"""
Unit tests for DatabricksConfigRepository.

Tests the functionality of Databricks configuration repository including
active configuration management, deactivation operations, and configuration creation.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update

from src.repositories.databricks_config_repository import DatabricksConfigRepository
from src.models.databricks_config import DatabricksConfig


# Mock Databricks config model
class MockDatabricksConfig:
    def __init__(self, id=1, workspace_url="https://test.databricks.com", 
                 token="test_token", is_active=True, created_at=None, updated_at=None, **kwargs):
        self.id = id
        self.workspace_url = workspace_url
        self.token = token
        self.is_active = is_active
        self.created_at = created_at or datetime.now(timezone.utc)
        self.updated_at = updated_at or datetime.now(timezone.utc)
        for key, value in kwargs.items():
            setattr(self, key, value)


@pytest.fixture
def mock_async_session():
    """Create a mock async database session."""
    session = AsyncMock(spec=AsyncSession)
    session.execute = AsyncMock()
    session.add = MagicMock()  # add() is synchronous in SQLAlchemy
    session.flush = AsyncMock()
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    return session


@pytest.fixture
def databricks_config_repository(mock_async_session):
    """Create a Databricks config repository with async session."""
    return DatabricksConfigRepository(session=mock_async_session)


@pytest.fixture
def sample_databricks_configs():
    """Create sample Databricks configurations for testing."""
    return [
        MockDatabricksConfig(id=1, workspace_url="https://active.databricks.com", is_active=True),
        MockDatabricksConfig(id=2, workspace_url="https://inactive1.databricks.com", is_active=False),
        MockDatabricksConfig(id=3, workspace_url="https://inactive2.databricks.com", is_active=False)
    ]


@pytest.fixture
def sample_config_data():
    """Create sample config data for creation."""
    return {
        "workspace_url": "https://new.databricks.com",
        "token": "new_test_token",
        "is_active": True
    }


class TestDatabricksConfigRepositoryInit:
    """Test cases for DatabricksConfigRepository initialization."""
    
    def test_init_success(self, mock_async_session):
        """Test successful initialization."""
        repository = DatabricksConfigRepository(session=mock_async_session)
        
        assert repository.model == DatabricksConfig
        assert repository.session == mock_async_session


class TestDatabricksConfigRepositoryGetActiveConfig:
    """Test cases for get_active_config method."""
    
    @pytest.mark.asyncio
    async def test_get_active_config_success(self, databricks_config_repository, mock_async_session, sample_databricks_configs):
        """Test successful retrieval of active configuration."""
        active_config = sample_databricks_configs[0]  # is_active=True
        
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = active_config
        mock_async_session.execute.return_value = mock_result
        
        result = await databricks_config_repository.get_active_config()
        
        assert result == active_config
        mock_async_session.execute.assert_called_once()
        
        # Verify the query was constructed correctly
        call_args = mock_async_session.execute.call_args[0][0]
        assert isinstance(call_args, type(select(DatabricksConfig)))
    
    @pytest.mark.asyncio
    async def test_get_active_config_none_found(self, databricks_config_repository, mock_async_session):
        """Test get active config when no active configuration exists."""
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_async_session.execute.return_value = mock_result
        
        result = await databricks_config_repository.get_active_config()
        
        assert result is None
        mock_async_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_active_config_database_error(self, databricks_config_repository, mock_async_session):
        """Test get active config with database error."""
        mock_async_session.execute.side_effect = Exception("Database error")
        
        with pytest.raises(Exception, match="Database error"):
            await databricks_config_repository.get_active_config()


class TestDatabricksConfigRepositoryDeactivateAll:
    """Test cases for deactivate_all method."""
    
    @pytest.mark.asyncio
    async def test_deactivate_all_success(self, databricks_config_repository, mock_async_session):
        """Test successful deactivation of all configurations."""
        mock_result = MagicMock()
        mock_async_session.execute.return_value = mock_result
        
        await databricks_config_repository.deactivate_all()
        
        mock_async_session.execute.assert_called_once()
        mock_async_session.commit.assert_called_once()
        
        # Verify the update query was constructed correctly
        call_args = mock_async_session.execute.call_args[0][0]
        assert isinstance(call_args, type(update(DatabricksConfig)))
    
    @pytest.mark.asyncio
    async def test_deactivate_all_no_active_configs(self, databricks_config_repository, mock_async_session):
        """Test deactivate all when no active configurations exist."""
        mock_result = MagicMock()
        mock_async_session.execute.return_value = mock_result
        
        await databricks_config_repository.deactivate_all()
        
        # Should still execute the query even if no rows are affected
        mock_async_session.execute.assert_called_once()
        mock_async_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_deactivate_all_database_error(self, databricks_config_repository, mock_async_session):
        """Test deactivate all with database error."""
        mock_async_session.execute.side_effect = Exception("Update failed")
        
        with pytest.raises(Exception, match="Update failed"):
            await databricks_config_repository.deactivate_all()
    
    @pytest.mark.asyncio
    async def test_deactivate_all_commit_error(self, databricks_config_repository, mock_async_session):
        """Test deactivate all with commit error."""
        mock_result = MagicMock()
        mock_async_session.execute.return_value = mock_result
        mock_async_session.commit.side_effect = Exception("Commit failed")
        
        with pytest.raises(Exception, match="Commit failed"):
            await databricks_config_repository.deactivate_all()
        
        mock_async_session.execute.assert_called_once()


class TestDatabricksConfigRepositoryCreateConfig:
    """Test cases for create_config method."""
    
    @pytest.mark.asyncio
    async def test_create_config_success(self, databricks_config_repository, mock_async_session, sample_config_data):
        """Test successful configuration creation."""
        with patch('src.repositories.databricks_config_repository.DatabricksConfig') as mock_config_class:
            created_config = MockDatabricksConfig(**sample_config_data)
            mock_config_class.return_value = created_config
            
            # Mock deactivate_all method
            with patch.object(databricks_config_repository, 'deactivate_all') as mock_deactivate:
                result = await databricks_config_repository.create_config(sample_config_data)
                
                assert result == created_config
                mock_deactivate.assert_called_once()
                mock_config_class.assert_called_once_with(**sample_config_data)
                mock_async_session.add.assert_called_once_with(created_config)
                mock_async_session.flush.assert_called_once()
                mock_async_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_create_config_with_complex_data(self, databricks_config_repository, mock_async_session):
        """Test configuration creation with complex data."""
        complex_config_data = {
            "workspace_url": "https://complex.databricks.com",
            "token": "complex_token_12345",
            "is_active": True,
            "cluster_id": "0123-456789-abcdef",
            "warehouse_id": "warehouse_abc123"
        }
        
        with patch('src.repositories.databricks_config_repository.DatabricksConfig') as mock_config_class:
            created_config = MockDatabricksConfig(**complex_config_data)
            mock_config_class.return_value = created_config
            
            with patch.object(databricks_config_repository, 'deactivate_all') as mock_deactivate:
                result = await databricks_config_repository.create_config(complex_config_data)
                
                assert result == created_config
                mock_config_class.assert_called_once_with(**complex_config_data)
    
    @pytest.mark.asyncio
    async def test_create_config_deactivate_all_error(self, databricks_config_repository, mock_async_session, sample_config_data):
        """Test create config when deactivate_all fails."""
        with patch.object(databricks_config_repository, 'deactivate_all', side_effect=Exception("Deactivate failed")):
            with pytest.raises(Exception, match="Deactivate failed"):
                await databricks_config_repository.create_config(sample_config_data)
            
            # Should not proceed to create the config
            mock_async_session.add.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_create_config_model_creation_error(self, databricks_config_repository, mock_async_session, sample_config_data):
        """Test create config when model creation fails."""
        with patch.object(databricks_config_repository, 'deactivate_all'):
            with patch('src.repositories.databricks_config_repository.DatabricksConfig', side_effect=Exception("Model creation failed")):
                with pytest.raises(Exception, match="Model creation failed"):
                    await databricks_config_repository.create_config(sample_config_data)
    
    @pytest.mark.asyncio
    async def test_create_config_session_add_error(self, databricks_config_repository, mock_async_session, sample_config_data):
        """Test create config when session add fails."""
        with patch('src.repositories.databricks_config_repository.DatabricksConfig') as mock_config_class:
            created_config = MockDatabricksConfig(**sample_config_data)
            mock_config_class.return_value = created_config
            
            with patch.object(databricks_config_repository, 'deactivate_all'):
                mock_async_session.add.side_effect = Exception("Add failed")
                
                with pytest.raises(Exception, match="Add failed"):
                    await databricks_config_repository.create_config(sample_config_data)
    
    @pytest.mark.asyncio
    async def test_create_config_flush_error(self, databricks_config_repository, mock_async_session, sample_config_data):
        """Test create config when flush fails."""
        with patch('src.repositories.databricks_config_repository.DatabricksConfig') as mock_config_class:
            created_config = MockDatabricksConfig(**sample_config_data)
            mock_config_class.return_value = created_config
            
            with patch.object(databricks_config_repository, 'deactivate_all'):
                mock_async_session.flush.side_effect = Exception("Flush failed")
                
                with pytest.raises(Exception, match="Flush failed"):
                    await databricks_config_repository.create_config(sample_config_data)
    
    @pytest.mark.asyncio
    async def test_create_config_commit_error(self, databricks_config_repository, mock_async_session, sample_config_data):
        """Test create config when commit fails."""
        with patch('src.repositories.databricks_config_repository.DatabricksConfig') as mock_config_class:
            created_config = MockDatabricksConfig(**sample_config_data)
            mock_config_class.return_value = created_config
            
            with patch.object(databricks_config_repository, 'deactivate_all'):
                mock_async_session.commit.side_effect = Exception("Commit failed")
                
                with pytest.raises(Exception, match="Commit failed"):
                    await databricks_config_repository.create_config(sample_config_data)


class TestDatabricksConfigRepositoryIntegration:
    """Integration test cases testing method interactions."""
    
    @pytest.mark.asyncio
    async def test_get_active_then_create_new_workflow(self, databricks_config_repository, mock_async_session, sample_config_data):
        """Test workflow of getting active config then creating new one."""
        # First, mock getting an active config
        existing_config = MockDatabricksConfig(id=1, is_active=True)
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = existing_config
        mock_async_session.execute.return_value = mock_result
        
        active_config = await databricks_config_repository.get_active_config()
        assert active_config == existing_config
        
        # Reset mock for create operation
        mock_async_session.reset_mock()
        
        # Now create new config (should deactivate existing one)
        with patch('src.repositories.databricks_config_repository.DatabricksConfig') as mock_config_class:
            new_config = MockDatabricksConfig(**sample_config_data)
            mock_config_class.return_value = new_config
            
            with patch.object(databricks_config_repository, 'deactivate_all') as mock_deactivate:
                created_config = await databricks_config_repository.create_config(sample_config_data)
                
                assert created_config == new_config
                mock_deactivate.assert_called_once()  # Should deactivate existing configs
    
    @pytest.mark.asyncio
    async def test_deactivate_all_then_get_active_workflow(self, databricks_config_repository, mock_async_session):
        """Test workflow of deactivating all then checking for active config."""
        # First deactivate all
        mock_deactivate_result = MagicMock()
        mock_async_session.execute.return_value = mock_deactivate_result
        
        await databricks_config_repository.deactivate_all()
        mock_async_session.commit.assert_called_once()
        
        # Reset mock for get operation
        mock_async_session.reset_mock()
        
        # Now get active config (should be None)
        mock_get_result = MagicMock()
        mock_get_result.scalar_one_or_none.return_value = None
        mock_async_session.execute.return_value = mock_get_result
        
        active_config = await databricks_config_repository.get_active_config()
        assert active_config is None
    
    @pytest.mark.asyncio
    async def test_create_multiple_configs_workflow(self, databricks_config_repository, mock_async_session):
        """Test creating multiple configs ensures only one is active."""
        config_data_1 = {"workspace_url": "https://config1.databricks.com", "token": "token1", "is_active": True}
        config_data_2 = {"workspace_url": "https://config2.databricks.com", "token": "token2", "is_active": True}
        
        with patch('src.repositories.databricks_config_repository.DatabricksConfig') as mock_config_class:
            config1 = MockDatabricksConfig(**config_data_1)
            config2 = MockDatabricksConfig(**config_data_2)
            mock_config_class.side_effect = [config1, config2]
            
            with patch.object(databricks_config_repository, 'deactivate_all') as mock_deactivate:
                # Create first config
                result1 = await databricks_config_repository.create_config(config_data_1)
                assert result1 == config1
                
                # Create second config
                result2 = await databricks_config_repository.create_config(config_data_2)
                assert result2 == config2
                
                # deactivate_all should be called twice (once for each creation)
                assert mock_deactivate.call_count == 2


class TestDatabricksConfigRepositoryErrorHandling:
    """Test cases for error handling scenarios."""
    
    @pytest.mark.asyncio
    async def test_get_active_config_session_error(self, databricks_config_repository, mock_async_session):
        """Test get active config with session error."""
        mock_async_session.execute.side_effect = Exception("Session error")
        
        with pytest.raises(Exception, match="Session error"):
            await databricks_config_repository.get_active_config()
    
    @pytest.mark.asyncio
    async def test_deactivate_all_update_error(self, databricks_config_repository, mock_async_session):
        """Test deactivate all with update error."""
        mock_async_session.execute.side_effect = Exception("Update error")
        
        with pytest.raises(Exception, match="Update error"):
            await databricks_config_repository.deactivate_all()
        
        # Commit should not be called if execute fails
        mock_async_session.commit.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_create_config_complete_failure_chain(self, databricks_config_repository, mock_async_session, sample_config_data):
        """Test create config with cascading failures."""
        # Test each step of the create_config method can fail independently
        
        # 1. Deactivate failure
        with patch.object(databricks_config_repository, 'deactivate_all', side_effect=Exception("Deactivate failed")):
            with pytest.raises(Exception, match="Deactivate failed"):
                await databricks_config_repository.create_config(sample_config_data)
        
        # 2. Model creation failure
        with patch.object(databricks_config_repository, 'deactivate_all'):
            with patch('src.repositories.databricks_config_repository.DatabricksConfig', side_effect=Exception("Model failed")):
                with pytest.raises(Exception, match="Model failed"):
                    await databricks_config_repository.create_config(sample_config_data)
        
        # 3. Session add failure
        with patch.object(databricks_config_repository, 'deactivate_all'):
            with patch('src.repositories.databricks_config_repository.DatabricksConfig') as mock_config_class:
                mock_config_class.return_value = MockDatabricksConfig(**sample_config_data)
                mock_async_session.add.side_effect = Exception("Add failed")
                
                with pytest.raises(Exception, match="Add failed"):
                    await databricks_config_repository.create_config(sample_config_data)


class TestDatabricksConfigRepositoryEdgeCases:
    """Test cases for edge cases and boundary conditions."""
    
    @pytest.mark.asyncio
    async def test_create_config_empty_data(self, databricks_config_repository, mock_async_session):
        """Test creating config with empty data."""
        empty_data = {}
        
        with patch('src.repositories.databricks_config_repository.DatabricksConfig') as mock_config_class:
            mock_config = MockDatabricksConfig()
            mock_config_class.return_value = mock_config
            
            with patch.object(databricks_config_repository, 'deactivate_all'):
                result = await databricks_config_repository.create_config(empty_data)
                
                assert result == mock_config
                mock_config_class.assert_called_once_with(**empty_data)
    
    @pytest.mark.asyncio
    async def test_create_config_none_data(self, databricks_config_repository, mock_async_session):
        """Test creating config with None data."""
        with patch.object(databricks_config_repository, 'deactivate_all'):
            with pytest.raises(TypeError):
                await databricks_config_repository.create_config(None)
    
    @pytest.mark.asyncio
    async def test_deactivate_all_multiple_calls(self, databricks_config_repository, mock_async_session):
        """Test calling deactivate_all multiple times."""
        mock_result = MagicMock()
        mock_async_session.execute.return_value = mock_result
        
        # Call multiple times
        await databricks_config_repository.deactivate_all()
        await databricks_config_repository.deactivate_all()
        await databricks_config_repository.deactivate_all()
        
        # Should execute and commit each time
        assert mock_async_session.execute.call_count == 3
        assert mock_async_session.commit.call_count == 3
    
    @pytest.mark.asyncio
    async def test_get_active_config_multiple_calls(self, databricks_config_repository, mock_async_session, sample_databricks_configs):
        """Test calling get_active_config multiple times."""
        active_config = sample_databricks_configs[0]
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = active_config
        mock_async_session.execute.return_value = mock_result
        
        # Call multiple times
        result1 = await databricks_config_repository.get_active_config()
        result2 = await databricks_config_repository.get_active_config()
        result3 = await databricks_config_repository.get_active_config()
        
        assert result1 == active_config
        assert result2 == active_config
        assert result3 == active_config
        assert mock_async_session.execute.call_count == 3
    
    @pytest.mark.asyncio
    async def test_create_config_with_is_active_false(self, databricks_config_repository, mock_async_session):
        """Test creating config with is_active=False."""
        config_data = {
            "workspace_url": "https://inactive.databricks.com",
            "token": "inactive_token",
            "is_active": False  # Explicitly set to False
        }
        
        with patch('src.repositories.databricks_config_repository.DatabricksConfig') as mock_config_class:
            created_config = MockDatabricksConfig(**config_data)
            mock_config_class.return_value = created_config
            
            with patch.object(databricks_config_repository, 'deactivate_all') as mock_deactivate:
                result = await databricks_config_repository.create_config(config_data)
                
                assert result == created_config
                # Should still deactivate all existing configs even when creating inactive one
                mock_deactivate.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_datetime_timezone_handling(self, databricks_config_repository, mock_async_session):
        """Test that datetime timezone handling works correctly in deactivate_all."""
        with patch('src.repositories.databricks_config_repository.datetime') as mock_datetime:
            mock_now = datetime(2023, 12, 25, 10, 30, 45, tzinfo=timezone.utc)
            mock_datetime.now.return_value = mock_now
            mock_datetime.timezone = timezone  # Preserve timezone object
            
            mock_result = MagicMock()
            mock_async_session.execute.return_value = mock_result
            
            await databricks_config_repository.deactivate_all()
            
            # Verify datetime.now was called with timezone.utc
            mock_datetime.now.assert_called_once_with(timezone.utc)
            mock_async_session.execute.assert_called_once()


class TestDatabricksConfigRepositoryLogging:
    """Test cases for logging integration."""
    
    def test_logger_initialization(self):
        """Test that logger is properly initialized."""
        from src.repositories.databricks_config_repository import logger
        
        assert logger is not None
        assert logger.name == "src.repositories.databricks_config_repository"
    
    @pytest.mark.asyncio
    async def test_operations_work_without_explicit_logging_calls(self, databricks_config_repository, mock_async_session):
        """Test that operations work correctly even though they don't explicitly log."""
        # This repository doesn't have explicit logging calls, but operations should still work
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_async_session.execute.return_value = mock_result
        
        result = await databricks_config_repository.get_active_config()
        assert result is None
        
        # No logging calls expected in this repository's methods
        # This test ensures the logger doesn't interfere with functionality