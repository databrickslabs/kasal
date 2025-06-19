"""
Unit tests for EngineConfigRepository.

Tests the functionality of engine config repository including
CRUD operations, configuration management, enabled/disabled filtering, and error handling.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime
from typing import List

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from src.repositories.engine_config_repository import EngineConfigRepository
from src.models.engine_config import EngineConfig


# Mock engine config model
class MockEngineConfig:
    def __init__(self, id=1, engine_name="test_engine", engine_type="workflow",
                 config_key="test_key", config_value="test_value", enabled=True,
                 description="Test Description", created_at=None, updated_at=None):
        self.id = id
        self.engine_name = engine_name
        self.engine_type = engine_type
        self.config_key = config_key
        self.config_value = config_value
        self.enabled = enabled
        self.description = description
        self.created_at = created_at or datetime.utcnow()
        self.updated_at = updated_at or datetime.utcnow()


# Mock SQLAlchemy result objects
class MockScalars:
    def __init__(self, results):
        self.results = results
    
    def first(self):
        return self.results[0] if self.results else None
    
    def all(self):
        return self.results


class MockResult:
    def __init__(self, results):
        self._scalars = MockScalars(results)
    
    def scalars(self):
        return self._scalars


@pytest.fixture
def mock_async_session():
    """Create a mock async database session."""
    session = AsyncMock(spec=AsyncSession)
    session.execute = AsyncMock()
    session.add = AsyncMock()
    session.flush = AsyncMock()
    session.commit = AsyncMock()
    session.refresh = AsyncMock()
    session.rollback = AsyncMock()
    return session


@pytest.fixture
def engine_config_repository(mock_async_session):
    """Create an engine config repository with async session."""
    return EngineConfigRepository(session=mock_async_session)


@pytest.fixture
def sample_engine_configs():
    """Create sample engine configs for testing."""
    return [
        MockEngineConfig(id=1, engine_name="crewai", engine_type="workflow", 
                        config_key="flow_enabled", config_value="true", enabled=True),
        MockEngineConfig(id=2, engine_name="crewai", engine_type="workflow", 
                        config_key="max_iterations", config_value="10", enabled=True),
        MockEngineConfig(id=3, engine_name="langchain", engine_type="chain", 
                        config_key="temperature", config_value="0.7", enabled=False),
        MockEngineConfig(id=4, engine_name="openai", engine_type="llm", 
                        config_key="model", config_value="gpt-4", enabled=True)
    ]


@pytest.fixture
def sample_config_data():
    """Create sample config data for creation."""
    return {
        "engine_name": "new_engine",
        "engine_type": "test",
        "config_key": "new_key",
        "config_value": "new_value",
        "enabled": True,
        "description": "A new test config"
    }


class TestEngineConfigRepositoryInit:
    """Test cases for EngineConfigRepository initialization."""
    
    def test_init_success(self, mock_async_session):
        """Test successful initialization."""
        repository = EngineConfigRepository(session=mock_async_session)
        
        assert repository.model == EngineConfig
        assert repository.session == mock_async_session


class TestEngineConfigRepositoryFindAll:
    """Test cases for find_all method."""
    
    @pytest.mark.asyncio
    async def test_find_all_success(self, engine_config_repository, mock_async_session, sample_engine_configs):
        """Test successful retrieval of all engine configs."""
        mock_result = MockResult(sample_engine_configs)
        mock_async_session.execute.return_value = mock_result
        
        result = await engine_config_repository.find_all()
        
        assert len(result) == len(sample_engine_configs)
        assert result == sample_engine_configs
        mock_async_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_find_all_empty_result(self, engine_config_repository, mock_async_session):
        """Test find all when no configs exist."""
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result
        
        result = await engine_config_repository.find_all()
        
        assert result == []
        mock_async_session.execute.assert_called_once()


class TestEngineConfigRepositoryFindByEngineName:
    """Test cases for find_by_engine_name method."""
    
    @pytest.mark.asyncio
    async def test_find_by_engine_name_success(self, engine_config_repository, mock_async_session):
        """Test successful engine config search by engine name."""
        config = MockEngineConfig(engine_name="crewai")
        mock_result = MockResult([config])
        mock_async_session.execute.return_value = mock_result
        
        result = await engine_config_repository.find_by_engine_name("crewai")
        
        assert result == config
        mock_async_session.execute.assert_called_once()
        # Verify the query was constructed correctly
        call_args = mock_async_session.execute.call_args[0][0]
        assert isinstance(call_args, type(select(EngineConfig)))
    
    @pytest.mark.asyncio
    async def test_find_by_engine_name_not_found(self, engine_config_repository, mock_async_session):
        """Test find by engine name when config not found."""
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result
        
        result = await engine_config_repository.find_by_engine_name("nonexistent")
        
        assert result is None
        mock_async_session.execute.assert_called_once()


class TestEngineConfigRepositoryFindByEngineAndKey:
    """Test cases for find_by_engine_and_key method."""
    
    @pytest.mark.asyncio
    async def test_find_by_engine_and_key_success(self, engine_config_repository, mock_async_session):
        """Test successful search by engine name and config key."""
        config = MockEngineConfig(engine_name="crewai", config_key="flow_enabled")
        mock_result = MockResult([config])
        mock_async_session.execute.return_value = mock_result
        
        result = await engine_config_repository.find_by_engine_and_key("crewai", "flow_enabled")
        
        assert result == config
        mock_async_session.execute.assert_called_once()
        # Verify the query was constructed with both conditions
        call_args = mock_async_session.execute.call_args[0][0]
        assert isinstance(call_args, type(select(EngineConfig)))
    
    @pytest.mark.asyncio
    async def test_find_by_engine_and_key_not_found(self, engine_config_repository, mock_async_session):
        """Test find by engine and key when config not found."""
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result
        
        result = await engine_config_repository.find_by_engine_and_key("crewai", "nonexistent_key")
        
        assert result is None
        mock_async_session.execute.assert_called_once()


class TestEngineConfigRepositoryFindEnabledConfigs:
    """Test cases for find_enabled_configs method."""
    
    @pytest.mark.asyncio
    async def test_find_enabled_configs_success(self, engine_config_repository, mock_async_session, sample_engine_configs):
        """Test successful retrieval of enabled configs."""
        enabled_configs = [config for config in sample_engine_configs if config.enabled]
        mock_result = MockResult(enabled_configs)
        mock_async_session.execute.return_value = mock_result
        
        result = await engine_config_repository.find_enabled_configs()
        
        assert len(result) == len(enabled_configs)
        assert all(config.enabled for config in result)
        mock_async_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_find_enabled_configs_none_enabled(self, engine_config_repository, mock_async_session):
        """Test find enabled configs when none are enabled."""
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result
        
        result = await engine_config_repository.find_enabled_configs()
        
        assert result == []
        mock_async_session.execute.assert_called_once()


class TestEngineConfigRepositoryFindByEngineType:
    """Test cases for find_by_engine_type method."""
    
    @pytest.mark.asyncio
    async def test_find_by_engine_type_success(self, engine_config_repository, mock_async_session, sample_engine_configs):
        """Test successful search by engine type."""
        workflow_configs = [config for config in sample_engine_configs if config.engine_type == "workflow"]
        mock_result = MockResult(workflow_configs)
        mock_async_session.execute.return_value = mock_result
        
        result = await engine_config_repository.find_by_engine_type("workflow")
        
        assert len(result) == len(workflow_configs)
        assert all(config.engine_type == "workflow" for config in result)
        mock_async_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_find_by_engine_type_not_found(self, engine_config_repository, mock_async_session):
        """Test find by engine type when no configs found."""
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result
        
        result = await engine_config_repository.find_by_engine_type("nonexistent_type")
        
        assert result == []
        mock_async_session.execute.assert_called_once()


class TestEngineConfigRepositoryToggleEnabled:
    """Test cases for toggle_enabled method."""
    
    @pytest.mark.asyncio
    async def test_toggle_enabled_success(self, engine_config_repository, mock_async_session):
        """Test successful toggling of engine enabled status."""
        config = MockEngineConfig(engine_name="crewai", enabled=False)
        
        with patch.object(engine_config_repository, 'find_by_engine_name', return_value=config):
            result = await engine_config_repository.toggle_enabled("crewai", True)
            
            assert result is True
            assert config.enabled is True
            mock_async_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_toggle_enabled_engine_not_found(self, engine_config_repository, mock_async_session):
        """Test toggle enabled when engine not found."""
        with patch.object(engine_config_repository, 'find_by_engine_name', return_value=None):
            result = await engine_config_repository.toggle_enabled("nonexistent", True)
            
            assert result is False
            mock_async_session.commit.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_toggle_enabled_disable(self, engine_config_repository, mock_async_session):
        """Test disabling an engine."""
        config = MockEngineConfig(engine_name="crewai", enabled=True)
        
        with patch.object(engine_config_repository, 'find_by_engine_name', return_value=config):
            result = await engine_config_repository.toggle_enabled("crewai", False)
            
            assert result is True
            assert config.enabled is False
            mock_async_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_toggle_enabled_database_error(self, engine_config_repository, mock_async_session):
        """Test toggle enabled with database error."""
        config = MockEngineConfig(engine_name="crewai")
        
        with patch.object(engine_config_repository, 'find_by_engine_name', return_value=config):
            mock_async_session.commit.side_effect = Exception("Commit failed")
            
            with pytest.raises(Exception, match="Commit failed"):
                await engine_config_repository.toggle_enabled("crewai", True)
            
            mock_async_session.rollback.assert_called_once()


class TestEngineConfigRepositoryUpdateConfigValue:
    """Test cases for update_config_value method."""
    
    @pytest.mark.asyncio
    async def test_update_config_value_success(self, engine_config_repository, mock_async_session):
        """Test successful config value update."""
        config = MockEngineConfig(engine_name="crewai", config_key="flow_enabled", config_value="false")
        
        with patch.object(engine_config_repository, 'find_by_engine_and_key', return_value=config):
            result = await engine_config_repository.update_config_value("crewai", "flow_enabled", "true")
            
            assert result is True
            assert config.config_value == "true"
            mock_async_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_config_value_not_found(self, engine_config_repository, mock_async_session):
        """Test config value update when config not found."""
        with patch.object(engine_config_repository, 'find_by_engine_and_key', return_value=None):
            result = await engine_config_repository.update_config_value("crewai", "nonexistent", "value")
            
            assert result is False
            mock_async_session.commit.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_update_config_value_database_error(self, engine_config_repository, mock_async_session):
        """Test config value update with database error."""
        config = MockEngineConfig(engine_name="crewai", config_key="flow_enabled")
        
        with patch.object(engine_config_repository, 'find_by_engine_and_key', return_value=config):
            mock_async_session.commit.side_effect = Exception("Update failed")
            
            with pytest.raises(Exception, match="Update failed"):
                await engine_config_repository.update_config_value("crewai", "flow_enabled", "true")
            
            mock_async_session.rollback.assert_called_once()


class TestEngineConfigRepositoryCrewAIFlowMethods:
    """Test cases for CrewAI flow specific methods."""
    
    @pytest.mark.asyncio
    async def test_get_crewai_flow_enabled_true(self, engine_config_repository, mock_async_session):
        """Test get CrewAI flow enabled when config exists and is true."""
        config = MockEngineConfig(engine_name="crewai", config_key="flow_enabled", config_value="true")
        
        with patch.object(engine_config_repository, 'find_by_engine_and_key', return_value=config):
            result = await engine_config_repository.get_crewai_flow_enabled()
            
            assert result is True
    
    @pytest.mark.asyncio
    async def test_get_crewai_flow_enabled_false(self, engine_config_repository, mock_async_session):
        """Test get CrewAI flow enabled when config exists and is false."""
        config = MockEngineConfig(engine_name="crewai", config_key="flow_enabled", config_value="false")
        
        with patch.object(engine_config_repository, 'find_by_engine_and_key', return_value=config):
            result = await engine_config_repository.get_crewai_flow_enabled()
            
            assert result is False
    
    @pytest.mark.asyncio
    async def test_get_crewai_flow_enabled_case_insensitive(self, engine_config_repository, mock_async_session):
        """Test get CrewAI flow enabled with different case values."""
        # Test uppercase TRUE
        config_upper = MockEngineConfig(config_value="TRUE")
        with patch.object(engine_config_repository, 'find_by_engine_and_key', return_value=config_upper):
            result = await engine_config_repository.get_crewai_flow_enabled()
            assert result is True
        
        # Test mixed case False
        config_mixed = MockEngineConfig(config_value="False")
        with patch.object(engine_config_repository, 'find_by_engine_and_key', return_value=config_mixed):
            result = await engine_config_repository.get_crewai_flow_enabled()
            assert result is False
    
    @pytest.mark.asyncio
    async def test_get_crewai_flow_enabled_not_found_defaults_false(self, engine_config_repository, mock_async_session):
        """Test get CrewAI flow enabled when config not found (defaults to False)."""
        with patch.object(engine_config_repository, 'find_by_engine_and_key', return_value=None):
            result = await engine_config_repository.get_crewai_flow_enabled()
            
            assert result is False
    
    @pytest.mark.asyncio
    async def test_set_crewai_flow_enabled_update_existing(self, engine_config_repository, mock_async_session):
        """Test set CrewAI flow enabled by updating existing config."""
        with patch.object(engine_config_repository, 'update_config_value', return_value=True) as mock_update:
            result = await engine_config_repository.set_crewai_flow_enabled(False)
            
            assert result is True
            mock_update.assert_called_once_with("crewai", "flow_enabled", "false")
    
    @pytest.mark.asyncio
    async def test_set_crewai_flow_enabled_create_new(self, engine_config_repository, mock_async_session):
        """Test set CrewAI flow enabled by creating new config when none exists."""
        with patch.object(engine_config_repository, 'update_config_value', return_value=False):
            with patch.object(engine_config_repository, 'create', return_value=MockEngineConfig()) as mock_create:
                result = await engine_config_repository.set_crewai_flow_enabled(True)
                
                assert result is True
                mock_create.assert_called_once()
                
                # Verify the config data passed to create
                create_call_args = mock_create.call_args[0][0]
                assert create_call_args["engine_name"] == "crewai"
                assert create_call_args["config_key"] == "flow_enabled"
                assert create_call_args["config_value"] == "true"
                assert create_call_args["enabled"] is True
    
    @pytest.mark.asyncio
    async def test_set_crewai_flow_enabled_create_error(self, engine_config_repository, mock_async_session):
        """Test set CrewAI flow enabled with error during creation."""
        with patch.object(engine_config_repository, 'update_config_value', return_value=False):
            with patch.object(engine_config_repository, 'create', side_effect=Exception("Create failed")):
                with pytest.raises(Exception, match="Create failed"):
                    await engine_config_repository.set_crewai_flow_enabled(True)
                
                mock_async_session.rollback.assert_called_once()


class TestEngineConfigRepositoryIntegration:
    """Integration test cases testing method interactions."""
    
    @pytest.mark.asyncio
    async def test_create_then_find_by_engine_and_key(self, engine_config_repository, mock_async_session):
        """Test creating config then finding it by engine and key."""
        config_data = {
            "engine_name": "integration_engine",
            "engine_type": "test",
            "config_key": "test_key",
            "config_value": "test_value",
            "enabled": True
        }
        
        with patch('src.repositories.engine_config_repository.EngineConfig') as mock_config_class:
            created_config = MockEngineConfig(**config_data)
            mock_config_class.return_value = created_config
            
            # Mock find_by_engine_and_key for retrieval
            mock_result = MockResult([created_config])
            mock_async_session.execute.return_value = mock_result
            
            # Create config using inherited create method
            with patch.object(engine_config_repository, 'create', return_value=created_config) as mock_create:
                create_result = await engine_config_repository.create(config_data)
                
                # Find config by engine and key
                find_result = await engine_config_repository.find_by_engine_and_key(
                    "integration_engine", "test_key"
                )
                
                assert create_result == created_config
                assert find_result == created_config
                mock_create.assert_called_once_with(config_data)
                mock_async_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_toggle_enabled_then_find_enabled_configs(self, engine_config_repository, mock_async_session):
        """Test toggling config enabled then finding enabled configs."""
        config = MockEngineConfig(engine_name="test_engine", enabled=False)
        
        # Toggle enabled
        with patch.object(engine_config_repository, 'find_by_engine_name', return_value=config):
            toggle_result = await engine_config_repository.toggle_enabled("test_engine", True)
            
            assert toggle_result is True
            assert config.enabled is True
            
            # Find enabled configs
            mock_result = MockResult([config])
            mock_async_session.execute.return_value = mock_result
            
            enabled_configs = await engine_config_repository.find_enabled_configs()
            
            assert len(enabled_configs) == 1
            assert enabled_configs[0].enabled is True
    
    @pytest.mark.asyncio
    async def test_crewai_flow_configuration_workflow(self, engine_config_repository, mock_async_session):
        """Test the complete workflow of CrewAI flow configuration."""
        # Initially not configured (should default to False)
        with patch.object(engine_config_repository, 'find_by_engine_and_key', return_value=None):
            initial_status = await engine_config_repository.get_crewai_flow_enabled()
            assert initial_status is False
        
        # Set to False (creates new config)
        with patch.object(engine_config_repository, 'update_config_value', return_value=False):
            with patch.object(engine_config_repository, 'create', return_value=MockEngineConfig()) as mock_create:
                result = await engine_config_repository.set_crewai_flow_enabled(False)
                assert result is True
                mock_create.assert_called_once()
        
        # Get status (should be False now)
        false_config = MockEngineConfig(config_value="false")
        with patch.object(engine_config_repository, 'find_by_engine_and_key', return_value=false_config):
            current_status = await engine_config_repository.get_crewai_flow_enabled()
            assert current_status is False
        
        # Set back to True (updates existing config)
        with patch.object(engine_config_repository, 'update_config_value', return_value=True) as mock_update:
            result = await engine_config_repository.set_crewai_flow_enabled(True)
            assert result is True
            mock_update.assert_called_once_with("crewai", "flow_enabled", "true")


class TestEngineConfigRepositoryErrorHandling:
    """Test cases for error handling scenarios."""
    
    @pytest.mark.asyncio
    async def test_find_by_engine_name_database_error(self, engine_config_repository, mock_async_session):
        """Test find by engine name with database error."""
        mock_async_session.execute.side_effect = Exception("Connection lost")
        
        with pytest.raises(Exception, match="Connection lost"):
            await engine_config_repository.find_by_engine_name("crewai")
    
    @pytest.mark.asyncio
    async def test_find_enabled_configs_database_error(self, engine_config_repository, mock_async_session):
        """Test find enabled configs with database error."""
        mock_async_session.execute.side_effect = Exception("Query timeout")
        
        with pytest.raises(Exception, match="Query timeout"):
            await engine_config_repository.find_enabled_configs()
    
    @pytest.mark.asyncio
    async def test_toggle_enabled_find_error(self, engine_config_repository, mock_async_session):
        """Test toggle enabled when find_by_engine_name fails."""
        with patch.object(engine_config_repository, 'find_by_engine_name', side_effect=Exception("Find failed")):
            with pytest.raises(Exception, match="Find failed"):
                await engine_config_repository.toggle_enabled("crewai", True)
            
            mock_async_session.rollback.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_config_value_find_error(self, engine_config_repository, mock_async_session):
        """Test update config value when find_by_engine_and_key fails."""
        with patch.object(engine_config_repository, 'find_by_engine_and_key', side_effect=Exception("Find failed")):
            with pytest.raises(Exception, match="Find failed"):
                await engine_config_repository.update_config_value("crewai", "flow_enabled", "true")
            
            mock_async_session.rollback.assert_called_once()


class TestEngineConfigRepositoryEdgeCases:
    """Test cases for edge cases and boundary conditions."""
    
    @pytest.mark.asyncio
    async def test_config_value_variations(self, engine_config_repository, mock_async_session):
        """Test various config value formats for boolean conversion."""
        test_cases = [
            ("true", True),
            ("TRUE", True),
            ("True", True),
            ("false", False),
            ("FALSE", False),
            ("False", False),
            ("yes", False),  # Should be False for non-"true" values
            ("1", False),
            ("", False),
        ]
        
        for config_value, expected in test_cases:
            config = MockEngineConfig(config_value=config_value)
            with patch.object(engine_config_repository, 'find_by_engine_and_key', return_value=config):
                result = await engine_config_repository.get_crewai_flow_enabled()
                assert result is expected, f"Failed for config_value='{config_value}'"
    
    @pytest.mark.asyncio
    async def test_set_crewai_flow_enabled_boolean_to_string_conversion(self, engine_config_repository, mock_async_session):
        """Test that boolean values are correctly converted to strings."""
        with patch.object(engine_config_repository, 'update_config_value') as mock_update:
            mock_update.return_value = True
            
            # Test True -> "true"
            await engine_config_repository.set_crewai_flow_enabled(True)
            mock_update.assert_called_with("crewai", "flow_enabled", "true")
            
            # Test False -> "false"
            await engine_config_repository.set_crewai_flow_enabled(False)
            mock_update.assert_called_with("crewai", "flow_enabled", "false")
    
    @pytest.mark.asyncio
    async def test_find_by_engine_type_multiple_engines(self, engine_config_repository, mock_async_session):
        """Test finding configs by engine type with multiple matching engines."""
        workflow_configs = [
            MockEngineConfig(engine_name="crewai", engine_type="workflow"),
            MockEngineConfig(engine_name="langchain", engine_type="workflow"),
            MockEngineConfig(engine_name="custom_engine", engine_type="workflow")
        ]
        
        mock_result = MockResult(workflow_configs)
        mock_async_session.execute.return_value = mock_result
        
        result = await engine_config_repository.find_by_engine_type("workflow")
        
        assert len(result) == 3
        assert all(config.engine_type == "workflow" for config in result)
        
        # Verify different engine names
        engine_names = {config.engine_name for config in result}
        assert engine_names == {"crewai", "langchain", "custom_engine"}
    
    @pytest.mark.asyncio
    async def test_multiple_configs_same_engine_different_keys(self, engine_config_repository, mock_async_session):
        """Test handling multiple configs for the same engine with different keys."""
        crewai_configs = [
            MockEngineConfig(engine_name="crewai", config_key="flow_enabled"),
            MockEngineConfig(engine_name="crewai", config_key="max_iterations"),
            MockEngineConfig(engine_name="crewai", config_key="temperature")
        ]
        
        # Test that find_by_engine_name returns first match
        mock_result = MockResult([crewai_configs[0]])  # Returns first match
        mock_async_session.execute.return_value = mock_result
        
        result = await engine_config_repository.find_by_engine_name("crewai")
        
        assert result == crewai_configs[0]
        assert result.config_key == "flow_enabled"
    
    @pytest.mark.asyncio
    async def test_empty_string_engine_name(self, engine_config_repository, mock_async_session):
        """Test handling of empty string engine name."""
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result
        
        result = await engine_config_repository.find_by_engine_name("")
        
        assert result is None
        mock_async_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_none_config_value_handling(self, engine_config_repository, mock_async_session):
        """Test handling of None config values."""
        config = MockEngineConfig(config_value=None)
        
        with patch.object(engine_config_repository, 'find_by_engine_and_key', return_value=config):
            # This should raise an AttributeError when trying to call .lower() on None
            with pytest.raises(AttributeError):
                await engine_config_repository.get_crewai_flow_enabled()