"""
Unit tests for PowerBIConfigRepository.

Tests the functionality of Power BI configuration repository including
active configuration management, deactivation operations, and configuration creation.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock
from datetime import datetime, timezone

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from src.repositories.powerbi_config_repository import PowerBIConfigRepository
from src.models.powerbi_config import PowerBIConfig


# Mock Power BI config model
class MockPowerBIConfig:
    def __init__(self, id=1, tenant_id="test-tenant", client_id="test-client",
                 workspace_id="test-workspace", semantic_model_id="test-model",
                 is_active=True, is_enabled=True, group_id=None,
                 created_at=None, updated_at=None, **kwargs):
        self.id = id
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.workspace_id = workspace_id
        self.semantic_model_id = semantic_model_id
        self.is_active = is_active
        self.is_enabled = is_enabled
        self.group_id = group_id
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
def powerbi_config_repository(mock_async_session):
    """Create a Power BI config repository with async session."""
    return PowerBIConfigRepository(session=mock_async_session)


@pytest.fixture
def sample_powerbi_configs():
    """Create sample Power BI configurations for testing."""
    return [
        MockPowerBIConfig(id=1, tenant_id="active-tenant", is_active=True, group_id="group1"),
        MockPowerBIConfig(id=2, tenant_id="inactive-tenant", is_active=False, group_id="group1"),
        MockPowerBIConfig(id=3, tenant_id="other-group", is_active=True, group_id="group2")
    ]


@pytest.fixture
def sample_config_data():
    """Create sample config data for creation."""
    return {
        "tenant_id": "new-tenant",
        "client_id": "new-client",
        "workspace_id": "new-workspace",
        "semantic_model_id": "new-model",
        "is_active": True,
        "is_enabled": True,
        "group_id": "group1"
    }


class TestPowerBIConfigRepositoryInit:
    """Test cases for PowerBIConfigRepository initialization."""

    def test_init_success(self, mock_async_session):
        """Test successful initialization."""
        repository = PowerBIConfigRepository(session=mock_async_session)

        assert repository.model == PowerBIConfig
        assert repository.session == mock_async_session


class TestPowerBIConfigRepositoryGetActiveConfig:
    """Test cases for get_active_config method."""

    @pytest.mark.asyncio
    async def test_get_active_config_success(self, powerbi_config_repository, mock_async_session, sample_powerbi_configs):
        """Test successful retrieval of active configuration."""
        active_config = sample_powerbi_configs[0]  # is_active=True, group1

        mock_result = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.first.return_value = active_config
        mock_result.scalars.return_value = mock_scalars
        mock_async_session.execute.return_value = mock_result

        result = await powerbi_config_repository.get_active_config(group_id="group1")

        assert result == active_config
        mock_async_session.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_active_config_no_group_filter(self, powerbi_config_repository, mock_async_session, sample_powerbi_configs):
        """Test get active config without group filter."""
        active_config = sample_powerbi_configs[0]

        mock_result = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.first.return_value = active_config
        mock_result.scalars.return_value = mock_scalars
        mock_async_session.execute.return_value = mock_result

        result = await powerbi_config_repository.get_active_config()

        assert result == active_config
        mock_async_session.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_active_config_none_found(self, powerbi_config_repository, mock_async_session):
        """Test get active config when no active configuration exists."""
        mock_result = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.first.return_value = None
        mock_result.scalars.return_value = mock_scalars
        mock_async_session.execute.return_value = mock_result

        result = await powerbi_config_repository.get_active_config(group_id="nonexistent")

        assert result is None
        mock_async_session.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_active_config_database_error(self, powerbi_config_repository, mock_async_session):
        """Test get active config handles database errors."""
        mock_async_session.execute.side_effect = Exception("Database connection error")

        with pytest.raises(Exception, match="Database connection error"):
            await powerbi_config_repository.get_active_config()


class TestPowerBIConfigRepositoryDeactivateAll:
    """Test cases for deactivate_all method."""

    @pytest.mark.asyncio
    async def test_deactivate_all_success(self, powerbi_config_repository, mock_async_session):
        """Test successful deactivation of all configs for a group."""
        mock_result = MagicMock()
        mock_async_session.execute.return_value = mock_result

        await powerbi_config_repository.deactivate_all(group_id="group1")

        # Should be called twice: once for update, once for commit
        assert mock_async_session.execute.call_count == 1
        mock_async_session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_deactivate_all_no_group_filter(self, powerbi_config_repository, mock_async_session):
        """Test deactivate all without group filter."""
        mock_result = MagicMock()
        mock_async_session.execute.return_value = mock_result

        await powerbi_config_repository.deactivate_all()

        assert mock_async_session.execute.call_count == 1
        mock_async_session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_deactivate_all_database_error(self, powerbi_config_repository, mock_async_session):
        """Test deactivate all handles database errors."""
        mock_async_session.execute.side_effect = Exception("Database error")

        with pytest.raises(Exception, match="Database error"):
            await powerbi_config_repository.deactivate_all(group_id="group1")


class TestPowerBIConfigRepositoryCreateConfig:
    """Test cases for create_config method."""

    @pytest.mark.asyncio
    async def test_create_config_success(self, powerbi_config_repository, mock_async_session, sample_config_data):
        """Test successful configuration creation."""
        # Mock deactivate_all
        mock_result = MagicMock()
        mock_async_session.execute.return_value = mock_result

        result = await powerbi_config_repository.create_config(sample_config_data)

        # Verify deactivate_all was called
        assert mock_async_session.execute.call_count >= 1

        # Verify config was added to session
        mock_async_session.add.assert_called_once()

        # Verify flush and commit were called
        mock_async_session.flush.assert_called_once()
        assert mock_async_session.commit.call_count >= 1

        # Verify returned config has expected attributes
        assert isinstance(result, PowerBIConfig)

    @pytest.mark.asyncio
    async def test_create_config_with_group_id(self, powerbi_config_repository, mock_async_session):
        """Test config creation with group_id."""
        config_data = {
            "tenant_id": "test-tenant",
            "client_id": "test-client",
            "group_id": "test-group"
        }

        mock_result = MagicMock()
        mock_async_session.execute.return_value = mock_result

        result = await powerbi_config_repository.create_config(config_data)

        mock_async_session.add.assert_called_once()
        mock_async_session.flush.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_config_none_data_error(self, powerbi_config_repository):
        """Test create config with None data raises error."""
        with pytest.raises(TypeError, match="config_data cannot be None"):
            await powerbi_config_repository.create_config(None)

    @pytest.mark.asyncio
    async def test_create_config_database_error(self, powerbi_config_repository, mock_async_session, sample_config_data):
        """Test create config handles database errors."""
        mock_async_session.add.side_effect = Exception("Database error")

        with pytest.raises(Exception, match="Database error"):
            await powerbi_config_repository.create_config(sample_config_data)


class TestPowerBIConfigRepositoryMultiTenancy:
    """Test cases for multi-tenant functionality."""

    @pytest.mark.asyncio
    async def test_get_active_config_different_groups(self, powerbi_config_repository, mock_async_session):
        """Test that get_active_config properly filters by group."""
        # Mock two different configs for different groups
        group1_config = MockPowerBIConfig(id=1, group_id="group1", is_active=True)
        group2_config = MockPowerBIConfig(id=2, group_id="group2", is_active=True)

        mock_result = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.first.return_value = group1_config
        mock_result.scalars.return_value = mock_scalars
        mock_async_session.execute.return_value = mock_result

        result = await powerbi_config_repository.get_active_config(group_id="group1")

        assert result == group1_config
        assert result.group_id == "group1"

    @pytest.mark.asyncio
    async def test_deactivate_only_affects_specified_group(self, powerbi_config_repository, mock_async_session):
        """Test that deactivate_all only affects the specified group."""
        mock_result = MagicMock()
        mock_async_session.execute.return_value = mock_result

        await powerbi_config_repository.deactivate_all(group_id="group1")

        # Verify execute was called with a query
        assert mock_async_session.execute.call_count == 1
        mock_async_session.commit.assert_called_once()
