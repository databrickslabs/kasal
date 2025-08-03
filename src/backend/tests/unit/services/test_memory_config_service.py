"""
Unit tests for MemoryConfigService.

Tests configuration management and active config retrieval logic.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock
from datetime import datetime, timedelta
from uuid import uuid4

from src.services.memory_config_service import MemoryConfigService
from src.models.memory_backend import MemoryBackend
from src.schemas.memory_backend import (
    MemoryBackendConfig,
    DatabricksMemoryConfig,
    MemoryBackendType
)
from src.core.unit_of_work import UnitOfWork


@pytest.fixture
def mock_uow():
    """Create a mock Unit of Work."""
    uow = AsyncMock(spec=UnitOfWork)
    uow.memory_backend_repository = AsyncMock()
    return uow


@pytest.fixture
def service(mock_uow):
    """Create a MemoryConfigService instance."""
    return MemoryConfigService(mock_uow)


@pytest.fixture
def databricks_backend():
    """Create a Databricks memory backend."""
    return MemoryBackend(
        id=str(uuid4()),
        group_id="test-group-123",
        name="Databricks Backend",
        description="Databricks Vector Search backend",
        backend_type=MemoryBackendType.DATABRICKS,
        databricks_config={
            "endpoint_name": "test-endpoint",
            "short_term_index": "ml.agents.short_term",
            "long_term_index": "ml.agents.long_term",
            "entity_index": "ml.agents.entity",
            "workspace_url": "https://test.databricks.com",
            "embedding_dimension": 768
        },
        enable_short_term=True,
        enable_long_term=True,
        enable_entity=True,
        is_active=True,
        is_default=False,
        created_at=datetime.utcnow() - timedelta(days=2),
        updated_at=datetime.utcnow() - timedelta(days=1)
    )


@pytest.fixture
def default_backend():
    """Create a default memory backend."""
    return MemoryBackend(
        id=str(uuid4()),
        group_id="test-group-123",
        name="Default Backend",
        description="Default memory backend",
        backend_type=MemoryBackendType.DEFAULT,
        databricks_config=None,
        enable_short_term=True,
        enable_long_term=True,
        enable_entity=False,
        is_active=True,
        is_default=False,
        created_at=datetime.utcnow() - timedelta(days=5),
        updated_at=datetime.utcnow() - timedelta(days=3)
    )


class TestMemoryConfigService:
    """Test cases for MemoryConfigService."""
    
    @pytest.mark.asyncio
    async def test_get_active_config_with_group_id_databricks_preferred(
        self, service, mock_uow, databricks_backend, default_backend
    ):
        """Test that Databricks backend is preferred when group_id is provided."""
        # Arrange
        group_id = "test-group-123"
        mock_uow.memory_backend_repository.get_by_group_id.return_value = [
            default_backend,
            databricks_backend
        ]
        
        # Act
        result = await service.get_active_config(group_id)
        
        # Assert
        assert result is not None
        assert result.backend_type == MemoryBackendType.DATABRICKS
        assert isinstance(result.databricks_config, DatabricksMemoryConfig)
        assert result.databricks_config.endpoint_name == "test-endpoint"
        assert result.enable_short_term is True
        assert result.enable_long_term is True
        assert result.enable_entity is True
    
    @pytest.mark.asyncio
    async def test_get_active_config_most_recent_when_no_databricks(
        self, service, mock_uow, default_backend
    ):
        """Test that most recently updated backend is selected when no Databricks backend."""
        # Arrange
        group_id = "test-group-123"
        older_backend = MagicMock(
            backend_type=MemoryBackendType.DEFAULT,
            is_active=True,
            updated_at=datetime.utcnow() - timedelta(days=5),
            enable_short_term=False,
            enable_long_term=False,
            enable_entity=False,
            databricks_config=None,
            custom_config=None
        )
        newer_backend = MagicMock(
            backend_type=MemoryBackendType.DEFAULT,
            is_active=True,
            updated_at=datetime.utcnow() - timedelta(days=1),
            enable_short_term=True,
            enable_long_term=True,
            enable_entity=True,
            databricks_config=None,
            custom_config=None
        )
        
        mock_uow.memory_backend_repository.get_by_group_id.return_value = [
            older_backend,
            newer_backend
        ]
        
        # Act
        result = await service.get_active_config(group_id)
        
        # Assert
        assert result is not None
        assert result.backend_type == MemoryBackendType.DEFAULT
        assert result.enable_short_term is True
        assert result.enable_long_term is True
        assert result.enable_entity is True
    
    @pytest.mark.asyncio
    async def test_get_active_config_only_active_backends(
        self, service, mock_uow, databricks_backend
    ):
        """Test that only active backends are considered."""
        # Arrange
        group_id = "test-group-123"
        databricks_backend.is_active = False
        active_backend = MagicMock(
            backend_type=MemoryBackendType.DEFAULT,
            is_active=True,
            updated_at=datetime.utcnow(),
            enable_short_term=True,
            enable_long_term=False,
            enable_entity=False,
            databricks_config=None,
            custom_config={"test": "value"}
        )
        
        mock_uow.memory_backend_repository.get_by_group_id.return_value = [
            databricks_backend,
            active_backend
        ]
        
        # Act
        result = await service.get_active_config(group_id)
        
        # Assert
        assert result is not None
        assert result.backend_type == MemoryBackendType.DEFAULT
        assert result.custom_config == {"test": "value"}
    
    @pytest.mark.asyncio
    async def test_get_active_config_no_active_backends(self, service, mock_uow):
        """Test that None is returned when no active backends exist."""
        # Arrange
        group_id = "test-group-123"
        mock_uow.memory_backend_repository.get_by_group_id.return_value = []
        
        # Act
        result = await service.get_active_config(group_id)
        
        # Assert
        assert result is None
    
    @pytest.mark.asyncio
    async def test_get_active_config_system_default_fallback(
        self, service, mock_uow, databricks_backend
    ):
        """Test fallback to system-wide default when no group_id provided."""
        # Arrange
        databricks_backend.is_default = True
        databricks_backend.is_active = True
        
        mock_uow.memory_backend_repository.get_all.return_value = [databricks_backend]
        
        # Act
        result = await service.get_active_config(group_id=None)
        
        # Assert
        assert result is not None
        assert result.backend_type == MemoryBackendType.DATABRICKS
        assert isinstance(result.databricks_config, DatabricksMemoryConfig)
        mock_uow.memory_backend_repository.get_all.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_active_config_no_system_default(self, service, mock_uow):
        """Test that None is returned when no system default exists."""
        # Arrange
        mock_uow.memory_backend_repository.get_all.return_value = []
        
        # Act
        result = await service.get_active_config(group_id=None)
        
        # Assert
        assert result is None
    
    @pytest.mark.asyncio
    async def test_get_active_config_databricks_config_conversion(
        self, service, mock_uow, databricks_backend
    ):
        """Test proper conversion of databricks_config dict to DatabricksMemoryConfig."""
        # Arrange
        group_id = "test-group-123"
        mock_uow.memory_backend_repository.get_by_group_id.return_value = [databricks_backend]
        
        # Act
        result = await service.get_active_config(group_id)
        
        # Assert
        assert result.databricks_config is not None
        assert isinstance(result.databricks_config, DatabricksMemoryConfig)
        assert result.databricks_config.endpoint_name == "test-endpoint"
        assert result.databricks_config.short_term_index == "ml.agents.short_term"
        assert result.databricks_config.workspace_url == "https://test.databricks.com"
        assert result.databricks_config.embedding_dimension == 768
    
    @pytest.mark.asyncio
    async def test_get_active_config_multiple_databricks_most_recent(
        self, service, mock_uow
    ):
        """Test that most recent Databricks backend is selected when multiple exist."""
        # Arrange
        group_id = "test-group-123"
        older_databricks = MagicMock(
            backend_type=MemoryBackendType.DATABRICKS,
            is_active=True,
            updated_at=datetime.utcnow() - timedelta(days=5),
            name="Older Databricks",
            databricks_config={"endpoint_name": "old-endpoint"},
            enable_short_term=True,
            enable_long_term=True,
            enable_entity=True,
            custom_config=None
        )
        newer_databricks = MagicMock(
            backend_type=MemoryBackendType.DATABRICKS,
            is_active=True,
            updated_at=datetime.utcnow() - timedelta(days=1),
            name="Newer Databricks",
            databricks_config={
                "endpoint_name": "new-endpoint",
                "short_term_index": "new.index",
                "workspace_url": "https://new.databricks.com",
                "embedding_dimension": 1024
            },
            enable_short_term=False,
            enable_long_term=True,
            enable_entity=True,
            custom_config=None
        )
        
        mock_uow.memory_backend_repository.get_by_group_id.return_value = [
            older_databricks,
            newer_databricks
        ]
        
        # Act
        result = await service.get_active_config(group_id)
        
        # Assert
        assert result is not None
        assert result.backend_type == MemoryBackendType.DATABRICKS
        assert result.databricks_config.endpoint_name == "new-endpoint"
        assert result.enable_short_term is False
    
    @pytest.mark.asyncio
    async def test_get_active_config_preserves_custom_config(self, service, mock_uow):
        """Test that custom_config is preserved in the result."""
        # Arrange
        group_id = "test-group-123"
        backend_with_custom = MagicMock(
            backend_type=MemoryBackendType.DEFAULT,
            is_active=True,
            updated_at=datetime.utcnow(),
            databricks_config=None,
            enable_short_term=True,
            enable_long_term=True,
            enable_entity=True,
            custom_config={"key1": "value1", "key2": 123}
        )
        
        mock_uow.memory_backend_repository.get_by_group_id.return_value = [backend_with_custom]
        
        # Act
        result = await service.get_active_config(group_id)
        
        # Assert
        assert result is not None
        assert result.custom_config == {"key1": "value1", "key2": 123}