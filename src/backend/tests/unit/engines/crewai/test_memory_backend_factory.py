"""
Comprehensive tests for memory_backend_factory.py

Tests cover:
- DatabricksIndexValidationError exception
- _validate_databricks_indexes method
- Index state validation (READY, PROVISIONING, MISSING)
- create_memory_backends with validation
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from typing import Dict, Any

from src.engines.crewai.memory.memory_backend_factory import (
    MemoryBackendFactory,
    DatabricksIndexValidationError
)
from src.schemas.memory_backend import (
    MemoryBackendConfig,
    MemoryBackendType,
    DatabricksMemoryConfig
)

# Correct path for patching - the import happens inside the method
REPO_PATCH_PATH = 'src.repositories.databricks_vector_index_repository.DatabricksVectorIndexRepository'


class TestDatabricksIndexValidationError:
    """Tests for the DatabricksIndexValidationError exception class."""

    def test_exception_creation_with_missing_indexes(self):
        """Test exception creation with missing indexes error type."""
        validation_result = {
            "valid": False,
            "valid_indexes": [],
            "missing_indexes": ["short_term: catalog.schema.stm_index"],
            "provisioning_indexes": [],
            "error_message": "Indexes not found",
            "error_type": "missing_indexes"
        }

        error = DatabricksIndexValidationError("Test error", validation_result)

        assert str(error) == "Test error"
        assert error.error_type == "missing_indexes"
        assert error.missing_indexes == ["short_term: catalog.schema.stm_index"]
        assert error.provisioning_indexes == []
        assert error.validation_result == validation_result

    def test_exception_creation_with_provisioning_indexes(self):
        """Test exception creation with provisioning indexes error type."""
        validation_result = {
            "valid": False,
            "valid_indexes": [],
            "missing_indexes": [],
            "provisioning_indexes": ["entity: catalog.schema.entity_index (state: PROVISIONING)"],
            "error_message": "Indexes provisioning",
            "error_type": "provisioning_indexes"
        }

        error = DatabricksIndexValidationError("Provisioning error", validation_result)

        assert error.error_type == "provisioning_indexes"
        assert error.missing_indexes == []
        assert error.provisioning_indexes == ["entity: catalog.schema.entity_index (state: PROVISIONING)"]

    def test_exception_creation_with_unknown_error_type(self):
        """Test exception creation with unknown error type defaults correctly."""
        validation_result = {
            "valid": False,
            "error_message": "Unknown error"
        }

        error = DatabricksIndexValidationError("Unknown", validation_result)

        assert error.error_type == "unknown"
        assert error.missing_indexes == []
        assert error.provisioning_indexes == []


class TestValidateDatabricksIndexes:
    """Tests for the _validate_databricks_indexes static method."""

    @pytest.fixture
    def databricks_config(self):
        """Create a standard Databricks memory config for testing."""
        return DatabricksMemoryConfig(
            workspace_url="https://example.databricks.com",
            endpoint_name="test-endpoint",
            short_term_index="catalog.schema.stm_index",
            long_term_index="catalog.schema.ltm_index",
            entity_index="catalog.schema.entity_index",
            embedding_dimension=1024
        )

    @pytest.fixture
    def memory_config(self, databricks_config):
        """Create a memory backend config with Databricks enabled."""
        return MemoryBackendConfig(
            backend_type=MemoryBackendType.DATABRICKS,
            databricks_config=databricks_config,
            enable_short_term=True,
            enable_long_term=True,
            enable_entity=True
        )

    @pytest.mark.asyncio
    async def test_validate_returns_empty_when_no_databricks_config(self):
        """Test validation returns success when no databricks config."""
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.DEFAULT
        )

        all_valid, valid, missing, provisioning = await MemoryBackendFactory._validate_databricks_indexes(config)

        assert all_valid is True
        assert valid == []
        assert missing == []
        assert provisioning == []

    @pytest.mark.asyncio
    async def test_validate_all_indexes_ready(self, memory_config):
        """Test validation when all indexes are ready."""
        mock_describe_result = {
            "success": True,
            "description": {
                "status": {
                    "state": "ONLINE",
                    "ready": True
                }
            }
        }

        with patch('src.repositories.databricks_vector_index_repository.DatabricksVectorIndexRepository') as MockRepo:
            mock_repo_instance = MagicMock()
            mock_repo_instance.describe_index = AsyncMock(return_value=mock_describe_result)
            MockRepo.return_value = mock_repo_instance

            all_valid, valid, missing, provisioning = await MemoryBackendFactory._validate_databricks_indexes(
                memory_config
            )

        assert all_valid is True
        assert len(valid) == 3
        assert missing == []
        assert provisioning == []

    @pytest.mark.asyncio
    async def test_validate_indexes_missing(self, memory_config):
        """Test validation when indexes are missing/not found."""
        mock_describe_result = {
            "success": False,
            "error": "Index not found"
        }

        with patch('src.repositories.databricks_vector_index_repository.DatabricksVectorIndexRepository') as MockRepo:
            mock_repo_instance = MagicMock()
            mock_repo_instance.describe_index = AsyncMock(return_value=mock_describe_result)
            MockRepo.return_value = mock_repo_instance

            all_valid, valid, missing, provisioning = await MemoryBackendFactory._validate_databricks_indexes(
                memory_config
            )

        assert all_valid is False
        assert valid == []
        assert len(missing) == 3
        assert provisioning == []
        # Check that each missing index contains the error message
        for m in missing:
            assert "Index not found" in m

    @pytest.mark.asyncio
    async def test_validate_indexes_provisioning(self, memory_config):
        """Test validation when indexes are still provisioning."""
        mock_describe_result = {
            "success": True,
            "description": {
                "status": {
                    "state": "PROVISIONING",
                    "ready": False
                }
            }
        }

        with patch('src.repositories.databricks_vector_index_repository.DatabricksVectorIndexRepository') as MockRepo:
            mock_repo_instance = MagicMock()
            mock_repo_instance.describe_index = AsyncMock(return_value=mock_describe_result)
            MockRepo.return_value = mock_repo_instance

            all_valid, valid, missing, provisioning = await MemoryBackendFactory._validate_databricks_indexes(
                memory_config
            )

        assert all_valid is False
        assert valid == []
        assert missing == []
        assert len(provisioning) == 3
        # Check that each provisioning index contains the state
        for p in provisioning:
            assert "PROVISIONING" in p

    @pytest.mark.asyncio
    async def test_validate_mixed_states(self, memory_config):
        """Test validation with mixed index states (some ready, some provisioning, some missing)."""

        async def mock_describe_index(index_name, endpoint_name, user_token=None):
            if "stm" in index_name:
                return {"success": True, "description": {"status": {"state": "ONLINE", "ready": True}}}
            elif "ltm" in index_name:
                return {"success": True, "description": {"status": {"state": "PROVISIONING", "ready": False}}}
            else:  # entity
                return {"success": False, "error": "Index not found"}

        with patch('src.repositories.databricks_vector_index_repository.DatabricksVectorIndexRepository') as MockRepo:
            mock_repo_instance = MagicMock()
            mock_repo_instance.describe_index = AsyncMock(side_effect=mock_describe_index)
            MockRepo.return_value = mock_repo_instance

            all_valid, valid, missing, provisioning = await MemoryBackendFactory._validate_databricks_indexes(
                memory_config
            )

        assert all_valid is False
        assert len(valid) == 1  # Only short-term is ready
        assert len(missing) == 1  # Entity is missing
        assert len(provisioning) == 1  # Long-term is provisioning

    @pytest.mark.asyncio
    async def test_validate_handles_exception(self, memory_config):
        """Test validation handles exceptions gracefully."""
        with patch('src.repositories.databricks_vector_index_repository.DatabricksVectorIndexRepository') as MockRepo:
            mock_repo_instance = MagicMock()
            mock_repo_instance.describe_index = AsyncMock(side_effect=Exception("Connection error"))
            MockRepo.return_value = mock_repo_instance

            all_valid, valid, missing, provisioning = await MemoryBackendFactory._validate_databricks_indexes(
                memory_config
            )

        assert all_valid is False
        assert valid == []
        assert len(missing) == 3  # All indexes marked as missing due to exception

    @pytest.mark.asyncio
    async def test_validate_respects_enabled_flags(self):
        """Test validation only checks indexes that are enabled."""
        databricks_config = DatabricksMemoryConfig(
            workspace_url="https://example.databricks.com",
            endpoint_name="test-endpoint",
            short_term_index="catalog.schema.stm_index",
            long_term_index="catalog.schema.ltm_index",
            entity_index="catalog.schema.entity_index",
            embedding_dimension=1024
        )

        # Only enable short_term
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.DATABRICKS,
            databricks_config=databricks_config,
            enable_short_term=True,
            enable_long_term=False,
            enable_entity=False
        )

        mock_describe_result = {
            "success": True,
            "description": {"status": {"state": "ONLINE", "ready": True}}
        }

        with patch('src.repositories.databricks_vector_index_repository.DatabricksVectorIndexRepository') as MockRepo:
            mock_repo_instance = MagicMock()
            mock_repo_instance.describe_index = AsyncMock(return_value=mock_describe_result)
            MockRepo.return_value = mock_repo_instance

            all_valid, valid, missing, provisioning = await MemoryBackendFactory._validate_databricks_indexes(
                config
            )

        assert all_valid is True
        assert len(valid) == 1  # Only short-term checked
        assert "short_term" in valid[0]


class TestCreateMemoryBackendsValidation:
    """Tests for create_memory_backends with index validation."""

    @pytest.fixture
    def databricks_config(self):
        """Create a standard Databricks memory config for testing."""
        return DatabricksMemoryConfig(
            workspace_url="https://example.databricks.com",
            endpoint_name="test-endpoint",
            short_term_index="catalog.schema.stm_index",
            embedding_dimension=1024
        )

    @pytest.fixture
    def memory_config(self, databricks_config):
        """Create a memory backend config with Databricks enabled."""
        return MemoryBackendConfig(
            backend_type=MemoryBackendType.DATABRICKS,
            databricks_config=databricks_config,
            enable_short_term=True,
            enable_long_term=False,
            enable_entity=False
        )

    @pytest.mark.asyncio
    async def test_create_raises_error_on_missing_indexes(self, memory_config):
        """Test that create_memory_backends raises DatabricksIndexValidationError for missing indexes."""
        mock_describe_result = {
            "success": False,
            "error": "Index not found"
        }

        with patch('src.repositories.databricks_vector_index_repository.DatabricksVectorIndexRepository') as MockRepo:
            mock_repo_instance = MagicMock()
            mock_repo_instance.describe_index = AsyncMock(return_value=mock_describe_result)
            MockRepo.return_value = mock_repo_instance

            with pytest.raises(DatabricksIndexValidationError) as exc_info:
                await MemoryBackendFactory.create_memory_backends(
                    config=memory_config,
                    crew_id="test_crew_123",
                    embedder=None
                )

            assert exc_info.value.error_type == "missing_indexes"
            assert len(exc_info.value.missing_indexes) == 1

    @pytest.mark.asyncio
    async def test_create_raises_error_on_provisioning_indexes(self, memory_config):
        """Test that create_memory_backends raises DatabricksIndexValidationError for provisioning indexes."""
        mock_describe_result = {
            "success": True,
            "description": {"status": {"state": "PROVISIONING", "ready": False}}
        }

        with patch('src.repositories.databricks_vector_index_repository.DatabricksVectorIndexRepository') as MockRepo:
            mock_repo_instance = MagicMock()
            mock_repo_instance.describe_index = AsyncMock(return_value=mock_describe_result)
            MockRepo.return_value = mock_repo_instance

            with pytest.raises(DatabricksIndexValidationError) as exc_info:
                await MemoryBackendFactory.create_memory_backends(
                    config=memory_config,
                    crew_id="test_crew_123",
                    embedder=None
                )

            assert exc_info.value.error_type == "provisioning_indexes"
            assert len(exc_info.value.provisioning_indexes) == 1

    @pytest.mark.asyncio
    async def test_create_succeeds_with_ready_indexes(self, memory_config):
        """Test that create_memory_backends succeeds when all indexes are ready."""
        mock_describe_result = {
            "success": True,
            "description": {"status": {"state": "ONLINE", "ready": True}}
        }

        with patch('src.repositories.databricks_vector_index_repository.DatabricksVectorIndexRepository') as MockRepo:
            mock_repo_instance = MagicMock()
            mock_repo_instance.describe_index = AsyncMock(return_value=mock_describe_result)
            MockRepo.return_value = mock_repo_instance

            with patch('src.engines.crewai.memory.databricks_vector_storage.DatabricksVectorStorage') as MockStorage:
                with patch('src.engines.crewai.memory.crewai_databricks_wrapper.CrewAIDatabricksWrapper') as MockWrapper:
                    MockWrapper.return_value = MagicMock()

                    result = await MemoryBackendFactory.create_memory_backends(
                        config=memory_config,
                        crew_id="test_crew_123",
                        embedder=None
                    )

                    assert 'short_term' in result

    @pytest.mark.asyncio
    async def test_create_default_backend_skips_validation(self):
        """Test that DEFAULT backend type skips Databricks validation."""
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.DEFAULT,
            enable_short_term=True
        )

        # Should not raise any errors - DEFAULT backend doesn't validate Databricks indexes
        result = await MemoryBackendFactory.create_memory_backends(
            config=config,
            crew_id="test_crew_123",
            embedder=None
        )

        assert result == {}  # Default backend returns empty dict


class TestIndexStateDetection:
    """Tests for different index state detection patterns."""

    @pytest.fixture
    def memory_config(self):
        """Create a memory config with only short-term enabled."""
        databricks_config = DatabricksMemoryConfig(
            workspace_url="https://example.databricks.com",
            endpoint_name="test-endpoint",
            short_term_index="catalog.schema.stm_index",
            embedding_dimension=1024
        )
        return MemoryBackendConfig(
            backend_type=MemoryBackendType.DATABRICKS,
            databricks_config=databricks_config,
            enable_short_term=True,
            enable_long_term=False,
            enable_entity=False
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("state,ready,expected_valid", [
        ("ONLINE", True, True),
        ("READY", True, True),
        ("ONLINE", False, True),  # State ONLINE takes precedence
        ("READY", False, True),   # State READY takes precedence
        ("PROVISIONING", False, False),
        ("PENDING", False, False),
        ("CREATING", False, False),
        ("UNKNOWN", False, False),
        ("ERROR", False, False),
    ])
    async def test_various_index_states(self, memory_config, state, ready, expected_valid):
        """Test detection of various index states."""
        mock_describe_result = {
            "success": True,
            "description": {
                "status": {
                    "state": state,
                    "ready": ready
                }
            }
        }

        with patch('src.repositories.databricks_vector_index_repository.DatabricksVectorIndexRepository') as MockRepo:
            mock_repo_instance = MagicMock()
            mock_repo_instance.describe_index = AsyncMock(return_value=mock_describe_result)
            MockRepo.return_value = mock_repo_instance

            all_valid, valid, missing, provisioning = await MemoryBackendFactory._validate_databricks_indexes(
                memory_config
            )

        assert all_valid == expected_valid
        if expected_valid:
            assert len(valid) == 1
        else:
            assert len(valid) == 0
            assert len(provisioning) == 1
