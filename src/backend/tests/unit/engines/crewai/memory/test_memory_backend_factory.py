"""
Unit tests for MemoryBackendFactory.
"""
import pytest
from unittest.mock import MagicMock, AsyncMock, patch

from src.engines.crewai.memory.memory_backend_factory import MemoryBackendFactory
from src.schemas.memory_backend import (
    MemoryBackendConfig,
    MemoryBackendType,
    DatabricksMemoryConfig,
    LakebaseMemoryConfig,
)


class TestCreateMemoryBackendsDefault:
    """Tests for default backend creation."""

    @pytest.mark.asyncio
    async def test_default_backend_returns_empty_dict(self):
        """Test that default backend returns empty dict for CrewAI to handle."""
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.DEFAULT,
            enable_short_term=True,
            enable_long_term=True,
            enable_entity=True,
        )
        result = await MemoryBackendFactory.create_memory_backends(
            config=config,
            crew_id="test_crew_123",
        )
        assert result == {}


class TestCreateMemoryBackendsLakebase:
    """Tests for Lakebase backend creation."""

    @pytest.mark.asyncio
    async def test_lakebase_missing_config_raises(self):
        """Test that missing lakebase_config raises ValueError."""
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.LAKEBASE,
            lakebase_config=None,
            enable_short_term=True,
        )
        with pytest.raises(ValueError, match="Lakebase configuration is required"):
            await MemoryBackendFactory.create_memory_backends(
                config=config,
                crew_id="test_crew_123",
            )

    @pytest.mark.asyncio
    async def test_lakebase_creates_all_memory_types(self):
        """Test that Lakebase backend creates all three memory types."""
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.LAKEBASE,
            lakebase_config=LakebaseMemoryConfig(
                embedding_dimension=1024,
                short_term_table="crew_short_term_memory",
                long_term_table="crew_long_term_memory",
                entity_table="crew_entity_memory",
            ),
            enable_short_term=True,
            enable_long_term=True,
            enable_entity=True,
        )
        mock_embedder = MagicMock()
        result = await MemoryBackendFactory.create_memory_backends(
            config=config,
            crew_id="test_group_crew_abc123",
            embedder=mock_embedder,
            job_id="job_001",
        )
        assert "short_term" in result
        assert "long_term" in result
        assert "entity" in result

    @pytest.mark.asyncio
    async def test_lakebase_respects_disabled_memory_types(self):
        """Test that disabled memory types are not created."""
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.LAKEBASE,
            lakebase_config=LakebaseMemoryConfig(),
            enable_short_term=True,
            enable_long_term=False,
            enable_entity=False,
        )
        result = await MemoryBackendFactory.create_memory_backends(
            config=config,
            crew_id="test_crew",
            embedder=MagicMock(),
        )
        assert "short_term" in result
        assert "long_term" not in result
        assert "entity" not in result

    @pytest.mark.asyncio
    async def test_lakebase_passes_job_id_to_short_term(self):
        """Test that job_id is passed to short-term storage for session scoping."""
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.LAKEBASE,
            lakebase_config=LakebaseMemoryConfig(),
            enable_short_term=True,
            enable_long_term=False,
            enable_entity=False,
        )
        result = await MemoryBackendFactory.create_memory_backends(
            config=config,
            crew_id="grp_crew_123",
            embedder=MagicMock(),
            job_id="job_42",
        )
        wrapper = result["short_term"]
        assert wrapper.storage.job_id == "job_42"

    @pytest.mark.asyncio
    async def test_lakebase_extracts_group_id_from_crew_id(self):
        """Test that group_id is extracted from crew_id format."""
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.LAKEBASE,
            lakebase_config=LakebaseMemoryConfig(),
            enable_short_term=True,
            enable_long_term=False,
            enable_entity=False,
        )
        result = await MemoryBackendFactory.create_memory_backends(
            config=config,
            crew_id="my_group_crew_abc123",
            embedder=MagicMock(),
        )
        wrapper = result["short_term"]
        assert wrapper.storage.group_id == "my_group"


class TestCreateMemoryBackendsDatabricks:
    """Tests for Databricks backend creation."""

    @pytest.mark.asyncio
    async def test_databricks_missing_config_raises(self):
        """Test that missing databricks_config raises ValueError."""
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.DATABRICKS,
            databricks_config=None,
        )
        with pytest.raises(ValueError, match="Databricks configuration is required"):
            await MemoryBackendFactory.create_memory_backends(
                config=config,
                crew_id="test_crew",
            )


class TestCreateEmbedderWrapper:
    """Tests for create_embedder_wrapper static method."""

    def test_creates_wrapper_with_embed_and_store(self):
        """Test that wrapper has embed_and_store method."""
        embedder = MagicMock()
        storage = MagicMock()
        wrapper = MemoryBackendFactory.create_embedder_wrapper(embedder, storage)
        assert hasattr(wrapper, "embed_and_store")
        assert hasattr(wrapper, "search")
        assert hasattr(wrapper, "reset")
