"""
Unit tests for MemoryBackendFactory.

Updated for the app-modes refactoring which replaced the per-memory-type
(short_term/long_term/entity) architecture with a single unified StorageBackend.

Key changes in the new API:
- MemoryBackendFactory.create_unified_storage() is the primary method
- create_memory_backends() is a legacy shim returning {"unified": backend}
- LakebaseMemoryConfig uses memory_table (not short_term/long_term/entity tables)
- MemoryBackendConfig no longer has enable_short_term/long_term/entity fields
- Databricks Vector Search memory is RETIRED: memory runs on Lakebase, and a
  ``databricks`` config degrades to the local store instead of building a
  VS-backed backend (the config row itself is still read by the knowledge
  services, so it cannot simply be rejected).
"""

from unittest.mock import MagicMock, patch

import pytest

from src.schemas.memory_backend import (
    DatabricksMemoryConfig,
    LakebaseMemoryConfig,
    MemoryBackendConfig,
    MemoryBackendType,
)
from src.services.memory.storage.factory import MemoryBackendFactory

# ─────────────────────────────────────────────────────────────────────────────
# Default backend — create_unified_storage
# ─────────────────────────────────────────────────────────────────────────────


class TestCreateUnifiedStorageDefault:
    """Tests for DEFAULT backend via create_unified_storage."""

    @pytest.mark.asyncio
    async def test_default_backend_returns_none(self):
        config = MemoryBackendConfig(backend_type=MemoryBackendType.DEFAULT)
        result = await MemoryBackendFactory.create_unified_storage(
            config=config, crew_id="test_crew_123", group_id="grp1"
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_default_backend_with_embedder_returns_none(self):
        config = MemoryBackendConfig(backend_type=MemoryBackendType.DEFAULT)
        result = await MemoryBackendFactory.create_unified_storage(
            config=config,
            crew_id="test_crew",
            group_id="grp1",
            embedder=MagicMock(),
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_unsupported_backend_type_returns_none(self):
        config = MemoryBackendConfig(backend_type=MemoryBackendType.DEFAULT)
        with patch.object(config, "backend_type", "some_unsupported_type"):
            result = await MemoryBackendFactory.create_unified_storage(
                config=config, crew_id="test_crew", group_id="grp1"
            )
        assert result is None


# ─────────────────────────────────────────────────────────────────────────────
# Legacy shim — create_memory_backends
# ─────────────────────────────────────────────────────────────────────────────


class TestCreateMemoryBackendsDefault:
    """Tests for DEFAULT backend via legacy create_memory_backends shim."""

    @pytest.mark.asyncio
    async def test_default_backend_returns_empty_dict(self):
        config = MemoryBackendConfig(backend_type=MemoryBackendType.DEFAULT)
        result = await MemoryBackendFactory.create_memory_backends(
            config=config, crew_id="test_crew_123"
        )
        assert result == {}

    @pytest.mark.asyncio
    async def test_default_backend_with_embedder_returns_empty_dict(self):
        config = MemoryBackendConfig(backend_type=MemoryBackendType.DEFAULT)
        result = await MemoryBackendFactory.create_memory_backends(
            config=config, crew_id="test_crew", embedder=MagicMock()
        )
        assert result == {}

    @pytest.mark.asyncio
    async def test_unsupported_backend_type_returns_empty_dict(self):
        config = MemoryBackendConfig(backend_type=MemoryBackendType.DEFAULT)
        with patch.object(config, "backend_type", "some_unsupported_type"):
            result = await MemoryBackendFactory.create_memory_backends(
                config=config, crew_id="test_crew"
            )
        assert result == {}


# ─────────────────────────────────────────────────────────────────────────────
# Lakebase backend — create_unified_storage
# ─────────────────────────────────────────────────────────────────────────────


class TestCreateUnifiedStorageLakebase:
    """Tests for LAKEBASE backend via create_unified_storage."""

    @pytest.mark.asyncio
    async def test_lakebase_missing_config_raises(self):
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.LAKEBASE,
            lakebase_config=None,
        )
        with pytest.raises(ValueError, match="Lakebase configuration is required"):
            await MemoryBackendFactory.create_unified_storage(
                config=config, crew_id="crew", group_id="grp1"
            )

    @pytest.mark.asyncio
    async def test_lakebase_missing_table_raises(self):
        """Empty memory_table is rejected at schema validation time
        (identifier validation moved into LakebaseMemoryConfig)."""
        with pytest.raises(ValueError, match="memory_table"):
            LakebaseMemoryConfig(memory_table="")

    @pytest.mark.asyncio
    async def test_lakebase_returns_backend_instance(self):
        """create_unified_storage returns a LakebaseStorageBackend when configured."""
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.LAKEBASE,
            lakebase_config=LakebaseMemoryConfig(memory_table="crew_memory"),
        )
        mock_backend = MagicMock()
        mock_lakebase_cls = MagicMock(return_value=mock_backend)

        with patch.dict(
            "sys.modules",
            {
                "src.services.memory.storage.lakebase": MagicMock(
                    LakebaseStorageBackend=mock_lakebase_cls
                )
            },
        ):
            result = await MemoryBackendFactory.create_unified_storage(
                config=config,
                crew_id="test_crew",
                group_id="grp1",
                embedder=MagicMock(),
                job_id="job_001",
            )

        assert result is mock_backend


class TestCreateMemoryBackendsLakebase:
    """Tests for LAKEBASE backend via legacy create_memory_backends shim."""

    @pytest.mark.asyncio
    async def test_lakebase_missing_config_raises(self):
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.LAKEBASE,
            lakebase_config=None,
        )
        with pytest.raises(ValueError, match="Lakebase configuration is required"):
            await MemoryBackendFactory.create_memory_backends(
                config=config, crew_id="test_crew_123"
            )

    @pytest.mark.asyncio
    async def test_lakebase_returns_unified_key(self):
        """Legacy shim wraps unified backend under 'unified' key."""
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.LAKEBASE,
            lakebase_config=LakebaseMemoryConfig(memory_table="crew_memory"),
        )
        mock_backend = MagicMock()
        mock_lakebase_cls = MagicMock(return_value=mock_backend)

        with patch.dict(
            "sys.modules",
            {
                "src.services.memory.storage.lakebase": MagicMock(
                    LakebaseStorageBackend=mock_lakebase_cls
                )
            },
        ):
            result = await MemoryBackendFactory.create_memory_backends(
                config=config,
                crew_id="test_group_crew_abc123",
                embedder=MagicMock(),
                job_id="job_001",
            )

        assert "unified" in result
        assert result["unified"] is mock_backend

    @pytest.mark.asyncio
    async def test_lakebase_extracts_group_id_from_crew_id(self):
        """Legacy shim extracts group_id from crew_id using the _crew_ separator."""
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.LAKEBASE,
            lakebase_config=LakebaseMemoryConfig(memory_table="crew_memory"),
        )
        captured_kwargs = {}

        def capture(**kwargs):
            captured_kwargs.update(kwargs)
            return MagicMock()

        with patch.dict(
            "sys.modules",
            {
                "src.services.memory.storage.lakebase": MagicMock(
                    LakebaseStorageBackend=MagicMock(side_effect=capture)
                )
            },
        ):
            await MemoryBackendFactory.create_memory_backends(
                config=config, crew_id="my_group_crew_abc123", embedder=MagicMock()
            )

        assert captured_kwargs.get("group_id") == "my_group"

    @pytest.mark.asyncio
    async def test_lakebase_no_group_id_when_crew_id_no_underscore_pattern(self):
        """When crew_id has no _crew_ pattern, group_id is empty string."""
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.LAKEBASE,
            lakebase_config=LakebaseMemoryConfig(memory_table="crew_memory"),
        )
        mock_backend = MagicMock()

        with patch.dict(
            "sys.modules",
            {
                "src.services.memory.storage.lakebase": MagicMock(
                    LakebaseStorageBackend=MagicMock(return_value=mock_backend)
                )
            },
        ):
            result = await MemoryBackendFactory.create_memory_backends(
                config=config,
                crew_id="simple_crew_id_without_pattern",
                embedder=MagicMock(),
            )

        assert "unified" in result


# ─────────────────────────────────────────────────────────────────────────────
# Databricks Vector Search memory — retired
# ─────────────────────────────────────────────────────────────────────────────


class TestDatabricksBackendIsRetired:
    """A ``databricks`` memory config must degrade, not build a VS backend."""

    @pytest.fixture
    def databricks_config(self):
        return MemoryBackendConfig(
            backend_type=MemoryBackendType.DATABRICKS,
            databricks_config=DatabricksMemoryConfig(
                memory_index="catalog.schema.memory_index",
                workspace_url="https://example.com",
                endpoint_name="test-endpoint",
                embedding_dimension=1024,
            ),
        )

    @pytest.mark.asyncio
    async def test_returns_none_so_the_local_store_is_used(self, databricks_config):
        result = await MemoryBackendFactory.create_unified_storage(
            config=databricks_config, crew_id="g1_crew_abc", group_id="g1"
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_legacy_shim_returns_empty_dict(self, databricks_config):
        result = await MemoryBackendFactory.create_memory_backends(
            config=databricks_config, crew_id="g1_crew_abc"
        )
        assert result == {}
