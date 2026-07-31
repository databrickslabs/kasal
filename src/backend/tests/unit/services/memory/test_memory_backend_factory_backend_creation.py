"""
Extended tests for memory_backend_factory.py — targeting uncovered branches.

Updated for app-modes refactoring:
- create_memory_backends returns {"unified": backend} or {}
- LakebaseMemoryConfig uses memory_table (not short_term/long_term/entity tables)
- create_embedder_wrapper was removed (no longer in API)
- Databricks Vector Search memory is retired; Lakebase is the only remote
  memory backend, so only its cases live here now.
"""

import sys
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.schemas.memory_backend import (
    DatabricksMemoryConfig,
    LakebaseMemoryConfig,
    MemoryBackendConfig,
    MemoryBackendType,
)
from src.services.memory.backend_factory import MemoryBackendFactory

# ─── create_memory_backends LAKEBASE extra cases ──────────────────────────────


class TestLakebaseBackendCases:

    @pytest.mark.asyncio
    async def test_lakebase_without_instance_name(self):
        """Lakebase backend works without instance_name (defaults to None)."""
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.LAKEBASE,
            lakebase_config=LakebaseMemoryConfig(
                memory_table="crew_memory",
                # instance_name not provided → defaults to None
            ),
        )
        mock_backend = MagicMock()
        captured_kwargs = {}

        def capture(**kwargs):
            captured_kwargs.update(kwargs)
            return mock_backend

        with patch.dict(
            "sys.modules",
            {
                "src.services.memory.lakebase_storage_backend": MagicMock(
                    LakebaseStorageBackend=MagicMock(side_effect=capture)
                )
            },
        ):
            result = await MemoryBackendFactory.create_memory_backends(
                config=config, crew_id="grp_crew_abc123"
            )

        assert "unified" in result
        assert captured_kwargs.get("instance_name") is None

    @pytest.mark.asyncio
    async def test_lakebase_with_instance_name(self):
        """instance_name is forwarded to LakebaseStorageBackend."""
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.LAKEBASE,
            lakebase_config=LakebaseMemoryConfig(
                memory_table="crew_memory",
                instance_name="my-lakebase-instance",
            ),
        )
        captured_kwargs = {}

        def capture(**kwargs):
            captured_kwargs.update(kwargs)
            return MagicMock()

        with patch.dict(
            "sys.modules",
            {
                "src.services.memory.lakebase_storage_backend": MagicMock(
                    LakebaseStorageBackend=MagicMock(side_effect=capture)
                )
            },
        ):
            await MemoryBackendFactory.create_memory_backends(
                config=config, crew_id="grp_crew_abc123"
            )

        assert captured_kwargs.get("instance_name") == "my-lakebase-instance"

    @pytest.mark.asyncio
    async def test_lakebase_embedding_dimension_forwarded(self):
        """embedding_dimension is forwarded to LakebaseStorageBackend."""
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.LAKEBASE,
            lakebase_config=LakebaseMemoryConfig(
                memory_table="crew_memory",
                embedding_dimension=768,
            ),
        )
        captured_kwargs = {}

        def capture(**kwargs):
            captured_kwargs.update(kwargs)
            return MagicMock()

        with patch.dict(
            "sys.modules",
            {
                "src.services.memory.lakebase_storage_backend": MagicMock(
                    LakebaseStorageBackend=MagicMock(side_effect=capture)
                )
            },
        ):
            await MemoryBackendFactory.create_memory_backends(
                config=config, crew_id="grp_crew_abc123"
            )

        assert captured_kwargs.get("embedding_dimension") == 768

    @pytest.mark.asyncio
    async def test_lakebase_empty_table_name_raises(self):
        """Empty memory_table is rejected at schema validation time
        (identifier validation moved into LakebaseMemoryConfig)."""
        with pytest.raises(ValueError, match="memory_table"):
            LakebaseMemoryConfig(memory_table="")
