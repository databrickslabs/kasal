"""
Unit tests for MemoryBackendFactory.

Updated for the app-modes refactoring which replaced the per-memory-type
(short_term/long_term/entity) architecture with a single unified StorageBackend.

Key changes in the new API:
- MemoryBackendFactory.create_unified_storage() is the primary method
- create_memory_backends() is a legacy shim returning {"unified": backend}
- _validate_databricks_index() validates a single index (not multiple)
- DatabricksMemoryConfig requires memory_index (not short_term/long_term/entity indexes)
- LakebaseMemoryConfig uses memory_table (not short_term/long_term/entity tables)
- MemoryBackendConfig no longer has enable_short_term/long_term/entity fields
"""
import pytest
from unittest.mock import MagicMock, AsyncMock, patch

from src.engines.crewai.memory.memory_backend_factory import (
    MemoryBackendFactory,
    DatabricksIndexValidationError,
)
from src.schemas.memory_backend import (
    MemoryBackendConfig,
    MemoryBackendType,
    DatabricksMemoryConfig,
    LakebaseMemoryConfig,
)


# ─────────────────────────────────────────────────────────────────────────────
# DatabricksIndexValidationError
# ─────────────────────────────────────────────────────────────────────────────


class TestDatabricksIndexValidationError:
    """Tests for the custom exception class."""

    def test_message_is_preserved(self):
        err = DatabricksIndexValidationError(
            "some error", {"error_type": "missing_indexes", "missing_indexes": ["a"]}
        )
        assert str(err) == "some error"

    def test_validation_result_stored(self):
        vr = {"error_type": "provisioning_indexes", "missing_indexes": [], "provisioning_indexes": ["b"]}
        err = DatabricksIndexValidationError("msg", vr)
        assert err.validation_result is vr

    def test_error_type_extracted(self):
        err = DatabricksIndexValidationError(
            "msg", {"error_type": "missing_indexes", "missing_indexes": []}
        )
        assert err.error_type == "missing_indexes"

    def test_missing_indexes_extracted(self):
        err = DatabricksIndexValidationError(
            "msg", {"missing_indexes": ["idx1", "idx2"], "error_type": "x"}
        )
        assert err.missing_indexes == ["idx1", "idx2"]

    def test_provisioning_indexes_extracted(self):
        err = DatabricksIndexValidationError(
            "msg", {"provisioning_indexes": ["p1"], "error_type": "x"}
        )
        assert err.provisioning_indexes == ["p1"]

    def test_unknown_error_type_default(self):
        err = DatabricksIndexValidationError("msg", {})
        assert err.error_type == "unknown"


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
                "src.engines.crewai.memory.lakebase_storage_backend": MagicMock(
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
                "src.engines.crewai.memory.lakebase_storage_backend": MagicMock(
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
                "src.engines.crewai.memory.lakebase_storage_backend": MagicMock(
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
                "src.engines.crewai.memory.lakebase_storage_backend": MagicMock(
                    LakebaseStorageBackend=MagicMock(return_value=mock_backend)
                )
            },
        ):
            result = await MemoryBackendFactory.create_memory_backends(
                config=config, crew_id="simple_crew_id_without_pattern", embedder=MagicMock()
            )

        assert "unified" in result


# ─────────────────────────────────────────────────────────────────────────────
# Databricks backend
# ─────────────────────────────────────────────────────────────────────────────


class TestCreateMemoryBackendsDatabricks:
    """Tests for DATABRICKS backend creation."""

    @pytest.mark.asyncio
    async def test_databricks_missing_config_raises(self):
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.DATABRICKS,
            databricks_config=None,
        )
        with pytest.raises(ValueError, match="Databricks configuration is required"):
            await MemoryBackendFactory.create_memory_backends(
                config=config, crew_id="test_crew"
            )

    @pytest.mark.asyncio
    async def test_databricks_raises_when_index_missing(self):
        """Index not found raises DatabricksIndexValidationError."""
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.DATABRICKS,
            databricks_config=DatabricksMemoryConfig(
                endpoint_name="ep",
                memory_index="catalog.schema.unified",
                workspace_url="https://example.databricks.com",
            ),
        )
        mock_repo = MagicMock()
        mock_repo.describe_index = AsyncMock(
            return_value={"success": False, "error": "Index not found"}
        )

        with patch.dict(
            "sys.modules",
            {
                "src.repositories.databricks_vector_index_repository": MagicMock(
                    DatabricksVectorIndexRepository=MagicMock(return_value=mock_repo)
                )
            },
        ):
            with pytest.raises(DatabricksIndexValidationError) as exc_info:
                await MemoryBackendFactory.create_memory_backends(
                    config=config, crew_id="test_crew"
                )
        assert exc_info.value.error_type == "missing_index"

    @pytest.mark.asyncio
    async def test_databricks_raises_when_index_provisioning(self):
        """Provisioning index raises DatabricksIndexValidationError."""
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.DATABRICKS,
            databricks_config=DatabricksMemoryConfig(
                endpoint_name="ep",
                memory_index="catalog.schema.unified",
                workspace_url="https://example.databricks.com",
            ),
        )
        mock_repo = MagicMock()
        mock_repo.describe_index = AsyncMock(
            return_value={
                "success": True,
                "description": {"status": {"state": "PROVISIONING", "ready": False}},
            }
        )

        with patch.dict(
            "sys.modules",
            {
                "src.repositories.databricks_vector_index_repository": MagicMock(
                    DatabricksVectorIndexRepository=MagicMock(return_value=mock_repo)
                )
            },
        ):
            with pytest.raises(DatabricksIndexValidationError) as exc_info:
                await MemoryBackendFactory.create_memory_backends(
                    config=config, crew_id="crew"
                )
        assert exc_info.value.error_type == "provisioning_indexes"

    @pytest.mark.asyncio
    async def test_databricks_creates_backend_when_index_ready(self):
        """Ready index creates DatabricksStorageBackend."""
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.DATABRICKS,
            databricks_config=DatabricksMemoryConfig(
                endpoint_name="ep",
                memory_index="catalog.schema.unified",
                workspace_url="https://example.databricks.com",
            ),
        )
        mock_repo = MagicMock()
        mock_repo.describe_index = AsyncMock(
            return_value={
                "success": True,
                "description": {"status": {"state": "ONLINE", "ready": True}},
            }
        )
        mock_backend = MagicMock()
        mock_backend_cls = MagicMock(return_value=mock_backend)

        with patch.dict(
            "sys.modules",
            {
                "src.repositories.databricks_vector_index_repository": MagicMock(
                    DatabricksVectorIndexRepository=MagicMock(return_value=mock_repo)
                ),
                "src.engines.crewai.memory.databricks_storage_backend": MagicMock(
                    DatabricksStorageBackend=mock_backend_cls
                ),
            },
        ):
            result = await MemoryBackendFactory.create_memory_backends(
                config=config, crew_id="test_crew"
            )

        assert "unified" in result
        assert result["unified"] is mock_backend

    @pytest.mark.asyncio
    async def test_databricks_passes_job_id(self):
        """job_id is passed to DatabricksStorageBackend as session_id."""
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.DATABRICKS,
            databricks_config=DatabricksMemoryConfig(
                endpoint_name="ep",
                memory_index="catalog.schema.unified",
                workspace_url="https://example.databricks.com",
            ),
        )
        captured_kwargs = {}

        def capture(**kwargs):
            captured_kwargs.update(kwargs)
            return MagicMock()

        mock_repo = MagicMock()
        mock_repo.describe_index = AsyncMock(
            return_value={
                "success": True,
                "description": {"status": {"state": "ONLINE", "ready": True}},
            }
        )

        with patch.dict(
            "sys.modules",
            {
                "src.repositories.databricks_vector_index_repository": MagicMock(
                    DatabricksVectorIndexRepository=MagicMock(return_value=mock_repo)
                ),
                "src.engines.crewai.memory.databricks_storage_backend": MagicMock(
                    DatabricksStorageBackend=MagicMock(side_effect=capture)
                ),
            },
        ):
            await MemoryBackendFactory.create_memory_backends(
                config=config, crew_id="group_crew_abc", job_id="job_session_99"
            )

        assert captured_kwargs.get("session_id") == "job_session_99"

    @pytest.mark.asyncio
    async def test_databricks_extracts_group_id_from_crew_id(self):
        """group_id is extracted from crew_id using the _crew_ pattern."""
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.DATABRICKS,
            databricks_config=DatabricksMemoryConfig(
                endpoint_name="ep",
                memory_index="catalog.schema.unified",
                workspace_url="https://example.databricks.com",
            ),
        )
        captured_kwargs = {}

        def capture(**kwargs):
            captured_kwargs.update(kwargs)
            return MagicMock()

        mock_repo = MagicMock()
        mock_repo.describe_index = AsyncMock(
            return_value={
                "success": True,
                "description": {"status": {"state": "ONLINE", "ready": True}},
            }
        )

        with patch.dict(
            "sys.modules",
            {
                "src.repositories.databricks_vector_index_repository": MagicMock(
                    DatabricksVectorIndexRepository=MagicMock(return_value=mock_repo)
                ),
                "src.engines.crewai.memory.databricks_storage_backend": MagicMock(
                    DatabricksStorageBackend=MagicMock(side_effect=capture)
                ),
            },
        ):
            await MemoryBackendFactory.create_memory_backends(
                config=config, crew_id="grp_alpha_crew_xyz"
            )

        assert captured_kwargs.get("group_id") == "grp_alpha"


# ─────────────────────────────────────────────────────────────────────────────
# _validate_databricks_index (single index)
# ─────────────────────────────────────────────────────────────────────────────


class TestValidateDatabricksIndex:
    """Tests for the internal _validate_databricks_index method."""

    @pytest.mark.asyncio
    async def test_skips_when_no_workspace_url(self):
        """No workspace_url means validation is skipped (no raise)."""
        await MemoryBackendFactory._validate_databricks_index(
            workspace_url=None,
            endpoint_name="ep",
            index_name="catalog.schema.mem",
            user_token=None,
            group_id=None,
        )

    @pytest.mark.asyncio
    async def test_skips_when_no_index_name(self):
        """Empty index_name means validation is skipped (no raise)."""
        await MemoryBackendFactory._validate_databricks_index(
            workspace_url="https://example.databricks.com",
            endpoint_name="ep",
            index_name="",
            user_token=None,
            group_id=None,
        )

    @pytest.mark.asyncio
    async def test_passes_when_index_is_online(self):
        mock_repo = MagicMock()
        mock_repo.describe_index = AsyncMock(
            return_value={
                "success": True,
                "description": {"status": {"state": "ONLINE", "ready": True}},
            }
        )

        with patch.dict(
            "sys.modules",
            {
                "src.repositories.databricks_vector_index_repository": MagicMock(
                    DatabricksVectorIndexRepository=MagicMock(return_value=mock_repo)
                )
            },
        ):
            # Should not raise
            await MemoryBackendFactory._validate_databricks_index(
                workspace_url="https://example.databricks.com",
                endpoint_name="ep",
                index_name="catalog.schema.mem",
                user_token=None,
                group_id=None,
            )

    @pytest.mark.asyncio
    async def test_raises_when_index_not_found(self):
        mock_repo = MagicMock()
        mock_repo.describe_index = AsyncMock(
            return_value={"success": False, "message": "Not found"}
        )

        with patch.dict(
            "sys.modules",
            {
                "src.repositories.databricks_vector_index_repository": MagicMock(
                    DatabricksVectorIndexRepository=MagicMock(return_value=mock_repo)
                )
            },
        ):
            with pytest.raises(DatabricksIndexValidationError) as exc_info:
                await MemoryBackendFactory._validate_databricks_index(
                    workspace_url="https://example.databricks.com",
                    endpoint_name="ep",
                    index_name="catalog.schema.mem",
                    user_token=None,
                    group_id=None,
                )
        assert exc_info.value.error_type == "missing_index"

    @pytest.mark.asyncio
    async def test_raises_when_index_provisioning(self):
        mock_repo = MagicMock()
        mock_repo.describe_index = AsyncMock(
            return_value={
                "success": True,
                "description": {"status": {"state": "PROVISIONING", "ready": False}},
            }
        )

        with patch.dict(
            "sys.modules",
            {
                "src.repositories.databricks_vector_index_repository": MagicMock(
                    DatabricksVectorIndexRepository=MagicMock(return_value=mock_repo)
                )
            },
        ):
            with pytest.raises(DatabricksIndexValidationError) as exc_info:
                await MemoryBackendFactory._validate_databricks_index(
                    workspace_url="https://example.databricks.com",
                    endpoint_name="ep",
                    index_name="catalog.schema.mem",
                    user_token=None,
                    group_id=None,
                )
        assert exc_info.value.error_type == "provisioning_indexes"

    @pytest.mark.asyncio
    async def test_raises_when_index_in_unexpected_state(self):
        mock_repo = MagicMock()
        mock_repo.describe_index = AsyncMock(
            return_value={
                "success": True,
                "description": {"status": {"state": "FAILED", "ready": False}},
            }
        )

        with patch.dict(
            "sys.modules",
            {
                "src.repositories.databricks_vector_index_repository": MagicMock(
                    DatabricksVectorIndexRepository=MagicMock(return_value=mock_repo)
                )
            },
        ):
            with pytest.raises(DatabricksIndexValidationError) as exc_info:
                await MemoryBackendFactory._validate_databricks_index(
                    workspace_url="https://example.databricks.com",
                    endpoint_name="ep",
                    index_name="catalog.schema.mem",
                    user_token=None,
                    group_id=None,
                )
        assert exc_info.value.error_type == "unexpected_state"

    @pytest.mark.asyncio
    async def test_raises_when_describe_fails(self):
        mock_repo = MagicMock()
        mock_repo.describe_index = AsyncMock(side_effect=RuntimeError("Connection refused"))

        with patch.dict(
            "sys.modules",
            {
                "src.repositories.databricks_vector_index_repository": MagicMock(
                    DatabricksVectorIndexRepository=MagicMock(return_value=mock_repo)
                )
            },
        ):
            with pytest.raises(DatabricksIndexValidationError) as exc_info:
                await MemoryBackendFactory._validate_databricks_index(
                    workspace_url="https://example.databricks.com",
                    endpoint_name="ep",
                    index_name="catalog.schema.mem",
                    user_token=None,
                    group_id=None,
                )
        assert exc_info.value.error_type == "describe_failed"

    @pytest.mark.asyncio
    async def test_passes_when_ready_flag_true_even_if_state_not_online(self):
        """ready=True flag overrides state check."""
        mock_repo = MagicMock()
        mock_repo.describe_index = AsyncMock(
            return_value={
                "success": True,
                "description": {"status": {"state": "SYNCING", "ready": True}},
            }
        )

        with patch.dict(
            "sys.modules",
            {
                "src.repositories.databricks_vector_index_repository": MagicMock(
                    DatabricksVectorIndexRepository=MagicMock(return_value=mock_repo)
                )
            },
        ):
            # Should not raise
            await MemoryBackendFactory._validate_databricks_index(
                workspace_url="https://example.databricks.com",
                endpoint_name="ep",
                index_name="catalog.schema.mem",
                user_token=None,
                group_id=None,
            )


# ─────────────────────────────────────────────────────────────────────────────
# Additional DatabricksIndexValidationError edge cases
# ─────────────────────────────────────────────────────────────────────────────


class TestDatabricksIndexValidationErrorEdgeCases:

    def test_missing_indexes_defaults_to_empty_list(self):
        err = DatabricksIndexValidationError("msg", {"error_type": "x"})
        assert err.missing_indexes == []

    def test_provisioning_indexes_defaults_to_empty_list(self):
        err = DatabricksIndexValidationError("msg", {"error_type": "x"})
        assert err.provisioning_indexes == []

    def test_inherits_from_exception(self):
        err = DatabricksIndexValidationError("msg", {})
        assert isinstance(err, Exception)

    def test_can_be_raised_and_caught(self):
        with pytest.raises(DatabricksIndexValidationError) as exc_info:
            raise DatabricksIndexValidationError(
                "test error",
                {"error_type": "missing_indexes", "missing_indexes": ["idx1"]}
            )
        assert exc_info.value.error_type == "missing_indexes"
        assert "idx1" in exc_info.value.missing_indexes
