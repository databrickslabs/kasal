"""
Extended tests for memory_backend_factory.py — targeting uncovered branches.

Updated for app-modes refactoring:
- Uses _validate_databricks_index (singular) — single unified index
- create_memory_backends returns {"unified": backend} or {}
- DatabricksMemoryConfig requires memory_index (not short_term/long_term/entity indexes)
- LakebaseMemoryConfig uses memory_table (not short_term/long_term/entity tables)
- create_embedder_wrapper was removed (no longer in API)
"""
import sys
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


# ─── _validate_databricks_index extra state branches ──────────────────────────

class TestValidateDatabricksIndexExtra:

    @pytest.mark.asyncio
    async def test_ready_via_is_ready_flag_even_non_online_state(self):
        """Index is valid when status.ready=True even if state is not ONLINE."""
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
                index_name="cat.sch.mem",
                user_token=None,
                group_id=None,
            )

    @pytest.mark.asyncio
    async def test_pending_state_treated_as_provisioning(self):
        """PENDING state raises with provisioning_indexes error_type."""
        mock_repo = MagicMock()
        mock_repo.describe_index = AsyncMock(
            return_value={
                "success": True,
                "description": {"status": {"state": "PENDING", "ready": False}},
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
                    index_name="cat.sch.mem",
                    user_token=None,
                    group_id=None,
                )
        assert exc_info.value.error_type == "provisioning_indexes"

    @pytest.mark.asyncio
    async def test_creating_state_treated_as_provisioning(self):
        """CREATING state raises with provisioning_indexes error_type."""
        mock_repo = MagicMock()
        mock_repo.describe_index = AsyncMock(
            return_value={
                "success": True,
                "description": {"status": {"state": "CREATING", "ready": False}},
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
                    index_name="cat.sch.mem",
                    user_token=None,
                    group_id=None,
                )
        assert exc_info.value.error_type == "provisioning_indexes"

    @pytest.mark.asyncio
    async def test_unknown_state_not_ready_raises_unexpected_state(self):
        """Unknown state + not ready raises with unexpected_state error_type."""
        mock_repo = MagicMock()
        mock_repo.describe_index = AsyncMock(
            return_value={
                "success": True,
                "description": {"status": {"state": "UNKNOWN_STATE", "ready": False}},
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
                    index_name="cat.sch.mem",
                    user_token=None,
                    group_id=None,
                )
        assert exc_info.value.error_type == "unexpected_state"

    @pytest.mark.asyncio
    async def test_describe_exception_raises_describe_failed(self):
        """Exception during describe raises with describe_failed error_type."""
        mock_repo = MagicMock()
        mock_repo.describe_index = AsyncMock(side_effect=RuntimeError("Connection timeout"))

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
                    index_name="cat.sch.mem",
                    user_token=None,
                    group_id=None,
                )
        assert exc_info.value.error_type == "describe_failed"

    @pytest.mark.asyncio
    async def test_all_skipped_when_no_workspace_url(self):
        """Validation is skipped entirely when workspace_url is absent."""
        # Should not raise at all
        await MemoryBackendFactory._validate_databricks_index(
            workspace_url=None,
            endpoint_name="ep",
            index_name="cat.sch.mem",
            user_token=None,
            group_id=None,
        )


# ─── create_memory_backends DATABRICKS ──────────────────────────────────────

class TestDatabricksBackendCases:

    @pytest.mark.asyncio
    async def test_databricks_with_user_token_forwarded_to_storage(self):
        """user_token is passed through to DatabricksStorageBackend."""
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.DATABRICKS,
            databricks_config=DatabricksMemoryConfig(
                endpoint_name="ep",
                memory_index="cat.sch.mem",
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
                config=config,
                crew_id="grp_crew_abc",
                user_token="user-obo-token",
            )

        assert captured_kwargs.get("user_token") == "user-obo-token"

    @pytest.mark.asyncio
    async def test_databricks_group_id_extracted_correctly(self):
        """group_id is correctly extracted from crew_id using _crew_ separator."""
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.DATABRICKS,
            databricks_config=DatabricksMemoryConfig(
                endpoint_name="ep",
                memory_index="cat.sch.mem",
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
                config=config,
                crew_id="my_tenant_group_crew_abc123",
            )

        assert captured_kwargs.get("group_id") == "my_tenant_group"

    @pytest.mark.asyncio
    async def test_databricks_job_id_forwarded_as_session_id(self):
        """job_id is forwarded to DatabricksStorageBackend as session_id."""
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.DATABRICKS,
            databricks_config=DatabricksMemoryConfig(
                endpoint_name="ep",
                memory_index="cat.sch.mem",
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
                "description": {"status": {"state": "READY", "ready": True}},
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
                config=config,
                crew_id="grp_crew_xyz",
                job_id="my-job-session-99",
            )

        assert captured_kwargs.get("session_id") == "my-job-session-99"

    @pytest.mark.asyncio
    async def test_databricks_no_memory_index_raises(self):
        """Empty memory_index raises ValueError before validation."""
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.DATABRICKS,
            databricks_config=DatabricksMemoryConfig(
                endpoint_name="ep",
                memory_index="",  # empty
                workspace_url="https://example.databricks.com",
            ),
        )
        with pytest.raises(ValueError, match="memory_index is required"):
            await MemoryBackendFactory.create_memory_backends(
                config=config, crew_id="grp_crew_xyz"
            )


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
                "src.engines.crewai.memory.lakebase_storage_backend": MagicMock(
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
                "src.engines.crewai.memory.lakebase_storage_backend": MagicMock(
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
                "src.engines.crewai.memory.lakebase_storage_backend": MagicMock(
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


# ─── DatabricksIndexValidationError edge cases ───────────────────────────────

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
                {"error_type": "missing_index", "missing_indexes": ["idx1"]}
            )
        assert exc_info.value.error_type == "missing_index"
        assert "idx1" in exc_info.value.missing_indexes
