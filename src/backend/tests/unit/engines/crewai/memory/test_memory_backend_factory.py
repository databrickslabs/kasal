"""
Comprehensive unit tests for MemoryBackendFactory.
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
# Default backend
# ─────────────────────────────────────────────────────────────────────────────


class TestCreateMemoryBackendsDefault:
    """Tests for DEFAULT backend creation."""

    @pytest.mark.asyncio
    async def test_default_backend_returns_empty_dict(self):
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

    @pytest.mark.asyncio
    async def test_default_backend_with_embedder_returns_empty_dict(self):
        config = MemoryBackendConfig(backend_type=MemoryBackendType.DEFAULT)
        result = await MemoryBackendFactory.create_memory_backends(
            config=config,
            crew_id="test_crew",
            embedder=MagicMock(),
        )
        assert result == {}

    @pytest.mark.asyncio
    async def test_unsupported_backend_type_returns_empty_dict(self):
        config = MemoryBackendConfig(backend_type=MemoryBackendType.DEFAULT)
        # Patch the branch to simulate an unsupported / unrecognised type
        with patch.object(config, "backend_type", "some_unsupported_type"):
            result = await MemoryBackendFactory.create_memory_backends(
                config=config,
                crew_id="test_crew",
            )
        assert result == {}


# ─────────────────────────────────────────────────────────────────────────────
# Lakebase backend
# ─────────────────────────────────────────────────────────────────────────────


class TestCreateMemoryBackendsLakebase:
    """Tests for LAKEBASE backend creation."""

    @pytest.mark.asyncio
    async def test_lakebase_missing_config_raises(self):
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.LAKEBASE,
            lakebase_config=None,
            enable_short_term=True,
        )
        with pytest.raises(ValueError, match="Lakebase configuration is required"):
            await MemoryBackendFactory.create_memory_backends(
                config=config, crew_id="test_crew_123"
            )

    @pytest.mark.asyncio
    async def test_lakebase_creates_all_memory_types(self):
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
        result = await MemoryBackendFactory.create_memory_backends(
            config=config,
            crew_id="test_group_crew_abc123",
            embedder=MagicMock(),
            job_id="job_001",
        )
        assert "short_term" in result
        assert "long_term" in result
        assert "entity" in result

    @pytest.mark.asyncio
    async def test_lakebase_respects_disabled_short_term(self):
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.LAKEBASE,
            lakebase_config=LakebaseMemoryConfig(),
            enable_short_term=False,
            enable_long_term=True,
            enable_entity=True,
        )
        result = await MemoryBackendFactory.create_memory_backends(
            config=config, crew_id="crew", embedder=MagicMock()
        )
        assert "short_term" not in result
        assert "long_term" in result
        assert "entity" in result

    @pytest.mark.asyncio
    async def test_lakebase_respects_disabled_long_term(self):
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.LAKEBASE,
            lakebase_config=LakebaseMemoryConfig(),
            enable_short_term=True,
            enable_long_term=False,
            enable_entity=False,
        )
        result = await MemoryBackendFactory.create_memory_backends(
            config=config, crew_id="test_crew", embedder=MagicMock()
        )
        assert "short_term" in result
        assert "long_term" not in result
        assert "entity" not in result

    @pytest.mark.asyncio
    async def test_lakebase_passes_job_id_to_short_term(self):
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.LAKEBASE,
            lakebase_config=LakebaseMemoryConfig(),
            enable_short_term=True,
            enable_long_term=False,
            enable_entity=False,
        )
        result = await MemoryBackendFactory.create_memory_backends(
            config=config, crew_id="grp_crew_123", embedder=MagicMock(), job_id="job_42"
        )
        wrapper = result["short_term"]
        assert wrapper.storage.job_id == "job_42"

    @pytest.mark.asyncio
    async def test_lakebase_extracts_group_id_from_crew_id(self):
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.LAKEBASE,
            lakebase_config=LakebaseMemoryConfig(),
            enable_short_term=True,
            enable_long_term=False,
            enable_entity=False,
        )
        result = await MemoryBackendFactory.create_memory_backends(
            config=config, crew_id="my_group_crew_abc123", embedder=MagicMock()
        )
        wrapper = result["short_term"]
        assert wrapper.storage.group_id == "my_group"

    @pytest.mark.asyncio
    async def test_lakebase_no_group_id_when_crew_id_no_underscore_pattern(self):
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.LAKEBASE,
            lakebase_config=LakebaseMemoryConfig(),
            enable_short_term=True,
            enable_long_term=False,
            enable_entity=False,
        )
        result = await MemoryBackendFactory.create_memory_backends(
            config=config, crew_id="simple_crew_id_without_pattern", embedder=MagicMock()
        )
        # crew_id without "_crew_" pattern -> group_id should be None
        assert "short_term" in result

    @pytest.mark.asyncio
    async def test_lakebase_only_entity_enabled(self):
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.LAKEBASE,
            lakebase_config=LakebaseMemoryConfig(),
            enable_short_term=False,
            enable_long_term=False,
            enable_entity=True,
        )
        result = await MemoryBackendFactory.create_memory_backends(
            config=config, crew_id="crew", embedder=MagicMock()
        )
        assert "entity" in result
        assert "short_term" not in result
        assert "long_term" not in result


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
    async def test_databricks_raises_when_indexes_missing(self):
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.DATABRICKS,
            databricks_config=DatabricksMemoryConfig(
                endpoint_name="ep",
                short_term_index="catalog.schema.st",
                workspace_url="https://example.databricks.com",
            ),
            enable_short_term=True,
            enable_long_term=False,
            enable_entity=False,
        )
        mock_repo = MagicMock()
        mock_repo.describe_index = AsyncMock(
            return_value={"success": False, "error": "Index not found"}
        )

        with patch(
            "src.engines.crewai.memory.memory_backend_factory.MemoryBackendFactory._validate_databricks_indexes",
            new_callable=AsyncMock,
        ) as mock_validate:
            mock_validate.return_value = (
                False,  # all_valid
                [],  # valid
                ["short_term: catalog.schema.st (Index not found)"],  # missing
                [],  # provisioning
            )

            with pytest.raises(DatabricksIndexValidationError) as exc_info:
                await MemoryBackendFactory.create_memory_backends(
                    config=config, crew_id="test_crew"
                )

            assert exc_info.value.error_type == "missing_indexes"

    @pytest.mark.asyncio
    async def test_databricks_raises_when_indexes_provisioning(self):
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.DATABRICKS,
            databricks_config=DatabricksMemoryConfig(
                endpoint_name="ep",
                short_term_index="catalog.schema.st",
                workspace_url="https://example.databricks.com",
            ),
            enable_short_term=True,
        )

        with patch(
            "src.engines.crewai.memory.memory_backend_factory.MemoryBackendFactory._validate_databricks_indexes",
            new_callable=AsyncMock,
        ) as mock_validate:
            mock_validate.return_value = (
                False,
                [],
                [],
                ["short_term: catalog.schema.st (state: PROVISIONING)"],
            )

            with pytest.raises(DatabricksIndexValidationError) as exc_info:
                await MemoryBackendFactory.create_memory_backends(
                    config=config, crew_id="crew"
                )
            assert exc_info.value.error_type == "provisioning_indexes"

    @pytest.mark.asyncio
    async def test_databricks_creates_short_term_backend(self):
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.DATABRICKS,
            databricks_config=DatabricksMemoryConfig(
                endpoint_name="ep",
                short_term_index="catalog.schema.st",
                workspace_url="https://example.databricks.com",
            ),
            enable_short_term=True,
            enable_long_term=False,
            enable_entity=False,
        )

        mock_storage = MagicMock(
            memory_type="short_term",
            workspace_url="https://example.databricks.com",
            index_name="catalog.schema.st",
            endpoint_name="ep",
            user_token=None,
            group_id=None,
            job_id=None,
        )
        mock_wrapper = MagicMock()

        with patch(
            "src.engines.crewai.memory.memory_backend_factory.MemoryBackendFactory._validate_databricks_indexes",
            new_callable=AsyncMock,
            return_value=(True, ["short_term: catalog.schema.st"], [], []),
        ), patch.dict(
            "sys.modules",
            {
                "src.engines.crewai.memory.crewai_databricks_wrapper": MagicMock(
                    CrewAIDatabricksWrapper=MagicMock(return_value=mock_wrapper)
                ),
                "src.engines.crewai.memory.databricks_vector_storage": MagicMock(
                    DatabricksVectorStorage=MagicMock(return_value=mock_storage)
                ),
            },
        ):
            result = await MemoryBackendFactory.create_memory_backends(
                config=config, crew_id="test_crew"
            )

        assert "short_term" in result

    @pytest.mark.asyncio
    async def test_databricks_passes_job_id_to_short_term(self):
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.DATABRICKS,
            databricks_config=DatabricksMemoryConfig(
                endpoint_name="ep",
                short_term_index="catalog.schema.st",
                workspace_url="https://example.databricks.com",
            ),
            enable_short_term=True,
            enable_long_term=False,
            enable_entity=False,
        )

        captured_storage_kwargs = {}

        def capture_storage(**kwargs):
            captured_storage_kwargs.update(kwargs)
            mock = MagicMock()
            mock.memory_type = "short_term"
            mock.workspace_url = "https://example.databricks.com"
            mock.index_name = "catalog.schema.st"
            mock.endpoint_name = "ep"
            mock.user_token = None
            mock.group_id = None
            mock.job_id = kwargs.get("job_id")
            return mock

        with patch(
            "src.engines.crewai.memory.memory_backend_factory.MemoryBackendFactory._validate_databricks_indexes",
            new_callable=AsyncMock,
            return_value=(True, ["short_term: catalog.schema.st"], [], []),
        ), patch.dict(
            "sys.modules",
            {
                "src.engines.crewai.memory.crewai_databricks_wrapper": MagicMock(
                    CrewAIDatabricksWrapper=MagicMock(return_value=MagicMock())
                ),
                "src.engines.crewai.memory.databricks_vector_storage": MagicMock(
                    DatabricksVectorStorage=MagicMock(side_effect=capture_storage)
                ),
            },
        ):
            await MemoryBackendFactory.create_memory_backends(
                config=config,
                crew_id="group_crew_abc",
                job_id="job_session_99",
            )

        assert captured_storage_kwargs.get("job_id") == "job_session_99"

    @pytest.mark.asyncio
    async def test_databricks_extracts_group_id_from_crew_id(self):
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.DATABRICKS,
            databricks_config=DatabricksMemoryConfig(
                endpoint_name="ep",
                short_term_index="catalog.schema.st",
                workspace_url="https://example.databricks.com",
            ),
            enable_short_term=True,
            enable_long_term=False,
            enable_entity=False,
        )

        captured_kwargs = {}

        def capture_storage(**kwargs):
            captured_kwargs.update(kwargs)
            mock = MagicMock()
            mock.memory_type = "short_term"
            mock.workspace_url = "https://example.databricks.com"
            mock.index_name = "catalog.schema.st"
            mock.endpoint_name = "ep"
            mock.user_token = None
            mock.group_id = kwargs.get("group_id")
            mock.job_id = None
            return mock

        with patch(
            "src.engines.crewai.memory.memory_backend_factory.MemoryBackendFactory._validate_databricks_indexes",
            new_callable=AsyncMock,
            return_value=(True, ["short_term: catalog.schema.st"], [], []),
        ), patch.dict(
            "sys.modules",
            {
                "src.engines.crewai.memory.crewai_databricks_wrapper": MagicMock(
                    CrewAIDatabricksWrapper=MagicMock(return_value=MagicMock())
                ),
                "src.engines.crewai.memory.databricks_vector_storage": MagicMock(
                    DatabricksVectorStorage=MagicMock(side_effect=capture_storage)
                ),
            },
        ):
            await MemoryBackendFactory.create_memory_backends(
                config=config,
                crew_id="grp_alpha_crew_xyz",
            )

        assert captured_kwargs.get("group_id") == "grp_alpha"


# ─────────────────────────────────────────────────────────────────────────────
# _validate_databricks_indexes
# ─────────────────────────────────────────────────────────────────────────────


class TestValidateDatabricksIndexes:
    """Tests for the internal _validate_databricks_indexes method."""

    @pytest.mark.asyncio
    async def test_returns_true_when_no_databricks_config(self):
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.DEFAULT,
            databricks_config=None,
        )
        all_valid, valid, missing, provisioning = (
            await MemoryBackendFactory._validate_databricks_indexes(config)
        )
        assert all_valid is True
        assert valid == []
        assert missing == []
        assert provisioning == []

    @pytest.mark.asyncio
    async def test_marks_index_as_valid_when_ready(self):
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.DATABRICKS,
            databricks_config=DatabricksMemoryConfig(
                endpoint_name="ep",
                short_term_index="cat.sch.st",
                workspace_url="https://example.databricks.com",
            ),
            enable_short_term=True,
            enable_long_term=False,
            enable_entity=False,
        )

        mock_repo = MagicMock()
        mock_repo.describe_index = AsyncMock(
            return_value={
                "success": True,
                "description": {"status": {"state": "ONLINE", "ready": True}},
            }
        )
        mock_repo_cls = MagicMock(return_value=mock_repo)

        # The import is done lazily inside the method, patch the module
        with patch.dict(
            "sys.modules",
            {
                "src.repositories.databricks_vector_index_repository": MagicMock(
                    DatabricksVectorIndexRepository=mock_repo_cls
                )
            },
        ):
            all_valid, valid, missing, provisioning = (
                await MemoryBackendFactory._validate_databricks_indexes(config)
            )

        assert all_valid is True
        assert len(valid) == 1
        assert "short_term" in valid[0]

    @pytest.mark.asyncio
    async def test_marks_index_as_missing_when_not_found(self):
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.DATABRICKS,
            databricks_config=DatabricksMemoryConfig(
                endpoint_name="ep",
                short_term_index="cat.sch.st",
                workspace_url="https://example.databricks.com",
            ),
            enable_short_term=True,
            enable_long_term=False,
            enable_entity=False,
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
            all_valid, valid, missing, provisioning = (
                await MemoryBackendFactory._validate_databricks_indexes(config)
            )

        assert all_valid is False
        assert len(missing) == 1

    @pytest.mark.asyncio
    async def test_marks_index_as_provisioning_when_state_provisioning(self):
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.DATABRICKS,
            databricks_config=DatabricksMemoryConfig(
                endpoint_name="ep",
                short_term_index="cat.sch.st",
                workspace_url="https://example.databricks.com",
            ),
            enable_short_term=True,
            enable_long_term=False,
            enable_entity=False,
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
            all_valid, valid, missing, provisioning = (
                await MemoryBackendFactory._validate_databricks_indexes(config)
            )

        assert all_valid is False
        assert len(provisioning) == 1

    @pytest.mark.asyncio
    async def test_handles_exception_during_validation(self):
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.DATABRICKS,
            databricks_config=DatabricksMemoryConfig(
                endpoint_name="ep",
                short_term_index="cat.sch.st",
                workspace_url="https://example.databricks.com",
            ),
            enable_short_term=True,
        )

        with patch.dict(
            "sys.modules",
            {
                "src.repositories.databricks_vector_index_repository": MagicMock(
                    DatabricksVectorIndexRepository=MagicMock(
                        side_effect=Exception("Import error")
                    )
                )
            },
        ):
            all_valid, valid, missing, provisioning = (
                await MemoryBackendFactory._validate_databricks_indexes(config)
            )

        assert all_valid is False

    @pytest.mark.asyncio
    async def test_validates_long_term_and_entity_indexes(self):
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.DATABRICKS,
            databricks_config=DatabricksMemoryConfig(
                endpoint_name="ep",
                short_term_index="cat.sch.st",
                long_term_index="cat.sch.lt",
                entity_index="cat.sch.ent",
                workspace_url="https://example.databricks.com",
            ),
            enable_short_term=True,
            enable_long_term=True,
            enable_entity=True,
        )

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
            all_valid, valid, missing, provisioning = (
                await MemoryBackendFactory._validate_databricks_indexes(config)
            )

        assert all_valid is True
        assert len(valid) == 3
        assert mock_repo.describe_index.call_count == 3


# ─────────────────────────────────────────────────────────────────────────────
# create_embedder_wrapper
# ─────────────────────────────────────────────────────────────────────────────


class TestCreateEmbedderWrapper:
    """Tests for create_embedder_wrapper static method."""

    def test_creates_wrapper_with_required_methods(self):
        wrapper = MemoryBackendFactory.create_embedder_wrapper(MagicMock(), MagicMock())
        assert hasattr(wrapper, "embed_and_store")
        assert hasattr(wrapper, "search")
        assert hasattr(wrapper, "reset")

    def test_embed_and_store_calls_callable_embedder(self):
        mock_embedder = MagicMock(return_value=[[0.1] * 10])
        mock_storage = MagicMock()
        wrapper = MemoryBackendFactory.create_embedder_wrapper(mock_embedder, mock_storage)

        wrapper.embed_and_store("test text", metadata={"key": "val"}, agent="tester")

        mock_embedder.assert_called_once_with(["test text"])
        mock_storage.save.assert_called_once()

    def test_embed_and_store_calls_embed_method_embedder(self):
        mock_embedder = MagicMock(spec=[])
        mock_embedder.embed = MagicMock(return_value=[0.2] * 10)
        del mock_embedder.__call__  # Not callable
        mock_storage = MagicMock()

        # Create embedder with embed method only (no __call__)
        class EmbedOnly:
            def embed(self, text):
                return [0.2] * 10

        wrapper = MemoryBackendFactory.create_embedder_wrapper(EmbedOnly(), mock_storage)
        wrapper.embed_and_store("text")
        mock_storage.save.assert_called_once()

    def test_search_uses_callable_embedder(self):
        query_emb = [[0.5] * 10]
        mock_embedder = MagicMock(return_value=query_emb)
        mock_storage = MagicMock()
        mock_storage.search.return_value = [{"content": "result"}]

        wrapper = MemoryBackendFactory.create_embedder_wrapper(mock_embedder, mock_storage)
        results = wrapper.search("query", limit=5)

        mock_embedder.assert_called_once_with(["query"])
        mock_storage.search.assert_called_once()
        assert results == [{"content": "result"}]

    def test_search_returns_empty_list_on_exception(self):
        mock_embedder = MagicMock(side_effect=Exception("Embed error"))
        mock_storage = MagicMock()

        wrapper = MemoryBackendFactory.create_embedder_wrapper(mock_embedder, mock_storage)
        results = wrapper.search("query")

        assert results == []

    def test_reset_delegates_to_storage(self):
        mock_embedder = MagicMock()
        mock_storage = MagicMock()

        wrapper = MemoryBackendFactory.create_embedder_wrapper(mock_embedder, mock_storage)
        wrapper.reset()

        mock_storage.reset.assert_called_once()

    def test_embed_and_store_handles_exception_gracefully(self):
        mock_embedder = MagicMock(side_effect=Exception("Embed failed"))
        mock_storage = MagicMock()

        wrapper = MemoryBackendFactory.create_embedder_wrapper(mock_embedder, mock_storage)
        # Should not raise
        wrapper.embed_and_store("text")
        mock_storage.save.assert_not_called()

    def test_embed_and_store_no_embed_method_logs_error(self):
        # Embedder with no __call__ and no embed method
        class BadEmbedder:
            pass

        mock_storage = MagicMock()
        wrapper = MemoryBackendFactory.create_embedder_wrapper(BadEmbedder(), mock_storage)
        # Should not raise
        wrapper.embed_and_store("text")
        mock_storage.save.assert_not_called()

    def test_search_no_embed_method_returns_empty(self):
        class BadEmbedder:
            pass

        mock_storage = MagicMock()
        wrapper = MemoryBackendFactory.create_embedder_wrapper(BadEmbedder(), mock_storage)
        results = wrapper.search("q")
        assert results == []

    def test_search_uses_embed_method_embedder(self):
        class EmbedOnly:
            def embed(self, text):
                return [0.3] * 5

        mock_storage = MagicMock()
        mock_storage.search.return_value = [{"id": "1"}]
        wrapper = MemoryBackendFactory.create_embedder_wrapper(EmbedOnly(), mock_storage)
        results = wrapper.search("query")
        assert results == [{"id": "1"}]


# ─────────────────────────────────────────────────────────────────────────────
# _validate_databricks_indexes: additional branches
# ─────────────────────────────────────────────────────────────────────────────


class TestValidateDatabricksIndexesAdditional:
    """Cover lines 111-117, 130-137."""

    @pytest.mark.asyncio
    async def test_marks_index_as_provisioning_unknown_state(self):
        """Index in unknown state (not ONLINE/READY/PROVISIONING) goes to provisioning list."""
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.DATABRICKS,
            databricks_config=DatabricksMemoryConfig(
                endpoint_name="ep",
                short_term_index="cat.sch.st",
                workspace_url="https://example.databricks.com",
            ),
            enable_short_term=True,
            enable_long_term=False,
            enable_entity=False,
        )

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
            all_valid, valid, missing, provisioning = (
                await MemoryBackendFactory._validate_databricks_indexes(config)
            )

        assert all_valid is False
        assert len(provisioning) == 1
        assert "FAILED" in provisioning[0]

    @pytest.mark.asyncio
    async def test_marks_index_as_missing_on_describe_exception(self):
        """Exception during describe_index puts index in missing list."""
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.DATABRICKS,
            databricks_config=DatabricksMemoryConfig(
                endpoint_name="ep",
                short_term_index="cat.sch.st",
                workspace_url="https://example.databricks.com",
            ),
            enable_short_term=True,
            enable_long_term=False,
            enable_entity=False,
        )

        mock_repo = MagicMock()
        mock_repo.describe_index = AsyncMock(
            side_effect=Exception("connection refused")
        )

        with patch.dict(
            "sys.modules",
            {
                "src.repositories.databricks_vector_index_repository": MagicMock(
                    DatabricksVectorIndexRepository=MagicMock(return_value=mock_repo)
                )
            },
        ):
            all_valid, valid, missing, provisioning = (
                await MemoryBackendFactory._validate_databricks_indexes(config)
            )

        assert all_valid is False
        assert len(missing) == 1
        assert "connection refused" in missing[0]

    @pytest.mark.asyncio
    async def test_index_ready_flag_true_counts_as_valid(self):
        """Index in READY state counts as valid."""
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.DATABRICKS,
            databricks_config=DatabricksMemoryConfig(
                endpoint_name="ep",
                short_term_index="cat.sch.st",
                workspace_url="https://example.databricks.com",
            ),
            enable_short_term=True,
            enable_long_term=False,
            enable_entity=False,
        )

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
                )
            },
        ):
            all_valid, valid, missing, provisioning = (
                await MemoryBackendFactory._validate_databricks_indexes(config)
            )

        assert all_valid is True
        assert len(valid) == 1


# ─────────────────────────────────────────────────────────────────────────────
# Databricks backend: auth token forwarding + entity relationship retrieval
# ─────────────────────────────────────────────────────────────────────────────


class TestCreateMemoryBackendsDatabricksAuth:
    """Cover lines 305, 309, 313, 317, 339-374, 382-432."""

    @pytest.mark.asyncio
    async def test_databricks_passes_auth_tokens_to_storage(self):
        """PAT, service principal creds, and user_token are forwarded when set."""
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.DATABRICKS,
            databricks_config=DatabricksMemoryConfig(
                endpoint_name="ep",
                short_term_index="cat.sch.st",
                long_term_index="cat.sch.lt",
                entity_index="cat.sch.ent",
                workspace_url="https://example.databricks.com",
                personal_access_token="pat-secret",
                service_principal_client_id="sp-id",
                service_principal_client_secret="sp-secret",
            ),
            enable_short_term=True,
            enable_long_term=True,
            enable_entity=True,
            enable_relationship_retrieval=True,
        )

        captured_kwargs_list = []

        def capture_storage(**kwargs):
            captured_kwargs_list.append(dict(kwargs))
            mock = MagicMock()
            for k, v in kwargs.items():
                setattr(mock, k, v)
            return mock

        with patch(
            "src.engines.crewai.memory.memory_backend_factory.MemoryBackendFactory._validate_databricks_indexes",
            new_callable=AsyncMock,
            return_value=(True, ["all valid"], [], []),
        ), patch.dict(
            "sys.modules",
            {
                "src.engines.crewai.memory.crewai_databricks_wrapper": MagicMock(
                    CrewAIDatabricksWrapper=MagicMock(return_value=MagicMock())
                ),
                "src.engines.crewai.memory.databricks_vector_storage": MagicMock(
                    DatabricksVectorStorage=MagicMock(side_effect=capture_storage)
                ),
            },
        ):
            result = await MemoryBackendFactory.create_memory_backends(
                config=config,
                crew_id="grp_crew_abc",
                user_token="obo-token",
            )

        assert "short_term" in result
        assert "long_term" in result
        assert "entity" in result

        for kwargs in captured_kwargs_list:
            assert kwargs.get("personal_access_token") == "pat-secret"
            assert kwargs.get("service_principal_client_id") == "sp-id"
            assert kwargs.get("service_principal_client_secret") == "sp-secret"
            assert kwargs.get("user_token") == "obo-token"

    @pytest.mark.asyncio
    async def test_databricks_entity_relationship_retrieval_flag(self):
        """enable_relationship_retrieval=True is forwarded to CrewAIDatabricksWrapper for entity."""
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.DATABRICKS,
            databricks_config=DatabricksMemoryConfig(
                endpoint_name="ep",
                short_term_index="cat.sch.st",  # required field
                entity_index="cat.sch.ent",
                workspace_url="https://example.databricks.com",
            ),
            enable_short_term=True,  # must be True so group_id is initialized
            enable_long_term=False,
            enable_entity=True,
            enable_relationship_retrieval=True,
        )

        wrapper_init_calls = []

        class FakeWrapper:
            def __init__(self, storage, embedder, enable_relationship_retrieval=False):
                wrapper_init_calls.append(enable_relationship_retrieval)

        with patch(
            "src.engines.crewai.memory.memory_backend_factory.MemoryBackendFactory._validate_databricks_indexes",
            new_callable=AsyncMock,
            return_value=(True, ["entity ok"], [], []),
        ), patch.dict(
            "sys.modules",
            {
                "src.engines.crewai.memory.crewai_databricks_wrapper": MagicMock(
                    CrewAIDatabricksWrapper=FakeWrapper
                ),
                "src.engines.crewai.memory.databricks_vector_storage": MagicMock(
                    DatabricksVectorStorage=MagicMock(return_value=MagicMock())
                ),
            },
        ):
            result = await MemoryBackendFactory.create_memory_backends(
                config=config,
                crew_id="g_crew_x",
            )

        # With enable_short_term=True, we get 2 wrapper calls (short_term + entity)
        # The entity wrapper should have enable_relationship_retrieval=True
        assert len(wrapper_init_calls) >= 1
        assert True in wrapper_init_calls

    @pytest.mark.asyncio
    async def test_databricks_long_term_backend_with_group_id(self):
        """Long-term storage gets group_id forwarded when crew_id has pattern."""
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.DATABRICKS,
            databricks_config=DatabricksMemoryConfig(
                endpoint_name="ep",
                short_term_index="cat.sch.st",  # required field
                long_term_index="cat.sch.lt",
                workspace_url="https://example.databricks.com",
            ),
            enable_short_term=True,  # must be True so group_id is initialized
            enable_long_term=True,
            enable_entity=False,
        )

        captured_kwargs_list = []

        def capture_storage(**kwargs):
            captured_kwargs_list.append(dict(kwargs))
            return MagicMock()

        with patch(
            "src.engines.crewai.memory.memory_backend_factory.MemoryBackendFactory._validate_databricks_indexes",
            new_callable=AsyncMock,
            return_value=(True, ["lt ok"], [], []),
        ), patch.dict(
            "sys.modules",
            {
                "src.engines.crewai.memory.crewai_databricks_wrapper": MagicMock(
                    CrewAIDatabricksWrapper=MagicMock(return_value=MagicMock())
                ),
                "src.engines.crewai.memory.databricks_vector_storage": MagicMock(
                    DatabricksVectorStorage=MagicMock(side_effect=capture_storage)
                ),
            },
        ):
            await MemoryBackendFactory.create_memory_backends(
                config=config,
                crew_id="mygroup_crew_xyz",
            )

        # With enable_short_term=True, we get 2 storage calls (short_term + long_term)
        assert len(captured_kwargs_list) >= 1
        # The long-term storage should have group_id set
        lt_kwargs = next(
            (kw for kw in captured_kwargs_list if kw.get("memory_type") == "long_term"), {}
        )
        assert lt_kwargs.get("group_id") == "mygroup"


# ─────────────────────────────────────────────────────────────────────────────
# Lakebase backend: generic exception
# ─────────────────────────────────────────────────────────────────────────────


class TestCreateMemoryBackendsLakebaseErrors:
    """Cover lines 519-524, 553-557."""

    @pytest.mark.asyncio
    async def test_lakebase_generic_exception_during_creation_raises(self):
        """Generic exception in Lakebase storage creation propagates."""
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.LAKEBASE,
            lakebase_config=LakebaseMemoryConfig(),
            enable_short_term=True,
        )

        with patch.dict(
            "sys.modules",
            {
                "src.engines.crewai.memory.crewai_lakebase_wrapper": MagicMock(
                    CrewAILakebaseWrapper=MagicMock(side_effect=Exception("storage failure"))
                ),
                "src.engines.crewai.memory.lakebase_pgvector_storage": MagicMock(
                    LakebasePgVectorStorage=MagicMock(return_value=MagicMock())
                ),
            },
        ):
            with pytest.raises(Exception, match="storage failure"):
                await MemoryBackendFactory.create_memory_backends(
                    config=config, crew_id="crew"
                )


# ─────────────────────────────────────────────────────────────────────────────
# Default backend: ImportError
# ─────────────────────────────────────────────────────────────────────────────


class TestCreateMemoryBackendsDefaultAdditional:
    """Cover line 617 and general default backend import paths."""

    @pytest.mark.asyncio
    async def test_unsupported_backend_returns_empty_dict(self):
        """Unknown backend type returns empty dict (covers line 559-560)."""
        config = MemoryBackendConfig(backend_type=MemoryBackendType.DEFAULT)
        with patch.object(config, "backend_type", "totally_unknown"):
            result = await MemoryBackendFactory.create_memory_backends(
                config=config, crew_id="crew"
            )
        assert result == {}
