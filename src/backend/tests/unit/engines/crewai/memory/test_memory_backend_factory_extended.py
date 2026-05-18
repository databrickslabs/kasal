"""
Extended tests for memory_backend_factory.py — targeting uncovered branches.

Focus areas:
- _validate_databricks_indexes: READY state (via status.ready=True), PENDING state,
  object-based exception during per-index check, outer exception handling
- create_memory_backends DATABRICKS: entity storage with relationship retrieval,
  all indexes with auth params (PAT, service principal), user_token, group_id extraction
- create_memory_backends LAKEBASE: no instance_name on lakebase_config
- create_embedder_wrapper: embed-method-only path, search with embed method
- DatabricksIndexValidationError: all field extraction paths
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


# ─── _validate_databricks_indexes extra branches ──────────────────────────────

class TestValidateDatabricksIndexesExtra:

    @pytest.mark.asyncio
    async def test_ready_via_is_ready_flag(self):
        """Index is valid when status.ready=True even if state is not ONLINE."""
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
            all_valid, valid, missing, provisioning = (
                await MemoryBackendFactory._validate_databricks_indexes(config)
            )

        assert all_valid is True
        assert len(valid) == 1

    @pytest.mark.asyncio
    async def test_pending_state_treated_as_provisioning(self):
        """PENDING state goes into provisioning list."""
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.DATABRICKS,
            databricks_config=DatabricksMemoryConfig(
                endpoint_name="ep",
                short_term_index="cat.sch.st",
                long_term_index="cat.sch.lt",
                workspace_url="https://example.databricks.com",
            ),
            enable_short_term=False,
            enable_long_term=True,
            enable_entity=False,
        )

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
            all_valid, valid, missing, provisioning = (
                await MemoryBackendFactory._validate_databricks_indexes(config)
            )

        assert all_valid is False
        assert len(provisioning) == 1

    @pytest.mark.asyncio
    async def test_creating_state_treated_as_provisioning(self):
        """CREATING state goes into provisioning list."""
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.DATABRICKS,
            databricks_config=DatabricksMemoryConfig(
                endpoint_name="ep",
                short_term_index="cat.sch.st",
                entity_index="cat.sch.ent",
                workspace_url="https://example.databricks.com",
            ),
            enable_short_term=False,
            enable_long_term=False,
            enable_entity=True,
        )

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
            all_valid, valid, missing, provisioning = (
                await MemoryBackendFactory._validate_databricks_indexes(config)
            )

        assert all_valid is False
        assert len(provisioning) == 1

    @pytest.mark.asyncio
    async def test_unknown_state_treated_as_provisioning(self):
        """Unknown state + not ready goes into provisioning list."""
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.DATABRICKS,
            databricks_config=DatabricksMemoryConfig(
                endpoint_name="ep",
                short_term_index="cat.sch.st",
                workspace_url="https://example.databricks.com",
            ),
            enable_short_term=True,
        )

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
            all_valid, valid, missing, provisioning = (
                await MemoryBackendFactory._validate_databricks_indexes(config)
            )

        assert all_valid is False
        assert len(provisioning) == 1

    @pytest.mark.asyncio
    async def test_per_index_exception_adds_to_missing(self):
        """Exception during individual index describe adds to missing list."""
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.DATABRICKS,
            databricks_config=DatabricksMemoryConfig(
                endpoint_name="ep",
                short_term_index="cat.sch.st",
                workspace_url="https://example.databricks.com",
            ),
            enable_short_term=True,
        )

        mock_repo = MagicMock()
        mock_repo.describe_index = AsyncMock(side_effect=Exception("Connection timeout"))

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
        assert "Connection timeout" in missing[0]

    @pytest.mark.asyncio
    async def test_no_indexes_configured_returns_valid(self):
        """When indexes are disabled, validation skips them and returns valid."""
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.DATABRICKS,
            databricks_config=DatabricksMemoryConfig(
                endpoint_name="ep",
                short_term_index="cat.sch.st",
                # No long_term_index, no entity_index
                workspace_url="https://example.databricks.com",
            ),
            enable_short_term=False,  # Disabled -> not checked
            enable_long_term=False,   # Disabled -> not checked
            enable_entity=False,      # Disabled -> not checked
        )

        with patch.dict(
            "sys.modules",
            {
                "src.repositories.databricks_vector_index_repository": MagicMock(
                    DatabricksVectorIndexRepository=MagicMock(return_value=MagicMock())
                )
            },
        ):
            all_valid, valid, missing, provisioning = (
                await MemoryBackendFactory._validate_databricks_indexes(config)
            )

        # No indexes to check means all_valid=True with empty lists
        assert all_valid is True
        assert valid == []
        assert missing == []
        assert provisioning == []


# ─── create_memory_backends DATABRICKS with auth params ──────────────────────

class TestDatabricksBackendWithAuthParams:

    @pytest.mark.asyncio
    async def test_databricks_with_personal_access_token(self):
        """PAT is passed to storage kwargs when provided."""
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.DATABRICKS,
            databricks_config=DatabricksMemoryConfig(
                endpoint_name="ep",
                short_term_index="cat.sch.st",
                workspace_url="https://example.databricks.com",
                personal_access_token="dapi-test-token",
            ),
            enable_short_term=True,
            enable_long_term=False,
            enable_entity=False,
        )

        captured_kwargs = {}

        def capture_storage(**kwargs):
            captured_kwargs.update(kwargs)
            m = MagicMock()
            m.memory_type = "short_term"
            m.workspace_url = "https://example.databricks.com"
            m.index_name = "cat.sch.st"
            m.endpoint_name = "ep"
            m.user_token = None
            m.group_id = None
            m.job_id = None
            return m

        with patch(
            "src.engines.crewai.memory.memory_backend_factory.MemoryBackendFactory._validate_databricks_indexes",
            new_callable=AsyncMock,
            return_value=(True, ["short_term: cat.sch.st"], [], []),
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
                config=config, crew_id="test_crew"
            )

        assert captured_kwargs.get("personal_access_token") == "dapi-test-token"

    @pytest.mark.asyncio
    async def test_databricks_with_service_principal(self):
        """Service principal creds passed to storage kwargs when provided."""
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.DATABRICKS,
            databricks_config=DatabricksMemoryConfig(
                endpoint_name="ep",
                short_term_index="cat.sch.st",
                long_term_index="cat.sch.lt",
                workspace_url="https://example.databricks.com",
                service_principal_client_id="sp-client",
                service_principal_client_secret="sp-secret",
            ),
            enable_short_term=True,  # must be True so group_id is initialized
            enable_long_term=True,
            enable_entity=False,
        )

        captured_kwargs_list = []

        def capture_storage(**kwargs):
            captured_kwargs_list.append(dict(kwargs))
            m = MagicMock()
            m.memory_type = kwargs.get("memory_type", "short_term")
            m.workspace_url = "https://example.databricks.com"
            m.index_name = kwargs.get("index_name", "cat.sch.st")
            m.endpoint_name = "ep"
            m.user_token = None
            m.group_id = None
            m.job_id = None
            return m

        with patch(
            "src.engines.crewai.memory.memory_backend_factory.MemoryBackendFactory._validate_databricks_indexes",
            new_callable=AsyncMock,
            return_value=(True, ["short_term: cat.sch.st", "long_term: cat.sch.lt"], [], []),
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
                config=config, crew_id="test_crew"
            )

        # The long-term storage should have the SP credentials
        lt_kwargs = next(
            (kw for kw in captured_kwargs_list if kw.get("memory_type") == "long_term"), {}
        )
        assert lt_kwargs.get("service_principal_client_id") == "sp-client"
        assert lt_kwargs.get("service_principal_client_secret") == "sp-secret"

    @pytest.mark.asyncio
    async def test_databricks_entity_with_relationship_retrieval(self):
        """Entity storage wrapper gets enable_relationship_retrieval from config."""
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.DATABRICKS,
            databricks_config=DatabricksMemoryConfig(
                endpoint_name="ep",
                short_term_index="cat.sch.st",
                entity_index="cat.sch.ent",
                workspace_url="https://example.databricks.com",
            ),
            enable_short_term=True,  # must be True so group_id is initialized
            enable_long_term=False,
            enable_entity=True,
            enable_relationship_retrieval=True,
        )

        captured_wrapper_kwargs = {}

        def capture_wrapper(storage, embedder, enable_relationship_retrieval=False):
            captured_wrapper_kwargs['enable_relationship_retrieval'] = enable_relationship_retrieval
            return MagicMock()

        with patch(
            "src.engines.crewai.memory.memory_backend_factory.MemoryBackendFactory._validate_databricks_indexes",
            new_callable=AsyncMock,
            return_value=(True, ["entity: cat.sch.ent"], [], []),
        ), patch.dict(
            "sys.modules",
            {
                "src.engines.crewai.memory.crewai_databricks_wrapper": MagicMock(
                    CrewAIDatabricksWrapper=MagicMock(side_effect=capture_wrapper)
                ),
                "src.engines.crewai.memory.databricks_vector_storage": MagicMock(
                    DatabricksVectorStorage=MagicMock(return_value=MagicMock(
                        memory_type="entity",
                        workspace_url="https://example.databricks.com",
                        index_name="cat.sch.ent",
                        endpoint_name="ep",
                        user_token=None,
                        group_id=None,
                    ))
                ),
            },
        ):
            result = await MemoryBackendFactory.create_memory_backends(
                config=config, crew_id="test_crew"
            )

        assert captured_wrapper_kwargs.get('enable_relationship_retrieval') is True

    @pytest.mark.asyncio
    async def test_databricks_all_three_types(self):
        """Creates short-term, long-term, entity all at once."""
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

        mock_storage = MagicMock()
        mock_storage.memory_type = "short_term"
        mock_storage.workspace_url = "https://example.databricks.com"
        mock_storage.index_name = "cat.sch.st"
        mock_storage.endpoint_name = "ep"
        mock_storage.user_token = None
        mock_storage.group_id = None
        mock_storage.job_id = None

        with patch(
            "src.engines.crewai.memory.memory_backend_factory.MemoryBackendFactory._validate_databricks_indexes",
            new_callable=AsyncMock,
            return_value=(True, ["short_term", "long_term", "entity"], [], []),
        ), patch.dict(
            "sys.modules",
            {
                "src.engines.crewai.memory.crewai_databricks_wrapper": MagicMock(
                    CrewAIDatabricksWrapper=MagicMock(return_value=MagicMock())
                ),
                "src.engines.crewai.memory.databricks_vector_storage": MagicMock(
                    DatabricksVectorStorage=MagicMock(return_value=mock_storage)
                ),
            },
        ):
            result = await MemoryBackendFactory.create_memory_backends(
                config=config, crew_id="group_crew_abc123", user_token="user-tok"
            )

        assert "short_term" in result
        assert "long_term" in result
        assert "entity" in result

    @pytest.mark.asyncio
    async def test_databricks_import_error_raises(self):
        """ImportError from lazy imports propagates."""
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.DATABRICKS,
            databricks_config=DatabricksMemoryConfig(
                endpoint_name="ep",
                short_term_index="cat.sch.st",
                workspace_url="https://example.databricks.com",
            ),
            enable_short_term=True,
        )

        with patch(
            "src.engines.crewai.memory.memory_backend_factory.MemoryBackendFactory._validate_databricks_indexes",
            new_callable=AsyncMock,
            return_value=(True, ["short_term: cat.sch.st"], [], []),
        ), patch.dict(
            "sys.modules",
            {
                "src.engines.crewai.memory.databricks_vector_storage": None,
                "src.engines.crewai.memory.crewai_databricks_wrapper": None,
            },
        ):
            with pytest.raises((ImportError, TypeError)):
                await MemoryBackendFactory.create_memory_backends(
                    config=config, crew_id="test_crew"
                )


# ─── create_embedder_wrapper: embed-method-only path ─────────────────────────

class TestCreateEmbedderWrapperExtended:

    def test_search_with_embed_method_embedder(self):
        """search() works when embedder has .embed() method (not __call__)."""
        class EmbedOnlyEmbedder:
            def embed(self, text):
                return [0.1, 0.2, 0.3]

        mock_storage = MagicMock()
        mock_storage.search.return_value = [{"id": "r1"}]

        wrapper = MemoryBackendFactory.create_embedder_wrapper(EmbedOnlyEmbedder(), mock_storage)
        results = wrapper.search("query", limit=3)

        mock_storage.search.assert_called_once()
        assert results == [{"id": "r1"}]

    def test_embed_and_store_with_embed_method_embedder(self):
        """embed_and_store() works with .embed() method embedder."""
        class EmbedOnlyEmbedder:
            def embed(self, text):
                return [0.5, 0.6]

        mock_storage = MagicMock()
        wrapper = MemoryBackendFactory.create_embedder_wrapper(EmbedOnlyEmbedder(), mock_storage)
        wrapper.embed_and_store("content", metadata={"key": "val"})

        mock_storage.save.assert_called_once()
        call_args = mock_storage.save.call_args
        value = call_args[0][0]
        assert "embedding" in value
        assert value["embedding"] == [0.5, 0.6]

    def test_search_exception_in_embed_returns_empty(self):
        """search() returns [] when embed raises an exception."""
        class BrokenEmbedder:
            def embed(self, text):
                raise ValueError("embed failed")

        mock_storage = MagicMock()
        wrapper = MemoryBackendFactory.create_embedder_wrapper(BrokenEmbedder(), mock_storage)
        results = wrapper.search("q")
        assert results == []

    def test_callable_embedder_in_search(self):
        """search() generates embedding using callable embedder."""
        mock_embedder = MagicMock(return_value=[[0.1, 0.2]])
        mock_storage = MagicMock()
        mock_storage.search.return_value = [{"id": "x"}]

        wrapper = MemoryBackendFactory.create_embedder_wrapper(mock_embedder, mock_storage)
        results = wrapper.search("my query", limit=10)

        mock_embedder.assert_called_once_with(["my query"])
        assert results == [{"id": "x"}]


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
                {"error_type": "missing_indexes", "missing_indexes": ["idx1"]}
            )
        assert exc_info.value.error_type == "missing_indexes"
        assert "idx1" in exc_info.value.missing_indexes
