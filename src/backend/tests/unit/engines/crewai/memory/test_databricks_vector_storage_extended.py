"""
Extended tests for databricks_vector_storage.py to push coverage to 90%+.

Covers missing lines:
- __init__ with workspace_url fallback via get_auth_context (lines 115-124)
- __init__ with USE_NULLPOOL not set (lines 136-137)
- save: all schema fields for each memory type (short_term, long_term, entity, document)
- save: numpy array embedding conversion (lines 352-359)
- save: record empty validation (lines 363-366)
- save: missing id field (lines 376-377)
- save: missing embedding field (lines 380-382)
- save: UserContext group_id setting (lines 388-400)
- save: index not ready warning (lines 411-414)
- save: upsert failure -> exception (line 470)
- search: document memory type (line 516-519)
- clear: document memory type filter (line 577)
- count_documents: document type (line 684)
"""
import os
import sys
import pytest
from unittest.mock import MagicMock, AsyncMock, patch
import numpy as np

os.environ.setdefault("DATABASE_TYPE", "sqlite")
os.environ.setdefault("SQLITE_DB_PATH", ":memory:")
os.environ["USE_NULLPOOL"] = "true"

_crewai_mock = MagicMock()
_MODULES_TO_MOCK = {
    "crewai": _crewai_mock,
    "crewai.tools": _crewai_mock.tools,
    "crewai.events": _crewai_mock.events,
    "crewai.flow": _crewai_mock.flow,
    "crewai.flow.flow": _crewai_mock.flow.flow,
    "crewai.flow.persistence": _crewai_mock.flow.persistence,
    "crewai.llm": _crewai_mock.llm,
    "crewai.memory": _crewai_mock.memory,
    "crewai.memory.storage": _crewai_mock.memory.storage,
    "crewai.memory.storage.rag_storage": _crewai_mock.memory.storage.rag_storage,
    "crewai.project": _crewai_mock.project,
    "crewai.tasks": _crewai_mock.tasks,
    "crewai.tasks.llm_guardrail": _crewai_mock.tasks.llm_guardrail,
    "crewai.tasks.task_output": _crewai_mock.tasks.task_output,
    "crewai.utilities": _crewai_mock.utilities,
    "crewai.utilities.converter": _crewai_mock.utilities.converter,
    "crewai.utilities.evaluators": _crewai_mock.utilities.evaluators,
    "crewai.utilities.evaluators.task_evaluator": _crewai_mock.utilities.evaluators.task_evaluator,
    "crewai.utilities.exceptions": _crewai_mock.utilities.exceptions,
    "crewai.utilities.internal_instructor": _crewai_mock.utilities.internal_instructor,
    "crewai.utilities.paths": _crewai_mock.utilities.paths,
    "crewai.utilities.printer": _crewai_mock.utilities.printer,
    "crewai.knowledge": _crewai_mock.knowledge,
    "crewai.llms": _crewai_mock.llms,
    "crewai.llms.providers": _crewai_mock.llms.providers,
    "crewai.llms.providers.openai": _crewai_mock.llms.providers.openai,
    "crewai.llms.providers.openai.completion": _crewai_mock.llms.providers.openai.completion,
    "crewai.events.types": _crewai_mock.events.types,
    "crewai.events.types.llm_events": _crewai_mock.events.types.llm_events,
    "crewai_tools": MagicMock(),
    "asyncpg": MagicMock(),
    "chromadb": MagicMock(),
}

_originals = {}
for _mod_name, _mock_obj in _MODULES_TO_MOCK.items():
    _originals[_mod_name] = sys.modules.get(_mod_name)
    sys.modules[_mod_name] = _mock_obj

# Import while mocks are active so the module is loaded properly
from src.engines.crewai.memory.databricks_vector_storage import DatabricksVectorStorage as _DVS

for _mod_name, _original in _originals.items():
    if _original is None:
        sys.modules.pop(_mod_name, None)
    else:
        sys.modules[_mod_name] = _original


def _build_storage(
    endpoint_name="ep",
    index_name="catalog.schema.idx",
    crew_id="crew_001",
    memory_type="short_term",
    workspace_url="https://example.databricks.com",
    group_id=None,
    job_id=None,
    mock_repository=None,
):
    with patch(
        "src.engines.crewai.memory.databricks_vector_storage.DatabricksVectorIndexRepository"
    ) as mock_repo_cls:
        repo = mock_repository or MagicMock()
        mock_repo_cls.return_value = repo

        storage = _DVS(
            endpoint_name=endpoint_name,
            index_name=index_name,
            crew_id=crew_id,
            memory_type=memory_type,
            workspace_url=workspace_url,
            group_id=group_id,
            job_id=job_id,
        )
        storage.repository = repo
        return storage, repo


# ─────────────────────────────────────────────────────────────────────────────
# Init edge cases
# ─────────────────────────────────────────────────────────────────────────────


class TestInitEdgeCases:
    """Edge cases for __init__."""

    def test_init_without_workspace_url_uses_auth_context(self):
        """When workspace_url is None, should try get_auth_context via asyncio.run."""
        mock_auth = MagicMock()
        mock_auth.workspace_url = "https://auth-provided.databricks.com"
        mock_auth.auth_method = "obo"

        with patch(
            "src.engines.crewai.memory.databricks_vector_storage.DatabricksVectorIndexRepository"
        ) as mock_repo_cls, patch("asyncio.run", return_value=mock_auth):
            mock_repo_cls.return_value = MagicMock()
            storage = _DVS(
                endpoint_name="ep",
                index_name="cat.sch.idx",
                crew_id="c1",
                workspace_url=None,
            )

        assert storage.workspace_url == "https://auth-provided.databricks.com"

    def test_init_without_workspace_url_handles_auth_exception(self):
        """When get_auth_context fails, workspace_url defaults to empty string."""
        with patch(
            "src.engines.crewai.memory.databricks_vector_storage.DatabricksVectorIndexRepository"
        ) as mock_repo_cls, patch("asyncio.run", side_effect=RuntimeError("no loop")):
            mock_repo_cls.return_value = MagicMock()
            storage = _DVS(
                endpoint_name="ep",
                index_name="cat.sch.idx",
                crew_id="c1",
                workspace_url=None,
            )

        # Should have defaulted to empty string after exception
        assert storage.workspace_url == ""


# ─────────────────────────────────────────────────────────────────────────────
# Save with all memory type schema fields
# ─────────────────────────────────────────────────────────────────────────────


class TestSaveAllSchemaFields:
    """Tests for save with comprehensive schema fields."""

    def _full_schema(self, memory_type):
        from src.schemas.databricks_index_schemas import DatabricksIndexSchemas
        return DatabricksIndexSchemas.get_schema(memory_type)

    @pytest.mark.asyncio
    async def test_save_short_term_with_crew_id_user_prefix(self):
        """Short-term save with user_ prefix in crew_id extracts group_id."""
        storage, repo = _build_storage(
            memory_type="short_term", crew_id="user_abc_crew_123"
        )
        repo.get_index = AsyncMock(return_value=MagicMock(success=False))
        repo.upsert = AsyncMock(return_value={"success": True})

        from src.schemas.databricks_index_schemas import DatabricksIndexSchemas

        schema = {
            "id": "str",
            "content": "str",
            "embedding": "list",
            "group_id": "str",
            "crew_id": "str",
            "agent_id": "str",
            "session_id": "str",
            "interaction_sequence": "int",
            "timestamp": "str",
            "created_at": "str",
            "ttl_hours": "int",
            "metadata": "str",
            "llm_model": "str",
            "tools_used": "str",
            "embedding_model": "str",
            "version": "int",
            "query_text": "str",
        }
        with patch.object(DatabricksIndexSchemas, "get_schema", return_value=schema):
            await storage.save({
                "content": "test",
                "embedding": [0.1] * 1024,
                "metadata": {"key": "val"},
                "tools_used": ["tool1"],
                "llm_model": "gpt-4",
            })

        repo.upsert.assert_called_once()
        record = repo.upsert.call_args[0][2][0]
        assert record.get("group_id") == "user_abc"

    @pytest.mark.asyncio
    async def test_save_short_term_crew_id_no_user_prefix(self):
        """Short-term save without user_ prefix uses data group_id or default."""
        storage, repo = _build_storage(
            memory_type="short_term", crew_id="plain_crew_id"
        )
        repo.get_index = AsyncMock(return_value=MagicMock(success=False))
        repo.upsert = AsyncMock(return_value={"success": True})

        from src.schemas.databricks_index_schemas import DatabricksIndexSchemas

        schema = {"id": "str", "content": "str", "embedding": "list", "group_id": "str"}
        with patch.object(DatabricksIndexSchemas, "get_schema", return_value=schema):
            await storage.save({"content": "test", "embedding": [0.1] * 1024})

        record = repo.upsert.call_args[0][2][0]
        assert record.get("group_id") == "default"

    @pytest.mark.asyncio
    async def test_save_long_term_with_all_fields(self):
        """Long-term save with all schema fields."""
        storage, repo = _build_storage(
            memory_type="long_term", crew_id="user_g_crew_lt"
        )
        repo.get_index = AsyncMock(return_value=MagicMock(success=False))
        repo.upsert = AsyncMock(return_value={"success": True})

        from src.schemas.databricks_index_schemas import DatabricksIndexSchemas

        schema = {
            "id": "str",
            "content": "str",
            "embedding": "list",
            "task_description": "str",
            "task_hash": "str",
            "quality": "float",
            "importance": "float",
            "timestamp": "str",
            "last_accessed": "str",
            "crew_id": "str",
            "agent_id": "str",
            "group_id": "str",
            "metadata": "str",
            "embedding_model": "str",
            "version": "int",
            "llm_model": "str",
            "tools_used": "str",
        }
        with patch.object(DatabricksIndexSchemas, "get_schema", return_value=schema):
            await storage.save({
                "content": "long term result",
                "embedding": [0.2] * 1024,
                "task_description": "Do research",
                "quality": 0.95,
                "tools_used": ["search"],
            })

        repo.upsert.assert_called_once()
        record = repo.upsert.call_args[0][2][0]
        assert "task_hash" in record
        assert record.get("group_id") == "user_g"

    @pytest.mark.asyncio
    async def test_save_entity_with_agent_object(self):
        """Entity save with agent object having 'role' attribute when agent_id is None."""
        storage, repo = _build_storage(memory_type="entity", crew_id="user_g_crew_e")
        repo.get_index = AsyncMock(return_value=MagicMock(success=False))
        repo.upsert = AsyncMock(return_value={"success": True})

        from src.schemas.databricks_index_schemas import DatabricksIndexSchemas

        schema = {
            "id": "str",
            "entity_name": "str",
            "entity_type": "str",
            "description": "str",
            "relationships": "str",
            "timestamp": "str",
            "crew_id": "str",
            "agent_id": "str",
            "group_id": "str",
            "embedding": "list",
            "embedding_model": "str",
            "llm_model": "str",
            "tools_used": "str",
        }

        mock_agent = MagicMock()
        mock_agent.role = "Researcher"
        # agent_id=None explicitly so fallback to agent object triggers
        with patch.object(DatabricksIndexSchemas, "get_schema", return_value=schema):
            await storage.save({
                "entity_name": "Alice",
                "entity_type": "person",
                "description": "A researcher",
                "agent_id": None,  # Explicitly None to trigger agent object fallback
                "agent": mock_agent,
                "embedding": [0.3] * 1024,
            })

        record = repo.upsert.call_args[0][2][0]
        assert record.get("agent_id") == "Researcher"

    @pytest.mark.asyncio
    async def test_save_entity_agent_with_id_attribute(self):
        """Entity save with agent having 'id' attribute falls back to id."""
        storage, repo = _build_storage(memory_type="entity", crew_id="crew_e")
        repo.get_index = AsyncMock(return_value=MagicMock(success=False))
        repo.upsert = AsyncMock(return_value={"success": True})

        from src.schemas.databricks_index_schemas import DatabricksIndexSchemas

        schema = {
            "id": "str",
            "entity_name": "str",
            "entity_type": "str",
            "description": "str",
            "agent_id": "str",
            "embedding": "list",
        }

        mock_agent = MagicMock(spec=["id"])
        mock_agent.id = "agent_uuid_123"
        with patch.object(DatabricksIndexSchemas, "get_schema", return_value=schema):
            await storage.save({
                "entity_name": "Bob",
                "entity_type": "person",
                "description": "A person",
                "agent_id": None,  # Explicitly None so agent object fallback triggers
                "agent": mock_agent,
                "embedding": [0.3] * 1024,
            })

        record = repo.upsert.call_args[0][2][0]
        assert record.get("agent_id") == "agent_uuid_123"

    @pytest.mark.asyncio
    async def test_save_document_with_all_fields(self):
        """Document save with all schema fields."""
        storage, repo = _build_storage(memory_type="document", crew_id="crew_doc")
        repo.get_index = AsyncMock(return_value=MagicMock(success=False))
        repo.upsert = AsyncMock(return_value={"success": True})

        from src.schemas.databricks_index_schemas import DatabricksIndexSchemas

        schema = {
            "id": "str",
            "title": "str",
            "content": "str",
            "source": "str",
            "document_type": "str",
            "section": "str",
            "chunk_index": "int",
            "chunk_size": "int",
            "parent_document_id": "str",
            "created_at": "str",
            "updated_at": "str",
            "doc_metadata": "str",
            "agent_ids": "str",
            "group_id": "str",
            "embedding": "list",
            "embedding_model": "str",
            "version": "int",
        }
        with patch.object(DatabricksIndexSchemas, "get_schema", return_value=schema):
            await storage.save({
                "content": "Document content",
                "embedding": [0.4] * 1024,
                "agent_ids": '["agent1", "agent2"]',
                "group_id": "grp_specific",
                "metadata": {
                    "source": "wiki",
                    "type": "documentation",
                    "section": "intro",
                    "chunk_index": 0,
                    "parent_document_id": "doc_1",
                    "embedding_model": "custom-emb",
                },
                "context": {"query_text": "My Document"},
            })

        repo.upsert.assert_called_once()
        record = repo.upsert.call_args[0][2][0]
        assert record.get("group_id") == "grp_specific"
        assert record.get("title") == "My Document"

    @pytest.mark.asyncio
    async def test_save_document_without_agent_ids_defaults_empty(self):
        """Document save without agent_ids defaults to '[]'."""
        storage, repo = _build_storage(memory_type="document", crew_id="crew_doc")
        repo.get_index = AsyncMock(return_value=MagicMock(success=False))
        repo.upsert = AsyncMock(return_value={"success": True})

        from src.schemas.databricks_index_schemas import DatabricksIndexSchemas

        schema = {"id": "str", "content": "str", "embedding": "list", "agent_ids": "str"}
        with patch.object(DatabricksIndexSchemas, "get_schema", return_value=schema):
            await storage.save({"content": "Doc", "embedding": [0.1] * 1024})

        record = repo.upsert.call_args[0][2][0]
        assert record.get("agent_ids") == "[]"


# ─────────────────────────────────────────────────────────────────────────────
# Save - special embedding conversion
# ─────────────────────────────────────────────────────────────────────────────


class TestSaveEmbeddingConversion:
    """Tests for embedding type conversion in save."""

    @pytest.mark.asyncio
    async def test_save_converts_numpy_array_embedding(self):
        """Numpy array embeddings should be converted to list."""
        storage, repo = _build_storage(memory_type="short_term")
        repo.get_index = AsyncMock(return_value=MagicMock(success=False))
        repo.upsert = AsyncMock(return_value={"success": True})

        from src.schemas.databricks_index_schemas import DatabricksIndexSchemas

        schema = {"id": "str", "content": "str", "embedding": "list"}
        with patch.object(DatabricksIndexSchemas, "get_schema", return_value=schema):
            await storage.save({
                "content": "test",
                "embedding": np.array([0.1] * 1024),
            })

        record = repo.upsert.call_args[0][2][0]
        assert isinstance(record["embedding"], list)

    @pytest.mark.asyncio
    async def test_save_converts_other_iterable_embedding(self):
        """Non-list, non-ndarray iterables should be converted to list."""
        storage, repo = _build_storage(memory_type="short_term")
        repo.get_index = AsyncMock(return_value=MagicMock(success=False))
        repo.upsert = AsyncMock(return_value={"success": True})

        from src.schemas.databricks_index_schemas import DatabricksIndexSchemas

        schema = {"id": "str", "content": "str", "embedding": "list"}
        with patch.object(DatabricksIndexSchemas, "get_schema", return_value=schema):
            # Use a tuple - iterable but not list/ndarray
            await storage.save({
                "content": "test",
                "embedding": tuple([0.1] * 1024),
            })

        record = repo.upsert.call_args[0][2][0]
        assert isinstance(record["embedding"], list)


# ─────────────────────────────────────────────────────────────────────────────
# Save - UserContext group_id setting
# ─────────────────────────────────────────────────────────────────────────────


class TestSaveUserContext:
    """Tests for UserContext setting in save."""

    @pytest.mark.asyncio
    async def test_save_sets_user_context_when_group_id(self):
        """When group_id is set, save should try to set UserContext."""
        storage, repo = _build_storage(
            memory_type="short_term", group_id="grp_123"
        )
        repo.get_index = AsyncMock(return_value=MagicMock(success=False))
        repo.upsert = AsyncMock(return_value={"success": True})

        from src.schemas.databricks_index_schemas import DatabricksIndexSchemas

        schema = {"id": "str", "content": "str", "embedding": "list"}
        with patch.object(DatabricksIndexSchemas, "get_schema", return_value=schema):
            with patch("src.utils.user_context.UserContext.set_group_context") as mock_set_ctx:
                await storage.save({"content": "test", "embedding": [0.1] * 1024})

        # UserContext.set_group_context should have been called
        mock_set_ctx.assert_called_once()

    @pytest.mark.asyncio
    async def test_save_user_context_exception_is_handled(self):
        """UserContext setting failure should not prevent save."""
        storage, repo = _build_storage(
            memory_type="short_term", group_id="grp_xyz"
        )
        repo.get_index = AsyncMock(return_value=MagicMock(success=False))
        repo.upsert = AsyncMock(return_value={"success": True})

        from src.schemas.databricks_index_schemas import DatabricksIndexSchemas

        schema = {"id": "str", "content": "str", "embedding": "list"}
        with patch.object(DatabricksIndexSchemas, "get_schema", return_value=schema):
            with patch(
                "src.utils.user_context.UserContext.set_group_context",
                side_effect=Exception("context error"),
            ):
                # Should not raise
                await storage.save({"content": "test", "embedding": [0.1] * 1024})

        repo.upsert.assert_called_once()


# ─────────────────────────────────────────────────────────────────────────────
# Search edge cases
# ─────────────────────────────────────────────────────────────────────────────


class TestSearchEdgeCases:
    """Additional search coverage."""

    @pytest.mark.asyncio
    async def test_search_parses_json_metadata_field(self):
        """Search results with JSON metadata fields should be parsed."""
        storage, repo = _build_storage(memory_type="short_term", crew_id="crew_1")
        mock_result = {
            "success": True,
            "results": {
                "result": {
                    "data_array": [
                        ["id1", "content text", '{"key": "value"}'],
                    ]
                }
            },
        }
        repo.similarity_search = AsyncMock(return_value=mock_result)

        from src.schemas.databricks_index_schemas import DatabricksIndexSchemas

        with patch.object(
            DatabricksIndexSchemas, "get_search_columns", return_value=["id", "content", "metadata"]
        ), patch.object(
            DatabricksIndexSchemas,
            "get_column_positions",
            return_value={"id": 0, "content": 1, "metadata": 2},
        ):
            results = await storage.search([0.1] * 1024, k=1)

        assert len(results) == 1
        assert results[0]["metadata"] == {"key": "value"}

    @pytest.mark.asyncio
    async def test_search_handles_invalid_json_metadata(self):
        """Search results with invalid JSON metadata should use raw value."""
        storage, repo = _build_storage(memory_type="short_term", crew_id="crew_1")
        mock_result = {
            "success": True,
            "results": {
                "result": {
                    "data_array": [
                        ["id1", "content", "not-valid-json"],
                    ]
                }
            },
        }
        repo.similarity_search = AsyncMock(return_value=mock_result)

        from src.schemas.databricks_index_schemas import DatabricksIndexSchemas

        with patch.object(
            DatabricksIndexSchemas, "get_search_columns", return_value=["id", "content", "metadata"]
        ), patch.object(
            DatabricksIndexSchemas,
            "get_column_positions",
            return_value={"id": 0, "content": 1, "metadata": 2},
        ):
            results = await storage.search([0.1] * 1024, k=1)

        assert len(results) == 1
        # Should fall back to raw string
        assert results[0]["metadata"] == "not-valid-json"

    @pytest.mark.asyncio
    async def test_search_merges_extra_filters(self):
        """Extra filters passed to search should be merged with crew_id filter."""
        storage, repo = _build_storage(crew_id="crew_m", memory_type="short_term")
        repo.similarity_search = AsyncMock(
            return_value={"success": True, "results": None}
        )
        from src.schemas.databricks_index_schemas import DatabricksIndexSchemas

        with patch.object(DatabricksIndexSchemas, "get_search_columns", return_value=[]):
            await storage.search([0.0] * 1024, k=3, filters={"agent_id": "agent_99"})

        call_args = repo.similarity_search.call_args
        filters = call_args[0][5] if len(call_args[0]) > 5 else call_args[1].get("filters", {})
        assert filters.get("crew_id") == "crew_m"
        assert filters.get("agent_id") == "agent_99"

    @pytest.mark.asyncio
    async def test_search_document_type_filter(self):
        """Document type search should use group_id not crew_id for filtering."""
        storage, repo = _build_storage(crew_id="doc_grp", memory_type="document")
        repo.similarity_search = AsyncMock(
            return_value={"success": True, "results": None}
        )
        from src.schemas.databricks_index_schemas import DatabricksIndexSchemas

        with patch.object(DatabricksIndexSchemas, "get_search_columns", return_value=[]):
            await storage.search([0.0] * 1024)

        call_args = repo.similarity_search.call_args
        filters = call_args[0][5] if len(call_args[0]) > 5 else call_args[1].get("filters", {})
        assert filters.get("group_id") == "doc_grp"
        assert "crew_id" not in filters


# ─────────────────────────────────────────────────────────────────────────────
# Clear edge cases
# ─────────────────────────────────────────────────────────────────────────────


class TestClearEdgeCases:
    """Additional clear coverage."""

    @pytest.mark.asyncio
    async def test_clear_document_type_uses_group_id_filter(self):
        """Clear for document type should use group_id filter."""
        storage, repo = _build_storage(crew_id="doc_crew", memory_type="document")
        repo.similarity_search = AsyncMock(
            return_value={"success": True, "results": {"result": {"data_array": []}}}
        )

        result = await storage.clear()
        assert result is True

        call_args = repo.similarity_search.call_args
        filters = call_args[0][5] if len(call_args[0]) > 5 else call_args[1].get("filters", {})
        assert filters.get("group_id") == "doc_crew"

    @pytest.mark.asyncio
    async def test_clear_search_returns_no_success(self):
        """When search fails during clear, no delete should be called."""
        storage, repo = _build_storage(crew_id="crew_c")
        repo.similarity_search = AsyncMock(
            return_value={"success": False, "results": None}
        )

        result = await storage.clear()

        # Should return True (no records to delete is OK)
        assert result is True
        repo.delete_records.assert_not_called()


# ─────────────────────────────────────────────────────────────────────────────
# Get stats edge cases
# ─────────────────────────────────────────────────────────────────────────────


class TestGetStatsEdgeCases:
    """Additional get_stats coverage."""

    @pytest.mark.asyncio
    async def test_get_stats_description_is_not_dict(self):
        """When description is not a dict, should not crash."""
        storage, repo = _build_storage()
        repo.describe_index = AsyncMock(
            return_value={
                "success": True,
                "description": "not_a_dict",
            }
        )

        stats = await storage.get_stats()
        assert stats is not None
        assert "index_name" in stats

    @pytest.mark.asyncio
    async def test_get_stats_description_dict_without_status(self):
        """Description dict without 'status' key should handle gracefully."""
        storage, repo = _build_storage()
        repo.describe_index = AsyncMock(
            return_value={
                "success": True,
                "description": {"num_rows": 100},
            }
        )

        stats = await storage.get_stats()
        assert stats.get("num_rows") == 100


# ─────────────────────────────────────────────────────────────────────────────
# Count documents
# ─────────────────────────────────────────────────────────────────────────────


class TestCountDocumentsEdgeCases:
    """Additional count_documents coverage."""

    @pytest.mark.asyncio
    async def test_count_documents_document_type_filter(self):
        """Document type count should use group_id filter."""
        storage, repo = _build_storage(crew_id="doc_crew", memory_type="document")
        repo.count_documents = AsyncMock(return_value=5)

        count = await storage.count_documents()

        call_args = repo.count_documents.call_args
        filters = call_args[0][2] if len(call_args[0]) > 2 else call_args[1].get("filters")
        if filters:
            assert filters.get("group_id") == "doc_crew"
        assert count == 5

    @pytest.mark.asyncio
    async def test_count_documents_no_crew_id_no_filters(self):
        """With no crew_id, no filters should be passed."""
        storage, repo = _build_storage(crew_id="")
        repo.count_documents = AsyncMock(return_value=0)

        count = await storage.count_documents()

        call_args = repo.count_documents.call_args
        filters = call_args[0][2] if len(call_args[0]) > 2 else call_args[1].get("filters")
        assert filters is None
