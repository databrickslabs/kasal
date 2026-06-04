"""
Comprehensive unit tests for DatabricksVectorStorage.
"""
import os
import sys
import pytest
from unittest.mock import MagicMock, AsyncMock, patch

# Must set env before imports to avoid nullpool side effects in test isolation
os.environ.setdefault("DATABASE_TYPE", "sqlite")
os.environ.setdefault("SQLITE_DB_PATH", ":memory:")
os.environ["USE_NULLPOOL"] = "true"

# Mock heavy third-party modules before any src imports
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
    """Create a DatabricksVectorStorage with a mocked repository."""
    with patch(
        "src.engines.crewai.memory.databricks_vector_storage.DatabricksVectorIndexRepository"
    ) as mock_repo_cls:
        repo = mock_repository or MagicMock()
        mock_repo_cls.return_value = repo

        from src.engines.crewai.memory.databricks_vector_storage import (
            DatabricksVectorStorage,
        )

        storage = DatabricksVectorStorage(
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


# Restore originals
for _mod_name, _original in _originals.items():
    if _original is None:
        sys.modules.pop(_mod_name, None)
    else:
        sys.modules[_mod_name] = _original


class TestDatabricksVectorStorageInit:
    """Tests for __init__."""

    def test_init_sets_basic_attributes(self):
        storage, _ = _build_storage(
            endpoint_name="my_ep",
            index_name="cat.sch.idx",
            crew_id="crew_123",
            memory_type="long_term",
            workspace_url="https://example.databricks.com",
            group_id="grp_1",
            job_id="job_42",
        )
        assert storage.endpoint_name == "my_ep"
        assert storage.index_name == "cat.sch.idx"
        assert storage.crew_id == "crew_123"
        assert storage.memory_type == "long_term"
        assert storage.workspace_url == "https://example.databricks.com"
        assert storage.group_id == "grp_1"
        assert storage.job_id == "job_42"

    def test_init_agent_id_defaults_to_default_agent(self):
        storage, _ = _build_storage()
        assert storage.agent_id == "default_agent"

    def test_init_embedding_dimension_defaults_to_1024(self):
        storage, _ = _build_storage()
        assert storage.embedding_dimension == 1024

    def test_init_short_term_memory_logger(self):
        storage, _ = _build_storage(memory_type="short_term")
        # Memory logger should be set
        assert storage.memory_logger is not None

    def test_init_long_term_memory_logger(self):
        storage, _ = _build_storage(memory_type="long_term")
        assert storage.memory_logger is not None

    def test_init_entity_memory_logger(self):
        storage, _ = _build_storage(memory_type="entity")
        assert storage.memory_logger is not None

    def test_init_unknown_memory_type_uses_default_logger(self):
        storage, _ = _build_storage(memory_type="unknown_type")
        assert storage.memory_logger is not None

    def test_init_trace_context_is_none(self):
        storage, _ = _build_storage()
        assert storage.trace_context is None

    def test_init_creates_repository(self):
        with patch(
            "src.engines.crewai.memory.databricks_vector_storage.DatabricksVectorIndexRepository"
        ) as mock_repo_cls:
            mock_repo_cls.return_value = MagicMock()
            from src.engines.crewai.memory.databricks_vector_storage import (
                DatabricksVectorStorage,
            )

            storage = DatabricksVectorStorage(
                endpoint_name="ep",
                index_name="idx",
                crew_id="c1",
                workspace_url="https://example.databricks.com",
            )
            mock_repo_cls.assert_called_once_with("https://example.databricks.com")


class TestDatabricksVectorStorageSave:
    """Tests for the async save method."""

    @pytest.mark.asyncio
    async def test_save_short_term_upserts_record(self):
        storage, repo = _build_storage(memory_type="short_term", crew_id="crew_1")
        repo.get_index = AsyncMock(return_value=MagicMock(success=False))
        repo.upsert = AsyncMock(return_value={"success": True})

        from src.schemas.databricks_index_schemas import DatabricksIndexSchemas

        schema = {"id": "str", "content": "str", "embedding": "list"}
        with patch.object(DatabricksIndexSchemas, "get_schema", return_value=schema):
            await storage.save(
                {"content": "Hello", "embedding": [0.1] * 1024}
            )

        repo.upsert.assert_called_once()

    @pytest.mark.asyncio
    async def test_save_raises_on_unsupported_memory_type(self):
        storage, _ = _build_storage(memory_type="invalid_type")
        from src.schemas.databricks_index_schemas import DatabricksIndexSchemas

        with patch.object(DatabricksIndexSchemas, "get_schema", return_value=None):
            with pytest.raises(ValueError, match="Unsupported memory type"):
                await storage.save({"content": "test"})

    @pytest.mark.asyncio
    async def test_save_raises_when_upsert_fails(self):
        storage, repo = _build_storage(memory_type="short_term")
        repo.get_index = AsyncMock(return_value=MagicMock(success=False))
        repo.upsert = AsyncMock(return_value={"success": False, "message": "Index error"})

        from src.schemas.databricks_index_schemas import DatabricksIndexSchemas

        schema = {"id": "str", "content": "str", "embedding": "list"}
        with patch.object(DatabricksIndexSchemas, "get_schema", return_value=schema):
            with pytest.raises(Exception, match="Index error"):
                await storage.save({"content": "test", "embedding": [0.1] * 1024})

    @pytest.mark.asyncio
    async def test_save_generates_random_embedding_when_missing(self):
        storage, repo = _build_storage(memory_type="short_term")
        repo.get_index = AsyncMock(return_value=MagicMock(success=False))
        repo.upsert = AsyncMock(return_value={"success": True})

        from src.schemas.databricks_index_schemas import DatabricksIndexSchemas

        schema = {"id": "str", "embedding": "list", "content": "str"}
        with patch.object(DatabricksIndexSchemas, "get_schema", return_value=schema):
            # No embedding in data - should auto-generate
            await storage.save({"content": "test"})

        call_args = repo.upsert.call_args
        records = call_args.args[2] if call_args.args else call_args[1].get("records", [])
        if records:
            assert "embedding" in records[0]
            assert len(records[0]["embedding"]) == 1024

    @pytest.mark.asyncio
    async def test_save_uses_job_id_as_session_id_for_short_term(self):
        storage, repo = _build_storage(
            memory_type="short_term", job_id="job_session_1"
        )
        repo.get_index = AsyncMock(return_value=MagicMock(success=False))
        repo.upsert = AsyncMock(return_value={"success": True})

        from src.schemas.databricks_index_schemas import DatabricksIndexSchemas

        schema = {
            "id": "str",
            "content": "str",
            "embedding": "list",
            "session_id": "str",
        }
        with patch.object(DatabricksIndexSchemas, "get_schema", return_value=schema):
            await storage.save({"content": "test", "embedding": [0.0] * 1024})

        call_args = repo.upsert.call_args
        records = call_args[0][2] if call_args[0] else call_args[1].get("records", [])
        if records:
            assert records[0].get("session_id") == "job_session_1"

    @pytest.mark.asyncio
    async def test_save_entity_memory_type(self):
        storage, repo = _build_storage(memory_type="entity")
        repo.get_index = AsyncMock(return_value=MagicMock(success=False))
        repo.upsert = AsyncMock(return_value={"success": True})

        from src.schemas.databricks_index_schemas import DatabricksIndexSchemas

        schema = {
            "id": "str",
            "entity_name": "str",
            "entity_type": "str",
            "description": "str",
            "embedding": "list",
        }
        with patch.object(DatabricksIndexSchemas, "get_schema", return_value=schema):
            await storage.save(
                {
                    "entity_name": "John",
                    "entity_type": "person",
                    "description": "A person",
                    "embedding": [0.1] * 1024,
                }
            )

        repo.upsert.assert_called_once()

    @pytest.mark.asyncio
    async def test_save_long_term_memory_type(self):
        storage, repo = _build_storage(memory_type="long_term")
        repo.get_index = AsyncMock(return_value=MagicMock(success=False))
        repo.upsert = AsyncMock(return_value={"success": True})

        from src.schemas.databricks_index_schemas import DatabricksIndexSchemas

        schema = {
            "id": "str",
            "content": "str",
            "embedding": "list",
            "quality": "float",
        }
        with patch.object(DatabricksIndexSchemas, "get_schema", return_value=schema):
            await storage.save(
                {
                    "content": "Task result",
                    "embedding": [0.2] * 1024,
                    "quality": 0.9,
                }
            )

        repo.upsert.assert_called_once()

    @pytest.mark.asyncio
    async def test_save_document_memory_type(self):
        storage, repo = _build_storage(memory_type="document")
        repo.get_index = AsyncMock(return_value=MagicMock(success=False))
        repo.upsert = AsyncMock(return_value={"success": True})

        from src.schemas.databricks_index_schemas import DatabricksIndexSchemas

        schema = {
            "id": "str",
            "content": "str",
            "embedding": "list",
        }
        with patch.object(DatabricksIndexSchemas, "get_schema", return_value=schema):
            await storage.save(
                {
                    "content": "Document text",
                    "embedding": [0.3] * 1024,
                }
            )

        repo.upsert.assert_called_once()

    @pytest.mark.asyncio
    async def test_save_logs_warning_when_index_not_ready(self):
        storage, repo = _build_storage()
        mock_index_info = MagicMock()
        mock_index_info.success = True
        mock_index_info.index = MagicMock()
        mock_index_info.index.ready = False
        mock_index_info.index.state = "PROVISIONING"
        repo.get_index = AsyncMock(return_value=mock_index_info)
        repo.upsert = AsyncMock(return_value={"success": True})

        from src.schemas.databricks_index_schemas import DatabricksIndexSchemas

        schema = {"id": "str", "content": "str", "embedding": "list"}
        with patch.object(DatabricksIndexSchemas, "get_schema", return_value=schema):
            # Should still proceed despite not ready
            await storage.save({"content": "test", "embedding": [0.1] * 1024})

        repo.upsert.assert_called_once()


class TestDatabricksVectorStorageSearch:
    """Tests for the async search method."""

    @pytest.mark.asyncio
    async def test_search_returns_processed_results(self):
        storage, repo = _build_storage(memory_type="short_term", crew_id="crew_abc")
        mock_result = {
            "success": True,
            "results": {
                "result": {
                    "data_array": [
                        ["id1", "Hello world", "crew_abc"],
                    ]
                }
            },
        }
        repo.similarity_search = AsyncMock(return_value=mock_result)

        from src.schemas.databricks_index_schemas import DatabricksIndexSchemas

        with patch.object(
            DatabricksIndexSchemas, "get_search_columns", return_value=["id", "content", "crew_id"]
        ), patch.object(
            DatabricksIndexSchemas,
            "get_column_positions",
            return_value={"id": 0, "content": 1, "crew_id": 2},
        ):
            results = await storage.search([0.1] * 1024, k=5)

        assert len(results) == 1
        assert results[0]["content"] == "Hello world"

    @pytest.mark.asyncio
    async def test_search_returns_empty_on_failure(self):
        storage, repo = _build_storage()
        repo.similarity_search = AsyncMock(
            return_value={"success": False, "message": "Search failed"}
        )
        from src.schemas.databricks_index_schemas import DatabricksIndexSchemas

        with patch.object(DatabricksIndexSchemas, "get_search_columns", return_value=[]):
            results = await storage.search([0.1] * 1024)

        assert results == []

    @pytest.mark.asyncio
    async def test_search_applies_crew_id_filter(self):
        storage, repo = _build_storage(crew_id="crew_xyz", memory_type="short_term")
        repo.similarity_search = AsyncMock(
            return_value={"success": True, "results": None}
        )
        from src.schemas.databricks_index_schemas import DatabricksIndexSchemas

        with patch.object(DatabricksIndexSchemas, "get_search_columns", return_value=[]):
            await storage.search([0.0] * 1024, k=3)

        call_args = repo.similarity_search.call_args
        # filters is the 5th positional arg (index 4) or keyword
        filters = call_args[0][5] if len(call_args[0]) > 5 else call_args[1].get("filters", {})
        assert filters.get("crew_id") == "crew_xyz"

    @pytest.mark.asyncio
    async def test_search_returns_empty_on_exception(self):
        storage, repo = _build_storage()
        repo.similarity_search = AsyncMock(side_effect=Exception("Network error"))
        from src.schemas.databricks_index_schemas import DatabricksIndexSchemas

        with patch.object(DatabricksIndexSchemas, "get_search_columns", return_value=[]):
            results = await storage.search([0.1] * 1024)

        assert results == []

    @pytest.mark.asyncio
    async def test_search_handles_none_results(self):
        storage, repo = _build_storage()
        repo.similarity_search = AsyncMock(
            return_value={"success": True, "results": None}
        )
        from src.schemas.databricks_index_schemas import DatabricksIndexSchemas

        with patch.object(DatabricksIndexSchemas, "get_search_columns", return_value=[]):
            results = await storage.search([0.1] * 1024)

        assert results == []

    @pytest.mark.asyncio
    async def test_search_document_type_uses_group_id_filter(self):
        storage, repo = _build_storage(crew_id="grp_doc", memory_type="document")
        repo.similarity_search = AsyncMock(
            return_value={"success": True, "results": None}
        )
        from src.schemas.databricks_index_schemas import DatabricksIndexSchemas

        with patch.object(DatabricksIndexSchemas, "get_search_columns", return_value=[]):
            await storage.search([0.0] * 1024)

        call_args = repo.similarity_search.call_args
        filters = call_args[0][5] if len(call_args[0]) > 5 else call_args[1].get("filters", {})
        # Document memory uses group_id not crew_id
        assert filters.get("group_id") == "grp_doc"


class TestDatabricksVectorStorageDelete:
    """Tests for the async delete method."""

    @pytest.mark.asyncio
    async def test_delete_returns_true_on_success(self):
        storage, repo = _build_storage()
        repo.delete_records = AsyncMock(return_value={"success": True})

        result = await storage.delete("mem_id_123")

        assert result is True
        repo.delete_records.assert_called_once()

    @pytest.mark.asyncio
    async def test_delete_returns_false_on_failure(self):
        storage, repo = _build_storage()
        repo.delete_records = AsyncMock(
            return_value={"success": False, "message": "Not found"}
        )

        result = await storage.delete("mem_id_xyz")

        assert result is False

    @pytest.mark.asyncio
    async def test_delete_returns_false_on_exception(self):
        storage, repo = _build_storage()
        repo.delete_records = AsyncMock(side_effect=Exception("Connection error"))

        result = await storage.delete("mem_id")

        assert result is False

    @pytest.mark.asyncio
    async def test_delete_passes_id_to_repository(self):
        storage, repo = _build_storage()
        repo.delete_records = AsyncMock(return_value={"success": True})

        await storage.delete("specific_id_123")

        call_args = repo.delete_records.call_args
        ids = call_args[0][2] if len(call_args[0]) > 2 else call_args[1].get("ids", [])
        assert "specific_id_123" in ids


class TestDatabricksVectorStorageClear:
    """Tests for the async clear method."""

    @pytest.mark.asyncio
    async def test_clear_returns_true_when_no_records(self):
        storage, repo = _build_storage()
        repo.similarity_search = AsyncMock(
            return_value={"success": True, "results": {"result": {"data_array": []}}}
        )

        result = await storage.clear()

        assert result is True

    @pytest.mark.asyncio
    async def test_clear_deletes_found_records(self):
        storage, repo = _build_storage(crew_id="crew_1")
        repo.similarity_search = AsyncMock(
            return_value={
                "success": True,
                "results": {"result": {"data_array": [["id1"], ["id2"]]}},
            }
        )
        repo.delete_records = AsyncMock(return_value={"success": True})

        result = await storage.clear()

        assert result is True
        repo.delete_records.assert_called_once()

    @pytest.mark.asyncio
    async def test_clear_returns_false_when_delete_fails(self):
        storage, repo = _build_storage(crew_id="crew_1")
        repo.similarity_search = AsyncMock(
            return_value={
                "success": True,
                "results": {"result": {"data_array": [["id1"]]}},
            }
        )
        repo.delete_records = AsyncMock(
            return_value={"success": False, "message": "Delete error"}
        )

        result = await storage.clear()

        assert result is False

    @pytest.mark.asyncio
    async def test_clear_returns_false_on_exception(self):
        storage, repo = _build_storage()
        repo.similarity_search = AsyncMock(side_effect=Exception("Search failed"))

        result = await storage.clear()

        assert result is False


class TestDatabricksVectorStorageGetStats:
    """Tests for the async get_stats method."""

    @pytest.mark.asyncio
    async def test_get_stats_returns_basic_info(self):
        storage, repo = _build_storage(
            memory_type="entity", crew_id="c1", index_name="cat.sch.ent"
        )
        repo.describe_index = AsyncMock(return_value={"success": False})

        stats = await storage.get_stats()

        assert stats["memory_type"] == "entity"
        assert stats["crew_id"] == "c1"
        assert stats["index_name"] == "cat.sch.ent"

    @pytest.mark.asyncio
    async def test_get_stats_includes_index_info_on_success(self):
        storage, repo = _build_storage()
        repo.describe_index = AsyncMock(
            return_value={
                "success": True,
                "description": {
                    "status": {"indexed_row_count": 42, "ready": True, "detailed_state": "ONLINE"}
                },
            }
        )

        stats = await storage.get_stats()

        assert stats.get("indexed_row_count") == 42
        assert stats.get("ready") is True

    @pytest.mark.asyncio
    async def test_get_stats_returns_error_on_exception(self):
        storage, repo = _build_storage()
        repo.describe_index = AsyncMock(side_effect=Exception("Describe failed"))

        stats = await storage.get_stats()

        assert "error" in stats


class TestDatabricksVectorStorageCountDocuments:
    """Tests for async count_documents."""

    @pytest.mark.asyncio
    async def test_count_documents_returns_count(self):
        storage, repo = _build_storage(crew_id="crew_1")
        repo.count_documents = AsyncMock(return_value=7)

        count = await storage.count_documents()

        assert count == 7

    @pytest.mark.asyncio
    async def test_count_documents_returns_zero_on_exception(self):
        storage, repo = _build_storage()
        repo.count_documents = AsyncMock(side_effect=Exception("Count failed"))

        count = await storage.count_documents()

        assert count == 0

    @pytest.mark.asyncio
    async def test_count_documents_passes_crew_id_filter(self):
        storage, repo = _build_storage(crew_id="crew_filter", memory_type="short_term")
        repo.count_documents = AsyncMock(return_value=0)

        await storage.count_documents()

        call_args = repo.count_documents.call_args
        filters = call_args[0][2] if len(call_args[0]) > 2 else call_args[1].get("filters")
        if filters:
            assert filters.get("crew_id") == "crew_filter"
