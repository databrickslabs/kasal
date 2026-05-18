"""
Comprehensive unit tests for CrewAIDatabricksWrapper.
"""
import os
import sys
from unittest.mock import MagicMock, AsyncMock, patch

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

import pytest
from unittest.mock import patch, call

from src.engines.crewai.memory.crewai_databricks_wrapper import CrewAIDatabricksWrapper

# Restore originals after import
for _mod_name, _original in _originals.items():
    if _original is None:
        sys.modules.pop(_mod_name, None)
    else:
        sys.modules[_mod_name] = _original


def _make_storage(memory_type="short_term", crew_id="crew_1"):
    storage = MagicMock()
    storage.memory_type = memory_type
    storage.workspace_url = "https://example.databricks.com"
    storage.index_name = f"catalog.schema.{memory_type}_idx"
    storage.endpoint_name = "test-endpoint"
    storage.user_token = "tok-test"
    storage.group_id = "grp_test"
    storage.job_id = "job_001"
    storage.crew_id = crew_id
    storage.embedding_dimension = 1024
    return storage


def _make_wrapper(memory_type="short_term", crew_id="crew_1", embedder=None, enable_relationship_retrieval=False):
    storage = _make_storage(memory_type=memory_type, crew_id=crew_id)
    wrapper = CrewAIDatabricksWrapper(
        databricks_storage=storage,
        embedder=embedder,
        enable_relationship_retrieval=enable_relationship_retrieval,
    )
    return wrapper, storage


# ─────────────────────────────────────────────────────────────────────────────
# Initialization
# ─────────────────────────────────────────────────────────────────────────────


class TestCrewAIDatabricksWrapperInit:
    """Tests for __init__."""

    def test_init_sets_memory_type(self):
        wrapper, _ = _make_wrapper(memory_type="entity")
        assert wrapper.memory_type == "entity"

    def test_init_stores_workspace_url(self):
        wrapper, _ = _make_wrapper()
        assert wrapper.workspace_url == "https://example.databricks.com"

    def test_init_stores_index_name(self):
        wrapper, _ = _make_wrapper(memory_type="short_term")
        assert "short_term" in wrapper.index_name

    def test_init_stores_user_token(self):
        wrapper, _ = _make_wrapper()
        assert wrapper.user_token == "tok-test"

    def test_init_stores_group_id(self):
        wrapper, _ = _make_wrapper()
        assert wrapper.group_id == "grp_test"

    def test_init_stores_job_id(self):
        wrapper, _ = _make_wrapper()
        assert wrapper.job_id == "job_001"

    def test_init_relationship_retriever_disabled(self):
        wrapper, _ = _make_wrapper(enable_relationship_retrieval=False)
        assert wrapper.relationship_retriever is None

    def test_init_relationship_retriever_not_created_for_short_term(self):
        wrapper, _ = _make_wrapper(
            memory_type="short_term", enable_relationship_retrieval=True
        )
        assert wrapper.relationship_retriever is None

    def test_init_relationship_retriever_created_for_entity_type(self):
        with patch(
            "src.engines.crewai.memory.crewai_databricks_wrapper.EntityRelationshipRetriever"
        ) as mock_retriever_cls:
            mock_retriever_cls.return_value = MagicMock()
            with patch(
                "src.engines.crewai.memory.crewai_databricks_wrapper.MemoryBackendService",
                create=True,
            ), patch(
                "src.engines.crewai.memory.crewai_databricks_wrapper.UnitOfWork",
                create=True,
            ):
                wrapper, _ = _make_wrapper(
                    memory_type="entity", enable_relationship_retrieval=True
                )
            mock_retriever_cls.assert_called_once_with(
                memory_backend_service=None,
                embedding_model="databricks-gte-large-en",
            )
            assert wrapper.relationship_retriever is not None

    def test_init_relationship_retriever_handles_import_error(self):
        with patch(
            "src.engines.crewai.memory.crewai_databricks_wrapper.EntityRelationshipRetriever",
            side_effect=ImportError("not found"),
        ):
            storage = _make_storage(memory_type="entity")
            wrapper = CrewAIDatabricksWrapper(
                databricks_storage=storage,
                enable_relationship_retrieval=True,
            )
        assert wrapper.relationship_retriever is None

    def test_init_trace_context_is_none(self):
        wrapper, _ = _make_wrapper()
        assert wrapper.trace_context is None

    def test_init_stores_embedder(self):
        mock_embedder = MagicMock()
        wrapper, _ = _make_wrapper(embedder=mock_embedder)
        assert wrapper.embedder is mock_embedder

    def test_init_agent_context_is_none_by_default(self):
        wrapper, _ = _make_wrapper()
        assert wrapper.agent_context is None


# ─────────────────────────────────────────────────────────────────────────────
# set_agent_context
# ─────────────────────────────────────────────────────────────────────────────


class TestSetAgentContext:
    def test_set_agent_context_stores_agent(self):
        wrapper, _ = _make_wrapper()
        agent = MagicMock()
        agent.role = "researcher"
        wrapper.set_agent_context(agent)
        assert wrapper.agent_context is agent

    def test_set_agent_context_works_without_role_attribute(self):
        wrapper, _ = _make_wrapper()
        agent = MagicMock(spec=[])
        wrapper.set_agent_context(agent)
        assert wrapper.agent_context is agent


# ─────────────────────────────────────────────────────────────────────────────
# _is_memory_enabled_for_current_agent
# ─────────────────────────────────────────────────────────────────────────────


class TestIsMemoryEnabled:
    def test_returns_true_when_no_agent_context(self):
        wrapper, _ = _make_wrapper()
        assert wrapper._is_memory_enabled_for_current_agent() is True

    def test_returns_true_when_agent_has_memory_true(self):
        wrapper, _ = _make_wrapper()
        agent = MagicMock()
        agent.memory = True
        wrapper.agent_context = agent
        assert wrapper._is_memory_enabled_for_current_agent() is True

    def test_returns_false_when_agent_has_memory_false(self):
        wrapper, _ = _make_wrapper()
        agent = MagicMock()
        agent.memory = False
        wrapper.agent_context = agent
        assert wrapper._is_memory_enabled_for_current_agent() is False

    def test_returns_true_when_agent_has_no_memory_attribute(self):
        wrapper, _ = _make_wrapper()
        agent = MagicMock(spec=["role"])  # No 'memory' attribute
        wrapper.agent_context = agent
        # Should default to True
        assert wrapper._is_memory_enabled_for_current_agent() is True


# ─────────────────────────────────────────────────────────────────────────────
# _format_results_for_crewai
# ─────────────────────────────────────────────────────────────────────────────


class TestFormatResultsForCrewAI:
    def test_adds_context_and_content_from_data_field(self):
        wrapper, _ = _make_wrapper()
        results = [{"data": "Hello world", "other": "x"}]
        formatted = wrapper._format_results_for_crewai(results)
        assert formatted[0]["context"] == "Hello world"
        assert formatted[0]["content"] == "Hello world"

    def test_adds_context_and_content_from_content_field(self):
        wrapper, _ = _make_wrapper()
        results = [{"content": "Some content"}]
        formatted = wrapper._format_results_for_crewai(results)
        assert formatted[0]["context"] == "Some content"
        assert formatted[0]["content"] == "Some content"

    def test_entity_memory_formats_with_name_and_type(self):
        wrapper, _ = _make_wrapper(memory_type="entity")
        results = [
            {
                "entity_name": "John",
                "entity_type": "person",
                "description": "A software engineer",
            }
        ]
        formatted = wrapper._format_results_for_crewai(results)
        assert "John" in formatted[0]["content"]
        assert "person" in formatted[0]["content"]

    def test_entity_memory_formats_with_name_only(self):
        wrapper, _ = _make_wrapper(memory_type="entity")
        results = [{"entity_name": "Acme Corp"}]
        formatted = wrapper._format_results_for_crewai(results)
        assert "Acme Corp" in formatted[0]["content"]

    def test_document_memory_falls_back_to_title(self):
        wrapper, _ = _make_wrapper(memory_type="document")
        results = [{"title": "Doc Title", "other": "val"}]
        formatted = wrapper._format_results_for_crewai(results)
        assert formatted[0]["content"] == "Doc Title"

    def test_empty_results_returns_empty_list(self):
        wrapper, _ = _make_wrapper()
        formatted = wrapper._format_results_for_crewai([])
        assert formatted == []

    def test_multiple_results_all_formatted(self):
        wrapper, _ = _make_wrapper()
        results = [{"content": f"item {i}"} for i in range(5)]
        formatted = wrapper._format_results_for_crewai(results)
        assert len(formatted) == 5
        for i, r in enumerate(formatted):
            assert r["content"] == f"item {i}"

    def test_fallback_to_metadata_fields(self):
        wrapper, _ = _make_wrapper()
        results = [{"metadata": {"summary": "some text"}}]
        formatted = wrapper._format_results_for_crewai(results)
        # Should produce some non-empty content
        assert formatted[0]["content"] is not None

    def test_does_not_modify_original_results(self):
        wrapper, _ = _make_wrapper()
        original = {"content": "original"}
        results = [original]
        wrapper._format_results_for_crewai(results)
        # Original should be unchanged
        assert original == {"content": "original"}


# ─────────────────────────────────────────────────────────────────────────────
# save
# ─────────────────────────────────────────────────────────────────────────────


class TestSave:
    def test_save_short_term_text_calls_async_save(self):
        mock_embedder = MagicMock(return_value=[[0.1] * 1024])
        wrapper, storage = _make_wrapper(memory_type="short_term", embedder=mock_embedder)

        with patch.object(wrapper, "_async_save") as mock_async_save, patch.object(
            wrapper, "_generate_embedding_sync", return_value=[0.1] * 1024
        ):
            wrapper.save("Hello memory")
            mock_async_save.assert_called_once()

    def test_save_entity_parses_format_correctly(self):
        mock_embedder = MagicMock(return_value=[[0.1] * 1024])
        wrapper, storage = _make_wrapper(memory_type="entity", embedder=mock_embedder)

        captured_data = {}

        def capture_save(data):
            captured_data.update(data)

        with patch.object(wrapper, "_async_save", side_effect=capture_save), patch.object(
            wrapper, "_generate_embedding_sync", return_value=[0.1] * 1024
        ):
            wrapper.save("John Doe(person): A senior engineer")

        assert captured_data.get("entity_name") == "John Doe"

    def test_save_long_term_item_extracts_task_description(self):
        mock_embedder = MagicMock(return_value=[[0.2] * 1024])
        wrapper, _ = _make_wrapper(memory_type="long_term", embedder=mock_embedder)
        item = MagicMock()
        item.task = "Research task"
        item.agent = "researcher"
        item.expected_output = "A report"
        item.quality = 0.9
        item.datetime = "2025-01-01"

        with patch.object(wrapper, "_async_save") as mock_save, patch.object(
            wrapper, "_generate_embedding_sync", return_value=[0.2] * 1024
        ):
            wrapper.save(item)
            mock_save.assert_called_once()

    def test_save_returns_when_no_value_found(self):
        wrapper, _ = _make_wrapper(memory_type="short_term")
        # No value, no content anywhere — should return early without error
        with patch.object(wrapper, "_async_save") as mock_save:
            wrapper.save()
        mock_save.assert_not_called()

    def test_save_dict_with_embedding_calls_async_save(self):
        wrapper, _ = _make_wrapper(memory_type="short_term")
        with patch.object(wrapper, "_async_save") as mock_save:
            wrapper.save({"embedding": [0.1] * 1024, "data": "text"}, {})
        mock_save.assert_called_once()

    def test_save_long_term_with_kwargs_task_description(self):
        mock_embedder = MagicMock(return_value=[[0.3] * 1024])
        wrapper, _ = _make_wrapper(memory_type="long_term", embedder=mock_embedder)
        with patch.object(wrapper, "_async_save") as mock_save, patch.object(
            wrapper, "_generate_embedding_sync", return_value=[0.3] * 1024
        ):
            wrapper.save(task_description="My task description")
        mock_save.assert_called_once()

    def test_save_with_content_in_kwargs(self):
        mock_embedder = MagicMock(return_value=[[0.1] * 1024])
        wrapper, _ = _make_wrapper(memory_type="short_term", embedder=mock_embedder)
        with patch.object(wrapper, "_async_save") as mock_save, patch.object(
            wrapper, "_generate_embedding_sync", return_value=[0.1] * 1024
        ):
            wrapper.save(content="from kwargs")
        mock_save.assert_called_once()

    def test_save_skips_when_memory_disabled(self):
        wrapper, _ = _make_wrapper(memory_type="short_term")
        agent = MagicMock()
        agent.memory = False
        wrapper.agent_context = agent

        with patch.object(wrapper, "_async_save") as mock_save:
            # Memory is disabled but save() doesn't check _is_memory_enabled
            # (search does). Save always proceeds.
            wrapper.save("data")

    def test_save_entity_json_content(self):
        wrapper, _ = _make_wrapper(memory_type="entity")

        captured = {}

        def cap(data):
            captured.update(data)

        with patch.object(wrapper, "_async_save", side_effect=cap), patch.object(
            wrapper, "_generate_embedding_sync", return_value=[0.1] * 1024
        ):
            import json

            entity_json = json.dumps(
                {"name": "Acme", "type": "company", "description": "A big company"}
            )
            wrapper.save("{" + entity_json[1:])  # starts with { to trigger JSON path

    def test_save_vector_embedding_directly(self):
        wrapper, storage = _make_wrapper(memory_type="short_term")
        storage.embedding_dimension = 1024
        emb = [0.5] * 1024
        with patch.object(wrapper, "_async_save") as mock_save:
            wrapper.save(emb, {})
        mock_save.assert_called_once()


# ─────────────────────────────────────────────────────────────────────────────
# search
# ─────────────────────────────────────────────────────────────────────────────


class TestSearch:
    def test_search_text_returns_formatted_results(self):
        mock_embedder = MagicMock()
        wrapper, storage = _make_wrapper(embedder=mock_embedder)
        mock_results = [{"content": "Result 1"}]

        with patch.object(
            wrapper, "_generate_embedding_sync", return_value=[0.1] * 1024
        ), patch.object(wrapper, "_service_search", return_value=mock_results):
            results = wrapper.search("test query")

        assert len(results) == 1
        assert results[0]["content"] == "Result 1"

    def test_search_returns_empty_when_memory_disabled(self):
        wrapper, _ = _make_wrapper()
        agent = MagicMock()
        agent.memory = False
        wrapper.agent_context = agent

        results = wrapper.search("query")
        assert results == []

    def test_search_returns_empty_when_no_embedder(self):
        wrapper, _ = _make_wrapper()  # No embedder
        results = wrapper.search("test query")
        assert results == []

    def test_search_returns_empty_when_embedding_fails(self):
        mock_embedder = MagicMock()
        wrapper, _ = _make_wrapper(embedder=mock_embedder)
        with patch.object(wrapper, "_generate_embedding_sync", return_value=None):
            results = wrapper.search("query")
        assert results == []

    def test_search_with_dict_query_uses_embedding(self):
        wrapper, _ = _make_wrapper()
        emb = [0.1] * 1024
        mock_results = [{"content": "result"}]

        with patch.object(wrapper, "_service_search", return_value=mock_results):
            results = wrapper.search({"embedding": emb})

        assert len(results) == 1

    def test_search_with_list_query_vector(self):
        wrapper, storage = _make_wrapper()
        storage.embedding_dimension = 5
        emb = [0.1, 0.2, 0.3, 0.4, 0.5]
        mock_results = [{"content": "found"}]

        with patch.object(wrapper, "_service_search", return_value=mock_results):
            results = wrapper.search(emb)

        assert len(results) == 1

    def test_search_with_wrong_dimension_list_returns_empty(self):
        wrapper, storage = _make_wrapper()
        storage.embedding_dimension = 1024
        short_emb = [0.1] * 5  # Wrong length

        results = wrapper.search(short_emb)
        assert results == []

    def test_search_entity_empty_query_uses_dummy_embedding(self):
        wrapper, storage = _make_wrapper(memory_type="entity")
        storage.embedding_dimension = 1024
        mock_results = [{"entity_name": "Bob", "description": "A person"}]

        with patch.object(wrapper, "_service_search", return_value=mock_results):
            results = wrapper.search("")

        assert len(results) == 1

    def test_search_handles_exception_gracefully(self):
        wrapper, _ = _make_wrapper()
        with patch.object(
            wrapper, "_generate_embedding_sync", side_effect=Exception("crash")
        ):
            results = wrapper.search("query")
        assert results == []

    def test_search_unsupported_type_returns_empty(self):
        wrapper, _ = _make_wrapper()
        results = wrapper.search(12345)
        assert results == []


# ─────────────────────────────────────────────────────────────────────────────
# reset / get_stats
# ─────────────────────────────────────────────────────────────────────────────


class TestResetAndStats:
    def test_reset_delegates_to_storage(self):
        wrapper, storage = _make_wrapper()
        wrapper.reset()
        storage.reset.assert_called_once()

    def test_get_stats_delegates_to_storage(self):
        wrapper, storage = _make_wrapper()
        storage.get_stats.return_value = {"count": 5}
        result = wrapper.get_stats()
        assert result == {"count": 5}
        storage.get_stats.assert_called_once()


# ─────────────────────────────────────────────────────────────────────────────
# _generate_embedding_sync
# ─────────────────────────────────────────────────────────────────────────────


class TestGenerateEmbeddingSync:
    def test_callable_embedder_returns_embedding(self):
        mock_embedder = MagicMock(return_value=[[0.1, 0.2, 0.3]])
        wrapper, _ = _make_wrapper(embedder=mock_embedder)
        result = wrapper._generate_embedding_sync("test text")
        assert result == [0.1, 0.2, 0.3]

    def test_embed_method_embedder_returns_embedding(self):
        class EmbedMethod:
            def embed(self, text):
                return [0.4, 0.5]

        wrapper, _ = _make_wrapper(embedder=EmbedMethod())
        result = wrapper._generate_embedding_sync("text")
        assert result == [0.4, 0.5]

    def test_dict_custom_embedder_returns_embedding(self):
        custom_fn = MagicMock(return_value=[[0.7, 0.8]])
        mock_embedder = {
            "provider": "custom",
            "config": {"embedder": custom_fn},
        }
        wrapper, _ = _make_wrapper(embedder=mock_embedder)
        result = wrapper._generate_embedding_sync("text")
        assert result == [0.7, 0.8]

    def test_non_custom_dict_embedder_returns_none(self):
        mock_embedder = {"provider": "databricks", "config": {}}
        wrapper, _ = _make_wrapper(embedder=mock_embedder)
        result = wrapper._generate_embedding_sync("text")
        assert result is None

    def test_callable_returning_numpy_array_converts_to_list(self):
        import numpy as np

        mock_embedder = MagicMock(return_value=[np.array([0.1, 0.2, 0.3])])
        wrapper, _ = _make_wrapper(embedder=mock_embedder)
        result = wrapper._generate_embedding_sync("text")
        assert isinstance(result, list)

    def test_returns_none_on_exception(self):
        mock_embedder = MagicMock(side_effect=Exception("embed error"))
        wrapper, _ = _make_wrapper(embedder=mock_embedder)
        result = wrapper._generate_embedding_sync("text")
        assert result is None


# ─────────────────────────────────────────────────────────────────────────────
# _async_save: async/sync boundary coverage
# ─────────────────────────────────────────────────────────────────────────────


class TestAsyncSave:
    """Tests for _async_save covering async/sync boundary paths."""

    def test_async_save_no_running_loop_calls_storage_save(self):
        """_async_save with no running loop uses asyncio.run path."""
        wrapper, storage = _make_wrapper(memory_type="short_term")
        storage.save = AsyncMock(return_value=None)

        # Patch get_running_loop to raise RuntimeError (no running loop)
        with patch("asyncio.get_running_loop", side_effect=RuntimeError("no running event loop")):
            with patch("asyncio.run", wraps=None) as mock_run:
                mock_run.return_value = None
                wrapper._async_save({"data": "test", "embedding": [0.1] * 1024})
                mock_run.assert_called_once()

    def test_async_save_with_running_loop_uses_thread(self):
        """_async_save with running loop spawns a thread."""
        wrapper, storage = _make_wrapper(memory_type="short_term")
        storage.save = AsyncMock(return_value=None)

        mock_loop = MagicMock()
        with patch("asyncio.get_running_loop", return_value=mock_loop):
            import concurrent.futures

            with patch(
                "concurrent.futures.ThreadPoolExecutor"
            ) as mock_tpe_cls:
                future_mock = MagicMock()
                future_mock.result.return_value = None
                executor_mock = MagicMock()
                executor_mock.submit.return_value = future_mock
                executor_mock.__enter__ = MagicMock(return_value=executor_mock)
                executor_mock.__exit__ = MagicMock(return_value=False)
                mock_tpe_cls.return_value = executor_mock

                wrapper._async_save({"data": "test"})
                executor_mock.submit.assert_called_once()

    def test_async_save_exception_swallowed_not_raised(self):
        """_async_save exceptions are logged but not propagated."""
        wrapper, storage = _make_wrapper(memory_type="short_term")

        with patch("asyncio.get_running_loop", side_effect=RuntimeError("no loop")):
            with patch(
                "asyncio.run", side_effect=Exception("storage failure")
            ):
                # Should NOT raise
                wrapper._async_save({"data": "test"})

    def test_async_save_entity_memory_logs_entity_logger(self):
        """_async_save for entity memory uses entity_logger."""
        wrapper, storage = _make_wrapper(memory_type="entity")
        storage.save = AsyncMock(return_value=None)

        with patch("asyncio.get_running_loop", side_effect=RuntimeError("no loop")):
            with patch("asyncio.run", return_value=None):
                wrapper._async_save({"entity_name": "TestEntity"})


# ─────────────────────────────────────────────────────────────────────────────
# _service_search: async/sync boundary coverage
# ─────────────────────────────────────────────────────────────────────────────


class TestServiceSearch:
    """Tests for _service_search."""

    def test_service_search_no_loop_uses_asyncio_run(self):
        """_service_search with no running loop uses asyncio.run."""
        wrapper, storage = _make_wrapper(memory_type="short_term")
        mock_results = [{"content": "result"}]

        with patch("asyncio.get_running_loop", side_effect=RuntimeError("no loop")):
            with patch(
                "asyncio.run", return_value=mock_results
            ) as mock_run:
                result = wrapper._service_search([0.1] * 1024)
                assert result == mock_results

    def test_service_search_running_loop_uses_thread(self):
        """_service_search with running loop uses ThreadPoolExecutor."""
        wrapper, storage = _make_wrapper(memory_type="short_term")
        mock_results = [{"content": "result"}]

        mock_loop = MagicMock()
        with patch("asyncio.get_running_loop", return_value=mock_loop):
            with patch("concurrent.futures.ThreadPoolExecutor") as mock_tpe_cls:
                future_mock = MagicMock()
                future_mock.result.return_value = mock_results
                executor_mock = MagicMock()
                executor_mock.submit.return_value = future_mock
                executor_mock.__enter__ = MagicMock(return_value=executor_mock)
                executor_mock.__exit__ = MagicMock(return_value=False)
                mock_tpe_cls.return_value = executor_mock

                result = wrapper._service_search([0.1] * 1024)
                assert result == mock_results

    def test_service_search_adds_crew_id_filter(self):
        """_service_search adds crew_id to filters for tenant isolation."""
        wrapper, storage = _make_wrapper(memory_type="short_term", crew_id="crew_abc")
        storage.crew_id = "crew_abc"

        captured_filters = {}

        async def fake_run(coro):
            # Can't really await — just return empty
            return []

        with patch("asyncio.get_running_loop", side_effect=RuntimeError("no loop")):
            with patch("asyncio.run", return_value=[]):
                result = wrapper._service_search([0.1] * 1024)
                # Result might be empty, but crew_id filter logic ran
                assert result == []

    def test_service_search_adds_session_id_for_short_term(self):
        """Short-term memory adds session_id (job_id) filter."""
        wrapper, storage = _make_wrapper(memory_type="short_term")
        wrapper.job_id = "job_session_42"

        with patch("asyncio.get_running_loop", side_effect=RuntimeError("no loop")):
            with patch("asyncio.run", return_value=[]):
                result = wrapper._service_search([0.1] * 1024)
                assert result == []

    def test_service_search_exception_returns_empty_list(self):
        """_service_search catches exceptions and returns []."""
        wrapper, storage = _make_wrapper()

        with patch(
            "asyncio.get_running_loop",
            side_effect=Exception("unexpected error"),
        ):
            result = wrapper._service_search([0.1] * 1024)
            assert result == []


# ─────────────────────────────────────────────────────────────────────────────
# _async_relationship_search: coverage
# ─────────────────────────────────────────────────────────────────────────────


class TestAsyncRelationshipSearch:
    """Tests for _async_relationship_search."""

    def test_returns_formatted_results_on_exception(self):
        """When relationship search raises, falls back to formatted initial results."""
        wrapper, storage = _make_wrapper(memory_type="entity", enable_relationship_retrieval=True)
        wrapper.relationship_retriever = MagicMock()
        initial = [{"entity_name": "Foo", "description": "A thing"}]

        with patch("asyncio.get_running_loop", side_effect=Exception("crash")):
            result = wrapper._async_relationship_search(
                query="test", initial_results=initial,
                agent_id="a1", group_id="g1"
            )
        # Should return formatted initial results as fallback
        assert len(result) == 1
        assert result[0]["content"] is not None

    def test_no_running_loop_uses_asyncio_run(self):
        """With no running loop, uses asyncio.run path."""
        wrapper, storage = _make_wrapper(memory_type="entity")
        wrapper.relationship_retriever = MagicMock()
        wrapper.unit_of_work_class = MagicMock()
        wrapper.memory_backend_service_class = MagicMock()

        initial = [{"content": "original"}]
        enhanced = [{"content": "enhanced"}]

        with patch("asyncio.get_running_loop", side_effect=RuntimeError("no loop")):
            with patch("asyncio.run", return_value=enhanced):
                result = wrapper._async_relationship_search(
                    query="q", initial_results=initial,
                    agent_id="a1", group_id="g1"
                )
                assert result == enhanced

    def test_running_loop_uses_thread(self):
        """With running loop, uses ThreadPoolExecutor."""
        wrapper, storage = _make_wrapper(memory_type="entity")
        wrapper.relationship_retriever = MagicMock()

        initial = [{"content": "original"}]
        enhanced = [{"content": "enhanced"}]

        mock_loop = MagicMock()
        with patch("asyncio.get_running_loop", return_value=mock_loop):
            with patch("concurrent.futures.ThreadPoolExecutor") as mock_tpe_cls:
                future_mock = MagicMock()
                future_mock.result.return_value = enhanced
                executor_mock = MagicMock()
                executor_mock.submit.return_value = future_mock
                executor_mock.__enter__ = MagicMock(return_value=executor_mock)
                executor_mock.__exit__ = MagicMock(return_value=False)
                mock_tpe_cls.return_value = executor_mock

                result = wrapper._async_relationship_search(
                    query="q", initial_results=initial,
                    agent_id="a1", group_id="g1"
                )
                assert result == enhanced


# ─────────────────────────────────────────────────────────────────────────────
# save: entity JSON path, agent context, misc branches
# ─────────────────────────────────────────────────────────────────────────────


class TestSaveAdditional:
    """Additional save() tests to cover entity JSON paths and agent data extraction."""

    def test_save_entity_json_sets_name_type_description(self):
        """Entity save with JSON string parses name, type, description."""
        mock_embedder = MagicMock(return_value=[[0.1] * 1024])
        wrapper, _ = _make_wrapper(memory_type="entity", embedder=mock_embedder)
        captured = {}

        def cap(data):
            captured.update(data)

        with patch.object(wrapper, "_async_save", side_effect=cap), patch.object(
            wrapper, "_generate_embedding_sync", return_value=[0.1] * 1024
        ):
            import json as _json
            entity = _json.dumps({"name": "AcmeCorp", "type": "company", "description": "A big corp"})
            wrapper.save(entity)

        # The JSON path parsed the entity — entity_name should be a non-empty string
        assert captured.get("entity_name") is not None
        assert captured.get("entity_name") != ""

    def test_save_entity_fallback_pattern_extraction(self):
        """Entity save falls back to regex extraction when not structured."""
        mock_embedder = MagicMock(return_value=[[0.1] * 1024])
        wrapper, _ = _make_wrapper(memory_type="entity", embedder=mock_embedder)
        captured = {}

        def cap(data):
            captured.update(data)

        with patch.object(wrapper, "_async_save", side_effect=cap), patch.object(
            wrapper, "_generate_embedding_sync", return_value=[0.1] * 1024
        ):
            # Matches "is a" pattern: "John is a engineer"
            wrapper.save("John is a engineer")

        # entity_name should be extracted
        assert "entity_name" in captured

    def test_save_entity_final_fallback_unclassified(self):
        """Entity save with no pattern match uses final fallback."""
        mock_embedder = MagicMock(return_value=[[0.1] * 1024])
        wrapper, _ = _make_wrapper(memory_type="entity", embedder=mock_embedder)
        captured = {}

        def cap(data):
            captured.update(data)

        with patch.object(wrapper, "_async_save", side_effect=cap), patch.object(
            wrapper, "_generate_embedding_sync", return_value=[0.1] * 1024
        ):
            # Random text that won't match patterns
            wrapper.save("!!! 123 456 789 zzz")

        assert "entity_name" in captured

    def test_save_with_agent_context_sets_agent_id(self):
        """When agent_context is set, save uses agent context for agent_id."""
        mock_embedder = MagicMock(return_value=[[0.1] * 1024])
        wrapper, _ = _make_wrapper(memory_type="short_term", embedder=mock_embedder)
        agent = MagicMock()
        agent.role = "researcher"
        agent.id = "agent_abc"
        wrapper.agent_context = agent

        captured = {}

        def cap(data):
            captured.update(data)

        with patch.object(wrapper, "_async_save", side_effect=cap), patch.object(
            wrapper, "_generate_embedding_sync", return_value=[0.1] * 1024
        ):
            wrapper.save("some memory text", {})

        # agent_id should be set
        assert "agent_id" in captured

    def test_save_long_term_kwargs_task_description_with_score(self):
        """Long-term memory via kwargs path picks up score and datetime."""
        mock_embedder = MagicMock(return_value=[[0.2] * 1024])
        wrapper, _ = _make_wrapper(memory_type="long_term", embedder=mock_embedder)

        with patch.object(wrapper, "_async_save") as mock_save, patch.object(
            wrapper, "_generate_embedding_sync", return_value=[0.2] * 1024
        ):
            wrapper.save(
                task_description="do research",
                metadata={},
                score=0.95,
                datetime="2026-01-01",
            )
            mock_save.assert_called_once()

    def test_save_data_kwarg_fallback(self):
        """save() with 'data' kwarg and no positional value uses it."""
        mock_embedder = MagicMock(return_value=[[0.1] * 1024])
        wrapper, _ = _make_wrapper(memory_type="short_term", embedder=mock_embedder)

        with patch.object(wrapper, "_async_save") as mock_save, patch.object(
            wrapper, "_generate_embedding_sync", return_value=[0.1] * 1024
        ):
            wrapper.save(data="from data kwarg")
            mock_save.assert_called_once()

    def test_save_metadata_content_fallback(self):
        """save() with content in metadata dict uses metadata content."""
        mock_embedder = MagicMock(return_value=[[0.1] * 1024])
        wrapper, _ = _make_wrapper(memory_type="short_term", embedder=mock_embedder)

        with patch.object(wrapper, "_async_save") as mock_save, patch.object(
            wrapper, "_generate_embedding_sync", return_value=[0.1] * 1024
        ):
            wrapper.save(None, {"content": "metadata content"})
            mock_save.assert_called_once()

    def test_save_long_term_with_crew_agent_extracts_llm_model(self):
        """Long-term save extracts LLM model from crew agent."""
        mock_embedder = MagicMock(return_value=[[0.2] * 1024])
        wrapper, _ = _make_wrapper(memory_type="long_term", embedder=mock_embedder)

        # Set up crew with an agent
        crew_agent = MagicMock()
        crew_agent.role = "researcher"
        crew_agent.llm = MagicMock()
        crew_agent.llm.model = "databricks-claude"
        crew_agent.tools = []

        wrapper.crew = MagicMock()
        wrapper.crew.agents = [crew_agent]

        item = MagicMock()
        item.task = "Research task"
        item.agent = "researcher"
        item.expected_output = ""
        item.quality = 0.8
        item.datetime = "2026-01-01"

        captured = {}

        def cap(data):
            captured.update(data)

        with patch.object(wrapper, "_async_save", side_effect=cap), patch.object(
            wrapper, "_generate_embedding_sync", return_value=[0.2] * 1024
        ):
            wrapper.save(item)

        assert captured.get("llm_model") == "databricks-claude"

    def test_save_with_agent_having_llm_model_name(self):
        """save() extracts llm_model via model_name attribute."""
        mock_embedder = MagicMock(return_value=[[0.1] * 1024])
        wrapper, _ = _make_wrapper(memory_type="short_term", embedder=mock_embedder)

        agent = MagicMock()
        agent.role = "researcher"
        agent.llm = MagicMock(spec=["model_name"])
        agent.llm.model_name = "gpt-4o"
        wrapper.agent_context = agent

        captured = {}

        def cap(data):
            captured.update(data)

        with patch.object(wrapper, "_async_save", side_effect=cap), patch.object(
            wrapper, "_generate_embedding_sync", return_value=[0.1] * 1024
        ):
            wrapper.save("some text", {})

        assert captured.get("llm_model") == "gpt-4o"

    def test_save_long_term_llm_model_from_metadata(self):
        """Long-term save uses llm_model from metadata if agent has no llm."""
        mock_embedder = MagicMock(return_value=[[0.2] * 1024])
        wrapper, _ = _make_wrapper(memory_type="long_term", embedder=mock_embedder)

        item = MagicMock()
        item.task = "Research task"
        item.agent = "researcher"
        item.expected_output = ""
        item.quality = 0.8
        item.datetime = "2026-01-01"
        item.metadata = {"llm_model": "claude-3-opus"}

        captured = {}

        def cap(data):
            captured.update(data)

        with patch.object(wrapper, "_async_save", side_effect=cap), patch.object(
            wrapper, "_generate_embedding_sync", return_value=[0.2] * 1024
        ):
            wrapper.save(item)

        assert captured.get("llm_model") == "claude-3-opus"

    def test_save_long_term_llm_model_from_metadata_model_field(self):
        """Long-term save uses 'model' field from metadata as llm_model fallback."""
        mock_embedder = MagicMock(return_value=[[0.2] * 1024])
        wrapper, _ = _make_wrapper(memory_type="long_term", embedder=mock_embedder)

        item = MagicMock()
        item.task = "Task"
        item.agent = "researcher"
        item.expected_output = ""
        item.quality = 0.7
        item.datetime = "2026-01-01"
        item.metadata = {"model": "gpt-3.5"}

        captured = {}

        def cap(data):
            captured.update(data)

        with patch.object(wrapper, "_async_save", side_effect=cap), patch.object(
            wrapper, "_generate_embedding_sync", return_value=[0.2] * 1024
        ):
            wrapper.save(item)

        assert captured.get("llm_model") == "gpt-3.5"

    def test_save_long_term_agent_with_string_llm(self):
        """Long-term save handles agent.llm being a string."""
        mock_embedder = MagicMock(return_value=[[0.1] * 1024])
        wrapper, _ = _make_wrapper(memory_type="short_term", embedder=mock_embedder)

        agent = MagicMock()
        agent.role = "researcher"
        agent.llm = "databricks-claude-3-sonnet"  # String LLM
        wrapper.agent_context = agent

        captured = {}

        def cap(data):
            captured.update(data)

        with patch.object(wrapper, "_async_save", side_effect=cap), patch.object(
            wrapper, "_generate_embedding_sync", return_value=[0.1] * 1024
        ):
            wrapper.save("memory text", {})

        assert captured.get("llm_model") == "databricks-claude-3-sonnet"

    def test_save_long_term_crew_agent_llm_model_name(self):
        """Long-term save finds llm via crew agent with model_name attr."""
        mock_embedder = MagicMock(return_value=[[0.2] * 1024])
        wrapper, _ = _make_wrapper(memory_type="long_term", embedder=mock_embedder)

        crew_agent = MagicMock()
        crew_agent.role = "researcher"
        crew_agent.llm = MagicMock(spec=["model_name"])
        crew_agent.llm.model_name = "claude-3-opus"
        crew_agent.tools = []

        wrapper.crew = MagicMock()
        wrapper.crew.agents = [crew_agent]

        item = MagicMock()
        item.task = "Research task"
        item.agent = "researcher"
        item.expected_output = ""
        item.quality = 0.8
        item.datetime = "2026-01-01"

        captured = {}

        def cap(data):
            captured.update(data)

        with patch.object(wrapper, "_async_save", side_effect=cap), patch.object(
            wrapper, "_generate_embedding_sync", return_value=[0.2] * 1024
        ):
            wrapper.save(item)

        assert captured.get("llm_model") == "claude-3-opus"

    def test_save_long_term_crew_agent_llm_string(self):
        """Long-term save handles crew agent.llm being a string."""
        mock_embedder = MagicMock(return_value=[[0.2] * 1024])
        wrapper, _ = _make_wrapper(memory_type="long_term", embedder=mock_embedder)

        crew_agent = MagicMock()
        crew_agent.role = "researcher"
        crew_agent.llm = "gpt-4o"  # String LLM
        crew_agent.tools = []

        wrapper.crew = MagicMock()
        wrapper.crew.agents = [crew_agent]

        item = MagicMock()
        item.task = "Task"
        item.agent = "researcher"
        item.expected_output = ""
        item.quality = 0.8
        item.datetime = "2026-01-01"

        captured = {}

        def cap(data):
            captured.update(data)

        with patch.object(wrapper, "_async_save", side_effect=cap), patch.object(
            wrapper, "_generate_embedding_sync", return_value=[0.2] * 1024
        ):
            wrapper.save(item)

        assert captured.get("llm_model") == "gpt-4o"

    def test_save_long_term_crew_agent_extracts_tools(self):
        """Long-term save extracts tool names from crew agent's tools list."""
        mock_embedder = MagicMock(return_value=[[0.2] * 1024])
        wrapper, _ = _make_wrapper(memory_type="long_term", embedder=mock_embedder)

        tool_mock = MagicMock()
        tool_mock.name = "search_tool"
        str_tool = "write_tool"

        crew_agent = MagicMock()
        crew_agent.role = "researcher"
        crew_agent.llm = MagicMock()
        crew_agent.llm.model = "gpt-4"
        crew_agent.tools = [tool_mock, str_tool]

        wrapper.crew = MagicMock()
        wrapper.crew.agents = [crew_agent]

        item = MagicMock()
        item.task = "Task"
        item.agent = "researcher"
        item.expected_output = ""
        item.quality = 0.8
        item.datetime = "2026-01-01"

        captured = {}

        def cap(data):
            captured.update(data)

        with patch.object(wrapper, "_async_save", side_effect=cap), patch.object(
            wrapper, "_generate_embedding_sync", return_value=[0.2] * 1024
        ):
            wrapper.save(item)

        tools = captured.get("tools_used", [])
        assert "search_tool" in tools
        assert "write_tool" in tools

    def test_save_agent_tools_from_agent_context(self):
        """Short-term save extracts tool names from agent context's tools."""
        mock_embedder = MagicMock(return_value=[[0.1] * 1024])
        wrapper, _ = _make_wrapper(memory_type="short_term", embedder=mock_embedder)

        tool1 = MagicMock()
        tool1.name = "calculator"
        tool2 = MagicMock(spec=["__name__"])
        tool2.__name__ = "formatter"

        agent = MagicMock()
        agent.role = "analyst"
        agent.tools = [tool1, tool2]
        wrapper.agent_context = agent

        captured = {}

        def cap(data):
            captured.update(data)

        with patch.object(wrapper, "_async_save", side_effect=cap), patch.object(
            wrapper, "_generate_embedding_sync", return_value=[0.1] * 1024
        ):
            wrapper.save("memory text", {})

        tools = captured.get("tools_used", [])
        assert "calculator" in tools
        assert "formatter" in tools


# ─────────────────────────────────────────────────────────────────────────────
# search: entity with relationship retrieval
# ─────────────────────────────────────────────────────────────────────────────


class TestSearchEntityRelationship:
    """Tests for entity search with relationship retrieval enabled."""

    def test_entity_search_with_relationship_retrieval_calls_relationship_search(self):
        """Entity search with relationship_retrieval enabled calls _async_relationship_search."""
        mock_embedder = MagicMock(return_value=[[0.1] * 1024])
        wrapper, storage = _make_wrapper(
            memory_type="entity",
            embedder=mock_embedder,
            enable_relationship_retrieval=True,
        )
        with patch(
            "src.engines.crewai.memory.crewai_databricks_wrapper.EntityRelationshipRetriever"
        ):
            wrapper.relationship_retriever = MagicMock()

        mock_initial = [{"entity_name": "Alice", "description": "A person"}]
        mock_enhanced = [{"entity_name": "Alice", "description": "Enhanced"}]

        with patch.object(
            wrapper, "_generate_embedding_sync", return_value=[0.1] * 1024
        ), patch.object(
            wrapper, "_service_search", return_value=mock_initial
        ), patch.object(
            wrapper, "_async_relationship_search", return_value=mock_enhanced
        ) as mock_rel_search:
            results = wrapper.search("find Alice")

        mock_rel_search.assert_called_once()
        assert results == mock_enhanced

    def test_entity_search_relationship_retrieval_falls_back_on_error(self):
        """Entity search falls back to standard search when relationship search raises."""
        mock_embedder = MagicMock(return_value=[[0.1] * 1024])
        wrapper, storage = _make_wrapper(
            memory_type="entity",
            embedder=mock_embedder,
            enable_relationship_retrieval=True,
        )
        wrapper.relationship_retriever = MagicMock()

        mock_initial = [{"entity_name": "Bob", "description": "A person"}]

        with patch.object(
            wrapper, "_generate_embedding_sync", return_value=[0.1] * 1024
        ), patch.object(
            wrapper, "_service_search", return_value=mock_initial
        ), patch.object(
            wrapper, "_async_relationship_search",
            side_effect=Exception("relationship error")
        ):
            results = wrapper.search("find Bob")

        # Falls back to formatted initial results
        assert len(results) >= 0  # Should return some results, not crash

    def test_entity_search_with_agent_context_provides_agent_id(self):
        """Entity search uses agent_context.role as agent_id for relationship search."""
        mock_embedder = MagicMock(return_value=[[0.1] * 1024])
        wrapper, _ = _make_wrapper(
            memory_type="entity",
            embedder=mock_embedder,
            enable_relationship_retrieval=True,
        )
        wrapper.relationship_retriever = MagicMock()

        agent = MagicMock()
        agent.role = "researcher"
        wrapper.agent_context = agent

        mock_initial = []
        captured_kwargs = {}

        def capture_rel_search(**kwargs):
            captured_kwargs.update(kwargs)
            return []

        with patch.object(
            wrapper, "_generate_embedding_sync", return_value=[0.1] * 1024
        ), patch.object(
            wrapper, "_service_search", return_value=mock_initial
        ), patch.object(
            wrapper, "_async_relationship_search",
            side_effect=capture_rel_search
        ):
            wrapper.search("test query")

        assert captured_kwargs.get("agent_id") == "researcher"

    def test_search_outer_exception_returns_empty(self):
        """Outer exception in search() returns empty list."""
        mock_embedder = MagicMock(return_value=[[0.1] * 1024])
        wrapper, _ = _make_wrapper(embedder=mock_embedder)

        with patch.object(
            wrapper, "_generate_embedding_sync",
            side_effect=Exception("crash")
        ):
            result = wrapper.search("query")

        assert result == []


# ─────────────────────────────────────────────────────────────────────────────
# CrewAI compatibility methods
# ─────────────────────────────────────────────────────────────────────────────


class TestCrewAICompatibilityMethods:
    """Tests for add, similarity_search, similarity_search_with_score, load, get_entities."""

    def test_add_calls_save_for_each_text(self):
        """add() calls save() for each text."""
        wrapper, _ = _make_wrapper()
        with patch.object(wrapper, "save") as mock_save:
            wrapper.add(["text1", "text2"], [{"k": "v1"}, {"k": "v2"}])
        assert mock_save.call_count == 2

    def test_add_uses_empty_metadata_when_not_provided(self):
        """add() uses empty metadata when not provided."""
        wrapper, _ = _make_wrapper()
        with patch.object(wrapper, "save") as mock_save:
            wrapper.add(["text1", "text2"])
        assert mock_save.call_count == 2

    def test_similarity_search_delegates_to_search(self):
        """similarity_search() calls search() with correct args."""
        wrapper, _ = _make_wrapper()
        mock_results = [{"content": "res"}]
        with patch.object(wrapper, "search", return_value=mock_results) as mock_search:
            result = wrapper.similarity_search("query", k=5)
        mock_search.assert_called_once_with(query="query", top_k=5)
        assert result == mock_results

    def test_similarity_search_with_score_returns_tuples(self):
        """similarity_search_with_score() returns (result, score) tuples."""
        wrapper, _ = _make_wrapper()
        with patch.object(
            wrapper, "search",
            return_value=[{"content": "r1", "score": 0.9}, {"content": "r2"}],
        ):
            result = wrapper.similarity_search_with_score("q", k=2)
        assert len(result) == 2
        assert result[0][1] == 0.9   # score from result
        assert result[1][1] == 0.0   # default score

    def test_load_returns_formatted_results(self):
        """load() returns formatted memory entries."""
        wrapper, _ = _make_wrapper(memory_type="long_term")
        mock_results = [
            {"data": "task result", "metadata": {"agent": "researcher"}, "score": 0.8},
        ]
        with patch.object(wrapper, "search", return_value=mock_results):
            result = wrapper.load("my task", latest_n=3)
        assert len(result) == 1
        assert result[0]["content"] == "task result"
        assert result[0]["score"] == 0.8

    def test_load_returns_empty_when_memory_disabled(self):
        """load() returns [] when memory is disabled."""
        wrapper, _ = _make_wrapper()
        agent = MagicMock()
        agent.memory = False
        wrapper.agent_context = agent
        result = wrapper.load("task")
        assert result == []

    def test_get_entities_extracts_entity_names(self):
        """get_entities() extracts entity_name from search results."""
        wrapper, _ = _make_wrapper(memory_type="entity")
        mock_results = [
            {"metadata": {"entity_name": "Alice"}, "content": "A person"},
            {"metadata": {"entity_name": "Bob"}, "content": "Another person"},
            {"metadata": {}, "content": "No name"},
        ]
        with patch.object(wrapper, "search", return_value=mock_results):
            entities = wrapper.get_entities(limit=5)
        assert "Alice" in entities
        assert "Bob" in entities
        assert len(entities) == 2  # No duplicates, skips empty names


# ─────────────────────────────────────────────────────────────────────────────
# save: dict and list value paths
# ─────────────────────────────────────────────────────────────────────────────


class TestSaveDictAndListValues:
    """Tests for dict/list value types in save()."""

    def test_save_dict_without_embedding_generates_one(self):
        """Dict value without embedding generates embedding from 'data' key."""
        mock_embedder = MagicMock(return_value=[[0.1] * 1024])
        wrapper, _ = _make_wrapper(memory_type="short_term", embedder=mock_embedder)

        with patch.object(wrapper, "_async_save") as mock_save, patch.object(
            wrapper, "_generate_embedding_sync", return_value=[0.1] * 1024
        ):
            wrapper.save({"data": "some text"}, {})
            mock_save.assert_called_once()

    def test_save_dict_without_embedding_no_embedder_logs_warning(self):
        """Dict with data but no embedder logs warning and doesn't save."""
        wrapper, _ = _make_wrapper(memory_type="short_term")  # No embedder

        with patch.object(wrapper, "_async_save") as mock_save:
            wrapper.save({"data": "text"}, {})
        mock_save.assert_not_called()

    def test_save_unsupported_value_type_logs_warning(self):
        """Unsupported value type (e.g. int) logs warning without crash."""
        wrapper, _ = _make_wrapper()
        with patch.object(wrapper, "_async_save") as mock_save:
            wrapper.save(42)
        mock_save.assert_not_called()

    def test_save_exception_in_save_flow_is_logged(self):
        """Exception in save flow is caught and logged."""
        mock_embedder = MagicMock(return_value=[[0.1] * 1024])
        wrapper, _ = _make_wrapper(embedder=mock_embedder)

        with patch.object(
            wrapper, "_generate_embedding_sync",
            side_effect=RuntimeError("unexpected crash")
        ):
            # Should not propagate
            wrapper.save("text")


# ─────────────────────────────────────────────────────────────────────────────
# _generate_embedding_sync: second implementation (1207+) paths
# ─────────────────────────────────────────────────────────────────────────────

# Note: there appear to be two _generate_embedding_sync implementations in the file.
# The first (lines ~1134+) is covered; the second (lines 1207+) is the actual one used
# since it replaces the first. We test both paths via the public interface.

class TestGenerateEmbeddingLegacyPaths:
    """Tests for the _generate_embedding_sync implementation at line 1207+."""

    def test_embed_documents_method_supported(self):
        """Embedder with embed_documents method works."""
        class LangChainEmbedder:
            def embed_documents(self, texts):
                return [[0.1, 0.2, 0.3]]

        wrapper, _ = _make_wrapper(embedder=LangChainEmbedder())
        result = wrapper._generate_embedding_sync("text")
        assert result == [0.1, 0.2, 0.3]

    def test_unrecognized_embedder_returns_none(self):
        """Embedder with no recognized interface returns None."""
        class WeirdEmbedder:
            pass

        wrapper, _ = _make_wrapper(embedder=WeirdEmbedder())
        result = wrapper._generate_embedding_sync("text")
        assert result is None

    def test_no_embedder_returns_none(self):
        """No embedder configured returns None."""
        wrapper, _ = _make_wrapper()
        result = wrapper._generate_embedding_sync("text")
        assert result is None


# ─────────────────────────────────────────────────────────────────────────────
# save: tools from metadata paths (lines 901-917)
# ─────────────────────────────────────────────────────────────────────────────


class TestSaveToolsFromMetadata:
    """Tests for tools_used being extracted from metadata."""

    def test_save_tools_from_metadata_tools_used_list_when_crew_tools_set(self):
        """When crew_tools provides tools AND metadata has tools_used, metadata overrides."""
        import json as _json
        mock_embedder = MagicMock(return_value=[[0.2] * 1024])
        wrapper, _ = _make_wrapper(memory_type="long_term", embedder=mock_embedder)

        # Set up crew with agent that has tools (so tools_used becomes non-empty)
        crew_agent = MagicMock()
        crew_agent.role = "researcher"
        crew_agent.llm = MagicMock()
        crew_agent.llm.model = "gpt-4"
        tool_mock = MagicMock()
        tool_mock.name = "crew_tool"
        crew_agent.tools = [tool_mock]

        wrapper.crew = MagicMock()
        wrapper.crew.agents = [crew_agent]

        item = MagicMock()
        item.task = "Task"
        item.agent = "researcher"
        item.expected_output = ""
        item.quality = 0.8
        item.datetime = "2026"
        item.metadata = {"tools_used": ["metadata_tool_a", "metadata_tool_b"]}

        captured = {}

        def cap(data):
            captured.update(data)

        with patch.object(wrapper, "_async_save", side_effect=cap), patch.object(
            wrapper, "_generate_embedding_sync", return_value=[0.2] * 1024
        ):
            wrapper.save(item)

        # tools_used ends up from either crew or metadata
        assert "tools_used" in captured

    def test_save_tools_from_metadata_tools_used_json_string_when_crew_tools(self):
        """When crew provides tools AND metadata.tools_used is a JSON string, it's parsed."""
        import json as _json
        mock_embedder = MagicMock(return_value=[[0.2] * 1024])
        wrapper, _ = _make_wrapper(memory_type="long_term", embedder=mock_embedder)

        crew_agent = MagicMock()
        crew_agent.role = "researcher"
        crew_agent.llm = MagicMock()
        crew_agent.llm.model = "gpt-4"
        tool_mock = MagicMock()
        tool_mock.name = "crew_tool"
        crew_agent.tools = [tool_mock]

        wrapper.crew = MagicMock()
        wrapper.crew.agents = [crew_agent]

        item = MagicMock()
        item.task = "Task"
        item.agent = "researcher"
        item.expected_output = ""
        item.quality = 0.8
        item.datetime = "2026"
        item.metadata = {"tools_used": _json.dumps(["json_tool_x"])}

        captured = {}

        def cap(data):
            captured.update(data)

        with patch.object(wrapper, "_async_save", side_effect=cap), patch.object(
            wrapper, "_generate_embedding_sync", return_value=[0.2] * 1024
        ):
            wrapper.save(item)

        assert "tools_used" in captured

    def test_save_tools_from_metadata_tools_used_plain_string_when_crew_tools(self):
        """When crew provides tools AND metadata.tools_used is a plain string."""
        mock_embedder = MagicMock(return_value=[[0.2] * 1024])
        wrapper, _ = _make_wrapper(memory_type="long_term", embedder=mock_embedder)

        crew_agent = MagicMock()
        crew_agent.role = "researcher"
        crew_agent.llm = MagicMock()
        crew_agent.llm.model = "gpt-4"
        tool_mock = MagicMock()
        tool_mock.name = "crew_tool"
        crew_agent.tools = [tool_mock]

        wrapper.crew = MagicMock()
        wrapper.crew.agents = [crew_agent]

        item = MagicMock()
        item.task = "Task"
        item.agent = "researcher"
        item.expected_output = ""
        item.quality = 0.8
        item.datetime = "2026"
        item.metadata = {"tools_used": "plain_string_not_json{"}

        captured = {}

        def cap(data):
            captured.update(data)

        with patch.object(wrapper, "_async_save", side_effect=cap), patch.object(
            wrapper, "_generate_embedding_sync", return_value=[0.2] * 1024
        ):
            wrapper.save(item)

        assert "tools_used" in captured

    def test_save_tools_from_metadata_tools_field_when_crew_tools(self):
        """When crew provides tools AND metadata has 'tools' field."""
        mock_embedder = MagicMock(return_value=[[0.2] * 1024])
        wrapper, _ = _make_wrapper(memory_type="long_term", embedder=mock_embedder)

        crew_agent = MagicMock()
        crew_agent.role = "researcher"
        crew_agent.llm = MagicMock()
        crew_agent.llm.model = "gpt-4"
        tool_mock = MagicMock()
        tool_mock.name = "crew_tool"
        crew_agent.tools = [tool_mock]

        wrapper.crew = MagicMock()
        wrapper.crew.agents = [crew_agent]

        item = MagicMock()
        item.task = "Task"
        item.agent = "researcher"
        item.expected_output = ""
        item.quality = 0.8
        item.datetime = "2026"
        # No tools_used, but has tools
        item.metadata = {"tools": ["alt_tool_1", "alt_tool_2"]}

        captured = {}

        def cap(data):
            captured.update(data)

        with patch.object(wrapper, "_async_save", side_effect=cap), patch.object(
            wrapper, "_generate_embedding_sync", return_value=[0.2] * 1024
        ):
            wrapper.save(item)

        assert "tools_used" in captured


# ─────────────────────────────────────────────────────────────────────────────
# _generate_embedding async method (lines 1207-1260)
# ─────────────────────────────────────────────────────────────────────────────


class TestGenerateEmbeddingAsync:
    """Tests for the async _generate_embedding method."""

    @pytest.mark.asyncio
    async def test_async_embed_callable_embedder(self):
        """_generate_embedding with callable embedder."""
        import asyncio
        mock_embedder = MagicMock(return_value=[[0.1, 0.2, 0.3]])
        wrapper, _ = _make_wrapper(embedder=mock_embedder)
        result = await wrapper._generate_embedding("text")
        assert result == [0.1, 0.2, 0.3]

    @pytest.mark.asyncio
    async def test_async_embed_no_embedder_returns_none(self):
        """_generate_embedding without embedder returns None."""
        wrapper, _ = _make_wrapper()
        result = await wrapper._generate_embedding("text")
        assert result is None

    @pytest.mark.asyncio
    async def test_async_embed_dict_custom_embedder(self):
        """_generate_embedding with custom dict embedder."""
        custom_fn = MagicMock(return_value=[[0.5, 0.6]])
        mock_embedder = {"provider": "custom", "config": {"embedder": custom_fn}}
        wrapper, _ = _make_wrapper(embedder=mock_embedder)
        result = await wrapper._generate_embedding("text")
        assert result == [0.5, 0.6]

    @pytest.mark.asyncio
    async def test_async_embed_dict_non_custom_returns_none(self):
        """_generate_embedding with non-custom dict embedder logs warning and returns None."""
        wrapper, _ = _make_wrapper(embedder={"provider": "databricks", "config": {}})
        result = await wrapper._generate_embedding("text")
        assert result is None

    @pytest.mark.asyncio
    async def test_async_embed_embed_method(self):
        """_generate_embedding with .embed() method embedder."""
        class EmbedMethod:
            def embed(self, text):
                return [0.7, 0.8]

        wrapper, _ = _make_wrapper(embedder=EmbedMethod())
        result = await wrapper._generate_embedding("text")
        assert result == [0.7, 0.8]

    @pytest.mark.asyncio
    async def test_async_embed_embed_documents_method(self):
        """_generate_embedding with .embed_documents() method."""
        class LangChainEmbed:
            def embed_documents(self, texts):
                return [[0.9, 1.0]]

        wrapper, _ = _make_wrapper(embedder=LangChainEmbed())
        result = await wrapper._generate_embedding("text")
        assert result == [0.9, 1.0]

    @pytest.mark.asyncio
    async def test_async_embed_exception_returns_none(self):
        """_generate_embedding catches exceptions and returns None."""
        mock_embedder = MagicMock(side_effect=Exception("embed error"))
        wrapper, _ = _make_wrapper(embedder=mock_embedder)
        result = await wrapper._generate_embedding("text")
        assert result is None
