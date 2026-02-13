"""
Unit tests for CrewAIDatabricksWrapper.

Covers the changed code path:
- EntityRelationshipRetriever embedding model changed from
  'sentence-transformers/all-MiniLM-L6-v2' to 'databricks-gte-large-en' (line 79)
"""
import os
import sys
from unittest.mock import MagicMock

# Set database type to sqlite for testing
os.environ.setdefault("DATABASE_TYPE", "sqlite")
os.environ.setdefault("SQLITE_DB_PATH", ":memory:")

# Mock heavy third-party modules that are not available in the test environment.
# Must be done BEFORE any src.engines imports due to deep import chains.
_crewai_mock = MagicMock()
_crewai_tools_mock = MagicMock()

_MODULES_TO_MOCK = {
    'crewai': _crewai_mock,
    'crewai.tools': _crewai_mock.tools,
    'crewai.events': _crewai_mock.events,
    'crewai.flow': _crewai_mock.flow,
    'crewai.flow.flow': _crewai_mock.flow.flow,
    'crewai.flow.persistence': _crewai_mock.flow.persistence,
    'crewai.llm': _crewai_mock.llm,
    'crewai.memory': _crewai_mock.memory,
    'crewai.memory.storage': _crewai_mock.memory.storage,
    'crewai.memory.storage.rag_storage': _crewai_mock.memory.storage.rag_storage,
    'crewai.project': _crewai_mock.project,
    'crewai.tasks': _crewai_mock.tasks,
    'crewai.tasks.llm_guardrail': _crewai_mock.tasks.llm_guardrail,
    'crewai.tasks.task_output': _crewai_mock.tasks.task_output,
    'crewai.utilities': _crewai_mock.utilities,
    'crewai.utilities.converter': _crewai_mock.utilities.converter,
    'crewai.utilities.evaluators': _crewai_mock.utilities.evaluators,
    'crewai.utilities.evaluators.task_evaluator': _crewai_mock.utilities.evaluators.task_evaluator,
    'crewai.utilities.exceptions': _crewai_mock.utilities.exceptions,
    'crewai.utilities.internal_instructor': _crewai_mock.utilities.internal_instructor,
    'crewai.utilities.paths': _crewai_mock.utilities.paths,
    'crewai.utilities.printer': _crewai_mock.utilities.printer,
    'crewai.knowledge': _crewai_mock.knowledge,
    'crewai_tools': _crewai_tools_mock,
    'asyncpg': MagicMock(),
    'chromadb': MagicMock(),
}

_originals = {}
for _mod_name, _mock_obj in _MODULES_TO_MOCK.items():
    _originals[_mod_name] = sys.modules.get(_mod_name)
    sys.modules[_mod_name] = _mock_obj

import pytest
from unittest.mock import patch, AsyncMock

from src.engines.crewai.memory.crewai_databricks_wrapper import CrewAIDatabricksWrapper

# Immediately restore original modules after our import so that other test
# files collected later by pytest do not see the mocked crewai modules.
for _mod_name, _original in _originals.items():
    if _original is None:
        sys.modules.pop(_mod_name, None)
    else:
        sys.modules[_mod_name] = _original


@pytest.fixture
def mock_databricks_storage():
    """Create a mock DatabricksVectorStorage."""
    storage = MagicMock()
    storage.memory_type = "entity"
    storage.workspace_url = "https://example.databricks.com"
    storage.index_name = "test.catalog.entity_index"
    storage.endpoint_name = "test-endpoint"
    storage.user_token = "test-token"
    storage.group_id = "test-group"
    storage.job_id = "test-job-123"
    storage.crew_id = "test-crew-id"
    storage.embedding_dimension = 1024
    return storage


class TestCrewAIDatabricksWrapperRelationshipRetrieverInit:
    """Tests for the relationship retriever initialization with new embedding model."""

    def test_relationship_retriever_uses_databricks_gte_large_model(self, mock_databricks_storage):
        """
        Verify that when relationship retrieval is enabled for entity memory,
        the EntityRelationshipRetriever is initialized with 'databricks-gte-large-en'
        instead of the old 'sentence-transformers/all-MiniLM-L6-v2'.
        """
        with patch(
            "src.engines.crewai.memory.crewai_databricks_wrapper.EntityRelationshipRetriever"
        ) as mock_retriever_cls:
            mock_retriever_cls.return_value = MagicMock()

            # Patch imports that happen inside __init__
            with patch(
                "src.engines.crewai.memory.crewai_databricks_wrapper.MemoryBackendService",
                create=True,
            ):
                with patch(
                    "src.engines.crewai.memory.crewai_databricks_wrapper.UnitOfWork",
                    create=True,
                ):
                    wrapper = CrewAIDatabricksWrapper(
                        databricks_storage=mock_databricks_storage,
                        enable_relationship_retrieval=True,
                    )

            # Verify EntityRelationshipRetriever was called with the new model
            mock_retriever_cls.assert_called_once_with(
                memory_backend_service=None,
                embedding_model="databricks-gte-large-en",
            )
            assert wrapper.relationship_retriever is not None

    def test_relationship_retriever_not_created_when_disabled(self, mock_databricks_storage):
        """Verify relationship retriever is None when enable_relationship_retrieval is False."""
        wrapper = CrewAIDatabricksWrapper(
            databricks_storage=mock_databricks_storage,
            enable_relationship_retrieval=False,
        )
        assert wrapper.relationship_retriever is None

    def test_relationship_retriever_not_created_for_non_entity_memory(self, mock_databricks_storage):
        """Verify relationship retriever is None for non-entity memory types."""
        mock_databricks_storage.memory_type = "short_term"

        wrapper = CrewAIDatabricksWrapper(
            databricks_storage=mock_databricks_storage,
            enable_relationship_retrieval=True,
        )
        assert wrapper.relationship_retriever is None

    def test_relationship_retriever_handles_import_failure(self, mock_databricks_storage):
        """Verify graceful handling when required imports fail."""
        with patch(
            "src.engines.crewai.memory.crewai_databricks_wrapper.EntityRelationshipRetriever",
            side_effect=ImportError("Module not found"),
        ):
            wrapper = CrewAIDatabricksWrapper(
                databricks_storage=mock_databricks_storage,
                enable_relationship_retrieval=True,
            )
            assert wrapper.relationship_retriever is None
