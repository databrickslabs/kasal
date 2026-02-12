"""
Unit tests for EntityRelationshipRetriever.

Covers the changed code paths:
- Default embedding model changed from 'sentence-transformers/all-MiniLM-L6-v2' to 'databricks-gte-large-en'
- _compute_embedding() rewritten to call LLMManager.get_embedding() and return 1024-dim vectors
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
import numpy as np
from unittest.mock import AsyncMock, patch

from src.engines.crewai.memory.entity_relationship_retriever import (
    EntityRelationshipRetriever,
    EntityNode,
    RelationshipEdge,
    RetrievalCandidate,
)


@pytest.fixture
def mock_memory_backend_service():
    """Create a mock MemoryBackendService."""
    return AsyncMock()


@pytest.fixture
def retriever(mock_memory_backend_service):
    """Create an EntityRelationshipRetriever with default settings."""
    return EntityRelationshipRetriever(
        memory_backend_service=mock_memory_backend_service
    )


class TestEntityRelationshipRetrieverInit:
    """Tests for __init__ default values."""

    def test_default_embedding_model_is_databricks_gte_large(self, mock_memory_backend_service):
        """Verify the default embedding model is 'databricks-gte-large-en' (not the old MiniLM)."""
        retriever = EntityRelationshipRetriever(
            memory_backend_service=mock_memory_backend_service
        )
        assert retriever.embedding_model == "databricks-gte-large-en"

    def test_custom_embedding_model(self, mock_memory_backend_service):
        """Verify a custom embedding model can be passed."""
        retriever = EntityRelationshipRetriever(
            memory_backend_service=mock_memory_backend_service,
            embedding_model="custom-model"
        )
        assert retriever.embedding_model == "custom-model"

    def test_initial_state(self, retriever):
        """Verify initial state of entity graph and edges."""
        assert retriever.entity_graph == {}
        assert retriever.relationship_edges == []
        assert retriever.description_embeddings == {}


class TestComputeEmbedding:
    """Tests for the rewritten _compute_embedding method."""

    @pytest.mark.asyncio
    async def test_compute_embedding_calls_llm_manager(self, retriever):
        """Verify _compute_embedding calls LLMManager.get_embedding with correct model."""
        fake_embedding = [0.1] * 1024
        mock_llm_manager = MagicMock()
        mock_llm_manager.get_embedding = AsyncMock(return_value=fake_embedding)

        with patch.dict(
            "sys.modules",
            {"src.core.llm_manager": MagicMock(LLMManager=mock_llm_manager)}
        ):
            result = await retriever._compute_embedding("test text")

            mock_llm_manager.get_embedding.assert_called_once_with(
                "test text", model="databricks-gte-large-en"
            )
            assert isinstance(result, np.ndarray)
            assert len(result) == 1024
            np.testing.assert_array_almost_equal(result, np.array(fake_embedding))

    @pytest.mark.asyncio
    async def test_compute_embedding_returns_1024_dim_on_success(self, retriever):
        """Verify successful embedding returns a 1024-dimensional numpy array."""
        fake_embedding = list(range(1024))
        mock_llm_manager = MagicMock()
        mock_llm_manager.get_embedding = AsyncMock(return_value=fake_embedding)

        with patch.dict(
            "sys.modules",
            {"src.core.llm_manager": MagicMock(LLMManager=mock_llm_manager)}
        ):
            result = await retriever._compute_embedding("some description")

            assert result.shape == (1024,)
            assert result[0] == 0.0
            assert result[1023] == 1023.0

    @pytest.mark.asyncio
    async def test_compute_embedding_returns_zeros_on_empty_result(self, retriever):
        """Verify _compute_embedding returns 1024 zeros when LLMManager returns empty/None."""
        mock_llm_manager = MagicMock()
        mock_llm_manager.get_embedding = AsyncMock(return_value=None)

        with patch.dict(
            "sys.modules",
            {"src.core.llm_manager": MagicMock(LLMManager=mock_llm_manager)}
        ):
            result = await retriever._compute_embedding("test text")

            assert isinstance(result, np.ndarray)
            assert result.shape == (1024,)
            np.testing.assert_array_equal(result, np.zeros(1024))

    @pytest.mark.asyncio
    async def test_compute_embedding_returns_zeros_on_empty_list(self, retriever):
        """Verify _compute_embedding returns 1024 zeros when LLMManager returns empty list."""
        mock_llm_manager = MagicMock()
        mock_llm_manager.get_embedding = AsyncMock(return_value=[])

        with patch.dict(
            "sys.modules",
            {"src.core.llm_manager": MagicMock(LLMManager=mock_llm_manager)}
        ):
            result = await retriever._compute_embedding("test text")

            assert isinstance(result, np.ndarray)
            assert result.shape == (1024,)
            np.testing.assert_array_equal(result, np.zeros(1024))

    @pytest.mark.asyncio
    async def test_compute_embedding_returns_zeros_on_exception(self, retriever):
        """Verify _compute_embedding returns 1024 zeros when LLMManager raises an exception."""
        mock_llm_manager = MagicMock()
        mock_llm_manager.get_embedding = AsyncMock(
            side_effect=RuntimeError("LLM service unavailable")
        )

        with patch.dict(
            "sys.modules",
            {"src.core.llm_manager": MagicMock(LLMManager=mock_llm_manager)}
        ):
            result = await retriever._compute_embedding("test text")

            assert isinstance(result, np.ndarray)
            assert result.shape == (1024,)
            np.testing.assert_array_equal(result, np.zeros(1024))

    @pytest.mark.asyncio
    async def test_compute_embedding_uses_configured_model(self, mock_memory_backend_service):
        """Verify _compute_embedding uses the model specified at construction time."""
        retriever = EntityRelationshipRetriever(
            memory_backend_service=mock_memory_backend_service,
            embedding_model="custom-embed-model"
        )
        mock_llm_manager = MagicMock()
        mock_llm_manager.get_embedding = AsyncMock(return_value=[0.5] * 1024)

        with patch.dict(
            "sys.modules",
            {"src.core.llm_manager": MagicMock(LLMManager=mock_llm_manager)}
        ):
            await retriever._compute_embedding("hello")

            mock_llm_manager.get_embedding.assert_called_once_with(
                "hello", model="custom-embed-model"
            )
