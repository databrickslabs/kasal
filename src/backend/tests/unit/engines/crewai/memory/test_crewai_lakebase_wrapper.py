"""
Tests for CrewAILakebaseWrapper.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.engines.crewai.memory.crewai_lakebase_wrapper import CrewAILakebaseWrapper
from src.engines.crewai.memory.lakebase_pgvector_storage import LakebasePgVectorStorage


@pytest.fixture
def mock_storage():
    """Create a mock LakebasePgVectorStorage."""
    storage = MagicMock(spec=LakebasePgVectorStorage)
    storage.memory_type = "short_term"
    storage.crew_id = "test_group_crew_abc123"
    storage.save = AsyncMock()
    storage.search = AsyncMock(return_value=[])
    storage.clear = AsyncMock()
    return storage


@pytest.fixture
def mock_embedder():
    """Create a mock embedder that returns fixed embeddings."""
    embedder = MagicMock()
    embedder.side_effect = lambda texts: [[0.1] * 1024 for _ in texts]
    return embedder


@pytest.fixture
def wrapper(mock_storage, mock_embedder):
    """Create a CrewAILakebaseWrapper instance."""
    return CrewAILakebaseWrapper(
        storage=mock_storage,
        embedder=mock_embedder,
    )


class TestSave:
    """Tests for the save method."""

    def test_save_string_content(self, wrapper, mock_storage, mock_embedder):
        """Test saving string content."""
        with patch(
            "src.engines.crewai.memory.crewai_lakebase_wrapper._run_async",
            side_effect=lambda coro: None,
        ):
            wrapper.save("Hello world")
            mock_embedder.assert_called_once_with(["Hello world"])

    def test_save_dict_content(self, wrapper, mock_storage, mock_embedder):
        """Test saving dict content with 'data' key."""
        with patch(
            "src.engines.crewai.memory.crewai_lakebase_wrapper._run_async",
            side_effect=lambda coro: None,
        ):
            wrapper.save({"data": "test data", "score": 0.95})
            mock_embedder.assert_called_once_with(["test data"])

    def test_save_empty_content_skipped(self, wrapper, mock_embedder):
        """Test that empty content is skipped."""
        with patch(
            "src.engines.crewai.memory.crewai_lakebase_wrapper._run_async",
        ) as mock_run:
            wrapper.save("")
            mock_run.assert_not_called()
            mock_embedder.assert_not_called()

    def test_save_with_agent_string(self, wrapper, mock_embedder):
        """Test saving with agent as string."""
        with patch(
            "src.engines.crewai.memory.crewai_lakebase_wrapper._run_async",
            side_effect=lambda coro: None,
        ):
            wrapper.save("content", agent="researcher")
            mock_embedder.assert_called_once()

    def test_save_with_agent_object(self, wrapper, mock_embedder):
        """Test saving with agent as object with role attribute."""
        agent = MagicMock()
        agent.role = "researcher"
        with patch(
            "src.engines.crewai.memory.crewai_lakebase_wrapper._run_async",
            side_effect=lambda coro: None,
        ):
            wrapper.save("content", agent=agent)
            mock_embedder.assert_called_once()


class TestSearch:
    """Tests for the search method."""

    def test_search_returns_formatted_results(self, wrapper, mock_embedder):
        """Test that search returns properly formatted results."""
        mock_results = [
            {"content": "result 1", "distance": 0.1, "metadata": {}, "score": 0.9},
            {"content": "result 2", "distance": 0.3, "metadata": {"key": "val"}, "score": 0.7},
        ]
        with patch(
            "src.engines.crewai.memory.crewai_lakebase_wrapper._run_async",
            return_value=mock_results,
        ):
            results = wrapper.search("query text", limit=5)
            assert len(results) == 2
            assert results[0]["context"] == "result 1"
            assert results[0]["score"] == pytest.approx(0.9)

    def test_search_empty_query_returns_empty(self, wrapper, mock_embedder):
        """Test that empty query returns empty results."""
        results = wrapper.search("")
        assert results == []
        mock_embedder.assert_not_called()

    def test_search_filters_by_score_threshold(self, wrapper, mock_embedder):
        """Test that results below score threshold are filtered out."""
        mock_results = [
            {"content": "good", "distance": 0.1, "metadata": {}, "score": 0.9},
            {"content": "bad", "distance": 0.9, "metadata": {}, "score": 0.1},
        ]
        with patch(
            "src.engines.crewai.memory.crewai_lakebase_wrapper._run_async",
            return_value=mock_results,
        ):
            results = wrapper.search("query", score_threshold=0.35)
            # distance 0.9 > (1.0 - 0.35) = 0.65, so "bad" should be filtered
            assert len(results) == 1
            assert results[0]["context"] == "good"

    def test_search_handles_errors_gracefully(self, wrapper, mock_embedder):
        """Test that search handles errors without raising."""
        mock_embedder.side_effect = Exception("Embedding failed")
        results = wrapper.search("query")
        assert results == []


class TestReset:
    """Tests for the reset method."""

    def test_reset_calls_clear(self, wrapper, mock_storage):
        """Test that reset delegates to storage.clear()."""
        with patch(
            "src.engines.crewai.memory.crewai_lakebase_wrapper._run_async",
            side_effect=lambda coro: None,
        ):
            wrapper.reset()

    def test_reset_handles_errors(self, wrapper, mock_storage):
        """Test that reset handles errors gracefully."""
        with patch(
            "src.engines.crewai.memory.crewai_lakebase_wrapper._run_async",
            side_effect=Exception("Clear failed"),
        ):
            # Should not raise
            wrapper.reset()


class TestSaveLongTermKwargs:
    """Tests for long-term memory save via keyword arguments (CrewAI's actual call pattern)."""

    @pytest.fixture
    def lt_wrapper(self, mock_embedder):
        """Create a long-term memory wrapper."""
        storage = MagicMock(spec=LakebasePgVectorStorage)
        storage.memory_type = "long_term"
        storage.crew_id = "test_group_crew_abc123"
        storage.save = AsyncMock()
        storage.search = AsyncMock(return_value=[])
        storage.clear = AsyncMock()
        return CrewAILakebaseWrapper(storage=storage, embedder=mock_embedder)

    def test_save_long_term_with_task_description_kwarg(self, lt_wrapper, mock_embedder):
        """Test saving long-term memory via kwargs (CrewAI LongTermMemory.save() pattern)."""
        with patch(
            "src.engines.crewai.memory.crewai_lakebase_wrapper._run_async",
            side_effect=lambda coro: None,
        ):
            lt_wrapper.save(
                task_description="Search for relevant documents",
                score=0.85,
                metadata={"agent": "researcher", "quality": 0.85},
                datetime="2026-02-28T20:00:00",
            )
            mock_embedder.assert_called_once_with(["Search for relevant documents"])

    def test_save_long_term_kwargs_populates_metadata(self, lt_wrapper, mock_embedder):
        """Test that long-term kwargs save populates metadata correctly."""
        captured_coro = None
        def capture_coro(coro):
            nonlocal captured_coro
            captured_coro = coro
        with patch(
            "src.engines.crewai.memory.crewai_lakebase_wrapper._run_async",
            side_effect=capture_coro,
        ):
            lt_wrapper.save(
                task_description="Analyze data trends",
                score=0.9,
                metadata={"agent": "analyst", "expected_output": "summary"},
                datetime="2026-02-28T21:00:00",
            )
            # Verify the storage.save was called (via the coroutine)
            mock_embedder.assert_called_once_with(["Analyze data trends"])

    def test_save_long_term_kwargs_empty_task_description_skipped(self, lt_wrapper, mock_embedder):
        """Test that empty task_description is skipped."""
        with patch(
            "src.engines.crewai.memory.crewai_lakebase_wrapper._run_async",
        ) as mock_run:
            lt_wrapper.save(
                task_description="",
                score=0.5,
                metadata={},
                datetime="2026-02-28T20:00:00",
            )
            mock_run.assert_not_called()
            mock_embedder.assert_not_called()

    def test_save_long_term_kwargs_adds_task_description_to_metadata(self, lt_wrapper, mock_embedder):
        """Test that task_description is added to metadata dict."""
        with patch(
            "src.engines.crewai.memory.crewai_lakebase_wrapper._run_async",
            side_effect=lambda coro: None,
        ):
            metadata = {"agent": "researcher"}
            lt_wrapper.save(
                task_description="Find all user mentions",
                score=0.7,
                metadata=metadata,
                datetime="2026-02-28T20:00:00",
            )
            # Verify task_description was added to metadata
            assert metadata["task_description"] == "Find all user mentions"
            assert metadata["quality"] == 0.7
            assert metadata["datetime"] == "2026-02-28T20:00:00"

    def test_save_long_term_item_object_still_works(self, lt_wrapper, mock_embedder):
        """Test that LongTermMemoryItem object path still works."""
        item = MagicMock()
        item.task = "Research market trends"
        item.task_description = "Research market trends"
        item.agent = "analyst"
        item.expected_output = "report"
        item.datetime = "2026-02-28T20:00:00"
        item.quality = 0.8
        item.metadata = {"extra": "data"}
        item.__dict__ = {
            "task": "Research market trends",
            "agent": "analyst",
            "expected_output": "report",
            "datetime": "2026-02-28T20:00:00",
            "quality": 0.8,
            "metadata": {"extra": "data"},
        }

        with patch(
            "src.engines.crewai.memory.crewai_lakebase_wrapper._run_async",
            side_effect=lambda coro: None,
        ):
            lt_wrapper.save(item)
            mock_embedder.assert_called_once_with(["Research market trends"])


class TestLoad:
    """Tests for the load method (LongTermMemory compatibility)."""

    def test_load_delegates_to_search(self, wrapper, mock_embedder):
        """Test that load calls search and formats results."""
        mock_results = [
            {"content": "result 1", "distance": 0.1, "metadata": {}, "score": 0.9},
        ]
        with patch(
            "src.engines.crewai.memory.crewai_lakebase_wrapper._run_async",
            return_value=mock_results,
        ):
            results = wrapper.load("task query", latest_n=3)
            assert len(results) == 1
            assert results[0]["content"] == "result 1"

    def test_load_handles_errors_gracefully(self, wrapper, mock_embedder):
        """Test that load handles errors without raising."""
        mock_embedder.side_effect = Exception("Embedding failed")
        results = wrapper.load("task query")
        assert results == []


class TestSetAgentContext:
    """Tests for agent context management."""

    def test_set_agent_context(self, wrapper):
        """Test setting agent context."""
        agent = MagicMock()
        agent.role = "researcher"
        wrapper.set_agent_context(agent)
        assert wrapper.agent_context == agent
