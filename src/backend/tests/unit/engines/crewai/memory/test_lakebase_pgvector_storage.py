"""
Tests for LakebasePgVectorStorage.
"""
import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.engines.crewai.memory.lakebase_pgvector_storage import LakebasePgVectorStorage


@pytest.fixture
def storage():
    """Create a LakebasePgVectorStorage instance for testing."""
    return LakebasePgVectorStorage(
        table_name="crew_short_term_memory",
        memory_type="short_term",
        crew_id="test_group_crew_abc123",
        group_id="test_group",
        job_id="job_001",
        embedding_dimension=1024,
    )


@pytest.fixture
def long_term_storage():
    """Create a long-term memory storage instance."""
    return LakebasePgVectorStorage(
        table_name="crew_long_term_memory",
        memory_type="long_term",
        crew_id="test_group_crew_abc123",
        group_id="test_group",
        embedding_dimension=1024,
    )


@pytest.fixture
def mock_session():
    """Create a mock async session."""
    session = AsyncMock()
    session.execute = AsyncMock()
    return session


def _make_lakebase_ctx(mock_session):
    """Create an async context manager mock for get_lakebase_session."""
    ctx = AsyncMock()
    ctx.__aenter__ = AsyncMock(return_value=mock_session)
    ctx.__aexit__ = AsyncMock(return_value=False)
    return ctx


class TestSave:
    """Tests for the save method."""

    @pytest.mark.asyncio
    async def test_save_record(self, storage, mock_session):
        """Test saving a memory record."""
        with patch(
            "src.engines.crewai.memory.lakebase_pgvector_storage.get_lakebase_session",
            return_value=_make_lakebase_ctx(mock_session),
        ):
            await storage.save(
                record_id="rec_1",
                content="Test content",
                embedding=[0.1] * 1024,
                metadata={"key": "value"},
                agent="test_agent",
            )
            mock_session.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_save_generates_id_when_empty(self, storage, mock_session):
        """Test that save generates an ID when record_id is empty."""
        with patch(
            "src.engines.crewai.memory.lakebase_pgvector_storage.get_lakebase_session",
            return_value=_make_lakebase_ctx(mock_session),
        ):
            await storage.save(
                record_id="",
                content="Test content",
                embedding=[0.1] * 1024,
            )
            # Verify the execute was called (ID was generated)
            mock_session.execute.assert_called_once()
            call_args = mock_session.execute.call_args
            params = call_args[0][1]
            assert params["id"] != ""


class TestSearch:
    """Tests for the search method."""

    @pytest.mark.asyncio
    async def test_search_short_term_filters_by_session(self, storage, mock_session):
        """Test that short-term search includes session_id filter."""
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [
            ("id1", "content1", "{}", 0.9, "agent1", 0.1),
        ]
        mock_session.execute = AsyncMock(return_value=mock_result)

        with patch(
            "src.engines.crewai.memory.lakebase_pgvector_storage.get_lakebase_session",
            return_value=_make_lakebase_ctx(mock_session),
        ):
            results = await storage.search(
                query_embedding=[0.1] * 1024,
                k=5,
            )
            assert len(results) == 1
            assert results[0]["content"] == "content1"
            # Verify session_id filter was applied
            call_args = mock_session.execute.call_args
            params = call_args[0][1]
            assert params["session_id"] == "job_001"

    @pytest.mark.asyncio
    async def test_search_long_term_no_session_filter(self, long_term_storage, mock_session):
        """Test that long-term search does not filter by session_id."""
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_session.execute = AsyncMock(return_value=mock_result)

        with patch(
            "src.engines.crewai.memory.lakebase_pgvector_storage.get_lakebase_session",
            return_value=_make_lakebase_ctx(mock_session),
        ):
            await long_term_storage.search(query_embedding=[0.1] * 1024, k=3)
            call_args = mock_session.execute.call_args
            params = call_args[0][1]
            assert "session_id" not in params

    @pytest.mark.asyncio
    async def test_search_returns_formatted_results(self, storage, mock_session):
        """Test that search results are properly formatted."""
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [
            ("id1", "content1", json.dumps({"key": "val"}), 0.95, "agent1", 0.05),
            ("id2", "content2", None, None, "", 0.2),
        ]
        mock_session.execute = AsyncMock(return_value=mock_result)

        with patch(
            "src.engines.crewai.memory.lakebase_pgvector_storage.get_lakebase_session",
            return_value=_make_lakebase_ctx(mock_session),
        ):
            results = await storage.search(query_embedding=[0.1] * 1024, k=5)
            assert len(results) == 2
            assert results[0]["metadata"] == {"key": "val"}
            assert results[0]["distance"] == 0.05
            assert results[1]["metadata"] == {}


class TestClear:
    """Tests for the clear method."""

    @pytest.mark.asyncio
    async def test_clear_short_term_filters_by_session(self, storage, mock_session):
        """Test that clear for short-term includes session_id filter."""
        with patch(
            "src.engines.crewai.memory.lakebase_pgvector_storage.get_lakebase_session",
            return_value=_make_lakebase_ctx(mock_session),
        ):
            await storage.clear()
            call_args = mock_session.execute.call_args
            params = call_args[0][1]
            assert params["crew_id"] == "test_group_crew_abc123"
            assert params["session_id"] == "job_001"

    @pytest.mark.asyncio
    async def test_clear_long_term_no_session(self, long_term_storage, mock_session):
        """Test that clear for long-term only filters by crew_id."""
        with patch(
            "src.engines.crewai.memory.lakebase_pgvector_storage.get_lakebase_session",
            return_value=_make_lakebase_ctx(mock_session),
        ):
            await long_term_storage.clear()
            call_args = mock_session.execute.call_args
            params = call_args[0][1]
            assert params["crew_id"] == "test_group_crew_abc123"
            assert "session_id" not in params


class TestDelete:
    """Tests for the delete method."""

    @pytest.mark.asyncio
    async def test_delete_record(self, storage, mock_session):
        """Test deleting a single record."""
        with patch(
            "src.engines.crewai.memory.lakebase_pgvector_storage.get_lakebase_session",
            return_value=_make_lakebase_ctx(mock_session),
        ):
            await storage.delete("rec_1")
            call_args = mock_session.execute.call_args
            params = call_args[0][1]
            assert params["id"] == "rec_1"
            assert params["crew_id"] == "test_group_crew_abc123"


class TestGetStats:
    """Tests for the get_stats method."""

    @pytest.mark.asyncio
    async def test_get_stats(self, storage, mock_session):
        """Test getting storage statistics."""
        mock_result = MagicMock()
        mock_result.scalar.return_value = 42
        mock_session.execute = AsyncMock(return_value=mock_result)

        with patch(
            "src.engines.crewai.memory.lakebase_pgvector_storage.get_lakebase_session",
            return_value=_make_lakebase_ctx(mock_session),
        ):
            stats = await storage.get_stats()
            assert stats["record_count"] == 42
            assert stats["memory_type"] == "short_term"
            assert stats["crew_id"] == "test_group_crew_abc123"
