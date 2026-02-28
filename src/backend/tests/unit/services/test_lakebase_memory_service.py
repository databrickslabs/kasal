"""
Tests for LakebaseMemoryService.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.services.lakebase_memory_service import LakebaseMemoryService


@pytest.fixture
def service():
    """Create a LakebaseMemoryService instance."""
    return LakebaseMemoryService()


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


class TestTestConnection:
    """Tests for the test_connection method."""

    @pytest.mark.asyncio
    async def test_connection_success_with_pgvector(self, service, mock_session):
        """Test successful connection with pgvector installed."""
        # First call: check pgvector extension
        pgvector_result = MagicMock()
        pgvector_result.fetchone.return_value = ("vector",)

        # Second call: pg version
        version_result = MagicMock()
        version_result.scalar.return_value = "PostgreSQL 15.4"

        mock_session.execute = AsyncMock(side_effect=[pgvector_result, version_result])

        with patch(
            "src.services.lakebase_memory_service.get_lakebase_session",
            return_value=_make_lakebase_ctx(mock_session),
        ):
            result = await service.test_connection()
            assert result["success"] is True
            assert "pgvector" in result["message"].lower()
            assert result["details"]["pgvector_available"] is True

    @pytest.mark.asyncio
    async def test_connection_no_pgvector(self, service, mock_session):
        """Test connection when pgvector is not installed."""
        pgvector_result = MagicMock()
        pgvector_result.fetchone.return_value = None
        mock_session.execute = AsyncMock(return_value=pgvector_result)

        with patch(
            "src.services.lakebase_memory_service.get_lakebase_session",
            return_value=_make_lakebase_ctx(mock_session),
        ):
            result = await service.test_connection()
            assert result["success"] is True
            assert result["details"]["pgvector_available"] is False

    @pytest.mark.asyncio
    async def test_connection_failure(self, service):
        """Test connection failure."""
        with patch(
            "src.services.lakebase_memory_service.get_lakebase_session",
            side_effect=Exception("Connection refused"),
        ):
            result = await service.test_connection()
            assert result["success"] is False
            assert "Connection refused" in result["message"]


class TestInitializeTables:
    """Tests for the initialize_tables method."""

    @pytest.mark.asyncio
    async def test_initialize_tables_success(self, service, mock_session):
        """Test successful table initialization."""
        mock_session.execute = AsyncMock()

        with patch(
            "src.services.lakebase_memory_service.get_lakebase_session",
            return_value=_make_lakebase_ctx(mock_session),
        ):
            result = await service.initialize_tables(embedding_dimension=1024)
            assert result["success"] is True
            assert "short_term" in result["tables"]
            assert "long_term" in result["tables"]
            assert "entity" in result["tables"]
            # CREATE EXTENSION + (CREATE TABLE + 3 indexes) * 3 tables + extra session_id index
            assert mock_session.execute.call_count > 0

    @pytest.mark.asyncio
    async def test_initialize_tables_custom_names(self, service, mock_session):
        """Test initialization with custom table names."""
        mock_session.execute = AsyncMock()

        with patch(
            "src.services.lakebase_memory_service.get_lakebase_session",
            return_value=_make_lakebase_ctx(mock_session),
        ):
            result = await service.initialize_tables(
                short_term_table="custom_st",
                long_term_table="custom_lt",
                entity_table="custom_entity",
            )
            assert result["success"] is True
            assert result["tables"]["short_term"]["table_name"] == "custom_st"
            assert result["tables"]["long_term"]["table_name"] == "custom_lt"
            assert result["tables"]["entity"]["table_name"] == "custom_entity"

    @pytest.mark.asyncio
    async def test_initialize_tables_connection_failure(self, service):
        """Test table initialization with connection failure."""
        with patch(
            "src.services.lakebase_memory_service.get_lakebase_session",
            side_effect=Exception("Connection failed"),
        ):
            result = await service.initialize_tables()
            assert result["success"] is False


class TestCheckTablesInitialized:
    """Tests for the check_tables_initialized method."""

    @pytest.mark.asyncio
    async def test_all_tables_exist(self, service, mock_session):
        """Test when all tables exist."""
        mock_result = MagicMock()
        mock_result.scalar.return_value = True
        mock_session.execute = AsyncMock(return_value=mock_result)

        with patch(
            "src.services.lakebase_memory_service.get_lakebase_session",
            return_value=_make_lakebase_ctx(mock_session),
        ):
            status = await service.check_tables_initialized()
            assert status["short_term"] is True
            assert status["long_term"] is True
            assert status["entity"] is True

    @pytest.mark.asyncio
    async def test_no_tables_exist(self, service, mock_session):
        """Test when no tables exist."""
        mock_result = MagicMock()
        mock_result.scalar.return_value = False
        mock_session.execute = AsyncMock(return_value=mock_result)

        with patch(
            "src.services.lakebase_memory_service.get_lakebase_session",
            return_value=_make_lakebase_ctx(mock_session),
        ):
            status = await service.check_tables_initialized()
            assert status["short_term"] is False
            assert status["long_term"] is False
            assert status["entity"] is False


class TestGetTableStats:
    """Tests for the get_table_stats method."""

    @pytest.mark.asyncio
    async def test_get_stats_all_tables(self, service, mock_session):
        """Test getting stats when all tables exist."""
        exists_result = MagicMock()
        exists_result.scalar.return_value = True
        count_result = MagicMock()
        count_result.scalar.return_value = 100

        mock_session.execute = AsyncMock(side_effect=[
            exists_result, count_result,  # short_term
            exists_result, count_result,  # long_term
            exists_result, count_result,  # entity
        ])

        with patch(
            "src.services.lakebase_memory_service.get_lakebase_session",
            return_value=_make_lakebase_ctx(mock_session),
        ):
            stats = await service.get_table_stats()
            assert stats["short_term"]["exists"] is True
            assert stats["short_term"]["row_count"] == 100
            assert stats["long_term"]["exists"] is True
            assert stats["entity"]["exists"] is True


class TestGetTableData:
    """Tests for the get_table_data method."""

    @pytest.mark.asyncio
    async def test_get_table_data_success(self, service, mock_session):
        """Test fetching rows from a valid memory table."""
        from datetime import datetime, timezone

        ts = datetime(2026, 2, 28, 20, 0, 0, tzinfo=timezone.utc)
        count_result = MagicMock()
        count_result.scalar.return_value = 2

        rows_result = MagicMock()
        rows_result.fetchall.return_value = [
            ("id1", "crew1", "grp1", "sess1", "researcher", "Hello world", '{"key": "val"}', 0.9, ts, ts),
            ("id2", "crew1", "grp1", "sess1", "analyst", "Second row", {}, None, ts, None),
        ]
        mock_session.execute = AsyncMock(side_effect=[count_result, rows_result])

        with patch(
            "src.services.lakebase_memory_service.get_lakebase_session",
            return_value=_make_lakebase_ctx(mock_session),
        ):
            result = await service.get_table_data("crew_short_term_memory", limit=50)
            assert result["success"] is True
            assert result["total"] == 2
            assert len(result["documents"]) == 2
            assert result["documents"][0]["id"] == "id1"
            assert result["documents"][0]["text"] == "Hello world"
            assert result["documents"][0]["agent"] == "researcher"
            assert result["documents"][0]["score"] == 0.9
            assert result["documents"][1]["metadata"] == {}

    @pytest.mark.asyncio
    async def test_get_table_data_invalid_table_name(self, service):
        """Test that invalid table names are rejected."""
        result = await service.get_table_data("malicious_table")
        assert result["success"] is False
        assert "Invalid table name" in result["message"]
        assert result["documents"] == []

    @pytest.mark.asyncio
    async def test_get_table_data_json_string_metadata(self, service, mock_session):
        """Test that JSON string metadata is parsed."""
        count_result = MagicMock()
        count_result.scalar.return_value = 1

        rows_result = MagicMock()
        rows_result.fetchall.return_value = [
            ("id1", "crew1", "grp1", "sess1", "agent1", "content", '{"parsed": true}', None, None, None),
        ]
        mock_session.execute = AsyncMock(side_effect=[count_result, rows_result])

        with patch(
            "src.services.lakebase_memory_service.get_lakebase_session",
            return_value=_make_lakebase_ctx(mock_session),
        ):
            result = await service.get_table_data("crew_long_term_memory")
            assert result["success"] is True
            assert result["documents"][0]["metadata"] == {"parsed": True}

    @pytest.mark.asyncio
    async def test_get_table_data_connection_failure(self, service):
        """Test table data fetch with connection failure."""
        with patch(
            "src.services.lakebase_memory_service.get_lakebase_session",
            side_effect=Exception("Connection refused"),
        ):
            result = await service.get_table_data("crew_short_term_memory")
            assert result["success"] is False
            assert result["documents"] == []

    @pytest.mark.asyncio
    async def test_get_table_data_all_allowed_tables(self, service, mock_session):
        """Test that all three allowed table names are accepted."""
        count_result = MagicMock()
        count_result.scalar.return_value = 0
        rows_result = MagicMock()
        rows_result.fetchall.return_value = []
        mock_session.execute = AsyncMock(return_value=count_result)

        for table_name in ["crew_short_term_memory", "crew_long_term_memory", "crew_entity_memory"]:
            mock_session.execute = AsyncMock(side_effect=[count_result, rows_result])
            with patch(
                "src.services.lakebase_memory_service.get_lakebase_session",
                return_value=_make_lakebase_ctx(mock_session),
            ):
                result = await service.get_table_data(table_name)
                assert result["success"] is True


class TestGetEntityData:
    """Tests for the get_entity_data method."""

    @pytest.mark.asyncio
    async def test_get_entity_data_success(self, service, mock_session):
        """Test fetching entity data for graph visualization."""
        rows_result = MagicMock()
        rows_result.fetchall.return_value = [
            ("id1", "crew1", "researcher", "Alice is a data scientist",
             '{"entity_name": "Alice", "entity_type": "person", "related_to": ["Bob"]}', 0.9),
            ("id2", "crew1", "analyst", "Bob is an engineer",
             '{"entity_name": "Bob", "entity_type": "person"}', 0.8),
        ]
        mock_session.execute = AsyncMock(return_value=rows_result)

        with patch(
            "src.services.lakebase_memory_service.get_lakebase_session",
            return_value=_make_lakebase_ctx(mock_session),
        ):
            result = await service.get_entity_data()
            assert len(result["entities"]) >= 2
            assert result["entities"][0]["name"] == "Alice"
            assert result["entities"][0]["type"] == "person"
            assert len(result["relationships"]) >= 1
            assert result["relationships"][0]["source"] == "Alice"
            assert result["relationships"][0]["target"] == "Bob"

    @pytest.mark.asyncio
    async def test_get_entity_data_invalid_table(self, service):
        """Test that invalid entity table names are rejected."""
        result = await service.get_entity_data(entity_table="malicious_table")
        assert result["entities"] == []
        assert result["relationships"] == []

    @pytest.mark.asyncio
    async def test_get_entity_data_no_metadata(self, service, mock_session):
        """Test entity data with missing metadata uses content as name."""
        rows_result = MagicMock()
        rows_result.fetchall.return_value = [
            ("id1", "crew1", "researcher", "Some entity description", None, None),
        ]
        mock_session.execute = AsyncMock(return_value=rows_result)

        with patch(
            "src.services.lakebase_memory_service.get_lakebase_session",
            return_value=_make_lakebase_ctx(mock_session),
        ):
            result = await service.get_entity_data()
            assert len(result["entities"]) == 1
            # Falls back to content[:80]
            assert result["entities"][0]["name"] == "Some entity description"
            assert result["entities"][0]["type"] == "entity"

    @pytest.mark.asyncio
    async def test_get_entity_data_deduplicates_entities(self, service, mock_session):
        """Test that duplicate entity IDs are deduplicated."""
        rows_result = MagicMock()
        rows_result.fetchall.return_value = [
            ("id1", "crew1", "agent1", "content1",
             '{"entity_name": "Alice", "entity_type": "person"}', 0.9),
            ("id2", "crew1", "agent2", "content2",
             '{"entity_name": "Alice", "entity_type": "person"}', 0.8),
        ]
        mock_session.execute = AsyncMock(return_value=rows_result)

        with patch(
            "src.services.lakebase_memory_service.get_lakebase_session",
            return_value=_make_lakebase_ctx(mock_session),
        ):
            result = await service.get_entity_data()
            # Alice should appear only once
            alice_entities = [e for e in result["entities"] if e["name"] == "Alice"]
            assert len(alice_entities) == 1

    @pytest.mark.asyncio
    async def test_get_entity_data_connection_failure(self, service):
        """Test entity data fetch with connection failure."""
        with patch(
            "src.services.lakebase_memory_service.get_lakebase_session",
            side_effect=Exception("Connection refused"),
        ):
            result = await service.get_entity_data()
            assert result["entities"] == []
            assert result["relationships"] == []

    @pytest.mark.asyncio
    async def test_get_entity_data_comma_separated_relationships(self, service, mock_session):
        """Test entity data with comma-separated related_to string."""
        rows_result = MagicMock()
        rows_result.fetchall.return_value = [
            ("id1", "crew1", "agent1", "Entity content",
             '{"entity_name": "Alice", "related_to": "Bob, Charlie"}', None),
        ]
        mock_session.execute = AsyncMock(return_value=rows_result)

        with patch(
            "src.services.lakebase_memory_service.get_lakebase_session",
            return_value=_make_lakebase_ctx(mock_session),
        ):
            result = await service.get_entity_data()
            assert len(result["relationships"]) == 2
            targets = [r["target"] for r in result["relationships"]]
            assert "Bob" in targets
            assert "Charlie" in targets
