"""
Tests for LakebaseMemoryService (unified single-table model).
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.services.lakebase_memory_service import (
    LakebaseMemoryService,
    PGVECTOR_ADMIN_INSTRUCTIONS,
)


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
        # First call: SELECT version()
        version_result = MagicMock()
        version_result.scalar.return_value = "PostgreSQL 15.4"

        # Second call: check pgvector extension
        pgvector_result = MagicMock()
        pgvector_result.fetchone.return_value = ("vector",)

        mock_session.execute = AsyncMock(side_effect=[version_result, pgvector_result])

        with patch(
            "src.services.lakebase_memory_service.get_lakebase_session",
            return_value=_make_lakebase_ctx(mock_session),
        ):
            result = await service.test_connection()
            assert result["success"] is True
            assert "pgvector" in result["message"].lower()
            assert result["details"]["pgvector_available"] is True
            assert result["details"]["pg_version"] == "PostgreSQL 15.4"
            assert "pgvector_setup_instructions" not in result["details"]

    @pytest.mark.asyncio
    async def test_connection_no_pgvector(self, service, mock_session):
        """Test connection when pgvector is not installed surfaces setup SQL."""
        version_result = MagicMock()
        version_result.scalar.return_value = None  # exercises "unknown" fallback

        pgvector_result = MagicMock()
        pgvector_result.fetchone.return_value = None

        mock_session.execute = AsyncMock(side_effect=[version_result, pgvector_result])

        with patch(
            "src.services.lakebase_memory_service.get_lakebase_session",
            return_value=_make_lakebase_ctx(mock_session),
        ):
            result = await service.test_connection()
            assert result["success"] is True
            assert result["details"]["pgvector_available"] is False
            assert result["details"]["pg_version"] == "unknown"
            assert "CREATE EXTENSION IF NOT EXISTS vector" in result["message"]
            assert result["details"]["pgvector_setup_instructions"] == PGVECTOR_ADMIN_INSTRUCTIONS
            assert result["details"]["pgvector_setup_sql"] == "CREATE EXTENSION IF NOT EXISTS vector;"

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
            assert result["details"]["error"] == "Connection refused"


class TestInitializeTables:
    """Tests for the initialize_tables method (unified single-table model)."""

    @pytest.mark.asyncio
    async def test_initialize_tables_existing_extension(self, service, mock_session):
        """Test successful initialization when pgvector extension already exists."""
        # First execute: SELECT extname ... -> scalar returns existing extension
        ext_result = MagicMock()
        ext_result.scalar.return_value = "vector"

        # All subsequent DDL executes return a generic mock
        generic = MagicMock()

        def _side_effect(*args, **kwargs):
            # Only the first execute is the extension probe
            if not getattr(_side_effect, "called", False):
                _side_effect.called = True
                return ext_result
            return generic

        mock_session.execute = AsyncMock(side_effect=_side_effect)

        with patch(
            "src.services.lakebase_memory_service.get_lakebase_session",
            return_value=_make_lakebase_ctx(mock_session),
        ):
            result = await service.initialize_tables(embedding_dimension=1024)
            assert result["success"] is True
            assert result["message"] == "All tables initialized"
            assert "memory" in result["tables"]
            assert result["tables"]["memory"]["table_name"] == "crew_memory"
            assert result["tables"]["memory"]["success"] is True

    @pytest.mark.asyncio
    async def test_initialize_tables_create_extension_fallback(self, service, mock_session):
        """Test the CREATE EXTENSION fallback loop succeeds for 'vector'."""
        # First execute: extension probe returns None (not existing)
        ext_probe = MagicMock()
        ext_probe.scalar.return_value = None
        generic = MagicMock()

        call_count = {"n": 0}

        def _side_effect(*args, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 1:
                return ext_probe
            return generic

        mock_session.execute = AsyncMock(side_effect=_side_effect)

        with patch(
            "src.services.lakebase_memory_service.get_lakebase_session",
            return_value=_make_lakebase_ctx(mock_session),
        ):
            result = await service.initialize_tables()
            assert result["success"] is True
            assert result["tables"]["memory"]["success"] is True

    @pytest.mark.asyncio
    async def test_initialize_tables_create_extension_total_failure(self, service, mock_session):
        """Test PGVECTOR_ADMIN_INSTRUCTIONS path when CREATE EXTENSION always fails."""
        ext_probe = MagicMock()
        ext_probe.scalar.return_value = None

        call_count = {"n": 0}

        def _side_effect(*args, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 1:
                return ext_probe  # probe -> None
            # SAVEPOINT, CREATE EXTENSION (fail), ROLLBACK ... raise on CREATE EXTENSION
            sql_text = str(args[0]) if args else ""
            if "CREATE EXTENSION" in sql_text:
                raise Exception("permission denied to create extension")
            return MagicMock()

        mock_session.execute = AsyncMock(side_effect=_side_effect)

        with patch(
            "src.services.lakebase_memory_service.get_lakebase_session",
            return_value=_make_lakebase_ctx(mock_session),
        ):
            result = await service.initialize_tables()
            assert result["success"] is False
            assert result["message"] == PGVECTOR_ADMIN_INSTRUCTIONS
            assert result["tables"] == {}

    @pytest.mark.asyncio
    async def test_initialize_tables_custom_name(self, service, mock_session):
        """Test initialization with a custom memory table name."""
        ext_result = MagicMock()
        ext_result.scalar.return_value = "vector"
        generic = MagicMock()

        call_count = {"n": 0}

        def _side_effect(*args, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 1:
                return ext_result
            return generic

        mock_session.execute = AsyncMock(side_effect=_side_effect)

        with patch(
            "src.services.lakebase_memory_service.get_lakebase_session",
            return_value=_make_lakebase_ctx(mock_session),
        ):
            result = await service.initialize_tables(memory_table="custom_mem")
            assert result["success"] is True
            assert result["tables"]["memory"]["table_name"] == "custom_mem"

    @pytest.mark.asyncio
    async def test_initialize_tables_per_table_exception(self, service, mock_session):
        """Test per-table exception handling (failed CREATE TABLE)."""
        ext_result = MagicMock()
        ext_result.scalar.return_value = "vector"

        call_count = {"n": 0}

        def _side_effect(*args, **kwargs):
            call_count["n"] += 1
            sql_text = str(args[0]) if args else ""
            if call_count["n"] == 1:
                return ext_result  # extension probe
            if "CREATE SCHEMA" in sql_text:
                return MagicMock()
            if "CREATE TABLE" in sql_text:
                raise Exception("table creation boom")
            return MagicMock()

        mock_session.execute = AsyncMock(side_effect=_side_effect)

        with patch(
            "src.services.lakebase_memory_service.get_lakebase_session",
            return_value=_make_lakebase_ctx(mock_session),
        ):
            result = await service.initialize_tables()
            assert result["success"] is False
            assert result["message"] == "Some tables failed to initialize"
            assert result["tables"]["memory"]["success"] is False
            assert "table creation boom" in result["tables"]["memory"]["message"]

    @pytest.mark.asyncio
    async def test_initialize_tables_connection_failure(self, service):
        """Test table initialization with connection failure."""
        with patch(
            "src.services.lakebase_memory_service.get_lakebase_session",
            side_effect=Exception("Connection failed"),
        ):
            result = await service.initialize_tables()
            assert result["success"] is False
            assert "Connection failed" in result["message"]
            assert result["tables"] == {}


class TestCheckTablesInitialized:
    """Tests for the check_tables_initialized method."""

    @pytest.mark.asyncio
    async def test_table_exists(self, service, mock_session):
        """Test when the memory table exists."""
        mock_result = MagicMock()
        mock_result.scalar.return_value = True
        mock_session.execute = AsyncMock(return_value=mock_result)

        with patch(
            "src.services.lakebase_memory_service.get_lakebase_session",
            return_value=_make_lakebase_ctx(mock_session),
        ):
            status = await service.check_tables_initialized()
            assert status["memory"] is True

    @pytest.mark.asyncio
    async def test_table_not_exists(self, service, mock_session):
        """Test when the memory table does not exist (scalar None -> False)."""
        mock_result = MagicMock()
        mock_result.scalar.return_value = None
        mock_session.execute = AsyncMock(return_value=mock_result)

        with patch(
            "src.services.lakebase_memory_service.get_lakebase_session",
            return_value=_make_lakebase_ctx(mock_session),
        ):
            status = await service.check_tables_initialized()
            assert status["memory"] is False

    @pytest.mark.asyncio
    async def test_check_tables_connection_failure(self, service):
        """Test check_tables_initialized exception path sets all to False."""
        with patch(
            "src.services.lakebase_memory_service.get_lakebase_session",
            side_effect=Exception("boom"),
        ):
            status = await service.check_tables_initialized()
            assert status["memory"] is False


class TestGetTableStats:
    """Tests for the get_table_stats method."""

    @pytest.mark.asyncio
    async def test_get_stats_exists_with_rows(self, service, mock_session):
        """Test getting stats when the table exists and has rows."""
        exists_result = MagicMock()
        exists_result.scalar.return_value = True
        count_result = MagicMock()
        count_result.scalar.return_value = 100

        mock_session.execute = AsyncMock(side_effect=[exists_result, count_result])

        with patch(
            "src.services.lakebase_memory_service.get_lakebase_session",
            return_value=_make_lakebase_ctx(mock_session),
        ):
            stats = await service.get_table_stats()
            assert stats["memory"]["exists"] is True
            assert stats["memory"]["row_count"] == 100
            assert stats["memory"]["table_name"] == "crew_memory"

    @pytest.mark.asyncio
    async def test_get_stats_not_exists(self, service, mock_session):
        """Test getting stats when the table does not exist (no count query)."""
        exists_result = MagicMock()
        exists_result.scalar.return_value = None
        mock_session.execute = AsyncMock(side_effect=[exists_result])

        with patch(
            "src.services.lakebase_memory_service.get_lakebase_session",
            return_value=_make_lakebase_ctx(mock_session),
        ):
            stats = await service.get_table_stats()
            assert stats["memory"]["exists"] is False
            assert stats["memory"]["row_count"] == 0

    @pytest.mark.asyncio
    async def test_get_stats_per_table_exception(self, service, mock_session):
        """Test per-table exception handling inside get_table_stats."""
        # The existence query itself raises -> hits inner except (330-341 region)
        mock_session.execute = AsyncMock(side_effect=Exception("stats boom"))

        with patch(
            "src.services.lakebase_memory_service.get_lakebase_session",
            return_value=_make_lakebase_ctx(mock_session),
        ):
            stats = await service.get_table_stats()
            assert stats["memory"]["exists"] is False
            assert stats["memory"]["row_count"] == 0
            assert "stats boom" in stats["memory"]["error"]

    @pytest.mark.asyncio
    async def test_get_stats_outer_exception(self, service):
        """Test outer exception handling (get_lakebase_session fails)."""
        with patch(
            "src.services.lakebase_memory_service.get_lakebase_session",
            side_effect=Exception("connect boom"),
        ):
            stats = await service.get_table_stats()
            assert stats["memory"]["exists"] is False
            assert stats["memory"]["row_count"] == 0
            assert "connect boom" in stats["memory"]["error"]


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
            assert result["documents"][0]["metadata"] == {"key": "val"}
            assert result["documents"][1]["metadata"] == {}
            assert result["documents"][1]["updated_at"] is None

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
    async def test_get_table_data_bad_json_metadata(self, service, mock_session):
        """Test that invalid JSON string metadata falls back to {} (lines 410-411)."""
        count_result = MagicMock()
        count_result.scalar.return_value = 1

        rows_result = MagicMock()
        rows_result.fetchall.return_value = [
            ("id1", "crew1", "grp1", "sess1", "agent1", "content", "not-json{", None, None, None),
        ]
        mock_session.execute = AsyncMock(side_effect=[count_result, rows_result])

        with patch(
            "src.services.lakebase_memory_service.get_lakebase_session",
            return_value=_make_lakebase_ctx(mock_session),
        ):
            result = await service.get_table_data("crew_entity_memory")
            assert result["success"] is True
            assert result["documents"][0]["metadata"] == {}

    @pytest.mark.asyncio
    async def test_get_table_data_connection_failure(self, service):
        """Test table data fetch with connection failure."""
        with patch(
            "src.services.lakebase_memory_service.get_lakebase_session",
            side_effect=Exception("Connection refused"),
        ):
            result = await service.get_table_data("crew_short_term_memory")
            assert result["success"] is False
            assert "Connection refused" in result["message"]
            assert result["documents"] == []


class TestGetEntityData:
    """Tests for the get_entity_data method (unified table)."""

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
    async def test_get_entity_data_legacy_table_name(self, service, mock_session):
        """Test that the legacy crew_entity_memory table name is accepted."""
        rows_result = MagicMock()
        rows_result.fetchall.return_value = []
        mock_session.execute = AsyncMock(return_value=rows_result)

        with patch(
            "src.services.lakebase_memory_service.get_lakebase_session",
            return_value=_make_lakebase_ctx(mock_session),
        ):
            result = await service.get_entity_data(memory_table="crew_entity_memory")
            assert result["entities"] == []
            assert result["relationships"] == []

    @pytest.mark.asyncio
    async def test_get_entity_data_invalid_table(self, service):
        """Test that invalid entity table names are rejected (line 462)."""
        result = await service.get_entity_data(memory_table="malicious_table")
        assert result["entities"] == []
        assert result["relationships"] == []

    @pytest.mark.asyncio
    async def test_get_entity_data_bad_json_metadata(self, service, mock_session):
        """Test that invalid JSON metadata falls back to {} (lines 496-497)."""
        rows_result = MagicMock()
        rows_result.fetchall.return_value = [
            ("id1", "crew1", "researcher", "Some entity description", "not-json{", None),
        ]
        mock_session.execute = AsyncMock(return_value=rows_result)

        with patch(
            "src.services.lakebase_memory_service.get_lakebase_session",
            return_value=_make_lakebase_ctx(mock_session),
        ):
            result = await service.get_entity_data()
            assert len(result["entities"]) == 1
            # metadata parse failed -> {} -> falls back to content[:80]
            assert result["entities"][0]["name"] == "Some entity description"
            assert result["entities"][0]["type"] == "entity"

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

    @pytest.mark.asyncio
    async def test_get_entity_data_non_string_relationship_target(self, service, mock_session):
        """Test entity data where related_to contains non-string targets."""
        rows_result = MagicMock()
        rows_result.fetchall.return_value = [
            ("id1", "crew1", "agent1", "Entity content",
             {"entity_name": "Alice", "relationships": [123]}, None),
        ]
        mock_session.execute = AsyncMock(return_value=rows_result)

        with patch(
            "src.services.lakebase_memory_service.get_lakebase_session",
            return_value=_make_lakebase_ctx(mock_session),
        ):
            result = await service.get_entity_data()
            assert len(result["relationships"]) == 1
            assert result["relationships"][0]["target"] == "123"
