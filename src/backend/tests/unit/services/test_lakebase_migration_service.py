"""
Unit tests for services/lakebase_migration_service.py

Auto-generated test template. TODO: Add comprehensive test coverage.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from src.services.lakebase_migration_service import LakebaseMigrationService



class TestLakebaseMigrationService:
    """Tests for LakebaseMigrationService"""

    @pytest.fixture
    def lakebasemigration(self):
        """Create LakebaseMigrationService instance for testing"""
        # TODO: Implement fixture
        pass

    def test_lakebasemigrationservice_initialization(self, lakebasemigration):
        """Test LakebaseMigrationService initializes correctly"""
        # TODO: Implement test
        pass

    def test_lakebasemigrationservice_basic_functionality(self, lakebasemigration):
        """Test LakebaseMigrationService basic functionality"""
        # TODO: Implement test
        pass

    def test_lakebasemigrationservice_error_handling(self, lakebasemigration):
        """Test LakebaseMigrationService handles errors correctly"""
        # TODO: Implement test
        pass


class TestGetTableListSync:
    """Tests for get_table_list_sync function"""

    def test_get_table_list_sync_success(self):
        """Test get_table_list_sync succeeds with valid input"""
        # TODO: Implement test
        pass

    def test_get_table_list_sync_invalid_input(self):
        """Test get_table_list_sync handles invalid input"""
        # TODO: Implement test
        pass


class TestGetSortedTables:
    """Tests for get_sorted_tables function"""

    def test_get_sorted_tables_success(self):
        """Test get_sorted_tables succeeds with valid input"""
        # TODO: Implement test
        pass

    def test_get_sorted_tables_invalid_input(self):
        """Test get_sorted_tables handles invalid input"""
        # TODO: Implement test
        pass


class TestConvertRowTypes:
    """Tests for convert_row_types function"""

    def test_convert_row_types_success(self):
        """Test convert_row_types succeeds with valid input"""
        # TODO: Implement test
        pass

    def test_convert_row_types_invalid_input(self):
        """Test convert_row_types handles invalid input"""
        # TODO: Implement test
        pass


class TestMigrateTableDataSync:
    """Tests for migrate_table_data_sync function"""

    @pytest.fixture
    def service(self):
        """Create a LakebaseMigrationService instance."""
        return LakebaseMigrationService()

    def test_migrate_table_data_sync_success(self):
        """Test migrate_table_data_sync succeeds with valid input"""
        # TODO: Implement test
        pass

    def test_migrate_table_data_sync_invalid_input(self):
        """Test migrate_table_data_sync handles invalid input"""
        # TODO: Implement test
        pass

    def test_migrate_table_data_sync_doc_embeddings_uses_explicit_columns_sqlite(
        self, service
    ):
        """Test that documentation_embeddings uses explicit column list (not SELECT *)
        when source is SQLite, to avoid the missing 'embedding' column error."""
        source_engine = MagicMock()
        lakebase_engine = MagicMock()

        # Set up source engine mock (SQLite path: connect())
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [
            (1, "web", "Title", "Content", "{}", "2024-01-01", "2024-01-01"),
        ]
        mock_conn.execute.return_value = mock_result
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        source_engine.connect.return_value = mock_conn

        # Set up lakebase engine mock
        mock_lb_conn = MagicMock()
        mock_lb_conn.__enter__ = MagicMock(return_value=mock_lb_conn)
        mock_lb_conn.__exit__ = MagicMock(return_value=False)
        lakebase_engine.begin.return_value = mock_lb_conn

        row_count, error = service.migrate_table_data_sync(
            table_name="documentation_embeddings",
            source_engine=source_engine,
            lakebase_engine=lakebase_engine,
            is_sqlite=True,
        )

        assert error is None
        assert row_count == 1

        # Verify the SQL used explicit columns, NOT SELECT *
        executed_sql = mock_conn.execute.call_args[0][0].text
        assert "SELECT *" not in executed_sql
        assert "id, source, title, content, doc_metadata" in executed_sql

    def test_migrate_table_data_sync_doc_embeddings_uses_explicit_columns_postgres(
        self, service
    ):
        """Test that documentation_embeddings uses explicit column list (not SELECT *)
        when source is PostgreSQL."""
        source_engine = MagicMock()
        lakebase_engine = MagicMock()

        # Set up source engine mock (PostgreSQL path: begin())
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [
            (1, "web", "Title", "Content", "{}", "2024-01-01", "2024-01-01"),
        ]
        mock_conn.execute.return_value = mock_result
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        source_engine.begin.return_value = mock_conn

        # Set up lakebase engine mock
        mock_lb_conn = MagicMock()
        mock_lb_conn.__enter__ = MagicMock(return_value=mock_lb_conn)
        mock_lb_conn.__exit__ = MagicMock(return_value=False)
        lakebase_engine.begin.return_value = mock_lb_conn

        row_count, error = service.migrate_table_data_sync(
            table_name="documentation_embeddings",
            source_engine=source_engine,
            lakebase_engine=lakebase_engine,
            is_sqlite=False,
        )

        assert error is None
        assert row_count == 1

        # Verify the SQL used explicit columns, NOT SELECT *
        executed_sql = mock_conn.execute.call_args[0][0].text
        assert "SELECT *" not in executed_sql
        assert "id, source, title, content, doc_metadata" in executed_sql

    def test_migrate_table_data_sync_regular_table_uses_select_star(self, service):
        """Test that regular tables (not documentation_embeddings) still use SELECT *."""
        source_engine = MagicMock()
        lakebase_engine = MagicMock()

        # Set up source engine mock (SQLite path)
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [
            (1, "value1", "value2"),
        ]
        mock_result.keys.return_value = ["id", "col1", "col2"]
        mock_conn.execute.return_value = mock_result
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        source_engine.connect.return_value = mock_conn

        # Set up lakebase engine mock
        mock_lb_conn = MagicMock()
        mock_lb_conn.__enter__ = MagicMock(return_value=mock_lb_conn)
        mock_lb_conn.__exit__ = MagicMock(return_value=False)
        lakebase_engine.begin.return_value = mock_lb_conn

        row_count, error = service.migrate_table_data_sync(
            table_name="agents",
            source_engine=source_engine,
            lakebase_engine=lakebase_engine,
            is_sqlite=True,
        )

        assert error is None
        assert row_count == 1

        # Verify regular tables use SELECT *
        executed_sql = mock_conn.execute.call_args[0][0].text
        assert "SELECT *" in executed_sql


class TestFkExistenceFilters:
    """Tests for fk_existence_filters dict populated during __init__."""

    @pytest.fixture
    def service(self):
        return LakebaseMigrationService()

    def test_fk_existence_filters_contains_execution_trace(self, service):
        """execution_trace must have an FK filter referencing executionhistory."""
        assert "execution_trace" in service.fk_existence_filters
        assert "executionhistory" in service.fk_existence_filters["execution_trace"]

    def test_fk_existence_filters_contains_taskstatus(self, service):
        """taskstatus must have an FK filter referencing executionhistory."""
        assert "taskstatus" in service.fk_existence_filters
        assert "executionhistory" in service.fk_existence_filters["taskstatus"]

    def test_fk_existence_filters_contains_llm_usage_billing(self, service):
        """llm_usage_billing must have an FK filter referencing executionhistory."""
        assert "llm_usage_billing" in service.fk_existence_filters
        assert "executionhistory" in service.fk_existence_filters["llm_usage_billing"]

    def test_fk_existence_filters_does_not_contain_users(self, service):
        """Top-level tables like users should not have FK filters."""
        assert "users" not in service.fk_existence_filters

    def test_fk_existence_filters_does_not_contain_agents(self, service):
        """agents should not have an FK filter."""
        assert "agents" not in service.fk_existence_filters


class TestMigrateTableDataSyncFKFilter:
    """Tests that migrate_table_data_sync adds WHERE clause for FK-filtered tables."""

    @pytest.fixture
    def service(self):
        return LakebaseMigrationService()

    def _make_source_engine(self, rows, columns, is_sqlite=True):
        """Build a mock source engine that returns given rows/columns."""
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = rows
        mock_result.keys.return_value = columns
        mock_conn.execute.return_value = mock_result
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)

        engine = MagicMock()
        if is_sqlite:
            engine.connect.return_value = mock_conn
        else:
            engine.begin.return_value = mock_conn
        return engine, mock_conn

    def _make_lakebase_engine(self):
        mock_lb_conn = MagicMock()
        mock_lb_conn.__enter__ = MagicMock(return_value=mock_lb_conn)
        mock_lb_conn.__exit__ = MagicMock(return_value=False)
        engine = MagicMock()
        engine.connect.return_value = mock_lb_conn
        return engine

    def test_execution_trace_gets_where_clause_sqlite(self, service):
        """execution_trace SELECT should include a WHERE clause for FK filtering (SQLite)."""
        rows = [(1, "job-1", "{}", "2024-01-01")]
        columns = ["id", "job_id", "output", "created_at"]
        source_engine, source_conn = self._make_source_engine(rows, columns, is_sqlite=True)
        lakebase_engine = self._make_lakebase_engine()

        row_count, error = service.migrate_table_data_sync(
            table_name="execution_trace",
            source_engine=source_engine,
            lakebase_engine=lakebase_engine,
            is_sqlite=True,
        )

        assert error is None
        assert row_count == 1
        executed_sql = source_conn.execute.call_args[0][0].text
        assert "WHERE" in executed_sql
        assert 'job_id IN (SELECT job_id FROM "executionhistory")' in executed_sql

    def test_execution_trace_gets_where_clause_postgres(self, service):
        """execution_trace SELECT should include a WHERE clause for FK filtering (PostgreSQL)."""
        rows = [(1, "job-1", "{}", "2024-01-01")]
        columns = ["id", "job_id", "output", "created_at"]
        source_engine, source_conn = self._make_source_engine(rows, columns, is_sqlite=False)
        lakebase_engine = self._make_lakebase_engine()

        row_count, error = service.migrate_table_data_sync(
            table_name="execution_trace",
            source_engine=source_engine,
            lakebase_engine=lakebase_engine,
            is_sqlite=False,
        )

        assert error is None
        assert row_count == 1
        executed_sql = source_conn.execute.call_args[0][0].text
        assert "WHERE" in executed_sql
        assert 'job_id IN (SELECT job_id FROM "executionhistory")' in executed_sql

    def test_normal_table_has_no_where_clause(self, service):
        """A table not in fk_existence_filters should NOT get a WHERE clause."""
        rows = [(1, "val")]
        columns = ["id", "col1"]
        source_engine, source_conn = self._make_source_engine(rows, columns, is_sqlite=True)
        lakebase_engine = self._make_lakebase_engine()

        row_count, error = service.migrate_table_data_sync(
            table_name="users",
            source_engine=source_engine,
            lakebase_engine=lakebase_engine,
            is_sqlite=True,
        )

        assert error is None
        assert row_count == 1
        executed_sql = source_conn.execute.call_args[0][0].text
        assert "WHERE" not in executed_sql
        assert "SELECT *" in executed_sql


class TestMigrateTableDataSyncDocEmbeddings:
    """Tests for documentation_embeddings special handling in migrate_table_data_sync."""

    @pytest.fixture
    def service(self):
        return LakebaseMigrationService()

    def test_doc_embeddings_uses_explicit_columns_not_select_star(self, service):
        """documentation_embeddings must use an explicit column list, never SELECT *."""
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [
            (1, "web", "Title", "Content", "{}", "2024-01-01", "2024-01-01"),
        ]
        mock_conn.execute.return_value = mock_result
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)

        source_engine = MagicMock()
        source_engine.connect.return_value = mock_conn

        mock_lb_conn = MagicMock()
        mock_lb_conn.__enter__ = MagicMock(return_value=mock_lb_conn)
        mock_lb_conn.__exit__ = MagicMock(return_value=False)
        lakebase_engine = MagicMock()
        lakebase_engine.connect.return_value = mock_lb_conn

        row_count, error = service.migrate_table_data_sync(
            table_name="documentation_embeddings",
            source_engine=source_engine,
            lakebase_engine=lakebase_engine,
            is_sqlite=True,
        )

        assert error is None
        assert row_count == 1
        executed_sql = mock_conn.execute.call_args[0][0].text
        assert "SELECT *" not in executed_sql
        assert "id, source, title, content, doc_metadata" in executed_sql

    def test_regular_table_uses_select_star(self, service):
        """Regular tables should use SELECT * (not explicit columns)."""
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [(1, "val")]
        mock_result.keys.return_value = ["id", "col1"]
        mock_conn.execute.return_value = mock_result
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)

        source_engine = MagicMock()
        source_engine.connect.return_value = mock_conn

        mock_lb_conn = MagicMock()
        mock_lb_conn.__enter__ = MagicMock(return_value=mock_lb_conn)
        mock_lb_conn.__exit__ = MagicMock(return_value=False)
        lakebase_engine = MagicMock()
        lakebase_engine.connect.return_value = mock_lb_conn

        row_count, error = service.migrate_table_data_sync(
            table_name="tools",
            source_engine=source_engine,
            lakebase_engine=lakebase_engine,
            is_sqlite=True,
        )

        assert error is None
        executed_sql = mock_conn.execute.call_args[0][0].text
        assert "SELECT *" in executed_sql


class TestGetMigrationWaves:
    """Tests for get_migration_waves grouping tables by FK dependencies."""

    @pytest.fixture
    def service(self):
        return LakebaseMigrationService()

    @patch("src.services.lakebase_migration_service.Base")
    def test_independent_tables_in_same_wave(self, mock_base, service):
        """Tables with no FK dependencies should land in the same wave."""
        # Build fake metadata with no FK constraints
        table_a = MagicMock()
        table_a.name = "table_a"
        table_a.foreign_keys = set()

        table_b = MagicMock()
        table_b.name = "table_b"
        table_b.foreign_keys = set()

        mock_base.metadata.sorted_tables = [table_a, table_b]

        waves = service.get_migration_waves(["table_a", "table_b"])

        # Both tables are independent, they should be in the first wave
        assert len(waves) == 1
        assert set(waves[0]) == {"table_a", "table_b"}

    @patch("src.services.lakebase_migration_service.Base")
    def test_dependent_tables_in_later_wave(self, mock_base, service):
        """A table with an FK to another should appear in a later wave."""
        # table_parent has no FK
        table_parent = MagicMock()
        table_parent.name = "table_parent"
        table_parent.foreign_keys = set()

        # table_child depends on table_parent via FK
        fk = MagicMock()
        fk.column.table.name = "table_parent"
        table_child = MagicMock()
        table_child.name = "table_child"
        table_child.foreign_keys = {fk}

        mock_base.metadata.sorted_tables = [table_parent, table_child]

        waves = service.get_migration_waves(["table_parent", "table_child"])

        assert len(waves) == 2
        assert "table_parent" in waves[0]
        assert "table_child" in waves[1]

    @patch("src.services.lakebase_migration_service.Base")
    def test_multi_level_dependencies(self, mock_base, service):
        """Three-level dependency chain produces three waves."""
        tbl_a = MagicMock()
        tbl_a.name = "a"
        tbl_a.foreign_keys = set()

        fk_b = MagicMock()
        fk_b.column.table.name = "a"
        tbl_b = MagicMock()
        tbl_b.name = "b"
        tbl_b.foreign_keys = {fk_b}

        fk_c = MagicMock()
        fk_c.column.table.name = "b"
        tbl_c = MagicMock()
        tbl_c.name = "c"
        tbl_c.foreign_keys = {fk_c}

        mock_base.metadata.sorted_tables = [tbl_a, tbl_b, tbl_c]

        waves = service.get_migration_waves(["a", "b", "c"])

        assert len(waves) == 3
        assert waves[0] == ["a"]
        assert waves[1] == ["b"]
        assert waves[2] == ["c"]

    @patch("src.services.lakebase_migration_service.Base")
    def test_table_not_in_metadata_treated_as_independent(self, mock_base, service):
        """Tables absent from SQLAlchemy metadata should be treated as having no FK deps."""
        tbl_known = MagicMock()
        tbl_known.name = "known"
        tbl_known.foreign_keys = set()

        mock_base.metadata.sorted_tables = [tbl_known]

        waves = service.get_migration_waves(["known", "unknown"])

        # Both should be in wave 0 since "unknown" has no metadata -> no deps
        assert len(waves) == 1
        assert set(waves[0]) == {"known", "unknown"}

    @patch("src.services.lakebase_migration_service.Base")
    def test_empty_table_list(self, mock_base, service):
        """An empty table list should produce no waves."""
        mock_base.metadata.sorted_tables = []
        waves = service.get_migration_waves([])
        assert waves == []


class TestConvertRowTypesExtended:
    """Additional tests for convert_row_types covering specific type conversions."""

    @pytest.fixture
    def service(self):
        return LakebaseMigrationService()

    def test_boolean_int_to_bool_conversion(self, service):
        """SQLite integer 0/1 should be converted to Python bool for boolean columns."""
        row = {"verbose": 1, "allow_delegation": 0, "name": "test"}
        result = service.convert_row_types(row, "agents", ["verbose", "allow_delegation", "name"])
        assert result["verbose"] is True
        assert result["allow_delegation"] is False
        assert result["name"] == "test"

    def test_json_dict_serialized(self, service):
        """Dict values in JSON columns should be serialized to JSON strings."""
        import json
        row = {"config": {"key": "value"}, "id": 1}
        result = service.convert_row_types(row, "tools", ["config", "id"])
        assert result["config"] == json.dumps({"key": "value"})
        assert result["id"] == 1

    def test_datetime_string_parsed(self, service):
        """ISO datetime strings should be parsed into datetime objects."""
        row = {"created_at": "2024-06-15T10:30:00", "id": 1}
        result = service.convert_row_types(row, "agents", ["created_at", "id"])
        from datetime import datetime
        assert isinstance(result["created_at"], datetime)
        assert result["created_at"].year == 2024
        assert result["created_at"].month == 6

    def test_datetime_with_z_suffix(self, service):
        """Datetime string with Z suffix should be parsed correctly."""
        row = {"created_at": "2024-06-15T10:30:00Z"}
        result = service.convert_row_types(row, "agents", ["created_at"])
        from datetime import datetime
        assert isinstance(result["created_at"], datetime)

    def test_none_values_preserved(self, service):
        """None values should pass through unchanged regardless of column type."""
        row = {"config": None, "verbose": None, "created_at": None, "name": None}
        result = service.convert_row_types(row, "agents", ["config", "verbose", "created_at", "name"])
        # JSON None -> None, bool None -> None, datetime None -> None
        assert result["config"] is None
        assert result["verbose"] is None
        assert result["created_at"] is None
        assert result["name"] is None


# TODO: Add more comprehensive tests
# TODO: Test edge cases and error handling
# TODO: Achieve 80%+ code coverage
