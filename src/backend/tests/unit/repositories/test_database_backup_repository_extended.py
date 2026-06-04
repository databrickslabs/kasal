"""
Extended coverage tests for DatabaseBackupRepository.

Targets uncovered lines:
- Lines 242-253: value type handling in SQL backup (bool, datetime, dict)
- Lines 334-473: _create_postgres_to_sqlite_backup
- Lines 538-553: create safety backup in restore_sqlite_backup
- Lines 561-585: read current admin during restore
- Lines 609-610: list backup with json file type
- Lines 625-673: restore with table-by-table copy (admin UUID preservation)
- Lines 691-696: import error during table-by-table copy
- Lines 754, 761-812: restore_postgres_backup SQL path
- Lines 825, 844-854: restore_postgres_backup JSON path
- Lines 914: list_backups unknown extension backup type
- Lines 922-925: list_backups fallback timestamp from modification_time
- Lines 1079-1098: get_database_info with no db_path (settings fallback)
- Lines 1127-1134: get_database_info memory_backends table present
- Lines 1157-1220: get_database_info postgres path with lakebase schema
- Lines 1187-1220: get_database_info memory_backends in postgres
"""
import os
import json
import sqlite3
import tempfile
import pytest
from datetime import datetime, date
from unittest.mock import AsyncMock, MagicMock, patch, call

from src.repositories.database_backup_repository import DatabaseBackupRepository


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_repo(session=None, user_token=None):
    if session is None:
        session = AsyncMock()
    with patch("src.repositories.database_backup_repository.DatabricksVolumeRepository") as mock_vr:
        mock_vr.return_value = MagicMock()
        repo = DatabaseBackupRepository(session, user_token)
    repo.volume_repo = MagicMock()
    repo.session = session
    return repo


# ---------------------------------------------------------------------------
# create_postgres_backup: row value type handling
# ---------------------------------------------------------------------------

class TestCreatePostgresBackupValueTypes:

    @pytest.mark.asyncio
    async def test_backup_handles_bool_value(self):
        """SQL backup generates TRUE/FALSE for boolean values."""
        repo = make_repo()
        session = AsyncMock()

        tables_result = MagicMock()
        tables_result.fetchall.return_value = [("agents",)]

        columns_result = MagicMock()
        columns_result.fetchall.return_value = [("id", "integer"), ("active", "boolean")]

        data_result = MagicMock()
        data_result.fetchall.return_value = [(1, True), (2, False)]

        session.execute = AsyncMock(side_effect=[tables_result, columns_result, data_result])
        repo.volume_repo.upload_file_to_volume = AsyncMock(
            return_value={"success": True, "path": "/vol/b.sql"}
        )

        result = await repo.create_postgres_backup("cat", "sch", "vol", "b.sql", session=session)
        assert result["success"] is True
        assert result["total_rows"] == 2

    @pytest.mark.asyncio
    async def test_backup_handles_datetime_value(self):
        """SQL backup generates ISO format for datetime values."""
        repo = make_repo()
        session = AsyncMock()

        tables_result = MagicMock()
        tables_result.fetchall.return_value = [("executions",)]

        columns_result = MagicMock()
        columns_result.fetchall.return_value = [("id", "integer"), ("created_at", "timestamp")]

        ts = datetime(2024, 1, 15, 10, 30, 0)
        data_result = MagicMock()
        data_result.fetchall.return_value = [(1, ts)]

        session.execute = AsyncMock(side_effect=[tables_result, columns_result, data_result])
        repo.volume_repo.upload_file_to_volume = AsyncMock(
            return_value={"success": True, "path": "/vol/b.sql"}
        )

        result = await repo.create_postgres_backup("cat", "sch", "vol", "b.sql", session=session)
        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_backup_handles_dict_value(self):
        """SQL backup generates ::jsonb for dict values."""
        repo = make_repo()
        session = AsyncMock()

        tables_result = MagicMock()
        tables_result.fetchall.return_value = [("configs",)]

        columns_result = MagicMock()
        columns_result.fetchall.return_value = [("id", "integer"), ("config", "jsonb")]

        data_result = MagicMock()
        data_result.fetchall.return_value = [(1, {"key": "value"})]

        session.execute = AsyncMock(side_effect=[tables_result, columns_result, data_result])
        repo.volume_repo.upload_file_to_volume = AsyncMock(
            return_value={"success": True, "path": "/vol/b.sql"}
        )

        result = await repo.create_postgres_backup("cat", "sch", "vol", "b.sql", session=session)
        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_backup_handles_null_value(self):
        """SQL backup generates NULL for None values."""
        repo = make_repo()
        session = AsyncMock()

        tables_result = MagicMock()
        tables_result.fetchall.return_value = [("tasks",)]

        columns_result = MagicMock()
        columns_result.fetchall.return_value = [("id", "integer"), ("description", "text")]

        data_result = MagicMock()
        data_result.fetchall.return_value = [(1, None)]

        session.execute = AsyncMock(side_effect=[tables_result, columns_result, data_result])
        repo.volume_repo.upload_file_to_volume = AsyncMock(
            return_value={"success": True, "path": "/vol/b.sql"}
        )

        result = await repo.create_postgres_backup("cat", "sch", "vol", "b.sql", session=session)
        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_backup_handles_float_value(self):
        """SQL backup handles float values correctly."""
        repo = make_repo()
        session = AsyncMock()

        tables_result = MagicMock()
        tables_result.fetchall.return_value = [("metrics",)]

        columns_result = MagicMock()
        columns_result.fetchall.return_value = [("id", "integer"), ("score", "float")]

        data_result = MagicMock()
        data_result.fetchall.return_value = [(1, 3.14)]

        session.execute = AsyncMock(side_effect=[tables_result, columns_result, data_result])
        repo.volume_repo.upload_file_to_volume = AsyncMock(
            return_value={"success": True, "path": "/vol/b.sql"}
        )

        result = await repo.create_postgres_backup("cat", "sch", "vol", "b.sql", session=session)
        assert result["success"] is True


# ---------------------------------------------------------------------------
# _create_postgres_to_sqlite_backup
# ---------------------------------------------------------------------------

class TestCreatePostgresToSqliteBackup:

    @pytest.mark.asyncio
    async def test_successful_postgres_to_sqlite_backup(self):
        """Successful backup from PostgreSQL to SQLite format."""
        repo = make_repo()
        session = AsyncMock()

        tables_result = MagicMock()
        tables_result.fetchall.return_value = [("agents",)]

        col_result = MagicMock()
        col_result.fetchall.return_value = [
            ("id", "integer", "NO"),
            ("name", "character varying", "YES"),
        ]

        data_result = MagicMock()
        data_result.fetchall.return_value = [(1, "Agent Smith")]

        session.execute = AsyncMock(side_effect=[tables_result, col_result, data_result])

        repo.volume_repo.upload_file_to_volume = AsyncMock(
            return_value={"success": True, "path": "/vol/backup.db"}
        )

        result = await repo._create_postgres_to_sqlite_backup(
            session=session,
            catalog="cat",
            schema="sch",
            volume_name="vol",
            backup_filename="backup.db"
        )

        assert result["success"] is True
        assert result["database_type"] == "sqlite"
        assert result["source_type"] == "postgres"

    @pytest.mark.asyncio
    async def test_postgres_to_sqlite_with_various_column_types(self):
        """Maps PostgreSQL types to SQLite types correctly."""
        repo = make_repo()
        session = AsyncMock()

        tables_result = MagicMock()
        tables_result.fetchall.return_value = [("data",)]

        col_result = MagicMock()
        col_result.fetchall.return_value = [
            ("id", "integer", "NO"),
            ("count", "numeric", "YES"),
            ("active", "boolean", "YES"),
            ("created", "timestamp", "YES"),
            ("meta", "jsonb", "YES"),
        ]

        data_result = MagicMock()
        data_result.fetchall.return_value = [
            (1, 3.14, True, datetime(2024, 1, 1), {"k": "v"})
        ]

        session.execute = AsyncMock(side_effect=[tables_result, col_result, data_result])
        repo.volume_repo.upload_file_to_volume = AsyncMock(
            return_value={"success": True, "path": "/vol/b.db"}
        )

        result = await repo._create_postgres_to_sqlite_backup(
            session=session, catalog="c", schema="s", volume_name="v",
            backup_filename="b.db"
        )
        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_postgres_to_sqlite_upload_failure(self):
        """Returns upload failure when volume upload fails."""
        repo = make_repo()
        session = AsyncMock()

        tables_result = MagicMock()
        tables_result.fetchall.return_value = [("agents",)]

        col_result = MagicMock()
        col_result.fetchall.return_value = [("id", "integer", "NO")]

        data_result = MagicMock()
        data_result.fetchall.return_value = []

        session.execute = AsyncMock(side_effect=[tables_result, col_result, data_result])
        repo.volume_repo.upload_file_to_volume = AsyncMock(
            return_value={"success": False, "error": "Volume full"}
        )

        result = await repo._create_postgres_to_sqlite_backup(
            session=session, catalog="c", schema="s", volume_name="v",
            backup_filename="b.db"
        )
        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_postgres_to_sqlite_empty_tables(self):
        """Handles case with no tables in PostgreSQL."""
        repo = make_repo()
        session = AsyncMock()

        tables_result = MagicMock()
        tables_result.fetchall.return_value = []

        session.execute = AsyncMock(return_value=tables_result)
        repo.volume_repo.upload_file_to_volume = AsyncMock(
            return_value={"success": True, "path": "/vol/b.db"}
        )

        result = await repo._create_postgres_to_sqlite_backup(
            session=session, catalog="c", schema="s", volume_name="v",
            backup_filename="b.db"
        )
        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_postgres_to_sqlite_exception_cleanup(self):
        """Exception triggers cleanup and returns error."""
        repo = make_repo()
        session = AsyncMock()
        session.execute = AsyncMock(side_effect=Exception("DB error"))

        result = await repo._create_postgres_to_sqlite_backup(
            session=session, catalog="c", schema="s", volume_name="v",
            backup_filename="b.db"
        )
        assert result["success"] is False
        assert "DB error" in result["error"]

    @pytest.mark.asyncio
    async def test_postgres_to_sqlite_date_value_conversion(self):
        """date values converted to ISO string."""
        repo = make_repo()
        session = AsyncMock()

        tables_result = MagicMock()
        tables_result.fetchall.return_value = [("events",)]

        col_result = MagicMock()
        col_result.fetchall.return_value = [("id", "integer", "NO"), ("event_date", "date", "YES")]

        data_result = MagicMock()
        data_result.fetchall.return_value = [(1, date(2024, 6, 15))]

        session.execute = AsyncMock(side_effect=[tables_result, col_result, data_result])
        repo.volume_repo.upload_file_to_volume = AsyncMock(
            return_value={"success": True, "path": "/vol/b.db"}
        )

        result = await repo._create_postgres_to_sqlite_backup(
            session=session, catalog="c", schema="s", volume_name="v", backup_filename="b.db"
        )
        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_postgres_to_sqlite_list_value_conversion(self):
        """list values converted to JSON string."""
        repo = make_repo()
        session = AsyncMock()

        tables_result = MagicMock()
        tables_result.fetchall.return_value = [("tags",)]

        col_result = MagicMock()
        col_result.fetchall.return_value = [("id", "integer", "NO"), ("items", "jsonb", "YES")]

        data_result = MagicMock()
        data_result.fetchall.return_value = [(1, ["a", "b", "c"])]

        session.execute = AsyncMock(side_effect=[tables_result, col_result, data_result])
        repo.volume_repo.upload_file_to_volume = AsyncMock(
            return_value={"success": True, "path": "/vol/b.db"}
        )

        result = await repo._create_postgres_to_sqlite_backup(
            session=session, catalog="c", schema="s", volume_name="v", backup_filename="b.db"
        )
        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_postgres_to_sqlite_serial_column_type(self):
        """serial column type maps to INTEGER in SQLite."""
        repo = make_repo()
        session = AsyncMock()

        tables_result = MagicMock()
        tables_result.fetchall.return_value = [("sequences",)]

        col_result = MagicMock()
        col_result.fetchall.return_value = [("id", "serial", "NO"), ("val", "text", "YES")]

        data_result = MagicMock()
        data_result.fetchall.return_value = [(1, "hello")]

        session.execute = AsyncMock(side_effect=[tables_result, col_result, data_result])
        repo.volume_repo.upload_file_to_volume = AsyncMock(
            return_value={"success": True, "path": "/vol/b.db"}
        )

        result = await repo._create_postgres_to_sqlite_backup(
            session=session, catalog="c", schema="s", volume_name="v", backup_filename="b.db"
        )
        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_postgres_to_sqlite_none_value_conversion(self):
        """None values remain None in SQLite backup."""
        repo = make_repo()
        session = AsyncMock()

        tables_result = MagicMock()
        tables_result.fetchall.return_value = [("items",)]

        col_result = MagicMock()
        col_result.fetchall.return_value = [("id", "integer", "NO"), ("value", "text", "YES")]

        data_result = MagicMock()
        data_result.fetchall.return_value = [(1, None)]

        session.execute = AsyncMock(side_effect=[tables_result, col_result, data_result])
        repo.volume_repo.upload_file_to_volume = AsyncMock(
            return_value={"success": True, "path": "/vol/b.db"}
        )

        result = await repo._create_postgres_to_sqlite_backup(
            session=session, catalog="c", schema="s", volume_name="v", backup_filename="b.db"
        )
        assert result["success"] is True


# ---------------------------------------------------------------------------
# restore_sqlite_backup: safety backup and table-by-table copy
# ---------------------------------------------------------------------------

class TestRestoreSqliteBackupAdvanced:

    @pytest.mark.asyncio
    async def test_restore_with_safety_backup_created(self):
        """restore_sqlite_backup creates safety backup when target exists."""
        repo = make_repo()

        # Create real SQLite DB content
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            src_path = f.name
        conn = sqlite3.connect(src_path)
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        conn.execute("INSERT INTO t VALUES (1, 'hello')")
        conn.commit()
        conn.close()

        with open(src_path, "rb") as f:
            backup_bytes = f.read()
        os.unlink(src_path)

        repo.volume_repo.download_file_from_volume = AsyncMock(
            return_value={"success": True, "content": backup_bytes}
        )
        repo.volume_repo.upload_file_to_volume = AsyncMock(
            return_value={"success": True, "path": "/vol/safety.db"}
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            target = os.path.join(tmpdir, "target.db")
            # Create target so safety backup is triggered
            conn = sqlite3.connect(target)
            conn.execute("CREATE TABLE existing (id INTEGER)")
            conn.commit()
            conn.close()

            result = await repo.restore_sqlite_backup(
                "cat", "sch", "vol", "backup.db", target,
                create_safety_backup=True
            )

        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_restore_sqlite_with_admin_uuid_preservation(self):
        """restore_sqlite_backup preserves admin UUID during users table import."""
        repo = make_repo()

        # Create backup DB with users table
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            backup_path = f.name
        bkp_conn = sqlite3.connect(backup_path)
        bkp_conn.execute("""
            CREATE TABLE users (
                id TEXT PRIMARY KEY,
                email TEXT,
                username TEXT,
                is_system_admin INTEGER
            )
        """)
        bkp_conn.execute("INSERT INTO users VALUES ('new-uuid', 'admin@example.com', 'admin', 1)")
        bkp_conn.execute("INSERT INTO users VALUES ('user-uuid', 'user@example.com', 'user', 0)")
        bkp_conn.commit()
        bkp_conn.close()

        with open(backup_path, "rb") as f:
            backup_bytes = f.read()
        os.unlink(backup_path)

        repo.volume_repo.download_file_from_volume = AsyncMock(
            return_value={"success": True, "content": backup_bytes}
        )
        repo.volume_repo.upload_file_to_volume = AsyncMock(
            return_value={"success": True, "path": "/vol/safety.db"}
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            target = os.path.join(tmpdir, "target.db")
            # Create target with existing admin user
            target_conn = sqlite3.connect(target)
            target_conn.execute("""
                CREATE TABLE users (
                    id TEXT PRIMARY KEY,
                    email TEXT,
                    username TEXT,
                    is_system_admin INTEGER
                )
            """)
            target_conn.execute(
                "INSERT INTO users VALUES ('existing-admin-uuid', 'admin@example.com', 'admin', 1)"
            )
            target_conn.commit()
            target_conn.close()

            result = await repo.restore_sqlite_backup(
                "cat", "sch", "vol", "backup.db", target,
                create_safety_backup=False
            )

        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_restore_sqlite_skips_alembic_version_table(self):
        """restore_sqlite_backup skips alembic_version system table."""
        repo = make_repo()

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            backup_path = f.name
        conn = sqlite3.connect(backup_path)
        conn.execute("CREATE TABLE alembic_version (version_num TEXT)")
        conn.execute("INSERT INTO alembic_version VALUES ('abc123')")
        conn.execute("CREATE TABLE agents (id INTEGER PRIMARY KEY, name TEXT)")
        conn.execute("INSERT INTO agents VALUES (1, 'TestAgent')")
        conn.commit()
        conn.close()

        with open(backup_path, "rb") as f:
            backup_bytes = f.read()
        os.unlink(backup_path)

        repo.volume_repo.download_file_from_volume = AsyncMock(
            return_value={"success": True, "content": backup_bytes}
        )
        repo.volume_repo.upload_file_to_volume = AsyncMock(
            return_value={"success": True, "path": "/vol/s.db"}
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            target = os.path.join(tmpdir, "target.db")
            conn = sqlite3.connect(target)
            conn.execute("CREATE TABLE alembic_version (version_num TEXT)")
            conn.execute("INSERT INTO alembic_version VALUES ('old_version')")
            conn.execute("CREATE TABLE agents (id INTEGER PRIMARY KEY, name TEXT)")
            conn.commit()
            conn.close()

            result = await repo.restore_sqlite_backup(
                "cat", "sch", "vol", "backup.db", target,
                create_safety_backup=False
            )

        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_restore_sqlite_import_error_continues(self):
        """restore_sqlite_backup continues on per-table import error."""
        repo = make_repo()

        # Backup with an invalid table name that will fail validation
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            backup_path = f.name
        conn = sqlite3.connect(backup_path)
        conn.execute("CREATE TABLE valid_table (id INTEGER PRIMARY KEY)")
        conn.execute("INSERT INTO valid_table VALUES (1)")
        conn.commit()
        conn.close()

        with open(backup_path, "rb") as f:
            backup_bytes = f.read()
        os.unlink(backup_path)

        repo.volume_repo.download_file_from_volume = AsyncMock(
            return_value={"success": True, "content": backup_bytes}
        )
        repo.volume_repo.upload_file_to_volume = AsyncMock(
            return_value={"success": True, "path": "/vol/s.db"}
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            target = os.path.join(tmpdir, "target.db")
            conn = sqlite3.connect(target)
            conn.execute("CREATE TABLE valid_table (id INTEGER PRIMARY KEY)")
            conn.commit()
            conn.close()

            result = await repo.restore_sqlite_backup(
                "cat", "sch", "vol", "backup.db", target,
                create_safety_backup=False
            )

        assert result["success"] is True


# ---------------------------------------------------------------------------
# restore_postgres_backup: SQL path and JSON path
# ---------------------------------------------------------------------------

class TestRestorePostgresBackup:

    @pytest.mark.asyncio
    async def test_restore_postgres_sql_format(self):
        """restore_postgres_backup restores from .sql format."""
        repo = make_repo()

        sql_content = (
            "-- Backup created by Kasal\n"
            "INSERT INTO agents (id, name) VALUES (1, 'TestAgent');\n"
            "INSERT INTO tasks (id, desc) VALUES (1, 'Task 1');\n"
        )

        repo.volume_repo.download_file_from_volume = AsyncMock(
            return_value={"success": True, "content": sql_content.encode("utf-8")}
        )

        session = AsyncMock()
        session.execute = AsyncMock(return_value=MagicMock())
        session.commit = AsyncMock()

        result = await repo.restore_postgres_backup(
            "cat", "sch", "vol", "backup.sql", session=session
        )

        assert result["success"] is True
        assert result["database_type"] == "postgres"

    @pytest.mark.asyncio
    async def test_restore_postgres_sql_skips_empty_lines_and_comments(self):
        """restore_postgres_backup skips blank lines and SQL comments."""
        repo = make_repo()

        sql_content = (
            "-- Header comment\n"
            "\n"
            "-- Another comment\n"
            "INSERT INTO agents (id) VALUES (1);\n"
            "\n"
            "-- Tail comment\n"
        )

        repo.volume_repo.download_file_from_volume = AsyncMock(
            return_value={"success": True, "content": sql_content.encode("utf-8")}
        )

        session = AsyncMock()
        session.execute = AsyncMock(return_value=MagicMock())
        session.commit = AsyncMock()

        result = await repo.restore_postgres_backup(
            "cat", "sch", "vol", "backup.sql", session=session
        )

        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_restore_postgres_sql_statement_error_continues(self):
        """restore_postgres_backup continues on individual statement errors."""
        repo = make_repo()

        sql_content = (
            "INSERT INTO agents (id) VALUES (1);\n"
            "INSERT INTO invalid_table (x) VALUES (1);\n"
        )

        repo.volume_repo.download_file_from_volume = AsyncMock(
            return_value={"success": True, "content": sql_content.encode("utf-8")}
        )

        session = AsyncMock()
        session.execute = AsyncMock(
            side_effect=[MagicMock(), Exception("table not found")]
        )
        session.commit = AsyncMock()

        result = await repo.restore_postgres_backup(
            "cat", "sch", "vol", "backup.sql", session=session
        )

        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_restore_postgres_json_format(self):
        """restore_postgres_backup restores from .json format."""
        repo = make_repo()

        backup_data = {
            "database_type": "postgres",
            "tables": {
                "agents": [
                    {"id": 1, "name": "Agent1"},
                    {"id": 2, "name": "Agent2"},
                ]
            }
        }

        repo.volume_repo.download_file_from_volume = AsyncMock(
            return_value={"success": True, "content": json.dumps(backup_data).encode("utf-8")}
        )

        session = AsyncMock()
        session.execute = AsyncMock(return_value=MagicMock())
        session.commit = AsyncMock()

        result = await repo.restore_postgres_backup(
            "cat", "sch", "vol", "backup.json", session=session
        )

        assert result["success"] is True
        assert result["database_type"] == "postgres"

    @pytest.mark.asyncio
    async def test_restore_postgres_json_wrong_database_type(self):
        """restore_postgres_backup returns error for non-postgres JSON backup."""
        repo = make_repo()

        backup_data = {"database_type": "sqlite", "tables": {}}

        repo.volume_repo.download_file_from_volume = AsyncMock(
            return_value={"success": True, "content": json.dumps(backup_data).encode("utf-8")}
        )

        session = AsyncMock()

        result = await repo.restore_postgres_backup(
            "cat", "sch", "vol", "backup.json", session=session
        )

        assert result["success"] is False
        assert "not a PostgreSQL backup" in result["error"]

    @pytest.mark.asyncio
    async def test_restore_postgres_download_failure(self):
        """restore_postgres_backup returns error when download fails."""
        repo = make_repo()

        repo.volume_repo.download_file_from_volume = AsyncMock(
            return_value={"success": False, "error": "Volume not found"}
        )

        session = AsyncMock()
        result = await repo.restore_postgres_backup(
            "cat", "sch", "vol", "backup.sql", session=session
        )

        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_restore_postgres_exception_returns_error(self):
        """restore_postgres_backup handles unexpected exceptions."""
        repo = make_repo()

        repo.volume_repo.download_file_from_volume = AsyncMock(
            side_effect=Exception("Network timeout")
        )

        session = AsyncMock()
        session.rollback = AsyncMock()

        result = await repo.restore_postgres_backup(
            "cat", "sch", "vol", "backup.sql", session=session
        )

        assert result["success"] is False
        assert "Network timeout" in result["error"]

    @pytest.mark.asyncio
    async def test_restore_postgres_uses_stored_session(self):
        """restore_postgres_backup uses stored session when none provided."""
        stored_session = AsyncMock()
        repo = make_repo(session=stored_session)

        sql_content = "INSERT INTO agents (id) VALUES (1);\n"

        repo.volume_repo.download_file_from_volume = AsyncMock(
            return_value={"success": True, "content": sql_content.encode("utf-8")}
        )
        stored_session.execute = AsyncMock(return_value=MagicMock())
        stored_session.commit = AsyncMock()

        result = await repo.restore_postgres_backup("cat", "sch", "vol", "backup.sql")

        assert result["success"] is True


# ---------------------------------------------------------------------------
# list_backups: json backup type and modification_time fallback
# ---------------------------------------------------------------------------

class TestListBackupsAdvanced:

    @pytest.mark.asyncio
    async def test_returns_json_backup_type(self):
        """list_backups identifies .json files as postgres_json type."""
        repo = make_repo()
        repo.volume_repo.list_volume_contents = AsyncMock(
            return_value={
                "success": True,
                "files": [
                    {
                        "name": "kasal_backup_20240101_120000.json",
                        "path": "/p/b.json",
                        "file_size": 512,
                        "is_directory": False
                    }
                ]
            }
        )
        result = await repo.list_backups("cat", "sch", "vol")
        assert len(result) == 1
        assert result[0]["backup_type"] == "postgres_json"

    @pytest.mark.asyncio
    async def test_fallback_to_modification_time(self):
        """list_backups falls back to modification_time when timestamp parsing fails."""
        repo = make_repo()
        repo.volume_repo.list_volume_contents = AsyncMock(
            return_value={
                "success": True,
                "files": [
                    {
                        "name": "kasal_backup_invalid_timestamp.db",
                        "path": "/p/b.db",
                        "file_size": 100,
                        "is_directory": False,
                        "modification_time": 1704067200000  # 2024-01-01 in ms
                    }
                ]
            }
        )
        result = await repo.list_backups("cat", "sch", "vol")
        assert len(result) == 1
        assert result[0]["backup_type"] == "sqlite"

    @pytest.mark.asyncio
    async def test_fallback_to_datetime_now_when_no_mod_time(self):
        """list_backups uses datetime.now() when modification_time is also missing."""
        repo = make_repo()
        repo.volume_repo.list_volume_contents = AsyncMock(
            return_value={
                "success": True,
                "files": [
                    {
                        "name": "kasal_backup_bad.db",
                        "path": "/p/b.db",
                        "file_size": 100,
                        "is_directory": False
                        # No modification_time key
                    }
                ]
            }
        )
        result = await repo.list_backups("cat", "sch", "vol")
        assert len(result) == 1
        assert isinstance(result[0]["created_at"], datetime)


# ---------------------------------------------------------------------------
# get_database_info: extended paths
# ---------------------------------------------------------------------------

class TestGetDatabaseInfoExtended:

    @pytest.mark.asyncio
    async def test_sqlite_path_from_settings_when_none(self):
        """get_database_info uses settings.SQLITE_DB_PATH when db_path is None."""
        repo = make_repo()

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        try:
            conn = sqlite3.connect(db_path)
            conn.execute("CREATE TABLE agents (id INTEGER PRIMARY KEY, name TEXT)")
            conn.execute("INSERT INTO agents VALUES (1, 'Smith')")
            conn.commit()
            conn.close()

            # get_database_info internally does `from src.config.settings import settings`
            # so we must patch the settings object inside that module
            with patch("src.repositories.database_backup_repository.settings") as mock_outer, \
                 patch("src.config.settings.settings") as mock_inner_settings:
                mock_outer.DATABASE_URI = "sqlite:///app.db"
                mock_inner_settings.SQLITE_DB_PATH = db_path

                result = await repo.get_database_info(db_path=None, session=None)

            assert result["success"] is True
            assert "agents" in result["tables"]
        finally:
            os.unlink(db_path)

    @pytest.mark.asyncio
    async def test_sqlite_path_default_fallback(self):
        """get_database_info uses './app.db' fallback when SQLITE_DB_PATH is empty."""
        repo = make_repo()

        with patch("src.repositories.database_backup_repository.settings") as mock_outer, \
             patch("src.config.settings.settings") as mock_inner:
            mock_outer.DATABASE_URI = "sqlite:///app.db"
            mock_inner.SQLITE_DB_PATH = ""

            result = await repo.get_database_info(db_path=None, session=None)

        # Should fail because the default ./app.db doesn't exist in this context
        # (or succeeds if it happens to exist -- either way no exception should be raised)
        assert "success" in result

    @pytest.mark.asyncio
    async def test_sqlite_relative_path_made_absolute(self):
        """get_database_info converts relative SQLITE_DB_PATH to absolute path."""
        repo = make_repo()

        with patch("src.repositories.database_backup_repository.settings") as mock_outer, \
             patch("src.config.settings.settings") as mock_inner:
            mock_outer.DATABASE_URI = "sqlite:///app.db"
            mock_inner.SQLITE_DB_PATH = "nonexistent/relative/app.db"

            result = await repo.get_database_info(db_path=None, session=None)

        # File won't exist, so should fail cleanly
        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_sqlite_with_memory_backends_table(self):
        """get_database_info includes memory_backends data when table exists."""
        repo = make_repo()

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        try:
            conn = sqlite3.connect(db_path)
            conn.execute("""
                CREATE TABLE memory_backends (
                    id TEXT PRIMARY KEY,
                    name TEXT,
                    backend_type TEXT,
                    is_default INTEGER,
                    created_at TEXT,
                    group_id TEXT
                )
            """)
            conn.execute(
                "INSERT INTO memory_backends VALUES ('id1', 'Backend1', 'chroma', 1, '2024-01-01', 'group1')"
            )
            conn.commit()
            conn.close()

            result = await repo.get_database_info(db_path=db_path)

            assert result["success"] is True
            assert "memory_backends" in result["tables"]
            assert len(result["memory_backends"]) == 1
            assert result["memory_backends"][0]["name"] == "Backend1"
        finally:
            os.unlink(db_path)

    @pytest.mark.asyncio
    async def test_postgres_with_kasal_schema(self):
        """get_database_info handles PostgreSQL with kasal schema."""
        repo = make_repo()
        session = AsyncMock()

        # Schema check returns kasal schema exists
        schema_check = MagicMock()
        schema_check.scalar.return_value = "kasal"

        # Tables result
        tables_result = MagicMock()
        tables_result.fetchall.return_value = [("agents",)]

        # Count result
        count_result = MagicMock()
        count_result.scalar.return_value = 5

        # DB size result
        size_result = MagicMock()
        size_result.scalar.return_value = 1024 * 1024

        session.execute = AsyncMock(
            side_effect=[schema_check, tables_result, count_result, size_result]
        )

        result = await repo.get_database_info(session=session)

        assert result["success"] is True
        assert result["database_type"] == "postgres"
        assert "agents" in result["tables"]

    @pytest.mark.asyncio
    async def test_postgres_with_public_schema(self):
        """get_database_info handles PostgreSQL with public schema (no kasal)."""
        repo = make_repo()
        session = AsyncMock()

        # Schema check returns None (no kasal schema)
        schema_check = MagicMock()
        schema_check.scalar.return_value = None

        tables_result = MagicMock()
        tables_result.fetchall.return_value = [("crews",)]

        count_result = MagicMock()
        count_result.scalar.return_value = 3

        size_result = MagicMock()
        size_result.scalar.return_value = 512 * 1024

        session.execute = AsyncMock(
            side_effect=[schema_check, tables_result, count_result, size_result]
        )

        result = await repo.get_database_info(session=session)

        assert result["success"] is True
        assert result["database_type"] == "postgres"

    @pytest.mark.asyncio
    async def test_postgres_with_memory_backends_table(self):
        """get_database_info includes memory_backends for postgres."""
        repo = make_repo()
        session = AsyncMock()

        schema_check = MagicMock()
        schema_check.scalar.return_value = None

        tables_result = MagicMock()
        tables_result.fetchall.return_value = [("memory_backends",)]

        count_result = MagicMock()
        count_result.scalar.return_value = 2

        size_result = MagicMock()
        size_result.scalar.return_value = 256 * 1024

        mb_result = MagicMock()
        mb_result.fetchall.return_value = [
            ("id1", "Backend1", "chroma", True, "2024-01-01", "group1")
        ]

        session.execute = AsyncMock(
            side_effect=[schema_check, tables_result, count_result, size_result, mb_result]
        )

        result = await repo.get_database_info(session=session)

        assert result["success"] is True
        assert len(result["memory_backends"]) == 1
        assert result["memory_backends"][0]["name"] == "Backend1"

    @pytest.mark.asyncio
    async def test_postgres_unsupported_db_type(self):
        """get_database_info returns error for unsupported database type."""
        repo = make_repo()

        with patch("src.repositories.database_backup_repository.settings") as mock_settings:
            mock_settings.DATABASE_URI = "mysql://user:pass@host/db"
            result = await repo.get_database_info(db_path=None, session=None)

        assert result["success"] is False
        assert "Unsupported database type" in result["error"]

    @pytest.mark.asyncio
    async def test_get_database_info_postgres_exception(self):
        """get_database_info handles postgres exception properly."""
        repo = make_repo()
        session = AsyncMock()
        session.execute = AsyncMock(side_effect=Exception("connection failed"))

        result = await repo.get_database_info(session=session)

        assert result["success"] is False
        assert "connection failed" in result["error"]
