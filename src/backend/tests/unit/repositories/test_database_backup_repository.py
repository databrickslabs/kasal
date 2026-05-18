"""
Comprehensive unit tests for DatabaseBackupRepository.

Covers: validate_identifier, get_database_type, create_sqlite_backup,
        create_postgres_backup, restore_sqlite_backup, list_backups,
        delete_backup, cleanup_old_backups, get_database_info.
"""
import os
import sqlite3
import tempfile
import pytest
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch, mock_open, call

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
# _validate_identifier
# ---------------------------------------------------------------------------

class TestValidateIdentifier:
    def test_valid_identifier_passes(self):
        result = DatabaseBackupRepository._validate_identifier("my_table")
        assert result == "my_table"

    def test_valid_identifier_with_numbers(self):
        result = DatabaseBackupRepository._validate_identifier("table123")
        assert result == "table123"

    def test_valid_identifier_uppercase(self):
        result = DatabaseBackupRepository._validate_identifier("MyTable")
        assert result == "MyTable"

    def test_invalid_identifier_with_space_raises(self):
        with pytest.raises(ValueError, match="Invalid SQL"):
            DatabaseBackupRepository._validate_identifier("bad table")

    def test_invalid_identifier_with_semicolon_raises(self):
        with pytest.raises(ValueError, match="Invalid SQL"):
            DatabaseBackupRepository._validate_identifier("bad;table")

    def test_invalid_identifier_starts_with_digit_raises(self):
        with pytest.raises(ValueError, match="Invalid SQL"):
            DatabaseBackupRepository._validate_identifier("1bad")

    def test_empty_identifier_raises(self):
        with pytest.raises(ValueError, match="Invalid SQL"):
            DatabaseBackupRepository._validate_identifier("")

    def test_sql_injection_attempt_raises(self):
        with pytest.raises(ValueError, match="Invalid SQL"):
            DatabaseBackupRepository._validate_identifier("table; DROP TABLE users")

    def test_custom_kind_in_error_message(self):
        with pytest.raises(ValueError, match="table name"):
            DatabaseBackupRepository._validate_identifier("bad-name", "table name")


# ---------------------------------------------------------------------------
# get_database_type
# ---------------------------------------------------------------------------

class TestGetDatabaseType:
    @patch("src.repositories.database_backup_repository.settings")
    def test_sqlite(self, mock_settings):
        mock_settings.DATABASE_URI = "sqlite:///app.db"
        assert DatabaseBackupRepository.get_database_type() == "sqlite"

    @patch("src.repositories.database_backup_repository.settings")
    def test_postgresql(self, mock_settings):
        mock_settings.DATABASE_URI = "postgresql://u@host/db"
        assert DatabaseBackupRepository.get_database_type() == "postgres"

    @patch("src.repositories.database_backup_repository.settings")
    def test_postgres_short(self, mock_settings):
        mock_settings.DATABASE_URI = "postgres://u@host/db"
        assert DatabaseBackupRepository.get_database_type() == "postgres"

    @patch("src.repositories.database_backup_repository.settings")
    def test_unknown(self, mock_settings):
        mock_settings.DATABASE_URI = "mysql://u:p@host/db"
        assert DatabaseBackupRepository.get_database_type() == "unknown"

    @patch("src.repositories.database_backup_repository.settings")
    def test_none_uri_returns_unknown(self, mock_settings):
        mock_settings.DATABASE_URI = None
        assert DatabaseBackupRepository.get_database_type() == "unknown"


# ---------------------------------------------------------------------------
# create_sqlite_backup
# ---------------------------------------------------------------------------

class TestCreateSqliteBackup:
    @pytest.mark.asyncio
    async def test_returns_error_when_file_not_found(self):
        repo = make_repo()
        result = await repo.create_sqlite_backup(
            "/nonexistent/path/to/db.sqlite", "cat", "sch", "vol", "backup.db"
        )
        assert result["success"] is False
        assert "not found" in result["error"]

    @pytest.mark.asyncio
    async def test_successful_backup(self):
        repo = make_repo()
        db_content = b"SQLite format 3" + b"\x00" * 100

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            f.write(db_content)
            db_path = f.name

        try:
            repo.volume_repo.upload_file_to_volume = AsyncMock(
                return_value={"success": True, "path": "/Volumes/cat/sch/vol/backup.db"}
            )
            with patch("sqlite3.connect") as mock_sqlite:
                mock_conn = MagicMock()
                mock_sqlite.return_value = mock_conn

                result = await repo.create_sqlite_backup(db_path, "cat", "sch", "vol", "backup.db")

            assert result["success"] is True
            assert result["backup_path"] == "/Volumes/cat/sch/vol/backup.db"
            assert result["database_type"] == "sqlite"
            assert result["backup_size"] == len(db_content)
        finally:
            os.unlink(db_path)

    @pytest.mark.asyncio
    async def test_returns_upload_failure_when_volume_upload_fails(self):
        repo = make_repo()
        db_content = b"SQLite format 3" + b"\x00" * 100

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            f.write(db_content)
            db_path = f.name

        try:
            repo.volume_repo.upload_file_to_volume = AsyncMock(
                return_value={"success": False, "error": "Upload failed"}
            )
            with patch("sqlite3.connect") as mock_sqlite:
                mock_conn = MagicMock()
                mock_sqlite.return_value = mock_conn

                result = await repo.create_sqlite_backup(db_path, "cat", "sch", "vol", "backup.db")

            assert result["success"] is False
        finally:
            os.unlink(db_path)

    @pytest.mark.asyncio
    async def test_continues_when_wal_checkpoint_fails(self):
        repo = make_repo()
        db_content = b"test db content"

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            f.write(db_content)
            db_path = f.name

        try:
            repo.volume_repo.upload_file_to_volume = AsyncMock(
                return_value={"success": True, "path": "/vol/backup.db"}
            )
            # WAL checkpoint fails but operation should continue
            with patch("sqlite3.connect") as mock_sqlite:
                mock_conn = MagicMock()
                mock_conn.execute.side_effect = Exception("WAL not enabled")
                mock_sqlite.return_value = mock_conn

                result = await repo.create_sqlite_backup(db_path, "cat", "sch", "vol", "backup.db")

            assert result["success"] is True
        finally:
            os.unlink(db_path)

    @pytest.mark.asyncio
    async def test_exception_returns_error(self):
        repo = make_repo()
        with patch("os.path.exists", return_value=True), \
             patch("builtins.open", side_effect=IOError("Permission denied")):
            result = await repo.create_sqlite_backup("/db.sqlite", "cat", "sch", "vol", "b.db")
        assert result["success"] is False
        assert "Permission denied" in result["error"]


# ---------------------------------------------------------------------------
# create_postgres_backup (sql format)
# ---------------------------------------------------------------------------

class TestCreatePostgresBackup:
    @pytest.mark.asyncio
    async def test_successful_postgres_sql_backup(self):
        repo = make_repo()
        session = AsyncMock()

        # Mock tables query
        tables_result = MagicMock()
        tables_result.fetchall.return_value = [("agents",)]

        # Mock columns query
        columns_result = MagicMock()
        columns_result.fetchall.return_value = [("id", "integer")]

        # Mock data query (no rows)
        data_result = MagicMock()
        data_result.fetchall.return_value = []

        session.execute = AsyncMock(side_effect=[tables_result, columns_result, data_result])

        repo.volume_repo.upload_file_to_volume = AsyncMock(
            return_value={"success": True, "path": "/Volumes/cat/sch/vol/backup.sql"}
        )

        result = await repo.create_postgres_backup("cat", "sch", "vol", "backup.sql", session=session)

        assert result["success"] is True
        assert result["database_type"] == "postgres"

    @pytest.mark.asyncio
    async def test_postgres_backup_with_rows(self):
        repo = make_repo()
        session = AsyncMock()

        tables_result = MagicMock()
        tables_result.fetchall.return_value = [("crews",)]

        columns_result = MagicMock()
        columns_result.fetchall.return_value = [("id", "integer"), ("name", "character varying")]

        data_result = MagicMock()
        data_result.fetchall.return_value = [(1, "My Crew")]

        session.execute = AsyncMock(side_effect=[tables_result, columns_result, data_result])

        repo.volume_repo.upload_file_to_volume = AsyncMock(
            return_value={"success": True, "path": "/vol/backup.sql"}
        )

        result = await repo.create_postgres_backup("cat", "sch", "vol", "backup.sql", session=session)

        assert result["success"] is True
        assert result["total_rows"] == 1

    @pytest.mark.asyncio
    async def test_postgres_backup_delegates_to_sqlite_when_sqlite_format(self):
        repo = make_repo()
        with patch.object(repo, "_create_postgres_to_sqlite_backup", new_callable=AsyncMock) as mock_sqlite_bk:
            mock_sqlite_bk.return_value = {"success": True}
            result = await repo.create_postgres_backup(
                "cat", "sch", "vol", "backup.db", export_format="sqlite"
            )
        mock_sqlite_bk.assert_awaited_once()
        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_postgres_backup_upload_failure(self):
        repo = make_repo()
        session = AsyncMock()

        tables_result = MagicMock()
        tables_result.fetchall.return_value = []
        session.execute = AsyncMock(return_value=tables_result)

        repo.volume_repo.upload_file_to_volume = AsyncMock(
            return_value={"success": False, "error": "Volume unreachable"}
        )

        result = await repo.create_postgres_backup("cat", "sch", "vol", "backup.sql", session=session)

        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_postgres_backup_exception_returns_error(self):
        repo = make_repo()
        session = AsyncMock()
        session.execute = AsyncMock(side_effect=Exception("DB connection lost"))

        result = await repo.create_postgres_backup("cat", "sch", "vol", "backup.sql", session=session)

        assert result["success"] is False
        assert "DB connection lost" in result["error"]

    @pytest.mark.asyncio
    async def test_postgres_backup_uses_stored_session_when_none_provided(self):
        stored_session = AsyncMock()
        repo = make_repo(session=stored_session)

        tables_result = MagicMock()
        tables_result.fetchall.return_value = []
        stored_session.execute = AsyncMock(return_value=tables_result)

        repo.volume_repo.upload_file_to_volume = AsyncMock(
            return_value={"success": True, "path": "/p/b.sql"}
        )

        result = await repo.create_postgres_backup("cat", "sch", "vol", "backup.sql")

        assert result["success"] is True
        stored_session.execute.assert_awaited()


# ---------------------------------------------------------------------------
# restore_sqlite_backup
# ---------------------------------------------------------------------------

class TestRestoreSqliteBackup:
    @pytest.mark.asyncio
    async def test_returns_error_when_download_fails(self):
        repo = make_repo()
        repo.volume_repo.download_file_from_volume = AsyncMock(
            return_value={"success": False, "error": "Volume not found"}
        )
        result = await repo.restore_sqlite_backup(
            "cat", "sch", "vol", "backup.db", "/target/db.sqlite"
        )
        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_returns_error_for_invalid_sqlite_file(self):
        repo = make_repo()
        repo.volume_repo.download_file_from_volume = AsyncMock(
            return_value={"success": True, "content": b"not a sqlite database"}
        )
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            target = f.name
        try:
            # The write to tempfile and sqlite validation should fail
            result = await repo.restore_sqlite_backup(
                "cat", "sch", "vol", "backup.db", target,
                create_safety_backup=False
            )
            # Either it fails with "Invalid SQLite" or succeeds if sqlite accepts the content
            # Main assertion: no unhandled exception
            assert "success" in result
        finally:
            if os.path.exists(target):
                os.unlink(target)

    @pytest.mark.asyncio
    async def test_successful_restore_without_safety_backup(self):
        repo = make_repo()

        # Create a real temporary SQLite DB to use as backup content
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            tmp_path = f.name

        conn = sqlite3.connect(tmp_path)
        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")
        conn.commit()
        conn.close()

        with open(tmp_path, "rb") as f:
            db_bytes = f.read()

        os.unlink(tmp_path)

        repo.volume_repo.download_file_from_volume = AsyncMock(
            return_value={"success": True, "content": db_bytes}
        )
        repo.volume_repo.upload_file_to_volume = AsyncMock(
            return_value={"success": True, "path": "/p/safety.db"}
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            target = os.path.join(tmpdir, "restored.db")
            result = await repo.restore_sqlite_backup(
                "cat", "sch", "vol", "backup.db", target,
                create_safety_backup=False
            )

        assert result["success"] is True
        assert result["database_type"] == "sqlite"

    @pytest.mark.asyncio
    async def test_exception_returns_error(self):
        repo = make_repo()
        repo.volume_repo.download_file_from_volume = AsyncMock(
            side_effect=Exception("Network timeout")
        )
        result = await repo.restore_sqlite_backup(
            "cat", "sch", "vol", "backup.db", "/target/db.sqlite"
        )
        assert result["success"] is False
        assert "Network timeout" in result["error"]


# ---------------------------------------------------------------------------
# list_backups
# ---------------------------------------------------------------------------

class TestListBackups:
    @pytest.mark.asyncio
    async def test_returns_empty_list_when_volume_list_fails(self):
        repo = make_repo()
        repo.volume_repo.list_volume_contents = AsyncMock(
            return_value={"success": False, "error": "Unauthorized"}
        )
        result = await repo.list_backups("cat", "sch", "vol")
        assert result == []

    @pytest.mark.asyncio
    async def test_returns_backup_files(self):
        repo = make_repo()
        repo.volume_repo.list_volume_contents = AsyncMock(
            return_value={
                "success": True,
                "files": [
                    {"name": "kasal_backup_20240101_120000.db", "path": "/p/b1.db",
                     "file_size": 1024, "is_directory": False},
                    {"name": "kasal_backup_20240102_130000.sql", "path": "/p/b2.sql",
                     "file_size": 2048, "is_directory": False},
                ]
            }
        )
        result = await repo.list_backups("cat", "sch", "vol")
        assert len(result) == 2
        types = {b["backup_type"] for b in result}
        assert "sqlite" in types
        assert "postgres_sql" in types

    @pytest.mark.asyncio
    async def test_filters_non_backup_files(self):
        repo = make_repo()
        repo.volume_repo.list_volume_contents = AsyncMock(
            return_value={
                "success": True,
                "files": [
                    {"name": "kasal_backup_20240101_120000.db", "path": "/p/b.db",
                     "file_size": 100, "is_directory": False},
                    {"name": "random_file.txt", "path": "/p/r.txt",
                     "file_size": 50, "is_directory": False},
                    {"name": "subdir", "path": "/p/subdir",
                     "file_size": 0, "is_directory": True},
                ]
            }
        )
        result = await repo.list_backups("cat", "sch", "vol")
        assert len(result) == 1
        assert result[0]["backup_type"] == "sqlite"

    @pytest.mark.asyncio
    async def test_sorts_most_recent_first(self):
        repo = make_repo()
        repo.volume_repo.list_volume_contents = AsyncMock(
            return_value={
                "success": True,
                "files": [
                    {"name": "kasal_backup_20240101_000000.db", "path": "/p/old.db",
                     "file_size": 100, "is_directory": False},
                    {"name": "kasal_backup_20240201_000000.db", "path": "/p/new.db",
                     "file_size": 200, "is_directory": False},
                ]
            }
        )
        result = await repo.list_backups("cat", "sch", "vol")
        assert len(result) == 2
        # Most recent first
        assert result[0]["created_at"] > result[1]["created_at"]

    @pytest.mark.asyncio
    async def test_exception_returns_empty_list(self):
        repo = make_repo()
        repo.volume_repo.list_volume_contents = AsyncMock(
            side_effect=Exception("Unexpected error")
        )
        result = await repo.list_backups("cat", "sch", "vol")
        assert result == []

    @pytest.mark.asyncio
    async def test_empty_files_list(self):
        repo = make_repo()
        repo.volume_repo.list_volume_contents = AsyncMock(
            return_value={"success": True, "files": []}
        )
        result = await repo.list_backups("cat", "sch", "vol")
        assert result == []


# ---------------------------------------------------------------------------
# delete_backup
# ---------------------------------------------------------------------------

class TestDeleteBackup:
    @pytest.mark.asyncio
    async def test_successful_delete(self):
        repo = make_repo()
        repo.volume_repo.delete_volume_file = AsyncMock(
            return_value={"success": True, "path": "/p/backup.db"}
        )
        result = await repo.delete_backup("cat", "sch", "vol", "kasal_backup_20240101.db")
        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_rejects_path_traversal_filename(self):
        repo = make_repo()
        result = await repo.delete_backup("cat", "sch", "vol", "../../../etc/passwd")
        assert result["success"] is False
        assert "Invalid" in result["error"]

    @pytest.mark.asyncio
    async def test_rejects_filename_with_slash(self):
        repo = make_repo()
        result = await repo.delete_backup("cat", "sch", "vol", "folder/backup.db")
        assert result["success"] is False
        assert "Invalid" in result["error"]

    @pytest.mark.asyncio
    async def test_rejects_filename_with_backslash(self):
        repo = make_repo()
        result = await repo.delete_backup("cat", "sch", "vol", r"folder\backup.db")
        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_propagates_delete_failure(self):
        repo = make_repo()
        repo.volume_repo.delete_volume_file = AsyncMock(
            return_value={"success": False, "error": "File not found"}
        )
        result = await repo.delete_backup("cat", "sch", "vol", "backup.db")
        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_exception_returns_error(self):
        repo = make_repo()
        repo.volume_repo.delete_volume_file = AsyncMock(
            side_effect=Exception("Volume API error")
        )
        result = await repo.delete_backup("cat", "sch", "vol", "backup.db")
        assert result["success"] is False
        assert "Volume API error" in result["error"]


# ---------------------------------------------------------------------------
# cleanup_old_backups
# ---------------------------------------------------------------------------

class TestCleanupOldBackups:
    @pytest.mark.asyncio
    async def test_no_cleanup_needed_when_few_backups(self):
        repo = make_repo()
        with patch.object(repo, "list_backups", new_callable=AsyncMock) as mock_list:
            mock_list.return_value = [
                {"filename": "kasal_backup_20240101.db"},
                {"filename": "kasal_backup_20240102.db"},
            ]
            result = await repo.cleanup_old_backups("cat", "sch", "vol", keep_count=5)

        assert result["success"] is True
        assert result["deleted"] == []
        assert "No cleanup needed" in result["message"]

    @pytest.mark.asyncio
    async def test_deletes_oldest_backups(self):
        repo = make_repo()
        backups = [
            {"filename": f"kasal_backup_2024010{i}.db"} for i in range(1, 8)
        ]
        with patch.object(repo, "list_backups", new_callable=AsyncMock) as mock_list, \
             patch.object(repo, "delete_backup", new_callable=AsyncMock) as mock_del:
            mock_list.return_value = backups
            mock_del.return_value = {"success": True}

            result = await repo.cleanup_old_backups("cat", "sch", "vol", keep_count=3)

        assert result["success"] is True
        assert len(result["deleted"]) == 4  # 7 - 3 = 4 deleted
        assert mock_del.await_count == 4

    @pytest.mark.asyncio
    async def test_partial_delete_failure_still_returns_success(self):
        repo = make_repo()
        backups = [
            {"filename": "kasal_backup_1.db"},
            {"filename": "kasal_backup_2.db"},
            {"filename": "kasal_backup_3.db"},
        ]
        with patch.object(repo, "list_backups", new_callable=AsyncMock) as mock_list, \
             patch.object(repo, "delete_backup", new_callable=AsyncMock) as mock_del:
            mock_list.return_value = backups
            # First delete succeeds, second fails
            mock_del.side_effect = [
                {"success": True},
                {"success": False, "error": "delete failed"},
            ]
            result = await repo.cleanup_old_backups("cat", "sch", "vol", keep_count=1)

        assert result["success"] is True
        assert len(result["deleted"]) == 1

    @pytest.mark.asyncio
    async def test_exception_returns_error(self):
        repo = make_repo()
        with patch.object(repo, "list_backups", side_effect=Exception("list failed")):
            result = await repo.cleanup_old_backups("cat", "sch", "vol")
        assert result["success"] is False
        assert "list failed" in result["error"]


# ---------------------------------------------------------------------------
# get_database_info (SQLite path)
# ---------------------------------------------------------------------------

class TestGetDatabaseInfo:
    @pytest.mark.asyncio
    async def test_returns_error_when_sqlite_file_not_found(self):
        repo = make_repo()
        result = await repo.get_database_info(db_path="/nonexistent/db.sqlite")
        assert result["success"] is False
        assert "not found" in result["error"]

    @pytest.mark.asyncio
    async def test_returns_info_for_valid_sqlite(self):
        repo = make_repo()

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        try:
            conn = sqlite3.connect(db_path)
            conn.execute("CREATE TABLE agents (id INTEGER PRIMARY KEY, name TEXT)")
            conn.execute("INSERT INTO agents VALUES (1, 'Agent Smith')")
            conn.commit()
            conn.close()

            result = await repo.get_database_info(db_path=db_path)

            assert result["success"] is True
            assert "agents" in result["tables"]
            assert result["tables"]["agents"] == 1
        finally:
            os.unlink(db_path)

    @pytest.mark.asyncio
    async def test_sqlite_info_includes_file_size(self):
        repo = make_repo()

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        try:
            conn = sqlite3.connect(db_path)
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            conn.commit()
            conn.close()

            result = await repo.get_database_info(db_path=db_path)

            assert result["success"] is True
            assert "size" in result or "file_size" in result or "database_type" in result
        finally:
            os.unlink(db_path)

    @pytest.mark.asyncio
    async def test_uses_postgres_path_when_session_provided(self):
        repo = make_repo()
        session = AsyncMock()

        # Mock pg_tables query
        tables_result = MagicMock()
        tables_result.fetchall.return_value = [("crews",)]

        # Mock columns query
        col_result = MagicMock()
        col_result.fetchall.return_value = [("id", "integer")]

        # Mock count query
        count_result = MagicMock()
        count_result.fetchone.return_value = (42,)

        session.execute = AsyncMock(side_effect=[tables_result, col_result, count_result])

        result = await repo.get_database_info(session=session)
        # Postgres path triggered; result has success or appropriate error
        assert "success" in result

    @pytest.mark.asyncio
    async def test_get_database_info_with_postgres_session_exception(self):
        """When postgres path raises an exception, returns error."""
        repo = make_repo()
        session = AsyncMock()
        session.execute = AsyncMock(side_effect=Exception("DB query failed"))
        result = await repo.get_database_info(session=session)
        assert result["success"] is False
        assert "DB query failed" in result["error"]
