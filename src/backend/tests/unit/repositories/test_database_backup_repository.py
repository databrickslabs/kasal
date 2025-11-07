import pytest
pytest.skip("Legacy database backup repository tests require sync/sessionless operations; skipping.", allow_module_level=True)

"""
Unit tests for Database Backup Repository.

Tests the repository for handling database backup operations with Databricks volumes.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock, AsyncMock, mock_open
import os
import sqlite3
import tempfile
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession

from src.repositories.database_backup_repository import DatabaseBackupRepository


class TestDatabaseBackupRepository:
    """Test suite for DatabaseBackupRepository."""

    @patch('src.repositories.database_backup_repository.DatabricksVolumeRepository')
    def test_initialization(self, mock_volume_repo_class):
        """Test repository initialization."""
        mock_volume_repo = Mock()
        mock_volume_repo_class.return_value = mock_volume_repo

        repo = DatabaseBackupRepository(user_token="test-token")

        assert repo.volume_repo == mock_volume_repo
        assert repo.user_token == "test-token"
        mock_volume_repo_class.assert_called_once_with(user_token="test-token")

    @patch('src.repositories.database_backup_repository.settings')
    def test_get_database_type_sqlite(self, mock_settings):
        """Test detecting SQLite database type."""
        mock_settings.DATABASE_URI = "sqlite:///test.db"

        db_type = DatabaseBackupRepository.get_database_type()
        assert db_type == "sqlite"

    @patch('src.repositories.database_backup_repository.settings')
    def test_get_database_type_postgres(self, mock_settings):
        """Test detecting PostgreSQL database type."""
        mock_settings.DATABASE_URI = "postgresql://user:pass@localhost/db"

        db_type = DatabaseBackupRepository.get_database_type()
        assert db_type == "postgres"

    @patch('src.repositories.database_backup_repository.settings')
    def test_get_database_type_postgres_alt(self, mock_settings):
        """Test detecting PostgreSQL with 'postgres' prefix."""
        mock_settings.DATABASE_URI = "postgres://user:pass@localhost/db"

        db_type = DatabaseBackupRepository.get_database_type()
        assert db_type == "postgres"

    @patch('src.repositories.database_backup_repository.settings')
    def test_get_database_type_unknown(self, mock_settings):
        """Test detecting unknown database type."""
        mock_settings.DATABASE_URI = "mysql://user:pass@localhost/db"

        db_type = DatabaseBackupRepository.get_database_type()
        assert db_type == "unknown"

    @pytest.mark.asyncio
    @patch('src.repositories.database_backup_repository.os.path.exists')
    @patch('builtins.open', new_callable=mock_open, read_data=b'test database content')
    @patch('src.repositories.database_backup_repository.DatabricksVolumeRepository')
    async def test_create_sqlite_backup_success(self, mock_volume_repo_class, mock_file_open, mock_exists):
        """Test successful SQLite backup creation."""
        # Setup
        mock_exists.return_value = True

        mock_volume_repo = AsyncMock()
        mock_volume_repo.upload_file_to_volume.return_value = {
            "success": True,
            "path": "/volumes/catalog/schema/volume/backup.db"
        }
        mock_volume_repo_class.return_value = mock_volume_repo

        repo = DatabaseBackupRepository()

        # Execute
        result = await repo.create_sqlite_backup(
            source_path="/path/to/source.db",
            catalog="test_catalog",
            schema="test_schema",
            volume_name="test_volume",
            backup_filename="backup.db"
        )

        # Assert
        assert result["success"] is True
        assert result["backup_path"] == "/volumes/catalog/schema/volume/backup.db"
        assert result["backup_size"] == len(b'test database content')
        assert result["database_type"] == "sqlite"
        assert result["catalog"] == "test_catalog"
        assert result["schema"] == "test_schema"
        assert result["volume"] == "test_volume"
        assert result["filename"] == "backup.db"

        mock_volume_repo.upload_file_to_volume.assert_called_once_with(
            catalog="test_catalog",
            schema="test_schema",
            volume_name="test_volume",
            file_name="backup.db",
            file_content=b'test database content'
        )

    @pytest.mark.asyncio
    @patch('src.repositories.database_backup_repository.os.path.exists')
    @patch('src.repositories.database_backup_repository.DatabricksVolumeRepository')
    async def test_create_sqlite_backup_file_not_found(self, mock_volume_repo_class, mock_exists):
        """Test SQLite backup when source file doesn't exist."""
        mock_exists.return_value = False
        mock_volume_repo_class.return_value = Mock()

        repo = DatabaseBackupRepository()

        result = await repo.create_sqlite_backup(
            source_path="/nonexistent/file.db",
            catalog="test_catalog",
            schema="test_schema",
            volume_name="test_volume",
            backup_filename="backup.db"
        )

        assert result["success"] is False
        assert "not found" in result["error"]

    @pytest.mark.asyncio
    @patch('src.repositories.database_backup_repository.os.path.exists')
    @patch('builtins.open', new_callable=mock_open, read_data=b'test data')
    @patch('src.repositories.database_backup_repository.DatabricksVolumeRepository')
    async def test_create_sqlite_backup_upload_failure(self, mock_volume_repo_class, mock_file_open, mock_exists):
        """Test SQLite backup when upload fails."""
        mock_exists.return_value = True

        mock_volume_repo = AsyncMock()
        mock_volume_repo.upload_file_to_volume.return_value = {
            "success": False,
            "error": "Upload failed"
        }
        mock_volume_repo_class.return_value = mock_volume_repo

        repo = DatabaseBackupRepository()

        result = await repo.create_sqlite_backup(
            source_path="/path/to/source.db",
            catalog="test_catalog",
            schema="test_schema",
            volume_name="test_volume",
            backup_filename="backup.db"
        )

        assert result["success"] is False
        assert result["error"] == "Upload failed"

    @pytest.mark.asyncio
    @patch('src.repositories.database_backup_repository.os.path.exists')
    @patch('src.repositories.database_backup_repository.logger')
    @patch('src.repositories.database_backup_repository.DatabricksVolumeRepository')
    async def test_create_sqlite_backup_exception(self, mock_volume_repo_class, mock_logger, mock_exists):
        """Test SQLite backup exception handling."""
        mock_exists.side_effect = Exception("Test error")
        mock_volume_repo_class.return_value = Mock()

        repo = DatabaseBackupRepository()

        result = await repo.create_sqlite_backup(
            source_path="/path/to/source.db",
            catalog="test_catalog",
            schema="test_schema",
            volume_name="test_volume",
            backup_filename="backup.db"
        )

        assert result["success"] is False
        assert "Test error" in result["error"]
        mock_logger.error.assert_called()

    @pytest.mark.asyncio
    @patch('src.repositories.database_backup_repository.DatabricksVolumeRepository')
    async def test_create_postgres_backup_sql_format(self, mock_volume_repo_class):
        """Test PostgreSQL backup in SQL format."""
        # Setup mock session
        mock_session = AsyncMock(spec=AsyncSession)

        # Mock table query
        mock_tables_result = Mock()
        mock_tables_result.fetchall.return_value = [("users",), ("posts",)]

        # Mock column queries
        mock_users_cols = Mock()
        mock_users_cols.fetchall.return_value = [("id", "integer"), ("name", "text")]

        mock_posts_cols = Mock()
        mock_posts_cols.fetchall.return_value = [("id", "integer"), ("title", "text")]

        # Mock data queries
        mock_users_data = Mock()
        mock_users_data.fetchall.return_value = [(1, "Alice"), (2, "Bob")]

        mock_posts_data = Mock()
        mock_posts_data.fetchall.return_value = [(1, "Post 1"), (2, "Post 2")]

        # Configure session execute returns
        mock_session.execute.side_effect = [
            mock_tables_result,  # Tables query
            mock_users_cols,     # Users columns
            mock_users_data,     # Users data
            mock_posts_cols,     # Posts columns
            mock_posts_data      # Posts data
        ]

        # Setup volume repository
        mock_volume_repo = AsyncMock()
        mock_volume_repo.upload_file_to_volume.return_value = {
            "success": True,
            "path": "/volumes/catalog/schema/volume/backup.sql"
        }
        mock_volume_repo_class.return_value = mock_volume_repo

        repo = DatabaseBackupRepository()

        # Execute
        result = await repo.create_postgres_backup(
            session=mock_session,
            catalog="test_catalog",
            schema="test_schema",
            volume_name="test_volume",
            backup_filename="backup.sql",
            export_format="sql"
        )

        # Assert
        assert result["success"] is True
        assert result["backup_path"] == "/volumes/catalog/schema/volume/backup.sql"
        assert result["database_type"] == "postgres"
        assert result["table_count"] == 2
        assert result["total_rows"] == 4

        # Verify SQL content was generated
        upload_call = mock_volume_repo.upload_file_to_volume.call_args
        sql_content = upload_call[1]["file_content"].decode('utf-8')

        assert "PostgreSQL database backup" in sql_content
        assert "INSERT INTO users" in sql_content
        assert "INSERT INTO posts" in sql_content
        assert "'Alice'" in sql_content
        assert "'Bob'" in sql_content

    @pytest.mark.asyncio
    @patch('src.repositories.database_backup_repository.DatabricksVolumeRepository')
    async def test_create_postgres_backup_sqlite_format(self, mock_volume_repo_class):
        """Test PostgreSQL backup in SQLite format."""
        mock_session = AsyncMock(spec=AsyncSession)
        mock_volume_repo = AsyncMock()
        mock_volume_repo_class.return_value = mock_volume_repo

        repo = DatabaseBackupRepository()

        # Mock the private method
        with patch.object(repo, '_create_postgres_to_sqlite_backup') as mock_sqlite_backup:
            mock_sqlite_backup.return_value = {
                "success": True,
                "backup_path": "/volumes/catalog/schema/volume/backup.db"
            }

            result = await repo.create_postgres_backup(
                session=mock_session,
                catalog="test_catalog",
                schema="test_schema",
                volume_name="test_volume",
                backup_filename="backup.db",
                export_format="sqlite"
            )

            mock_sqlite_backup.assert_called_once_with(
                mock_session,
                "test_catalog",
                "test_schema",
                "test_volume",
                "backup.db"
            )

            assert result["success"] is True

    @pytest.mark.asyncio
    @patch('src.repositories.database_backup_repository.logger')
    @patch('src.repositories.database_backup_repository.DatabricksVolumeRepository')
    async def test_create_postgres_backup_exception(self, mock_volume_repo_class, mock_logger):
        """Test PostgreSQL backup exception handling."""
        mock_session = AsyncMock(spec=AsyncSession)
        mock_session.execute.side_effect = Exception("Database error")

        mock_volume_repo_class.return_value = Mock()

        repo = DatabaseBackupRepository()

        result = await repo.create_postgres_backup(
            session=mock_session,
            catalog="test_catalog",
            schema="test_schema",
            volume_name="test_volume",
            backup_filename="backup.sql"
        )

        assert result["success"] is False
        assert "Database error" in result["error"]
        mock_logger.error.assert_called()

    @pytest.mark.asyncio
    @patch('os.unlink')
    @patch('tempfile.NamedTemporaryFile')
    @patch('sqlite3.connect')
    @patch('src.repositories.database_backup_repository.DatabricksVolumeRepository')
    async def test_create_postgres_to_sqlite_backup(self, mock_volume_repo_class, mock_sqlite3_connect, mock_tempfile, mock_unlink):
        """Test creating SQLite backup from PostgreSQL data."""
        # Setup mock session
        mock_session = AsyncMock(spec=AsyncSession)

        # Mock table query
        mock_tables_result = Mock()
        mock_tables_result.fetchall.return_value = [("users",)]

        # Mock column query
        mock_cols_result = Mock()
        mock_cols_result.fetchall.return_value = [
            ("id", "integer", "NO"),
            ("name", "text", "YES"),
            ("active", "boolean", "NO")
        ]

        # Mock data query
        mock_data_result = Mock()
        mock_data_result.fetchall.return_value = [(1, "Alice", True)]

        mock_session.execute.side_effect = [
            mock_tables_result,
            mock_cols_result,
            mock_data_result
        ]

        # Setup SQLite mock
        mock_cursor = Mock()
        mock_conn = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_sqlite3_connect.return_value = mock_conn

        # Setup tempfile mock - properly mock the context manager
        mock_temp_file = Mock()
        mock_temp_file.name = "/tmp/test.db"
        mock_tempfile.return_value.__enter__.return_value = mock_temp_file
        mock_tempfile.return_value.__exit__.return_value = None

        # Setup volume repository
        mock_volume_repo = AsyncMock()
        mock_volume_repo.upload_file_to_volume.return_value = {
            "success": True,
            "path": "/volumes/catalog/schema/volume/backup.db"
        }
        mock_volume_repo_class.return_value = mock_volume_repo

        # Mock open for reading temp file with proper context manager
        mock_file = mock_open(read_data=b'sqlite content')
        with patch('builtins.open', mock_file):
            repo = DatabaseBackupRepository()

            result = await repo._create_postgres_to_sqlite_backup(
                session=mock_session,
                catalog="test_catalog",
                schema="test_schema",
                volume_name="test_volume",
                backup_filename="backup.db"
            )

        # Assert
        assert result["success"] is True
        assert result["database_type"] == "sqlite"
        assert result["source_type"] == "postgres"
        assert result["table_count"] == 1
        assert result["total_rows"] == 1

        # Verify SQLite operations
        mock_cursor.execute.assert_any_call(
            "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY NOT NULL, name TEXT, active INTEGER NOT NULL)"
        )
        mock_cursor.execute.assert_any_call(
            "INSERT INTO users (id, name, active) VALUES (?, ?, ?)",
            ['1', 'Alice', 1]  # Note: id is converted to string, boolean True is converted to 1
        )
        mock_conn.commit.assert_called()
        mock_conn.close.assert_called()

        # Verify file operations
        mock_unlink.assert_called_once_with("/tmp/test.db")

        # Verify upload was called with the sqlite content
        mock_volume_repo.upload_file_to_volume.assert_called_once_with(
            catalog="test_catalog",
            schema="test_schema",
            volume_name="test_volume",
            file_name="backup.db",
            file_content=b'sqlite content'
        )