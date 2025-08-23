"""
Unit tests for Database Management Service.

Tests the service for managing database export and import operations with Databricks volumes.
"""

import pytest
from unittest.mock import Mock, patch, AsyncMock, MagicMock
import os
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession

from src.services.database_management_service import DatabaseManagementService


class TestDatabaseManagementService:
    """Test suite for DatabaseManagementService."""
    
    def test_initialization_with_repository(self):
        """Test service initialization with provided repository."""
        mock_repo = Mock()
        service = DatabaseManagementService(repository=mock_repo, user_token="test-token")
        
        assert service.repository == mock_repo
        assert service.user_token == "test-token"
    
    @patch('src.services.database_management_service.DatabaseBackupRepository')
    def test_initialization_without_repository(self, mock_repo_class):
        """Test service initialization creates default repository."""
        mock_repo = Mock()
        mock_repo_class.return_value = mock_repo
        
        service = DatabaseManagementService(user_token="test-token")
        
        assert service.repository == mock_repo
        assert service.user_token == "test-token"
        mock_repo_class.assert_called_once_with(user_token="test-token")
    
    @pytest.mark.asyncio
    @patch('src.services.database_management_service.settings')
    @patch('src.services.database_management_service.os.path.exists')
    @patch('src.services.database_management_service.os.path.getsize')
    @patch('src.services.database_management_service.DatabaseBackupRepository.get_database_type')
    @patch.dict(os.environ, {"DATABRICKS_HOST": "https://workspace.databricks.com"})
    async def test_export_sqlite_success(self, mock_get_db_type, mock_getsize, mock_exists, mock_settings):
        """Test successful SQLite database export."""
        # Setup mocks
        mock_get_db_type.return_value = "sqlite"
        mock_settings.SQLITE_DB_PATH = "/path/to/test.db"
        mock_exists.return_value = True
        mock_getsize.return_value = 1024 * 1024 * 10  # 10 MB
        
        mock_repo = AsyncMock()
        mock_repo.create_sqlite_backup.return_value = {
            "success": True,
            "backup_path": "/Volumes/catalog/schema/volume/backup.db",
            "backup_size": 1024 * 1024 * 10
        }
        mock_repo.cleanup_old_backups.return_value = {"success": True, "deleted": []}
        
        service = DatabaseManagementService(repository=mock_repo)
        
        # Execute
        with patch('src.services.database_management_service.datetime') as mock_datetime:
            mock_now = Mock()
            mock_now.strftime.return_value = "20240101_120000"
            mock_now.isoformat.return_value = "2024-01-01T12:00:00"
            mock_datetime.now.return_value = mock_now
            
            result = await service.export_to_volume(
                catalog="test_catalog",
                schema="test_schema",
                volume_name="backups"
            )
        
        # Assert
        assert result["success"] is True
        assert result["backup_filename"] == "kasal_backup_20240101_120000.db"
        assert result["volume_path"] == "test_catalog.test_schema.backups"
        assert result["size_mb"] == 10.0
        assert result["original_size_mb"] == 10.0
        assert result["database_type"] == "sqlite"
        assert "databricks_url" in result
        
        # Verify repository calls
        mock_repo.create_sqlite_backup.assert_called_once_with(
            source_path="/path/to/test.db",
            catalog="test_catalog",
            schema="test_schema",
            volume_name="backups",
            backup_filename="kasal_backup_20240101_120000.db"
        )
        mock_repo.cleanup_old_backups.assert_called_once()
    
    @pytest.mark.asyncio
    @patch('src.services.database_management_service.settings')
    @patch('src.services.database_management_service.os.path.exists')
    @patch('src.services.database_management_service.DatabaseBackupRepository.get_database_type')
    async def test_export_sqlite_file_not_found(self, mock_get_db_type, mock_exists, mock_settings):
        """Test SQLite export when database file doesn't exist."""
        mock_get_db_type.return_value = "sqlite"
        mock_settings.SQLITE_DB_PATH = "/nonexistent/test.db"
        mock_exists.return_value = False
        
        service = DatabaseManagementService()
        
        result = await service.export_to_volume(
            catalog="test_catalog",
            schema="test_schema"
        )
        
        assert result["success"] is False
        assert "not found" in result["error"]
    
    @pytest.mark.asyncio
    @patch('src.services.database_management_service.async_session_factory')
    @patch('src.services.database_management_service.DatabaseBackupRepository.get_database_type')
    @patch.dict(os.environ, {"DATABRICKS_HOST": "https://workspace.databricks.com"})
    async def test_export_postgres_with_session(self, mock_get_db_type, mock_session_factory):
        """Test PostgreSQL export with provided session."""
        mock_get_db_type.return_value = "postgres"
        
        # Setup mock session
        mock_session = AsyncMock(spec=AsyncSession)
        
        # Setup mock repository
        mock_repo = AsyncMock()
        mock_repo.create_postgres_backup.return_value = {
            "success": True,
            "backup_path": "/Volumes/catalog/schema/volume/backup.sql",
            "backup_size": 1024 * 1024 * 5
        }
        mock_repo.cleanup_old_backups.return_value = {"success": True}
        
        service = DatabaseManagementService(repository=mock_repo)
        
        # Execute
        with patch('src.services.database_management_service.datetime') as mock_datetime:
            mock_now = Mock()
            mock_now.strftime.return_value = "20240101_120000"
            mock_now.isoformat.return_value = "2024-01-01T12:00:00"
            mock_datetime.now.return_value = mock_now
            
            result = await service.export_to_volume(
                catalog="test_catalog",
                schema="test_schema",
                export_format="sql",
                session=mock_session
            )
        
        # Assert
        assert result["success"] is True
        assert result["backup_filename"] == "kasal_backup_20240101_120000.sql"
        assert result["size_mb"] == 5.0
        assert "original_size_mb" not in result  # PostgreSQL doesn't have original file size
        
        # Verify repository was called with correct format
        mock_repo.create_postgres_backup.assert_called_once()
        call_args = mock_repo.create_postgres_backup.call_args
        assert call_args[1]["export_format"] == "sql"
        assert call_args[1]["session"] == mock_session
    
    @pytest.mark.asyncio
    @patch('src.services.database_management_service.async_session_factory')
    @patch('src.services.database_management_service.DatabaseBackupRepository.get_database_type')
    async def test_export_postgres_sqlite_format(self, mock_get_db_type, mock_session_factory):
        """Test PostgreSQL export in SQLite format."""
        mock_get_db_type.return_value = "postgres"
        
        # Setup mock session
        mock_session = AsyncMock()
        mock_session_factory.return_value = mock_session
        
        # Setup mock repository
        mock_repo = AsyncMock()
        mock_repo.create_postgres_backup.return_value = {
            "success": True,
            "backup_path": "/Volumes/catalog/schema/volume/backup.db",
            "backup_size": 1024 * 1024 * 3
        }
        mock_repo.cleanup_old_backups.return_value = {"success": True}
        
        service = DatabaseManagementService(repository=mock_repo)
        
        # Execute
        with patch('src.services.database_management_service.datetime') as mock_datetime:
            mock_now = Mock()
            mock_now.strftime.return_value = "20240101_120000"
            mock_datetime.now.return_value = mock_now
            
            result = await service.export_to_volume(
                catalog="test_catalog",
                schema="test_schema",
                export_format="sqlite"
            )
        
        # Assert
        assert result["success"] is True
        assert result["backup_filename"] == "kasal_backup_20240101_120000.db"
        
        # Verify SQLite format was requested
        call_args = mock_repo.create_postgres_backup.call_args
        assert call_args[1]["export_format"] == "sqlite"
        assert call_args[1]["backup_filename"].endswith(".db")
        
        # Verify session was closed
        mock_session.close.assert_called_once()
    
    @pytest.mark.asyncio
    @patch('src.services.database_management_service.DatabaseBackupRepository.get_database_type')
    async def test_export_unsupported_database(self, mock_get_db_type):
        """Test export with unsupported database type."""
        mock_get_db_type.return_value = "mysql"
        
        service = DatabaseManagementService()
        
        result = await service.export_to_volume(
            catalog="test_catalog",
            schema="test_schema"
        )
        
        assert result["success"] is False
        assert "Unsupported database type: mysql" in result["error"]
    
    @pytest.mark.asyncio
    @patch('src.services.database_management_service.DatabaseBackupRepository.get_database_type')
    @patch('src.services.database_management_service.logger')
    async def test_export_exception_handling(self, mock_logger, mock_get_db_type):
        """Test exception handling during export."""
        mock_get_db_type.side_effect = Exception("Test error")
        
        service = DatabaseManagementService()
        
        result = await service.export_to_volume(
            catalog="test_catalog",
            schema="test_schema"
        )
        
        assert result["success"] is False
        assert "Test error" in result["error"]
        mock_logger.error.assert_called()
    
    @pytest.mark.asyncio
    @patch('src.services.database_management_service.DatabaseBackupRepository.get_database_type')
    @patch.dict(os.environ, {}, clear=True)
    async def test_export_without_databricks_host(self, mock_get_db_type):
        """Test export generates default Databricks URL when host not configured."""
        mock_get_db_type.return_value = "sqlite"
        
        mock_repo = AsyncMock()
        mock_repo.create_sqlite_backup.return_value = {
            "success": True,
            "backup_path": "/path",
            "backup_size": 1000
        }
        mock_repo.cleanup_old_backups.return_value = {"success": True}
        
        service = DatabaseManagementService(repository=mock_repo)
        
        with patch('src.services.database_management_service.os.path.exists', return_value=True):
            with patch('src.services.database_management_service.os.path.getsize', return_value=1000):
                with patch('src.services.database_management_service.settings') as mock_settings:
                    mock_settings.SQLITE_DB_PATH = "/test.db"
                    
                    result = await service.export_to_volume("catalog", "schema")
        
        assert result["success"] is True
        assert "your-workspace.databricks.com" in result["databricks_url"]
    
    @pytest.mark.asyncio
    async def test_import_from_volume_invalid_filename(self):
        """Test import rejects invalid filenames."""
        service = DatabaseManagementService()
        
        # Test path traversal attempt
        result = await service.import_from_volume(
            catalog="catalog",
            schema="schema",
            volume_name="volume",
            backup_filename="../evil.db"
        )
        
        assert result["success"] is False
        assert "Invalid backup filename" in result["error"]
        
        # Test with forward slash
        result = await service.import_from_volume(
            catalog="catalog",
            schema="schema",
            volume_name="volume",
            backup_filename="path/to/file.db"
        )
        
        assert result["success"] is False
        assert "Invalid backup filename" in result["error"]
    
    @pytest.mark.asyncio
    @patch('src.services.database_management_service.settings')
    @patch('src.services.database_management_service.DatabaseBackupRepository.get_database_type')
    async def test_import_sqlite_success(self, mock_get_db_type, mock_settings):
        """Test successful SQLite import."""
        mock_get_db_type.return_value = "sqlite"
        mock_settings.SQLITE_DB_PATH = "/path/to/test.db"
        
        mock_repo = AsyncMock()
        mock_repo.restore_sqlite_backup.return_value = {
            "success": True,
            "restored_size": 1024 * 1024
        }
        
        service = DatabaseManagementService(repository=mock_repo)
        
        result = await service.import_from_volume(
            catalog="catalog",
            schema="schema",
            volume_name="volume",
            backup_filename="backup.db"
        )
        
        # Verify restore was called
        mock_repo.restore_sqlite_backup.assert_called_once_with(
            catalog="catalog",
            schema="schema",
            volume_name="volume",
            backup_filename="backup.db",
            target_path="/path/to/test.db",
            create_safety_backup=True
        )
    
    @pytest.mark.asyncio
    @patch('src.services.database_management_service.DatabaseBackupRepository.get_database_type')
    async def test_import_type_mismatch_sqlite(self, mock_get_db_type):
        """Test import fails when backup type doesn't match SQLite database."""
        mock_get_db_type.return_value = "sqlite"
        
        service = DatabaseManagementService()
        
        result = await service.import_from_volume(
            catalog="catalog",
            schema="schema",
            volume_name="volume",
            backup_filename="backup.sql"  # SQL backup for SQLite database
        )
        
        assert result["success"] is False
        assert "Cannot restore postgres_sql backup to SQLite database" in result["error"]
    
    @pytest.mark.asyncio
    @patch('src.services.database_management_service.DatabaseBackupRepository.get_database_type')
    async def test_import_type_mismatch_postgres(self, mock_get_db_type):
        """Test import fails when backup type doesn't match PostgreSQL database."""
        mock_get_db_type.return_value = "postgres"
        
        service = DatabaseManagementService()
        
        result = await service.import_from_volume(
            catalog="catalog",
            schema="schema",
            volume_name="volume",
            backup_filename="wrong.txt"  # Unknown backup type
        )
        
        assert result["success"] is False
        assert "Cannot restore unknown backup to PostgreSQL database" in result["error"]
    
    @pytest.mark.asyncio
    @patch('src.services.database_management_service.DatabaseBackupRepository.get_database_type')
    @patch('src.services.database_management_service.logger')
    async def test_export_cleanup_old_backups(self, mock_logger, mock_get_db_type):
        """Test that old backups are cleaned up after export."""
        mock_get_db_type.return_value = "sqlite"
        
        mock_repo = AsyncMock()
        mock_repo.create_sqlite_backup.return_value = {
            "success": True,
            "backup_path": "/path",
            "backup_size": 1000
        }
        mock_repo.cleanup_old_backups.return_value = {
            "success": True,
            "deleted": ["old1.db", "old2.db"]
        }
        
        service = DatabaseManagementService(repository=mock_repo)
        
        with patch('src.services.database_management_service.os.path.exists', return_value=True):
            with patch('src.services.database_management_service.os.path.getsize', return_value=1000):
                with patch('src.services.database_management_service.settings') as mock_settings:
                    mock_settings.SQLITE_DB_PATH = "/test.db"
                    
                    await service.export_to_volume("catalog", "schema")
        
        # Verify cleanup was called
        mock_repo.cleanup_old_backups.assert_called_once_with(
            catalog="catalog",
            schema="schema",
            volume_name="kasal_backups",
            keep_count=5
        )
        
        # Verify cleanup was logged
        mock_logger.info.assert_any_call("Cleaned up old backups: ['old1.db', 'old2.db']")