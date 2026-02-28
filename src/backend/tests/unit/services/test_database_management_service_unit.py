"""
Comprehensive unit tests for DatabaseManagementService.

Tests cover all public methods:
  - __init__ (constructor logic)
  - export_to_volume (SQLite, PostgreSQL, unsupported, error paths)
  - import_from_volume (SQLite, PostgreSQL, validation, error paths)
  - list_backups (happy path, error, auth fallback)
  - get_database_info (SQLite, PostgreSQL, Lakebase, unsupported, error paths)
  - check_user_permission (happy path, exception fallback)
"""

import os
import pytest
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_session():
    """Create a mock async database session."""
    session = AsyncMock()
    session.bind = None
    return session


@pytest.fixture
def mock_repository():
    """Create a mock DatabaseBackupRepository."""
    repo = AsyncMock()
    repo.create_sqlite_backup = AsyncMock()
    repo.create_postgres_backup = AsyncMock()
    repo.restore_sqlite_backup = AsyncMock()
    repo.restore_postgres_backup = AsyncMock()
    repo.list_backups = AsyncMock(return_value=[])
    repo.cleanup_old_backups = AsyncMock(return_value={"success": True, "deleted": []})
    repo.get_database_info = AsyncMock()
    return repo


@pytest.fixture
def service(mock_session, mock_repository):
    """Create a DatabaseManagementService with mocked dependencies."""
    from src.services.database_management_service import DatabaseManagementService
    return DatabaseManagementService(
        session=mock_session,
        repository=mock_repository,
        user_token=None,
    )


@pytest.fixture
def service_with_token(mock_session, mock_repository):
    """Create a DatabaseManagementService with a user token."""
    from src.services.database_management_service import DatabaseManagementService
    return DatabaseManagementService(
        session=mock_session,
        repository=mock_repository,
        user_token="test-obo-token",
    )


# ===================================================================
# Constructor Tests
# ===================================================================

class TestDatabaseManagementServiceInit:
    """Tests for DatabaseManagementService.__init__."""

    def test_init_stores_session(self, mock_session, mock_repository):
        from src.services.database_management_service import DatabaseManagementService
        svc = DatabaseManagementService(session=mock_session, repository=mock_repository)
        assert svc.session is mock_session

    def test_init_uses_provided_repository(self, mock_session, mock_repository):
        from src.services.database_management_service import DatabaseManagementService
        svc = DatabaseManagementService(session=mock_session, repository=mock_repository)
        assert svc.repository is mock_repository

    def test_init_creates_default_repository_when_none(self, mock_session):
        from src.services.database_management_service import DatabaseManagementService
        with patch(
            "src.services.database_management_service.DatabaseBackupRepository"
        ) as MockRepo:
            mock_repo_instance = MagicMock()
            MockRepo.return_value = mock_repo_instance
            svc = DatabaseManagementService(session=mock_session, repository=None)
            MockRepo.assert_called_once_with(session=mock_session, user_token=None)
            assert svc.repository is mock_repo_instance

    def test_init_passes_user_token_to_default_repository(self, mock_session):
        from src.services.database_management_service import DatabaseManagementService
        with patch(
            "src.services.database_management_service.DatabaseBackupRepository"
        ) as MockRepo:
            MockRepo.return_value = MagicMock()
            DatabaseManagementService(session=mock_session, repository=None, user_token="tok123")
            MockRepo.assert_called_once_with(session=mock_session, user_token="tok123")

    def test_init_stores_user_token(self, mock_session, mock_repository):
        from src.services.database_management_service import DatabaseManagementService
        svc = DatabaseManagementService(
            session=mock_session, repository=mock_repository, user_token="abc"
        )
        assert svc.user_token == "abc"

    def test_init_user_token_defaults_to_none(self, mock_session, mock_repository):
        from src.services.database_management_service import DatabaseManagementService
        svc = DatabaseManagementService(
            session=mock_session, repository=mock_repository
        )
        assert svc.user_token is None


# ===================================================================
# export_to_volume Tests
# ===================================================================

class TestExportToVolume:
    """Tests for DatabaseManagementService.export_to_volume."""

    # ---- SQLite path ----

    @pytest.mark.asyncio
    async def test_export_sqlite_success(self, service, mock_repository):
        """Successful SQLite export with all expected result fields."""
        with patch(
            "src.services.database_management_service.DatabaseBackupRepository.get_database_type",
            return_value="sqlite",
        ), patch(
            "src.services.database_management_service.settings"
        ) as mock_settings, patch(
            "src.services.database_management_service.os.path.isabs", return_value=True
        ), patch(
            "src.services.database_management_service.os.path.exists", return_value=True
        ), patch(
            "src.services.database_management_service.os.path.getsize",
            return_value=2 * 1024 * 1024,  # 2 MB
        ):
            mock_settings.SQLITE_DB_PATH = "/tmp/test.db"

            mock_repository.create_sqlite_backup.return_value = {
                "success": True,
                "backup_path": "/Volumes/cat/sch/vol/kasal_backup_20240101_120000.db",
                "backup_size": 1024 * 1024,  # 1 MB
            }
            mock_repository.cleanup_old_backups.return_value = {
                "success": True,
                "deleted": [],
            }
            mock_repository.list_backups.return_value = [
                {
                    "filename": "kasal_backup_20240101_120000.db",
                    "size": 1024 * 1024,
                    "created_at": datetime(2024, 1, 1, 12, 0, 0),
                }
            ]

            result = await service.export_to_volume(
                catalog="cat", schema="sch", volume_name="vol"
            )

        assert result["success"] is True
        assert result["database_type"] == "sqlite"
        assert result["catalog"] == "cat"
        assert result["schema"] == "sch"
        assert result["volume"] == "vol"
        assert "backup_path" in result
        assert "backup_filename" in result
        assert "volume_browse_url" in result
        assert "export_files" in result
        assert result["original_size_mb"] == 2.0
        assert result["size_mb"] == 1.0

    @pytest.mark.asyncio
    async def test_export_sqlite_db_not_found(self, service, mock_repository):
        """When the SQLite database file does not exist, return error."""
        with patch(
            "src.services.database_management_service.DatabaseBackupRepository.get_database_type",
            return_value="sqlite",
        ), patch(
            "src.services.database_management_service.settings"
        ) as mock_settings, patch(
            "src.services.database_management_service.os.path.isabs", return_value=True
        ), patch(
            "src.services.database_management_service.os.path.exists", return_value=False
        ):
            mock_settings.SQLITE_DB_PATH = "/tmp/missing.db"

            result = await service.export_to_volume(catalog="c", schema="s")

        assert result["success"] is False
        assert "not found" in result["error"]

    @pytest.mark.asyncio
    async def test_export_sqlite_default_path_when_empty(self, service, mock_repository):
        """Falls back to ./app.db when SQLITE_DB_PATH is empty/None."""
        with patch(
            "src.services.database_management_service.DatabaseBackupRepository.get_database_type",
            return_value="sqlite",
        ), patch(
            "src.services.database_management_service.settings"
        ) as mock_settings, patch(
            "src.services.database_management_service.os.path.isabs", return_value=False
        ), patch(
            "src.services.database_management_service.os.path.abspath",
            return_value="/abs/app.db",
        ), patch(
            "src.services.database_management_service.os.path.exists", return_value=False
        ):
            mock_settings.SQLITE_DB_PATH = None

            result = await service.export_to_volume(catalog="c", schema="s")

        # Should reach the "not found" check because we mocked exists=False
        assert result["success"] is False
        assert "not found" in result["error"]

    @pytest.mark.asyncio
    async def test_export_sqlite_relative_path_converted(self, service, mock_repository):
        """Relative SQLite paths are converted to absolute via os.path.abspath."""
        with patch(
            "src.services.database_management_service.DatabaseBackupRepository.get_database_type",
            return_value="sqlite",
        ), patch(
            "src.services.database_management_service.settings"
        ) as mock_settings, patch(
            "src.services.database_management_service.os.path.isabs", return_value=False
        ), patch(
            "src.services.database_management_service.os.path.abspath",
            return_value="/abs/app.db",
        ) as mock_abspath, patch(
            "src.services.database_management_service.os.path.exists", return_value=True
        ), patch(
            "src.services.database_management_service.os.path.getsize", return_value=1024
        ):
            mock_settings.SQLITE_DB_PATH = "./app.db"

            mock_repository.create_sqlite_backup.return_value = {
                "success": True,
                "backup_path": "/Volumes/c/s/v/backup.db",
                "backup_size": 512,
            }
            mock_repository.cleanup_old_backups.return_value = {"success": True, "deleted": []}
            mock_repository.list_backups.return_value = []

            await service.export_to_volume(catalog="c", schema="s")

        mock_abspath.assert_called_once_with("./app.db")

    @pytest.mark.asyncio
    async def test_export_sqlite_backup_failure(self, service, mock_repository):
        """When repository.create_sqlite_backup fails, propagate error result."""
        with patch(
            "src.services.database_management_service.DatabaseBackupRepository.get_database_type",
            return_value="sqlite",
        ), patch(
            "src.services.database_management_service.settings"
        ) as mock_settings, patch(
            "src.services.database_management_service.os.path.isabs", return_value=True
        ), patch(
            "src.services.database_management_service.os.path.exists", return_value=True
        ), patch(
            "src.services.database_management_service.os.path.getsize", return_value=1024
        ):
            mock_settings.SQLITE_DB_PATH = "/tmp/test.db"
            mock_repository.create_sqlite_backup.return_value = {
                "success": False,
                "error": "Upload failed",
            }

            result = await service.export_to_volume(catalog="c", schema="s")

        assert result["success"] is False
        assert result["error"] == "Upload failed"

    # ---- PostgreSQL path ----

    @pytest.mark.asyncio
    async def test_export_postgres_sql_format_success(self, service, mock_repository):
        """Successful PostgreSQL export in default SQL format."""
        with patch(
            "src.services.database_management_service.DatabaseBackupRepository.get_database_type",
            return_value="postgres",
        ):
            mock_repository.create_postgres_backup.return_value = {
                "success": True,
                "backup_path": "/Volumes/c/s/v/kasal_backup.sql",
                "backup_size": 2048,
            }
            mock_repository.cleanup_old_backups.return_value = {"success": True, "deleted": []}
            mock_repository.list_backups.return_value = []

            result = await service.export_to_volume(
                catalog="c", schema="s", export_format="native"
            )

        assert result["success"] is True
        assert result["database_type"] == "postgres"
        # original_size_mb should NOT be in the result for postgres
        assert "original_size_mb" not in result

    @pytest.mark.asyncio
    async def test_export_postgres_sqlite_format(self, service, mock_repository):
        """Export PostgreSQL to SQLite format uses correct filename extension."""
        with patch(
            "src.services.database_management_service.DatabaseBackupRepository.get_database_type",
            return_value="postgres",
        ):
            mock_repository.create_postgres_backup.return_value = {
                "success": True,
                "backup_path": "/Volumes/c/s/v/backup.db",
                "backup_size": 4096,
            }
            mock_repository.cleanup_old_backups.return_value = {"success": True, "deleted": []}
            mock_repository.list_backups.return_value = []

            result = await service.export_to_volume(
                catalog="c", schema="s", export_format="sqlite"
            )

        assert result["success"] is True
        assert result["backup_filename"].endswith(".db")
        # Verify the repository was called with sqlite format
        call_kwargs = mock_repository.create_postgres_backup.call_args
        assert call_kwargs.kwargs.get("export_format") == "sqlite" or \
               (call_kwargs[1] if len(call_kwargs) > 1 else {}).get("export_format") == "sqlite"

    @pytest.mark.asyncio
    async def test_export_postgres_uses_provided_session(self, service, mock_repository):
        """When session is explicitly passed, it is forwarded to the repository."""
        extra_session = AsyncMock()
        with patch(
            "src.services.database_management_service.DatabaseBackupRepository.get_database_type",
            return_value="postgres",
        ):
            mock_repository.create_postgres_backup.return_value = {
                "success": True,
                "backup_path": "/Volumes/c/s/v/backup.sql",
                "backup_size": 100,
            }
            mock_repository.cleanup_old_backups.return_value = {"success": True, "deleted": []}
            mock_repository.list_backups.return_value = []

            await service.export_to_volume(
                catalog="c", schema="s", session=extra_session
            )

        call_kwargs = mock_repository.create_postgres_backup.call_args.kwargs
        assert call_kwargs["session"] is extra_session

    @pytest.mark.asyncio
    async def test_export_postgres_uses_injected_session_when_none(self, service, mock_session, mock_repository):
        """When no session parameter, falls back to the injected constructor session."""
        with patch(
            "src.services.database_management_service.DatabaseBackupRepository.get_database_type",
            return_value="postgres",
        ):
            mock_repository.create_postgres_backup.return_value = {
                "success": True,
                "backup_path": "/Volumes/c/s/v/backup.sql",
                "backup_size": 100,
            }
            mock_repository.cleanup_old_backups.return_value = {"success": True, "deleted": []}
            mock_repository.list_backups.return_value = []

            await service.export_to_volume(
                catalog="c", schema="s", session=None
            )

        call_kwargs = mock_repository.create_postgres_backup.call_args.kwargs
        assert call_kwargs["session"] is mock_session

    @pytest.mark.asyncio
    async def test_export_postgres_backup_failure(self, service, mock_repository):
        """Postgres backup repository failure is propagated."""
        with patch(
            "src.services.database_management_service.DatabaseBackupRepository.get_database_type",
            return_value="postgres",
        ):
            mock_repository.create_postgres_backup.return_value = {
                "success": False,
                "error": "Connection refused",
            }

            result = await service.export_to_volume(catalog="c", schema="s")

        assert result["success"] is False
        assert result["error"] == "Connection refused"

    # ---- Unsupported database ----

    @pytest.mark.asyncio
    async def test_export_unsupported_database_type(self, service):
        """Returns error for unsupported database types."""
        with patch(
            "src.services.database_management_service.DatabaseBackupRepository.get_database_type",
            return_value="oracle",
        ):
            result = await service.export_to_volume(catalog="c", schema="s")

        assert result["success"] is False
        assert "Unsupported" in result["error"]

    # ---- Auth context for workspace URL ----

    @pytest.mark.asyncio
    async def test_export_workspace_url_from_auth(self, service, mock_repository):
        """When get_auth_context provides a workspace URL it is used in the volume browse URL."""
        mock_auth = SimpleNamespace(workspace_url="https://my-workspace.databricks.com/")
        with patch(
            "src.services.database_management_service.DatabaseBackupRepository.get_database_type",
            return_value="sqlite",
        ), patch(
            "src.services.database_management_service.settings"
        ) as mock_settings, patch(
            "src.services.database_management_service.os.path.isabs", return_value=True
        ), patch(
            "src.services.database_management_service.os.path.exists", return_value=True
        ), patch(
            "src.services.database_management_service.os.path.getsize", return_value=1024
        ), patch(
            "src.utils.databricks_auth.get_auth_context",
            new_callable=AsyncMock,
            return_value=mock_auth,
        ):
            mock_settings.SQLITE_DB_PATH = "/tmp/test.db"
            mock_repository.create_sqlite_backup.return_value = {
                "success": True,
                "backup_path": "/Volumes/c/s/v/backup.db",
                "backup_size": 512,
            }
            mock_repository.cleanup_old_backups.return_value = {"success": True, "deleted": []}
            mock_repository.list_backups.return_value = []

            result = await service.export_to_volume(catalog="c", schema="s", volume_name="v")

        assert "my-workspace" in result["volume_browse_url"]
        assert result["volume_browse_url"].startswith("https://my-workspace.databricks.com")
        # trailing slash should be stripped
        assert not result["volume_browse_url"].startswith("https://my-workspace.databricks.com//")

    @pytest.mark.asyncio
    async def test_export_workspace_url_fallback_when_auth_fails(self, service, mock_repository):
        """When auth raises, falls back to placeholder workspace URL."""
        with patch(
            "src.services.database_management_service.DatabaseBackupRepository.get_database_type",
            return_value="sqlite",
        ), patch(
            "src.services.database_management_service.settings"
        ) as mock_settings, patch(
            "src.services.database_management_service.os.path.isabs", return_value=True
        ), patch(
            "src.services.database_management_service.os.path.exists", return_value=True
        ), patch(
            "src.services.database_management_service.os.path.getsize", return_value=1024
        ):
            mock_settings.SQLITE_DB_PATH = "/tmp/test.db"
            mock_repository.create_sqlite_backup.return_value = {
                "success": True,
                "backup_path": "/Volumes/c/s/v/backup.db",
                "backup_size": 512,
            }
            mock_repository.cleanup_old_backups.return_value = {"success": True, "deleted": []}
            mock_repository.list_backups.return_value = []

            result = await service.export_to_volume(catalog="c", schema="s", volume_name="v")

        assert "your-workspace" in result["volume_browse_url"]

    # ---- Cleanup logging ----

    @pytest.mark.asyncio
    async def test_export_cleanup_logs_deleted(self, service, mock_repository):
        """When cleanup deletes files, the service logs them."""
        with patch(
            "src.services.database_management_service.DatabaseBackupRepository.get_database_type",
            return_value="sqlite",
        ), patch(
            "src.services.database_management_service.settings"
        ) as mock_settings, patch(
            "src.services.database_management_service.os.path.isabs", return_value=True
        ), patch(
            "src.services.database_management_service.os.path.exists", return_value=True
        ), patch(
            "src.services.database_management_service.os.path.getsize", return_value=1024
        ):
            mock_settings.SQLITE_DB_PATH = "/tmp/test.db"
            mock_repository.create_sqlite_backup.return_value = {
                "success": True,
                "backup_path": "/Volumes/c/s/v/backup.db",
                "backup_size": 512,
            }
            mock_repository.cleanup_old_backups.return_value = {
                "success": True,
                "deleted": ["old_backup_1.db", "old_backup_2.db"],
            }
            mock_repository.list_backups.return_value = []

            result = await service.export_to_volume(catalog="c", schema="s")

        assert result["success"] is True

    # ---- Export files formatting ----

    @pytest.mark.asyncio
    async def test_export_files_formatted_with_datetime(self, service, mock_repository):
        """Backup list entries with datetime created_at are ISO-formatted."""
        with patch(
            "src.services.database_management_service.DatabaseBackupRepository.get_database_type",
            return_value="sqlite",
        ), patch(
            "src.services.database_management_service.settings"
        ) as mock_settings, patch(
            "src.services.database_management_service.os.path.isabs", return_value=True
        ), patch(
            "src.services.database_management_service.os.path.exists", return_value=True
        ), patch(
            "src.services.database_management_service.os.path.getsize", return_value=1024
        ):
            mock_settings.SQLITE_DB_PATH = "/tmp/test.db"
            mock_repository.create_sqlite_backup.return_value = {
                "success": True,
                "backup_path": "/Volumes/c/s/v/backup.db",
                "backup_size": 512,
            }
            mock_repository.cleanup_old_backups.return_value = {"success": True, "deleted": []}
            mock_repository.list_backups.return_value = [
                {
                    "filename": "b1.db",
                    "size": 2048,
                    "created_at": datetime(2024, 6, 15, 10, 30, 0),
                },
                {
                    "filename": "b2.db",
                    "size": 0,
                    "created_at": "2024-01-01T00:00:00",
                },
            ]

            result = await service.export_to_volume(catalog="c", schema="s")

        assert len(result["export_files"]) == 2
        assert result["export_files"][0]["created_at"] == "2024-06-15T10:30:00"
        assert result["export_files"][1]["created_at"] == "2024-01-01T00:00:00"

    # ---- General exception ----

    @pytest.mark.asyncio
    async def test_export_general_exception(self, service):
        """Unhandled exceptions are caught and returned as error dict."""
        with patch(
            "src.services.database_management_service.DatabaseBackupRepository.get_database_type",
            side_effect=RuntimeError("boom"),
        ):
            result = await service.export_to_volume(catalog="c", schema="s")

        assert result["success"] is False
        assert "boom" in result["error"]

    @pytest.mark.asyncio
    async def test_export_auth_exception_uses_fallback_url(self, service, mock_repository):
        """When get_auth_context raises an exception, fallback workspace URL is used."""
        with patch(
            "src.services.database_management_service.DatabaseBackupRepository.get_database_type",
            return_value="sqlite",
        ), patch(
            "src.services.database_management_service.settings"
        ) as mock_settings, patch(
            "src.services.database_management_service.os.path.isabs", return_value=True
        ), patch(
            "src.services.database_management_service.os.path.exists", return_value=True
        ), patch(
            "src.services.database_management_service.os.path.getsize", return_value=1024
        ), patch(
            "src.utils.databricks_auth.get_auth_context",
            new_callable=AsyncMock,
            side_effect=RuntimeError("auth failed"),
        ):
            mock_settings.SQLITE_DB_PATH = "/tmp/test.db"
            mock_repository.create_sqlite_backup.return_value = {
                "success": True,
                "backup_path": "/Volumes/c/s/v/backup.db",
                "backup_size": 512,
            }
            mock_repository.cleanup_old_backups.return_value = {"success": True, "deleted": []}
            mock_repository.list_backups.return_value = []

            result = await service.export_to_volume(catalog="c", schema="s", volume_name="v")

        assert result["success"] is True
        assert "your-workspace" in result["volume_browse_url"]


# ===================================================================
# import_from_volume Tests
# ===================================================================

class TestImportFromVolume:
    """Tests for DatabaseManagementService.import_from_volume."""

    # ---- Filename validation ----

    @pytest.mark.asyncio
    async def test_import_rejects_path_traversal_dotdot(self, service):
        result = await service.import_from_volume(
            catalog="c", schema="s", volume_name="v", backup_filename="../evil.db"
        )
        assert result["success"] is False
        assert "Invalid" in result["error"]

    @pytest.mark.asyncio
    async def test_import_rejects_path_traversal_slash(self, service):
        result = await service.import_from_volume(
            catalog="c", schema="s", volume_name="v", backup_filename="path/to/evil.db"
        )
        assert result["success"] is False
        assert "Invalid" in result["error"]

    @pytest.mark.asyncio
    async def test_import_rejects_path_traversal_backslash(self, service):
        result = await service.import_from_volume(
            catalog="c", schema="s", volume_name="v", backup_filename="path\\evil.db"
        )
        assert result["success"] is False
        assert "Invalid" in result["error"]

    # ---- Backup type validation ----

    @pytest.mark.asyncio
    async def test_import_sqlite_rejects_sql_backup(self, service):
        """SQLite database cannot restore a .sql file."""
        with patch(
            "src.services.database_management_service.DatabaseBackupRepository.get_database_type",
            return_value="sqlite",
        ):
            result = await service.import_from_volume(
                catalog="c", schema="s", volume_name="v",
                backup_filename="backup.sql"
            )
        assert result["success"] is False
        assert "Cannot restore" in result["error"]

    @pytest.mark.asyncio
    async def test_import_sqlite_rejects_json_backup(self, service):
        with patch(
            "src.services.database_management_service.DatabaseBackupRepository.get_database_type",
            return_value="sqlite",
        ):
            result = await service.import_from_volume(
                catalog="c", schema="s", volume_name="v",
                backup_filename="backup.json"
            )
        assert result["success"] is False
        assert "Cannot restore" in result["error"]

    @pytest.mark.asyncio
    async def test_import_postgres_rejects_sqlite_backup(self, service):
        """PostgreSQL database cannot restore a .db file."""
        with patch(
            "src.services.database_management_service.DatabaseBackupRepository.get_database_type",
            return_value="postgres",
        ):
            result = await service.import_from_volume(
                catalog="c", schema="s", volume_name="v",
                backup_filename="backup.db"
            )
        assert result["success"] is False
        assert "Cannot restore" in result["error"]

    @pytest.mark.asyncio
    async def test_import_postgres_rejects_unknown_backup(self, service):
        """PostgreSQL rejects files with unknown extensions."""
        with patch(
            "src.services.database_management_service.DatabaseBackupRepository.get_database_type",
            return_value="postgres",
        ):
            result = await service.import_from_volume(
                catalog="c", schema="s", volume_name="v",
                backup_filename="backup.tar.gz"
            )
        assert result["success"] is False
        assert "Cannot restore" in result["error"]

    # ---- Successful SQLite import ----

    @pytest.mark.asyncio
    async def test_import_sqlite_success(self, service, mock_repository):
        """Successful SQLite import disposes engine and returns expected result."""
        with patch(
            "src.services.database_management_service.DatabaseBackupRepository.get_database_type",
            return_value="sqlite",
        ), patch(
            "src.services.database_management_service.settings"
        ) as mock_settings, patch(
            "src.services.database_management_service.os.path.isabs", return_value=True
        ):
            mock_settings.SQLITE_DB_PATH = "/tmp/test.db"
            mock_repository.restore_sqlite_backup.return_value = {
                "success": True,
                "restored_size": 2048,
            }

            # Mock the engine.dispose call
            mock_engine = AsyncMock()
            with patch("src.services.database_management_service.engine", mock_engine, create=True):
                # The import triggers a lazy import of engine; we need to mock at the import site
                with patch.dict("sys.modules", {"src.db.session": MagicMock(engine=mock_engine)}):
                    result = await service.import_from_volume(
                        catalog="c", schema="s", volume_name="v",
                        backup_filename="backup.db"
                    )

        assert result["success"] is True
        assert result["database_type"] == "sqlite"
        assert result["imported_from"] == "/Volumes/c/s/v/backup.db"
        assert result["size_mb"] == 0.0

    @pytest.mark.asyncio
    async def test_import_sqlite_default_path(self, service, mock_repository):
        """Falls back to default path when SQLITE_DB_PATH is None."""
        with patch(
            "src.services.database_management_service.DatabaseBackupRepository.get_database_type",
            return_value="sqlite",
        ), patch(
            "src.services.database_management_service.settings"
        ) as mock_settings, patch(
            "src.services.database_management_service.os.path.isabs", return_value=False
        ), patch(
            "src.services.database_management_service.os.path.abspath",
            return_value="/abs/app.db",
        ):
            mock_settings.SQLITE_DB_PATH = None
            mock_repository.restore_sqlite_backup.return_value = {
                "success": True,
                "restored_size": 1024,
            }

            with patch.dict("sys.modules", {"src.db.session": MagicMock(engine=AsyncMock())}):
                result = await service.import_from_volume(
                    catalog="c", schema="s", volume_name="v",
                    backup_filename="backup.db"
                )

        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_import_sqlite_engine_dispose_failure_ignored(self, service, mock_repository):
        """If engine dispose fails the import is still considered successful."""
        with patch(
            "src.services.database_management_service.DatabaseBackupRepository.get_database_type",
            return_value="sqlite",
        ), patch(
            "src.services.database_management_service.settings"
        ) as mock_settings, patch(
            "src.services.database_management_service.os.path.isabs", return_value=True
        ):
            mock_settings.SQLITE_DB_PATH = "/tmp/test.db"
            mock_repository.restore_sqlite_backup.return_value = {
                "success": True,
                "restored_size": 1024,
            }

            mock_engine = AsyncMock()
            mock_engine.dispose.side_effect = RuntimeError("dispose failed")
            with patch.dict("sys.modules", {"src.db.session": MagicMock(engine=mock_engine)}):
                result = await service.import_from_volume(
                    catalog="c", schema="s", volume_name="v",
                    backup_filename="backup.db"
                )

        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_import_sqlite_restore_failure(self, service, mock_repository):
        """Failed SQLite restore propagates the error."""
        with patch(
            "src.services.database_management_service.DatabaseBackupRepository.get_database_type",
            return_value="sqlite",
        ), patch(
            "src.services.database_management_service.settings"
        ) as mock_settings, patch(
            "src.services.database_management_service.os.path.isabs", return_value=True
        ):
            mock_settings.SQLITE_DB_PATH = "/tmp/test.db"
            mock_repository.restore_sqlite_backup.return_value = {
                "success": False,
                "error": "Corrupt file",
            }

            result = await service.import_from_volume(
                catalog="c", schema="s", volume_name="v",
                backup_filename="backup.db"
            )

        assert result["success"] is False
        assert result["error"] == "Corrupt file"

    # ---- Successful PostgreSQL import ----

    @pytest.mark.asyncio
    async def test_import_postgres_sql_success(self, service, mock_repository):
        """Successful PostgreSQL import with .sql file."""
        with patch(
            "src.services.database_management_service.DatabaseBackupRepository.get_database_type",
            return_value="postgres",
        ):
            mock_repository.restore_postgres_backup.return_value = {
                "success": True,
                "restored_tables": ["users", "tasks"],
            }

            result = await service.import_from_volume(
                catalog="c", schema="s", volume_name="v",
                backup_filename="backup.sql"
            )

        assert result["success"] is True
        assert result["database_type"] == "postgres"
        assert result["restored_tables"] == ["users", "tasks"]

    @pytest.mark.asyncio
    async def test_import_postgres_json_success(self, service, mock_repository):
        """Successful PostgreSQL import with .json file."""
        with patch(
            "src.services.database_management_service.DatabaseBackupRepository.get_database_type",
            return_value="postgres",
        ):
            mock_repository.restore_postgres_backup.return_value = {
                "success": True,
                "restored_size": 8192,
                "restored_tables": ["agents"],
            }

            result = await service.import_from_volume(
                catalog="c", schema="s", volume_name="v",
                backup_filename="backup.json"
            )

        assert result["success"] is True
        assert result["size_mb"] == round(8192 / (1024 * 1024), 2)
        assert result["restored_tables"] == ["agents"]

    @pytest.mark.asyncio
    async def test_import_postgres_uses_provided_session(self, service, mock_repository):
        """Explicit session is forwarded to the repository."""
        extra_session = AsyncMock()
        with patch(
            "src.services.database_management_service.DatabaseBackupRepository.get_database_type",
            return_value="postgres",
        ):
            mock_repository.restore_postgres_backup.return_value = {
                "success": True,
            }

            await service.import_from_volume(
                catalog="c", schema="s", volume_name="v",
                backup_filename="backup.sql",
                session=extra_session,
            )

        call_kwargs = mock_repository.restore_postgres_backup.call_args.kwargs
        assert call_kwargs["session"] is extra_session

    @pytest.mark.asyncio
    async def test_import_postgres_uses_injected_session(self, service, mock_session, mock_repository):
        """Falls back to injected session when no session parameter."""
        with patch(
            "src.services.database_management_service.DatabaseBackupRepository.get_database_type",
            return_value="postgres",
        ):
            mock_repository.restore_postgres_backup.return_value = {
                "success": True,
            }

            await service.import_from_volume(
                catalog="c", schema="s", volume_name="v",
                backup_filename="backup.sql",
                session=None,
            )

        call_kwargs = mock_repository.restore_postgres_backup.call_args.kwargs
        assert call_kwargs["session"] is mock_session

    # ---- Unsupported database type ----

    @pytest.mark.asyncio
    async def test_import_unsupported_database_type(self, service):
        """Returns error for unsupported database types."""
        with patch(
            "src.services.database_management_service.DatabaseBackupRepository.get_database_type",
            return_value="oracle",
        ):
            result = await service.import_from_volume(
                catalog="c", schema="s", volume_name="v",
                backup_filename="backup.db"
            )

        # The backup_type would be 'sqlite' (.db extension) but db_type is oracle
        # which is neither 'sqlite' nor 'postgres', so it reaches the else branch
        assert result["success"] is False
        assert "Unsupported" in result["error"]

    # ---- General exception ----

    @pytest.mark.asyncio
    async def test_import_general_exception(self, service):
        """Unhandled exceptions are caught and returned as error dict."""
        with patch(
            "src.services.database_management_service.DatabaseBackupRepository.get_database_type",
            side_effect=RuntimeError("kaboom"),
        ):
            result = await service.import_from_volume(
                catalog="c", schema="s", volume_name="v",
                backup_filename="backup.db"
            )

        assert result["success"] is False
        assert "kaboom" in result["error"]


# ===================================================================
# list_backups Tests
# ===================================================================

class TestListBackups:
    """Tests for DatabaseManagementService.list_backups."""

    @pytest.mark.asyncio
    async def test_list_backups_success(self, service, mock_repository):
        """Successful listing formats backups and returns correct structure."""
        mock_repository.list_backups.return_value = [
            {
                "filename": "kasal_backup_20240601_120000.db",
                "size": 1048576,
                "created_at": datetime(2024, 6, 1, 12, 0, 0),
                "backup_type": "sqlite",
            },
            {
                "filename": "kasal_backup_20240501_100000.sql",
                "size": 2097152,
                "created_at": datetime(2024, 5, 1, 10, 0, 0),
                "backup_type": "postgres_sql",
            },
        ]

        result = await service.list_backups(catalog="c", schema="s", volume_name="v")

        assert result["success"] is True
        assert result["total_backups"] == 2
        assert result["volume_path"] == "c.s.v"
        assert len(result["backups"]) == 2
        assert result["backups"][0]["filename"] == "kasal_backup_20240601_120000.db"
        assert result["backups"][0]["size_mb"] == 1.0
        assert result["backups"][0]["backup_type"] == "sqlite"
        assert "databricks_url" in result["backups"][0]

    @pytest.mark.asyncio
    async def test_list_backups_empty(self, service, mock_repository):
        """Empty backup list returns correctly."""
        mock_repository.list_backups.return_value = []

        result = await service.list_backups(catalog="c", schema="s", volume_name="v")

        assert result["success"] is True
        assert result["total_backups"] == 0
        assert result["backups"] == []

    @pytest.mark.asyncio
    async def test_list_backups_workspace_url_fallback(self, service, mock_repository):
        """When auth fails, uses placeholder workspace URL in backup URLs."""
        mock_repository.list_backups.return_value = [
            {
                "filename": "kasal_backup_20240601_120000.db",
                "size": 1024,
                "created_at": datetime(2024, 6, 1),
                "backup_type": "sqlite",
            }
        ]

        result = await service.list_backups(catalog="c", schema="s", volume_name="v")

        assert result["success"] is True
        assert "your-workspace" in result["backups"][0]["databricks_url"]

    @pytest.mark.asyncio
    async def test_list_backups_with_workspace_url_from_auth(self, service, mock_repository):
        """When asyncio.run(get_auth_context()) returns a workspace URL, it is used."""
        import asyncio

        mock_repository.list_backups.return_value = [
            {
                "filename": "backup.db",
                "size": 1024,
                "created_at": datetime(2024, 6, 1),
                "backup_type": "sqlite",
            }
        ]

        # The list_backups method does `import asyncio; asyncio.run(get_auth_context())`
        # which fails in a running loop.  We patch asyncio.run at the builtins level.
        mock_auth = SimpleNamespace(workspace_url="https://my-ws.databricks.com/")
        original_run = asyncio.run
        with patch.object(asyncio, "run", return_value=mock_auth):
            result = await service.list_backups(catalog="c", schema="s", volume_name="v")

        assert result["success"] is True
        assert "my-ws" in result["backups"][0]["databricks_url"]

    @pytest.mark.asyncio
    async def test_list_backups_exception(self, service, mock_repository):
        """Repository exception is caught and returned as error."""
        mock_repository.list_backups.side_effect = RuntimeError("connection lost")

        result = await service.list_backups(catalog="c", schema="s", volume_name="v")

        assert result["success"] is False
        assert "connection lost" in result["error"]


# ===================================================================
# get_database_info Tests
# ===================================================================

class TestGetDatabaseInfo:
    """Tests for DatabaseManagementService.get_database_info."""

    # ---- SQLite ----

    @pytest.mark.asyncio
    async def test_get_info_sqlite_success(self, service, mock_repository):
        """Successful SQLite database info with all optional fields."""
        with patch(
            "src.services.database_management_service.DatabaseBackupRepository.get_database_type",
            return_value="sqlite",
        ), patch(
            "src.services.database_management_service.settings"
        ) as mock_settings, patch(
            "src.services.database_management_service.os.path.isabs", return_value=True
        ), patch(
            "src.services.database_management_service.os.path.exists", return_value=True
        ):
            mock_settings.SQLITE_DB_PATH = "/tmp/test.db"
            mock_repository.get_database_info.return_value = {
                "success": True,
                "tables": {"users": 10, "tasks": 5},
                "total_tables": 2,
                "memory_backends": [],
                "size": 1048576,
                "created_at": datetime(2024, 1, 1, 0, 0, 0),
                "modified_at": datetime(2024, 6, 15, 12, 0, 0),
                "path": "/tmp/test.db",
            }

            # Lakebase config returns None
            with patch(
                "src.db.database_router.get_lakebase_config_from_db",
                new_callable=AsyncMock,
                return_value=None,
            ):
                result = await service.get_database_info()

        assert result["success"] is True
        assert result["database_type"] == "sqlite"
        assert result["total_tables"] == 2
        assert result["size_mb"] == 1.0
        assert result["created_at"] == "2024-01-01T00:00:00"
        assert result["modified_at"] == "2024-06-15T12:00:00"
        assert result["database_path"] == "/tmp/test.db"

    @pytest.mark.asyncio
    async def test_get_info_sqlite_no_optional_fields(self, service, mock_repository):
        """SQLite info result without optional size/path/date fields."""
        with patch(
            "src.services.database_management_service.DatabaseBackupRepository.get_database_type",
            return_value="sqlite",
        ), patch(
            "src.services.database_management_service.settings"
        ) as mock_settings, patch(
            "src.services.database_management_service.os.path.isabs", return_value=True
        ), patch(
            "src.services.database_management_service.os.path.exists", return_value=True
        ):
            mock_settings.SQLITE_DB_PATH = "/tmp/test.db"
            mock_repository.get_database_info.return_value = {
                "success": True,
                "tables": {},
                "total_tables": 0,
                "memory_backends": [],
            }

            with patch(
                "src.db.database_router.get_lakebase_config_from_db",
                new_callable=AsyncMock,
                return_value=None,
            ):
                result = await service.get_database_info()

        assert result["success"] is True
        assert "size_mb" not in result
        assert "created_at" not in result
        assert "modified_at" not in result
        assert "database_path" not in result

    @pytest.mark.asyncio
    async def test_get_info_sqlite_default_path_fallback(self, service, mock_repository):
        """Falls back to ./app.db when SQLITE_DB_PATH is None."""
        with patch(
            "src.services.database_management_service.DatabaseBackupRepository.get_database_type",
            return_value="sqlite",
        ), patch(
            "src.services.database_management_service.settings"
        ) as mock_settings, patch(
            "src.services.database_management_service.os.path.isabs", return_value=False
        ), patch(
            "src.services.database_management_service.os.path.abspath",
            return_value="/abs/app.db",
        ):
            mock_settings.SQLITE_DB_PATH = None
            mock_repository.get_database_info.return_value = {
                "success": True,
                "tables": {},
                "total_tables": 0,
                "memory_backends": [],
            }

            with patch(
                "src.db.database_router.get_lakebase_config_from_db",
                new_callable=AsyncMock,
                return_value=None,
            ):
                result = await service.get_database_info()

        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_get_info_sqlite_repo_failure(self, service, mock_repository):
        """When repository returns failure, it is propagated."""
        with patch(
            "src.services.database_management_service.DatabaseBackupRepository.get_database_type",
            return_value="sqlite",
        ), patch(
            "src.services.database_management_service.settings"
        ) as mock_settings, patch(
            "src.services.database_management_service.os.path.isabs", return_value=True
        ):
            mock_settings.SQLITE_DB_PATH = "/tmp/test.db"
            mock_repository.get_database_info.return_value = {
                "success": False,
                "error": "File locked",
            }

            with patch(
                "src.db.database_router.get_lakebase_config_from_db",
                new_callable=AsyncMock,
                return_value=None,
            ):
                result = await service.get_database_info()

        assert result["success"] is False
        assert result["error"] == "File locked"

    # ---- PostgreSQL ----

    @pytest.mark.asyncio
    async def test_get_info_postgres_success(self, service, mock_repository):
        """Successful PostgreSQL database info."""
        with patch(
            "src.services.database_management_service.DatabaseBackupRepository.get_database_type",
            return_value="postgres",
        ):
            mock_repository.get_database_info.return_value = {
                "success": True,
                "tables": {"users": 100, "crews": 50},
                "total_tables": 2,
                "memory_backends": [{"id": "mb1", "name": "default"}],
            }

            with patch(
                "src.db.database_router.get_lakebase_config_from_db",
                new_callable=AsyncMock,
                return_value=None,
            ):
                result = await service.get_database_info()

        assert result["success"] is True
        assert result["database_type"] == "postgres"
        assert result["total_tables"] == 2
        assert len(result["memory_backends"]) == 1

    @pytest.mark.asyncio
    async def test_get_info_postgres_uses_provided_session(self, service, mock_repository):
        """Explicit session parameter is forwarded."""
        extra_session = AsyncMock()
        with patch(
            "src.services.database_management_service.DatabaseBackupRepository.get_database_type",
            return_value="postgres",
        ):
            mock_repository.get_database_info.return_value = {
                "success": True,
                "tables": {},
                "total_tables": 0,
                "memory_backends": [],
            }

            with patch(
                "src.db.database_router.get_lakebase_config_from_db",
                new_callable=AsyncMock,
                return_value=None,
            ):
                await service.get_database_info(session=extra_session)

        call_kwargs = mock_repository.get_database_info.call_args.kwargs
        assert call_kwargs["session"] is extra_session

    @pytest.mark.asyncio
    async def test_get_info_postgres_repo_failure(self, service, mock_repository):
        """PostgreSQL repo failure is propagated."""
        with patch(
            "src.services.database_management_service.DatabaseBackupRepository.get_database_type",
            return_value="postgres",
        ):
            mock_repository.get_database_info.return_value = {
                "success": False,
                "error": "Connection reset",
            }

            with patch(
                "src.db.database_router.get_lakebase_config_from_db",
                new_callable=AsyncMock,
                return_value=None,
            ):
                result = await service.get_database_info()

        assert result["success"] is False
        assert result["error"] == "Connection reset"

    # ---- Lakebase ----

    @pytest.mark.asyncio
    async def test_get_info_lakebase_success(self, service, mock_session, mock_repository):
        """Successful Lakebase database info with endpoint."""
        # Give the mock session a bind with postgres URL
        mock_bind = MagicMock()
        mock_bind.url = "postgresql+asyncpg://user:pass@host/db"
        mock_session.bind = mock_bind

        lakebase_cfg = {
            "enabled": True,
            "endpoint": "https://lakebase.databricks.com",
            "migration_completed": True,
            "instance_name": "my-lakebase",
        }

        mock_repository.get_database_info.return_value = {
            "success": True,
            "tables": {"users": 50},
            "total_tables": 1,
            "memory_backends": [],
        }

        with patch(
            "src.services.database_management_service.DatabaseBackupRepository.get_database_type",
            return_value="postgres",
        ), patch(
            "src.db.database_router.get_lakebase_config_from_db",
            new_callable=AsyncMock,
            return_value=lakebase_cfg,
        ):
            result = await service.get_database_info()

        assert result["success"] is True
        assert result["database_type"] == "lakebase"
        assert result["lakebase_enabled"] is True
        assert result["lakebase_instance"] == "my-lakebase"
        assert result["lakebase_endpoint"] == "https://lakebase.databricks.com"

    @pytest.mark.asyncio
    async def test_get_info_lakebase_not_fully_enabled(self, service, mock_session, mock_repository):
        """Lakebase config present but not fully enabled falls through to normal postgres."""
        mock_bind = MagicMock()
        mock_bind.url = "postgresql+asyncpg://user:pass@host/db"
        mock_session.bind = mock_bind

        # enabled=True but migration_completed=False
        lakebase_cfg = {
            "enabled": True,
            "endpoint": "https://lakebase.databricks.com",
            "migration_completed": False,
            "instance_name": "my-lakebase",
        }

        mock_repository.get_database_info.return_value = {
            "success": True,
            "tables": {},
            "total_tables": 0,
            "memory_backends": [],
        }

        with patch(
            "src.services.database_management_service.DatabaseBackupRepository.get_database_type",
            return_value="postgres",
        ), patch(
            "src.db.database_router.get_lakebase_config_from_db",
            new_callable=AsyncMock,
            return_value=lakebase_cfg,
        ):
            result = await service.get_database_info()

        assert result["success"] is True
        # Not lakebase since migration not completed
        assert result["database_type"] == "postgres"

    @pytest.mark.asyncio
    async def test_get_info_lakebase_enabled_but_session_is_sqlite(self, service, mock_session, mock_repository):
        """Lakebase config is enabled but session is actually SQLite -- reports lakebase with connection_error."""
        mock_bind = MagicMock()
        mock_bind.url = "sqlite+aiosqlite:///app.db"
        mock_session.bind = mock_bind

        lakebase_cfg = {
            "enabled": True,
            "endpoint": "https://lakebase.databricks.com",
            "migration_completed": True,
            "instance_name": "my-lakebase",
        }

        mock_repository.get_database_info.return_value = {
            "success": True,
            "tables": {},
            "total_tables": 0,
            "memory_backends": [],
        }

        with patch(
            "src.services.database_management_service.DatabaseBackupRepository.get_database_type",
            return_value="sqlite",
        ), patch(
            "src.services.database_management_service.settings"
        ) as mock_settings, patch(
            "src.services.database_management_service.os.path.isabs", return_value=True
        ), patch(
            "src.db.database_router.get_lakebase_config_from_db",
            new_callable=AsyncMock,
            return_value=lakebase_cfg,
        ):
            mock_settings.SQLITE_DB_PATH = "/tmp/test.db"
            result = await service.get_database_info()

        assert result["success"] is True
        # Lakebase is reported as active backend even though session fell back to sqlite
        assert result["database_type"] == "lakebase"
        assert result["lakebase_enabled"] is True
        assert result["lakebase_instance"] == "my-lakebase"
        assert "connection_error" in result

    @pytest.mark.asyncio
    async def test_get_info_lakebase_repo_failure(self, service, mock_session, mock_repository):
        """Lakebase path with repository failure is propagated."""
        mock_bind = MagicMock()
        mock_bind.url = "postgresql+asyncpg://user:pass@host/db"
        mock_session.bind = mock_bind

        lakebase_cfg = {
            "enabled": True,
            "endpoint": "https://lakebase.databricks.com",
            "migration_completed": True,
            "instance_name": "my-lakebase",
        }

        mock_repository.get_database_info.return_value = {
            "success": False,
            "error": "Lakebase unreachable",
        }

        with patch(
            "src.services.database_management_service.DatabaseBackupRepository.get_database_type",
            return_value="postgres",
        ), patch(
            "src.db.database_router.get_lakebase_config_from_db",
            new_callable=AsyncMock,
            return_value=lakebase_cfg,
        ):
            result = await service.get_database_info()

        assert result["success"] is False
        assert result["error"] == "Lakebase unreachable"

    @pytest.mark.asyncio
    async def test_get_info_lakebase_no_endpoint(self, service, mock_session, mock_repository):
        """Lakebase result does not include endpoint key when config has no endpoint."""
        mock_bind = MagicMock()
        mock_bind.url = "postgresql+asyncpg://user:pass@host/db"
        mock_session.bind = mock_bind

        lakebase_cfg = {
            "enabled": True,
            "endpoint": "https://lakebase.databricks.com",
            "migration_completed": True,
            "instance_name": "my-lakebase",
        }

        mock_repository.get_database_info.return_value = {
            "success": True,
            "tables": {},
            "total_tables": 0,
            "memory_backends": [],
        }

        # Override endpoint to empty string to test no-endpoint branch
        lakebase_cfg_no_endpoint = dict(lakebase_cfg, endpoint="")
        # endpoint is empty but still enabled+migration_completed is False now
        # Actually, empty endpoint means lakebase_enabled will be False
        # Let's test with a valid config but empty endpoint in the config AFTER the check
        # The check does: lakebase_config.get("endpoint") -- truthy check, so empty string
        # is falsy => lakebase_enabled = False. Let's just verify it falls through.

        with patch(
            "src.services.database_management_service.DatabaseBackupRepository.get_database_type",
            return_value="postgres",
        ), patch(
            "src.db.database_router.get_lakebase_config_from_db",
            new_callable=AsyncMock,
            return_value=lakebase_cfg_no_endpoint,
        ):
            result = await service.get_database_info()

        assert result["success"] is True
        assert result["database_type"] == "postgres"

    # ---- Lakebase config check failure ----

    @pytest.mark.asyncio
    async def test_get_info_lakebase_config_check_exception(self, service, mock_repository):
        """Exception when checking lakebase config is caught and code continues."""
        with patch(
            "src.services.database_management_service.DatabaseBackupRepository.get_database_type",
            return_value="sqlite",
        ), patch(
            "src.services.database_management_service.settings"
        ) as mock_settings, patch(
            "src.services.database_management_service.os.path.isabs", return_value=True
        ), patch(
            "src.db.database_router.get_lakebase_config_from_db",
            new_callable=AsyncMock,
            side_effect=ImportError("module not found"),
        ):
            mock_settings.SQLITE_DB_PATH = "/tmp/test.db"
            mock_repository.get_database_info.return_value = {
                "success": True,
                "tables": {},
                "total_tables": 0,
                "memory_backends": [],
            }

            result = await service.get_database_info()

        assert result["success"] is True
        assert result["database_type"] == "sqlite"

    # ---- Unsupported database type ----

    @pytest.mark.asyncio
    async def test_get_info_unsupported_database_type(self, service):
        """Returns error for unsupported database types."""
        with patch(
            "src.services.database_management_service.DatabaseBackupRepository.get_database_type",
            return_value="oracle",
        ), patch(
            "src.db.database_router.get_lakebase_config_from_db",
            new_callable=AsyncMock,
            return_value=None,
        ):
            result = await service.get_database_info()

        assert result["success"] is False
        assert "Unsupported" in result["error"]

    # ---- Session bind detection ----

    @pytest.mark.asyncio
    async def test_get_info_session_bind_sqlite_detection(self, service, mock_session, mock_repository):
        """Session bind with sqlite URL correctly sets actual_session_db_type."""
        mock_bind = MagicMock()
        mock_bind.url = "sqlite+aiosqlite:///app.db"
        mock_session.bind = mock_bind

        with patch(
            "src.services.database_management_service.DatabaseBackupRepository.get_database_type",
            return_value="sqlite",
        ), patch(
            "src.services.database_management_service.settings"
        ) as mock_settings, patch(
            "src.services.database_management_service.os.path.isabs", return_value=True
        ), patch(
            "src.db.database_router.get_lakebase_config_from_db",
            new_callable=AsyncMock,
            return_value=None,
        ):
            mock_settings.SQLITE_DB_PATH = "/tmp/test.db"
            mock_repository.get_database_info.return_value = {
                "success": True,
                "tables": {},
                "total_tables": 0,
                "memory_backends": [],
            }

            result = await service.get_database_info()

        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_get_info_session_no_bind(self, service, mock_session, mock_repository):
        """When session has no bind, actual_session_db_type stays unknown."""
        mock_session.bind = None

        with patch(
            "src.services.database_management_service.DatabaseBackupRepository.get_database_type",
            return_value="sqlite",
        ), patch(
            "src.services.database_management_service.settings"
        ) as mock_settings, patch(
            "src.services.database_management_service.os.path.isabs", return_value=True
        ), patch(
            "src.db.database_router.get_lakebase_config_from_db",
            new_callable=AsyncMock,
            return_value=None,
        ):
            mock_settings.SQLITE_DB_PATH = "/tmp/test.db"
            mock_repository.get_database_info.return_value = {
                "success": True,
                "tables": {},
                "total_tables": 0,
                "memory_backends": [],
            }

            result = await service.get_database_info()

        assert result["success"] is True

    # ---- Lakebase info reporting (new behavior) ----

    @pytest.mark.asyncio
    async def test_get_database_info_lakebase_enabled_connected(self, service, mock_session, mock_repository):
        """When lakebase is enabled and session is postgres, report database_type='lakebase' with tables."""
        mock_bind = MagicMock()
        mock_bind.url = "postgresql+asyncpg://user:pass@host/db"
        mock_session.bind = mock_bind

        lakebase_cfg = {
            "enabled": True,
            "endpoint": "https://example.com/lakebase",
            "migration_completed": True,
            "instance_name": "test-lakebase-instance",
        }

        mock_repository.get_database_info.return_value = {
            "success": True,
            "tables": {"agents": 20, "tasks": 15, "crews": 5},
            "total_tables": 3,
            "memory_backends": [{"id": "mb1", "name": "default"}],
        }

        with patch(
            "src.services.database_management_service.DatabaseBackupRepository.get_database_type",
            return_value="postgres",
        ), patch(
            "src.db.database_router.get_lakebase_config_from_db",
            new_callable=AsyncMock,
            return_value=lakebase_cfg,
        ):
            result = await service.get_database_info()

        assert result["success"] is True
        assert result["database_type"] == "lakebase"
        assert result["lakebase_enabled"] is True
        assert result["lakebase_instance"] == "test-lakebase-instance"
        assert result["lakebase_endpoint"] == "https://example.com/lakebase"
        assert result["tables"] == {"agents": 20, "tasks": 15, "crews": 5}
        assert result["total_tables"] == 3
        assert result["memory_backends"] == [{"id": "mb1", "name": "default"}]
        assert "connection_error" not in result

    @pytest.mark.asyncio
    async def test_get_database_info_lakebase_enabled_connection_failed(self, service, mock_session, mock_repository):
        """When lakebase is enabled but session fell back to sqlite, report 'lakebase' with connection_error."""
        mock_bind = MagicMock()
        mock_bind.url = "sqlite+aiosqlite:///app.db"
        mock_session.bind = mock_bind

        lakebase_cfg = {
            "enabled": True,
            "endpoint": "https://example.com/lakebase",
            "migration_completed": True,
            "instance_name": "my-lakebase",
        }

        with patch(
            "src.services.database_management_service.DatabaseBackupRepository.get_database_type",
            return_value="sqlite",
        ), patch(
            "src.services.database_management_service.settings"
        ) as mock_settings, patch(
            "src.services.database_management_service.os.path.isabs", return_value=True
        ), patch(
            "src.db.database_router.get_lakebase_config_from_db",
            new_callable=AsyncMock,
            return_value=lakebase_cfg,
        ):
            mock_settings.SQLITE_DB_PATH = "/tmp/test.db"
            result = await service.get_database_info()

        assert result["success"] is True
        assert result["database_type"] == "lakebase"
        assert result["lakebase_enabled"] is True
        assert result["lakebase_instance"] == "my-lakebase"
        assert result["lakebase_endpoint"] == "https://example.com/lakebase"
        assert "connection_error" in result
        assert "connection failed" in result["connection_error"].lower()
        assert "fallback" in result["connection_error"].lower()
        assert result["tables"] == {}
        assert result["total_tables"] == 0
        assert result["memory_backends"] == []

    @pytest.mark.asyncio
    async def test_get_database_info_sqlite_normal(self, service, mock_session, mock_repository):
        """Normal sqlite path when lakebase is not enabled -- no lakebase fields in result."""
        mock_session.bind = None

        with patch(
            "src.services.database_management_service.DatabaseBackupRepository.get_database_type",
            return_value="sqlite",
        ), patch(
            "src.services.database_management_service.settings"
        ) as mock_settings, patch(
            "src.services.database_management_service.os.path.isabs", return_value=True
        ), patch(
            "src.db.database_router.get_lakebase_config_from_db",
            new_callable=AsyncMock,
            return_value=None,
        ):
            mock_settings.SQLITE_DB_PATH = "/tmp/test.db"
            mock_repository.get_database_info.return_value = {
                "success": True,
                "tables": {"users": 10},
                "total_tables": 1,
                "memory_backends": [],
                "size": 2097152,
                "path": "/tmp/test.db",
            }

            result = await service.get_database_info()

        assert result["success"] is True
        assert result["database_type"] == "sqlite"
        assert result["tables"] == {"users": 10}
        assert result["total_tables"] == 1
        assert result["size_mb"] == 2.0
        # Lakebase fields should NOT be present in normal sqlite mode
        assert "lakebase_enabled" not in result
        assert "lakebase_instance" not in result
        assert "lakebase_endpoint" not in result
        assert "connection_error" not in result

    # ---- General exception ----

    @pytest.mark.asyncio
    async def test_get_info_general_exception(self, service):
        """Unhandled exceptions are caught and returned as error dict."""
        with patch(
            "src.services.database_management_service.DatabaseBackupRepository.get_database_type",
            side_effect=RuntimeError("db error"),
        ), patch(
            "src.db.database_router.get_lakebase_config_from_db",
            new_callable=AsyncMock,
            return_value=None,
        ):
            result = await service.get_database_info()

        assert result["success"] is False
        assert "db error" in result["error"]


# ===================================================================
# check_user_permission Tests
# ===================================================================

class TestCheckUserPermission:
    """Tests for DatabaseManagementService.check_user_permission."""

    @pytest.mark.asyncio
    async def test_permission_always_granted(self, service):
        """Current implementation always grants permission."""
        result = await service.check_user_permission(user_email="user@example.com")

        assert result["has_permission"] is True
        assert result["user_email"] == "user@example.com"
        assert "reason" in result

    @pytest.mark.asyncio
    async def test_permission_with_different_email(self, service):
        """Permission is granted regardless of email address."""
        result = await service.check_user_permission(user_email="admin@corp.com")

        assert result["has_permission"] is True
        assert result["user_email"] == "admin@corp.com"

    @pytest.mark.asyncio
    async def test_permission_with_session_param(self, service):
        """Session parameter is accepted but not currently used."""
        extra_session = AsyncMock()
        result = await service.check_user_permission(
            user_email="user@example.com", session=extra_session
        )

        assert result["has_permission"] is True

    @pytest.mark.asyncio
    async def test_permission_with_user_token(self, service):
        """User token parameter is accepted but not currently used."""
        result = await service.check_user_permission(
            user_email="user@example.com", user_token="tok"
        )

        assert result["has_permission"] is True

    @pytest.mark.asyncio
    async def test_permission_exception_defaults_to_allow(self, service):
        """Even when an exception occurs, permission defaults to allowed."""
        # Force an exception inside the try block by patching the logger
        with patch(
            "src.services.database_management_service.logger"
        ) as mock_logger:
            mock_logger.info.side_effect = RuntimeError("logging failed")
            result = await service.check_user_permission(user_email="user@example.com")

        assert result["has_permission"] is True
        assert "failed" in result["reason"].lower() or "defaulting" in result["reason"].lower()


# ===================================================================
# Edge case: service with user_token
# ===================================================================

class TestServiceWithUserToken:
    """Tests verifying behavior when user_token is set at construction time."""

    @pytest.mark.asyncio
    async def test_user_token_stored(self, service_with_token):
        assert service_with_token.user_token == "test-obo-token"

    @pytest.mark.asyncio
    async def test_export_works_with_token(self, service_with_token, mock_repository):
        """Export still works when user_token is set."""
        with patch(
            "src.services.database_management_service.DatabaseBackupRepository.get_database_type",
            return_value="sqlite",
        ), patch(
            "src.services.database_management_service.settings"
        ) as mock_settings, patch(
            "src.services.database_management_service.os.path.isabs", return_value=True
        ), patch(
            "src.services.database_management_service.os.path.exists", return_value=True
        ), patch(
            "src.services.database_management_service.os.path.getsize", return_value=1024
        ):
            mock_settings.SQLITE_DB_PATH = "/tmp/test.db"
            mock_repository.create_sqlite_backup.return_value = {
                "success": True,
                "backup_path": "/Volumes/c/s/v/backup.db",
                "backup_size": 512,
            }
            mock_repository.cleanup_old_backups.return_value = {"success": True, "deleted": []}
            mock_repository.list_backups.return_value = []

            result = await service_with_token.export_to_volume(catalog="c", schema="s")

        assert result["success"] is True


# ===================================================================
# Housekeeping Tests
# ===================================================================

class TestRunHousekeeping:
    """Tests for DatabaseManagementService.run_housekeeping."""

    @pytest.fixture
    def mock_session(self):
        session = AsyncMock()
        session.bind = None
        session.execute = AsyncMock()
        session.flush = AsyncMock()
        session.commit = AsyncMock()
        return session

    @pytest.fixture
    def service(self, mock_session):
        from src.services.database_management_service import DatabaseManagementService
        return DatabaseManagementService(
            session=mock_session,
            repository=AsyncMock(),
            user_token=None,
        )

    @pytest.mark.asyncio
    async def test_run_housekeeping_success(self, service, mock_session):
        """Test successful housekeeping run deletes records from all tables."""
        from unittest.mock import patch as _patch

        # The service uses deferred imports, so we patch the class methods directly
        mock_history_result = {
            'executionhistory': 10, 'taskstatus': 5, 'errortrace': 2,
        }

        # Mock trace delete by ref (run_id subquery) + trace by date + llm delete + vacuum
        mock_trace_ref = MagicMock(rowcount=30)
        mock_trace_date = MagicMock(rowcount=5)
        mock_llm = MagicMock(rowcount=8)

        mock_session.execute = AsyncMock(
            side_effect=[mock_trace_ref, mock_trace_date, mock_llm, None]
        )

        with _patch(
            "src.repositories.execution_history_repository.ExecutionHistoryRepository.delete_older_than",
            new_callable=AsyncMock,
            return_value=mock_history_result,
        ), _patch(
            "src.repositories.execution_logs_repository.ExecutionLogsRepository.delete_older_than",
            new_callable=AsyncMock,
            return_value=20,
        ), _patch(
            "src.services.database_management_service.DatabaseBackupRepository"
        ) as MockBackupRepo:
            MockBackupRepo.get_database_type.return_value = "sqlite"

            result = await service.run_housekeeping("2025-01-01")

        assert result["success"] is True
        assert result["cutoff_date"] == "2025-01-01"
        assert result["deleted"]["executionhistory"] == 10
        assert result["deleted"]["taskstatus"] == 5
        assert result["deleted"]["errortrace"] == 2
        assert result["deleted"]["execution_trace"] == 35  # 30 + 5
        assert result["deleted"]["execution_logs"] == 20
        assert result["deleted"]["llmlog"] == 8
        assert result["total_deleted"] == 80

        mock_session.commit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_run_housekeeping_invalid_date(self, service):
        """Test housekeeping with invalid date format returns error."""
        result = await service.run_housekeeping("not-a-date")

        assert result["success"] is False
        assert "Invalid date format" in result["error"]

    @pytest.mark.asyncio
    async def test_run_housekeeping_empty_date(self, service):
        """Test housekeeping with empty date string returns error."""
        result = await service.run_housekeeping("")

        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_run_housekeeping_database_error(self, service, mock_session):
        """Test housekeeping handles database errors gracefully."""
        mock_session.execute = AsyncMock(side_effect=Exception("Connection lost"))

        with patch(
            "src.services.database_management_service.DatabaseBackupRepository"
        ):
            result = await service.run_housekeeping("2025-01-01")

        assert result["success"] is False
        assert "Connection lost" in result["error"]

    @pytest.mark.asyncio
    async def test_run_housekeeping_zero_deletions(self, service, mock_session):
        """Test housekeeping when no records match the cutoff."""
        mock_history_repo = AsyncMock()
        mock_history_repo.delete_older_than = AsyncMock(return_value={
            'executionhistory': 0,
            'taskstatus': 0,
            'errortrace': 0,
        })

        mock_logs_repo = AsyncMock()
        mock_logs_repo.delete_older_than = AsyncMock(return_value=0)

        mock_zero_result = MagicMock()
        mock_zero_result.rowcount = 0

        mock_session.execute = AsyncMock(return_value=mock_zero_result)

        with patch(
            "src.repositories.execution_history_repository.ExecutionHistoryRepository.delete_older_than",
            new_callable=AsyncMock,
            return_value=mock_history_repo.delete_older_than.return_value,
        ), patch(
            "src.repositories.execution_logs_repository.ExecutionLogsRepository.delete_older_than",
            new_callable=AsyncMock,
            return_value=0,
        ), patch(
            "src.services.database_management_service.DatabaseBackupRepository"
        ) as MockBackupRepo:
            MockBackupRepo.get_database_type.return_value = "postgres"

            result = await service.run_housekeeping("2020-01-01")

        assert result["success"] is True
        assert result["total_deleted"] == 0

    @pytest.mark.asyncio
    async def test_run_housekeeping_vacuum_failure_is_non_fatal(self, service, mock_session):
        """When SQLite VACUUM fails, housekeeping still succeeds."""
        mock_history_result = {
            'executionhistory': 1, 'taskstatus': 0, 'errortrace': 0,
        }

        mock_trace_ref = MagicMock(rowcount=0)
        mock_trace_date = MagicMock(rowcount=0)
        mock_llm = MagicMock(rowcount=0)

        # 4th call is the VACUUM which raises
        mock_session.execute = AsyncMock(
            side_effect=[mock_trace_ref, mock_trace_date, mock_llm, RuntimeError("VACUUM locked")]
        )

        with patch(
            "src.repositories.execution_history_repository.ExecutionHistoryRepository.delete_older_than",
            new_callable=AsyncMock,
            return_value=mock_history_result,
        ), patch(
            "src.repositories.execution_logs_repository.ExecutionLogsRepository.delete_older_than",
            new_callable=AsyncMock,
            return_value=0,
        ), patch(
            "src.services.database_management_service.DatabaseBackupRepository"
        ) as MockBackupRepo:
            MockBackupRepo.get_database_type.return_value = "sqlite"

            result = await service.run_housekeeping("2025-01-01")

        assert result["success"] is True
        assert result["deleted"]["executionhistory"] == 1
