"""Unit tests for DatabaseManagementService.list_backups.

Split out of ``test_database_management_service.py``: covers the happy path,
the empty listing, the awaited workspace-URL lookup (and its placeholder
fallback) and the repository-error path.
"""

from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

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
    from src.services.databricks.lakebase.management import DatabaseManagementService

    return DatabaseManagementService(
        session=mock_session,
        repository=mock_repository,
        user_token=None,
    )


class TestListBackups:
    """Tests for DatabaseManagementService.list_backups."""

    @pytest.fixture(autouse=True)
    def _no_workspace_auth(self):
        """``list_backups`` awaits the real auth lookup; keep it off the network.

        Tests that need a workspace URL patch it again inside the test.
        """
        with patch(
            "src.utils.databricks_auth.get_auth_context",
            new_callable=AsyncMock,
            return_value=None,
        ):
            yield

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

        with patch(
            "src.utils.databricks_auth.get_auth_context",
            new_callable=AsyncMock,
            side_effect=RuntimeError("no auth"),
        ):
            result = await service.list_backups(
                catalog="c", schema="s", volume_name="v"
            )

        assert result["success"] is True
        assert "your-workspace" in result["backups"][0]["databricks_url"]

    @pytest.mark.asyncio
    async def test_list_backups_with_workspace_url_from_auth(
        self, service, mock_repository
    ):
        """The awaited get_auth_context() workspace URL is used in backup links.

        This used to call ``asyncio.run`` inside the running loop, which always
        raised, so every link got the placeholder; the test patched
        ``asyncio.run`` and so pinned the bug instead of catching it.
        """
        mock_repository.list_backups.return_value = [
            {
                "filename": "backup.db",
                "size": 1024,
                "created_at": datetime(2024, 6, 1),
                "backup_type": "sqlite",
            }
        ]

        mock_auth = SimpleNamespace(workspace_url="https://example.com/")
        with patch(
            "src.utils.databricks_auth.get_auth_context",
            new_callable=AsyncMock,
            return_value=mock_auth,
        ) as mock_get_auth:
            result = await service.list_backups(
                catalog="c", schema="s", volume_name="v"
            )

        mock_get_auth.assert_awaited_once()
        assert result["success"] is True
        assert result["backups"][0]["databricks_url"] == (
            "https://example.com/explore/data/volumes/c/s/v/backup.db"
        )

    @pytest.mark.asyncio
    async def test_list_backups_exception(self, service, mock_repository):
        """Repository exception is caught and returned as error."""
        mock_repository.list_backups.side_effect = RuntimeError("connection lost")

        result = await service.list_backups(catalog="c", schema="s", volume_name="v")

        assert result["success"] is False
        assert "connection lost" in result["error"]
