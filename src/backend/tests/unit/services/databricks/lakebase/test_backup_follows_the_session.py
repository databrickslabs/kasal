"""Export, import and housekeeping act on the database the session is connected to.

Inside Databricks Apps ``settings.DATABASE_URI`` names the boot SQLite file
(``entrypoint.py`` defaults to SQLite) while every request runs on the Lakebase
resource. Choosing the backup branch from that URI made Export look for, or
upload, the unused boot file; Import write into it and report success; and
Housekeeping send ``PRAGMA defer_foreign_keys`` to PostgreSQL, so nothing was
ever purged. These tests pin the choice to the SESSION's dialect, and pin a
clear refusal where in-app backup cannot capture the data (Lakebase keeps its
tables in schema ``kasal``; the backup reads ``public``).
"""

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.core.exceptions import BadRequestError
from src.repositories.database_backup_repository import DatabaseBackupRepository
from src.services.databricks.lakebase.management import DatabaseManagementService

_BOOT_FILE = "/app/kasal.db"
_MGMT = "src.services.databricks.lakebase.management"


def _session(dialect: str, schema: str = "public") -> MagicMock:
    """A session bound to ``dialect`` whose current_schema() is ``schema``."""
    session = MagicMock()
    session.get_bind.return_value.dialect.name = dialect
    result = MagicMock()
    result.scalar.return_value = schema
    result.rowcount = 0
    session.execute = AsyncMock(return_value=result)
    session.flush = AsyncMock()
    session.commit = AsyncMock()
    session.connection = AsyncMock()
    return session


def _repository() -> AsyncMock:
    repo = AsyncMock()
    repo.create_postgres_backup = AsyncMock(
        return_value={
            "success": True,
            "backup_path": "/Volumes/c/s/v/x.sql",
            "backup_size": 10,
        }
    )
    repo.restore_postgres_backup = AsyncMock(return_value={"success": True})
    repo.list_backups = AsyncMock(return_value=[])
    repo.cleanup_old_backups = AsyncMock(return_value={"success": True, "deleted": []})

    async def _current_schema(session=None):
        # The real query, so the answer comes from the session under test.
        return await DatabaseBackupRepository.get_current_schema(repo, session)

    repo.get_current_schema = AsyncMock(side_effect=_current_schema)
    return repo


@pytest.fixture
def hosted_boot_settings():
    """What the process settings say inside Databricks Apps: the boot SQLite file."""
    with (
        patch(f"{_MGMT}.settings") as mgmt_settings,
        patch(f"{_MGMT}.os.path.exists", return_value=True) as exists,
        patch(f"{_MGMT}.os.path.getsize", return_value=0),
    ):
        mgmt_settings.DATABASE_URI = f"sqlite+aiosqlite:///{_BOOT_FILE}"
        mgmt_settings.SQLITE_DB_PATH = _BOOT_FILE
        yield exists


def _never_touched_the_boot_file(repo: AsyncMock, exists: MagicMock) -> None:
    repo.create_sqlite_backup.assert_not_called()
    repo.restore_sqlite_backup.assert_not_called()
    assert all(_BOOT_FILE not in str(c) for c in exists.call_args_list)


class TestTheDatabaseTypeComesFromTheSession:
    def test_a_postgres_session_is_postgres_whatever_the_settings_say(self):
        from src.config.settings import settings

        # The test environment, like a Databricks App, configures SQLite.
        assert str(settings.DATABASE_URI).startswith("sqlite")
        assert (
            DatabaseBackupRepository.get_database_type(_session("postgresql"))
            == "postgres"
        )

    def test_a_sqlite_session_is_sqlite(self):
        assert (
            DatabaseBackupRepository.get_database_type(_session("sqlite")) == "sqlite"
        )

    def test_anything_else_is_unknown_not_a_guess(self):
        assert (
            DatabaseBackupRepository.get_database_type(_session("mysql")) == "unknown"
        )


@pytest.mark.asyncio
class TestExport:
    async def test_lakebase_is_refused_clearly_and_never_reads_the_boot_file(
        self, hosted_boot_settings
    ):
        repo = _repository()
        service = DatabaseManagementService(_session("postgresql", "kasal"), repo)

        with pytest.raises(BadRequestError) as refused:
            await service.export_to_volume(catalog="c", schema="s")

        assert "Lakebase" in str(refused.value) and "point-in-time" in str(
            refused.value
        )
        repo.create_postgres_backup.assert_not_called()
        _never_touched_the_boot_file(repo, hosted_boot_settings)

    async def test_plain_postgres_backs_up_the_session_not_the_boot_file(
        self, hosted_boot_settings
    ):
        repo = _repository()
        session = _session("postgresql", "public")
        service = DatabaseManagementService(session, repo)

        with patch(
            "src.utils.databricks_auth.get_auth_context", AsyncMock(return_value=None)
        ):
            result = await service.export_to_volume(catalog="c", schema="s")

        assert result["success"] is True and result["database_type"] == "postgres"
        assert repo.create_postgres_backup.await_args.kwargs["session"] is session
        _never_touched_the_boot_file(repo, hosted_boot_settings)

    async def test_an_unsupported_dialect_is_refused_by_name(
        self, hosted_boot_settings
    ):
        repo = _repository()
        service = DatabaseManagementService(_session("mysql"), repo)

        with pytest.raises(BadRequestError, match="mysql"):
            await service.export_to_volume(catalog="c", schema="s")
        _never_touched_the_boot_file(repo, hosted_boot_settings)


@pytest.mark.asyncio
class TestImport:
    async def test_lakebase_is_refused_clearly_and_never_writes_the_boot_file(
        self, hosted_boot_settings
    ):
        repo = _repository()
        service = DatabaseManagementService(_session("postgresql", "kasal"), repo)

        with pytest.raises(BadRequestError, match="Lakebase"):
            await service.import_from_volume("c", "s", "v", "kasal_backup_1.db")

        repo.restore_postgres_backup.assert_not_called()
        _never_touched_the_boot_file(repo, hosted_boot_settings)

    async def test_plain_postgres_restores_into_the_session(self, hosted_boot_settings):
        repo = _repository()
        session = _session("postgresql", "public")
        service = DatabaseManagementService(session, repo)

        result = await service.import_from_volume("c", "s", "v", "kasal_backup_1.sql")

        assert result["success"] is True
        assert repo.restore_postgres_backup.await_args.kwargs["session"] is session
        _never_touched_the_boot_file(repo, hosted_boot_settings)


@pytest.mark.asyncio
class TestHousekeeping:
    @pytest.mark.parametrize(
        "dialect,sqlite_only_sql", [("postgresql", False), ("sqlite", True)]
    )
    async def test_sqlite_only_statements_follow_the_purge_sessions_dialect(
        self, hosted_boot_settings, dialect, sqlite_only_sql
    ):
        """The settings say SQLite in both cases; only the session decides."""
        purge_session = _session(dialect)
        executed: list[str] = []

        async def _capture(stmt, *args, **kwargs):
            executed.append(str(stmt))
            return MagicMock(rowcount=0)

        purge_session.execute = AsyncMock(side_effect=_capture)
        raw = AsyncMock()
        purge_session.connection = AsyncMock(return_value=raw)

        @asynccontextmanager
        async def _isolated():
            yield purge_session

        zero = AsyncMock(return_value=0)
        with (
            patch(
                "src.db.session.get_isolated_db_session",
                side_effect=lambda: _isolated(),
            ),
            patch(
                "src.repositories.execution_history_repository."
                "ExecutionHistoryRepository.delete_older_than",
                AsyncMock(return_value={}),
            ),
            patch(
                "src.repositories.execution_logs_repository."
                "ExecutionLogsRepository.delete_older_than",
                zero,
            ),
            patch(
                "src.repositories.billing_repository.BillingRepository.delete_older_than",
                zero,
            ),
            patch(
                "src.repositories.hitl_repository.HITLApprovalRepository.delete_older_than",
                zero,
            ),
        ):
            service = DatabaseManagementService(_session("sqlite"), _repository())
            result = await service.run_housekeeping("2025-01-01")

        assert result["success"] is True, result
        assert any("defer_foreign_keys" in s for s in executed) is sqlite_only_sql
        vacuumed = any("VACUUM" in str(c) for c in raw.exec_driver_sql.await_args_list)
        assert vacuumed is sqlite_only_sql
