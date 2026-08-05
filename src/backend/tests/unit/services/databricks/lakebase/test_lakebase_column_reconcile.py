"""Expanding a Lakebase schema reconciles COLUMNS, not just tables.

``create_tables_async`` / ``create_tables_sync_stream`` are
``CREATE TABLE IF NOT EXISTS``. They add a missing TABLE and leave an existing
one completely untouched — column list included. So on an instance that already
has the ``kasal`` schema, a column added to a model after provisioning never
lands, while both UI paths report "schema expanded" / "schema created".

That shipped. ``modelconfig.thinking_budget_tokens`` was missing on a live
Lakebase, and because every LLM call reads the model catalogue, the app answered
crew generation and ``GET /models/enabled`` with:

    column modelconfig.thinking_budget_tokens does not exist

The user's recovery attempts both failed for this reason: "Connect & Expand
Schema" created no columns, and "Schema Only" was wired to
``recreate_schema=true``, so the only path that WOULD have fixed the columns
first tried to ``DROP SCHEMA kasal CASCADE`` — i.e. the option presented as
non-destructive offered to do the most destructive thing available.

``run_schema_self_heal`` is the piece that emits ``ADD COLUMN IF NOT EXISTS``.
Both paths must call it, and neither may fail the operation if it errors.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


def _make_service():
    with (
        patch("src.services.databricks.lakebase.service.LakebaseConnectionService"),
        patch("src.services.databricks.lakebase.service.LakebaseSchemaService"),
        patch("src.services.databricks.lakebase.service.LakebasePermissionService"),
        patch("src.services.databricks.lakebase.service.DatabaseConfigRepository"),
    ):
        from src.services.databricks.lakebase.service import LakebaseService

        svc = LakebaseService(
            session=AsyncMock(), user_token="tok", user_email="user@example.com"
        )
    svc.connection_service = AsyncMock()
    svc.schema_service = MagicMock()
    svc.schema_service.create_schema_async = AsyncMock()
    svc.schema_service.set_search_path_async = AsyncMock()
    svc.schema_service.create_tables_async = AsyncMock()
    svc.permission_service = MagicMock()
    svc.get_config = AsyncMock(return_value={})
    svc.save_config = AsyncMock(return_value={"enabled": True})
    return svc


def _engine_stub():
    """An async engine whose ``begin()`` hands back a connection."""
    engine = MagicMock()
    conn = AsyncMock()
    ctx = AsyncMock()
    ctx.__aenter__ = AsyncMock(return_value=conn)
    ctx.__aexit__ = AsyncMock(return_value=False)
    engine.begin = MagicMock(return_value=ctx)
    engine.dispose = AsyncMock()
    return engine, conn


@pytest.mark.asyncio
class TestEnableWithExpandSchema:
    async def test_expand_runs_the_column_self_heal(self):
        svc = _make_service()
        engine, conn = _engine_stub()
        svc.connection_service.create_lakebase_engine_async = AsyncMock(
            return_value=engine
        )
        heal = AsyncMock()
        with patch("src.db.session.run_schema_self_heal", heal):
            result = await svc.enable_lakebase(
                "inst", "h.example.com", expand_schema=True
            )

        assert result["schema_reconcile"] == "reconciled"
        # Tables AND columns — create_tables_async alone cannot add a column.
        svc.schema_service.create_tables_async.assert_awaited_once()
        heal.assert_awaited_once()
        assert heal.await_args.args[0] is conn

    async def test_plain_connect_changes_nothing(self):
        """Without expand_schema the user asked to connect, not to alter."""
        svc = _make_service()
        heal = AsyncMock()
        with patch("src.db.session.run_schema_self_heal", heal):
            result = await svc.enable_lakebase("inst", "h.example.com")

        assert "schema_reconcile" not in result
        heal.assert_not_awaited()
        svc.schema_service.create_tables_async.assert_not_awaited()

    async def test_a_heal_failure_does_not_block_enabling(self):
        """Enabling Lakebase matters more than the reconcile; degrade, don't fail."""
        svc = _make_service()
        engine, _ = _engine_stub()
        svc.connection_service.create_lakebase_engine_async = AsyncMock(
            return_value=engine
        )
        with patch(
            "src.db.session.run_schema_self_heal",
            AsyncMock(side_effect=Exception("permission denied")),
        ):
            result = await svc.enable_lakebase(
                "inst", "h.example.com", expand_schema=True
            )

        assert result["success"] is True
        assert result["schema_reconcile"] == "skipped"

    async def test_the_engine_is_disposed(self):
        svc = _make_service()
        engine, _ = _engine_stub()
        svc.connection_service.create_lakebase_engine_async = AsyncMock(
            return_value=engine
        )
        with patch("src.db.session.run_schema_self_heal", AsyncMock()):
            await svc.enable_lakebase("inst", "h.example.com", expand_schema=True)
        engine.dispose.assert_awaited()


@pytest.mark.asyncio
class TestMigrateStreamReconcilesColumns:
    async def _run(self, svc, **kw):
        events = []
        # The stream builds a SOURCE engine from settings.DATABASE_URI. Pin it to
        # in-memory SQLite: left unpinned, this test passes alone and fails after
        # any test that points DATABASE_URI at Postgres, because it then tries a
        # real pg8000 connection ("sorry, too many clients already").
        with (
            patch("src.services.databricks.lakebase.service.LAKEBASE_AVAILABLE", True),
            patch(
                "src.services.databricks.lakebase.service.settings.DATABASE_URI",
                "sqlite+aiosqlite://",
            ),
            patch(
                "src.services.databricks.lakebase.service.create_engine", MagicMock()
            ),
        ):
            async for ev in svc.migrate_existing_data_stream(
                "inst", "h.example.com", **kw
            ):
                events.append(ev)
        return events

    def _wire_stream(self, svc):
        svc.connection_service.generate_credentials = AsyncMock(
            return_value=MagicMock(token="tok")
        )
        svc.connection_service.get_username = AsyncMock(return_value="user@example.com")
        svc.connection_service.create_lakebase_engine_sync = MagicMock()
        svc.migration_service = MagicMock()
        svc.migration_service.get_table_list_sync.return_value = ["agents"]
        svc.migration_service.get_sorted_tables.return_value = ["agents"]
        svc.schema_service.create_tables_sync_stream.return_value = iter([])
        svc.schema_service.create_schema_sync = MagicMock()
        svc.schema_service.set_search_path_async = AsyncMock()
        svc.permission_service.grant_all_permissions_sync = MagicMock()

    async def test_schema_only_adds_missing_columns(self):
        """The path the user reaches for when a column is missing must add it."""
        svc = _make_service()
        self._wire_stream(svc)
        engine, conn = _engine_stub()
        heal = AsyncMock()
        with (
            patch("src.db.session.run_schema_self_heal", heal),
            patch(
                "src.services.databricks.lakebase.service._make_async_lakebase_engine",
                return_value=engine,
            ),
        ):
            events = await self._run(svc, recreate_schema=False, migrate_data=False)

        heal.assert_awaited_once()
        assert heal.await_args.args[0] is conn
        engine.dispose.assert_awaited()
        assert any(e.get("step") == "reconcile_columns" for e in events), events

    async def test_a_heal_failure_only_warns(self):
        svc = _make_service()
        self._wire_stream(svc)
        engine, _ = _engine_stub()
        with (
            patch(
                "src.db.session.run_schema_self_heal",
                AsyncMock(side_effect=Exception("must be owner of table modelconfig")),
            ),
            patch(
                "src.services.databricks.lakebase.service._make_async_lakebase_engine",
                return_value=engine,
            ),
        ):
            events = await self._run(svc, recreate_schema=False, migrate_data=False)

        assert any(e["type"] == "warning" for e in events), events
        assert not any(e["type"] == "error" for e in events), events


class TestSchemaOnlyIsNotDestructive:
    """The UI wiring, asserted on the source.

    "Schema Only" passed ``recreateSchema=true``, which the backend turns into
    ``DROP SCHEMA kasal CASCADE``. A label reading "Create empty tables without
    migrating data" must never drop a schema, and a user reaching for it to
    recover a missing column would instead have lost the database.
    """

    def _source(self) -> str:
        import pathlib

        src_root = pathlib.Path(__file__).resolve().parents[6]  # .../src
        path = src_root / "frontend/src/components/Configuration/DatabaseManagement.tsx"
        assert path.exists(), path
        return path.read_text()

    def test_no_call_site_hardcodes_a_recreate(self):
        source = self._source()
        # migrateLakebase(recreateSchema, migrateData). A literal `true` in the
        # first slot with `false` in the second is "drop everything, then create
        # empty tables" — never what Schema Only should mean.
        assert "migrateLakebase(true, false)" not in source, (
            "'Schema Only' is wired to recreate_schema=true, which DROPs the "
            "kasal schema. Pass false — the backend's CREATE ... IF NOT EXISTS "
            "plus the column self-heal already handle an existing schema."
        )

    def test_the_reset_path_still_exists(self):
        """Recreate-and-migrate is a legitimate choice; it must stay available."""
        assert "migrateLakebase(true, true)" in self._source()
