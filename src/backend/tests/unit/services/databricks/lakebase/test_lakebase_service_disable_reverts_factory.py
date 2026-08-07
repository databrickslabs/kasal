"""Disabling Lakebase through the service must revert this process, not just the row.

``save_config(enabled=False)`` used to only delete the ``lakebase`` config row.
That is enough for ROUTED sessions — ``get_smart_db_session`` and
``routed_scoped_session`` both re-read ``is_lakebase_enabled()`` per call — but
not for the raw ``async_session_factory``, a process global that ``main.py``'s
lifespan hot-swaps to Lakebase and that nothing ever swapped back.

So after "disable", ``utils/databricks_auth`` (and through it
``routed_scoped_session``'s reentrant ``_RESOLVING_AUTH`` branch) kept reading
Lakebase until the backend was restarted. From the outside that looked like the
toggle had simply not worked.
"""

from unittest.mock import AsyncMock, patch

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
    svc.config_repository = AsyncMock()
    return svc


class TestDisableRevertsTheProcess:
    @pytest.mark.asyncio
    async def test_disable_deactivates_the_global_factory(self):
        svc = _make_service()
        with patch(
            "src.db.database_router.deactivate_lakebase_in_process", new=AsyncMock()
        ) as deactivate:
            result = await svc.save_config({"enabled": False})

        deactivate.assert_awaited_once()
        assert result["enabled"] is False

    @pytest.mark.asyncio
    async def test_the_config_row_is_still_deleted(self):
        """The original behaviour must survive — the revert is additive."""
        svc = _make_service()
        with patch(
            "src.db.database_router.deactivate_lakebase_in_process", new=AsyncMock()
        ):
            await svc.save_config({"enabled": False})

        svc.config_repository.delete_by_key.assert_awaited_once_with("lakebase")

    @pytest.mark.asyncio
    async def test_enabling_does_not_deactivate(self):
        svc = _make_service()
        with patch(
            "src.db.database_router.deactivate_lakebase_in_process", new=AsyncMock()
        ) as deactivate:
            await svc.save_config({"enabled": True, "instance_name": "kasal-lakebase"})

        deactivate.assert_not_awaited()
        svc.config_repository.upsert.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_missing_enabled_key_is_treated_as_disable(self):
        """``config.get("enabled", False)`` — an absent key means disabled."""
        svc = _make_service()
        with patch(
            "src.db.database_router.deactivate_lakebase_in_process", new=AsyncMock()
        ) as deactivate:
            await svc.save_config({})

        deactivate.assert_awaited_once()
