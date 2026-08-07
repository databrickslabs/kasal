"""Disabling Lakebase must actually return this process to the local database.

Deleting the config row was never enough. Routed sessions follow it — both
``get_smart_db_session`` and ``routed_scoped_session`` re-read
``is_lakebase_enabled()`` per call — but the raw ``async_session_factory`` is a
process global that ``main.py``'s lifespan hot-swaps to Lakebase and that nothing
swapped back. Everything holding that factory (``utils/databricks_auth``, and
through it ``routed_scoped_session``'s reentrant ``_RESOLVING_AUTH`` branch) kept
reading Lakebase until the process restarted, which is why "disable" appeared not
to take effect.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import src.db.lakebase_state as lakebase_state_mod
from src.db.database_router import deactivate_lakebase_in_process
from src.db.lakebase_state import (
    is_fallback_allowed,
    mark_lakebase_activated,
    mark_lakebase_deactivated,
)
from src.db.session import _SwappableSessionFactory


@pytest.fixture(autouse=True)
def _reset_state():
    lakebase_state_mod._lakebase_ever_activated = False
    yield
    lakebase_state_mod._lakebase_ever_activated = False


class TestTheFactoryActuallyReverts:
    """The point of the fix: the global factory stops producing Lakebase sessions."""

    @pytest.mark.asyncio
    async def test_the_global_factory_goes_back_to_local(self):
        from src.db.session import _local_session_factory

        lakebase = MagicMock(name="lakebase_sessionmaker")
        factory = _SwappableSessionFactory(_local_session_factory)
        factory.activate_lakebase(lakebase)
        assert factory.is_lakebase is True

        with (
            patch("src.db.database_router.async_session_factory", factory),
            patch("src.db.lakebase_session.dispose_lakebase_factory", new=AsyncMock()),
        ):
            await deactivate_lakebase_in_process()

        assert factory.is_lakebase is False
        # Not merely a flag: the underlying sessionmaker must be the local one
        # again, since that is what every raw-factory caller will now use.
        # deactivate_lakebase always reverts to the module-level local factory.
        assert factory._factory is _local_session_factory
        assert factory._factory is not lakebase

    @pytest.mark.asyncio
    async def test_the_lakebase_pool_is_torn_down(self):
        """A live Lakebase pool outliving the switch would keep serving it."""
        disposer = AsyncMock()
        with (
            patch(
                "src.db.database_router.async_session_factory",
                _SwappableSessionFactory(MagicMock()),
            ),
            patch("src.db.lakebase_session.dispose_lakebase_factory", disposer),
        ):
            await deactivate_lakebase_in_process()

        disposer.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_local_engines_are_not_disposed(self):
        """dispose_engines() would tear down the backend we are switching TO."""
        with (
            patch(
                "src.db.database_router.async_session_factory",
                _SwappableSessionFactory(MagicMock()),
            ),
            patch("src.db.lakebase_session.dispose_lakebase_factory", new=AsyncMock()),
            patch("src.db.session.dispose_engines", new=AsyncMock()) as dispose_all,
        ):
            await deactivate_lakebase_in_process()

        dispose_all.assert_not_awaited()


class TestCachesKeyedToTheOldBackend:
    @pytest.mark.asyncio
    async def test_on_swap_callbacks_fire(self):
        """Registered caches (e.g. ExecutionService's) must be cleared on the switch."""
        factory = _SwappableSessionFactory(MagicMock())
        factory.activate_lakebase(MagicMock())
        cleared = []
        factory.register_on_swap(lambda: cleared.append(True))

        with (
            patch("src.db.database_router.async_session_factory", factory),
            patch("src.db.lakebase_session.dispose_lakebase_factory", new=AsyncMock()),
        ):
            await deactivate_lakebase_in_process()

        assert cleared == [True]


class TestItIsSafeWhenLakebaseWasNeverOn:
    @pytest.mark.asyncio
    async def test_deactivating_an_already_local_factory_is_a_no_op(self):
        factory = _SwappableSessionFactory(MagicMock())
        fired = []
        factory.register_on_swap(lambda: fired.append(True))

        with (
            patch("src.db.database_router.async_session_factory", factory),
            patch("src.db.lakebase_session.dispose_lakebase_factory", new=AsyncMock()),
        ):
            await deactivate_lakebase_in_process()

        assert factory.is_lakebase is False
        # No transition happened, so caches must NOT be needlessly wiped.
        assert fired == []


class TestFallbackStateTracking:
    def test_deactivating_re_arms_startup_mode_fallback(self):
        mark_lakebase_activated()
        assert is_fallback_allowed() is False

        mark_lakebase_deactivated()

        # A deliberate disable makes the local DB authoritative again, so falling
        # back to it is no longer silent data loss.
        assert is_fallback_allowed() is True

    @pytest.mark.asyncio
    async def test_the_healthcheck_stops_reporting_lakebase(self):
        """/healthcheck/db read `lakebase_activated: true` forever before this."""
        mark_lakebase_activated()

        with (
            patch(
                "src.db.database_router.async_session_factory",
                _SwappableSessionFactory(MagicMock()),
            ),
            patch("src.db.lakebase_session.dispose_lakebase_factory", new=AsyncMock()),
        ):
            await deactivate_lakebase_in_process()

        from src.db.lakebase_state import is_lakebase_activated

        assert is_lakebase_activated() is False
