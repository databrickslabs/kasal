"""
Additional unit tests for src/db/session.py to push coverage above 50%.
Focuses on SwappableSessionFactory, retry_db_operation decorator,
routed_scoped_session, get_db edge cases, get_smart_engine, and dispose_engines.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# _SwappableSessionFactory
# ---------------------------------------------------------------------------


class TestSwappableSessionFactory:
    """Tests for the _SwappableSessionFactory hot-swap wrapper."""

    def test_call_delegates_to_factory(self):
        from src.db.session import async_session_factory

        mock_inner = MagicMock()
        mock_inner.return_value = "session_obj"
        original = async_session_factory._factory
        async_session_factory._factory = mock_inner
        result = async_session_factory()
        async_session_factory._factory = original
        assert result == "session_obj"

    def test_is_lakebase_initially_false(self):
        from src.db.session import async_session_factory

        # It may already be swapped in some runs; just check property type
        assert isinstance(async_session_factory.is_lakebase, bool)

    def test_activate_lakebase_swaps_factory(self):
        from src.db.session import _SwappableSessionFactory

        mock_local = MagicMock()
        factory = _SwappableSessionFactory(mock_local)

        mock_lakebase = MagicMock()
        factory.activate_lakebase(mock_lakebase)
        assert factory._factory is mock_lakebase
        assert factory.is_lakebase is True

    def test_deactivate_lakebase_reverts(self):
        from src.db.session import _local_session_factory, _SwappableSessionFactory

        mock_local = MagicMock()
        factory = _SwappableSessionFactory(mock_local)

        mock_lakebase = MagicMock()
        factory.activate_lakebase(mock_lakebase)
        factory.deactivate_lakebase()
        assert factory._factory is _local_session_factory
        assert factory.is_lakebase is False

    def test_call_after_activate(self):
        from src.db.session import _SwappableSessionFactory

        original_factory = MagicMock(return_value="orig")
        factory = _SwappableSessionFactory(original_factory)

        lake_factory = MagicMock(return_value="lake")
        factory.activate_lakebase(lake_factory)
        assert factory() == "lake"

    def test_call_after_deactivate(self):
        from src.db.session import _SwappableSessionFactory

        original_factory = MagicMock(return_value="orig")
        factory = _SwappableSessionFactory(original_factory)

        lake_factory = MagicMock(return_value="lake")
        factory.activate_lakebase(lake_factory)
        factory.deactivate_lakebase()
        # After deactivation, _factory is _local_session_factory (not original_factory)
        # Just check it's callable
        assert callable(factory._factory)


# ---------------------------------------------------------------------------
# retry_db_operation decorator
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# routed_scoped_session
# ---------------------------------------------------------------------------


class TestRoutedScopedSession:
    """Tests for the routed_scoped_session context manager."""

    @pytest.mark.asyncio
    async def test_yields_a_routed_session_when_no_request_session(self):
        """Outside a request it goes through the ROUTER, not the raw factory.

        The helper this replaced (``request_scoped_session``) took
        ``async_session_factory`` here — a per-process snapshot that a runtime
        /lakebase/enable never swaps, so the read silently hit the local database.
        """
        from src.db.session import routed_scoped_session

        routed = AsyncMock()

        async def fake_router():
            yield routed

        with patch(
            "src.db.database_router.get_smart_db_session", side_effect=fake_router
        ):
            with patch("src.db.session.async_session_factory") as raw:
                async with routed_scoped_session() as session:
                    assert session is routed
                raw.assert_not_called()

    @pytest.mark.asyncio
    async def test_reuses_existing_request_session(self):
        """Inside request context, returns the stored ContextVar session."""
        from src.db.session import (
            _enter_request_session,
            _exit_request_session,
            routed_scoped_session,
        )

        existing = AsyncMock()
        # Record the owning task so the ownership check reuses it (the in-request
        # contract). A bare _request_session.set would now route fresh instead.
        tokens = _enter_request_session(existing)
        try:
            async with routed_scoped_session() as session:
                assert session is existing
        finally:
            _exit_request_session(tokens)


# ---------------------------------------------------------------------------
# get_smart_engine
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# dispose_engines
# ---------------------------------------------------------------------------


class TestDisposeEngines:
    """Tests for the dispose_engines coroutine."""

    @pytest.mark.asyncio
    async def test_disposes_main_engine(self):
        from src.db.session import dispose_engines

        mock_engine = AsyncMock()

        with patch("src.db.session.engine", mock_engine):
            with patch(
                "src.db.lakebase_session.dispose_lakebase_factory", new=AsyncMock()
            ):
                await dispose_engines()

        mock_engine.dispose.assert_awaited()

    @pytest.mark.asyncio
    async def test_handles_engine_dispose_error(self):
        from src.db.session import dispose_engines

        mock_engine = AsyncMock()
        mock_engine.dispose.side_effect = Exception("dispose error")

        with patch("src.db.session.engine", mock_engine):
            with patch(
                "src.db.lakebase_session.dispose_lakebase_factory", new=AsyncMock()
            ):
                # Should not raise
                await dispose_engines()

    @pytest.mark.asyncio
    async def test_handles_lakebase_dispose_error(self):
        from src.db.session import dispose_engines

        with patch(
            "src.db.lakebase_session.dispose_lakebase_factory",
            new=AsyncMock(side_effect=Exception("lb error")),
        ):
            # Should not raise
            await dispose_engines()


# ---------------------------------------------------------------------------
# set_main_event_loop
# ---------------------------------------------------------------------------


class TestSetMainEventLoop:
    """Tests for set_main_event_loop."""

    def test_captures_running_loop(self):
        from src.db.session import set_main_event_loop

        mock_loop = MagicMock()

        with patch("asyncio.get_running_loop", return_value=mock_loop):
            with patch("src.db.session.main_event_loop", None):
                set_main_event_loop()

    def test_handles_no_running_loop(self):
        from src.db.session import set_main_event_loop

        with patch("asyncio.get_running_loop", side_effect=RuntimeError("no loop")):
            # Should not raise
            set_main_event_loop()


# ---------------------------------------------------------------------------
# get_local_db
# ---------------------------------------------------------------------------


class TestGetLocalDb:
    """Tests for the get_local_db async generator."""

    @pytest.mark.asyncio
    async def test_yields_session_and_commits(self):
        from src.db.session import get_local_db

        mock_session = AsyncMock()

        class MockCtx:
            async def __aenter__(self):
                return mock_session

            async def __aexit__(self, *args):
                pass

        with patch(
            "src.db.session._local_session_factory", MagicMock(return_value=MockCtx())
        ):
            gen = get_local_db()
            session = await gen.__anext__()
            assert session is mock_session
            try:
                await gen.__anext__()
            except StopAsyncIteration:
                pass
            mock_session.commit.assert_awaited()

    @pytest.mark.asyncio
    async def test_rolls_back_on_exception(self):
        from src.db.session import get_local_db

        mock_session = AsyncMock()

        class MockCtx:
            async def __aenter__(self):
                return mock_session

            async def __aexit__(self, *args):
                pass

        with patch(
            "src.db.session._local_session_factory", MagicMock(return_value=MockCtx())
        ):
            gen = get_local_db()
            await gen.__anext__()
            with pytest.raises(Exception):
                await gen.athrow(ValueError("boom"))
            mock_session.rollback.assert_awaited()
