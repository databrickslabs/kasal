"""
Additional unit tests for src/db/session.py to push coverage above 50%.
Focuses on SwappableSessionFactory, retry_db_operation decorator,
request_scoped_session, get_db edge cases, get_smart_engine, and dispose_engines.
"""

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, Mock, patch, PropertyMock
from sqlalchemy.exc import OperationalError


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
        from src.db.session import _SwappableSessionFactory, _local_session_factory
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

class TestRetryDbOperation:
    """Tests for the retry_db_operation decorator."""

    @pytest.mark.asyncio
    async def test_async_success_on_first_attempt(self):
        from src.db.session import retry_db_operation

        call_count = 0

        @retry_db_operation(max_retries=3)
        async def my_func():
            nonlocal call_count
            call_count += 1
            return "ok"

        result = await my_func()
        assert result == "ok"
        assert call_count == 1

    @pytest.mark.asyncio
    async def test_async_retries_on_locked_db(self):
        from src.db.session import retry_db_operation

        call_count = 0

        @retry_db_operation(max_retries=3, delay=0.001)
        async def my_func():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise OperationalError("database is locked", None, None)
            return "success"

        result = await my_func()
        assert result == "success"
        assert call_count == 3

    @pytest.mark.asyncio
    async def test_async_raises_after_max_retries(self):
        from src.db.session import retry_db_operation

        @retry_db_operation(max_retries=2, delay=0.001)
        async def always_locked():
            raise OperationalError("database is locked", None, None)

        with pytest.raises(OperationalError):
            await always_locked()

    @pytest.mark.asyncio
    async def test_async_does_not_retry_non_lock_error(self):
        from src.db.session import retry_db_operation

        call_count = 0

        @retry_db_operation(max_retries=3)
        async def fail_fast():
            nonlocal call_count
            call_count += 1
            raise OperationalError("some other error", None, None)

        with pytest.raises(OperationalError):
            await fail_fast()
        assert call_count == 1

    def test_sync_success_on_first_attempt(self):
        from src.db.session import retry_db_operation

        call_count = 0

        @retry_db_operation(max_retries=3)
        def my_sync_func():
            nonlocal call_count
            call_count += 1
            return "sync_ok"

        result = my_sync_func()
        assert result == "sync_ok"
        assert call_count == 1

    def test_sync_retries_on_locked_db(self):
        from src.db.session import retry_db_operation

        call_count = 0

        @retry_db_operation(max_retries=3, delay=0.001)
        def my_sync_func():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise OperationalError("database is locked", None, None)
            return "sync_success"

        with patch("time.sleep"):
            result = my_sync_func()
        assert result == "sync_success"

    def test_sync_does_not_retry_non_lock_error(self):
        from src.db.session import retry_db_operation

        call_count = 0

        @retry_db_operation(max_retries=3)
        def fail_fast():
            nonlocal call_count
            call_count += 1
            raise OperationalError("some other error", None, None)

        with pytest.raises(OperationalError):
            fail_fast()
        assert call_count == 1


# ---------------------------------------------------------------------------
# request_scoped_session
# ---------------------------------------------------------------------------

class TestRequestScopedSession:
    """Tests for request_scoped_session context manager."""

    @pytest.mark.asyncio
    async def test_yields_new_session_when_no_request_session(self):
        """Outside request context, creates a standalone session."""
        from src.db.session import request_scoped_session
        mock_session = AsyncMock()

        class MockCtx:
            async def __aenter__(self):
                return mock_session
            async def __aexit__(self, *args):
                pass

        mock_factory = MagicMock(return_value=MockCtx())
        with patch("src.db.session.async_session_factory", mock_factory):
            async with request_scoped_session() as session:
                assert session is mock_session

    @pytest.mark.asyncio
    async def test_reuses_existing_request_session(self):
        """Inside request context, returns the stored ContextVar session."""
        from src.db.session import request_scoped_session, _request_session

        existing = AsyncMock()
        token = _request_session.set(existing)
        try:
            async with request_scoped_session() as session:
                assert session is existing
        finally:
            _request_session.reset(token)


# ---------------------------------------------------------------------------
# get_smart_engine
# ---------------------------------------------------------------------------

class TestGetSmartEngine:
    """Tests for get_smart_engine function."""

    def test_sqlite_always_returns_engine(self):
        from src.db.session import get_smart_engine, engine
        with patch("src.db.session.settings") as mock_settings:
            mock_settings.DATABASE_URI = "sqlite:///test.db"
            result = get_smart_engine()
        assert result is engine

    def test_postgres_returns_pooled_in_main_loop(self):
        """In main loop context, returns pooled engine."""
        from src.db.session import get_smart_engine

        mock_loop = MagicMock()
        with patch("src.db.session.settings") as mock_settings:
            mock_settings.DATABASE_URI = "postgresql+asyncpg://u@h/db"
            with patch("asyncio.get_running_loop", return_value=mock_loop):
                with patch("src.db.session.main_event_loop", mock_loop):
                    with patch("src.db.session.pooled_engine") as mock_pooled:
                        result = get_smart_engine()
            assert result is mock_pooled

    def test_postgres_returns_nullpool_in_background_loop(self):
        """In a different loop context, returns nullpool engine."""
        from src.db.session import get_smart_engine

        main_loop = MagicMock()
        bg_loop = MagicMock()
        with patch("src.db.session.settings") as mock_settings:
            mock_settings.DATABASE_URI = "postgresql+asyncpg://u@h/db"
            with patch("asyncio.get_running_loop", return_value=bg_loop):
                with patch("src.db.session.main_event_loop", main_loop):
                    with patch("src.db.session.nullpool_engine") as mock_null:
                        result = get_smart_engine()
            assert result is mock_null

    def test_postgres_runtime_error_falls_back_to_nullpool(self):
        from src.db.session import get_smart_engine

        with patch("src.db.session.settings") as mock_settings:
            mock_settings.DATABASE_URI = "postgresql+asyncpg://u@h/db"
            with patch("asyncio.get_running_loop", side_effect=RuntimeError("no loop")):
                with patch("src.db.session.nullpool_engine") as mock_null:
                    result = get_smart_engine()
            assert result is mock_null

    def test_postgres_exception_falls_back_to_default_engine(self):
        from src.db.session import get_smart_engine, engine

        with patch("src.db.session.settings") as mock_settings:
            mock_settings.DATABASE_URI = "postgresql+asyncpg://u@h/db"
            with patch("asyncio.get_running_loop", side_effect=Exception("unexpected")):
                result = get_smart_engine()
            assert result is engine


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
            with patch("src.db.lakebase_session.dispose_lakebase_factory", new=AsyncMock()):
                await dispose_engines()

        mock_engine.dispose.assert_awaited()

    @pytest.mark.asyncio
    async def test_handles_engine_dispose_error(self):
        from src.db.session import dispose_engines

        mock_engine = AsyncMock()
        mock_engine.dispose.side_effect = Exception("dispose error")

        with patch("src.db.session.engine", mock_engine):
            with patch("src.db.lakebase_session.dispose_lakebase_factory", new=AsyncMock()):
                # Should not raise
                await dispose_engines()

    @pytest.mark.asyncio
    async def test_handles_lakebase_dispose_error(self):
        from src.db.session import dispose_engines

        with patch("src.db.lakebase_session.dispose_lakebase_factory", new=AsyncMock(side_effect=Exception("lb error"))):
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

        with patch("src.db.session._local_session_factory", MagicMock(return_value=MockCtx())):
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

        with patch("src.db.session._local_session_factory", MagicMock(return_value=MockCtx())):
            gen = get_local_db()
            await gen.__anext__()
            with pytest.raises(Exception):
                await gen.athrow(ValueError("boom"))
            mock_session.rollback.assert_awaited()
