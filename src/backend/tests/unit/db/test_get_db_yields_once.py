"""``get_db`` yields exactly one session, even when "database is locked".

FastAPI drives a yield dependency as a context manager, which allows ONE yield.
``get_db`` retried "database is locked" by looping back to a second ``yield`` —
also when the error came from the handler or from the post-handler commit, i.e.
after the request had already used the session. The context manager then raised
``RuntimeError("generator didn't stop ...")``, which hid the lock error, and the
rolled-back write was gone. Work the handler already did cannot be replayed by
the dependency, so after the yield the error must propagate as itself.
"""

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.exc import OperationalError

from src.db import session as session_module


def _locked() -> OperationalError:
    return OperationalError("COMMIT", None, Exception("database is locked"))


def _factory(sessions):
    """A session factory handing out ``sessions`` in order."""
    remaining = list(sessions)

    def make():
        session = remaining.pop(0)
        cm = MagicMock()
        cm.__aenter__ = AsyncMock(return_value=session)
        cm.__aexit__ = AsyncMock(return_value=False)
        return cm

    return MagicMock(side_effect=make)


@pytest.fixture
def two_sessions():
    first, second = AsyncMock(name="first"), AsyncMock(name="second")
    factory = _factory([first, second])
    with (
        patch.object(session_module, "async_session_factory", factory),
        patch("asyncio.sleep", new_callable=AsyncMock),
    ):
        yield first, second, factory


async def test_a_locked_commit_is_raised_not_retried(two_sessions):
    """THE bug: a failed commit after the handler re-entered the loop and yielded."""
    first, _second, factory = two_sessions
    first.commit.side_effect = _locked()

    with pytest.raises(OperationalError, match="database is locked"):
        async with asynccontextmanager(session_module.get_db)() as session:
            assert session is first

    first.rollback.assert_awaited()
    assert factory.call_count == 1


async def test_a_lock_error_from_the_handler_is_raised_not_retried(two_sessions):
    first, _second, factory = two_sessions

    with pytest.raises(OperationalError, match="database is locked"):
        async with asynccontextmanager(session_module.get_db)():
            raise _locked()

    first.rollback.assert_awaited()
    assert factory.call_count == 1


async def test_a_clean_request_commits_once(two_sessions):
    first, _second, factory = two_sessions

    async with asynccontextmanager(session_module.get_db)() as session:
        assert session is first

    first.commit.assert_awaited_once()
    assert factory.call_count == 1
