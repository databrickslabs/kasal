"""Connection-disposal diagnostics must not hide failed transactions.

A closed connection can mean an uncommitted write was lost. Both Lakebase
session paths must propagate that primary failure, even if rollback also fails.
"""

import pytest

from src.db.lakebase_session import _is_disposed_connection_error


class TestWhatCountsAsADisposedConnection:
    @pytest.mark.parametrize(
        "message",
        [
            # asyncpg — the one that reached the user as a 500.
            "cannot call Transaction.commit(): the underlying connection is closed",
            # SQLAlchemy — already handled before this change.
            "no active connection",
            # Wording variants seen across drivers/versions.
            "connection is closed",
            "the connection was closed",
            "connection already closed",
            # Real messages arrive wrapped in driver/dialect prefixes.
            "(sqlalchemy.dialects.postgresql.asyncpg.InterfaceError) "
            "<class 'asyncpg.exceptions._base.InterfaceError'>: cannot call "
            "Transaction.commit(): the underlying connection is closed",
        ],
    )
    def test_teardown_races_are_recognised(self, message):
        assert _is_disposed_connection_error(Exception(message)) is True

    def test_it_is_case_insensitive(self):
        assert _is_disposed_connection_error(
            Exception("The Underlying Connection Is CLOSED")
        )

    @pytest.mark.parametrize(
        "message",
        [
            # A REAL asyncpg concurrency bug — same exception type, must not be
            # swallowed, or genuine session misuse becomes invisible.
            "another operation is in progress",
            # Schema problems: the class of bug that caused this incident.
            'relation "agents" does not exist',
            'column "thinking_budget_tokens" of relation "agents" does not exist',
            # Auth and transaction-state failures are actionable, not races.
            "must be owner of table modelconfig",
            "current transaction is aborted, commands ignored",
            "password authentication failed",
            'type "vector" does not exist',
        ],
    )
    def test_real_failures_still_surface(self, message):
        assert _is_disposed_connection_error(Exception(message)) is False

    def test_an_empty_message_is_not_a_race(self):
        assert _is_disposed_connection_error(Exception()) is False


@pytest.mark.asyncio
@pytest.mark.parametrize("crew_thread", [False, True])
@pytest.mark.parametrize("rollback_fails", [False, True])
async def test_lakebase_commit_failure_is_not_suppressed(crew_thread, rollback_fails):
    from contextlib import asynccontextmanager
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, patch

    from src.db import lakebase_session as module

    primary = RuntimeError("connection is closed")
    session = AsyncMock()
    session.commit.side_effect = primary
    if rollback_fails:
        session.rollback.side_effect = RuntimeError("rollback also failed")

    @asynccontextmanager
    async def session_context():
        yield session

    factory = SimpleNamespace(
        instance_name="audit", user_email=None, get_session=session_context
    )
    with (
        patch.object(module, "_is_crew_thread", return_value=crew_thread),
        patch.object(module, "_lakebase_factory", factory),
        patch.object(module, "_thread_local", SimpleNamespace(factory=factory)),
        pytest.raises(RuntimeError) as caught,
    ):
        async with module.get_lakebase_session(instance_name="audit") as actual:
            assert actual is session
    assert caught.value is primary
    session.rollback.assert_awaited_once()
    session.close.assert_awaited_once()
