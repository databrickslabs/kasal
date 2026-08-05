"""Committing on a connection that was disposed underneath us is not a failure.

Enabling or migrating Lakebase calls ``dispose_engines()`` to switch backends.
That closes connections held by any CONCURRENT request, so when such a request
reaches its commit there is nothing left to commit — and nothing to roll back
either. The work either landed before the switch or was never going to.

The router already tolerated SQLAlchemy's wording for this ("no active
connection"). It did not tolerate asyncpg's, so the deployed app answered a
polling client with a raw 500 seven times in one 14ms burst::

    InterfaceError: cannot call Transaction.commit():
                    the underlying connection is closed

The Lakebase session path had no guard at all and re-raised both.

The match is phrase-based on purpose: the drivers share no exception type for
this, and asyncpg's ``InterfaceError`` also covers genuine protocol misuse
("another operation is in progress") which MUST still surface — swallowing that
would hide real concurrency bugs.
"""

import pytest

from src.db.database_router import _is_disposed_connection_error


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


class TestBothSessionPathsUseIt:
    """The router and the Lakebase session must agree.

    The helper lives in ``lakebase_session`` because the router imports THAT
    module, not the reverse — putting it in the router would have made the import
    circular.
    """

    def test_the_lakebase_session_path_guards_its_commits(self):
        import inspect

        import src.db.lakebase_session as lakebase_session

        source = inspect.getsource(lakebase_session)
        # Both branches: the crew-thread factory and the main event loop.
        assert source.count("_is_disposed_connection_error(exc)") == 2, (
            "a commit site in lakebase_session lost its disposed-connection "
            "guard; a backend switch there 500s the in-flight request"
        )

    def test_the_router_guards_its_commit(self):
        import inspect

        import src.db.database_router as database_router

        source = inspect.getsource(database_router)
        assert "_is_disposed_connection_error(e)" in source
        # The old narrow literal must not come back.
        assert '"no active connection" in str(e).lower()' not in source
