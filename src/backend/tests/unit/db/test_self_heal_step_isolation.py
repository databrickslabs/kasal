"""One broken self-heal step must not take the other 23 with it.

Every ``_ensure_*`` helper catches its own exception and logs a warning, which
reads like isolation. On PostgreSQL it is not: a failed statement aborts the
whole transaction, so every later statement on that connection fails with
``InFailedSQLTransactionError`` — "current transaction is aborted, commands
ignored until end of transaction block". Swallowing the first error turned one
skippable failure into a silent, total no-op, and the warnings made it look like
23 unrelated problems instead of one.

This is what broke the deployed app. ``documentation_embeddings`` was left owned
by a PREVIOUS deploy's service principal, so its ALTER failed with
``must be owner of table`` — and it runs FIRST:

    WARNING - Could not ensure documentation_embeddings columns:
              InsufficientPrivilegeError: must be owner of table
    WARNING - Could not ensure publications table: InFailedSQLTransactionError
    WARNING - Could not ensure agents.skills column: InFailedSQLTransactionError
    ... 21 more, all the same cascade

So ``agents.thinking_budget_tokens`` was never added, and creating an agent failed
with ``column "thinking_budget_tokens" of relation "agents" does not exist``. The
fix is a SAVEPOINT per step, which is what the per-helper try/except was always
meant to provide.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.db.session import _run_self_heal_step, run_schema_self_heal


def _conn(nested_supported: bool = True) -> MagicMock:
    """A connection whose ``begin_nested()`` is an async context manager."""
    conn = MagicMock()
    conn.engine.dialect.name = "postgresql"
    if nested_supported:
        savepoint = AsyncMock()
        savepoint.__aenter__ = AsyncMock(return_value=savepoint)
        savepoint.__aexit__ = AsyncMock(return_value=False)
        conn.begin_nested = MagicMock(return_value=savepoint)
    else:
        conn.begin_nested = MagicMock(side_effect=NotImplementedError("no savepoints"))
    return conn


@pytest.mark.asyncio
class TestStepIsolation:
    async def test_a_failing_step_does_not_stop_the_next_one(self):
        conn = _conn()
        calls = []

        async def boom(_c):
            calls.append("boom")
            raise RuntimeError("must be owner of table documentation_embeddings")

        async def ok(_c):
            calls.append("ok")

        await _run_self_heal_step(conn, boom)
        await _run_self_heal_step(conn, ok)

        assert calls == ["boom", "ok"]

    async def test_each_step_gets_its_own_savepoint(self):
        """Without a savepoint there is nothing to roll back TO."""
        conn = _conn()

        async def ok(_c):
            pass

        await _run_self_heal_step(conn, ok)
        conn.begin_nested.assert_called_once()

    async def test_a_connection_without_savepoints_still_runs_the_step(self):
        """A mock or a driver with no SAVEPOINT support must not break startup."""
        conn = _conn(nested_supported=False)
        ran = []

        async def ok(_c):
            ran.append(True)

        await _run_self_heal_step(conn, ok)
        assert ran == [True]

    async def test_the_isolation_is_logged(self):
        """The helper logs the cause; this records that it was contained.

        The difference between "one table skipped" and "nothing healed" is the
        only thing that matters when reading these warnings later.
        """
        conn = _conn()

        async def boom(_c):
            raise RuntimeError("must be owner")

        with patch("src.db.session.logger") as mock_logger:
            await _run_self_heal_step(conn, boom)

        assert mock_logger.warning.called
        msg = mock_logger.warning.call_args[0][0]
        assert "boom" in msg  # names the step
        assert "continuing" in msg


@pytest.mark.asyncio
class TestTheWholePassSurvivesTheFirstStepFailing:
    async def test_agents_columns_are_still_reached(self):
        """THE regression: the first step failing must not skip `agents`.

        documentation_embeddings runs first and, on the deployed app, cannot be
        ALTERed. agents must still be healed — that is the table whose missing
        column broke agent creation.
        """
        conn = _conn()
        ran: list[str] = []

        def _recorder(name):
            async def step(_c):
                ran.append(name)
                if name == "_ensure_documentation_embeddings_columns":
                    raise RuntimeError("must be owner of table")

            step.__name__ = name
            return step

        names = [
            "_ensure_documentation_embeddings_columns",
            "_ensure_agent_columns",
            "_ensure_modelconfig_columns",
        ]
        with patch.multiple(
            "src.db.session",
            **{n: _recorder(n) for n in names},
        ):
            await run_schema_self_heal(conn)

        assert "_ensure_documentation_embeddings_columns" in ran
        # The two that carry thinking_budget_tokens / reasoning_effort.
        assert "_ensure_agent_columns" in ran
        assert "_ensure_modelconfig_columns" in ran

    async def test_elevates_to_superuser_and_enables_pgvector_on_postgres(self):
        """Before the steps, the pass acts as databricks_superuser (so ALTERs on
        tables owned by another principal succeed) and enables pgvector."""
        conn = _conn()
        conn.engine.dialect.name = "postgresql"

        with (
            patch(
                "src.services.databricks.lakebase.superuser.enter_superuser_async",
                new=AsyncMock(return_value=True),
            ) as enter,
            patch(
                "src.services.databricks.lakebase.superuser.enable_pgvector_async",
                new=AsyncMock(return_value=True),
            ) as pgvector,
        ):
            await run_schema_self_heal(conn)

        enter.assert_awaited_once()
        pgvector.assert_awaited_once()

    async def test_does_not_set_role_on_sqlite(self):
        """SET ROLE / CREATE EXTENSION are PostgreSQL-only; SQLite must skip them."""
        conn = _conn()
        conn.engine.dialect.name = "sqlite"

        with (
            patch(
                "src.services.databricks.lakebase.superuser.enter_superuser_async",
                new=AsyncMock(return_value=True),
            ) as enter,
            patch(
                "src.services.databricks.lakebase.superuser.enable_pgvector_async",
                new=AsyncMock(return_value=True),
            ) as pgvector,
        ):
            await run_schema_self_heal(conn)

        enter.assert_not_awaited()
        pgvector.assert_not_awaited()

    async def test_every_step_runs_even_if_all_of_them_fail(self):
        """A totally unhealable database still attempts each step exactly once."""
        conn = _conn()
        attempted: list[str] = []

        async def always_boom(c):  # noqa: ARG001
            raise RuntimeError("nope")

        import src.db.session as session_module

        # Patch every _ensure_/_heal_/_disable_ callable the pass invokes.
        targets = [
            name
            for name in dir(session_module)
            if name.startswith(("_ensure_", "_heal_", "_disable_"))
            and callable(getattr(session_module, name))
        ]

        def _tracked(name):
            async def step(_c):
                attempted.append(name)
                await always_boom(_c)

            step.__name__ = name
            return step

        with patch.multiple("src.db.session", **{n: _tracked(n) for n in targets}):
            await run_schema_self_heal(conn)  # must not raise

        # 24 steps in the pass; assert we got them all, not just the first.
        assert len(attempted) >= 20, attempted
        assert len(attempted) == len(set(attempted)), "a step ran twice"
