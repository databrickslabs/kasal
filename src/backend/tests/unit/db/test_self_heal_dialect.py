"""The self-heal asks the CONNECTION which dialect it is, never the environment.

``run_schema_self_heal`` is called twice with different engines:

1. from ``init_db``, against the local/default engine;
2. from the ``main.py`` lifespan, against the LAKEBASE engine, immediately after
   the runtime hot-swap — and that second call is the only path that heals a
   pre-existing Lakebase, because ``init_db`` fires before Lakebase activation.

On that second call ``settings.DATABASE_URI`` still reads ``sqlite``. It
describes the CONFIGURED default, not the connection in hand. Every ``_ensure_*``
helper branched on it, so all nine took the SQLite branch and issued
SQLite-flavoured DDL at PostgreSQL — ``PRAGMA table_info(...)`` and
``ALTER TABLE ... ADD COLUMN`` without ``IF NOT EXISTS``.

It failed silently: each helper swallows its exception as a warning. And it
stayed latent, because a column added BEFORE a given Lakebase was provisioned
gets created by ``create_all`` along with the whole table. The first column added
AFTER one existed is what exposed it — ``modelconfig.thinking_budget_tokens``
never landed in Lakebase, and since every LLM call reads the model catalogue,
production answered every crew generation with:

    column modelconfig.thinking_budget_tokens does not exist

The lesson generalises past this one bug: on a process that hot-swaps its
database at runtime, the environment is not a safe source of truth about the
connection you are holding.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.db.self_heal.columns import _ensure_agent_columns, _ensure_modelconfig_columns
from src.db.self_heal.dialect import _conn_is_sqlite


def _conn(dialect: str) -> MagicMock:
    conn = MagicMock()
    conn.engine.dialect.name = dialect
    conn.exec_driver_sql = AsyncMock(return_value=MagicMock())
    return conn


def _pg_conn_missing_columns(table_columns: list[str]) -> MagicMock:
    """A Postgres connection that reports ``table_columns`` as already present.

    The Postgres branch now reads information_schema BEFORE altering (an ALTER on
    a table this role does not own raises "must be owner" even when the column is
    there, and that aborts the whole self-heal transaction). So the catalogue read
    has to be answered, and only the genuinely missing columns get an ALTER.
    """
    conn = MagicMock()
    conn.engine.dialect.name = "postgresql"
    catalogue = MagicMock()
    catalogue.fetchall = MagicMock(return_value=[(c,) for c in table_columns])
    conn.exec_driver_sql = AsyncMock(
        side_effect=[catalogue] + [MagicMock() for _ in range(12)]
    )
    return conn


def _ddl(conn) -> list[str]:
    """Statements issued, excluding the information_schema catalogue read."""
    return [
        c.args[0]
        for c in conn.exec_driver_sql.await_args_list
        if "information_schema" not in c.args[0]
    ]


class TestConnIsSqlite:
    def test_reads_the_connections_dialect(self):
        assert _conn_is_sqlite(_conn("sqlite")) is True
        assert _conn_is_sqlite(_conn("postgresql")) is False

    def test_ignores_the_configured_uri(self):
        """THE bug: a Postgres/Lakebase connection while DATABASE_URI says sqlite."""
        with patch("src.db.self_heal.dialect.settings") as mock_settings:
            mock_settings.DATABASE_URI = "sqlite+aiosqlite:///app.db"
            assert _conn_is_sqlite(_conn("postgresql")) is False

    def test_falls_back_to_the_uri_when_the_connection_cannot_answer(self):
        """A mock or exotic connection must not break startup."""
        broken = MagicMock()
        type(broken).engine = property(
            lambda self: (_ for _ in ()).throw(RuntimeError("no engine"))
        )
        with patch("src.db.self_heal.dialect.settings") as mock_settings:
            mock_settings.DATABASE_URI = "sqlite+aiosqlite:///app.db"
            assert _conn_is_sqlite(broken) is True


@pytest.mark.asyncio
class TestPostgresBranchIsTakenForALakebaseConnection:
    """With DATABASE_URI still on sqlite — exactly the lifespan's situation."""

    async def test_modelconfig_uses_add_column_if_not_exists(self):
        conn = _pg_conn_missing_columns(["id", "key"])
        with patch("src.db.self_heal.dialect.settings") as mock_settings:
            mock_settings.DATABASE_URI = "sqlite+aiosqlite:///app.db"
            await _ensure_modelconfig_columns(conn)

        statements = _ddl(conn)
        assert statements, "no DDL issued at all"
        # Postgres form, and no PRAGMA anywhere.
        assert all("IF NOT EXISTS" in s for s in statements), statements
        assert not any("PRAGMA" in s for s in statements), statements
        # The column whose absence broke production.
        assert any("thinking_budget_tokens" in s for s in statements), statements

    async def test_agents_uses_add_column_if_not_exists(self):
        conn = _pg_conn_missing_columns(["id", "name"])
        with patch("src.db.self_heal.dialect.settings") as mock_settings:
            mock_settings.DATABASE_URI = "sqlite+aiosqlite:///app.db"
            await _ensure_agent_columns(conn)

        statements = _ddl(conn)
        assert statements
        assert all("IF NOT EXISTS" in s for s in statements), statements
        assert not any("PRAGMA" in s for s in statements), statements
        assert any("thinking_budget_tokens" in s for s in statements), statements

    async def test_a_column_that_already_exists_is_not_altered(self):
        """No ALTER means no "must be owner" on work that need not happen.

        This is the guard that keeps one orphaned-owner table from aborting the
        transaction and skipping every later step.
        """
        conn = _pg_conn_missing_columns(
            [
                "id",
                "name",
                "skills",
                "thinking_budget_tokens",
                "reasoning_effort",
                "max_tokens",
            ]
        )
        await _ensure_agent_columns(conn)
        assert _ddl(conn) == []


@pytest.mark.asyncio
class TestSqliteBranchStillWorks:
    """The local path must be unchanged — this fix must not trade one for the other."""

    async def test_modelconfig_pragma_then_alter(self):
        conn = _conn("sqlite")
        pragma = MagicMock()
        pragma.fetchall = MagicMock(return_value=[(0, "id"), (1, "key")])
        conn.exec_driver_sql = AsyncMock(
            side_effect=[pragma] + [MagicMock() for _ in range(6)]
        )

        # DATABASE_URI deliberately says POSTGRES here: the connection wins.
        with patch("src.db.self_heal.dialect.settings") as mock_settings:
            mock_settings.DATABASE_URI = "postgresql+asyncpg://x/y"
            await _ensure_modelconfig_columns(conn)

        statements = [c.args[0] for c in conn.exec_driver_sql.await_args_list]
        assert statements[0] == "PRAGMA table_info(modelconfig)"
        # SQLite has no ADD COLUMN IF NOT EXISTS, hence the PRAGMA check first.
        assert not any("IF NOT EXISTS" in s for s in statements[1:]), statements
        assert any("thinking_budget_tokens" in s for s in statements), statements

    async def test_an_empty_table_is_left_to_create_all(self):
        """No columns back means the table does not exist yet."""
        conn = _conn("sqlite")
        pragma = MagicMock()
        pragma.fetchall = MagicMock(return_value=[])
        conn.exec_driver_sql = AsyncMock(return_value=pragma)

        await _ensure_modelconfig_columns(conn)

        statements = [c.args[0] for c in conn.exec_driver_sql.await_args_list]
        assert statements == ["PRAGMA table_info(modelconfig)"]


class TestNoHelperReadsTheUriDirectly:
    def test_the_env_gate_is_gone_from_every_self_heal(self):
        """Guards the whole class of bug rather than the two columns that found it.

        Nine helpers shared this gate. Any new one that copies the old pattern
        reintroduces a failure that is silent, latent until the next column, and
        fatal to every LLM call when it finally lands.
        """
        import inspect

        from src.db.self_heal import columns, data, dialect, runner, tables, vectors

        source = "".join(
            inspect.getsource(m)
            for m in (columns, data, dialect, runner, tables, vectors)
        )
        offending = 'is_sqlite = str(settings.DATABASE_URI).startswith("sqlite")'
        assert offending not in source, (
            "A self-heal helper is deciding its dialect from settings.DATABASE_URI. "
            "That is wrong on the Lakebase path, where the URI still says sqlite "
            "while the connection is PostgreSQL. Use _conn_is_sqlite(conn)."
        )
