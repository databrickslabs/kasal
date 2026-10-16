"""The ``embedding`` column is added back once pgvector is available.

Vector tables are created WITHOUT their vector column, because a deployed app
cannot install pgvector (``CREATE EXTENSION`` needs ``databricks_superuser``) and
``CREATE TABLE ... vector(1024)`` fails outright without it. That keeps the rest
of the table usable.

Nothing added the column back, though — so on an instance where an owner HAD
enabled the extension, the ORM kept inserting a column that did not exist::

    column "embedding" of relation "knowledge_embeddings" does not exist

That broke knowledge upload AFTER the file was parsed and 56 chunks were embedded:
the expensive work happened and was then thrown away. And the embedding is the
entire point of a knowledge source — without it there is no similarity search, so
"table exists but has no embedding column" is not a degraded state, it is a broken
one.

Confirmed on the live Lakebase: ``documentation_embeddings`` HAD the column (its
table predates the vector-free creation path) while ``knowledge_embeddings`` did
not (created after). Same code, different history — which is why it looked
intermittent.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Registering EVERY model matters here: workflow_recipes lives in a module that
# nothing else in this file imports, and an unregistered model is invisible to the
# metadata-derived table list — which would make this suite pass while the real
# app still missed the column.
import src.db.all_models  # noqa: F401
from src.db.self_heal.vectors import (
    _ensure_pgvector_embedding_columns,
    _vector_embedding_tables,
)


class _Catalogue:
    """Just enough of PostgreSQL's catalogue for the vector step to read.

    ``fail(sql)`` returns an exception to raise for a statement, or None.
    """

    def __init__(self, pgvector=True, embedding=False, indexed=False, fail=None):
        tables = _vector_embedding_tables()
        self.columns = {
            t: {"id"} | ({"embedding"} if embedding else set()) for t in tables
        }
        self.indexes = {f"idx_{t}_embedding" for t in tables} if indexed else set()
        self.pgvector = pgvector
        self.fail = fail or (lambda sql: None)

    async def execute(self, sql, *args, **kwargs):
        error = self.fail(sql)
        if error is not None:
            raise error
        result = MagicMock()
        if "pg_extension" in sql:
            result.fetchone.return_value = ("public",) if self.pgvector else None
        elif "pg_attribute" in sql:
            table = sql.split("to_regclass('")[1].split("'")[0]
            result.fetchall.return_value = [(c,) for c in self.columns.get(table, ())]
        elif "pg_index" in sql:
            name = sql.split("i.relname = '")[1].split("'")[0]
            result.fetchone.return_value = (1,) if name in self.indexes else None
        elif sql.startswith("ALTER TABLE"):
            self.columns[sql.split()[2]].add("embedding")
        elif sql.startswith("CREATE INDEX"):
            self.indexes.add(sql.split()[5])
        return result


def _ddl_fails(sql):
    """A table another role owns: every ALTER / CREATE is refused."""
    if sql.startswith(("ALTER", "CREATE")):
        return Exception("must be owner of table")
    return None


def _conn(dialect: str = "postgresql", pgvector: bool = True, **catalogue) -> MagicMock:
    conn = MagicMock()
    conn.engine.dialect.name = dialect
    conn.exec_driver_sql = AsyncMock(
        side_effect=_Catalogue(pgvector=pgvector, **catalogue).execute
    )
    savepoint = AsyncMock()
    savepoint.__aenter__ = AsyncMock(return_value=savepoint)
    savepoint.__aexit__ = AsyncMock(return_value=False)
    conn.begin_nested = MagicMock(return_value=savepoint)
    return conn


def _statements(conn) -> list[str]:
    return [c.args[0] for c in conn.exec_driver_sql.await_args_list]


@pytest.mark.asyncio
class TestWithPgvectorAvailable:
    async def test_both_vector_tables_get_the_column(self):
        conn = _conn()
        await _ensure_pgvector_embedding_columns(conn)
        sql = " ".join(_statements(conn))
        for table in _vector_embedding_tables():
            assert f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS embedding" in sql

    async def test_every_model_with_a_vector_column_is_covered(self):
        """DERIVED from the models, so a new vector column needs no edit here.

        The first version hardcoded two table names and missed workflow_recipes —
        the deployed app then failed every recipe lookup with "column
        workflow_recipes.embedding does not exist". Third time a hand-maintained
        list of models has drifted in this codebase.
        """
        from src.db.base import Base
        from src.models.documentation_embedding import Vector

        expected = {
            table.name
            for table in Base.metadata.sorted_tables
            if any(isinstance(column.type, Vector) for column in table.columns)
        }
        assert set(_vector_embedding_tables()) == expected
        # The three that exist today, named so a silent shrink is caught.
        assert expected >= {
            "documentation_embeddings",
            "knowledge_embeddings",
            "workflow_recipes",
        }

    async def test_the_column_type_matches_the_model(self):
        conn = _conn()
        await _ensure_pgvector_embedding_columns(conn)
        assert all("vector(1024)" in s for s in _statements(conn) if "ADD COLUMN" in s)

    async def test_an_hnsw_index_is_created(self):
        """Without it, similarity search degrades to a full scan per query."""
        conn = _conn()
        await _ensure_pgvector_embedding_columns(conn)
        sql = " ".join(_statements(conn))
        assert "USING hnsw (embedding public.vector_cosine_ops)" in sql

    async def test_it_is_idempotent(self):
        """Runs on every startup, so it must be safe to repeat."""
        conn = _conn()
        await _ensure_pgvector_embedding_columns(conn)
        for stmt in _statements(conn):
            if stmt.startswith(("ALTER", "CREATE")):
                assert "IF NOT EXISTS" in stmt, stmt

    async def test_each_statement_is_savepointed(self):
        """An orphaned-owner table (42501) must not abort the whole self-heal."""
        conn = _conn()
        await _ensure_pgvector_embedding_columns(conn)
        assert conn.begin_nested.call_count >= 2 * len(_vector_embedding_tables())

    async def test_one_failing_table_does_not_stop_the_other(self):
        # The first table's ALTER blows up; everything after must still be attempted.
        conn = _conn(
            fail=lambda sql: (
                Exception("must be owner")
                if sql.startswith("ALTER TABLE documentation_embeddings")
                else None
            )
        )
        await _ensure_pgvector_embedding_columns(conn)
        sql = " ".join(_statements(conn))
        assert "ALTER TABLE knowledge_embeddings ADD COLUMN" in sql

    async def test_a_table_another_role_owns_gets_no_ddl_when_nothing_is_missing(
        self,
    ):
        """PostgreSQL checks ownership before existence: ADD COLUMN IF NOT EXISTS
        and CREATE INDEX IF NOT EXISTS fail on such a table even when the column
        and index are there. Read the catalogue first, so a healthy install with
        a re-created app (or a DBA-owned schema) records no gap on every boot."""
        from src.db.self_heal import report

        conn = _conn(embedding=True, indexed=True, fail=_ddl_fails)
        with report.heal_pass(conn, record=False) as gaps:
            await _ensure_pgvector_embedding_columns(conn)

        assert gaps == []
        assert not any(s.startswith(("ALTER", "CREATE")) for s in _statements(conn))

    async def test_a_missing_table_is_left_to_create_all(self):
        """No ALTER on a table that is not there yet, so no gap for it."""
        conn = _conn()
        catalogue = conn.exec_driver_sql.side_effect.__self__
        catalogue.columns["workflow_recipes"] = set()
        await _ensure_pgvector_embedding_columns(conn)
        assert not any(
            "workflow_recipes" in s and "ALTER" in s for s in _statements(conn)
        )


@pytest.mark.asyncio
class TestWithoutPgvector:
    async def test_the_probe_asks_which_schema_holds_the_extension(self):
        conn = _conn()
        await _ensure_pgvector_embedding_columns(conn)
        probe = _statements(conn)[0]
        assert "extnamespace" in probe and "nspname" in probe, probe

    async def test_no_ddl_is_attempted(self):
        """`ADD COLUMN ... vector(1024)` would fail; skip and say why."""
        conn = _conn(pgvector=False)
        await _ensure_pgvector_embedding_columns(conn)
        statements = _statements(conn)
        assert len(statements) == 1  # just the pg_extension probe
        assert "pg_extension" in statements[0]

    async def test_a_probe_failure_reaches_the_steps_savepoint(self):
        """Not swallowed: on PostgreSQL a failed probe aborts the transaction,
        so it must reach the runner's SAVEPOINT to be rolled back. The runner
        then records it and carries on (it is not fatal to startup)."""
        conn = _conn()
        conn.exec_driver_sql = AsyncMock(side_effect=Exception("connection reset"))
        with pytest.raises(Exception, match="connection reset"):
            await _ensure_pgvector_embedding_columns(conn)


@pytest.mark.asyncio
class TestSqlite:
    async def test_it_is_skipped_entirely(self):
        """On SQLite the column is TEXT holding JSON and create_all makes it.

        The repository also uses a Python-side similarity path there, so pgvector
        DDL is meaningless — issuing it would just error.
        """
        conn = _conn(dialect="sqlite")
        await _ensure_pgvector_embedding_columns(conn)
        conn.exec_driver_sql.assert_not_awaited()


class TestItRunsOnEveryStartup:
    def test_the_self_heal_calls_it(self):
        """Otherwise the column only ever appears on a freshly created table.

        It is its own step in the pass, so a documentation_embeddings this role
        cannot ALTER no longer skips it."""
        import inspect

        import src.db.self_heal.runner as runner_module

        source = inspect.getsource(runner_module.run_schema_self_heal)
        assert "_ensure_pgvector_embedding_columns," in source


@pytest.mark.asyncio
class TestTheLogTellsTheTruth:
    """A heal that applied nothing must not log success.

    The first version logged "Ensured pgvector embedding columns + HNSW indexes"
    unconditionally, so on the deployed app that line appeared at 09:11:48 while
    EVERY statement had failed — and knowledge upload was still broken two minutes
    later. The misleading line is what made the fix look deployed and working.
    """

    async def test_success_is_logged_only_when_everything_applied(self):
        conn = _conn()
        with patch("src.db.self_heal.vectors.logger") as mock_logger:
            await _ensure_pgvector_embedding_columns(conn)
        messages = [c.args[0] for c in mock_logger.info.call_args_list]
        assert any("Ensured pgvector embedding columns" in m for m in messages)

    async def test_a_failure_is_reported_as_incomplete(self):
        conn = _conn(fail=_ddl_fails)
        with patch("src.db.self_heal.vectors.logger") as mock_logger:
            await _ensure_pgvector_embedding_columns(conn)

        infos = [c.args[0] for c in mock_logger.info.call_args_list]
        warnings = [c.args[0] for c in mock_logger.warning.call_args_list]
        assert not any("Ensured pgvector embedding columns" in m for m in infos), (
            "logged success while every statement failed — exactly what hid this "
            "bug on the deployed app"
        )
        assert any("INCOMPLETE" in m for m in warnings), warnings
