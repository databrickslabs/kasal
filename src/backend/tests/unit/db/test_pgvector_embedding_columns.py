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


def _conn(dialect: str = "postgresql", pgvector: bool = True) -> MagicMock:
    conn = MagicMock()
    conn.engine.dialect.name = dialect
    probe = MagicMock()
    probe.fetchone = MagicMock(return_value=("public",) if pgvector else None)
    conn.exec_driver_sql = AsyncMock(
        side_effect=[probe] + [MagicMock() for _ in range(12)]
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
        conn = _conn()
        probe = MagicMock()
        probe.fetchone = MagicMock(return_value=("public",))
        # First ALTER blows up; everything after must still be attempted.
        conn.exec_driver_sql = AsyncMock(
            side_effect=[probe, Exception("must be owner")]
            + [MagicMock() for _ in range(8)]
        )
        await _ensure_pgvector_embedding_columns(conn)
        sql = " ".join(str(c.args[0]) for c in conn.exec_driver_sql.await_args_list)
        assert "knowledge_embeddings" in sql


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

    async def test_a_probe_failure_is_not_fatal(self):
        conn = _conn()
        conn.exec_driver_sql = AsyncMock(side_effect=Exception("connection reset"))
        await _ensure_pgvector_embedding_columns(conn)  # must not raise


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
        """Otherwise the column only ever appears on a freshly created table."""
        import inspect

        import src.db.self_heal.columns as columns_module

        source = inspect.getsource(columns_module)
        assert "_ensure_pgvector_embedding_columns(conn)" in source


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
        conn = _conn()
        probe = MagicMock()
        probe.fetchone = MagicMock(return_value=("public",))
        conn.exec_driver_sql = AsyncMock(
            side_effect=[probe] + [Exception("must be owner")] * 8
        )
        with patch("src.db.self_heal.vectors.logger") as mock_logger:
            await _ensure_pgvector_embedding_columns(conn)

        infos = [c.args[0] for c in mock_logger.info.call_args_list]
        warnings = [c.args[0] for c in mock_logger.warning.call_args_list]
        assert not any("Ensured pgvector embedding columns" in m for m in infos), (
            "logged success while every statement failed — exactly what hid this "
            "bug on the deployed app"
        )
        assert any("INCOMPLETE" in m for m in warnings), warnings
