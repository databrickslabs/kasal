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

from unittest.mock import AsyncMock, MagicMock

import pytest

from src.db.session import _VECTOR_EMBEDDING_TABLES, _ensure_pgvector_embedding_columns


def _conn(dialect: str = "postgresql", pgvector: bool = True) -> MagicMock:
    conn = MagicMock()
    conn.engine.dialect.name = dialect
    probe = MagicMock()
    probe.fetchone = MagicMock(return_value=(1,) if pgvector else None)
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
        for table in _VECTOR_EMBEDDING_TABLES:
            assert f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS embedding" in sql

    async def test_knowledge_embeddings_is_covered(self):
        """The specific table whose missing column broke upload."""
        assert "knowledge_embeddings" in _VECTOR_EMBEDDING_TABLES

    async def test_the_column_type_matches_the_model(self):
        conn = _conn()
        await _ensure_pgvector_embedding_columns(conn)
        assert all("vector(1024)" in s for s in _statements(conn) if "ADD COLUMN" in s)

    async def test_an_hnsw_index_is_created(self):
        """Without it, similarity search degrades to a full scan per query."""
        conn = _conn()
        await _ensure_pgvector_embedding_columns(conn)
        sql = " ".join(_statements(conn))
        assert "USING hnsw (embedding vector_cosine_ops)" in sql

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
        assert conn.begin_nested.call_count >= 2 * len(_VECTOR_EMBEDDING_TABLES)

    async def test_one_failing_table_does_not_stop_the_other(self):
        conn = _conn()
        probe = MagicMock()
        probe.fetchone = MagicMock(return_value=(1,))
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

        import src.db.session as session_module

        source = inspect.getsource(session_module)
        assert "_ensure_pgvector_embedding_columns(conn)" in source
