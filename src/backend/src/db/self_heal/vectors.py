"""pgvector self-heal: the embedding columns and HNSW indexes on PostgreSQL.

Separate from the plain column steps because it depends on the ``vector`` type
being installed and schema-qualified, and because its log line must only claim
success when every statement applied (see the tests).
"""

import logging

from src.db.base import Base
from src.db.self_heal.dialect import _conn_is_sqlite

logger = logging.getLogger(__name__)


def _vector_embedding_tables() -> tuple[str, ...]:
    """Tables whose ``embedding vector(1024)`` column is added AFTER the table.

    DERIVED from the models, not listed. The first version of this helper hardcoded
    ``("documentation_embeddings", "knowledge_embeddings")`` and so missed
    ``workflow_recipes``, which carries the same ``Vector(1024)`` column — the
    deployed app then failed every recipe lookup with

        column workflow_recipes.embedding does not exist

    That is the third time a hand-maintained list of models has drifted from
    reality here. Asking the metadata covers a new vector column on the day it is
    added.

    Computed locally rather than importing the equivalent helper from
    ``services/databricks/lakebase/schema.py``: ``db`` sits below ``services`` and
    must not reach up into it.
    """
    from src.models.documentation_embedding import Vector

    return tuple(
        sorted(
            table.name
            for table in Base.metadata.sorted_tables
            if any(isinstance(column.type, Vector) for column in table.columns)
        )
    )


async def _ensure_pgvector_embedding_columns(conn) -> None:
    """Add back the ``embedding`` column on PostgreSQL when pgvector is present.

    Vector tables are created WITHOUT their vector column, because a deployed app
    cannot install pgvector (``CREATE EXTENSION`` needs ``databricks_superuser``)
    and ``CREATE TABLE ... vector(1024)`` fails outright without it. That keeps the
    rest of the table usable — but nothing added the column back once an instance
    owner HAD enabled the extension, so the ORM kept inserting a column that did
    not exist::

        column "embedding" of relation "knowledge_embeddings" does not exist

    which broke knowledge upload after the text was extracted and 56 chunks were
    embedded — the work was done and then discarded.

    SQLite is skipped: there the column is TEXT holding JSON and ``create_all``
    makes it with the table, and the repository uses a Python-side similarity path.
    """
    if _conn_is_sqlite(conn):
        return
    # Which schema holds the extension. pgvector installs its `vector` type and
    # its operator classes into ONE schema — usually `public` — and this
    # connection's search_path is not guaranteed to include it. The first version
    # of this helper used the bare name and every statement failed with
    # `type "vector" does not exist` / `operator class "vector_cosine_ops" does
    # not exist` even though the extension WAS installed. Qualifying removes the
    # dependency on search_path entirely.
    try:
        result = await conn.exec_driver_sql(
            "SELECT n.nspname FROM pg_extension e "
            "JOIN pg_namespace n ON n.oid = e.extnamespace "
            "WHERE e.extname IN ('vector', 'pgvector')"
        )
        row = result.fetchone()
        if row is None:
            logger.info(
                "pgvector not enabled; embedding columns skipped. An instance owner "
                "can run 'CREATE EXTENSION IF NOT EXISTS vector;' and restart to "
                "enable similarity search."
            )
            return
        ext_schema = row[0]
    except Exception as e:  # noqa: BLE001 — never block startup on the probe
        logger.warning(f"Could not check for pgvector: {e}")
        return

    applied = True
    for table in _vector_embedding_tables():
        # Each statement in its own SAVEPOINT: an orphaned-owner table (42501)
        # must not abort the surrounding self-heal transaction.
        for stmt in (
            f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS embedding "
            f"{ext_schema}.vector(1024)",
            f"CREATE INDEX IF NOT EXISTS idx_{table}_embedding ON {table} "
            f"USING hnsw (embedding {ext_schema}.vector_cosine_ops)",
        ):
            try:
                async with conn.begin_nested():
                    await conn.exec_driver_sql(stmt)
            except Exception as e:  # noqa: BLE001 — best-effort per statement
                applied = False
                logger.warning(f"Could not apply '{stmt[:60]}...': {e}")
    if applied:
        logger.info("Ensured pgvector embedding columns + HNSW indexes")
    else:
        # Do NOT log success when nothing was applied. The first version did, so
        # "Ensured pgvector embedding columns" appeared in the logs while every
        # ALTER had failed and knowledge upload stayed broken.
        logger.warning(
            "pgvector embedding columns INCOMPLETE — knowledge/document similarity "
            "search will not work until the warnings above are resolved"
        )
