"""add group_id/file_path scoping + pgvector embedding column to documentation_embeddings

Document and uploaded-knowledge embeddings were migrated off Databricks Vector
Search onto the application's pgvector store (Lakebase in production, SQLite
locally). Two changes are needed on the ``documentation_embeddings`` table:

1. ``group_id`` + ``file_path`` columns so uploaded knowledge can be scoped per
   workspace (tenant isolation) and filtered to a crew's knowledge sources.
   Built-in CrewAI docs leave these NULL.
2. The ``embedding vector(1024)`` column + HNSW index. On Lakebase this table
   was historically created WITHOUT the vector column (Vector Search held the
   vectors), so similarity search via pgvector needs the column added.

All statements are idempotent (``IF NOT EXISTS``) so the migration is safe to
run repeatedly and against tables created by the Lakebase schema service. On
SQLite the embedding column already exists (stored as TEXT) and the vector
index is skipped.

Revision ID: 20260604_doc_emb_scope
Revises: 20260511_add_lakebase_enum
Create Date: 2026-06-04
"""
from alembic import op


revision = "20260604_doc_emb_scope"
down_revision = "20260511_add_lakebase_enum"
branch_labels = None
depends_on = None


def _has_pgvector(bind) -> bool:
    row = bind.exec_driver_sql(
        "SELECT 1 FROM pg_extension WHERE extname IN ('vector', 'pgvector')"
    ).fetchone()
    return row is not None


def upgrade() -> None:
    bind = op.get_bind()
    dialect = bind.dialect.name

    if dialect == "postgresql":
        op.execute(
            "ALTER TABLE documentation_embeddings "
            "ADD COLUMN IF NOT EXISTS group_id VARCHAR(100)"
        )
        op.execute(
            "ALTER TABLE documentation_embeddings "
            "ADD COLUMN IF NOT EXISTS file_path VARCHAR"
        )
        op.execute(
            "CREATE INDEX IF NOT EXISTS idx_doc_emb_group_id "
            "ON documentation_embeddings (group_id)"
        )
        op.execute(
            "CREATE INDEX IF NOT EXISTS idx_doc_emb_file_path "
            "ON documentation_embeddings (file_path)"
        )
        if _has_pgvector(bind):
            op.execute(
                "ALTER TABLE documentation_embeddings "
                "ADD COLUMN IF NOT EXISTS embedding vector(1024)"
            )
            op.execute(
                "CREATE INDEX IF NOT EXISTS idx_doc_emb_embedding "
                "ON documentation_embeddings USING hnsw (embedding vector_cosine_ops)"
            )
        return

    if dialect == "sqlite":
        # SQLite has no ADD COLUMN IF NOT EXISTS; guard via PRAGMA. The embedding
        # column already exists here (Vector maps to TEXT), so it is left alone.
        existing = {
            row[1]
            for row in bind.exec_driver_sql(
                "PRAGMA table_info(documentation_embeddings)"
            ).fetchall()
        }
        if "group_id" not in existing:
            op.execute("ALTER TABLE documentation_embeddings ADD COLUMN group_id VARCHAR(100)")
        if "file_path" not in existing:
            op.execute("ALTER TABLE documentation_embeddings ADD COLUMN file_path VARCHAR")


def downgrade() -> None:
    bind = op.get_bind()
    dialect = bind.dialect.name

    if dialect == "postgresql":
        op.execute("DROP INDEX IF EXISTS idx_doc_emb_group_id")
        op.execute("DROP INDEX IF EXISTS idx_doc_emb_file_path")
        op.execute("DROP INDEX IF EXISTS idx_doc_emb_embedding")
        op.execute("ALTER TABLE documentation_embeddings DROP COLUMN IF EXISTS group_id")
        op.execute("ALTER TABLE documentation_embeddings DROP COLUMN IF EXISTS file_path")
        # The embedding column is intentionally kept on downgrade (it may hold data).
    # SQLite downgrade is a no-op: dropping columns requires a table rebuild.
