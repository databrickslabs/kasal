"""unify memory backend for cognitive memory

Drops the legacy per-memory-type flags (``enable_short_term``,
``enable_long_term``, ``enable_entity``, ``enable_relationship_retrieval``)
that the CrewAI 1.10+ unified cognitive memory no longer uses, and adds a
``cognitive_config`` JSON column for tuning knobs (composite-score weights,
consolidation thresholds, recall depth, etc.).

Revision ID: 20260424_unify_memory
Revises: 51c143594378
Create Date: 2026-04-24
"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20260424_unify_memory"
down_revision = "51c143594378"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "memory_backends",
        sa.Column("cognitive_config", sa.JSON(), nullable=True),
    )
    with op.batch_alter_table("memory_backends") as batch:
        batch.drop_column("enable_short_term")
        batch.drop_column("enable_long_term")
        batch.drop_column("enable_entity")
        batch.drop_column("enable_relationship_retrieval")


def downgrade() -> None:
    with op.batch_alter_table("memory_backends") as batch:
        batch.add_column(
            sa.Column(
                "enable_relationship_retrieval",
                sa.Boolean(),
                nullable=True,
                server_default=sa.false(),
            )
        )
        batch.add_column(
            sa.Column(
                "enable_entity",
                sa.Boolean(),
                nullable=True,
                server_default=sa.true(),
            )
        )
        batch.add_column(
            sa.Column(
                "enable_long_term",
                sa.Boolean(),
                nullable=True,
                server_default=sa.true(),
            )
        )
        batch.add_column(
            sa.Column(
                "enable_short_term",
                sa.Boolean(),
                nullable=True,
                server_default=sa.true(),
            )
        )
    op.drop_column("memory_backends", "cognitive_config")
