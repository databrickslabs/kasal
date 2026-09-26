"""add LAKEBASE to memorybackendtypeenum

The Lakebase memory backend was added via revision ``51c143594378``,
which created the ``lakebase_config`` column but never extended the
existing ``memorybackendtypeenum`` Postgres enum. As a result, inserting
a row with ``backend_type='LAKEBASE'`` fails on PostgreSQL/Lakebase with
``invalid input value for enum`` and the save silently rolls back —
saved configs disappear on refresh.

This migration adds ``'LAKEBASE'`` to the enum. SQLite stores the
column as plain VARCHAR so the operation is a no-op there.

Revision ID: 20260511_add_lakebase_enum
Revises: 20260424_unify_memory
Create Date: 2026-05-11
"""
from alembic import op


revision = "20260511_add_lakebase_enum"
down_revision = "20260424_unify_memory"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return

    with op.get_context().autocommit_block():
        op.execute(
            "ALTER TYPE memorybackendtypeenum ADD VALUE IF NOT EXISTS 'LAKEBASE'"
        )


def downgrade() -> None:
    # PostgreSQL does not support removing values from an enum directly.
    pass
