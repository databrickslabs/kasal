"""add a2a_push_configs — webhook registrations for A2A task notifications

Revision ID: 20260729_a2a_push
Revises: 20260728_crew_publications
Create Date: 2026-07-29

A2A lets a client register a webhook per task rather than holding a stream open.
That matters more here than for most agents: crew runs take minutes and the
budget work contemplates an hour, so a held connection is the weakest option and
push is what makes a long run practical.
"""

import sqlalchemy as sa
from alembic import op

revision = "20260729_a2a_push"
down_revision = "20260728_crew_publications"
branch_labels = None
depends_on = None

_TABLE = "a2a_push_configs"


def upgrade() -> None:
    if not op.get_context().as_sql:
        inspector = sa.inspect(op.get_bind())
        if _TABLE in inspector.get_table_names():
            return

    op.create_table(
        _TABLE,
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("task_id", sa.String(length=255), nullable=False),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("token", sa.String(length=512), nullable=True),
        sa.Column("secret", sa.String(length=512), nullable=True),
        sa.Column("group_id", sa.String(length=100), nullable=False),
        sa.Column("created_by_email", sa.String(length=255), nullable=True),
        sa.Column("last_status", sa.String(length=32), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("last_attempt_at", sa.DateTime(), nullable=True),
        sa.Column("consecutive_failures", sa.Integer(), nullable=False,
                  server_default="0"),
        sa.Column("created_at", sa.DateTime(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_a2a_push_configs_task_id", _TABLE, ["task_id"])
    op.create_index("ix_a2a_push_configs_group_id", _TABLE, ["group_id"])


def downgrade() -> None:
    if not op.get_context().as_sql:
        inspector = sa.inspect(op.get_bind())
        if _TABLE not in inspector.get_table_names():
            return
    op.drop_index("ix_a2a_push_configs_group_id", table_name=_TABLE)
    op.drop_index("ix_a2a_push_configs_task_id", table_name=_TABLE)
    op.drop_table(_TABLE)
