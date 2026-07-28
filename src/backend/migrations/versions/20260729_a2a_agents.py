"""add a2a_agents — remote A2A agents Kasal can call

Revision ID: 20260729_a2a_agents
Revises: 20260729_a2a_push
Create Date: 2026-07-29

The outbound direction. Until now A2A was one-way: other agents could call
Kasal, but a Kasal agent had no way to delegate to one. This table is the
registry of remotes an operator has attached, scoped per workspace like every
other credential-bearing configuration.
"""

import sqlalchemy as sa
from alembic import op

revision = "20260729_a2a_agents"
down_revision = "20260729_a2a_push"
branch_labels = None
depends_on = None

_TABLE = "a2a_agents"


def upgrade() -> None:
    if not op.get_context().as_sql:
        inspector = sa.inspect(op.get_bind())
        if _TABLE in inspector.get_table_names():
            return

    op.create_table(
        _TABLE,
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("card_url", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("auth_type", sa.String(length=32), nullable=True),
        sa.Column("encrypted_api_key", sa.Text(), nullable=True),
        sa.Column("enabled", sa.Boolean(), nullable=True),
        sa.Column("global_enabled", sa.Boolean(), nullable=True),
        sa.Column("group_id", sa.String(length=100), nullable=True),
        sa.Column("created_by_email", sa.String(length=255), nullable=True),
        sa.Column("timeout_seconds", sa.Integer(), nullable=True),
        sa.Column("cached_card", sa.JSON(), nullable=True),
        sa.Column("card_fetched_at", sa.DateTime(), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name", "group_id", name="uq_a2aagent_name_group"),
    )
    op.create_index("ix_a2a_agents_group_id", _TABLE, ["group_id"])


def downgrade() -> None:
    if not op.get_context().as_sql:
        inspector = sa.inspect(op.get_bind())
        if _TABLE not in inspector.get_table_names():
            return
    op.drop_index("ix_a2a_agents_group_id", table_name=_TABLE)
    op.drop_table(_TABLE)
