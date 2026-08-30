"""Per-membership builder-capability overrides.

group_users gains two NULLABLE booleans: NULL = derive from the role
(operator -> no builders, editor/admin -> builders); an explicit value set
from the Access screen wins over the role.

Revision ID: 20260829_builder_caps
Revises: 20260825_trigger_queue
"""

import sqlalchemy as sa
from alembic import op

revision = "20260829_builder_caps"
down_revision = "20260825_trigger_queue"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "group_users", sa.Column("allow_agent_builder", sa.Boolean(), nullable=True)
    )
    op.add_column(
        "group_users", sa.Column("allow_flow_builder", sa.Boolean(), nullable=True)
    )


def downgrade() -> None:
    op.drop_column("group_users", "allow_flow_builder")
    op.drop_column("group_users", "allow_agent_builder")
