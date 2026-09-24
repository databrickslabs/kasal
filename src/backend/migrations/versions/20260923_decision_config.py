"""Add an opt-in Jev configuration per workspace."""

import sqlalchemy as sa
from alembic import op

revision = "20260923_decision_config"
down_revision = "20260909_builder_sessions"
branch_labels = None
depends_on = None


def upgrade():
    if "decision_config" not in sa.inspect(op.get_bind()).get_table_names():
        op.create_table(
            "decision_config",
            sa.Column(
                "group_id",
                sa.String(100),
                sa.ForeignKey("groups.id", ondelete="CASCADE"),
                primary_key=True,
            ),
            sa.Column(
                "enabled", sa.Boolean(), nullable=False, server_default=sa.false()
            ),
        )


def downgrade():
    op.drop_table("decision_config")
