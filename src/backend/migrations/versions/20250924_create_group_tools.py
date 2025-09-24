"""Create group_tools mapping table

Revision ID: 20250924_create_group_tools
Revises: a2521320b527
Create Date: 2025-09-24

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "20250924_create_group_tools"
down_revision: Union[str, None] = "a2521320b527"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "group_tools",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("tool_id", sa.Integer(), sa.ForeignKey("tools.id", ondelete="CASCADE"), nullable=False, index=True),
        sa.Column("group_id", sa.String(length=100), nullable=False, index=True),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.text("0")),
        sa.Column("config", sa.JSON(), nullable=True),
        sa.Column("credentials_status", sa.String(length=50), nullable=False, server_default="unknown"),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column(
            "updated_at",
            sa.DateTime(),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.UniqueConstraint("tool_id", "group_id", name="uq_group_tools_tool_group"),
    )
    # Composite index for queries by group and tool
    op.create_index("ix_group_tools_group_tool", "group_tools", ["group_id", "tool_id"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_group_tools_group_tool", table_name="group_tools")
    op.drop_table("group_tools")

