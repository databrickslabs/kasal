"""add cascade delete on flows crew_id

Revision ID: c3f4d5eef044
Revises: a1f2a3cbd822
Create Date: 2026-02-10

Adds ON DELETE CASCADE to flows.crew_id foreign key so that deleting
a crew automatically deletes its associated flows.
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "c3f4d5eef044"
down_revision = "a1f2a3cbd822"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Use batch_alter_table for SQLite compatibility (recreates the table)
    with op.batch_alter_table("flows", schema=None) as batch_op:
        batch_op.drop_constraint("flows_crew_id_fkey", type_="foreignkey")
        batch_op.create_foreign_key(
            "flows_crew_id_fkey",
            "crews",
            ["crew_id"],
            ["id"],
            ondelete="CASCADE",
        )


def downgrade() -> None:
    with op.batch_alter_table("flows", schema=None) as batch_op:
        batch_op.drop_constraint("flows_crew_id_fkey", type_="foreignkey")
        batch_op.create_foreign_key(
            "flows_crew_id_fkey",
            "crews",
            ["crew_id"],
            ["id"],
        )
