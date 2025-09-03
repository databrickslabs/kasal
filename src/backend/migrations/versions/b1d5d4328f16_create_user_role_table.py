"""create_user_role_table

Revision ID: b1d5d4328f16
Revises: cd2fe0ce549e
Create Date: 2025-06-07 14:52:36.278674

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b1d5d4328f16'
down_revision: Union[str, None] = 'cd2fe0ce549e'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create user_roles table for RBAC
    op.create_table('user_roles',
        sa.Column('id', sa.String(), nullable=False),
        sa.Column('user_id', sa.String(), nullable=True),
        sa.Column('role_id', sa.String(), nullable=True),
        sa.Column('assigned_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('assigned_by', sa.String(), nullable=True),
        sa.ForeignKeyConstraint(['role_id'], ['roles.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('user_id', 'role_id', name='uq_user_role')
    )


def downgrade() -> None:
    # Drop user_roles table
    op.drop_table('user_roles') 