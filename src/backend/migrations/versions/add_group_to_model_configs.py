"""Add group_id and created_by_email to model_configs

Revision ID: add_group_to_model_configs
Revises: 9fbe565a5288
Create Date: 2025-09-11 20:30:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'add_group_to_model_configs'
down_revision: Union[str, None] = '9fbe565a5288'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add group_id column to modelconfig table
    op.add_column('modelconfig', sa.Column('group_id', sa.String(100), nullable=True))
    op.create_index(op.f('ix_modelconfig_group_id'), 'modelconfig', ['group_id'], unique=False)
    
    # Add created_by_email column to modelconfig table
    op.add_column('modelconfig', sa.Column('created_by_email', sa.String(255), nullable=True))


def downgrade() -> None:
    # Remove created_by_email column
    op.drop_column('modelconfig', 'created_by_email')
    
    # Remove group_id column and index
    op.drop_index(op.f('ix_modelconfig_group_id'), table_name='modelconfig')
    op.drop_column('modelconfig', 'group_id')