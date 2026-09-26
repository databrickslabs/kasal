"""Fix modelconfig unique constraint to allow same key for different groups

Revision ID: fix_modelconfig_unique
Revises: add_group_to_model_configs
Create Date: 2025-09-11 20:38:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'fix_modelconfig_unique'
down_revision: Union[str, None] = 'add_group_to_model_configs'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Drop the unique constraint on key column
    op.drop_constraint('modelconfig_key_key', 'modelconfig', type_='unique')
    
    # Create a new unique constraint on (key, group_id) combination
    # This allows the same key for different groups
    op.create_unique_constraint('uq_modelconfig_key_group', 'modelconfig', ['key', 'group_id'])


def downgrade() -> None:
    # Remove the composite unique constraint
    op.drop_constraint('uq_modelconfig_key_group', 'modelconfig', type_='unique')
    
    # Re-add the unique constraint on key column
    op.create_unique_constraint('modelconfig_key_key', 'modelconfig', ['key'])