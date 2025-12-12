"""add_auth_method_to_powerbi_config

Revision ID: 538129a124e7
Revises: 20251006_add_powerbi
Create Date: 2025-10-07 11:40:53.597866

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '538129a124e7'
down_revision: Union[str, None] = '20251006_add_powerbi'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add auth_method column to powerbiconfig table
    op.add_column('powerbiconfig', sa.Column('auth_method', sa.String(), nullable=False, server_default='username_password'))


def downgrade() -> None:
    # Remove auth_method column from powerbiconfig table
    op.drop_column('powerbiconfig', 'auth_method') 