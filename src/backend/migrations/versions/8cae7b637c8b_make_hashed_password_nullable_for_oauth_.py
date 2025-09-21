"""drop_hashed_password_column_for_oauth_proxy

Revision ID: 8cae7b637c8b
Revises: 6efefa1a1df7
Create Date: 2025-09-21 14:51:20.988608

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '8cae7b637c8b'
down_revision: Union[str, None] = '6efefa1a1df7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Drop hashed_password column since we're using OAuth proxy authentication
    op.drop_column('users', 'hashed_password')


def downgrade() -> None:
    # Add back hashed_password column (for rollback purposes)
    op.add_column('users', sa.Column('hashed_password', sa.String(), nullable=False, server_default='')) 