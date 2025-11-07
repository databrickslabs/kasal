"""Merge callback_config and temperature branches

Revision ID: 48ad0d8dcc5f
Revises: 52262b55c54d, add_callback_config
Create Date: 2025-09-11 23:07:55.002679

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '48ad0d8dcc5f'
down_revision: Union[str, None] = ('52262b55c54d', 'add_callback_config')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass 