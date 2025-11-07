"""Merge migration heads

Revision ID: 37d59d9ad39b
Revises: 6d23ffae21aa, update_user_roles_3tier
Create Date: 2025-09-21 11:09:19.069802

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '37d59d9ad39b'
down_revision: Union[str, None] = ('6d23ffae21aa', 'update_user_roles_3tier')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass 