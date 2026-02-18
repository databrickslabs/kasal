"""merge_heads

Revision ID: da9793624705
Revises: 8f708c7a24de, c3f4d5eef044
Create Date: 2026-02-16 15:44:31.715236

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'da9793624705'
down_revision: Union[str, None] = ('8f708c7a24de', 'c3f4d5eef044')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass 