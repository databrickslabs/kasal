"""Merge heads

Revision ID: 7a02c9c7ac15
Revises: 4568c86db33a, add_stop_execution_fields
Create Date: 2025-09-10 21:05:20.614417

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '7a02c9c7ac15'
down_revision: Union[str, None] = ('4568c86db33a', 'add_stop_execution_fields')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass 