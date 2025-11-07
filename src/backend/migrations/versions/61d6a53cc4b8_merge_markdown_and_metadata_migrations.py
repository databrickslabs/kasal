"""merge_markdown_and_metadata_migrations

Revision ID: 61d6a53cc4b8
Revises: 789abc123def, 68d79ef
Create Date: 2025-05-26 08:42:35.570761

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '61d6a53cc4b8'
down_revision: Union[str, None] = ('789abc123def', '68d79ef')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass 