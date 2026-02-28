"""add lakebase memory backend

Revision ID: 51c143594378
Revises: da9793624705
Create Date: 2026-02-28

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '51c143594378'
down_revision = 'da9793624705'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add lakebase_config JSON column to memory_backends table
    op.add_column('memory_backends', sa.Column('lakebase_config', sa.JSON(), nullable=True))


def downgrade() -> None:
    op.drop_column('memory_backends', 'lakebase_config')
