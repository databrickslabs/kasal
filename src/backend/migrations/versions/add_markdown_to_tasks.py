"""Add markdown column to tasks table

Revision ID: 789abc123def
Revises: f1224c788466
Create Date: 2025-05-26 18:40:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '789abc123def'
down_revision = 'f1224c788466'
branch_labels = None
depends_on = None


def upgrade():
    # Add markdown column to tasks table
    op.add_column('tasks', sa.Column('markdown', sa.Boolean(), nullable=False, server_default='false'))


def downgrade():
    # Remove markdown column from tasks table
    op.drop_column('tasks', 'markdown') 