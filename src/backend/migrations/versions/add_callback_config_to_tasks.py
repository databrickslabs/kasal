"""Add callback_config to tasks table

Revision ID: add_callback_config
Revises: add_stop_execution_fields
Create Date: 2024-01-01 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'add_callback_config'
down_revision = 'add_stop_execution_fields'
branch_labels = None
depends_on = None


def upgrade():
    # Add callback_config column to tasks table
    op.add_column('tasks', sa.Column('callback_config', sa.JSON(), nullable=True))


def downgrade():
    # Remove callback_config column from tasks table
    op.drop_column('tasks', 'callback_config')