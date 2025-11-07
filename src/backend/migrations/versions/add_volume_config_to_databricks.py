"""Add volume configuration fields to databricks_configs

Revision ID: add_volume_config
Revises: 
Create Date: 2025-09-12 08:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'add_volume_config'
down_revision = '48ad0d8dcc5f'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add volume configuration columns
    op.add_column('databricksconfig', sa.Column('volume_enabled', sa.Boolean(), nullable=True, server_default='false'))
    op.add_column('databricksconfig', sa.Column('volume_path', sa.String(), nullable=True))
    op.add_column('databricksconfig', sa.Column('volume_file_format', sa.String(), nullable=True, server_default='json'))
    op.add_column('databricksconfig', sa.Column('volume_create_date_dirs', sa.Boolean(), nullable=True, server_default='true'))


def downgrade() -> None:
    # Remove volume configuration columns
    op.drop_column('databricksconfig', 'volume_create_date_dirs')
    op.drop_column('databricksconfig', 'volume_file_format')
    op.drop_column('databricksconfig', 'volume_path')
    op.drop_column('databricksconfig', 'volume_enabled')