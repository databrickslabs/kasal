"""remove_apps_enabled_column

Revision ID: 665ffadb181e
Revises: 92fd57c71d02
Create Date: 2025-10-05 17:49:59.826196

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '665ffadb181e'
down_revision: Union[str, None] = '92fd57c71d02'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Remove apps_enabled column from databricks_config table
    op.drop_column('databricks_config', 'apps_enabled')


def downgrade() -> None:
    # Add apps_enabled column back if rolling back
    op.add_column('databricks_config',
                  sa.Column('apps_enabled', sa.Boolean(), nullable=True, server_default='false')) 