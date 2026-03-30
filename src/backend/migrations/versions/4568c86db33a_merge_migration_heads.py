"""merge_migration_heads

Revision ID: 4568c86db33a
Revises: 20250613_remove_tenant_id, add_global_enabled_mcp
Create Date: 2025-09-04 10:45:30.021673

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '4568c86db33a'
down_revision: Union[str, None] = ('20250613_remove_tenant_id', 'add_global_enabled_mcp')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass 