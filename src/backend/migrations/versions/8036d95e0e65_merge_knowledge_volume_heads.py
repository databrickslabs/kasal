"""merge_knowledge_volume_heads

Revision ID: 8036d95e0e65
Revises: add_knowledge_volume_fields, add_volume_config
Create Date: 2025-09-12 10:07:34.548385

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '8036d95e0e65'
down_revision: Union[str, None] = ('add_knowledge_volume_fields', 'add_volume_config')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass 