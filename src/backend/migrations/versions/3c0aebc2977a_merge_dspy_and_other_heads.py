"""merge_dspy_and_other_heads

Revision ID: 3c0aebc2977a
Revises: 20250924_create_group_tools, 20250925_eval_judge, dspy_001, alter_prompttemplate_unique
Create Date: 2025-09-27 13:42:23.580697

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '3c0aebc2977a'
down_revision: Union[str, None] = ('20250924_create_group_tools', '20250925_eval_judge', 'dspy_001', 'alter_prompttemplate_unique')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass 