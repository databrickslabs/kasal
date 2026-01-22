"""merge_heads_for_flow_id

Revision ID: 7b636e814fa7
Revises: 20251207_flow_schedule, 20260112_add_hitl, 538129a124e7
Create Date: 2026-01-13 10:32:33.442781

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '7b636e814fa7'
down_revision: Union[str, None] = ('20251207_flow_schedule', '20260112_add_hitl', '538129a124e7')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass 