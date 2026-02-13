"""merge migration branches

Revision ID: 8f708c7a24de
Revises: 20260209_cache, 3e3762648db4
Create Date: 2026-02-13 07:37:50.310894

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '8f708c7a24de'
down_revision: Union[str, None] = ('20260209_cache', '3e3762648db4')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass 