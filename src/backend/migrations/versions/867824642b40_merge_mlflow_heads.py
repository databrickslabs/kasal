"""merge_mlflow_heads

Revision ID: 867824642b40
Revises: 1b22cb80b41f, cf0a3479e307
Create Date: 2026-02-09 07:35:55.039719

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '867824642b40'
down_revision: Union[str, None] = ('1b22cb80b41f', 'cf0a3479e307')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass 