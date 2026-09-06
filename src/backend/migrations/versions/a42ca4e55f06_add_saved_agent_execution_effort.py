"""add saved agent execution effort

Revision ID: a42ca4e55f06
Revises: 20260829_builder_caps
Create Date: 2026-09-06 21:57:06.672090

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "a42ca4e55f06"
down_revision: Union[str, None] = "20260829_builder_caps"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Startup self-healing may already have added the column.
    if "execution_effort" not in {
        c["name"] for c in sa.inspect(op.get_bind()).get_columns("agents")
    }:
        op.add_column("agents", sa.Column("execution_effort", sa.JSON(), nullable=True))


def downgrade() -> None:
    op.drop_column("agents", "execution_effort")
