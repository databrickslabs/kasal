"""add_run_name_to_flow_executions

Revision ID: 90f4428dfe2f
Revises: 000000010836
Create Date: 2025-11-16 11:01:30.386275

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '90f4428dfe2f'
down_revision: Union[str, None] = '000000010836'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add run_name column to flow_executions table
    op.add_column('flow_executions', sa.Column('run_name', sa.String(), nullable=True))


def downgrade() -> None:
    # Remove run_name column from flow_executions table
    op.drop_column('flow_executions', 'run_name') 