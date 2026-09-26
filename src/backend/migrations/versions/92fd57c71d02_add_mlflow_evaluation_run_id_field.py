"""add_mlflow_evaluation_run_id_field

Revision ID: 92fd57c71d02
Revises: eb746dc0cf9d
Create Date: 2025-09-28 20:00:29.570543

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '92fd57c71d02'
down_revision: Union[str, None] = 'eb746dc0cf9d'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add mlflow_evaluation_run_id column to executionhistory table
    op.add_column('executionhistory', sa.Column('mlflow_evaluation_run_id', sa.String(), nullable=True))
    op.create_index('ix_executionhistory_mlflow_evaluation_run_id', 'executionhistory', ['mlflow_evaluation_run_id'])


def downgrade() -> None:
    # Remove mlflow_evaluation_run_id column and index
    op.drop_index('ix_executionhistory_mlflow_evaluation_run_id', table_name='executionhistory')
    op.drop_column('executionhistory', 'mlflow_evaluation_run_id')