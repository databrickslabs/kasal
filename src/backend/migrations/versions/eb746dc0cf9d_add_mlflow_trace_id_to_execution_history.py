"""add_mlflow_trace_id_to_execution_history

Revision ID: eb746dc0cf9d
Revises: dspy_workspace_001
Create Date: 2025-09-28 19:11:17.134166

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'eb746dc0cf9d'
down_revision: Union[str, None] = 'dspy_workspace_001'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add MLflow trace ID fields to execution history table
    op.add_column('executionhistory', sa.Column('mlflow_trace_id', sa.String(), nullable=True))
    op.add_column('executionhistory', sa.Column('mlflow_experiment_name', sa.String(), nullable=True))

    # Add index on mlflow_trace_id for faster lookups
    op.create_index('ix_executionhistory_mlflow_trace_id', 'executionhistory', ['mlflow_trace_id'])


def downgrade() -> None:
    # Remove index and columns
    op.drop_index('ix_executionhistory_mlflow_trace_id', 'executionhistory')
    op.drop_column('executionhistory', 'mlflow_experiment_name')
    op.drop_column('executionhistory', 'mlflow_trace_id')