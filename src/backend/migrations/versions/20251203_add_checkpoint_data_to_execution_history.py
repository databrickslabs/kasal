"""Add checkpoint_data column to executionhistory table for crew-level checkpoints.

Revision ID: 20251203_checkpoint_data
Revises:
Create Date: 2025-12-03

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '20251203_checkpoint_data'
down_revision: Union[str, None] = 'add_checkpoint_cols'  # Chain from checkpoint columns migration
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add checkpoint_data JSON column to executionhistory table."""
    # Use batch mode for SQLite compatibility
    with op.batch_alter_table('executionhistory', schema=None) as batch_op:
        batch_op.add_column(
            sa.Column('checkpoint_data', sa.JSON(), nullable=True, default=None)
        )


def downgrade() -> None:
    """Remove checkpoint_data column from executionhistory table."""
    with op.batch_alter_table('executionhistory', schema=None) as batch_op:
        batch_op.drop_column('checkpoint_data')
