"""Add checkpoint columns to execution_history table

Revision ID: add_checkpoint_cols
Revises:
Create Date: 2025-12-03

This migration adds columns to support CrewAI Flow checkpoint/persistence functionality:
- flow_uuid: CrewAI's state.id for @persist decorator
- checkpoint_status: Status of the checkpoint ('active', 'resumed', 'expired', None)
- checkpoint_method: The last checkpointed method name
"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'add_checkpoint_cols'
down_revision = 'add_crew_exec_cfg'  # Chain from the current head
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add checkpoint/persistence columns to executionhistory table
    op.add_column('executionhistory', sa.Column('flow_uuid', sa.String(255), nullable=True))
    op.add_column('executionhistory', sa.Column('checkpoint_status', sa.String(50), nullable=True))
    op.add_column('executionhistory', sa.Column('checkpoint_method', sa.String(255), nullable=True))

    # Create index on flow_uuid for efficient lookups
    op.create_index(op.f('ix_executionhistory_flow_uuid'), 'executionhistory', ['flow_uuid'], unique=False)


def downgrade() -> None:
    # Drop index
    op.drop_index(op.f('ix_executionhistory_flow_uuid'), table_name='executionhistory')

    # Drop columns
    op.drop_column('executionhistory', 'checkpoint_method')
    op.drop_column('executionhistory', 'checkpoint_status')
    op.drop_column('executionhistory', 'flow_uuid')
