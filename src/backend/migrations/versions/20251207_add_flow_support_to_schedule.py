"""Add flow execution support to schedule table.

Revision ID: 20251207_flow_schedule
Revises: 20251203_checkpoint_data
Create Date: 2025-12-07

Adds columns to support both crew and flow scheduling:
- execution_type: 'crew' or 'flow'
- flow_id: UUID reference to saved flow
- nodes: JSON for flow nodes configuration
- edges: JSON for flow edges configuration
- flow_config: JSON for flow-specific configuration
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = '20251207_flow_schedule'
down_revision: Union[str, None] = '20251203_checkpoint_data'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add flow scheduling columns to schedule table."""
    # Add execution_type column with default 'crew'
    op.add_column(
        'schedule',
        sa.Column('execution_type', sa.String(20), nullable=False, server_default='crew')
    )

    # Add flow_id column (UUID for PostgreSQL, String for SQLite)
    # Using dialect-specific approach
    op.add_column(
        'schedule',
        sa.Column('flow_id', postgresql.UUID(as_uuid=True), nullable=True)
    )

    # Add nodes column (JSON for flow node configuration)
    op.add_column(
        'schedule',
        sa.Column('nodes', sa.JSON(), nullable=True)
    )

    # Add edges column (JSON for flow edge configuration)
    op.add_column(
        'schedule',
        sa.Column('edges', sa.JSON(), nullable=True)
    )

    # Add flow_config column (JSON for flow-specific configuration)
    op.add_column(
        'schedule',
        sa.Column('flow_config', sa.JSON(), nullable=True)
    )

    # Create indexes for efficient querying
    op.create_index('ix_schedule_execution_type', 'schedule', ['execution_type'])
    op.create_index('ix_schedule_flow_id', 'schedule', ['flow_id'])


def downgrade() -> None:
    """Remove flow scheduling columns from schedule table."""
    # Drop indexes first
    op.drop_index('ix_schedule_flow_id', table_name='schedule')
    op.drop_index('ix_schedule_execution_type', table_name='schedule')

    # Drop columns
    op.drop_column('schedule', 'flow_config')
    op.drop_column('schedule', 'edges')
    op.drop_column('schedule', 'nodes')
    op.drop_column('schedule', 'flow_id')
    op.drop_column('schedule', 'execution_type')
