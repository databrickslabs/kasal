"""add stop execution fields

Revision ID: add_stop_execution_fields
Revises: add_global_enabled_mcp
Create Date: 2025-01-04

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'add_stop_execution_fields'
down_revision = 'add_global_enabled_mcp'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add stop execution fields to executionhistory table
    op.add_column('executionhistory', sa.Column('stopped_at', sa.DateTime(), nullable=True))
    op.add_column('executionhistory', sa.Column('stop_reason', sa.String(), nullable=True))
    op.add_column('executionhistory', sa.Column('stop_requested_by', sa.String(length=255), nullable=True))
    op.add_column('executionhistory', sa.Column('partial_results', postgresql.JSON(astext_type=sa.Text()), nullable=True))
    op.add_column('executionhistory', sa.Column('is_stopping', sa.Boolean(), nullable=False, server_default='false'))
    
    # Create index on is_stopping for quick queries of stopping executions
    op.create_index('idx_executionhistory_is_stopping', 'executionhistory', ['is_stopping'], unique=False, postgresql_where=sa.text('is_stopping = TRUE'))


def downgrade() -> None:
    # Drop index first
    op.drop_index('idx_executionhistory_is_stopping', table_name='executionhistory')
    
    # Remove stop execution fields from executionhistory table
    op.drop_column('executionhistory', 'is_stopping')
    op.drop_column('executionhistory', 'partial_results')
    op.drop_column('executionhistory', 'stop_requested_by')
    op.drop_column('executionhistory', 'stop_reason')
    op.drop_column('executionhistory', 'stopped_at')