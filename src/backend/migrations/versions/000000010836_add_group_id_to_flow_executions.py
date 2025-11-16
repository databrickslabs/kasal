"""Add group_id to flow_executions and flow_node_executions for multi-tenancy

Revision ID: 000000010836
Revises: 665ffadb181e
Create Date: 2025-01-13

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '000000010836'
down_revision = '665ffadb181e'
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Add group_id columns to flow execution tables for multi-tenant isolation."""
    # Add group_id to flow_executions table
    op.add_column('flow_executions', sa.Column('group_id', sa.String(length=100), nullable=True))
    op.create_index(op.f('ix_flow_executions_group_id'), 'flow_executions', ['group_id'], unique=False)

    # Add group_id to flow_node_executions table
    op.add_column('flow_node_executions', sa.Column('group_id', sa.String(length=100), nullable=True))
    op.create_index(op.f('ix_flow_node_executions_group_id'), 'flow_node_executions', ['group_id'], unique=False)


def downgrade() -> None:
    """Remove group_id columns from flow execution tables."""
    # Remove group_id from flow_node_executions table
    op.drop_index(op.f('ix_flow_node_executions_group_id'), table_name='flow_node_executions')
    op.drop_column('flow_node_executions', 'group_id')

    # Remove group_id from flow_executions table
    op.drop_index(op.f('ix_flow_executions_group_id'), table_name='flow_executions')
    op.drop_column('flow_executions', 'group_id')
