"""add_tenant_columns_to_existing_tables

Revision ID: 96bc3fb62ba7
Revises: add_tenant_support
Create Date: 2025-06-06 16:21:21.593073

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '96bc3fb62ba7'
down_revision: Union[str, None] = 'add_tenant_support'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add tenant_id and tenant_email columns to existing tables."""
    
    # Add tenant columns to execution tracking tables
    with op.batch_alter_table('executionhistory', schema=None) as batch_op:
        batch_op.add_column(sa.Column('tenant_id', sa.String(length=100), nullable=True))
        batch_op.add_column(sa.Column('tenant_email', sa.String(length=255), nullable=True))
        batch_op.create_index('ix_executionhistory_tenant_id', ['tenant_id'])
        batch_op.create_index('ix_executionhistory_tenant_email', ['tenant_email'])
    
    with op.batch_alter_table('execution_logs', schema=None) as batch_op:
        batch_op.add_column(sa.Column('tenant_id', sa.String(length=100), nullable=True))
        batch_op.add_column(sa.Column('tenant_email', sa.String(length=255), nullable=True))
        batch_op.create_index('ix_execution_logs_tenant_id', ['tenant_id'])
        batch_op.create_index('ix_execution_logs_tenant_email', ['tenant_email'])
        batch_op.create_index('idx_execution_logs_tenant_timestamp', ['tenant_id', 'timestamp'])
        batch_op.create_index('idx_execution_logs_tenant_exec_id', ['tenant_id', 'execution_id'])
    
    with op.batch_alter_table('execution_trace', schema=None) as batch_op:
        batch_op.add_column(sa.Column('tenant_id', sa.String(length=100), nullable=True))
        batch_op.add_column(sa.Column('tenant_email', sa.String(length=255), nullable=True))
        batch_op.create_index('ix_execution_trace_tenant_id', ['tenant_id'])
        batch_op.create_index('ix_execution_trace_tenant_email', ['tenant_email'])
    
    # Add tenant columns to core workflow entities
    with op.batch_alter_table('agents', schema=None) as batch_op:
        batch_op.add_column(sa.Column('tenant_id', sa.String(length=100), nullable=True))
        batch_op.add_column(sa.Column('created_by_email', sa.String(length=255), nullable=True))
        batch_op.create_index('ix_agents_tenant_id', ['tenant_id'])
        batch_op.create_index('ix_agents_created_by_email', ['created_by_email'])
    
    with op.batch_alter_table('tasks', schema=None) as batch_op:
        batch_op.add_column(sa.Column('tenant_id', sa.String(length=100), nullable=True))
        batch_op.add_column(sa.Column('created_by_email', sa.String(length=255), nullable=True))
        batch_op.create_index('ix_tasks_tenant_id', ['tenant_id'])
        batch_op.create_index('ix_tasks_created_by_email', ['created_by_email'])
    
    with op.batch_alter_table('crews', schema=None) as batch_op:
        batch_op.add_column(sa.Column('tenant_id', sa.String(length=100), nullable=True))
        batch_op.add_column(sa.Column('created_by_email', sa.String(length=255), nullable=True))
        batch_op.create_index('ix_crews_tenant_id', ['tenant_id'])
        batch_op.create_index('ix_crews_created_by_email', ['created_by_email'])
    
    with op.batch_alter_table('flows', schema=None) as batch_op:
        batch_op.add_column(sa.Column('tenant_id', sa.String(length=100), nullable=True))
        batch_op.add_column(sa.Column('created_by_email', sa.String(length=255), nullable=True))
        batch_op.create_index('ix_flows_tenant_id', ['tenant_id'])
        batch_op.create_index('ix_flows_created_by_email', ['created_by_email'])
    
    with op.batch_alter_table('tools', schema=None) as batch_op:
        batch_op.add_column(sa.Column('tenant_id', sa.String(length=100), nullable=True))
        batch_op.add_column(sa.Column('created_by_email', sa.String(length=255), nullable=True))
        batch_op.create_index('ix_tools_tenant_id', ['tenant_id'])
        batch_op.create_index('ix_tools_created_by_email', ['created_by_email'])
    
    with op.batch_alter_table('prompttemplate', schema=None) as batch_op:
        batch_op.add_column(sa.Column('tenant_id', sa.String(length=100), nullable=True))
        batch_op.add_column(sa.Column('created_by_email', sa.String(length=255), nullable=True))
        batch_op.create_index('ix_prompttemplate_tenant_id', ['tenant_id'])
        batch_op.create_index('ix_prompttemplate_created_by_email', ['created_by_email'])


def downgrade() -> None:
    """Remove tenant columns from existing tables."""
    
    # Remove tenant columns from prompttemplate
    with op.batch_alter_table('prompttemplate', schema=None) as batch_op:
        batch_op.drop_index('ix_prompttemplate_created_by_email')
        batch_op.drop_index('ix_prompttemplate_tenant_id')
        batch_op.drop_column('created_by_email')
        batch_op.drop_column('tenant_id')
    
    # Remove tenant columns from tools
    with op.batch_alter_table('tools', schema=None) as batch_op:
        batch_op.drop_index('ix_tools_created_by_email')
        batch_op.drop_index('ix_tools_tenant_id')
        batch_op.drop_column('created_by_email')
        batch_op.drop_column('tenant_id')
    
    # Remove tenant columns from flows
    with op.batch_alter_table('flows', schema=None) as batch_op:
        batch_op.drop_index('ix_flows_created_by_email')
        batch_op.drop_index('ix_flows_tenant_id')
        batch_op.drop_column('created_by_email')
        batch_op.drop_column('tenant_id')
    
    # Remove tenant columns from crews
    with op.batch_alter_table('crews', schema=None) as batch_op:
        batch_op.drop_index('ix_crews_created_by_email')
        batch_op.drop_index('ix_crews_tenant_id')
        batch_op.drop_column('created_by_email')
        batch_op.drop_column('tenant_id')
    
    # Remove tenant columns from tasks
    with op.batch_alter_table('tasks', schema=None) as batch_op:
        batch_op.drop_index('ix_tasks_created_by_email')
        batch_op.drop_index('ix_tasks_tenant_id')
        batch_op.drop_column('created_by_email')
        batch_op.drop_column('tenant_id')
    
    # Remove tenant columns from agents
    with op.batch_alter_table('agents', schema=None) as batch_op:
        batch_op.drop_index('ix_agents_created_by_email')
        batch_op.drop_index('ix_agents_tenant_id')
        batch_op.drop_column('created_by_email')
        batch_op.drop_column('tenant_id')
    
    # Remove tenant columns from execution tracking tables
    with op.batch_alter_table('execution_trace', schema=None) as batch_op:
        batch_op.drop_index('ix_execution_trace_tenant_email')
        batch_op.drop_index('ix_execution_trace_tenant_id')
        batch_op.drop_column('tenant_email')
        batch_op.drop_column('tenant_id')
    
    with op.batch_alter_table('execution_logs', schema=None) as batch_op:
        batch_op.drop_index('idx_execution_logs_tenant_exec_id')
        batch_op.drop_index('idx_execution_logs_tenant_timestamp')
        batch_op.drop_index('ix_execution_logs_tenant_email')
        batch_op.drop_index('ix_execution_logs_tenant_id')
        batch_op.drop_column('tenant_email')
        batch_op.drop_column('tenant_id')
    
    with op.batch_alter_table('executionhistory', schema=None) as batch_op:
        batch_op.drop_index('ix_executionhistory_tenant_email')
        batch_op.drop_index('ix_executionhistory_tenant_id')
        batch_op.drop_column('tenant_email')
        batch_op.drop_column('tenant_id') 