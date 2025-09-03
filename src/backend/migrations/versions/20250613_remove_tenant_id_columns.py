"""remove tenant_id columns from all tables

Revision ID: 20250613_remove_tenant_id
Revises: 
Create Date: 2025-06-13 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '20250613_remove_tenant_id'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Remove tenant_id columns from all tables (PostgreSQL only)"""
    
    # List of tables that have tenant_id columns to remove
    tables_with_tenant_id = [
        'agents',
        'llm_usage_billing', 
        'billing_periods',
        'billing_alerts',
        'crews',
        'executionhistory',
        'execution_logs',
        'flows',
        'llmlog',
        'tasks',
        'prompttemplate',
        'tools',
        'chat_history'
    ]
    
    # Drop tenant_id columns from all tables
    for table_name in tables_with_tenant_id:
        try:
            op.drop_column(table_name, 'tenant_id')
        except Exception as e:
            # Continue if column doesn't exist
            print(f"Could not drop tenant_id from {table_name}: {e}")
    
    # Also remove tenant_email columns where they exist
    tables_with_tenant_email = [
        'executionhistory',
        'execution_logs', 
        'llmlog',
        'chat_history'
    ]
    
    for table_name in tables_with_tenant_email:
        try:
            op.drop_column(table_name, 'tenant_email')
        except Exception as e:
            # Continue if column doesn't exist
            print(f"Could not drop tenant_email from {table_name}: {e}")
    
    # Drop tenant-related indexes if they exist
    tenant_indexes = [
        'idx_execution_logs_tenant_timestamp',
        'idx_execution_logs_tenant_exec_id'
    ]
    
    for index_name in tenant_indexes:
        try:
            op.drop_index(index_name, table_name='execution_logs')
        except Exception as e:
            # Continue if index doesn't exist
            print(f"Could not drop index {index_name}: {e}")


def downgrade() -> None:
    """Add back tenant_id columns (not implemented as tenant system is being removed)"""
    # Not implementing downgrade as tenant system is being permanently removed
    pass