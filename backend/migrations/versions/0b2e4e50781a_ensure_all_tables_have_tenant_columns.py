"""ensure_all_tables_have_tenant_columns

Revision ID: 0b2e4e50781a
Revises: 2bc5aee43ad9
Create Date: 2025-06-06 17:20:15.012345

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '0b2e4e50781a'
down_revision: Union[str, None] = '2bc5aee43ad9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Ensure all core tables have tenant_id and created_by_email columns."""
    
    # Tables that need tenant isolation
    tables_to_update = [
        'agents',
        'tasks', 
        'crews',
        'flows',
        'tools',
        'executionhistory',
        'execution_trace',
        'flow_executions',
        'schedule'
    ]
    
    conn = op.get_bind()
    
    for table_name in tables_to_update:
        print(f"Checking table: {table_name}")
        
        # Check if tenant_id column exists
        result = conn.execute(
            sa.text(f"SELECT column_name FROM information_schema.columns WHERE table_name='{table_name}' AND column_name='tenant_id'")
        )
        if not result.fetchone():
            print(f"Adding tenant_id column to {table_name}")
            try:
                with op.batch_alter_table(table_name, schema=None) as batch_op:
                    batch_op.add_column(sa.Column('tenant_id', sa.String(length=100), nullable=True))
                    batch_op.create_index(f'ix_{table_name}_tenant_id', ['tenant_id'])
            except Exception as e:
                print(f"Error adding tenant_id to {table_name}: {e}")
        else:
            print(f"tenant_id column already exists in {table_name}")
        
        # Check if created_by_email column exists
        result = conn.execute(
            sa.text(f"SELECT column_name FROM information_schema.columns WHERE table_name='{table_name}' AND column_name='created_by_email'")
        )
        if not result.fetchone():
            print(f"Adding created_by_email column to {table_name}")
            try:
                with op.batch_alter_table(table_name, schema=None) as batch_op:
                    batch_op.add_column(sa.Column('created_by_email', sa.String(length=255), nullable=True))
                    batch_op.create_index(f'ix_{table_name}_created_by_email', ['created_by_email'])
            except Exception as e:
                print(f"Error adding created_by_email to {table_name}: {e}")
        else:
            print(f"created_by_email column already exists in {table_name}")
        
        # Also add tenant_email for execution tracking tables
        if table_name in ['executionhistory', 'execution_trace', 'flow_executions']:
            result = conn.execute(
                sa.text(f"SELECT column_name FROM information_schema.columns WHERE table_name='{table_name}' AND column_name='tenant_email'")
            )
            if not result.fetchone():
                print(f"Adding tenant_email column to {table_name}")
                try:
                    with op.batch_alter_table(table_name, schema=None) as batch_op:
                        batch_op.add_column(sa.Column('tenant_email', sa.String(length=255), nullable=True))
                        batch_op.create_index(f'ix_{table_name}_tenant_email', ['tenant_email'])
                except Exception as e:
                    print(f"Error adding tenant_email to {table_name}: {e}")
            else:
                print(f"tenant_email column already exists in {table_name}")


def downgrade() -> None:
    """Remove tenant columns from all tables."""
    
    tables_to_update = [
        'agents',
        'tasks', 
        'crews',
        'flows',
        'tools',
        'executionhistory',
        'execution_trace',
        'flow_executions',
        'schedule'
    ]
    
    for table_name in tables_to_update:
        try:
            with op.batch_alter_table(table_name, schema=None) as batch_op:
                # Drop indexes first
                try:
                    batch_op.drop_index(f'ix_{table_name}_created_by_email')
                except:
                    pass
                try:
                    batch_op.drop_index(f'ix_{table_name}_tenant_id')
                except:
                    pass
                try:
                    batch_op.drop_index(f'ix_{table_name}_tenant_email')
                except:
                    pass
                
                # Drop columns
                try:
                    batch_op.drop_column('created_by_email')
                except:
                    pass
                try:
                    batch_op.drop_column('tenant_id')
                except:
                    pass
                try:
                    batch_op.drop_column('tenant_email')
                except:
                    pass
        except Exception as e:
            print(f"Error in downgrade for {table_name}: {e}")