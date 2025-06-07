"""Add tenant support for multi-tenant isolation

Revision ID: add_tenant_support
Revises: 
Create Date: 2025-01-06 10:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = 'add_tenant_support'
down_revision = 'add_encrypted_token'  # Chain from the latest migration
branch_labels = None
depends_on = None


def upgrade():
    """Add tenant support tables and columns."""
    
    # Create tenants table with string status for simplicity
    op.create_table('tenants',
        sa.Column('id', sa.String(length=100), nullable=False),
        sa.Column('name', sa.String(length=255), nullable=False),
        sa.Column('email_domain', sa.String(length=255), nullable=False),
        sa.Column('status', sa.String(length=50), nullable=False, server_default='ACTIVE'),
        sa.Column('description', sa.String(length=500), nullable=True),
        sa.Column('auto_created', sa.Boolean(), nullable=False, server_default='false'),
        sa.Column('created_by_email', sa.String(length=255), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.text('NOW()')),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.text('NOW()')),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('email_domain')
    )
    
    # Create tenant_users table with string enums for simplicity
    op.create_table('tenant_users',
        sa.Column('id', sa.String(length=100), nullable=False),
        sa.Column('tenant_id', sa.String(length=100), nullable=False),
        sa.Column('user_id', sa.String(length=255), nullable=False),
        sa.Column('role', sa.String(length=50), nullable=False, server_default='USER'),
        sa.Column('status', sa.String(length=50), nullable=False, server_default='ACTIVE'),
        sa.Column('joined_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.text('NOW()')),
        sa.Column('auto_created', sa.Boolean(), nullable=False, server_default='false'),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.text('NOW()')),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.text('NOW()')),
        sa.ForeignKeyConstraint(['tenant_id'], ['tenants.id'], ),
        sa.PrimaryKeyConstraint('id')
    )
    
    # Add indexes for tenant_users
    op.create_index('ix_tenant_users_tenant_id', 'tenant_users', ['tenant_id'])
    op.create_index('ix_tenant_users_user_id', 'tenant_users', ['user_id'])
    op.create_unique_constraint('uq_tenant_users_tenant_user', 'tenant_users', ['tenant_id', 'user_id'])
    
    # Add tenant_id columns to execution tracking tables
    with op.batch_alter_table('execution_logs', schema=None) as batch_op:
        batch_op.add_column(sa.Column('tenant_id', sa.String(length=100), nullable=True))
        batch_op.add_column(sa.Column('tenant_email', sa.String(length=255), nullable=True))
        batch_op.create_index('ix_execution_logs_tenant_id', ['tenant_id'])
        batch_op.create_index('ix_execution_logs_tenant_email', ['tenant_email'])
        batch_op.create_index('idx_execution_logs_tenant_timestamp', ['tenant_id', 'timestamp'])
        batch_op.create_index('idx_execution_logs_tenant_exec_id', ['tenant_id', 'execution_id'])
    
    with op.batch_alter_table('executionhistory', schema=None) as batch_op:
        batch_op.add_column(sa.Column('tenant_id', sa.String(length=100), nullable=True))
        batch_op.add_column(sa.Column('tenant_email', sa.String(length=255), nullable=True))
        batch_op.create_index('ix_executionhistory_tenant_id', ['tenant_id'])
        batch_op.create_index('ix_executionhistory_tenant_email', ['tenant_email'])
    
    with op.batch_alter_table('execution_trace', schema=None) as batch_op:
        batch_op.add_column(sa.Column('tenant_id', sa.String(length=100), nullable=True))
        batch_op.add_column(sa.Column('tenant_email', sa.String(length=255), nullable=True))
        batch_op.create_index('ix_execution_trace_tenant_id', ['tenant_id'])
        batch_op.create_index('ix_execution_trace_tenant_email', ['tenant_email'])
    
    # Add tenant_id columns to core workflow entities
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
    
    # Create a default tenant for existing data (if any)
    # This ensures backward compatibility
    try:
        connection = op.get_bind()
        connection.execute(text("""
            INSERT INTO tenants (id, name, email_domain, status, description, auto_created, created_at, updated_at) 
            VALUES ('default', 'Default Tenant', 'default.local', 'ACTIVE', 'Default tenant for existing data', true, NOW(), NOW())
        """))
        
        # If there are existing users without tenant associations, add them to default tenant
        connection.execute(text("""
            INSERT INTO tenant_users (id, tenant_id, user_id, role, status, joined_at, auto_created, created_at, updated_at)
            SELECT 
                CONCAT('default_', users.id) as id,
                'default' as tenant_id,
                users.id as user_id,
                CASE 
                    WHEN users.role = 'admin' THEN 'ADMIN'
                    WHEN users.role = 'technical' THEN 'MANAGER'
                    ELSE 'USER'
                END as role,
                'ACTIVE' as status,
                COALESCE(users.created_at, NOW()) as joined_at,
                true as auto_created,
                NOW() as created_at,
                NOW() as updated_at
            FROM users 
            WHERE users.id NOT IN (SELECT user_id FROM tenant_users)
        """))
        
    except Exception as e:
        # If tables don't exist yet or other errors, that's okay
        print(f"Note: Could not create default tenant data: {e}")


def downgrade():
    """Remove tenant support."""
    
    # Remove tenant columns from core workflow entities
    with op.batch_alter_table('prompttemplate', schema=None) as batch_op:
        batch_op.drop_index('ix_prompttemplate_created_by_email')
        batch_op.drop_index('ix_prompttemplate_tenant_id')
        batch_op.drop_column('created_by_email')
        batch_op.drop_column('tenant_id')
    
    with op.batch_alter_table('tools', schema=None) as batch_op:
        batch_op.drop_index('ix_tools_created_by_email')
        batch_op.drop_index('ix_tools_tenant_id')
        batch_op.drop_column('created_by_email')
        batch_op.drop_column('tenant_id')
    
    with op.batch_alter_table('flows', schema=None) as batch_op:
        batch_op.drop_index('ix_flows_created_by_email')
        batch_op.drop_index('ix_flows_tenant_id')
        batch_op.drop_column('created_by_email')
        batch_op.drop_column('tenant_id')
    
    with op.batch_alter_table('crews', schema=None) as batch_op:
        batch_op.drop_index('ix_crews_created_by_email')
        batch_op.drop_index('ix_crews_tenant_id')
        batch_op.drop_column('created_by_email')
        batch_op.drop_column('tenant_id')
    
    with op.batch_alter_table('tasks', schema=None) as batch_op:
        batch_op.drop_index('ix_tasks_created_by_email')
        batch_op.drop_index('ix_tasks_tenant_id')
        batch_op.drop_column('created_by_email')
        batch_op.drop_column('tenant_id')
    
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
    
    with op.batch_alter_table('executionhistory', schema=None) as batch_op:
        batch_op.drop_index('ix_executionhistory_tenant_email')
        batch_op.drop_index('ix_executionhistory_tenant_id')
        batch_op.drop_column('tenant_email')
        batch_op.drop_column('tenant_id')
    
    with op.batch_alter_table('execution_logs', schema=None) as batch_op:
        batch_op.drop_index('idx_execution_logs_tenant_exec_id')
        batch_op.drop_index('idx_execution_logs_tenant_timestamp')
        batch_op.drop_index('ix_execution_logs_tenant_email')
        batch_op.drop_index('ix_execution_logs_tenant_id')
        batch_op.drop_column('tenant_email')
        batch_op.drop_column('tenant_id')
    
    # Drop tenant_users table
    try:
        op.drop_constraint('uq_tenant_users_tenant_user', 'tenant_users', type_='unique')
    except Exception:
        pass
    op.drop_index('ix_tenant_users_user_id', table_name='tenant_users')
    op.drop_index('ix_tenant_users_tenant_id', table_name='tenant_users')
    op.drop_table('tenant_users')
    
    # Drop tenants table
    op.drop_table('tenants')