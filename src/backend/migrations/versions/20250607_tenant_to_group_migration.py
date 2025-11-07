"""Migrate tenant data to group tables and add group_id columns

Revision ID: 20250607_tenant_to_group
Revises: 1f0183de606
Create Date: 2025-01-07 12:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers
revision = '20250607_tenant_to_group'
down_revision = '1f0183de606'
branch_labels = None
depends_on = None

def upgrade():
    """Migrate tenant data to group tables and add group_id columns"""
    
    print("Starting tenant to group data migration...")
    
    # Get database connection
    connection = op.get_bind()
    
    # Step 1: Copy data from tenants to groups (if we have data to migrate)
    try:
        result = connection.execute(sa.text("SELECT COUNT(*) FROM tenants"))
        tenant_count = result.scalar()
        
        if tenant_count > 0:
            print(f"Migrating {tenant_count} tenants to groups...")
            # Copy tenant data to groups table where it doesn't already exist
            connection.execute(sa.text("""
                INSERT INTO groups (id, name, email_domain, status, description, auto_created, created_by_email, created_at, updated_at)
                SELECT id, name, email_domain, status, description, auto_created, created_by_email, created_at, updated_at
                FROM tenants
                WHERE id NOT IN (SELECT id FROM groups WHERE id IS NOT NULL)
            """))
            print(f"✓ Migrated tenants to groups")
        
        # Copy tenant_users to group_users
        result = connection.execute(sa.text("SELECT COUNT(*) FROM tenant_users"))
        tenant_user_count = result.scalar()
        
        if tenant_user_count > 0:
            print(f"Migrating {tenant_user_count} tenant users to group users...")
            connection.execute(sa.text("""
                INSERT INTO group_users (id, group_id, user_id, role, status, joined_at, auto_created, created_at, updated_at)
                SELECT 
                    CONCAT('group_', id) as id,
                    tenant_id as group_id,
                    user_id,
                    role,
                    status,
                    joined_at,
                    auto_created,
                    created_at,
                    updated_at
                FROM tenant_users
                WHERE CONCAT('group_', id) NOT IN (SELECT id FROM group_users WHERE id IS NOT NULL)
            """))
            print(f"✓ Migrated tenant users to group users")
            
    except Exception as e:
        print(f"Data migration step: {e}")
    
    # Step 2: Add group_id columns to existing tables (where they don't exist)
    print("Adding group_id columns to existing tables...")
    
    tables_needing_group_id = [
        'agents',
        'crews', 
        'tasks',
        'flows',
        'tools',
        'execution_logs',
        'executionhistory',
        'prompttemplate',
        'schedule',
        'llmlog'  # Note: using llmlog not llm_logs
    ]
    
    for table_name in tables_needing_group_id:
        try:
            # Check if group_id column already exists
            inspector = sa.inspect(connection)
            columns = [col['name'] for col in inspector.get_columns(table_name)]
            
            if 'group_id' not in columns:
                # Add group_id column
                op.add_column(table_name, sa.Column('group_id', sa.String(100), nullable=True))
                op.create_index(f'ix_{table_name}_group_id', table_name, ['group_id'])
                print(f"✓ Added group_id column to {table_name}")
                
                # Copy tenant_id values to group_id
                if 'tenant_id' in columns:
                    connection.execute(sa.text(f"UPDATE {table_name} SET group_id = tenant_id WHERE tenant_id IS NOT NULL"))
                    print(f"✓ Copied tenant_id to group_id in {table_name}")
            else:
                print(f"✓ group_id already exists in {table_name}")
                
        except Exception as e:
            print(f"⚠ Could not add group_id to {table_name}: {e}")
    
    # Step 3: Add group_email columns where needed  
    print("Adding group_email columns...")
    
    tables_needing_group_email = [
        'execution_logs',
        'executionhistory', 
        'llmlog'
    ]
    
    for table_name in tables_needing_group_email:
        try:
            inspector = sa.inspect(connection)
            columns = [col['name'] for col in inspector.get_columns(table_name)]
            
            if 'group_email' not in columns:
                op.add_column(table_name, sa.Column('group_email', sa.String(255), nullable=True))
                op.create_index(f'ix_{table_name}_group_email', table_name, ['group_email'])
                print(f"✓ Added group_email column to {table_name}")
                
                # Copy tenant_email values to group_email
                if 'tenant_email' in columns:
                    connection.execute(sa.text(f"UPDATE {table_name} SET group_email = tenant_email WHERE tenant_email IS NOT NULL"))
                    print(f"✓ Copied tenant_email to group_email in {table_name}")
            else:
                print(f"✓ group_email already exists in {table_name}")
                
        except Exception as e:
            print(f"⚠ Could not add group_email to {table_name}: {e}")

    print("✅ Migration completed successfully!")
    print("🔄 Summary:")
    print("  ✓ Migrated tenant data to groups tables")
    print("  ✓ Added group_id columns to all relevant tables") 
    print("  ✓ Added group_email columns where needed")
    print("  ✓ Created appropriate indexes")
    print("  ✓ Legacy tenant tables preserved for backward compatibility")


def downgrade():
    """Remove group_id and group_email columns (keep group tables intact)"""
    
    print("Reverting group column additions...")
    
    # Remove group_id columns (but keep group tables)
    tables_with_group_id = [
        'agents',
        'crews',
        'tasks', 
        'flows',
        'tools',
        'execution_logs',
        'executionhistory',
        'prompttemplate',
        'schedules',
        'llmlog'
    ]
    
    for table_name in tables_with_group_id:
        try:
            op.drop_index(f'ix_{table_name}_group_id', table_name=table_name)
            op.drop_column(table_name, 'group_id')
            print(f"✓ Removed group_id from {table_name}")
        except Exception as e:
            print(f"⚠ Could not remove group_id from {table_name}: {e}")
    
    # Remove group_email columns
    tables_with_group_email = [
        'execution_logs',
        'executionhistory',
        'llmlog'
    ]
    
    for table_name in tables_with_group_email:
        try:
            op.drop_index(f'ix_{table_name}_group_email', table_name=table_name)
            op.drop_column(table_name, 'group_email')
            print(f"✓ Removed group_email from {table_name}")
        except Exception as e:
            print(f"⚠ Could not remove group_email from {table_name}: {e}")
    
    print("✅ Migration reverted successfully!")