"""Update user roles to 3-tier system

Revision ID: update_user_roles_3tier
Revises: latest
Create Date: 2025-09-20
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers
revision = 'update_user_roles_3tier'
down_revision = None  # Will be set to latest
branch_labels = None
depends_on = None


def upgrade():
    """
    Update existing user roles from 4-tier to 3-tier system:
    - manager -> editor
    - user -> operator
    - viewer -> operator
    - admin stays admin
    """

    # Update group_users table
    op.execute("""
        UPDATE group_users
        SET role = CASE
            WHEN role = 'manager' THEN 'editor'
            WHEN role = 'user' THEN 'operator'
            WHEN role = 'viewer' THEN 'operator'
            ELSE role
        END
        WHERE role IN ('manager', 'user', 'viewer')
    """)

    # Update any role columns in other tables if they exist
    # Check if user_roles table exists and update it
    connection = op.get_bind()
    inspector = sa.inspect(connection)

    if 'user_roles' in inspector.get_table_names():
        op.execute("""
            UPDATE user_roles
            SET role_name = CASE
                WHEN role_name = 'manager' THEN 'editor'
                WHEN role_name = 'user' THEN 'operator'
                WHEN role_name = 'viewer' THEN 'operator'
                ELSE role_name
            END
            WHERE role_name IN ('manager', 'user', 'viewer')
        """)

    # Update roles table if it exists
    if 'roles' in inspector.get_table_names():
        # Delete old roles
        op.execute("DELETE FROM roles WHERE name IN ('manager', 'user', 'viewer')")

        # Ensure new roles exist
        op.execute("""
            INSERT INTO roles (id, name, description, created_at, updated_at)
            SELECT
                gen_random_uuid(),
                'editor',
                'Workflow developer - Build and modify AI agent tasks',
                NOW(),
                NOW()
            WHERE NOT EXISTS (SELECT 1 FROM roles WHERE name = 'editor')
        """)

        op.execute("""
            INSERT INTO roles (id, name, description, created_at, updated_at)
            SELECT
                gen_random_uuid(),
                'operator',
                'Execution operator - Execute workflows and monitor',
                NOW(),
                NOW()
            WHERE NOT EXISTS (SELECT 1 FROM roles WHERE name = 'operator')
        """)


def downgrade():
    """
    Revert back to 4-tier system
    """

    # Revert group_users table
    # Note: This is lossy - we can't distinguish which operators were originally users vs viewers
    op.execute("""
        UPDATE group_users
        SET role = CASE
            WHEN role = 'editor' THEN 'manager'
            WHEN role = 'operator' THEN 'user'  -- Default operators to user
            ELSE role
        END
        WHERE role IN ('editor', 'operator')
    """)

    # Revert user_roles table if it exists
    connection = op.get_bind()
    inspector = sa.inspect(connection)

    if 'user_roles' in inspector.get_table_names():
        op.execute("""
            UPDATE user_roles
            SET role_name = CASE
                WHEN role_name = 'editor' THEN 'manager'
                WHEN role_name = 'operator' THEN 'user'
                ELSE role_name
            END
            WHERE role_name IN ('editor', 'operator')
        """)