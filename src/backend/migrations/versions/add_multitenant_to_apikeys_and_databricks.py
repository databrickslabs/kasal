"""Add multi-tenant fields to apikey and databricksconfig

Revision ID: add_multitenant_apikeys
Revises: 7a02c9c7ac15
Create Date: 2025-01-11 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


# revision identifiers, used by Alembic.
revision = 'add_multitenant_apikeys'
down_revision = '7a02c9c7ac15'
branch_labels = None
depends_on = None


def upgrade():
    """Add group_id and created_by_email to apikey and databricksconfig tables."""
    
    # First, create the apikey table if it doesn't exist
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    
    if 'apikey' not in inspector.get_table_names():
        op.create_table('apikey',
            sa.Column('id', sa.Integer(), primary_key=True),
            sa.Column('name', sa.String(), nullable=False),
            sa.Column('encrypted_value', sa.String(), nullable=False),
            sa.Column('description', sa.String(), nullable=True),
            sa.Column('created_at', sa.DateTime(), nullable=True),
            sa.Column('updated_at', sa.DateTime(), nullable=True),
            sa.Column('group_id', sa.String(length=100), nullable=True),
            sa.Column('created_by_email', sa.String(length=255), nullable=True)
        )
        op.create_index('ix_apikey_name', 'apikey', ['name'], unique=False)
        op.create_index('ix_apikey_group_id', 'apikey', ['group_id'], unique=False)
        op.create_index('ix_apikey_created_by_email', 'apikey', ['created_by_email'], unique=False)
        op.create_unique_constraint('uq_apikey_name_group', 'apikey', ['name', 'group_id'])
    else:
        # Add columns to existing apikey table
        op.add_column('apikey', sa.Column('group_id', sa.String(length=100), nullable=True))
        op.add_column('apikey', sa.Column('created_by_email', sa.String(length=255), nullable=True))
        
        # Create indexes for apikey
        op.create_index('ix_apikey_group_id', 'apikey', ['group_id'], unique=False)
        op.create_index('ix_apikey_created_by_email', 'apikey', ['created_by_email'], unique=False)
        
        # Drop the unique constraint on name (to allow same name across different groups)
        try:
            op.drop_index('ix_apikey_name', table_name='apikey')
            op.create_index('ix_apikey_name', 'apikey', ['name'], unique=False)
        except:
            # Index might not exist or have different name
            pass
        
        # Add composite unique constraint for name + group_id
        op.create_unique_constraint('uq_apikey_name_group', 'apikey', ['name', 'group_id'])
    
    # Create databricksconfig table if it doesn't exist
    if 'databricksconfig' not in inspector.get_table_names():
        op.create_table('databricksconfig',
            sa.Column('id', sa.Integer(), primary_key=True),
            sa.Column('workspace_url', sa.String(), nullable=True),
            sa.Column('warehouse_id', sa.String(), nullable=False),
            sa.Column('catalog', sa.String(), nullable=False),
            sa.Column('schema', sa.String(), nullable=False),
            sa.Column('secret_scope', sa.String(), nullable=False),
            sa.Column('is_active', sa.Boolean(), default=True),
            sa.Column('is_enabled', sa.Boolean(), default=True),
            sa.Column('apps_enabled', sa.Boolean(), default=False),
            sa.Column('encrypted_personal_access_token', sa.String(), nullable=True),
            sa.Column('created_at', sa.DateTime(timezone=True), nullable=True),
            sa.Column('updated_at', sa.DateTime(timezone=True), nullable=True),
            sa.Column('group_id', sa.String(length=100), nullable=True),
            sa.Column('created_by_email', sa.String(length=255), nullable=True)
        )
        op.create_index('ix_databricksconfig_group_id', 'databricksconfig', ['group_id'], unique=False)
        op.create_index('ix_databricksconfig_created_by_email', 'databricksconfig', ['created_by_email'], unique=False)
    else:
        # Add columns to existing databricksconfig table
        op.add_column('databricksconfig', sa.Column('group_id', sa.String(length=100), nullable=True))
        op.add_column('databricksconfig', sa.Column('created_by_email', sa.String(length=255), nullable=True))
        
        # Create indexes for databricksconfig
        op.create_index('ix_databricksconfig_group_id', 'databricksconfig', ['group_id'], unique=False)
        op.create_index('ix_databricksconfig_created_by_email', 'databricksconfig', ['created_by_email'], unique=False)


def downgrade():
    """Remove multi-tenant fields from apikey and databricksconfig tables."""
    
    # Remove from databricksconfig
    op.drop_index('ix_databricksconfig_created_by_email', table_name='databricksconfig')
    op.drop_index('ix_databricksconfig_group_id', table_name='databricksconfig')
    op.drop_column('databricksconfig', 'created_by_email')
    op.drop_column('databricksconfig', 'group_id')
    
    # Remove from apikey
    op.drop_constraint('uq_apikey_name_group', 'apikey', type_='unique')
    op.drop_index('ix_apikey_created_by_email', table_name='apikey')
    op.drop_index('ix_apikey_group_id', table_name='apikey')
    op.drop_column('apikey', 'created_by_email')
    op.drop_column('apikey', 'group_id')
    
    # Restore unique constraint on name
    try:
        op.drop_index('ix_apikey_name', table_name='apikey')
        op.create_index('ix_apikey_name', 'apikey', ['name'], unique=True)
    except:
        pass