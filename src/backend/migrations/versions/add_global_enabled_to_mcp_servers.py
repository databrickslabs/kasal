"""Add global_enabled column to mcp_servers table

Revision ID: add_global_enabled_mcp
Revises: add_tool_configs_to_agents_tasks
Create Date: 2025-08-24 20:45:00.000000

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'add_global_enabled_mcp'
down_revision = 'add_tool_configs_001'
branch_labels = None
depends_on = None

def upgrade():
    """Add global_enabled column to mcp_servers table"""
    # Add global_enabled column to mcp_servers table
    op.add_column('mcp_servers', sa.Column('global_enabled', sa.Boolean(), default=False, nullable=False, server_default=sa.text('false')))
    
    # Add individual_enabled column to mcp_settings table  
    op.add_column('mcp_settings', sa.Column('individual_enabled', sa.Boolean(), default=True, nullable=False, server_default=sa.text('true')))

def downgrade():
    """Remove global_enabled column from mcp_servers table"""
    # Remove columns
    op.drop_column('mcp_settings', 'individual_enabled')
    op.drop_column('mcp_servers', 'global_enabled')