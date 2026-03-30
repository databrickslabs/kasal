"""Add tool_configs field to agents and tasks tables

Revision ID: add_tool_configs_001
Revises: 
Create Date: 2025-08-23 22:30:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'add_tool_configs_001'
down_revision = 'ddfd70895919'
branch_labels = None
depends_on = None


def upgrade():
    """Add tool_configs column to agents and tasks tables."""
    
    # Add tool_configs to agents table
    op.add_column('agents', 
        sa.Column('tool_configs', sa.JSON(), nullable=True, server_default='{}',
                  comment='User-specific tool configurations override')
    )
    
    # Add tool_configs to tasks table
    op.add_column('tasks',
        sa.Column('tool_configs', sa.JSON(), nullable=True, server_default='{}',
                  comment='User-specific tool configurations override')
    )


def downgrade():
    """Remove tool_configs column from agents and tasks tables."""
    
    # Remove from agents table
    op.drop_column('agents', 'tool_configs')
    
    # Remove from tasks table  
    op.drop_column('tasks', 'tool_configs')