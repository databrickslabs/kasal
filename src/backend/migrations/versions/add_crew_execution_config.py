"""Add crew execution configuration fields

Revision ID: add_crew_exec_cfg
Revises: consolidate_exec_001
Create Date: 2025-11-26 17:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'add_crew_exec_cfg'
down_revision = 'consolidate_exec_001'
branch_labels = None
depends_on = None


def upgrade():
    # Add crew execution configuration columns to crews table
    # These columns enable storing process type, planning, reasoning, and other
    # execution configuration options for each crew

    # Process type: sequential or hierarchical
    op.add_column('crews', sa.Column('process', sa.String(50), nullable=True, server_default='sequential'))

    # Planning configuration
    op.add_column('crews', sa.Column('planning', sa.Boolean(), nullable=True, server_default='false'))
    op.add_column('crews', sa.Column('planning_llm', sa.String(255), nullable=True))

    # Reasoning configuration
    op.add_column('crews', sa.Column('reasoning', sa.Boolean(), nullable=True, server_default='false'))
    op.add_column('crews', sa.Column('reasoning_llm', sa.String(255), nullable=True))

    # Manager LLM for hierarchical process
    op.add_column('crews', sa.Column('manager_llm', sa.String(255), nullable=True))

    # Tool configurations (JSON) for crew-level settings like MCP servers
    op.add_column('crews', sa.Column('tool_configs', sa.JSON(), nullable=True))

    # Memory and verbose settings
    op.add_column('crews', sa.Column('memory', sa.Boolean(), nullable=True, server_default='true'))
    op.add_column('crews', sa.Column('verbose', sa.Boolean(), nullable=True, server_default='true'))

    # Max RPM (can be int or null)
    op.add_column('crews', sa.Column('max_rpm', sa.JSON(), nullable=True))


def downgrade():
    # Remove all the added columns
    op.drop_column('crews', 'process')
    op.drop_column('crews', 'planning')
    op.drop_column('crews', 'planning_llm')
    op.drop_column('crews', 'reasoning')
    op.drop_column('crews', 'reasoning_llm')
    op.drop_column('crews', 'manager_llm')
    op.drop_column('crews', 'tool_configs')
    op.drop_column('crews', 'memory')
    op.drop_column('crews', 'verbose')
    op.drop_column('crews', 'max_rpm')
