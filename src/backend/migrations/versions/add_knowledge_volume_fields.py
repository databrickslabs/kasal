"""Add knowledge volume fields to databricks_config

Revision ID: add_knowledge_volume_fields
Revises: add_stop_execution_fields
Create Date: 2025-01-12 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'add_knowledge_volume_fields'
down_revision = 'add_stop_execution_fields'
branch_labels = None
depends_on = None


def upgrade():
    """Add knowledge volume configuration fields to databricksconfig table."""
    # Add knowledge_volume_enabled column
    op.add_column('databricksconfig', 
        sa.Column('knowledge_volume_enabled', sa.Boolean(), nullable=True)
    )
    
    # Add knowledge_volume_path column
    op.add_column('databricksconfig', 
        sa.Column('knowledge_volume_path', sa.String(), nullable=True)
    )
    
    # Add knowledge_chunk_size column
    op.add_column('databricksconfig', 
        sa.Column('knowledge_chunk_size', sa.Integer(), nullable=True)
    )
    
    # Add knowledge_chunk_overlap column
    op.add_column('databricksconfig', 
        sa.Column('knowledge_chunk_overlap', sa.Integer(), nullable=True)
    )
    
    # Set default values for existing rows
    op.execute("UPDATE databricksconfig SET knowledge_volume_enabled = FALSE WHERE knowledge_volume_enabled IS NULL")
    op.execute("UPDATE databricksconfig SET knowledge_volume_path = 'main.default.knowledge' WHERE knowledge_volume_path IS NULL")
    op.execute("UPDATE databricksconfig SET knowledge_chunk_size = 1000 WHERE knowledge_chunk_size IS NULL")
    op.execute("UPDATE databricksconfig SET knowledge_chunk_overlap = 200 WHERE knowledge_chunk_overlap IS NULL")


def downgrade():
    """Remove knowledge volume configuration fields from databricksconfig table."""
    op.drop_column('databricksconfig', 'knowledge_chunk_overlap')
    op.drop_column('databricksconfig', 'knowledge_chunk_size')
    op.drop_column('databricksconfig', 'knowledge_volume_path')
    op.drop_column('databricksconfig', 'knowledge_volume_enabled')