"""Rename metadata column to doc_metadata

Revision ID: 68d79ef
Revises: 9a54bcd
Create Date: 2024-05-16

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# revision identifiers, used by Alembic.
revision: str = '68d79ef'
down_revision: Union[str, None] = '9a54bcd'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

def upgrade() -> None:
    connection = op.get_bind()
    
    # Check if the 'metadata' column exists
    inspector = sa.inspect(connection)
    columns = [c['name'] for c in inspector.get_columns('documentation_embeddings')]
    
    if 'metadata' in columns:
        # Rename 'metadata' to 'doc_metadata'
        try:
            op.alter_column('documentation_embeddings', 'metadata', new_column_name='doc_metadata')
            logger.info("Successfully renamed 'metadata' column to 'doc_metadata'")
        except Exception as e:
            logger.error(f"Error renaming 'metadata' column: {str(e)}")
    elif 'doc_metadata' not in columns:
        # If no 'metadata' column and no 'doc_metadata', add 'doc_metadata'
        try:
            op.add_column('documentation_embeddings', sa.Column('doc_metadata', sa.JSON, nullable=True))
            logger.info("Added 'doc_metadata' column")
        except Exception as e:
            logger.error(f"Error adding 'doc_metadata' column: {str(e)}")

def downgrade() -> None:
    connection = op.get_bind()
    
    # Check if 'doc_metadata' exists
    inspector = sa.inspect(connection)
    columns = [c['name'] for c in inspector.get_columns('documentation_embeddings')]
    
    if 'doc_metadata' in columns:
        # Rename 'doc_metadata' back to 'metadata'
        try:
            op.alter_column('documentation_embeddings', 'doc_metadata', new_column_name='metadata')
            logger.info("Successfully renamed 'doc_metadata' column back to 'metadata'")
        except Exception as e:
            logger.error(f"Error renaming 'doc_metadata' column: {str(e)}") 