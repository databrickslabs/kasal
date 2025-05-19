"""Update embedding column to vector type

Revision ID: 9a54bcd
Revises: 3254abc
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
revision: str = '9a54bcd'
down_revision: Union[str, None] = '3254abc'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

def upgrade() -> None:
    connection = op.get_bind()
    
    # Check if the vector extension exists
    vector_exists = connection.execute(sa.text(
        "SELECT 1 FROM pg_extension WHERE extname = 'vector'"
    )).scalar() is not None
    
    if not vector_exists:
        logger.warning("Vector extension is not installed. Installing it now.")
        connection.execute(sa.text("CREATE EXTENSION IF NOT EXISTS vector"))
    
    # Alter the embedding column to use vector type
    # First, we need to drop any existing data since we can't directly cast arrays to vectors
    # In a production system, you'd want to migrate the data properly
    connection.execute(sa.text("TRUNCATE documentation_embeddings"))
    
    # Now change the column type
    connection.execute(sa.text("ALTER TABLE documentation_embeddings ALTER COLUMN embedding TYPE vector(1536) USING NULL"))
    
    # Create the vector index
    try:
        connection.execute(sa.text(
            "CREATE INDEX documentation_embeddings_embedding_idx ON documentation_embeddings " +
            "USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100)"
        ))
        logger.info("Vector index created successfully")
    except Exception as e:
        logger.warning(f"Error creating vector index: {str(e)}")

def downgrade() -> None:
    connection = op.get_bind()
    
    # Drop the vector index if it exists
    try:
        connection.execute(sa.text("DROP INDEX IF EXISTS documentation_embeddings_embedding_idx"))
    except Exception as e:
        logger.warning(f"Error dropping index: {str(e)}")
    
    # Change the column back to array type
    connection.execute(sa.text("TRUNCATE documentation_embeddings"))
    connection.execute(sa.text("ALTER TABLE documentation_embeddings ALTER COLUMN embedding TYPE float[] USING NULL")) 