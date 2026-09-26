"""Create documentation_embeddings table

Revision ID: 3254abc
Revises: f1224c788466
Create Date: 2024-05-15

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB, ARRAY
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# revision identifiers, used by Alembic.
revision: str = '3254abc'
down_revision: Union[str, None] = 'f1224c788466'  # Set to latest migration
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

def upgrade() -> None:
    connection = op.get_bind()
    
    # Check if table already exists
    inspector = sa.inspect(connection)
    if 'documentation_embeddings' in inspector.get_table_names():
        logger.info("Table documentation_embeddings already exists, skipping creation")
        return
    
    # Create documentation_embeddings table
    op.create_table(
        'documentation_embeddings',
        sa.Column('id', sa.Integer, primary_key=True, index=True),
        sa.Column('source', sa.String, nullable=False),
        sa.Column('title', sa.String, nullable=False),
        sa.Column('content', sa.Text, nullable=False),
        sa.Column('embedding', ARRAY(sa.Float), nullable=False),
        sa.Column('metadata', JSONB, nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now(), onupdate=sa.func.now(), nullable=False)
    )
    
    # Create indexes directly in SQL for more control
    try:
        connection.execute(sa.text("CREATE INDEX IF NOT EXISTS ix_documentation_embeddings_source ON documentation_embeddings (source)"))
        connection.execute(sa.text("CREATE INDEX IF NOT EXISTS ix_documentation_embeddings_title ON documentation_embeddings (title)"))
        logger.info("Created standard indexes for documentation_embeddings")
    except Exception as e:
        logger.warning(f"Error creating standard indexes: {str(e)}")
    
    # Try to create pgvector extension and index in separate transaction
    try:
        # We need to manually commit here to ensure the table is created
        connection.execute(sa.text("COMMIT"))
        
        # Try to create the extension in a new transaction
        connection.execute(sa.text("CREATE EXTENSION IF NOT EXISTS vector"))
        logger.info("pgvector extension successfully created or already exists")
        
        # Create vector index if pgvector is available
        connection.execute(sa.text(
            "CREATE INDEX IF NOT EXISTS documentation_embeddings_embedding_idx ON documentation_embeddings " +
            "USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100)"
        ))
        logger.info("Vector index created successfully")
    except Exception as e:
        # Manually roll back any failed pgvector operations
        try:
            connection.execute(sa.text("ROLLBACK"))
        except:
            pass
        
        logger.warning(f"pgvector extension or index creation failed: {str(e)}")
        logger.warning("The table has been created, but vector similarity search will not be available.")
        logger.warning("You can install pgvector manually and create the index later if needed.")

def downgrade() -> None:
    # Drop the table if it exists
    op.drop_table('documentation_embeddings', if_exists=True) 