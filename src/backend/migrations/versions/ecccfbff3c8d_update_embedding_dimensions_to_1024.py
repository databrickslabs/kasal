"""update_embedding_dimensions_to_1024

Revision ID: ecccfbff3c8d
Revises: b1d5d4328f16
Create Date: 2025-06-07 15:14:09.543763

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'ecccfbff3c8d'
down_revision: Union[str, None] = 'b1d5d4328f16'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Clear existing embeddings as they are 1536-dimensional and incompatible with 1024-dimensional model
    op.execute("DELETE FROM documentation_embeddings")
    
    # Use raw SQL to alter the column type directly since our Vector type isn't available in migrations
    op.execute("ALTER TABLE documentation_embeddings ALTER COLUMN embedding TYPE vector(1024)")


def downgrade() -> None:
    # Clear existing embeddings as they are 1024-dimensional and incompatible with 1536-dimensional model
    op.execute("DELETE FROM documentation_embeddings")
    
    # Revert to 1536 dimensions
    op.execute("ALTER TABLE documentation_embeddings ALTER COLUMN embedding TYPE vector(1536)") 