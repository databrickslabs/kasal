"""Add encrypted_personal_access_token to databricks config

Revision ID: add_encrypted_token
Revises: 61d6a53cc4b8
Create Date: 2025-06-05 17:25:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'add_encrypted_token'
down_revision: Union[str, None] = '61d6a53cc4b8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add the encrypted_personal_access_token column to databricksconfig table
    op.add_column('databricksconfig', sa.Column('encrypted_personal_access_token', sa.String(), nullable=True))


def downgrade() -> None:
    # Remove the encrypted_personal_access_token column from databricksconfig table
    op.drop_column('databricksconfig', 'encrypted_personal_access_token')