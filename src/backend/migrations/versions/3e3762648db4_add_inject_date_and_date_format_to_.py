"""add inject_date and date_format to agents

Revision ID: 3e3762648db4
Revises: 1b22cb80b41f
Create Date: 2026-02-05 15:41:28.814806

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '3e3762648db4'
down_revision: Union[str, None] = '1b22cb80b41f'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add inject_date and date_format columns to agents table.

    These columns support CrewAI 1.9+ date awareness feature:
    - inject_date: When True, injects current date into agent's context (default: True)
    - date_format: Custom date format string (e.g., '%B %d, %Y')
    """
    # Add columns with server_default to set value for existing rows
    op.add_column('agents', sa.Column('inject_date', sa.Boolean(), nullable=True, server_default='true'))
    op.add_column('agents', sa.Column('date_format', sa.String(), nullable=True))

    # Update any NULL values to True (for existing agents)
    op.execute("UPDATE agents SET inject_date = true WHERE inject_date IS NULL")


def downgrade() -> None:
    """Remove inject_date and date_format columns from agents table."""
    op.drop_column('agents', 'date_format')
    op.drop_column('agents', 'inject_date')
