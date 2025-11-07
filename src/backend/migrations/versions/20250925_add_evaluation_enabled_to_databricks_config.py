"""add evaluation_enabled to databricks_config (direct)

Revision ID: 20250925_eval_toggle
Revises: 154b6927acd9
Create Date: 2025-09-25 12:10:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '20250925_eval_toggle'
down_revision: Union[str, None] = '154b6927acd9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add evaluation_enabled column to databricksconfig if it doesn't exist
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = [c['name'] for c in inspector.get_columns('databricksconfig')]
    if 'evaluation_enabled' not in columns:
        is_sqlite = 'sqlite' in bind.dialect.name.lower()
        default_expr = sa.text('0') if is_sqlite else sa.text('false')
        op.add_column(
            'databricksconfig',
            sa.Column('evaluation_enabled', sa.Boolean(), server_default=default_expr, nullable=False)
        )


def downgrade() -> None:
    # Remove evaluation_enabled column if it exists
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = [c['name'] for c in inspector.get_columns('databricksconfig')]
    if 'evaluation_enabled' in columns:
        op.drop_column('databricksconfig', 'evaluation_enabled')

