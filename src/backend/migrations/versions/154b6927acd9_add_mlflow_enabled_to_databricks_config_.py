"""add mlflow_enabled to databricks_config (direct)

Revision ID: 154b6927acd9
Revises: 456ff78ce120
Create Date: 2025-09-25 09:21:05.171785

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '154b6927acd9'
down_revision: Union[str, None] = '456ff78ce120'
branch_labels: Union[str, Sequence[str], None] = ('mlflow',)
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add mlflow_enabled column to databricksconfig if it doesn't exist
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = [c['name'] for c in inspector.get_columns('databricksconfig')]
    if 'mlflow_enabled' not in columns:
        is_sqlite = 'sqlite' in bind.dialect.name.lower()
        default_expr = sa.text('0') if is_sqlite else sa.text('false')
        op.add_column(
            'databricksconfig',
            sa.Column('mlflow_enabled', sa.Boolean(), server_default=default_expr, nullable=False)
        )


def downgrade() -> None:
    # Remove mlflow_enabled column if it exists
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = [c['name'] for c in inspector.get_columns('databricksconfig')]
    if 'mlflow_enabled' in columns:
        op.drop_column('databricksconfig', 'mlflow_enabled')