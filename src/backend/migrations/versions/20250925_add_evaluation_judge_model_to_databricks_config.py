"""add evaluation_judge_model to databricks_config (direct)

Revision ID: 20250925_eval_judge
Revises: 20250925_eval_toggle
Create Date: 2025-09-25 13:10:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '20250925_eval_judge'
down_revision: Union[str, None] = '20250925_eval_toggle'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add evaluation_judge_model column to databricksconfig if it doesn't exist
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = [c['name'] for c in inspector.get_columns('databricksconfig')]
    if 'evaluation_judge_model' not in columns:
        op.add_column(
            'databricksconfig',
            sa.Column('evaluation_judge_model', sa.String(length=255), nullable=True)
        )


def downgrade() -> None:
    # Remove evaluation_judge_model column if it exists
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = [c['name'] for c in inspector.get_columns('databricksconfig')]
    if 'evaluation_judge_model' in columns:
        op.drop_column('databricksconfig', 'evaluation_judge_model')

