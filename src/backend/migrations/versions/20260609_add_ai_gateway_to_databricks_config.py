"""add ai_gateway_enabled to databricks_config

Revision ID: 20260609_ai_gateway
Revises: 20260604_doc_emb_scope
Create Date: 2026-06-09 00:00:00.000000

Adds the ai_gateway_enabled flag that routes LLM/embedding traffic through the
workspace AI Gateway (/ai-gateway/mlflow/v1) instead of per-model
serving-endpoints invocations.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '20260609_ai_gateway'
down_revision: Union[str, None] = '20260604_doc_emb_scope'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add ai_gateway_enabled column to databricksconfig if it doesn't exist
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = [c['name'] for c in inspector.get_columns('databricksconfig')]
    if 'ai_gateway_enabled' not in columns:
        is_sqlite = 'sqlite' in bind.dialect.name.lower()
        default_expr = sa.text('0') if is_sqlite else sa.text('false')
        op.add_column(
            'databricksconfig',
            sa.Column('ai_gateway_enabled', sa.Boolean(), server_default=default_expr, nullable=False)
        )


def downgrade() -> None:
    # Remove ai_gateway_enabled column if it exists
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = [c['name'] for c in inspector.get_columns('databricksconfig')]
    if 'ai_gateway_enabled' in columns:
        op.drop_column('databricksconfig', 'ai_gateway_enabled')
