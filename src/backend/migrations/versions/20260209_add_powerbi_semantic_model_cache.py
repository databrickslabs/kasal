"""add_powerbi_semantic_model_cache

Revision ID: 20260209_cache
Revises: 337188149f25
Create Date: 2026-02-09 18:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '20260209_cache'
down_revision: Union[str, None] = '337188149f25'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create powerbi_semantic_model_cache table
    op.create_table(
        'powerbi_semantic_model_cache',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('group_id', sa.String(length=255), nullable=False),
        sa.Column('dataset_id', sa.String(length=255), nullable=False),
        sa.Column('workspace_id', sa.String(length=255), nullable=False),
        sa.Column('report_id', sa.String(length=255), nullable=True),
        sa.Column('cached_date', sa.Date(), nullable=False),
        sa.Column('cache_data', sa.JSON(), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('group_id', 'dataset_id', 'cached_date', 'report_id', name='uq_semantic_model_cache_daily')
    )

    # Create indexes for semantic model cache
    op.create_index(
        'idx_semantic_cache_group_dataset',
        'powerbi_semantic_model_cache',
        ['group_id', 'dataset_id']
    )

    op.create_index(
        'idx_semantic_cache_date',
        'powerbi_semantic_model_cache',
        ['cached_date']
    )


def downgrade() -> None:
    # Drop indexes first
    op.drop_index('idx_semantic_cache_date', table_name='powerbi_semantic_model_cache')
    op.drop_index('idx_semantic_cache_group_dataset', table_name='powerbi_semantic_model_cache')

    # Drop table
    op.drop_table('powerbi_semantic_model_cache')
