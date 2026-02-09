"""add_powerbi_context_config_tables

Revision ID: 337188149f25
Revises: 867824642b40
Create Date: 2026-02-09 07:35:58.408375

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '337188149f25'
down_revision: Union[str, None] = '867824642b40'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create powerbi_business_mappings table
    op.create_table(
        'powerbi_business_mappings',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('group_id', sa.String(length=255), nullable=False),
        sa.Column('semantic_model_id', sa.String(length=255), nullable=False),
        sa.Column('natural_term', sa.String(length=500), nullable=False),
        sa.Column('dax_expression', sa.Text(), nullable=False),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
        sa.Column('updated_at', sa.DateTime(), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('group_id', 'semantic_model_id', 'natural_term', name='uq_business_mapping_term')
    )

    # Create indexes for business mappings
    op.create_index(
        'idx_business_mappings_group_model',
        'powerbi_business_mappings',
        ['group_id', 'semantic_model_id']
    )

    # Create powerbi_field_synonyms table
    op.create_table(
        'powerbi_field_synonyms',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('group_id', sa.String(length=255), nullable=False),
        sa.Column('semantic_model_id', sa.String(length=255), nullable=False),
        sa.Column('field_name', sa.String(length=255), nullable=False),
        sa.Column('synonyms', sa.JSON(), nullable=False),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
        sa.Column('updated_at', sa.DateTime(), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('group_id', 'semantic_model_id', 'field_name', name='uq_field_synonym')
    )

    # Create indexes for field synonyms
    op.create_index(
        'idx_field_synonyms_group_model',
        'powerbi_field_synonyms',
        ['group_id', 'semantic_model_id']
    )


def downgrade() -> None:
    # Drop indexes
    op.drop_index('idx_field_synonyms_group_model', table_name='powerbi_field_synonyms')
    op.drop_index('idx_business_mappings_group_model', table_name='powerbi_business_mappings')

    # Drop tables
    op.drop_table('powerbi_field_synonyms')
    op.drop_table('powerbi_business_mappings') 