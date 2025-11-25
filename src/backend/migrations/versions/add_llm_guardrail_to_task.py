"""Add llm_guardrail to task

Revision ID: add_llm_guardrail
Revises: 90f4428dfe2f
Create Date: 2025-11-23

Adds the llm_guardrail JSON column to the tasks table for storing
LLM-based guardrail configuration (description and llm_model).
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers
revision = 'add_llm_guardrail'
down_revision = '90f4428dfe2f'
branch_labels = None
depends_on = None


def upgrade():
    # Add llm_guardrail column to tasks table
    # Using JSON type which works for both PostgreSQL and SQLite
    op.add_column('tasks', sa.Column('llm_guardrail', sa.JSON(), nullable=True))


def downgrade():
    op.drop_column('tasks', 'llm_guardrail')
