"""Add guardrail column to task model

Revision ID: 456ff78ce120
Revises: 321f0168ea68
Create Date: 2023-08-11 12:34:56.789012

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '456ff78ce120'
down_revision = '321f0168ea68'
branch_labels = None
depends_on = None


def upgrade():
    # Add guardrail column to tasks table, but only if it doesn't already exist
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    existing_columns = [col['name'] for col in inspector.get_columns('tasks')]
    if 'guardrail' not in existing_columns:
        op.add_column('tasks', sa.Column('guardrail', sa.String(), nullable=True))


def downgrade():
    # Remove guardrail column from tasks table if it exists
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    existing_columns = [col['name'] for col in inspector.get_columns('tasks')]
    if 'guardrail' in existing_columns:
        op.drop_column('tasks', 'guardrail')