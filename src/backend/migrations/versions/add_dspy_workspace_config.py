"""add dspy workspace configuration fields

Revision ID: dspy_workspace_001
Revises: 3c0aebc2977a
Create Date: 2024-09-27

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'dspy_workspace_001'
down_revision: Union[str, None] = '3c0aebc2977a'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add DSPy configuration fields to Databricks config table (name differs by env)
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    tables = set(inspector.get_table_names())
    table_name = 'databricks_configs' if 'databricks_configs' in tables else 'databricksconfig' if 'databricksconfig' in tables else None
    if not table_name:
        # Table not present in this environment; skip
        return

    is_sqlite = 'sqlite' in bind.dialect.name.lower()
    def_bool = '0' if is_sqlite else 'false'
    def_true = '1' if is_sqlite else 'true'

    with op.batch_alter_table(table_name, schema=None) as batch_op:
        # DSPy Optimization Configuration
        batch_op.add_column(sa.Column('dspy_enabled', sa.Boolean(), nullable=True, server_default=sa.text(def_bool)))
        batch_op.add_column(sa.Column('dspy_intent_detection', sa.Boolean(), nullable=True, server_default=sa.text(def_true)))
        batch_op.add_column(sa.Column('dspy_agent_generation', sa.Boolean(), nullable=True, server_default=sa.text(def_true)))
        batch_op.add_column(sa.Column('dspy_task_generation', sa.Boolean(), nullable=True, server_default=sa.text(def_true)))
        batch_op.add_column(sa.Column('dspy_crew_generation', sa.Boolean(), nullable=True, server_default=sa.text(def_true)))
        batch_op.add_column(sa.Column('dspy_optimization_interval', sa.Integer(), nullable=True, server_default='24'))
        batch_op.add_column(sa.Column('dspy_min_examples', sa.Integer(), nullable=True, server_default='10'))
        batch_op.add_column(sa.Column('dspy_confidence_threshold', sa.Float(), nullable=True, server_default='0.7'))


def downgrade() -> None:
    # Remove DSPy configuration fields from databricks_configs table
    with op.batch_alter_table('databricks_configs', schema=None) as batch_op:
        batch_op.drop_column('dspy_confidence_threshold')
        batch_op.drop_column('dspy_min_examples')
        batch_op.drop_column('dspy_optimization_interval')
        batch_op.drop_column('dspy_crew_generation')
        batch_op.drop_column('dspy_task_generation')
        batch_op.drop_column('dspy_agent_generation')
        batch_op.drop_column('dspy_intent_detection')
        batch_op.drop_column('dspy_enabled')