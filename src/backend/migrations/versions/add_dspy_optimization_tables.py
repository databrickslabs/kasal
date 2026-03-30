"""add dspy optimization tables

Revision ID: dspy_001
Revises:
Create Date: 2024-01-27

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
import os

# revision identifiers, used by Alembic.
revision: str = 'dspy_001'
down_revision: Union[str, None] = 'dc4860e3b69c'  # Link to one of the existing heads
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Use consistent string IDs for compatibility with existing models
    # Both id and group_id are stored as String(100) in ORM models.
    bind = op.get_bind()
    id_type = sa.String(length=100)
    group_id_type = sa.String(length=100)

    # Create dspy_configs table
    op.create_table(
        'dspy_configs',
        sa.Column('id', id_type, nullable=False),
        sa.Column('optimization_type', sa.String(length=50), nullable=False),
        sa.Column('version', sa.Integer(), nullable=False),
        sa.Column('mlflow_run_id', sa.Text(), nullable=True),
        sa.Column('mlflow_model_uri', sa.Text(), nullable=True),
        sa.Column('mlflow_experiment_id', sa.Text(), nullable=True),
        sa.Column('prompts_json', sa.JSON(), nullable=True),
        sa.Column('module_config', sa.JSON(), nullable=True),
        sa.Column('optimizer_config', sa.JSON(), nullable=True),
        sa.Column('performance_metrics', sa.JSON(), nullable=True),
        sa.Column('test_score', sa.Float(), nullable=True),
        sa.Column('num_training_examples', sa.Integer(), nullable=True),
        sa.Column('is_active', sa.Boolean(), nullable=True),
        sa.Column('deployment_stage', sa.String(length=20), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.Column('deployed_at', sa.DateTime(), nullable=True),
        sa.Column('group_id', group_id_type, nullable=True),
        sa.ForeignKeyConstraint(['group_id'], ['groups.id'], ),
        sa.PrimaryKeyConstraint('id')
    )

    # Create indexes for dspy_configs
    op.create_index('idx_dspy_config_type_active', 'dspy_configs', ['optimization_type', 'is_active'], unique=False)
    op.create_index('idx_dspy_config_group_type', 'dspy_configs', ['group_id', 'optimization_type'], unique=False)
    op.create_index('idx_dspy_config_stage', 'dspy_configs', ['deployment_stage'], unique=False)

    # Create dspy_training_examples table
    op.create_table(
        'dspy_training_examples',
        sa.Column('id', id_type, nullable=False),
        sa.Column('optimization_type', sa.String(length=50), nullable=False),
        sa.Column('input_data', sa.JSON(), nullable=False),
        sa.Column('output_data', sa.JSON(), nullable=False),
        sa.Column('metadata', sa.JSON(), nullable=True),
        sa.Column('quality_score', sa.Float(), nullable=True),
        sa.Column('trace_id', sa.Text(), nullable=True),
        sa.Column('execution_id', id_type, nullable=True),
        sa.Column('source_type', sa.String(length=20), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('collected_at', sa.DateTime(), nullable=True),
        sa.Column('used_in_optimization', sa.Boolean(), nullable=True),
        sa.Column('optimization_run_ids', sa.JSON(), nullable=True),
        sa.Column('group_id', group_id_type, nullable=True),
        sa.Column('config_id', id_type, nullable=True),
        sa.ForeignKeyConstraint(['config_id'], ['dspy_configs.id'], ),
        sa.ForeignKeyConstraint(['group_id'], ['groups.id'], ),
        sa.PrimaryKeyConstraint('id')
    )

    # Create indexes for dspy_training_examples
    op.create_index('idx_dspy_example_type_score', 'dspy_training_examples', ['optimization_type', 'quality_score'], unique=False)
    op.create_index('idx_dspy_example_trace', 'dspy_training_examples', ['trace_id'], unique=False)
    op.create_index('idx_dspy_example_created', 'dspy_training_examples', ['created_at'], unique=False)

    # Create dspy_optimization_runs table
    op.create_table(
        'dspy_optimization_runs',
        sa.Column('id', id_type, nullable=False),
        sa.Column('optimization_type', sa.String(length=50), nullable=False),
        sa.Column('optimizer_type', sa.String(length=50), nullable=True),
        sa.Column('optimizer_params', sa.JSON(), nullable=True),
        sa.Column('num_training_examples', sa.Integer(), nullable=True),
        sa.Column('num_validation_examples', sa.Integer(), nullable=True),
        sa.Column('min_quality_threshold', sa.Float(), nullable=True),
        sa.Column('status', sa.String(length=20), nullable=True),
        sa.Column('metrics', sa.JSON(), nullable=True),
        sa.Column('best_score', sa.Float(), nullable=True),
        sa.Column('started_at', sa.DateTime(), nullable=True),
        sa.Column('completed_at', sa.DateTime(), nullable=True),
        sa.Column('duration_seconds', sa.Integer(), nullable=True),
        sa.Column('error_message', sa.Text(), nullable=True),
        sa.Column('error_traceback', sa.Text(), nullable=True),
        sa.Column('mlflow_run_id', sa.Text(), nullable=True),
        sa.Column('triggered_by', sa.String(length=20), nullable=True),
        sa.Column('triggered_by_user', sa.String(length=100), nullable=True),
        sa.Column('group_id', group_id_type, nullable=True),
        sa.Column('config_id', id_type, nullable=True),
        sa.ForeignKeyConstraint(['config_id'], ['dspy_configs.id'], ),
        sa.ForeignKeyConstraint(['group_id'], ['groups.id'], ),
        sa.PrimaryKeyConstraint('id')
    )

    # Create indexes for dspy_optimization_runs
    op.create_index('idx_dspy_run_type_status', 'dspy_optimization_runs', ['optimization_type', 'status'], unique=False)
    op.create_index('idx_dspy_run_started', 'dspy_optimization_runs', ['started_at'], unique=False)

    # Create dspy_module_cache table
    op.create_table(
        'dspy_module_cache',
        sa.Column('id', id_type, nullable=False),
        sa.Column('optimization_type', sa.String(length=50), nullable=False),
        sa.Column('config_version', sa.Integer(), nullable=False),
        sa.Column('module_pickle', sa.Text(), nullable=True),
        sa.Column('module_hash', sa.String(length=64), nullable=True),
        sa.Column('cache_key', sa.String(length=255), nullable=True),
        sa.Column('loaded_at', sa.DateTime(), nullable=True),
        sa.Column('last_accessed', sa.DateTime(), nullable=True),
        sa.Column('access_count', sa.Integer(), nullable=True),
        sa.Column('ttl_hours', sa.Integer(), nullable=True),
        sa.Column('group_id', group_id_type, nullable=True),
        sa.ForeignKeyConstraint(['group_id'], ['groups.id'], ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('cache_key')
    )

    # Create indexes for dspy_module_cache
    op.create_index('idx_dspy_cache_key', 'dspy_module_cache', ['cache_key'], unique=False)
    op.create_index('idx_dspy_cache_type', 'dspy_module_cache', ['optimization_type', 'group_id'], unique=False)


def downgrade() -> None:
    # Drop all tables in reverse order
    op.drop_index('idx_dspy_cache_type', table_name='dspy_module_cache')
    op.drop_index('idx_dspy_cache_key', table_name='dspy_module_cache')
    op.drop_table('dspy_module_cache')

    op.drop_index('idx_dspy_run_started', table_name='dspy_optimization_runs')
    op.drop_index('idx_dspy_run_type_status', table_name='dspy_optimization_runs')
    op.drop_table('dspy_optimization_runs')

    op.drop_index('idx_dspy_example_created', table_name='dspy_training_examples')
    op.drop_index('idx_dspy_example_trace', table_name='dspy_training_examples')
    op.drop_index('idx_dspy_example_type_score', table_name='dspy_training_examples')
    op.drop_table('dspy_training_examples')

    op.drop_index('idx_dspy_config_stage', table_name='dspy_configs')
    op.drop_index('idx_dspy_config_group_type', table_name='dspy_configs')
    op.drop_index('idx_dspy_config_type_active', table_name='dspy_configs')
    op.drop_table('dspy_configs')