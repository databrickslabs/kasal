"""add_converter_models

Revision ID: cf0a3479e307
Revises: 665ffadb181e
Create Date: 2025-12-01 07:31:43.174060

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'cf0a3479e307'
down_revision: Union[str, None] = '665ffadb181e'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create conversion_history table
    op.create_table(
        'conversion_history',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('execution_id', sa.String(length=100), nullable=True),
        sa.Column('job_id', sa.String(length=100), nullable=True),
        sa.Column('source_format', sa.String(length=50), nullable=False),
        sa.Column('target_format', sa.String(length=50), nullable=False),
        sa.Column('input_data', sa.JSON(), nullable=True),
        sa.Column('input_summary', sa.Text(), nullable=True),
        sa.Column('output_data', sa.JSON(), nullable=True),
        sa.Column('output_summary', sa.Text(), nullable=True),
        sa.Column('configuration', sa.JSON(), nullable=True),
        sa.Column('status', sa.String(length=20), nullable=False, server_default='pending'),
        sa.Column('error_message', sa.Text(), nullable=True),
        sa.Column('warnings', sa.JSON(), nullable=True),
        sa.Column('measure_count', sa.Integer(), nullable=True),
        sa.Column('execution_time_ms', sa.Integer(), nullable=True),
        sa.Column('converter_version', sa.String(length=50), nullable=True),
        sa.Column('extra_metadata', sa.JSON(), nullable=True),
        sa.Column('group_id', sa.String(length=100), nullable=True),
        sa.Column('created_by_email', sa.String(length=255), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('CURRENT_TIMESTAMP')),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('CURRENT_TIMESTAMP')),
        sa.PrimaryKeyConstraint('id')
    )

    # Create indexes for conversion_history
    op.create_index('ix_conversion_history_execution_id', 'conversion_history', ['execution_id'])
    op.create_index('ix_conversion_history_job_id', 'conversion_history', ['job_id'])
    op.create_index('ix_conversion_history_source_format', 'conversion_history', ['source_format'])
    op.create_index('ix_conversion_history_target_format', 'conversion_history', ['target_format'])
    op.create_index('ix_conversion_history_group_id', 'conversion_history', ['group_id'])
    op.create_index('ix_conversion_history_group_created', 'conversion_history', ['group_id', 'created_at'])
    op.create_index('ix_conversion_history_status_created', 'conversion_history', ['status', 'created_at'])
    op.create_index('ix_conversion_history_formats', 'conversion_history', ['source_format', 'target_format'])

    # Create conversion_jobs table
    op.create_table(
        'conversion_jobs',
        sa.Column('id', sa.String(length=100), nullable=False),
        sa.Column('name', sa.String(length=255), nullable=True),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('tool_id', sa.Integer(), nullable=True),
        sa.Column('source_format', sa.String(length=50), nullable=False),
        sa.Column('target_format', sa.String(length=50), nullable=False),
        sa.Column('configuration', sa.JSON(), nullable=False),
        sa.Column('status', sa.String(length=20), nullable=False, server_default='pending'),
        sa.Column('progress', sa.Float(), nullable=True),
        sa.Column('result', sa.JSON(), nullable=True),
        sa.Column('error_message', sa.Text(), nullable=True),
        sa.Column('execution_id', sa.String(length=100), nullable=True),
        sa.Column('history_id', sa.Integer(), nullable=True),
        sa.Column('extra_metadata', sa.JSON(), nullable=True),
        sa.Column('group_id', sa.String(length=100), nullable=True),
        sa.Column('created_by_email', sa.String(length=255), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('CURRENT_TIMESTAMP')),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('CURRENT_TIMESTAMP')),
        sa.Column('started_at', sa.DateTime(), nullable=True),
        sa.Column('completed_at', sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(['tool_id'], ['tools.id']),
        sa.ForeignKeyConstraint(['history_id'], ['conversion_history.id']),
        sa.PrimaryKeyConstraint('id')
    )

    # Create indexes for conversion_jobs
    op.create_index('ix_conversion_jobs_execution_id', 'conversion_jobs', ['execution_id'])
    op.create_index('ix_conversion_jobs_group_id', 'conversion_jobs', ['group_id'])
    op.create_index('ix_conversion_jobs_group_created', 'conversion_jobs', ['group_id', 'created_at'])
    op.create_index('ix_conversion_jobs_status', 'conversion_jobs', ['status'])

    # Create saved_converter_configurations table
    op.create_table(
        'saved_converter_configurations',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('name', sa.String(length=255), nullable=False),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('source_format', sa.String(length=50), nullable=False),
        sa.Column('target_format', sa.String(length=50), nullable=False),
        sa.Column('configuration', sa.JSON(), nullable=False),
        sa.Column('use_count', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('last_used_at', sa.DateTime(), nullable=True),
        sa.Column('is_public', sa.Boolean(), nullable=False, server_default='0'),
        sa.Column('is_template', sa.Boolean(), nullable=False, server_default='0'),
        sa.Column('tags', sa.JSON(), nullable=True),
        sa.Column('extra_metadata', sa.JSON(), nullable=True),
        sa.Column('group_id', sa.String(length=100), nullable=True),
        sa.Column('created_by_email', sa.String(length=255), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('CURRENT_TIMESTAMP')),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('CURRENT_TIMESTAMP')),
        sa.PrimaryKeyConstraint('id')
    )

    # Create indexes for saved_converter_configurations
    op.create_index('ix_saved_converter_configurations_group_id', 'saved_converter_configurations', ['group_id'])
    op.create_index('ix_saved_converter_configurations_created_by_email', 'saved_converter_configurations', ['created_by_email'])
    op.create_index('ix_saved_configs_group_user', 'saved_converter_configurations', ['group_id', 'created_by_email'])
    op.create_index('ix_saved_configs_formats', 'saved_converter_configurations', ['source_format', 'target_format'])
    op.create_index('ix_saved_configs_public', 'saved_converter_configurations', ['is_public', 'is_template'])


def downgrade() -> None:
    # Drop tables in reverse order (respecting foreign key constraints)
    op.drop_table('saved_converter_configurations')
    op.drop_table('conversion_jobs')
    op.drop_table('conversion_history') 