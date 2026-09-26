"""Consolidate flow_executions into executionhistory

This migration adds execution_type and flow_id columns to executionhistory
and migrates data from flow_executions. This consolidates all execution
tracking into a single table.

Revision ID: consolidate_exec_001
Revises: add_llm_guardrail, remove_flow_id_fk
Create Date: 2024-11-26
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'consolidate_exec_001'
down_revision = ('add_llm_guardrail', 'remove_flow_id_fk')  # Merge both heads
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add execution_type column to executionhistory
    op.add_column('executionhistory',
        sa.Column('execution_type', sa.String(20), nullable=True, server_default='crew')
    )
    op.create_index(op.f('ix_executionhistory_execution_type'), 'executionhistory', ['execution_type'], unique=False)

    # Add flow_id column to executionhistory (UUID, nullable)
    op.add_column('executionhistory',
        sa.Column('flow_id', postgresql.UUID(as_uuid=True), nullable=True)
    )
    op.create_index(op.f('ix_executionhistory_flow_id'), 'executionhistory', ['flow_id'], unique=False)

    # Migrate data from flow_executions to executionhistory
    connection = op.get_bind()

    # Check if flow_executions table exists
    inspector = sa.inspect(connection)
    if 'flow_executions' in inspector.get_table_names():
        # Migrate flow executions that have a job_id not already in executionhistory
        connection.execute(sa.text("""
            INSERT INTO executionhistory (job_id, status, inputs, result, error, run_name,
                                         created_at, completed_at, group_id, execution_type, flow_id)
            SELECT fe.job_id, fe.status, fe.config, fe.result, fe.error, fe.run_name,
                   fe.created_at, fe.completed_at, fe.group_id, 'flow', fe.flow_id
            FROM flow_executions fe
            WHERE NOT EXISTS (
                SELECT 1 FROM executionhistory eh WHERE eh.job_id = fe.job_id
            )
        """))

        # Update existing executionhistory records that correspond to flow executions
        connection.execute(sa.text("""
            UPDATE executionhistory
            SET execution_type = 'flow',
                flow_id = (SELECT fe.flow_id FROM flow_executions fe WHERE fe.job_id = executionhistory.job_id)
            WHERE EXISTS (
                SELECT 1 FROM flow_executions fe WHERE fe.job_id = executionhistory.job_id
            )
        """))


def downgrade() -> None:
    # Remove flow_id column from executionhistory
    op.drop_index(op.f('ix_executionhistory_flow_id'), table_name='executionhistory')
    op.drop_column('executionhistory', 'flow_id')

    # Remove execution_type column from executionhistory
    op.drop_index(op.f('ix_executionhistory_execution_type'), table_name='executionhistory')
    op.drop_column('executionhistory', 'execution_type')
