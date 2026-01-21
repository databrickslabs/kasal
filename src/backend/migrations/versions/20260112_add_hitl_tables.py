"""Add HITL (Human in the Loop) tables.

This migration creates the hitl_approvals and hitl_webhooks tables
for supporting human approval gates in flow execution.

Revision ID: 20260112_add_hitl
Revises:
Create Date: 2026-01-12

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '20260112_add_hitl'
down_revision = None  # Will be updated by Alembic
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create hitl_approvals table
    op.create_table(
        'hitl_approvals',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('execution_id', sa.String(), nullable=False),
        sa.Column('flow_id', sa.String(), nullable=False),
        sa.Column('gate_node_id', sa.String(), nullable=False),
        sa.Column('crew_sequence', sa.Integer(), nullable=False),
        sa.Column('status', sa.String(50), nullable=False, server_default='pending'),
        sa.Column('gate_config', sa.JSON(), nullable=False, server_default='{}'),
        sa.Column('previous_crew_name', sa.String(255), nullable=True),
        sa.Column('previous_crew_output', sa.Text(), nullable=True),
        sa.Column('flow_state_snapshot', sa.JSON(), nullable=True),
        sa.Column('responded_by', sa.String(255), nullable=True),
        sa.Column('responded_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('approval_comment', sa.Text(), nullable=True),
        sa.Column('rejection_reason', sa.Text(), nullable=True),
        sa.Column('rejection_action', sa.String(50), nullable=True),
        sa.Column('expires_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('webhook_sent', sa.Boolean(), nullable=False, server_default='false'),
        sa.Column('webhook_sent_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('webhook_response', sa.JSON(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column('group_id', sa.String(100), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.ForeignKeyConstraint(['execution_id'], ['executionhistory.job_id'], name='fk_hitl_approvals_execution_id')
    )

    # Create indexes for hitl_approvals
    op.create_index('ix_hitl_approvals_id', 'hitl_approvals', ['id'])
    op.create_index('ix_hitl_approvals_execution_id', 'hitl_approvals', ['execution_id'])
    op.create_index('ix_hitl_approvals_flow_id', 'hitl_approvals', ['flow_id'])
    op.create_index('ix_hitl_approvals_gate_node_id', 'hitl_approvals', ['gate_node_id'])
    op.create_index('ix_hitl_approvals_status', 'hitl_approvals', ['status'])
    op.create_index('ix_hitl_approvals_expires_at', 'hitl_approvals', ['expires_at'])
    op.create_index('ix_hitl_approvals_group_id', 'hitl_approvals', ['group_id'])

    # Create hitl_webhooks table
    op.create_table(
        'hitl_webhooks',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('group_id', sa.String(100), nullable=False),
        sa.Column('name', sa.String(255), nullable=False),
        sa.Column('url', sa.String(1000), nullable=False),
        sa.Column('enabled', sa.Boolean(), nullable=False, server_default='true'),
        sa.Column('events', sa.JSON(), nullable=False, server_default='["gate_reached"]'),
        sa.Column('headers', sa.JSON(), nullable=True),
        sa.Column('secret', sa.String(255), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )

    # Create indexes for hitl_webhooks
    op.create_index('ix_hitl_webhooks_id', 'hitl_webhooks', ['id'])
    op.create_index('ix_hitl_webhooks_group_id', 'hitl_webhooks', ['group_id'])


def downgrade() -> None:
    # Drop indexes first
    op.drop_index('ix_hitl_webhooks_group_id', table_name='hitl_webhooks')
    op.drop_index('ix_hitl_webhooks_id', table_name='hitl_webhooks')
    op.drop_index('ix_hitl_approvals_group_id', table_name='hitl_approvals')
    op.drop_index('ix_hitl_approvals_expires_at', table_name='hitl_approvals')
    op.drop_index('ix_hitl_approvals_status', table_name='hitl_approvals')
    op.drop_index('ix_hitl_approvals_gate_node_id', table_name='hitl_approvals')
    op.drop_index('ix_hitl_approvals_flow_id', table_name='hitl_approvals')
    op.drop_index('ix_hitl_approvals_execution_id', table_name='hitl_approvals')
    op.drop_index('ix_hitl_approvals_id', table_name='hitl_approvals')

    # Drop tables
    op.drop_table('hitl_webhooks')
    op.drop_table('hitl_approvals')
