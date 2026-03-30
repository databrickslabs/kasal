"""add_flow_id_to_hitl_webhooks

Revision ID: e4296c12a33d
Revises: 7b636e814fa7
Create Date: 2026-01-13 10:32:39.798383

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = 'e4296c12a33d'
down_revision: Union[str, None] = '7b636e814fa7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add flow_id column to hitl_webhooks table
    # This allows webhooks to be scoped to specific flows
    # If flow_id is NULL, the webhook applies to all flows in the group
    op.add_column('hitl_webhooks', sa.Column('flow_id', sa.String(length=100), nullable=True))
    op.create_index(op.f('ix_hitl_webhooks_flow_id'), 'hitl_webhooks', ['flow_id'], unique=False)


def downgrade() -> None:
    op.drop_index(op.f('ix_hitl_webhooks_flow_id'), table_name='hitl_webhooks')
    op.drop_column('hitl_webhooks', 'flow_id')
