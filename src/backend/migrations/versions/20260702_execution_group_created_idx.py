"""add composite (group_id, created_at) index to executionhistory

Revision ID: 20260702_exec_group_created
Revises: 20260609_ai_gateway
Create Date: 2026-07-02 00:00:00.000000

The executions list endpoint (the most-polled endpoint) filters by group_id and
orders by created_at DESC. group_id and created_at were each covered separately
(created_at not at all), forcing a full-partition sort per page load. A composite
btree on (group_id, created_at) serves the filter + ORDER BY + LIMIT via an index
range scan (backward for DESC).
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '20260702_exec_group_created'
down_revision: Union[str, None] = '20260609_ai_gateway'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

INDEX_NAME = 'idx_executionhistory_group_created'
TABLE_NAME = 'executionhistory'


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    existing = {ix['name'] for ix in inspector.get_indexes(TABLE_NAME)}
    if INDEX_NAME not in existing:
        op.create_index(INDEX_NAME, TABLE_NAME, ['group_id', 'created_at'])


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    existing = {ix['name'] for ix in inspector.get_indexes(TABLE_NAME)}
    if INDEX_NAME in existing:
        op.drop_index(INDEX_NAME, table_name=TABLE_NAME)
