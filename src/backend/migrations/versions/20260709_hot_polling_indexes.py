"""add hot-polling indexes to executionhistory and execution_trace

Revision ID: 20260709_hot_polling_indexes
Revises: 20260609_ai_gateway
Create Date: 2026-07-09

The run-polling queries filter/sort on columns that had no index, so the two
biggest, fastest-growing tables were scanned on every 2s poll:
- executions list: WHERE group_id IN (...) ORDER BY created_at DESC
- trace broadcaster: WHERE status IN ('RUNNING', ...) every second
- trace reads/deletes by run_id, ordered reads by created_at

Existing deployed DBs are healed at startup by _ensure_hot_polling_indexes
(src/db/session.py) with the same index names; this migration keeps the
Alembic chain in sync. IF NOT EXISTS makes both paths idempotent.
"""
from alembic import op

# revision identifiers, used by Alembic.
revision = "20260709_hot_polling_indexes"
down_revision = "20260609_ai_gateway"
branch_labels = None
depends_on = None

_INDEXES = (
    ("idx_executionhistory_group_created", "executionhistory", "(group_id, created_at)"),
    ("ix_executionhistory_status", "executionhistory", "(status)"),
    ("ix_executionhistory_created_at", "executionhistory", "(created_at)"),
    ("ix_execution_trace_run_id", "execution_trace", "(run_id)"),
    ("ix_execution_trace_created_at", "execution_trace", "(created_at)"),
)


def upgrade() -> None:
    for name, table, cols in _INDEXES:
        op.execute(f"CREATE INDEX IF NOT EXISTS {name} ON {table} {cols}")


def downgrade() -> None:
    for name, _table, _cols in _INDEXES:
        op.execute(f"DROP INDEX IF EXISTS {name}")
