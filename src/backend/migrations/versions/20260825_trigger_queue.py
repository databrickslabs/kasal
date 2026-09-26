"""triggerqueue: the event-driven trigger queue

Revision ID: 20260825_trigger_queue
Revises: 20260818_harness
Create Date: 2026-08-25

A durable Postgres/Lakebase queue that drives crew/flow runs (see
``services/triggers/`` and ``src/docs/EVENT_TRIGGERS.md``).

Note that alembic does not run at startup in this project (``init_db`` uses
``create_all`` plus ``run_schema_self_heal``), so this table is ALSO created by
``create_all`` because ``TriggerQueue`` is imported in ``models/__init__.py``.
This migration is the alembic-managed path for prod DBs. Idempotent: guarded by
a table-existence check.
"""

from alembic import op
import sqlalchemy as sa

revision = "20260825_trigger_queue"
down_revision = "20260818_harness"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    if "triggerqueue" in sa.inspect(bind).get_table_names():
        return
    op.create_table(
        "triggerqueue",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("group_id", sa.String(length=100), nullable=True),
        sa.Column("event_type", sa.String(length=255), nullable=True),
        sa.Column("target", sa.JSON(), nullable=True),
        sa.Column("payload", sa.JSON(), nullable=True),
        sa.Column(
            "status", sa.String(length=20), nullable=False, server_default="pending"
        ),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("available_at", sa.DateTime(), nullable=True),
        sa.Column("claimed_at", sa.DateTime(), nullable=True),
        sa.Column("last_error", sa.String(), nullable=True),
        sa.Column("correlation_id", sa.String(length=100), nullable=True),
        sa.Column("causation_run_id", sa.String(length=100), nullable=True),
        sa.Column(
            "idempotency_key", sa.String(length=255), nullable=True, unique=True
        ),
        sa.Column("created_at", sa.DateTime(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=True),
    )
    op.create_index(
        "ix_triggerqueue_status_available", "triggerqueue", ["status", "available_at"]
    )
    op.create_index("ix_triggerqueue_group_id", "triggerqueue", ["group_id"])
    op.create_index(
        "ix_triggerqueue_correlation_id", "triggerqueue", ["correlation_id"]
    )
    op.create_index("ix_triggerqueue_event_type", "triggerqueue", ["event_type"])


def downgrade() -> None:
    op.drop_index("ix_triggerqueue_event_type", table_name="triggerqueue")
    op.drop_index("ix_triggerqueue_correlation_id", table_name="triggerqueue")
    op.drop_index("ix_triggerqueue_group_id", table_name="triggerqueue")
    op.drop_index("ix_triggerqueue_status_available", table_name="triggerqueue")
    op.drop_table("triggerqueue")
