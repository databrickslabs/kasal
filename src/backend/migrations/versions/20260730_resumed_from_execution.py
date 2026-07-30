"""executionhistory — link a resumed run to the run it was resumed from

Revision ID: 20260730_resumed_from
Revises: 20260730_mlflow_config
Create Date: 2026-07-30

Resuming used to re-run the SAME execution record: the crashed row was flipped
back to RUNNING and reused. That made three things impossible at once — a
terminal FAILED record that stays failed for audit, per-attempt cost
attribution, and a readable trace timeline (``execution_trace`` and
``execution_logs`` are both keyed by ``job_id``, so a resumed run's rows
interleaved with the crashed attempt's under one id with no boundary between
them).

Resume now creates a NEW execution and points it here. The flow path already
worked this way; this is what lets the crew path join it.

Nullable with no backfill: every existing row predates resume-as-new-execution
and genuinely has no source, so NULL is the correct value rather than a gap.

No ForeignKey on purpose. Purging an old run must not cascade away the
successful resume that replaced it, and executionhistory is already referenced
by job_id from several tables without FK constraints. Indexed because the run
detail view looks up a run's resume chain on every open.
"""

import sqlalchemy as sa
from alembic import op

revision = "20260730_resumed_from"
down_revision = "20260730_mlflow_config"
branch_labels = None
depends_on = None


TABLE = "executionhistory"
COLUMN = "resumed_from_execution_id"
INDEX = "ix_executionhistory_resumed_from_execution_id"


def _has_column(bind) -> bool:
    return COLUMN in {col["name"] for col in sa.inspect(bind).get_columns(TABLE)}


def _has_index(bind) -> bool:
    return INDEX in {ix["name"] for ix in sa.inspect(bind).get_indexes(TABLE)}


def upgrade() -> None:
    bind = op.get_bind()

    # Guarded rather than unconditional: this branchy migration graph has
    # several heads, and a re-run against a database that already took the
    # column should be a no-op instead of a hard failure.
    if not _has_column(bind):
        op.add_column(TABLE, sa.Column(COLUMN, sa.Integer(), nullable=True))

    if not _has_index(bind):
        op.create_index(INDEX, TABLE, [COLUMN])


def downgrade() -> None:
    bind = op.get_bind()

    if _has_index(bind):
        op.drop_index(INDEX, table_name=TABLE)

    if _has_column(bind):
        op.drop_column(TABLE, COLUMN)
