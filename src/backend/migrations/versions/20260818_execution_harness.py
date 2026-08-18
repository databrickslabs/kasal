"""executionhistory.harness: which agent runtime ran this execution

Revision ID: 20260818_harness
Revises: 20260812_span_kasal
Create Date: 2026-08-18

CrewAI is a selectable engine again, beside Kasal's own runtime, so "which
engine ran this?" becomes a question with two possible answers and therefore
one that has to be recorded rather than inferred.

It is recorded PER EXECUTION, not read from the configuration when needed, and
that distinction is the whole point:

  * a run that started before an operator switched engines must finish on the
    engine it started on — re-reading the setting mid-run would build half a
    crew on one runtime and half on the other;
  * a RESUME has to continue on whatever wrote the checkpoint it is resuming
    from, and that engine may no longer be the configured one;
  * and a finished run has to be able to say what produced it, months later,
    after the setting has changed several times.

NULL is meaningful and is not backfilled: every row written before this column
existed ran on the Kasal runtime, because it was the only one, and readers
treat NULL as "kasal". Writing the string into millions of historical rows
would claim a decision nobody made.

Note that alembic does not run at startup in this project (``init_db`` uses
``create_all`` plus ``run_schema_self_heal``), so the column is ALSO added by
``_ensure_execution_history_columns`` in ``db/session.py``. Both are idempotent
and either can run first.
"""

from alembic import op
import sqlalchemy as sa

revision = "20260818_harness"
down_revision = "20260812_span_kasal"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    existing = {c["name"] for c in sa.inspect(bind).get_columns("executionhistory")}
    if "harness" not in existing:
        op.add_column(
            "executionhistory",
            sa.Column("harness", sa.String(length=20), nullable=True),
        )
        op.create_index(
            "ix_executionhistory_harness",
            "executionhistory",
            ["harness"],
        )


def downgrade() -> None:
    op.drop_index("ix_executionhistory_harness", table_name="executionhistory")
    op.drop_column("executionhistory", "harness")
