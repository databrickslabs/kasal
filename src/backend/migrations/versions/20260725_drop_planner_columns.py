"""Drop the dead planner columns (crews.planning/planning_llm, schedule.planning)

The inherited CrewAI-style prose planner was removed: the native ``kasal_engine``
has no planner at all, so these columns were written by the UI and then ignored
end to end. Dropping them keeps the schema honest.

Deliberately NOT dropped: ``executionhistory.planning`` — that is a historical
record of what a past run was submitted with, not live configuration. And
``schedule.model`` is the model the scheduled RUN uses (its old comment said
"model to use for planning", which was wrong), so it stays.

Uses batch_alter_table so SQLite (which needs a table rebuild for DROP COLUMN
before 3.35) works the same as PostgreSQL / Lakebase.

Verified: offline generation for PostgreSQL/Lakebase (``alembic upgrade
<rev>:<rev> --sql``) emits the three DROP COLUMNs plus the version bump.
Offline generation against SQLite is NOT possible — Alembic's batch mode needs
a live connection to reflect the table unless a full Table is passed via
``copy_from`` — so run SQLite online (the normal path). Dev SQLite databases
are built by ``create_all`` + the column self-heals rather than by Alembic, so
they simply stop growing these columns; existing dev files keep them harmlessly.

Revision ID: 20260725_drop_planner_columns
Revises: 20260724_engine_name_kasal
Create Date: 2026-07-25 10:00:00.000000

"""
import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision = '20260725_drop_planner_columns'
down_revision = '20260724_engine_name_kasal'
branch_labels = None
depends_on = None


# (table, column) pairs, dropped in upgrade / re-added in downgrade.
_COLUMNS = (
    ('crews', 'planning', sa.Boolean()),
    ('crews', 'planning_llm', sa.String(length=255)),
    ('schedule', 'planning', sa.Boolean()),
)


def _existing_columns(table: str) -> set:
    """Columns currently on ``table``, or None when introspection is impossible.

    Offline mode (``alembic upgrade --sql``, used to hand a reviewable script to
    a DBA) has no live connection: ``sa.inspect`` on the MockConnection raises
    NoInspectionAvailable. Returning None there means "unknown" so callers emit
    the DDL unconditionally, which is what an offline script must contain.
    """
    if op.get_context().as_sql:
        return None
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if table not in inspector.get_table_names():
        return set()
    return {c['name'] for c in inspector.get_columns(table)}


def upgrade():
    for table in ('crews', 'schedule'):
        existing = _existing_columns(table)
        targets = [
            c for t, c, _ in _COLUMNS
            if t == table and (existing is None or c in existing)
        ]
        if not targets:
            continue
        with op.batch_alter_table(table) as batch_op:
            for column in targets:
                batch_op.drop_column(column)


def downgrade():
    # Re-added as nullable (no backfill possible — the values were meaningless).
    for table in ('crews', 'schedule'):
        existing = _existing_columns(table)
        targets = [
            (c, t_) for t, c, t_ in _COLUMNS
            if t == table and (existing is None or c not in existing)
        ]
        if not targets:
            continue
        with op.batch_alter_table(table) as batch_op:
            for column, coltype in targets:
                batch_op.add_column(sa.Column(column, coltype, nullable=True))
