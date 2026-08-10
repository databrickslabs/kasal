"""executionhistory.crew_id — the crew half of flow_id

Revision ID: 20260810_exec_crew_id
Revises: 20260806_uicfg_toggles
Create Date: 2026-08-10

A flow execution records the saved flow it was built from (``flow_id``); a crew
execution recorded nothing. That gap is why a crew resume could only replay the
``inputs`` snapshot frozen when the original run started — with no link back to
a definition, there was nothing current to rebuild from, so a task description
edited on the canvas afterwards was invisible to the resume.

Nullable, because it is genuinely optional: a crew run started from an unsaved
canvas has no row to point at, and those runs keep resuming from the snapshot.

Note that Alembic does not run at startup in this project — ``init_db`` uses
``create_all`` plus ``run_schema_self_heal``. ``_ensure_execution_history_columns``
in ``src/db/session.py`` is what actually heals a deployed database; this
migration keeps the declared schema honest for anyone who does run the chain.
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID

revision = "20260810_exec_crew_id"
down_revision = "20260806_uicfg_toggles"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # SQLite has no UUID type and renders it as CHAR(32); binding the dialect
    # here keeps both backends on the same column the model declares.
    op.add_column(
        "executionhistory",
        sa.Column("crew_id", UUID(as_uuid=True), nullable=True),
    )
    op.create_index(
        "ix_executionhistory_crew_id", "executionhistory", ["crew_id"], unique=False
    )


def downgrade() -> None:
    op.drop_index("ix_executionhistory_crew_id", table_name="executionhistory")
    op.drop_column("executionhistory", "crew_id")
