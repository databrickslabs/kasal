"""flow_states — add the tenant column every other model already has

Revision ID: 20260801_flow_group
Revises: 20260730_resumed_from
Create Date: 2026-08-01

``flow_states`` holds a flow's checkpoints — the state a run resumes from, and
now the running state of a conversation. It was the only table in the checkpoint
path with no ``group_id``, so nothing about it could be filtered, listed or
purged per tenant; every query had to address a row by its ``flow_uuid`` and
trust that the caller had come by it honestly.

That was defensible while a lineage id was a random UUID minted per run and read
by nothing but a resume. It stops being defensible now that a lineage is DERIVED
from a chat session and holds a conversation: the rows are user-facing data, and
a listing endpoint over them cannot be written safely without this column.

Nullable with no backfill. Existing rows are checkpoints of runs that predate
the column, and their group cannot be recovered from the row itself — the
``flow_uuid`` is a state id, not a foreign key to anything carrying a group. A
wrong backfill would be worse than a NULL: it would look authoritative. Reads
therefore treat NULL as "unknown tenant, match only by lineage id", which is
exactly the behaviour those rows have today.

Indexed together with ``flow_uuid``, because every tenant-scoped read of this
table asks the same question — this group's rows for this lineage.
"""

import sqlalchemy as sa
from alembic import op

revision = "20260801_flow_group"
down_revision = "20260730_resumed_from"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("flow_states") as batch:
        batch.add_column(sa.Column("group_id", sa.String(length=100), nullable=True))
    op.create_index(
        "ix_flow_states_group_uuid", "flow_states", ["group_id", "flow_uuid"]
    )


def downgrade() -> None:
    op.drop_index("ix_flow_states_group_uuid", table_name="flow_states")
    with op.batch_alter_table("flow_states") as batch:
        batch.drop_column("group_id")
