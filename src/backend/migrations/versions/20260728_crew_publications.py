"""add crew_publications — the registry of crews exposed outside the workspace

Revision ID: 20260728_crew_publications
Revises: 20260726_recipe_trials
Create Date: 2026-07-28

A crew is reachable from outside Kasal only if it has a row here. Nothing is
published by default, and the row IS the record of someone deciding to publish.

One record per crew with a `protocols` list, rather than an `mcp_published` flag
beside an `a2a_published` flag: `description` and `input_schema` are needed
identically by both surfaces, and two copies drift until one is quietly wrong
while each surface still looks correct on its own. The MCP tool list and the A2A
Agent Card's skills[] are two projections of this one table.

`group_id` is NOT NULL. Every read of this table is group-filtered because the
callers are external by definition; a row with no group would sail past a filter
written as `.in_(group_ids)` and be exposed to whoever asked first.
"""

import sqlalchemy as sa
from alembic import op

revision = "20260728_crew_publications"
down_revision = "20260726_recipe_trials"
branch_labels = None
depends_on = None

_TABLE = "crew_publications"


def upgrade() -> None:
    # Offline (--sql) generation cannot introspect; emit the CREATE and let the
    # deployment's own guard handle a pre-existing table.
    if not op.get_context().as_sql:
        inspector = sa.inspect(op.get_bind())
        if _TABLE in inspector.get_table_names():
            return

    op.create_table(
        _TABLE,
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("crew_id", sa.String(length=255), nullable=False),
        # e.g. ["mcp", "a2a"]. An empty list keeps the name/description/schema
        # someone wrote while exposing nothing — toggling a protocol off must
        # not destroy the publication.
        sa.Column("protocols", sa.JSON(), nullable=False),
        sa.Column("external_name", sa.String(length=64), nullable=False),
        sa.Column("description", sa.String(length=1024), nullable=False),
        sa.Column("input_schema", sa.JSON(), nullable=True),
        sa.Column("group_id", sa.String(length=100), nullable=False),
        sa.Column("created_by_email", sa.String(length=255), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        # A crew is published once; its protocols live inside the row.
        sa.UniqueConstraint("crew_id", name="uq_crew_publication_crew"),
        # The external name is how a caller addresses the capability, so it must
        # be unambiguous within a group. Across groups it may repeat.
        sa.UniqueConstraint(
            "external_name", "group_id", name="uq_crew_publication_name_group"
        ),
    )
    op.create_index("ix_crew_publications_crew_id", _TABLE, ["crew_id"])
    op.create_index("ix_crew_publications_group_id", _TABLE, ["group_id"])
    # The hot path: every external capability listing filters on group first.
    op.create_index(
        "idx_crew_publications_group_name", _TABLE, ["group_id", "external_name"]
    )


def downgrade() -> None:
    if not op.get_context().as_sql:
        inspector = sa.inspect(op.get_bind())
        if _TABLE not in inspector.get_table_names():
            return

    op.drop_index("idx_crew_publications_group_name", table_name=_TABLE)
    op.drop_index("ix_crew_publications_group_id", table_name=_TABLE)
    op.drop_index("ix_crew_publications_crew_id", table_name=_TABLE)
    op.drop_table(_TABLE)
