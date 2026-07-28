"""add agents.skills — Agent Skills attached to an agent

Revision ID: 20260729_agent_skills
Revises: 20260729_skills
Create Date: 2026-07-29

Stored as NAMES rather than ids: a skill's name is its identity in the Agent
Skills format and must match the folder it exports to, so a name survives an
export/import round trip and keeps resolving when a workspace overrides a
built-in skill with its own version.
"""

import sqlalchemy as sa
from alembic import op

revision = "20260729_agent_skills"
down_revision = "20260729_skills"
branch_labels = None
depends_on = None


def upgrade() -> None:
    if not op.get_context().as_sql:
        inspector = sa.inspect(op.get_bind())
        if "agents" not in inspector.get_table_names():
            return
        if any(c["name"] == "skills" for c in inspector.get_columns("agents")):
            return
    op.add_column("agents", sa.Column("skills", sa.JSON(), nullable=True))


def downgrade() -> None:
    if not op.get_context().as_sql:
        inspector = sa.inspect(op.get_bind())
        if not any(c["name"] == "skills" for c in inspector.get_columns("agents")):
            return
    op.drop_column("agents", "skills")
