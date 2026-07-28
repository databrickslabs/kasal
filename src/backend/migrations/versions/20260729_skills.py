"""add skills + skill_files — Agent Skills storage

Revision ID: 20260729_skills
Revises: 20260729_a2a_agents
Create Date: 2026-07-29

Agent Skills package procedural know-how — "how we do X here" — which today has
nowhere to live but an agent's backstory. Rows rather than a directory because
Kasal is multi-tenant and runs on stateless containers; the FORMAT stays the
standard's, and packaging round-trips a row to the folder every other client
reads.
"""

import sqlalchemy as sa
from alembic import op

revision = "20260729_skills"
down_revision = "20260729_a2a_agents"
branch_labels = None
depends_on = None

_SKILLS = "skills"
_FILES = "skill_files"


def upgrade() -> None:
    inspector = None
    if not op.get_context().as_sql:
        inspector = sa.inspect(op.get_bind())

    if inspector is None or _SKILLS not in inspector.get_table_names():
        op.create_table(
            _SKILLS,
            sa.Column("id", sa.Integer(), nullable=False),
            sa.Column("name", sa.String(length=64), nullable=False),
            sa.Column("description", sa.String(length=1024), nullable=False),
            sa.Column("body", sa.Text(), nullable=False, server_default=""),
            sa.Column("license", sa.String(length=255), nullable=True),
            sa.Column("compatibility", sa.String(length=500), nullable=True),
            sa.Column("skill_metadata", sa.JSON(), nullable=True),
            sa.Column("source", sa.String(length=32), nullable=False,
                      server_default="authored"),
            sa.Column("group_id", sa.String(length=100), nullable=True),
            sa.Column("created_by_email", sa.String(length=255), nullable=True),
            sa.Column("enabled", sa.Boolean(), nullable=True),
            sa.Column("global_enabled", sa.Boolean(), nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=True),
            sa.Column("updated_at", sa.DateTime(), nullable=True),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint("name", "group_id", name="uq_skill_name_group"),
        )
        op.create_index("ix_skills_group_id", _SKILLS, ["group_id"])

    if inspector is None or _FILES not in inspector.get_table_names():
        op.create_table(
            _FILES,
            sa.Column("id", sa.Integer(), nullable=False),
            sa.Column("skill_id", sa.Integer(), nullable=False),
            sa.Column("path", sa.String(length=500), nullable=False),
            sa.Column("content", sa.Text(), nullable=False, server_default=""),
            sa.Column("sha256", sa.String(length=64), nullable=True),
            sa.Column("size_bytes", sa.Integer(), nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=True),
            sa.PrimaryKeyConstraint("id"),
            sa.ForeignKeyConstraint(["skill_id"], ["skills.id"], ondelete="CASCADE"),
            sa.UniqueConstraint("skill_id", "path", name="uq_skillfile_skill_path"),
        )
        op.create_index("ix_skill_files_skill_id", _FILES, ["skill_id"])


def downgrade() -> None:
    if not op.get_context().as_sql:
        inspector = sa.inspect(op.get_bind())
        names = inspector.get_table_names()
        if _FILES in names:
            op.drop_index("ix_skill_files_skill_id", table_name=_FILES)
            op.drop_table(_FILES)
        if _SKILLS in names:
            op.drop_index("ix_skills_group_id", table_name=_SKILLS)
            op.drop_table(_SKILLS)
        return
    op.drop_table(_FILES)
    op.drop_table(_SKILLS)
