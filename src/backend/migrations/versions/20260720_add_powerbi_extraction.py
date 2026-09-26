"""add powerbi_extraction table for raw Power BI extraction artifacts

Revision ID: 20260720_powerbi_extraction
Revises: 20260709_hot_polling_indexes
Create Date: 2026-07-20

Persists the raw artifacts the Pipeline Config Generator extracts per run
(relationships, measures + DAX, admin/TMDL table metadata, report definition,
derived config) so they are SQL-queryable after the fact. Existing deployed DBs
are healed at startup by _ensure_powerbi_extraction_table (src/db/session.py)
with the same table; this migration keeps the Alembic chain in sync. checkfirst
/ IF NOT EXISTS make both paths idempotent.
"""
from alembic import op
import sqlalchemy as sa


revision = "20260720_powerbi_extraction"
down_revision = "20260709_hot_polling_indexes"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if "powerbi_extraction" in inspector.get_table_names():
        return  # startup self-heal already created it

    op.create_table(
        "powerbi_extraction",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("execution_id", sa.String(length=100), nullable=True),
        sa.Column("workspace_id", sa.String(length=100), nullable=True),
        sa.Column("dataset_id", sa.String(length=100), nullable=True),
        sa.Column("report_id", sa.String(length=100), nullable=True),
        sa.Column("relationships", sa.JSON(), nullable=True),
        sa.Column("measures", sa.JSON(), nullable=True),
        sa.Column("admin_tables", sa.JSON(), nullable=True),
        sa.Column("report_definition", sa.JSON(), nullable=True),
        sa.Column("proposed_config", sa.JSON(), nullable=True),
        sa.Column("warnings", sa.JSON(), nullable=True),
        sa.Column("relationships_count", sa.Integer(), nullable=True),
        sa.Column("measures_count", sa.Integer(), nullable=True),
        sa.Column("measures_with_dax_count", sa.Integer(), nullable=True),
        sa.Column("admin_tables_count", sa.Integer(), nullable=True),
        sa.Column("summary", sa.Text(), nullable=True),
        sa.Column("group_id", sa.String(length=100), nullable=True),
        sa.Column("created_by_email", sa.String(length=255), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_powerbi_extraction_execution_id", "powerbi_extraction", ["execution_id"])
    op.create_index("ix_powerbi_extraction_workspace_id", "powerbi_extraction", ["workspace_id"])
    op.create_index("ix_powerbi_extraction_dataset_id", "powerbi_extraction", ["dataset_id"])
    op.create_index("ix_powerbi_extraction_group_id", "powerbi_extraction", ["group_id"])
    op.create_index(
        "ix_powerbi_extraction_group_created", "powerbi_extraction", ["group_id", "created_at"])
    op.create_index(
        "ix_powerbi_extraction_workspace_dataset",
        "powerbi_extraction", ["workspace_id", "dataset_id"])


def downgrade() -> None:
    op.drop_table("powerbi_extraction")
