"""add otel span columns to execution_trace

Revision ID: a1f2a3cbd822
Revises: 3e3762648db4
Create Date: 2026-02-10

Adds OTel span hierarchy columns (span_id, trace_id, parent_span_id) and
OTel-native columns (span_name, status_code, duration_ms) to the
execution_trace table. All nullable — existing rows get NULL.
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "a1f2a3cbd822"
down_revision = "3e3762648db4"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # OTel span hierarchy
    op.add_column(
        "execution_trace",
        sa.Column("span_id", sa.String(32), nullable=True),
    )
    op.add_column(
        "execution_trace",
        sa.Column("trace_id", sa.String(32), nullable=True),
    )
    op.add_column(
        "execution_trace",
        sa.Column("parent_span_id", sa.String(32), nullable=True),
    )
    # OTel-native fields
    op.add_column(
        "execution_trace",
        sa.Column("span_name", sa.String(200), nullable=True),
    )
    op.add_column(
        "execution_trace",
        sa.Column("status_code", sa.String(10), nullable=True),
    )
    op.add_column(
        "execution_trace",
        sa.Column("duration_ms", sa.Integer(), nullable=True),
    )
    # Indexes for hierarchy lookups
    op.create_index(
        "ix_execution_trace_span_id", "execution_trace", ["span_id"]
    )
    op.create_index(
        "ix_execution_trace_trace_id", "execution_trace", ["trace_id"]
    )


def downgrade() -> None:
    op.drop_index("ix_execution_trace_trace_id", table_name="execution_trace")
    op.drop_index("ix_execution_trace_span_id", table_name="execution_trace")
    op.drop_column("execution_trace", "duration_ms")
    op.drop_column("execution_trace", "status_code")
    op.drop_column("execution_trace", "span_name")
    op.drop_column("execution_trace", "parent_span_id")
    op.drop_column("execution_trace", "trace_id")
    op.drop_column("execution_trace", "span_id")
