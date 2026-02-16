"""add span_name status_code duration_ms to execution_trace

Revision ID: b2f3c4dee933
Revises: a1f2a3cbd822
Create Date: 2026-02-10

Adds OTel-native columns to execution_trace:
- span_name: raw OTel span name before mapping (e.g. "CrewAI.task.execute")
- status_code: OTel status ("OK", "ERROR", "UNSET")
- duration_ms: span duration in milliseconds
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "b2f3c4dee933"
down_revision = "a1f2a3cbd822"
branch_labels = None
depends_on = None


def upgrade() -> None:
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


def downgrade() -> None:
    op.drop_column("execution_trace", "duration_ms")
    op.drop_column("execution_trace", "status_code")
    op.drop_column("execution_trace", "span_name")
