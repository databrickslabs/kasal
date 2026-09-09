"""Teamspace model rates for usage estimates.

Revision ID: 20260908_model_billing
Revises: a42ca4e55f06
"""

import sqlalchemy as sa
from alembic import op

revision = "20260908_model_billing"
down_revision = "a42ca4e55f06"
branch_labels = None
depends_on = None


def upgrade():
    if "model_billing_rates" in sa.inspect(op.get_bind()).get_table_names():
        return
    op.create_table(
        "model_billing_rates",
        sa.Column("group_id", sa.String(100), primary_key=True),
        sa.Column("model", sa.String(255), primary_key=True),
        sa.Column("input_per_million", sa.Numeric(18, 8), nullable=False),
        sa.Column("output_per_million", sa.Numeric(18, 8), nullable=False),
        sa.Column("cached_input_per_million", sa.Numeric(18, 8), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=True),
    )


def downgrade():
    op.drop_table("model_billing_rates")
