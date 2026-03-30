"""Add PowerBI configuration table

Revision ID: 20251006_add_powerbi
Revises: 92fd57c71d02
Create Date: 2025-10-06 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


# revision identifiers, used by Alembic.
revision = '20251006_add_powerbi'
down_revision = '92fd57c71d02'
branch_labels = None
depends_on = None


def upgrade():
    """Create powerbiconfig table for Power BI integration."""

    conn = op.get_bind()
    inspector = sa.inspect(conn)

    if 'powerbiconfig' not in inspector.get_table_names():
        op.create_table('powerbiconfig',
            sa.Column('id', sa.Integer(), primary_key=True),
            sa.Column('tenant_id', sa.String(), nullable=False),
            sa.Column('client_id', sa.String(), nullable=False),
            sa.Column('encrypted_client_secret', sa.String(), nullable=True),
            sa.Column('workspace_id', sa.String(), nullable=True),
            sa.Column('semantic_model_id', sa.String(), nullable=True),
            sa.Column('encrypted_username', sa.String(), nullable=True),
            sa.Column('encrypted_password', sa.String(), nullable=True),
            sa.Column('is_active', sa.Boolean(), default=True),
            sa.Column('is_enabled', sa.Boolean(), default=True),
            sa.Column('group_id', sa.String(length=100), nullable=True),
            sa.Column('created_by_email', sa.String(length=255), nullable=True),
            sa.Column('created_at', sa.DateTime(timezone=True), nullable=True),
            sa.Column('updated_at', sa.DateTime(timezone=True), nullable=True)
        )
        op.create_index('ix_powerbiconfig_group_id', 'powerbiconfig', ['group_id'], unique=False)
        op.create_index('ix_powerbiconfig_created_by_email', 'powerbiconfig', ['created_by_email'], unique=False)


def downgrade():
    """Remove powerbiconfig table."""

    op.drop_index('ix_powerbiconfig_created_by_email', table_name='powerbiconfig')
    op.drop_index('ix_powerbiconfig_group_id', table_name='powerbiconfig')
    op.drop_table('powerbiconfig')
