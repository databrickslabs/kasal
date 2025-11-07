"""add_tenant_id_to_prompttemplate

Revision ID: 2bc5aee43ad9
Revises: bb791d3a4812
Create Date: 2025-06-06 17:17:13.012345

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '2bc5aee43ad9'
down_revision: Union[str, None] = 'bb791d3a4812'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add tenant_id and created_by_email columns to prompttemplate if they don't exist."""
    
    # Check if columns already exist and add them if they don't
    conn = op.get_bind()
    
    # Check if tenant_id column exists
    result = conn.execute(
        sa.text("SELECT column_name FROM information_schema.columns WHERE table_name='prompttemplate' AND column_name='tenant_id'")
    )
    if not result.fetchone():
        print("Adding tenant_id column to prompttemplate")
        with op.batch_alter_table('prompttemplate', schema=None) as batch_op:
            batch_op.add_column(sa.Column('tenant_id', sa.String(length=100), nullable=True))
            batch_op.create_index('ix_prompttemplate_tenant_id', ['tenant_id'])
    else:
        print("tenant_id column already exists in prompttemplate")
    
    # Check if created_by_email column exists
    result = conn.execute(
        sa.text("SELECT column_name FROM information_schema.columns WHERE table_name='prompttemplate' AND column_name='created_by_email'")
    )
    if not result.fetchone():
        print("Adding created_by_email column to prompttemplate")
        with op.batch_alter_table('prompttemplate', schema=None) as batch_op:
            batch_op.add_column(sa.Column('created_by_email', sa.String(length=255), nullable=True))
            batch_op.create_index('ix_prompttemplate_created_by_email', ['created_by_email'])
    else:
        print("created_by_email column already exists in prompttemplate")


def downgrade() -> None:
    """Remove tenant_id and created_by_email columns from prompttemplate."""
    
    with op.batch_alter_table('prompttemplate', schema=None) as batch_op:
        batch_op.drop_index('ix_prompttemplate_created_by_email')
        batch_op.drop_index('ix_prompttemplate_tenant_id')
        batch_op.drop_column('created_by_email')
        batch_op.drop_column('tenant_id')