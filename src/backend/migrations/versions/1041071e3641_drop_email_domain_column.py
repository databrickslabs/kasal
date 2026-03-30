"""drop_email_domain_column

Revision ID: 1041071e3641
Revises: 8036d95e0e65
Create Date: 2025-09-20 16:59:56.066616

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '1041071e3641'
down_revision: Union[str, None] = '8036d95e0e65'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Drop email_domain column from groups table
    op.drop_column('groups', 'email_domain')


def downgrade() -> None:
    # Add back email_domain column in case of rollback
    op.add_column('groups', sa.Column('email_domain', sa.String(255), nullable=True)) 