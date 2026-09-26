"""merge migration heads for chat history

Revision ID: a2521320b527
Revises: 20250607_tenant_to_group, remove_domain_unique
Create Date: 2025-06-10 15:22:17.278623

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a2521320b527'
down_revision: Union[str, None] = ('20250607_tenant_to_group', 'remove_domain_unique')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass 