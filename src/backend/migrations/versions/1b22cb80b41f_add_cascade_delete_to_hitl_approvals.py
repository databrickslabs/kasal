"""add_cascade_delete_to_hitl_approvals

Revision ID: 1b22cb80b41f
Revises: e4296c12a33d
Create Date: 2026-01-14 14:45:04.776182

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '1b22cb80b41f'
down_revision: Union[str, None] = 'e4296c12a33d'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add CASCADE delete to hitl_approvals.execution_id foreign key.

    When an execution is deleted, all associated HITL approvals should be
    automatically deleted to prevent foreign key constraint violations.
    """
    # Drop the old foreign key constraint
    op.drop_constraint(
        'hitl_approvals_execution_id_fkey',
        'hitl_approvals',
        type_='foreignkey'
    )

    # Create new foreign key constraint with CASCADE delete
    op.create_foreign_key(
        'hitl_approvals_execution_id_fkey',
        'hitl_approvals',
        'executionhistory',
        ['execution_id'],
        ['job_id'],
        ondelete='CASCADE'
    )


def downgrade() -> None:
    """Revert to foreign key without CASCADE delete."""
    # Drop the CASCADE foreign key
    op.drop_constraint(
        'hitl_approvals_execution_id_fkey',
        'hitl_approvals',
        type_='foreignkey'
    )

    # Recreate the original foreign key without CASCADE
    op.create_foreign_key(
        'hitl_approvals_execution_id_fkey',
        'hitl_approvals',
        'executionhistory',
        ['execution_id'],
        ['job_id']
    ) 