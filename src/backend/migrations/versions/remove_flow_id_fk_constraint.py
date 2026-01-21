"""remove flow_id foreign key constraint for ad-hoc executions

Revision ID: remove_flow_id_fk
Revises: update_user_roles_to_3tier
Create Date: 2025-11-16 17:55:00.000000

This migration removes the foreign key constraint on flow_executions.flow_id
and makes it nullable, allowing ad-hoc flow execution without requiring
a saved flow in the database (similar to how crew executions work).
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'remove_flow_id_fk'
down_revision = 'update_user_roles_3tier'
branch_labels = None
depends_on = None


def upgrade():
    """
    Remove foreign key constraint and make flow_id nullable.
    This allows "test before save" workflow for flows.
    """
    # Drop the foreign key constraint
    op.drop_constraint('flow_executions_flow_id_fkey', 'flow_executions', type_='foreignkey')

    # Make flow_id nullable
    op.alter_column('flow_executions', 'flow_id',
                    existing_type=postgresql.UUID(),
                    nullable=True)


def downgrade():
    """
    Re-add foreign key constraint and make flow_id non-nullable.
    Warning: This will fail if there are flow_executions with NULL flow_id
    or flow_id values that don't exist in the flows table.
    """
    # Make flow_id non-nullable
    op.alter_column('flow_executions', 'flow_id',
                    existing_type=postgresql.UUID(),
                    nullable=False)

    # Re-add the foreign key constraint
    op.create_foreign_key('flow_executions_flow_id_fkey',
                         'flow_executions', 'flows',
                         ['flow_id'], ['id'])
