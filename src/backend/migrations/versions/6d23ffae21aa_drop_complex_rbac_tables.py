"""drop_complex_rbac_tables

Revision ID: 6d23ffae21aa
Revises: 1041071e3641
Create Date: 2025-09-20 17:14:27.423636

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '6d23ffae21aa'
down_revision: Union[str, None] = '1041071e3641'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Drop complex RBAC tables - using simplified group-based roles instead
    op.drop_table('user_roles')
    op.drop_table('role_privileges')
    op.drop_table('external_identities')
    op.drop_table('identity_providers')
    op.drop_table('privileges')
    op.drop_table('roles')


def downgrade() -> None:
    # Recreate tables in reverse order (if rollback needed)
    # Note: This is for emergency rollback only - simplified system is preferred

    # Create roles table
    op.create_table('roles',
        sa.Column('id', sa.String(), nullable=False),
        sa.Column('name', sa.String(), nullable=False),
        sa.Column('description', sa.String(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('name')
    )

    # Create privileges table
    op.create_table('privileges',
        sa.Column('id', sa.String(), nullable=False),
        sa.Column('name', sa.String(), nullable=False),
        sa.Column('description', sa.String(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('name')
    )

    # Create role_privileges junction table
    op.create_table('role_privileges',
        sa.Column('id', sa.String(), nullable=False),
        sa.Column('role_id', sa.String(), nullable=True),
        sa.Column('privilege_id', sa.String(), nullable=True),
        sa.ForeignKeyConstraint(['privilege_id'], ['privileges.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['role_id'], ['roles.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('role_id', 'privilege_id', name='uq_role_privilege')
    )

    # Create user_roles junction table
    op.create_table('user_roles',
        sa.Column('id', sa.String(), nullable=False),
        sa.Column('user_id', sa.String(), nullable=True),
        sa.Column('role_id', sa.String(), nullable=True),
        sa.Column('assigned_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('assigned_by', sa.String(), nullable=True),
        sa.ForeignKeyConstraint(['role_id'], ['roles.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('user_id', 'role_id', name='uq_user_role')
    )

    # Create external_identities table
    op.create_table('external_identities',
        sa.Column('id', sa.String(), nullable=False),
        sa.Column('user_id', sa.String(), nullable=True),
        sa.Column('provider', sa.String(), nullable=False),
        sa.Column('provider_user_id', sa.String(), nullable=False),
        sa.Column('email', sa.String(), nullable=True),
        sa.Column('profile_data', sa.String(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('last_login', sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('provider', 'provider_user_id', name='uq_external_identity_provider_user')
    )

    # Create identity_providers table
    op.create_table('identity_providers',
        sa.Column('id', sa.String(), nullable=False),
        sa.Column('name', sa.String(), nullable=False),
        sa.Column('type', sa.Enum('oauth2', 'saml', 'ldap', name='identity_provider_type_enum'), nullable=False),
        sa.Column('config', sa.String(), nullable=False),
        sa.Column('enabled', sa.Boolean(), nullable=True),
        sa.Column('is_default', sa.Boolean(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint('id')
    ) 