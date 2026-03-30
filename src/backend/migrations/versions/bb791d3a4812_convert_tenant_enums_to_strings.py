"""convert_tenant_enums_to_strings

Revision ID: bb791d3a4812
Revises: 96bc3fb62ba7
Create Date: 2025-06-06 16:40:28.615497

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'bb791d3a4812'
down_revision: Union[str, None] = '96bc3fb62ba7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Convert enum columns to string columns for tenant tables."""
    
    # Convert tenants.status from enum to string
    with op.batch_alter_table('tenants', schema=None) as batch_op:
        batch_op.alter_column('status',
                              existing_type=sa.Enum('ACTIVE', 'SUSPENDED', 'ARCHIVED', name='tenantstatus'),
                              type_=sa.String(length=50),
                              existing_nullable=False,
                              existing_server_default='ACTIVE')
    
    # Convert tenant_users.role from enum to string
    with op.batch_alter_table('tenant_users', schema=None) as batch_op:
        batch_op.alter_column('role',
                              existing_type=sa.Enum('ADMIN', 'MANAGER', 'USER', 'VIEWER', name='tenantuserrole'),
                              type_=sa.String(length=50),
                              existing_nullable=False,
                              existing_server_default='USER')
    
    # Convert tenant_users.status from enum to string
    with op.batch_alter_table('tenant_users', schema=None) as batch_op:
        batch_op.alter_column('status',
                              existing_type=sa.Enum('ACTIVE', 'INACTIVE', 'SUSPENDED', name='tenantuserstatus'),
                              type_=sa.String(length=50),
                              existing_nullable=False,
                              existing_server_default='ACTIVE')
    
    # Drop the enum types after converting columns
    op.execute('DROP TYPE IF EXISTS tenantstatus CASCADE')
    op.execute('DROP TYPE IF EXISTS tenantuserrole CASCADE')  
    op.execute('DROP TYPE IF EXISTS tenantuserstatus CASCADE')


def downgrade() -> None:
    """Convert string columns back to enum columns for tenant tables."""
    
    # Recreate enum types
    tenantstatus_enum = sa.Enum('ACTIVE', 'SUSPENDED', 'ARCHIVED', name='tenantstatus')
    tenantstatus_enum.create(op.get_bind())
    
    tenantuserrole_enum = sa.Enum('ADMIN', 'MANAGER', 'USER', 'VIEWER', name='tenantuserrole')
    tenantuserrole_enum.create(op.get_bind())
    
    tenantuserstatus_enum = sa.Enum('ACTIVE', 'INACTIVE', 'SUSPENDED', name='tenantuserstatus')
    tenantuserstatus_enum.create(op.get_bind())
    
    # Convert tenants.status from string to enum
    with op.batch_alter_table('tenants', schema=None) as batch_op:
        batch_op.alter_column('status',
                              existing_type=sa.String(length=50),
                              type_=tenantstatus_enum,
                              existing_nullable=False,
                              existing_server_default='ACTIVE',
                              postgresql_using='status::tenantstatus')
    
    # Convert tenant_users.role from string to enum
    with op.batch_alter_table('tenant_users', schema=None) as batch_op:
        batch_op.alter_column('role',
                              existing_type=sa.String(length=50),
                              type_=tenantuserrole_enum,
                              existing_nullable=False,
                              existing_server_default='USER',
                              postgresql_using='role::tenantuserrole')
    
    # Convert tenant_users.status from string to enum
    with op.batch_alter_table('tenant_users', schema=None) as batch_op:
        batch_op.alter_column('status',
                              existing_type=sa.String(length=50),
                              type_=tenantuserstatus_enum,
                              existing_nullable=False,
                              existing_server_default='ACTIVE',
                              postgresql_using='status::tenantuserstatus')