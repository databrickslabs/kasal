"""change_flow_id_to_uuid

Revision ID: 09ed462c60e7
Revises: 456ff78ce120
Create Date: 2025-04-21 20:39:39.481063

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID
import uuid


# revision identifiers, used by Alembic.
revision: str = '09ed462c60e7'
down_revision: Union[str, None] = '456ff78ce120'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # First create a new UUID column
    op.add_column('flows', sa.Column('uuid_id', UUID(as_uuid=True), nullable=True))
    
    # Generate UUIDs for existing rows
    connection = op.get_bind()
    connection.execute(sa.text(
        """
        UPDATE flows
        SET uuid_id = gen_random_uuid()
        WHERE uuid_id IS NULL
        """
    ))
    
    # Create new UUID primary key
    op.execute('ALTER TABLE flows DROP CONSTRAINT flows_pkey CASCADE')
    connection.execute(sa.text('ALTER TABLE flows ADD PRIMARY KEY (uuid_id)'))
    
    # Drop the old integer id column
    op.drop_column('flows', 'id')
    
    # Rename uuid_id to id
    op.alter_column('flows', 'uuid_id', new_column_name='id')
    
    # We also need to update foreign key constraints in any tables that reference flows
    # This would be done here if there are any tables with foreign keys to flows
    

def downgrade() -> None:
    # To downgrade, we need to reverse the process
    # Add integer id column
    op.add_column('flows', sa.Column('int_id', sa.Integer(), autoincrement=True, nullable=True))
    
    # Generate sequential IDs
    connection = op.get_bind()
    connection.execute(sa.text(
        """
        UPDATE flows
        SET int_id = nextval('flows_id_seq')
        WHERE int_id IS NULL
        """
    ))
    
    # Drop primary key constraint
    op.execute('ALTER TABLE flows DROP CONSTRAINT flows_pkey CASCADE')
    
    # Make int_id the primary key
    connection.execute(sa.text('ALTER TABLE flows ADD PRIMARY KEY (int_id)'))
    
    # Drop the UUID id column
    op.drop_column('flows', 'id')
    
    # Rename int_id to id
    op.alter_column('flows', 'int_id', new_column_name='id') 