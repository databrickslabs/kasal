"""Update flow_executions to use UUID for flow_id

Revision ID: be5bfc77b066
Revises: 09ed462c60e7
Create Date: 2025-04-21 21:31:30.156179

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.engine.reflection import Inspector


# revision identifiers, used by Alembic.
revision: str = 'be5bfc77b066'
down_revision: Union[str, None] = '09ed462c60e7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create a connection to get the Inspector
    conn = op.get_bind()
    inspector = Inspector.from_engine(conn)
    
    # ### Create a temporary column with UUID type
    op.add_column('flow_executions', sa.Column('flow_id_uuid', UUID(as_uuid=True), nullable=True))
    
    # ### Update the temporary column with values converted from the original column
    # ### This will involve a database lookup to convert integer IDs to their UUID equivalents
    op.execute("""
        UPDATE flow_executions fe
        SET flow_id_uuid = f.id
        FROM flows f
        WHERE fe.flow_id = f.id::text::integer
    """)
    
    # ### Handle dynamic flows (flow_id = 0)
    op.execute("""
        UPDATE flow_executions
        SET flow_id_uuid = '00000000-0000-0000-0000-000000000000'
        WHERE flow_id = 0
    """)
    
    # Drop any foreign key constraints on flow_id column
    foreign_keys = inspector.get_foreign_keys('flow_executions')
    for fk in foreign_keys:
        if 'flow_id' in fk['constrained_columns']:
            op.drop_constraint(fk['name'], 'flow_executions', type_='foreignkey')
    
    # ### Drop the old column
    op.drop_column('flow_executions', 'flow_id')
    
    # ### Rename the new column to the original name
    op.alter_column('flow_executions', 'flow_id_uuid', new_column_name='flow_id')
    
    # ### Create the foreign key constraint with the new column type
    op.create_foreign_key(
        'flow_executions_flow_id_fkey', 
        'flow_executions', 'flows', 
        ['flow_id'], ['id']
    )
    
    # ### Make the column not nullable
    op.alter_column('flow_executions', 'flow_id', nullable=False)


def downgrade() -> None:
    # Create a connection to get the Inspector
    conn = op.get_bind()
    inspector = Inspector.from_engine(conn)
    
    # ### Create a temporary column with Integer type
    op.add_column('flow_executions', sa.Column('flow_id_int', sa.Integer(), nullable=True))
    
    # ### This is a lossy conversion as we cannot reliably map UUIDs back to integers
    # ### We'll set to 0 (dynamic flow) if we can't determine the original ID
    op.execute("""
        UPDATE flow_executions
        SET flow_id_int = 0
    """)
    
    # Drop any foreign key constraints on flow_id column
    foreign_keys = inspector.get_foreign_keys('flow_executions')
    for fk in foreign_keys:
        if 'flow_id' in fk['constrained_columns']:
            op.drop_constraint(fk['name'], 'flow_executions', type_='foreignkey')
    
    # ### Drop the UUID column
    op.drop_column('flow_executions', 'flow_id')
    
    # ### Rename the new column to the original name
    op.alter_column('flow_executions', 'flow_id_int', new_column_name='flow_id')
    
    # ### Make the column not nullable
    op.alter_column('flow_executions', 'flow_id', nullable=False)
    
    # ### Note: We don't recreate the foreign key since the flows table has UUIDs now 