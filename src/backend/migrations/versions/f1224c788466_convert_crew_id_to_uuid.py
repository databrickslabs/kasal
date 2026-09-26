"""convert_crew_id_to_uuid

Revision ID: f1224c788466
Revises: be5bfc77b066
Create Date: 2025-04-22 17:16:04.024360

"""
from typing import Sequence, Union
from uuid import uuid4

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = 'f1224c788466'
down_revision: Union[str, None] = 'be5bfc77b066'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Ensure UUID extension is available
    op.execute('CREATE EXTENSION IF NOT EXISTS "uuid-ossp"')
    
    # Create a temporary table with UUID primary keys
    op.create_table(
        'crews_temp',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text('uuid_generate_v4()')),
        sa.Column('name', sa.String(), nullable=True),
        sa.Column('agent_ids', postgresql.JSONB(), nullable=True),
        sa.Column('task_ids', postgresql.JSONB(), nullable=True),
        sa.Column('nodes', postgresql.JSONB(), nullable=True),
        sa.Column('edges', postgresql.JSONB(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
    )
    
    # Create indexes
    op.create_index(op.f('ix_crews_temp_id'), 'crews_temp', ['id'], unique=False)
    op.create_index(op.f('ix_crews_temp_name'), 'crews_temp', ['name'], unique=False)
    
    # Create a mapping table to keep track of old and new IDs
    op.create_table(
        'crew_id_mapping',
        sa.Column('old_id', sa.Integer(), primary_key=True),
        sa.Column('new_id', postgresql.UUID(as_uuid=True), nullable=False),
    )
    
    # Copy data from the original table to the temporary table with new UUIDs
    op.execute(
        """
        INSERT INTO crews_temp (id, name, agent_ids, task_ids, nodes, edges, created_at, updated_at)
        SELECT 
            uuid_generate_v4() as id,
            name,
            agent_ids,
            task_ids,
            nodes,
            edges,
            created_at,
            updated_at
        FROM crews
        """
    )
    
    # Populate the mapping table to link old integer IDs to new UUIDs
    op.execute(
        """
        INSERT INTO crew_id_mapping (old_id, new_id)
        SELECT c.id, ct.id
        FROM crews c
        JOIN crews_temp ct ON c.name = ct.name AND 
                              c.created_at = ct.created_at
        """
    )
    
    # First check and drop any foreign key constraints related to crews
    op.execute(
        """
        DO $$
        DECLARE
            fk_constraint_name text;
        BEGIN
            -- Look for foreign key constraints pointing to crews table
            SELECT conname INTO fk_constraint_name
            FROM pg_constraint c
            JOIN pg_class t ON c.conrelid = t.oid
            JOIN pg_class r ON c.confrelid = r.oid
            WHERE t.relname = 'flows' 
              AND r.relname = 'crews'
              AND c.contype = 'f'
              AND c.conkey @> ARRAY[
                (SELECT attnum FROM pg_attribute 
                 WHERE attrelid = t.oid AND attname = 'crew_id')
              ]::smallint[];
            
            -- Drop constraint if found
            IF fk_constraint_name IS NOT NULL THEN
                EXECUTE 'ALTER TABLE flows DROP CONSTRAINT ' || fk_constraint_name;
                RAISE NOTICE 'Dropped foreign key constraint: %', fk_constraint_name;
            END IF;
        END;
        $$;
        """
    )
    
    # Then handle the crew_id column in flows table
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_name = 'flows'
            ) AND EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = 'flows' AND column_name = 'crew_id'
            ) THEN
                -- First alter column type to text to avoid type mismatch
                ALTER TABLE flows 
                ALTER COLUMN crew_id TYPE text USING crew_id::text;
                
                -- Create a temporary column for the UUID values
                ALTER TABLE flows 
                ADD COLUMN crew_id_new uuid;
                
                -- Update with the new UUIDs
                UPDATE flows f
                SET crew_id_new = m.new_id
                FROM crew_id_mapping m
                WHERE f.crew_id = m.old_id::text;
                
                -- Drop the old column and rename the new one
                ALTER TABLE flows DROP COLUMN crew_id;
                ALTER TABLE flows RENAME COLUMN crew_id_new TO crew_id;
            END IF;
        END
        $$;
        """
    )
    
    # Rename crews tables to swap them
    op.execute("ALTER TABLE crews RENAME TO crews_old")
    op.execute("ALTER TABLE crews_temp RENAME TO crews")
    
    # Add back foreign key constraint if needed
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_name = 'flows'
            ) AND EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = 'flows' AND column_name = 'crew_id'
            ) THEN
                -- Add foreign key constraint pointing to new UUID column
                ALTER TABLE flows 
                ADD CONSTRAINT flows_crew_id_fkey 
                FOREIGN KEY (crew_id) REFERENCES crews (id) ON DELETE CASCADE;
            END IF;
        END
        $$;
        """
    )
    
    # Drop the old table
    op.drop_table('crews_old')


def downgrade() -> None:
    # First drop any foreign key constraints to the crews table
    op.execute(
        """
        DO $$
        DECLARE
            fk_constraint_name text;
        BEGIN
            -- Look for foreign key constraints pointing to crews table
            SELECT conname INTO fk_constraint_name
            FROM pg_constraint c
            JOIN pg_class t ON c.conrelid = t.oid
            JOIN pg_class r ON c.confrelid = r.oid
            WHERE t.relname = 'flows' 
              AND r.relname = 'crews'
              AND c.contype = 'f'
              AND c.conkey @> ARRAY[
                (SELECT attnum FROM pg_attribute 
                 WHERE attrelid = t.oid AND attname = 'crew_id')
              ]::smallint[];
            
            -- Drop constraint if found
            IF fk_constraint_name IS NOT NULL THEN
                EXECUTE 'ALTER TABLE flows DROP CONSTRAINT ' || fk_constraint_name;
                RAISE NOTICE 'Dropped foreign key constraint: %', fk_constraint_name;
            END IF;
        END;
        $$;
        """
    )
    
    # Create a temporary integer ID table
    op.create_table(
        'crews_temp',
        sa.Column('id', sa.Integer(), autoincrement=True, primary_key=True),
        sa.Column('name', sa.String(), nullable=True),
        sa.Column('agent_ids', postgresql.JSONB(), nullable=True),
        sa.Column('task_ids', postgresql.JSONB(), nullable=True),
        sa.Column('nodes', postgresql.JSONB(), nullable=True),
        sa.Column('edges', postgresql.JSONB(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
    )
    
    # Create indexes
    op.create_index(op.f('ix_crews_temp_id'), 'crews_temp', ['id'], unique=False)
    op.create_index(op.f('ix_crews_temp_name'), 'crews_temp', ['name'], unique=False)
    
    # Check if the mapping table exists
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_name = 'crew_id_mapping'
            ) THEN
                -- Copy data back from UUID table to integer ID table using the mapping
                INSERT INTO crews_temp (id, name, agent_ids, task_ids, nodes, edges, created_at, updated_at)
                SELECT 
                    m.old_id as id,
                    c.name,
                    c.agent_ids,
                    c.task_ids,
                    c.nodes,
                    c.edges,
                    c.created_at,
                    c.updated_at
                FROM crews c
                JOIN crew_id_mapping m ON c.id = m.new_id;
                
                -- Check if the flows table exists and has a crew_id foreign key
                IF EXISTS (
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_name = 'flows'
                ) AND EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_name = 'flows' AND column_name = 'crew_id'
                ) THEN
                    -- Convert uuid to text for the comparison
                    ALTER TABLE flows 
                    ALTER COLUMN crew_id TYPE text USING crew_id::text;
                    
                    -- Add temporary column
                    ALTER TABLE flows 
                    ADD COLUMN crew_id_new integer;
                    
                    -- Update foreign keys in flows table back to integer IDs
                    UPDATE flows f
                    SET crew_id_new = m.old_id
                    FROM crew_id_mapping m
                    WHERE f.crew_id = m.new_id::text;
                    
                    -- Drop old column and rename new one
                    ALTER TABLE flows DROP COLUMN crew_id;
                    ALTER TABLE flows RENAME COLUMN crew_id_new TO crew_id;
                END IF;
            ELSE
                -- If mapping table doesn't exist, just copy the data with new auto-incrementing IDs
                INSERT INTO crews_temp (name, agent_ids, task_ids, nodes, edges, created_at, updated_at)
                SELECT 
                    name,
                    agent_ids,
                    task_ids,
                    nodes,
                    edges,
                    created_at,
                    updated_at
                FROM crews;
            END IF;
        END
        $$;
        """
    )
    
    # Rename tables to swap them
    op.execute("ALTER TABLE crews RENAME TO crews_old")
    op.execute("ALTER TABLE crews_temp RENAME TO crews")
    
    # Add back foreign key constraint if needed
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_name = 'flows'
            ) AND EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = 'flows' AND column_name = 'crew_id'
            ) THEN
                -- Add foreign key constraint pointing to integer crews.id
                ALTER TABLE flows 
                ADD CONSTRAINT flows_crew_id_fkey 
                FOREIGN KEY (crew_id) REFERENCES crews (id) ON DELETE CASCADE;
            END IF;
        END
        $$;
        """
    )
    
    # Drop the old table and mapping table
    op.drop_table('crews_old')
    op.execute("DROP TABLE IF EXISTS crew_id_mapping") 