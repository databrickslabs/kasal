"""Remove unique constraint from tenant email_domain

Revision ID: remove_domain_unique
Revises: ecccfbff3c8d
Create Date: 2025-01-07 14:19:38.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'remove_domain_unique'
down_revision = 'ecccfbff3c8d'
branch_labels = None
depends_on = None


def upgrade():
    """Remove unique constraint from tenants.email_domain to allow multiple groups per domain"""
    # Drop the unique constraint on email_domain
    # First, let's check if there's an index we need to drop
    op.execute("DROP INDEX IF EXISTS ix_tenants_email_domain")
    
    # Drop any unique constraints that might exist
    op.execute("""
        DO $$ 
        DECLARE 
            constraint_name text;
        BEGIN 
            -- Find and drop unique constraints on email_domain
            FOR constraint_name IN 
                SELECT conname 
                FROM pg_constraint c
                JOIN pg_class t ON c.conrelid = t.oid
                WHERE t.relname = 'tenants' 
                AND c.contype = 'u'
                AND EXISTS (
                    SELECT 1 FROM pg_attribute a 
                    WHERE a.attrelid = t.oid 
                    AND a.attnum = ANY(c.conkey) 
                    AND a.attname = 'email_domain'
                )
            LOOP
                EXECUTE 'ALTER TABLE tenants DROP CONSTRAINT ' || constraint_name;
            END LOOP;
        END $$;
    """)


def downgrade():
    """Add back the unique constraint on tenants.email_domain"""
    # Re-add the unique constraint
    op.create_unique_constraint('uq_tenants_email_domain', 'tenants', ['email_domain'])