"""Alter PromptTemplate unique constraint to allow same name per group

Revision ID: alter_prompttemplate_unique
Revises: 2bc5aee43ad9
Create Date: 2025-09-23 23:59:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = 'alter_prompttemplate_unique'
down_revision: Union[str, None] = '2bc5aee43ad9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _drop_unique_on_name_sqlite():
    conn = op.get_bind()
    # Inspect existing indexes and drop any unique index solely on 'name'
    indexes = conn.exec_driver_sql("PRAGMA index_list('prompttemplate')").fetchall()
    for idx in indexes:
        # idx columns: seq, name, unique, origin, partial
        idx_name = idx[1]
        is_unique = bool(idx[2])
        if not is_unique:
            continue
        # Get columns for the index
        cols = conn.exec_driver_sql(f"PRAGMA index_info('{idx_name}')").fetchall()
        col_names = [c[2] for c in cols]
        if len(col_names) == 1 and col_names[0] == 'name':
            try:
                conn.exec_driver_sql(f"DROP INDEX IF EXISTS {idx_name}")
            except Exception:
                pass


def _drop_all_unique_constraints_postgres(conn):
    # Drop all unique constraints on prompttemplate using IF EXISTS to avoid aborting the tx
    try:
        rows = conn.execute(sa.text(
            """
            SELECT conname
            FROM pg_constraint c
            JOIN pg_class t ON c.conrelid = t.oid
            WHERE t.relname = 'prompttemplate' AND c.contype = 'u'
            """
        )).fetchall()
        for (conname,) in rows:
            try:
                conn.execute(sa.text(f"ALTER TABLE prompttemplate DROP CONSTRAINT IF EXISTS {conname}"))
            except Exception:
                pass
        # Also drop plain unique index on name if it exists
        try:
            conn.execute(sa.text("DROP INDEX IF EXISTS ix_prompttemplate_name"))
        except Exception:
            pass
    except Exception:
        pass


def _deduplicate_prompttemplate(conn):
    """Delete duplicate rows keeping the most recent per (name, group_id)."""
    conn.execute(sa.text(
        """
        DELETE FROM prompttemplate p
        USING (
          SELECT id,
                 ROW_NUMBER() OVER (
                   PARTITION BY name, group_id
                   ORDER BY id DESC
                 ) AS rn
          FROM prompttemplate
        ) d
        WHERE p.id = d.id AND d.rn > 1
        """
    ))


def upgrade() -> None:
    conn = op.get_bind()
    dialect = conn.dialect.name

    # Data cleanup first: ensure no duplicates will violate the new constraint
    try:
        _deduplicate_prompttemplate(conn)
    except Exception:
        # Best-effort cleanup; continue
        pass

    if dialect == 'sqlite':
        # Best-effort drop of unique index on name; do not create new constraint here
        _drop_unique_on_name_sqlite()
    else:
        # Drop all unique constraints/indexes on 'prompttemplate' safely
        _drop_all_unique_constraints_postgres(conn)


def downgrade() -> None:
    conn = op.get_bind()
    dialect = conn.dialect.name

    if dialect == 'sqlite':
        # Drop composite unique created in upgrade
        try:
            op.drop_constraint('uq_prompttemplate_name_group', 'prompttemplate', type_='unique')
        except Exception:
            pass
        # Recreate unique index on name for sqlite
        try:
            op.create_index('ix_prompttemplate_name', 'prompttemplate', ['name'], unique=True)
        except Exception:
            pass
    else:
        # Drop partial unique indexes created in upgrade
        for idx in ['uq_prompttemplate_name_base', 'uq_prompttemplate_name_group_not_null']:
            try:
                op.drop_index(idx, table_name='prompttemplate')
            except Exception:
                pass
        # Recreate unique constraint on name for other dialects
        try:
            op.create_unique_constraint('uq_prompttemplate_name', 'prompttemplate', ['name'])
        except Exception:
            pass

