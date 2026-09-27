"""Drop the ``decision_config.group_id`` foreign key to ``groups.id``.

Personal workspaces (``user_<email>``) have no ``groups`` row, so the key made
the decision-model switch impossible to save there: every insert failed with an
IntegrityError. The column stays a String(100) primary key — the same plain
workspace id ``apikey.group_id`` uses.

``recreate="always"`` with an explicit ``copy_from`` rebuilds the table from a
definition without the key: SQLite cannot drop a constraint in place (and its
reflected foreign keys are unnamed), and on PostgreSQL / Lakebase the rebuild
drops the constraint with it. Runtime installs get the same change from
``db/self_heal/tables.py``; this records it for Alembic users.
"""

import sqlalchemy as sa
from alembic import op

revision = "20260927_decision_config_drop_group_fk"
down_revision = "20260927_mlflow_local_tracking_uri"
branch_labels = None
depends_on = None

TABLE = "decision_config"


def _table(with_group_fk: bool) -> sa.Table:
    group_id_args: list = [sa.String(100)]
    if with_group_fk:
        group_id_args.append(sa.ForeignKey("groups.id", ondelete="CASCADE"))
    metadata = sa.MetaData()
    if with_group_fk:
        # Only so the ForeignKey above can resolve its target while copying.
        sa.Table("groups", metadata, sa.Column("id", sa.String(100)))
    return sa.Table(
        TABLE,
        metadata,
        sa.Column("group_id", *group_id_args, primary_key=True),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.false()),
    )


def _has_group_fk(inspector: sa.Inspector) -> bool:
    return any(
        fk.get("referred_table") == "groups" for fk in inspector.get_foreign_keys(TABLE)
    )


def upgrade():
    inspector = sa.inspect(op.get_bind())
    if TABLE not in inspector.get_table_names() or not _has_group_fk(inspector):
        return
    with op.batch_alter_table(
        TABLE, recreate="always", copy_from=_table(with_group_fk=False)
    ):
        pass


def downgrade():
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if TABLE not in inspector.get_table_names() or _has_group_fk(inspector):
        return
    # The restored key cannot hold a row for a workspace without a ``groups``
    # row (a personal workspace); those opt-ins are lost on downgrade.
    op.execute(
        sa.text(f"DELETE FROM {TABLE} WHERE group_id NOT IN (SELECT id FROM groups)")
    )
    with op.batch_alter_table(
        TABLE, recreate="always", copy_from=_table(with_group_fk=True)
    ):
        pass
