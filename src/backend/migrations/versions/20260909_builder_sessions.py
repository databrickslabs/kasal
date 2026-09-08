"""Store builder canvases alongside their shared conversation sessions."""

from alembic import op
import sqlalchemy as sa

revision = "20260909_builder_sessions"
down_revision = "20260908_model_billing"
branch_labels = None
depends_on = None


def upgrade():
    existing = {
        column["name"]
        for column in sa.inspect(op.get_bind()).get_columns("chat_sessions")
    }
    for column in (
        sa.Column("mode", sa.String(16), nullable=False, server_default="chat"),
        sa.Column("canvas_state", sa.Text(), nullable=True),
        sa.Column("canvas_revision", sa.Integer(), nullable=False, server_default="0"),
    ):
        if column.name not in existing:
            op.add_column("chat_sessions", column)


def downgrade():
    with op.batch_alter_table("chat_sessions") as batch:
        for column in ("canvas_revision", "canvas_state", "mode"):
            batch.drop_column(column)
