"""The local MLflow server a workspace traces to, set in Configuration → MLflow.

``mlflowconfig.local_tracking_uri`` replaces launching the backend with
MCP_SERVER_ENABLED=true and MLFLOW_TRACKING_URI=<server>. NULL means no local
server. Runtime installs get it from db/self_heal/columns.py; this records the
change for Alembic users.
"""

import sqlalchemy as sa
from alembic import op

revision = "20260927_mlflow_local_tracking_uri"
down_revision = "20260927_ui_config_settings_json"
branch_labels = None
depends_on = None


def upgrade():
    existing = {
        c["name"] for c in sa.inspect(op.get_bind()).get_columns("mlflowconfig")
    }
    if "local_tracking_uri" not in existing:
        with op.batch_alter_table("mlflowconfig") as batch:
            batch.add_column(
                sa.Column("local_tracking_uri", sa.String(), nullable=True)
            )


def downgrade():
    with op.batch_alter_table("mlflowconfig") as batch:
        batch.drop_column("local_tracking_uri")
