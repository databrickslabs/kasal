"""Workspace overrides of the A2UI defaults.

``ui_config.settings_json`` holds a workspace's overrides of the system-wide A2UI
defaults (Configuration → Output design); NULL uses the defaults for every knob.
Runtime installs get it from db/self_heal/columns.py; this records the change
for Alembic users.
"""

import sqlalchemy as sa
from alembic import op

revision = "20260927_ui_config_settings_json"
down_revision = "20260926_mlflow_advanced_settings"
branch_labels = None
depends_on = None


def upgrade():
    existing = {c["name"] for c in sa.inspect(op.get_bind()).get_columns("ui_config")}
    if "settings_json" not in existing:
        with op.batch_alter_table("ui_config") as batch:
            batch.add_column(sa.Column("settings_json", sa.Text(), nullable=True))


def downgrade():
    with op.batch_alter_table("ui_config") as batch:
        batch.drop_column("settings_json")
