"""The timezone a schedule's cron line is read in, and the saved crew it runs.

``schedule.timezone`` holds an IANA name such as "Europe/Berlin"; NULL keeps the
server's clock. ``schedule.crew_id`` names the saved crew each run is rebuilt
from; NULL keeps the stored copy. Both NULL is how every existing schedule keeps
behaving. Runtime installs get the columns from db/self_heal/columns.py; this
records the change for Alembic users.
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID

revision = "20261017_schedule_timezone_and_crew"
down_revision = "20260927_decision_config_drop_group_fk"
branch_labels = None
depends_on = None


def upgrade():
    existing = {c["name"] for c in sa.inspect(op.get_bind()).get_columns("schedule")}
    with op.batch_alter_table("schedule") as batch:
        if "crew_id" not in existing:
            batch.add_column(sa.Column("crew_id", UUID(as_uuid=True), nullable=True))
        if "timezone" not in existing:
            batch.add_column(sa.Column("timezone", sa.String(64), nullable=True))


def downgrade():
    with op.batch_alter_table("schedule") as batch:
        batch.drop_column("timezone")
        batch.drop_column("crew_id")
