"""mlflowconfig — MLflow settings get their own table, out of the Databricks one

Revision ID: 20260730_mlflow_config
Revises: 20260729_agent_skills
Create Date: 2026-07-30

``mlflow_enabled`` / ``mlflow_experiment_name`` / ``evaluation_enabled`` /
``evaluation_judge_model`` lived on ``databricksconfig``. That was coherent while
MLflow *was* Databricks, and became a bug once tracing could also target a local
OSS server: ``MLflowRepository.is_enabled`` read the flag off the Databricks row
and returned False whenever no such row existed, so a workspace with no
Databricks configuration could never turn MLflow on at all.

COPIES, it does not move. The old columns stay exactly where they are, so this
is a two-way door: downgrade drops only the new table and no setting is lost, and
the dead columns can be removed in a later release once this one has proven
itself. The alternative — dropping four columns in the same migration that
introduces their replacement — makes a rollback a data-loss event.

The experiment name is stripped of a leading "/Shared/" on the way across. It is
a Databricks WORKSPACE PATH, not part of the name; the Databricks backend adds
the prefix back and the local backend must not carry it (see
services/mlflow/local.local_experiment_name).
"""

import sqlalchemy as sa
from alembic import op

revision = "20260730_mlflow_config"
down_revision = "20260729_agent_skills"
branch_labels = None
depends_on = None


def upgrade() -> None:
    if not op.get_context().as_sql:
        inspector = sa.inspect(op.get_bind())
        if "mlflowconfig" in inspector.get_table_names():
            return

    op.create_table(
        "mlflowconfig",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.text("0")),
        sa.Column("experiment_name", sa.String(), nullable=True),
        sa.Column(
            "evaluation_enabled",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column("evaluation_judge_model", sa.String(), nullable=True),
        sa.Column("group_id", sa.String(length=100), nullable=True),
        sa.Column("created_by_email", sa.String(length=255), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_mlflowconfig_group_id"), "mlflowconfig", ["group_id"], unique=False
    )
    op.create_index(
        op.f("ix_mlflowconfig_created_by_email"),
        "mlflowconfig",
        ["created_by_email"],
        unique=False,
    )

    if op.get_context().as_sql:
        # Offline mode cannot read the source rows; the table is created and the
        # copy is skipped rather than emitting SQL against unknown data.
        return

    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if "databricksconfig" not in inspector.get_table_names():
        return
    existing = {c["name"] for c in inspector.get_columns("databricksconfig")}
    if "mlflow_enabled" not in existing:
        return

    # Carry every workspace's current settings across, so enabling MLflow does
    # not silently reset for anyone already using it.
    rows = bind.execute(
        sa.text(
            "SELECT mlflow_enabled, mlflow_experiment_name, evaluation_enabled, "
            "evaluation_judge_model, group_id, created_by_email "
            "FROM databricksconfig"
        )
    ).fetchall()

    for row in rows:
        name = (row[1] or "kasal-crew-execution-traces").strip()
        if name.startswith("/Shared/"):
            name = name[len("/Shared/") :]
        bind.execute(
            sa.text(
                "INSERT INTO mlflowconfig "
                "(enabled, experiment_name, evaluation_enabled, "
                " evaluation_judge_model, group_id, created_by_email) "
                "VALUES (:enabled, :name, :evaluation, :judge, :group_id, :email)"
            ),
            {
                "enabled": bool(row[0]),
                "name": name.strip("/") or "kasal-crew-execution-traces",
                "evaluation": bool(row[2]),
                "judge": row[3],
                "group_id": row[4],
                "email": row[5],
            },
        )


def downgrade() -> None:
    # The source columns were never touched, so dropping this table restores the
    # previous behaviour exactly.
    op.drop_index(op.f("ix_mlflowconfig_created_by_email"), table_name="mlflowconfig")
    op.drop_index(op.f("ix_mlflowconfig_group_id"), table_name="mlflowconfig")
    op.drop_table("mlflowconfig")
