"""MLflow Advanced settings that used to be env vars.

``mlflowconfig.evaluation_max_rows`` (was MLFLOW_EVAL_MAX_ROWS) and
``mlflowconfig.optimization_judge_samples`` (was GEPA_JUDGE_SAMPLES). Nullable:
NULL keeps the built-in default. Runtime installs get them from
db/self_heal/columns.py; this records the change for Alembic users.
"""

import sqlalchemy as sa
from alembic import op

revision = "20260926_mlflow_advanced_settings"
down_revision = "20260923_decision_config"
branch_labels = None
depends_on = None


def upgrade():
    existing = {
        c["name"] for c in sa.inspect(op.get_bind()).get_columns("mlflowconfig")
    }
    with op.batch_alter_table("mlflowconfig") as batch:
        if "evaluation_max_rows" not in existing:
            batch.add_column(
                sa.Column("evaluation_max_rows", sa.Integer(), nullable=True)
            )
        if "optimization_judge_samples" not in existing:
            batch.add_column(
                sa.Column("optimization_judge_samples", sa.Integer(), nullable=True)
            )


def downgrade():
    with op.batch_alter_table("mlflowconfig") as batch:
        batch.drop_column("optimization_judge_samples")
        batch.drop_column("evaluation_max_rows")
