"""add prompt_optimization_runs table for durable GEPA optimization runs

Revision ID: 20260726_prompt_opt_runs
Revises: 20260725_drop_planner_columns
Create Date: 2026-07-26

Prompt-optimization run state used to live in an in-process dict (capped at
50 entries, wiped by every `--reload`), so a completed proposal — and the
before-image needed to undo an apply — did not survive a backend restart.
This table is the durable record.

Existing deployed DBs are healed at startup by
_ensure_prompt_optimization_runs_table (src/db/session.py) with the same
table; this migration keeps the Alembic chain in sync. The table-exists
guard below and `checkfirst` there make both paths idempotent.

Note on the DSPy tables (dspy_configs, dspy_training_examples,
dspy_optimization_runs, dspy_module_cache): they are orphaned — no ORM model
backs them and the service that used them was deleted. They are NOT dropped
here. Their creating migration (`dspy_001`) cannot be removed because
`3c0aebc2977a` merges it as one of four down_revisions, so the tables keep
being created; dropping them in this revision would contradict a migration
still in the chain and could not be honestly reversed in `downgrade`.
"""

from alembic import op
import sqlalchemy as sa

revision = "20260726_prompt_opt_runs"
down_revision = "20260725_drop_planner_columns"
branch_labels = None
depends_on = None

_TABLE = "prompt_optimization_runs"


def upgrade() -> None:
    # Offline (--sql) generation cannot introspect; emit the CREATE and let the
    # startup self-heal's checkfirst handle a pre-existing table.
    if not op.get_context().as_sql:
        inspector = sa.inspect(op.get_bind())
        if _TABLE in inspector.get_table_names():
            return  # startup self-heal already created it

    op.create_table(
        _TABLE,
        sa.Column("id", sa.String(length=64), nullable=False),
        sa.Column("kind", sa.String(length=16), nullable=False),
        sa.Column("target_name", sa.String(length=255), nullable=False),
        sa.Column("crew_id", sa.String(length=64), nullable=True),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("model", sa.String(length=255), nullable=True),
        sa.Column("judge_model", sa.String(length=255), nullable=True),
        sa.Column("reflection_model", sa.String(length=255), nullable=True),
        sa.Column("budget", sa.Integer(), nullable=True),
        sa.Column("dataset_size", sa.Integer(), nullable=False),
        sa.Column("executions_used", sa.Integer(), nullable=True),
        sa.Column("execution_cap", sa.Integer(), nullable=True),
        sa.Column("candidates_tried", sa.Integer(), nullable=True),
        sa.Column("human_feedback_count", sa.Integer(), nullable=True),
        sa.Column("initial_score", sa.Float(), nullable=True),
        sa.Column("final_score", sa.Float(), nullable=True),
        sa.Column("baseline_template", sa.Text(), nullable=True),
        sa.Column("optimized_template", sa.Text(), nullable=True),
        sa.Column("baseline_fields", sa.JSON(), nullable=True),
        sa.Column("optimized_fields", sa.JSON(), nullable=True),
        # Values every touched field held at APPLY time — what revert restores.
        sa.Column("before_image", sa.JSON(), nullable=True),
        sa.Column("applied", sa.Boolean(), nullable=False),
        sa.Column("applied_at", sa.DateTime(), nullable=True),
        sa.Column("applied_by", sa.String(length=255), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.Column("group_id", sa.String(length=100), nullable=True),
        sa.Column("group_email", sa.String(length=255), nullable=True),
        sa.Column("created_by_email", sa.String(length=255), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_prompt_optimization_runs_group_id", _TABLE, ["group_id"])
    op.create_index("ix_prompt_optimization_runs_crew_id", _TABLE, ["crew_id"])
    op.create_index(
        "idx_prompt_opt_runs_group_created", _TABLE, ["group_id", "created_at"]
    )
    op.create_index("idx_prompt_opt_runs_status", _TABLE, ["status"])


def downgrade() -> None:
    if not op.get_context().as_sql:
        inspector = sa.inspect(op.get_bind())
        if _TABLE not in inspector.get_table_names():
            return
    op.drop_index("idx_prompt_opt_runs_status", table_name=_TABLE)
    op.drop_index("idx_prompt_opt_runs_group_created", table_name=_TABLE)
    op.drop_index("ix_prompt_optimization_runs_crew_id", table_name=_TABLE)
    op.drop_index("ix_prompt_optimization_runs_group_id", table_name=_TABLE)
    op.drop_table(_TABLE)
