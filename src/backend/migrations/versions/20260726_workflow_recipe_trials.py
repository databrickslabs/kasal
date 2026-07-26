"""add workflow_recipe_trials table for measuring workflow-recipe reuse

Revision ID: 20260726_recipe_trials
Revises: 20260726_prompt_opt_runs
Create Date: 2026-07-26

Workflow recipes inject few-shot exemplars into the crew-generation prompt.
That effect is invisible in every existing table — the generated crew looks
like any other — so there was no way to answer whether reuse helps. This
table records, per generation: what retrieval found, whether exemplars were
injected or deliberately withheld (the holdout control arm), and the ids of
the agents produced, which later link the generation to the run it became.

Existing deployed DBs are healed at startup by
_ensure_workflow_recipe_trials_table (src/db/session.py) with the same table;
this migration keeps the Alembic chain in sync. The table-exists guard below
and `checkfirst` there make both paths idempotent.
"""

from alembic import op
import sqlalchemy as sa

revision = "20260726_recipe_trials"
down_revision = "20260726_prompt_opt_runs"
branch_labels = None
depends_on = None

_TABLE = "workflow_recipe_trials"


def upgrade() -> None:
    # Offline (--sql) generation cannot introspect; emit the CREATE and let the
    # startup self-heal's checkfirst handle a pre-existing table.
    if not op.get_context().as_sql:
        inspector = sa.inspect(op.get_bind())
        if _TABLE in inspector.get_table_names():
            return  # startup self-heal already created it

    op.create_table(
        _TABLE,
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("group_id", sa.String(length=100), nullable=True),
        sa.Column("group_email", sa.String(length=255), nullable=True),
        sa.Column("prompt_hash", sa.String(length=64), nullable=False),
        sa.Column("prompt_text", sa.Text(), nullable=True),
        sa.Column("candidates", sa.JSON(), nullable=False),
        sa.Column("candidate_count", sa.Integer(), nullable=False),
        sa.Column("blessed_count", sa.Integer(), nullable=False),
        sa.Column("best_similarity", sa.Float(), nullable=True),
        # 'exemplar' | 'control' | 'none_available' — the population split the
        # whole report rests on.
        sa.Column("arm", sa.String(length=16), nullable=False),
        sa.Column("injected_recipe_ids", sa.JSON(), nullable=False),
        sa.Column("agent_ids", sa.JSON(), nullable=False),
        sa.Column("task_ids", sa.JSON(), nullable=False),
        sa.Column("agent_count", sa.Integer(), nullable=True),
        sa.Column("task_count", sa.Integer(), nullable=True),
        sa.Column("linked_job_id", sa.String(length=255), nullable=True),
        sa.Column("linked_at", sa.DateTime(), nullable=True),
        sa.Column("outcome_status", sa.String(length=32), nullable=True),
        sa.Column("outcome_duration_ms", sa.Integer(), nullable=True),
        sa.Column("outcome_error_spans", sa.Integer(), nullable=True),
        sa.Column("outcome_tool_calls", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_workflow_recipe_trials_id", _TABLE, ["id"])
    op.create_index("ix_workflow_recipe_trials_group_id", _TABLE, ["group_id"])
    op.create_index("ix_workflow_recipe_trials_prompt_hash", _TABLE, ["prompt_hash"])
    op.create_index("ix_workflow_recipe_trials_arm", _TABLE, ["arm"])
    op.create_index("ix_workflow_recipe_trials_linked_job_id", _TABLE, ["linked_job_id"])
    op.create_index("idx_recipe_trials_group_arm", _TABLE, ["group_id", "arm"])
    op.create_index("idx_recipe_trials_unlinked", _TABLE, ["linked_job_id", "created_at"])


def downgrade() -> None:
    if not op.get_context().as_sql:
        inspector = sa.inspect(op.get_bind())
        if _TABLE not in inspector.get_table_names():
            return
    op.drop_index("idx_recipe_trials_unlinked", table_name=_TABLE)
    op.drop_index("idx_recipe_trials_group_arm", table_name=_TABLE)
    op.drop_index("ix_workflow_recipe_trials_linked_job_id", table_name=_TABLE)
    op.drop_index("ix_workflow_recipe_trials_arm", table_name=_TABLE)
    op.drop_index("ix_workflow_recipe_trials_prompt_hash", table_name=_TABLE)
    op.drop_index("ix_workflow_recipe_trials_group_id", table_name=_TABLE)
    op.drop_index("ix_workflow_recipe_trials_id", table_name=_TABLE)
    op.drop_table(_TABLE)
