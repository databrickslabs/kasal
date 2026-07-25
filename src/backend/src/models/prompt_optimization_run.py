"""
Durable record of a GEPA prompt-optimization run.

Replaces the in-process `_RUNS` dict the service used to keep runs in
(capped at 50 entries and wiped by every `--reload`), which made a run's
proposal — and the before-image needed to undo an apply — unrecoverable
after a backend restart.

The row is the system of record for everything Kasal needs AFTER the run:
the proposal (`optimized_template` / `optimized_fields`), the scores, and
`before_image` — the values every field held at APPLY time, which is what
`revert` writes back. In-flight progress counters are also persisted (via
a heartbeat) so a reload does not lose a half-spent execution budget and
so runs orphaned by a restart can be told apart from live ones.
"""

from datetime import datetime
from uuid import uuid4

from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    DateTime,
    Float,
    Index,
    Integer,
    String,
    Text,
)

from src.db.base import Base


def generate_uuid():
    return str(uuid4())


class PromptOptimizationRun(Base):
    """One prompt-optimization run (a seeded template, or a saved crew)."""

    __tablename__ = "prompt_optimization_runs"

    # The service's short hex run_id is the PK — it is what the API, the
    # frontend and the in-memory cache already key on.
    id = Column(String(64), primary_key=True, default=generate_uuid)

    # 'template' (a seeded meta-prompt) or 'crew' (a saved crew's prompt fields)
    kind = Column(String(16), nullable=False, default="template")
    # Display/target identifier: the template name, or 'crew:<crew name>'
    target_name = Column(String(255), nullable=False)
    # Set for crew runs — the concrete crew whose agent/task rows get written
    crew_id = Column(String(64), nullable=True, index=True)

    status = Column(String(16), nullable=False, default="pending")
    error = Column(Text, nullable=True)

    # Model wiring, recorded so a run's result can be interpreted later —
    # notably whether judge_model == model (self-preference, see the service).
    model = Column(String(255), nullable=True)
    judge_model = Column(String(255), nullable=True)
    reflection_model = Column(String(255), nullable=True)

    # Budget + progress
    budget = Column(Integer, nullable=True)
    dataset_size = Column(Integer, nullable=False, default=0)
    executions_used = Column(Integer, nullable=True)
    execution_cap = Column(Integer, nullable=True)
    candidates_tried = Column(Integer, nullable=True)
    human_feedback_count = Column(Integer, nullable=True)

    initial_score = Column(Float, nullable=True)
    final_score = Column(Float, nullable=True)

    # The proposal. Template runs use the *_template text columns; crew runs
    # additionally carry the per-field maps ('agent.<id>.role' -> text).
    baseline_template = Column(Text, nullable=True)
    optimized_template = Column(Text, nullable=True)
    baseline_fields = Column(JSON, nullable=True)
    optimized_fields = Column(JSON, nullable=True)

    # REVERSIBILITY: the values every touched field held immediately BEFORE
    # the apply wrote over them, captured at apply time (NOT the run's
    # baseline — the rows may have been edited between run and apply).
    # Crew runs: {'agent.<id>.role': '...'}. Template runs: {'template': '...'}.
    before_image = Column(JSON, nullable=True)

    applied = Column(Boolean, nullable=False, default=False)
    applied_at = Column(DateTime, nullable=True)
    applied_by = Column(String(255), nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    # Bumped by the run's heartbeat while it is active: a pending/running row
    # whose updated_at has gone stale was orphaned by a backend restart.
    updated_at = Column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False
    )

    # Multi-group fields (REQUIRED for all models)
    group_id = Column(String(100), index=True, nullable=True)
    group_email = Column(String(255), nullable=True)
    created_by_email = Column(String(255), nullable=True)

    __table_args__ = (
        Index("idx_prompt_opt_runs_group_created", "group_id", "created_at"),
        Index("idx_prompt_opt_runs_status", "status"),
    )

    def __repr__(self) -> str:
        return (
            f"<PromptOptimizationRun id={self.id} kind={self.kind} "
            f"target={self.target_name} status={self.status}>"
        )
