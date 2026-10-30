"""Workflow-recipe trials — one row per crew generation that consulted the library.

Exemplars are injected into the crew-generation prompt, which means their effect
is invisible: the generated crew looks like any other. This table is how the
effect becomes measurable at all. Each row records what the retrieval found, what
was done with it, and — once the generated crew is actually run — how that run
turned out.

Two design points do the real work:

**The control arm.** ``arm`` distinguishes a generation that received exemplars
from one that qualified for them but was deliberately denied them (the holdout).
Without that distinction the comparison is worthless: a prompt that matches a
past crew is by definition REPEAT work, and repeat work succeeds more often than
novel work whether or not exemplars exist. Comparing "had a match" against "had
no match" measures how familiar the request was, then credits the feature for it.

**The join key.** ``agent_ids`` holds the database ids of the agents this
generation created. A crew execution stores its agents under ``agents_yaml`` keys
of the form ``agent_<id>``, so those ids link a generation to the run it
eventually produced — exactly, with no heuristics and no frontend changes. The
link is best-effort and often absent: plenty of generated crews are edited beyond
recognition, or never run at all.

Everything here is descriptive. Nothing in this table claims a recipe is good.
"""

from datetime import datetime
from typing import Any, Optional

from sqlalchemy import JSON, DateTime, Float, Index, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from src.db.base import Base

# --- arm values -------------------------------------------------------------
# The population split that makes the report readable. Kept as constants because
# both the writer (assignment) and the reader (aggregation) must agree, and a
# typo in either would silently produce an empty arm rather than an error.
ARM_EXEMPLAR = "exemplar"  # blessed matches existed and were injected
ARM_CONTROL = "control"  # blessed matches existed, withheld by the holdout
ARM_NONE = "none_available"  # nothing blessed cleared the bar; baseline population

ARMS = (ARM_EXEMPLAR, ARM_CONTROL, ARM_NONE)


class WorkflowRecipeTrial(Base):
    """One crew generation, and what the recipe library contributed to it."""

    __tablename__ = "workflow_recipe_trials"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)

    group_id: Mapped[Optional[str]] = mapped_column(
        String(100), index=True, nullable=True
    )
    group_email: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    # --- The request -------------------------------------------------------
    # The prompt is kept truncated: this is a measurement ledger, not a second
    # copy of the user's input, and llmlog already stores the full prompt.
    prompt_hash: Mapped[str] = mapped_column(String(64), index=True, nullable=False)
    prompt_text: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # --- What retrieval found ---------------------------------------------
    # Full candidate list with scores, so a disappointing report can be
    # diagnosed (was the threshold wrong, or was nothing curated?) without
    # re-running retrieval against a library that has since changed.
    candidates: Mapped[Any] = mapped_column(JSON, nullable=False, default=list)
    candidate_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    blessed_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    best_similarity: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # --- What was done with it --------------------------------------------
    arm: Mapped[str] = mapped_column(String(16), index=True, nullable=False)
    injected_recipe_ids: Mapped[Any] = mapped_column(JSON, nullable=False, default=list)

    # --- What came out -----------------------------------------------------
    agent_ids: Mapped[Any] = mapped_column(JSON, nullable=False, default=list)
    task_ids: Mapped[Any] = mapped_column(JSON, nullable=False, default=list)
    agent_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    task_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # --- How the resulting run went (filled by the linker, often never) -----
    linked_job_id: Mapped[Optional[str]] = mapped_column(
        String(255), index=True, nullable=True
    )
    linked_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    outcome_status: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    outcome_duration_ms: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    outcome_error_spans: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    outcome_tool_calls: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=func.now(), nullable=False
    )

    __table_args__ = (
        # The report groups by (group, arm) over a time window, and the linker
        # scans for rows that still have no run attached.
        Index("idx_recipe_trials_group_arm", "group_id", "arm"),
        Index("idx_recipe_trials_unlinked", "linked_job_id", "created_at"),
    )

    def __repr__(self) -> str:
        return (
            f"<WorkflowRecipeTrial id={self.id} arm={self.arm!r} "
            f"injected={self.injected_recipe_ids} job={self.linked_job_id!r}>"
        )
