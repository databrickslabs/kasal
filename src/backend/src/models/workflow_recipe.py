"""Workflow recipes — executed crews, kept so they can be reused.

A completed Kasal run already IS a validated plan: the crew graph (agents,
tasks, their ``context`` wiring) plus the tools and MCP servers it actually
exercised, and the fact that it ran to completion. Today that is thrown away and
the next equivalent request pays an LLM to derive the same crew again — in this
workspace "Load US and EU" was derived from scratch 29 times.

A recipe is one distilled run, scoped to a workspace. Phase 1 only WRITES them;
retrieval and reuse come later.

Deliberately NOT modelled as a quality signal. ``COMPLETED`` means the crew
finished, not that its output was correct — a crew can run clean and load
garbage — so nothing here claims a recipe is *good*. The curation columns exist
for a later phase to record an explicit human judgement, which is the only
trustworthy source of that. Until then the ranking is recency, which is honest
about knowing nothing.
"""

from datetime import datetime
from typing import Any, Optional

from sqlalchemy import JSON, DateTime, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from src.db.base import Base

# Reuse the pgvector column with its SQLite fallback rather than redefining it —
# one definition, so dev (SQLite, JSON-encoded) and Lakebase (real pgvector)
# cannot drift.
from src.models.documentation_embedding import Vector


class WorkflowRecipe(Base):
    """One executed crew, distilled for reuse."""

    __tablename__ = "workflow_recipes"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)

    # --- Workspace isolation (a recipe must never cross a group boundary) ---
    group_id: Mapped[Optional[str]] = mapped_column(
        String(100), index=True, nullable=True
    )
    group_email: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    # --- What this crew was for ---
    # intent_text is what a later phase embeds and matches against; intent_hash
    # is a cheap exact-dedup key so 29 runs of one intent collapse to one row.
    intent_text: Mapped[str] = mapped_column(Text, nullable=False)
    intent_hash: Mapped[str] = mapped_column(String(64), index=True, nullable=False)
    embedding: Mapped[Any] = mapped_column(
        Vector(1024), nullable=True
    )  # populated in the retrieval phase

    # --- The reusable artifact ---
    agents_yaml: Mapped[Any] = mapped_column(JSON, nullable=False)
    tasks_yaml: Mapped[Any] = mapped_column(JSON, nullable=False)
    # Tools/servers OBSERVED in the trace, not merely configured — a tool that
    # was bound but never called is not part of what made this crew work.
    tool_names: Mapped[Any] = mapped_column(JSON, nullable=True)
    mcp_servers: Mapped[Any] = mapped_column(JSON, nullable=True)

    # --- Where it came from ---
    # The run this recipe currently reflects (refreshed as repeats arrive).
    source_job_id: Mapped[str] = mapped_column(String(255), index=True, nullable=False)
    # EVERY execution folded into this recipe. Needed for idempotency: dedup
    # rewrites source_job_id to the newest run, so without this the runs it
    # replaced would look unmined and be swept in again on every pass, inflating
    # run_count forever.
    mined_job_ids: Mapped[Any] = mapped_column(JSON, nullable=False, default=list)
    # PROVENANCE. Set when the source run was itself started from a recipe, so
    # mining can skip it. Without this the corpus eventually feeds on its own
    # output: a cached plan shapes the next run, that run gets mined, and the
    # library collapses onto whatever was cached first while looking healthier
    # each round because it is agreeing with itself.
    source_recipe_id: Mapped[Optional[int]] = mapped_column(
        Integer, index=True, nullable=True
    )

    # --- Observed execution shape (descriptive, NOT a quality score) ---
    run_count: Mapped[int] = mapped_column(
        Integer, default=1, nullable=False
    )  # times this intent recurred
    span_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    tool_call_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    error_span_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    duration_ms: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # --- Human curation (written by a later phase; the only real quality signal) ---
    curation: Mapped[Optional[str]] = mapped_column(
        String(16), index=True, nullable=True
    )  # 'good' | 'bad' | 'hidden'
    curated_by: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    curated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    times_reused: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=func.now(), onupdate=func.now(), nullable=False
    )

    def __repr__(self) -> str:
        return (
            f"<WorkflowRecipe id={self.id} group={self.group_id!r} "
            f"runs={self.run_count} job={self.source_job_id!r}>"
        )
