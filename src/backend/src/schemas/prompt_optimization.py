"""
Pydantic schemas for prompt optimization operations.

Data-driven optimization of the seeded meta-prompts (GEPA via
mlflow.genai.optimize_prompts): a run mines training examples from the
LLM interaction log (or takes them inline), searches for a better
template, and proposes it for explicit review-and-apply.
"""

from datetime import datetime
from typing import Dict, List, Literal, Optional

from pydantic import BaseModel, Field


class PromptOptimizationRequest(BaseModel):
    """Request to start a prompt optimization run for a seeded template."""

    template_name: Literal[
        "detect_intent",
        "generate_agent",
        "generate_task",
        "generate_crew",
        "generate_crew_plan",
        "generate_job_name",
    ] = Field(
        ...,
        description="The prompt template to optimize (must be wired in TEMPLATE_TASKS)",
    )
    model: Optional[str] = Field(
        None,
        description="Target model the optimized prompt must perform on "
        "(defaults to the dispatcher's fast model)",
    )
    judge_model: Optional[str] = Field(
        None,
        description="Model used to judge output correctness (defaults to the target model)",
    )
    reflection_model: Optional[str] = Field(
        None,
        description="Model GEPA uses to reflect on failures and mutate the prompt "
        "(defaults to the target model)",
    )
    examples: Optional[List[str]] = Field(
        None,
        description="Explicit training inputs (user messages). When omitted, "
        "examples are mined from the LLM interaction log.",
    )
    lookback_days: int = Field(
        30, ge=1, le=365, description="Log-mining window in days"
    )
    max_examples: int = Field(50, ge=5, le=200, description="Maximum training examples")
    max_metric_calls: int = Field(
        40,
        ge=8,
        le=400,
        description="GEPA evaluation budget — each call is one LLM invocation of the "
        "target model (plus judge calls), so this bounds run cost",
    )


class CrewOptimizationRequest(BaseModel):
    """Request to GEPA-optimize a saved crew's prompt fields.

    EXPENSIVE BY DESIGN: every metric call executes the crew for real (tools
    included) and judges the final deliverable — the budget is the number of
    crew executions.
    """

    crew_id: str = Field(..., description="The saved crew to optimize")
    model: Optional[str] = Field(
        None, description="Model the crew executes with during evaluation"
    )
    judge_model: Optional[str] = Field(None, description="Judge model for deliverables")
    reflection_model: Optional[str] = Field(None, description="GEPA reflection model")
    guidance: Optional[str] = Field(
        None, description="Optional extra judging guidance (what 'good' looks like)"
    )
    max_metric_calls: int = Field(
        10,
        ge=4,
        le=40,
        description="HARD CAP on crew executions (includes 1 validation + 1 "
        "baseline, so 10 buys ~8 candidate evaluations)",
    )
    execution_timeout_seconds: int = Field(
        900, ge=60, le=3600, description="Per-execution timeout"
    )


class PromptOptimizationStartResponse(BaseModel):
    """Acknowledgement that an optimization run started in the background."""

    run_id: str = Field(..., description="Identifier to poll for status")
    status: str = Field(..., description="Initial run status")
    dataset_size: int = Field(
        ..., description="Number of training examples the run will use"
    )


class PromptOptimizationRunStatus(BaseModel):
    """Status/result of an optimization run."""

    run_id: str
    template_name: str
    status: Literal["pending", "running", "completed", "failed", "cancelled"]
    dataset_size: int = 0
    model: Optional[str] = None
    initial_score: Optional[float] = Field(None, description="Baseline template score")
    final_score: Optional[float] = Field(None, description="Optimized template score")
    baseline_template: Optional[str] = Field(
        None, description="Template text the run started from"
    )
    optimized_template: Optional[str] = Field(
        None, description="Proposed template (present when status=completed)"
    )
    error: Optional[str] = None
    applied: bool = Field(
        False, description="Whether the proposal was applied as a group override"
    )
    created_at: Optional[datetime] = None
    kind: Optional[str] = Field(
        None, description="'template' (seeded meta-prompt) or 'crew' (saved crew)"
    )
    crew_id: Optional[str] = Field(None, description="Crew id for crew runs")
    baseline_fields: Optional[Dict[str, str]] = Field(
        None, description="Crew runs: per-field baseline texts (agent.<id>.<field>)"
    )
    optimized_fields: Optional[Dict[str, str]] = Field(
        None, description="Crew runs: per-field proposed texts"
    )
    executions_used: Optional[int] = Field(
        None, description="Crew runs: crew executions spent so far"
    )
    execution_cap: Optional[int] = Field(
        None, description="Crew runs: hard cap on crew executions"
    )
    human_feedback_count: Optional[int] = Field(
        None,
        description="Crew runs: human grades/expectations steering this run",
    )
    candidates_tried: Optional[int] = Field(
        None,
        description="Crew runs: distinct candidate prompt sets actually executed",
    )


class PromptOptimizationRunList(BaseModel):
    """Recent optimization runs for the caller's group."""

    runs: List[PromptOptimizationRunStatus]


class PromptOptimizationApplyResponse(BaseModel):
    """Result of applying a completed run's proposed template."""

    run_id: str
    template_name: str
    applied: bool
