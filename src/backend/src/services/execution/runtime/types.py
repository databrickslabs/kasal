"""Orchestration data models (Process, outputs, planning).

Generated from the kasal_engine datamodel — do not edit by hand.
Edit the component/component_member rows and re-run generator/generate.py.
"""

import json
import uuid
from collections.abc import Sequence
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Literal, Self
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class OutputFormat(str, Enum):
    """Task output format."""

    JSON = "json"
    PYDANTIC = "pydantic"
    RAW = "raw"


class TaskOutput(BaseModel):
    """Engine replacement for crewai.tasks.task_output.TaskOutput"""

    description: str = Field(description="Description of the task")
    name: str | None = Field(description="Name of the task", default=None)
    expected_output: str | None = Field(
        description="Expected output of the task", default=None
    )
    summary: str | None = Field(description="Summary of the task", default=None)
    raw: str = Field(description="Raw output of the task", default="")
    pydantic: BaseModel | None = Field(
        description="Pydantic output of task", default=None
    )
    json_dict: dict[str, Any] | None = Field(
        description="JSON dictionary of task", default=None
    )
    agent: str = Field(description="Agent that executed the task")
    output_format: OutputFormat = Field(
        description="Output format of the task", default=OutputFormat.RAW
    )
    messages: list[Any] = Field(
        description="Messages of the task", default_factory=list
    )

    def to_dict(self) -> dict[str, Any]:
        """Structured content: json_dict if present, else pydantic dump."""
        if self.json_dict:
            return dict(self.json_dict)
        if self.pydantic:
            return self.pydantic.model_dump()
        return {}

    @property
    def json(self) -> str | None:
        if self.output_format != OutputFormat.JSON:
            raise ValueError(
                "No JSON output found in the final task. Make sure to set the "
                "output_json property in the task."
            )
        return json.dumps(self.json_dict)


class UsageMetrics(BaseModel):
    """Aggregated token usage for a crew run."""

    total_tokens: int = 0
    prompt_tokens: int = 0
    cached_prompt_tokens: int = 0
    completion_tokens: int = 0
    reasoning_tokens: int = 0
    cache_creation_tokens: int = 0
    successful_requests: int = 0

    def add_usage_metrics(self, usage_metrics: "UsageMetrics") -> None:
        self.total_tokens += usage_metrics.total_tokens
        self.prompt_tokens += usage_metrics.prompt_tokens
        self.cached_prompt_tokens += usage_metrics.cached_prompt_tokens
        self.completion_tokens += usage_metrics.completion_tokens
        self.reasoning_tokens += usage_metrics.reasoning_tokens
        self.cache_creation_tokens += usage_metrics.cache_creation_tokens
        self.successful_requests += usage_metrics.successful_requests


class CrewOutput(BaseModel):
    """Result of Crew.kickoff."""

    raw: str = Field(description="Raw output of crew", default="")
    pydantic: BaseModel | None = None
    json_dict: dict[str, Any] | None = None
    tasks_output: list[TaskOutput] = Field(default_factory=list)
    token_usage: UsageMetrics = Field(default_factory=UsageMetrics)

    @property
    def json(self) -> str | None:
        return json.dumps(self.json_dict) if self.json_dict is not None else None

    def to_dict(self) -> dict[str, Any]:
        if self.json_dict:
            return dict(self.json_dict)
        if self.pydantic:
            return self.pydantic.model_dump()
        return {}

    def __str__(self) -> str:
        if self.pydantic:
            return str(self.pydantic)
        if self.json_dict:
            return str(self.json_dict)
        return self.raw


class LiteAgentOutput(BaseModel):
    """Result of Agent.kickoff (lite agent path)."""

    raw: str = ""
    pydantic: BaseModel | None = None
    agent_role: str | None = None
    usage_metrics: UsageMetrics | None = None
    messages: list[dict[str, Any]] = Field(default_factory=list)

    def __str__(self) -> str:
        return self.raw


class PlanningConfig(BaseModel):
    """Engine replacement for crewai.agent.planning_config.PlanningConfig"""

    reasoning_effort: Literal["low", "medium", "high"] = Field(
        default="medium",
        description=(
            "Controls post-step observation and replanning behavior. "
            "'low' skips per-step PlannerObserver LLM calls (fastest). "
            "'medium' observes via LLM and replans only on step failure (balanced). "
            "'high' runs full observation pipeline with replanning, refinement, "
            "and early goal detection (most adaptive, highest latency)."
        ),
    )
    observe_steps: bool | None = Field(
        default=None,
        description=(
            "Run PlannerObserver LLM calls after each step. "
            "None (default): LLM observation for 'medium' and 'high' only; "
            "'low' uses a heuristic (no extra LLM). "
            "Set False to disable observation at any effort level."
        ),
    )
    max_attempts: int | None = Field(
        default=None,
        description=(
            "Maximum number of planning refinement attempts. "
            "If None, will continue until the agent indicates readiness."
        ),
    )
    max_steps: int = Field(
        default=20,
        description="Maximum number of steps in the generated plan.",
        ge=1,
    )
    system_prompt: str | None = Field(
        default=None,
        description="Custom system prompt for planning. Uses default if None.",
    )
    plan_prompt: str | None = Field(
        default=None,
        description="Custom prompt for creating the initial plan.",
    )
    refine_prompt: str | None = Field(
        default=None,
        description="Custom prompt for refining the plan.",
    )
    max_replans: int = Field(
        default=3,
        description="Maximum number of full replanning attempts before finalizing.",
        ge=0,
    )
    max_step_iterations: int = Field(
        default=15,
        description=(
            "Maximum LLM iterations per step in the StepExecutor multi-turn loop. "
            "Lower values make steps faster but less thorough."
        ),
        ge=1,
    )
    step_timeout: int | None = Field(
        default=None,
        description=(
            "Maximum wall-clock seconds for a single step execution. "
            "If exceeded, the step is marked as failed and observation decides "
            "whether to continue or replan. None means no per-step timeout."
        ),
    )
    llm: str | Any | None = Field(
        default=None,
        description="LLM to use for planning. Uses agent's LLM if None.",
    )
    model_config = {"arbitrary_types_allowed": True}


class Process(str, Enum):
    """Engine replacement for crewai.Process"""

    sequential = "sequential"
    hierarchical = "hierarchical"
