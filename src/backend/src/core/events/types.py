"""Event types for the kasal engine.

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


class BaseEvent(BaseModel):
    """Base class for all engine events. Carries a caller-supplied context dict natively (native requirement #1 — kills kasal's memory-event __init__ patch)."""

    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    type: str
    source_fingerprint: str | None = None
    source_type: str | None = None
    fingerprint_metadata: dict[str, Any] | None = None
    task_id: str | None = None
    task_name: str | None = None
    agent_id: str | None = None
    agent_role: str | None = None
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    parent_event_id: str | None = None
    previous_event_id: str | None = None
    triggered_by_event_id: str | None = None
    started_event_id: str | None = None
    emission_sequence: int | None = None
    execution_context: dict[str, Any] = Field(default_factory=dict)
    """Caller-supplied execution context (group/tenant/user). Native requirement #1: replaces kasal's crewai_patches.py memory-event injection. Named to avoid colliding with crewAI's per-event context fields (TaskStartedEvent.context is the task context string)."""


class AgentExecutionCompletedEvent(BaseEvent):
    """Engine replacement for crewai.events.AgentExecutionCompletedEvent"""

    agent: Any
    task: Any
    output: str
    type: Literal["agent_execution_completed"] = "agent_execution_completed"
    model_config = ConfigDict(arbitrary_types_allowed=True)

    @model_validator(mode="after")
    def set_fingerprint_data(self) -> Self:
        agent = self.agent
        if agent is not None:
            role = getattr(agent, "role", None)
            if role is not None:
                self.agent_role = str(role)
            agent_id = getattr(agent, "id", None)
            if agent_id is not None:
                self.agent_id = str(agent_id)
            fingerprint = getattr(agent, "fingerprint", None)
            uuid_str = getattr(fingerprint, "uuid_str", None)
            if uuid_str is not None:
                self.source_fingerprint = uuid_str
        return self


class AgentExecutionStartedEvent(BaseEvent):
    """Emitted before an agent executes a task."""

    agent: Any
    task: Any
    tools: Sequence[Any] | None = None
    task_prompt: str
    type: Literal["agent_execution_started"] = "agent_execution_started"
    model_config = ConfigDict(arbitrary_types_allowed=True)


class CrewBaseEvent(BaseEvent):
    """Base for crew lifecycle events; carries crew_name."""

    crew_name: str | None = None


class CrewKickoffCompletedEvent(CrewBaseEvent):
    """Engine replacement for crewai.events.CrewKickoffCompletedEvent"""

    output: Any
    type: Literal["crew_kickoff_completed"] = "crew_kickoff_completed"
    total_tokens: int = 0


class CrewCheckpointRestoredEvent(CrewBaseEvent):
    """A crew was RESTORED from a checkpoint instead of being executed.

    Emitted by a flow resume for every crew it skips. Without it those crews
    leave no trace at all, so a resumed run's timeline shows only the part that
    re-ran and reads as a partial job.

    Deliberately its OWN event rather than a synthetic CrewKickoffCompletedEvent:
    that would put rows in the trace claiming work ran when it did not, and it
    would also reach the checkpoint recorder, which would re-record the crew
    from a stub that has no tasks and so overwrite a verified identity with an
    unverifiable one.
    """

    output: Any
    type: Literal["crew_checkpoint_restored"] = "crew_checkpoint_restored"
    #: The run this output was originally produced by, when known.
    restored_from: str | None = None


class CrewKickoffStartedEvent(CrewBaseEvent):
    """Engine replacement for crewai.events.CrewKickoffStartedEvent"""

    inputs: dict[str, Any] | None
    type: Literal["crew_kickoff_started"] = "crew_kickoff_started"


class FlowBaseEvent(BaseEvent):
    """Base for flow lifecycle events; carries flow_name."""

    flow_name: str | None = None


class FlowStartedEvent(FlowBaseEvent):
    """Emitted once when a Flow begins its kickoff.

    Opens the outermost causality scope of a flow run, so the crew kickoffs the
    flow drives become children of the flow rather than unrelated roots.
    """

    inputs: dict[str, Any] | None = None
    type: Literal["flow_started"] = "flow_started"


class FlowCheckpointSavedEvent(FlowBaseEvent):
    """A flow's state was persisted — or could not be.

    The trace already showed a checkpoint being RESTORED
    (:class:`CrewCheckpointRestoredEvent`) and never showed one being WRITTEN,
    so the half that a resume depends on was the invisible half. When nothing
    resumed, there was no way to tell "no checkpoint was written" from "the
    checkpoint was written and ignored" without querying the database by hand.

    Failures are emitted too, with ``error`` set. A checkpoint write that fails
    is not a run failure — the run carries on and returns its answer — which is
    exactly why it needs to be visible: silently, every later turn starts from
    scratch and the flow looks like it simply has no memory.
    """

    method_name: str | None = None
    flow_uuid: str | None = None
    error: str | None = None
    type: Literal["flow_checkpoint_saved"] = "flow_checkpoint_saved"


class FlowFinishedEvent(FlowBaseEvent):
    """Emitted once when a Flow's kickoff finishes (success or failure).

    Closes the scope opened by :class:`FlowStartedEvent` (see the bus's
    ``_SCOPE_CLOSERS``).
    """

    result: Any | None = None
    error: str | None = None
    type: Literal["flow_finished"] = "flow_finished"
    model_config = ConfigDict(arbitrary_types_allowed=True)


class LLMEventBase(BaseEvent):
    """Base for LLM call events; carries model and call attribution."""

    from_task: Any | None = None
    from_agent: Any | None = None
    model: str | None = None
    call_id: str = Field(default_factory=lambda: str(uuid.uuid4()))


class LLMCallType(str, Enum):
    """Engine replacement for crewai.events.types.llm_events.LLMCallType"""

    LLM_CALL = "llm_call"
    TOOL_CALL = "tool_call"


class LLMCallCompletedEvent(LLMEventBase):
    """Engine replacement for crewai.events.types.llm_events.LLMCallCompletedEvent"""

    type: Literal["llm_call_completed"] = "llm_call_completed"
    messages: str | list[dict[str, Any]] | None = None
    response: Any
    call_type: LLMCallType
    usage: dict[str, Any] | None = None
    finish_reason: str | None = None
    response_id: str | None = None

    @field_validator("finish_reason", "response_id", mode="before")
    @classmethod
    def _coerce_non_string_to_none(cls, value: Any) -> str | None:
        return value if isinstance(value, str) else None


class LLMCallFailedEvent(LLMEventBase):
    """Engine replacement for crewai.events.types.llm_events.LLMCallFailedEvent"""

    error: str
    type: Literal["llm_call_failed"] = "llm_call_failed"


class LLMStreamChunkEvent(LLMEventBase):
    """A text delta received while an LLM call streams (LLM.stream=True).

    Emitted between LLMCallStartedEvent and LLMCallCompletedEvent, once per
    provider delta. `chunk` is the incremental text only; the completed event
    still carries the full response. Not traced (kasal's otel bridge skips it);
    consumers are live-UI forwarders.
    """

    type: Literal["llm_stream_chunk"] = "llm_stream_chunk"
    chunk: str
    chunk_index: int | None = None


class ContextCompactionEvent(LLMEventBase):
    """The conversation was compacted to fit the model's context window.

    Emitted ONLY when compaction actually drops something, so a quiet run stays
    quiet in the trace. Compaction is lossy — the tool-loop variant replaces the
    OLDEST tool results with a stub — and until this event existed it happened
    with no log, event or span at all. An agent that silently lost the schema it
    had just read would re-query it and loop until the round budget ran out
    ("Tool-calling did not converge within N rounds"), and nothing in the trace
    explained why. Surfacing it makes that failure mode self-evident.
    """

    type: Literal["context_compaction"] = "context_compaction"
    strategy: str = "tool_result_stub"
    tokens_before: int | None = None
    tokens_after: int | None = None
    window: int | None = None
    messages_compacted: int = 0
    reason: str | None = None


class A2UISurfaceEvent(LLMEventBase):
    """An answer was considered for rendering as an A2UI surface.

    Emitted for EVERY outcome, not only the successful one. Composition is a
    gauntlet of quiet skips — the workspace has A2UI off, the request implies no
    rich surface, the composer fell back to prose, a dashboard came back with no
    data component — and each one returns the answer as plain text with nothing
    in the event bus or the trace to say which gate closed. "Why did I not get a
    presentation?" was unanswerable without reading the logs of a subprocess.

    ``outcome`` is the gate that decided, and ``reason`` says it in words.
    """

    type: Literal["a2ui_surface"] = "a2ui_surface"
    #: composed | disabled | no_rich_intent | no_text | composer_unavailable |
    #: compose_failed | conversation_fallback | no_data_component
    outcome: str = "composed"
    reason: str | None = None
    surface_kind: str | None = None
    component_count: int | None = None
    query: str | None = None
    purpose: str | None = None
    duration_ms: float | None = None


class LLMCallStartedEvent(LLMEventBase):
    """Engine replacement for crewai.events.types.llm_events.LLMCallStartedEvent"""

    type: Literal["llm_call_started"] = "llm_call_started"
    messages: str | list[dict[str, Any]] | None = None
    tools: list[dict[str, Any]] | None = None
    callbacks: list[Any] | None = None
    available_functions: dict[str, Any] | None = None
    temperature: float | None = None
    top_p: float | None = None
    max_tokens: int | float | None = None
    stream: bool | None = None
    seed: int | None = None
    stop_sequences: list[str] | None = None
    frequency_penalty: float | None = None
    presence_penalty: float | None = None
    n: int | None = None

    @field_validator("stop_sequences", mode="before")
    @classmethod
    def _coerce_stop_sequences_to_str_list(cls, value: Any) -> list[str] | None:
        if value is None:
            return None
        if isinstance(value, str):
            return [value]
        return [str(item) for item in value]


class LiteAgentExecutionCompletedEvent(BaseEvent):
    """Engine replacement for crewai.events.types.agent_events.LiteAgentExecutionCompletedEvent"""

    agent_info: dict[str, Any]
    output: str
    type: Literal["lite_agent_execution_completed"] = "lite_agent_execution_completed"


class LiteAgentExecutionErrorEvent(BaseEvent):
    """Engine replacement for crewai.events.types.agent_events.LiteAgentExecutionErrorEvent"""

    agent_info: dict[str, Any]
    error: str
    type: Literal["lite_agent_execution_error"] = "lite_agent_execution_error"


class LiteAgentExecutionStartedEvent(BaseEvent):
    """Engine replacement for crewai.events.types.agent_events.LiteAgentExecutionStartedEvent"""

    agent_info: dict[str, Any]
    tools: Sequence[Any] | None
    messages: str | list[dict[str, str]]
    type: Literal["lite_agent_execution_started"] = "lite_agent_execution_started"
    model_config = ConfigDict(arbitrary_types_allowed=True)


class LLMGuardrailStartedEvent(BaseEvent):
    """Emitted before a guardrail validates a task's output (one per attempt)."""

    guardrail: str
    retry_count: int = 0
    type: Literal["llm_guardrail_started"] = "llm_guardrail_started"
    task: Any | None = None
    model_config = ConfigDict(arbitrary_types_allowed=True)


class LLMGuardrailCompletedEvent(BaseEvent):
    """Emitted after a guardrail validation attempt, pass or fail."""

    guardrail: str
    success: bool
    result: str | None = None
    error: str | None = None
    retry_count: int = 0
    type: Literal["llm_guardrail_completed"] = "llm_guardrail_completed"
    task: Any | None = None
    model_config = ConfigDict(arbitrary_types_allowed=True)


class LLMGuardrailFailedEvent(BaseEvent):
    """Emitted when a guardrail rejects the output after exhausting retries."""

    guardrail: str
    error: str
    retry_count: int = 0
    type: Literal["llm_guardrail_failed"] = "llm_guardrail_failed"
    task: Any | None = None
    model_config = ConfigDict(arbitrary_types_allowed=True)


class MemoryBaseEvent(BaseEvent):
    """Base for memory events; task/agent attribution inherited from BaseEvent."""

    from_task: Any | None = None
    from_agent: Any | None = None


class MemoryQueryCompletedEvent(MemoryBaseEvent):
    """Engine replacement for crewai.events.types.memory_events.MemoryQueryCompletedEvent"""

    type: Literal["memory_query_completed"] = "memory_query_completed"
    query: str
    results: Any
    limit: int
    score_threshold: float | None = None
    query_time_ms: float


class MemoryQueryFailedEvent(MemoryBaseEvent):
    """Emitted when a memory query fails."""

    type: Literal["memory_query_failed"] = "memory_query_failed"
    query: str
    limit: int
    score_threshold: float | None = None
    error: str


class MemoryQueryStartedEvent(MemoryBaseEvent):
    """Emitted before a memory query."""

    type: Literal["memory_query_started"] = "memory_query_started"
    query: str
    limit: int
    score_threshold: float | None = None


class MemoryRetrievalCompletedEvent(MemoryBaseEvent):
    """Engine replacement for crewai.events.types.memory_events.MemoryRetrievalCompletedEvent"""

    type: Literal["memory_retrieval_completed"] = "memory_retrieval_completed"
    task_id: str | None = None
    memory_content: str
    retrieval_time_ms: float


class MemorySaveCompletedEvent(MemoryBaseEvent):
    """Engine replacement for crewai.events.types.memory_events.MemorySaveCompletedEvent"""

    type: Literal["memory_save_completed"] = "memory_save_completed"
    value: str
    metadata: dict[str, Any] | None = None
    agent_role: str | None = None
    save_time_ms: float


class MemorySaveFailedEvent(MemoryBaseEvent):
    """Emitted when a memory save fails."""

    type: Literal["memory_save_failed"] = "memory_save_failed"
    value: str | None = None
    metadata: dict[str, Any] | None = None
    agent_role: str | None = None
    error: str


class MemorySaveStartedEvent(MemoryBaseEvent):
    """Emitted before a memory save."""

    type: Literal["memory_save_started"] = "memory_save_started"
    value: str | None = None
    metadata: dict[str, Any] | None = None
    agent_role: str | None = None


class TaskCompletedEvent(BaseEvent):
    """Emitted when a task completes."""

    output: Any
    type: Literal["task_completed"] = "task_completed"
    task: Any | None = None


class TaskCheckpointRestoredEvent(BaseEvent):
    """A task was RESTORED from a checkpoint instead of being executed.

    The crew counterpart of CrewCheckpointRestoredEvent. A resumed crew skips
    every task in the restored prefix, and skipping used to be completely
    silent — correct while a resume reused the original run's record, and wrong
    now that it creates a NEW execution with its own trace, where those tasks
    left no mark at all and the run read as though it had started midway.

    Not a synthetic TaskCompletedEvent: that would claim the task ran, and it
    would also reach the checkpoint recorder and the callbacks/sinks that
    already fired on the original attempt.
    """

    output: Any
    type: Literal["task_checkpoint_restored"] = "task_checkpoint_restored"
    task: Any | None = None


class TaskFailedEvent(BaseEvent):
    """Emitted when a task fails."""

    error: str
    type: Literal["task_failed"] = "task_failed"
    task: Any | None = None


class TaskStartedEvent(BaseEvent):
    """Emitted when a task starts executing."""

    type: Literal["task_started"] = "task_started"
    context: str | None = None
    task: Any | None = None


class ToolUsageEvent(BaseEvent):
    """Base for tool usage events; carries tool_name/tool_args and agent attribution."""

    agent_key: str | None = None
    agent_role: str | None = None
    agent_id: str | None = None
    tool_name: str
    tool_args: dict[str, Any] | str
    tool_class: str | None = None
    run_attempts: int = 0
    delegations: int | None = None
    agent: Any | None = None
    task_name: str | None = None
    task_id: str | None = None
    from_task: Any | None = None
    from_agent: Any | None = None
    model_config = ConfigDict(arbitrary_types_allowed=True)


class ToolUsageErrorEvent(ToolUsageEvent):
    """Engine replacement for crewai.events.types.tool_usage_events.ToolUsageErrorEvent"""

    error: Any
    type: Literal["tool_usage_error"] = "tool_usage_error"


class ToolUsageFinishedEvent(ToolUsageEvent):
    """Engine replacement for crewai.events.types.tool_usage_events.ToolUsageFinishedEvent"""

    started_at: datetime
    finished_at: datetime
    from_cache: bool = False
    output: Any
    type: Literal["tool_usage_finished"] = "tool_usage_finished"


class ToolUsageStartedEvent(ToolUsageEvent):
    """Engine replacement for crewai.events.types.tool_usage_events.ToolUsageStartedEvent"""

    type: Literal["tool_usage_started"] = "tool_usage_started"
