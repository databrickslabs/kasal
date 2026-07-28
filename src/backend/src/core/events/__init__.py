"""
The event bus: what a run announces while it happens.

Agents, tasks, tools, LLM calls, memory, guardrails and A2UI surfaces all emit
here. Exactly ONE subscriber writes traces — ``OTelEventBridge`` in
``services/otel_tracing`` — and adding a second is how this codebase ended up
with three generations of dead listeners (see services/execution/CLAUDE.md).

**This is core infrastructure, not a service.** No business logic, no group
scoping, no repository: process-wide pub/sub that the LLM transport, the agent
runtime, tools, memory and guardrails all publish to.

It spent a few hours under ``services/execution/`` during the engine flattening,
and that was wrong in a way worth recording. ``core/llm/transport`` emits events,
so a service-layer bus made ``core`` import ``services`` at module level — the
layering inverted, and the import graph stayed acyclic only by luck.

``event_bus`` is the process-wide singleton. It was ``crewai_event_bus`` until
the engine stopped being a vendored package; the old name is gone, not aliased,
because a compatibility alias here would have outlived everyone who remembered
why.
"""

from .bus import (
    BaseEventListener,
    EventBus,
    EventsBus,
    current_event_context,
    event_bus,
    event_context,
    reset_event_causality,
    set_event_context,
    triggering_event,
)
from .types import (
    A2UISurfaceEvent,
    AgentExecutionCompletedEvent,
    AgentExecutionStartedEvent,
    BaseEvent,
    ContextCompactionEvent,
    CrewBaseEvent,
    CrewKickoffCompletedEvent,
    CrewKickoffStartedEvent,
    FlowBaseEvent,
    FlowFinishedEvent,
    FlowStartedEvent,
    LiteAgentExecutionCompletedEvent,
    LiteAgentExecutionErrorEvent,
    LiteAgentExecutionStartedEvent,
    LLMCallCompletedEvent,
    LLMCallFailedEvent,
    LLMCallStartedEvent,
    LLMCallType,
    LLMEventBase,
    LLMGuardrailCompletedEvent,
    LLMGuardrailFailedEvent,
    LLMGuardrailStartedEvent,
    LLMStreamChunkEvent,
    MemoryBaseEvent,
    MemoryQueryCompletedEvent,
    MemoryQueryFailedEvent,
    MemoryQueryStartedEvent,
    MemoryRetrievalCompletedEvent,
    MemorySaveCompletedEvent,
    MemorySaveFailedEvent,
    MemorySaveStartedEvent,
    TaskCompletedEvent,
    TaskFailedEvent,
    TaskStartedEvent,
    ToolUsageErrorEvent,
    ToolUsageEvent,
    ToolUsageFinishedEvent,
    ToolUsageStartedEvent,
)

__all__ = [
    "AgentExecutionCompletedEvent",
    "AgentExecutionStartedEvent",
    "BaseEvent",
    "BaseEventListener",
    "A2UISurfaceEvent",
    "ContextCompactionEvent",
    "EventsBus",
    "CrewBaseEvent",
    "CrewKickoffCompletedEvent",
    "CrewKickoffStartedEvent",
    "EventBus",
    "FlowBaseEvent",
    "FlowFinishedEvent",
    "FlowStartedEvent",
    "LLMCallCompletedEvent",
    "LLMCallFailedEvent",
    "LLMCallStartedEvent",
    "LLMStreamChunkEvent",
    "LLMCallType",
    "LLMGuardrailCompletedEvent",
    "LLMGuardrailFailedEvent",
    "LLMGuardrailStartedEvent",
    "LLMEventBase",
    "LiteAgentExecutionCompletedEvent",
    "LiteAgentExecutionErrorEvent",
    "LiteAgentExecutionStartedEvent",
    "MemoryBaseEvent",
    "MemoryQueryCompletedEvent",
    "MemoryQueryFailedEvent",
    "MemoryQueryStartedEvent",
    "MemoryRetrievalCompletedEvent",
    "MemorySaveCompletedEvent",
    "MemorySaveFailedEvent",
    "MemorySaveStartedEvent",
    "TaskCompletedEvent",
    "TaskFailedEvent",
    "TaskStartedEvent",
    "ToolUsageErrorEvent",
    "ToolUsageEvent",
    "ToolUsageFinishedEvent",
    "ToolUsageStartedEvent",
    "event_bus",
    "current_event_context",
    "event_context",
    "reset_event_causality",
    "set_event_context",
    "triggering_event",
]
