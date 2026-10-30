"""Event class subscriptions shared by the trace bridge and subprocess pipe."""

# Every event class the bridge subscribes to, as (module_path, class_name).
#
# Module-level, not a local inside register(): it is a constant, and as a local
# it could only be checked by duplicating it in the test file — which is exactly
# what happened, and the copy drifted out of sync with this one.
#
# Every entry here must actually resolve. Twelve did not: Knowledge*, AgentReasoning*,
# MCP* and HumanFeedback* were crewAI event names the kasal engine never
# implemented — nothing defines them, nothing emits them. They made register()
# log a "missing" WARNING twelve times on every single run, which is how a
# warning that means "a whole event type is invisible in every trace" stops
# being read at all.
_EVENT_CLASSES = [
    ("src.core.events.types", "DecisionEvaluatedEvent"),
    # Crew lifecycle
    ("src.core.events", "CrewKickoffStartedEvent"),
    ("src.core.events", "CrewKickoffCompletedEvent"),
    ("src.core.events", "CrewCheckpointRestoredEvent"),
    ("src.core.events", "FlowCheckpointSavedEvent"),
    ("src.core.events", "CheckpointUnitSavedEvent"),
    ("src.core.events", "PlanUpdatedEvent"),
    # Agent execution
    ("src.core.events", "AgentExecutionStartedEvent"),
    ("src.core.events", "AgentExecutionCompletedEvent"),
    # Task lifecycle
    ("src.core.events", "TaskStartedEvent"),
    ("src.core.events", "TaskCompletedEvent"),
    ("src.core.events", "TaskCheckpointRestoredEvent"),
    ("src.core.events", "TaskFailedEvent"),
    # Tool usage
    ("src.core.events", "ToolUsageStartedEvent"),
    ("src.core.events", "ToolUsageFinishedEvent"),
    ("src.core.events", "ToolUsageErrorEvent"),
    # LLM calls
    ("src.core.events", "LLMCallStartedEvent"),
    ("src.core.events", "LLMCallCompletedEvent"),
    ("src.core.events", "LLMCallFailedEvent"),
    ("src.core.events", "LLMStreamChunkEvent"),
    # Subscribed so the event pipe can forward it to the live UI; the bridge
    # itself skips writing a span (see _SKIP_EVENTS) — one per delta is noise,
    # and LLMCallCompletedEvent.reasoning already records it once per call.
    ("src.core.events", "LLMReasoningChunkEvent"),
    # Memory
    ("src.core.events", "MemorySaveStartedEvent"),
    ("src.core.events", "MemorySaveCompletedEvent"),
    ("src.core.events", "MemorySaveFailedEvent"),
    ("src.core.events", "MemoryQueryStartedEvent"),
    ("src.core.events", "MemoryQueryCompletedEvent"),
    ("src.core.events", "MemoryQueryFailedEvent"),
    ("src.core.events", "MemoryRetrievalCompletedEvent"),
    # Knowledge
    # Reasoning
    # Guardrails
    ("src.core.events", "LLMGuardrailStartedEvent"),
    ("src.core.events", "LLMGuardrailCompletedEvent"),
    ("src.core.events", "LLMGuardrailFailedEvent"),
    # Flow. No FlowCreatedEvent: the engine's Flow has no "created"
    # lifecycle point (only kickoff start/finish), so subscribing to one
    # would be permanently dead wiring — which is exactly how flow spans
    # went missing in the first place.
    ("src.core.events", "FlowStartedEvent"),
    ("src.core.events", "FlowFinishedEvent"),
    # MCP
    # HITL
    # A2UI surface composition. Emitted for every outcome, including the
    # gates that decline — a chat turn that produced no surface is the
    # case people ask about, and it left no trace at all.
    ("src.core.events", "A2UISurfaceEvent"),
    # Context compaction. Lossy, and its absence from this list is how
    # a whole event type stays invisible: the map below is not enough,
    # the bridge only ever sees what it SUBSCRIBES to here.
    ("src.core.events", "ContextCompactionEvent"),
]
