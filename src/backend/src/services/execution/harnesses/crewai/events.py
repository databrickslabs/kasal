"""CrewAI's event bus, republished onto Kasal's.

Everything downstream of a run — ``OTelEventBridge`` writing traces, the event
pipe streaming to the live UI, the log writer, the checkpoint recorder — is
subscribed to ``src.core.events.event_bus`` and knows nothing about harnesses.
Rather than teach each of them a second bus, ONE listener here translates.

That is not merely convenient. ``services/execution/CLAUDE.md`` has a standing
rule that exactly one subscriber writes traces, because this codebase already
accumulated three generations of dead listeners. Bridging at the bus keeps that
rule true with a second harness in the tree.

## Why the translation is small

Kasal's event types were derived from CrewAI's and still carry its names and,
for the types that matter here, its FIELDS — ``AgentExecutionStartedEvent``
is ``(agent, task, tools, task_prompt)`` on both sides. So the bridge copies
fields by name and only needs a table of which class maps to which.

## What is NOT bridged, and why that is correct

Most of a run's events never touch CrewAI's bus under this harness:

* **LLM calls** — emitted by Kasal's transport, which the CrewAI harness still
  uses (see ``llm.py``). Bridging them would DOUBLE every llm_call trace.
* **Tools** — emitted by the Kasal tool the adapter wraps.
* **Memory, guardrails, A2UI, plan, checkpoints** — Kasal subsystems that run
  outside whichever runtime is executing.

So the bridge deliberately covers only the lifecycle CrewAI itself owns: agent,
lite-agent, task and crew. ``_SOURCED_FROM_KASAL`` records the rest as a
decision rather than an omission, and a test asserts the two sets together
account for every event the trace map knows about.
"""

from __future__ import annotations

import importlib
from contextlib import contextmanager
from typing import Any, Dict, Iterator, List, Tuple

from src.core import events as kasal_events
from src.core.events import event_bus
from src.core.logger import LoggerManager

logger = LoggerManager.get_instance().crew

#: How long teardown waits for in-flight bridge handlers. CrewAI's own default
#: is 30s, which is a request-length pause on the Chat path; a few seconds
#: covers the real case (handlers that only re-emit) without holding a turn open.
_FLUSH_TIMEOUT_SECONDS = 5.0

#: CrewAI event class name → (its module, the Kasal class to re-emit).
#:
#: Only the lifecycle CrewAI OWNS. Anything Kasal's own subsystems emit is
#: absent on purpose — see the module docstring, and _SOURCED_FROM_KASAL below.
_BRIDGED: Dict[str, Tuple[str, str]] = {
    # Agent lifecycle (crew path)
    "AgentExecutionStartedEvent": (
        "crewai.events.types.agent_events",
        "AgentExecutionStartedEvent",
    ),
    "AgentExecutionCompletedEvent": (
        "crewai.events.types.agent_events",
        "AgentExecutionCompletedEvent",
    ),
    # Lite-agent lifecycle (the Chat path — `Agent.kickoff_async`)
    "LiteAgentExecutionStartedEvent": (
        "crewai.events.types.agent_events",
        "LiteAgentExecutionStartedEvent",
    ),
    "LiteAgentExecutionCompletedEvent": (
        "crewai.events.types.agent_events",
        "LiteAgentExecutionCompletedEvent",
    ),
    "LiteAgentExecutionErrorEvent": (
        "crewai.events.types.agent_events",
        "LiteAgentExecutionErrorEvent",
    ),
    # Task lifecycle
    "TaskStartedEvent": ("crewai.events.types.task_events", "TaskStartedEvent"),
    "TaskCompletedEvent": ("crewai.events.types.task_events", "TaskCompletedEvent"),
    "TaskFailedEvent": ("crewai.events.types.task_events", "TaskFailedEvent"),
    # Crew lifecycle
    "CrewKickoffStartedEvent": (
        "crewai.events.types.crew_events",
        "CrewKickoffStartedEvent",
    ),
    "CrewKickoffCompletedEvent": (
        "crewai.events.types.crew_events",
        "CrewKickoffCompletedEvent",
    ),
    # Flow lifecycle is DELIBERATELY absent — see the note in
    # _SOURCED_FROM_KASAL. CrewAI's flow events do not describe a flow here.
}

#: Kasal event types that reach the bus WITHOUT CrewAI, under either harness.
#:
#: Written down so "why is X not in _BRIDGED?" has an answer that is not "we
#: forgot". A test cross-checks this against the trace map: an event that is in
#: neither set is a hole, and a silent hole in the trace is the failure this
#: whole file exists to avoid.
_SOURCED_FROM_KASAL: frozenset = frozenset(
    {
        # src/core/llm/transport — used by BOTH harnesses (see harnesses/crewai/llm.py)
        "LLMCallStartedEvent",
        "LLMCallCompletedEvent",
        "LLMCallFailedEvent",
        "LLMStreamChunkEvent",
        "LLMReasoningChunkEvent",
        "ContextCompactionEvent",
        # services/tools — the wrapped tool emits; the adapter does not re-emit
        "ToolUsageStartedEvent",
        "ToolUsageFinishedEvent",
        "ToolUsageErrorEvent",
        # services/memory
        "MemorySaveStartedEvent",
        "MemorySaveCompletedEvent",
        "MemorySaveFailedEvent",
        "MemoryQueryStartedEvent",
        "MemoryQueryCompletedEvent",
        "MemoryQueryFailedEvent",
        "MemoryRetrievalCompletedEvent",
        "MemoryRetrievalFailedEvent",
        # services/guardrails
        "LLMGuardrailStartedEvent",
        "LLMGuardrailCompletedEvent",
        "LLMGuardrailFailedEvent",
        # services/flow_builder/runtime/flow.py — the flow layer is Kasal's
        # under BOTH harnesses, so these never need bridging. Bridging them was
        # actively wrong: in CrewAI 1.15 `AgentExecutor` IS a Flow
        # (`class AgentExecutor(Flow[AgentExecutorState], BaseAgentExecutor)`),
        # so every agent TURN kicks off a CrewAI flow and emits
        # FlowStartedEvent(flow_name="AgentExecutor"). Republished on Kasal's
        # bus those read as new flow runs — and `flow_started` opens the
        # OUTERMOST causality scope (see bus._SCOPE_CLOSERS), so each one
        # re-rooted everything after it. One measured flow run recorded six
        # flow_started rows against two flow_completed: one real "DynamicFlow"
        # and five "AgentExecutor".
        "FlowStartedEvent",
        "FlowFinishedEvent",
        # services/a2ui, kernel/agent_plan, execution/checkpointing
        "A2UISurfaceEvent",
        "PlanUpdatedEvent",
        "CheckpointUnitSavedEvent",
        "CrewCheckpointRestoredEvent",
        "TaskCheckpointRestoredEvent",
        "FlowCheckpointSavedEvent",
        # Emitted by neither today; listed so the completeness test is honest
        # about them rather than silently passing.
        "KnowledgeRetrievalStartedEvent",
        "KnowledgeRetrievalCompletedEvent",
        "AgentReasoningStartedEvent",
        "AgentReasoningCompletedEvent",
        "AgentReasoningFailedEvent",
        "MCPConnectionStartedEvent",
        "MCPConnectionCompletedEvent",
        "MCPToolExecutionStartedEvent",
        "MCPToolExecutionCompletedEvent",
        "HumanFeedbackRequestedEvent",
        "HumanFeedbackReceivedEvent",
        "FlowCreatedEvent",
    }
)


def _translate(kasal_cls: type, crew_event: Any) -> Any:
    """Build the Kasal event from the CrewAI one, field by declared field.

    Copies only what the TARGET declares, so a field CrewAI adds in a future
    release cannot break construction, and a field it removes surfaces as a
    validation error naming the field rather than as an empty trace row.
    """
    values: Dict[str, Any] = {}
    for name in getattr(kasal_cls, "model_fields", {}):
        if name == "type":
            continue  # a Literal default; setting it is at best a no-op
        if hasattr(crew_event, name):
            values[name] = getattr(crew_event, name)
    return kasal_cls(**values)


def _pairs() -> List[Tuple[type, type]]:
    """(CrewAI class, Kasal class) for everything bridgeable in this build.

    A CrewAI class this release does not have is SKIPPED with a debug line, not
    raised: the binding must keep working across a dependency bump, and losing
    one event type is a smaller failure than refusing to run at all.
    """
    resolved: List[Tuple[type, type]] = []
    for kasal_name, (module_path, crew_name) in _BRIDGED.items():
        kasal_cls = getattr(kasal_events, kasal_name, None)
        if kasal_cls is None:  # pragma: no cover — _BRIDGED is checked by a test
            logger.debug("No Kasal event type named %s; not bridging", kasal_name)
            continue
        try:
            crew_cls = getattr(importlib.import_module(module_path), crew_name)
        except Exception as e:  # noqa: BLE001
            logger.debug(
                "CrewAI has no %s.%s (%s); not bridging", module_path, crew_name, e
            )
            continue
        resolved.append((crew_cls, kasal_cls))
    return resolved


@contextmanager
def bridge_events() -> Iterator[None]:
    """Republish CrewAI's events on Kasal's bus for the duration of a run.

    Handlers are removed on exit, including when the run raises. The Chat path
    serves many turns in one process, so a handler that outlived its run would
    keep firing against a finished execution — which is how a trace ends up
    attached to the wrong job.
    """
    crewai_bus = importlib.import_module("crewai.events.event_bus").crewai_event_bus

    registered: List[Tuple[type, Any]] = []

    def _make(kasal_cls: type):
        def _handler(source: Any, event: Any) -> None:
            try:
                event_bus.emit(source, _translate(kasal_cls, event))
            except Exception as e:  # noqa: BLE001 — telemetry never fails a run
                logger.warning(
                    "Could not bridge CrewAI %s onto the Kasal bus: %s",
                    type(event).__name__,
                    e,
                )

        return _handler

    for crew_cls, kasal_cls in _pairs():
        handler = _make(kasal_cls)
        crewai_bus.register_handler(crew_cls, handler)
        registered.append((crew_cls, handler))

    logger.info("CrewAI event bridge installed for %d event type(s)", len(registered))
    try:
        yield
    finally:
        # FLUSH BEFORE UNREGISTERING. CrewAI's bus dispatches handlers on a
        # thread pool, fire-and-forget — `emit` returns a Future rather than
        # waiting. So when a run finishes, the bridge handlers for its LAST
        # events are typically still queued. Removing the handlers first drops
        # them, and what gets dropped is exactly the tail of the run: the
        # completion events the timeline ends on.
        #
        # Bounded and never fatal: a hung listener must not hold a chat turn
        # open, and a lost trace row is a smaller failure than a hung request.
        try:
            crewai_bus.flush(timeout=_FLUSH_TIMEOUT_SECONDS)
        except Exception as e:  # noqa: BLE001 — teardown must not raise
            logger.warning(
                "CrewAI event bus did not flush within %ss (%s); trailing trace "
                "rows for this run may be missing",
                _FLUSH_TIMEOUT_SECONDS,
                e,
            )

        for crew_cls, handler in registered:
            try:
                crewai_bus.off(crew_cls, handler)
            except Exception as e:  # noqa: BLE001 — teardown must not raise
                logger.debug("Could not remove CrewAI handler for %s: %s", crew_cls, e)
