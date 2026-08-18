"""What a harness has to provide, and what it may not be able to do.

A "harness" here is the **agent runtime** that actually executes a task: Kasal's
own (``services/execution/runtime/``) or CrewAI (the ``crewai`` package). It is
NOT the dispatch machinery — ``KasalEngineService`` stays the one hub, the three
paths stay ``chat/`` / ``agent_builder/`` / ``flow_builder/``, and log capture,
status, cancel, history, broadcast and checkpoint lifecycle are shared by both
harnesses because none of them cares which runtime ran the task.

## Why the seam is at construction, not at the service

``build_agent(**kwargs)`` rather than ``Agent``-the-class. The kernel assembles
one kwargs dict per agent/task/crew; the two runtimes accept overlapping but not
identical sets. Handing the whole dict to the binding keeps the translation in
ONE readable table per harness instead of scattering ``if harness == ...`` across
the twenty modules that build things — and it makes "what did CrewAI not
support?" something you can read, rather than reconstruct.

Every kwarg a binding cannot honour is DROPPED LOUDLY (see ``DroppedKwargs``).
A silent drop is how a run "works" at settings nobody chose; the temperature
column that no code path read for months is the same failure in a different
place.

## Capabilities are declared, not discovered

A binding states what it can do. Callers ask before offering — the API surfaces
the active harness's set so the UI can grey out resume on a harness that has no
checkpoint story, instead of letting someone press a button that raises. This is
also what lets a feature land on one harness first and say so, rather than
pretending parity that does not exist.
"""

from __future__ import annotations

from contextlib import AbstractContextManager
from enum import Enum
from typing import Any, Dict, List, Optional, Protocol, runtime_checkable


class HarnessName(str, Enum):
    """The harnesses this platform can run. The wire values are stable.

    These strings are persisted (``engine_config.config_value``,
    ``execution_history.harness``) and travel into subprocesses, so they
    are a data format, not a display label.
    """

    KASAL = "kasal"
    CREWAI = "crewai"

    def __str__(self) -> str:
        return self.value


class Capability(str, Enum):
    """Things a harness may or may not be able to do.

    Named for the user-visible behaviour, not the implementation, because the
    frontend renders these directly.
    """

    #: Crash-resume from a recorded checkpoint (``execution/checkpointing/``).
    CHECKPOINT_RESUME = "checkpoint_resume"
    #: The human-in-the-loop gate in front of a tool call
    #: (``kernel/tool_approval.py``).
    TOOL_APPROVAL = "tool_approval"
    #: Reusing a recorded tool result instead of paying for the call again
    #: (``kernel/tool_replay.py``).
    TOOL_REPLAY = "tool_replay"
    #: The agent's own plan for the task it was given (``runtime/plan.py``).
    AGENT_PLAN = "agent_plan"
    #: Memory recall wired into task-context assembly.
    CONTEXT_PROVIDERS = "context_providers"
    #: Memory persistence wired into task completion.
    OUTPUT_SINKS = "output_sinks"
    #: A wall-clock ceiling for the whole run, not just per call.
    RUN_DEADLINE = "run_deadline"
    #: ``Process.hierarchical`` with a manager agent.
    HIERARCHICAL = "hierarchical"
    #: The Flow Builder path.
    FLOW = "flow"
    #: Exporting a workflow as a standalone Databricks App.
    EXPORT = "export"


class DroppedKwargs:
    """Collects what a binding could not honour, so it can be reported once.

    A per-build list rather than a log call per kwarg: an agent with six
    unsupported settings should produce one line naming all six, not six lines
    interleaved with everyone else's.
    """

    def __init__(self, subject: str) -> None:
        self.subject = subject
        self.entries: List[str] = []

    def drop(self, key: str, reason: str) -> None:
        self.entries.append(f"{key} ({reason})")

    def __bool__(self) -> bool:
        return bool(self.entries)

    def summary(self) -> str:
        return f"{self.subject}: dropped {', '.join(self.entries)}"


class HarnessUnavailableError(RuntimeError):
    """The requested harness cannot run here, with a reason a human can act on.

    Raised at RESOLUTION time — before an execution row reaches RUNNING — so the
    failure reads as "CrewAI is not installed" rather than as a crew that died
    halfway through for no stated reason.
    """


@runtime_checkable
class HarnessBinding(Protocol):
    """One agent runtime, behind a stable construction surface."""

    #: Which harness this is. Matches the persisted value.
    name: HarnessName
    #: The runtime's own version, for traces and the config API.
    version: str

    def capabilities(self) -> frozenset[Capability]:
        """What this harness can do here. Callers ask before offering."""
        ...

    def supports(self, capability: Capability) -> bool:
        """Convenience over :meth:`capabilities`."""
        ...

    # ------------------------------------------------------------------
    # Construction. Each takes the kwargs dict the kernel already assembles.
    # ------------------------------------------------------------------

    def build_agent(self, **kwargs: Any) -> Any:
        """An agent. ``kwargs`` is what ``kernel/agent_builder`` produced."""
        ...

    def build_task(self, **kwargs: Any) -> Any:
        """A task. ``kwargs`` is what ``kernel/task_builder`` produced."""
        ...

    def build_crew(self, **kwargs: Any) -> Any:
        """A crew. ``kwargs`` is what ``config/crew_config_builder`` produced."""
        ...

    async def build_llm(
        self,
        model_name: str,
        group_id: str,
        temperature: Optional[float] = None,
    ) -> Any:
        """The LLM an agent runs on, scoped to ``group_id``.

        Both harnesses resolve the model through ``ModelConfigService`` and run
        the call through ``src.core.llm.transport`` — see
        ``harnesses/crewai/llm.py`` for why the CrewAI harness does not use
        CrewAI's own LLM class. ``group_id`` is required: an unscoped model is a
        multi-tenant isolation hole, not a fallback.
        """
        ...

    def adapt_tools(self, tools: Optional[List[Any]]) -> List[Any]:
        """Kasal ``BaseTool`` instances in the shape this harness calls.

        The 38 first-party tools are never ported per harness; they are wrapped.
        """
        ...

    def guardrail(self, description: str, llm: Any) -> Any:
        """An LLM-backed guardrail this harness's tasks accept."""
        ...

    def crew_memory(self, crew: Any) -> Any:
        """The Kasal memory backend attached to this crew, or None.

        Asked rather than read off ``crew.memory``, because that field means
        different things per harness: the Kasal runtime carries the backend
        there, while the CrewAI binding forces it False so CrewAI's own store
        never initialises and keeps the backend elsewhere.
        """
        ...

    def wire_memory(self, crew: Any, provider: Any = None, sink: Any = None) -> None:
        """Attach runtime recall and per-task persistence to a built crew.

        A crew configured with memory but never wired is silently memory-less —
        it neither reads nor writes, with no error to show for it.
        """
        ...

    def process(self, name: str) -> Any:
        """``"sequential"`` / ``"hierarchical"`` as this harness's enum."""
        ...

    def event_bridge(self) -> AbstractContextManager[None]:
        """Make this harness's run publish on ``src.core.events.event_bus``.

        For Kasal this is a no-op: the runtime already publishes there. For
        CrewAI it installs a listener on ``crewai_event_bus`` that republishes.
        Either way, everything downstream — ``OTelEventBridge``, the event pipe,
        the log writer, the checkpoint recorder — is unchanged and stays the
        single subscriber it was.
        """
        ...

    def describe(self) -> Dict[str, Any]:
        """Harness identity for the config API, traces and run rows."""
        ...
