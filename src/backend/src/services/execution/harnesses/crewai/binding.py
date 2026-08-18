"""The CrewAI harness binding.

Assembles the four bridges into the ``HarnessBinding`` surface:

* ``llm.py`` — CrewAI's ``BaseLLM``, forwarding to Kasal's transport
* ``tools.py`` — Kasal's 38 tools, in CrewAI's shape
* ``events.py`` — CrewAI's bus, republished onto Kasal's
* ``build.py`` — the kernel's kwargs, translated

What it declares it CANNOT do is as much of the contract as what it can. See
``capabilities`` below: a capability absent here makes the API report it and the
UI disable it, which is how a feature can land on one harness first without the
product implying parity it does not have.
"""

from __future__ import annotations

from contextlib import AbstractContextManager
from typing import Any, Dict, List, Optional

from src.core.logger import LoggerManager
from src.services.execution.harnesses.binding import Capability, HarnessName
from src.services.execution.harnesses.crewai import build as crew_build
from src.services.execution.harnesses.crewai.availability import require_crewai
from src.services.execution.harnesses.crewai.events import bridge_events
from src.services.execution.harnesses.crewai.tools import adapt_tools

logger = LoggerManager.get_instance().crew

#: What this harness supports TODAY.
#:
#: Present because the bridges give them for free:
#:   RUN_DEADLINE  — the transport owns the deadline, and both harnesses use it
#:   HIERARCHICAL  — crewai.Process.hierarchical with a manager agent
#:   TOOL_APPROVAL / TOOL_REPLAY / AGENT_PLAN — the wrapped Kasal tool applies
#:                   the same policy whichever runtime calls it
#:
#: Absent, and each is a phase of its own rather than an oversight:
#:   CONTEXT_PROVIDERS / OUTPUT_SINKS — memory recall and per-task persistence,
#:                   through the crew subclass in `memory.py`
#:   CHECKPOINT_RESUME — writing needed nothing (the event bridge feeds the
#:                   shared recorder); reading is `checkpoint.py`
#:
#:   FLOW              — see below; the Flow Builder path runs on this harness
#:
#: Absent, deliberately:
#:   EXPORT            — the exported Databricks App VENDORS the Kasal runtime
#:                   (services/export/runtime_vendor.py) so it runs standalone
#:                   with no third-party agent framework. Shipping CrewAI into
#:                   exported apps is a separate project with its own licensing
#:                   and bundle-size questions, not a gap in this one.
#:
#: A word on FLOW, because "does CrewAI run flows?" has a subtler answer than
#: yes or no. The Flow Builder's ORCHESTRATOR — routing, conditions, HITL
#: gates, per-crew checkpoints, conversational state — is Kasal's own
#: (`flow_builder/runtime/flow.py`) and stays that way under both harnesses. What
#: the harness setting selects is the AGENT RUNTIME, and every crew a flow
#: composes is built through this binding, so a flow running under CrewAI
#: executes its agents and crews on CrewAI.
#:
#: Swapping the orchestrator for `crewai.flow.Flow` was considered and NOT
#: taken: it would re-implement routing, HITL and checkpointing against a
#: second set of primitives, for no behaviour a user could observe — the agents
#: already run on the chosen harness either way.
_CAPABILITIES = frozenset(
    {
        Capability.TOOL_APPROVAL,
        Capability.TOOL_REPLAY,
        Capability.AGENT_PLAN,
        Capability.RUN_DEADLINE,
        Capability.HIERARCHICAL,
        Capability.CONTEXT_PROVIDERS,
        Capability.OUTPUT_SINKS,
        Capability.CHECKPOINT_RESUME,
        Capability.FLOW,
    }
)


class CrewAIBinding:
    """CrewAI, as a runtime this platform can be switched to."""

    name = HarnessName.CREWAI

    def __init__(self) -> None:
        # Raises HarnessUnavailableError when crewai cannot be imported, which
        # is what makes `binding_for` fail at RESOLUTION time rather than
        # halfway through a run.
        _, self.version = require_crewai()

    def capabilities(self) -> frozenset:
        return _CAPABILITIES

    def supports(self, capability: Capability) -> bool:
        return capability in _CAPABILITIES

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    def build_agent(self, **kwargs: Any) -> Any:
        return crew_build.build_agent(**kwargs)

    def build_task(self, **kwargs: Any) -> Any:
        return crew_build.build_task(**kwargs)

    def build_crew(self, **kwargs: Any) -> Any:
        return crew_build.build_crew(**kwargs)

    async def build_llm(
        self,
        model_name: str,
        group_id: str,
        temperature: Optional[float] = None,
    ) -> Any:
        """The SAME transport object the Kasal harness builds.

        Deliberately not wrapped here. The kernel stamps reasoning effort,
        thinking budget and per-agent overrides onto whatever this returns, and
        those attributes live on the transport object; wrapping now would put
        them on the wrapper and leave the object that makes the request
        untouched. ``build_agent`` wraps at the last moment instead.
        """
        from src.services.llm.manager import LLMManager

        return await LLMManager.configure_kasal_llm(model_name, group_id, temperature)

    def adapt_tools(self, tools: Optional[List[Any]]) -> List[Any]:
        return adapt_tools(tools)

    def guardrail(self, description: str, llm: Any) -> Any:
        """CrewAI's own ``LLMGuardrail``, over the shared transport."""
        from src.services.execution.harnesses.crewai.availability import crewai_symbols
        from src.services.execution.harnesses.crewai.llm import build_kasal_backed_llm

        symbols = crewai_symbols()
        if llm is not None and not isinstance(llm, (str, symbols["BaseLLM"])):
            llm = build_kasal_backed_llm(llm)
        return symbols["LLMGuardrail"](description=description, llm=llm)

    def crew_memory(self, crew: Any) -> Any:
        """The Kasal memory backend for this crew.

        Not ``crew.memory``: that field is forced False so CrewAI's own
        chromadb/lancedb store never initialises. See ``memory.py``.
        """
        from src.services.execution.harnesses.crewai.memory import crew_memory

        return crew_memory(crew)

    def wire_memory(self, crew: Any, provider: Any = None, sink: Any = None) -> None:
        """Attach runtime recall and per-task persistence to a built crew."""
        from src.services.execution.harnesses.crewai.memory import wire_memory

        wire_memory(crew, provider=provider, sink=sink)

    def process(self, name: str) -> Any:
        from src.services.execution.harnesses.crewai.availability import crewai_symbols

        return crewai_symbols()["Process"](str(name).strip().lower())

    def event_bridge(self) -> AbstractContextManager[None]:
        """Everything this harness needs installed for the life of one run.

        Two things, both driven by CrewAI's bus and both scoped to the run:
        the event bridge onto Kasal's bus, and per-turn deadline enforcement.
        They share this entry point because they share a lifetime — and because
        one install site is one thing to remember, where the bridge was already
        missed on the crew and flow paths once.
        """
        from contextlib import ExitStack, contextmanager

        from src.services.execution.harnesses.crewai.deadline import (
            enforce_turn_deadlines,
        )

        @contextmanager
        def _run_scope():
            with ExitStack() as stack:
                stack.enter_context(bridge_events())
                stack.enter_context(enforce_turn_deadlines())
                yield

        return _run_scope()

    def describe(self) -> Dict[str, Any]:
        return {
            "name": self.name.value,
            "label": "CrewAI",
            "version": self.version,
            "available": True,
            "capabilities": sorted(c.value for c in self.capabilities()),
        }
