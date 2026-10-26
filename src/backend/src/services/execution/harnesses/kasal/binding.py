"""The Kasal harness: ``services/execution/runtime/``, first-party.

This binding is a pass-through by design. It exists so that "which harness" has
an answer even when the answer is "the only one there used to be" — every
construction site now goes through a binding, and the Kasal one must behave
EXACTLY as the direct ``runtime.Agent(**kwargs)`` calls it replaced. Anything
clever here would be a behaviour change smuggled into a refactor.

There is no translation table and no dropped kwargs: the kernel's kwargs are
this runtime's kwargs, because the kernel was written against it.
"""

from __future__ import annotations

from contextlib import AbstractContextManager, nullcontext
from typing import Any, Dict, List, Optional

from src.services.execution.harnesses.binding import Capability, HarnessName
from src.services.execution.runtime import (
    Agent,
    Crew,
    LLMGuardrail,
    Process,
    Task,
)

#: Everything. The Kasal runtime is where all of these behaviours were built,
#: so it is the reference the CrewAI binding is measured against.
_CAPABILITIES = frozenset(Capability)


class KasalBinding:
    """The first-party agent runtime."""

    name = HarnessName.KASAL

    def __init__(self) -> None:
        self.version = self._runtime_version()

    @staticmethod
    def _runtime_version() -> str:
        """The app's version — the runtime ships inside it and has no own tag."""
        try:
            from src.config.settings import settings

            return str(getattr(settings, "VERSION", "") or "in-tree")
        except Exception:  # noqa: BLE001 — identity must never fail a run
            return "in-tree"

    def capabilities(self) -> frozenset[Capability]:
        return _CAPABILITIES

    def supports(self, capability: Capability) -> bool:
        return capability in _CAPABILITIES

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    def build_agent(self, **kwargs: Any) -> Any:
        return Agent(**kwargs)

    def build_task(self, **kwargs: Any) -> Any:
        return Task(**kwargs)

    def build_crew(self, **kwargs: Any) -> Any:
        return Crew(**kwargs)

    async def build_llm(
        self,
        model_name: str,
        group_id: str,
        temperature: Optional[float] = None,
    ) -> Any:
        from src.services.llm.manager import LLMManager

        return await LLMManager.configure_kasal_llm(model_name, group_id, temperature)

    def adapt_tools(self, tools: Optional[List[Any]]) -> List[Any]:
        """Identity: the tool factory already produces this runtime's tools."""
        return list(tools or [])

    def guardrail(self, description: str, llm: Any) -> Any:
        return LLMGuardrail(description=description, llm=llm)

    def crew_memory(self, crew: Any) -> Any:
        """The runtime carries the backend on ``Crew.memory`` itself."""
        return getattr(crew, "memory", None)

    def wire_memory(self, crew: Any, provider: Any = None, sink: Any = None) -> None:
        if provider is not None:
            crew.context_providers.append(provider)
        if sink is not None:
            crew.output_sinks.append(sink)

    def process(self, name: str) -> Any:
        return Process(str(name).strip().lower())

    def event_bridge(self) -> AbstractContextManager[None]:
        """Nothing to bridge — the runtime publishes on the bus directly."""
        return nullcontext()

    def describe(self) -> Dict[str, Any]:
        return {
            "name": self.name.value,
            "label": "Kasal",
            "version": self.version,
            "available": True,
            "capabilities": sorted(c.value for c in self.capabilities()),
        }
