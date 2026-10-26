"""The harness layer: which agent runtime executes a run.

``active_harness()`` is the one call the rest of the codebase makes. Everything
else here is registry and reporting.

Two rules this package keeps:

* **It never reads the database.** The operator's setting is resolved once, by
  the layer that creates the execution row; ``selection`` only resolves an
  already-decided value. See ``selection`` for why that split matters.
* **It never imports a path package** (``chat/``, ``agent_builder/``,
  ``flow_builder/``). It is a capability package in the sense
  ``services/CLAUDE.md`` uses the word: usable from a chat turn, a crew
  subprocess, or a flow, without dragging an orchestrator in behind it.

Bindings are built lazily and cached per process. The CrewAI one especially:
``import crewai`` costs ~2.6s and pulls chromadb, so an installation running the
Kasal harness must never pay for it.
"""

from __future__ import annotations

from typing import Any, Dict, List, Union

from src.core.logger import LoggerManager
from src.services.execution.harnesses.binding import (
    Capability,
    DroppedKwargs,
    HarnessBinding,
    HarnessName,
    HarnessUnavailableError,
)
from src.services.execution.harnesses.selection import (
    DEFAULT_HARNESS,
    HARNESS_CONFIG_KEY,
    HARNESS_ENV_VAR,
    active_name,
    bind,
    coerce,
    set_process_default,
)

logger = LoggerManager.get_instance().crew

_cache: Dict[HarnessName, HarnessBinding] = {}


def _construct(name: HarnessName) -> HarnessBinding:
    """Import and build one binding. Imports are local and lazy on purpose."""
    if name is HarnessName.KASAL:
        from src.services.execution.harnesses.kasal import KasalBinding

        return KasalBinding()
    if name is HarnessName.CREWAI:
        from src.services.execution.harnesses.crewai import CrewAIBinding

        return CrewAIBinding()
    raise HarnessUnavailableError(f"No binding is registered for harness {name!r}")


def binding_for(name: Union[str, HarnessName, None]) -> HarnessBinding:
    """The binding for ``name``, cached.

    Raises ``HarnessUnavailableError`` when the harness exists but cannot run here
    — CrewAI not installed, say. That is deliberately loud: it is raised at
    resolution time, before an execution reaches RUNNING, so the operator reads
    "CrewAI is not installed" instead of watching a crew die mid-run.
    """
    resolved = coerce(name) or DEFAULT_HARNESS
    cached = _cache.get(resolved)
    if cached is not None:
        return cached
    binding = _construct(resolved)
    _cache[resolved] = binding
    return binding


def active_harness() -> HarnessBinding:
    """The harness in force here. The call the rest of the codebase makes."""
    return binding_for(active_name())


def describe_harnesses() -> List[Dict[str, Any]]:
    """Every harness and whether it can run here — for the configuration API.

    Never raises: a harness that cannot be built is REPORTED as unavailable
    with its reason, because "why is CrewAI greyed out?" is the question this
    endpoint exists to answer.
    """
    out: List[Dict[str, Any]] = []
    for name in HarnessName:
        try:
            out.append(binding_for(name).describe())
        except Exception as e:  # noqa: BLE001 — reporting must survive anything
            out.append(
                {
                    "name": name.value,
                    "label": name.value.title(),
                    "version": None,
                    "available": False,
                    "unavailable_reason": str(e),
                    "capabilities": [],
                }
            )
    return out


def reset_for_tests() -> None:
    """Drop cached bindings alongside ``selection.reset_for_tests``."""
    from src.services.execution.harnesses import selection

    _cache.clear()
    selection.reset_for_tests()


__all__ = [
    "Capability",
    "DEFAULT_HARNESS",
    "DroppedKwargs",
    "HARNESS_CONFIG_KEY",
    "HARNESS_ENV_VAR",
    "HarnessBinding",
    "HarnessName",
    "HarnessUnavailableError",
    "active_harness",
    "active_name",
    "bind",
    "binding_for",
    "coerce",
    "describe_harnesses",
    "reset_for_tests",
    "set_process_default",
]
