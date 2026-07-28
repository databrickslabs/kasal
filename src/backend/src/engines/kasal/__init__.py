"""Kasal engine.

Exports are LAZY. Importing any submodule initialises this package first, and
eagerly importing the hub here made that pull the entire engine — the hub
imports ``CrewPreparation``, which imports back into the package, so touching
``paths.crew.crew_preparation`` before anything else warmed the package raised
"cannot import name 'CrewPreparation' from partially initialized module".

The cycle was always here; it was hidden because ``tools/`` lived under this
package, so importing a tool warmed the whole engine before anything could hit
it. Moving tools to ``services/`` removed that accident and made the cycle
reachable, which is a good argument for not having had it.

PEP 562 module ``__getattr__`` keeps ``from src.engines.kasal import
KasalEngineService`` working for every existing caller, while a submodule
import no longer drags the hub in behind it.
"""

from typing import Any

__all__ = [
    "KasalEngineService",
    "KasalFlowService",
    "BackendFlow",
    "FlowRunnerService",
]


def __getattr__(name: str) -> Any:
    if name == "KasalEngineService":
        from src.engines.kasal.kasal_engine_service import KasalEngineService

        return KasalEngineService
    if name == "KasalFlowService":
        from src.services.flow_builder.kasal_flow_service import KasalFlowService

        return KasalFlowService
    if name in ("BackendFlow", "FlowRunnerService"):
        from src.engines.kasal.paths import flow

        return getattr(flow, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
