"""Hydrate a saved flow through the run's injected async data access."""

from typing import Any


async def hydrate_saved_flow(backend_flow: Any, config: dict[str, Any]) -> None:
    """Keep frontend starting points while supplying the saved graph/listeners.

    Loading failures propagate: starting an empty or partially hydrated flow
    would turn a database failure into an incorrect execution.
    """
    data = await backend_flow.load_flow(repository=backend_flow.repositories["flow"])
    if not data.get("nodes"):
        raise ValueError("The saved flow has no nodes to execute")
    config["nodes"] = data["nodes"]
    config["edges"] = data.get("edges") or []
    saved = data.get("flow_config") or {}
    incoming = config.get("flow_config") or {}
    if "startingPoints" in incoming:
        merged = dict(incoming)
        if not merged.get("listeners") and saved.get("listeners"):
            merged["listeners"] = saved["listeners"]
        config["flow_config"] = merged
    else:
        config["flow_config"] = dict(saved)
