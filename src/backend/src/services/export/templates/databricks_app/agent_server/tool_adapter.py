"""Present a foreign tool object to the Kasal runtime as a ``BaseTool``.

``runtime.Agent.tools`` and ``runtime.Task.tools`` are typed
``list[BaseTool] | None``, so pydantic rejects anything that is not one — with a
validation error that names a type, not a fix.

**Nothing this app ships needs adapting any more.** Search, scraping and image
generation are Kasal's own tools; MCP tools are built as Kasal tools by
``mcp_tools``; the bundled Perplexity and Genie tools always were. During the
migration off CrewAI this wrapped ``crewai_tools`` built-ins and
``MCPServerAdapter`` output; both are gone.

It stays for the case this is now for: an exported project is YOURS to edit, and
a tool you add — a LangChain tool, a plain class with a ``run`` method, one from
another agent library — should work without you first learning Kasal's tool
contract. ``wrapped_tool_names()`` reports anything that needed adapting; for a
stock export it is empty, and that is the point. A non-empty list says a tool is
running through a compatibility path rather than as a first-class tool.
"""

from __future__ import annotations

import logging
from typing import Any, List

from agent_server.kasal_runtime.services.tools.base import BaseTool

logger = logging.getLogger(__name__)

# Names of tools that had to be wrapped this process. Diagnostic only.
_wrapped: List[str] = []


def wrapped_tool_names() -> List[str]:
    """Tools that are not native Kasal tools yet, in the order first seen."""
    return list(_wrapped)


class _AdaptedTool(BaseTool):
    """A foreign tool, wearing Kasal's tool interface."""

    inner: Any = None

    def _run(self, *args: Any, **kwargs: Any) -> Any:
        return _invoke(self.inner, *args, **kwargs)


# Entry points a foreign tool might offer, best first. ``_run`` before ``run``:
# on a CrewAI tool ``run`` adds usage accounting and result formatting we do not
# want applied twice, since Kasal's ``BaseTool.run`` already did it on the way in.
_ENTRY_POINTS = ("_run", "run", "func", "__call__")


def _entry_point(tool: Any):
    """The callable that runs this tool, or None if it has none."""
    for attr in _ENTRY_POINTS:
        fn = getattr(tool, attr, None)
        if callable(fn):
            return fn
    return None


def _invoke(tool: Any, *args: Any, **kwargs: Any) -> Any:
    fn = _entry_point(tool)
    if fn is None:
        raise TypeError(
            f"{type(tool).__name__} exposes no callable "
            f"{'/'.join(_ENTRY_POINTS)} — it cannot be used as a tool"
        )
    return fn(*args, **kwargs)


def _describe(tool: Any) -> tuple:
    name = str(getattr(tool, "name", "") or type(tool).__name__)
    description = str(getattr(tool, "description", "") or f"{name} tool.")
    return name, description


def as_kasal_tool(tool: Any) -> BaseTool:
    """Return ``tool`` unchanged if it is already a Kasal tool, else wrap it.

    Raises ``TypeError`` for an object with no callable body. Checked HERE
    rather than at call time: a tool that fails to wrap is skipped with a
    warning before the crew starts, whereas one that wraps and then raises has
    already been advertised to the model, and the agent burns a turn discovering
    it does not work.
    """
    if isinstance(tool, BaseTool):
        return tool

    if _entry_point(tool) is None:
        raise TypeError(
            f"{type(tool).__name__} exposes no callable "
            f"{'/'.join(_ENTRY_POINTS)} — it cannot be used as a tool"
        )

    name, description = _describe(tool)
    kwargs: dict = {"name": name, "description": description, "inner": tool}

    # Reuse the foreign tool's own argument schema when it has one — it is what
    # the model is shown, and re-deriving it from a ``*args, **kwargs`` wrapper
    # would advertise a tool that accepts anything and validates nothing.
    args_schema = getattr(tool, "args_schema", None)
    if isinstance(args_schema, type):
        kwargs["args_schema"] = args_schema

    if name not in _wrapped:
        _wrapped.append(name)
        logger.debug(f"Adapted foreign tool {name!r} ({type(tool).__name__})")
    return _AdaptedTool(**kwargs)


def as_kasal_tools(tools: Any) -> List[BaseTool]:
    """Adapt a list, dropping (with a warning) anything that cannot be a tool.

    A single unusable tool must not take the whole crew down: the agent is more
    useful missing one capability than not starting at all.
    """
    adapted: List[BaseTool] = []
    for tool in tools or []:
        try:
            adapted.append(as_kasal_tool(tool))
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"Skipping unusable tool {tool!r}: {exc}")
    return adapted
