"""Tools: the capabilities an agent can call, and the factory that builds them.

Flat on purpose. These lived under ``tools/custom/`` — CrewAI's convention for
"tools you wrote yourself", which meant something when the alternative was
CrewAI's own bundled tools and means nothing here: every tool in this package is
ours. The nesting only made every import a word longer.

A tool exposes ``BaseTool`` (name, args schema, ``_run``) because that is what
an agent calls, but nothing about that interface requires an agent to be
running: crew generation, a chat turn or an exported app can construct one and
call it. That is why these live under services rather than inside the engine.

When a tool grows real logic beyond its adapter AND a caller that is not an
agent, the logic earns its own service — see ``services/knowledge`` — and the
tool becomes a thin wrapper over it. Until then, one file per tool is the right
size.


``base`` is the tool CONTRACT — ``BaseTool``, its schema handling, the tool-call
result formatting. It came out of the engine library, where 38 tools in this
package were already subclassing it across a package boundary.

The eager re-exports below are LAZY (PEP 562). ``base`` is imported by the agent
runtime, and a plain re-export made ``import services.tools.base`` drag in the
whole ToolFactory — which imports the concrete tools, which import ``base``.
Importing a leaf module must not execute the package.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:  # import-time only for type checkers, never at runtime
    from .mcp_handler import (
        register_mcp_adapter,
        stop_all_adapters,
        stop_mcp_adapter,
        wrap_mcp_tool,
    )
    from .tool_factory import ToolFactory

_LAZY = {
    "ToolFactory": ".tool_factory",
    "register_mcp_adapter": ".mcp_handler",
    "stop_all_adapters": ".mcp_handler",
    "stop_mcp_adapter": ".mcp_handler",
    "wrap_mcp_tool": ".mcp_handler",
}


def __getattr__(name):
    if name in _LAZY:
        import importlib

        module = importlib.import_module(_LAZY[name], __name__)
        value = getattr(module, name)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "ToolFactory",
    "register_mcp_adapter",
    "stop_all_adapters",
    "stop_mcp_adapter",
    "wrap_mcp_tool",
]
