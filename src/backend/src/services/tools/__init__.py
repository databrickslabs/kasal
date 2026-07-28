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
"""

from .mcp_handler import (
    register_mcp_adapter,
    stop_all_adapters,
    stop_mcp_adapter,
    wrap_mcp_tool,
)
from .tool_factory import ToolFactory

__all__ = [
    "ToolFactory",
    "register_mcp_adapter",
    "stop_all_adapters",
    "stop_mcp_adapter",
    "wrap_mcp_tool",
]
