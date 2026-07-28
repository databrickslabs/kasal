"""Kasal CALLING MCP servers — the outbound half.

The configuration service for MCP servers a workspace has attached: which exist,
who may use them, and the credentials to reach them. The tool-side machinery
that turns a configured server into tools an agent can call lives in
``services/tools/`` (``mcp_adapter``, ``mcp_handler``, ``mcp_integration``),
because that is a tool-factory concern rather than a protocol one.

Paired with ``mcp_server`` for the same reason the A2A package is split: the two
directions have opposite trust models, and which side you are on should be
obvious from the path.
"""
