"""
MCP servers: registration, health, and the adapters that reach them.

``service`` owns the server records and their lifecycle. The client side — the
adapter, the session guard, the tool wrappers — is ``services/tools/mcp_*``,
because from an agent's point of view an MCP server is just more tools.
"""
