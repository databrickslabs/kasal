"""MCP, both directions.

``mcp_server`` is Kasal ANSWERING external agents; ``mcp_client`` is Kasal
CALLING MCP servers a workspace has attached. They used to sit apart —
``services/mcp/service.py`` beside a top-level ``services/mcp_server/`` — which
put two halves of one protocol in two places and made "which direction is this?"
a question you answered by reading the file.

The A2A package is split the same way, for the same reason: the two directions
have opposite trust models.
"""
