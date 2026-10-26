"""Helpers shared by the tools that call a Databricks workspace.

auth  resolving the credential, and refusing any host it does not belong to
"""

from src.services.tools.databricks_tool_utils.auth import (
    apply_host_override,
    assert_mcp_server_host,
    assert_tool_host,
    resolve_tool_auth,
)

__all__ = [
    "apply_host_override",
    "assert_mcp_server_host",
    "assert_tool_host",
    "resolve_tool_auth",
]
