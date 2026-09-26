"""The one way a tool resolves a Databricks credential and the host it goes to.

Five tools (Genie space generator, dashboard creator, PowerBI visual→UCMV
mapper, UCMV Genie config generator, metric view deployer) each carried an
identical ``_authenticate(host_override)`` that assigned a ``databricks_host``
straight onto ``auth.workspace_url``. That field is in the LLM-facing args
schema, so a prompt-injected agent could point the resolved OBO/PAT/SPN token at
a host it controls (audit F3a / N1). The Jobs tool had the same hole through its
tool configuration (audit M4).

Every override now goes through ``host_guard``: it may only name the workspace
the credential was issued for, so at most it re-spells that host.
"""

from typing import TYPE_CHECKING, Optional

from src.services.databricks.workspace.host_guard import (
    assert_credentialed_host,
    assert_host_is_configured_workspace,
)
from src.services.tools.async_bridge import run_async_with_context

if TYPE_CHECKING:
    from src.utils.databricks_auth import AuthContext

__all__ = ["apply_host_override", "resolve_tool_auth", "assert_tool_host"]

_SUBJECT = "Tool databricks_host"


def apply_host_override(
    auth: Optional["AuthContext"], host_override: Optional[str]
) -> Optional["AuthContext"]:
    """Point ``auth`` at ``host_override`` only if it is the auth context's host.

    Raises ``ForbiddenError`` for any other host; the callers already turn an
    exception from ``_authenticate`` into an "Authentication failed" result.
    """
    if auth is None or not host_override:
        return auth
    host = assert_host_is_configured_workspace(
        host_override, (auth.workspace_url,), subject=_SUBJECT
    )
    auth.workspace_url = f"https://{host}"
    return auth


def resolve_tool_auth(
    host_override: Optional[str] = None, timeout: float = 30
) -> Optional["AuthContext"]:
    """Resolve the auth context (OBO → PAT → SPN) from synchronous tool code.

    Safe inside a running event loop (CrewAI always has one), and it keeps the
    caller's ContextVars, so the group's PAT is looked up for the right group.
    """
    from src.utils.databricks_auth import get_auth_context

    auth = run_async_with_context(get_auth_context(), timeout=timeout)
    return apply_host_override(auth, host_override)


async def assert_tool_host(host: Optional[str]) -> str:
    """Refuse a tool-configured host unless the credentials belong to it."""
    return await assert_credentialed_host(host, subject=_SUBJECT)
