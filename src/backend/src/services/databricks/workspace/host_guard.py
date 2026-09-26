"""Guard for caller-supplied Databricks workspace hosts.

The workspace listing endpoints (warehouses, catalogs, schemas) accept an
optional ``host`` so a tool configuration can browse the workspace it names.
The request goes out with the server's resolved credential (OBO token, the
group's PAT or the app's service-principal token), so an unchecked host is a
credential-exfiltration and SSRF primitive: point it at a host you control and
the backend hands you its bearer token.

A Databricks credential is only valid on the workspace it was issued for, so a
legitimate override can only ever name the workspace Kasal is already
configured for. Anything else is refused: this module accepts a host only when
it normalises to the configured workspace host (or the host the auth context
resolved to). ``is_trusted_databricks_host`` is deliberately NOT used — it
trusts every ``*.databricks.com`` host, including a workspace an attacker owns.
"""

from typing import Iterable, Optional
from urllib.parse import urlparse

from src.core.exceptions import ForbiddenError

__all__ = ["normalize_workspace_host", "assert_host_is_configured_workspace"]


def normalize_workspace_host(host_or_url: Optional[str]) -> str:
    """Return the bare, lower-cased hostname (with port if any) of a host or URL."""
    if not host_or_url or not isinstance(host_or_url, str):
        return ""
    candidate = host_or_url.strip()
    if "://" not in candidate:
        candidate = "https://" + candidate
    parsed = urlparse(candidate)
    hostname = (parsed.hostname or "").rstrip(".").lower()
    if not hostname:
        return ""
    try:
        port = parsed.port
    except ValueError:
        return ""
    if port and port != 443:
        return f"{hostname}:{port}"
    return hostname


def assert_host_is_configured_workspace(
    host: str, configured_hosts: Iterable[Optional[str]]
) -> None:
    """Raise ``ForbiddenError`` unless ``host`` is one of the configured workspaces.

    The comparison is on normalised hostnames: scheme, case, trailing slash,
    path and the default port do not matter; a different host or port does.
    A non-https scheme is refused outright — the credential must not travel in
    clear text.
    """
    stripped = (host or "").strip()
    if "://" in stripped and not stripped.lower().startswith("https://"):
        raise ForbiddenError(detail="Workspace host override must use https")

    target = normalize_workspace_host(stripped)
    allowed = {normalize_workspace_host(h) for h in configured_hosts}
    allowed.discard("")
    if not target or target not in allowed:
        raise ForbiddenError(
            detail=(
                "Workspace host override is only allowed for the configured "
                "Databricks workspace"
            )
        )
