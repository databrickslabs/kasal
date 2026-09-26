"""Guard for the Databricks host a credential is sent to.

Kasal attaches a server-side credential (the user's OBO token, the group's PAT
or the app's service-principal token) to every Databricks call. A Databricks
credential is only valid on the workspace it was issued for, so the one host
that may ever receive it is the workspace the auth context resolved: the
Databricks Apps installation host, or the process's configured workspace.

Every other source of a host is untrusted and must be checked here first:

- the ``?host=`` override on the workspace listing endpoints (audit C2);
- a team's stored ``workspace_url`` — a workspace admin can set it (audit N1);
- a tool's ``databricks_host`` — an LLM can set it (audit F3a / M4).

``is_trusted_databricks_host`` is deliberately NOT enough on its own: it trusts
every ``*.databricks.com`` host, including a workspace an attacker owns. It is
used only to bootstrap a fresh install that has no workspace configured yet, and
no credential is bound to a host at that point.
"""

from typing import Iterable, Optional
from urllib.parse import urlparse

from src.core.databricks_app import DatabricksAppInstallation
from src.core.exceptions import ForbiddenError
from src.utils.url_security import _extract_hostname, is_trusted_databricks_host

__all__ = [
    "normalize_workspace_host",
    "assert_host_is_configured_workspace",
    "credentialed_workspace_host",
    "assert_credentialed_host",
    "validate_stored_workspace_url",
]


def _require_https(host_or_url: str, subject: str) -> str:
    stripped = (host_or_url or "").strip()
    if "://" in stripped and not stripped.lower().startswith("https://"):
        raise ForbiddenError(detail=f"{subject} must use https")
    return stripped


def normalize_workspace_host(host_or_url: Optional[str]) -> str:
    """Return the bare, lower-cased hostname (with port if any) of a host or URL."""
    if not host_or_url or not isinstance(host_or_url, str):
        return ""
    candidate = host_or_url.strip()
    hostname = _extract_hostname(candidate)
    if not hostname:
        return ""
    if "://" not in candidate:
        candidate = "https://" + candidate
    try:
        port = urlparse(candidate).port
    except ValueError:
        return ""
    if port and port != 443:
        return f"{hostname}:{port}"
    return hostname


def assert_host_is_configured_workspace(
    host: Optional[str],
    configured_hosts: Iterable[Optional[str]],
    *,
    subject: str = "Workspace host override",
) -> str:
    """Raise ``ForbiddenError`` unless ``host`` is one of the configured workspaces.

    The comparison is on normalised hostnames: scheme, case, trailing slash,
    path and the default port do not matter; a different host or port does.
    A non-https scheme is refused outright — the credential must not travel in
    clear text. Returns the normalised host on success.
    """
    target = normalize_workspace_host(_require_https(host or "", subject))
    allowed = {normalize_workspace_host(h) for h in configured_hosts}
    allowed.discard("")
    if not target or target not in allowed:
        raise ForbiddenError(
            detail=f"{subject} is only allowed for the configured Databricks workspace"
        )
    return target


async def credentialed_workspace_host() -> str:
    """The host Kasal's Databricks credentials are bound to, or ``""`` if none.

    Inside Databricks Apps that is the installation host. Elsewhere it is the
    auth context's workspace (``DATABRICKS_HOST``, the SDK profile, or the
    system-level configuration), never a team's own ``workspace_url`` row.
    """
    installation = DatabricksAppInstallation.from_env()
    if installation.hosted and installation.host:
        return str(installation.host)
    from src.utils.databricks_auth import _databricks_auth

    return (await _databricks_auth.get_workspace_url()) or ""


async def assert_credentialed_host(
    host: Optional[str], *, subject: str = "Databricks host"
) -> str:
    """Refuse ``host`` unless it is the workspace the credentials belong to."""
    trusted = await credentialed_workspace_host()
    return assert_host_is_configured_workspace(host, (trusted,), subject=subject)


async def validate_stored_workspace_url(workspace_url: Optional[str]) -> str:
    """Validate a team's ``workspace_url`` before it is stored.

    Returns ``""`` for an empty value (the auth context's host is used) or the
    normalised ``https://host``. A value that names another workspace is refused:
    the listing calls send the caller's credential to it.
    """
    subject = "Databricks workspace URL"
    stripped = _require_https(workspace_url or "", subject)
    if not stripped:
        return ""
    trusted = await credentialed_workspace_host()
    if trusted:
        host = assert_host_is_configured_workspace(
            stripped, (trusted,), subject=subject
        )
    else:
        # Fresh install: no workspace to compare with, and no credential is bound
        # to one yet. Accept only a well-known Databricks domain.
        host = normalize_workspace_host(stripped)
        if not host or not is_trusted_databricks_host(host):
            raise ForbiddenError(detail=f"{subject} must be a Databricks workspace")
    return f"https://{host}"
