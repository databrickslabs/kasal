"""
URL / SSRF safety helpers.

Two distinct protections live here:

1. ``is_trusted_databricks_host`` — an *allow-list* used before a Databricks
   credential (OBO user token / PAT / SPN token) is attached as a Bearer header
   to an outbound request. Credentials must only ever be sent to the configured
   workspace host or a well-known Databricks domain, otherwise a
   tenant-registered URL (e.g. an MCP server) becomes a confused deputy that
   exfiltrates the token.

2. ``assert_safe_outbound_url`` — a *deny-list* used for user-supplied outbound
   request targets (e.g. HITL webhooks) that legitimately point at third-party
   hosts. It requires https and rejects loopback / link-local / private (RFC1918)
   / cloud-metadata destinations, including after DNS resolution to defeat
   DNS-rebinding.
"""

import ipaddress
import socket
from typing import Iterable, Optional
from urllib.parse import urlparse

__all__ = [
    "UnsafeUrlError",
    "is_trusted_databricks_host",
    "check_url_structure",
    "assert_safe_outbound_url",
]


class UnsafeUrlError(ValueError):
    """Raised when a URL is not safe to send a server-side request to."""


# Well-known Databricks control/serving domains across clouds.
_DATABRICKS_HOST_SUFFIXES = (
    ".databricks.com",
    ".cloud.databricks.com",
    ".azuredatabricks.net",
    ".gcp.databricks.com",
    ".databricksapps.com",
)

# Hostnames that commonly front cloud instance-metadata services.
_BLOCKED_METADATA_HOSTS = {
    "169.254.169.254",
    "metadata.google.internal",
    "metadata",
    "metadata.goog",
}


def _normalize_host(host: Optional[str]) -> str:
    if not host or not isinstance(host, str):
        return ""
    return host.strip().rstrip(".").lower().split(":")[0]


def _extract_hostname(host_or_url: Optional[str]) -> str:
    """Pull a bare hostname out of either a hostname or a full URL."""
    if not host_or_url or not isinstance(host_or_url, str):
        return ""
    candidate = host_or_url
    if "://" not in candidate:
        candidate = "https://" + candidate
    return _normalize_host(urlparse(candidate).hostname)


def is_trusted_databricks_host(
    host: Optional[str], workspace_host: Optional[str] = None
) -> bool:
    """
    Return True only if ``host`` is a Databricks endpoint we may send a
    Databricks credential to.

    Args:
        host: The target hostname (or URL) of the outbound request.
        workspace_host: The configured workspace URL/host (e.g. from the
            resolved auth context). An exact match is always trusted.
    """
    target = _extract_hostname(host)
    if not target:
        return False

    if workspace_host:
        wh = _extract_hostname(workspace_host)
        if wh and target == wh:
            return True

    return any(
        target == suffix.lstrip(".") or target.endswith(suffix)
        for suffix in _DATABRICKS_HOST_SUFFIXES
    )


def _ip_is_private(value: str) -> bool:
    try:
        ip = ipaddress.ip_address(value)
    except ValueError:
        return False
    return (
        ip.is_private
        or ip.is_loopback
        or ip.is_link_local
        or ip.is_reserved
        or ip.is_multicast
        or ip.is_unspecified
    )


def check_url_structure(url: str, require_https: bool = True) -> str:
    """
    Synchronous structural validation (no DNS): scheme, host presence, blocked
    literal hosts, and private/loopback IP literals. Returns the hostname.

    Raises:
        UnsafeUrlError: on any violation.
    """
    if not url or not isinstance(url, str):
        raise UnsafeUrlError("URL is empty")

    parsed = urlparse(url)
    allowed_schemes = ("https",) if require_https else ("http", "https")
    if parsed.scheme.lower() not in allowed_schemes:
        raise UnsafeUrlError(
            f"URL scheme '{parsed.scheme}' is not allowed (must be "
            f"{' or '.join(allowed_schemes)})"
        )

    host = _normalize_host(parsed.hostname)
    if not host:
        raise UnsafeUrlError("URL has no host")

    if (
        host in _BLOCKED_METADATA_HOSTS
        or host == "localhost"
        or host.endswith(".local")
        or host.endswith(".internal")
    ):
        raise UnsafeUrlError(f"URL targets a blocked host: {host}")

    if _ip_is_private(host):
        raise UnsafeUrlError(f"URL targets a private/loopback address: {host}")

    return host


async def assert_safe_outbound_url(
    url: str,
    *,
    require_https: bool = True,
    extra_blocked_hosts: Optional[Iterable[str]] = None,
) -> str:
    """
    Validate a user-supplied outbound URL for SSRF safety, resolving DNS to
    catch hostnames that point at internal/metadata addresses (anti
    DNS-rebinding). Use this before issuing a server-side request to a target
    a tenant controls (e.g. a HITL webhook).

    Returns the validated URL. Raises UnsafeUrlError on any violation.
    """
    host = check_url_structure(url, require_https=require_https)

    if extra_blocked_hosts and host in {h.lower() for h in extra_blocked_hosts}:
        raise UnsafeUrlError(f"URL targets a blocked host: {host}")

    # Resolve and re-check every returned address (defeats DNS-rebinding).
    parsed = urlparse(url)
    port = parsed.port or (443 if parsed.scheme.lower() == "https" else 80)
    try:
        import asyncio

        loop = asyncio.get_event_loop()
        infos = await loop.getaddrinfo(host, port, proto=socket.IPPROTO_TCP)
    except OSError as exc:
        raise UnsafeUrlError(f"Could not resolve host '{host}': {exc}") from exc

    for info in infos:
        sockaddr = info[4]
        resolved_ip = sockaddr[0]
        if _ip_is_private(resolved_ip):
            raise UnsafeUrlError(
                f"URL host '{host}' resolves to a private/loopback address "
                f"({resolved_ip})"
            )

    return url
