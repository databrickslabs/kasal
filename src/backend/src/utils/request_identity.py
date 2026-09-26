"""Who is calling — read from the request headers in exactly one place.

Kasal never authenticates a user itself. A reverse proxy does, and forwards
the identity it established as request headers. Which proxy that is decides
which headers can be trusted:

- **Databricks Apps** (``DATABRICKS_APP_NAME`` is set by the platform). The Apps
  proxy sets ``X-Forwarded-Email``, ``X-Forwarded-User`` and, with user
  authorization on, ``X-Forwarded-Access-Token``. It does NOT set the
  oauth2-proxy ``X-Auth-Request-*`` family, and custom client headers pass
  through it (the frontend's own ``group_id`` header proves that). So inside
  Apps an ``X-Auth-Request-Email`` header can only have come from the client:
  honouring it — let alone preferring it, as this code used to — let any app
  user impersonate any other (audit C1). Inside Apps only the ``X-Forwarded-*``
  headers are read.
- **Anywhere else** (a deployment fronted by oauth2-proxy with
  ``--set-xauthrequest``, or a local run). The long-standing behaviour is kept:
  ``X-Auth-Request-*`` first, ``X-Forwarded-*`` as the fallback.

Every surface that turns a request into a user — the browser API's
``get_group_context``, the MCP and A2A endpoints, the request-context
middleware — goes through :func:`resolve_request_identity`, so they cannot
disagree about who is calling. A second resolution path is how C1 happened:
the fix landed in one router and not in the four others.

:class:`UntrustedIdentityHeadersMiddleware` additionally deletes the
``X-Auth-Request-*`` headers inside Apps before anything else sees them, so a
reader that bypasses this module (a new router, the rate limiter) cannot be
fooled either.
"""

import logging
import os
from dataclasses import dataclass, field
from typing import Mapping, Optional

from starlette.types import ASGIApp, Receive, Scope, Send

logger = logging.getLogger(__name__)

# Header names, as sent. Starlette header lookups are case-insensitive.
FORWARDED_EMAIL = "X-Forwarded-Email"
FORWARDED_USER = "X-Forwarded-User"
FORWARDED_ACCESS_TOKEN = "X-Forwarded-Access-Token"
AUTH_REQUEST_EMAIL = "X-Auth-Request-Email"
AUTH_REQUEST_USER = "X-Auth-Request-User"
AUTH_REQUEST_ACCESS_TOKEN = "X-Auth-Request-Access-Token"

#: The header family Databricks Apps never sets, as it appears in an ASGI scope.
_UNTRUSTED_IN_APPS_PREFIX = b"x-auth-request-"

SOURCE_DATABRICKS_APPS = "databricks_apps"
SOURCE_OAUTH2_PROXY = "oauth2_proxy"
SOURCE_FORWARDED = "forwarded"
SOURCE_NONE = "none"


def running_in_databricks_apps() -> bool:
    """Whether this process runs behind the Databricks Apps proxy."""
    return bool(os.getenv("DATABRICKS_APP_NAME"))


@dataclass(frozen=True)
class RequestIdentity:
    """The caller as the fronting proxy described it.

    ``access_token`` is excluded from ``repr`` so that logging an identity can
    never write a user's bearer token to a log sink.
    """

    email: Optional[str] = None
    user: Optional[str] = None
    access_token: Optional[str] = field(default=None, repr=False)
    #: Which header family the email came from; see the SOURCE_* constants.
    source: str = SOURCE_NONE

    @property
    def is_authenticated(self) -> bool:
        return bool(self.email)


def _clean(value: Optional[str]) -> Optional[str]:
    """Treat a missing, non-string or blank header as absent."""
    if not isinstance(value, str):
        return None
    value = value.strip()
    return value or None


def resolve_request_identity(
    *,
    forwarded_email: Optional[str] = None,
    forwarded_user: Optional[str] = None,
    forwarded_access_token: Optional[str] = None,
    auth_request_email: Optional[str] = None,
    auth_request_user: Optional[str] = None,
    auth_request_access_token: Optional[str] = None,
    databricks_apps: Optional[bool] = None,
) -> RequestIdentity:
    """Choose the caller's identity from the candidate header values.

    Args:
        forwarded_*: The ``X-Forwarded-*`` values (Databricks Apps proxy).
        auth_request_*: The ``X-Auth-Request-*`` values (oauth2-proxy).
        databricks_apps: Override the deployment-mode detection (tests).

    Returns:
        The resolved identity. ``email`` is None when the request carries no
        trusted identity; callers decide whether that is a 401.

    ``X-Forwarded-User`` is carried as ``user`` but never used as the email:
    in Apps it has the form ``<user id>@<workspace id>``, which contains an
    ``@`` and would otherwise resolve to a fabricated personal workspace.
    """
    apps = running_in_databricks_apps() if databricks_apps is None else databricks_apps
    f_email = _clean(forwarded_email)
    f_user = _clean(forwarded_user)
    f_token = _clean(forwarded_access_token)

    if apps:
        if (
            _clean(auth_request_email)
            or _clean(auth_request_user)
            or _clean(auth_request_access_token)
        ):
            # Never log the values: this is either an attack or a misconfigured
            # client, and in both cases the header content is not ours to keep.
            logger.warning(
                "Ignoring X-Auth-Request-* identity headers inside Databricks "
                "Apps; identity comes from X-Forwarded-* only"
            )
        return RequestIdentity(
            email=f_email,
            user=f_user,
            access_token=f_token,
            source=SOURCE_DATABRICKS_APPS if f_email else SOURCE_NONE,
        )

    a_email = _clean(auth_request_email)
    if a_email:
        source = SOURCE_OAUTH2_PROXY
    elif f_email:
        source = SOURCE_FORWARDED
    else:
        source = SOURCE_NONE
    return RequestIdentity(
        email=a_email or f_email,
        user=_clean(auth_request_user) or f_user,
        access_token=_clean(auth_request_access_token) or f_token,
        source=source,
    )


def identity_from_headers(
    headers: Mapping[str, str], databricks_apps: Optional[bool] = None
) -> RequestIdentity:
    """:func:`resolve_request_identity` over a header mapping.

    Accepts Starlette ``Headers`` (case-insensitive) or a plain dict keyed by
    the canonical or the lower-case header name.
    """

    def get(name: str) -> Optional[str]:
        value = headers.get(name)
        return value if value is not None else headers.get(name.lower())

    return resolve_request_identity(
        forwarded_email=get(FORWARDED_EMAIL),
        forwarded_user=get(FORWARDED_USER),
        forwarded_access_token=get(FORWARDED_ACCESS_TOKEN),
        auth_request_email=get(AUTH_REQUEST_EMAIL),
        auth_request_user=get(AUTH_REQUEST_USER),
        auth_request_access_token=get(AUTH_REQUEST_ACCESS_TOKEN),
        databricks_apps=databricks_apps,
    )


class UntrustedIdentityHeadersMiddleware:
    """Pure ASGI middleware: drop ``X-Auth-Request-*`` inside Databricks Apps.

    Defence in depth behind :func:`resolve_request_identity`. Register it as the
    OUTERMOST middleware so the rate limiter and every dependency see the
    request without the client-supplied family. Pure ASGI (not
    BaseHTTPMiddleware) so SSE responses are not buffered.
    """

    def __init__(self, app: ASGIApp, enabled: Optional[bool] = None) -> None:
        self.app = app
        self.enabled = running_in_databricks_apps() if enabled is None else enabled

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if self.enabled and scope.get("type") in ("http", "websocket"):
            headers = scope.get("headers") or []
            kept = [
                (name, value)
                for name, value in headers
                if not name.lower().startswith(_UNTRUSTED_IN_APPS_PREFIX)
            ]
            if len(kept) != len(headers):
                scope = dict(scope)
                scope["headers"] = kept
        await self.app(scope, receive, send)
