"""Turning an external caller into a tenant — in exactly one place.

An MCP client or an A2A agent is, by definition, outside the workspace. Before
any of its requests touch data, it has to become a ``GroupContext``, and every
subsequent query has to be scoped to it. Getting this wrong does not produce a
visible bug: it produces another tenant's crews in the caller's tool list.

That is why it is one module shared by both adapters rather than a helper in
each: the cross-tenant tests are written once, against this, and neither adapter
is allowed its own resolution path.

The chain deliberately mirrors ``src/core/dependencies.py`` — the same headers,
in the same order, that the browser-facing API already trusts. An external
surface that authenticated differently from the rest of the app would be a
second security model to reason about.
"""

import logging
from dataclasses import dataclass
from typing import Optional

from src.utils.user_context import GroupContext

logger = logging.getLogger(__name__)


class ExternalAuthError(Exception):
    """The caller could not be resolved to a tenant.

    Raised rather than returning an empty context, because "no group" must never
    be able to reach a query. Adapters translate this into their own
    ``auth_required`` answer: it is the one external state with no
    ``ExecutionStatus`` behind it, since it describes an invocation that never
    became a run.
    """

    def __init__(self, detail: str, scheme: str = "oauth2"):
        super().__init__(detail)
        self.detail = detail
        #: Which security scheme the caller should satisfy — an A2A caller needs
        #: to know WHICH auth to present, not merely that it failed.
        self.scheme = scheme


@dataclass(frozen=True)
class ExternalCaller:
    """Who is calling, and over what.

    ``origin`` is stamped onto the execution so "who called this crew, over which
    protocol, and what did it cost" is answerable from the first run rather than
    being retrofitted onto an audit trail later.
    """

    group_context: GroupContext
    protocol: str  # "mcp" | "a2a"
    identifier: str  # caller email, agent id, or "unknown"

    @property
    def origin(self) -> str:
        return f"{self.protocol}:{self.identifier}"

    @property
    def group_ids(self) -> list:
        return self.group_context.group_ids or []


async def resolve_caller(
    protocol: str,
    email: Optional[str] = None,
    access_token: Optional[str] = None,
    group_id: Optional[str] = None,
) -> ExternalCaller:
    """Resolve an external caller to a tenant, or refuse.

    Args:
        protocol: "mcp" or "a2a" — recorded as the invocation origin.
        email: The caller's identity, from the same forwarded headers the
            browser API uses.
        access_token: The caller's Databricks/OAuth token, used for OBO.
        group_id: An explicitly requested workspace. Honoured ONLY if the
            resolved user is actually a member — a caller-supplied group id that
            is taken on trust is a one-header cross-tenant read.

    Raises:
        ExternalAuthError: when no identity is present, or the resolved identity
            belongs to no group.
    """
    if not email:
        raise ExternalAuthError(
            "No caller identity. Present a forwarded identity header or a bearer token."
        )

    context = await GroupContext.from_email(
        email=email, access_token=access_token, group_id=group_id
    )

    if not context.group_ids:
        # A user with no group has no data of their own to expose. Refusing is
        # the safe answer; an empty context would sail past every group filter
        # that uses `.in_(group_ids)` and quietly match nothing — or, in a query
        # written without the guard, everything.
        raise ExternalAuthError(f"Caller {email} belongs to no workspace.")

    if group_id and group_id not in context.group_ids:
        # The requested workspace is not one this caller belongs to. Do not
        # silently fall back to their primary group: the caller asked for
        # something specific and getting different data back without being told
        # is worse than an error.
        raise ExternalAuthError(
            f"Caller {email} is not a member of workspace {group_id}."
        )

    logger.info(
        "[external] resolved %s caller %s to groups %s",
        protocol,
        email,
        context.group_ids,
    )
    return ExternalCaller(group_context=context, protocol=protocol, identifier=email)
