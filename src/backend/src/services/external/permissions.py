"""Role checks for external callers — the workspace's own roles, nothing new.

``identity.py`` answers "which workspace's data is this?". This answers the
second question: within that workspace, what is this caller allowed to do?

There is no separate permission model here, and deliberately so. A caller
resolves to a ``GroupContext`` exactly as a browser request does, carrying the
role they were given in that workspace, and the check is
``core.permissions.check_role_in_context`` — the same function every router
uses, with the same role lists. An admin of a workspace can create crews from an
external agent for precisely the reason they can create them in the UI: they are
an admin of that workspace.

The only thing this module adds is a single place for both protocols to call it.
A capability that is admin-only over MCP and open over A2A is a
privilege-escalation bug that looks correct in each file on its own.

    admin, editor      may create and modify
    admin, editor, operator   may run published capabilities and read state
"""

import logging
from typing import List

from src.core.permissions import check_role_in_context, get_effective_role
from src.services.external.identity import ExternalCaller

logger = logging.getLogger(__name__)

#: Who may create or change crews, flows and agents. Mirrors crews_router, which
#: guards its write endpoints with exactly this list.
AUTHOR_ROLES: List[str] = ["admin", "editor"]

#: Who may run a published capability and read its state — every member of the
#: workspace. Operators exist to run things.
RUN_ROLES: List[str] = ["admin", "editor", "operator"]


class ExternalPermissionError(Exception):
    """The caller is in the workspace, but their role does not allow this.

    Distinct from ``ExternalAuthError``, which means "we do not know who you
    are". Collapsing the two would tell an already-authenticated caller to
    authenticate, and it would retry with the same credentials forever.
    """

    def __init__(self, detail: str, required_roles: List[str], actual_role: str):
        super().__init__(detail)
        self.detail = detail
        self.required_roles = required_roles
        #: Included so a calling agent can tell a permission boundary from a
        #: misconfigured token. "Forbidden" alone is not actionable.
        self.actual_role = actual_role


def require_role(caller: ExternalCaller, allowed_roles: List[str]) -> None:
    """Raise unless the caller holds one of ``allowed_roles`` in their workspace.

    Raising rather than returning False is deliberate: a check whose result can
    be ignored eventually is. The adapters translate the exception into their
    own refusal.
    """
    if check_role_in_context(caller.group_context, allowed_roles):
        return

    role = (get_effective_role(caller.group_context) or "").lower()
    logger.warning(
        "[external] %s refused: role %r, needs one of %s",
        caller.origin,
        role or "none",
        allowed_roles,
    )
    raise ExternalPermissionError(
        (
            f"This action requires the {' or '.join(allowed_roles)} role in this "
            f"workspace; you have {role or 'no role'}."
        ),
        required_roles=allowed_roles,
        actual_role=role,
    )
