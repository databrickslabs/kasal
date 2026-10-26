"""The decisions runtime's credential lookup, and the session it runs on.

This used to live in ``src/db/decision_context.py``, which made ``src.db``
import ``src.services`` (an upward dependency) so that a service could have a
session opened for it through a proxy module. The decisions runtime is the
entry point here: it is called from memory, the kernel and chat, inside and
outside a request, and through ``decide_sync``, which runs ``decide`` on an
event loop of its own (``asyncio.run``).

That last case decides the helper. ``routed_scoped_session`` would hand back
the shared SQLite StaticPool connection, which is bound to whichever loop
opened it first, so the sync bridge would fail with "Future attached to a
different loop". ``get_isolated_db_session`` is the sanctioned answer for code
on its own loop (see ``services/CLAUDE.md``, "Which helper to use"): a private
connection on SQLite, and it still follows ``is_lakebase_enabled()``.

One session per lookup: :meth:`DecisionSettingsService.credential` reads the
opt-in row and the key on this same session.

The Databricks connection has no key. Its credential is the workspace's own
Databricks auth, resolved by ``utils.databricks_auth.get_auth_context`` (OBO
from the request's user token, then the workspace's PAT, then the service
principal), the chain every Databricks REST caller uses. That lookup runs
AFTER the isolated session closes, and keeps its own sessions and caches.
"""

from src.db.session import get_isolated_db_session
from src.services.decisions.connection import DATABRICKS
from src.services.decisions.connection import current as current_connection
from src.services.decisions.provider import Credential, WorkspaceCredential
from src.services.decisions.settings import DecisionSettingsService


async def decision_credential(group_id: str) -> Credential | None:
    """The credential the connection spends when the workspace opted in, else None.

    The workspace's API key for TypeSafe and OpenRouter; for Databricks, the
    workspace's host and bearer (:func:`workspace_credential`).
    """
    databricks = current_connection().kind == DATABRICKS
    async with get_isolated_db_session() as session:
        service = DecisionSettingsService(session, group_id)
        if not databricks:
            return await service.credential()
        if not await service.opted_in():
            return None
    return await workspace_credential(group_id)


def _user_token() -> str | None:
    """The request's OBO token, when this task runs on behalf of a user."""
    try:
        from src.utils.user_context import UserContext

        token = UserContext.get_user_token()
        if not token:
            context = UserContext.get_group_context()
            token = getattr(context, "access_token", None) if context else None
        return token if isinstance(token, str) and token else None
    except Exception:  # noqa: BLE001 — no user context (a worker, a unit test)
        return None


async def workspace_credential(group_id: str) -> WorkspaceCredential | None:
    """The workspace's Databricks host and bearer, or None when either is missing.

    OBO first, then the workspace's PAT (looked up under ``group_id`` only, so
    another workspace's token is never used), then the service principal.
    """
    from src.utils.databricks_auth import get_auth_context

    auth = await get_auth_context(user_token=_user_token(), group_id=group_id)
    if auth is None or not auth.token or not auth.workspace_url:
        return None
    return WorkspaceCredential(token=auth.token, host=auth.workspace_url)
