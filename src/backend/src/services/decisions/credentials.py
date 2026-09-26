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
"""

from src.db.session import get_isolated_db_session
from src.services.decisions.settings import DecisionSettingsService


async def decision_credential(group_id: str) -> str | None:
    """The workspace's Jev API key when it has opted in, else None."""
    async with get_isolated_db_session() as session:
        return await DecisionSettingsService(session, group_id).credential()
