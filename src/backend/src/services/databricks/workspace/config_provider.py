"""Reading the workspace's Databricks config without a router above you.

``DatabricksConfigProvider.get()`` replaces a block that appeared verbatim in
seven unrelated files — tool factory, MCP handler, chat, mlflow setup, the embedder
config builder, the crew task adapter:

    async with routed_scoped_session() as session:
        service = DatabricksService(session, group_id=group_id)
        config = await service.get_databricks_config()

Every one of those runs outside an HTTP request (a crew/flow SUBPROCESS, a chat
background task, a tool invoked mid-run), so there is no ``SessionDep`` to inject
and each had to acquire its own session. That is legitimate — but repeating the
wiring seven times meant seven chances to get it wrong, and two were:

* ``tool_factory`` called ``get_databricks_config(group_id=...)``. The method takes
  NO arguments, so it raised ``TypeError``, the enclosing ``try`` swallowed it, and
  the auth check reported "no Databricks config found" for every workspace.
* the same call built ``DatabricksService(session)`` with no ``group_id``, which
  would have read a different tenant's row once the TypeError was fixed.

Both are impossible here: there is one call site, and ``group_id`` is a required
argument rather than something you may forget.

This is a PROVIDER, not a new layer. It still goes
service → repository → session; it just owns the session acquisition and the
construction, the way a FastAPI DI provider does for a router.
"""

import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)


class DatabricksConfigProvider:
    """Session-free access to the active Databricks config."""

    @staticmethod
    async def get(
        group_id: Optional[str] = None,
        user_token: Optional[str] = None,
    ) -> Optional[Any]:
        """The workspace's active Databricks config, or None.

        Args:
            group_id: Workspace to read for. Pass it — omitting it reads the row
                with no group, which is a different tenant's configuration.
            user_token: OBO token, when the caller has one.

        Returns:
            ``DatabricksConfigResponse``, or None when unset or unreadable.

        Never raises: every caller treats a missing config as "not configured"
        and degrades to environment variables, so an exception here would turn a
        soft fallback into a failed run.
        """
        try:
            from src.db.session import routed_scoped_session
            from src.services.databricks.workspace.service import DatabricksService

            async with routed_scoped_session() as session:
                return await DatabricksService(
                    session, group_id=group_id, user_token=user_token
                ).get_databricks_config()
        except Exception as exc:  # noqa: BLE001 - see docstring
            logger.debug(f"[databricks-config] not available: {exc}")
            return None

    @staticmethod
    async def workspace_url(group_id: Optional[str] = None) -> Optional[str]:
        """Just the workspace URL — the field most callers actually wanted."""
        config = await DatabricksConfigProvider.get(group_id=group_id)
        return getattr(config, "workspace_url", None) if config else None
