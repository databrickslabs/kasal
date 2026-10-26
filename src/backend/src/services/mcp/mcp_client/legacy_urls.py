"""Moving MCP registrations off the legacy Databricks external-MCP proxy.

Registrations made before UC MCP Services existed point at
``/api/2.0/mcp/external/{connection}``. When the workspace's UC MCP Services
listing (read with the caller's credentials) returns a service of the same
name, the registration is re-pointed at its ``/ai-gateway/mcp-services/``
URL.

This used to run as a side effect of ``GET /mcp/databricks/available`` and
loaded every tenant's rows to filter them in Python. It is now two explicit,
tenant-scoped operations: the GET only *counts* what would change, and
``POST /mcp/databricks/migrate-external-urls`` performs the write.

Scope rules are unchanged: only rows in the caller's workspace, plus the base
rows when the caller is a system admin. Custom endpoints are never rewritten —
the old URL must be the Databricks external proxy and the new one must come
from the schema-scoped UC listing.
"""

import logging
from typing import Any, Dict, Iterable, List, Optional, Tuple

from sqlalchemy.ext.asyncio import AsyncSession

from src.repositories.mcp_repository import MCPServerRepository

logger = logging.getLogger(__name__)

__all__ = ["MCPLegacyUrlService", "plan_legacy_url_migration"]

_LEGACY_MARKER = "/api/2.0/mcp/external/"
_GATEWAY_MARKER = "/ai-gateway/mcp-services/"


def plan_legacy_url_migration(
    servers: Iterable[Any], options: List[Dict[str, Any]]
) -> List[Tuple[Any, str]]:
    """Pure: ``(server, new_url)`` for each server that should be re-pointed."""
    replacements = {
        str(option.get("name", "")).lower(): option.get("server_url")
        for option in options
        if _GATEWAY_MARKER in str(option.get("server_url", ""))
    }
    plan: List[Tuple[Any, str]] = []
    for server in servers:
        new_url = replacements.get(str(server.name).lower())
        if (
            new_url
            and _LEGACY_MARKER in str(server.server_url)
            and server.server_url != new_url
        ):
            plan.append((server, new_url))
    return plan


class MCPLegacyUrlService:
    """Counts and applies the legacy-proxy → AI Gateway URL migration."""

    def __init__(self, session: AsyncSession) -> None:
        self.server_repository = MCPServerRepository(session)

    async def _plan(
        self,
        options: List[Dict[str, Any]],
        group_id: Optional[str],
        include_base: bool,
    ) -> List[Tuple[Any, str]]:
        if not options:
            return []
        servers = await self.server_repository.find_legacy_external_in_scope(
            group_id, include_base
        )
        return plan_legacy_url_migration(servers, options)

    async def count_pending(
        self,
        options: List[Dict[str, Any]],
        group_id: Optional[str],
        include_base: bool,
    ) -> int:
        """Read-only: how many in-scope registrations the migration would change."""
        return len(await self._plan(options, group_id, include_base))

    async def migrate(
        self,
        options: List[Dict[str, Any]],
        group_id: Optional[str],
        include_base: bool,
    ) -> int:
        """Re-point in-scope legacy registrations; returns how many changed."""
        plan = await self._plan(options, group_id, include_base)
        for server, new_url in plan:
            await self.server_repository.update(server.id, {"server_url": new_url})
        if plan:
            logger.info("Migrated %s MCP registration(s) to AI Gateway", len(plan))
        return len(plan)
