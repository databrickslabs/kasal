from typing import List, Optional

from sqlalchemy import select, delete as sa_delete
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from src.core.base_repository import BaseRepository
from src.models.mcp_server import MCPServer
from src.models.mcp_settings import MCPSettings


class MCPServerRepository(BaseRepository[MCPServer]):
    """
    Repository for MCPServer model with custom query methods.
    Inherits base CRUD operations from BaseRepository.
    """

    def __init__(self, session: AsyncSession):
        """
        Initialize the repository with session.

        Args:
            session: SQLAlchemy async session
        """
        super().__init__(MCPServer, session)

    async def find_by_name(self, name: str) -> Optional[MCPServer]:
        """
        Find a MCP server by name.

        Args:
            name: Server name to search for

        Returns:
            MCPServer if found, else None
        """
        query = select(self.model).where(self.model.name == name)
        result = await self.session.execute(query)
        return result.scalars().first()

    async def find_enabled(self) -> List[MCPServer]:
        """
        Find all enabled MCP servers.

        Returns:
            List of enabled MCP servers
        """
        query = select(self.model).where(self.model.enabled == True)
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def find_global_enabled(self) -> List[MCPServer]:
        """
        Find all globally enabled MCP servers.

        Returns:
            List of globally enabled MCP servers
        """
        query = select(self.model).where(
            (self.model.enabled == True) &
            (self.model.global_enabled == True)
        )
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def find_by_names(self, names: List[str]) -> List[MCPServer]:
        """
        Find MCP servers by a list of names.

        Args:
            names: List of server names to search for

        Returns:
            List of MCP servers matching the names
        """
        if not names:
            return []

        query = select(self.model).where(
            (self.model.name.in_(names)) &
            (self.model.enabled == True)
        )
        result = await self.session.execute(query)
        return list(result.scalars().all())
    async def find_by_name_and_group(self, name: str, group_id: str) -> Optional[MCPServer]:
        """
        Find a MCP server by name scoped to a group/workspace.
        """
        query = select(self.model).where((self.model.name == name) & (self.model.group_id == group_id))
        result = await self.session.execute(query)
        return result.scalars().first()

    async def find_base_by_name(self, name: str) -> Optional[MCPServer]:
        """
        Find a base (global) MCP server by name (group_id is NULL).
        """
        query = select(self.model).where((self.model.name == name) & (self.model.group_id.is_(None)))
        result = await self.session.execute(query)
        return result.scalars().first()

    async def find_all_base(self) -> List[MCPServer]:
        """
        List all base/global MCP servers (group_id IS NULL).

        These are the system-admin catalog: a base server is "available to all
        workspaces" when its ``enabled`` flag is True. Used by the global admin
        view (Configuration → System Administration → MCP (Global)).
        """
        query = select(self.model).where(self.model.group_id.is_(None))
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def list_for_group_scope(self, group_id: Optional[str]) -> List[MCPServer]:
        """
        List servers effective for a workspace under the GLOBAL + per-workspace
        override model:
        - A base server (group_id IS NULL) is visible to workspaces only when it
          is GLOBALLY AVAILABLE (``enabled == True``) — i.e. published by a system
          admin in MCP (Global). Globally-unavailable base servers are hidden.
        - A group-specific row of the same name SHADOWS the base for THAT group
          only (other workspaces keep seeing the base), carrying the workspace's
          own enabled/disabled state.
        With no group_id, return the globally-available set.
        """
        if not group_id:
            query = select(self.model).where(
                (self.model.group_id.is_(None)) & (self.model.enabled == True)
            )
            result = await self.session.execute(query)
            return list(result.scalars().all())
        # Names THIS group has overridden (only its own group-specific rows).
        group_override_names = (
            select(self.model.name).where(self.model.group_id == group_id).distinct()
        )
        # Names whose GLOBAL base row is disabled — a system admin disabling a
        # global server cascades to workspaces, so hide the workspace override too.
        disabled_base_names = (
            select(self.model.name)
            .where((self.model.group_id.is_(None)) & (self.model.enabled == False))
        )
        query = select(self.model).where(
            (
                (self.model.group_id == group_id)
                & (~self.model.name.in_(disabled_base_names))
            )
            | (
                (self.model.group_id.is_(None))
                & (self.model.enabled == True)
                & (~self.model.name.in_(group_override_names))
            )
        )
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def find_by_names_group_scope(self, names: List[str], group_id: Optional[str]) -> List[MCPServer]:
        """
        Find ENABLED servers by names usable in a workspace under the OPT-IN
        global + per-workspace model:
        - A workspace can only use servers it has explicitly enabled — i.e. its
          OWN group-specific rows (``group_id == group_id``) with ``enabled``.
          A globally-available base server (``group_id IS NULL``) does NOT
          auto-resolve for a workspace; the workspace admin must opt in (which
          creates an enabled override row) in Configuration -> MCP (Workspace).
        - With no group_id (system/personal-global context, e.g. seeding or the
          global admin), fall back to enabled base rows.
        - A workspace override is ALSO gated by the GLOBAL base: if a base row of
          the same name exists and is DISABLED, the server is unavailable to the
          workspace even if its override is enabled — a system admin disabling a
          global server cascades to every workspace. Servers with no base row
          (workspace-only) are unaffected.
        """
        if not names:
            return []
        if not group_id:
            query = select(self.model).where(
                (self.model.name.in_(names))
                & (self.model.enabled == True)
                & (self.model.group_id.is_(None))
            )
        else:
            # Names whose GLOBAL base row is disabled — excluded so a global
            # disable cascades to workspaces regardless of their own override.
            disabled_base_names = (
                select(self.model.name)
                .where((self.model.group_id.is_(None)) & (self.model.enabled == False))
            )
            query = select(self.model).where(
                (self.model.name.in_(names))
                & (self.model.enabled == True)
                & (self.model.group_id == group_id)
                & (~self.model.name.in_(disabled_base_names))
            )
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def delete_overrides_by_name(self, name: str) -> int:
        """Hard-delete all per-workspace override rows (group_id IS NOT NULL) for
        a server name.

        Used when a system admin deletes a GLOBAL (base) server so the deletion
        cascades to every workspace that had opted in — otherwise the override
        rows are orphaned and those workspaces keep the server. Does not commit
        (the session dependency owns the transaction, matching BaseRepository).

        Returns the number of override rows deleted.
        """
        stmt = sa_delete(self.model).where(
            (self.model.name == name) & (self.model.group_id.isnot(None))
        )
        result = await self.session.execute(stmt)
        await self.session.flush()
        return getattr(result, "rowcount", 0) or 0


    async def toggle_enabled(self, server_id: int) -> Optional[MCPServer]:
        """
        Toggle the enabled status of a MCP server.

        Args:
            server_id: ID of the server to toggle

        Returns:
            Updated MCP server if found, else None
        """
        try:
            server = await self.get(server_id)
            if not server:
                return None

            # Toggle the enabled status
            server.enabled = not server.enabled
            await self.session.flush()
            await self.session.refresh(server)
            return server
        except Exception as e:
            # Log the error and rollback
            import logging
            logging.error(f"Error in toggle_enabled for MCP server ID {server_id}: {str(e)}")
            await self.session.rollback()
            raise

    async def toggle_global_enabled(self, server_id: int) -> Optional[MCPServer]:
        """
        Toggle the global enabled status of a MCP server.

        Args:
            server_id: ID of the server to toggle global enablement

        Returns:
            Updated MCP server if found, else None
        """
        try:
            server = await self.get(server_id)
            if not server:
                return None

            # Toggle the global enabled status
            server.global_enabled = not server.global_enabled
            await self.session.flush()
            await self.session.refresh(server)
            return server
        except Exception as e:
            # Log the error and rollback
            import logging
            logging.error(f"Error in toggle_global_enabled for MCP server ID {server_id}: {str(e)}")
            await self.session.rollback()
            raise


class MCPSettingsRepository(BaseRepository[MCPSettings]):
    """
    Repository for MCPSettings model with custom query methods.
    Inherits base CRUD operations from BaseRepository.
    """

    def __init__(self, session: AsyncSession):
        """
        Initialize the repository with session.

        Args:
            session: SQLAlchemy async session
        """
        super().__init__(MCPSettings, session)

    async def get_settings(self) -> MCPSettings:
        """
        Get global MCP settings, creating default settings if none exist.

        Returns:
            MCPSettings object
        """
        query = select(self.model)
        result = await self.session.execute(query)
        settings = result.scalars().first()

        if not settings:
            # Create default settings
            settings = MCPSettings(global_enabled=False)
            self.session.add(settings)
            await self.session.flush()
            await self.session.refresh(settings)

        return settings

    async def update_global_enabled(self, enabled: bool) -> MCPSettings:
        """
        Update the global enabled status.

        Args:
            enabled: New enabled status

        Returns:
            Updated MCPSettings object
        """
        settings = await self.get_settings()
        settings.global_enabled = enabled
        await self.session.flush()
        await self.session.refresh(settings)
        return settings

    async def update_individual_enabled(self, enabled: bool) -> MCPSettings:
        """
        Update the individual enabled status.

        Args:
            enabled: New individual enabled status

        Returns:
            Updated MCPSettings object
        """
        settings = await self.get_settings()
        settings.individual_enabled = enabled
        await self.session.flush()
        await self.session.refresh(settings)
        return settings

    async def update_settings(self, global_enabled: Optional[bool] = None, individual_enabled: Optional[bool] = None) -> MCPSettings:
        """
        Update MCP settings.

        Args:
            global_enabled: New global enabled status (optional)
            individual_enabled: New individual enabled status (optional)

        Returns:
            Updated MCPSettings object
        """
        settings = await self.get_settings()

        if global_enabled is not None:
            settings.global_enabled = global_enabled

        if individual_enabled is not None:
            settings.individual_enabled = individual_enabled

        await self.session.flush()
        await self.session.refresh(settings)
        return settings


class SyncMCPServerRepository:
    """
    Synchronous repository for MCPServer model.
    Used by services that require synchronous DB operations.
    """

    def __init__(self, db: Session):
        """
        Initialize the repository with session.

        Args:
            db: SQLAlchemy synchronous session
        """
        self.db = db

    def find_by_id(self, server_id: int) -> Optional[MCPServer]:
        """
        Find a MCP server by ID.

        Args:
            server_id: ID of the server to find

        Returns:
            MCPServer if found, else None
        """
        return self.db.query(MCPServer).filter(MCPServer.id == server_id).first()

    def find_by_name(self, name: str) -> Optional[MCPServer]:
        """
        Find a MCP server by name.

        Args:
            name: Name to search for

        Returns:
            MCPServer if found, else None
        """
        return self.db.query(MCPServer).filter(MCPServer.name == name).first()

    def find_all(self) -> List[MCPServer]:
        """
        Find all MCP servers.

        Returns:
            List of all MCP servers
        """
        return self.db.query(MCPServer).all()

    def find_enabled(self) -> List[MCPServer]:
        """
        Find all enabled MCP servers.

        Returns:
            List of enabled MCP servers
        """
        return self.db.query(MCPServer).filter(MCPServer.enabled == True).all()

    def find_global_enabled(self) -> List[MCPServer]:
        """
        Find all globally enabled MCP servers.

        Returns:
            List of globally enabled MCP servers
        """
        return self.db.query(MCPServer).filter(
            (MCPServer.enabled == True) &
            (MCPServer.global_enabled == True)
        ).all()

    def find_by_names(self, names: List[str]) -> List[MCPServer]:
        """
        Find MCP servers by a list of names.

        Args:
            names: List of server names to search for

        Returns:
            List of MCP servers matching the names
        """
        if not names:
            return []

        return self.db.query(MCPServer).filter(
            (MCPServer.name.in_(names)) &
            (MCPServer.enabled == True)
        ).all()