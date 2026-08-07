"""
Centralized DB session provider for CrewAI tools.

Tools run in subprocess context where there is no request-scoped session.

Sessions come from ``routed_scoped_session``, which asks the database ROUTER on
every call. The raw ``async_session_factory`` was used here before: that is a
per-process SNAPSHOT, correct only where something has hot-swapped it to Lakebase
— which happens in a subprocess but NOT in the main process after a runtime
``/lakebase/enable``. A tool running in-process (chat) therefore read the local
database while the same tool in a crew subprocess read Lakebase, with no error
either way. Routing removes the distinction.
Instead of each tool importing a session factory directly, they use
this provider — giving us a single point of control for session lifecycle,
logging, and future connection pool guards.

**This is the one place in `tools/` that may import a session helper**, and it is
why the import below is correct rather than a layering violation: a tool runs in a
subprocess with no request-scoped session, so something has to acquire one, and
concentrating that here is what let a single change route every tool at once.

Prefer the typed context managers — `cache_service()`, `conversion_repo()`,
`powerbi_extraction_repo()`, `knowledge_service()` — which yield a repository or
service and never expose the session. `session()` is the escape hatch for a read
with no repository wrapper yet; hand what it yields to a repository rather than
querying it, or the tool has re-created the bypass this provider removed.
"""

import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


class ToolSessionProvider:
    """Provides DB sessions and ready-to-use services to CrewAI tools."""

    @staticmethod
    @asynccontextmanager
    async def session() -> AsyncGenerator[AsyncSession, None]:
        """Yield a scoped async session for tool DB operations.

        Usage — hand the session to a repository, do not query it directly::

            async with ToolSessionProvider.session() as session:
                rows = await SomeRepository(session).find_whatever(...)
        """
        from src.db.session import routed_scoped_session

        async with routed_scoped_session() as session:
            try:
                yield session
            except Exception:
                await session.rollback()
                raise

    @staticmethod
    @asynccontextmanager
    async def cache_service():
        """Yield a PowerBISemanticModelCacheService with scoped session.

        Usage::

            async with ToolSessionProvider.cache_service() as svc:
                cached = await svc.get_cached_metadata(group_id=gid, ...)
        """
        from src.db.session import routed_scoped_session
        from src.services.powerbi.semantic_model_cache import (
            PowerBISemanticModelCacheService,
        )

        async with routed_scoped_session() as session:
            try:
                yield PowerBISemanticModelCacheService(session)
            except Exception:
                await session.rollback()
                raise

    @staticmethod
    @asynccontextmanager
    async def converter_service(group_context=None):
        """Yield a ConverterService — the owner of conversion history.

        Prefer this over :meth:`conversion_repo`: the service stamps ``group_id``
        and ``created_by_email`` from the group context, which every tool that used
        the repository directly had to remember to do by hand (and did
        inconsistently).
        """
        from src.db.session import routed_scoped_session
        from src.services.powerbi.conversions import ConverterService

        async with routed_scoped_session() as session:
            try:
                yield ConverterService(session, group_context=group_context)
            except Exception:
                await session.rollback()
                raise

    @staticmethod
    @asynccontextmanager
    async def powerbi_extraction_service(group_context=None):
        """Yield a PowerBIExtractionService — the owner of extraction rows.

        Replaced ``conversion_repo``/``powerbi_extraction_repo``, which handed tools a
        raw repository and left each one to stamp ``group_id`` itself.
        """
        from src.db.session import routed_scoped_session
        from src.services.powerbi.extractions import PowerBIExtractionService

        async with routed_scoped_session() as session:
            try:
                yield PowerBIExtractionService(session, group_context=group_context)
            except Exception:
                await session.rollback()
                raise

    @staticmethod
    @asynccontextmanager
    async def knowledge_service(group_id: str = "default", user_token: str = None):
        """Yield a DatabricksKnowledgeService with scoped session.

        Usage::

            async with ToolSessionProvider.knowledge_service(gid, token) as svc:
                results = await svc.search_knowledge(query=q, ...)
        """
        from src.db.session import routed_scoped_session
        from src.services.knowledge.databricks_service import DatabricksKnowledgeService

        async with routed_scoped_session() as session:
            try:
                yield DatabricksKnowledgeService(
                    session=session,
                    group_id=group_id,
                    user_token=user_token,
                )
            except Exception:
                await session.rollback()
                raise
