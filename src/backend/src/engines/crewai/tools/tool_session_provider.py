"""
Centralized DB session provider for CrewAI tools.

Tools run in subprocess context where there is no request-scoped session.
Instead of each tool importing async_session_factory() directly, they use
this provider — giving us a single point of control for session lifecycle,
logging, and future connection pool guards.

Option B: Service context managers (`cache_service()`, `conversion_repo()`)
hide session+service construction so tools need only one import.
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

        Usage::

            async with ToolSessionProvider.session() as session:
                result = await session.execute(stmt)
        """
        from src.db.session import async_session_factory

        async with async_session_factory() as session:
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
        from src.db.session import async_session_factory
        from src.services.powerbi_semantic_model_cache_service import PowerBISemanticModelCacheService

        async with async_session_factory() as session:
            try:
                yield PowerBISemanticModelCacheService(session)
            except Exception:
                await session.rollback()
                raise

    @staticmethod
    @asynccontextmanager
    async def conversion_repo():
        """Yield a ConversionHistoryRepository with scoped session.

        The caller is responsible for committing if needed (the session
        is accessible via ``repo.session``).

        Usage::

            async with ToolSessionProvider.conversion_repo() as repo:
                record = await repo.create(data)
                await repo.session.commit()
        """
        from src.db.session import async_session_factory
        from src.repositories.conversion_repository import ConversionHistoryRepository

        async with async_session_factory() as session:
            try:
                yield ConversionHistoryRepository(session)
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
        from src.db.session import async_session_factory
        from src.services.databricks_knowledge_service import DatabricksKnowledgeService

        async with async_session_factory() as session:
            try:
                yield DatabricksKnowledgeService(
                    session=session,
                    group_id=group_id,
                    user_token=user_token,
                )
            except Exception:
                await session.rollback()
                raise
