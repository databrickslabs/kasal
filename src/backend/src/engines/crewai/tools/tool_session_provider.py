"""
Centralized DB session provider for CrewAI tools.

Tools run in subprocess context where there is no request-scoped session.
Instead of each tool importing async_session_factory() directly, they use
this provider — giving us a single point of control for session lifecycle,
logging, and future connection pool guards.
"""

import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


class ToolSessionProvider:
    """Provides DB sessions to tools running in CrewAI subprocess context."""

    @staticmethod
    @asynccontextmanager
    async def session() -> AsyncGenerator[AsyncSession, None]:
        """Yield a scoped async session for tool DB operations.

        Usage::

            from src.engines.crewai.tools.tool_session_provider import ToolSessionProvider

            async with ToolSessionProvider.session() as session:
                service = SomeService(session)
                await service.do_work()
        """
        from src.db.session import async_session_factory

        async with async_session_factory() as session:
            try:
                yield session
            except Exception:
                await session.rollback()
                raise
