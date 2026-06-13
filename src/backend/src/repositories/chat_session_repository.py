"""
Repository for named chat sessions (chat-mode workspace).

Follows the repository pattern: all chat_sessions table access goes through
here. Group scoping is enforced on every read/write that takes group_ids.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.chat_session import ChatSession


class ChatSessionRepository:
    """Repository for ChatSession model with group isolation."""

    def __init__(self, session: AsyncSession):
        self.session = session

    async def create(self, data: Dict[str, Any]) -> ChatSession:
        record = ChatSession(**data)
        self.session.add(record)
        await self.session.flush()
        return record

    async def get_by_id_and_group(
        self, session_id: str, group_ids: List[str]
    ) -> Optional[ChatSession]:
        if not group_ids:
            return None
        stmt = select(ChatSession).where(
            ChatSession.id == session_id,
            ChatSession.group_id.in_(group_ids),
        )
        result = await self.session.execute(stmt)
        return result.scalars().first()

    async def list_by_group_and_user(
        self,
        group_ids: List[str],
        user_id: str,
        page: int = 0,
        per_page: int = 50,
    ) -> List[ChatSession]:
        """Most-recently-updated sessions for this workspace and user."""
        if not group_ids:
            return []
        stmt = (
            select(ChatSession)
            .where(
                ChatSession.group_id.in_(group_ids),
                ChatSession.user_id == user_id,
            )
            .order_by(ChatSession.updated_at.desc())
            .offset(page * per_page)
            .limit(per_page)
        )
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    async def update_title(
        self, session_id: str, group_ids: List[str], title: str
    ) -> Optional[ChatSession]:
        record = await self.get_by_id_and_group(session_id, group_ids)
        if not record:
            return None
        record.title = title
        record.updated_at = datetime.utcnow()
        await self.session.flush()
        return record

    async def touch(self, session_id: str) -> None:
        """Bump updated_at (called when a message lands in the session)."""
        stmt = (
            update(ChatSession)
            .where(ChatSession.id == session_id)
            .values(updated_at=datetime.utcnow())
        )
        await self.session.execute(stmt)

    async def set_running_job(
        self, session_id: str, group_ids: List[str], job_id: Optional[str]
    ) -> Optional[ChatSession]:
        """Set (or clear, when job_id is None) the in-flight job marker."""
        record = await self.get_by_id_and_group(session_id, group_ids)
        if not record:
            return None
        record.running_job_id = job_id
        await self.session.flush()
        return record

    async def set_preview(
        self,
        session_id: str,
        group_ids: List[str],
        preview_type: Optional[str],
        preview_data: Optional[str],
        preview_title: Optional[str],
    ) -> Optional[ChatSession]:
        """Save (or clear, when all None) the session's rendered preview."""
        record = await self.get_by_id_and_group(session_id, group_ids)
        if not record:
            return None
        record.preview_type = preview_type
        record.preview_data = preview_data
        record.preview_title = preview_title
        await self.session.flush()
        return record

    async def delete_by_id_and_group(
        self, session_id: str, group_ids: List[str]
    ) -> bool:
        if not group_ids:
            return False
        stmt = delete(ChatSession).where(
            ChatSession.id == session_id,
            ChatSession.group_id.in_(group_ids),
        )
        result = await self.session.execute(stmt)
        await self.session.flush()
        return (result.rowcount or 0) > 0
