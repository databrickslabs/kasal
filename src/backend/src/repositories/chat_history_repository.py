from typing import List, Optional
from sqlalchemy import select, desc, and_, func
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.base_repository import BaseRepository
from src.models.chat_history import ChatHistory
from src.db.session import async_session_factory


class ChatHistoryRepository(BaseRepository[ChatHistory]):
    """
    Repository for ChatHistory model with group-aware operations.
    Follows Kasal's patterns for multi-group data isolation.
    """

    def __init__(self, session: AsyncSession):
        super().__init__(ChatHistory, session)

    async def get_by_session_and_group(
        self, 
        session_id: str, 
        group_ids: List[str], 
        page: int = 0, 
        per_page: int = 50
    ) -> List[ChatHistory]:
        """
        Get chat messages by session ID with group filtering and pagination.
        
        Args:
            session_id: Chat session identifier
            group_ids: List of allowed group IDs for filtering
            page: Page number (0-based)
            per_page: Number of messages per page
            
        Returns:
            List of ChatHistory messages ordered by timestamp
        """
        if not group_ids:
            return []

        try:
            query = select(self.model).where(
                and_(
                    self.model.session_id == session_id,
                    self.model.group_id.in_(group_ids)
                )
            ).order_by(self.model.timestamp.asc())
            
            # Apply pagination
            query = query.offset(page * per_page).limit(per_page)
            
            result = await self.session.execute(query)
            return list(result.scalars().all())
        except Exception as e:
            await self.session.rollback()
            raise

    async def get_sessions_by_group(
        self, 
        group_ids: List[str], 
        user_id: Optional[str] = None,
        page: int = 0, 
        per_page: int = 20
    ) -> List[dict]:
        """
        Get distinct chat sessions for a group with optional user filtering.
        
        Args:
            group_ids: List of allowed group IDs for filtering
            user_id: Optional user ID to filter by
            page: Page number (0-based)
            per_page: Number of sessions per page
            
        Returns:
            List of session info with latest message timestamp
        """
        if not group_ids:
            return []

        try:
            # Build base query conditions
            conditions = [self.model.group_id.in_(group_ids)]
            if user_id:
                conditions.append(self.model.user_id == user_id)

            # Get distinct session_ids with latest timestamp and message count
            subquery = select(
                self.model.session_id,
                self.model.user_id,
                func.max(self.model.timestamp).label('latest_timestamp'),
                func.count(self.model.id).label('message_count')
            ).where(
                and_(*conditions)
            ).group_by(
                self.model.session_id,
                self.model.user_id
            ).order_by(
                desc('latest_timestamp')
            ).offset(page * per_page).limit(per_page).subquery()

            # Execute query
            result = await self.session.execute(select(subquery))
            rows = result.fetchall()
            
            return [
                {
                    'session_id': row.session_id,
                    'user_id': row.user_id,
                    'latest_timestamp': row.latest_timestamp,
                    'message_count': row.message_count
                }
                for row in rows
            ]
        except Exception as e:
            await self.session.rollback()
            raise

    async def get_user_sessions(
        self, 
        user_id: str, 
        group_ids: List[str], 
        page: int = 0, 
        per_page: int = 20
    ) -> List[ChatHistory]:
        """
        Get recent chat sessions for a specific user with group filtering.
        
        Args:
            user_id: User identifier
            group_ids: List of allowed group IDs for filtering
            page: Page number (0-based)
            per_page: Number of sessions per page
            
        Returns:
            List of ChatHistory messages (one per session)
        """
        if not group_ids:
            return []

        try:
            # Get the latest message from each session for this user
            subquery = select(
                self.model.session_id,
                func.max(self.model.timestamp).label('max_timestamp')
            ).where(
                and_(
                    self.model.user_id == user_id,
                    self.model.group_id.in_(group_ids)
                )
            ).group_by(self.model.session_id).subquery()

            # Join back to get full message details
            query = select(self.model).join(
                subquery,
                and_(
                    self.model.session_id == subquery.c.session_id,
                    self.model.timestamp == subquery.c.max_timestamp
                )
            ).order_by(desc(subquery.c.max_timestamp))
            
            # Apply pagination
            query = query.offset(page * per_page).limit(per_page)
            
            result = await self.session.execute(query)
            return list(result.scalars().all())
        except Exception as e:
            await self.session.rollback()
            raise

    async def delete_session(self, session_id: str, group_ids: List[str]) -> bool:
        """
        Delete all messages in a chat session with group filtering.
        
        Args:
            session_id: Chat session identifier
            group_ids: List of allowed group IDs for filtering
            
        Returns:
            True if messages were deleted, False if none found
        """
        if not group_ids:
            return False

        try:
            # Get all messages in the session for this group
            messages = await self.get_by_session_and_group(session_id, group_ids)
            
            if not messages:
                return False

            # Delete all messages
            for message in messages:
                await self.session.delete(message)
            
            await self.session.flush()
            await self.session.commit()
            return True
        except Exception as e:
            await self.session.rollback()
            raise

    async def count_messages_by_session(
        self, 
        session_id: str, 
        group_ids: List[str]
    ) -> int:
        """
        Count total messages in a session with group filtering.
        
        Args:
            session_id: Chat session identifier
            group_ids: List of allowed group IDs for filtering
            
        Returns:
            Total number of messages in the session
        """
        if not group_ids:
            return 0

        try:
            query = select(self.model.id).where(
                and_(
                    self.model.session_id == session_id,
                    self.model.group_id.in_(group_ids)
                )
            )
            
            result = await self.session.execute(query)
            return len(result.scalars().all())
        except Exception as e:
            await self.session.rollback()
            raise