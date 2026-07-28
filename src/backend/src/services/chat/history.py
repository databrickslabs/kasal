from typing import List, Optional, Type
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime
from uuid import uuid4

from src.core.base_service import BaseService
from src.models.chat_history import ChatHistory
from src.repositories.chat_history_repository import ChatHistoryRepository
from src.schemas.chat_history import ChatHistoryCreate, ChatHistoryResponse
from src.utils.user_context import GroupContext


class ChatHistoryService(BaseService[ChatHistory, ChatHistoryCreate]):
    """
    Service for ChatHistory model with business logic and group isolation.
    Follows Kasal's service patterns for multi-group deployments.
    """

    def __init__(self, session):
        """
        Initialize the service with session.

        Args:
            session: Database session from FastAPI DI (from core.dependencies)
        """
        super().__init__(session)
        self.repository = ChatHistoryRepository(session)
        from src.repositories.chat_session_repository import ChatSessionRepository

        self.session_repository = ChatSessionRepository(session)

    async def save_message(
        self,
        session_id: str,
        user_id: str,
        message_type: str,
        content: str,
        intent: Optional[str] = None,
        confidence: Optional[float] = None,
        generation_result: Optional[dict] = None,
        group_context: Optional[GroupContext] = None,
        message_id_override: Optional[str] = None,
    ) -> ChatHistoryResponse:
        """
        Save a chat message with group context.

        Args:
            session_id: Chat session identifier
            user_id: User identifier
            message_type: 'user' or 'assistant'
            content: Message content
            intent: Detected intent (optional)
            confidence: Confidence score (optional)
            generation_result: Generated data (optional)
            group_context: Group context for multi-tenant support

        Returns:
            Created chat message response DTO (avoids async lazy-loading)
        """
        # Generate ID and timestamp up-front so we can return a DTO without touching ORM lazy-loaders
        message_id = message_id_override or str(uuid4())
        ts = datetime.utcnow()

        message_data = {
            "id": message_id,
            "session_id": session_id,
            "user_id": user_id,
            "message_type": message_type,
            "content": content,
            "intent": intent,
            "confidence": str(confidence) if confidence is not None else None,
            "generation_result": generation_result,
            "timestamp": ts,
        }

        # Add group context if available
        if group_context:
            message_data.update(
                {
                    "group_id": group_context.primary_group_id,
                    "group_email": group_context.group_email,
                }
            )

        # Persist to DB using the same ID/timestamp to keep DB and response consistent
        await self.repository.create(message_data)

        # Keep the named session's updated_at in step with its latest message
        # (no-op for sessions without a chat_sessions row, e.g. sidebar chat).
        try:
            await self.session_repository.touch(session_id)
        except Exception:
            pass

        # Return a pure Pydantic DTO built from the explicit data (no ORM access -> no MissingGreenlet)
        return ChatHistoryResponse(**message_data)

    async def get_chat_session(
        self,
        session_id: str,
        page: int = 0,
        per_page: int = 50,
        group_context: Optional[GroupContext] = None,
    ) -> List[ChatHistoryResponse]:
        """
        Get chat messages for a specific session with group filtering.

        Args:
            session_id: Chat session identifier
            page: Page number (0-based)
            per_page: Number of messages per page
            group_context: Group context for filtering

        Returns:
            List of ChatHistory messages
        """
        if not group_context or not group_context.group_ids:
            return []

        messages = await self.repository.get_by_session_and_group(
            session_id=session_id,
            group_ids=group_context.group_ids,
            page=page,
            per_page=per_page,
        )
        # Convert SQLAlchemy models to Pydantic schemas
        return [ChatHistoryResponse.model_validate(msg) for msg in messages]

    async def get_user_sessions(
        self,
        user_id: str,
        page: int = 0,
        per_page: int = 20,
        group_context: Optional[GroupContext] = None,
    ) -> List[ChatHistoryResponse]:
        """
        Get recent chat sessions for a user with group filtering.

        Args:
            user_id: User identifier
            page: Page number (0-based)
            per_page: Number of sessions per page
            group_context: Group context for filtering

        Returns:
            List of ChatHistory messages (latest from each session)
        """
        if not group_context or not group_context.group_ids:
            return []

        sessions = await self.repository.get_user_sessions(
            user_id=user_id,
            group_ids=group_context.group_ids,
            page=page,
            per_page=per_page,
        )
        # Convert SQLAlchemy models to Pydantic schemas
        return [ChatHistoryResponse.model_validate(session) for session in sessions]

    async def get_group_sessions(
        self,
        page: int = 0,
        per_page: int = 20,
        user_id: Optional[str] = None,
        group_context: Optional[GroupContext] = None,
    ) -> List[dict]:
        """
        Get chat sessions for a group with optional user filtering.

        Args:
            page: Page number (0-based)
            per_page: Number of sessions per page
            user_id: Optional user ID filter
            group_context: Group context for filtering

        Returns:
            List of session information
        """
        if not group_context or not group_context.group_ids:
            return []

        return await self.repository.get_sessions_by_group(
            group_ids=group_context.group_ids,
            user_id=user_id,
            page=page,
            per_page=per_page,
        )

    async def delete_session(
        self, session_id: str, group_context: Optional[GroupContext] = None
    ) -> bool:
        """
        Delete a complete chat session with group filtering.

        Args:
            session_id: Chat session identifier
            group_context: Group context for filtering

        Returns:
            True if session was deleted, False if not found
        """
        if not group_context or not group_context.group_ids:
            return False

        deleted_messages = await self.repository.delete_session(
            session_id=session_id, group_ids=group_context.group_ids
        )
        # Also drop the named-session row (chat-mode sessions). Either part
        # existing counts as a successful delete: an empty named session has
        # no messages, a sidebar session has no chat_sessions row.
        deleted_named = await self.session_repository.delete_by_id_and_group(
            session_id, group_context.group_ids
        )
        return deleted_messages or deleted_named

    async def count_session_messages(
        self, session_id: str, group_context: Optional[GroupContext] = None
    ) -> int:
        """
        Count messages in a chat session with group filtering.

        Args:
            session_id: Chat session identifier
            group_context: Group context for filtering

        Returns:
            Number of messages in the session
        """
        if not group_context or not group_context.group_ids:
            return 0

        return await self.repository.count_messages_by_session(
            session_id=session_id, group_ids=group_context.group_ids
        )

    def generate_session_id(self) -> str:
        """
        Generate a new unique session ID.

        Returns:
            UUID string for new session
        """
        return str(uuid4())

    # ------------------------------------------------------------------
    # Named chat sessions (chat-mode workspace). Sessions live server-side
    # (SQLite locally / Lakebase when active) instead of browser IndexedDB.
    # ------------------------------------------------------------------

    async def create_named_session(
        self,
        user_id: str,
        title: str = "New Chat",
        session_id: Optional[str] = None,
        group_context: Optional[GroupContext] = None,
    ):
        """Create a named chat session owned by user_id in the current group."""
        data = {
            "id": session_id or str(uuid4()),
            "title": title or "New Chat",
            "user_id": user_id,
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow(),
        }
        if group_context:
            data.update(
                {
                    "group_id": group_context.primary_group_id,
                    "group_email": group_context.group_email,
                }
            )
        return await self.session_repository.create(data)

    async def list_named_sessions(
        self,
        user_id: str,
        page: int = 0,
        per_page: int = 50,
        group_context: Optional[GroupContext] = None,
    ):
        """List the user's named sessions in the current workspace, most recent first."""
        if not group_context or not group_context.group_ids:
            return []
        return await self.session_repository.list_by_group_and_user(
            group_ids=group_context.group_ids,
            user_id=user_id,
            page=page,
            per_page=per_page,
        )

    async def rename_named_session(
        self,
        session_id: str,
        title: str,
        group_context: Optional[GroupContext] = None,
    ):
        """Rename a named session (group-checked). Returns None when not found."""
        if not group_context or not group_context.group_ids:
            return None
        return await self.session_repository.update_title(
            session_id, group_context.group_ids, title
        )

    # ------------------------------------------------------------------
    # Per-session preview + in-flight job marker (chat-mode). These moved off
    # browser IndexedDB onto the session row so they survive reload and follow
    # the user across browsers/devices.
    # ------------------------------------------------------------------

    async def get_preview(
        self, session_id: str, group_context: Optional[GroupContext] = None
    ) -> Optional[dict]:
        """Return {type, data, title} for the session, or None when not found."""
        if not group_context or not group_context.group_ids:
            return None
        record = await self.session_repository.get_by_id_and_group(
            session_id, group_context.group_ids
        )
        if not record:
            return None
        return {
            "type": record.preview_type,
            "data": record.preview_data,
            "title": record.preview_title,
        }

    async def set_preview(
        self,
        session_id: str,
        preview_type: Optional[str],
        preview_data: Optional[str],
        preview_title: Optional[str],
        group_context: Optional[GroupContext] = None,
    ) -> bool:
        """Save (or clear, when fields are None) the session's preview."""
        if not group_context or not group_context.group_ids:
            return False
        record = await self.session_repository.set_preview(
            session_id,
            group_context.group_ids,
            preview_type,
            preview_data,
            preview_title,
        )
        return record is not None

    async def get_running_job(
        self, session_id: str, group_context: Optional[GroupContext] = None
    ) -> Optional[str]:
        """Return the session's in-flight job id, or None."""
        if not group_context or not group_context.group_ids:
            return None
        record = await self.session_repository.get_by_id_and_group(
            session_id, group_context.group_ids
        )
        return record.running_job_id if record else None

    async def set_running_job(
        self,
        session_id: str,
        job_id: Optional[str],
        group_context: Optional[GroupContext] = None,
    ) -> bool:
        """Set (or clear, when job_id is None) the session's in-flight job."""
        if not group_context or not group_context.group_ids:
            return False
        record = await self.session_repository.set_running_job(
            session_id, group_context.group_ids, job_id
        )
        return record is not None

    async def update_message(
        self,
        message_id: str,
        group_context: Optional[GroupContext] = None,
        content: Optional[str] = None,
        intent: Optional[str] = None,
        generation_result: Optional[dict] = None,
    ) -> Optional[ChatHistoryResponse]:
        """Update a message in place (streaming append / attach result).

        Group-checked: only messages belonging to the caller's groups are
        touchable. Returns the updated DTO or None when not found.
        """
        if not group_context or not group_context.group_ids:
            return None
        record = await self.repository.get_by_id_and_group(
            message_id, group_context.group_ids
        )
        if not record:
            return None
        if content is not None:
            record.content = content
        if intent is not None:
            record.intent = intent
        if generation_result is not None:
            record.generation_result = generation_result
        await self.session.flush()
        return ChatHistoryResponse.model_validate(record)
