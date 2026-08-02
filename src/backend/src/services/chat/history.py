import asyncio
import logging
from datetime import datetime
from typing import List, Optional, Type
from uuid import uuid4

from sqlalchemy.ext.asyncio import AsyncSession

from src.core.base_service import BaseService
from src.models.chat_history import ChatHistory
from src.repositories.chat_history_repository import ChatHistoryRepository
from src.schemas.chat_history import ChatHistoryCreate, ChatHistoryResponse
from src.utils.user_context import GroupContext

logger = logging.getLogger(__name__)


#: Content the UI posts for an ACTIVITY card rather than an answer.
_ACTIVITY_CONTENT = {"[ui-card]"}


def _is_activity_card(content: str, generation_result: Optional[dict]) -> bool:
    """Whether this assistant row is a progress card rather than an answer.

    A single turn writes many: one per crew start, checkpoint, restore. Four
    observed turns produced 22 of these against 5 real answers, and remembering
    them would fill the store with "Checkpoint saved / turn_end" and crowd out
    what was actually said.

    Two independent signals, because either alone has been wrong: the content
    the UI posts for a card, and the ``resultType`` the envelope carries.
    """
    if (content or "").strip() in _ACTIVITY_CONTENT:
        return True
    payload = (generation_result or {}).get("__chatmode")
    if isinstance(payload, dict) and payload.get("resultType") == "trace":
        return True
    return False


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

        # The session's own memory of this exchange. Fire-and-forget: it must
        # never delay the answer reaching the screen, and a memory backend that
        # is down must never fail a message that is already persisted.
        await self._remember_exchange(
            session_id, message_type, content, generation_result, group_context
        )

        # Return a pure Pydantic DTO built from the explicit data (no ORM access -> no MissingGreenlet)
        return ChatHistoryResponse(**message_data)

    async def _remember_exchange(
        self,
        session_id: str,
        message_type: str,
        content: str,
        generation_result: Optional[dict],
        group_context: Optional[GroupContext],
    ) -> None:
        """Record a completed exchange in memory, off the request path.

        THE one funnel every assistant answer passes through, whichever path
        produced it. The light agent already remembered its own turns, but a
        turn routed to a published crew or flow is answered by that capability
        and the light agent never runs — so those exchanges were remembered
        nowhere. The conversation existed only as ``flow_states.messages``:
        checkpoint JSON reachable by one derived thread id, never embedded, so a
        new session could not recall a word of it.

        Hooking here rather than at each run's completion is what makes it cover
        the crew path, the flow path and any future one for free — an answer that
        is not saved to the session did not happen as far as the user is
        concerned, so this is the honest definition of "the turn completed".
        """
        if message_type != "assistant" or not (content or "").strip():
            return
        if _is_activity_card(content, generation_result):
            return
        group_id = getattr(group_context, "primary_group_id", None)
        if not group_id:
            return  # no tenant, nowhere to put it — see the memory scope rules

        # Read the question HERE, on the request, while this service's session is
        # still open. Doing it inside the detached task read through a closed
        # session, so every record was stored as "User: \nAssistant: …" — an
        # answer with no subject, which is the one thing this write exists to
        # avoid. The memory BUILD stays detached: it opens its own session.
        question = await self._last_user_message(session_id, group_context)

        async def _record() -> None:
            try:
                from src.services.memory.crew_memory import build_session_memory
                from src.services.memory.hooks import (
                    format_turn_for_memory,
                    remember_async,
                )

                memory = await build_session_memory(
                    group_id,
                    session_id=session_id,
                    user_token=getattr(group_context, "access_token", None),
                )
                if memory is None:
                    return
                remember_async(
                    memory,
                    format_turn_for_memory(question, content),
                    source="chat",
                    metadata={"session_id": session_id},
                )
            except Exception as exc:  # noqa: BLE001 — never fail a saved message
                logger.debug("Could not record the exchange in memory: %s", exc)

        try:
            # Detached on purpose: the memory build opens its OWN session
            # (CrewMemoryService.fetch_memory_backend_config uses
            # request_scoped_session), so it does not outlive this request's
            # session or hold it open while an embedder is configured.
            asyncio.ensure_future(_record())
        except RuntimeError:  # no running loop (sync call sites, tests)
            pass

    async def _last_user_message(
        self, session_id: str, group_context: Optional[GroupContext]
    ) -> str:
        """The question this answer is answering, or ''.

        An answer on its own is a statement with no subject — "the official
        website is https://ag2.ai" recalls usefully only next to what was asked.
        """
        try:
            group_ids = getattr(group_context, "group_ids", None) or []
            recent = await self.repository.get_recent_by_session_and_group(
                session_id, group_ids, limit=10
            )
            for message in reversed(recent or []):
                if getattr(message, "message_type", None) == "user":
                    return str(getattr(message, "content", "") or "")
        except Exception as exc:  # noqa: BLE001
            logger.debug("Could not read the question for this answer: %s", exc)
        return ""

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
