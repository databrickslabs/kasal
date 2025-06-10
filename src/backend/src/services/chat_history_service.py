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
    
    def __init__(
        self,
        session: AsyncSession,
        repository_class: Type[ChatHistoryRepository] = ChatHistoryRepository,
        model_class: Type[ChatHistory] = ChatHistory
    ):
        """
        Initialize the service with session and optional repository and model classes.
        
        Args:
            session: Database session for operations
            repository_class: Repository class to use for data access (optional)
            model_class: Model class associated with this service (optional)
        """
        super().__init__(session)
        self.repository_class = repository_class
        self.model_class = model_class
        self.repository = repository_class(session)
    
    @classmethod
    def create(cls, session: AsyncSession) -> 'ChatHistoryService':
        """
        Factory method to create a properly configured ChatHistoryService instance.
        
        Args:
            session: Database session for operations
            
        Returns:
            An instance of ChatHistoryService
        """
        return cls(session=session)

    async def save_message(
        self, 
        session_id: str,
        user_id: str,
        message_type: str,
        content: str,
        intent: Optional[str] = None,
        confidence: Optional[float] = None,
        generation_result: Optional[dict] = None,
        group_context: Optional[GroupContext] = None
    ) -> ChatHistory:
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
            Created ChatHistory instance
        """
        message_data = {
            'session_id': session_id,
            'user_id': user_id,
            'message_type': message_type,
            'content': content,
            'intent': intent,
            'confidence': str(confidence) if confidence is not None else None,
            'generation_result': generation_result,
            'timestamp': datetime.utcnow(),
        }

        # Add group context if available
        if group_context:
            message_data.update({
                'group_id': group_context.primary_group_id,
                'group_email': group_context.group_email,
                # Legacy fields for compatibility
                'tenant_id': group_context.primary_group_id,
                'tenant_email': group_context.group_email
            })

        return await self.repository.create(message_data)

    async def get_chat_session(
        self, 
        session_id: str, 
        page: int = 0, 
        per_page: int = 50,
        group_context: Optional[GroupContext] = None
    ) -> List[ChatHistory]:
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

        return await self.repository.get_by_session_and_group(
            session_id=session_id,
            group_ids=group_context.group_ids,
            page=page,
            per_page=per_page
        )

    async def get_user_sessions(
        self, 
        user_id: str, 
        page: int = 0, 
        per_page: int = 20,
        group_context: Optional[GroupContext] = None
    ) -> List[ChatHistory]:
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

        return await self.repository.get_user_sessions(
            user_id=user_id,
            group_ids=group_context.group_ids,
            page=page,
            per_page=per_page
        )

    async def get_group_sessions(
        self, 
        page: int = 0, 
        per_page: int = 20,
        user_id: Optional[str] = None,
        group_context: Optional[GroupContext] = None
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
            per_page=per_page
        )

    async def delete_session(
        self, 
        session_id: str, 
        group_context: Optional[GroupContext] = None
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

        return await self.repository.delete_session(
            session_id=session_id,
            group_ids=group_context.group_ids
        )

    async def count_session_messages(
        self, 
        session_id: str, 
        group_context: Optional[GroupContext] = None
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
            session_id=session_id,
            group_ids=group_context.group_ids
        )

    def generate_session_id(self) -> str:
        """
        Generate a new unique session ID.
        
        Returns:
            UUID string for new session
        """
        return str(uuid4())