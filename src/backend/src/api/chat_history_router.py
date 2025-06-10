from typing import Annotated, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Path, Query, status
import logging

from src.core.dependencies import SessionDep, GroupContextDep, get_service
from src.models.chat_history import ChatHistory
from src.repositories.chat_history_repository import ChatHistoryRepository
from src.schemas.chat_history import (
    ChatHistoryResponse, 
    ChatHistoryListResponse,
    ChatSessionListResponse,
    SaveMessageRequest,
    GetSessionRequest,
    GetUserSessionsRequest
)
from src.services.chat_history_service import ChatHistoryService

router = APIRouter(
    prefix="/chat-history",
    tags=["chat-history"],
    responses={404: {"description": "Not found"}},
)

# Set up logging
logger = logging.getLogger(__name__)

# Dependency to get ChatHistoryService
get_chat_history_service = get_service(ChatHistoryService, ChatHistoryRepository, ChatHistory)


@router.post("/messages", response_model=ChatHistoryResponse, status_code=status.HTTP_201_CREATED)
async def save_chat_message(
    message_request: SaveMessageRequest,
    service: Annotated[ChatHistoryService, Depends(get_chat_history_service)],
    group_context: GroupContextDep,
):
    """
    Save a chat message with group isolation.
    
    Args:
        message_request: Chat message data
        service: Chat history service injected by dependency
        group_context: Group context from headers
        
    Returns:
        Saved chat message
    """
    try:
        if not group_context or not group_context.is_valid():
            raise HTTPException(status_code=400, detail="No valid group context provided")

        # Extract user_id from group context (assuming it's available)
        user_id = group_context.group_email or "unknown_user"
        
        return await service.save_message(
            session_id=message_request.session_id,
            user_id=user_id,
            message_type=message_request.message_type,
            content=message_request.content,
            intent=message_request.intent,
            confidence=message_request.confidence,
            generation_result=message_request.generation_result,
            group_context=group_context
        )
    except Exception as e:
        logger.error(f"Error saving chat message: {e}")
        raise HTTPException(status_code=500, detail="Failed to save chat message")


@router.get("/sessions/{session_id}/messages", response_model=ChatHistoryListResponse)
async def get_chat_session_messages(
    session_id: Annotated[str, Path(..., description="Chat session identifier")],
    service: Annotated[ChatHistoryService, Depends(get_chat_history_service)],
    group_context: GroupContextDep,
    page: int = Query(0, ge=0, description="Page number (0-based)"),
    per_page: int = Query(50, ge=1, le=100, description="Messages per page"),
):
    """
    Get chat messages for a specific session with group filtering.
    
    Args:
        session_id: Chat session identifier
        page: Page number for pagination
        per_page: Number of messages per page
        service: Chat history service injected by dependency
        group_context: Group context from headers
        
    Returns:
        List of chat messages with pagination info
    """
    try:
        if not group_context or not group_context.is_valid():
            raise HTTPException(status_code=400, detail="No valid group context provided")

        messages = await service.get_chat_session(
            session_id=session_id,
            page=page,
            per_page=per_page,
            group_context=group_context
        )
        
        # Get total count for pagination
        total_messages = await service.count_session_messages(
            session_id=session_id,
            group_context=group_context
        )

        return ChatHistoryListResponse(
            messages=messages,
            total_messages=total_messages,
            page=page,
            per_page=per_page,
            session_id=session_id
        )
    except Exception as e:
        logger.error(f"Error getting chat session messages: {e}")
        raise HTTPException(status_code=500, detail="Failed to get chat session messages")


@router.get("/users/sessions", response_model=List[ChatHistoryResponse])
async def get_user_chat_sessions(
    service: Annotated[ChatHistoryService, Depends(get_chat_history_service)],
    group_context: GroupContextDep,
    page: int = Query(0, ge=0, description="Page number (0-based)"),
    per_page: int = Query(20, ge=1, le=50, description="Sessions per page"),
):
    """
    Get recent chat sessions for the current user with group filtering.
    
    Args:
        page: Page number for pagination
        per_page: Number of sessions per page
        service: Chat history service injected by dependency
        group_context: Group context from headers
        
    Returns:
        List of latest messages from each chat session
    """
    try:
        if not group_context or not group_context.is_valid():
            raise HTTPException(status_code=400, detail="No valid group context provided")

        # Extract user_id from group context
        user_id = group_context.group_email or "unknown_user"

        return await service.get_user_sessions(
            user_id=user_id,
            page=page,
            per_page=per_page,
            group_context=group_context
        )
    except Exception as e:
        logger.error(f"Error getting user chat sessions: {e}")
        raise HTTPException(status_code=500, detail="Failed to get user chat sessions")


@router.get("/sessions", response_model=ChatSessionListResponse)
async def get_group_chat_sessions(
    service: Annotated[ChatHistoryService, Depends(get_chat_history_service)],
    group_context: GroupContextDep,
    page: int = Query(0, ge=0, description="Page number (0-based)"),
    per_page: int = Query(20, ge=1, le=50, description="Sessions per page"),
    user_id: Optional[str] = Query(None, description="Filter by user ID"),
):
    """
    Get chat sessions for the group with optional user filtering.
    
    Args:
        page: Page number for pagination
        per_page: Number of sessions per page
        user_id: Optional user ID filter
        service: Chat history service injected by dependency
        group_context: Group context from headers
        
    Returns:
        List of chat session information with pagination
    """
    try:
        if not group_context or not group_context.is_valid():
            raise HTTPException(status_code=400, detail="No valid group context provided")

        sessions = await service.get_group_sessions(
            page=page,
            per_page=per_page,
            user_id=user_id,
            group_context=group_context
        )

        return ChatSessionListResponse(
            sessions=sessions,
            total_sessions=len(sessions),  # This is approximate - could be improved
            page=page,
            per_page=per_page
        )
    except Exception as e:
        logger.error(f"Error getting group chat sessions: {e}")
        raise HTTPException(status_code=500, detail="Failed to get group chat sessions")


@router.delete("/sessions/{session_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_chat_session(
    session_id: Annotated[str, Path(..., description="Chat session identifier")],
    service: Annotated[ChatHistoryService, Depends(get_chat_history_service)],
    group_context: GroupContextDep,
):
    """
    Delete a complete chat session with group filtering.
    
    Args:
        session_id: Chat session identifier
        service: Chat history service injected by dependency
        group_context: Group context from headers
    """
    try:
        if not group_context or not group_context.is_valid():
            raise HTTPException(status_code=400, detail="No valid group context provided")

        deleted = await service.delete_session(
            session_id=session_id,
            group_context=group_context
        )
        
        if not deleted:
            raise HTTPException(status_code=404, detail="Chat session not found")
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting chat session: {e}")
        raise HTTPException(status_code=500, detail="Failed to delete chat session")


@router.post("/sessions/new", response_model=dict, status_code=status.HTTP_201_CREATED)
async def create_new_chat_session(
    service: Annotated[ChatHistoryService, Depends(get_chat_history_service)],
    group_context: GroupContextDep,
):
    """
    Generate a new chat session ID.
    
    Args:
        service: Chat history service injected by dependency
        group_context: Group context from headers
        
    Returns:
        New session ID
    """
    try:
        if not group_context or not group_context.is_valid():
            raise HTTPException(status_code=400, detail="No valid group context provided")

        session_id = service.generate_session_id()
        
        return {"session_id": session_id}
    except Exception as e:
        logger.error(f"Error creating new chat session: {e}")
        raise HTTPException(status_code=500, detail="Failed to create new chat session")