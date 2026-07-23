from typing import Annotated, List, Optional

from fastapi import APIRouter, Depends, Path, Query, status

from src.core.exceptions import BadRequestError, NotFoundError

from src.core.dependencies import GroupContextDep, SessionDep
from src.models.chat_history import ChatHistory
from src.schemas.chat_history import (
    ChatHistoryListResponse,
    ChatHistoryResponse,
    ChatSessionCreateRequest,
    ChatSessionListResponse,
    ChatSessionRenameRequest,
    GetSessionRequest,
    GetUserSessionsRequest,
    NamedChatSessionResponse,
    RunningJobResponse,
    SaveMessageRequest,
    SavePreviewRequest,
    SessionPreviewResponse,
    SetRunningJobRequest,
    UpdateMessageRequest,
)
from src.services.chat_history_service import ChatHistoryService

router = APIRouter(
    prefix="/chat-history",
    tags=["chat-history"],
    responses={404: {"description": "Not found"}},
)


# Dependency to get ChatHistoryService
def get_chat_history_service(session: SessionDep) -> ChatHistoryService:
    """
    Dependency provider for ChatHistoryService.

    Creates service with session following the pattern:
    Router → Service → Repository → DB

    Args:
        session: Database session from FastAPI DI (from core.dependencies)

    Returns:
        ChatHistoryService instance with session
    """
    return ChatHistoryService(session)


# Type alias for cleaner function signatures
ChatHistoryServiceDep = Annotated[ChatHistoryService, Depends(get_chat_history_service)]


@router.post(
    "/messages", response_model=ChatHistoryResponse, status_code=status.HTTP_201_CREATED
)
async def save_chat_message(
    message_request: SaveMessageRequest,
    service: ChatHistoryServiceDep,
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
    if not group_context or not group_context.is_valid():
        raise BadRequestError("No valid group context provided")

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
        group_context=group_context,
        message_id_override=message_request.id,
    )


@router.get("/sessions/{session_id}/messages", response_model=ChatHistoryListResponse)
async def get_chat_session_messages(
    session_id: Annotated[str, Path(..., description="Chat session identifier")],
    service: ChatHistoryServiceDep,
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
    if not group_context or not group_context.is_valid():
        raise BadRequestError("No valid group context provided")

    messages = await service.get_chat_session(
        session_id=session_id,
        page=page,
        per_page=per_page,
        group_context=group_context,
    )

    # Get total count for pagination
    total_messages = await service.count_session_messages(
        session_id=session_id, group_context=group_context
    )

    return ChatHistoryListResponse(
        messages=messages,
        total_messages=total_messages,
        page=page,
        per_page=per_page,
        session_id=session_id,
    )


@router.get("/users/sessions", response_model=List[ChatHistoryResponse])
async def get_user_chat_sessions(
    service: ChatHistoryServiceDep,
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
    if not group_context or not group_context.is_valid():
        raise BadRequestError("No valid group context provided")

    # Extract user_id from group context
    user_id = group_context.group_email or "unknown_user"

    return await service.get_user_sessions(
        user_id=user_id, page=page, per_page=per_page, group_context=group_context
    )


@router.get("/sessions", response_model=ChatSessionListResponse)
async def get_group_chat_sessions(
    service: ChatHistoryServiceDep,
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
    if not group_context or not group_context.is_valid():
        raise BadRequestError("No valid group context provided")

    sessions = await service.get_group_sessions(
        page=page, per_page=per_page, user_id=user_id, group_context=group_context
    )

    return ChatSessionListResponse(
        sessions=sessions,
        total_sessions=len(sessions),  # This is approximate - could be improved
        page=page,
        per_page=per_page,
    )


@router.delete("/sessions/{session_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_chat_session(
    session_id: Annotated[str, Path(..., description="Chat session identifier")],
    service: ChatHistoryServiceDep,
    group_context: GroupContextDep,
):
    """
    Delete a complete chat session with group filtering.

    Args:
        session_id: Chat session identifier
        service: Chat history service injected by dependency
        group_context: Group context from headers
    """
    if not group_context or not group_context.is_valid():
        raise BadRequestError("No valid group context provided")

    deleted = await service.delete_session(
        session_id=session_id, group_context=group_context
    )

    if not deleted:
        raise NotFoundError("Chat session not found")


@router.post("/sessions/new", response_model=dict, status_code=status.HTTP_201_CREATED)
async def create_new_chat_session(
    service: ChatHistoryServiceDep,
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
    if not group_context or not group_context.is_valid():
        raise BadRequestError("No valid group context provided")

    session_id = service.generate_session_id()

    return {"session_id": session_id}


# ---------------------------------------------------------------------------
# Named chat sessions (chat-mode workspace). Server-side replacement for the
# browser IndexedDB session store: sessions live in SQLite locally and in
# Lakebase when a Lakebase backend is active (smart-routed session).
# ---------------------------------------------------------------------------


@router.post(
    "/sessions",
    response_model=NamedChatSessionResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_named_session(
    request: ChatSessionCreateRequest,
    service: ChatHistoryServiceDep,
    group_context: GroupContextDep,
):
    """Create a named chat session for the current user and workspace."""
    if not group_context or not group_context.is_valid():
        raise BadRequestError("No valid group context provided")

    user_id = group_context.group_email or "unknown_user"
    record = await service.create_named_session(
        user_id=user_id,
        title=request.title,
        session_id=request.id,
        group_context=group_context,
    )
    return NamedChatSessionResponse.model_validate(record)


@router.get("/sessions/named", response_model=List[NamedChatSessionResponse])
async def list_named_sessions(
    service: ChatHistoryServiceDep,
    group_context: GroupContextDep,
    page: int = Query(0, ge=0, description="Page number (0-based)"),
    per_page: int = Query(50, ge=1, le=100, description="Sessions per page"),
):
    """List the current user's named sessions in this workspace (most recent first)."""
    if not group_context or not group_context.is_valid():
        raise BadRequestError("No valid group context provided")

    user_id = group_context.group_email or "unknown_user"
    records = await service.list_named_sessions(
        user_id=user_id, page=page, per_page=per_page, group_context=group_context
    )
    return [NamedChatSessionResponse.model_validate(r) for r in records]


@router.put("/sessions/{session_id}", response_model=NamedChatSessionResponse)
async def rename_named_session(
    session_id: Annotated[str, Path(..., description="Chat session identifier")],
    request: ChatSessionRenameRequest,
    service: ChatHistoryServiceDep,
    group_context: GroupContextDep,
):
    """Rename a named chat session (group-checked)."""
    if not group_context or not group_context.is_valid():
        raise BadRequestError("No valid group context provided")

    record = await service.rename_named_session(
        session_id=session_id, title=request.title, group_context=group_context
    )
    if not record:
        raise NotFoundError("Chat session not found")
    return NamedChatSessionResponse.model_validate(record)


# ---------------------------------------------------------------------------
# Per-session preview + in-flight job marker. Server-side replacement for the
# browser IndexedDB stores, so previews and refresh-reconnect survive reload
# and follow the user across browsers/devices.
# ---------------------------------------------------------------------------


@router.get("/sessions/{session_id}/preview", response_model=SessionPreviewResponse)
async def get_session_preview(
    session_id: Annotated[str, Path(..., description="Chat session identifier")],
    service: ChatHistoryServiceDep,
    group_context: GroupContextDep,
):
    """Return the session's rendered preview, or all-null when it has none."""
    if not group_context or not group_context.is_valid():
        raise BadRequestError("No valid group context provided")
    preview = await service.get_preview(session_id, group_context=group_context)
    if not preview:
        return SessionPreviewResponse()
    return SessionPreviewResponse(**preview)


@router.put("/sessions/{session_id}/preview", status_code=status.HTTP_204_NO_CONTENT)
async def save_session_preview(
    session_id: Annotated[str, Path(..., description="Chat session identifier")],
    request: SavePreviewRequest,
    service: ChatHistoryServiceDep,
    group_context: GroupContextDep,
):
    """Save (replace) the session's rendered preview (group-checked)."""
    if not group_context or not group_context.is_valid():
        raise BadRequestError("No valid group context provided")
    ok = await service.set_preview(
        session_id,
        request.type,
        request.data,
        request.title,
        group_context=group_context,
    )
    if not ok:
        raise NotFoundError("Chat session not found")


@router.delete("/sessions/{session_id}/preview", status_code=status.HTTP_204_NO_CONTENT)
async def delete_session_preview(
    session_id: Annotated[str, Path(..., description="Chat session identifier")],
    service: ChatHistoryServiceDep,
    group_context: GroupContextDep,
):
    """Clear the session's rendered preview (group-checked)."""
    if not group_context or not group_context.is_valid():
        raise BadRequestError("No valid group context provided")
    # Missing session → treat as already-clear (idempotent).
    await service.set_preview(session_id, None, None, None, group_context=group_context)


@router.get("/sessions/{session_id}/running-job", response_model=RunningJobResponse)
async def get_session_running_job(
    session_id: Annotated[str, Path(..., description="Chat session identifier")],
    service: ChatHistoryServiceDep,
    group_context: GroupContextDep,
):
    """Return the session's in-flight crew job id (for refresh reconnect), or null."""
    if not group_context or not group_context.is_valid():
        raise BadRequestError("No valid group context provided")
    job_id = await service.get_running_job(session_id, group_context=group_context)
    return RunningJobResponse(job_id=job_id)


@router.put(
    "/sessions/{session_id}/running-job", status_code=status.HTTP_204_NO_CONTENT
)
async def set_session_running_job(
    session_id: Annotated[str, Path(..., description="Chat session identifier")],
    request: SetRunningJobRequest,
    service: ChatHistoryServiceDep,
    group_context: GroupContextDep,
):
    """Record the in-flight crew job for a session (group-checked)."""
    if not group_context or not group_context.is_valid():
        raise BadRequestError("No valid group context provided")
    ok = await service.set_running_job(
        session_id, request.job_id, group_context=group_context
    )
    if not ok:
        raise NotFoundError("Chat session not found")


@router.delete(
    "/sessions/{session_id}/running-job", status_code=status.HTTP_204_NO_CONTENT
)
async def clear_session_running_job(
    session_id: Annotated[str, Path(..., description="Chat session identifier")],
    service: ChatHistoryServiceDep,
    group_context: GroupContextDep,
):
    """Clear the session's in-flight crew job marker (run finished/stopped)."""
    if not group_context or not group_context.is_valid():
        raise BadRequestError("No valid group context provided")
    # Idempotent: missing session → nothing to clear.
    await service.set_running_job(session_id, None, group_context=group_context)


@router.put("/messages/{message_id}", response_model=ChatHistoryResponse)
async def update_chat_message(
    message_id: Annotated[str, Path(..., description="Message identifier")],
    request: UpdateMessageRequest,
    service: ChatHistoryServiceDep,
    group_context: GroupContextDep,
):
    """Update a message in place (streaming append / result attach), group-checked."""
    if not group_context or not group_context.is_valid():
        raise BadRequestError("No valid group context provided")

    updated = await service.update_message(
        message_id=message_id,
        group_context=group_context,
        content=request.content,
        intent=request.intent,
        generation_result=request.generation_result,
    )
    if not updated:
        raise NotFoundError("Chat message not found")
    return updated
