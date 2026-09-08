"""Shared request-level intent policy for dispatch and its preview endpoint."""

from typing import TYPE_CHECKING, Any

from src.schemas.dispatcher import DispatcherRequest
from src.utils.user_context import GroupContext

if TYPE_CHECKING:
    from src.services.chat.dispatcher import DispatcherService


async def detect_request_intent(
    service: "DispatcherService",
    request: DispatcherRequest,
    group_context: GroupContext | None,
    available_tools: list[dict[str, str]] | None,
    default_model: str,
) -> dict[str, Any]:
    # Builder/API design requests honour the picker throughout generation.
    # Chat answer runs retain the fast classification policy (and normally
    # bypass classification altogether). The walker still handles failures
    # and records whichever model actually answered.
    builder_request = not request.chat_mode and not request.auto_execute
    model = (request.model or default_model) if builder_request else default_model
    return await service.detect_intent_logged(
        request.message,
        model,
        group_context,
        available_tools,
        chat_mode=request.chat_mode,
        last_resort_model=request.model,
        prefer_existing=request.prefer_existing,
    )
