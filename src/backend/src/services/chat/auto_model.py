"""Resolve Auto for a chat message before anything calls a model.

The dispatcher uses the request's model for intent detection, generation and
the answer run, so "auto" is replaced here, once per message, before any of
them. The run the message starts then arrives at ``ExecutionService`` with a
concrete model, and records this pick in its trace (see
``services/execution/config/auto_model.selection_for``).
"""

from typing import Any, Optional

from src.schemas.dispatcher import DispatcherRequest
from src.services.decisions.model_selection import (
    ModelSelection,
    is_auto,
    select_for_workspace,
)


async def resolve_dispatch_model(
    request: DispatcherRequest, session: Any, group_context: Any
) -> Optional[ModelSelection]:
    """Replace ``request.model == "auto"``; None when the message named a model."""
    if not is_auto(request.model):
        return None
    prompt = request.original_prompt or request.message
    selection = await select_for_workspace(session, group_context, prompt)
    request.model = selection.model
    return selection
