"""Start progressive crew generation, or await it inside a traced builder turn."""

import asyncio
import uuid
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from src.core.events.bus import current_event_context
from src.core.sse_manager import sse_manager
from src.services.chat.streaming_request import streaming_request_for

if TYPE_CHECKING:
    from src.schemas.dispatcher import DispatcherRequest
    from src.services.generation.crews import CrewGenerationService
    from src.utils.user_context import GroupContext


async def dispatch_progressive(
    service: "CrewGenerationService",
    request: "DispatcherRequest",
    prompt: Optional[str],
    tools: Optional[List[Any]],
    group_context: Optional["GroupContext"],
    mlflow_enabled: bool,
) -> Dict[str, Any]:
    trace_job_id = current_event_context().get("generation_job_id")
    generation_id = trace_job_id or str(uuid.uuid4())
    sse_manager.register_job_owner(
        generation_id, getattr(group_context, "primary_group_id", None)
    )
    work = service.create_crew_progressive(
        streaming_request_for(request, prompt, tools),
        group_context,
        generation_id,
        mlflow_enabled=mlflow_enabled,
    )
    if trace_job_id:
        # The parent turn owns trace lifetime, so it must include every detail
        # call and retry, not just the initial dispatcher completion.
        await work
        terminal = sse_manager.get_terminal_event(
            generation_id,
            # No group: the job was registered without an owner (above).
            group_ids=group_context.group_ids if group_context else None,
        )
        if terminal is not None and terminal.event == "generation_failed":
            raise ValueError(terminal.data.get("error", "Crew generation failed"))
        return {
            "generation_id": generation_id,
            "type": "streaming",
            "completed": True,
            "generated_crew": (
                {key: terminal.data.get(key, []) for key in ("agents", "tasks")}
                if terminal
                else {}
            ),
        }
    asyncio.create_task(work)
    return {"generation_id": generation_id, "type": "streaming"}
