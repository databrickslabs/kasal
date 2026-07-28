"""Creating things in a workspace from outside it.

Everything else in the EIL reads or runs; this is the only module that lets an
external caller CREATE, so every entry point starts with the same role check
crews_router uses on its write endpoints: admin or editor.

That is the whole rule. An admin of a workspace can build a crew from an
external agent for exactly the reason they can build one in the UI — they are an
admin of that workspace, and the role travelled with them through the
GroupContext. An operator cannot, in either place.

No new generation logic lives here. It goes through
``CrewGenerationService.create_crew_complete`` — the same call the browser makes
— so an externally-authored crew is indistinguishable from a hand-built one.
"""

import logging
from typing import Any, Dict, List, Optional

from src.services.external.identity import ExternalCaller
from src.services.external.permissions import AUTHOR_ROLES, require_role

logger = logging.getLogger(__name__)


async def create_crew(
    caller: ExternalCaller,
    prompt: str,
    model: Optional[str] = None,
    tools: Optional[List[str]] = None,
    session: Any = None,
) -> Dict[str, Any]:
    """Generate a crew from a natural-language description.

    Admin and editor only. An operator gets a permission error naming the role
    it would need, rather than a silent failure or a generic 403 it cannot act
    on.

    Returns the created agents and tasks. The crew is NOT published by
    definition — publishing is a separate, deliberate decision, and a tool that
    both created and exposed a crew in one call would make that decision
    invisible.
    """
    require_role(caller, AUTHOR_ROLES)

    if not prompt or not prompt.strip():
        raise ValueError("prompt must not be empty")

    from src.schemas.crew import CrewGenerationRequest
    from src.services.generation.crews import CrewGenerationService

    service = CrewGenerationService(session)
    result = await service.create_crew_complete(
        CrewGenerationRequest(prompt=prompt, model=model, tools=tools or []),
        caller.group_context,
    )

    agents = result.get("agents", []) or []
    tasks = result.get("tasks", []) or []
    logger.info(
        "[external] %s created a crew: %d agents, %d tasks",
        caller.origin,
        len(agents),
        len(tasks),
    )

    return {
        "agents": [
            {"id": str(a.get("id", "")), "name": a.get("name") or a.get("role")}
            for a in agents
            if isinstance(a, dict)
        ],
        "tasks": [
            {"id": str(t.get("id", "")), "name": t.get("name")}
            for t in tasks
            if isinstance(t, dict)
        ],
        "note": (
            "Created in the workspace catalogue. It is NOT reachable from "
            "outside until someone publishes it."
        ),
    }
