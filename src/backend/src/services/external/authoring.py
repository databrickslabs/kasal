"""Creating things in a workspace from outside it.

Everything else in the EIL reads or runs; this is the only module that lets an
external caller CREATE, so every entry point starts with the same role check
crews_router uses on its write endpoints: admin or editor.

That is the whole rule. An admin of a workspace can build a crew from an
external agent for exactly the reason they can build one in the UI — they are an
admin of that workspace, and the role travelled with them through the
GroupContext. An operator cannot, in either place.

No new generation logic lives here. Generation goes through
``CrewGenerationService.create_crew_complete`` — the same call the browser makes
— and the catalogue entry through ``CrewService.create_with_group``, so an
externally-authored crew is indistinguishable from a hand-built one.

Both steps are needed, which is easy to get wrong: create_crew_complete creates
AGENTS and TASKS and returns them, and creates no Crew row at all. The browser
assembles the crew afterwards from the canvas. Without that second step an
external caller is told "created", nothing appears in the catalogue, and the
result cannot be published — publication addresses a crew id, and there is none.
"""

import logging
from typing import Any, Dict, List, Optional

from src.services.external.identity import ExternalCaller
from src.services.external.permissions import AUTHOR_ROLES, require_role

logger = logging.getLogger(__name__)


async def create_crew(
    caller: ExternalCaller,
    prompt: str,
    name: Optional[str] = None,
    model: Optional[str] = None,
    tools: Optional[List[str]] = None,
    session: Any = None,
) -> Dict[str, Any]:
    """Generate a crew from a natural-language description.

    Admin and editor only. An operator gets a permission error naming the role
    it would need, rather than a silent failure or a generic 403 it cannot act
    on.

    Returns the crew id along with its agents and tasks. The crew IS saved to
    the catalogue, so it is visible in the workspace and can be published.

    It is not published, though. Exposing a crew to the outside is a separate
    deliberate decision, and a tool that created and exposed one in a single
    call would make that decision invisible to the workspace it affects.
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

    agent_ids = [
        str(a.get("id")) for a in agents if isinstance(a, dict) and a.get("id")
    ]
    task_ids = [str(x.get("id")) for x in tasks if isinstance(x, dict) and x.get("id")]

    crew_id = await _save_to_catalogue(
        name=name or _name_from_prompt(prompt),
        agent_ids=agent_ids,
        task_ids=task_ids,
        group_context=caller.group_context,
        session=session,
    )

    return {
        "crew_id": crew_id,
        "agents": [
            {"id": str(a.get("id", "")), "name": a.get("name") or a.get("role")}
            for a in agents
            if isinstance(a, dict)
        ],
        "tasks": [
            {"id": str(x.get("id", "")), "name": x.get("name")}
            for x in tasks
            if isinstance(x, dict)
        ],
        "note": (
            "Saved to the workspace catalogue. It is NOT reachable from outside "
            "until someone publishes it."
        ),
    }


def _name_from_prompt(prompt: str) -> str:
    """A readable catalogue name from the request.

    First words rather than the whole prompt: the catalogue shows this on a
    card, and a paragraph there is unreadable.
    """
    words = prompt.strip().split()
    name = " ".join(words[:6])
    return (name[:1].upper() + name[1:]) if name else "Untitled crew"


async def _save_to_catalogue(
    name: str,
    agent_ids: List[str],
    task_ids: List[str],
    group_context: Any,
    session: Any,
) -> Optional[str]:
    """Create the Crew row that makes the generated agents and tasks a CREW.

    Best-effort about the canvas: nodes and edges are the UI's layout, and a
    crew with none still opens, runs and publishes. Failing the whole call for
    want of node positions would be the wrong trade.
    """
    from src.schemas.crew import CrewCreate
    from src.services.catalog.crews import CrewService

    crew = await CrewService(session).create_with_group(
        CrewCreate(
            name=name, agent_ids=agent_ids, task_ids=task_ids, nodes=[], edges=[]
        ),
        group_context,
    )
    return str(crew.id) if crew is not None else None
