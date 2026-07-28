"""The tools Kasal advertises to MCP clients.

A thin adapter. Every tool takes a resolved :class:`ExternalCaller` as its FIRST
argument and does nothing but translate — the policy (who the caller is, what
their group may see, what a run's state means) lives in ``services/external/``
and is shared with the A2A surface.

Making the caller the first parameter is deliberate and structural: a tool
signature without one cannot compile against this module, so "the tool that
forgot to scope by group" is not a thing that can be written here by accident.

Phase 2 ships two tools:

* ``ask_kasal``  — blocking, over the chat path. Fits an ordinary tool-call
  timeout because the chat path is in-process and sub-second.
* ``list_crews`` — the capability list, group-scoped.

Deliberately absent: a blocking ``run_crew``. Crew runs take minutes; such a
tool would pass testing against a small crew and time out in production. Crews
become reachable through async handles in phase 3.
"""

import logging
from typing import Any, Dict, List, Optional

from src.services.external.identity import ExternalCaller
from src.services.external.invocation import ask
from src.services.external.publication import PublicationService

logger = logging.getLogger(__name__)


#: The advertised tool definitions. Kept as data rather than decorators so the
#: list can be asserted against in tests and rendered into the MCP capability
#: response without importing the server machinery.
TOOL_DEFINITIONS: List[Dict[str, Any]] = [
    {
        "name": "ask_kasal",
        "description": (
            "Ask this Kasal workspace a question and get a direct answer. "
            "Answers immediately using a single agent — use this for questions, "
            "not for long-running analysis."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "question": {
                    "type": "string",
                    "description": "The question to answer.",
                },
                "model": {
                    "type": "string",
                    "description": "Optional model override.",
                },
            },
            "required": ["question"],
        },
    },
    {
        "name": "list_crews",
        "description": (
            "List the crews this workspace has published for external use, with "
            "a description of what each one does and when to use it."
        ),
        "inputSchema": {"type": "object", "properties": {}},
    },
]


async def ask_kasal(
    caller: ExternalCaller,
    question: str,
    model: Optional[str] = None,
    session: Any = None,
) -> Dict[str, Any]:
    """Blocking question over the chat path."""
    result = await ask(caller=caller, question=question, model=model, session=session)
    return result.as_dict()


async def list_crews(caller: ExternalCaller, session: Any = None) -> Dict[str, Any]:
    """The capabilities this caller may see.

    Reads through ``PublicationService``, the SAME call the A2A card's
    ``skills[]`` is built from — the two surfaces cannot advertise different
    capabilities because there is only one list.
    """
    service = PublicationService(session)
    capabilities = await service.list_capabilities(caller)
    return {
        "crews": [
            {
                "name": c.name,
                "description": c.description,
                "input_schema": c.input_schema,
            }
            for c in capabilities
        ]
    }


#: Dispatch table. A tool that is not here is not callable, which keeps the
#: advertised list and the executable set from drifting apart.
TOOL_HANDLERS = {
    "ask_kasal": ask_kasal,
    "list_crews": list_crews,
}
