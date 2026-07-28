"""The tools Kasal advertises to MCP clients.

A thin adapter. Every tool takes a resolved :class:`ExternalCaller` as its FIRST
argument and does nothing but translate — the policy (who the caller is, what
their group may see, what a run's state means) lives in ``services/external/``
and is shared with the A2A surface.

Making the caller the first parameter is deliberate and structural: a tool
signature without one cannot compile against this module, so "the tool that
forgot to scope by group" is not a thing that can be written here by accident.

The tool set:

* ``ask_kasal``       — blocking, over the chat path. Fits an ordinary tool-call
  timeout because the chat path is in-process and sub-second.
* ``list_crews``      — the capability list, group-scoped.
* ``start_crew``      — returns a run id immediately; never waits.
* ``get_run_status``  — the canonical state, plus the prompt when a run is
  waiting for a human.
* ``get_run_result``  — the finished output.
* ``cancel_run``      — stop it.
* ``respond_to_run``  — answer a run that paused for approval.

Deliberately absent: a BLOCKING ``run_crew``. Crew runs take minutes; such a
tool would pass testing against a small crew and time out in production.

``respond_to_run`` is the tool that would not exist if MCP had been designed
alone. MCP has no notion of a call that pauses for a human; A2A does, and
because the state machine and the round-trip live in the shared layer, the MCP
surface gets the behaviour for the cost of one definition. A crew with an
approval gate is therefore fully usable from Claude Code or Cursor — which is a
differentiator, not a detail: almost nothing else an MCP client can call knows
how to ask a human mid-task.
"""

import logging
from typing import Any, Dict, List, Optional

from src.services.external import artifacts, interaction
from src.services.external.identity import ExternalCaller
from src.services.external.invocation import (
    ask,
)
from src.services.external.invocation import cancel_run as _cancel_run
from src.services.external.invocation import run_result as _run_result
from src.services.external.invocation import run_status as _run_status
from src.services.external.invocation import (
    start_run,
)
from src.services.external.publication import PublicationService

logger = logging.getLogger(__name__)


class UnknownCapabilityError(Exception):
    """The caller named a crew that is not published to it."""


class UnknownRunError(Exception):
    """The caller named a run it may not see, or that does not exist."""


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
    {
        "name": "start_crew",
        "description": (
            "Start a published crew. Returns a run id immediately — crews take "
            "minutes, so poll get_run_status rather than waiting."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "name": {
                    "type": "string",
                    "description": "The crew name from list_crews.",
                },
                "inputs": {
                    "type": "object",
                    "description": "Inputs for the crew, matching its input schema.",
                },
            },
            "required": ["name"],
        },
    },
    {
        "name": "get_run_status",
        "description": (
            "Check a run. Returns its state; when the state is input_required "
            "the run is waiting for a human answer — reply with respond_to_run."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {"run_id": {"type": "string"}},
            "required": ["run_id"],
        },
    },
    {
        "name": "get_run_result",
        "description": (
            "Fetch the output of a run. Returns the state too, so a run that has "
            "not finished yet answers with its state and no output."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {"run_id": {"type": "string"}},
            "required": ["run_id"],
        },
    },
    {
        "name": "cancel_run",
        "description": (
            "Stop a run that is still in progress. Already-finished runs are "
            "unaffected; use this to abandon work you no longer need."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {"run_id": {"type": "string"}},
            "required": ["run_id"],
        },
    },
    {
        "name": "respond_to_run",
        "description": (
            "Answer a run whose state is input_required, so it can continue. "
            "Reply with 'no' or 'reject' to decline; any other text approves "
            "and is recorded as the comment."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "run_id": {"type": "string"},
                "response": {"type": "string"},
                "approval_id": {
                    "type": "integer",
                    "description": "Only needed when several gates are pending.",
                },
            },
            "required": ["run_id", "response"],
        },
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
async def start_crew(
    caller: ExternalCaller,
    name: str,
    inputs: Optional[Dict[str, Any]] = None,
    session: Any = None,
) -> Dict[str, Any]:
    """Start a published crew, returning a handle."""
    service = PublicationService(session)
    publication = await service.resolve_capability(caller, name)
    if publication is None:
        # The single place a caller is authorised for a crew. None covers both
        # "no such capability" and "another tenant's" deliberately.
        raise UnknownCapabilityError(f"No published crew named {name!r}")

    result = await start_run(
        caller=caller, publication=publication, inputs=inputs, session=session
    )
    return result.as_dict()


async def get_run_status(
    caller: ExternalCaller, run_id: str, session: Any = None
) -> Dict[str, Any]:
    """A run's state, and what it is waiting for if it has paused."""
    result = await _run_status(caller, run_id, session=session)
    if result is None:
        raise UnknownRunError(f"No run {run_id!r}")

    payload = result.as_dict()

    # Surface the pending question INLINE. A caller that has to make a second
    # call to discover what a paused run wants will mostly not make it — and a
    # run waiting for an answer nobody knows to give is a run that times out.
    pending = await interaction.pending_for_run(caller, run_id, session=session)
    if pending:
        payload["state"] = "input_required"
        payload["waiting_for"] = [p.as_dict() for p in pending]
    return payload


async def get_run_result(
    caller: ExternalCaller, run_id: str, session: Any = None
) -> Dict[str, Any]:
    """A run's output, shaped by the shared artifact builder."""
    result = await _run_result(caller, run_id, session=session)
    if result is None:
        raise UnknownRunError(f"No run {run_id!r}")

    payload = result.as_dict()
    if result.output is not None:
        payload["artifact"] = artifacts.build(result.output).as_dict()
    return payload


async def cancel_run(
    caller: ExternalCaller, run_id: str, session: Any = None
) -> Dict[str, Any]:
    """Stop a run."""
    result = await _cancel_run(caller, run_id, session=session)
    if result is None:
        raise UnknownRunError(f"No run {run_id!r}")
    return result.as_dict()


async def respond_to_run(
    caller: ExternalCaller,
    run_id: str,
    response: str,
    approval_id: Optional[int] = None,
    session: Any = None,
) -> Dict[str, Any]:
    """Answer a run that paused for a human.

    The tool MCP would not have. See the module docstring.
    """
    accepted = await interaction.respond(
        caller=caller,
        run_id=run_id,
        response=response,
        approval_id=approval_id,
        session=session,
    )
    if not accepted:
        raise UnknownRunError(f"Run {run_id!r} is not waiting for a response")
    return {"run_id": run_id, "accepted": True}


TOOL_HANDLERS = {
    "ask_kasal": ask_kasal,
    "list_crews": list_crews,
    "start_crew": start_crew,
    "get_run_status": get_run_status,
    "get_run_result": get_run_result,
    "cancel_run": cancel_run,
    "respond_to_run": respond_to_run,
}
