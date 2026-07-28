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

from src.services.external import artifacts, authoring, interaction
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
from src.services.external.permissions import (
    AUTHOR_ROLES,
    RUN_ROLES,
    require_role,
)
from src.services.external.publication import PublicationService

logger = logging.getLogger(__name__)


class UnknownToolError(Exception):
    """The caller asked for a tool that is not advertised.

    Defined here rather than in server.py because server imports this module at
    module scope; the reverse direction is a circular import. All the adapter's
    errors living together is the better arrangement anyway.
    """


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
        "name": "create_crew",
        "description": (
            "Build a NEW crew in this workspace from a description of what it "
            "should do. Requires the admin or editor role. The crew is added to "
            "the catalogue but is NOT reachable from outside until someone "
            "publishes it."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "prompt": {
                    "type": "string",
                    "description": "What the crew should do, in plain language.",
                },
                "name": {
                    "type": "string",
                    "description": "Catalogue name. Derived from the prompt if omitted.",
                },
                "model": {"type": "string", "description": "Optional model override."},
                "tools": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Optional tool names the crew may use.",
                },
            },
            "required": ["prompt"],
        },
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
    """Blocking question over the chat path. Any workspace member may ask."""
    require_role(caller, RUN_ROLES)
    result = await ask(caller=caller, question=question, model=model, session=session)
    return result.as_dict()


async def list_crews(caller: ExternalCaller, session: Any = None) -> Dict[str, Any]:
    """The capabilities this caller may see.

    Reads through ``PublicationService``, the SAME call the A2A card's
    ``skills[]`` is built from — the two surfaces cannot advertise different
    capabilities because there is only one list.
    """
    require_role(caller, RUN_ROLES)
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
    require_role(caller, RUN_ROLES)
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
    require_role(caller, RUN_ROLES)
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
    require_role(caller, RUN_ROLES)
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
    require_role(caller, RUN_ROLES)
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
    require_role(caller, RUN_ROLES)
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


async def create_crew(
    caller: ExternalCaller,
    prompt: str,
    name: Optional[str] = None,
    model: Optional[str] = None,
    tools: Optional[List[str]] = None,
    session: Any = None,
) -> Dict[str, Any]:
    """Build a new crew. Admin and editor only — the check is in the EIL."""
    return await authoring.create_crew(
        caller=caller,
        prompt=prompt,
        name=name,
        model=model,
        tools=tools,
        session=session,
    )


TOOL_HANDLERS = {
    "ask_kasal": ask_kasal,
    "create_crew": create_crew,
    "list_crews": list_crews,
    "start_crew": start_crew,
    "get_run_status": get_run_status,
    "get_run_result": get_run_result,
    "cancel_run": cancel_run,
    "respond_to_run": respond_to_run,
}


# ---------------------------------------------------------------------------
# Layer 2 — one tool per published crew.
#
# The generic start_crew works, but a calling agent selects on DESCRIPTIONS. A
# tool called `analyse_powerbi_model` that says when to use it gets chosen; a
# generic `start_crew` requires the agent to be told out of band which crew
# names exist. Same rows, better discovery — and exactly symmetric with the A2A
# card's skills[], which has projected per-crew capabilities from the start.
# ---------------------------------------------------------------------------

#: Names the fixed tools already occupy. A crew published under one of these
#: would shadow a control tool, so it is skipped and logged rather than silently
#: winning or silently losing.
_RESERVED_NAMES = frozenset(t["name"] for t in TOOL_DEFINITIONS)


async def build_crew_tool_definitions(
    caller: ExternalCaller, session: Any = None
) -> List[Dict[str, Any]]:
    """One tool definition per crew this caller may see."""
    service = PublicationService(session)
    capabilities = await service.list_capabilities(caller)

    definitions: List[Dict[str, Any]] = []
    for capability in capabilities:
        if capability.name in _RESERVED_NAMES:
            logger.warning(
                "[mcp-server] published crew %r shadows a built-in tool; skipping",
                capability.name,
            )
            continue
        definitions.append(
            {
                "name": capability.name,
                "description": (
                    f"{capability.description}\n\n"
                    "Starts a crew and returns a run id immediately — crews take "
                    "minutes, so poll get_run_status."
                ),
                "inputSchema": capability.input_schema
                or {
                    # No declared input contract, so accept one free-text request.
                    # Better than an empty schema, which tells a calling agent it
                    # can pass nothing at all.
                    "type": "object",
                    "properties": {
                        "request": {
                            "type": "string",
                            "description": "What you want this crew to do.",
                        }
                    },
                },
            }
        )
    return definitions


async def call_crew_tool(
    caller: ExternalCaller,
    name: str,
    arguments: Dict[str, Any],
    session: Any = None,
) -> Dict[str, Any]:
    """Invoke a Layer-2 tool — i.e. start the crew published under ``name``."""
    require_role(caller, RUN_ROLES)
    service = PublicationService(session)
    publication = await service.resolve_capability(caller, name)
    if publication is None:
        # Genuinely unknown: not a fixed tool, and not a capability published to
        # this caller. Raised as UnknownToolError so the router answers 404 the
        # same way it does for a misspelt built-in — a caller must not be able
        # to distinguish "no such tool" from "another tenant's tool".
        raise UnknownToolError(f"Unknown tool: {name}")

    logger.info(
        "[mcp-server] %s called published crew %s (groups=%s)",
        caller.origin,
        name,
        caller.group_ids,
    )
    result = await start_run(
        caller=caller, publication=publication, inputs=arguments, session=session
    )
    return result.as_dict()
