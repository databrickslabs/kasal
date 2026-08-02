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
* ``create_crew``     — authoring, for admins and editors.
* ``get_run_status``  — the canonical state, plus the prompt when a run is
  waiting for a human.
* ``get_run_result``  — the finished output.
* ``cancel_run``      — stop it.
* ``respond_to_run``  — answer a run that paused for approval.

plus **one tool per published capability**, which is how a run is started.

Deliberately absent: a BLOCKING ``run_crew``. Crew runs take minutes; such a
tool would pass testing against a small crew and time out in production.

Also absent, and this one was here until the transport caught up:
``list_crews`` and ``start_crew``. They named the published set at runtime and
ran something out of it — a second way to do what calling the capability's own
tool already does, and one that costs an extra round trip and an extra decision
for the calling agent. Their real job was working around a tool list that could
not refresh: a client fetched it once at ``initialize`` and had no way to learn
that something had been published since. Now that the server declares
``tools.listChanged`` and pushes ``notifications/tools/list_changed`` down the
GET stream (see ``sessions.py``), the list a client holds is current, and the
pair had nothing left to do that calling the tool does not do better.

The one thing lost with them: a capability published under a built-in tool's
name is skipped from the list to avoid shadowing, and there is no longer a
generic runner to reach it by name. It has to be renamed. That is a fair trade
for a surface with one obvious way to run something — and the skip is logged.

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
from src.services.publications.publication import PublicationService

logger = logging.getLogger(__name__)


class UnknownToolError(Exception):
    """The caller asked for a tool that is not advertised.

    Defined here rather than in server.py because server imports this module at
    module scope; the reverse direction is a circular import. All the adapter's
    errors living together is the better arrangement anyway.
    """


class UnknownCapabilityError(Exception):
    """The caller named a capability that is not published to it.

    Raised by the REST surface in ``mcp_server_router.py``. The JSON-RPC
    transport answers the same case as :class:`UnknownToolError`, because there
    a capability IS a tool.
    """


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
                "process": {
                    "type": "string",
                    "enum": ["sequential", "hierarchical", "parallel"],
                    "description": (
                        "How the crew runs. sequential: tasks in order (default). "
                        "hierarchical: a manager delegates to the agents. "
                        "parallel: independent tasks run concurrently."
                    ),
                },
                "reasoning": {
                    "type": "boolean",
                    "description": (
                        "Give the agents the model's native reasoning budget. "
                        "Models without one ignore it."
                    ),
                },
                "reasoning_effort": {
                    "type": "string",
                    "enum": ["low", "medium", "high"],
                    "description": "How much reasoning budget. Implies reasoning.",
                },
                "manager_llm": {
                    "type": "string",
                    "description": "Manager model for a hierarchical crew.",
                },
            },
            "required": ["prompt"],
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
    process: Optional[str] = None,
    reasoning: Optional[bool] = None,
    reasoning_effort: Optional[str] = None,
    manager_llm: Optional[str] = None,
    session: Any = None,
) -> Dict[str, Any]:
    """Build a new crew. Admin and editor only — the check is in the EIL."""
    return await authoring.create_crew(
        caller=caller,
        prompt=prompt,
        name=name,
        model=model,
        tools=tools,
        process=process,
        reasoning=reasoning,
        reasoning_effort=reasoning_effort,
        manager_llm=manager_llm,
        session=session,
    )


#: Dispatch table. A tool that is not here is not callable, which keeps the
#: advertised list and the executable set from drifting apart.
TOOL_HANDLERS = {
    "ask_kasal": ask_kasal,
    "create_crew": create_crew,
    "get_run_status": get_run_status,
    "get_run_result": get_run_result,
    "cancel_run": cancel_run,
    "respond_to_run": respond_to_run,
}


# ---------------------------------------------------------------------------
# One tool per published capability — how a run is started.
#
# A calling agent selects on DESCRIPTIONS. A tool called `analyse_powerbi_model`
# that says when to use it gets chosen; a generic runner requires the agent to
# be told out of band which names exist, and costs a discovery call before every
# run. Same rows, better discovery — and exactly symmetric with the A2A card's
# skills[], which has projected per-capability entries from the start.
#
# This used to be "Layer 2", sitting beside a generic list/start pair. The pair
# is gone: see the module docstring for why the refreshable tool list is what
# made it redundant.
# ---------------------------------------------------------------------------

#: Names the fixed tools already occupy. A crew published under one of these
#: would shadow a control tool, so it is skipped and logged rather than silently
#: winning or silently losing.
_RESERVED_NAMES = frozenset(t["name"] for t in TOOL_DEFINITIONS)


def _capability_hint(capability: Any) -> str:
    """What a calling agent needs to know about the thing behind the tool.

    A flow is not a crew with more steps: it can pause at an approval gate, and
    a conversational one carries a thread across calls, so a follow-up belongs
    in the same capability instead of a fresh run. Saying "crew" for all of them
    told a client the opposite of both.
    """
    if getattr(capability, "entity_type", "crew") != "flow":
        return (
            "Starts a crew and returns a run id immediately — crews take "
            "minutes, so poll get_run_status."
        )
    if getattr(capability, "conversational", False):
        return (
            "Starts a flow that holds a CONVERSATION and returns a run id "
            "immediately — poll get_run_status. Send follow-ups to this same "
            "tool with the same session_id: it continues the thread rather than "
            "starting over."
        )
    return (
        "Starts a flow — several crews with routing between them — and returns "
        "a run id immediately; poll get_run_status. A flow may pause for human "
        "approval, in which case the status is input_required and respond_to_run "
        "answers it."
    )


async def build_crew_tool_definitions(
    caller: ExternalCaller, session: Any = None
) -> List[Dict[str, Any]]:
    """One tool definition per capability this caller may see: crews and flows."""
    service = PublicationService(session)
    capabilities = await service.list_capabilities(caller)

    definitions: List[Dict[str, Any]] = []
    for capability in capabilities:
        if capability.name in _RESERVED_NAMES:
            logger.warning(
                "[mcp-server] published capability %r shadows a built-in tool; skipping",
                capability.name,
            )
            continue
        teamspace = getattr(capability, "teamspace", None)
        definitions.append(
            {
                "name": capability.name,
                "description": (
                    f"{capability.description}\n\n{_capability_hint(capability)}"
                    # Which teamspace it belongs to. A caller identified by email
                    # alone sees every teamspace they are a member of, so a tool
                    # with no teamspace on it leaves the reader unable to tell
                    # whose data a run will touch.
                    + (f"\nTeamspace: {teamspace}." if teamspace else "")
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
                            "description": "What you want this capability to do.",
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
    """Invoke a Layer-2 tool — start the crew OR flow published under ``name``.

    Which of the two it is is decided by ``start_run`` from the publication's
    entity type, so nothing here has to know: an external caller invokes a
    capability and has no reason to care which engine path runs it.
    """
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
        "[mcp-server] %s called published %s %s (groups=%s)",
        caller.origin,
        getattr(publication, "entity_type", "crew"),
        name,
        caller.group_ids,
    )
    result = await start_run(
        caller=caller, publication=publication, inputs=arguments, session=session
    )
    return result.as_dict()
