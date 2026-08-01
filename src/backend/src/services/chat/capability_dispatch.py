"""A route decision -> the dispatch result ChatMode already knows how to run.

Deliberately produces the EXISTING ``execute_crew`` / ``execute_flow`` shapes
rather than a new one. The frontend already detects variables, renders the
input-variables card, builds a crew config, creates an execution and streams it
back; a new result type would mean reimplementing all of that beside it.

Two extra keys ride along — ``extracted_inputs`` and ``input_schema`` — so the
consumer can work out what is genuinely still missing instead of asking for
everything.

The third shape is the one that does not exist today: ``catalog_no_match``.
When the user has selected "Use existing" they have said *run what we have*, so
falling through to generation would run work they did not ask for and bill a full
crew run for it. It returns one line and a "build instead" offer, and the choice
stays theirs.
"""

import logging
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple
from uuid import UUID

from src.core.exceptions import ForbiddenError, NotFoundError
from src.models.crew_publication import Publication
from src.schemas.crew_publication import PublishedCapability
from src.services.catalog.templates import TemplateService
from src.services.chat.capability_router import (
    CHAT_PROTOCOL,
    RouteDecision,
    build_route_messages,
    continue_decision,
    held_conversation,
    parse_route_response,
)
from src.services.chat.conversation_context import (
    recent_turns,
    render_turns,
    turn_by_index,
)
from src.services.publications.publication import PublicationService
from src.utils.user_context import GroupContext

logger = logging.getLogger(__name__)

#: Ask the model chain for JSON, returning ``(parsed, served_model, attempted)``.
#: Supplied by the dispatcher, which owns the retry, circuit breakers, semaphore
#: and tracing — this module must not grow a second implementation of any of it.
AskModels = Callable[
    [List[Dict[str, str]]],
    Awaitable[Tuple[Optional[Dict[str, Any]], Optional[str], int]],
]

#: How much of a referenced answer rides into the run.
#:
#: Enough for a crew to work FROM it (a news summary, a report), not so much
#: that a routed follow-up carries a whole deck in its config on every turn.
REFERENCED_ANSWER_CHAR_CAP = 8000

#: Why no capability ran, phrased for the person who asked. They read
#: differently on purpose: an empty workspace needs a signpost to the publish
#: dialog, a genuine miss needs to say so, and a name that would not resolve is
#: our bug and must not be dressed up as the user's.
NO_MATCH_MESSAGES = {
    "nothing_published": (
        "Nothing is published to chat yet, so there is nothing to run. Publish a "
        "crew or flow to chat to make it available here."
    ),
    "no_match": "Nothing published to chat matches this.",
    "unresolved": "Nothing published to chat matches this.",
}


def no_match(reason: str, answer_here: bool = False) -> Dict[str, Any]:
    """The result for "we are not running anything, and here is why".

    ``build_instead`` is what the frontend hangs its one-click offer off: flip
    the source control back to "Build new" and re-dispatch at whatever answer
    mode was already selected.

    ``answer_here`` says the turn should be ANSWERED rather than left as a dead
    end — the router declined while a conversation was in progress, which is
    what "what is this Aviation sector" looks like from here. The offer to build
    stays beside the answer instead of replacing it: declining to run a crew is
    not the same as having nothing to say.

    Note what this does NOT do: start a crew. The rule "with Use existing on,
    never silently generate" is about spending a full crew run the user did not
    ask for. Answering in chat costs one light-agent turn and is the thing they
    were plainly asking for.
    """
    return {
        "type": "catalog_no_match",
        "reason": reason,
        "answer_here": answer_here,
        "message": NO_MATCH_MESSAGES.get(reason, NO_MATCH_MESSAGES["no_match"]),
        "build_instead": True,
    }


async def route_and_dispatch(
    session: Any,
    group_context: Optional[GroupContext],
    message: str,
    ask_models: AskModels,
    log_llm: Callable[..., Awaitable[Any]],
    catalog_service: Any,
    flow_service: Any,
    session_id: Optional[str] = None,
    allow_continuation: bool = True,
) -> Dict[str, Any]:
    """The whole "Use existing" turn: read the catalog, pick, resolve, render.

    Never falls through to generation. With the source control set to reuse the
    user asked to run something they already have, so quietly building a crew
    instead would run work they did not ask for and bill a full crew run for it.
    A miss returns ``catalog_no_match`` and the frontend offers the build — one
    click, at the answer mode they already had selected.
    """
    group_ids = list(getattr(group_context, "group_ids", None) or [])
    publications = PublicationService(session)
    capabilities = await publications.list_capabilities_for_group(
        group_ids, CHAT_PROTOCOL
    )

    # Nothing published to chat — say so without spending an LLM call, and point
    # at the publish dialog rather than leaving a dead end.
    if not capabilities:
        logger.info("[capability_router] nothing published to chat for this group")
        return no_match("nothing_published")

    # The routing prompt is a template row like every other prompt here, so a
    # group can override it and GEPA can optimise it. The catalog goes in the
    # user message, not into the template.
    system_prompt = await TemplateService.get_effective_template_content(
        "route_capability", group_context
    )
    # The conversation, so the turn is read as a turn and not as an isolated
    # instruction. Best-effort: no session, or a failed read, and the router
    # decides exactly as it did before.
    turns = await recent_turns(session, session_id, group_ids, exclude_message=message)
    parsed, used_model, attempted = await ask_models(
        build_route_messages(message, capabilities, system_prompt, render_turns(turns))
    )

    if parsed is None:
        # Every candidate failed, or every breaker was open. Declining is the
        # only honest answer: we cannot tell a genuine miss from an outage, and
        # running the wrong capability is worse than running nothing.
        logger.warning(
            "[capability_router] no model answered (%d attempted); declining",
            attempted,
        )
        return no_match("no_match")

    await log_llm(
        endpoint="capability-route",
        prompt=message,
        response=str(parsed),
        model=used_model,
        status="success",
        group_context=group_context,
    )

    continued = False
    decision = parse_route_response(parsed, message, capabilities)
    if decision is None or not decision.is_confident:
        # Before declining: is a capability mid-conversation here? A flow that
        # holds a conversation expects the next turn ITSELF, and a follow-up to
        # one is usually a fragment ("and Germany?", "shorter") that matches
        # nothing in the catalogue on its own words. Declining would answer it
        # in the chat and leave the flow never knowing the turn happened — its
        # state would silently stop tracking the conversation the user is
        # having.
        held = held_conversation(turns, capabilities) if allow_continuation else None
        if held is not None:
            logger.info(
                "[capability_router] continuing %s: it is mid-conversation and "
                "the router declined (confidence=%s)",
                held.name,
                getattr(decision, "confidence", None),
            )
            decision = continue_decision(held, message)
            continued = True
        else:
            logger.info(
                "[capability_router] declining: capability=%s confidence=%s reason=%s",
                getattr(decision, "capability", None),
                getattr(decision, "confidence", None),
                getattr(decision, "reason", ""),
            )
            # Mid-conversation, declining almost always means the turn is ABOUT
            # what is already on screen — a question, not a request. Answer it.
            # With no conversation behind it there is nothing to answer from, so
            # the offer to build is all there is.
            return no_match("no_match", answer_here=bool(turns))

    # ALWAYS through resolve_capability_for_group — the single authorisation
    # choke point, which returns None for "does not exist" and "another
    # tenant's" alike so a name cannot be used as a cross-tenant oracle.
    publication = await publications.resolve_capability_for_group(
        group_ids, CHAT_PROTOCOL, decision.capability
    )
    if publication is None:
        # The router named something the catalog we handed it does not resolve.
        # That is a prompt bug, not a user error, and it is logged as one.
        logger.warning(
            "[capability_router] %r did not resolve; the route catalog and the "
            "resolve disagree",
            decision.capability,
        )
        return no_match("unresolved")

    capability = next((c for c in capabilities if c.name == decision.capability), None)
    logger.info(
        "[capability_router] %s -> %s (confidence %.2f, %d input(s) bound)",
        publication.entity_type,
        decision.capability,
        decision.confidence,
        len(decision.inputs),
    )
    # The answer this request works FROM, when it works from one. Resolved
    # against the turns actually rendered, so a number the model could not have
    # read binds nothing — the same stance as a value whose span is not in the
    # message.
    referenced = turn_by_index(turns, decision.refers_to)
    if decision.refers_to is not None and referenced is None:
        logger.warning(
            "[capability_router] refers_to=%s does not name an answer it was "
            "shown; binding nothing",
            decision.refers_to,
        )

    return await build_dispatch_result(
        decision,
        publication,
        capability,
        catalog_service,
        flow_service,
        group_context,
        message,
        referenced.content if referenced else None,
        continued,
    )


def _entity_key(entity_id: str) -> Any:
    """``entity_id`` as the owning service expects it.

    It is persisted as a string on the publication because it addresses two id
    types, but both ``Crew.id`` and ``Flow.id`` are UUID columns — asyncpg
    rejects a ``str`` with "'str' object has no attribute 'hex'". Falls back to
    the raw string so a non-UUID id (SQLite, fixtures) still reaches a service
    that can handle it.
    """
    try:
        return UUID(entity_id)
    except (ValueError, AttributeError, TypeError):
        return entity_id


async def build_dispatch_result(
    decision: RouteDecision,
    publication: Publication,
    capability: Optional[PublishedCapability],
    catalog_service: Any,
    flow_service: Any,
    group_context: GroupContext,
    message: str = "",
    referenced_answer: Optional[str] = None,
    continued: bool = False,
) -> Dict[str, Any]:
    """Load the published entity and render it as an execute_* result."""
    if (publication.entity_type or "crew") == "flow":
        return await _build_flow_result(
            decision,
            publication,
            capability,
            flow_service,
            group_context,
            message,
            referenced_answer,
            continued,
        )
    return await _build_crew_result(
        decision,
        publication,
        capability,
        catalog_service,
        message,
        referenced_answer,
        continued,
    )


async def _build_crew_result(
    decision: RouteDecision,
    publication: Publication,
    capability: Optional[PublishedCapability],
    catalog_service: Any,
    message: str = "",
    referenced_answer: Optional[str] = None,
    continued: bool = False,
) -> Dict[str, Any]:
    crew = await catalog_service.get(_entity_key(publication.entity_id))
    if crew is None:
        logger.warning(
            "[capability_router] %s resolves to crew %s, which no longer exists",
            publication.external_name,
            publication.entity_id,
        )
        return no_match("unresolved")

    return {
        "type": "execute_crew",
        "plan": {
            "id": str(crew.id),
            "name": crew.name,
            "nodes": crew.nodes or [],
            "edges": crew.edges or [],
            "process": crew.process,
            "memory": crew.memory,
            "verbose": crew.verbose,
            "max_rpm": crew.max_rpm,
        },
        # One plain line, no card. The user typed a sentence; the answer is what
        # they want, and naming what is running is all the acknowledgement the
        # turn needs. See the execute_crew case in ChatMessage.
        "message": f"Running **{crew.name}**",
        **_routing_payload(
            decision, publication, capability, message, referenced_answer, continued
        ),
    }


async def _build_flow_result(
    decision: RouteDecision,
    publication: Publication,
    capability: Optional[PublishedCapability],
    flow_service: Any,
    group_context: GroupContext,
    message: str = "",
    referenced_answer: Optional[str] = None,
    continued: bool = False,
) -> Dict[str, Any]:
    # Unlike the crew leg, this RAISES rather than returning None for a flow that
    # has been deleted — a `flow is None` check would sail straight past it. The
    # group-checked variant asserts the tenant filter a second time; the
    # publication resolve already applied it, and belt-and-braces is cheap on a
    # surface where the failure is cross-tenant.
    try:
        flow = await flow_service.get_flow_with_group_check(
            _entity_key(publication.entity_id), group_context
        )
    except NotFoundError:
        logger.warning(
            "[capability_router] %s resolves to flow %s, which no longer exists",
            publication.external_name,
            publication.entity_id,
        )
        return no_match("unresolved")
    except ForbiddenError:
        # The publication passed the group filter but the flow it points at did
        # not. That is not a permission decision about this user — it is the two
        # rows disagreeing about which tenant owns the thing.
        logger.error(
            "[capability_router] publication %s is visible to this group but flow "
            "%s is not; the rows disagree about ownership",
            publication.external_name,
            publication.entity_id,
        )
        return no_match("unresolved")

    return {
        "type": "execute_flow",
        "flow": {
            "id": str(flow.id),
            "name": flow.name,
            "nodes": flow.nodes or [],
            "edges": flow.edges or [],
            "flow_config": flow.flow_config or {},
        },
        "message": f"Running **{flow.name}**",
        **_routing_payload(
            decision, publication, capability, message, referenced_answer, continued
        ),
    }


def _routing_payload(
    decision: RouteDecision,
    publication: Publication,
    capability: Optional[PublishedCapability],
    message: str = "",
    referenced_answer: Optional[str] = None,
    continued: bool = False,
) -> Dict[str, Any]:
    """The keys that make this a ROUTED run rather than a picked-from-a-list one.

    ``input_schema`` travels with the result because it is the authority on what
    is required. Without it the consumer falls back to treating every detected
    ``{placeholder}`` as required — correct, but it interrogates the user for
    cosmetic ones too.
    """
    schema = capability.input_schema if capability is not None else None
    return {
        "extracted_inputs": decision.inputs,
        "input_schema": schema if schema is not None else publication.input_schema,
        "capability": publication.external_name,
        "routed_from": CHAT_PROTOCOL,
        # Whether this capability holds a conversation, and whether THIS turn
        # was routed to it because it was already holding one. The UI needs both:
        # the first to know a follow-up will stay here, the second to say so on
        # the turn it happened.
        "conversational": bool(getattr(capability, "conversational", False)),
        "continued": bool(continued),
        # The sentence that selected this capability, carried back so the run can
        # send it as `user_request`. It is what memory recall queries on: a saved
        # crew's task description is identical on every run, so without it recall
        # matches the crew's own history rather than this run's subject.
        "request": message,
        # The earlier answer this run works FROM, when the router pointed at one.
        # "Turn this into a deck" is useless if the deck crew starts from
        # nothing: it goes and re-gathers, and on a polluted memory pool it
        # re-gathers the wrong subject entirely. Handing it the actual text is
        # both cheaper and the only version that answers what was asked.
        #
        # Capped: an assistant turn can be a whole deck, and this rides in the
        # run config on every routed follow-up.
        "referenced_answer": (
            referenced_answer[:REFERENCED_ANSWER_CHAR_CAP]
            if referenced_answer
            else None
        ),
    }
