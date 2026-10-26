"""The Agent Card — Kasal's A2A discovery document.

Workspace-scoped: Kasal presents as ONE agent whose ``skills[]`` are the crews
this caller's workspace has published. A card per crew was the alternative; a
single card is one endpoint and one discovery story, and it lets the caller pick
a skill rather than having to already know a URL.

``skills[]`` is a projection of ``PublicationService.list_capabilities`` — the
SAME call the MCP tool list renders from. That is not an implementation detail
to preserve by discipline: there is only one query, so the two surfaces cannot
advertise different capabilities.

The card is a PROMISE. Capability flags are only flipped once the behaviour is
real — ``pushNotifications`` stays false until delivery exists, because
advertising a webhook that never arrives is worse than advertising none.
"""

import logging
from typing import Any, Optional

from src.schemas.a2a import (
    AgentCapabilities,
    AgentCard,
    AgentInterface,
    AgentProvider,
    AgentSkill,
    SecurityScheme,
)
from src.services.external.identity import ExternalCaller
from src.services.publications.publication import PublicationService

logger = logging.getLogger(__name__)

#: Pinned deliberately. A2A reached v1.0 recently and the well-known URI is
#: still pending IANA registration, so the card states which revision it was
#: written against and the router is versioned alongside it.
PROTOCOL_VERSION = "1.0"

#: Kasal's own version as an A2A agent. Independent of the app version: it
#: describes this SURFACE's contract, which changes when skills or capabilities
#: change, not when the product ships a UI fix.
AGENT_VERSION = "1.0.0"


async def build_card(
    caller: ExternalCaller,
    base_url: str,
    session: Any = None,
    workspace_name: Optional[str] = None,
) -> AgentCard:
    """The card this caller should see.

    Caller-specific by necessity: ``skills[]`` is group-scoped, so an
    unauthenticated card would either leak every workspace's capabilities or
    advertise none. Kasal requires identity to serve one, which is a deviation
    from the "public discovery document" reading of the spec and the correct
    one for a multi-tenant host.
    """
    service = PublicationService(session)
    capabilities = await service.list_capabilities(caller, protocol="a2a")

    skills = [
        AgentSkill(
            id=cap.name,
            name=cap.name,
            description=cap.description,
            inputSchema=cap.input_schema,
        )
        for cap in capabilities
    ]

    name = workspace_name or "Kasal"
    return AgentCard(
        protocolVersion=PROTOCOL_VERSION,
        name=name,
        description=(
            f"{name} runs multi-agent crews. Each skill below is a crew this "
            "workspace has published; send a message naming one to start it. "
            "Crews take minutes, so tasks are asynchronous — poll GetTask."
        ),
        version=AGENT_VERSION,
        provider=AgentProvider(organization=name),
        capabilities=AgentCapabilities(
            # True because message:stream and tasks/{id}:subscribe now render
            # the shared stream frames as TaskStatusUpdateEvent /
            # TaskArtifactUpdateEvent over SSE, terminating on final. It was
            # false while only Kasal's own frame shape existed — a client cannot
            # use a stream it has to guess the schema of.
            streaming=True,
            # True because delivery now exists: subscribers are registered per
            # task and POSTed a status-update on every state change, from the
            # same choke point that announces to the browser. The flag was false
            # for as long as that was untrue — a card is a promise, and
            # advertising a webhook that never arrives is worse than advertising
            # none.
            pushNotifications=True,
            stateTransitionHistory=False,
        ),
        securitySchemes={
            "databricks_obo": SecurityScheme(
                type="http",
                scheme="bearer",
                description=(
                    "Work runs on the CALLER's Databricks token (on-behalf-of). "
                    "Present it as a bearer token or via X-Forwarded-Access-Token. "
                    "Without one, tasks answer TASK_STATE_AUTH_REQUIRED."
                ),
            )
        },
        security=[{"databricks_obo": []}],
        interfaces=[AgentInterface(url=f"{base_url.rstrip('/')}/a2a/v1")],
        skills=skills,
    )
