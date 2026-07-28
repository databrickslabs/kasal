"""Kasal as an A2A agent — the endpoint other agents call.

Versioned at ``/a2a/v1`` because A2A reached v1.0 recently, the well-known URI
is still pending IANA registration, and external clients pin behaviour.

Transport is HTTP+JSON. JSON-RPC 2.0 and gRPC are both permitted bindings and
both add a layer for no gain here: Kasal is a FastAPI service and its callers
speak HTTP.

As with the MCP surface, identity is resolved in a dependency, so an endpoint
cannot reach a task operation without a caller.
"""

import logging
from typing import Annotated, Optional

from fastapi import APIRouter, Depends, Header, Query, Request

from src.core.dependencies import SessionDep
from src.core.exceptions import KasalError, NotFoundError, UnprocessableEntityError
from src.schemas.a2a import AgentCard, SendMessageRequest, Task
from src.services.a2a import card as a2a_card
from src.services.a2a import tasks as a2a_tasks
from src.services.external.identity import (
    ExternalAuthError,
    ExternalCaller,
    resolve_caller,
)
from src.services.external.permissions import ExternalPermissionError

#: Task operations. Mounted under the API prefix like every other router.
router = APIRouter(tags=["a2a"], responses={404: {"description": "Not found"}})

#: The Agent Card, mounted at the application ROOT — see well_known_router below.
well_known_router = APIRouter(tags=["a2a"])

logger = logging.getLogger(__name__)


class A2AAuthRequired(KasalError):
    """401 — the caller must authenticate.

    The HTTP face of ``TASK_STATE_AUTH_REQUIRED``: work here runs on the
    caller's own Databricks token, so a caller without one is told to present
    it rather than watching a task fail for reasons it cannot act on.
    """

    status_code = 401
    detail = "Authentication required"


async def get_a2a_caller(
    x_forwarded_email: Annotated[
        Optional[str], Header(alias="X-Forwarded-Email")
    ] = None,
    x_forwarded_access_token: Annotated[
        Optional[str], Header(alias="X-Forwarded-Access-Token")
    ] = None,
    x_auth_request_email: Annotated[
        Optional[str], Header(alias="X-Auth-Request-Email")
    ] = None,
    x_auth_request_access_token: Annotated[
        Optional[str], Header(alias="X-Auth-Request-Access-Token")
    ] = None,
    x_group_id: Annotated[Optional[str], Header(alias="X-Group-Id")] = None,
) -> ExternalCaller:
    """Resolve the calling agent, or refuse."""
    try:
        return await resolve_caller(
            protocol="a2a",
            email=x_auth_request_email or x_forwarded_email,
            access_token=x_auth_request_access_token or x_forwarded_access_token,
            group_id=x_group_id,
        )
    except ExternalAuthError as exc:
        logger.warning("[a2a] refused caller: %s", exc.detail)
        raise A2AAuthRequired(exc.detail)


CallerDep = Annotated[ExternalCaller, Depends(get_a2a_caller)]


@well_known_router.get("/.well-known/agent.json", response_model=AgentCard)
async def agent_card(request: Request, caller: CallerDep, session: SessionDep):
    """The Agent Card.

    Served at the DOMAIN ROOT, not under the API prefix. A well-known URI is
    discovery by convention: an A2A client fetches
    https://host/.well-known/agent.json without being told where to look, so a
    card at /api/v1/.well-known/agent.json is a card nothing will ever find.
    This router is therefore included on the app directly rather than with the
    API prefix.

    Requires identity, which is a deliberate deviation from reading the card as
    a fully public document: ``skills[]`` is group-scoped, so an anonymous card
    would have to either leak every workspace's capabilities or advertise none.
    For a multi-tenant host, identity-scoped discovery is the only correct
    reading.
    """
    # The CARD lives at the domain root, but the task operations it points to
    # are on the prefixed api_router. The interface URL must therefore carry the
    # API prefix — a card advertising /a2a/v1 while the endpoints are at
    # /api/v1/a2a/v1 sends every client straight into a 404.
    from src.config.settings import settings

    base_url = str(request.base_url).rstrip("/") + settings.API_V1_STR
    return await a2a_card.build_card(caller, base_url=base_url, session=session)


@router.post("/a2a/v1/message:send", response_model=Task)
async def send_message(
    body: SendMessageRequest, caller: CallerDep, session: SessionDep
):
    """Start a task, or answer one that is waiting for input.

    Returns the task handle IMMEDIATELY. Crew runs take minutes and the budget
    work contemplates an hour, so this must never block — the caller polls
    GetTask.
    """
    try:
        return await a2a_tasks.send_message(
            caller=caller,
            message=body.message,
            skill_id=body.skillId,
            task_id=body.taskId,
            session=session,
        )
    except a2a_tasks.UnknownSkillError as exc:
        raise NotFoundError(str(exc))
    except a2a_tasks.UnknownTaskError as exc:
        raise NotFoundError(str(exc))
    except ExternalAuthError as exc:
        raise A2AAuthRequired(exc.detail)
    except ExternalPermissionError as exc:
        from src.core.exceptions import ForbiddenError

        raise ForbiddenError(exc.detail)
    except ValueError as exc:
        raise UnprocessableEntityError(str(exc))


@router.get("/a2a/v1/tasks/{task_id}", response_model=Task)
async def get_task(task_id: str, caller: CallerDep, session: SessionDep):
    """A task's state, its output, or the question it is waiting on."""
    try:
        return await a2a_tasks.get_task(caller, task_id, session=session)
    except a2a_tasks.UnknownTaskError as exc:
        # 404 for "does not exist" AND "not yours" — task ids must not become an
        # oracle for other workspaces' activity.
        raise NotFoundError(str(exc))


@router.post("/a2a/v1/tasks/{task_id}:cancel", response_model=Task)
async def cancel_task(task_id: str, caller: CallerDep, session: SessionDep):
    """Stop a task."""
    try:
        return await a2a_tasks.cancel_task(caller, task_id, session=session)
    except a2a_tasks.UnknownTaskError as exc:
        raise NotFoundError(str(exc))


@router.get("/a2a/v1/tasks")
async def list_tasks(
    caller: CallerDep,
    session: SessionDep,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
):
    """This caller's tasks, group-scoped.

    An unscoped ListTasks is a cross-tenant leak in a single call, which is why
    the scoping is in the service rather than left to this handler.
    """
    return {"tasks": await a2a_tasks.list_tasks(caller, limit=limit, session=session)}
