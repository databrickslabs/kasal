"""FastAPI session, request-context and generic dependency providers."""

import logging
import os
from typing import Annotated, Callable, Optional, Type

from fastapi import Depends, Header, HTTPException, Request
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.base_repository import BaseRepository
from src.core.base_service import BaseService
from src.core.exceptions import KasalError, UnauthorizedError
from src.db.base import Base
from src.db.database_router import get_smart_db_session
from src.db.session import get_db, get_local_db
from src.utils.request_identity import (
    resolve_request_identity,
    running_in_databricks_apps,
)
from src.utils.user_context import GroupContext

logger = logging.getLogger("src.core.dependencies")

# Type definitions for dependencies
# Use smart session that automatically selects between regular DB and Lakebase
SessionDep = Annotated[AsyncSession, Depends(get_smart_db_session)]
# Ordinary mutation endpoints must commit before their response is sent. Keep
# SessionDep for streams and consumers that need a session during response work.
WriteSessionDep = Annotated[
    AsyncSession, Depends(get_smart_db_session, scope="function")
]
# Keep legacy session dependency for backward compatibility if needed
LegacySessionDep = Annotated[AsyncSession, Depends(get_db)]
# Always use the LOCAL database (SQLite/PG), bypassing Lakebase swap.
# Used for bootstrap config tables like database_configs.
LocalSessionDep = Annotated[AsyncSession, Depends(get_local_db)]


async def get_group_context(
    request: Request,
    x_forwarded_email: Optional[str] = Header(None, alias="X-Forwarded-Email"),
    x_forwarded_access_token: Optional[str] = Header(
        None, alias="X-Forwarded-Access-Token"
    ),
    x_auth_request_email: Optional[str] = Header(None, alias="X-Auth-Request-Email"),
    x_auth_request_user: Optional[str] = Header(None, alias="X-Auth-Request-User"),
    x_auth_request_access_token: Optional[str] = Header(
        None, alias="X-Auth-Request-Access-Token"
    ),
    x_group_id: Optional[str] = Header(None, alias="group_id"),
    x_group_domain: Optional[str] = Header(None, alias="X-Group-Domain"),
) -> GroupContext:
    """Resolve the caller's workspace (GroupContext), or refuse the request.

    Identity comes from :func:`src.utils.request_identity.resolve_request_identity`:
    inside Databricks Apps only the ``X-Forwarded-*`` headers the platform proxy
    sets are trusted; elsewhere oauth2-proxy's ``X-Auth-Request-*`` headers are
    preferred with ``X-Forwarded-*`` as the fallback. ``group_id`` selects the
    workspace and is validated against the user's memberships.

    FAILS CLOSED. Every protected API route depends on this, so it is the
    identity gate for the API:

    - no identity                              -> 401
    - identity that resolves to no workspace   -> 401 (e.g. a non-email name)
    - workspace not permitted / not resolvable -> 403
    - unexpected resolver failure              -> 503

    It used to return an empty ``GroupContext`` in the first, second and last
    cases, and some repositories read "no groups" as "no filter" (audit M1).
    Routes that must stay public (health checks) simply do not depend on it;
    ``tests/unit/architecture/test_api_routes_require_identity.py`` keeps that
    list explicit.

    PERFORMANCE: Uses request-scoped caching to avoid repeated database queries
    when multiple dependencies need the GroupContext in the same request.
    """
    # Native EventSource cannot attach the local-development headers used by
    # the Axios client. Direct loopback SSE URLs therefore carry the same email
    # and selected workspace as query parameters. Never honor those parameters
    # in Databricks Apps, production, non-SSE routes, or non-loopback requests.
    path = getattr(getattr(request, "url", None), "path", "")
    client_host = getattr(getattr(request, "client", None), "host", "")
    production = running_in_databricks_apps() or os.getenv(
        "ENVIRONMENT", ""
    ).strip().lower() in ("production", "prod")
    local_sse = (
        not production
        and isinstance(path, str)
        and "/sse/" in path
        and client_host in ("127.0.0.1", "::1", "localhost")
    )
    query = request.query_params if local_sse else {}
    sse_email = query.get("_sse_email") if local_sse else None
    sse_group_id = query.get("_sse_group_id") if local_sse else None

    identity = resolve_request_identity(
        forwarded_email=x_forwarded_email,
        forwarded_access_token=x_forwarded_access_token,
        auth_request_email=x_auth_request_email,
        auth_request_user=x_auth_request_user,
        auth_request_access_token=x_auth_request_access_token,
    )
    # Query values are local SSE fallbacks only; header identity always wins.
    user_email = identity.email or sse_email
    x_group_id = x_group_id or sse_group_id
    access_token = identity.access_token

    if not user_email:
        logger.debug("Request to %s carries no identity; refusing with 401", path)
        raise UnauthorizedError("Authentication required")

    # Request-scoped cache: keyed by email and group_id to handle switching.
    cache_key = f"group_context:{user_email}:{x_group_id}"
    cache = getattr(request.state, "_group_context_cache", None)
    if isinstance(cache, dict):
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

    try:
        group_context = await GroupContext.from_email(
            email=user_email,
            access_token=access_token,
            group_id=x_group_id,  # Pass the selected group ID from header
        )
    except ValueError as e:
        # SECURITY: unauthorized workspace, or one that could not be resolved.
        logger.warning(f"Unauthorized group access attempt: {e}")
        raise HTTPException(status_code=403, detail=str(e))
    except Exception as e:
        # An unexpected failure must not become an empty context: that reads
        # as "no tenant filter" in some queries. Tell the client to retry.
        logger.error(f"Error resolving group context for {user_email}: {e}")
        raise KasalError(
            "Could not resolve the caller's workspace; try again", status_code=503
        )

    if not group_context.group_ids:
        # from_email returns an empty context for an identity it cannot map to
        # a workspace (no "@"). That is not an authenticated tenant.
        logger.warning("Identity from %s resolved to no workspace", identity.source)
        raise UnauthorizedError("Authentication required")

    logger.debug(
        "Resolved group context: primary_group_id=%s, group_ids=%s, role=%s",
        group_context.primary_group_id,
        group_context.group_ids,
        group_context.user_role,
    )
    if not isinstance(cache, dict):
        cache = {}
        request.state._group_context_cache = cache
    cache[cache_key] = group_context
    return group_context


async def get_request_email(
    x_forwarded_email: Annotated[
        Optional[str], Header(alias="X-Forwarded-Email")
    ] = None,
    x_auth_request_email: Annotated[
        Optional[str], Header(alias="X-Auth-Request-Email")
    ] = None,
) -> str:
    """The caller's email, via the shared resolver; 401 when there is none.

    For the few routes that need the bare identity rather than a workspace.
    """
    identity = resolve_request_identity(
        forwarded_email=x_forwarded_email, auth_request_email=x_auth_request_email
    )
    if not identity.email:
        raise UnauthorizedError("Authentication required")
    return identity.email


RequestEmailDep = Annotated[str, Depends(get_request_email)]

# Type definitions for group-aware dependencies
GroupContextDep = Annotated[GroupContext, Depends(get_group_context)]


def get_repository(
    repository_class: Type[BaseRepository], model_class: Type[Base]
) -> Callable[[SessionDep], BaseRepository]:
    """
    Factory function for repository dependencies.

    Args:
        repository_class: Repository class to instantiate
        model_class: Model class to use with the repository

    Returns:
        Callable: Dependency function that returns a repository instance
    """

    def _get_repo(session: SessionDep) -> BaseRepository:
        return repository_class(model_class, session)

    return _get_repo


def get_service(
    service_class: Type[BaseService],
    repository_class: Type[BaseRepository],
    model_class: Type[Base],
) -> Callable[[SessionDep], BaseService]:
    """
    Factory function for service dependencies.

    Args:
        service_class: Service class to instantiate
        repository_class: Repository class to use with the service
        model_class: Model class to use with the repository

    Returns:
        Callable: Dependency function that returns a service instance
    """

    def _get_service(session: SessionDep) -> BaseService:
        # The consistent pattern across services is to have session as the first parameter,
        # with repository_class and model_class as optional parameters with defaults
        try:
            # Create service with session and default repo/model classes
            service = service_class(session)
            return service
        except Exception:
            # Handle any initialization errors with fallback
            # If the service expects additional parameters, this will catch it
            try:
                # Try creating with explicit repository and model classes
                service = service_class(
                    session=session,
                    repository_class=repository_class,
                    model_class=model_class,
                )
                return service
            except Exception as inner_e:
                # Log the error and re-raise
                logger.error(f"Error creating service: {inner_e}")
                raise

    return _get_service
