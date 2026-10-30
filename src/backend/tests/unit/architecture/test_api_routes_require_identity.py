"""Every API route resolves a caller identity, except an explicit public list.

The identity gate is a dependency, not middleware: ``get_group_context`` raises
401 when a request carries no identity (audit M1), and the external surfaces
(MCP, A2A) refuse through their own caller dependencies. That only protects a
route that DEPENDS on one of them, so this test walks every registered route
and fails on one that neither resolves an identity nor is listed below.

Adding a route to ``PUBLIC_ROUTES`` is a security decision: say why in the
comment beside it.
"""

from typing import Iterator, Tuple

from fastapi.routing import APIRoute

# (method, path) pairs that are deliberately reachable without an identity.
PUBLIC_ROUTES = {
    # Liveness/readiness probes: the platform calls these without a user.
    ("GET", "/health"),
    ("GET", "/api/v1/health"),
    ("GET", "/api/v1/health/db"),
    ("GET", "/api/v1/health/cache"),
    ("GET", "/api/v1/templates/health"),
    ("GET", "/api/v1/executions/health"),
    ("GET", "/api/v1/api/converters/health"),
    ("GET", "/api/v1/sse/health"),
    # MCP transport session termination: forgets a notification queue by its
    # unguessable id and authorises nothing (see mcp_endpoint_delete).
    ("DELETE", "/mcp"),
}

# Routes that resolve the caller INSIDE the handler, through the shared
# resolver, and answer 401 themselves (the MCP JSON-RPC transport must return a
# JSON-RPC error body, so it cannot use a raising dependency).
SELF_AUTHENTICATING_ROUTES = {
    ("POST", "/mcp"),
    ("GET", "/mcp"),
}

_IDENTITY_DEPENDENCIES = {
    "get_group_context",
    "get_request_email",
    "get_a2a_caller",
    "get_external_caller",
}


def _iter_routes(routes) -> Iterator[Tuple[str, set, object]]:
    """Yield (path, methods, dependant) for every APIRoute, however nested."""
    for route in routes:
        if isinstance(route, APIRoute):
            yield route.path, set(route.methods or ()), route.dependant
            continue
        contexts = getattr(route, "effective_route_contexts", None)
        if callable(contexts):
            for ctx in contexts():
                dependant = getattr(ctx, "dependant", None)
                if dependant is not None:
                    yield ctx.path, set(ctx.methods or ()), dependant


def _resolves_identity(dependant) -> bool:
    for dep in dependant.dependencies:
        if getattr(dep.call, "__name__", "") in _IDENTITY_DEPENDENCIES:
            return True
        if _resolves_identity(dep):
            return True
    return False


def _collect():
    from src.main import app

    return list(_iter_routes(app.routes))


def test_every_route_resolves_identity_or_is_explicitly_public():
    routes = _collect()
    # Guard against the walker silently finding nothing on a FastAPI upgrade.
    assert len(routes) > 100, f"route walker found only {len(routes)} routes"

    allowed = PUBLIC_ROUTES | SELF_AUTHENTICATING_ROUTES
    unprotected = sorted(
        (method, path)
        for path, methods, dependant in routes
        for method in methods - {"HEAD", "OPTIONS"}
        if (method, path) not in allowed and not _resolves_identity(dependant)
    )
    assert not unprotected, (
        "These routes resolve no caller identity. Depend on GroupContextDep "
        "(or RequestEmailDep), or add them to PUBLIC_ROUTES with a reason: "
        f"{unprotected}"
    )


def test_allowlist_has_no_stale_entries():
    present = {(method, path) for path, methods, _ in _collect() for method in methods}
    stale = sorted((PUBLIC_ROUTES | SELF_AUTHENTICATING_ROUTES) - present)
    assert not stale, f"Allow-listed routes no longer exist: {stale}"
