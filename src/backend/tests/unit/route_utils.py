"""Flatten a FastAPI app's routes to their path strings.

FastAPI 0.141 / starlette 1.x registers each ``include_router()`` call as one
lazy ``_IncludedRouter`` entry in ``app.routes`` — it has NO ``.path`` of its
own; the real, fully-prefixed routes come from ``effective_candidates()``.
Every test that used to write ``[r.path for r in app.routes]`` goes through
this helper instead of crashing on the wrapper.
"""

from typing import List


def route_paths(app) -> List[str]:
    out: List[str] = []
    _walk(app.routes, out)
    return out


def _walk(routes, out: List[str]) -> None:
    for route in routes:
        path = getattr(route, "path", None)
        if path is not None:
            out.append(path)
            continue
        # Nested include_router() calls nest _IncludedRouters — recurse.
        candidates = getattr(route, "effective_candidates", None)
        if callable(candidates):
            _walk(candidates(), out)
