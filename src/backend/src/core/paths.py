"""
Where the backend root is — resolved once, by a marker, not by counting.

Two defaults in this codebase resolved paths as
``Path(__file__).parent.parent.parent / "x"``. That is correct only from a module
at one exact depth, and both broke:

- ``services/llm/manager.py`` started writing LLM logs and the litellm disk cache
  into ``backend/src/logs`` — inside the source tree, in production as well as in
  tests — the moment it moved out of ``core/``.
- ``settings.py`` used ``"./app.db"``, which is worse: it resolves against the
  process CWD, so stray app.db files appeared at the repo root, in ``src/`` and
  even in ``src/frontend/``.

Walking up for a marker file is immune to both. Import this instead of counting.
"""

from pathlib import Path


def _find_backend_root() -> Path:
    """The directory holding pyproject.toml — i.e. src/backend."""
    for candidate in Path(__file__).resolve().parents:
        if (candidate / "pyproject.toml").is_file():
            return candidate
    # Installed without the source tree (a wheel): fall back to the historical
    # layout rather than raising at import time.
    return Path(__file__).resolve().parent.parent.parent


#: src/backend — the anchor for every default path the backend writes to.
BACKEND_ROOT = _find_backend_root()
