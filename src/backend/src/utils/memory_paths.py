"""Deterministic on-disk location for LOCAL (DEFAULT) memory.

The DEFAULT backend is kasal's own SQLite store — ``memory.db`` under the group
directory, written by services/memory/local_storage_backend.py (embeddings
as float32 blobs, search by numpy cosine). It is NOT LanceDB: that was crewAI's
built-in default and left with the crewai library. A leftover
``<group>/memory/memories.lance/`` directory from before the engine migration is
dead weight and safe to delete.

crewAI resolved a *relative* ``CREWAI_STORAGE_DIR`` inconsistently — sometimes
under the process CWD (the backend source tree), sometimes under the platform
data dir (macOS Application Support, Linux ``~/.local/share``). That scattered
the store across locations and left the memory browser reading a different place
than the runtime wrote.

We pin an **absolute** root *outside* the source tree so the writer and the
browser always agree. Local dev: ``~/.kasal/memory``. Inside Databricks Apps the
default is app-relative (``core.databricks_app.apps_data_dir``) — and a warning
is logged, because that filesystem does not survive a redeploy: configure the
Lakebase memory backend there. ``KASAL_MEMORY_DIR`` overrides both.

Storage model mirrors ChatMode / Lakebase: ONE store per group
(``kasal_default_<group_id>``). Session scoping is NOT a separate directory — it
is encoded in each record's scope path (``/<group_id>/<session_id>/...``), so a
session record is visible both workspace-wide (group) and session-scoped.
"""

import logging
import os
import re
from pathlib import Path

from src.core.databricks_app import apps_data_dir

# Default root when KASAL_MEMORY_DIR is unset. Outside the backend source tree
# and writable on Linux; set KASAL_MEMORY_DIR to your data folder in production.
_DEFAULT_MEMORY_DIRNAME = ".kasal/memory"


def sanitize_dir_component(value: str) -> str:
    """Make a string safe to embed in a filesystem directory name."""
    return re.sub(r"[^A-Za-z0-9_.-]", "_", str(value))


def local_memory_root() -> Path:
    """Absolute base dir holding every group's local SQLite memory store.

    ``KASAL_MEMORY_DIR`` overrides the default (``~/.kasal/memory`` locally,
    app-relative inside Databricks Apps). The base dir is created if missing so
    the store can be written underneath it.
    """
    override = os.environ.get("KASAL_MEMORY_DIR")
    apps_dir = apps_data_dir("memory")
    if override:
        root = Path(override).expanduser()
    elif apps_dir is not None:
        root = apps_dir
    else:
        root = Path.home() / _DEFAULT_MEMORY_DIRNAME
    if apps_dir is not None:
        _warn_local_memory_in_apps(root)
    root.mkdir(parents=True, exist_ok=True)
    return root


_warned_in_apps = False


def warn_if_local_memory_is_ephemeral() -> None:
    """Startup check: inside Databricks Apps, warn where local memory would go.

    Any teamspace without the Lakebase memory backend uses the local store, and
    the app filesystem does not survive a redeploy. A no-op outside Apps.
    """
    if apps_data_dir("memory") is not None:
        local_memory_root()


def _warn_local_memory_in_apps(root: Path) -> None:
    """Once per process: local memory inside Apps is lost on redeploy."""
    global _warned_in_apps
    if _warned_in_apps:
        return
    _warned_in_apps = True
    logging.getLogger(__name__).warning(
        "Local (DEFAULT) memory is in use inside Databricks Apps at %s. The app "
        "filesystem does not survive a redeploy, so this memory will be lost: "
        "configure the Lakebase memory backend for durable memory.",
        root,
    )


def local_memory_store_dir(group_id: str) -> Path:
    """Absolute store dir for a group's unified local memory — one per group.

    Session scoping lives in the record scope path, never in the directory, so
    every run for a group (workspace- or session-scoped) shares this one store.
    """
    safe_group = sanitize_dir_component(group_id or "default")
    return local_memory_root() / f"kasal_default_{safe_group}"
