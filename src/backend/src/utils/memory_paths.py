"""Deterministic on-disk location for LOCAL (DEFAULT / LanceDB) cognitive memory.

CrewAI resolves a *relative* ``CREWAI_STORAGE_DIR`` inconsistently — sometimes
under the process CWD (the backend source tree), sometimes under the platform
data dir (macOS Application Support, Linux ``~/.local/share``). That scattered
the store across locations and left the memory browser reading a different place
than the runtime wrote.

We pin an **absolute** root *outside* the source tree so the writer and the
browser always agree, and so production (Linux) uses a known data folder.
Override the root with the ``KASAL_MEMORY_DIR`` environment variable.

Storage model mirrors ChatMode / Lakebase: ONE store per group
(``kasal_default_<group_id>``). Session scoping is NOT a separate directory — it
is encoded in each record's scope path (``/<group_id>/<session_id>/...``), so a
session record is visible both workspace-wide (group) and session-scoped.
"""

import os
import re
from pathlib import Path

# Default root when KASAL_MEMORY_DIR is unset. Outside the backend source tree
# and writable on Linux; set KASAL_MEMORY_DIR to your data folder in production.
_DEFAULT_MEMORY_DIRNAME = ".kasal/memory"


def sanitize_dir_component(value: str) -> str:
    """Make a string safe to embed in a filesystem directory name."""
    return re.sub(r"[^A-Za-z0-9_.-]", "_", str(value))


def local_memory_root() -> Path:
    """Absolute base dir holding every group's local LanceDB memory store.

    ``KASAL_MEMORY_DIR`` overrides the default ``~/.kasal/memory``. The base dir
    is created if missing so CrewAI can write the store underneath it.
    """
    override = os.environ.get("KASAL_MEMORY_DIR")
    root = (
        Path(override).expanduser()
        if override
        else Path.home() / _DEFAULT_MEMORY_DIRNAME
    )
    root.mkdir(parents=True, exist_ok=True)
    return root


def local_memory_store_dir(group_id: str) -> Path:
    """Absolute store dir for a group's unified local memory — one per group.

    Session scoping lives in the record scope path, never in the directory, so
    every run for a group (workspace- or session-scoped) shares this one store.
    """
    safe_group = sanitize_dir_component(group_id or "default")
    return local_memory_root() / f"kasal_default_{safe_group}"
