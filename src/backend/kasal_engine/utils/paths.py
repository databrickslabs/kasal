"""Storage paths.

Authored module; surface validated against the kasal_engine datamodel.
"""

import os
from pathlib import Path


def db_storage_path() -> str:
    """Directory for engine-local storage (flow checkpoints, memory dbs).

    Honors CREWAI_STORAGE_DIR for drop-in compatibility with kasal's current
    configuration, then KASAL_ENGINE_STORAGE_DIR, else a per-user data dir.
    """
    configured = os.environ.get("CREWAI_STORAGE_DIR") or os.environ.get(
        "KASAL_ENGINE_STORAGE_DIR"
    )
    if configured:
        path = Path(configured)
    else:
        path = Path.home() / ".local" / "share" / "kasal_engine"
    path.mkdir(parents=True, exist_ok=True)
    return str(path)
