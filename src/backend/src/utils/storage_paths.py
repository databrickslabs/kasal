"""Storage paths.

Authored module; surface validated against the kasal_engine datamodel.
"""

import os
from pathlib import Path

from src.core.databricks_app import apps_data_dir


def db_storage_path() -> str:
    """Directory for engine-local storage (flow checkpoints, memory dbs).

    Honors CREWAI_STORAGE_DIR for drop-in compatibility with kasal's current
    configuration, then KASAL_ENGINE_STORAGE_DIR, else a per-user data dir
    locally and an app-relative one inside Databricks Apps (not ``~``, which is
    not where an app keeps data; neither survives a redeploy).
    """
    configured = os.environ.get("CREWAI_STORAGE_DIR") or os.environ.get(
        "KASAL_ENGINE_STORAGE_DIR"
    )
    apps_dir = apps_data_dir("engine")
    if configured:
        path = Path(configured)
    elif apps_dir is not None:
        path = apps_dir
    else:
        path = Path.home() / ".local" / "share" / "kasal_engine"
    path.mkdir(parents=True, exist_ok=True)
    return str(path)
