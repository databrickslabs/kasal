"""What the startup schema self-heal could not apply, for ``GET /health/db``.

The report is kept where the heal runs (``src/db/self_heal/report.py``); routers
may not import ``src.db`` (import-linter), so the health router reads it here.
Step names only, never error text: the endpoint needs no identity, and the
causes (table and role names) belong in the startup log.
"""

from typing import Union

from src.db.self_heal.report import schema_heal_summary


def schema_heal_status() -> dict[str, Union[str, list[str]]]:
    """``status`` ("ok", "incomplete" or "not_run"), ``failed_steps``, ``databases``."""
    return schema_heal_summary()
