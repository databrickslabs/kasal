"""Startup schema self-heal — the only thing that ALTERs an existing database.

``run_schema_self_heal`` is re-exported from ``src.db.session`` too; that is the
import path ``main.py`` and the Lakebase service use.
"""

from src.db.self_heal.runner import run_schema_self_heal

__all__ = ["run_schema_self_heal"]
