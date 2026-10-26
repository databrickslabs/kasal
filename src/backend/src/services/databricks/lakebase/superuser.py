"""Compatibility exports for Lakebase DDL helpers; implementation lives in db."""

from src.db.lakebase_ddl import (
    SUPERUSER_ROLE,
    _run_isolated_async,
    _run_isolated_sync,
    enable_pgvector_async,
    enable_pgvector_sync,
    enter_superuser_async,
    enter_superuser_sync,
)

__all__ = [
    "SUPERUSER_ROLE",
    "_run_isolated_async",
    "_run_isolated_sync",
    "enable_pgvector_async",
    "enable_pgvector_sync",
    "enter_superuser_async",
    "enter_superuser_sync",
]
