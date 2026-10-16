"""Run each self-heal piece in its own SAVEPOINT, and keep what could not apply.

Why the error must reach the savepoint: on PostgreSQL a failed statement aborts
the whole transaction. A step that catches its own error lets its SAVEPOINT be
RELEASED instead of rolled back; the RELEASE fails, SQLAlchemy marks the
savepoint finished without sending ROLLBACK TO SAVEPOINT, every later SAVEPOINT
then fails too, and the final COMMIT turns silently into a ROLLBACK. On the
Lakebase boot path that last step also discards ``create_all``'s new tables
(``db/app_resource.py`` runs both in one transaction). So the steps let their
errors propagate, and :func:`run_isolated` is the one place that catches them,
after the savepoint has rolled back.

What could not apply is a schema gap, kept per database dialect for the life of
the process: the pass logs it at ERROR, and ``GET /health/db`` reports it
(:func:`schema_heal_summary`). A gap never stops startup.
"""

import logging
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Awaitable, Callable, Iterator, Optional, Union

from sqlalchemy.ext.asyncio import AsyncConnection

logger = logging.getLogger(__name__)

Step = Callable[[AsyncConnection], Awaitable[None]]


@dataclass(frozen=True)
class HealGap:
    """One piece of the heal that did not apply, and why."""

    step: str
    error: str


@dataclass(frozen=True)
class HealReport:
    """The outcome of the last pass against one kind of database."""

    dialect: str
    finished_at: datetime
    gaps: tuple[HealGap, ...]


#: The gaps of the pass running in this task; None outside a pass.
_current_gaps: ContextVar[Optional[list[HealGap]]] = ContextVar(
    "self_heal_current_gaps", default=None
)
#: The last finished pass per dialect ("sqlite", "postgresql").
_last_reports: dict[str, HealReport] = {}


def record_gap(step: str, exc: BaseException) -> None:
    """Log a piece that did not apply and add it to the running pass."""
    error = f"{type(exc).__name__}: {exc}"
    logger.warning(
        "Schema self-heal step %s failed and was rolled back, continuing: %s",
        step,
        error,
    )
    gaps = _current_gaps.get()
    if gaps is not None:
        gaps.append(HealGap(step=step, error=error))


async def run_isolated(conn: AsyncConnection, name: str, work: Step) -> bool:
    """Run ``work(conn)`` in its own SAVEPOINT; True when it applied.

    On failure the savepoint rolls back (so the transaction stays usable and the
    piece leaves nothing half-done), the gap is recorded, and False is returned.
    A connection that cannot nest (a test double) runs the work bare.
    """
    try:
        nested = conn.begin_nested()
    except Exception:  # noqa: BLE001 — no savepoint support; degrade, don't fail
        nested = None
    try:
        if nested is None:
            await work(conn)
        else:
            async with nested:
                await work(conn)
    except Exception as exc:  # noqa: BLE001 — one broken piece must not stop the rest
        record_gap(name, exc)
        return False
    return True


def _dialect(conn: AsyncConnection) -> str:
    try:
        name: str = conn.engine.dialect.name
    except Exception:  # noqa: BLE001 — a test double without an engine
        return "unknown"
    return name if isinstance(name, str) else "unknown"


@contextmanager
def heal_pass(conn: AsyncConnection) -> Iterator[list[HealGap]]:
    """Collect the gaps of one pass, then log and keep them."""
    gaps: list[HealGap] = []
    token = _current_gaps.set(gaps)
    try:
        yield gaps
    finally:
        _current_gaps.reset(token)
        dialect = _dialect(conn)
        _last_reports[dialect] = HealReport(
            dialect=dialect,
            finished_at=datetime.now(timezone.utc),
            gaps=tuple(gaps),
        )
        if gaps:
            logger.error(
                "Schema self-heal INCOMPLETE on %s: %d step(s) did not apply: %s. "
                "Tables they touch may be missing columns, tables or indexes; "
                "the warnings above give each cause. Startup continues.",
                dialect,
                len(gaps),
                ", ".join(gap.step for gap in gaps),
            )
        else:
            logger.info("Schema self-heal applied every step on %s", dialect)


def schema_heal_summary() -> dict[str, Union[str, list[str]]]:
    """What ``/health/db`` shows: names only, never the error text."""
    if not _last_reports:
        return {"status": "not_run", "failed_steps": [], "databases": []}
    failed = sorted({gap.step for r in _last_reports.values() for gap in r.gaps})
    return {
        "status": "incomplete" if failed else "ok",
        "failed_steps": failed,
        "databases": sorted(_last_reports),
    }


def last_reports() -> dict[str, HealReport]:
    """The last pass per dialect (a copy)."""
    return dict(_last_reports)
