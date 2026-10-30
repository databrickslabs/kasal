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

What could not apply is a schema gap, kept per database for the life of the
process: the pass logs it at ERROR, and ``GET /health/db`` reports it
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
    """The outcome of the last pass against one database."""

    dialect: str
    finished_at: datetime
    gaps: tuple[HealGap, ...]
    #: False when the pass stopped on an exception: the schema was not verified.
    completed: bool = True


#: The gaps of the pass running in this task; None outside a pass.
_current_gaps: ContextVar[Optional[list[HealGap]]] = ContextVar(
    "self_heal_current_gaps", default=None
)
#: The last pass per database the app itself runs on (see :func:`_database_key`).
#: Heals of a migration target pass ``record=False`` and are not kept.
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


def _database_key(conn: AsyncConnection) -> str:
    """Which database ``conn`` is on (dialect, host, name), so two databases of
    one dialect keep separate reports. Internal only: /health/db shows dialects."""
    try:
        url = conn.engine.url
        return f"{url.get_backend_name()}://{url.host or ''}/{url.database or ''}"
    except Exception:  # noqa: BLE001 — a test double without an engine
        return _dialect(conn)


def _log_pass(dialect: str, gaps: list[HealGap], completed: bool) -> None:
    if not completed:
        logger.error(
            "Schema self-heal on %s stopped before it finished (%d gap(s) so far); "
            "the schema was not verified. Startup continues.",
            dialect,
            len(gaps),
        )
    elif gaps:
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


@contextmanager
def heal_pass(conn: AsyncConnection, record: bool = True) -> Iterator[list[HealGap]]:
    """Collect the gaps of one pass, then log them, and keep them when ``record``.

    ``record=False`` is for a database the app does not run on yet (a migration
    target): its result must not replace the active database's report.
    """
    gaps: list[HealGap] = []
    token = _current_gaps.set(gaps)
    completed = False
    try:
        yield gaps
        completed = True
    finally:
        _current_gaps.reset(token)
        dialect = _dialect(conn)
        if record:
            _last_reports[_database_key(conn)] = HealReport(
                dialect=dialect,
                finished_at=datetime.now(timezone.utc),
                gaps=tuple(gaps),
                completed=completed,
            )
        _log_pass(dialect, gaps, completed)


def schema_heal_summary() -> dict[str, Union[str, list[str]]]:
    """What ``/health/db`` shows: names only, never the error text.

    ``ok`` only when every kept pass finished with no gap; ``incomplete`` when a
    step did not apply; ``not_run`` when no pass ran or one stopped early.
    """
    reports = list(_last_reports.values())
    failed = sorted({gap.step for r in reports for gap in r.gaps})
    if failed:
        status = "incomplete"
    elif not reports or not all(r.completed for r in reports):
        status = "not_run"
    else:
        status = "ok"
    return {
        "status": status,
        "failed_steps": failed,
        "databases": sorted({r.dialect for r in reports}),
    }


def last_reports() -> dict[str, HealReport]:
    """The last pass per database (a copy)."""
    return dict(_last_reports)
