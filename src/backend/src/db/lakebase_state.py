"""
Lakebase activation state tracker.

Distinguishes between startup (fallback to local DB is safe) and runtime
(fallback means silent data loss because writes already went to Lakebase).
"""
import logging
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

# ── Module-level state ──────────────────────────────────────────────────────
_lakebase_ever_activated: bool = False
_last_successful_connection: Optional[datetime] = None


def mark_lakebase_activated() -> None:
    """Call once after Lakebase is successfully activated during lifespan."""
    global _lakebase_ever_activated
    _lakebase_ever_activated = True
    logger.info("Lakebase marked as activated — silent fallback is now disabled")


def record_successful_connection() -> None:
    """Call after each successful Lakebase session creation."""
    global _last_successful_connection
    _last_successful_connection = datetime.now(timezone.utc)


def is_fallback_allowed() -> bool:
    """Return True only before the first activation (startup mode).

    Once Lakebase has been activated, falling back to the local DB would
    cause queries to miss data that was written to Lakebase — so fallback
    is forbidden.
    """
    return not _lakebase_ever_activated


def is_lakebase_activated() -> bool:
    """Return whether Lakebase has been activated at least once."""
    return _lakebase_ever_activated


def get_last_successful_connection() -> Optional[datetime]:
    """Return the timestamp of the last successful Lakebase connection."""
    return _last_successful_connection
