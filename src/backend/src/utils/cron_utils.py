import logging
from datetime import datetime, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import croniter

logger = logging.getLogger(__name__)

ZERO = timedelta(0)


def load_timezone(tz_name: str) -> ZoneInfo:
    """The IANA zone ``tz_name`` names, or ``ValueError`` when there is none."""
    try:
        return ZoneInfo(tz_name)
    except (ZoneInfoNotFoundError, ValueError, TypeError) as exc:
        raise ValueError(f"Unknown timezone {tz_name!r}") from exc


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


#: How many cron candidates to step past while a clock change puts them at or
#: behind the base time: a minutely line crossing a one-hour change skips ~60.
_SCAN_LIMIT = 200


def _runs_every_hour(cron_expression: str) -> bool:
    """Classic cron's "wildcard" job: its minute or hour field starts with "*".

    Such a job runs again in the hour an autumn change repeats; a job at a fixed
    time of day does not, so it runs once that night.
    """
    fields = cron_expression.split()
    return len(fields) >= 2 and (fields[0][:1] == "*" or fields[1][:1] == "*")


def _first_after(
    cron_expression: str, start: datetime, zone: ZoneInfo, base: datetime, fold: int
) -> Optional[datetime]:
    """The first candidate after wall time ``start`` whose ``fold`` reading is
    later than ``base`` (aware UTC), or None within the scan limit."""
    # A bad expression raises croniter's CroniterBadCronError, a ValueError.
    candidates = croniter.croniter(cron_expression, start)
    for _ in range(_SCAN_LIMIT):
        wall = candidates.get_next(datetime)
        instant = wall.replace(tzinfo=zone, fold=fold).astimezone(timezone.utc)
        if instant > base:
            return instant
    return None


def next_run_in_timezone(
    cron_expression: str, tz_name: str, after: Optional[datetime] = None
) -> datetime:
    """The first cron occurrence after ``after``, read on the clock of ``tz_name``.

    "0 9 * * *" in Europe/Berlin is 09:00 Berlin time: 07:00 UTC in summer and
    08:00 UTC in winter. ``after`` defaults to now; a naive value is UTC.

    croniter is driven on naive wall-clock time and each candidate is placed in
    the zone afterwards, because croniter's own tz-aware stepping lands an hour
    off on the days the clocks change. Around those days it behaves like
    classic cron:

    - a wall time the spring change skips runs at the same offset past it;
    - in the hour the autumn change repeats, a fixed-time job ("30 2 * * *")
      runs once, at the first pass, however often its next run is recomputed
      during the second; a job whose minute or hour field is a wildcard
      (hourly, "*/15 * * * *") runs in both passes.

    Returns naive UTC, like every stored schedule time.
    """
    zone = load_timezone(tz_name)
    base = ensure_utc(after) or _utcnow()
    local = base.astimezone(zone)
    wall = local.replace(tzinfo=None, fold=0)
    # Every candidate at its first (or only) reading.
    nxt = _first_after(cron_expression, wall, zone, base, fold=0)
    if nxt is None:
        raise ValueError(f"No run time found for {cron_expression!r} in {tz_name}")
    if _runs_every_hour(cron_expression):
        # The repeated hour, read a second time: when ``base`` is inside it, or
        # the clocks go back between ``base`` and ``nxt``.
        shift = (local.utcoffset() or ZERO) - (nxt.astimezone(zone).utcoffset() or ZERO)
        if local.fold or shift > ZERO:
            start = wall if local.fold else wall - shift
            again = _first_after(cron_expression, start, zone, base, fold=1)
            if again is not None and again < nxt:
                nxt = again
    return nxt.replace(tzinfo=None)


def ensure_utc(dt: Optional[datetime]) -> Optional[datetime]:
    """
    Ensure a datetime is timezone-aware in UTC.

    Args:
        dt: Datetime object to convert

    Returns:
        Timezone-aware datetime in UTC, or None if input is None
    """
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def calculate_next_run(
    cron_expression: str, base_time: Optional[datetime] = None
) -> datetime:
    """
    Calculate the next run time for a cron expression.

    Args:
        cron_expression: Cron expression to calculate next run time
        base_time: Base time to calculate from, defaults to now

    Returns:
        Next run time as timezone-naive datetime for database storage

    Raises:
        ValueError: If cron expression is invalid
    """
    if base_time is None:
        base_time = datetime.now()
    elif base_time.tzinfo is not None:
        base_time = base_time.astimezone().replace(tzinfo=None)

    try:
        cron = croniter.croniter(cron_expression, base_time)
        next_run = cron.get_next(datetime)
        local_tz = datetime.now().astimezone().tzinfo
        next_run_local = next_run.replace(tzinfo=local_tz)
        next_run_utc = next_run_local.astimezone(timezone.utc)
        logger.info(
            f"Calculated next run time: {next_run} (naive) -> {next_run_local} (local) -> {next_run_utc} (UTC)"
        )
        # Return timezone-naive datetime for database storage
        return next_run_utc.replace(tzinfo=None)
    except Exception as e:
        logger.error(f"Error in calculate_next_run: {e}")
        raise ValueError(f"Invalid cron expression: {str(e)}")


def calculate_next_run_from_last(
    cron_expression: str,
    last_run: Optional[datetime] = None,
    tz_name: Optional[str] = None,
) -> datetime:
    """
    Calculate the next run time from the last run time or today's start.

    This function tries to find the next scheduled time based on either:
    1. The last run time (if provided and in the past)
    2. Today's schedule (if there's a time remaining today)
    3. The next occurrence from now

    Args:
        cron_expression: Cron expression to calculate next run time
        last_run: Last run time, if available. A naive value is UTC, which is
            how every schedule and run time is stored.
        tz_name: The schedule's IANA timezone. The cron line is read on that
            clock. None keeps the server's own clock (schedules saved before
            schedules had a timezone).

    Returns:
        Next run time as timezone-naive datetime for database storage

    Raises:
        ValueError: If cron expression or timezone is invalid
    """
    if tz_name:
        base = _utcnow()
        last = ensure_utc(last_run)
        if last is not None and last > base:
            base = last
        return next_run_in_timezone(cron_expression, tz_name, base)

    now = datetime.now()
    now_utc = datetime.now(timezone.utc)
    local_tz = now.astimezone().tzinfo

    if last_run is not None:
        # Naive means UTC here. Reading it as server-local time pushed every
        # next run out by the server's offset on hosts west of UTC.
        if last_run.tzinfo is None:
            last_run = last_run.replace(tzinfo=timezone.utc)
        last_run = last_run.astimezone(local_tz).replace(tzinfo=None)

    logger.info(
        f"Calculating next run from last. Last run: {last_run}, Now (local): {now}, Now (UTC): {now_utc}"
    )

    if last_run is None or last_run < now:
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)

        try:
            cron = croniter.croniter(cron_expression, today_start)
            next_run = cron.get_next(datetime)

            next_run_local = next_run.replace(tzinfo=local_tz)
            next_run_utc = next_run_local.astimezone(timezone.utc)

            if next_run.date() == now.date() and next_run > now:
                logger.info(
                    f"Found next run time today: {next_run} (naive) -> {next_run_local} (local) -> {next_run_utc} (UTC)"
                )
                # Return timezone-naive datetime for database storage
                return next_run_utc.replace(tzinfo=None)

            logger.info(f"No more runs today, calculating from now: {now}")
            next_run_calculated = calculate_next_run(cron_expression, now)
            # Return timezone-naive datetime for database storage
            return next_run_calculated.replace(tzinfo=None)

        except Exception as e:
            logger.error(f"Error calculating next run time: {e}")
            next_run_calculated = calculate_next_run(cron_expression, now)
            # Return timezone-naive datetime for database storage
            return next_run_calculated.replace(tzinfo=None)

    next_run_calculated = calculate_next_run(cron_expression, last_run)
    # Return timezone-naive datetime for database storage
    return next_run_calculated.replace(tzinfo=None)
