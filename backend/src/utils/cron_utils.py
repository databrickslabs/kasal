import logging
from datetime import datetime, timezone
from typing import Optional

import croniter

logger = logging.getLogger(__name__)

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


def calculate_next_run(cron_expression: str, base_time: Optional[datetime] = None) -> datetime:
    """
    Calculate the next run time for a cron expression.
    
    Args:
        cron_expression: Cron expression to calculate next run time
        base_time: Base time to calculate from, defaults to now
        
    Returns:
        Next run time as timezone-aware datetime in UTC
        
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
        logger.info(f"Calculated next run time: {next_run} (naive) -> {next_run_local} (local) -> {next_run_utc} (UTC)")
        return next_run_utc
    except Exception as e:
        logger.error(f"Error in calculate_next_run: {e}")
        raise ValueError(f"Invalid cron expression: {str(e)}")


def calculate_next_run_from_last(cron_expression: str, last_run: Optional[datetime] = None) -> datetime:
    """
    Calculate the next run time from the last run time or today's start.
    
    This function tries to find the next scheduled time based on either:
    1. The last run time (if provided and in the past)
    2. Today's schedule (if there's a time remaining today)
    3. The next occurrence from now
    
    Args:
        cron_expression: Cron expression to calculate next run time
        last_run: Last run time, if available
        
    Returns:
        Next run time as timezone-aware datetime in UTC
        
    Raises:
        ValueError: If cron expression is invalid
    """
    now = datetime.now()
    now_utc = datetime.now(timezone.utc)
    local_tz = now.astimezone().tzinfo
    
    if last_run is not None and last_run.tzinfo is not None:
        last_run = last_run.astimezone(local_tz).replace(tzinfo=None)
    
    logger.info(f"Calculating next run from last. Last run: {last_run}, Now (local): {now}, Now (UTC): {now_utc}")
    
    if last_run is None or last_run < now:
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        
        try:
            cron = croniter.croniter(cron_expression, today_start)
            next_run = cron.get_next(datetime)
            
            next_run_local = next_run.replace(tzinfo=local_tz)
            next_run_utc = next_run_local.astimezone(timezone.utc)
            
            if next_run.date() == now.date() and next_run > now:
                logger.info(f"Found next run time today: {next_run} (naive) -> {next_run_local} (local) -> {next_run_utc} (UTC)")
                return next_run_utc
                
            logger.info(f"No more runs today, calculating from now: {now}")
            return calculate_next_run(cron_expression, now)
            
        except Exception as e:
            logger.error(f"Error calculating next run time: {e}")
            return calculate_next_run(cron_expression, now)
    
    return calculate_next_run(cron_expression, last_run) 