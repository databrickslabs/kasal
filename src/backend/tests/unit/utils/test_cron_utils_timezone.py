"""A schedule's cron line is read on the clock of the schedule's timezone.

"0 9 * * *" saved by a user in Berlin means 09:00 in Berlin: 07:00 UTC under
summer time and 08:00 UTC under winter time, including on the nights the clocks
change. A schedule with no timezone keeps the server's clock, as before.
"""

import time
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from src.utils.cron_utils import calculate_next_run_from_last, next_run_in_timezone

BERLIN = "Europe/Berlin"
DAILY_9 = "0 9 * * *"


def _utc(*args: int) -> datetime:
    return datetime(*args, tzinfo=timezone.utc)


@pytest.fixture
def server_tz(monkeypatch):
    """Run the server's clock in a given zone for one test."""

    def _set(name: str) -> None:
        monkeypatch.setenv("TZ", name)
        time.tzset()

    yield _set
    monkeypatch.undo()
    time.tzset()


class TestNextRunInTimezone:
    def test_summer_time_is_seven_utc(self):
        assert next_run_in_timezone(DAILY_9, BERLIN, _utc(2026, 7, 1, 10)) == (
            datetime(2026, 7, 2, 7, 0)
        )

    def test_winter_time_is_eight_utc(self):
        assert next_run_in_timezone(DAILY_9, BERLIN, _utc(2026, 1, 15, 10)) == (
            datetime(2026, 1, 16, 8, 0)
        )

    def test_later_today_in_berlin(self):
        # 06:00 UTC is 08:00 in Berlin: today's 09:00 is still ahead.
        assert next_run_in_timezone(DAILY_9, BERLIN, _utc(2026, 10, 24, 6)) == (
            datetime(2026, 10, 24, 7, 0)
        )

    def test_across_the_autumn_change(self):
        # Saturday noon in summer time; the clocks go back on Sunday 25 Oct.
        assert next_run_in_timezone(DAILY_9, BERLIN, _utc(2026, 10, 24, 10)) == (
            datetime(2026, 10, 25, 8, 0)
        )

    def test_across_the_spring_change(self):
        # Saturday in winter time; the clocks go forward on Sunday 29 Mar.
        assert next_run_in_timezone(DAILY_9, BERLIN, _utc(2026, 3, 28, 10)) == (
            datetime(2026, 3, 29, 7, 0)
        )

    def test_a_week_of_runs_follows_the_change(self):
        runs, after = [], _utc(2026, 10, 22, 12)
        for _ in range(5):
            nxt = next_run_in_timezone(DAILY_9, BERLIN, after)
            runs.append(nxt.hour)
            after = nxt.replace(tzinfo=timezone.utc)
        assert runs == [7, 7, 8, 8, 8]  # 23rd, 24th in CEST; 25th on in CET

    # The night of 25 Oct 2026 in Berlin: 02:00-02:59 happens twice, first in
    # CEST (00:00-00:59 UTC), then in CET (01:00-01:59 UTC).

    def _runs(self, cron, after, count):
        seen = []
        for _ in range(count):
            nxt = next_run_in_timezone(cron, BERLIN, after)
            seen.append(nxt)
            after = nxt.replace(tzinfo=timezone.utc)
        return seen

    def test_hourly_runs_once_per_real_hour_through_the_autumn_change(self):
        # 02:00 CEST, 02:00 CET (the repeated hour), 03:00 CET, 04:00 CET.
        assert self._runs("0 * * * *", _utc(2026, 10, 24, 23, 30), 4) == [
            datetime(2026, 10, 25, 0, 0),
            datetime(2026, 10, 25, 1, 0),
            datetime(2026, 10, 25, 2, 0),
            datetime(2026, 10, 25, 3, 0),
        ]

    def test_quarter_hourly_runs_through_both_passes(self):
        runs = self._runs("*/15 * * * *", _utc(2026, 10, 25, 0, 40), 6)
        assert runs == [
            datetime(2026, 10, 25, 0, 45),  # 02:45 CEST
            datetime(2026, 10, 25, 1, 0),  # 02:00 CET, the repeat begins
            datetime(2026, 10, 25, 1, 15),
            datetime(2026, 10, 25, 1, 30),
            datetime(2026, 10, 25, 1, 45),
            datetime(2026, 10, 25, 2, 0),  # 03:00 CET
        ]

    def test_inside_the_repeated_hour_the_next_quarter_is_next(self):
        # 01:30 UTC is 02:30 CET, the SECOND 02:30 of the night.
        after = _utc(2026, 10, 25, 1, 30)
        assert next_run_in_timezone("*/15 * * * *", BERLIN, after) == (
            datetime(2026, 10, 25, 1, 45)
        )

    def test_a_fixed_time_in_the_repeated_hour_runs_once(self):
        daily = "30 2 * * *"
        first = next_run_in_timezone(daily, BERLIN, _utc(2026, 10, 24, 22))
        assert first == datetime(2026, 10, 25, 0, 30)  # 02:30 CEST
        tomorrow = datetime(2026, 10, 26, 1, 30)  # 02:30 CET on the 26th
        assert next_run_in_timezone(daily, BERLIN, _utc(2026, 10, 25, 0, 31)) == (
            tomorrow
        )
        # Recomputed during the second 02:xx (an edit, a toggle, a catch-up):
        # still tomorrow, not 02:30 CET tonight.
        for minute in (5, 29, 30, 45):
            after = _utc(2026, 10, 25, 1, minute)
            assert next_run_in_timezone(daily, BERLIN, after) == tomorrow

    def test_a_fixed_time_recomputed_from_the_last_run_runs_once(self):
        # The repository passes the stored last run with the current time.
        last = datetime(2026, 10, 25, 0, 30)  # it ran at 02:30 CEST
        with patch(
            "src.utils.cron_utils._utcnow", return_value=_utc(2026, 10, 25, 1, 10)
        ):
            assert calculate_next_run_from_last("30 2 * * *", last, BERLIN) == (
                datetime(2026, 10, 26, 1, 30)
            )

    def test_a_skipped_wall_time_still_runs_that_day(self):
        # 02:30 does not exist in Berlin on 29 Mar 2026.
        nxt = next_run_in_timezone("30 2 * * *", BERLIN, _utc(2026, 3, 28, 12))
        assert nxt.date() == datetime(2026, 3, 29).date()

    def test_naive_after_is_utc(self):
        assert next_run_in_timezone(DAILY_9, BERLIN, datetime(2026, 7, 1, 10)) == (
            datetime(2026, 7, 2, 7, 0)
        )

    def test_unknown_timezone_is_a_value_error(self):
        with pytest.raises(ValueError, match="Unknown timezone"):
            next_run_in_timezone(DAILY_9, "Mars/Olympus_Mons", _utc(2026, 7, 1))

    def test_bad_cron_is_a_value_error(self):
        with pytest.raises(ValueError):
            next_run_in_timezone("not a cron", BERLIN, _utc(2026, 7, 1))

    def test_the_server_zone_does_not_matter(self, server_tz):
        server_tz("America/Los_Angeles")
        assert next_run_in_timezone(DAILY_9, BERLIN, _utc(2026, 7, 1, 10)) == (
            datetime(2026, 7, 2, 7, 0)
        )


class TestCalculateNextRunFromLastWithTimezone:
    def test_from_now_in_the_schedule_zone(self):
        with patch("src.utils.cron_utils._utcnow", return_value=_utc(2026, 1, 15, 10)):
            assert calculate_next_run_from_last(DAILY_9, None, BERLIN) == (
                datetime(2026, 1, 16, 8, 0)
            )

    def test_after_a_past_last_run_it_counts_from_now(self):
        with patch("src.utils.cron_utils._utcnow", return_value=_utc(2026, 7, 1, 10)):
            last = datetime(2026, 6, 1, 7, 0)  # naive UTC, as stored
            assert calculate_next_run_from_last(DAILY_9, last, BERLIN) == (
                datetime(2026, 7, 2, 7, 0)
            )

    def test_utc_on_a_utc_server_runs_at_the_cron_hour(self, server_tz):
        server_tz("UTC")
        with patch("src.utils.cron_utils._utcnow", return_value=_utc(2026, 7, 1, 10)):
            assert calculate_next_run_from_last(DAILY_9, None, "UTC") == (
                datetime(2026, 7, 2, 9, 0)
            )


class TestNoTimezoneKeepsTheServerClock:
    """NULL timezone: the schedules saved before the column exist read this way."""

    def test_server_clock_is_used(self, server_tz):
        server_tz("Europe/Berlin")
        nxt = calculate_next_run_from_last(DAILY_9, None, None)
        # 09:00 on the server's (Berlin) clock: 07:00 or 08:00 UTC.
        assert nxt.minute == 0 and nxt.hour in (7, 8)

    def test_utc_server_unchanged(self, server_tz):
        server_tz("UTC")
        nxt = calculate_next_run_from_last(DAILY_9, None, None)
        assert (nxt.hour, nxt.minute) == (9, 0)
        assert nxt > datetime.utcnow()

    def test_naive_last_run_is_read_as_utc_west_of_utc(self, server_tz):
        # The scheduler passes naive UTC. Read as New York time, an hourly
        # schedule's next run landed ~5 hours out instead of within the hour.
        server_tz("America/New_York")
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        nxt = calculate_next_run_from_last("0 * * * *", now, None)
        assert timedelta(0) < nxt - now <= timedelta(hours=1)
