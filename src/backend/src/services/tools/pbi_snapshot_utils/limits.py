"""Limits and settings for the opt-in Power BI snapshot of PBI-only tables.

Power BI limits, as documented for ``POST .../executeQueries`` (checked against
Microsoft Learn; the page was updated 2026-09-15):

* at most 100,000 rows or 1,000,000 values per query, whichever is hit first;
* at most 15 MB of data per query (the row in progress is completed, then no
  more rows are written);
* one query per API call, one table per query;
* 120 query requests per minute per user;
* only DAX: INFO functions and DMV queries are not supported.

Hitting a row or value limit does not fail the HTTP call. The response is HTTP
200 with an ``error`` on the table and only the rows up to the limit. The
caller therefore never trusts a row count: it compares the rows it read with a
``COUNTROWS`` probe and refuses the table on any difference.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

PBI_MAX_ROWS_PER_QUERY = 100_000
PBI_MAX_VALUES_PER_QUERY = 1_000_000
PBI_MAX_BYTES_PER_QUERY = 15 * 1024 * 1024

#: Rows requested per keyset page. Well under the row limit, so the 15 MB cap
#: is not reached for ordinary rows; a page cut short still continues from its
#: last key, so nothing is lost.
PAGE_ROWS = 10_000

#: Default ``pbi_snapshot_max_rows``. A table over the cap is skipped, never
#: partially loaded.
DEFAULT_MAX_ROWS = 200_000
MAX_ALLOWED_ROWS = 5_000_000

CONFIG_ENABLED = "pbi_snapshot_tables"
CONFIG_MAX_ROWS = "pbi_snapshot_max_rows"


@dataclass(frozen=True)
class SnapshotSettings:
    """Parsed ``pbi_snapshot_tables`` / ``pbi_snapshot_max_rows`` config.

    ``pbi_snapshot_tables`` (default false). OPT-IN. When true, the run COPIES
    the rows of the listed PBI-only tables out of Power BI into a Delta table in
    the run's source-layer schema. The copy is a snapshot: it is not refreshed
    when Power BI refreshes, and the table comment records when it was taken.

    ``pbi_snapshot_max_rows`` (default 200,000). A table with more rows is
    skipped with the reason and stays an ingestion task.
    """

    enabled: bool = False
    max_rows: int = DEFAULT_MAX_ROWS

    @classmethod
    def from_config(cls, config: Mapping[str, object] | None) -> SnapshotSettings:
        """Read the two keys. Anything not clearly on is off (fail closed).

        Raises ``ValueError`` for an unusable ``pbi_snapshot_max_rows``; the
        caller reports that for every table rather than guessing a cap.
        """
        raw = dict(config or {})
        flag = raw.get(CONFIG_ENABLED, False)
        if isinstance(flag, str):
            flag = flag.strip().lower() == "true"
        enabled = flag is True
        raw_max = raw.get(CONFIG_MAX_ROWS, DEFAULT_MAX_ROWS)
        if isinstance(raw_max, bool) or not isinstance(raw_max, (int, str)):
            raise ValueError(f"{CONFIG_MAX_ROWS} must be an integer")
        try:
            max_rows = int(raw_max)
        except ValueError as exc:
            raise ValueError(f"{CONFIG_MAX_ROWS} must be an integer") from exc
        if not 1 <= max_rows <= MAX_ALLOWED_ROWS:
            raise ValueError(
                f"{CONFIG_MAX_ROWS} must be between 1 and {MAX_ALLOWED_ROWS}"
            )
        return cls(enabled=enabled, max_rows=max_rows)
