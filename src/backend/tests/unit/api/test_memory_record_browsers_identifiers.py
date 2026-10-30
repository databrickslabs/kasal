"""The memory browser validates the Lakebase table name before interpolating it.

Apps security scan (2026-10-28): ``_browse_lakebase_records`` and
``_delete_lakebase_records`` put the configured ``memory_table`` straight into
SELECT / DELETE text. The schema validates it on write, but a row written before
that check, or straight to the database, reached the SQL unchecked.
"""

import asyncio
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from src.api.memory_backend.record_browsers import (
    _browse_lakebase_records,
    _delete_lakebase_records,
)

HOSTILE = SimpleNamespace(
    memory_table="crew_memory; DROP TABLE users; --", instance_name=None
)


@pytest.mark.parametrize(
    "call",
    [
        lambda: _browse_lakebase_records(
            HOSTILE, group_id="g", scope=None, limit=10, offset=0
        ),
        lambda: _delete_lakebase_records(HOSTILE, group_id="g", scope=None),
    ],
)
def test_a_hostile_table_name_never_reaches_a_session(call):
    with (
        patch("src.db.lakebase_session.get_lakebase_session") as session,
        pytest.raises(ValueError, match="Invalid memory table name"),
    ):
        asyncio.run(call())
    session.assert_not_called()
