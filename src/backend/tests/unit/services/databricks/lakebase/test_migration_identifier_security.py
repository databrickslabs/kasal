"""Source column names are identifiers too, and are checked before any clear.

Apps security scan triage (2026-10-28): the SQLite/PostgreSQL → Lakebase
migration interpolated source result keys into ``"<col>"`` identifiers and
``:<col>`` bind names unchecked, after the target table had been cleared.
"""

from unittest.mock import MagicMock

import pytest

from src.services.databricks.lakebase.migration import (
    LakebaseMigrationService,
    _validate_identifier,
)

HOSTILE = 'x" TEXT); DROP TABLE agents; --'


def _engine(conn):
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    engine = MagicMock()
    engine.connect.return_value = conn
    engine.begin.return_value = conn
    return engine


def test_a_trailing_newline_is_not_an_identifier():
    with pytest.raises(ValueError):
        _validate_identifier("agents\n")


def test_a_hostile_source_column_fails_the_table_before_it_is_cleared():
    source = MagicMock()
    source.execute.return_value.fetchall.return_value = [(1, "v")]
    source.execute.return_value.keys.return_value = ["id", HOSTILE]
    lakebase = MagicMock()
    lakebase.execute.return_value.fetchall.return_value = []  # no target columns

    rows, error = LakebaseMigrationService().migrate_table_data_sync(
        table_name="agents",
        source_engine=_engine(source),
        lakebase_engine=_engine(lakebase),
        is_sqlite=True,
    )

    assert rows == 0 and "column name" in (error or "")
    executed = [str(c.args[0]) for c in lakebase.execute.call_args_list]
    assert not any("DELETE" in sql or "INSERT" in sql for sql in executed)
