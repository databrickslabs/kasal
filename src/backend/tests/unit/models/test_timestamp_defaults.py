"""Timestamp defaults must be evaluated per row, not once at import.

`default=datetime.now(timezone.utc)` is a single value computed when the model
module is imported, so every row a process writes got the process start time as
its created_at / updated_at, and an UPDATE never moved updated_at.
"""

import importlib
import pkgutil
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from sqlalchemy import create_engine, update
from sqlalchemy.orm import Session

import src.models
from src.db.base import Base
from src.models.databricks_config import DatabricksConfig

for _module in pkgutil.iter_modules(src.models.__path__):
    importlib.import_module(f"src.models.{_module.name}")


def _is_frozen_timestamp(default: object) -> bool:
    arg = getattr(default, "arg", None)
    return isinstance(arg, datetime)


def test_no_column_defaults_to_a_timestamp_frozen_at_import() -> None:
    frozen = [
        f"{table.name}.{column.name}"
        for table in Base.metadata.tables.values()
        for column in table.columns
        if _is_frozen_timestamp(column.default) or _is_frozen_timestamp(column.onupdate)
    ]
    assert not frozen, f"default/onupdate evaluated once at import: {frozen}"


class _Clock:
    """Stands in for `datetime` inside the model module; now() advances 1 min."""

    def __init__(self) -> None:
        self.current = datetime(2026, 1, 1, tzinfo=timezone.utc)

    def now(self, tz: timezone | None = None) -> datetime:
        self.current += timedelta(minutes=1)
        return self.current


def _config(name: str) -> DatabricksConfig:
    return DatabricksConfig(
        workspace_url=f"https://{name}.example.com",
        warehouse_id="w",
        catalog="c",
        schema="s",
    )


def test_each_row_gets_its_own_created_at_and_updates_advance_updated_at() -> None:
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine, tables=[DatabricksConfig.__table__])
    clock = _Clock()
    with patch("src.models.databricks_config.datetime", clock), Session(engine) as s:
        first = _config("one")
        s.add(first)
        s.commit()
        second = _config("two")
        s.add(second)
        s.commit()
        assert first.created_at != second.created_at

        before = first.updated_at
        s.execute(
            update(DatabricksConfig)
            .where(DatabricksConfig.id == first.id)
            .values(catalog="changed")
        )
        s.commit()
        s.refresh(first)
        assert first.updated_at != before
