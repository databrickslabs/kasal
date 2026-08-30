"""Forgetting (M3): what is allowed to leave the store.

Nothing ever left before — consolidation removed exact duplicates and
supersession retired contradicted facts, but every row stayed forever.
Supersession made that worse by creating a class of record that exists purely as
history.

Two rules, both conservative, and the pass is OPT-IN because it is the only one
that deletes something a user might still want.
"""

from datetime import datetime, timedelta, timezone

import pytest

from src.services.memory.engine import (
    KIND_EPISODIC,
    KIND_PROCEDURAL,
    KIND_SEMANTIC,
    Memory,
    MemoryRecord,
)
from src.services.memory.maintenance.forgetting import (
    forget_expired_memories,
    forgetting_enabled,
    retention_settings,
)
from src.services.memory.storage.adapter import EngineStorageAdapter
from src.services.memory.storage.local import LocalStorageBackend


def _embedder(texts):
    return [[1.0, 0.0] for _ in texts]


@pytest.fixture
def memory(tmp_path):
    backend = LocalStorageBackend(tmp_path / "m.db", embedder=_embedder)
    return Memory(
        storage=EngineStorageAdapter(backend), root_scope="/g1", analyze_on_save=False
    )


@pytest.fixture(autouse=True)
def _enabled(monkeypatch):
    monkeypatch.setenv("KASAL_MEMORY_FORGETTING", "true")


def _save(
    memory,
    content,
    *,
    kind=KIND_EPISODIC,
    age_days=0.0,
    importance=0.2,
    retired_days_ago=None,
):
    record = MemoryRecord(
        content=content,
        scope="/g1",
        kind=kind,
        importance=importance,
        created_at=datetime.now(timezone.utc) - timedelta(days=age_days),
        embedding=[1.0, 0.0],
    )
    if retired_days_ago is not None:
        record.valid_to = datetime.now(timezone.utc) - timedelta(days=retired_days_ago)
        record.superseded_by = "some-newer-record"
    memory.storage.save([record])
    return record


def _remaining(memory):
    return {r.content for r in memory.list_records(limit=100)}


class TestOptIn:
    def test_off_by_default(self, monkeypatch):
        monkeypatch.delenv("KASAL_MEMORY_FORGETTING", raising=False)
        assert forgetting_enabled() is False

    def test_disabled_deletes_nothing(self, memory, monkeypatch):
        monkeypatch.setenv("KASAL_MEMORY_FORGETTING", "false")
        _save(memory, "ancient chatter", age_days=999)

        assert forget_expired_memories(memory)["forgotten"] == 0
        assert _remaining(memory) == {"ancient chatter"}

    def test_sentinels_are_no_ops(self):
        for sentinel in (None, True, False):
            assert forget_expired_memories(sentinel) == {"scanned": 0, "forgotten": 0}


class TestSupersededRetention:
    """Already excluded from recall, so removing them changes no recall result."""

    def test_long_retired_records_are_removed(self, memory):
        _save(memory, "the old deadline", kind=KIND_SEMANTIC, retired_days_ago=120)

        assert forget_expired_memories(memory)["forgotten"] == 1
        assert _remaining(memory) == set()

    def test_recently_retired_records_are_kept(self, memory):
        """The window is what makes 'what did we believe last week' answerable."""
        _save(memory, "the old deadline", kind=KIND_SEMANTIC, retired_days_ago=3)

        assert forget_expired_memories(memory)["forgotten"] == 0
        assert _remaining(memory) == {"the old deadline"}

    def test_window_is_configurable(self, memory, monkeypatch):
        monkeypatch.setenv("KASAL_MEMORY_SUPERSEDED_RETENTION_DAYS", "1")
        _save(memory, "the old deadline", kind=KIND_SEMANTIC, retired_days_ago=3)

        assert forget_expired_memories(memory)["forgotten"] == 1

    def test_a_retired_record_is_judged_on_when_it_was_RETIRED(self, memory):
        """Not on its age. A fact recorded a year ago and retired yesterday is
        recent history and must survive."""
        _save(
            memory,
            "the old deadline",
            kind=KIND_SEMANTIC,
            age_days=365,
            retired_days_ago=1,
        )

        assert forget_expired_memories(memory)["forgotten"] == 0


class TestEpisodicTtl:
    def test_old_low_importance_episodic_is_removed(self, memory):
        _save(memory, "routine chatter", age_days=400, importance=0.2)

        assert forget_expired_memories(memory)["forgotten"] == 1

    def test_recent_episodic_is_kept(self, memory):
        _save(memory, "yesterday's run", age_days=1, importance=0.2)

        assert forget_expired_memories(memory)["forgotten"] == 0

    def test_important_episodic_survives_any_age(self, memory):
        """Importance is the honest proxy for 'worth keeping' — last_accessed is
        inert (never refreshed on recall), so it cannot be used here."""
        _save(memory, "the incident postmortem", age_days=999, importance=0.9)

        assert forget_expired_memories(memory)["forgotten"] == 0

    def test_ttl_and_floor_are_configurable(self, memory, monkeypatch):
        monkeypatch.setenv("KASAL_MEMORY_EPISODIC_TTL_DAYS", "5")
        monkeypatch.setenv("KASAL_MEMORY_IMPORTANCE_FLOOR", "0.95")
        _save(memory, "a week-old note", age_days=7, importance=0.9)

        assert forget_expired_memories(memory)["forgotten"] == 1

    def test_settings_are_reported(self, monkeypatch):
        monkeypatch.setenv("KASAL_MEMORY_EPISODIC_TTL_DAYS", "12")
        assert retention_settings()["episodic_ttl_days"] == 12.0

    def test_malformed_setting_falls_back_to_the_default(self, monkeypatch):
        monkeypatch.setenv("KASAL_MEMORY_EPISODIC_TTL_DAYS", "soon")
        assert retention_settings()["episodic_ttl_days"] == 180.0


class TestFactsAreNeverAgedOut:
    """The whole point of the kind split: a current fact does not expire."""

    @pytest.mark.parametrize("kind", [KIND_SEMANTIC, KIND_PROCEDURAL])
    def test_current_facts_survive_any_age_and_importance(self, memory, kind):
        _save(
            memory, "the user prefers duckdb", kind=kind, age_days=999, importance=0.05
        )

        assert forget_expired_memories(memory)["forgotten"] == 0
        assert _remaining(memory) == {"the user prefers duckdb"}


class TestSafety:
    def test_a_mixed_store_loses_only_what_it_should(self, memory):
        _save(memory, "stale chatter", age_days=400, importance=0.1)
        _save(memory, "long-retired fact", kind=KIND_SEMANTIC, retired_days_ago=200)
        _save(memory, "current fact", kind=KIND_SEMANTIC, age_days=400)
        _save(memory, "important old episode", age_days=400, importance=0.9)
        _save(memory, "fresh episode", age_days=0, importance=0.1)

        stats = forget_expired_memories(memory)

        assert stats["forgotten"] == 2
        assert _remaining(memory) == {
            "current fact",
            "important old episode",
            "fresh episode",
        }

    def test_listing_failure_never_propagates(self):
        from unittest.mock import MagicMock

        memory = MagicMock()
        memory.list_records.side_effect = RuntimeError("backend down")

        assert forget_expired_memories(memory) == {"scanned": 0, "forgotten": 0}

    def test_records_without_timestamps_are_kept(self, memory):
        """A record we cannot date is a record we cannot judge."""
        from unittest.mock import MagicMock

        broken = MagicMock(id="x", kind=KIND_EPISODIC, importance=0.1)
        broken.created_at = None
        broken.valid_to = None
        fake = MagicMock()
        fake.list_records.return_value = [broken]
        fake.storage.delete = MagicMock(return_value=True)

        assert forget_expired_memories(fake)["forgotten"] == 0
        fake.storage.delete.assert_not_called()


class TestOrchestration:
    def test_forgetting_is_part_of_the_full_pass(self, memory):
        from src.services.memory.maintenance.passes import run_memory_maintenance

        _save(memory, "stale chatter", age_days=400, importance=0.1)

        assert run_memory_maintenance(memory)["forgotten"] == 1

    def test_full_pass_reports_zero_when_disabled(self, memory, monkeypatch):
        from src.services.memory.maintenance.passes import run_memory_maintenance

        monkeypatch.setenv("KASAL_MEMORY_FORGETTING", "false")
        _save(memory, "stale chatter", age_days=400, importance=0.1)

        assert run_memory_maintenance(memory)["forgotten"] == 0
