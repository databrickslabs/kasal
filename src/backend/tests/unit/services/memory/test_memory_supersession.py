"""Truth maintenance (M2): a new fact retires the old one.

The failure this exists to prevent:

    "The project deadline is 15 June."   (recorded first)
    "The deadline moved to 30 July."     (recorded later)

Both used to sit in the store forever. Recall returned whichever the blended
score favoured, and as soon as the correct fact was older than the incorrect
one, ordering could invert. Nothing is deleted — the retired record stays so
"what did we believe on 3 March" is answerable — it just stops being recalled.
"""

from datetime import datetime, timedelta, timezone

import pytest

from src.services.memory.engine import (
    KIND_EPISODIC,
    KIND_SEMANTIC,
    Memory,
    MemoryRecord,
)
from src.services.memory.engine_storage_adapter import EngineStorageAdapter
from src.services.memory.local_storage_backend import LocalMemoryStorage
from src.services.memory.supersession import supersede_outdated_facts


def _embedder(texts):
    return [[1.0, 0.0] for _ in texts]


class _FakeLLM:
    def __init__(self, reply):
        self.reply = reply
        self.calls = []

    def call(self, prompt):
        self.calls.append(prompt)
        return self.reply


def _memory(tmp_path, llm=None, name="m.db"):
    backend = LocalMemoryStorage(tmp_path / name, embedder=_embedder)
    return Memory(
        storage=EngineStorageAdapter(backend),
        root_scope="/g1",
        llm=llm,
        analyze_on_save=False,
    )


def _fact(memory, text, age_days=0, kind=KIND_SEMANTIC):
    """Save a fact with an explicit age (list_records orders newest first)."""
    created = datetime.now(timezone.utc) - timedelta(days=age_days)
    record = MemoryRecord(
        content=text, scope="/g1", kind=kind, created_at=created, embedding=[1.0, 0.0]
    )
    memory.storage.save([record])
    return record


def _by_content(memory):
    return {r.content: r for r in memory.list_records(limit=50)}


class TestSupersession:
    def test_older_contradicted_fact_is_retired(self, tmp_path):
        llm = _FakeLLM('[{"current": 0, "outdated": [1]}]')
        memory = _memory(tmp_path, llm=llm)
        _fact(memory, "The deadline moved to 30 July.", age_days=0)
        old = _fact(memory, "The project deadline is 15 June.", age_days=10)

        stats = supersede_outdated_facts(memory)

        assert stats["superseded"] == 1
        records = _by_content(memory)
        retired = records["The project deadline is 15 June."]
        assert retired.valid_to is not None
        assert retired.superseded_by is not None
        assert retired.id == old.id

    def test_the_winner_stays_current(self, tmp_path):
        llm = _FakeLLM('[{"current": 0, "outdated": [1]}]')
        memory = _memory(tmp_path, llm=llm)
        _fact(memory, "The deadline moved to 30 July.", age_days=0)
        _fact(memory, "The project deadline is 15 June.", age_days=10)

        supersede_outdated_facts(memory)

        winner = _by_content(memory)["The deadline moved to 30 July."]
        assert winner.is_current
        assert winner.superseded_by is None

    def test_retired_facts_are_not_recalled(self, tmp_path):
        """The whole point: history is kept, but it stops entering prompts."""
        llm = _FakeLLM('[{"current": 0, "outdated": [1]}]')
        memory = _memory(tmp_path, llm=llm)
        _fact(memory, "The deadline moved to 30 July.", age_days=0)
        _fact(memory, "The project deadline is 15 June.", age_days=10)

        supersede_outdated_facts(memory)

        recalled = [r.content for r in memory.recall("deadline", limit=10)]
        assert recalled == ["The deadline moved to 30 July."]
        # Still on disk — "what did we believe last week" stays answerable.
        assert len(memory.list_records(limit=50)) == 2

    def test_nothing_outdated_is_a_no_op(self, tmp_path):
        memory = _memory(tmp_path, llm=_FakeLLM("[]"))
        _fact(memory, "The deadline is 30 July.")
        _fact(memory, "The budget is 40k.", age_days=3)

        stats = supersede_outdated_facts(memory)

        assert stats["superseded"] == 0
        assert all(r.is_current for r in memory.list_records(limit=50))


class TestSafetyRails:
    def test_episodic_records_are_never_superseded(self, tmp_path):
        """Two accounts of different moments are both true. If episodic records
        were eligible, every re-run of a crew would retire its own history."""
        llm = _FakeLLM('[{"current": 0, "outdated": [1]}]')
        memory = _memory(tmp_path, llm=llm)
        _fact(memory, "Ran the report. Result: 12.", age_days=0, kind=KIND_EPISODIC)
        _fact(memory, "Ran the report. Result: 9.", age_days=5, kind=KIND_EPISODIC)

        stats = supersede_outdated_facts(memory)

        assert stats["scanned"] == 0
        assert stats["superseded"] == 0
        assert llm.calls == [], "no LLM call when there are no facts to check"

    def test_a_newer_record_is_never_retired_by_an_older_one(self, tmp_path):
        """The model is told newest-first; if it inverts the direction it would
        retire the CURRENT fact and keep the stale one. Timestamps decide."""
        llm = _FakeLLM('[{"current": 1, "outdated": [0]}]')  # inverted
        memory = _memory(tmp_path, llm=llm)
        _fact(memory, "The deadline moved to 30 July.", age_days=0)
        _fact(memory, "The project deadline is 15 June.", age_days=10)

        stats = supersede_outdated_facts(memory)

        assert stats["superseded"] == 0
        assert _by_content(memory)["The deadline moved to 30 July."].is_current

    def test_already_retired_facts_are_not_rescanned(self, tmp_path):
        llm = _FakeLLM('[{"current": 0, "outdated": [1]}]')
        memory = _memory(tmp_path, llm=llm)
        _fact(memory, "The deadline moved to 30 July.", age_days=0)
        _fact(memory, "The project deadline is 15 June.", age_days=10)
        supersede_outdated_facts(memory)

        second = supersede_outdated_facts(memory)

        assert second["scanned"] == 1  # only the surviving fact
        assert second["superseded"] == 0

    def test_a_record_cannot_supersede_itself(self, tmp_path):
        llm = _FakeLLM('[{"current": 0, "outdated": [0]}]')
        memory = _memory(tmp_path, llm=llm)
        _fact(memory, "The deadline is 30 July.")
        _fact(memory, "The budget is 40k.", age_days=1)

        assert supersede_outdated_facts(memory)["superseded"] == 0

    def test_out_of_range_indices_are_ignored(self, tmp_path):
        llm = _FakeLLM('[{"current": 0, "outdated": [99, -1, "x"]}]')
        memory = _memory(tmp_path, llm=llm)
        _fact(memory, "The deadline is 30 July.")
        _fact(memory, "The budget is 40k.", age_days=1)

        assert supersede_outdated_facts(memory)["superseded"] == 0

    def test_retiring_does_not_blank_the_record(self, tmp_path):
        """Memory.update forwards only the fields given. Before it filtered
        Nones, retiring a fact would also have wiped its content."""
        llm = _FakeLLM('[{"current": 0, "outdated": [1]}]')
        memory = _memory(tmp_path, llm=llm)
        _fact(memory, "The deadline moved to 30 July.", age_days=0)
        _fact(memory, "The project deadline is 15 June.", age_days=10)

        supersede_outdated_facts(memory)

        retired = _by_content(memory)["The project deadline is 15 June."]
        assert retired.content == "The project deadline is 15 June."
        assert retired.scope == "/g1"
        assert retired.kind == KIND_SEMANTIC


class TestGating:
    def test_no_llm_means_no_pass(self, tmp_path):
        memory = _memory(tmp_path)
        _fact(memory, "The deadline is 30 July.")
        assert supersede_outdated_facts(memory)["superseded"] == 0

    def test_env_kill_switch(self, tmp_path, monkeypatch):
        monkeypatch.setenv("KASAL_MEMORY_SUPERSESSION", "false")
        llm = _FakeLLM('[{"current": 0, "outdated": [1]}]')
        memory = _memory(tmp_path, llm=llm)
        _fact(memory, "The deadline moved to 30 July.")
        _fact(memory, "The project deadline is 15 June.", age_days=10)

        assert supersede_outdated_facts(memory)["superseded"] == 0
        assert llm.calls == []

    def test_a_single_fact_is_not_worth_a_call(self, tmp_path):
        llm = _FakeLLM("[]")
        memory = _memory(tmp_path, llm=llm)
        _fact(memory, "The deadline is 30 July.")

        assert supersede_outdated_facts(memory)["superseded"] == 0
        assert llm.calls == []

    def test_sentinels_are_no_ops(self):
        for sentinel in (None, True, False):
            assert supersede_outdated_facts(sentinel) == {
                "scanned": 0,
                "superseded": 0,
            }

    def test_llm_failure_never_propagates(self, tmp_path):
        class _Boom:
            def call(self, prompt):
                raise RuntimeError("model down")

        memory = _memory(tmp_path, llm=_Boom())
        _fact(memory, "The deadline is 30 July.")
        _fact(memory, "The budget is 40k.", age_days=1)

        assert supersede_outdated_facts(memory)["superseded"] == 0

    def test_listing_failure_never_propagates(self):
        from unittest.mock import MagicMock

        memory = MagicMock()
        memory.llm.call = MagicMock(return_value="[]")
        memory.list_records.side_effect = RuntimeError("backend down")

        assert supersede_outdated_facts(memory) == {"scanned": 0, "superseded": 0}


class TestRunMemoryMaintenanceIntegration:
    def test_supersession_is_part_of_the_full_pass(self, tmp_path):
        from src.services.memory.maintenance import run_memory_maintenance

        llm = _FakeLLM('[{"current": 0, "outdated": [1]}]')
        memory = _memory(tmp_path, llm=llm)
        _fact(memory, "The deadline moved to 30 July.", age_days=0)
        _fact(memory, "The project deadline is 15 June.", age_days=10)

        stats = run_memory_maintenance(memory)

        assert stats["superseded"] == 1

    def test_merge_prompt_forbids_merging_contradictions(self):
        """The merge pass preserves every distinct detail, which is right for
        fragments and would destroy a supersession if applied to a conflict."""
        from src.services.memory.maintenance import _MERGE_PROMPT

        assert "CONTRADICTIONS ARE NOT FRAGMENTS" in _MERGE_PROMPT

    def test_merged_records_keep_the_most_durable_kind(self):
        from src.services.memory.maintenance import _merged_kind

        episodic = MemoryRecord(content="a", kind=KIND_EPISODIC)
        semantic = MemoryRecord(content="b", kind=KIND_SEMANTIC)
        assert _merged_kind([episodic, semantic]) == KIND_SEMANTIC
        assert _merged_kind([episodic, episodic]) == KIND_EPISODIC
