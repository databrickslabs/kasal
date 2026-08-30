"""Typed memory (M1): episodic vs semantic vs procedural, end to end.

The point of the type is that the two shapes want OPPOSITE retrieval policies:

* episodic — "what happened in run 47": time-anchored, high volume, should fade.
* semantic — "the user prefers Databricks SQL": atemporal, low volume, must stay
  current. A 30-day half-life is roughly right for the first and actively wrong
  for the second — a stable preference learned two months ago is not less true
  than one learned yesterday.

Covers the record contract, the save-time classifier, and the local backend's
scoring. ``test_memory_supersession.py`` covers the validity window (M2).
"""

import time
from datetime import datetime, timedelta, timezone

import pytest

from src.services.memory.engine import (
    KIND_EPISODIC,
    KIND_PROCEDURAL,
    KIND_SEMANTIC,
    Memory,
    MemoryRecord,
)
from src.services.memory.storage.adapter import EngineStorageAdapter
from src.services.memory.storage.local import LocalStorageBackend


def _embedder(texts):
    return [[1.0, 0.0] for _ in texts]


def _store(tmp_path, name="m.db"):
    return LocalStorageBackend(tmp_path / name, embedder=_embedder)


def _memory(tmp_path, name="m.db", llm=None):
    return Memory(
        storage=EngineStorageAdapter(_store(tmp_path, name)),
        root_scope="/g1",
        llm=llm,
        analyze_on_save=llm is not None,
        # The stub embedder gives every text the same vector; save-time
        # consolidation off so the behaviour under test is what acts.
        consolidation_threshold=0,
    )


class _FakeLLM:
    """Minimal Memory.llm: returns a canned analysis reply."""

    def __init__(self, reply):
        self.reply = reply
        self.calls = []

    def call(self, prompt):
        self.calls.append(prompt)
        return self.reply


class TestRecordContract:
    def test_defaults_to_episodic(self):
        assert MemoryRecord(content="x").kind == KIND_EPISODIC

    def test_unknown_kind_reads_as_episodic(self):
        """Records written before the field existed come back without it, and
        episodic is the safe reading — it decays and claims nothing."""
        for value in (None, "", "bogus", "SEMANTIC-ish"):
            assert MemoryRecord(content="x", kind=value).kind == KIND_EPISODIC

    def test_known_kinds_survive_case_and_whitespace(self):
        assert MemoryRecord(content="x", kind=" Semantic ").kind == KIND_SEMANTIC
        assert MemoryRecord(content="x", kind="PROCEDURAL").kind == KIND_PROCEDURAL

    def test_is_current_tracks_valid_to(self):
        assert MemoryRecord(content="x").is_current is True
        retired = MemoryRecord(content="x", valid_to=datetime.now(timezone.utc))
        assert retired.is_current is False


class TestClassificationOnSave:
    def test_kind_comes_from_the_analysis_pass(self, tmp_path):
        llm = _FakeLLM(
            '{"categories": ["prefs"], "importance": 0.8, "kind": "semantic"}'
        )
        record = _memory(tmp_path, llm=llm).remember(
            "The user always wants query results rendered as a table."
        )
        assert record.kind == KIND_SEMANTIC
        assert len(llm.calls) == 1, "classification rides the existing call"

    def test_durable_records_open_a_validity_window(self, tmp_path):
        llm = _FakeLLM('{"categories": ["p"], "importance": 0.8, "kind": "semantic"}')
        # Long enough to clear _MIN_ANALYSIS_CHARS — a shorter string is never
        # analysed at all and would save unclassified.
        record = _memory(tmp_path, llm=llm).remember(
            "The user prefers query results rendered as a markdown table."
        )
        assert record.valid_from is not None
        assert record.valid_to is None

    def test_episodic_records_have_no_validity_window(self, tmp_path):
        llm = _FakeLLM('{"categories": ["c"], "importance": 0.3, "kind": "episodic"}')
        record = _memory(tmp_path, llm=llm).remember("User asked X. Assistant said Y.")
        assert record.valid_from is None

    def test_unparseable_reply_falls_back_to_episodic(self, tmp_path):
        record = _memory(tmp_path, llm=_FakeLLM("not json at all")).remember(
            "Some content long enough to be analysed by the labelling pass."
        )
        assert record.kind == KIND_EPISODIC

    def test_explicit_kind_wins_over_analysis(self, tmp_path):
        llm = _FakeLLM('{"categories": ["c"], "importance": 0.3, "kind": "episodic"}')
        record = _memory(tmp_path, llm=llm).remember("x" * 100, kind=KIND_SEMANTIC)
        assert record.kind == KIND_SEMANTIC

    def test_no_llm_means_episodic_and_no_call(self, tmp_path):
        """The common configuration in tests and in a workspace without an
        analysis model — it must still save, just unclassified."""
        record = _memory(tmp_path).remember("plain content")
        assert record.kind == KIND_EPISODIC


class TestKindSurvivesStorage:
    def test_round_trips_through_sqlite(self, tmp_path):
        memory = _memory(tmp_path)
        memory.remember("a durable fact", kind=KIND_SEMANTIC)
        memory.remember("something that happened", kind=KIND_EPISODIC)

        by_content = {r.content: r for r in memory.list_records(limit=10)}
        assert by_content["a durable fact"].kind == KIND_SEMANTIC
        assert by_content["something that happened"].kind == KIND_EPISODIC

    def test_existing_store_is_migrated(self, tmp_path):
        """A dev store created before these columns existed must keep working —
        SQLite has no ADD COLUMN IF NOT EXISTS and CREATE TABLE IF NOT EXISTS is
        a no-op, so without the migration every insert fails."""
        import sqlite3

        path = tmp_path / "old.db"
        conn = sqlite3.connect(str(path))
        conn.executescript("""
            CREATE TABLE memories (
                id TEXT PRIMARY KEY, content TEXT NOT NULL,
                scope TEXT NOT NULL DEFAULT '/', categories TEXT NOT NULL DEFAULT '[]',
                metadata TEXT NOT NULL DEFAULT '{}', importance REAL NOT NULL DEFAULT 0.5,
                source TEXT, private INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL, last_accessed TEXT NOT NULL, embedding BLOB
            );
            """)
        conn.execute(
            "INSERT INTO memories (id, content, scope, created_at, last_accessed) "
            "VALUES ('old-1', 'a pre-existing memory', '/g1', ?, ?)",
            (datetime.now(timezone.utc).isoformat(),) * 2,
        )
        conn.commit()
        conn.close()

        backend = LocalStorageBackend(path, embedder=_embedder)
        records = backend.list_records(scope_prefix="/g1")

        assert [r.content for r in records] == ["a pre-existing memory"]
        assert records[0].kind == KIND_EPISODIC  # the correct reading of NULL
        backend.save([MemoryRecord(content="new", scope="/g1", kind=KIND_SEMANTIC)])
        assert len(backend.list_records(scope_prefix="/g1")) == 2


class TestRecencyPolicySplit:
    """Age must penalise episodic records and leave facts alone."""

    def _score(self, backend, kind, age_days):
        created = datetime.now(timezone.utc) - timedelta(days=age_days)
        backend.save(
            [
                MemoryRecord(
                    content="the user prefers duckdb",
                    scope="/g1",
                    kind=kind,
                    created_at=created,
                    embedding=[1.0, 0.0],
                )
            ]
        )
        hits = backend.search([1.0, 0.0], scope_prefix="/g1", limit=5)
        return hits[0][1]

    def test_old_episodic_scores_below_fresh_episodic(self, tmp_path):
        fresh = self._score(_store(tmp_path, "a.db"), KIND_EPISODIC, 0)
        stale = self._score(_store(tmp_path, "b.db"), KIND_EPISODIC, 120)
        assert stale < fresh

    def test_semantic_does_not_decay(self, tmp_path):
        fresh = self._score(_store(tmp_path, "c.db"), KIND_SEMANTIC, 0)
        old = self._score(_store(tmp_path, "d.db"), KIND_SEMANTIC, 120)
        assert old == pytest.approx(fresh)

    def test_old_fact_outranks_equally_old_episode(self, tmp_path):
        """The user-visible consequence: a durable fact does not sink below a
        stale log entry just because both are old."""
        fact = self._score(_store(tmp_path, "e.db"), KIND_SEMANTIC, 90)
        episode = self._score(_store(tmp_path, "f.db"), KIND_EPISODIC, 90)
        assert fact > episode


class _Mem:
    """Only ``root_scope`` is read by the selection; no in-flight overlay."""

    root_scope = None


class TestRecallReservesDurableSlots:
    """A burst of episodic records must not evict every fact from the block.

    The reservation now lives inside ``_select_records``, the ONE selection over
    the oversampled pool — it used to be a separate trim that ran before a
    second one, and non-redundancy applied after a trim can only shrink the
    block rather than promote the next distinct memory.
    """

    def _select(self, records, limit=6):
        from src.services.memory.run.recall import _select_records

        return _select_records(_Mem(), records, limit)

    def _records(self, episodic, semantic):
        return [
            MemoryRecord(content=f"episode {i}", kind=KIND_EPISODIC)
            for i in range(episodic)
        ] + [
            MemoryRecord(content=f"fact {i}", kind=KIND_SEMANTIC)
            for i in range(semantic)
        ]

    def test_facts_ranked_last_still_reach_the_block(self):
        selected = self._select(self._records(12, 3))

        assert len(selected) == 6
        assert sum(1 for r in selected if r.kind == KIND_SEMANTIC) == 2
        # The top episodic records keep their places; only the tail is displaced.
        assert [r.content for r in selected[:4]] == [f"episode {i}" for i in range(4)]

    def test_no_facts_means_plain_top_n(self):
        selected = self._select(self._records(12, 0))
        assert [r.content for r in selected] == [f"episode {i}" for i in range(6)]

    def test_short_result_set_is_untouched(self):
        records = self._records(2, 1)
        assert self._select(records) == records

    def test_reservation_is_a_ceiling_not_a_quota(self):
        """One fact available means one fact reserved — the rest stay episodic."""
        selected = self._select(self._records(12, 1))
        assert sum(1 for r in selected if r.kind == KIND_SEMANTIC) == 1
        assert len(selected) == 6

    def test_preamble_includes_a_low_ranked_fact(self, tmp_path):
        """End to end through build_memory_preamble, not just the helper."""
        from src.services.memory.run.recall import build_memory_preamble

        memory = _memory(tmp_path)
        for index in range(10):
            memory.remember(f"run log entry number {index}", kind=KIND_EPISODIC)
            time.sleep(0.005)
        memory.remember("the user prefers duckdb", kind=KIND_SEMANTIC)

        block = build_memory_preamble(memory, "duckdb")

        assert "the user prefers duckdb" in block
