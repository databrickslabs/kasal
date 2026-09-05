"""Save-time consolidation — consolidation_threshold / consolidation_limit,
which used to be dropped on the floor, now fold a near-duplicate write into
the record it duplicates."""

from src.services.memory.engine import Memory
from src.services.memory.engine.consolidation import (
    MERGED_CATEGORY_CAP,
    consolidate_on_save,
    find_duplicate,
    merge_categories,
    similarity_of,
)
from src.services.memory.engine.types import MemoryRecord


class _FakeLLM:
    def __init__(self, reply: str):
        self.reply = reply
        self.calls: list = []

    def call(self, messages, *args, **kwargs):
        self.calls.append(messages)
        return self.reply


class _Store:
    """Records with the similarity the backend would stamp on a hit."""

    def __init__(self, existing: list[tuple[MemoryRecord, dict]]):
        self.records = {r.id: r for r, _ in existing}
        self.stamps = {r.id: meta for r, meta in existing}
        self.saved: list[list[MemoryRecord]] = []
        self.updates: list[tuple[str, dict]] = []
        self.searches: list[dict] = []

    def has_records(self, scope):
        return bool(self.records)

    def search(self, query, limit=10, scope=None, score_threshold=None):
        self.searches.append({"query": query, "limit": limit, "scope": scope})
        out = []
        for rid, record in self.records.items():
            copy = record.model_copy(deep=True)
            copy.metadata.update(self.stamps[rid])
            out.append(copy)
        return out[:limit]

    def save(self, records):
        self.saved.append(list(records))
        for r in records:
            self.records[r.id] = r
        return list(records)

    def update(self, record_id, **changes):
        self.updates.append((record_id, changes))
        record = self.records[record_id]
        for k, v in changes.items():
            if hasattr(record, k):
                setattr(record, k, v)
        return record


def _existing(content="Aarau rave shooting: one dead, five injured.", **stamp):
    rec = MemoryRecord(
        id="existing-1",
        content=content,
        scope="/g",
        categories=["swiss-news"],
        importance=0.6,
        metadata={"agent_role": "Analyst"},
    )
    return rec, stamp


NEW = "Shooting at the Aarau rave: 1 killed, 5 hurt, manhunt under way."


class TestSimilaritySource:
    def test_semantic_component_wins_over_the_blend(self):
        rec = MemoryRecord(
            content="x", scope="/g", metadata={"similarity": 0.7, "semantic": 0.9}
        )
        assert similarity_of(rec) == 0.9

    def test_blend_when_no_semantic(self):
        rec = MemoryRecord(content="x", scope="/g", metadata={"similarity": 0.7})
        assert similarity_of(rec) == 0.7
        assert similarity_of(MemoryRecord(content="x", scope="/g")) is None


class TestFindDuplicate:
    def test_above_threshold_is_the_fold_target(self):
        store = _Store([_existing(semantic=0.91)])
        memory = Memory(storage=store, analyze_on_save=False)
        hit = find_duplicate(memory, MemoryRecord(content=NEW, scope="/g"), "/g")
        assert hit is not None and hit[0].id == "existing-1" and hit[1] == 0.91
        assert store.searches[0]["limit"] == 5  # consolidation_limit

    def test_below_threshold_inserts(self):
        store = _Store([_existing(semantic=0.6)])
        memory = Memory(storage=store, analyze_on_save=False)
        assert (
            find_duplicate(memory, MemoryRecord(content=NEW, scope="/g"), "/g") is None
        )

    def test_threshold_zero_or_limit_zero_disables(self):
        store = _Store([_existing(semantic=0.99)])
        off = Memory(storage=store, analyze_on_save=False, consolidation_threshold=0)
        assert find_duplicate(off, MemoryRecord(content=NEW, scope="/g"), "/g") is None
        off = Memory(storage=store, analyze_on_save=False, consolidation_limit=0)
        assert find_duplicate(off, MemoryRecord(content=NEW, scope="/g"), "/g") is None
        assert store.searches == []

    def test_maintenance_output_is_never_folded(self):
        store = _Store([_existing(semantic=0.99)])
        memory = Memory(storage=store, analyze_on_save=False)
        merged = MemoryRecord(content=NEW, scope="/g", source="consolidation")
        assert find_duplicate(memory, merged, "/g") is None


class TestConsolidateOnSave:
    def test_llm_merge_rewrites_the_existing_record(self):
        store = _Store([_existing(semantic=0.9)])
        llm = _FakeLLM(
            '{"content": "Aarau rave shooting: one dead, five injured; manhunt under way."}'
        )
        memory = Memory(storage=store, llm=llm, analyze_on_save=False)
        new = MemoryRecord(
            content=NEW, scope="/g", categories=["crime"], importance=0.8
        )
        folded = consolidate_on_save(memory, new, "/g")
        assert folded is not None and folded.id == "existing-1"
        rid, changes = store.updates[0]
        assert rid == "existing-1"
        assert changes["content"].endswith("manhunt under way.")
        assert changes["embedding"] is None  # content changed → re-embed
        assert changes["categories"] == ["crime", "swiss-news"]
        assert changes["importance"] == 0.8
        assert changes["metadata"]["consolidated_writes"] == 1
        assert changes["metadata"]["consolidation_similarity"] == 0.9
        assert changes["metadata"]["agent_role"] == "Analyst"
        assert "semantic" not in changes["metadata"]

    def test_a_folded_write_keeps_the_runs_that_contributed(self):
        """A run whose only write was folded into an older record looked like it
        saved nothing: the record carried the old run's stamp. Provenance
        survives the fold, oldest first, no repeats."""
        existing_rec, stamp = _existing(semantic=0.9)
        existing_rec.metadata["execution_id"] = "run-old"
        store = _Store([(existing_rec, stamp)])
        memory = Memory(
            storage=store,
            llm=_FakeLLM('{"content": "merged note"}'),
            analyze_on_save=False,
        )
        new = MemoryRecord(
            content=NEW,
            scope="/g",
            metadata={"execution_id": "run-new"},
            importance=0.8,
        )
        consolidate_on_save(memory, new, "/g")
        assert store.updates[0][1]["metadata"]["execution_ids"] == [
            "run-old",
            "run-new",
        ]

        again = MemoryRecord(
            content=NEW + " Again.", scope="/g", metadata={"execution_id": "run-new"}
        )
        consolidate_on_save(memory, again, "/g")
        assert store.updates[1][1]["metadata"]["execution_ids"] == [
            "run-old",
            "run-new",
        ]

    def test_without_an_llm_the_pass_is_skipped_and_the_note_inserted(self):
        # No model → no safe merge. Folding would silently drop a note that
        # merely resembles an old one (yesterday's report vs today's).
        store = _Store([_existing(semantic=0.9)])
        memory = Memory(storage=store, llm=None, analyze_on_save=False)
        assert (
            consolidate_on_save(memory, MemoryRecord(content=NEW, scope="/g"), "/g")
            is None
        )
        assert store.updates == [] and store.searches == []

    def test_llm_reply_without_content_inserts_normally(self):
        store = _Store([_existing(semantic=0.9)])
        memory = Memory(
            storage=store, llm=_FakeLLM("no json here"), analyze_on_save=False
        )
        assert (
            consolidate_on_save(memory, MemoryRecord(content=NEW, scope="/g"), "/g")
            is None
        )
        assert store.updates == []

    def test_failures_insert_normally(self):
        class Broken(_Store):
            def search(self, *a, **k):
                raise RuntimeError("backend down")

        memory = Memory(
            storage=Broken([_existing(semantic=0.9)]), analyze_on_save=False
        )
        assert (
            consolidate_on_save(memory, MemoryRecord(content=NEW, scope="/g"), "/g")
            is None
        )


class TestRememberIntegration:
    def test_duplicate_write_is_folded_not_inserted_and_hooks_see_the_target(self):
        store = _Store([_existing(semantic=0.9)])
        llm = _FakeLLM('{"content": "merged note"}')
        memory = Memory(storage=store, llm=llm, analyze_on_save=False, root_scope="/g")
        seen: list = []
        memory.add_save_hook(lambda recs: seen.extend(recs))

        landed = memory.remember(NEW)

        assert store.saved == []  # nothing inserted
        assert landed is not None and landed.id == "existing-1"
        assert landed.content == "merged note"
        assert [r.id for r in seen] == ["existing-1"]

    def test_distinct_write_is_inserted(self):
        store = _Store([_existing(semantic=0.4)])
        memory = Memory(storage=store, analyze_on_save=False, root_scope="/g")
        landed = memory.remember("The Federal Council kept the 2026 quotas unchanged.")
        assert landed is not None and landed.id != "existing-1"
        assert len(store.saved) == 1 and store.updates == []


class TestMergedTagsStayBounded:
    """Merges took the UNION of every folded record's tags: one record in a real
    store carried 22, and recalled once it painted the whole concept graph."""

    def test_shared_tags_lead_then_each_sources_own_in_order(self):
        newer = ["lebanon-news", "displacement", "middle-east"]
        older = ["middle-east", "lebanon-politics", "lebanon-news"]
        assert merge_categories(newer, older) == [
            "lebanon-news",
            "middle-east",
            "displacement",
            "lebanon-politics",
        ]

    def test_the_cap_holds_and_blanks_and_none_are_ignored(self):
        many = [f"tag-{i}" for i in range(12)]
        assert merge_categories(many, None, [""]) == many[:MERGED_CATEGORY_CAP]
        assert merge_categories(None, []) == []

    def test_a_fold_keeps_at_most_the_cap(self):
        existing_rec, stamp = _existing(semantic=0.9)
        existing_rec.categories = [f"old-{i}" for i in range(10)]
        store = _Store([(existing_rec, stamp)])
        memory = Memory(
            storage=store, llm=_FakeLLM('{"content": "merged"}'), analyze_on_save=False
        )
        new = MemoryRecord(
            content=NEW, scope="/g", categories=["crime", "old-3"], importance=0.8
        )
        consolidate_on_save(memory, new, "/g")
        tags = store.updates[0][1]["categories"]
        assert len(tags) == MERGED_CATEGORY_CAP
        assert tags[0] == "old-3"  # shared by both sides leads
        assert tags[1] == "crime"  # then the newer record's own
