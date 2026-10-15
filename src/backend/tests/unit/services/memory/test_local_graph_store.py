"""LocalGraphStore: dedupe on the fact's key, supersession of single-valued
facts, fused search with one hop, and tenant / session scoping."""

import pytest

from src.services.memory.engine.extract import parse_facts
from src.services.memory.storage.local_graph import LocalGraphStore

from .graph_fakes import embed, fact


def _add(store, *args, **kwargs):
    (parsed,) = parse_facts({"facts": [fact(*args, **kwargs)]})
    return store.add_fact(parsed, store.embed(parsed.statement), source="chat")


@pytest.fixture
def db(tmp_path):
    return tmp_path / "memory.db"


@pytest.fixture
def store(db):
    return LocalGraphStore(db, embedder=embed, scope="/grp")


def _search(store, query, **kwargs):
    kwargs.setdefault("min_similarity", 0.3)
    return store.search(query, store.embed(query), **kwargs)


class TestWrite:
    def test_same_fact_in_another_spelling_is_not_stored_twice(self, store):
        assert _add(store, "user", "prefers", "tables", "The user prefers tables.")
        assert _add(store, "User", "prefers", " Tables ", "User likes tables.") is None
        assert store.count() == 1

    def test_single_valued_fact_retires_the_value_it_replaces(self, store):
        old = _add(
            store,
            "Atlas",
            "deadline_is",
            "15 June",
            "Atlas is due 15 June.",
            kind="project",
        )
        new = _add(
            store,
            "atlas",
            "deadline_is",
            "30 July",
            "Atlas is due 30 July.",
            kind="project",
        )
        facts = {f.object: f for f in store.list_facts(include_retired=True)}
        assert facts["15 June"].valid_to is not None
        assert facts["15 June"].superseded_by == new.id != old.id
        assert facts["30 July"].valid_to is None
        assert store.count() == 1

    def test_multi_valued_facts_coexist(self, store):
        _add(store, "user", "uses", "Spark", "The user uses Spark.")
        _add(store, "user", "uses", "dbt", "The user uses dbt.")
        assert store.count() == 2

    def test_a_retired_value_can_become_current_again(self, store):
        _add(store, "Atlas", "status_is", "active", "Atlas is active.", kind="project")
        _add(store, "Atlas", "status_is", "paused", "Atlas is paused.", kind="project")
        assert _add(
            store,
            "Atlas",
            "status_is",
            "active",
            "Atlas is active again.",
            kind="project",
        )
        current = [f.object for f in store.list_facts()]
        assert current == ["active"]


class TestCorrections:
    """The same fact with different qualifiers is a corrected measure."""

    def _campaign(self, store, qualifiers):
        (parsed,) = parse_facts(
            {
                "facts": [
                    fact(
                        "Spring Promo",
                        "run_by",
                        "Acme",
                        "Spring Promo is run by Acme.",
                        object_type="organization",
                        kind="project",
                    )
                ]
            }
        )
        return store.add_fact(
            parsed,
            store.embed(parsed.statement),
            metadata={"qualifiers": qualifiers} if qualifiers else None,
        )

    def test_a_corrected_measure_replaces_the_old_one(self, store):
        old = self._campaign(store, {"roas": "4.10x"})
        new = self._campaign(store, {"roas": "4.36x"})
        facts = {f.id: f for f in store.list_facts(include_retired=True)}
        assert facts[old.id].valid_to is not None
        assert facts[old.id].superseded_by == new.id
        assert [f.metadata["qualifiers"] for f in store.list_facts()] == [
            {"roas": "4.36x"}
        ]

    def test_the_same_measure_again_is_a_repeat(self, store):
        assert self._campaign(store, {"roas": "4.36x"})
        assert self._campaign(store, {"roas": "4.36x"}) is None
        assert store.count() == 1

    def test_a_terser_restatement_keeps_the_detail(self, store):
        assert self._campaign(store, {"roas": "4.36x"})
        assert self._campaign(store, None) is None
        assert [f.metadata["qualifiers"] for f in store.list_facts()] == [
            {"roas": "4.36x"}
        ]


class TestSearch:
    def test_retired_facts_are_never_recalled(self, store):
        _add(
            store,
            "Atlas",
            "deadline_is",
            "15 June",
            "The Atlas deadline is 15 June.",
            kind="project",
        )
        _add(
            store,
            "Atlas",
            "deadline_is",
            "30 July",
            "The Atlas deadline is 30 July.",
            kind="project",
        )
        statements = [f.statement for f, _ in _search(store, "Atlas deadline")]
        assert statements == ["The Atlas deadline is 30 July."]

    def test_unrelated_query_recalls_nothing(self, store):
        _add(store, "user", "works_at", "Acme", "The user works at Acme.")
        assert _search(store, "latest news from Lebanon today") == []

    def test_only_what_matches_is_recalled(self, store):
        """Following a matched fact's entity to its other facts was tried: a
        Swiss-news request matched "based in Zurich, Switzerland" and came back
        with that person's whole career."""
        _add(
            store,
            "Atlas",
            "deadline_is",
            "30 July",
            "The Atlas deadline is 30 July.",
            kind="project",
        )
        _add(
            store,
            "Atlas",
            "owned_by",
            "Dana",
            "Dana runs it.",
            kind="project",
            object_type="person",
        )
        hits = _search(store, "Atlas deadline", min_similarity=0.5)
        assert [(f.statement, sim is not None) for f, sim in hits] == [
            ("The Atlas deadline is 30 July.", True)
        ]

    def test_no_embedding_means_no_recall(self, store):
        _add(store, "user", "works_at", "Acme", "The user works at Acme.")
        assert store.search("where does the user work", None) == []


class TestScoping:
    def test_scopes_in_one_file_do_not_see_each_other(self, db):
        a = LocalGraphStore(db, embedder=embed, scope="/a")
        b = LocalGraphStore(db, embedder=embed, scope="/b")
        _add(a, "user", "works_at", "Acme", "The user works at Acme.")
        assert _search(b, "where does the user work", min_similarity=0.0) == []
        assert (a.count(), b.count()) == (1, 0)
        b.reset()
        assert a.count() == 1

    def test_session_only_reads_stay_in_the_session(self, db):
        one = LocalGraphStore(db, embedder=embed, scope="/a", session_id="s1")
        _add(one, "user", "works_at", "Acme", "The user works at Acme.")
        other = LocalGraphStore(
            db, embedder=embed, scope="/a", session_id="s2", workspace_wide=False
        )
        assert _search(other, "where does the user work") == []
        assert len(_search(one, "where does the user work")) == 1

    def test_shares_the_file_with_the_record_store(self, db):
        from src.services.memory.storage.local import LocalStorageBackend

        LocalStorageBackend(db, embedder=embed)
        store = LocalGraphStore(db, embedder=embed, scope="/grp")
        assert _add(store, "user", "works_at", "Acme", "The user works at Acme.")
