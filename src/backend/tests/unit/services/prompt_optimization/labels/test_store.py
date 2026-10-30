"""Crew labels in the MLflow Prompt Registry: round trip and workspace isolation."""

from types import SimpleNamespace

import pytest

from src.services.prompt_optimization.labels.store import CrewLabels, LabelStore

CREW = "11111111-2222-3333-4444-555555555555"


class FakeRegistry:
    """The prompt-registry calls the store makes, in memory."""

    def __init__(self):
        self.versions = {}
        self.writes = 0

    def search_prompt_versions(self, name, max_results=None):
        if name not in self.versions:
            raise RuntimeError(f"Prompt {name} does not exist")
        return list(self.versions[name])

    def load_prompt(self, name, version, allow_missing=True):
        for item in self.versions.get(name, []):
            if item.version == version:
                return item
        return None

    def register_prompt(self, name, template, commit_message=None, tags=None):
        items = self.versions.setdefault(name, [])
        item = SimpleNamespace(
            version=len(items) + 1, template=template, tags=dict(tags or {})
        )
        items.append(item)
        self.writes += 1
        return item


@pytest.fixture
def registry():
    return FakeRegistry()


def _store(registry, uc_schema=None):
    return LabelStore("http://localhost:5000", uc_schema, client=registry)


LABELS = CrewLabels.of([" Zurich is listed ", "", "prices in CHF"], " A table ")


class TestCrewLabels:
    def test_blank_facts_are_dropped_and_text_trimmed(self):
        assert LABELS.expected_facts == ("Zurich is listed", "prices in CHF")
        assert LABELS.expected_response == "A table"

    def test_expectations_carry_only_what_is_set(self):
        assert CrewLabels.of(["a"], "").expectations() == {"expected_facts": ["a"]}
        assert CrewLabels.of([], "r").expectations() == {"expected_response": "r"}
        assert CrewLabels().expectations() == {}
        assert CrewLabels().is_empty()


class TestLabelStore:
    def test_round_trip(self, registry):
        store = _store(registry)
        assert store.load(CREW, "group_a") is None
        assert store.save(CREW, "group_a", LABELS) is True
        assert store.load(CREW, "group_a") == LABELS

    def test_the_newest_version_wins(self, registry):
        store = _store(registry)
        store.save(CREW, "group_a", LABELS)
        newer = CrewLabels.of(["only this"])
        store.save(CREW, "group_a", newer)
        assert store.load(CREW, "group_a") == newer

    def test_workspace_a_never_sees_workspace_b(self, registry):
        store = _store(registry)
        store.save(CREW, "group_b", CrewLabels.of(["B's secret fact"]))
        assert store.load(CREW, "group_a") is None
        store.save(CREW, "group_a", LABELS)
        assert store.load(CREW, "group_a") == LABELS
        assert store.load(CREW, "group_b") == CrewLabels.of(["B's secret fact"])

    def test_a_prompt_tagged_for_another_workspace_is_not_read(self, registry):
        store = _store(registry)
        name = store.prompt_name(CREW, "group_a")
        registry.register_prompt(
            name,
            '{"expected_facts": ["planted"], "expected_response": ""}',
            tags={"kasal_crew_id": CREW, "kasal_group_id": "group_b"},
        )
        assert store.load(CREW, "group_a") is None

    def test_versions_are_tagged_with_crew_and_workspace(self, registry):
        store = _store(registry)
        store.save(CREW, "group_a", LABELS)
        (version,) = registry.versions[store.prompt_name(CREW, "group_a")]
        assert version.tags == {
            "kasal_kind": "labels",
            "kasal_crew_id": CREW,
            "kasal_group_id": "group_a",
        }

    def test_an_unchanged_save_writes_nothing(self, registry):
        store = _store(registry)
        store.save(CREW, "group_a", LABELS)
        assert store.save(CREW, "group_a", LABELS) is False
        assert store.save(CREW, "group_x", CrewLabels()) is False  # nothing to clear
        assert registry.writes == 1

    def test_clearing_saved_labels_is_a_new_version(self, registry):
        store = _store(registry)
        store.save(CREW, "group_a", LABELS)
        assert store.save(CREW, "group_a", CrewLabels()) is True
        assert store.load(CREW, "group_a") == CrewLabels()

    def test_names_are_per_crew_and_workspace_and_uc_prefixed(self, registry):
        local = _store(registry).prompt_name(CREW, "user@example.com")
        assert local == "kasal_labels__crew_111111112222__user_example_com"
        uc = _store(registry, "main.kasal").prompt_name(CREW, None)
        assert uc == "main.kasal.kasal_labels__crew_111111112222__base"

    def test_a_uc_permission_denial_names_the_grant(self, registry):
        def denied(**_kwargs):
            raise RuntimeError("PERMISSION_DENIED: User does not have MANAGE")

        registry.register_prompt = denied
        with pytest.raises(ValueError, match="GRANT"):
            _store(registry, "main.kasal").save(CREW, "g", LABELS)
