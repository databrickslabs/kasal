"""Labels in the Optimize dialog and at run start (the service mixin).

MLflow is mocked: the backend resolution, ``mlflow_session`` and the trace
search. The label store is the real one over an in-memory registry.
"""

import asyncio
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.schemas.prompt_optimization import CrewLabelsPayload, CrewOptimizationRequest
from src.services.prompt_optimization.gepa.mlflow_session import MLflowBackend
from src.services.prompt_optimization.labels import operations
from src.services.prompt_optimization.labels.review import ReviewHarvest
from src.services.prompt_optimization.labels.store import CrewLabels, LabelStore
from src.services.prompt_optimization.service import PromptOptimizationService
from tests.unit.services.prompt_optimization.labels.test_store import FakeRegistry

CREW = "11111111-2222-3333-4444-555555555555"
DATABRICKS = MLflowBackend(
    kind="databricks",
    experiment="/Shared/kasal",
    auth=SimpleNamespace(workspace_url="https://example.com", token="t"),
    warehouse_id="wh",
)
NOTES = ReviewHarvest(
    note_count=2,
    req_texts=("German side only", "Wrong region"),
    suggestions=("German side only",),
)


def _group(group_id):
    return SimpleNamespace(primary_group_id=group_id, access_token=None)


class _Mlflow:
    """Records what the mixin did against MLflow."""

    def __init__(self, review=NOTES, backend=DATABRICKS):
        self.registry = FakeRegistry()
        self.review = review
        self.backend = backend
        self.sessions = []
        self.reads = []

    @contextmanager
    def session(self, backend):
        self.sessions.append(backend.kind)
        yield

    def read_review(self, crew_id, group_id):
        self.reads.append((crew_id, group_id))
        if isinstance(self.review, Exception):
            raise self.review
        return self.review

    @contextmanager
    def patched(self):
        with (
            patch.object(
                operations,
                "resolve_mlflow_backend",
                AsyncMock(return_value=self.backend),
            ),
            patch.object(operations, "mlflow_session", self.session),
            patch.object(operations, "read_review", self.read_review),
            patch.object(
                operations,
                "LabelStore",
                lambda uri, schema: LabelStore(uri, schema, client=self.registry),
            ),
        ):
            yield self


def _service():
    service = PromptOptimizationService.__new__(PromptOptimizationService)
    service.session = MagicMock()
    service._judge_registry_target = AsyncMock(return_value=("http://mlflow", None))
    return service


def _prepare(fake, builtin_judges, labels=None, group="group_a"):
    request = CrewOptimizationRequest(
        crew_id=CREW, builtin_judges=builtin_judges, labels=labels
    )
    with fake.patched():
        return asyncio.run(_service()._prepare_run_labels(CREW, request, _group(group)))


CONFIRMED = CrewLabelsPayload(expected_facts=["Zurich is listed"], expected_response="")


class TestRunStart:
    def test_suggestions_are_never_labels_unless_confirmed(self):
        fake = _Mlflow()
        prepared = _prepare(fake, ["Correctness"])
        assert prepared.expectations == {}
        assert prepared.judge_ids == []
        assert prepared.skipped == ["Correctness skipped: not labelled"]
        assert fake.registry.writes == 0  # nothing confirmed, nothing stored

    def test_confirmed_labels_are_used_and_saved(self):
        fake = _Mlflow()
        prepared = _prepare(fake, ["Correctness"], labels=CONFIRMED)
        assert prepared.judge_ids == ["Correctness"]
        assert prepared.skipped == []
        assert prepared.expectations == {"expected_facts": ["Zurich is listed"]}
        store = LabelStore("http://mlflow", client=fake.registry)
        assert store.load(CREW, "group_a") == CrewLabels.of(["Zurich is listed"])

    def test_review_notes_are_expectations_guidelines_labels(self):
        prepared = _prepare(_Mlflow(), ["ExpectationsGuidelines", "Safety"])
        assert prepared.judge_ids == ["Safety", "ExpectationsGuidelines"]
        assert prepared.human_requirements == ["German side only", "Wrong region"]
        assert prepared.feedback_count == 2

    def test_without_review_notes_expectations_guidelines_is_skipped(self):
        prepared = _prepare(_Mlflow(review=ReviewHarvest()), ["ExpectationsGuidelines"])
        assert prepared.judge_ids == []
        assert prepared.skipped == ["Expectations guidelines skipped: not labelled"]

    def test_databricks_harvests_this_workspace_only(self):
        fake = _Mlflow(backend=DATABRICKS)
        _prepare(fake, [], labels=CONFIRMED, group="group_b")
        assert fake.sessions == ["databricks"]
        assert fake.reads == [(CREW, "group_b")]
        (name,) = fake.registry.versions
        assert name.endswith("__group_b")

    def test_a_failed_harvest_does_not_stop_the_run(self, caplog):
        fake = _Mlflow(review=RuntimeError("no SQL warehouse"))
        prepared = _prepare(fake, ["Correctness"], labels=CONFIRMED)
        assert prepared.judge_ids == ["Correctness"]
        assert prepared.human_requirements == []
        assert "could not harvest" in caplog.text

    def test_no_mlflow_backend_still_uses_the_request_labels(self):
        fake = _Mlflow(backend=None)
        prepared = _prepare(fake, ["Correctness"], labels=CONFIRMED)
        assert prepared.judge_ids == ["Correctness"]
        assert fake.reads == []

    def test_an_unknown_judge_is_still_refused(self):
        with pytest.raises(ValueError, match="Unknown built-in judge"):
            _prepare(_Mlflow(), ["Nope"])


class TestDialogRead:
    def _get(self, fake, group):
        with fake.patched():
            return asyncio.run(_service().get_crew_labels(CREW, _group(group)))

    def test_confirmed_labels_suggestions_and_review_flag(self):
        fake = _Mlflow()
        _prepare(fake, [], labels=CONFIRMED, group="group_a")
        info = self._get(fake, "group_a")
        assert info.labels == CONFIRMED
        assert info.suggestions == ["German side only"]
        assert info.has_review_notes is True

    def test_workspace_b_never_sees_workspace_a_labels(self):
        fake = _Mlflow(review=ReviewHarvest())
        _prepare(fake, [], labels=CONFIRMED, group="group_a")
        info = self._get(fake, "group_b")
        assert info.labels is None
        assert fake.reads[-1] == (CREW, "group_b")

    def test_a_failed_read_leaves_the_fields_empty(self):
        info = self._get(_Mlflow(review=RuntimeError("down")), "group_a")
        assert info.labels is None and info.suggestions == []

    def test_no_backend_is_empty(self):
        info = self._get(_Mlflow(backend=None), "group_a")
        assert info.model_dump() == {
            "labels": None,
            "suggestions": [],
            "has_review_notes": False,
        }
