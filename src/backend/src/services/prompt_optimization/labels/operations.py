"""Crew labels for the Optimize dialog and the run start.

Mixed into ``PromptOptimizationService`` like the judge operations. The dialog
reads the crew's confirmed labels plus the suggestions; a run starts with the
labels the request carries (what the user confirmed), saves them when they
changed, and decides which label judges have enough labels to count.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from src.schemas.prompt_optimization import (
    CrewLabelsInfo,
    CrewLabelsPayload,
    CrewOptimizationRequest,
)
from src.services.prompt_optimization.builtin_judges.catalog import resolve_selection
from src.services.prompt_optimization.builtin_judges.coverage import (
    split_by_coverage,
)
from src.services.prompt_optimization.gepa.crew_doc import _distill_requirements
from src.services.prompt_optimization.gepa.mlflow_session import (
    MLflowBackend,
    mlflow_session,
    resolve_mlflow_backend,
)
from src.services.prompt_optimization.host import PromptOptimizationHost
from src.services.prompt_optimization.labels.review import ReviewHarvest, read_review
from src.services.prompt_optimization.labels.store import CrewLabels, LabelStore
from src.utils.user_context import GroupContext

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RunLabels:
    """What a crew run grades against, decided at its start."""

    #: Built-in judges that count (label judges below the coverage floor out).
    judge_ids: List[str] = field(default_factory=list)
    #: Why each dropped judge is out, e.g. "Correctness skipped: not labelled".
    skipped: List[str] = field(default_factory=list)
    #: The user's labels as mlflow expectation fields.
    expectations: Dict[str, object] = field(default_factory=dict)
    #: Deduplicated review notes (the run refines them with the judge model).
    human_requirements: List[str] = field(default_factory=list)
    feedback_count: int = 0


def _group_id(group_context: Optional[GroupContext]) -> Optional[str]:
    return group_context.primary_group_id if group_context else None


class LabelOperationsMixin(PromptOptimizationHost):
    async def _label_target(
        self, group_context: Optional[GroupContext]
    ) -> Tuple[Optional[MLflowBackend], str, Optional[str]]:
        """``(backend, registry_uri, uc_schema)``; no backend when MLflow is
        not configured, and no registry when the registry does not resolve."""
        backend = await resolve_mlflow_backend(self.session, group_context)
        if backend is None:
            return None, "", None
        try:
            registry_uri, uc_schema = await self._judge_registry_target(group_context)
        except ValueError as exc:
            logger.warning("[labels] no prompt registry for labels: %s", exc)
            return backend, "", None
        return backend, registry_uri, uc_schema

    async def get_crew_labels(
        self, crew_id: str, group_context: Optional[GroupContext] = None
    ) -> CrewLabelsInfo:
        """The crew's confirmed labels, the suggested ones, and whether its
        review notes give ExpectationsGuidelines something to check."""
        backend, registry_uri, uc_schema = await self._label_target(group_context)
        if backend is None:
            return CrewLabelsInfo()
        group_id = _group_id(group_context)

        def _read() -> Tuple[Optional[CrewLabels], ReviewHarvest]:
            labels: Optional[CrewLabels] = None
            review = ReviewHarvest()
            with mlflow_session(backend):
                try:
                    if registry_uri:
                        store = LabelStore(registry_uri, uc_schema)
                        labels = store.load(crew_id, group_id)
                except Exception as exc:  # noqa: BLE001 — the fields just start empty
                    logger.warning("[labels] could not read labels: %s", exc)
                try:
                    review = read_review(crew_id, group_id)
                except Exception as exc:  # noqa: BLE001 — no suggestions then
                    logger.warning("[labels] could not read review notes: %s", exc)
            return labels, review

        labels, review = await asyncio.to_thread(_read)
        return CrewLabelsInfo(
            labels=CrewLabelsPayload(**labels.as_dict()) if labels else None,
            suggestions=list(review.suggestions),
            has_review_notes=bool(_distill_requirements(list(review.req_texts))),
        )

    async def _review_and_save(
        self,
        crew_id: str,
        labels: Optional[CrewLabels],
        group_context: Optional[GroupContext],
    ) -> ReviewHarvest:
        """Harvest the crew's review and, when given, save its labels.

        Advisory: a failure is logged and the run goes on with the labels the
        request carries and whatever review was read.
        """
        backend, registry_uri, uc_schema = await self._label_target(group_context)
        if backend is None:
            return ReviewHarvest()
        group_id = _group_id(group_context)

        def _run() -> ReviewHarvest:
            with mlflow_session(backend):
                if labels is not None and registry_uri:
                    try:
                        LabelStore(registry_uri, uc_schema).save(
                            crew_id, group_id, labels
                        )
                    except Exception as exc:  # noqa: BLE001 — labels still ride the run
                        logger.warning("[labels] could not save labels: %s", exc)
                return read_review(crew_id, group_id)

        try:
            return await asyncio.to_thread(_run)
        except Exception as exc:  # noqa: BLE001 — review steers, never blocks, a run
            logger.warning("[labels] could not harvest review notes: %s", exc)
            return ReviewHarvest()

    async def _prepare_run_labels(
        self,
        crew_id: str,
        request: CrewOptimizationRequest,
        group_context: Optional[GroupContext],
    ) -> RunLabels:
        """Labels, review and the judges that count, for a run about to start.

        Raises ValueError (400) for a built-in the run could not use at all;
        a label judge without labels is skipped with a reason instead.
        """
        given = request.labels
        labels = (
            CrewLabels.of(given.expected_facts, given.expected_response)
            if given is not None
            else CrewLabels()
        )
        # Off the loop: the first check imports mlflow's scorer module.
        selected = await asyncio.to_thread(resolve_selection, request.builtin_judges)
        review = await self._review_and_save(
            crew_id, labels if given is not None else None, group_context
        )
        # Deduplicated constraints, NOT the grade litany: repeating
        # "human_grade: 0.0 ..." anchored the judge to zero even for a
        # compliant answer (verified live A/B).
        requirements = _distill_requirements(list(review.req_texts))
        row = labels.expectations()
        if requirements:
            row["guidelines"] = requirements
        usable, skipped = split_by_coverage(selected, [row])
        for reason in skipped.values():
            logger.info("[labels] crew %s: %s", crew_id, reason)
        return RunLabels(
            judge_ids=[judge.id for judge in usable],
            skipped=list(skipped.values()),
            expectations=labels.expectations(),
            human_requirements=requirements,
            feedback_count=review.note_count,
        )
