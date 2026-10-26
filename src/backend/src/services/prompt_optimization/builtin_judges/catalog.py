"""The MLflow built-in judges Kasal's Optimize can run.

A short static list. Each entry names an ``mlflow.genai.scorers`` class and how
its yes/no verdict joins the optimization score. Availability is checked
against the INSTALLED mlflow at runtime, so a version that drops or renames a
class disables that entry instead of crashing a run (the evaluation runner's
old ``Groundedness``/``Relevance``/``ContextSufficiency`` names are what that
looks like when it is not checked: silently skipped forever).

Judges run on demand as plain scorer objects. Nothing here registers a scorer,
lists the scorer registry or starts monitoring; the only stored judges are the
custom ones in the MLflow Prompt Registry.

Some judges need labels on the row (``label_fields``): Correctness reads the
crew's expected facts or expected answer, ExpectationsGuidelines the
requirements distilled from the user's review notes. ``coverage`` decides
whether a run has enough of them; a judge without them is skipped, never called.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, Dict, List, Literal, Sequence, Tuple

if TYPE_CHECKING:
    from mlflow.genai.scorers import Scorer

logger = logging.getLogger(__name__)

JudgeRole = Literal["gate", "graded"]

#: The judge score is multiplied by this when any gate judge answers "no".
#: 0.0 means an unsafe deliverable keeps only its format credit, so it can
#: never outscore a safe one.
GATE_FAILURE_MULTIPLIER = 0.0


@dataclass(frozen=True)
class BuiltinJudge:
    """One selectable built-in judge."""

    id: str  # the scorer's class name in mlflow.genai.scorers
    label: str
    description: str
    role: JudgeRole = "graded"
    #: Weight of a graded judge's 0/1 verdict in the judge score's weighted
    #: mean (Kasal's own judges weigh 1 each). Gates are not averaged.
    weight: float = 1.0
    #: Needs labels on the row: at least one of ``label_fields`` present.
    needs_labels: bool = False
    #: The expectation keys mlflow reads for this judge (any one suffices).
    label_fields: Tuple[str, ...] = ()


CATALOG: Tuple[BuiltinJudge, ...] = (
    BuiltinJudge(
        id="Safety",
        label="Safety",
        description="Flags harmful, offensive or toxic content. A 'no' zeroes "
        "the judge score.",
        role="gate",
    ),
    BuiltinJudge(
        id="RelevanceToQuery",
        label="Relevance to query",
        description="Checks that the deliverable addresses the crew's objective.",
    ),
    BuiltinJudge(
        id="Guidelines",
        label="Guidelines",
        description="Checks that the deliverable meets the tasks' expected "
        "outputs and your judging guidance.",
    ),
    BuiltinJudge(
        id="Completeness",
        label="Completeness",
        description="Checks that every part of the objective is answered.",
    ),
    BuiltinJudge(
        id="Correctness",
        label="Correctness",
        description="Checks the deliverable contains your expected facts, or "
        "agrees with your expected answer.",
        needs_labels=True,
        label_fields=("expected_facts", "expected_response"),
    ),
    BuiltinJudge(
        id="ExpectationsGuidelines",
        label="Expectations guidelines",
        description="Checks the deliverable meets the requirements distilled "
        "from your review notes on past answers.",
        needs_labels=True,
        label_fields=("guidelines",),
    ),
)

_BY_ID: Dict[str, BuiltinJudge] = {judge.id: judge for judge in CATALOG}

#: Guidelines needs guidelines to be constructed at all; used when a run has none.
DEFAULT_GUIDELINE = "The response fully and accurately addresses the request."


def get(judge_id: str) -> BuiltinJudge | None:
    return _BY_ID.get(judge_id)


def is_available(judge_id: str) -> bool:
    """Whether the installed mlflow provides this scorer."""
    try:
        from mlflow.genai import scorers
    except ImportError:
        return False
    return callable(getattr(scorers, judge_id, None))


def build_scorer(
    judge_id: str, model: str | None, guidelines: Sequence[str] = ()
) -> Scorer:
    """A fresh scorer object for ``judge_id`` (on demand; never registered).

    ``model`` is an MLflow model URI; None keeps mlflow's default judge model.
    Raises ValueError for an id that is not in the catalog or not installed
    (and pydantic's ValidationError, a ValueError, if mlflow rejects a field).
    """
    if get(judge_id) is None or not is_available(judge_id):
        raise ValueError(f"Built-in judge '{judge_id}' is not available")
    from mlflow.genai import scorers

    scorer_class: Callable[..., Scorer] = getattr(scorers, judge_id)
    kwargs: Dict[str, object] = {}
    if model:
        kwargs["model"] = model
    if judge_id == "Guidelines":
        kwargs["guidelines"] = list(guidelines) or [DEFAULT_GUIDELINE]
    return scorer_class(**kwargs)


def resolve_selection(judge_ids: Sequence[str]) -> List[BuiltinJudge]:
    """The catalog entries for a run's selection, in catalog order.

    Raises ValueError (a 400 at the router) for an unknown id or one the
    installed mlflow does not provide — a run must not start with a judge that
    would silently never score. Whether a label judge has its labels is
    ``coverage``'s call: it is skipped with a reason, not refused.
    """
    wanted = set(judge_ids)
    for judge_id in wanted:
        judge = get(judge_id)
        if judge is None:
            raise ValueError(f"Unknown built-in judge '{judge_id}'")
        if not is_available(judge_id):
            raise ValueError(
                f"Built-in judge '{judge_id}' is not provided by the installed MLflow"
            )
    return [judge for judge in CATALOG if judge.id in wanted]


def _entries() -> List[Tuple[BuiltinJudge, bool]]:
    return [(judge, is_available(judge.id)) for judge in CATALOG]


async def list_with_availability() -> List[Tuple[BuiltinJudge, bool]]:
    """Every catalog entry with whether the installed mlflow provides it.

    Off the event loop: the first call imports mlflow's scorer module.
    """
    return await asyncio.to_thread(_entries)


#: The retrieval judges. The evaluation runner once asked for them as
#: ``Groundedness``/``Relevance``/``ContextSufficiency``, names mlflow 3.x does
#: not have, so they were skipped with a warning on every run.
RETRIEVAL_JUDGES = (
    "RetrievalGroundedness",
    "RetrievalRelevance",
    "RetrievalSufficiency",
)


def evaluation_scorers(
    model_uri: str | None, has_reference: bool, has_context: bool
) -> List[Scorer]:
    """The built-in scorers an evaluation run uses.

    RelevanceToQuery and Safety always; Correctness when the rows carry a
    reference answer. The retrieval judges are NOT added: they read documents
    from RETRIEVER spans on a trace, and evaluation rows carry retrieved
    context as a plain ``contexts`` column mlflow does not read, so they could
    only fail or judge against empty context.
    """
    judge_ids = ["RelevanceToQuery", "Safety"]
    if has_reference:
        judge_ids.append("Correctness")
    if has_context:
        logger.info(
            "Retrieval judges %s skipped: they need RETRIEVER spans on a trace, "
            "and evaluation rows carry context as a plain column",
            ", ".join(RETRIEVAL_JUDGES),
        )
    scorers: List[Scorer] = []
    for judge_id in judge_ids:
        try:
            scorers.append(build_scorer(judge_id, model_uri))
        except ValueError:  # not installed, or mlflow rejected the model URI
            logger.warning("Built-in judge %s unavailable; skipped", judge_id)
    logger.info("Evaluation scorers: %s", [type(s).__name__ for s in scorers])
    return scorers
