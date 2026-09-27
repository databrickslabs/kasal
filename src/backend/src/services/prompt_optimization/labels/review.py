"""The crew's evaluation traces: how they are tagged, and the human review on them.

Every evaluation answer a crew optimization produces is logged as a trace
tagged with the crew id AND the workspace (group) id. Several workspaces can
share one experiment, so every read filters on BOTH tags: one workspace's
review notes, suggested labels and alignment examples never reach another's.

The review on those traces is the Feedback and Expectations people add (in
the Optimize dialog or the MLflow UI). It is harvested into:

* ``req_texts``: every expectation and rationale, distilled into the run's
  human requirements (the checklist, and ExpectationsGuidelines' guidelines);
* ``suggestions``: the ``human_expectation`` notes, offered in the dialog as
  SUGGESTED labels. They are never used as labels until the user confirms one.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Protocol, Sequence, Tuple

#: Traces searched per harvest (newest notes win; see ``harvest_review``).
TRACE_WINDOW = 50
TAG_CREW = "kasal_crew_id"
TAG_GROUP = "kasal_group_id"
SUGGESTION_NAME = "human_expectation"
MAX_SUGGESTIONS = 5


class _Trace(Protocol):
    """The part of an mlflow ``Trace`` the harvest reads."""

    info: object

    def search_assessments(self) -> Optional[Sequence[object]]: ...


def group_tag(group_id: Optional[str]) -> str:
    """The workspace tag value (quotes dropped: it goes into a filter string)."""
    return (group_id or "base").replace("'", "")


def trace_tags(crew_id: str, group_id: Optional[str]) -> Dict[str, str]:
    """The tags an evaluation trace is logged with."""
    return {TAG_CREW: crew_id, TAG_GROUP: group_tag(group_id)}


def trace_filter(crew_id: str, group_id: Optional[str]) -> str:
    """``search_traces`` filter for one crew's traces in one workspace."""
    crew = str(crew_id).replace("'", "")
    return f"tags.{TAG_CREW} = '{crew}' AND tags.{TAG_GROUP} = '{group_tag(group_id)}'"


@dataclass(frozen=True)
class ReviewHarvest:
    """What people said about a crew's past answers."""

    #: Assessments with a value or a reason (the run shows this count).
    note_count: int = 0
    req_texts: Tuple[str, ...] = ()
    #: ``human_expectation`` notes, newest first, deduplicated.
    suggestions: Tuple[str, ...] = ()


def _value(assessment: object, kind: str) -> object:
    return getattr(getattr(assessment, kind, None), "value", None)


def _request_time(trace: _Trace) -> int:
    return int(getattr(trace.info, "request_time", 0) or 0)


def harvest_review(traces: Iterable[_Trace]) -> ReviewHarvest:
    """The review on ``traces``, oldest first so later steps keep the newest."""
    notes = 0
    req_texts: List[str] = []
    suggestions: List[str] = []
    for trace in sorted(traces, key=_request_time):
        for assessment in trace.search_assessments() or []:
            value = _value(assessment, "feedback")
            expected = _value(assessment, "expectation")
            if expected is not None:
                req_texts.append(str(expected))
                if getattr(assessment, "name", "") == SUGGESTION_NAME:
                    suggestions.append(str(expected).strip())
            rationale = getattr(assessment, "rationale", None) or ""
            if rationale:
                req_texts.append(rationale)
            if value is not None or expected is not None or rationale:
                notes += 1
    newest = list(dict.fromkeys(s for s in reversed(suggestions) if s))
    return ReviewHarvest(notes, tuple(req_texts), tuple(newest[:MAX_SUGGESTIONS]))


def read_review(crew_id: str, group_id: Optional[str]) -> ReviewHarvest:
    """Harvest one crew's review in one workspace. Blocking; call inside
    ``mlflow_session(backend)`` (on Databricks it also sets the SQL warehouse
    that reading traces needs)."""
    import mlflow

    traces = mlflow.search_traces(
        filter_string=trace_filter(crew_id, group_id),
        max_results=TRACE_WINDOW,
        return_type="list",
    )
    return harvest_review(traces)
