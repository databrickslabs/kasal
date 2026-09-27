"""The run's Auto decision as the first span of the run's own trace.

Auto is decided in the API process before the run exists (``run_freeze``), and
the decision is written there as an ``execution_trace`` row whose ``span_id`` and
``trace_id`` are OTel ids made at decision time (``ModelSelection``). The run
then continues that trace instead of starting its own:

- **OTel / MLflow** (crew and flow subprocesses, ``OTelEventBridge``): the
  decision is emitted once, as span ``kasal.decision.model_selection`` with
  exactly those ids (``SeedableIdGenerator`` on the run's provider), and every
  span that would otherwise start a new trace is parented to it. The run's LLM
  spans therefore share the decision's trace id, and the MLflow exporter, which
  buffers the run's spans, holds the decision beside them.
- **The chosen model's LLM spans** carry ``kasal.auto.selected_model`` and
  ``kasal.auto.decision_span_id`` (``trace_metadata`` ``auto_selected_model`` /
  ``auto_decision_span_id`` on their rows).
- **execution_trace**: subprocess rows come from those spans; the DB exporter
  skips the decision span, since the API process already wrote its row. The
  chat path has no bridge and links its hand-built rows with ``link_row``.

Nothing here reads a prompt or a key: the payload is ``DecisionTrace``.
"""

import threading
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Iterator, Mapping, MutableMapping, Optional

from opentelemetry.context import Context
from opentelemetry.sdk.trace.id_generator import RandomIdGenerator
from opentelemetry.trace import (
    NonRecordingSpan,
    SpanContext,
    TraceFlags,
    Tracer,
    set_span_in_context,
)
from opentelemetry.util.types import AttributeValue

from src.core.llm.transport.served_model import served_model_if_different
from src.services.decisions.model_selection import (
    POLICY,
    SPAN_NAME,
    DecisionTrace,
)

#: Where the decision rides in a crew/flow subprocess payload.
PAYLOAD_KEY = "auto_decision"

ATTR_SELECTED_MODEL = "kasal.auto.selected_model"
ATTR_DECISION_SPAN = "kasal.auto.decision_span_id"
#: On the decision span: its row is already in execution_trace.
ATTR_PERSISTED = "kasal.decision.persisted"

LLM_EVENT_TYPES = frozenset({"llm_call", "llm_response", "llm_call_failed"})


class SeedableIdGenerator(RandomIdGenerator):
    """Random ids, except for the one span a thread asks to give given ids.

    The only way to give an SDK span chosen ids through public API: the
    provider's id generator. One-shot and thread-local, so a span another
    thread starts meanwhile still gets random ids.
    """

    def __init__(self) -> None:
        self._seed = threading.local()

    @contextmanager
    def seeded(self, trace_id: int, span_id: int) -> Iterator[None]:
        self._seed.trace_id, self._seed.span_id = trace_id, span_id
        try:
            yield
        finally:
            self._seed.trace_id = self._seed.span_id = None

    def generate_span_id(self) -> int:
        seeded = getattr(self._seed, "span_id", None)
        if seeded is None:
            return super().generate_span_id()
        self._seed.span_id = None
        return int(seeded)

    def generate_trace_id(self) -> int:
        seeded = getattr(self._seed, "trace_id", None)
        if seeded is None:
            return super().generate_trace_id()
        self._seed.trace_id = None
        return int(seeded)


def _hex_id(value: object) -> Optional[int]:
    try:
        parsed = int(str(value), 16)
    except ValueError:
        return None
    return parsed or None


@dataclass(frozen=True)
class AutoDecision:
    """A run's Auto decision, as its trace needs it."""

    trace: DecisionTrace
    trace_id: int
    span_id: int

    @classmethod
    def from_trace(cls, trace: object) -> Optional["AutoDecision"]:
        """From a ``DecisionTrace`` (or a payload that should be one), else None."""
        if not isinstance(trace, Mapping) or trace.get("policy") != POLICY:
            return None
        trace_id, span_id = _hex_id(trace.get("trace_id")), _hex_id(
            trace.get("span_id")
        )
        if trace_id is None or span_id is None:
            return None
        known: DecisionTrace = {
            "policy": POLICY,
            "trace_id": str(trace.get("trace_id")),
            "span_id": str(trace.get("span_id")),
            "model": _text(trace.get("model")),
            "status": str(trace.get("status") or ""),
            "reason": _text(trace.get("reason")),
            "connection": _text(trace.get("connection")),
            "picked": _text(trace.get("picked")),
            "candidates": _number(trace.get("candidates")),
            "duration_ms": _duration(trace.get("duration_ms")),
            "summary": str(trace.get("summary") or ""),
        }
        return cls(known, trace_id, span_id)

    @classmethod
    def from_payload(cls, config: object) -> Optional["AutoDecision"]:
        """The decision stamped into a run's payload (``PAYLOAD_KEY``), if any."""
        if not isinstance(config, Mapping):
            return None
        return cls.from_trace(config.get(PAYLOAD_KEY))

    @property
    def span_context(self) -> SpanContext:
        return SpanContext(
            self.trace_id, self.span_id, False, TraceFlags(TraceFlags.SAMPLED)
        )

    def parent_context(self) -> Context:
        """Parent a span to the decision (it has ended; the ids are enough)."""
        return set_span_in_context(NonRecordingSpan(self.span_context))

    def span_attributes(self) -> dict[str, AttributeValue]:
        """The decision span's attributes: what was decided, never the prompt."""
        trace = self.trace
        attributes: dict[str, AttributeValue] = {
            "kasal.event_type": "decision_evaluated",
            "kasal.output_content": trace["summary"],
            "kasal.decision.policy": POLICY,
            "kasal.decision.status": trace["status"],
            "kasal.decision.candidates": trace["candidates"],
            "kasal.decision.duration_ms": trace["duration_ms"],
            ATTR_PERSISTED: True,
        }
        optional = {
            "kasal.decision.connection": trace["connection"],
            "kasal.decision.picked": trace["picked"],
            "kasal.decision.model": trace["model"],
            "kasal.decision.reason": trace["reason"],
        }
        attributes.update({k: v for k, v in optional.items() if v})
        return attributes

    def emit(self, tracer: Tracer) -> bool:
        """Emit the decision span with its own ids; False if the ids cannot be given."""
        ids = getattr(tracer, "id_generator", None)
        if not isinstance(ids, SeedableIdGenerator):
            return False
        with ids.seeded(self.trace_id, self.span_id):
            span = tracer.start_span(
                SPAN_NAME, context=Context(), attributes=self.span_attributes()
            )
        span.end()
        return True

    def llm_attributes(self, model: object) -> dict[str, str]:
        """The link a call to the chosen model carries; {} for any other model."""
        chosen = self.trace["model"]
        if not chosen or not isinstance(model, str) or not model:
            return {}
        if served_model_if_different(chosen, model) is not None:
            return {}
        return {
            ATTR_SELECTED_MODEL: chosen,
            ATTR_DECISION_SPAN: self.trace["span_id"],
        }


def _text(value: object) -> Optional[str]:
    return str(value) if value not in (None, "") else None


def _number(value: object) -> int:
    return int(value) if isinstance(value, (int, float)) else 0


def _duration(value: object) -> float:
    return float(value) if isinstance(value, (int, float)) else 0.0


def link_row(
    row: MutableMapping[str, object], decision: Optional[AutoDecision]
) -> MutableMapping[str, object]:
    """Put a hand-built ``execution_trace`` row of the run in the decision's trace.

    The row joins the decision's trace as its child; an LLM row for the chosen
    model also gets ``auto_selected_model`` / ``auto_decision_span_id``.
    """
    if decision is None:
        return row
    row["trace_id"] = decision.trace["trace_id"]
    row.setdefault("parent_span_id", decision.trace["span_id"])
    metadata = row.get("trace_metadata")
    if row.get("event_type") in LLM_EVENT_TYPES and isinstance(metadata, dict):
        output = row.get("output")
        model = output.get("input") if isinstance(output, dict) else None
        for key, value in decision.llm_attributes(model).items():
            metadata[key.replace("kasal.auto.", "auto_")] = value
    return row
