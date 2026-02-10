"""KasalMLflowSpanExporter — exports OTel spans to MLflow as structured traces.

Receives completed ReadableSpan objects from the OTel pipeline, buffers them
per execution, pairs start/end events into duration spans, and constructs
MLflow traces via the MlflowClient imperative API on flush.

Event pairing:
    OTelEventBridge emits instant spans (start==end) for each CrewAI event.
    Events come in start/end pairs (e.g. crew_started/crew_completed,
    task_started/task_completed). The exporter pairs these to create duration
    spans in MLflow. Unpaired events become instant spans.

Hierarchy:
    crew_execution (root, full duration)
    ├── agent:Researcher (duration)
    │   ├── task:Research Topic (duration)
    │   │   ├── llm_call (duration/instant)
    │   │   ├── tool:WebSearch (duration)
    │   │   └── memory_write (instant)
    │   └── task:Write Article
    └── agent:Writer

Buffering strategy:
    Each exporter instance is scoped to ONE execution (crew or flow).
    All spans are buffered in a single flat list (NOT keyed by OTel trace_id,
    since flow-level events get unique trace_ids different from crew events).
    Flush triggers:
    - crew_completed: for crew-only executions
    - flow_completed: for flow executions (waits for all crews to finish)
    - shutdown()/force_flush(): final safety net
"""

import logging
import threading
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Tuple

from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult

logger = logging.getLogger(__name__)


# Pairing map: start_event_type -> end_event_type
_EVENT_PAIRS: Dict[str, str] = {
    "crew_started": "crew_completed",
    "agent_execution": "llm_response",
    "task_started": "task_completed",
    "tool_usage": "tool_usage",  # start and end both map to "tool_usage"
    "llm_call": "llm_response",
    "memory_write_started": "memory_write",
    "memory_retrieval_started": "memory_retrieval",
    "knowledge_retrieval_started": "knowledge_operation",
    "reasoning_started": "agent_reasoning",
    "guardrail_started": "llm_guardrail",
    "flow_started": "flow_completed",
    "mcp_connection_started": "mcp_connection_completed",
    "mcp_tool_started": "mcp_tool_completed",
}

# Reverse: end_event_type -> set of possible start_event_types
# (handles cases where multiple starts map to the same end, e.g. llm_response)
_END_TO_STARTS: Dict[str, List[str]] = defaultdict(list)
for _start, _end in _EVENT_PAIRS.items():
    _END_TO_STARTS[_end].append(_start)


@dataclass
class _PairedSpan:
    """A duration span constructed from a start/end event pair."""

    name: str
    start_time: int
    end_time: int
    event_type: str = ""
    agent_name: str = ""
    task_name: str = ""
    inputs: Dict[str, Any] = field(default_factory=dict)
    outputs: Dict[str, Any] = field(default_factory=dict)
    attributes: Dict[str, Any] = field(default_factory=dict)
    status: str = "OK"


@dataclass
class _InstantSpan:
    """An unpaired event that becomes a point-in-time span."""

    name: str
    timestamp: int
    event_type: str = ""
    agent_name: str = ""
    task_name: str = ""
    outputs: Dict[str, Any] = field(default_factory=dict)
    attributes: Dict[str, Any] = field(default_factory=dict)
    status: str = "OK"


# Event types that belong to each hierarchy level
_CREW_EVENTS = {"crew_started", "crew_completed", "flow_started", "flow_completed", "flow_created"}
_AGENT_EVENTS = {"agent_execution", "llm_response"}  # agent_execution start, llm_response is its end
_TASK_EVENTS = {"task_started", "task_completed", "task_failed"}


def _build_span_name(event_type: str, attrs: Dict[str, Any]) -> str:
    """Build a descriptive span name from event type and attributes."""
    agent = attrs.get("kasal.agent_name") or attrs.get("kasal.extra.agent_role", "")
    task = attrs.get("kasal.task_name") or attrs.get("kasal.extra.task_name", "")
    tool = attrs.get("kasal.tool_name") or attrs.get("kasal.extra.tool_name", "")

    if agent and "agent" in event_type:
        return f"agent:{agent}"
    if task and "task" in event_type:
        return f"task:{task[:80]}"
    if tool and "tool" in event_type:
        return f"tool:{tool}"
    return event_type


def _build_pairing_key(event_type: str, attrs: Dict[str, Any]) -> str:
    """Build a key to match start/end events.

    Uses event_type + agent/task/tool name so that concurrent agents
    or tasks don't collide.
    """
    agent = attrs.get("kasal.agent_name") or attrs.get("kasal.extra.agent_role", "")
    task = attrs.get("kasal.task_name") or attrs.get("kasal.extra.task_name", "")
    tool = attrs.get("kasal.tool_name") or attrs.get("kasal.extra.tool_name", "")

    # Use the most specific identifier available
    suffix = task or agent or tool or ""
    return f"{event_type}:{suffix}"


def _extract_agent_name(attrs: Dict[str, Any]) -> str:
    """Extract agent name from span attributes."""
    return str(
        attrs.get("kasal.agent_name")
        or attrs.get("kasal.extra.agent_role")
        or ""
    )


def _extract_task_name(attrs: Dict[str, Any]) -> str:
    """Extract task name from span attributes."""
    return str(
        attrs.get("kasal.task_name")
        or attrs.get("kasal.extra.task_name")
        or ""
    )


def _extract_span_outputs(span: ReadableSpan) -> Dict[str, Any]:
    """Extract output data from a span for MLflow outputs field."""
    attrs = dict(span.attributes) if span.attributes else {}
    outputs: Dict[str, Any] = {}

    content = attrs.get("kasal.output_content")
    if content:
        outputs["content"] = str(content)[:4000]

    # Extra data
    for key, val in attrs.items():
        if key.startswith("kasal.extra.") and val is not None:
            outputs[key[len("kasal.extra."):]] = val

    return outputs


def _extract_span_attrs(span: ReadableSpan) -> Dict[str, Any]:
    """Extract all kasal.* attributes from a span."""
    attrs = dict(span.attributes) if span.attributes else {}
    result: Dict[str, Any] = {}
    for key, val in attrs.items():
        if key.startswith("kasal.") and val is not None:
            result[key] = val
    return result


class KasalMLflowSpanExporter(SpanExporter):
    """Export OTel spans to MLflow as structured traces via MlflowClient.

    Buffers ALL spans for this execution in a single flat list.  For crew
    executions, flush triggers on ``crew_completed``.  For flow executions
    (detected by seeing a ``flow_started`` or ``flow_created`` event), flush
    waits for ``flow_completed``.  ``shutdown()`` and ``force_flush()`` act
    as safety nets to flush remaining buffers.

    Args:
        job_id: Execution/job ID for logging.
        mlflow_result: MlflowSetupResult with experiment_id and tracing_ready.
        group_context: Optional group context for tenant isolation.
        max_workers: Thread pool size for async flush operations.
    """

    def __init__(
        self,
        job_id: str,
        mlflow_result: Any,
        group_context: Any = None,
        max_workers: int = 1,
    ):
        self._job_id = job_id
        self._mlflow_result = mlflow_result
        self._group_context = group_context
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._lock = threading.Lock()

        # Single flat buffer — all spans for this execution
        self._buffer: List[ReadableSpan] = []
        self._flushed = False
        # Track whether this is a flow execution (detected from events)
        self._is_flow = False

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        """Buffer spans and trigger flush on completion events."""
        should_flush = False

        with self._lock:
            for span in spans:
                # Only buffer spans from our event bridge (have kasal.event_type).
                # Skip instrumentor spans — they lack this attribute and would
                # produce empty traces on shutdown flush.
                attrs = dict(span.attributes) if span.attributes else {}
                event_type = str(attrs.get("kasal.event_type", ""))
                if not event_type:
                    continue

                self._buffer.append(span)
                logger.debug(
                    f"[OTel-MLflow][{self._job_id}] Buffered span: "
                    f"event_type={event_type}, name={span.name}, "
                    f"buffer_size={len(self._buffer)}, is_flow={self._is_flow}"
                )

                # Detect flow context
                if event_type in ("flow_started", "flow_created"):
                    if not self._is_flow:
                        logger.info(
                            f"[OTel-MLflow][{self._job_id}] Detected flow context"
                        )
                    self._is_flow = True

                # Determine flush trigger:
                # - For flows: only flow_completed triggers flush
                # - For crews: crew_completed triggers flush
                if not self._flushed:
                    if self._is_flow and event_type == "flow_completed":
                        should_flush = True
                        logger.info(
                            f"[OTel-MLflow][{self._job_id}] Flow completed — "
                            f"triggering flush ({len(self._buffer)} spans)"
                        )
                    elif not self._is_flow and event_type == "crew_completed":
                        should_flush = True
                        logger.info(
                            f"[OTel-MLflow][{self._job_id}] Crew completed — "
                            f"triggering flush ({len(self._buffer)} spans)"
                        )

        if should_flush:
            self._executor.submit(self._flush)

        return SpanExportResult.SUCCESS

    def _flush(self) -> None:
        """Construct and send MLflow trace from buffered spans."""
        with self._lock:
            if self._flushed:
                return
            self._flushed = True
            spans = list(self._buffer)
            self._buffer.clear()

        if not spans:
            return

        try:
            # Sort by start_time
            spans.sort(key=lambda s: s.start_time or 0)

            # Pair start/end events into duration spans
            paired, instants = self._pair_events(spans)

            # Build MLflow trace
            self._build_mlflow_trace(paired, instants, spans)

            logger.info(
                f"[OTel-MLflow][{self._job_id}] Flushed trace: "
                f"{len(paired)} paired + {len(instants)} instant spans "
                f"(from {len(spans)} raw spans, flow={self._is_flow})"
            )
        except Exception as e:
            logger.error(
                f"[OTel-MLflow][{self._job_id}] Error flushing trace: {e}",
                exc_info=True,
            )

    def _pair_events(
        self, spans: List[ReadableSpan]
    ) -> Tuple[List[_PairedSpan], List[_InstantSpan]]:
        """Match *_started/*_completed events into duration pairs.

        Returns:
            Tuple of (paired_spans, instant_spans).
        """
        # Pending start events: pairing_key → (start_time_ns, span_name, event_type, agent, task, attrs, outputs)
        pending: Dict[str, Tuple[int, str, str, str, str, Dict, Dict]] = {}
        paired: List[_PairedSpan] = []
        instants: List[_InstantSpan] = []

        for span in spans:
            attrs = dict(span.attributes) if span.attributes else {}
            event_type = str(attrs.get("kasal.event_type", ""))
            span_name = _build_span_name(event_type, attrs)
            agent_name = _extract_agent_name(attrs)
            task_name = _extract_task_name(attrs)
            outputs = _extract_span_outputs(span)
            span_attrs = _extract_span_attrs(span)
            status = "ERROR" if (span.status and span.status.status_code.name == "ERROR") else "OK"

            # Try to PAIR first (check if a pending start matches this end).
            # This must come before the start-buffer check because some event
            # types (e.g. tool_usage) appear in both _EVENT_PAIRS and
            # _END_TO_STARTS — the second occurrence should pair, not buffer.
            paired_this = False
            if event_type in _END_TO_STARTS:
                for possible_start in _END_TO_STARTS[event_type]:
                    pairing_key = f"{possible_start}:{_build_pairing_key(possible_start, attrs)}"
                    if pairing_key in pending:
                        start_time, start_name, start_evt, start_agent, start_task, start_attrs, start_outputs = pending.pop(
                            pairing_key
                        )
                        merged_attrs = {**start_attrs, **span_attrs}
                        merged_outputs = {**start_outputs, **outputs}
                        merged_agent = agent_name or start_agent
                        merged_task = task_name or start_task
                        paired.append(
                            _PairedSpan(
                                name=start_name or span_name,
                                start_time=start_time,
                                end_time=span.end_time or span.start_time or 0,
                                event_type=start_evt,
                                agent_name=merged_agent,
                                task_name=merged_task,
                                inputs={},
                                outputs=merged_outputs,
                                attributes=merged_attrs,
                                status=status,
                            )
                        )
                        paired_this = True
                        break

            if not paired_this and event_type in _EVENT_PAIRS:
                # No pending match — buffer as a new start event
                pairing_key = f"{event_type}:{_build_pairing_key(event_type, attrs)}"
                pending[pairing_key] = (
                    span.start_time or 0,
                    span_name,
                    event_type,
                    agent_name,
                    task_name,
                    span_attrs,
                    outputs,
                )
            elif not paired_this:
                # Standalone event — instant span
                instants.append(
                    _InstantSpan(
                        name=span_name,
                        timestamp=span.start_time or 0,
                        event_type=event_type,
                        agent_name=agent_name,
                        task_name=task_name,
                        outputs=outputs,
                        attributes=span_attrs,
                        status=status,
                    )
                )

        # Any unmatched start events become instants
        for pairing_key, (start_time, start_name, start_evt, start_agent, start_task, start_attrs, start_outputs) in pending.items():
            instants.append(
                _InstantSpan(
                    name=start_name,
                    timestamp=start_time,
                    event_type=start_evt,
                    agent_name=start_agent,
                    task_name=start_task,
                    outputs=start_outputs,
                    attributes=start_attrs,
                )
            )

        return paired, instants

    def _determine_hierarchy_level(self, event_type: str) -> str:
        """Determine hierarchy level: 'crew', 'agent', 'task', or 'leaf'."""
        if event_type in _CREW_EVENTS:
            return "crew"
        if event_type in _AGENT_EVENTS:
            return "agent"
        if event_type in _TASK_EVENTS:
            return "task"
        return "leaf"

    def _create_mlflow_span(
        self,
        client: Any,
        trace_id: str,
        parent_id: str,
        name: str,
        start_time: int,
        end_time: int,
        outputs: Dict[str, Any],
        attributes: Dict[str, Any],
        status: str = "OK",
    ) -> Optional[str]:
        """Create an MLflow span and return its span_id."""
        try:
            child = client.start_span(
                name=name,
                trace_id=trace_id,
                parent_id=parent_id,
                start_time_ns=start_time,
            )
            end_kwargs: Dict[str, Any] = {
                "trace_id": trace_id,
                "span_id": child.span_id,
                "end_time_ns": end_time,
                "status": status,
            }
            if outputs:
                end_kwargs["outputs"] = outputs
            if attributes:
                end_kwargs["attributes"] = attributes
            client.end_span(**end_kwargs)
            return child.span_id
        except Exception as e:
            logger.debug(
                f"[OTel-MLflow][{self._job_id}] Error creating span '{name}': {e}"
            )
            return None

    def _build_mlflow_trace(
        self,
        paired: List[_PairedSpan],
        instants: List[_InstantSpan],
        all_spans: List[ReadableSpan],
    ) -> None:
        """Create MLflow trace with proper hierarchy using MlflowClient.

        Hierarchy:
            root (crew_execution)
            ├── agent:Researcher (duration)
            │   ├── task:Research Topic (duration)
            │   │   ├── llm_call (duration/instant)
            │   │   ├── tool:WebSearch (duration)
            │   │   ├── memory_retrieval (instant)
            │   │   └── memory_write (instant)
            │   └── task:Write Article
            └── agent:Writer
        """
        try:
            from mlflow.tracking import MlflowClient
        except ImportError:
            logger.warning(
                f"[OTel-MLflow][{self._job_id}] mlflow not installed, skipping trace export"
            )
            return

        client = MlflowClient()

        # Compute trace time boundaries
        start_times = [s.start_time for s in all_spans if s.start_time]
        end_times = [s.end_time or s.start_time for s in all_spans if s.start_time]
        if not start_times:
            return

        first_time = min(start_times)
        last_time = max(end_times) if end_times else first_time

        # Build trace inputs
        trace_inputs: Dict[str, Any] = {"job_id": self._job_id}
        if self._group_context:
            gid = getattr(self._group_context, "primary_group_id", None)
            if gid:
                trace_inputs["group_id"] = str(gid)

        experiment_id = getattr(self._mlflow_result, "experiment_id", None)

        try:
            start_trace_kwargs: Dict[str, Any] = {
                "name": f"kasal:{self._job_id}",
                "start_time_ns": first_time,
                "inputs": trace_inputs,
            }
            if experiment_id:
                start_trace_kwargs["experiment_id"] = str(experiment_id)

            root = client.start_trace(**start_trace_kwargs)
            root_trace_id = root.trace_id
            root_span_id = root.span_id
        except Exception as e:
            logger.error(
                f"[OTel-MLflow][{self._job_id}] Failed to start MLflow trace: {e}",
                exc_info=True,
            )
            return

        try:
            # Track created MLflow span IDs for hierarchy
            # agent_name → mlflow_span_id
            agent_spans: Dict[str, str] = {}
            # (agent_name, task_name) → mlflow_span_id
            task_spans: Dict[Tuple[str, str], str] = {}

            # Combine all items with a unified interface for ordering
            all_items: List[Dict[str, Any]] = []
            for p in paired:
                all_items.append({
                    "type": "paired",
                    "name": p.name,
                    "start_time": p.start_time,
                    "end_time": p.end_time,
                    "event_type": p.event_type,
                    "agent_name": p.agent_name,
                    "task_name": p.task_name,
                    "outputs": p.outputs,
                    "attributes": p.attributes,
                    "status": p.status,
                })
            for i in instants:
                all_items.append({
                    "type": "instant",
                    "name": i.name,
                    "start_time": i.timestamp,
                    "end_time": i.timestamp,
                    "event_type": i.event_type,
                    "agent_name": i.agent_name,
                    "task_name": i.task_name,
                    "outputs": i.outputs,
                    "attributes": i.attributes,
                    "status": i.status,
                })

            # Sort by start_time to process in chronological order
            all_items.sort(key=lambda x: x["start_time"])

            # ── Pass 1: Discover and create AGENT spans ──
            # Scan ALL items to find agent names (tasks/leaves reference agents
            # before the agent_execution event arrives).
            # Collect: agent_name → {min_start, max_end, outputs, attributes, status}
            agent_info: Dict[str, Dict[str, Any]] = {}
            for item in all_items:
                agent = item["agent_name"]
                if not agent:
                    continue
                if agent not in agent_info:
                    agent_info[agent] = {
                        "start_time": item["start_time"],
                        "end_time": item["end_time"],
                        "outputs": {},
                        "attributes": {},
                        "status": "OK",
                    }
                else:
                    info = agent_info[agent]
                    info["start_time"] = min(info["start_time"], item["start_time"])
                    info["end_time"] = max(info["end_time"], item["end_time"])
                # Use agent-level event data if available (richer outputs)
                level = self._determine_hierarchy_level(item["event_type"])
                if level == "agent":
                    agent_info[agent]["outputs"] = item["outputs"]
                    agent_info[agent]["attributes"] = item["attributes"]
                    agent_info[agent]["status"] = item["status"]

            # Create agent spans under root
            for agent, info in agent_info.items():
                span_id = self._create_mlflow_span(
                    client, root_trace_id, root_span_id,
                    name=f"agent:{agent}",
                    start_time=info["start_time"],
                    end_time=info["end_time"],
                    outputs=info["outputs"],
                    attributes=info["attributes"],
                    status=info["status"],
                )
                if span_id:
                    agent_spans[agent] = span_id

            # ── Pass 2: Create TASK spans under their agents ──
            # Also track task time ranges for leaf→task resolution
            # task_key → (start_time, end_time)
            task_time_ranges: Dict[Tuple[str, str], Tuple[int, int]] = {}
            for item in all_items:
                level = self._determine_hierarchy_level(item["event_type"])
                if level != "task":
                    continue

                agent = item["agent_name"]
                task = item["task_name"]
                task_key = (agent, task)
                if task_key not in task_spans:
                    parent = agent_spans.get(agent, root_span_id)
                    span_id = self._create_mlflow_span(
                        client, root_trace_id, parent,
                        name=item["name"],
                        start_time=item["start_time"],
                        end_time=item["end_time"],
                        outputs=item["outputs"],
                        attributes=item["attributes"],
                        status=item["status"],
                    )
                    if span_id:
                        task_spans[task_key] = span_id
                        task_time_ranges[task_key] = (
                            item["start_time"], item["end_time"]
                        )

            # ── Pass 3: Create LEAF spans (llm, tool, memory, etc.) ──
            for item in all_items:
                level = self._determine_hierarchy_level(item["event_type"])
                if level in ("crew", "agent", "task"):
                    continue

                agent = item["agent_name"]
                task = item["task_name"]

                # Parent: task (by name) > task (by time) > agent > root
                parent_id = root_span_id
                task_key = (agent, task)
                if task and task_key in task_spans:
                    parent_id = task_spans[task_key]
                elif agent:
                    # No task_name on this event — find the task that was
                    # active for this agent at this timestamp.
                    # Strategy: first try strict containment, then fall back
                    # to the most recently started task for this agent
                    # (handles post-task events like memory writes and final
                    # LLM calls that occur after task_completed).
                    ts = item["start_time"]
                    resolved_task_id = None
                    best_fallback_id = None
                    best_fallback_start = -1
                    for (tk_agent, tk_task), (t_start, t_end) in task_time_ranges.items():
                        if tk_agent != agent:
                            continue
                        if t_start <= ts <= t_end:
                            resolved_task_id = task_spans.get((tk_agent, tk_task))
                            break
                        # Track most recently started task as fallback
                        if t_start <= ts and t_start > best_fallback_start:
                            best_fallback_start = t_start
                            best_fallback_id = task_spans.get((tk_agent, tk_task))
                    if resolved_task_id:
                        parent_id = resolved_task_id
                    elif best_fallback_id:
                        # Post-task event — attach to most recent task
                        parent_id = best_fallback_id
                    elif agent in agent_spans:
                        parent_id = agent_spans[agent]

                self._create_mlflow_span(
                    client, root_trace_id, parent_id,
                    name=item["name"],
                    start_time=item["start_time"],
                    end_time=item["end_time"],
                    outputs=item["outputs"],
                    attributes=item["attributes"],
                    status=item["status"],
                )

            # End root trace
            client.end_trace(
                trace_id=root_trace_id,
                end_time_ns=last_time,
                outputs={
                    "paired_spans": len(paired),
                    "instant_spans": len(instants),
                    "total_otel_spans": len(all_spans),
                    "agents": list(agent_spans.keys()),
                    "tasks": len(task_spans),
                },
            )
        except Exception as e:
            logger.error(
                f"[OTel-MLflow][{self._job_id}] Error building MLflow trace: {e}",
                exc_info=True,
            )
            try:
                client.end_trace(
                    trace_id=root_trace_id,
                    end_time_ns=last_time,
                    status="ERROR",
                )
            except Exception:
                pass

    def shutdown(self) -> None:
        """Flush any remaining buffers and shutdown thread pool."""
        with self._lock:
            buf_size = len(self._buffer)
            flushed = self._flushed
            is_flow = self._is_flow

        logger.info(
            f"[OTel-MLflow][{self._job_id}] shutdown() called — "
            f"buffer_size={buf_size}, already_flushed={flushed}, is_flow={is_flow}"
        )

        # Flush remaining buffered spans if not already flushed
        has_remaining = buf_size > 0 and not flushed

        if has_remaining:
            try:
                self._flush()
            except Exception as e:
                logger.warning(
                    f"[OTel-MLflow][{self._job_id}] Error flushing on shutdown: {e}"
                )

        self._executor.shutdown(wait=True)
        logger.info(f"[OTel-MLflow][{self._job_id}] shutdown() complete")

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        """Flush all remaining buffered spans."""
        with self._lock:
            has_remaining = bool(self._buffer) and not self._flushed

        if has_remaining:
            try:
                self._flush()
            except Exception as e:
                logger.warning(
                    f"[OTel-MLflow][{self._job_id}] Error in force_flush: {e}"
                )
        return True
