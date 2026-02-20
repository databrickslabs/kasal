"""KasalDBSpanExporter — converts OTel ReadableSpan → execution_trace DB record.

Maps OTel span names to Kasal event_types and extracts agent/task context
from span attributes. Uses ThreadPoolExecutor + asyncio.run() to call the
async ExecutionTraceService (service → repository → DB pattern).
"""

import json
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Sequence
from uuid import UUID

from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult

logger = logging.getLogger(__name__)

# Map OTel span names (from CrewAIInstrumentor) to Kasal event_type strings.
# The instrumentor creates spans named like "CrewAI.crew.kickoff",
# "CrewAI.task.execute", "CrewAI.agent.execute", etc.
SPAN_NAME_MAP: Dict[str, str] = {
    # Crew lifecycle
    "CrewAI.crew.kickoff": "crew_started",
    "CrewAI.crew.complete": "crew_completed",
    # Task lifecycle
    "CrewAI.task.execute": "task_started",
    "CrewAI.task.complete": "task_completed",
    "CrewAI.task.fail": "task_failed",
    # Agent execution
    "CrewAI.agent.execute": "agent_execution",
    "CrewAI.agent.complete": "llm_response",
    # Tool usage
    "CrewAI.tool.execute": "tool_usage",
    "CrewAI.tool.complete": "tool_usage",
    "CrewAI.tool.error": "tool_error",
    # LLM calls
    "CrewAI.llm.call": "llm_call",
    "CrewAI.llm.complete": "llm_response",
    # Event bridge spans (kasal.* prefix) — event_types aligned with frontend
    "kasal.llm.call_started": "llm_call",
    "kasal.llm.call_completed": "llm_response",
    "kasal.llm.call_failed": "llm_call_failed",
    "kasal.memory.save_started": "memory_write_started",
    "kasal.memory.save_completed": "memory_write",
    "kasal.memory.query_started": "memory_retrieval_started",
    "kasal.memory.query_completed": "memory_retrieval",
    "kasal.memory.retrieval_completed": "memory_retrieval_completed",
    "kasal.knowledge.retrieval_started": "knowledge_retrieval_started",
    "kasal.knowledge.retrieval_completed": "knowledge_operation",
    "kasal.reasoning.started": "reasoning_started",
    "kasal.reasoning.completed": "agent_reasoning",
    "kasal.reasoning.failed": "agent_reasoning_error",
    "kasal.guardrail.started": "guardrail_started",
    "kasal.guardrail.completed": "llm_guardrail",
    "kasal.guardrail.failed": "llm_guardrail",
    "kasal.flow.started": "flow_started",
    "kasal.flow.finished": "flow_completed",
    "kasal.flow.created": "flow_created",
    "kasal.mcp.connection_started": "mcp_connection_started",
    "kasal.mcp.connection_completed": "mcp_connection_completed",
    "kasal.mcp.tool_started": "mcp_tool_started",
    "kasal.mcp.tool_completed": "mcp_tool_completed",
    "kasal.hitl.feedback_requested": "hitl_feedback_requested",
    "kasal.hitl.feedback_received": "hitl_feedback_received",
    # LLM retry backoff (emitted by DatabricksRetryLLM)
    "kasal.llm.retry": "llm_retry",
}


class UUIDEncoder(json.JSONEncoder):
    """JSON encoder that handles UUID serialization."""

    def default(self, obj: Any) -> Any:
        if isinstance(obj, UUID):
            return str(obj)
        return super().default(obj)


def _span_to_hex(span_id: int) -> str:
    """Convert a span/trace ID integer to a hex string."""
    return f"{span_id:016x}"


def _extract_event_type(span: ReadableSpan) -> str:
    """Determine Kasal event_type from span name or attributes."""
    # Check explicit kasal.event_type attribute first (manual spans)
    attrs = dict(span.attributes) if span.attributes else {}
    explicit = attrs.get("kasal.event_type")
    if explicit:
        return str(explicit)

    # Map from span name
    name = span.name or ""
    for prefix, event_type in SPAN_NAME_MAP.items():
        if name.startswith(prefix) or name == prefix:
            return event_type

    # Handle instrumentor spans like "AgentName._execute_core", "AgentName.execute_task"
    if "._execute_core" in name or ".execute_task" in name:
        return "agent_execution"

    # Handle crew kickoff spans like "CrewName.kickoff" (instrumentor root span)
    # Don't map to crew_started — the bridge already emits that.
    # Map to crew_execution so the frontend can use it for span hierarchy only.
    if name.endswith(".kickoff"):
        return "crew_execution"

    # Fallback: use the span name itself
    return name.replace(".", "_").lower() if name else "unknown"


def _extract_event_source(span: ReadableSpan) -> str:
    """Extract event_source (agent role) from span attributes.

    Checks CrewAI instrumentor attrs, OpenInference graph attrs, kasal bridge
    attrs, and falls back to span-name heuristics.
    """
    attrs = dict(span.attributes) if span.attributes else {}

    # CrewAI instrumentor + kasal bridge + OpenInference graph node
    for key in (
        "crewai.agent.role",
        "kasal.agent_name",
        "agent.role",
        "graph.node.id",
    ):
        val = attrs.get(key)
        if val:
            return str(val)

    # For crew/flow spans, use "crew" or "flow"
    name = span.name or ""
    if "crew" in name.lower():
        return "crew"
    if "flow" in name.lower():
        return "flow"
    # Crew kickoff inside a flow: span name is "<crew_name>.kickoff"
    if name.endswith(".kickoff"):
        return "crew"

    return "System"


def _extract_event_context(span: ReadableSpan) -> str:
    """Extract event_context (task description) from span attributes.

    Checks CrewAI instrumentor task attrs, formatted descriptions, kasal
    bridge attrs, and tool names as fallback.
    """
    attrs = dict(span.attributes) if span.attributes else {}

    for key in (
        "crewai.task.description",
        "kasal.task_name",
        "task.description",
        "formatted_description",
    ):
        val = attrs.get(key)
        if val:
            return str(val)[:500]

    # For tool spans — instrumentor uses tool.name
    tool_name = (
        attrs.get("tool.name")
        or attrs.get("crewai.tool.name")
        or attrs.get("kasal.tool_name")
    )
    if tool_name:
        return f"tool:{tool_name}"

    return span.name or ""


def _safe_json_parse(val: Any) -> Any:
    """Try to parse a string as JSON; return as-is if not JSON."""
    if isinstance(val, str):
        try:
            return json.loads(val)
        except (json.JSONDecodeError, ValueError):
            pass
    return val


def _extract_output(span: ReadableSpan) -> Any:
    """Build the output JSON from span attributes and events.

    Captures both kasal bridge attrs and CrewAI instrumentor attrs
    (output.value, input.value, tool results, memory data).
    """
    attrs = dict(span.attributes) if span.attributes else {}
    output: Dict[str, Any] = {}

    # Content: prefer kasal explicit → instrumentor output.value → crewai.output
    content = (
        attrs.get("kasal.output_content")
        or attrs.get("output.value")
        or attrs.get("crewai.output")
    )
    if content:
        output["content"] = str(content)

    # Input: instrumentor's input.value (task prompt, tool input, etc.)
    input_val = attrs.get("input.value")
    if input_val:
        output["input"] = str(input_val)

    # Add span timing
    if span.start_time and span.end_time:
        duration_ms = (span.end_time - span.start_time) / 1_000_000
        output["duration_ms"] = round(duration_ms, 2)

    # Tool-specific fields from instrumentor
    tool_name = attrs.get("tool.name")
    if tool_name:
        output["tool_name"] = str(tool_name)
    tool_desc = attrs.get("tool.description")
    if tool_desc:
        output["tool_description"] = str(tool_desc)[:300]

    # Memory-specific fields from instrumentor
    for prefix in ("long_term_memory.", "short_term_memory."):
        for suffix in ("save_time_ms", "query_time_ms", "source_type", "agent_role"):
            key = f"{prefix}{suffix}"
            val = attrs.get(key)
            if val is not None:
                output[suffix] = val

    # Add extra_data from kasal bridge attributes
    extra: Dict[str, Any] = {}
    for key, val in attrs.items():
        if key.startswith("kasal.extra."):
            extra[key[len("kasal.extra.") :]] = val
    if extra:
        output["extra_data"] = extra

    return output if output else {"content": span.name}


def _extract_trace_metadata(span: ReadableSpan) -> Dict[str, Any]:
    """Build trace_metadata from span attributes.

    Merges kasal bridge attrs with CrewAI instrumentor attrs to produce
    a rich metadata dict for downstream consumption.
    """
    attrs = dict(span.attributes) if span.attributes else {}
    metadata: Dict[str, Any] = {}

    # Kasal bridge attributes — capture ALL kasal.extra.* dynamically
    prefix = "kasal.extra."
    for key, val in attrs.items():
        if key.startswith(prefix) and val is not None:
            metadata[key[len(prefix) :]] = val

    # CrewAI instrumentor IDs
    for key, meta_key in (
        ("crew_key", "crew_key"),
        ("crew_id", "crew_id"),
        ("task_key", "task_key"),
        ("task_id", "task_id"),
        ("flow_id", "flow_id"),
    ):
        val = attrs.get(key)
        if val and meta_key not in metadata:
            metadata[meta_key] = str(val)

    # OpenInference span kind (AGENT, CHAIN, TOOL)
    span_kind = attrs.get("openinference.span.kind")
    if span_kind:
        metadata["span_kind"] = str(span_kind)

    # Graph hierarchy from instrumentor
    parent_agent = attrs.get("graph.node.parent_id")
    if parent_agent:
        metadata["parent_agent_role"] = str(parent_agent)

    # Tool metadata from instrumentor
    tool_params = attrs.get("tool.parameters")
    if tool_params:
        metadata["tool_parameters"] = _safe_json_parse(tool_params)

    # Crew-level rich data (JSON strings from instrumentor)
    for key in ("crew_agents", "crew_tasks", "crew_inputs", "flow_inputs"):
        val = attrs.get(key)
        if val:
            metadata[key] = _safe_json_parse(val)

    # Formatted task info from instrumentor
    for key in ("formatted_description", "formatted_expected_output"):
        val = attrs.get(key)
        if val:
            metadata[key] = str(val)

    return metadata


class KasalDBSpanExporter(SpanExporter):
    """Converts OTel ReadableSpan objects into execution_trace DB records.

    Each span is mapped to the same schema as existing _enqueue_trace() calls:
    job_id, event_source, event_context, event_type, output, trace_metadata,
    plus new OTel columns: span_id, trace_id, parent_span_id.

    Uses the service → repository → DB pattern via ExecutionTraceService.
    ThreadPoolExecutor + asyncio.run() provides non-blocking exports with
    a dedicated event loop per thread (safe since threads have no pre-existing loop).
    """

    def __init__(
        self,
        job_id: str,
        group_context: Optional[Any] = None,
        max_workers: int = 2,
    ):
        self._job_id = job_id
        self._group_context = group_context
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._total_exported = 0

        # Dedicated NullPool engine for thread-pool workers.
        # NullPool closes connections immediately when sessions end,
        # preventing MissingGreenlet errors that occur when aiosqlite
        # connections outlive the asyncio.run() event loop in worker threads.
        from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
        from sqlalchemy.pool import NullPool
        from src.config.settings import settings

        connect_args = {}
        if str(settings.DATABASE_URI).startswith("sqlite"):
            connect_args["check_same_thread"] = False

        self._thread_engine = create_async_engine(
            str(settings.DATABASE_URI),
            poolclass=NullPool,
            connect_args=connect_args,
        )
        self._thread_session_factory = async_sessionmaker(
            self._thread_engine,
            expire_on_commit=False,
            autoflush=False,
            autocommit=False,
        )

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        logger.info(
            f"[OTel-DB][{self._job_id}] export() called with {len(spans)} span(s)"
        )
        records = []
        for span in spans:
            try:
                record = self._span_to_record(span)
                if record:
                    records.append(record)
                    logger.info(
                        f"[OTel-DB][{self._job_id}] Span: name={span.name}, "
                        f"event_type={record['event_type']}"
                    )
            except Exception as e:
                logger.error(
                    f"[OTel-DB][{self._job_id}] Error converting span '{span.name}': {e}",
                    exc_info=True,
                )

        if records:
            self._executor.submit(self._write_batch, records)
            self._total_exported += len(records)
        else:
            logger.warning(
                f"[OTel-DB][{self._job_id}] export() produced 0 records from {len(spans)} spans"
            )

        return SpanExportResult.SUCCESS

    def _span_to_record(self, span: ReadableSpan) -> Optional[Dict[str, Any]]:
        """Convert a single span to a trace DB record dict."""
        event_type = _extract_event_type(span)

        # Build the record matching execution_trace columns
        record: Dict[str, Any] = {
            "job_id": self._job_id,
            "event_source": _extract_event_source(span),
            "event_context": _extract_event_context(span),
            "event_type": event_type,
            "output": _extract_output(span),
            "trace_metadata": _extract_trace_metadata(span),
            # OTel hierarchy columns
            "span_id": _span_to_hex(span.context.span_id),
            "trace_id": _span_to_hex(span.context.trace_id),
            "parent_span_id": (
                _span_to_hex(span.parent.span_id) if span.parent else None
            ),
            # OTel-native fields
            "span_name": span.name,
            "status_code": (span.status.status_code.name if span.status else "UNSET"),
            "duration_ms": (
                round((span.end_time - span.start_time) / 1_000_000)
                if span.start_time and span.end_time
                else None
            ),
        }

        # Group context
        if self._group_context:
            record["group_id"] = getattr(self._group_context, "primary_group_id", None)
            record["group_email"] = getattr(self._group_context, "group_email", None)

        return record

    def _write_batch(self, records: list) -> None:
        """Write a batch of records to the DB via the service layer (runs in thread pool).

        Creates a new asyncio event loop in the thread (safe — ThreadPoolExecutor
        threads never have a pre-existing loop) and uses ExecutionTraceService
        through a dedicated NullPool session factory.  NullPool ensures every
        connection is closed before asyncio.run() tears down the event loop,
        preventing MissingGreenlet errors from dangling aiosqlite connections.
        """
        import asyncio

        async def _write_async():
            from src.services.execution_trace_service import ExecutionTraceService

            written = 0
            async with self._thread_session_factory() as session:
                svc = ExecutionTraceService(session)
                for record in records:
                    try:
                        # Clean output for JSON serialization
                        output = record.get("output", {})
                        if output:
                            cleaned = json.loads(json.dumps(output, cls=UUIDEncoder))
                        else:
                            cleaned = {}

                        await svc.create_trace(
                            {
                                "job_id": record["job_id"],
                                "event_source": record["event_source"],
                                "event_context": record["event_context"],
                                "event_type": record["event_type"],
                                "output": cleaned,
                                "trace_metadata": record.get("trace_metadata", {}),
                                "span_id": record.get("span_id"),
                                "trace_id": record.get("trace_id"),
                                "parent_span_id": record.get("parent_span_id"),
                                "span_name": record.get("span_name"),
                                "status_code": record.get("status_code"),
                                "duration_ms": record.get("duration_ms"),
                                "group_id": record.get("group_id"),
                                "group_email": record.get("group_email"),
                            }
                        )
                        written += 1
                    except ValueError as ve:
                        logger.warning(
                            f"[OTel-DB][{self._job_id}] ValueError writing trace "
                            f"(job may not exist yet): {ve}"
                        )
                    except Exception as e:
                        logger.error(
                            f"[OTel-DB][{self._job_id}] Failed to write trace: {e}",
                            exc_info=True,
                        )

                # Explicit commit — not using get_db() DI which auto-commits
                if written:
                    await session.commit()

            if written:
                logger.info(
                    f"[OTel-DB][{self._job_id}] Wrote {written}/{len(records)} traces to DB"
                )
            elif records:
                logger.warning(
                    f"[OTel-DB][{self._job_id}] 0/{len(records)} traces written"
                )

        try:
            asyncio.run(_write_async())
        except Exception as e:
            logger.error(
                f"[OTel-DB][{self._job_id}] Batch write error: {e}",
                exc_info=True,
            )

    def shutdown(self) -> None:
        """Shutdown thread pool and log final count.

        Uses wait=False with a bounded join to avoid deadlocking the
        subprocess when a thread-pool task is stuck (e.g. on a Databricks
        embedding call).  A 10-second grace period is given for pending
        futures to drain before we move on.
        """
        logger.info(
            f"[OTel-DB][{self._job_id}] shutdown() called, "
            f"total_exported={self._total_exported}, waiting for thread pool..."
        )
        # Use wait=False so shutdown() returns immediately, then give
        # pending futures a bounded grace period to finish.
        self._executor.shutdown(wait=False, cancel_futures=True)
        # Wait up to 10 seconds for any in-flight futures to complete
        import time
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            # _executor._threads is empty once all workers have drained
            if not getattr(self._executor, "_threads", None):
                break
            time.sleep(0.2)
        logger.info(
            f"[OTel-DB][{self._job_id}] shutdown() complete, "
            f"exported {self._total_exported} spans total"
        )

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        return True
