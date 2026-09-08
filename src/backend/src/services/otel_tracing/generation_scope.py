"""Generation uses the same event bridge and durable trace exporter as runs."""

import asyncio
from contextlib import asynccontextmanager, contextmanager
from uuid import uuid4

from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor

from src.core.events.bus import current_event_context, event_bus, event_context
from src.core.events.types import TaskCompletedEvent, TaskFailedEvent, TaskStartedEvent
from src.services.otel_tracing.db_exporter import KasalDBSpanExporter
from src.services.otel_tracing.event_bridge import OTelEventBridge


@contextmanager
def generation_step(label):
    """Attribute a real generation stage to its own task in the existing trace."""
    if not current_event_context().get("generation_job_id"):
        yield
        return
    with event_context(task_name=label, task_id=str(uuid4())):
        event_bus.emit(None, TaskStartedEvent(context=label))
        try:
            yield
        except BaseException as exc:
            event_bus.emit(
                None, TaskFailedEvent(error=str(exc) or "Generation cancelled")
            )
            raise
        else:
            event_bus.emit(None, TaskCompletedEvent(output=f"{label} completed"))


@asynccontextmanager
async def generation_trace(job_id, group_context, label, step_name="Generate plan"):
    # An explicit provider is local to this generation, never the global provider.
    provider = TracerProvider(
        resource=Resource.create(
            {"service.name": "kasal-builder", "kasal.job_id": job_id}
        )
    )
    provider.add_span_processor(
        SimpleSpanProcessor(KasalDBSpanExporter(job_id, group_context))
    )
    bridge = OTelEventBridge(
        provider.get_tracer(__name__), job_id, group_context, scoped=True
    )
    bridge.register(event_bus)
    try:
        with event_context(
            generation_job_id=job_id,
            agent_role=label,
            task_name=step_name,
            task_id=job_id,
        ):
            event_bus.emit(None, TaskStartedEvent(context=step_name))
            try:
                yield
            except BaseException as exc:
                event_bus.emit(
                    None, TaskFailedEvent(error=str(exc) or "Generation cancelled")
                )
                raise
            else:
                event_bus.emit(None, TaskCompletedEvent(output="Generation completed"))
    finally:
        bridge.unregister()
        # Drain the exporter before terminal status: final answers must already
        # be readable when the conversation stops its live trace polling.
        await asyncio.to_thread(provider.force_flush)
        await asyncio.to_thread(provider.shutdown)
