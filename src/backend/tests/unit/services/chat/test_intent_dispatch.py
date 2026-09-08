"""Picker policy and live classification attribution in builder generation."""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from src.core.cache import intent_cache
from src.core.events.bus import current_event_context, event_bus
from src.core.events.types import LLMCallCompletedEvent, LLMCallStartedEvent
from src.schemas.dispatcher import DispatcherRequest
from src.services.chat import dispatcher
from src.services.chat.intent_dispatch import detect_request_intent
from src.services.otel_tracing import generation_scope


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "options,expected",
    [
        ({"model": "selected"}, "selected"),
        ({}, "default"),
        ({"model": "selected", "chat_mode_type": "chat"}, "selected"),
        ({"model": "selected", "chat_mode": True}, "default"),
        ({"model": "selected", "auto_execute": True}, "default"),
        ({"model": "selected", "prefer_existing": True}, "selected"),
    ],
)
async def test_request_model_policy_preserves_surface_flags(options, expected):
    service = AsyncMock()
    request = DispatcherRequest(message="Build a crew", **options)
    group = SimpleNamespace(primary_group_id="g1")
    tools = [{"title": "Search", "description": "Find sources"}]
    result = await detect_request_intent(service, request, group, tools, "default")
    service.detect_intent_logged.assert_awaited_once_with(
        request.message,
        expected,
        group,
        tools,
        chat_mode=request.chat_mode,
        last_resort_model=request.model,
        prefer_existing=request.prefer_existing,
    )
    assert result is service.detect_intent_logged.return_value


@pytest.mark.asyncio
@pytest.mark.parametrize("fail_selected", [False, True])
async def test_builder_classification_trace_uses_selected_model_then_fallback(
    monkeypatch, fail_selected
):
    monkeypatch.setattr(dispatcher, "_HAS_MLFLOW", False)
    monkeypatch.setattr(dispatcher, "DISPATCHER_FALLBACK_MODELS", ["fallback"])
    monkeypatch.setattr(dispatcher.DispatcherService, "_intent_failures", {})
    monkeypatch.setattr(dispatcher.DispatcherService, "_concurrency_semaphore", None)
    monkeypatch.setattr(intent_cache, "_cache", {})
    exporter = InMemorySpanExporter()
    monkeypatch.setattr(generation_scope, "KasalDBSpanExporter", lambda *args: exporter)
    service = dispatcher.DispatcherService.__new__(dispatcher.DispatcherService)
    service.template_service = AsyncMock()
    service.template_service.get_template_content.return_value = "Classify the request"
    service._log_llm_interaction = AsyncMock()
    calls = []

    async def complete(messages, model, **kwargs):
        calls.append(model)
        if model == "selected" and fail_selected:
            raise RuntimeError("Endpoint unavailable")
        event_bus.emit(None, LLMCallStartedEvent(model=model, messages=messages))
        # The UI can read the named request while the answer is still pending.
        live = exporter.get_finished_spans()[-1]
        assert live.attributes["kasal.event_type"] == "llm_call"
        assert live.attributes["kasal.task_name"] == "Understand request"
        response = json.dumps({"intent": "generate_crew", "confidence": 0.99})
        event_bus.emit(
            None,
            LLMCallCompletedEvent(call_type="llm_call", model=model, response=response),
        )
        return response, model

    service._call_llm_with_retry = complete
    request = DispatcherRequest(message="Gather news", model="selected")
    group = SimpleNamespace(primary_group_id="g1", group_email="user@example.com")
    async with generation_scope.generation_trace(
        "design", group, "Agent Builder", "Create the crew plan"
    ):
        result = await detect_request_intent(service, request, group, None, "default")
        # Subsequent plan/agent/task calls belong to the original generation step.
        assert current_event_context()["task_name"] == "Create the crew plan"
        assert current_event_context()["task_id"] == "design"
        assert current_event_context()["generation_job_id"] == "design"
        before_cache = len(exporter.get_finished_spans())
        await detect_request_intent(service, request, group, None, "default")
        assert len(exporter.get_finished_spans()) == before_cache

    served = "fallback" if fail_selected else "selected"
    assert calls == (["selected", "fallback"] if fail_selected else ["selected"])
    assert result["model"] == served
    assert service._log_llm_interaction.call_args.kwargs["model"] == served
    spans = exporter.get_finished_spans()
    classification = [
        span
        for span in spans
        if span.attributes.get("kasal.task_name") == "Understand request"
    ]
    assert [span.attributes["kasal.event_type"] for span in classification] == [
        "task_started",
        "llm_call",
        "llm_response",
        "task_completed",
    ]
    assert len({span.attributes["kasal.extra.task_id"] for span in classification}) == 1
    assert classification[0].attributes["kasal.extra.task_id"] != "design"


def test_generation_step_is_inert_outside_builder_generation():
    context = dict(current_event_context())
    with generation_scope.generation_step("Understand request"):
        assert current_event_context() == context


@pytest.mark.asyncio
async def test_generation_step_restores_parent_on_cancellation(monkeypatch):
    import asyncio

    exporter = InMemorySpanExporter()
    monkeypatch.setattr(generation_scope, "KasalDBSpanExporter", lambda *args: exporter)
    group = SimpleNamespace(primary_group_id="g1", group_email="user@example.com")
    async with generation_scope.generation_trace("design", group, "Agent Builder"):
        with pytest.raises(asyncio.CancelledError):
            with generation_scope.generation_step("Understand request"):
                raise asyncio.CancelledError()
        assert current_event_context()["task_id"] == "design"
    failed = [
        span
        for span in exporter.get_finished_spans()
        if span.attributes.get("kasal.event_type") == "task_failed"
    ]
    assert len(failed) == 1
    assert failed[0].attributes["kasal.task_name"] == "Understand request"
