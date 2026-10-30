"""A chat message asking for Auto is resolved once, before any model call."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.schemas.dispatcher import DispatcherRequest
from src.services.chat import auto_model
from src.services.chat.auto_model import resolve_dispatch_model
from src.services.decisions.model_selection import ModelSelection

PICK = ModelSelection("databricks-gpt-5-5", "selected", 1.0)


@pytest.mark.asyncio
async def test_an_explicit_model_is_never_second_guessed():
    request = DispatcherRequest(message="hi", model="databricks-claude-opus-5-5")
    with patch.object(auto_model, "select_for_workspace") as select:
        assert await resolve_dispatch_model(request, MagicMock(), MagicMock()) is None
    select.assert_not_called()
    assert request.model == "databricks-claude-opus-5-5"


@pytest.mark.asyncio
async def test_no_model_stays_no_model():
    request = DispatcherRequest(message="hi")
    assert await resolve_dispatch_model(request, MagicMock(), MagicMock()) is None
    assert request.model is None


@pytest.mark.asyncio
async def test_auto_becomes_a_concrete_model_from_the_clean_prompt():
    request = DispatcherRequest(
        message="hi [hidden steering]", original_prompt="hi", model="auto"
    )
    session, context = MagicMock(), MagicMock()
    with patch.object(
        auto_model, "select_for_workspace", new=AsyncMock(return_value=PICK)
    ) as select:
        result = await resolve_dispatch_model(request, session, context)
    select.assert_awaited_once_with(session, context, "hi")
    assert result is PICK and request.model == PICK.model


@pytest.mark.asyncio
async def test_router_reports_the_pick_and_the_service_never_sees_auto():
    import importlib

    dispatcher_router = importlib.import_module("src.api.dispatcher_router")

    seen = {}

    async def dispatch(request, group_context, available_tools=None):
        seen["model"] = request.model
        return {"dispatcher": {}, "generation_result": None}

    service = MagicMock(dispatch=AsyncMock(side_effect=dispatch))
    with (
        patch.object(
            dispatcher_router,
            "resolve_dispatch_model",
            new=AsyncMock(
                side_effect=lambda r, s, g: setattr(r, "model", PICK.model) or PICK
            ),
        ),
        patch.object(
            dispatcher_router.DispatcherService, "create", return_value=service
        ),
        patch.object(
            dispatcher_router, "_fetch_available_tools", new=AsyncMock(return_value=[])
        ),
    ):
        result = await dispatcher_router.dispatch_request(
            DispatcherRequest(message="hi", model="auto"),
            MagicMock(access_token=None),
            MagicMock(),
        )
    assert seen["model"] == PICK.model
    assert result["model_selection"] == {
        "requested": "auto",
        "model": PICK.model,
        "status": "selected",
        "reason": None,
        "connection": None,
    }


@pytest.mark.asyncio
async def test_detect_intent_resolves_auto_before_its_model_call():
    """/detect-intent used to hand "auto" straight to intent detection."""
    import importlib

    dispatcher_router = importlib.import_module("src.api.dispatcher_router")
    seen = {}

    async def detect(service, request, group_context, tools, default_model):
        seen["model"] = request.model
        return {
            "intent": "generate_agent",
            "confidence": 0.9,
            "extracted_info": {},
            "suggested_prompt": "p",
        }

    with (
        patch.object(
            dispatcher_router,
            "resolve_dispatch_model",
            new=AsyncMock(
                side_effect=lambda r, s, g: setattr(r, "model", PICK.model) or PICK
            ),
        ) as resolve,
        patch.object(dispatcher_router.DispatcherService, "create"),
        patch.object(
            dispatcher_router, "_fetch_available_tools", new=AsyncMock(return_value=[])
        ),
        patch.object(
            dispatcher_router,
            "detect_request_intent",
            new=AsyncMock(side_effect=detect),
        ),
    ):
        await dispatcher_router.detect_intent_only(
            DispatcherRequest(message="hi", model="auto"),
            MagicMock(access_token=None),
            MagicMock(),
        )
    resolve.assert_awaited_once()
    assert seen["model"] == PICK.model
