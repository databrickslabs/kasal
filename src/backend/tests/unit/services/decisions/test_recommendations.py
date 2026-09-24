from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest

from src.services.decisions.contracts import Choice
from src.services.decisions.recommendations import recommend


@pytest.mark.asyncio
async def test_recommendations_are_scoped_to_enabled_models_and_do_not_persist():
    models = [
        SimpleNamespace(
            key="enabled-model",
            name="Model",
            provider="local",
            context_window=32000,
            max_output_tokens=8000,
        )
    ]
    service = Mock(find_enabled_models_for_group=AsyncMock(return_value=models))
    context = SimpleNamespace(primary_group_id="one")
    session = Mock()
    with (
        patch(
            "src.services.decisions.recommendations.ModelConfigService",
            return_value=service,
        ),
        patch(
            "src.services.decisions.recommendations.decide",
            new=AsyncMock(
                return_value={
                    "model": Choice("0", 0.99, {"0": 0.99}),
                    "effort": Choice("low", 0.99, {"low": 0.99}),
                }
            ),
        ) as decide,
    ):
        result = await recommend(session, context, "edit a sentence")
    assert result.model == "enabled-model" and result.effort == "low"
    service.find_enabled_models_for_group.assert_awaited_once_with(context)
    assert decide.call_args.kwargs["group_id"] == "one"
    session.commit.assert_not_called()
