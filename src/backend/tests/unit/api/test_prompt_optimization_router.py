"""
Unit tests for the prompt-optimization API router.

The router is a thin boundary: every handler builds the service, translates
ValueError → BadRequestError (client input) or NotFoundError (missing runs),
and wraps results in response schemas. Handlers are invoked directly with a
patched service, mirroring the other router unit tests.
"""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# NOTE: both `from src.api import prompt_optimization_router` and
# `import src.api.prompt_optimization_router as m` yield the APIRouter object —
# the package __init__ rebinds the module name to the router, and the `as m`
# form resolves through that shadowed package attribute. importlib returns the
# actual module from sys.modules.
import importlib

router_module = importlib.import_module("src.api.prompt_optimization_router")
from src.api.prompt_optimization_router import (
    add_eval_feedback,
    apply_run,
    assign_judge,
    cancel_run,
    create_judge,
    delete_judge,
    get_run,
    list_crew_evals,
    list_judges,
    list_runs,
    router,
    start_crew_optimization,
    start_optimization,
    update_judge,
)
from src.core.exceptions import BadRequestError, NotFoundError
from src.schemas.prompt_optimization import (
    CrewOptimizationRequest,
    PromptOptimizationRequest,
)


def _group(token=None):
    context = MagicMock()
    context.access_token = token
    context.primary_group_id = "grp1"
    return context


RUN = {
    "run_id": "abc123",
    "template_name": "detect_intent",
    "status": "completed",
    "dataset_size": 5,
    "initial_score": 0.5,
    "final_score": 0.9,
    "applied": False,
    "created_at": datetime.now(timezone.utc),
    "human_feedback_count": 3,
    "candidates_tried": 4,
}


@pytest.fixture()
def service():
    svc = MagicMock()
    with patch.object(
        router_module, "PromptOptimizationService", return_value=svc
    ):
        yield svc


class TestRouterConfiguration:
    def test_prefix_and_tags(self):
        assert router.prefix == "/prompt-optimization"
        assert "prompt optimization" in router.tags


class TestStartEndpoints:
    @pytest.mark.asyncio
    async def test_start_optimization_success(self, service):
        service.start_optimization = AsyncMock(
            return_value={"run_id": "r1", "status": "pending", "dataset_size": 5}
        )
        request = PromptOptimizationRequest(
            template_name="detect_intent", examples=["a", "b", "c", "d", "e"]
        )
        response = await start_optimization(request, _group(), MagicMock())
        assert response.run_id == "r1"
        assert response.dataset_size == 5

    @pytest.mark.asyncio
    async def test_start_optimization_value_error_becomes_400(self, service):
        service.start_optimization = AsyncMock(
            side_effect=ValueError("need at least 4 examples")
        )
        request = PromptOptimizationRequest(
            template_name="detect_intent", examples=["a"] * 5
        )
        with pytest.raises(BadRequestError, match="at least 4"):
            await start_optimization(request, _group(), MagicMock())

    @pytest.mark.asyncio
    async def test_start_crew_optimization_success(self, service):
        service.start_crew_optimization = AsyncMock(
            return_value={"run_id": "r2", "status": "pending", "dataset_size": 1}
        )
        request = CrewOptimizationRequest(crew_id="c1")
        response = await start_crew_optimization(request, _group(), MagicMock())
        assert response.run_id == "r2"
        assert response.dataset_size == 1

    @pytest.mark.asyncio
    async def test_start_crew_optimization_value_error_becomes_400(self, service):
        service.start_crew_optimization = AsyncMock(
            side_effect=ValueError("crew not found")
        )
        with pytest.raises(BadRequestError, match="crew not found"):
            await start_crew_optimization(
                CrewOptimizationRequest(crew_id="c1"), _group(), MagicMock()
            )

    @pytest.mark.asyncio
    async def test_start_publishes_user_context(self, service):
        service.start_optimization = AsyncMock(
            return_value={"run_id": "r1", "status": "pending", "dataset_size": 5}
        )
        request = PromptOptimizationRequest(
            template_name="detect_intent", examples=["a"] * 5
        )
        with patch("src.utils.user_context.UserContext") as user_context:
            await start_optimization(request, _group(token="tok"), MagicMock())
            user_context.set_group_context.assert_called_once()
            user_context.set_user_token.assert_called_once_with("tok")


class TestCrewOptimizationRequestBounds:
    def test_budget_defaults_and_bounds(self):
        assert CrewOptimizationRequest(crew_id="c").max_metric_calls == 10
        assert (
            CrewOptimizationRequest(crew_id="c", max_metric_calls=4).max_metric_calls
            == 4
        )
        with pytest.raises(ValueError):
            CrewOptimizationRequest(crew_id="c", max_metric_calls=3)
        with pytest.raises(ValueError):
            CrewOptimizationRequest(crew_id="c", max_metric_calls=41)

    def test_template_name_is_a_closed_set(self):
        with pytest.raises(ValueError):
            PromptOptimizationRequest(template_name="not_a_template")


class TestEvalEndpoints:
    @pytest.mark.asyncio
    async def test_list_crew_evals(self, service):
        service.list_crew_evals = AsyncMock(return_value=[{"trace_id": "t1"}])
        result = await list_crew_evals("crew1", _group(), MagicMock())
        assert result == {"evals": [{"trace_id": "t1"}]}
        service.list_crew_evals.assert_awaited_once_with("crew1")

    @pytest.mark.asyncio
    async def test_add_eval_feedback_coerces_numeric_value(self, service):
        service.add_eval_feedback = AsyncMock(return_value=True)
        result = await add_eval_feedback(
            "t1",
            {"value": "7", "comment": "good", "expectation": "german side"},
            _group(),
            MagicMock(),
        )
        assert result == {"ok": True}
        service.add_eval_feedback.assert_awaited_once_with(
            "t1", 7.0, "good", "german side"
        )

    @pytest.mark.asyncio
    async def test_add_eval_feedback_rejects_non_numeric_value(self, service):
        with pytest.raises(BadRequestError, match="number"):
            await add_eval_feedback(
                "t1", {"value": "not-a-number"}, _group(), MagicMock()
            )

    @pytest.mark.asyncio
    async def test_add_eval_feedback_service_error_becomes_400(self, service):
        service.add_eval_feedback = AsyncMock(
            side_effect=ValueError("local MLflow required")
        )
        with pytest.raises(BadRequestError, match="local MLflow"):
            await add_eval_feedback("t1", {"value": 5}, _group(), MagicMock())


class TestJudgeEndpoints:
    @pytest.mark.asyncio
    async def test_list_judges(self, service):
        service.list_judges = AsyncMock(return_value=[{"name": "acc"}])
        result = await list_judges(_group(), MagicMock())
        assert result == {"judges": [{"name": "acc"}]}

    @pytest.mark.asyncio
    async def test_create_judge_passes_fields(self, service):
        service.create_judge = AsyncMock(return_value={"name": "acc"})
        body = {
            "name": "acc",
            "instructions": "criteria",
            "model": "qwen",
            "crew_id": "c1",
        }
        result = await create_judge(body, _group(), MagicMock())
        assert result == {"name": "acc"}
        kwargs = service.create_judge.await_args.kwargs
        assert kwargs["name"] == "acc"
        assert kwargs["instructions"] == "criteria"
        assert kwargs["model"] == "qwen"
        assert kwargs["crew_id"] == "c1"

    @pytest.mark.asyncio
    async def test_create_judge_error_becomes_400(self, service):
        service.create_judge = AsyncMock(side_effect=ValueError("name required"))
        with pytest.raises(BadRequestError, match="name required"):
            await create_judge({"name": ""}, _group(), MagicMock())

    @pytest.mark.asyncio
    async def test_assign_judge_requires_crew_id(self, service):
        with pytest.raises(BadRequestError, match="crew_id"):
            await assign_judge("acc", {}, _group(), MagicMock())

    @pytest.mark.asyncio
    async def test_assign_judge_passes_through(self, service):
        service.assign_judge = AsyncMock(return_value={"full_name": "crew_x__acc"})
        result = await assign_judge("acc", {"crew_id": "c1"}, _group(), MagicMock())
        assert result["full_name"] == "crew_x__acc"

    @pytest.mark.asyncio
    async def test_update_judge_passes_changes(self, service):
        service.update_judge = AsyncMock(return_value={"name": "acc"})
        result = await update_judge(
            "acc", {"instructions": "new", "model": "qwen"}, _group(), MagicMock()
        )
        assert result == {"name": "acc"}
        kwargs = service.update_judge.await_args.kwargs
        assert kwargs["name"] == "acc"
        assert kwargs["instructions"] == "new"
        assert kwargs["model"] == "qwen"

    @pytest.mark.asyncio
    async def test_update_judge_error_becomes_400(self, service):
        service.update_judge = AsyncMock(side_effect=ValueError("Nothing to update"))
        with pytest.raises(BadRequestError, match="Nothing to update"):
            await update_judge("acc", {}, _group(), MagicMock())

    @pytest.mark.asyncio
    async def test_delete_judge(self, service):
        service.delete_judge = AsyncMock(return_value=True)
        assert await delete_judge("acc", _group(), MagicMock()) == {"ok": True}

    @pytest.mark.asyncio
    async def test_delete_judge_error_becomes_400(self, service):
        service.delete_judge = AsyncMock(side_effect=ValueError("local MLflow"))
        with pytest.raises(BadRequestError):
            await delete_judge("acc", _group(), MagicMock())


class TestRunEndpoints:
    @pytest.mark.asyncio
    async def test_list_runs_wraps_status_models(self, service):
        service.list_runs = MagicMock(return_value=[RUN])
        response = await list_runs(_group(), MagicMock())
        assert len(response.runs) == 1
        status = response.runs[0]
        assert status.run_id == "abc123"
        assert status.human_feedback_count == 3
        assert status.candidates_tried == 4

    @pytest.mark.asyncio
    async def test_get_run_found(self, service):
        service.get_run = MagicMock(return_value=RUN)
        status = await get_run("abc123", _group(), MagicMock())
        assert status.final_score == 0.9

    @pytest.mark.asyncio
    async def test_get_run_missing_is_404(self, service):
        service.get_run = MagicMock(return_value=None)
        with pytest.raises(NotFoundError, match="abc123"):
            await get_run("abc123", _group(), MagicMock())

    @pytest.mark.asyncio
    async def test_cancel_run_passthrough_and_404(self, service):
        service.cancel_run = MagicMock(
            return_value={"run_id": "abc123", "cancelling": True}
        )
        assert (await cancel_run("abc123", _group(), MagicMock()))["cancelling"]
        service.cancel_run = MagicMock(side_effect=ValueError("not found"))
        with pytest.raises(NotFoundError):
            await cancel_run("abc123", _group(), MagicMock())

    @pytest.mark.asyncio
    async def test_apply_run_success_and_404(self, service):
        service.apply_run = AsyncMock(
            return_value={
                "run_id": "abc123",
                "template_name": "detect_intent",
                "applied": True,
            }
        )
        response = await apply_run("abc123", _group(), MagicMock())
        assert response.applied is True
        service.apply_run = AsyncMock(side_effect=ValueError("no completed proposal"))
        with pytest.raises(NotFoundError):
            await apply_run("abc123", _group(), MagicMock())
