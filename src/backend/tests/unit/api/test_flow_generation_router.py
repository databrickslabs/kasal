from unittest.mock import AsyncMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.api.flows_router import router
from src.dependencies.providers import get_group_context, get_smart_db_session
from src.schemas.flow_generation import FlowGenerationResponse
from src.utils.user_context import GroupContext
from tests.unit.api.conftest import register_exception_handlers


def client(role):
    app = FastAPI()
    app.include_router(router)
    register_exception_handlers(app)
    app.dependency_overrides[get_group_context] = lambda: GroupContext(
        group_ids=["team"], group_email="user@example.com", user_role=role
    )
    app.dependency_overrides[get_smart_db_session] = lambda: AsyncMock()
    return TestClient(app)


def test_operator_cannot_generate():
    with patch("src.api.flows_router.FlowGenerationService") as service:
        response = client("operator").post(
            "/flows/generate", json={"prompt": "Research"}
        )
    assert response.status_code == 403
    service.assert_not_called()


def test_editor_can_generate_without_saving_or_executing():
    with patch("src.api.flows_router.FlowGenerationService") as service:
        service.return_value.generate = AsyncMock(
            return_value=FlowGenerationResponse(
                name="Draft", message="Needs a saved crew"
            )
        )
        response = client("editor").post("/flows/generate", json={"prompt": "Research"})
    assert response.status_code == 200
    assert response.json()["name"] == "Draft"
    service.return_value.generate.assert_awaited_once()


def test_blank_prompt_rejected_before_generation():
    with patch("src.api.flows_router.FlowGenerationService") as service:
        response = client("editor").post("/flows/generate", json={"prompt": "   "})
    assert response.status_code == 422
    service.assert_not_called()
