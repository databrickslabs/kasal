"""POST /skills/drafts: the run's identity comes back before any model work,
so the chat can open the drafting call's trace while it runs."""

from unittest.mock import AsyncMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.api.skills_router import router
from src.dependencies.providers import get_group_context, get_smart_db_session
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


def test_editor_starts_a_draft_and_gets_its_job_id():
    with patch(
        "src.api.skills_router.draft_job.start", AsyncMock(return_value="job-7")
    ) as start:
        response = client("editor").post(
            "/skills/drafts",
            json={
                "request": "release notes",
                "transcript": [{"role": "user", "content": "hi"}],
                "model": "m",
            },
        )
    assert response.status_code == 202
    assert response.json() == {"job_id": "job-7"}
    args, kwargs = start.await_args
    assert args[0] == "release notes"
    assert args[1].primary_group_id == "team"  # the caller's workspace owns the run
    assert kwargs["transcript"] == [{"role": "user", "content": "hi"}]
    assert kwargs["model"] == "m"


def test_operator_cannot_start_a_draft():
    with patch("src.api.skills_router.draft_job.start", AsyncMock()) as start:
        response = client("operator").post("/skills/drafts", json={"request": "x"})
    assert response.status_code == 403
    start.assert_not_awaited()
